"""Source-only Tushare event raw ingestion for unified event signals."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib
import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional
from uuid import UUID

import psycopg2.extras
from dotenv import load_dotenv

from backend.db.init_tushare_event_raw_schema import init_tushare_event_raw_schema
from backend.db.pg_pool import get_conn
from backend.services.tushare_rate_limiter import get_limiter


ROOT = Path(__file__).resolve().parents[3]

FORECAST_FIELDS = (
    "ts_code",
    "ann_date",
    "end_date",
    "type",
    "p_change_min",
    "p_change_max",
    "net_profit_min",
    "net_profit_max",
    "last_parent_net",
    "first_ann_date",
    "summary",
    "change_reason",
)

EXPRESS_FIELDS = (
    "ts_code",
    "ann_date",
    "end_date",
    "revenue",
    "operate_profit",
    "total_profit",
    "n_income",
    "total_assets",
    "total_hldr_eqy_exc_min_int",
    "diluted_eps",
    "diluted_roe",
    "yoy_net_profit",
    "bps",
    "yoy_sales",
    "yoy_op",
    "yoy_tp",
    "yoy_dedu_np",
    "yoy_eps",
    "yoy_roe",
    "growth_assets",
    "yoy_equity",
    "growth_bps",
    "or_last_year",
    "op_last_year",
    "tp_last_year",
    "np_last_year",
    "eps_last_year",
    "open_net_assets",
    "open_bps",
    "perf_summary",
    "is_audit",
    "remark",
)

FINA_INDICATOR_CORE_FIELDS = (
    "ts_code",
    "ann_date",
    "end_date",
    "eps",
    "dt_eps",
    "bps",
    "roe",
    "roe_waa",
    "roe_dt",
    "roa",
    "grossprofit_margin",
    "netprofit_margin",
    "profit_to_gr",
    "debt_to_assets",
    "current_ratio",
    "quick_ratio",
    "ocfps",
    "netprofit_yoy",
    "dt_netprofit_yoy",
    "tr_yoy",
    "or_yoy",
    "op_yoy",
    "ebt_yoy",
    "ocf_yoy",
    "q_sales_yoy",
    "q_op_yoy",
    "q_profit_yoy",
    "q_netprofit_yoy",
    "q_ocf_to_sales",
    "update_flag",
)


@dataclass(frozen=True)
class RawDatasetConfig:
    name: str
    table_name: str
    default_api: str
    vip_api: str
    fields: tuple[str, ...]
    record_key_extra_fields: tuple[str, ...] = ()
    rate_per_minute: int = 200


DATASET_CONFIGS: dict[str, RawDatasetConfig] = {
    "forecast": RawDatasetConfig(
        name="tushare_forecast_raw",
        table_name="market.tushare_forecast_raw",
        default_api="forecast",
        vip_api="forecast_vip",
        fields=FORECAST_FIELDS,
        record_key_extra_fields=("type", "first_ann_date"),
    ),
    "express": RawDatasetConfig(
        name="tushare_express_raw",
        table_name="market.tushare_express_raw",
        default_api="express",
        vip_api="express_vip",
        fields=EXPRESS_FIELDS,
    ),
    "fina_indicator": RawDatasetConfig(
        name="tushare_fina_indicator_raw",
        table_name="market.tushare_fina_indicator_raw",
        default_api="fina_indicator",
        vip_api="fina_indicator_vip",
        fields=FINA_INDICATOR_CORE_FIELDS,
    ),
}


@dataclass(frozen=True)
class RawSyncSummary:
    dataset: str
    source_api: str
    period: str
    fetched_rows: int
    written_rows: int
    skipped_rows: int


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        import pandas as pd  # type: ignore

        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return a JSON-stable source row without pandas/numpy null sentinels."""

    return {str(key): _clean_value(value) for key, value in row.items()}


def parse_tushare_date(value: Any) -> dt.date:
    cleaned = _clean_value(value)
    if cleaned is None:
        raise ValueError("missing Tushare date")
    text = str(cleaned).strip()
    if len(text) == 8 and text.isdigit():
        return dt.date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return dt.date.fromisoformat(text[:10])


def source_row_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def source_record_key(config: RawDatasetConfig, payload: dict[str, Any]) -> str:
    pieces = [
        config.name,
        str(payload.get("ts_code") or ""),
        str(payload.get("ann_date") or ""),
        str(payload.get("end_date") or ""),
    ]
    for field in config.record_key_extra_fields:
        value = payload.get(field)
        if value not in (None, ""):
            pieces.append(f"{field}={value}")
    return ":".join(pieces)


def _pro_api():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts = importlib.import_module("tushare")
    return ts.pro_api(token)


def dataframe_to_rows(df: Any) -> list[dict[str, Any]]:
    if df is None:
        return []
    empty = getattr(df, "empty", False)
    if empty:
        return []
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        rows.append(normalize_row(dict(row)))
    return rows


def fetch_period_rows(
    pro: Any,
    config: RawDatasetConfig,
    *,
    period: str,
    source_api: Optional[str] = None,
    fields: Optional[Iterable[str]] = None,
) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    api_name = source_api or config.vip_api
    fetch_params: dict[str, Any] = {"period": period}
    field_list = tuple(fields) if fields is not None else config.fields
    if field_list:
        fetch_params["fields"] = ",".join(field_list)

    limiter = get_limiter(api_name, config.rate_per_minute)
    limiter.acquire()
    api_fn = getattr(pro, api_name)
    df = api_fn(**fetch_params)
    return api_name, fetch_params, dataframe_to_rows(df)


def build_raw_values(
    config: RawDatasetConfig,
    rows: list[dict[str, Any]],
    *,
    source_api: str,
    fetch_params: dict[str, Any],
    observed_at: dt.datetime,
    job_id: Optional[UUID] = None,
) -> tuple[list[tuple[Any, ...]], int]:
    values: list[tuple[Any, ...]] = []
    skipped = 0
    seen_keys: set[tuple[str, str]] = set()
    for row in rows:
        payload = normalize_row(row)
        try:
            ann_date = parse_tushare_date(payload.get("ann_date"))
            report_period = parse_tushare_date(payload.get("end_date"))
            ts_code = str(payload["ts_code"])
        except Exception:
            skipped += 1
            continue
        record_key = source_record_key(config, payload)
        row_hash = source_row_hash(payload)
        dedupe_key = (record_key, row_hash)
        if dedupe_key in seen_keys:
            skipped += 1
            continue
        seen_keys.add(dedupe_key)
        values.append(
            (
                source_api,
                psycopg2.extras.Json(fetch_params, dumps=_json_dumps),
                record_key,
                ts_code,
                ann_date,
                report_period,
                row_hash,
                psycopg2.extras.Json(payload, dumps=_json_dumps),
                observed_at,
                job_id,
                job_id,
                job_id,
            )
        )
    return values, skipped


def upsert_raw_rows(
    conn: Any,
    config: RawDatasetConfig,
    rows: list[dict[str, Any]],
    *,
    source_api: str,
    fetch_params: dict[str, Any],
    observed_at: Optional[dt.datetime] = None,
    job_id: Optional[UUID] = None,
) -> tuple[int, int]:
    observed = observed_at or dt.datetime.now(dt.timezone.utc)
    values, skipped = build_raw_values(
        config,
        rows,
        source_api=source_api,
        fetch_params=fetch_params,
        observed_at=observed,
        job_id=job_id,
    )
    if not values:
        return 0, skipped

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"""
            INSERT INTO {config.table_name}
                (
                    source_api, fetch_params, source_record_key, ts_code,
                    ann_date, report_period, source_row_hash, raw_payload,
                    observed_at, first_seen_job_id, last_seen_job_id, job_id
                )
            VALUES %s
            ON CONFLICT (source_record_key, source_row_hash) DO UPDATE SET
                source_api = EXCLUDED.source_api,
                fetch_params = EXCLUDED.fetch_params,
                last_seen_at = NOW(),
                observed_at = EXCLUDED.observed_at,
                last_seen_job_id = EXCLUDED.last_seen_job_id,
                job_id = EXCLUDED.job_id,
                updated_at = NOW()
            """,
            values,
            page_size=1000,
        )
    return len(values), skipped


class TushareEventRawSyncService:
    """Fetch Tushare financial event source rows and store raw JSON versions."""

    def __init__(self, pro: Any | None = None) -> None:
        self._pro = pro

    @property
    def pro(self) -> Any:
        if self._pro is None:
            self._pro = _pro_api()
        return self._pro

    def sync_period(
        self,
        dataset: str,
        *,
        period: str,
        source_api: Optional[str] = None,
        ensure_schema: bool = True,
        job_id: Optional[UUID] = None,
    ) -> RawSyncSummary:
        config = DATASET_CONFIGS[dataset]
        if ensure_schema:
            init_tushare_event_raw_schema()

        api_name, fetch_params, rows = fetch_period_rows(
            self.pro,
            config,
            period=period,
            source_api=source_api,
        )
        with get_conn() as conn:
            written, skipped = upsert_raw_rows(
                conn,
                config,
                rows,
                source_api=api_name,
                fetch_params=fetch_params,
                job_id=job_id,
            )
        return RawSyncSummary(
            dataset=config.name,
            source_api=api_name,
            period=period,
            fetched_rows=len(rows),
            written_rows=written,
            skipped_rows=skipped,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Tushare event raw rows by report period")
    parser.add_argument(
        "--dataset",
        choices=sorted(DATASET_CONFIGS),
        required=True,
        help="Logical dataset: forecast, express, or fina_indicator",
    )
    parser.add_argument("--period", required=True, help="Report period end date, for example 20231231")
    parser.add_argument("--source-api", default=None, help="Override API, for example forecast_vip")
    parser.add_argument("--no-ensure-schema", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(ROOT / ".env", override=True)
    load_dotenv(override=False)
    service = TushareEventRawSyncService()
    summary = service.sync_period(
        args.dataset,
        period=args.period,
        source_api=args.source_api,
        ensure_schema=not args.no_ensure_schema,
    )
    print(_json_dumps(summary.__dict__))
    return 0


if __name__ == "__main__":
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    raise SystemExit(main())
