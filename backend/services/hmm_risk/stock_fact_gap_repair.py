from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from psycopg2.extras import RealDictCursor, execute_values

PLAN_SCHEMA = "hmm_risk_b3_stock_fact_gap_repair_plan_v2"
RECEIPT_SCHEMA = "hmm_risk_b3_stock_fact_gap_repair_receipt_v1"
ROLLBACK_SCHEMA = "hmm_risk_b3_stock_fact_gap_repair_rollback_v1"
READBACK_SCHEMA = "hmm_risk_b3_stock_fact_gap_repair_readback_v1"
CONFIRM_APPLY = "APPLY_HMM_B3_STOCK_FACT_GAP_REPAIR"
CONFIRM_ROLLBACK = "ROLLBACK_HMM_B3_STOCK_FACT_GAP_REPAIR"
LOCK_IDENTITY = "aistock.hmm_risk.b3_stock_fact_gap_repair.v1"

DAILY_BASIC_COLUMNS = (
    "trade_date",
    "ts_code",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
)
MONEYFLOW_COLUMNS = (
    "trade_date",
    "ts_code",
    "buy_sm_vol",
    "buy_sm_amount",
    "sell_sm_vol",
    "sell_sm_amount",
    "buy_md_vol",
    "buy_md_amount",
    "sell_md_vol",
    "sell_md_amount",
    "buy_lg_vol",
    "buy_lg_amount",
    "sell_lg_vol",
    "sell_lg_amount",
    "buy_elg_vol",
    "buy_elg_amount",
    "sell_elg_vol",
    "sell_elg_amount",
    "net_mf_vol",
    "net_mf_amount",
)
REQUIRED_NUMERIC_FIELDS = {
    "daily_basic": ("close", "total_mv", "circ_mv"),
    "moneyflow_ts": (
        "buy_sm_amount",
        "sell_sm_amount",
        "buy_elg_amount",
        "sell_elg_amount",
        "net_mf_amount",
    ),
}
DATASET_COLUMNS = {
    "daily_basic": DAILY_BASIC_COLUMNS,
    "moneyflow_ts": MONEYFLOW_COLUMNS,
}
DATASET_ORDER = ("daily_basic", "moneyflow_ts")
PLAN_FALSE_FLAGS = (
    "ddl",
    "db_writes",
    "fit_performed",
    "selection_performed",
    "model_ready_write_performed",
    "runtime_action_performed",
)


class StockFactGapRepairError(RuntimeError):
    pass


@dataclass(frozen=True, order=True)
class GapKey:
    trade_date: dt.date
    ts_code: str

    def as_dict(self) -> dict[str, str]:
        return {"trade_date": self.trade_date.isoformat(), "ts_code": self.ts_code}


@dataclass(frozen=True)
class RepairSpec:
    universe_key: str
    source_start: dt.date
    source_end: dt.date
    datasets: tuple[str, ...] = DATASET_ORDER

    def validate(self) -> None:
        _require(bool(self.universe_key.strip()), "universe_key is required")
        _require(self.source_start <= self.source_end, "source_start must not exceed source_end")
        _require(bool(self.datasets), "at least one repair dataset is required")
        _require(len(self.datasets) == len(set(self.datasets)), "repair datasets contain duplicates")
        _require(set(self.datasets) <= set(DATASET_ORDER), "repair datasets contain an unsupported dataset")
        _require(
            self.datasets == tuple(dataset for dataset in DATASET_ORDER if dataset in self.datasets),
            "repair datasets are not in canonical order",
        )


class GapStore(Protocol):
    def acquire_lock(self) -> None: ...

    def find_candidates(self, spec: RepairSpec) -> Mapping[str, Sequence[GapKey]]: ...

    def fetch_rows(self, dataset: str, keys: Sequence[GapKey]) -> Sequence[Mapping[str, Any]]: ...

    def insert_missing(self, dataset: str, rows: Sequence[Mapping[str, Any]]) -> Sequence[GapKey]: ...

    def delete_exact(self, dataset: str, rows: Sequence[Mapping[str, Any]]) -> Sequence[GapKey]: ...


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise StockFactGapRepairError(message)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _date(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value or "").strip().replace("-", "")
    _require(len(text) == 8 and text.isdigit(), f"invalid trade_date={value!r}")
    try:
        return dt.date(int(text[:4]), int(text[4:6]), int(text[6:]))
    except ValueError as exc:
        raise StockFactGapRepairError(f"invalid trade_date={value!r}") from exc


def _numeric(value: Any, *, field: str, required: bool) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        _require(not required, f"provider field {field} is missing")
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise StockFactGapRepairError(f"provider field {field} is not numeric") from exc
    if number.is_nan():
        _require(not required, f"provider field {field} is missing")
        return None
    _require(number.is_finite(), f"provider field {field} is not finite")
    if required and field in {"close", "total_mv", "circ_mv"}:
        _require(number > 0, f"provider field {field} must be positive")
    normalized = number.normalize()
    return format(normalized, "f")


def normalize_provider_row(dataset: str, row: Mapping[str, Any]) -> dict[str, Any]:
    columns = DATASET_COLUMNS.get(dataset)
    _require(columns is not None, f"unsupported dataset={dataset}")
    missing_columns = sorted(set(columns) - set(row))
    _require(not missing_columns, f"provider row lacks columns: {missing_columns}")
    trade_date = _date(row["trade_date"])
    ts_code = str(row["ts_code"] or "").strip()
    _require(bool(ts_code), "provider row has empty ts_code")
    required = set(REQUIRED_NUMERIC_FIELDS[dataset])
    normalized: dict[str, Any] = {"trade_date": trade_date.isoformat(), "ts_code": ts_code}
    for field in columns[2:]:
        normalized[field] = _numeric(row[field], field=field, required=field in required)
    return normalized


def _keys_payload(candidates: Mapping[str, Sequence[GapKey]]) -> dict[str, list[dict[str, str]]]:
    return {dataset: [key.as_dict() for key in sorted(candidates.get(dataset, ()))] for dataset in DATASET_ORDER}


def build_plan(store: GapStore, spec: RepairSpec) -> dict[str, Any]:
    spec.validate()
    candidates = store.find_candidates(spec)
    payload = {
        "schema_version": PLAN_SCHEMA,
        "status": "planned",
        "universe_key": spec.universe_key,
        "source_start": spec.source_start.isoformat(),
        "source_end": spec.source_end.isoformat(),
        "selected_datasets": list(spec.datasets),
        "candidates": _keys_payload(candidates),
        "candidate_counts": {dataset: len(candidates.get(dataset, ())) for dataset in DATASET_ORDER},
        "candidate_key_sha256": canonical_sha256(_keys_payload(candidates)),
        "ddl": False,
        "db_writes": False,
        "fit_performed": False,
        "selection_performed": False,
        "model_ready_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**payload, "plan_sha256": canonical_sha256(payload)}


def verify_plan(plan: Mapping[str, Any]) -> RepairSpec:
    _require(plan.get("schema_version") == PLAN_SCHEMA, "repair plan schema mismatch")
    _require(plan.get("status") == "planned", "repair plan status is not planned")
    _require(all(plan.get(field) is False for field in PLAN_FALSE_FLAGS), "repair plan side-effect flags are invalid")
    supplied = str(plan.get("plan_sha256") or "")
    payload = {key: value for key, value in plan.items() if key != "plan_sha256"}
    _require(supplied == canonical_sha256(payload), "repair plan hash mismatch")
    spec = RepairSpec(
        universe_key=str(plan.get("universe_key") or ""),
        source_start=_date(plan.get("source_start")),
        source_end=_date(plan.get("source_end")),
        datasets=tuple(plan.get("selected_datasets") or ()),
    )
    spec.validate()
    keys = plan_keys(plan)
    for dataset in DATASET_ORDER:
        if dataset not in spec.datasets:
            _require(not keys[dataset], f"repair plan contains candidates for unselected dataset {dataset}")
    counts = plan.get("candidate_counts")
    _require(isinstance(counts, Mapping), "repair plan candidate counts are missing")
    _require(
        {dataset: counts.get(dataset) for dataset in DATASET_ORDER}
        == {dataset: len(keys[dataset]) for dataset in DATASET_ORDER},
        "repair plan candidate counts differ from candidate keys",
    )
    return spec


def plan_keys(plan: Mapping[str, Any]) -> dict[str, list[GapKey]]:
    values: dict[str, list[GapKey]] = {}
    candidates = plan.get("candidates")
    _require(isinstance(candidates, Mapping), "repair plan candidates are missing")
    for dataset in DATASET_ORDER:
        raw = candidates.get(dataset)
        _require(isinstance(raw, list), f"repair plan candidate list is missing for {dataset}")
        _require(all(isinstance(item, Mapping) for item in raw), f"repair plan contains invalid {dataset} keys")
        keys = [GapKey(_date(item.get("trade_date")), str(item.get("ts_code") or "").strip()) for item in raw]
        _require(all(key.ts_code for key in keys), f"repair plan contains empty {dataset} ts_code")
        _require(len(keys) == len(set(keys)), f"repair plan contains duplicate {dataset} keys")
        values[dataset] = sorted(keys)
    _require(
        canonical_sha256(_keys_payload(values)) == str(plan.get("candidate_key_sha256") or ""),
        "repair candidate-key hash mismatch",
    )
    return values


def normalize_provider_rows(
    candidates: Mapping[str, Sequence[GapKey]],
    provider_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    normalized: dict[str, list[dict[str, Any]]] = {}
    for dataset in DATASET_ORDER:
        rows = [normalize_provider_row(dataset, row) for row in provider_rows.get(dataset, ())]
        keys = [GapKey(_date(row["trade_date"]), str(row["ts_code"])) for row in rows]
        expected = set(candidates.get(dataset, ()))
        _require(len(keys) == len(set(keys)), f"provider returned duplicate {dataset} keys")
        _require(set(keys) == expected, f"provider {dataset} key set differs from the repair plan")
        normalized[dataset] = sorted(rows, key=lambda item: (item["trade_date"], item["ts_code"]))
    return normalized


def _normalized_db_rows(dataset: str, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (normalize_provider_row(dataset, row) for row in rows),
        key=lambda item: (item["trade_date"], item["ts_code"]),
    )


def _row_receipts(rows: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for dataset in DATASET_ORDER:
        result[dataset] = [
            {"row": dict(row), "row_sha256": canonical_sha256(dict(row))} for row in rows.get(dataset, ())
        ]
    return result


def apply_plan(
    store: GapStore,
    plan: Mapping[str, Any],
    provider_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    spec = verify_plan(plan)
    expected = plan_keys(plan)
    _require(sum(map(len, expected.values())) > 0, "repair plan contains no candidate keys")
    normalized = normalize_provider_rows(expected, provider_rows)
    store.acquire_lock()
    current = {dataset: sorted(values) for dataset, values in store.find_candidates(spec).items()}
    current = {dataset: current.get(dataset, []) for dataset in DATASET_ORDER}
    if current == {dataset: [] for dataset in current}:
        existing = {
            dataset: _normalized_db_rows(dataset, store.fetch_rows(dataset, expected[dataset])) for dataset in expected
        }
        _require(existing == normalized, "repair candidates disappeared but durable rows differ from the plan")
        return _apply_receipt(plan, normalized, status="already_applied", db_writes=False)
    _require(current == expected, "repair candidate set drifted after preflight")

    for dataset in DATASET_ORDER:
        inserted = sorted(store.insert_missing(dataset, normalized[dataset]))
        _require(inserted == expected[dataset], f"concurrent or partial insert detected for {dataset}")
    readback = {
        dataset: _normalized_db_rows(dataset, store.fetch_rows(dataset, expected[dataset])) for dataset in expected
    }
    _require(readback == normalized, "repair readback differs from provider rows")
    return _apply_receipt(plan, readback, status="applied", db_writes=True)


def _apply_receipt(
    plan: Mapping[str, Any],
    rows: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    status: str,
    db_writes: bool,
) -> dict[str, Any]:
    payload = {
        "schema_version": RECEIPT_SCHEMA,
        "status": status,
        "plan_sha256": str(plan["plan_sha256"]),
        "rows": _row_receipts(rows),
        "row_counts": {dataset: len(rows.get(dataset, ())) for dataset in rows},
        "db_writes": db_writes,
        "ddl": False,
        "fit_performed": False,
        "selection_performed": False,
        "model_ready_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**payload, "receipt_sha256": canonical_sha256(payload)}


def verify_receipt(receipt: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    _require(receipt.get("schema_version") == RECEIPT_SCHEMA, "repair receipt schema mismatch")
    status = receipt.get("status")
    _require(status in {"applied", "already_applied"}, "repair receipt status is invalid")
    _require(
        receipt.get("db_writes") is (status == "applied"),
        "repair receipt status and db_writes are inconsistent",
    )
    _require(
        receipt.get("ddl") is False
        and receipt.get("fit_performed") is False
        and receipt.get("selection_performed") is False
        and receipt.get("model_ready_write_performed") is False
        and receipt.get("runtime_action_performed") is False,
        "repair receipt side-effect flags are invalid",
    )
    supplied = str(receipt.get("receipt_sha256") or "")
    payload = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    _require(supplied == canonical_sha256(payload), "repair receipt hash mismatch")
    rows_payload = receipt.get("rows")
    _require(isinstance(rows_payload, Mapping), "repair receipt rows are missing")
    rows: dict[str, list[dict[str, Any]]] = {}
    for dataset in DATASET_ORDER:
        items = rows_payload.get(dataset)
        _require(isinstance(items, list), f"repair receipt lacks {dataset} rows")
        dataset_rows: list[dict[str, Any]] = []
        for item in items:
            row = item.get("row") if isinstance(item, Mapping) else None
            _require(isinstance(row, Mapping), f"repair receipt has invalid {dataset} row")
            normalized = normalize_provider_row(dataset, row)
            _require(canonical_sha256(normalized) == str(item.get("row_sha256") or ""), "row hash mismatch")
            dataset_rows.append(normalized)
        rows[dataset] = dataset_rows
    counts = receipt.get("row_counts")
    _require(isinstance(counts, Mapping), "repair receipt row counts are missing")
    _require(
        {dataset: counts.get(dataset) for dataset in DATASET_ORDER}
        == {dataset: len(rows[dataset]) for dataset in DATASET_ORDER},
        "repair receipt row counts differ from durable rows",
    )
    return rows


def rollback_receipt(store: GapStore, receipt: Mapping[str, Any]) -> dict[str, Any]:
    _require(
        receipt.get("status") == "applied" and receipt.get("db_writes") is True,
        "rollback requires an applied receipt that performed database writes",
    )
    rows = verify_receipt(receipt)
    store.acquire_lock()
    for dataset, expected_rows in rows.items():
        keys = [GapKey(_date(row["trade_date"]), str(row["ts_code"])) for row in expected_rows]
        current = _normalized_db_rows(dataset, store.fetch_rows(dataset, keys))
        _require(current == expected_rows, f"rollback refused because {dataset} rows changed after apply")
    deleted_counts: dict[str, int] = {}
    for dataset, expected_rows in rows.items():
        deleted = store.delete_exact(dataset, expected_rows)
        _require(len(deleted) == len(expected_rows), f"guarded rollback did not delete every {dataset} row")
        deleted_counts[dataset] = len(deleted)
        keys = [GapKey(_date(row["trade_date"]), str(row["ts_code"])) for row in expected_rows]
        _require(not store.fetch_rows(dataset, keys), f"rollback readback still finds {dataset} rows")
    payload = {
        "schema_version": ROLLBACK_SCHEMA,
        "status": "rolled_back",
        "source_receipt_sha256": str(receipt["receipt_sha256"]),
        "deleted_counts": deleted_counts,
        "db_writes": True,
        "ddl": False,
        "runtime_action_performed": False,
    }
    return {**payload, "rollback_sha256": canonical_sha256(payload)}


def readback_receipt(store: GapStore, receipt: Mapping[str, Any]) -> dict[str, Any]:
    rows = verify_receipt(receipt)
    counts: dict[str, int] = {}
    for dataset, expected_rows in rows.items():
        keys = [GapKey(_date(row["trade_date"]), str(row["ts_code"])) for row in expected_rows]
        current = _normalized_db_rows(dataset, store.fetch_rows(dataset, keys))
        _require(current == expected_rows, f"readback differs from receipt for {dataset}")
        counts[dataset] = len(current)
    payload = {
        "schema_version": READBACK_SCHEMA,
        "status": "verified",
        "source_receipt_sha256": str(receipt["receipt_sha256"]),
        "row_counts": counts,
        "db_writes": False,
        "ddl": False,
        "runtime_action_performed": False,
    }
    return {**payload, "readback_sha256": canonical_sha256(payload)}


_MAPPING_CTE = """
WITH calendar_history AS (
  SELECT trade_date,lag(trade_date) OVER (ORDER BY trade_date) previous_trade_date
  FROM (
    SELECT cal_date::date trade_date FROM market.trading_calendar
    WHERE is_trading=true AND cal_date BETWEEN %s AND %s
  ) trading_days
), price_base AS MATERIALIZED (
  SELECT DISTINCT trade_date,ts_code
  FROM market.kline_daily_raw
  WHERE trade_date BETWEEN %s AND %s
), canonical_observed AS (
  SELECT c.trade_date,c.previous_trade_date,s.ts_code,
         l1.index_code AS l1_code,l2.index_code AS l2_code
  FROM price_base k
  JOIN calendar_history c ON c.trade_date=k.trade_date
  JOIN market.stock_universe_pit_spans s
    ON s.ts_code=k.ts_code AND s.universe_key=%s AND s.eligible_start<=c.trade_date
   AND (s.eligible_end IS NULL OR s.eligible_end>=c.trade_date)
  JOIN market.sw_index_member m
    ON m.ts_code=s.ts_code AND m.in_date<=c.trade_date
   AND (m.out_date IS NULL OR m.out_date>=c.trade_date)
  JOIN market.sw_index_classify l1
    ON l1.level='L1' AND m.l1_code IN (l1.index_code,l1.industry_code)
  JOIN market.sw_index_classify l2
    ON l2.level='L2' AND m.l2_code IN (l2.index_code,l2.industry_code)
  GROUP BY c.trade_date,c.previous_trade_date,s.ts_code,l1.index_code,l2.index_code
)
"""
SOURCE_SUMMARY_SQL = (
    _MAPPING_CTE
    + """
SELECT (SELECT count(*) FROM calendar_history WHERE trade_date BETWEEN %s AND %s) AS expected_dates,
       count(DISTINCT trade_date) AS observed_dates,
       count(*) AS canonical_rows,
       count(DISTINCT l1_code) AS l1_count,
       count(DISTINCT l2_code) AS l2_count
FROM canonical_observed
"""
)
DAILY_BASIC_CANDIDATE_SQL = """
WITH calendar_history AS (
  SELECT cal_date::date trade_date,
         lag(cal_date::date) OVER (ORDER BY cal_date) previous_trade_date
  FROM market.trading_calendar
  WHERE is_trading=true AND cal_date BETWEEN %s AND %s
), price_base AS MATERIALIZED (
  SELECT DISTINCT trade_date,ts_code FROM market.kline_daily_raw
  WHERE trade_date BETWEEN %s AND %s
), basic_base AS MATERIALIZED (
  SELECT DISTINCT trade_date,ts_code,total_mv,circ_mv FROM market.daily_basic
  WHERE trade_date BETWEEN %s AND %s
), canonical_observed AS (
  SELECT p.trade_date,p.ts_code
  FROM price_base p
  JOIN market.stock_universe_pit_spans s
    ON s.ts_code=p.ts_code AND s.universe_key=%s AND s.eligible_start<=p.trade_date
   AND (s.eligible_end IS NULL OR s.eligible_end>=p.trade_date)
  JOIN market.sw_index_member m
    ON m.ts_code=s.ts_code AND m.in_date<=p.trade_date
   AND (m.out_date IS NULL OR m.out_date>=p.trade_date)
  JOIN market.sw_index_classify l1
    ON l1.level='L1' AND m.l1_code IN (l1.index_code,l1.industry_code)
  JOIN market.sw_index_classify l2
    ON l2.level='L2' AND m.l2_code IN (l2.index_code,l2.industry_code)
  GROUP BY p.trade_date,p.ts_code
), candidate_rows AS (
  SELECT c.trade_date,c.ts_code,
         CASE WHEN current_basic.trade_date IS NULL THEN 'missing_current_row'
              ELSE 'invalid_current_total_mv' END AS gap_kind
  FROM canonical_observed c
  LEFT JOIN basic_base current_basic
    ON current_basic.trade_date=c.trade_date AND current_basic.ts_code=c.ts_code
  WHERE current_basic.trade_date IS NULL OR current_basic.total_mv IS NULL
  UNION ALL
  SELECT cal.previous_trade_date,c.ts_code,
         CASE WHEN previous_basic.trade_date IS NULL THEN 'missing_previous_row'
              ELSE 'invalid_previous_circ_mv' END AS gap_kind
  FROM canonical_observed c
  JOIN calendar_history cal ON cal.trade_date=c.trade_date
  LEFT JOIN basic_base previous_basic
    ON previous_basic.trade_date=cal.previous_trade_date AND previous_basic.ts_code=c.ts_code
  WHERE cal.previous_trade_date IS NOT NULL
    AND (previous_basic.trade_date IS NULL OR previous_basic.circ_mv IS NULL)
)
SELECT DISTINCT trade_date,ts_code,gap_kind FROM candidate_rows
WHERE trade_date IS NOT NULL
ORDER BY trade_date,ts_code,gap_kind
"""
MONEYFLOW_SYMBOLS_SQL = """
SELECT DISTINCT s.ts_code
FROM market.stock_universe_pit_spans s
JOIN market.sw_index_member m
  ON m.ts_code=s.ts_code AND m.in_date<=%s
 AND (m.out_date IS NULL OR m.out_date>=%s)
JOIN market.sw_index_classify l1
  ON l1.level='L1' AND m.l1_code IN (l1.index_code,l1.industry_code)
JOIN market.sw_index_classify l2
  ON l2.level='L2' AND m.l2_code IN (l2.index_code,l2.industry_code)
WHERE s.universe_key=%s AND s.eligible_start<=%s
  AND (s.eligible_end IS NULL OR s.eligible_end>=%s)
ORDER BY s.ts_code
"""
MONEYFLOW_SYMBOL_CANDIDATE_SQL = """
SELECT DISTINCT k.trade_date,k.ts_code
FROM market.kline_daily_raw k
JOIN market.stock_universe_pit_spans s
  ON s.ts_code=k.ts_code AND s.universe_key=%s AND s.eligible_start<=k.trade_date
 AND (s.eligible_end IS NULL OR s.eligible_end>=k.trade_date)
JOIN market.sw_index_member m
  ON m.ts_code=s.ts_code AND m.in_date<=k.trade_date
 AND (m.out_date IS NULL OR m.out_date>=k.trade_date)
JOIN market.sw_index_classify l1
  ON l1.level='L1' AND m.l1_code IN (l1.index_code,l1.industry_code)
JOIN market.sw_index_classify l2
  ON l2.level='L2' AND m.l2_code IN (l2.index_code,l2.industry_code)
WHERE k.ts_code=ANY(%s) AND k.trade_date BETWEEN %s AND %s
  AND NOT EXISTS (
  SELECT 1 FROM market.moneyflow_ts mf
  WHERE mf.trade_date=k.trade_date AND mf.ts_code=k.ts_code
)
ORDER BY k.trade_date
"""
MONEYFLOW_INVALID_SQL = """
SELECT count(*)
FROM market.moneyflow_ts mf
JOIN market.stock_universe_pit_spans s
  ON s.ts_code=mf.ts_code AND s.universe_key=%s AND s.eligible_start<=mf.trade_date
 AND (s.eligible_end IS NULL OR s.eligible_end>=mf.trade_date)
JOIN market.kline_daily_raw k ON k.trade_date=mf.trade_date AND k.ts_code=mf.ts_code
JOIN market.sw_index_member m
  ON m.ts_code=mf.ts_code AND m.in_date<=mf.trade_date
 AND (m.out_date IS NULL OR m.out_date>=mf.trade_date)
JOIN market.sw_index_classify l1
  ON l1.level='L1' AND m.l1_code IN (l1.index_code,l1.industry_code)
JOIN market.sw_index_classify l2
  ON l2.level='L2' AND m.l2_code IN (l2.index_code,l2.industry_code)
WHERE mf.trade_date BETWEEN %s AND %s
  AND (mf.buy_sm_amount IS NULL OR mf.sell_sm_amount IS NULL
       OR mf.buy_elg_amount IS NULL OR mf.sell_elg_amount IS NULL OR mf.net_mf_amount IS NULL)
"""
SOURCE_CONFLICT_SQL = """
SELECT source_name,conflict_groups FROM (
  SELECT 'kline_daily_raw' source_name,count(*) conflict_groups FROM (
    SELECT trade_date,ts_code FROM market.kline_daily_raw t
    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
  ) q
  UNION ALL
  SELECT 'daily_basic',count(*) FROM (
    SELECT trade_date,ts_code FROM market.daily_basic t
    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
  ) q
  UNION ALL
  SELECT 'moneyflow_ts',count(*) FROM (
    SELECT trade_date,ts_code FROM market.moneyflow_ts t
    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
  ) q
  UNION ALL
  SELECT 'stk_limit',count(*) FROM (
    SELECT trade_date,ts_code FROM market.stk_limit t
    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
  ) q
) conflicts WHERE conflict_groups>0
"""


class PostgresGapStore:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def _params(self, spec: RepairSpec) -> tuple[Any, ...]:
        return (
            spec.source_start - dt.timedelta(days=60),
            spec.source_end,
            spec.source_start,
            spec.source_end,
            spec.universe_key,
        )

    def _daily_candidate_params(self, spec: RepairSpec) -> tuple[Any, ...]:
        history_start = spec.source_start - dt.timedelta(days=60)
        return (
            history_start,
            spec.source_end,
            spec.source_start,
            spec.source_end,
            history_start,
            spec.source_end,
            spec.universe_key,
        )

    def acquire_lock(self) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (LOCK_IDENTITY,))

    def find_candidates(self, spec: RepairSpec) -> Mapping[str, Sequence[GapKey]]:
        spec.validate()
        with self.conn.cursor() as cursor:
            self._execute(
                cursor,
                "source_summary",
                SOURCE_SUMMARY_SQL,
                (*self._params(spec), spec.source_start, spec.source_end),
            )
            expected_dates, observed_dates, canonical_rows, l1_count, l2_count = map(int, cursor.fetchone())
            _require(expected_dates > 0, "repair source calendar is empty")
            _require(
                observed_dates == expected_dates and canonical_rows > 0,
                "repair source does not cover every trading date in the frozen window",
            )
            _require(l1_count == 31 and l2_count == 131, "repair source canonical sector set is not 31/131")
            history_start = spec.source_start - dt.timedelta(days=60)
            self._execute(
                cursor,
                "source_conflicts",
                SOURCE_CONFLICT_SQL,
                (
                    spec.source_start,
                    spec.source_end,
                    history_start,
                    spec.source_end,
                    spec.source_start,
                    spec.source_end,
                    spec.source_start,
                    spec.source_end,
                ),
            )
            conflicts = cursor.fetchall()
            _require(not conflicts, f"repair source contains conflicting duplicate keys: {conflicts}")
            daily: list[GapKey] = []
            if "daily_basic" in spec.datasets:
                self._execute(
                    cursor,
                    "daily_basic_candidates",
                    DAILY_BASIC_CANDIDATE_SQL,
                    self._daily_candidate_params(spec),
                )
                daily_rows = cursor.fetchall()
                invalid_daily = [row for row in daily_rows if str(row[2]).startswith("invalid_")]
                _require(
                    not invalid_daily,
                    f"daily_basic contains {len(invalid_daily)} existing rows with required NULLs",
                )
                daily = sorted({GapKey(_date(row[0]), str(row[1])) for row in daily_rows})
            moneyflow: list[GapKey] = []
            if "moneyflow_ts" in spec.datasets:
                self._execute(
                    cursor,
                    "moneyflow_invalid",
                    MONEYFLOW_INVALID_SQL,
                    (spec.universe_key, spec.source_start, spec.source_end),
                )
                invalid_moneyflow = int(cursor.fetchone()[0])
                _require(
                    invalid_moneyflow == 0,
                    f"moneyflow_ts contains {invalid_moneyflow} existing rows with required NULLs",
                )
                self._execute(
                    cursor,
                    "moneyflow_symbols",
                    MONEYFLOW_SYMBOLS_SQL,
                    (spec.source_end, spec.source_start, spec.universe_key, spec.source_end, spec.source_start),
                )
                symbols = [str(row[0]) for row in cursor.fetchall()]
                batch_size = 100
                for offset in range(0, len(symbols), batch_size):
                    batch = symbols[offset : offset + batch_size]
                    self._execute(
                        cursor,
                        f"moneyflow_candidates_batch:{offset // batch_size + 1}",
                        MONEYFLOW_SYMBOL_CANDIDATE_SQL,
                        (spec.universe_key, batch, spec.source_start, spec.source_end),
                    )
                    moneyflow.extend(GapKey(_date(row[0]), str(row[1])) for row in cursor.fetchall())
        return {"daily_basic": daily, "moneyflow_ts": moneyflow}

    @staticmethod
    def _execute(cursor: Any, stage: str, sql: str, params: Sequence[Any]) -> None:
        try:
            cursor.execute(sql, params)
        except Exception as exc:
            raise StockFactGapRepairError(
                f"repair preflight stage {stage} failed: {type(exc).__name__}: {exc}"
            ) from exc

    def fetch_rows(self, dataset: str, keys: Sequence[GapKey]) -> Sequence[Mapping[str, Any]]:
        columns = DATASET_COLUMNS.get(dataset)
        _require(columns is not None, f"unsupported dataset={dataset}")
        if not keys:
            return []
        table = "market.daily_basic" if dataset == "daily_basic" else "market.moneyflow_ts"
        grouped: dict[dt.date, list[str]] = defaultdict(list)
        for key in keys:
            grouped[key.trade_date].append(key.ts_code)
        rows: list[Mapping[str, Any]] = []
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            for trade_date, symbols in sorted(grouped.items()):
                cursor.execute(
                    f"SELECT {','.join(columns)} FROM {table} WHERE trade_date=%s AND ts_code=ANY(%s) ORDER BY ts_code",
                    (trade_date, sorted(symbols)),
                )
                rows.extend(cursor.fetchall())
        return rows

    def insert_missing(self, dataset: str, rows: Sequence[Mapping[str, Any]]) -> Sequence[GapKey]:
        columns = DATASET_COLUMNS.get(dataset)
        _require(columns is not None, f"unsupported dataset={dataset}")
        if not rows:
            return []
        table = "market.daily_basic" if dataset == "daily_basic" else "market.moneyflow_ts"
        values = [tuple(row[column] for column in columns) for row in rows]
        sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES %s ON CONFLICT DO NOTHING RETURNING trade_date,ts_code"
        with self.conn.cursor() as cursor:
            returned = execute_values(cursor, sql, values, page_size=500, fetch=True)
        return [GapKey(_date(row[0]), str(row[1])) for row in returned]

    def delete_exact(self, dataset: str, rows: Sequence[Mapping[str, Any]]) -> Sequence[GapKey]:
        columns = DATASET_COLUMNS.get(dataset)
        _require(columns is not None, f"unsupported dataset={dataset}")
        table = "market.daily_basic" if dataset == "daily_basic" else "market.moneyflow_ts"
        deleted: list[GapKey] = []
        predicates = " AND ".join(f"{column} IS NOT DISTINCT FROM %s" for column in columns)
        with self.conn.cursor() as cursor:
            for row in rows:
                cursor.execute(
                    f"DELETE FROM {table} WHERE {predicates} RETURNING trade_date,ts_code",
                    tuple(row[column] for column in columns),
                )
                value = cursor.fetchone()
                if value:
                    deleted.append(GapKey(_date(value[0]), str(value[1])))
        return deleted
