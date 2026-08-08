from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256

QLIB_DAILY_FIELDS = (
    "$open",
    "$high",
    "$low",
    "$close",
    "$volume",
    "$amount",
    "$factor",
    "$up_limit_price",
    "$down_limit_price",
    "$prev_close",
    "$limit_up",
    "$limit_down",
)

STATIC_FACTOR_COLUMNS = (
    "db_turnover_rate",
    "db_volume_ratio",
    "db_pe_ttm",
    "db_pb",
    "db_circ_mv",
    "mf_lg_buy_amt",
    "mf_lg_sell_amt",
    "mf_elg_buy_amt",
    "mf_elg_sell_amt",
    "bb_rev_yoy",
    "bb_profit_yoy",
    "bb_gpr",
    "bb_npr",
    "cp_cost_5pct",
    "cp_cost_50pct",
    "cp_cost_95pct",
    "cp_winner_rate",
    "md_rzye",
    "sw2_close",
    "sw2_amount",
    "sw2_mf_net_amt",
    "l2_code_id",
)

H5_EXPECTED_COLUMNS = {
    "daily_pv.h5": ("open", "close", "high", "low", "volume", "factor", "amount"),
    "daily_basic.h5": ("db_turnover_rate", "db_volume_ratio", "db_pe_ttm", "db_pb", "db_circ_mv"),
    "moneyflow.h5": ("mf_lg_buy_amt", "mf_lg_sell_amt", "mf_elg_buy_amt", "mf_elg_sell_amt"),
    "bak_basic.h5": ("bb_rev_yoy", "bb_profit_yoy", "bb_gpr", "bb_npr"),
    "margin_detail.h5": ("md_rzye",),
    "cyq_perf.h5": ("cp_cost_5pct", "cp_cost_50pct", "cp_cost_95pct", "cp_winner_rate"),
    "sector_data.h5": ("sw2_close", "sw2_amount", "sw2_mf_net_amt", "l2_code_id"),
}


@dataclass(frozen=True)
class QEFileSchemaReceipt:
    factor_root: str
    data_cutoff: str
    h5_schema_hashes: dict[str, str]
    static_factor_schema_hash: str


def initialize_qlib(provider_uri: str | Path) -> None:
    try:
        import qlib

        qlib.init(
            provider_uri=str(Path(provider_uri)),
            region="cn",
            dataset_cache=None,
            expression_cache=None,
        )
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "Qlib daily provider cannot be initialized",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"provider_uri": str(provider_uri), "error_type": type(exc).__name__},
        ) from exc


def load_trading_calendar(start: str, end: str) -> pd.DatetimeIndex:
    from qlib.data import D

    values = D.calendar(start_time=start, end_time=end, freq="day")
    calendar = pd.DatetimeIndex(pd.to_datetime(values)).normalize().sort_values().unique()
    if calendar.empty:
        raise AdvisoryModelFirstError(
            "Qlib trading calendar is empty",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"start": start, "end": end},
        )
    return calendar


def load_qlib_daily(
    instruments: Sequence[str] | object,
    *,
    start: str,
    end: str,
    fields: Sequence[str] = QLIB_DAILY_FIELDS,
) -> pd.DataFrame:
    from qlib.data import D

    try:
        raw = D.features(
            instruments=instruments,
            fields=list(fields),
            start_time=start,
            end_time=end,
            freq="day",
        )
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "Qlib daily fields cannot be read",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"start": start, "end": end, "fields": list(fields), "error_type": type(exc).__name__},
        ) from exc
    if raw is None or raw.empty:
        raise AdvisoryModelFirstError(
            "Qlib daily field selection returned no rows",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"start": start, "end": end, "fields": list(fields)},
        )
    frame = raw.copy()
    frame.columns = [str(column).lstrip("$") for column in frame.columns]
    frame = _normalize_market_index(frame)
    return frame.replace([float("inf"), float("-inf")], np.nan)


def all_qlib_instruments() -> object:
    from qlib.data import D

    return D.instruments("all")


def load_static_factors(
    factor_root: str | Path,
    *,
    columns: Sequence[str],
    start: str,
    end: str,
    instruments: Iterable[str] | None = None,
) -> pd.DataFrame:
    path = Path(factor_root) / "static_factors.parquet"
    if not path.is_file():
        raise AdvisoryModelFirstError(
            "static factor parquet does not exist",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"path": str(path)},
        )
    wanted_columns = tuple(dict.fromkeys(columns))
    filters: list[tuple[str, str, object]] = [
        ("datetime", ">=", pd.Timestamp(start)),
        ("datetime", "<=", pd.Timestamp(end)),
    ]
    symbols = sorted({str(item).upper() for item in instruments or ()})
    if symbols:
        filters.append(("instrument", "in", symbols))
    try:
        raw = pd.read_parquet(
            path,
            columns=[*wanted_columns, "datetime", "instrument"],
            filters=filters,
        )
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "static factor parquet projection failed",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"path": str(path), "columns": list(wanted_columns), "error_type": type(exc).__name__},
        ) from exc
    if raw.empty:
        raise AdvisoryModelFirstError(
            "static factor parquet projection returned no rows",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"start": start, "end": end, "symbol_count": len(symbols)},
        )
    if isinstance(raw.index, pd.MultiIndex):
        raw = raw.reset_index()
    required = {"datetime", "instrument", *wanted_columns}
    if not required.issubset(raw.columns):
        raise AdvisoryModelFirstError(
            "static factor parquet projection has an invalid schema",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": sorted(required - set(raw.columns))},
        )
    raw["datetime"] = pd.to_datetime(raw["datetime"]).dt.normalize()
    raw["instrument"] = raw["instrument"].astype(str).str.upper()
    return raw.set_index(["datetime", "instrument"]).sort_index()


def load_suspend_rows(
    suspend_root: str | Path,
    *,
    start: str,
    end: str,
    instruments: Iterable[str] | None = None,
) -> pd.DataFrame:
    path = Path(suspend_root) / "suspend_d.parquet"
    if not path.is_file():
        raise AdvisoryModelFirstError(
            "suspend sidecar does not exist",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"path": str(path)},
        )
    filters: list[tuple[str, str, object]] = [
        ("trade_date", ">=", pd.Timestamp(start)),
        ("trade_date", "<=", pd.Timestamp(end)),
        ("suspend_type", "=", "S"),
    ]
    symbols = sorted({str(item).upper() for item in instruments or ()})
    if symbols:
        filters.append(("ts_code", "in", symbols))
    try:
        frame = pd.read_parquet(path, filters=filters)
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "suspend sidecar projection failed",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"path": str(path), "error_type": type(exc).__name__},
        ) from exc
    required = {"trade_date", "ts_code", "suspend_type"}
    if not required.issubset(frame.columns):
        raise AdvisoryModelFirstError(
            "suspend sidecar has an invalid schema",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": sorted(required - set(frame.columns))},
        )
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.normalize()
    frame["instrument"] = frame["ts_code"].astype(str).str.upper()
    return frame[["trade_date", "instrument", "suspend_type"]].drop_duplicates(
        ["trade_date", "instrument"]
    )


def validate_factor_file_schemas(factor_root: str | Path, *, data_cutoff: str) -> QEFileSchemaReceipt:
    root = Path(factor_root)
    hashes: dict[str, str] = {}
    for filename, expected_columns in H5_EXPECTED_COLUMNS.items():
        path = root / filename
        if not path.is_file():
            raise AdvisoryModelFirstError(
                "required H5 factor file is missing",
                reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
                context={"path": str(path)},
            )
        try:
            with pd.HDFStore(path, "r") as store:
                if store.keys() != ["/data"]:
                    raise ValueError(f"expected only /data key, got {store.keys()}")
                columns = tuple(str(item) for item in store.select("/data", start=0, stop=1).columns)
        except Exception as exc:
            raise AdvisoryModelFirstError(
                "required H5 factor schema cannot be read",
                reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
                context={"path": str(path), "error_type": type(exc).__name__},
            ) from exc
        missing = sorted(set(expected_columns) - set(columns))
        if missing:
            raise AdvisoryModelFirstError(
                "required H5 factor columns are missing",
                reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
                context={"path": str(path), "missing_columns": missing},
            )
        hashes[filename] = canonical_json_sha256(
            {"filename": filename, "columns": columns, "data_cutoff": data_cutoff}
        )
    static_path = root / "static_factors.parquet"
    try:
        import pyarrow.parquet as pq

        schema = pq.ParquetFile(static_path).schema_arrow
        static_columns = tuple(schema.names)
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "static factor parquet schema cannot be read",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"path": str(static_path), "error_type": type(exc).__name__},
        ) from exc
    missing_static = sorted(set(STATIC_FACTOR_COLUMNS) - set(static_columns))
    if missing_static:
        raise AdvisoryModelFirstError(
            "static factor parquet is missing required columns",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_columns": missing_static},
        )
    return QEFileSchemaReceipt(
        factor_root=str(root),
        data_cutoff=data_cutoff,
        h5_schema_hashes=hashes,
        static_factor_schema_hash=canonical_json_sha256(
            {"columns": static_columns, "data_cutoff": data_cutoff}
        ),
    )


def _normalize_market_index(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame.index, pd.MultiIndex) or set(frame.index.names) != {"datetime", "instrument"}:
        raise AdvisoryModelFirstError(
            "Qlib daily result index must be datetime,instrument",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"index_names": list(frame.index.names)},
        )
    reset = frame.reset_index()
    reset["datetime"] = pd.to_datetime(reset["datetime"]).dt.normalize()
    reset["instrument"] = reset["instrument"].astype(str).str.upper()
    if reset.duplicated(["datetime", "instrument"]).any():
        raise AdvisoryModelFirstError(
            "Qlib daily result contains duplicate date-symbol rows",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    return reset.set_index(["datetime", "instrument"]).sort_index()
