from __future__ import annotations

import argparse
import json
import os
import struct
import sys
import warnings
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.data_service.moneyflow_contract import (  # noqa: E402
    MONEYFLOW_UNIT_CONTRACT_VERSION,
    assert_moneyflow_frame_parity,
)
from backend.qlib_exporter.config import IPO_FILTER_DAYS  # noqa: E402


DEFAULT_START = "2018-08-01"
DEFAULT_END = "2026-04-28"
DEFAULT_RECENT_START = "2026-03-10"
DEFAULT_SNAPSHOT_DIR = PROJECT_ROOT / "qlib_snapshots" / "qlib_20260428_shsz_candidate"
DEFAULT_BIN_DIR = PROJECT_ROOT / "qlib_bin" / "qlib_bin_20260428_shsz_candidate"
DEFAULT_RDAGENT_PROD = Path("F:/Dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data_20260428_candidate")
DEFAULT_RDAGENT_DEBUG = Path("F:/Dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data_debug_20260428_candidate")
BASELINE_STATIC = PROJECT_ROOT / "qlib_snapshots" / "qlib_test" / "static_factors.parquet"
REPORT_JSON = PROJECT_ROOT / "reports" / "qlib_candidate_20260428_validation.json"
REPORT_MD = PROJECT_ROOT / "reports" / "qlib_candidate_20260428_validation.md"


@dataclass
class Check:
    name: str
    status: str
    details: dict[str, Any] = field(default_factory=dict)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Strictly validate the 2026-04-28 QE/Qlib candidate datasets.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--recent-start", default=DEFAULT_RECENT_START)
    parser.add_argument("--snapshot-dir", default=str(DEFAULT_SNAPSHOT_DIR))
    parser.add_argument(
        "--static-schema-source",
        default=os.getenv("QE_STATIC_SCHEMA_SOURCE", str(BASELINE_STATIC)),
        help="Parquet file whose ordered columns define the expected static schema.",
    )
    parser.add_argument("--bin-dir", default=str(DEFAULT_BIN_DIR))
    parser.add_argument("--rdagent-prod-dir", default=str(DEFAULT_RDAGENT_PROD))
    parser.add_argument("--rdagent-debug-dir", default=str(DEFAULT_RDAGENT_DEBUG))
    parser.add_argument("--report-json", default=str(REPORT_JSON))
    parser.add_argument("--report-md", default=str(REPORT_MD))
    parser.add_argument("--skip-rdagent", action="store_true")
    parser.add_argument(
        "--skip-bin",
        action="store_true",
        help="Validate only the H5/static candidate when no new bin candidate was built.",
    )
    parser.add_argument("--skip-recent-db", action="store_true")
    return parser.parse_args()


def to_date(value: str) -> date:
    return date.fromisoformat(str(value))


def run_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=params)


def normalize_code(code: str) -> str:
    s = str(code).strip().upper()
    if "." in s:
        return s
    if len(s) >= 8 and s[:2] in {"SH", "SZ", "BJ"}:
        return f"{s[2:]}.{s[:2]}"
    return s


def baseline_static_columns(schema_source: Path) -> list[str]:
    import pyarrow.parquet as pq

    if not schema_source.exists():
        raise FileNotFoundError(
            f"Baseline static_factors schema source not found: {schema_source}; "
            "pass --static-schema-source or set QE_STATIC_SCHEMA_SOURCE"
        )
    pf = pq.ParquetFile(schema_source)
    columns = [
        name
        for name in pf.schema_arrow.names
        if not name.startswith("__index_level_") and name not in {"datetime", "instrument"}
    ]
    if "l2_code_id" not in columns:
        raise ValueError(
            f"Static schema source is stale and lacks l2_code_id: {schema_source}"
        )
    return columns


def parse_all_txt(path: Path) -> pd.DataFrame:
    rows = []
    if not path.exists():
        return pd.DataFrame(columns=["instrument", "start", "end"])
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        if "\t" in s:
            parts = s.split("\t")
        else:
            parts = s.split(",")
        if len(parts) >= 3:
            rows.append({"instrument": parts[0].strip(), "start": parts[1].strip(), "end": parts[2].strip()})
    return pd.DataFrame(rows)


def get_expected_daily_dates(start: date, end: date) -> list[str]:
    sql = """
        SELECT cal_date
        FROM market.trading_calendar
        WHERE cal_date >= %(start)s
          AND cal_date <= %(end)s
          AND is_trading = true
        ORDER BY cal_date
    """
    df = run_df(sql, {"start": start, "end": end})
    return [str(x) for x in pd.to_datetime(df["cal_date"]).dt.date.tolist()]


def get_h5_pool(end: date) -> pd.DataFrame:
    sql = """
        SELECT s.ts_code, s.list_date
        FROM market.stock_basic s
        WHERE (s.ts_code LIKE '%%.SH' OR s.ts_code LIKE '%%.SZ')
          AND (s.list_date IS NULL OR s.list_date <= %(end)s)
          AND (s.list_status IS NULL OR s.list_status NOT IN ('D', 'P'))
          AND NOT EXISTS (
              SELECT 1
              FROM market.stock_st st
              WHERE st.ts_code = s.ts_code
                AND st.ann_date <= %(end)s
          )
        ORDER BY s.ts_code
    """
    df = run_df(sql, {"end": end})
    if df.empty:
        return df
    df["ts_code"] = df["ts_code"].map(normalize_code)
    df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce").dt.date
    return df


def get_expected_daily_rows(pool: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    codes = pool["ts_code"].tolist()
    frames = []
    for i in range(0, len(codes), 1000):
        batch = codes[i : i + 1000]
        sql = """
            SELECT d.ts_code, MIN(d.trade_date) AS data_start, MAX(d.trade_date) AS data_end, COUNT(*) AS rows
            FROM market.kline_daily_raw d
            INNER JOIN market.stock_basic s ON s.ts_code = d.ts_code
            WHERE d.ts_code = ANY(%(codes)s)
              AND d.trade_date >= %(start)s
              AND d.trade_date <= %(end)s
              AND (s.list_date IS NULL OR d.trade_date >= s.list_date)
            GROUP BY d.ts_code
        """
        df = run_df(sql, {"codes": batch, "start": start, "end": end})
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["ts_code", "data_start", "data_end", "rows"])
    out = pd.concat(frames, ignore_index=True)
    out["ts_code"] = out["ts_code"].map(normalize_code)
    out["data_start"] = pd.to_datetime(out["data_start"]).dt.date
    out["data_end"] = pd.to_datetime(out["data_end"]).dt.date
    out["rows"] = out["rows"].astype(int)
    return out.sort_values("ts_code").reset_index(drop=True)


def expected_official_universe(pool: pd.DataFrame, daily_ranges: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    merged = daily_ranges.merge(pool.rename(columns={"ts_code": "instrument"})[["instrument", "list_date"]], on="instrument", how="left")
    rows = []
    for row in merged.itertuples(index=False):
        data_start = row.data_start
        data_end = row.data_end
        eff = max(data_start, start)
        if pd.notna(row.list_date):
            eff = max(eff, row.list_date + timedelta(days=IPO_FILTER_DAYS))
        if eff <= end and eff <= data_end:
            rows.append({"instrument": row.instrument, "start": eff.isoformat(), "end": min(data_end, end).isoformat()})
    return pd.DataFrame(rows).sort_values("instrument").reset_index(drop=True)


def expected_h5_all_txt(daily_ranges: pd.DataFrame) -> pd.DataFrame:
    out = daily_ranges[["instrument", "data_start", "data_end"]].copy()
    out["start"] = out["data_start"].map(lambda x: x.isoformat())
    out["end"] = out["data_end"].map(lambda x: x.isoformat())
    return out[["instrument", "start", "end"]].sort_values("instrument").reset_index(drop=True)


def bj_count(values: pd.Series) -> int:
    return int(values.astype(str).str.upper().str.endswith(".BJ").sum() + values.astype(str).str.upper().str.startswith("BJ").sum())


def check_snapshot(
    snapshot_dir: Path,
    start: date,
    end: date,
    expected_dates: list[str],
    static_schema_source: Path,
) -> tuple[list[Check], dict[str, Any]]:
    checks: list[Check] = []
    facts: dict[str, Any] = {}
    pool = get_h5_pool(end)
    daily_expected = get_expected_daily_rows(pool, start, end)
    daily_expected["instrument"] = daily_expected["ts_code"]
    expected_h5_instruments = set(daily_expected["instrument"])

    daily_path = snapshot_dir / "daily_pv.h5"
    if not daily_path.exists():
        checks.append(Check("snapshot_daily_exists", "FAIL", {"path": str(daily_path)}))
        return checks, facts

    daily = pd.read_hdf(daily_path, key="data")
    dates = pd.to_datetime(daily.index.get_level_values("datetime")).normalize()
    instruments = pd.Index(daily.index.get_level_values("instrument").astype(str))
    daily_date_list = [d.strftime("%Y-%m-%d") for d in sorted(dates.unique())]
    facts["daily_rows"] = int(len(daily))
    facts["daily_instruments"] = int(instruments.nunique())
    facts["daily_start"] = min(daily_date_list) if daily_date_list else None
    facts["daily_end"] = max(daily_date_list) if daily_date_list else None
    facts["expected_h5_instruments"] = int(len(expected_h5_instruments))
    facts["expected_daily_rows"] = int(daily_expected["rows"].sum())

    status = "PASS"
    details = {}
    if len(daily) != int(daily_expected["rows"].sum()):
        status = "FAIL"
        details["row_delta"] = int(len(daily) - int(daily_expected["rows"].sum()))
    actual_set = set(instruments.unique())
    if actual_set != expected_h5_instruments:
        status = "FAIL"
        details["missing_in_h5"] = sorted(expected_h5_instruments - actual_set)[:20]
        details["extra_in_h5"] = sorted(actual_set - expected_h5_instruments)[:20]
    if daily_date_list != expected_dates:
        status = "FAIL"
        details["date_mismatch"] = {"actual_tail": daily_date_list[-5:], "expected_tail": expected_dates[-5:]}
    if bj_count(pd.Series(list(actual_set))) > 0:
        status = "FAIL"
        details["bj_count"] = bj_count(pd.Series(list(actual_set)))
    if daily.index.has_duplicates:
        status = "FAIL"
        details["duplicate_index"] = True
    checks.append(Check("snapshot_daily_pv_matches_db", status, details))

    meta_path = snapshot_dir / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    meta_status = "PASS" if meta.get("start") == start.isoformat() and meta.get("end") == end.isoformat() else "FAIL"
    checks.append(Check("snapshot_meta_date_range", meta_status, {"meta": meta}))
    contract = meta.get("moneyflow_unit_contract", {})
    checks.append(
        Check(
            "snapshot_moneyflow_unit_contract",
            "PASS" if contract.get("version") == MONEYFLOW_UNIT_CONTRACT_VERSION else "FAIL",
            {"contract": contract, "expected": MONEYFLOW_UNIT_CONTRACT_VERSION},
        )
    )

    cal_path = snapshot_dir / "calendars" / "day.txt"
    cal = [x.strip() for x in cal_path.read_text(encoding="utf-8").splitlines() if x.strip()] if cal_path.exists() else []
    checks.append(Check("snapshot_day_calendar", "PASS" if cal == expected_dates else "FAIL", {"count": len(cal), "tail": cal[-5:]}))

    all_txt = parse_all_txt(snapshot_dir / "instruments" / "all.txt")
    daily_ranges_for_official = daily_expected[["instrument", "data_start", "data_end"]].copy()
    expected_official = expected_official_universe(
        pool,
        daily_ranges_for_official,
        start,
        end,
    )
    facts["official_instruments"] = int(len(expected_official))
    if not all_txt.empty:
        all_txt = all_txt.sort_values("instrument").reset_index(drop=True)
    inst_status = "PASS"
    inst_details = {"actual": int(len(all_txt)), "expected": int(len(expected_official))}
    if len(all_txt) != len(expected_official):
        inst_status = "FAIL"
    else:
        merged = all_txt.merge(
            expected_official,
            on="instrument",
            suffixes=("_actual", "_expected"),
            how="outer",
            indicator=True,
        )
        bad = merged[(merged["_merge"] != "both") | (merged["start_actual"] != merged["start_expected"]) | (merged["end_actual"] != merged["end_expected"])]
        if not bad.empty:
            inst_status = "FAIL"
            inst_details["examples"] = bad.head(20).to_dict(orient="records")
    checks.append(Check("snapshot_all_txt_data_range_rule", inst_status, inst_details))

    expected_static_cols = baseline_static_columns(static_schema_source)
    static_path = snapshot_dir / "static_factors.parquet"
    static: pd.DataFrame | None = None
    if static_path.exists():
        static = pd.read_parquet(static_path)
        sf_dates = pd.to_datetime(static.index.get_level_values("datetime")).normalize()
        sf_inst = pd.Index(static.index.get_level_values("instrument").astype(str))
        sf_status = "PASS"
        sf_details = {
            "rows": int(len(static)),
            "columns": int(len(static.columns)),
            "start": sf_dates.min().strftime("%Y-%m-%d") if len(sf_dates) else None,
            "end": sf_dates.max().strftime("%Y-%m-%d") if len(sf_dates) else None,
        }
        if list(static.columns) != expected_static_cols:
            sf_status = "FAIL"
            sf_details["schema_mismatch"] = True
        if "l2_code_id" not in static.columns or str(static["l2_code_id"].dtype) != "int16":
            sf_status = "FAIL"
            sf_details["l2_code_id_dtype"] = (
                str(static["l2_code_id"].dtype)
                if "l2_code_id" in static.columns
                else "missing"
            )
        if sf_details["end"] != end.isoformat():
            sf_status = "FAIL"
        if bj_count(pd.Series(sf_inst.unique())) > 0 or static.index.has_duplicates:
            sf_status = "FAIL"
            sf_details["bj_or_duplicate"] = True
        facts["static_rows"] = int(len(static))
        facts["static_columns"] = int(len(static.columns))
    else:
        sf_status = "FAIL"
        sf_details = {"missing": str(static_path)}
    checks.append(Check("snapshot_static_factors_schema_and_range", sf_status, sf_details))

    aux_files = {
        "daily_basic.h5": 16,
        "moneyflow.h5": 18,
        "bak_basic.h5": 15,
        "cyq_perf.h5": 9,
        "sector_data.h5": 23,
        "margin_detail.h5": 8,
    }
    for name, expected_cols in aux_files.items():
        path = snapshot_dir / name
        if not path.exists():
            checks.append(Check(f"snapshot_aux_{name}", "FAIL", {"missing": str(path)}))
            continue
        df = pd.read_hdf(path, key="data")
        inst = pd.Index(df.index.get_level_values("instrument").astype(str))
        dt = pd.to_datetime(df.index.get_level_values("datetime")).normalize()
        status = "PASS"
        details = {
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
            "start": dt.min().strftime("%Y-%m-%d") if len(dt) else None,
            "end": dt.max().strftime("%Y-%m-%d") if len(dt) else None,
        }
        if len(df.columns) != expected_cols:
            status = "WARN"
        if bj_count(pd.Series(inst.unique())) > 0:
            status = "FAIL"
        extra = set(inst.unique()) - actual_set
        if extra:
            status = "FAIL"
            details["extra_instruments"] = sorted(extra)[:20]
        checks.append(Check(f"snapshot_aux_{name}", status, details))
        if name == "moneyflow.h5" and static is not None:
            try:
                assert_moneyflow_frame_parity(df, static)
                checks.append(Check("snapshot_moneyflow_h5_static_parity", "PASS"))
            except ValueError as exc:
                checks.append(Check("snapshot_moneyflow_h5_static_parity", "FAIL", {"error": str(exc)}))

    facts["expected_official_df"] = expected_official
    return checks, facts


def read_bin_length(path: Path) -> int:
    size = path.stat().st_size
    if size < 4:
        return -1
    return size // 4 - 1


def read_bin_first_index(path: Path) -> int:
    with path.open("rb") as fh:
        raw = fh.read(4)
    return int(struct.unpack("<f", raw)[0]) if raw else -1


def first_calendar_on_or_after(calendar: list[str], value: str) -> str | None:
    for item in calendar:
        if item >= value:
            return item
    return None


def check_bin(bin_dir: Path, start: date, end: date, expected_dates: list[str], expected_official: pd.DataFrame) -> list[Check]:
    checks: list[Check] = []
    all_txt = parse_all_txt(bin_dir / "instruments" / "all.txt")
    if not all_txt.empty:
        all_txt = all_txt.sort_values("instrument").reset_index(drop=True)
    exp = expected_official.sort_values("instrument").reset_index(drop=True)
    status = "PASS"
    details = {"actual": int(len(all_txt)), "expected": int(len(exp))}
    if len(all_txt) != len(exp):
        status = "FAIL"
    else:
        merged = all_txt.merge(exp, on="instrument", suffixes=("_actual", "_expected"), how="outer", indicator=True)
        bad = merged[(merged["_merge"] != "both") | (merged["start_actual"] != merged["start_expected"]) | (merged["end_actual"] != merged["end_expected"])]
        if not bad.empty:
            status = "FAIL"
            details["examples"] = bad.head(20).to_dict(orient="records")
    if bj_count(all_txt["instrument"] if not all_txt.empty else pd.Series(dtype=str)):
        status = "FAIL"
        details["bj_in_all_txt"] = True
    checks.append(Check("bin_all_txt_matches_official_universe", status, details))

    cal_path = bin_dir / "calendars" / "day.txt"
    cal = [x.strip() for x in cal_path.read_text(encoding="utf-8").splitlines() if x.strip()] if cal_path.exists() else []
    checks.append(Check("bin_day_calendar", "PASS" if cal == expected_dates else "FAIL", {"count": len(cal), "tail": cal[-5:]}))

    meta_path = bin_dir / "meta_export.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    meta_status = "PASS"
    expected_last = {"stock_daily": end.isoformat(), "index_000300.SH": end.isoformat()}
    if meta.get("start") != start.isoformat() or meta.get("end") != end.isoformat():
        meta_status = "FAIL"
    if meta.get("exchanges") != ["sh", "sz"] or meta.get("last_end_dates", {}) != expected_last:
        meta_status = "FAIL"
    checks.append(Check("bin_meta_export", meta_status, {"meta": meta}))

    features_dir = bin_dir / "features"
    required_fields = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "factor",
        "up_limit_price",
        "down_limit_price",
        "prev_close",
        "limit_up",
        "limit_down",
    ]
    missing_examples = []
    bad_lengths = []
    for row in exp.itertuples(index=False):
        feature_name = str(row.instrument).lower()
        stock_dir = features_dir / feature_name
        if not stock_dir.exists():
            missing_examples.append({"instrument": row.instrument, "missing_dir": str(stock_dir)})
            continue
        feature_start = first_calendar_on_or_after(expected_dates, row.start)
        expected_len = expected_dates.index(feature_start) if feature_start in expected_dates else None
        expected_bars = expected_dates.index(row.end) - expected_dates.index(feature_start) + 1 if feature_start in expected_dates and row.end in expected_dates else None
        for field_name in required_fields:
            p = stock_dir / f"{field_name}.day.bin"
            if not p.exists():
                missing_examples.append({"instrument": row.instrument, "missing_field": field_name})
                break
            if expected_bars is not None and read_bin_length(p) != expected_bars:
                bad_lengths.append({"instrument": row.instrument, "field": field_name, "len": read_bin_length(p), "expected": expected_bars})
                break
            if expected_len is not None and read_bin_first_index(p) != expected_len:
                bad_lengths.append({"instrument": row.instrument, "field": field_name, "start_index": read_bin_first_index(p), "expected": expected_len})
                break
        if len(missing_examples) >= 20 or len(bad_lengths) >= 20:
            break
    checks.append(
        Check(
            "bin_stock_feature_files",
            "PASS" if not missing_examples and not bad_lengths else "FAIL",
            {"missing_examples": missing_examples[:20], "bad_lengths": bad_lengths[:20]},
        )
    )

    index_path = bin_dir / "instruments" / "index.txt"
    idx_lines = index_path.read_text(encoding="utf-8").splitlines() if index_path.exists() else []
    idx_status = "PASS" if any("000300" in line and end.isoformat() in line for line in idx_lines) else "FAIL"
    checks.append(Check("bin_index_000300_present", idx_status, {"index_lines": idx_lines[:10]}))
    return checks


def check_recent_db_completeness(recent_start: date, end: date) -> tuple[list[Check], dict[str, Any]]:
    checks: list[Check] = []
    facts: dict[str, Any] = {}
    expected_dates = get_expected_daily_dates(recent_start, end)
    sql_daily_dates = """
        SELECT DISTINCT trade_date
        FROM market.kline_daily_raw
        WHERE trade_date >= %(start)s AND trade_date <= %(end)s
          AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ')
        ORDER BY trade_date
    """
    daily_dates = [str(x) for x in pd.to_datetime(run_df(sql_daily_dates, {"start": recent_start, "end": end})["trade_date"]).dt.date.tolist()]
    sql_min_dates = """
        SELECT DISTINCT trade_time::date AS trade_date
        FROM market.kline_minute_raw
        WHERE freq = '1m'
          AND trade_time::date >= %(start)s
          AND trade_time::date <= %(end)s
          AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ')
        ORDER BY trade_date
    """
    minute_dates = [str(x) for x in pd.to_datetime(run_df(sql_min_dates, {"start": recent_start, "end": end})["trade_date"]).dt.date.tolist()]
    checks.append(Check("db_recent_daily_dates", "PASS" if daily_dates == expected_dates else "FAIL", {"actual": len(daily_dates), "expected": len(expected_dates), "tail": daily_dates[-5:]}))
    checks.append(Check("db_recent_minute_dates", "PASS" if minute_dates == expected_dates else "FAIL", {"actual": len(minute_dates), "expected": len(expected_dates), "tail": minute_dates[-5:]}))

    sql_minute_bad = """
        WITH daily_active AS (
            SELECT d.trade_date, d.ts_code
            FROM market.kline_daily_raw d
            INNER JOIN market.stock_basic s ON s.ts_code = d.ts_code
            WHERE trade_date >= %(start)s AND trade_date <= %(end)s
              AND (d.ts_code LIKE '%%.SH' OR d.ts_code LIKE '%%.SZ')
              AND (s.list_date IS NULL OR d.trade_date >= s.list_date)
              AND d.volume_hand > 0
              AND d.amount_li > 0
        ),
        minute_counts AS (
            SELECT trade_time::date AS trade_date, ts_code,
                   COUNT(*) AS bars,
                   MIN(trade_time::time) AS first_time,
                   MAX(trade_time::time) AS last_time
            FROM market.kline_minute_raw
            WHERE freq = '1m'
              AND trade_time::date >= %(start)s
              AND trade_time::date <= %(end)s
              AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ')
            GROUP BY trade_time::date, ts_code
        )
        SELECT a.trade_date, a.ts_code, COALESCE(m.bars, 0) AS bars, m.first_time, m.last_time
        FROM daily_active a
        LEFT JOIN minute_counts m
          ON m.trade_date = a.trade_date AND m.ts_code = a.ts_code
        WHERE COALESCE(m.bars, 0) <> 240
           OR m.first_time <> TIME '09:31:00'
           OR m.last_time <> TIME '15:00:00'
        ORDER BY a.trade_date, a.ts_code
        LIMIT 50
    """
    bad_minute = run_df(sql_minute_bad, {"start": recent_start, "end": end})
    sql_minute_total = """
        SELECT COUNT(*) AS active_stock_dates
        FROM market.kline_daily_raw d
        INNER JOIN market.stock_basic s ON s.ts_code = d.ts_code
        WHERE d.trade_date >= %(start)s AND d.trade_date <= %(end)s
          AND (d.ts_code LIKE '%%.SH' OR d.ts_code LIKE '%%.SZ')
          AND (s.list_date IS NULL OR d.trade_date >= s.list_date)
          AND d.volume_hand > 0
          AND d.amount_li > 0
    """
    total_active = int(run_df(sql_minute_total, {"start": recent_start, "end": end}).iloc[0]["active_stock_dates"])
    facts["recent_active_shsz_stock_dates"] = total_active
    checks.append(
        Check(
            "db_recent_minute_240_bars_per_daily_row",
            "PASS" if bad_minute.empty else "FAIL",
            {"active_stock_dates": total_active, "bad_examples": bad_minute.to_dict(orient="records")},
        )
    )

    sql_zero_volume_no_suspend = """
        WITH zero_daily AS (
            SELECT d.trade_date, d.ts_code
            FROM market.kline_daily_raw d
            INNER JOIN market.stock_basic s ON s.ts_code = d.ts_code
            WHERE d.trade_date >= %(start)s AND d.trade_date <= %(end)s
              AND (d.ts_code LIKE '%%.SH' OR d.ts_code LIKE '%%.SZ')
              AND (s.list_date IS NULL OR d.trade_date >= s.list_date)
              AND (d.volume_hand = 0 OR d.amount_li = 0)
        )
        SELECT z.trade_date, z.ts_code
        FROM zero_daily z
        LEFT JOIN market.suspend_d sd ON sd.trade_date = z.trade_date AND sd.ts_code = z.ts_code
        WHERE sd.ts_code IS NULL
        ORDER BY z.trade_date, z.ts_code
        LIMIT 50
    """
    zero_no_suspend = run_df(sql_zero_volume_no_suspend, {"start": recent_start, "end": end})
    checks.append(
        Check(
            "db_recent_zero_volume_missing_minutes_have_suspend_d",
            "PASS" if zero_no_suspend.empty else "FAIL",
            {"bad_examples": zero_no_suspend.to_dict(orient="records")},
        )
    )

    coverage_tables = {
        "adj_factor": "market.adj_factor",
        "stk_limit": "market.stk_limit",
        "daily_basic": "market.daily_basic",
        "moneyflow_ts": "market.moneyflow_ts",
        "cyq_perf": "market.cyq_perf",
        "sector_data": "market.sector_data",
        "bak_basic": "market.bak_basic",
    }
    pool = get_h5_pool(end)
    expected_daily = get_expected_daily_rows(pool, recent_start, end)
    expected_daily["instrument"] = expected_daily["ts_code"]
    official = expected_official_universe(
        pool,
        expected_daily[["instrument", "data_start", "data_end"]],
        recent_start,
        end,
    )
    eligible = official["instrument"].tolist()
    facts["recent_eligible_instruments"] = int(len(eligible))
    for dataset, table in coverage_tables.items():
        expected_count = 0
        matched_count = 0
        frames = []
        require_positive_volume = dataset in {"daily_basic", "moneyflow_ts", "cyq_perf"}
        volume_filter = "AND d.volume_hand > 0 AND d.amount_li > 0" if require_positive_volume else ""
        for i in range(0, len(eligible), 1000):
            batch = eligible[i : i + 1000]
            sql_count = f"""
                WITH expected AS (
                    SELECT d.trade_date, d.ts_code
                    FROM market.kline_daily_raw d
                    INNER JOIN market.stock_basic s ON s.ts_code = d.ts_code
                    WHERE d.ts_code = ANY(%(codes)s)
                      AND d.trade_date >= %(start)s
                      AND d.trade_date <= %(end)s
                      AND (s.list_date IS NULL OR d.trade_date >= s.list_date)
                      {volume_filter}
                )
                SELECT COUNT(*) AS expected_rows,
                       COUNT(t.ts_code) AS matched_rows
                FROM expected e
                LEFT JOIN {table} t
                  ON t.trade_date = e.trade_date AND t.ts_code = e.ts_code
            """
            counts = run_df(sql_count, {"codes": batch, "start": recent_start, "end": end}).iloc[0]
            expected_count += int(counts["expected_rows"])
            matched_count += int(counts["matched_rows"])
            if int(counts["expected_rows"]) != int(counts["matched_rows"]) and not frames:
                sql_missing = f"""
                    WITH expected AS (
                        SELECT d.trade_date, d.ts_code
                        FROM market.kline_daily_raw d
                        INNER JOIN market.stock_basic s ON s.ts_code = d.ts_code
                        WHERE d.ts_code = ANY(%(codes)s)
                          AND d.trade_date >= %(start)s
                          AND d.trade_date <= %(end)s
                          AND (s.list_date IS NULL OR d.trade_date >= s.list_date)
                          {volume_filter}
                    )
                    SELECT e.trade_date, e.ts_code
                    FROM expected e
                    LEFT JOIN {table} t
                      ON t.trade_date = e.trade_date AND t.ts_code = e.ts_code
                    WHERE t.ts_code IS NULL
                    LIMIT 20
                """
                frames.append(run_df(sql_missing, {"codes": batch, "start": recent_start, "end": end}))
        missing = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        facts[f"recent_{dataset}_expected_rows"] = expected_count
        checks.append(
            Check(
                f"db_recent_{dataset}_coverage",
                "PASS" if missing.empty and matched_count >= expected_count else "FAIL",
                {
                    "expected_rows": expected_count,
                    "matched_rows": matched_count,
                    "positive_volume_only": require_positive_volume,
                    "missing_examples": missing.to_dict(orient="records")[:20],
                },
            )
        )
    return checks, facts


def check_rdagent_dirs(
    prod_dir: Path,
    debug_dir: Path,
    static_schema_source: Path,
) -> list[Check]:
    checks: list[Check] = []
    required = [
        "daily_pv.h5",
        "daily_basic.h5",
        "moneyflow.h5",
        "bak_basic.h5",
        "cyq_perf.h5",
        "sector_data.h5",
        "margin_detail.h5",
        "static_factors.parquet",
        "README.md",
        "static_factors_schema.csv",
        "static_factors_schema.json",
    ]
    for label, directory in [("prod", prod_dir), ("debug", debug_dir)]:
        missing = [name for name in required if not (directory / name).exists()]
        details = {"dir": str(directory), "missing": missing}
        if not missing:
            sf = pd.read_parquet(directory / "static_factors.parquet")
            details["static_rows"] = int(len(sf))
            details["static_columns"] = int(len(sf.columns))
            if list(sf.columns) != baseline_static_columns(static_schema_source):
                missing.append("static_schema_mismatch")
        checks.append(Check(f"rdagent_{label}_candidate_files", "PASS" if not missing else "FAIL", details))
    return checks


def make_markdown(report: dict[str, Any]) -> str:
    lines = []
    lines.append("# QE/Qlib Candidate Validation 2026-04-28")
    lines.append("")
    lines.append(f"- Generated at: {report['generated_at']}")
    lines.append(f"- Overall: {'PASS' if report['ok'] else 'FAIL'}")
    lines.append(f"- Snapshot: `{report['paths']['snapshot_dir']}`")
    lines.append(f"- Bin: `{report['paths']['bin_dir']}`")
    lines.append("")
    lines.append("## Key Facts")
    for key, value in report.get("facts", {}).items():
        if key.endswith("_df"):
            continue
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Checks")
    for item in report["checks"]:
        lines.append(f"- {item['status']} `{item['name']}`: {json.dumps(item.get('details', {}), ensure_ascii=False, default=str)[:1200]}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    start = to_date(args.start)
    end = to_date(args.end)
    recent_start = to_date(args.recent_start)
    snapshot_dir = Path(args.snapshot_dir)
    bin_dir = Path(args.bin_dir)
    static_schema_source = Path(args.static_schema_source)

    checks: list[Check] = []
    facts: dict[str, Any] = {}
    expected_dates = get_expected_daily_dates(start, end)
    facts["expected_trading_days"] = len(expected_dates)
    facts["expected_start"] = expected_dates[0] if expected_dates else None
    facts["expected_end"] = expected_dates[-1] if expected_dates else None

    snapshot_checks, snapshot_facts = check_snapshot(
        snapshot_dir,
        start,
        end,
        expected_dates,
        static_schema_source,
    )
    checks.extend(snapshot_checks)
    facts.update({k: v for k, v in snapshot_facts.items() if not k.endswith("_df")})
    expected_official = snapshot_facts.get("expected_official_df")
    if args.skip_bin:
        checks.append(
            Check(
                "bin_validation_skipped",
                "WARN",
                {"reason": "explicit --skip-bin for H5/static-only candidate"},
            )
        )
    elif expected_official is not None:
        checks.extend(check_bin(bin_dir, start, end, expected_dates, expected_official))
    else:
        checks.append(Check("bin_validation_skipped", "FAIL", {"reason": "missing expected official universe from snapshot checks"}))

    if not args.skip_recent_db:
        db_checks, db_facts = check_recent_db_completeness(recent_start, end)
        checks.extend(db_checks)
        facts.update(db_facts)

    if not args.skip_rdagent:
        checks.extend(
            check_rdagent_dirs(
                Path(args.rdagent_prod_dir),
                Path(args.rdagent_debug_dir),
                static_schema_source,
            )
        )

    ok = all(c.status in {"PASS", "WARN"} for c in checks)
    report = {
        "ok": ok,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "paths": {
            "snapshot_dir": str(snapshot_dir),
            "bin_dir": str(bin_dir),
            "rdagent_prod_dir": args.rdagent_prod_dir,
            "rdagent_debug_dir": args.rdagent_debug_dir,
        },
        "facts": facts,
        "checks": [c.__dict__ for c in checks],
    }
    report_json = Path(args.report_json)
    report_md = Path(args.report_md)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    report_md.write_text(make_markdown(report), encoding="utf-8")
    print(json.dumps({"ok": ok, "report_json": str(report_json), "report_md": str(report_md), "failures": [c.name for c in checks if c.status == "FAIL"]}, ensure_ascii=False, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
