from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
import time
import warnings
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
BACKEND_ROOT = PROJECT_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.data_service import qe_data_service as qe_data  # noqa: E402
from backend.data_service.moneyflow_contract import (  # noqa: E402
    assert_moneyflow_frame_parity,
    moneyflow_unit_contract_receipt,
)
from backend.qlib_exporter.config import IPO_FILTER_DAYS  # noqa: E402
from backend.qlib_exporter.db_reader import DBReader  # noqa: E402
from backend.qlib_exporter.field_map_service import export_field_map_for_snapshot  # noqa: E402
from backend.qlib_exporter.snapshot_writer import SnapshotWriter  # noqa: E402
from backend.services.industry_code_map import UNKNOWN_L2_CODE_ID  # noqa: E402


DEFAULT_START = "2018-08-01"
DEFAULT_END = "2026-04-28"
DEFAULT_SNAPSHOT_ID = "qlib_20260428_shsz_candidate"
DEFAULT_BIN_ID = "qlib_bin_20260428_shsz_candidate"
DEFAULT_INDEX_CODES = ["000300.SH"]
STATIC_SCHEMA_SOURCE = PROJECT_ROOT / "qlib_snapshots" / "qlib_test" / "static_factors.parquet"
RDAGENT_GIT_IGNORE = Path("F:/Dev/RD-Agent-main/git_ignore_folder")
RDAGENT_PROD_SOURCE = RDAGENT_GIT_IGNORE / "factor_implementation_source_data"
RDAGENT_DEBUG_SOURCE = RDAGENT_GIT_IGNORE / "factor_implementation_source_data_debug"


@dataclass
class ExportProfile:
    start: str
    end: str
    exchanges: list[str]
    exclude_st: bool
    exclude_delisted_or_paused: bool
    ipo_filter_days: int
    h5_ipo_mode: str
    bin_ipo_mode: str
    index_codes: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build SH/SZ QE H5 and Qlib bin candidate datasets without "
            "overwriting the active datasets."
        )
    )
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--snapshot-id", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--bin-id", default=DEFAULT_BIN_ID)
    parser.add_argument("--snapshot-root", default=str(PROJECT_ROOT / "qlib_snapshots"))
    parser.add_argument(
        "--static-schema-source",
        default=os.getenv("QE_STATIC_SCHEMA_SOURCE", str(STATIC_SCHEMA_SOURCE)),
        help="Parquet file whose ordered columns define the static factor schema.",
    )
    parser.add_argument("--bin-root", default=str(PROJECT_ROOT / "qlib_bin"))
    parser.add_argument("--csv-root", default=str(PROJECT_ROOT / "qlib_csv"))
    parser.add_argument("--rdagent-root", default=str(RDAGENT_GIT_IGNORE))
    parser.add_argument("--wsl-copy-dir", default="/home/lc999/data/qlib_bin_20260428_shsz_candidate")
    parser.add_argument("--wsl-distro", default=os.getenv("QLIB_WSL_DISTRO", "Ubuntu"))
    parser.add_argument("--wsl-conda-sh", default=os.getenv("QLIB_WSL_CONDA_SH", "/home/lc999/miniconda3/etc/profile.d/conda.sh"))
    parser.add_argument("--wsl-conda-env", default=os.getenv("QLIB_WSL_CONDA_ENV", "rdagent-gpu"))
    parser.add_argument("--rdagent-root-wsl", default=os.getenv("QLIB_RDAGENT_ROOT_WSL", "/mnt/f/Dev/RD-Agent-main"))
    parser.add_argument("--dump-workers", type=int, default=8)
    parser.add_argument("--load-batch-size", type=int, default=400)
    parser.add_argument("--debug-instruments", type=int, default=100)
    parser.add_argument("--debug-end", default="2019-12-31")
    parser.add_argument("--index-code", action="append", dest="index_codes", default=None)
    parser.add_argument("--limit-instruments", type=int, default=None, help="Debug only: export the first N H5 instruments.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite-candidate", action="store_true")
    parser.add_argument("--skip-bin", action="store_true")
    parser.add_argument("--skip-rdagent-copy", action="store_true")
    parser.add_argument("--skip-wsl-copy", action="store_true")
    parser.add_argument("--rdagent-link-mode", choices=["hardlink", "copy"], default="hardlink")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def to_date(value: str) -> date:
    return date.fromisoformat(str(value))


def safe_candidate_path(path: Path, allowed_root: Path | None = None) -> Path:
    resolved = path.expanduser().resolve()
    if "candidate" not in resolved.name.lower():
        raise ValueError(f"Refusing to operate on non-candidate path: {resolved}")
    if allowed_root is not None:
        root = allowed_root.expanduser().resolve()
        if root != resolved and root not in resolved.parents:
            raise ValueError(f"Path {resolved} is not under expected root {root}")
    return resolved


def prepare_dir(path: Path, *, allowed_root: Path, overwrite: bool, dry_run: bool = False) -> Path:
    resolved = safe_candidate_path(path, allowed_root)
    if resolved.exists():
        if not overwrite:
            raise FileExistsError(f"Candidate path already exists; pass --overwrite-candidate to rebuild it: {resolved}")
        if dry_run:
            logging.info("[dry-run] would remove candidate directory %s", resolved)
        else:
            shutil.rmtree(resolved)
    if not dry_run:
        resolved.mkdir(parents=True, exist_ok=True)
    return resolved


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


def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def get_h5_universe(end: date, limit: int | None = None) -> pd.DataFrame:
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
    if limit:
        df = df.head(limit).copy()
    return df.reset_index(drop=True)


def load_daily_data(pool_df: pd.DataFrame, start: date, end: date, batch_size: int) -> pd.DataFrame:
    reader = DBReader()
    frames: list[pd.DataFrame] = []
    codes = (
        pool_df["ts_code"]
        .dropna()
        .map(normalize_code)
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    start_dates = {}
    for row in pool_df.itertuples(index=False):
        code = normalize_code(row.ts_code)
        list_date = getattr(row, "list_date")
        start_dates[code] = max(start, list_date) if pd.notna(list_date) else start
    batches = list(chunked(codes, batch_size))
    for batch_no, batch in enumerate(batches, start=1):
        logging.info(
            "Loading daily PV batch %s/%s (%s codes, start=%s)",
            batch_no,
            len(batches),
            len(batch),
            start,
        )
        df = reader.load_qlib_daily_data(
            batch,
            start,
            end,
            use_tushare_adj=True,
            instrument_start_dates={code: start_dates[code] for code in batch},
        )
        if not df.empty:
            frames.append(df)
    if not frames:
        raise RuntimeError("No daily data loaded from DB")
    daily = pd.concat(frames, axis=0).sort_index()
    daily = daily[~daily.index.duplicated(keep="last")]
    return daily


def normalize_daily_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {c: c[1:] for c in df.columns if isinstance(c, str) and c.startswith("$")}
    return df.rename(columns=rename_map)


def write_h5(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        logging.warning("Skipping empty H5: %s", path)
        return
    df = df.sort_index()
    df.to_hdf(path, key="data", mode="w", format="fixed")


def write_data_range_all_txt(df: pd.DataFrame, path: Path, sep: str) -> None:
    ranges = (
        df.reset_index()
        .groupby("instrument")["datetime"]
        .agg(start="min", end="max")
        .reset_index()
        .sort_values("instrument")
    )
    lines = [
        sep.join(
            [
                str(row.instrument),
                pd.Timestamp(row.start).strftime("%Y-%m-%d"),
                pd.Timestamp(row.end).strftime("%Y-%m-%d"),
            ]
        )
        for row in ranges.itertuples(index=False)
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def write_official_all_txt(official: pd.DataFrame, path: Path, sep: str) -> None:
    lines = [
        sep.join([str(row.instrument), str(row.start), str(row.end)])
        for row in official.sort_values("instrument").itertuples(index=False)
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def read_static_schema_columns(schema_source: Path) -> list[str]:
    if not schema_source.exists():
        raise FileNotFoundError(
            f"Baseline static_factors schema source not found: {schema_source}; "
            "pass --static-schema-source or set QE_STATIC_SCHEMA_SOURCE"
        )
    import pyarrow.parquet as pq

    schema = pq.ParquetFile(schema_source).schema_arrow
    columns = [
        name
        for name in schema.names
        if not name.startswith("__index_level_") and name not in {"datetime", "instrument"}
    ]
    if "l2_code_id" not in columns:
        raise ValueError(
            f"Static schema source is stale and lacks l2_code_id: {schema_source}"
        )
    return columns


def align_static_schema(static: pd.DataFrame, expected_cols: list[str]) -> pd.DataFrame:
    aligned = static.copy()
    for col in expected_cols:
        if col not in aligned.columns:
            aligned[col] = np.nan
    aligned = aligned[expected_cols]
    for col in aligned.columns:
        numeric = pd.to_numeric(aligned[col], errors="coerce")
        if col == "l2_code_id":
            aligned[col] = numeric.fillna(UNKNOWN_L2_CODE_ID).astype("int16")
        else:
            aligned[col] = numeric.astype("float32")
    return aligned


def build_aux_and_static(
    snapshot_dir: Path,
    instruments: list[str],
    daily_norm: pd.DataFrame,
    start: date,
    end: date,
    static_schema_source: Path,
) -> dict:
    logging.info("Loading auxiliary H5 datasets for %s instruments", len(instruments))
    df_db = qe_data.load_daily_basic(instruments, start, end)
    df_mf = qe_data.load_moneyflow(instruments, start, end)
    df_bb = qe_data.load_bak_basic(instruments, start, end)
    df_cp = qe_data.load_cyq_perf(instruments, start, end)
    df_sd = qe_data.load_sector_data(instruments, start, end)
    df_md = qe_data.load_margin_detail(instruments, start, end)

    aux = {
        "daily_basic.h5": df_db,
        "moneyflow.h5": df_mf,
        "bak_basic.h5": df_bb,
        "cyq_perf.h5": df_cp,
        "sector_data.h5": df_sd,
        "margin_detail.h5": df_md,
    }
    for name, df in aux.items():
        logging.info("Writing %s shape=%s", name, getattr(df, "shape", None))
        write_h5(df, snapshot_dir / name)

    logging.info("Building static_factors.parquet")
    df_mf_derived = qe_data.compute_moneyflow_derived_factors(df_mf, daily_norm)
    df_db_precomp = qe_data.compute_daily_basic_precomputed_factors(df_db)
    if not daily_norm.empty and "close" in daily_norm.columns:
        df_price = pd.DataFrame(index=daily_norm.index)
        df_price["PriceStrength_10D"] = daily_norm["close"].groupby(level="instrument").pct_change(10)
    else:
        df_price = pd.DataFrame()

    frames = [
        df_db,
        df_mf,
        df_bb,
        df_cp,
        df_sd,
        df_md,
        df_mf_derived,
        df_db_precomp,
        df_price,
    ]
    frames = [df.sort_index() for df in frames if df is not None and not df.empty]
    if not frames:
        raise RuntimeError("No auxiliary frames available to build static_factors")

    static = frames[0]
    for nxt in frames[1:]:
        overlap = static.columns.intersection(nxt.columns)
        if len(overlap):
            nxt = nxt.drop(columns=list(overlap))
        if not nxt.empty:
            static = static.join(nxt, how="left")
    static = static.sort_index()

    expected_cols = read_static_schema_columns(static_schema_source)
    static = align_static_schema(static, expected_cols)

    # Fail before writing a candidate when alternate export paths drift in units.
    assert_moneyflow_frame_parity(df_mf, static)

    parquet_path = snapshot_dir / "static_factors.parquet"
    static.to_parquet(parquet_path)
    stats = {
        "daily_basic_rows": int(len(df_db)),
        "moneyflow_rows": int(len(df_mf)),
        "bak_basic_rows": int(len(df_bb)),
        "cyq_perf_rows": int(len(df_cp)),
        "sector_data_rows": int(len(df_sd)),
        "margin_detail_rows": int(len(df_md)),
        "static_rows": int(len(static)),
        "static_columns": int(len(static.columns)),
        "moneyflow_unit_contract": moneyflow_unit_contract_receipt(),
    }
    del static, frames
    qe_data.clear_data_cache()
    return stats


def compute_official_universe(daily_norm: pd.DataFrame, pool_df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    ranges = (
        daily_norm.reset_index()
        .groupby("instrument")["datetime"]
        .agg(data_start="min", data_end="max")
        .reset_index()
    )
    pool = pool_df.copy()
    pool["instrument"] = pool["ts_code"].map(normalize_code)
    merged = ranges.merge(pool[["instrument", "list_date"]], on="instrument", how="left")

    rows = []
    for row in merged.itertuples(index=False):
        data_start = pd.Timestamp(row.data_start).date()
        data_end = pd.Timestamp(row.data_end).date()
        eff = max(data_start, start)
        list_date = row.list_date
        if pd.notna(list_date):
            eff = max(eff, list_date + timedelta(days=IPO_FILTER_DAYS))
        if eff <= end and eff <= data_end:
            rows.append(
                {
                    "instrument": row.instrument,
                    "start": eff,
                    "end": min(data_end, end),
                    "data_start": data_start,
                    "data_end": data_end,
                    "list_date": list_date if pd.notna(list_date) else None,
                }
            )
    return pd.DataFrame(rows).sort_values("instrument").reset_index(drop=True)


def load_limit_data(codes: list[str], start: date, end: date) -> pd.DataFrame:
    frames = []
    for batch in chunked(codes, 1000):
        sql = """
            SELECT trade_date, ts_code,
                   pre_close AS prev_close,
                   up_limit AS up_limit_price,
                   down_limit AS down_limit_price
            FROM market.stk_limit
            WHERE ts_code = ANY(%(codes)s)
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
            ORDER BY trade_date, ts_code
        """
        df = run_df(sql, {"codes": batch, "start": start, "end": end})
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["date", "symbol", "prev_close", "up_limit_price", "down_limit_price"])
    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["trade_date"]).dt.date
    out["symbol"] = out["ts_code"].map(normalize_code)
    for col in ["prev_close", "up_limit_price", "down_limit_price"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float32")
    return out[["date", "symbol", "prev_close", "up_limit_price", "down_limit_price"]]


def write_stock_csv_for_bin(daily_norm: pd.DataFrame, official: pd.DataFrame, csv_dir: Path, start: date, end: date) -> dict:
    csv_dir.mkdir(parents=True, exist_ok=True)
    # Keep full post-listing feature history in bin files. IPO-365 eligibility
    # is expressed by instruments/all.txt, not by deleting feature rows.
    codes = sorted(str(value) for value in daily_norm.index.get_level_values("instrument").unique())
    limits = load_limit_data(codes, start, end)

    df = daily_norm.reset_index()
    df["date_obj"] = pd.to_datetime(df["datetime"]).dt.date
    df["symbol"] = df["instrument"].astype(str)
    if df.empty:
        raise RuntimeError("No daily rows available for bin CSV")

    df = df.merge(limits, left_on=["date_obj", "symbol"], right_on=["date", "symbol"], how="left")
    raw_close = pd.to_numeric(df["close"], errors="coerce") / pd.to_numeric(df["factor"], errors="coerce")
    df["_raw_close"] = raw_close
    df = df.sort_values(["symbol", "date_obj"]).copy()
    prev_from_raw = df.groupby("symbol")["_raw_close"].shift(1)
    df["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce").fillna(prev_from_raw)
    have_limits = df["up_limit_price"].notna() & df["down_limit_price"].notna()
    df["limit_up"] = np.where(have_limits, (df["_raw_close"] >= df["up_limit_price"] - 1e-4).astype("float32"), np.nan)
    df["limit_down"] = np.where(have_limits, (df["_raw_close"] <= df["down_limit_price"] + 1e-4).astype("float32"), np.nan)
    df["date"] = df["date_obj"].astype(str)

    csv_cols = [
        "date",
        "symbol",
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
    for col in csv_cols:
        if col not in df.columns:
            df[col] = np.nan

    written = 0
    rows = 0
    for symbol, group in df[csv_cols].groupby("symbol", sort=True):
        group.to_csv(csv_dir / f"{symbol}.csv", index=False)
        written += 1
        rows += len(group)
    return {"csv_files": written, "csv_rows": rows}


def win_to_wsl(path: Path | str) -> str:
    p = str(path).replace("\\", "/")
    if len(p) >= 2 and p[1] == ":":
        drive = p[0].lower()
        return f"/mnt/{drive}{p[2:]}"
    return p


def run_wsl_script(args: argparse.Namespace, script_name: str, script_args: list[str], timeout: int | None = None) -> subprocess.CompletedProcess:
    script_path = f"{args.rdagent_root_wsl.rstrip('/')}/scripts/{script_name}"
    quoted = " ".join(shlex.quote(str(x)) for x in [script_path, *script_args])
    command = (
        f"source {shlex.quote(args.wsl_conda_sh)} && "
        f"conda activate {shlex.quote(args.wsl_conda_env)} && "
        f"python {quoted}"
    )
    logging.info("Running WSL script: %s %s", script_name, " ".join(map(str, script_args[:8])))
    res = subprocess.run(
        ["wsl", "-d", args.wsl_distro, "--", "bash", "-lc", command],
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=timeout,
    )
    if res.stdout:
        logging.info("WSL stdout tail:\n%s", res.stdout[-4000:])
    if res.stderr:
        logging.info("WSL stderr tail:\n%s", res.stderr[-4000:])
    return res


def dump_stock_bin(args: argparse.Namespace, csv_dir: Path, bin_dir: Path) -> None:
    dump_args = [
        "dump_all",
        "--data_path",
        win_to_wsl(csv_dir),
        "--qlib_dir",
        win_to_wsl(bin_dir),
        "--freq",
        "day",
        "--date_field_name",
        "date",
        "--symbol_field_name",
        "symbol",
        "--exclude_fields",
        "date,symbol",
        "--max_workers",
        str(args.dump_workers),
    ]
    res = run_wsl_script(args, "dump_bin.py", dump_args)
    if res.returncode != 0:
        raise RuntimeError(f"stock dump_bin.py failed with code {res.returncode}\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}")


def write_index_csv(index_code: str, start: date, end: date, csv_dir: Path) -> dict:
    reader = DBReader()
    df = reader.load_index_daily(index_code, start, end)
    if df.empty:
        raise RuntimeError(f"No index daily data for {index_code} {start}~{end}")
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)
    out["symbol"] = df["ts_code"].astype(str)
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
    out["amount"] = pd.to_numeric(df["amount"], errors="coerce").astype("float64") * 1000.0
    csv_dir.mkdir(parents=True, exist_ok=True)
    out.to_csv(csv_dir / f"{index_code}.csv", index=False)
    return {"rows": len(out), "start": out["date"].min(), "end": out["date"].max()}


def update_index_file(bin_dir: Path, index_code: str, all_backup: bytes | None) -> None:
    all_path = bin_dir / "instruments" / "all.txt"
    generated = all_path.read_text(encoding="utf-8") if all_path.exists() else ""
    entry = None
    for line in generated.splitlines():
        parts = line.strip().split("\t")
        if len(parts) >= 3:
            entry = (parts[0], parts[1], parts[2])
            break
    if entry is None:
        raise RuntimeError(f"Could not extract generated index instrument entry for {index_code}")
    index_path = bin_dir / "instruments" / "index.txt"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, tuple[str, str]] = {}
    if index_path.exists():
        for line in index_path.read_text(encoding="utf-8").splitlines():
            parts = line.strip().split("\t")
            if len(parts) >= 3:
                existing[parts[0]] = (parts[1], parts[2])
    existing[entry[0]] = (entry[1], entry[2])
    index_path.write_text("".join(f"{k}\t{v[0]}\t{v[1]}\n" for k, v in sorted(existing.items())), encoding="utf-8")

    if all_backup is not None:
        all_path.write_bytes(all_backup)
    else:
        all_path.unlink(missing_ok=True)


def dump_index_bins(args: argparse.Namespace, index_codes: list[str], bin_dir: Path, csv_root: Path, start: date, end: date) -> dict:
    results = {}
    for index_code in index_codes:
        index_csv_dir = csv_root / "index" / index_code
        stats = write_index_csv(index_code, start, end, index_csv_dir)
        all_path = bin_dir / "instruments" / "all.txt"
        backup = all_path.read_bytes() if all_path.exists() else None
        dump_args = [
            "dump_all",
            "--data_path",
            win_to_wsl(index_csv_dir),
            "--qlib_dir",
            win_to_wsl(bin_dir),
            "--freq",
            "day",
            "--date_field_name",
            "date",
            "--symbol_field_name",
            "symbol",
            "--exclude_fields",
            "date,symbol",
            "--max_workers",
            str(max(1, min(args.dump_workers, 4))),
        ]
        res = run_wsl_script(args, "dump_bin.py", dump_args)
        if res.returncode != 0:
            raise RuntimeError(f"index dump_bin.py failed for {index_code}: {res.stderr}")
        update_index_file(bin_dir, index_code, backup)
        results[index_code] = stats
    return results


def write_bin_meta(bin_dir: Path, args: argparse.Namespace, profile: ExportProfile, last_end_dates: dict[str, str]) -> None:
    meta = {
        "snapshot_id": args.bin_id,
        "start": profile.start,
        "end": profile.end,
        "exchanges": profile.exchanges,
        "exclude_st": profile.exclude_st,
        "exclude_delisted_or_paused": profile.exclude_delisted_or_paused,
        "freq_types": ["daily"],
        "index_codes": profile.index_codes,
        "last_end_dates": last_end_dates,
        "export_mode": "full_candidate",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    (bin_dir / "meta_export.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def copy_or_link(src: Path, dst: Path, mode: str) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    if mode == "hardlink":
        try:
            os.link(src, dst)
            return
        except OSError:
            logging.warning("Hardlink failed, falling back to copy: %s -> %s", src, dst)
    shutil.copy2(src, dst)


def copy_rdagent_candidate(args: argparse.Namespace, snapshot_dir: Path) -> dict:
    root = Path(args.rdagent_root).resolve()
    prod_dir = prepare_dir(
        root / "factor_implementation_source_data_20260428_candidate",
        allowed_root=root,
        overwrite=args.overwrite_candidate,
    )
    debug_dir = prepare_dir(
        root / "factor_implementation_source_data_debug_20260428_candidate",
        allowed_root=root,
        overwrite=args.overwrite_candidate,
    )

    data_files = [
        "daily_pv.h5",
        "daily_basic.h5",
        "moneyflow.h5",
        "bak_basic.h5",
        "cyq_perf.h5",
        "sector_data.h5",
        "margin_detail.h5",
        "static_factors.parquet",
    ]
    for name in data_files:
        src = snapshot_dir / name
        if src.exists():
            copy_or_link(src, prod_dir / name, args.rdagent_link_mode)

    for name in ["README.md", "static_factors_schema.csv", "static_factors_schema.json"]:
        src = RDAGENT_PROD_SOURCE / name
        if src.exists():
            shutil.copy2(src, prod_dir / name)

    debug_stats = build_debug_candidate(snapshot_dir, debug_dir, args.debug_instruments, args.debug_end)
    return {
        "rdagent_prod": str(prod_dir),
        "rdagent_debug": str(debug_dir),
        "rdagent_link_mode": args.rdagent_link_mode,
        "debug": debug_stats,
    }


def build_debug_candidate(snapshot_dir: Path, debug_dir: Path, n_instruments: int, debug_end: str) -> dict:
    daily = pd.read_hdf(snapshot_dir / "daily_pv.h5", key="data")
    instruments = sorted(daily.index.get_level_values("instrument").unique().tolist())[:n_instruments]
    end_ts = pd.Timestamp(debug_end)
    stats = {}
    files = [
        "daily_pv.h5",
        "daily_basic.h5",
        "moneyflow.h5",
        "bak_basic.h5",
        "cyq_perf.h5",
        "sector_data.h5",
        "margin_detail.h5",
    ]
    for name in files:
        path = snapshot_dir / name
        if not path.exists():
            continue
        df = pd.read_hdf(path, key="data")
        idx_dt = df.index.get_level_values("datetime")
        idx_inst = df.index.get_level_values("instrument")
        part = df[(idx_inst.isin(instruments)) & (idx_dt <= end_ts)].sort_index()
        write_h5(part, debug_dir / name)
        stats[name] = {"rows": int(len(part)), "columns": int(len(part.columns))}

    sf = pd.read_parquet(snapshot_dir / "static_factors.parquet")
    idx_dt = sf.index.get_level_values("datetime")
    idx_inst = sf.index.get_level_values("instrument")
    sf_part = sf[(idx_inst.isin(instruments)) & (idx_dt <= end_ts)].sort_index()
    sf_part.to_parquet(debug_dir / "static_factors.parquet")
    stats["static_factors.parquet"] = {"rows": int(len(sf_part)), "columns": int(len(sf_part.columns))}

    for name in ["README.md", "static_factors_schema.csv", "static_factors_schema.json"]:
        src = RDAGENT_DEBUG_SOURCE / name
        if src.exists():
            shutil.copy2(src, debug_dir / name)
        else:
            prod_src = RDAGENT_PROD_SOURCE / name
            if prod_src.exists():
                shutil.copy2(prod_src, debug_dir / name)
    return stats


def copy_bin_to_wsl(args: argparse.Namespace, bin_dir: Path) -> None:
    target = args.wsl_copy_dir.rstrip("/")
    if "candidate" not in Path(target).name.lower():
        raise ValueError(f"Refusing to overwrite non-candidate WSL path: {target}")
    src = win_to_wsl(bin_dir)
    parent = str(Path(target).parent).replace("\\", "/")
    command = (
        f"set -euo pipefail; "
        f"rm -rf {shlex.quote(target)}; "
        f"mkdir -p {shlex.quote(parent)}; "
        f"cp -a {shlex.quote(src)} {shlex.quote(target)}"
    )
    res = subprocess.run(
        ["wsl", "-d", args.wsl_distro, "--", "bash", "-lc", command],
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
    )
    if res.returncode != 0:
        raise RuntimeError(f"WSL copy failed: {res.stderr}")


def file_size_map(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    out = {}
    for child in sorted(path.iterdir()):
        if child.is_file():
            out[child.name] = child.stat().st_size
    return out


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)
    started = time.time()

    start = to_date(args.start)
    end = to_date(args.end)
    index_codes = args.index_codes or DEFAULT_INDEX_CODES
    profile = ExportProfile(
        start=args.start,
        end=args.end,
        exchanges=["sh", "sz"],
        exclude_st=True,
        exclude_delisted_or_paused=True,
        ipo_filter_days=IPO_FILTER_DAYS,
        h5_ipo_mode="data_full_all_txt_only",
        bin_ipo_mode="feature_bins_full_history_all_txt_only",
        index_codes=index_codes,
    )

    snapshot_root = Path(args.snapshot_root)
    bin_root = Path(args.bin_root)
    csv_root = Path(args.csv_root)
    snapshot_dir = snapshot_root / args.snapshot_id
    bin_dir = bin_root / args.bin_id
    csv_dir = csv_root / args.bin_id

    if "candidate" not in args.snapshot_id.lower() or "candidate" not in args.bin_id.lower():
        raise ValueError("snapshot-id and bin-id must contain 'candidate'")

    static_schema_source = Path(args.static_schema_source)
    schema_columns = read_static_schema_columns(static_schema_source)
    logging.info(
        "Static schema preflight: source=%s columns=%s",
        static_schema_source,
        len(schema_columns),
    )

    pool = get_h5_universe(end, args.limit_instruments)
    if pool.empty:
        raise RuntimeError("H5 universe query returned no stocks")
    logging.info("H5 universe: %s instruments (SH/SZ, no BJ)", len(pool))
    if args.dry_run:
        print(json.dumps({"profile": asdict(profile), "h5_universe": len(pool)}, ensure_ascii=False, indent=2))
        return 0

    snapshot_dir = prepare_dir(snapshot_dir, allowed_root=snapshot_root, overwrite=args.overwrite_candidate)
    prepare_dir(csv_dir, allowed_root=csv_root, overwrite=args.overwrite_candidate)
    if not args.skip_bin:
        prepare_dir(bin_dir, allowed_root=bin_root, overwrite=args.overwrite_candidate)

    daily = load_daily_data(pool, start, end, args.load_batch_size)
    daily_norm = normalize_daily_columns(daily)
    instruments = sorted(daily_norm.index.get_level_values("instrument").unique().tolist())
    logging.info("Daily PV loaded: rows=%s instruments=%s", len(daily_norm), len(instruments))

    writer = SnapshotWriter()
    writer.write_daily_full(args.snapshot_id, daily)
    write_data_range_all_txt(daily_norm, snapshot_dir / "instruments" / "all.txt", ",")
    aux_stats = build_aux_and_static(
        snapshot_dir,
        instruments,
        daily_norm,
        start,
        end,
        static_schema_source,
    )
    meta_path = snapshot_dir / "meta.json"
    snapshot_meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    snapshot_meta["moneyflow_unit_contract"] = moneyflow_unit_contract_receipt()
    meta_path.write_text(json.dumps(snapshot_meta, ensure_ascii=False, indent=2), encoding="utf-8")
    field_map = export_field_map_for_snapshot(snapshot_id=args.snapshot_id, write_to_h5=True)

    official = compute_official_universe(daily_norm, pool, start, end)
    if official.empty:
        raise RuntimeError("Official IPO-filtered universe is empty")
    official_universe_path = snapshot_dir / "metadata" / "official_universe.csv"
    official_universe_path.parent.mkdir(parents=True, exist_ok=True)
    official.to_csv(official_universe_path, index=False)
    write_official_all_txt(official, snapshot_dir / "instruments" / "all.txt", ",")
    logging.info("Official IPO-filtered universe: %s instruments", len(official))

    bin_stats = {}
    if not args.skip_bin:
        stock_csv_dir = csv_dir / "stock_daily"
        csv_stats = write_stock_csv_for_bin(daily_norm, official, stock_csv_dir, start, end)
        dump_stock_bin(args, stock_csv_dir, bin_dir)
        write_official_all_txt(official, bin_dir / "instruments" / "all.txt", "\t")
        index_stats = dump_index_bins(args, index_codes, bin_dir, csv_dir, start, end)
        write_official_all_txt(official, bin_dir / "instruments" / "all.txt", "\t")
        last_end_dates = {"stock_daily": args.end}
        for code in index_codes:
            last_end_dates[f"index_{code}"] = args.end
        write_bin_meta(bin_dir, args, profile, last_end_dates)
        bin_stats = {
            "csv": csv_stats,
            "index": index_stats,
            "bin_dir": str(bin_dir),
            "feature_dirs": len(list((bin_dir / "features").iterdir())) if (bin_dir / "features").exists() else 0,
        }
        if not args.skip_wsl_copy:
            copy_bin_to_wsl(args, bin_dir)
            bin_stats["wsl_copy_dir"] = args.wsl_copy_dir

    rdagent_stats = {}
    if not args.skip_rdagent_copy:
        rdagent_stats = copy_rdagent_candidate(args, snapshot_dir)

    report = {
        "ok": True,
        "profile": asdict(profile),
        "snapshot_dir": str(snapshot_dir),
        "bin_dir": str(bin_dir) if not args.skip_bin else None,
        "csv_dir": str(csv_dir),
        "h5_universe": int(len(pool)),
        "daily_rows": int(len(daily_norm)),
        "daily_instruments": int(len(instruments)),
        "official_universe": int(len(official)),
        "aux": aux_stats,
        "field_map": field_map,
        "bin": bin_stats,
        "rdagent": rdagent_stats,
        "snapshot_file_sizes": file_size_map(snapshot_dir),
        "elapsed_seconds": round(time.time() - started, 3),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    report_path = snapshot_dir / "candidate_export_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("Candidate export completed: %s", report_path)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
