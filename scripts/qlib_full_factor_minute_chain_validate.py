from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
BACKEND_ROOT = PROJECT_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

try:
    from backend.db.pg_pool import get_conn
except Exception:  # pragma: no cover - WSL backtest stage does not need DB.
    get_conn = None


DEFAULT_CODES = [
    "000001.SZ",
    "000063.SZ",
    "000333.SZ",
    "000651.SZ",
    "000858.SZ",
    "600000.SH",
    "600036.SH",
    "600519.SH",
    "601318.SH",
    "601688.SH",
]

DAILY_FIELDS = [
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
]

MINUTE_FIELDS = [
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
]

SNAPSHOT_AUX_FILES = [
    "daily_basic.h5",
    "moneyflow.h5",
    "bak_basic.h5",
    "cyq_perf.h5",
    "sector_data.h5",
    "margin_detail.h5",
]


@dataclass
class Paths:
    output_root: Path
    csv_dir: Path
    minute_bin_dir: Path
    reports_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the 2026-04-28 candidate with all available daily/static factor "
            "datasets and a small-stock Qlib NestedExecutor minute execution backtest."
        )
    )
    parser.add_argument("--stage", choices=["all", "export", "dump", "backtest"], default="all")
    parser.add_argument("--codes", default=",".join(DEFAULT_CODES))
    parser.add_argument("--daily-start", default="2018-08-01")
    parser.add_argument("--minute-start", default="2024-01-02")
    parser.add_argument("--end", default="2026-04-28")
    parser.add_argument("--output-root", default=str(PROJECT_ROOT / "qlib_minute_validation" / "full_factor_minute_chain_20260428_candidate"))
    parser.add_argument("--reports-dir", default=str(PROJECT_ROOT / "reports" / "qlib_full_factor_minute_chain_20260428"))
    parser.add_argument("--daily-provider-wsl", default="/home/lc999/data/qlib_bin_20260428_shsz_candidate")
    parser.add_argument("--snapshot-dir-wsl", default="/mnt/f/Dev/AIstock/qlib_snapshots/qlib_20260428_shsz_candidate")
    parser.add_argument("--wsl-distro", default=os.getenv("QLIB_WSL_DISTRO", "Ubuntu"))
    parser.add_argument("--wsl-conda-sh", default=os.getenv("QLIB_WSL_CONDA_SH", "/home/lc999/miniconda3/etc/profile.d/conda.sh"))
    parser.add_argument("--wsl-conda-env", default=os.getenv("QLIB_WSL_CONDA_ENV", "rdagent-gpu"))
    parser.add_argument("--rdagent-root-wsl", default=os.getenv("QLIB_RDAGENT_ROOT_WSL", "/mnt/f/Dev/RD-Agent-main"))
    parser.add_argument("--account", type=float, default=10_000_000)
    parser.add_argument("--topk", type=int, default=5)
    parser.add_argument("--drop", type=int, default=2)
    parser.add_argument("--overwrite", action="store_true", help="Allow rebuilding candidate CSV/bin/report dirs.")
    parser.add_argument("--skip-export-if-present", action="store_true")
    parser.add_argument("--export-only-first-n", type=int, default=None, help="Debug only.")
    return parser.parse_args()


def split_codes(value: str) -> list[str]:
    codes = [part.strip().upper() for part in value.replace(";", ",").split(",") if part.strip()]
    if not codes:
        raise ValueError("No stock codes were provided")
    return codes


def win_to_wsl(path: str | Path) -> str:
    text = str(path).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


def make_paths(args: argparse.Namespace) -> Paths:
    root = Path(args.output_root)
    return Paths(
        output_root=root,
        csv_dir=root / "csv",
        minute_bin_dir=root / "bin",
        reports_dir=Path(args.reports_dir),
    )


def safe_candidate_dir(path: Path) -> Path:
    resolved = path.resolve()
    if "candidate" not in resolved.name.lower() and "candidate" not in str(resolved.parent).lower():
        raise ValueError(f"Refusing to remove or rebuild non-candidate path: {resolved}")
    return resolved


def prepare_dir(path: Path, overwrite: bool) -> None:
    resolved = safe_candidate_dir(path)
    if resolved.exists():
        if not overwrite:
            raise FileExistsError(f"{resolved} already exists; pass --overwrite to rebuild candidate outputs")
        shutil.rmtree(resolved)
    resolved.mkdir(parents=True, exist_ok=True)


def run_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    if get_conn is None:
        raise RuntimeError("DB connection helper is unavailable in this Python environment")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("set enable_seqscan=off")
        return pd.read_sql(sql, conn, params=params)


def export_selected_minute_csv(args: argparse.Namespace, paths: Paths, codes: list[str]) -> dict:
    if paths.csv_dir.exists() and args.skip_export_if_present and list(paths.csv_dir.glob("*.csv")):
        return {"skipped": True, "reason": "CSV already present", "csv_files": len(list(paths.csv_dir.glob("*.csv")))}
    prepare_dir(paths.csv_dir, args.overwrite)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)

    start_ts = f"{args.minute_start} 00:00:00+08"
    end_exclusive = (pd.Timestamp(args.end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d 00:00:00+08")
    codes = codes[: args.export_only_first_n] if args.export_only_first_n else codes

    print(f"[INFO] Exporting minute CSV for {len(codes)} codes: {', '.join(codes)}")
    print(f"[INFO] Minute range: {args.minute_start} ~ {args.end}")

    adj = run_df(
        """
        SELECT ts_code, trade_date, adj_factor
        FROM market.adj_factor
        WHERE ts_code = ANY(%(codes)s)
          AND trade_date >= %(start)s
          AND trade_date <= %(end)s
        ORDER BY ts_code, trade_date
        """,
        {"codes": codes, "start": args.minute_start, "end": args.end},
    )
    if adj.empty:
        raise RuntimeError("No adj_factor rows found for selected codes")
    adj["trade_date"] = pd.to_datetime(adj["trade_date"]).dt.date
    adj["adj_factor"] = pd.to_numeric(adj["adj_factor"], errors="coerce")
    adj["qfq_factor"] = adj["adj_factor"] / adj.groupby("ts_code")["adj_factor"].transform("max")

    limits = run_df(
        """
        SELECT ts_code, trade_date, pre_close, up_limit, down_limit
        FROM market.stk_limit
        WHERE ts_code = ANY(%(codes)s)
          AND trade_date >= %(start)s
          AND trade_date <= %(end)s
        ORDER BY ts_code, trade_date
        """,
        {"codes": codes, "start": args.minute_start, "end": args.end},
    )
    if limits.empty:
        raise RuntimeError("No stk_limit rows found for selected codes")
    limits["trade_date"] = pd.to_datetime(limits["trade_date"]).dt.date
    for col in ["pre_close", "up_limit", "down_limit"]:
        limits[col] = pd.to_numeric(limits[col], errors="coerce")

    calendar = run_df(
        """
        SELECT cal_date
        FROM market.trading_calendar
        WHERE is_trading = true
          AND cal_date >= %(start)s
          AND cal_date <= %(end)s
        ORDER BY cal_date
        """,
        {"start": args.minute_start, "end": args.end},
    )
    trading_dates = pd.to_datetime(calendar["cal_date"]).dt.date.tolist()

    suspend = run_df(
        """
        SELECT ts_code, trade_date
        FROM market.suspend_d
        WHERE ts_code = ANY(%(codes)s)
          AND trade_date >= %(start)s
          AND trade_date <= %(end)s
        """,
        {"codes": codes, "start": args.minute_start, "end": args.end},
    )
    suspend_keys = set()
    if not suspend.empty:
        suspend["trade_date"] = pd.to_datetime(suspend["trade_date"]).dt.date
        suspend_keys = set(zip(suspend["ts_code"], suspend["trade_date"]))

    all_stats: list[pd.DataFrame] = []
    total_rows = 0
    t0 = time.time()
    for i, code in enumerate(codes, start=1):
        sql = """
            SELECT trade_time AT TIME ZONE 'Asia/Shanghai' AS trade_time,
                   ts_code, open_li, high_li, low_li, close_li,
                   volume_hand, amount_li
            FROM market.kline_minute_raw
            WHERE ts_code = %(code)s
              AND freq = '1m'
              AND trade_time >= %(start_ts)s::timestamptz
              AND trade_time < %(end_exclusive)s::timestamptz
            ORDER BY trade_time
        """
        df = run_df(sql, {"code": code, "start_ts": start_ts, "end_exclusive": end_exclusive})
        if df.empty:
            raise RuntimeError(f"No minute rows found for {code}")
        df["trade_time"] = pd.to_datetime(df["trade_time"])
        df["trade_date"] = df["trade_time"].dt.date
        df = df.merge(adj[["ts_code", "trade_date", "qfq_factor"]], on=["ts_code", "trade_date"], how="left")
        df = df.merge(
            limits[["ts_code", "trade_date", "pre_close", "up_limit", "down_limit"]],
            on=["ts_code", "trade_date"],
            how="left",
        )
        df["qfq_factor"] = pd.to_numeric(df["qfq_factor"], errors="coerce")
        if df["qfq_factor"].isna().any():
            missing = df.loc[df["qfq_factor"].isna(), "trade_date"].drop_duplicates().head(10).tolist()
            raise RuntimeError(f"{code} has minute rows without adj_factor, examples={missing}")

        scale = 1000.0
        raw_open = pd.to_numeric(df["open_li"], errors="coerce") / scale
        raw_high = pd.to_numeric(df["high_li"], errors="coerce") / scale
        raw_low = pd.to_numeric(df["low_li"], errors="coerce") / scale
        raw_close = pd.to_numeric(df["close_li"], errors="coerce") / scale
        qfq = pd.to_numeric(df["qfq_factor"], errors="coerce")

        df["open"] = (raw_open * qfq).astype("float32")
        df["high"] = (raw_high * qfq).astype("float32")
        df["low"] = (raw_low * qfq).astype("float32")
        df["close"] = (raw_close * qfq).astype("float32")
        df["volume"] = (pd.to_numeric(df["volume_hand"], errors="coerce") * 100.0 / qfq).astype("float32")
        df["amount"] = (pd.to_numeric(df["amount_li"], errors="coerce") / scale).astype("float32")
        df["factor"] = qfq.astype("float32")
        df["up_limit_price"] = pd.to_numeric(df["up_limit"], errors="coerce").astype("float32")
        df["down_limit_price"] = pd.to_numeric(df["down_limit"], errors="coerce").astype("float32")
        df["prev_close"] = pd.to_numeric(df["pre_close"], errors="coerce").astype("float32")
        have_limits = df["up_limit_price"].notna() & df["down_limit_price"].notna()
        df["limit_up"] = np.where(have_limits, (raw_close >= df["up_limit_price"] - 1e-4).astype("float32"), np.nan)
        df["limit_down"] = np.where(have_limits, (raw_close <= df["down_limit_price"] + 1e-4).astype("float32"), np.nan)
        df["date"] = df["trade_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df["symbol"] = code

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
        df[csv_cols].to_csv(paths.csv_dir / f"{code}.csv", index=False)

        stats = (
            df.groupby("trade_date")
            .agg(
                bars=("close", "size"),
                first_time=("trade_time", lambda s: s.min().strftime("%H:%M:%S")),
                last_time=("trade_time", lambda s: s.max().strftime("%H:%M:%S")),
                close_nan=("close", lambda s: int(s.isna().sum())),
                limit_nan=("up_limit_price", lambda s: int(s.isna().sum())),
            )
            .reset_index()
        )
        stats["ts_code"] = code
        all_stats.append(stats)
        total_rows += len(df)
        print(f"[INFO] [{i}/{len(codes)}] {code}: rows={len(df):,}, dates={len(stats)}, elapsed={time.time() - t0:.1f}s")

    stat_df = pd.concat(all_stats, ignore_index=True)
    expected = pd.MultiIndex.from_product([codes, trading_dates], names=["ts_code", "trade_date"]).to_frame(index=False)
    stat_df["trade_date"] = pd.to_datetime(stat_df["trade_date"]).dt.date
    merged = expected.merge(stat_df, on=["ts_code", "trade_date"], how="left")
    merged["has_suspend_d"] = [bool((row.ts_code, row.trade_date) in suspend_keys) for row in merged.itertuples(index=False)]
    missing = merged[merged["bars"].isna()].copy()
    bad_bars = merged[merged["bars"].notna() & ~merged["bars"].isin([240, 241])].copy()
    bad_times = merged[
        merged["bars"].notna()
        & ~(
            ((merged["bars"] == 240) & (merged["first_time"] == "09:31:00") & (merged["last_time"] == "15:00:00"))
            | ((merged["bars"] == 241) & (merged["first_time"] == "09:30:00") & (merged["last_time"] == "15:00:00"))
        )
    ].copy()

    stat_df.to_csv(paths.reports_dir / "minute_db_stock_date_bars.csv", index=False)
    missing.to_csv(paths.reports_dir / "minute_db_missing_stock_dates.csv", index=False)
    bad_bars.to_csv(paths.reports_dir / "minute_db_bad_bar_counts.csv", index=False)
    bad_times.to_csv(paths.reports_dir / "minute_db_bad_time_ranges.csv", index=False)

    summary = {
        "stage": "export",
        "codes": codes,
        "minute_start": args.minute_start,
        "end": args.end,
        "trading_dates": len(trading_dates),
        "csv_files": len(list(paths.csv_dir.glob("*.csv"))),
        "csv_rows": int(total_rows),
        "stock_dates_expected": int(len(expected)),
        "stock_dates_with_minutes": int(merged["bars"].notna().sum()),
        "missing_stock_dates": int(len(missing)),
        "missing_with_suspend_d": int(missing["has_suspend_d"].sum()) if not missing.empty else 0,
        "bad_bar_count_stock_dates": int(len(bad_bars)),
        "bad_time_range_stock_dates": int(len(bad_times)),
        "bar_count_distribution": {
            str(k): int(v) for k, v in merged["bars"].dropna().astype(int).value_counts().sort_index().to_dict().items()
        },
        "first_last_distribution": (
            merged.dropna(subset=["bars"]).groupby(["bars", "first_time", "last_time"]).size().reset_index(name="stock_dates").to_dict(orient="records")
        ),
    }
    (paths.reports_dir / "minute_export_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if summary["missing_stock_dates"] or summary["bad_bar_count_stock_dates"] or summary["bad_time_range_stock_dates"]:
        raise RuntimeError(f"Minute DB coverage failed: {summary}")
    print(f"[INFO] Minute CSV export complete: rows={total_rows:,}, files={summary['csv_files']}")
    return summary


def run_wsl(args: argparse.Namespace, command: str, timeout: int | None = None) -> subprocess.CompletedProcess:
    full = (
        f"source {args.wsl_conda_sh} && conda activate {args.wsl_conda_env} && "
        f"export PYTHONUNBUFFERED=1 && {command}"
    )
    return subprocess.run(
        ["wsl", "-d", args.wsl_distro, "--", "bash", "-lc", full],
        cwd=PROJECT_ROOT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=True,
    )


def dump_minute_bin(args: argparse.Namespace, paths: Paths) -> dict:
    prepare_dir(paths.minute_bin_dir, args.overwrite)
    csv_wsl = win_to_wsl(paths.csv_dir)
    bin_wsl = win_to_wsl(paths.minute_bin_dir)
    dump = f"{args.rdagent_root_wsl.rstrip('/')}/scripts/dump_bin.py"
    cmd = (
        f"python {dump} dump_all "
        f"--data_path {csv_wsl} "
        f"--qlib_dir {bin_wsl} "
        f"--freq 1min "
        f"--date_field_name date "
        f"--symbol_field_name symbol "
        f"--exclude_fields date,symbol"
    )
    print(f"[INFO] Running dump_bin: {cmd}")
    t0 = time.time()
    run_wsl(args, cmd, timeout=None)
    elapsed = time.time() - t0
    summary = {"stage": "dump", "minute_bin_dir": str(paths.minute_bin_dir), "elapsed_sec": elapsed}
    paths.reports_dir.mkdir(parents=True, exist_ok=True)
    (paths.reports_dir / "minute_dump_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[INFO] dump_bin complete in {elapsed:.1f}s")
    return summary


def flatten_qlib(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).lstrip("$") for col in out.columns]
    idx = out.index
    frame = idx.to_frame(index=False)
    names = list(idx.names)
    if "datetime" in names and "instrument" in names:
        dt_col = names.index("datetime")
        inst_col = names.index("instrument")
    else:
        parsed0 = pd.to_datetime(frame.iloc[:, 0], errors="coerce")
        parsed1 = pd.to_datetime(frame.iloc[:, 1], errors="coerce")
        dt_col, inst_col = (0, 1) if parsed0.notna().sum() >= parsed1.notna().sum() else (1, 0)
    normalized = pd.DataFrame(
        {
            "datetime": pd.to_datetime(frame.iloc[:, dt_col]).dt.normalize(),
            "instrument": frame.iloc[:, inst_col].astype(str).str.upper(),
        }
    )
    out.index = pd.MultiIndex.from_frame(normalized, names=["datetime", "instrument"])
    return out.sort_index()


def source_for_feature(col: str) -> str:
    if col.startswith("db_"):
        return "daily_basic"
    if col.startswith("bb_"):
        return "bak_basic"
    if col.startswith("cp_"):
        return "cyq_perf"
    if col == "l2_code_id" or col.startswith("sw2_"):
        return "sector_data"
    if col.startswith("md_"):
        return "margin_detail"
    if col.startswith("mf_"):
        if any(token in col for token in ["_5d", "_20d", "ratio", "share"]):
            return "moneyflow_derived"
        return "moneyflow"
    if col.startswith("value_") or col.startswith("size_") or col.startswith("liquidity_") or col.startswith("PriceStrength"):
        return "static_derived"
    return "daily_qlib_bin"


def coverage_frame(df: pd.DataFrame, source_hint: str | None = None) -> pd.DataFrame:
    rows = []
    total = len(df)
    for col in df.columns:
        non_null = int(pd.to_numeric(df[col], errors="coerce").notna().sum())
        rows.append(
            {
                "source": source_hint or source_for_feature(str(col)),
                "feature": str(col),
                "rows": int(total),
                "non_null": non_null,
                "coverage": float(non_null / total) if total else 0.0,
            }
        )
    return pd.DataFrame(rows)


def read_static_selected(snapshot_dir: Path, codes: list[str], start: str, end: str) -> pd.DataFrame:
    path = snapshot_dir / "static_factors.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        df = pd.read_parquet(
            path,
            filters=[
                ("instrument", "in", codes),
                ("datetime", ">=", pd.Timestamp(start)),
                ("datetime", "<=", pd.Timestamp(end)),
            ],
        )
    except Exception as exc:
        print(f"[WARN] static parquet filtered read failed, fallback to full selected-column scan: {exc}")
        df = pd.read_parquet(path)
        df = df[df["instrument"].isin(codes)].copy()
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.normalize()
    df["instrument"] = df["instrument"].astype(str).str.upper()
    dates = df["datetime"]
    df = df[(dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end))]
    return df.set_index(["datetime", "instrument"]).sort_index()


def read_margin_selected(snapshot_dir: Path, codes: list[str], start: str, end: str) -> pd.DataFrame:
    path = snapshot_dir / "margin_detail.h5"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_hdf(path, "data")
    if df.empty:
        return df
    df = df.sort_index()
    inst = df.index.get_level_values("instrument").astype(str).str.upper()
    dt = pd.to_datetime(df.index.get_level_values("datetime")).normalize()
    mask = inst.isin(codes) & (dt >= pd.Timestamp(start)) & (dt <= pd.Timestamp(end))
    out = df.loc[mask].copy()
    frame = pd.DataFrame({"datetime": dt[mask], "instrument": inst[mask]})
    out.index = pd.MultiIndex.from_frame(frame, names=["datetime", "instrument"])
    return out.sort_index()


def df_to_markdown(df: pd.DataFrame, index: bool = False, max_rows: int | None = None) -> str:
    table = df.reset_index() if index else df.copy()
    if max_rows is not None:
        table = table.head(max_rows)
    if table.empty:
        return "_empty_"
    cols = [str(c) for c in table.columns]
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for row in table.itertuples(index=False):
        vals = []
        for value in row:
            if isinstance(value, float):
                vals.append("" if math.isnan(value) else f"{value:.6g}")
            else:
                vals.append(str(value))
        lines.append("| " + " | ".join(v.replace("|", "\\|") for v in vals) + " |")
    return "\n".join(lines)


def prepare_validation_day_provider(day_provider: Path, end: str) -> Path:
    dst = Path("/home/lc999/data/qlib_bin_20260428_shsz_candidate_validation_calendar")
    if dst.exists() or dst.is_symlink():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)
    os.symlink(day_provider / "features", dst / "features", target_is_directory=True)
    os.symlink(day_provider / "instruments", dst / "instruments", target_is_directory=True)
    (dst / "calendars").mkdir()
    src_cal = day_provider / "calendars" / "day.txt"
    lines = [line.strip() for line in src_cal.read_text(encoding="utf-8").splitlines() if line.strip()]
    future = (pd.Timestamp(end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    if pd.Timestamp(lines[-1]) <= pd.Timestamp(end):
        lines.append(future)
    (dst / "calendars" / "day.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return dst


def append_minute_future_calendar(minute_bin: Path, end: str) -> None:
    cal = minute_bin / "calendars" / "1min.txt"
    lines = [line.strip() for line in cal.read_text(encoding="utf-8").splitlines() if line.strip()]
    future = (pd.Timestamp(end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d 09:31:00")
    if pd.Timestamp(lines[-1]) <= pd.Timestamp(f"{end} 15:00:00"):
        lines.append(future)
        cal.write_text("\n".join(lines) + "\n", encoding="utf-8")


def summarize_portfolio(portfolio_metric_dict: dict) -> tuple[dict, pd.DataFrame | None]:
    report = None
    if isinstance(portfolio_metric_dict, dict):
        for value in portfolio_metric_dict.values():
            if isinstance(value, tuple) and len(value) >= 1 and isinstance(value[0], pd.DataFrame):
                report = value[0]
                break
            if isinstance(value, pd.DataFrame):
                report = value
                break
    summary = {"report_found": report is not None}
    if report is None:
        return summary, None
    summary["report_rows"] = int(len(report))
    if "return" in report.columns:
        returns = pd.to_numeric(report["return"], errors="coerce").fillna(0.0)
        nav = (1.0 + returns).cumprod()
        summary.update(
            {
                "total_return": float(nav.iloc[-1] - 1.0) if len(nav) else None,
                "annualized_return": float(nav.iloc[-1] ** (252 / len(nav)) - 1.0) if len(nav) else None,
                "daily_return_mean": float(returns.mean()),
                "daily_return_std": float(returns.std()),
                "sharpe_annualized": float(np.sqrt(252) * returns.mean() / returns.std()) if returns.std() > 0 else None,
                "max_drawdown": float((nav / nav.cummax() - 1.0).min()) if len(nav) else None,
                "nav_last": float(nav.iloc[-1]) if len(nav) else None,
            }
        )
    if "turnover" in report.columns:
        summary["turnover_mean"] = float(pd.to_numeric(report["turnover"], errors="coerce").mean())
    if "cost" in report.columns:
        summary["cost_sum"] = float(pd.to_numeric(report["cost"], errors="coerce").sum())
    return summary, report


def json_default(obj):
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, pd.Timestamp):
        return obj.strftime("%Y-%m-%d")
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError(type(obj).__name__)


def run_backtest_stage(args: argparse.Namespace, paths: Paths, codes: list[str]) -> dict:
    import qlib
    from qlib.backtest import backtest as qlib_backtest
    from qlib.backtest.executor import NestedExecutor, SimulatorExecutor
    from qlib.config import C
    from qlib.contrib.strategy.rule_strategy import TWAPStrategy
    from qlib.contrib.strategy.signal_strategy import TopkDropoutStrategy
    from qlib.data import D

    reports_dir = Path(win_to_wsl(paths.reports_dir))
    reports_dir.mkdir(parents=True, exist_ok=True)
    minute_bin = Path(win_to_wsl(paths.minute_bin_dir))
    snapshot_dir = Path(args.snapshot_dir_wsl)
    day_provider = prepare_validation_day_provider(Path(args.daily_provider_wsl), args.end)
    append_minute_future_calendar(minute_bin, args.end)

    C["kernels"] = 1
    qlib.init(
        provider_uri={"day": str(day_provider), "1min": str(minute_bin)},
        region="cn",
        dataset_cache=None,
        expression_cache=None,
    )

    print("[INFO] Loading daily Qlib fields for full daily coverage")
    daily = flatten_qlib(D.features(codes, DAILY_FIELDS, start_time=args.daily_start, end_time=args.end, freq="day"))
    daily_cov = coverage_frame(daily, "daily_qlib_bin")

    print("[INFO] Loading static_factors parquet for selected instruments")
    static = read_static_selected(snapshot_dir, codes, args.daily_start, args.end)
    static_cov = coverage_frame(static)

    print("[INFO] Loading margin_detail H5 for selected instruments")
    margin = read_margin_selected(snapshot_dir, codes, args.daily_start, args.end)
    margin_cov = coverage_frame(margin, "margin_detail") if not margin.empty else pd.DataFrame()

    feature_panel = daily.join(static, how="outer", rsuffix="_static")
    if not margin.empty:
        overlap = feature_panel.columns.intersection(margin.columns)
        margin_use = margin.drop(columns=list(overlap)) if len(overlap) else margin
        feature_panel = feature_panel.join(margin_use, how="outer")
    feature_panel = feature_panel.sort_index()
    for col in feature_panel.columns:
        feature_panel[col] = pd.to_numeric(feature_panel[col], errors="coerce")

    coverage = pd.concat([daily_cov, static_cov, margin_cov], ignore_index=True)
    coverage.to_csv(reports_dir / "full_daily_factor_coverage.csv", index=False)
    source_summary = (
        coverage.groupby("source")
        .agg(features=("feature", "count"), min_coverage=("coverage", "min"), mean_coverage=("coverage", "mean"))
        .reset_index()
        .sort_values("source")
    )
    source_summary.to_csv(reports_dir / "full_daily_factor_source_summary.csv", index=False)
    low_coverage = coverage[coverage["coverage"] < 0.95].sort_values(["coverage", "source", "feature"])
    low_coverage.to_csv(reports_dir / "full_daily_low_coverage_features.csv", index=False)

    print("[INFO] Loading Qlib minute fields from generated minute bin")
    minute = D.features(
        codes,
        MINUTE_FIELDS,
        start_time=f"{args.minute_start} 09:30:00",
        end_time=f"{args.end} 15:00:00",
        freq="1min",
    )
    minute_flat = minute.copy()
    minute_flat.columns = [str(c).lstrip("$") for c in minute_flat.columns]
    min_nan = minute_flat[["open", "high", "low", "close", "volume", "amount", "factor", "limit_up", "limit_down"]].isna().sum()
    minute_index = minute_flat.index.to_frame(index=False)
    names = list(minute_flat.index.names)
    dt_col = names.index("datetime") if "datetime" in names else 1
    inst_col = names.index("instrument") if "instrument" in names else 0
    minute_dates = pd.to_datetime(minute_index.iloc[:, dt_col]).dt.normalize()
    minute_inst = minute_index.iloc[:, inst_col].astype(str).str.upper()
    minute_nonnull = pd.DataFrame({"datetime": minute_dates, "instrument": minute_inst, "close_ok": minute_flat["close"].notna().to_numpy()})
    minute_nonnull["date"] = minute_nonnull["datetime"].dt.date
    qlib_minute_counts = (
        minute_nonnull[minute_nonnull["close_ok"]]
        .groupby(["instrument", "date"])
        .size()
        .reset_index(name="non_null_close_bars")
    )
    qlib_minute_counts.to_csv(reports_dir / "qlib_minute_non_null_bars.csv", index=False)

    print("[INFO] Building no-leak full-factor signal")
    dates = feature_panel.index.get_level_values("datetime")
    bt_panel = feature_panel[(dates >= pd.Timestamp(args.minute_start) - pd.Timedelta(days=10)) & (dates <= pd.Timestamp(args.end))]
    ranked = bt_panel.groupby(level="datetime").rank(method="average", pct=True) - 0.5
    score = ranked.mean(axis=1, skipna=True).fillna(0.0)
    shifted = score.groupby(level="instrument").shift(1)
    sig_dates = shifted.index.get_level_values("datetime")
    signal = shifted[(sig_dates >= pd.Timestamp(args.minute_start)) & (sig_dates <= pd.Timestamp(args.end))].dropna().to_frame("score")
    signal.to_parquet(reports_dir / "full_factor_shifted_signal.parquet")

    print(f"[INFO] Signal rows={len(signal)}, dates={signal.index.get_level_values('datetime').nunique()}")
    t0 = time.time()
    portfolio_metric_dict, indicator_dict = qlib_backtest(
        start_time=args.minute_start,
        end_time=args.end,
        strategy=TopkDropoutStrategy(signal=signal, topk=args.topk, n_drop=args.drop),
        executor=NestedExecutor(
            time_per_step="day",
            inner_executor=SimulatorExecutor(time_per_step="1min", generate_portfolio_metrics=False),
            inner_strategy=TWAPStrategy(),
            generate_portfolio_metrics=True,
        ),
        benchmark=None,
        account=args.account,
        exchange_kwargs={
            "freq": "1min",
            "limit_threshold": ("$limit_up", "$limit_down"),
            "deal_price": "close",
            "open_cost": 0.000095,
            "close_cost": 0.000595,
            "min_cost": 5,
            "trade_unit": 100,
            "subscribe_fields": [
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
            ],
        },
    )
    elapsed = time.time() - t0
    backtest_summary, portfolio_report = summarize_portfolio(portfolio_metric_dict)
    if portfolio_report is not None:
        portfolio_report.to_csv(reports_dir / "minute_chain_portfolio_report.csv")

    failures = []
    warnings = []
    required_daily = coverage[
        (coverage["source"] == "daily_qlib_bin")
        & (coverage["feature"].isin([c.lstrip("$") for c in DAILY_FIELDS]))
    ]
    required_daily_below_999 = required_daily[required_daily["coverage"] < 0.999]
    required_daily_below_99 = required_daily[required_daily["coverage"] < 0.99]
    if not required_daily_below_999.empty:
        warnings.append(
            "Required daily Qlib fields below 99.9% full-calendar coverage; "
            "these need suspension/date-range cross-check if not already classified: "
            f"{required_daily_below_999[['feature','coverage']].to_dict(orient='records')}"
        )
    if not required_daily_below_99.empty:
        failures.append(f"Required daily Qlib fields below 99% coverage: {required_daily_below_99[['feature','coverage']].to_dict(orient='records')}")
    bad_minute_fields = {k: int(v) for k, v in min_nan.to_dict().items() if int(v) > 0}
    if bad_minute_fields:
        failures.append(f"Minute Qlib required fields contain NaN: {bad_minute_fields}")
    if not backtest_summary.get("report_found"):
        failures.append("Qlib NestedExecutor minute backtest returned no portfolio report")

    result = {
        "ok": not failures,
        "failures": failures,
        "warnings": warnings,
        "codes": codes,
        "daily_start": args.daily_start,
        "minute_start": args.minute_start,
        "end": args.end,
        "daily_provider_validation": str(day_provider),
        "minute_bin": str(minute_bin),
        "snapshot_dir": str(snapshot_dir),
        "feature_panel_rows": int(len(feature_panel)),
        "feature_panel_columns": int(feature_panel.shape[1]),
        "source_summary": source_summary.to_dict(orient="records"),
        "low_coverage_feature_count_lt_95pct": int(len(low_coverage)),
        "signal_rows": int(len(signal)),
        "signal_dates": int(signal.index.get_level_values("datetime").nunique()),
        "minute_rows_loaded": int(len(minute_flat)),
        "minute_required_nan": {k: int(v) for k, v in min_nan.to_dict().items()},
        "qlib_minute_bar_count_distribution": {
            str(k): int(v) for k, v in qlib_minute_counts["non_null_close_bars"].value_counts().sort_index().to_dict().items()
        },
        "backtest_elapsed_sec": elapsed,
        "backtest_summary": backtest_summary,
        "indicator_keys": sorted(list(indicator_dict.keys())) if isinstance(indicator_dict, dict) else [],
    }
    (reports_dir / "report.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    md = [
        "# Qlib Full-Factor Minute Chain Validation",
        "",
        f"- ok: `{result['ok']}`",
        f"- codes: {', '.join(codes)}",
        f"- daily coverage: `{args.daily_start} ~ {args.end}`",
        f"- minute execution: `{args.minute_start} ~ {args.end}`",
        f"- feature columns: `{result['feature_panel_columns']}`",
        f"- signal rows/dates: `{result['signal_rows']}` / `{result['signal_dates']}`",
        f"- minute rows loaded: `{result['minute_rows_loaded']}`",
        "",
        "## Source Coverage Summary",
        "",
        df_to_markdown(source_summary),
        "",
        "## Backtest Summary",
        "",
        "```json",
        json.dumps(backtest_summary, ensure_ascii=False, indent=2, default=json_default),
        "```",
        "",
        "## Minute Required NaN",
        "",
        "```json",
        json.dumps(result["minute_required_nan"], ensure_ascii=False, indent=2),
        "```",
    ]
    if failures:
        md.extend(["", "## Failures", ""])
        md.extend([f"- {failure}" for failure in failures])
    if warnings:
        md.extend(["", "## Warnings", ""])
        md.extend([f"- {warning}" for warning in warnings])
    if portfolio_report is not None:
        md.extend(["", "## Portfolio Tail", "", df_to_markdown(portfolio_report.tail(10), index=True)])
    (reports_dir / "report.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))
    if failures:
        raise RuntimeError("; ".join(failures))
    return result


def main() -> int:
    args = parse_args()
    codes = split_codes(args.codes)
    paths = make_paths(args)
    if args.stage in {"all", "export"}:
        export_selected_minute_csv(args, paths, codes)
    if args.stage in {"all", "dump"}:
        dump_minute_bin(args, paths)
    if args.stage == "all":
        script_wsl = win_to_wsl(Path(__file__).resolve())
        cmd = (
            f"python {script_wsl} --stage backtest "
            f"--codes {','.join(codes)} "
            f"--daily-start {args.daily_start} --minute-start {args.minute_start} --end {args.end} "
            f"--output-root {win_to_wsl(paths.output_root)} "
            f"--reports-dir {win_to_wsl(paths.reports_dir)} "
            f"--daily-provider-wsl {args.daily_provider_wsl} "
            f"--snapshot-dir-wsl {args.snapshot_dir_wsl} "
            f"--account {args.account:.0f} --topk {args.topk} --drop {args.drop} "
            f"--overwrite"
        )
        run_wsl(args, cmd, timeout=None)
    elif args.stage == "backtest":
        run_backtest_stage(args, paths, codes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
