"""Export the frozen suspend_d candidate dataset (offline dataset build phase).

BUG-989 continuation: the QE/multi-alpha computation data plane must not
access PostgreSQL.  This script is part of the *offline dataset construction*
phase: it reads the authoritative ``market.suspend_d`` table read-only once,
and materializes a versioned candidate dataset (``suspend_d.parquet`` +
``manifest.json``) that QE runtimes consume exclusively as frozen files.

Hard rules honoured here:

- READ-ONLY transaction; no DDL/DML, no writes to any database.
- Output goes to an explicit *candidate* directory only; this script never
  touches production bin/H5/Parquet, never modifies a production symlink and
  never deletes old datasets.
- Universe contract: sh/sz only (``^[0-9]{6}\\.(SH|SZ)$``); Beijing Stock
  Exchange rows are excluded and counted in the manifest.
- Business semantics preserved: a row means the instrument was suspended on
  that trading day (``suspend_type='S'``; full-day when ``suspend_timing`` is
  NULL, intraday otherwise).  Rows are stored sparsely; the manifest carries
  an independent per-trading-day completeness receipt so "zero rows on a day"
  is provably different from "day not exported".
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SCHEMA_VERSION = "suspend_d_dataset_manifest_v1"
DATASET_KIND = "qe_suspend_filter_source"
SOURCE_CONTRACT = "tushare_suspend_d_shsz_S_v1"
CANONICAL_TS_CODE_RE = re.compile(r"^[0-9]{6}\.(SH|SZ)$")

# Frozen qlib bin pins this dataset must stay aligned with (BUG-989 contract;
# see backend/services/quantevolver/qe_dataset_contract.py).
FROZEN_BIN_SNAPSHOT_ID = "qlib_bin_st_pit_active_daily_candidate_20180801_20260630"
FROZEN_CALENDAR_SHA256 = "6ab71db126fd8c0173831162d5413691c33bfecbbc81db687d8a2de7cc776031"
FROZEN_INSTRUMENTS_SHA256 = "94c9d82de1ba60446d7d6114b39b1066fa3bda3f2a7b9787bb7f0ad4a2a05ca4"
FROZEN_UNIVERSE_KEY = "shsz_st_pit_active_v1"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_calendar(path: Path) -> list[date]:
    days: list[date] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for line in handle:
            text = line.strip()
            if text:
                days.append(date.fromisoformat(text[:10]))
    days.sort()
    if not days:
        raise SystemExit(f"calendar file is empty: {path}")
    return days


def _connect():
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env", override=False)
    # Task worktrees do not carry .env; an explicit fallback env file may be
    # supplied via controlled configuration (never hardcoded in source).
    fallback_env = os.getenv("AISTOCK_FALLBACK_ENV_FILE", "").strip()
    if fallback_env:
        load_dotenv(Path(fallback_env), override=False)
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, help="candidate output directory (created; must not exist)")
    parser.add_argument("--calendar-file", required=True, help="pinned frozen calendars/day.txt copy")
    parser.add_argument("--start", required=True, help="window start YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="window end YYYY-MM-DD (inclusive)")
    parser.add_argument("--dataset-id", required=True, help="versioned dataset id, must contain 'candidate'")
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir)
    if "candidate" not in args.dataset_id or "candidate" not in output_dir.name:
        raise SystemExit("candidate rule violated: --dataset-id and --output-dir name must contain 'candidate'")
    if output_dir.exists():
        raise SystemExit(f"refusing to overwrite existing directory: {output_dir}")
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if end < start:
        raise SystemExit(f"end {end} earlier than start {start}")

    calendar_path = Path(args.calendar_file)
    calendar_sha256 = _sha256_file(calendar_path)
    if calendar_sha256 != FROZEN_CALENDAR_SHA256:
        raise SystemExit(
            f"calendar pin mismatch: {calendar_path} sha256={calendar_sha256} "
            f"expected={FROZEN_CALENDAR_SHA256} (must equal the frozen qlib bin calendar)"
        )
    calendar_days = [d for d in _load_calendar(calendar_path) if start <= d <= end]
    if not calendar_days:
        raise SystemExit(f"no calendar days inside window {start}..{end}")

    import pandas as pd

    conn = _connect()
    try:
        conn.set_session(readonly=True, autocommit=True)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT trade_date, ts_code, suspend_type, suspend_timing
            FROM market.suspend_d
            WHERE trade_date BETWEEN %s AND %s
            ORDER BY trade_date, ts_code
            """,
            (start, end),
        )
        rows = cur.fetchall()

        # Sanity cross-check (read-only): the pinned frozen calendar must equal
        # the authoritative trading calendar inside the window.
        cur.execute(
            """
            SELECT DISTINCT cal_date FROM market.trading_calendar
            WHERE is_trading AND cal_date BETWEEN %s AND %s ORDER BY 1
            """,
            (start, end),
        )
        db_open_days = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if db_open_days != calendar_days:
        missing = sorted(set(calendar_days) - set(db_open_days))
        extra = sorted(set(db_open_days) - set(calendar_days))
        raise SystemExit(
            "frozen calendar does not match market.trading_calendar inside window: "
            f"missing_in_db={len(missing)} extra_in_db={len(extra)} "
            f"first_missing={missing[:3]} first_extra={extra[:3]}"
        )

    frame = pd.DataFrame(rows, columns=["trade_date", "ts_code", "suspend_type", "suspend_timing"])
    total_rows = int(len(frame))
    bj_rows = int(frame["ts_code"].astype(str).str.endswith(".BJ").sum()) if total_rows else 0
    frame = frame[frame["suspend_type"] == "S"].copy()
    s_rows_before_universe = int(len(frame))
    bj_s_rows = int(frame["ts_code"].astype(str).str.endswith(".BJ").sum()) if len(frame) else 0
    frame = frame[~frame["ts_code"].astype(str).str.endswith(".BJ")].copy()
    canonical_mask = frame["ts_code"].astype(str).map(lambda v: bool(CANONICAL_TS_CODE_RE.match(v)))
    non_canonical = sorted(frame.loc[~canonical_mask, "ts_code"].astype(str).unique().tolist())
    if non_canonical:
        raise SystemExit(f"non-canonical ts_code values inside sh/sz scope: {non_canonical[:10]}")
    frame = frame[canonical_mask].copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    frame = frame.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

    # Independent per-trading-day completeness receipt: every calendar day in
    # the window appears with an explicit row count (0 allowed).
    counts = frame.groupby("trade_date").size().to_dict() if len(frame) else {}
    daily_row_counts = {d.isoformat(): int(counts.get(d, 0)) for d in calendar_days}
    zero_suspension_days = [d for d, n in daily_row_counts.items() if n == 0]

    output_dir.mkdir(parents=True)
    parquet_path = output_dir / "suspend_d.parquet"
    frame_out = frame.assign(trade_date=pd.to_datetime(frame["trade_date"]))
    frame_out.to_parquet(parquet_path, engine="pyarrow", index=False)
    parquet_sha256 = _sha256_file(parquet_path)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": DATASET_KIND,
        "dataset_id": args.dataset_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "cutoff": end.isoformat(),
        "universe_key": FROZEN_UNIVERSE_KEY,
        "exchanges": ["sh", "sz"],
        "exclude_bj": True,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": {
            "schema": "market",
            "table": "suspend_d",
            "filter": "suspend_type='S'",
            "coding": "tushare_ts_code_upper",
            "contract": SOURCE_CONTRACT,
            "access": "read_only_offline_export",
        },
        "semantics": {
            "row_meaning": "instrument suspended on the trading day (suspend_type='S'); "
            "suspend_timing NULL = full-day, otherwise intraday window",
            "sparse_storage": True,
            "zero_rows_day_meaning": "exported and verified: zero suspensions that day",
        },
        "counts": {
            "row_count": int(len(frame)),
            "stock_count": int(frame["ts_code"].nunique()),
            "trade_date_count": len(calendar_days),
            "days_with_suspensions": sum(1 for n in daily_row_counts.values() if n > 0),
            "zero_suspension_day_count": len(zero_suspension_days),
            "source_rows_in_window": total_rows,
            "source_s_rows_before_universe_filter": s_rows_before_universe,
            "excluded_bj_rows": bj_rows,
            "excluded_bj_s_rows": bj_s_rows,
        },
        "alignment": {
            "frozen_bin_snapshot_id": FROZEN_BIN_SNAPSHOT_ID,
            "calendar_sha256": calendar_sha256,
            "instruments_sha256": FROZEN_INSTRUMENTS_SHA256,
        },
        "artifacts": {
            "suspend_d.parquet": {"sha256": parquet_sha256},
        },
        "zero_suspension_days": zero_suspension_days,
        "daily_row_counts": daily_row_counts,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    print(
        "EXPORTED suspend candidate dataset:\n"
        f"  dir={output_dir}\n"
        f"  dataset_id={args.dataset_id}\n"
        f"  rows={len(frame)} stocks={frame['ts_code'].nunique()} "
        f"trade_dates={len(calendar_days)} zero_suspension_days={len(zero_suspension_days)}\n"
        f"  suspend_d.parquet sha256={parquet_sha256}\n"
        f"  manifest sha256={_sha256_file(manifest_path)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
