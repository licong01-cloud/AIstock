"""Run the read-only full-market ``suspend_d`` coverage audit."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Sequence

import psycopg2


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.suspend_d_coverage import (  # noqa: E402
    SuspendCoverageError,
    audit_suspend_d_coverage,
)
from scripts.repair_minute_via_minute_api import (  # noqa: E402
    _database_config,
    _discover_env_file,
)


def canonical_json_bytes(payload: Any) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-db", choices=("dev", "production"), required=True)
    parser.add_argument("--end-date", type=_date, required=True)
    bounds = parser.add_mutually_exclusive_group(required=True)
    bounds.add_argument("--start-date", type=_date)
    bounds.add_argument("--lookback-trading-days", type=int)
    parser.add_argument("--max-findings", type=int, default=500)
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    conn = psycopg2.connect(
        **_database_config(args.target_db, _discover_env_file(None))
    )
    try:
        receipt = audit_suspend_d_coverage(
            conn,
            start_date=args.start_date,
            end_date=args.end_date,
            lookback_trading_days=args.lookback_trading_days,
            max_findings=args.max_findings,
            statement_timeout_ms=args.statement_timeout_ms,
        )
    except SuspendCoverageError as exc:
        print(f"suspend_d coverage audit failed: {exc}", file=sys.stderr)
        return 3
    finally:
        conn.rollback()
        conn.close()

    raw = canonical_json_bytes(receipt)
    if args.output is not None:
        output = args.output.expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("xb") as handle:
            handle.write(raw)
    print(raw.decode("utf-8"), end="")
    return 0 if receipt["summary"]["coverage_complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
