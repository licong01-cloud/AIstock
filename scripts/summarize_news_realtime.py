from __future__ import annotations

"""Hourly summary for news_realtime ingestion.

Usage (from project root, with AIstock env activated):

    python scripts/summarize_news_realtime.py --window-minutes 60

This script only *reads* from app.news_articles_ts and prints a compact
summary for the last time window. It does not modify any tables and does
not affect other ingestion tasks.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Tuple

from dotenv import load_dotenv


# Ensure project root (which contains the ``backend`` package) is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(override=True)

from backend.db.pg_pool import get_conn  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Summarize news_realtime ingestion window")
    p.add_argument(
        "--window-minutes",
        type=int,
        default=60,
        help="Look back this many minutes based on ingest_time (default: 60)",
    )
    return p.parse_args()


def fetch_summary(window_minutes: int) -> List[Tuple[str, int, str, str]]:
    sql = """
        SELECT source,
               COUNT(*) AS cnt,
               MIN(publish_time) AS min_ts,
               MAX(publish_time) AS max_ts
          FROM app.news_articles_ts
         WHERE ingest_time >= NOW() - (%s || ' minutes')::interval
         GROUP BY source
         ORDER BY source
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (window_minutes,))
            rows = cur.fetchall()
    result: List[Tuple[str, int, str, str]] = []
    for source, cnt, min_ts, max_ts in rows:
        min_str = min_ts.strftime("%Y-%m-%d %H:%M:%S") if min_ts else ""
        max_str = max_ts.strftime("%Y-%m-%d %H:%M:%S") if max_ts else ""
        result.append((source, int(cnt or 0), min_str, max_str))
    return result


def main() -> int:
    args = parse_args()
    window = max(1, args.window_minutes)

    summary = fetch_summary(window)
    print(f"[news_realtime_summary] window(last_minutes)={window}")
    if not summary:
        print("[news_realtime_summary] no rows ingested in this window.")
        return 0

    for source, cnt, min_ts, max_ts in summary:
        print(
            f"[news_realtime_summary] source={source} count={cnt} "
            f"publish_time_range=[{min_ts} .. {max_ts}]",
            flush=True,
        )

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
