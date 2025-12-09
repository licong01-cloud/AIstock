from __future__ import annotations

"""Benchmark script for /api/v1/news/fast equivalent SQL.

Usage (from project root, with AIstock env activated):

    python scripts/test_news_fast_query.py --limit 20 --offset 0 --source sina_finance --runs 5

It uses the same SQL as backend.routers.news.fast_news and the same
PostgreSQL configuration (via .env and backend.db.pg_pool).
"""

import argparse
import time
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


# Ensure project root (which contains the ``backend`` package) is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables (DB config, etc.)
load_dotenv(override=True)

from backend.db.pg_pool import get_conn, init_db_pool


SQL_FAST_NEWS = """
SELECT id,
       source,
       title,
       content,
       url,
       publish_time,
       is_important
  FROM app.news_articles_ts
  {where}
 ORDER BY publish_time DESC
 LIMIT %s OFFSET %s
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark news.fast query")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--source", type=str, default="", help="Optional exact source filter")
    p.add_argument("--runs", type=int, default=5, help="Number of times to repeat the query")
    return p.parse_args()


def run_once(limit: int, offset: int, source: Optional[str]) -> float:
    where = ""
    params = []
    if source:
        where = "WHERE source = %s"
        params.append(source)
    params.append(limit)
    params.append(offset)

    sql = SQL_FAST_NEWS.format(where=where)

    t0 = time.perf_counter()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            _rows = cur.fetchall()
    t1 = time.perf_counter()
    return t1 - t0


def main() -> int:
    args = parse_args()

    print(
        f"Running news.fast benchmark: limit={args.limit}, offset={args.offset}, "
        f"source='{args.source}', runs={args.runs}",
        flush=True,
    )

    # Initialize connection pool once and measure how long it takes
    t0 = time.perf_counter()
    init_db_pool(minconn=1, maxconn=5)
    t1 = time.perf_counter()
    print(f"Connection pool init time: {t1 - t0:.4f} s", flush=True)

    durations = []
    for i in range(args.runs):
        dt = run_once(args.limit, args.offset, args.source or None)
        durations.append(dt)
        print(f"Run {i+1}/{args.runs}: {dt:.4f} s", flush=True)

    if durations:
        avg = sum(durations) / len(durations)
        print(f"Average: {avg:.4f} s over {len(durations)} runs", flush=True)

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
