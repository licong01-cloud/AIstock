from __future__ import annotations

"""Inspect latest news rows from app.news_articles_ts for debugging.

Usage (from project root, with AIstock env activated):

    python scripts/inspect_news_latest.py --limit 20
    python scripts/inspect_news_latest.py --limit 20 --source sina_finance

It prints the latest N rows ordered by publish_time DESC, so you can
compare with what the Go program is showing.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


# Ensure project root (which contains the ``backend`` package) is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(override=True)

from backend.db.pg_pool import get_conn  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inspect latest news rows")
    p.add_argument("--limit", type=int, default=20, help="How many rows to show")
    p.add_argument("--source", type=str, default="", help="Optional exact source filter")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    limit = max(1, args.limit)
    source: Optional[str] = args.source or None

    print(
        f"Inspecting latest {limit} news rows from app.news_articles_ts, "
        f"source={'ALL' if not source else source}",
        flush=True,
    )

    where = ""
    params = []
    if source:
        where = "WHERE source = %s"
        params.append(source)
    params.append(limit)

    sql = f"""
        SELECT id,
               source,
               publish_time,
               is_important,
               title,
               content
          FROM app.news_articles_ts
          {where}
         ORDER BY publish_time DESC
         LIMIT %s
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

    if not rows:
        print("No rows found.", flush=True)
        return 0

    # Print simple table
    for row in rows:
        rid, source_val, publish_time, is_important, title, content = row
        ts = publish_time.astimezone() if publish_time.tzinfo else publish_time
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
        imp = "重要" if is_important else "普通"
        text = (title or "" or content or "").strip()
        if not text:
            text = content or "(无标题/内容)"
        snippet = text.replace("\n", " ")
        if len(snippet) > 80:
            snippet = snippet[:77] + "..."
        print(f"[{ts_str}] [{source_val}] [{imp}] id={rid} {snippet}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
