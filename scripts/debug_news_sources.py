from __future__ import annotations

"""Debug script: fetch latest CLS & Sina news directly from source APIs and
compare with DB records in app.news_articles_ts.

Usage (from project root, with AIstock env activated):

    python scripts/debug_news_sources.py --limit 50

It will:
- call fetch_cls_telegraph() and fetch_sina_live() from backend.ingestion.news_ingestion
- for each fetched item, check if a row with same (source, external_id, publish_time)
  already exists in app.news_articles_ts
- print how many items are NEW (not in DB) vs ALREADY in DB for each source
- list a few example NEW items for manual inspection
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

from dotenv import load_dotenv


# Ensure project root (which contains the ``backend`` package) is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(override=True)

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.ingestion.news_ingestion import (  # noqa: E402
    fetch_cls_telegraph,
    fetch_sina_live,
    NewsItem,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Debug news sources vs DB")
    p.add_argument("--limit", type=int, default=50, help="Max items per source to inspect")
    return p.parse_args()


def check_items_in_db(items: Sequence[NewsItem], source_name: str, limit: int) -> None:
    sample = list(items)[:limit]
    if not sample:
        print(f"[{source_name}] fetched 0 items from remote API.")
        return

    print(f"[{source_name}] fetched {len(items)} items from remote API, inspecting {len(sample)}.")

    sql = """
        SELECT 1
          FROM app.news_articles_ts
         WHERE source = %s
           AND external_id = %s
           AND publish_time = %s
         LIMIT 1
    """

    new_count = 0
    existing_count = 0
    examples_new = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            for it in sample:
                eid = it.external_id or ""
                cur.execute(sql, (it.source, eid, it.publish_time))
                if cur.fetchone() is None:
                    new_count += 1
                    if len(examples_new) < 5:
                        examples_new.append(it)
                else:
                    existing_count += 1

    print(
        f"[{source_name}] among inspected items: NEW={new_count}, ALREADY_IN_DB={existing_count}",
        flush=True,
    )

    if examples_new:
        print(f"[{source_name}] example NEW items (not in DB yet):")
        for it in examples_new:
            ts = it.publish_time
            ts_str = ts.astimezone().strftime("%Y-%m-%d %H:%M:%S") if ts.tzinfo else ts.strftime("%Y-%m-%d %H:%M:%S")
            text = (it.title or it.content or "").strip().replace("\n", " ")
            if len(text) > 80:
                text = text[:77] + "..."
            print(f"  - [{ts_str}] external_id={it.external_id} important={it.is_important} {text}")
    else:
        print(f"[{source_name}] no NEW items found in inspected set.")


def main() -> int:
    args = parse_args()
    limit = max(1, args.limit)

    # 1) CLS telegraph
    cls_items = fetch_cls_telegraph(timeout=30)
    check_items_in_db(cls_items, "cls_telegraph", limit)

    # 2) Sina Finance live
    try:
        sina_items = fetch_sina_live(timeout=30)
    except Exception as e:  # pragma: no cover
        print(f"[sina_finance] fetch_sina_live raised error: {e}")
    else:
        check_items_in_db(sina_items, "sina_finance", limit)

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
