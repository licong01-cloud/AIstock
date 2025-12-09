from __future__ import annotations

"""One-off script to de-duplicate news articles in app.news_articles.

This script is intended to be run manually from the project root:

    (AIstock) python scripts/dedup_news_articles.py

It removes duplicate rows based on (source, external_id, publish_time),
keeping the lowest id in each group. This is safe for the current
news ingestion design where external_id comes from the upstream source
(财联社 / 新浪财经 / TradingView 等) and uniquely identifies a news item
within a source.
"""

import sys
from pathlib import Path
from typing import NoReturn

from dotenv import load_dotenv


# Ensure project root (which contains the ``backend`` package) is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables (DB config, etc.)
load_dotenv(override=True)

from backend.db.pg_pool import get_conn


SQL_DEDUP = """
DELETE FROM app.news_articles a
USING app.news_articles b
WHERE a.id > b.id
  AND a.source = b.source
  AND a.publish_time = b.publish_time
  AND COALESCE(a.external_id, '') = COALESCE(b.external_id, '');
"""


def main() -> int:
    print("[dedup_news_articles] starting de-duplication...", flush=True)
    deleted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL_DEDUP)
            # rowcount is number of deleted rows for this statement
            deleted = cur.rowcount or 0
    print(f"[dedup_news_articles] deleted {deleted} duplicate rows.", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
