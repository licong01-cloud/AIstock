from __future__ import annotations

"""Initialize Timescale hypertable for news data and migrate existing rows.

Usage (from project root, with AIstock env activated):

    python scripts/init_news_hypertable.py

This script will:
1. Create app.news_articles_ts if not exists.
2. Turn it into a Timescale hypertable on publish_time.
3. Create recommended indexes and unique constraint.
4. Migrate data from app.news_articles into the new table (deduplicated).
"""

import sys
from pathlib import Path

from dotenv import load_dotenv


# Ensure project root (which contains the ``backend`` package) is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(override=True)

from backend.db.pg_pool import get_conn  # noqa: E402


SQL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS app.news_articles_ts (
    id           BIGSERIAL,
    source       VARCHAR(64) NOT NULL,
    external_id  VARCHAR(128),
    title        TEXT,
    content      TEXT,
    url          TEXT,
    ts_codes     TEXT[],
    publish_time TIMESTAMPTZ NOT NULL,
    ingest_time  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_important BOOLEAN NOT NULL DEFAULT FALSE,
    raw_source   JSONB
);
"""

SQL_CREATE_HYPER = """
SELECT create_hypertable(
    'app.news_articles_ts',
    'publish_time',
    chunk_time_interval => interval '1 day',
    if_not_exists       => true
);
"""

SQL_INDEXES = [
    """
    CREATE INDEX IF NOT EXISTS idx_news_articles_ts_publish_time_desc
        ON app.news_articles_ts (publish_time DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_news_articles_ts_source_publish_time_desc
        ON app.news_articles_ts (source, publish_time DESC);
    """,
]

SQL_UNIQUE = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_news_articles_ts_source_external_time
    ON app.news_articles_ts (source, external_id, publish_time);
"""

SQL_MIGRATE = """
INSERT INTO app.news_articles_ts (
    source,
    external_id,
    title,
    content,
    url,
    ts_codes,
    publish_time,
    ingest_time,
    is_important,
    raw_source
)
SELECT
    source,
    external_id,
    title,
    content,
    url,
    ts_codes,
    publish_time,
    ingest_time,
    is_important,
    raw_source
FROM app.news_articles
ON CONFLICT (source, external_id, publish_time) DO NOTHING;
"""


def main() -> int:
    print("[init_news_hypertable] Creating app.news_articles_ts table...", flush=True)
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1) Create table
            cur.execute(SQL_CREATE_TABLE)
            print("  - table ensured.", flush=True)

            # 2) Drop incompatible primary key if it exists (from earlier runs)
            # Timescale requires that any PRIMARY KEY / UNIQUE index used for
            # partitioning includes the time column; we keep id as a plain
            # column and let Timescale manage internal indexes.
            for constraint_name in (
                "news_articles_ts_pkey",  # default name without schema prefix
                "app_news_articles_ts_pkey",  # possible schema-prefixed variant
            ):
                cur.execute(
                    f"ALTER TABLE app.news_articles_ts DROP CONSTRAINT IF EXISTS {constraint_name};"
                )

            # 3) Create hypertable
            cur.execute(SQL_CREATE_HYPER)
            print("  - hypertable ensured.", flush=True)

            # 4) Create indexes
            for sql in SQL_INDEXES:
                cur.execute(sql)
            print("  - indexes ensured.", flush=True)

            # 5) Unique constraint (ignore if already exists)
            try:
                cur.execute(SQL_UNIQUE)
                print("  - unique constraint ensured.", flush=True)
            except Exception as e:  # pragma: no cover
                # If constraint already exists under a different name, just log.
                print(f"  - unique constraint step raised: {e}", flush=True)

            # 6) Migrate data
            print("[init_news_hypertable] Migrating data from app.news_articles...", flush=True)
            cur.execute(SQL_MIGRATE)
            migrated = cur.rowcount or 0
            print(f"  - migrated rows (inserted new): {migrated}", flush=True)

    print("[init_news_hypertable] Done.", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
