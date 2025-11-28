from __future__ import annotations

"""One-off helper script to create trend analysis tables.

Uses the existing pg_pool.get_conn() so it respects TDX_DB_* env vars.
You can delete this file after running once if you like.
"""

import os
from pathlib import Path

from .pg_pool import get_conn


SQL = """
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.trend_analysis_records (
    id              SERIAL PRIMARY KEY,
    ts_code         TEXT NOT NULL,
    analysis_date   TIMESTAMPTZ NOT NULL,
    mode            TEXT NOT NULL,
    stock_info      JSONB NOT NULL,
    final_predictions JSONB NOT NULL,
    prediction_evolution JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trend_records_ts_code
    ON app.trend_analysis_records (ts_code);

CREATE INDEX IF NOT EXISTS idx_trend_records_analysis_date
    ON app.trend_analysis_records (analysis_date);

CREATE TABLE IF NOT EXISTS app.trend_analyst_results (
    id              SERIAL PRIMARY KEY,
    record_id       INTEGER NOT NULL REFERENCES app.trend_analysis_records(id) ON DELETE CASCADE,
    analyst_key     TEXT,
    analyst_name    TEXT NOT NULL,
    role            TEXT,
    raw_text        TEXT,
    conclusion_json JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trend_analyst_record_id
    ON app.trend_analyst_results (record_id);
"""


def _load_dotenv_from_project_root() -> None:
    """Best-effort load of .env from project root into os.environ.

    This avoids the need for external dotenv dependencies and is enough
    for TDX_DB_* style variables used by pg_pool._db_cfg().
    """

    try:
        root = Path(__file__).resolve().parents[3]
    except Exception:
        return

    env_path = root / ".env"
    if not env_path.exists():
        return

    try:
        text = env_path.read_text(encoding="utf-8")
    except Exception:
        return

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def main() -> None:
    _load_dotenv_from_project_root()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
    print("✅ trend_analysis_records / trend_analyst_results created (or already exist)")


if __name__ == "__main__":
    main()
