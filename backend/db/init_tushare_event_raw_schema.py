"""Idempotent initializer for Tushare event-related source-only raw tables."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

from .pg_pool import get_conn


ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = ROOT / "backend" / "migrations" / "tushare_event_raw_schema_20260506.sql"


def init_tushare_event_raw_schema() -> None:
    """Create raw Tushare forecast, express, and fina_indicator source tables."""

    load_dotenv(override=True)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(MIGRATION_PATH.read_text(encoding="utf-8"))


if __name__ == "__main__":
    init_tushare_event_raw_schema()
    print("[DONE] Tushare event raw schema ensured")
