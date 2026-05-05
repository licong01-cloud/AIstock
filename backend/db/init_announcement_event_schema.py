"""Idempotent initializer for announcement event classification schema."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

from .pg_pool import get_conn


ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = ROOT / "backend" / "migrations" / "announcement_event_signal_schema_20260505.sql"


def init_announcement_event_schema() -> None:
    """Create announcement taxonomy, rule-set, classification, and signal tables."""

    load_dotenv(override=True)
    sql = MIGRATION_PATH.read_text(encoding="utf-8")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


if __name__ == "__main__":
    init_announcement_event_schema()
    print("[DONE] announcement event classification schema ensured")
