"""Idempotent initializer for announcement event classification schema."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

from .pg_pool import get_conn


ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATHS = [
    ROOT / "backend" / "migrations" / "announcement_event_signal_schema_20260505.sql",
    ROOT / "backend" / "migrations" / "announcement_observation_time_fields_20260505.sql",
]


def init_announcement_event_schema() -> None:
    """Create announcement taxonomy, rule-set, classification, and signal tables."""

    load_dotenv(override=True)
    with get_conn() as conn:
        with conn.cursor() as cur:
            for path in MIGRATION_PATHS:
                cur.execute(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    init_announcement_event_schema()
    print("[DONE] announcement event classification schema ensured")
