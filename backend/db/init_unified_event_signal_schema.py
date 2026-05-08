"""Idempotent initializer for the unified event signal schema."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

from .pg_pool import get_conn


ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATHS = [
    ROOT / "backend" / "migrations" / "unified_event_signal_schema_20260506.sql",
    ROOT / "backend" / "migrations" / "event_signal_policy_lifecycle_schema_20260507.sql",
]


def init_unified_event_signal_schema() -> None:
    """Create unified event fact, relation, signal, rule-set, and run tables."""

    load_dotenv(override=True)
    with get_conn() as conn:
        with conn.cursor() as cur:
            for migration_path in MIGRATION_PATHS:
                cur.execute(migration_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    init_unified_event_signal_schema()
    print("[DONE] unified event signal schema ensured")
