"""Add QE task-level label_horizon support.

Run:
    python -m backend.db.migrations.add_qe_label_horizon
"""

from __future__ import annotations

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.label_horizon_schema import (
    ALLOWED_LABEL_HORIZONS,
    CONSTRAINT_NAME,
    ensure_qe_label_horizon_schema,
    parse_label_horizon_constraint_values,
)


ALLOWED = ALLOWED_LABEL_HORIZONS


def _constraint_missing_values(definition: str | None) -> list[int]:
    existing = parse_label_horizon_constraint_values(definition)
    return [value for value in ALLOWED if value not in existing]


def _constraint_requires_replacement(definition: str | None) -> bool:
    return parse_label_horizon_constraint_values(definition) != frozenset(ALLOWED)


def run_migration() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE qe_evolution_tasks
                ADD COLUMN IF NOT EXISTS label_horizon INTEGER NOT NULL DEFAULT 1
                """
            )
            cur.execute(
                """
                UPDATE qe_evolution_tasks
                SET label_horizon = 1
                WHERE label_horizon IS NULL
                """
            )
            cur.execute(
                """
                SELECT task_id, label_horizon
                FROM qe_evolution_tasks
                WHERE label_horizon <> ALL(%s)
                LIMIT 20
                """,
                (list(ALLOWED),),
            )
            invalid = cur.fetchall()
            if invalid:
                raise RuntimeError(
                    "qe_evolution_tasks.label_horizon has invalid existing values: "
                    f"{invalid}"
                )
            cur.execute(
                """
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conname = %s
                  AND conrelid = 'qe_evolution_tasks'::regclass
                """,
                (CONSTRAINT_NAME,),
            )
            constraint = cur.fetchone()
            definition = str(constraint[0]) if constraint else None
            if constraint and _constraint_requires_replacement(definition):
                cur.execute(
                    f"""
                    ALTER TABLE qe_evolution_tasks
                    DROP CONSTRAINT {CONSTRAINT_NAME}
                    """
                )
                definition = None

            if _constraint_requires_replacement(definition):
                cur.execute(
                    f"""
                    ALTER TABLE qe_evolution_tasks
                    ADD CONSTRAINT {CONSTRAINT_NAME}
                    CHECK (label_horizon IN {ALLOWED})
                    """
                )
        conn.commit()


def preflight() -> None:
    ensure_qe_label_horizon_schema.cache_clear()
    ensure_qe_label_horizon_schema()


if __name__ == "__main__":
    run_migration()
    preflight()
    print("OK: qe_evolution_tasks.label_horizon migration and preflight passed")
