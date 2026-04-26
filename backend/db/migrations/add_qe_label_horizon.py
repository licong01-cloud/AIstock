"""Add QE task-level label_horizon support.

Run:
    python -m backend.db.migrations.add_qe_label_horizon
"""

from __future__ import annotations

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.label_horizon_schema import (
    CONSTRAINT_NAME,
    ensure_qe_label_horizon_schema,
)


ALLOWED = (1, 3, 5, 10)


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
                SELECT 1
                FROM pg_constraint
                WHERE conname = %s
                  AND conrelid = 'qe_evolution_tasks'::regclass
                """,
                (CONSTRAINT_NAME,),
            )
            if cur.fetchone() is None:
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
