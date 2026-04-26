"""DB schema preflight for QE label_horizon support."""

from __future__ import annotations

from functools import lru_cache

from ...db.pg_pool import get_conn


CONSTRAINT_NAME = "ck_qe_evolution_tasks_label_horizon"
ALLOWED_LABEL_HORIZONS = (1, 3, 5, 10)


@lru_cache(maxsize=1)
def ensure_qe_label_horizon_schema() -> None:
    """Fail fast when the current DB cannot persist QE task label_horizon."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = 'qe_evolution_tasks'
                  AND column_name = 'label_horizon'
                ORDER BY CASE WHEN table_schema = 'public' THEN 0 ELSE 1 END
                LIMIT 1
                """
            )
            column = cur.fetchone()
            if column is None:
                raise RuntimeError(
                    "DB schema missing qe_evolution_tasks.label_horizon; "
                    "run and verify backend.db.migrations.add_qe_label_horizon before "
                    "submitting label_horizon experiments"
                )

            cur.execute(
                """
                SELECT pg_get_constraintdef(c.oid)
                FROM pg_constraint c
                JOIN pg_class r ON r.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = r.relnamespace
                WHERE r.relname = 'qe_evolution_tasks'
                  AND c.conname = %s
                ORDER BY CASE WHEN n.nspname = 'public' THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (CONSTRAINT_NAME,),
            )
            constraint = cur.fetchone()
            if constraint is None:
                raise RuntimeError(
                    f"DB schema missing {CONSTRAINT_NAME}; run and verify "
                    "backend.db.migrations.add_qe_label_horizon before submitting "
                    "label_horizon experiments"
                )
            definition = str(constraint[0])
            missing_values = [
                str(value)
                for value in ALLOWED_LABEL_HORIZONS
                if str(value) not in definition
            ]
            if "label_horizon" not in definition or missing_values:
                raise RuntimeError(
                    f"DB constraint {CONSTRAINT_NAME} does not validate "
                    f"label_horizon in {ALLOWED_LABEL_HORIZONS}: {definition}"
                )
