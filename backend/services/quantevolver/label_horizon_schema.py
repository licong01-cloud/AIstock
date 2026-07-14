"""DB schema preflight for QE label_horizon support."""

from __future__ import annotations

from functools import lru_cache
import re

from ...db.pg_pool import get_conn
from .experiment_config import ALLOWED_LABEL_HORIZONS


CONSTRAINT_NAME = "ck_qe_evolution_tasks_label_horizon"


def parse_label_horizon_constraint_values(definition: str | None) -> frozenset[int]:
    """Extract the integer allow-list from a PostgreSQL CHECK definition."""
    if not definition or "label_horizon" not in definition:
        return frozenset()
    return frozenset(int(value) for value in re.findall(r"(?<![\w.])-?\d+(?![\w.])", definition))


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
            constraint_values = parse_label_horizon_constraint_values(definition)
            if constraint_values != frozenset(ALLOWED_LABEL_HORIZONS):
                raise RuntimeError(
                    f"DB constraint {CONSTRAINT_NAME} does not validate "
                    f"label_horizon in {ALLOWED_LABEL_HORIZONS}: {definition}"
                )
