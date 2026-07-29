from pathlib import Path

import pytest

from backend.routers.quantevolver_evolution import (
    CustomEvolutionCreateRequest,
    EvolutionTaskCreateRequest,
)
from backend.services.quantevolver.qe_evolution_service import (
    AutoEvolutionScheduler,
    normalize_long_trend_profile_id,
)
from backend.services.quantevolver.long_trend_evaluation_contract import QELongTrendError
from backend.services.quantevolver.payload_summary import compact_task_row


def test_task_create_profile_is_default_off_and_registered_only() -> None:
    base = {
        "task_name": "task",
        "target_desc": "target",
        "base_experiment_id": "exp-1",
    }

    assert EvolutionTaskCreateRequest(**base).long_trend_profile_id is None
    assert EvolutionTaskCreateRequest(
        **base,
        long_trend_profile_id="qe_long_trend_v1",
    ).long_trend_profile_id == "qe_long_trend_v1"
    assert normalize_long_trend_profile_id(None) is None
    assert normalize_long_trend_profile_id("qe_long_trend_v1") == "qe_long_trend_v1"
    with pytest.raises(QELongTrendError, match="unregistered long-trend profile"):
        normalize_long_trend_profile_id("inline_profile_override")


def test_custom_task_create_exposes_profile_without_frozen_parameter_overrides() -> None:
    fields = CustomEvolutionCreateRequest.model_fields

    assert "long_trend_profile_id" in fields
    assert "long_trend_horizons" not in fields
    assert "long_trend_barriers" not in fields
    assert "long_trend_calendar_slices" not in fields


def test_compact_task_payload_restores_persisted_profile() -> None:
    payload = compact_task_row(
        {"task_id": "task-1", "long_trend_profile_id": "qe_long_trend_v1"}
    )

    assert payload["long_trend_profile_id"] == "qe_long_trend_v1"


def test_existing_task_identity_rejects_profile_mutation(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        row = None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, _params):  # type: ignore[no-untyped-def]
            if "FROM qe_experiments" in sql:
                self.row = {
                    "experiment_id": "exp-1",
                    "status": "completed",
                    "parent_experiment_id": None,
                    "is_evolution_loop": False,
                    "loop_index": None,
                }
            elif "FROM qe_evolution_tasks" in sql:
                self.row = {
                    "task_id": "exp-1",
                    "status": "completed",
                    "current_loop": 1,
                    "strategy_params": {"random_seed": 42},
                    "long_trend_profile_id": None,
                }
            else:  # pragma: no cover - mutation must fail before any write.
                raise AssertionError(f"unexpected SQL after immutable-profile check: {sql}")

        def fetchone(self):
            return self.row

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, **_kwargs):
            return _Cursor()

    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    monkeypatch.setattr(
        scheduler,
        "_resolve_new_task_label_horizon",
        lambda *_args, **_kwargs: 1,
    )
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_evolution_service.get_conn",
        lambda: _Connection(),
    )

    with pytest.raises(ValueError, match="immutable"):
        import asyncio

        asyncio.run(
            scheduler.create_task(
                task_name="task",
                target_desc="target",
                max_loops=2,
                base_experiment_id="exp-1",
                random_seed=42,
                long_trend_profile_id="qe_long_trend_v1",
            )
        )


def test_task_profile_migration_has_preflight_rollback_and_init_mirror() -> None:
    root = Path(__file__).resolve().parents[3]
    forward = (root / "backend/migrations/qe_long_trend_task_profile_phase4_20260730.sql").read_text(
        encoding="utf-8"
    )
    preflight = (
        root / "backend/migrations/qe_long_trend_task_profile_phase4_20260730.preflight.sql"
    ).read_text(encoding="utf-8")
    rollback = (
        root / "backend/migrations/qe_long_trend_task_profile_phase4_20260730.rollback.sql"
    ).read_text(encoding="utf-8")
    init_mirror = (root / "backend/init_catalog_db.py").read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS long_trend_profile_id TEXT" in forward
    assert "udt_name <> 'text' OR is_nullable <> 'YES'" in preflight
    assert "guarded rollback refused" in rollback
    assert "LOCK TABLE qe_evolution_tasks IN ACCESS EXCLUSIVE MODE" in rollback
    assert "WHERE long_trend_profile_id IS NOT NULL" in rollback
    assert '("long_trend_profile_id", "TEXT")' in init_mirror
