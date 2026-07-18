from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import pytest

from backend.services.multi_alpha.durable_backfill import MultiAlphaLegacyBackfill
from backend.services.multi_alpha.durable_repository import MultiAlphaDurableRepositoryError


def _run() -> dict[str, Any]:
    return {
        "id": "macb_roster_20260101_20260629_20260718T000000Z",
        "task_id": None,
        "roster_hash": "roster_hash",
        "roster_json": [{"leg_id": "L1"}, {"leg_id": "L2"}],
        "oos_start": "2026-01-01",
        "oos_end": "2026-06-29",
        "normalize_method": "rank",
        "walk_forward_json": {"enabled": True, "window": 60, "min_periods": 20, "expanding": False},
        "backtest_config_json": {"topk": 25, "initial_cash": 10_000_000},
        "baseline_leg_id": "L1",
        "status": "succeeded",
        "reason": {"logical_status": "succeeded"},
        "created_at": "2026-07-18T00:00:00+00:00",
    }


def test_compile_plan_maps_legacy_tasks_and_result_children_without_attempts() -> None:
    run = _run()
    schemes = [
        {"id": 10, "run_id": run["id"], "weighting_scheme": "equal", "skipped": False},
        {"id": 11, "run_id": run["id"], "weighting_scheme": "risk_parity", "skipped": True},
    ]
    loo = [
        {
            "id": 12,
            "run_id": run["id"],
            "weighting_scheme": "equal",
            "dropped_leg_id": "L2",
            "marginal_sharpe": 0.1,
        }
    ]

    plan = MultiAlphaLegacyBackfill.compile_plan(
        runs=[run], schemes=schemes, loo_rows=loo, protected_digest="protected"
    )

    assert len(plan.tasks) == 1
    assert plan.tasks[0].task_id.startswith("mact_legacy_")
    assert len(plan.assignments) == 1
    assert plan.assignments[0].task_id == plan.tasks[0].task_id
    assert [child.child_key for child in plan.children] == [
        "scheme:equal",
        "scheme:risk_parity",
        "loo:equal:drop:L2",
    ]
    assert [child.status for child in plan.children] == ["succeeded", "not_computable", "succeeded"]
    assert all(child.source_kind == "legacy_result_backfill" for child in plan.children)
    assert "attempt" not in plan.summary()


class NullCursor:
    def __enter__(self) -> "NullCursor":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class NullConnection:
    def cursor(self, **_: Any) -> NullCursor:
        return NullCursor()


class TrackingProvider:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    @contextmanager
    def __call__(self) -> Iterator[NullConnection]:
        try:
            yield NullConnection()
        except Exception:
            self.rollbacks += 1
            raise
        else:
            self.commits += 1


class BackfillHarness(MultiAlphaLegacyBackfill):
    def __init__(self, provider: TrackingProvider, *, digest_after: str = "protected") -> None:
        super().__init__(connection_provider=provider)
        self.plan = MultiAlphaLegacyBackfill.compile_plan(
            runs=[_run()], schemes=[], loo_rows=[], protected_digest="protected"
        )
        self.digest_after = digest_after
        self.applied: list[str] = []

    def _build_plan(self, cur: Any):  # type: ignore[no-untyped-def]
        return self.plan

    def _upsert_task(self, cur: Any, task: Any) -> None:
        self.applied.append(f"task:{task.task_id}")

    def _assign_run(self, cur: Any, assignment: Any) -> None:
        self.applied.append(f"run:{assignment.run_id}")

    def _upsert_child(self, cur: Any, child: Any) -> None:
        self.applied.append(f"child:{child.child_id}")

    def _protected_digest(self, cur: Any) -> str:
        return self.digest_after

    def _readback(self, cur: Any, plan: Any) -> dict[str, Any]:
        return {"ready": True, "protected_unchanged": True}


def test_execute_uses_one_transaction_and_returns_readback() -> None:
    provider = TrackingProvider()
    backfill = BackfillHarness(provider)

    receipt = backfill.execute()

    assert receipt["mode"] == "execute"
    assert receipt["readback"]["ready"] is True
    assert provider.commits == 1
    assert provider.rollbacks == 0
    assert backfill.applied == [f"task:{backfill.plan.tasks[0].task_id}", f"run:{_run()['id']}"]


def test_execute_rolls_back_if_protected_history_changes() -> None:
    provider = TrackingProvider()
    backfill = BackfillHarness(provider, digest_after="changed")

    with pytest.raises(MultiAlphaDurableRepositoryError) as caught:
        backfill.execute()

    assert caught.value.reason_code == "multi_alpha_backfill_protected_data_changed"
    assert provider.commits == 0
    assert provider.rollbacks == 1


def test_backfill_cli_has_explicit_modes_and_no_backup_side_effect() -> None:
    from pathlib import Path

    source = (Path(__file__).resolve().parents[3] / "scripts/backfill_multi_alpha_durable_tasks.py").read_text(
        encoding="utf-8"
    )

    assert 'choices=("dry-run", "execute", "readback")' in source
    assert "pg_dump" not in source.lower()
    assert "subprocess" not in source
