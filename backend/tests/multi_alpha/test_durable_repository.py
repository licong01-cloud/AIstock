from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pytest

from backend.services.multi_alpha.durable_models import (
    DurableAttemptSpec,
    DurableRunSpec,
    OwnershipToken,
    artifact_manifest_hash_for,
    make_attempt_id,
    make_child_id,
    make_remote_task_id,
    request_hash_for,
)
from backend.services.multi_alpha.durable_repository import (
    MultiAlphaDurableRepository,
    MultiAlphaDurableRepositoryError,
)


REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass
class Step:
    contains: str
    one: Any = None
    all_rows: list[Any] | None = None
    error: Exception | None = None


class ScriptedCursor:
    def __init__(self, steps: list[Step]) -> None:
        self.steps = list(steps)
        self.executions: list[tuple[str, Any]] = []
        self.current: Step | None = None

    def __enter__(self) -> "ScriptedCursor":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def execute(self, query: str, params: Any = None) -> None:
        if not self.steps:
            raise AssertionError(f"unexpected SQL: {query}")
        step = self.steps.pop(0)
        normalized = " ".join(query.split())
        assert step.contains in normalized
        self.executions.append((normalized, params))
        self.current = step
        if step.error is not None:
            raise step.error

    def fetchone(self) -> Any:
        assert self.current is not None
        return self.current.one

    def fetchall(self) -> list[Any]:
        assert self.current is not None
        return list(self.current.all_rows or [])


class ScriptedConnection:
    def __init__(self, cursor: ScriptedCursor) -> None:
        self.scripted_cursor = cursor

    def cursor(self, **_: Any) -> ScriptedCursor:
        return self.scripted_cursor


class ScriptedProvider:
    def __init__(self, steps: list[Step]) -> None:
        self.cursor = ScriptedCursor(steps)
        self.connection = ScriptedConnection(self.cursor)
        self.commits = 0
        self.rollbacks = 0

    @contextmanager
    def __call__(self) -> Iterator[ScriptedConnection]:
        try:
            yield self.connection
        except Exception:
            self.rollbacks += 1
            raise
        else:
            self.commits += 1


def _claimed_run() -> dict[str, Any]:
    return {
        "id": "macb_test",
        "status": "queued",
        "phase": "queued",
        "owner_id": "worker_1",
        "fencing_token": 1,
        "row_version": 2,
    }


def test_identity_helpers_are_stable_and_explicit() -> None:
    payload = {"b": [2, 1], "a": {"x": True}}
    assert request_hash_for(payload) == request_hash_for({"a": {"x": True}, "b": [2, 1]})
    assert artifact_manifest_hash_for(payload) == request_hash_for(payload)

    child_id = make_child_id("macb_test", "scheme:equal")
    assert child_id == make_child_id("macb_test", "scheme:equal")
    assert child_id.startswith("macbc_")
    assert make_attempt_id(child_id, 1).startswith("macba_")
    assert make_remote_task_id("macb_test", child_id, 1).endswith("_a1")


def test_claim_uses_skip_locked_and_writes_event_in_same_transaction() -> None:
    provider = ScriptedProvider(
        [
            Step(contains="FOR UPDATE SKIP LOCKED", one=_claimed_run()),
            Step(
                contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_event",
                one={"event_id": 1, "run_id": "macb_test", "event_type": "claimed"},
            ),
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)

    row = repository.claim_next_run(owner_id="worker_1", lease_seconds=30)

    assert row == _claimed_run()
    assert provider.commits == 1
    assert provider.rollbacks == 0
    assert not provider.cursor.steps


def test_event_failure_rolls_back_the_state_transition() -> None:
    current = _claimed_run()
    updated = {**current, "status": "preparing", "phase": "prepare", "row_version": 3}
    provider = ScriptedProvider(
        [
            Step(contains="FOR UPDATE", one=current),
            Step(contains="UPDATE strategy_pkg.multi_alpha_combine_backtest_run", one=updated),
            Step(
                contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_event",
                error=RuntimeError("event write failed"),
            ),
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)

    with pytest.raises(RuntimeError, match="event write failed"):
        repository.transition_run_with_event(
            "macb_test",
            token=OwnershipToken(owner_id="worker_1", fencing_token=1, row_version=2),
            expected_statuses=("queued",),
            next_status="preparing",
            phase="prepare",
        )

    assert provider.commits == 0
    assert provider.rollbacks == 1


def test_stale_fencing_token_is_rejected_before_transition_write() -> None:
    provider = ScriptedProvider(
        [
            Step(
                contains="FOR UPDATE",
                one={**_claimed_run(), "owner_id": "worker_new", "fencing_token": 2, "row_version": 5},
            )
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)

    with pytest.raises(MultiAlphaDurableRepositoryError) as caught:
        repository.transition_run_with_event(
            "macb_test",
            token=OwnershipToken(owner_id="worker_1", fencing_token=1, row_version=2),
            expected_statuses=("queued",),
            next_status="preparing",
            phase="prepare",
        )

    assert caught.value.reason_code == "multi_alpha_stale_fencing_token"
    assert provider.rollbacks == 1
    assert len(provider.cursor.executions) == 1


def test_same_run_identity_with_different_request_hash_fails_loudly() -> None:
    provider = ScriptedProvider(
        [
            Step(contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_run", one=None),
            Step(
                contains="SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_run",
                one={"id": "macb_test", "task_id": "mact_test", "request_hash": "b" * 64},
            ),
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)
    spec = DurableRunSpec(
        run_id="macb_test",
        task_id="mact_test",
        request_hash="a" * 64,
        roster_hash="roster",
        roster=[{"leg_id": "L1"}],
        oos_start="2026-01-01",
        oos_end="2026-06-29",
        normalize_method="rank",
        walk_forward={"enabled": True},
        backtest_config={"topk": 25},
    )

    with pytest.raises(MultiAlphaDurableRepositoryError) as caught:
        repository.create_run(spec)

    assert caught.value.reason_code == "multi_alpha_identity_payload_conflict"
    assert provider.rollbacks == 1


def test_retry_attempt_creation_is_idempotent_for_the_same_append_only_identity() -> None:
    child_id = make_child_id("macb_test", "scheme:equal")
    attempt_id = make_attempt_id(child_id, 2)
    previous_id = make_attempt_id(child_id, 1)
    existing = {
        "attempt_id": attempt_id,
        "child_id": child_id,
        "attempt_no": 2,
        "retry_mode": "backtest_only",
        "retry_of_attempt_id": previous_id,
        "submission_intent_hash": "c" * 64,
        "qe_task_id": "qe_task",
        "qe_loop_id": "Loop1",
    }
    provider = ScriptedProvider(
        [
            Step(contains="SELECT child_id, run_id", one={"child_id": child_id, "run_id": "macb_test"}),
            Step(contains="ORDER BY attempt_no DESC", one={"attempt_id": attempt_id, "child_id": child_id, "attempt_no": 2}),
            Step(contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_child_attempt", one=None),
            Step(contains="SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt", one=existing),
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)
    spec = DurableAttemptSpec(
        attempt_id=attempt_id,
        child_id=child_id,
        attempt_no=2,
        retry_mode="backtest_only",
        retry_of_attempt_id=previous_id,
        qe_task_id="qe_task",
        qe_loop_id="Loop1",
        submission_intent_hash="c" * 64,
    )

    assert repository.create_attempt(spec) == existing
    assert provider.commits == 1
    assert provider.rollbacks == 0


def test_transition_outside_the_declared_state_machine_is_rejected_without_sql() -> None:
    provider = ScriptedProvider([])
    repository = MultiAlphaDurableRepository(connection_provider=provider)

    with pytest.raises(MultiAlphaDurableRepositoryError) as caught:
        repository.transition_run_with_event(
            "macb_test",
            token=OwnershipToken(owner_id="worker_1", fencing_token=1, row_version=2),
            expected_statuses=("queued",),
            next_status="succeeded",
            phase="terminal",
        )

    assert caught.value.reason_code == "multi_alpha_invalid_state_transition"
    assert provider.commits == 0
    assert provider.rollbacks == 0


def test_repository_source_contains_no_silent_fallback() -> None:
    source = (REPO_ROOT / "backend/services/multi_alpha/durable_repository.py").read_text(encoding="utf-8")

    assert "except Exception: pass" not in source
    assert "multi_alpha_identity_payload_conflict" in source
    assert "multi_alpha_stale_fencing_token" in source
    assert "multi_alpha_event_persistence_failed" in source
