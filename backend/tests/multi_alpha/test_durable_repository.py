from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pytest

from backend.services.multi_alpha.durable_models import (
    DurableAttemptSpec,
    DurableChildSpec,
    DurableContractError,
    DurableRunSpec,
    OwnershipToken,
    artifact_manifest_hash_for,
    durable_run_request_payload,
    make_attempt_id,
    make_child_id,
    make_remote_task_id,
    request_hash_for,
    submission_intent_hash_for,
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
        "lease_valid": True,
    }


def _run_request() -> dict[str, Any]:
    return durable_run_request_payload(
        roster_hash="roster",
        roster=[{"leg_id": "L1"}],
        oos_start="2026-01-01",
        oos_end="2026-06-29",
        normalize_method="rank",
        walk_forward={"enabled": True},
        backtest_config={"topk": 25},
    )


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
    claim_sql = provider.cursor.executions[0][0]
    assert "task_id IS NOT NULL" in claim_sql
    assert "request_hash IS NOT NULL" in claim_sql
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


def test_expired_lease_cannot_heartbeat_or_resurrect_ownership() -> None:
    provider = ScriptedProvider(
        [
            Step(contains="UPDATE strategy_pkg.multi_alpha_combine_backtest_run", one=None),
            Step(
                contains="lease_valid",
                one={
                    **_claimed_run(),
                    "lease_valid": False,
                    "lease_expires_at": "2026-07-18T00:00:00+00:00",
                },
            ),
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)

    with pytest.raises(MultiAlphaDurableRepositoryError) as caught:
        repository.heartbeat_run(
            "macb_test",
            token=OwnershipToken(owner_id="worker_1", fencing_token=1, row_version=2),
            lease_seconds=30,
        )

    assert caught.value.reason_code == "multi_alpha_lease_expired"
    assert provider.commits == 0
    assert provider.rollbacks == 1
    assert "lease_expires_at > clock_timestamp()" in provider.cursor.executions[0][0]


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
        request_hash=request_hash_for(_run_request()),
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


def test_same_request_hash_with_mutated_persisted_payload_fails_loudly() -> None:
    request_hash = request_hash_for(_run_request())
    provider = ScriptedProvider(
        [
            Step(contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_run", one=None),
            Step(
                contains="SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_run",
                one={
                    "id": "macb_test",
                    "task_id": "mact_test",
                    "request_hash": request_hash,
                    "roster_hash": "roster",
                    "roster_json": [{"leg_id": "L2"}],
                    "oos_start": "2026-01-01",
                    "oos_end": "2026-06-29",
                    "normalize_method": "rank",
                    "walk_forward_json": {"enabled": True},
                    "backtest_config_json": {"topk": 25},
                    "baseline_leg_id": None,
                    "retry_of_run_id": None,
                    "node_parallelism_json": {},
                },
            ),
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)
    spec = DurableRunSpec(
        run_id="macb_test",
        task_id="mact_test",
        request_hash=request_hash,
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


def test_identity_hashes_must_match_their_canonical_payloads() -> None:
    with pytest.raises(DurableContractError) as run_error:
        DurableRunSpec(
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
    assert run_error.value.reason_code == "multi_alpha_identity_hash_mismatch"

    child_id = make_child_id("macb_test", "scheme:equal")
    with pytest.raises(DurableContractError) as child_error:
        DurableChildSpec(
            child_id=child_id,
            run_id="macb_test",
            child_key="scheme:equal",
            child_kind="scheme",
            weighting_scheme="equal",
            ordinal=0,
            input_manifest={"topk": 25},
            input_manifest_hash="b" * 64,
        )
    assert child_error.value.reason_code == "multi_alpha_identity_hash_mismatch"

    attempt_id = make_attempt_id(child_id, 1)
    with pytest.raises(DurableContractError) as attempt_error:
        DurableAttemptSpec(
            attempt_id=attempt_id,
            child_id=child_id,
            attempt_no=1,
            retry_mode="initial",
            node_id="wsl2-5080",
            qe_task_id="qe_task",
            qe_loop_id="Loop1",
            submission_intent_hash="c" * 64,
        )
    assert attempt_error.value.reason_code == "multi_alpha_identity_hash_mismatch"


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
        "submission_intent_hash": submission_intent_hash_for(
            child_id=child_id,
            attempt_no=2,
            retry_mode="backtest_only",
            retry_of_attempt_id=previous_id,
            node_id=None,
            qe_task_id="qe_task",
            qe_loop_id="Loop1",
        ),
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
        submission_intent_hash=existing["submission_intent_hash"],
    )

    assert repository.create_attempt(spec) == existing
    assert provider.commits == 1
    assert provider.rollbacks == 0


def test_attempt_claim_policy_never_dispatches_queued_work_for_cancelling_parent() -> None:
    provider = ScriptedProvider([Step(contains="FOR UPDATE SKIP LOCKED", one=None)])
    repository = MultiAlphaDurableRepository(connection_provider=provider)

    assert repository.claim_next_attempt(owner_id="worker_1", lease_seconds=30, claim_kind="cancel") is None
    params = provider.cursor.executions[0][1]
    assert "queued" not in params[0]
    assert set(params[1]) == {"cancel_requested", "cancelling"}


def test_control_reason_does_not_pollute_error_columns() -> None:
    current = {**_claimed_run(), "status": "running"}
    updated = {**current, "status": "pause_requested", "phase": "pause", "row_version": 3, "error_code": None}
    provider = ScriptedProvider(
        [
            Step(contains="FOR UPDATE", one=current),
            Step(contains="UPDATE strategy_pkg.multi_alpha_combine_backtest_run", one=updated),
            Step(
                contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_event",
                one={"event_id": 2, "run_id": "macb_test", "event_type": "status"},
            ),
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)

    result = repository.transition_run_with_event(
        "macb_test",
        token=OwnershipToken(owner_id="worker_1", fencing_token=1, row_version=2),
        expected_statuses=("running",),
        next_status="pause_requested",
        phase="pause",
        reason_code="user_pause_requested",
    )

    assert result["error_code"] is None
    update_params = provider.cursor.executions[1][1]
    assert update_params[4] is None
    assert update_params[5] is None


def test_cancel_unknown_cannot_be_collapsed_to_failed() -> None:
    provider = ScriptedProvider([])
    repository = MultiAlphaDurableRepository(connection_provider=provider)

    with pytest.raises(MultiAlphaDurableRepositoryError) as caught:
        repository.transition_run_with_event(
            "macb_test",
            token=OwnershipToken(owner_id="worker_1", fencing_token=1, row_version=2),
            expected_statuses=("cancelling",),
            next_status="failed",
            phase="cancel",
            error={"reason_code": "remote_state_unknown"},
        )

    assert caught.value.reason_code == "multi_alpha_invalid_state_transition"
    assert provider.commits == 0


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
