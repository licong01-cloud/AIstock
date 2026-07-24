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
    DurableTaskSpec,
    OwnershipToken,
    artifact_manifest_hash_for,
    durable_run_request_payload,
    implicit_task_group_key,
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


def test_list_attempts_for_run_uses_one_run_scoped_join_query() -> None:
    provider = ScriptedProvider([
        Step(
            contains="JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child",
            all_rows=[{"attempt_id": "attempt_1", "child_id": "child_1"}],
        ),
    ])
    repository = MultiAlphaDurableRepository(connection_provider=provider)

    rows = repository.list_attempts_for_run("macb_run")

    assert rows == [{"attempt_id": "attempt_1", "child_id": "child_1"}]
    assert provider.cursor.executions[0][1] == ("macb_run",)


def test_task_group_collision_reuses_existing_task_and_ignores_scenario_defaults() -> None:
    group_key = implicit_task_group_key(
        roster_hash="roster",
        normalize_method="rank",
        walk_forward={"enabled": True, "window": 60, "min_periods": 20},
    )
    existing = {
        "task_id": "mact_legacy_existing",
        "roster_hash": "roster",
        "roster_json": [{"leg_id": "L1"}],
        "default_request_json": {
            "normalize_method": "rank",
            "walk_forward": {"enabled": True, "window": 60, "min_periods": 20},
            "topk": 25,
            "backtest_config": {"initial_cash": 10_000_000},
        },
        "legacy_group_key": group_key,
    }
    provider = ScriptedProvider(
        [
            Step(contains="INSERT INTO strategy_pkg.multi_alpha_combine_task", one=None),
            Step(contains="FROM strategy_pkg.multi_alpha_combine_task", one=existing),
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)
    spec = DurableTaskSpec(
        task_id="mact_auto_candidate",
        task_name="Implicit candidate",
        roster_hash="roster",
        roster=[{"leg_id": "L1"}],
        default_request={
            "normalize_method": "rank",
            "walk_forward": {"enabled": True, "window": 60, "min_periods": 20},
            "topk": 50,
            "backtest_config": {"initial_cash": 100_000_000},
        },
        source_kind="api",
        legacy_group_key=group_key,
    )

    assert repository.create_task(spec) == existing
    assert provider.commits == 1
    assert provider.rollbacks == 0


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


def test_source_delete_is_blocked_only_during_published_recovery_copy_window() -> None:
    class ReadyP0_2Health:
        ready = True

    provider = ScriptedProvider(
        [
            Step(
                contains="staging_manifest_json IS NOT NULL",
                one={
                    "command_id": "macmd_copy",
                    "status": "reconciling",
                    "staging_manifest_hash": "a" * 64,
                },
            )
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)
    repository.preflight_p0_2_schema = lambda **_kwargs: ReadyP0_2Health()  # type: ignore[method-assign]

    with pytest.raises(MultiAlphaDurableRepositoryError) as caught:
        repository.assert_recovery_source_delete_allowed("macb_test")

    assert caught.value.reason_code == "recovery_source_copy_in_progress"
    assert caught.value.context["command_id"] == "macmd_copy"


def test_source_delete_remains_available_when_p0_2_schema_is_not_deployed() -> None:
    class MissingP0_2Health:
        ready = False

    repository = MultiAlphaDurableRepository(connection_provider=lambda: (_ for _ in ()).throw(AssertionError("no DB query")))
    repository.preflight_p0_2_schema = lambda **_kwargs: MissingP0_2Health()  # type: ignore[method-assign]

    result = repository.assert_recovery_source_delete_allowed("macb_test")

    assert result == {
        "allowed": True,
        "p0_2_schema_ready": False,
        "reason_code": "multi_alpha_p0_2_schema_unavailable",
    }


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


def test_deadline_evidence_and_event_are_one_idempotent_attempt_transaction() -> None:
    evidence = {
        "scheme": {
            "timeout_seconds": 60,
            "started_at": "2026-01-01T00:00:00Z",
            "deadline_at": "2026-01-01T00:01:00Z",
            "effective_observed_at": "2026-01-01T00:02:00Z",
            "elapsed_seconds": 120.0,
            "timestamp_source": "submission_receipt.finished_at",
            "remote_status": "completed",
        }
    }
    current = {
        "attempt_id": "macba_test",
        "child_id": "macbc_test",
        "status": "running",
        "owner_id": "worker_1",
        "fencing_token": 1,
        "row_version": 2,
        "lease_valid": True,
        "result_manifest_json": {},
    }
    updated = {
        **current,
        "row_version": 3,
        "result_manifest_json": {"execution_deadline": evidence},
    }
    provider = ScriptedProvider(
        [
            Step(contains="FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt", one=current),
            Step(
                contains="UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt",
                one=updated,
            ),
            Step(contains="SELECT run_id FROM strategy_pkg.multi_alpha_combine_backtest_child", one={"run_id": "macb_test"}),
            Step(
                contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_event",
                one={"event_id": 9, "phase": "deadline_exceeded"},
            ),
        ]
    )
    repository = MultiAlphaDurableRepository(connection_provider=provider)

    result = repository.record_attempt_deadline_evidence(
        "macba_test",
        token=OwnershipToken(owner_id="worker_1", fencing_token=1, row_version=2),
        evidence=evidence,
    )

    assert result == updated
    assert provider.commits == 1
    assert provider.rollbacks == 0
    assert provider.cursor.executions[1][1][0].adapted == {
        "execution_deadline": evidence
    }
    event_params = provider.cursor.executions[3][1]
    assert event_params[4] == "deadline_exceeded"
    assert event_params[5] == "multi_alpha_execution_deadline_exceeded"

    replay_current = {**updated, "lease_valid": True}
    replay_provider = ScriptedProvider(
        [
            Step(
                contains="FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt",
                one=replay_current,
            )
        ]
    )
    replay_repository = MultiAlphaDurableRepository(connection_provider=replay_provider)

    assert replay_repository.record_attempt_deadline_evidence(
        "macba_test",
        token=OwnershipToken(owner_id="worker_1", fencing_token=1, row_version=3),
        evidence=evidence,
    ) == replay_current
    assert len(replay_provider.cursor.executions) == 1


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


def test_legacy_optional_insert_shapes_keep_required_attempt_run_id() -> None:
    request_payload = _run_request()
    run_spec = DurableRunSpec(
        run_id="macb_test",
        task_id="mact_test",
        request_hash=request_hash_for(request_payload),
        roster_hash="roster",
        roster=[{"leg_id": "L1"}],
        oos_start="2026-01-01",
        oos_end="2026-06-29",
        normalize_method="rank",
        walk_forward={"enabled": True},
        backtest_config={"topk": 25},
    )
    run_provider = ScriptedProvider(
        [
            Step(contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_run", one={"id": "macb_test"}),
            Step(contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_event", one={"event_id": 1}),
        ]
    )
    run_repository = MultiAlphaDurableRepository(connection_provider=run_provider)
    run_repository.create_run(run_spec)
    run_sql = run_provider.cursor.executions[0][0]
    assert "execution_identity_json" not in run_sql
    assert "recovery_scope_json" not in run_sql

    child_id = make_child_id("macb_test", "scheme:equal")
    child_manifest = {"schema_version": "child", "run_id": "macb_test"}
    child_spec = DurableChildSpec(
        child_id=child_id,
        run_id="macb_test",
        child_key="scheme:equal",
        child_kind="scheme",
        weighting_scheme="equal",
        ordinal=0,
        input_manifest=child_manifest,
        input_manifest_hash=artifact_manifest_hash_for(child_manifest),
    )
    child_provider = ScriptedProvider(
        [
            Step(contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_child", one={"child_id": child_id}),
            Step(contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_event", one={"event_id": 2}),
        ]
    )
    child_repository = MultiAlphaDurableRepository(connection_provider=child_provider)
    child_repository.create_child(child_spec)
    child_sql = child_provider.cursor.executions[0][0]
    assert "source_child_id" not in child_sql
    assert "execution_disposition" not in child_sql

    attempt_spec = DurableAttemptSpec(
        attempt_id=make_attempt_id(child_id, 1),
        child_id=child_id,
        attempt_no=1,
        retry_mode="initial",
        node_id="wsl2-5080",
        status="queued",
        phase="queued",
    )
    attempt_provider = ScriptedProvider(
        [
            Step(contains="SELECT child_id, run_id", one={"child_id": child_id, "run_id": "macb_test"}),
            Step(contains="SELECT attempt_id", one=None),
            Step(
                contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_child_attempt",
                one={"attempt_id": attempt_spec.attempt_id},
            ),
            Step(contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_event", one={"event_id": 3}),
        ]
    )
    attempt_repository = MultiAlphaDurableRepository(connection_provider=attempt_provider)
    attempt_repository.create_attempt(attempt_spec)
    attempt_sql = attempt_provider.cursor.executions[2][0]
    attempt_params = attempt_provider.cursor.executions[2][1]
    assert "attempt_id, run_id, child_id" in " ".join(attempt_sql.split())
    assert attempt_params[:3] == [attempt_spec.attempt_id, "macb_test", child_id]
    assert "source_attempt_id" not in attempt_sql
    assert "execution_kind" not in attempt_sql
    assert "result_manifest_hash" not in attempt_sql


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


def test_early_pause_cancel_and_resume_sql_cover_zero_child_runs() -> None:
    source = (REPO_ROOT / "backend/services/multi_alpha/durable_repository.py").read_text(
        encoding="utf-8"
    )

    pause_start = source.index("def claim_next_pause_drain_run")
    pause_end = source.index("def append_archive_delivery_event", pause_start)
    pause_source = source[pause_start:pause_end]
    assert "NOT EXISTS (" in pause_source
    assert "multi_alpha_combine_backtest_child AS child" in pause_source
    assert "child.status = 'materializing'" in pause_source

    finalizer_start = source.index("def claim_next_finalizable_run")
    finalizer_end = source.index("def list_runs_pending_archive", finalizer_start)
    finalizer_source = source[finalizer_start:finalizer_end]
    assert "run.status IN ('cancel_requested', 'cancelling')" in finalizer_source
    assert "OR EXISTS (" in finalizer_source

    apply_start = source.index("def apply_control_command_intent")
    apply_end = source.index("def reconcile_control_command", apply_start)
    apply_source = source[apply_start:apply_end]
    assert "NOT EXISTS (" in apply_source
    assert 'next_status="preparing" if needs_planning else "running"' in apply_source

    cancel_start = source.index("def _persist_cancel_intent_for_attempts_in_transaction")
    cancel_source = source[cancel_start:]
    assert "if target_attempt_ids is None:" in cancel_source
    assert "status <> ALL(%s)" in cancel_source
    assert "affected_child_ids.update" in cancel_source


def test_zero_child_pause_and_cancel_are_claimable_for_terminalization() -> None:
    pause_provider = ScriptedProvider(
        [
            Step(
                contains="FOR UPDATE SKIP LOCKED",
                one={**_claimed_run(), "status": "pause_requested"},
            ),
            Step(
                contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_event",
                one={"event_id": 1},
            ),
        ]
    )
    pause_repository = MultiAlphaDurableRepository(connection_provider=pause_provider)

    pause_claim = pause_repository.claim_next_pause_drain_run(
        owner_id="worker_1",
        lease_seconds=30,
    )

    assert pause_claim is not None
    assert pause_claim["status"] == "pause_requested"
    pause_sql = pause_provider.cursor.executions[0][0]
    assert "NOT EXISTS ( SELECT 1 FROM strategy_pkg.multi_alpha_combine_backtest_child" in pause_sql

    cancel_provider = ScriptedProvider(
        [
            Step(
                contains="FOR UPDATE SKIP LOCKED",
                one={**_claimed_run(), "status": "cancel_requested"},
            ),
            Step(
                contains="INSERT INTO strategy_pkg.multi_alpha_combine_backtest_event",
                one={"event_id": 2},
            ),
        ]
    )
    cancel_repository = MultiAlphaDurableRepository(connection_provider=cancel_provider)

    cancel_claim = cancel_repository.claim_next_finalizable_run(
        owner_id="worker_1",
        lease_seconds=30,
    )

    assert cancel_claim is not None
    assert cancel_claim["status"] == "cancel_requested"
    cancel_sql = cancel_provider.cursor.executions[0][0]
    assert "run.status IN ('cancel_requested', 'cancelling') OR EXISTS" in cancel_sql
