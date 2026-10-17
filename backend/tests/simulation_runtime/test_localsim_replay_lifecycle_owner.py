from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.services.simulation_runtime.localsim_control import LocalSimControlPlaneService
from backend.services.simulation_runtime.localsim_product_control import LocalSimHistoricalSourceResolutionV1
from backend.services.simulation_runtime.localsim_replay import LocalSimHistoricalDayRunner, LocalSimReplayCoordinator
from backend.services.simulation_runtime.localsim_replay_runtime import (
    LocalSimHistoricalReplayDayExecutor, LocalSimReplayLifecycleOwner,
)
from backend.services.simulation_runtime.models import SimulationDailyRunStatus
from backend.services.simulation_runtime.successor_models import LocalSimReplayStatus, LocalSimSafeBoundaryDecisionV1
from backend.services.simulation_runtime.successor_repository import InMemoryLocalSimSuccessorRepository
from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_snapshot
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError, DataUnavailableError, InvalidStateTransitionError,
)


NOW = datetime(2026, 8, 31, 0, 30, tzinfo=UTC)
DAYS = (date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28))
SOURCE_ID = "market.kline_minute_raw.v1"
SOURCE_SHA256 = "e" * 64


class _SourceAuthority:
    def __init__(self, *, current_date=date(2026, 8, 31), days=DAYS):
        self.current_date = current_date
        self.days = days

    def resolve(
        self, *, historical_source_id: str, start_trade_date: date, end_trade_date: date,
        require_latest_completed: bool = True,
    ):
        assert historical_source_id == SOURCE_ID
        if require_latest_completed and end_trade_date != self.days[-1]:
            raise InvalidStateTransitionError("range does not reach the latest completed day")
        return LocalSimHistoricalSourceResolutionV1(
            historical_source_id=SOURCE_ID,
            historical_source_sha256=SOURCE_SHA256,
            trading_days=tuple(day for day in self.days if start_trade_date <= day <= end_trade_date),
            current_trading_date=self.current_date,
            latest_completed_trade_date=self.days[-1],
        )


class _SafeBoundaryAuthority:
    def __init__(self, *, eligible: bool, trade_date=date(2026, 8, 31)) -> None:
        self.eligible = eligible
        self.trade_date = trade_date

    def evaluate(self, *, as_of_time: datetime) -> LocalSimSafeBoundaryDecisionV1:
        return LocalSimSafeBoundaryDecisionV1(
            eligible=self.eligible,
            evaluated_at=as_of_time,
            current_trading_date=self.trade_date,
            activation_trade_date=self.trade_date,
            market_phase="PRE_OPEN" if self.eligible else "NON_TRADING_DAY",
            in_flight_economic_transactions=0,
            writer_claim_available=True,
            historical_provider_closed=True,
            reason_code=(
                "LOCALSIM_REPLAY_PREOPEN_SAFE_BOUNDARY"
                if self.eligible
                else "LOCALSIM_REPLAY_WAITING_FOR_PREOPEN_BOUNDARY"
            ),
        )


class _ProductAuthority:
    def __init__(self, policy: dict) -> None:
        self._policy = policy

    def resolve_product(self, **_kwargs):
        profile = SimpleNamespace(profile_id="profile_1")
        version = SimpleNamespace(
            profile_version_id="profile_version_1",
            config_sha256="b" * 64,
            daily_strategy_profile_version_id="daily_1",
            config_json={
                "daily_strategy": {"strategy_id": "score_weighted_topk_v2", "strategy_version": "v2", "top_k": 20},
                "hmm": {"enabled": False}, "risk_policy": {"enabled": False}, "fee_policy": {},
            },
        )
        execution = SimpleNamespace(
            policy_id=self._policy["policy_version_id"],
            policy_sha256=self._policy["policy_sha256"],
            policy_json=self._policy["policy_json"],
        )
        return SimpleNamespace(
            runtime_profile=profile,
            runtime_profile_version=version,
            execution_policy=execution,
            tail_policy_version_id="tail_1",
            tail_policy_sha256="c" * 64,
            release_validation_evidence=lambda: {"admission_receipt_id": "admission_1"},
        )


class _FailAtomicActivationRepository(InMemoryLocalSimSuccessorRepository):
    def transition_replay_job(self, **kwargs):
        if kwargs["update"].get("status") is LocalSimReplayStatus.LIVE_ACTIVE:
            raise RuntimeError("injected atomic activation failure")
        return super().transition_replay_job(**kwargs)


def _replay(repository: InMemoryLocalSimSuccessorRepository):
    policy = local_sim_twap_only_policy_snapshot()
    control = LocalSimControlPlaneService(repository=repository, clock=lambda: NOW)
    account, _scope, release, binding = control.create_account(
        account_name="six month replay",
        package_id="pkg_1",
        manifest_sha256="a" * 64,
        admission_receipt_id="admission_1",
        initial_capital=1_000_000,
        runtime_profile_id="profile_1",
        runtime_profile_version_id="profile_version_1",
        runtime_profile_sha256="b" * 64,
        daily_strategy_profile_version_id="daily_1",
        execution_policy_version_id=policy["policy_version_id"],
        execution_policy_sha256=policy["policy_sha256"],
        execution_policy_json=policy["policy_json"],
        tail_policy_version_id="tail_1",
        tail_policy_sha256="c" * 64,
        effective_from=DAYS[0],
        effective_to=DAYS[-1],
        created_by="test",
    )
    runner = LocalSimHistoricalDayRunner(
        historical_source_id=SOURCE_ID,
        historical_source_sha256=SOURCE_SHA256,
        run_day=lambda *_args: None,
    )
    job = LocalSimReplayCoordinator(repository=repository, historical_day_runner=runner, clock=lambda: NOW).create_job(
        simulation_account_id=account.account_id,
        release_id=release.release_id,
        binding_id=binding.binding_id,
        start_trade_date=DAYS[0],
        end_trade_date=DAYS[-1],
        historical_source_id=SOURCE_ID,
        historical_source_sha256=SOURCE_SHA256,
        trading_days=DAYS,
        created_by="test",
    )
    return policy, control, job


def _owner(repository, control, policy, *, eligible: bool, executed: list[date]):
    return LocalSimReplayLifecycleOwner(
        repository=repository,
        control=control,
        product_authority=_ProductAuthority(policy),
        historical_source_authority=_SourceAuthority(),
        historical_day_executor=lambda _job, trade_date: executed.append(trade_date),
        safe_boundary_authority=_SafeBoundaryAuthority(eligible=eligible),
        max_days_per_job=2,
        clock=lambda: NOW,
    )


def test_background_owner_resumes_bounded_replay_and_atomically_activates_at_preopen() -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    policy, control, job = _replay(repository)
    executed: list[date] = []

    waiting_owner = _owner(repository, control, policy, eligible=False, executed=executed)
    first = waiting_owner.tick(as_of_time=NOW)
    assert first["outcomes"][0]["status"] == LocalSimReplayStatus.RUNNING_HISTORICAL.value
    second = waiting_owner.tick(as_of_time=NOW)
    assert second["outcomes"][0]["status"] == LocalSimReplayStatus.READY_FOR_LIVE.value
    assert executed == list(DAYS)

    restarted_owner = _owner(repository, control, policy, eligible=True, executed=executed)
    activated = restarted_owner.tick(as_of_time=NOW)
    current = repository.get_replay_job(job.replay_job_id)
    assert activated["outcomes"][0]["status"] == LocalSimReplayStatus.LIVE_ACTIVE.value
    assert current.status is LocalSimReplayStatus.LIVE_ACTIVE
    assert current.live_release_id is not None
    assert current.live_binding_id is not None
    assert repository.get_binding(current.live_binding_id).effective_from == date(2026, 8, 31)
    assert executed == list(DAYS)


def test_atomic_live_activation_rolls_back_release_binding_and_job_on_failure() -> None:
    repository = _FailAtomicActivationRepository()
    policy, control, job = _replay(repository)
    executed: list[date] = []
    waiting_owner = _owner(repository, control, policy, eligible=False, executed=executed)
    waiting_owner.tick(as_of_time=NOW)
    waiting_owner.tick(as_of_time=NOW)
    before_release_ids = set(repository.releases)
    before_binding_ids = set(repository.bindings)

    active_owner = _owner(repository, control, policy, eligible=True, executed=executed)
    result = active_owner.tick(as_of_time=NOW)

    assert result["outcomes"][0]["error"]["type"] == "RuntimeError"
    assert repository.get_replay_job(job.replay_job_id).status is LocalSimReplayStatus.READY_FOR_LIVE
    assert set(repository.releases) == before_release_ids
    assert set(repository.bindings) == before_binding_ids


def test_long_running_selection_timeout_retries_until_actual_result_is_available() -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    policy, control, job = _replay(repository)
    executed: list[date] = []
    owner = _owner(repository, control, policy, eligible=False, executed=executed)
    ready = False

    def run_day(_job, trade_date):
        if not ready:
            raise ArtifactGenerationFailedError(
                "simulation scheduler selection inference timed out; tick remains non-blocking",
                context={
                    "reason_code": "SIMULATION_SELECTION_INFERENCE_TIMEOUT", "thread_isolated": True,
                    "elapsed_seconds": 610, "timeout_seconds": 600, "stdout_tail": "must not persist",
                },
            )
        executed.append(trade_date)

    owner.historical_day_executor = run_day
    result = owner.tick(as_of_time=NOW)
    current = repository.get_replay_job(job.replay_job_id)
    assert result["outcomes"][0]["status"] == "FAILED_RETRYABLE"
    assert current.completed_trade_date is None and current.next_trade_date == DAYS[0]
    assert current.failure_context["reason_code"] == "SIMULATION_SELECTION_INFERENCE_TIMEOUT"
    assert "stdout_tail" not in current.failure_context
    ready = True
    owner.tick(as_of_time=NOW)
    owner.tick(as_of_time=NOW)
    assert repository.get_replay_job(job.replay_job_id).status is LocalSimReplayStatus.READY_FOR_LIVE
    assert executed == list(DAYS)


@pytest.mark.parametrize("recoverable", [True, False])
def test_legacy_terminal_timeout_recovery_is_precise_and_preserves_completed_cursor(recoverable) -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    policy, control, job = _replay(repository)
    failure_context = {"message": "simulation scheduler selection inference timed out; tick remains non-blocking"}
    if not recoverable:
        failure_context["reason_code"] = "INVALID_MODEL_ASSET"
    repository.transition_replay_job(
        replay_job_id=job.replay_job_id, expected_version=job.version,
        update={
            "status": LocalSimReplayStatus.FAILED_TERMINAL,
            "completed_trade_date": DAYS[1], "next_trade_date": DAYS[2],
            "failure_code": "ArtifactGenerationFailedError", "failure_context": failure_context,
        }, updated_at=NOW,
    )
    executed: list[date] = []
    owner = _owner(repository, control, policy, eligible=False, executed=executed)
    before = repository.get_account(job.simulation_account_id).model_dump()
    result = owner.tick(as_of_time=NOW)
    assert executed == (list(DAYS[2:]) if recoverable else [])
    assert result["eligible_job_count"] == (1 if recoverable else 0)
    assert repository.get_account(job.simulation_account_id).model_dump() == before


def test_cross_day_replay_catches_up_on_same_account_before_preopen_activation() -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    policy, control, root = _replay(repository)
    executed: list[date] = []
    owner = _owner(repository, control, policy, eligible=False, executed=executed)
    owner.historical_source_authority = _SourceAuthority(
        current_date=date(2026, 9, 1), days=(*DAYS, date(2026, 8, 31)),
    )
    original_identity = root.replay_hash, root.calendar_snapshot_sha256, root.end_trade_date
    original_account = repository.get_account(root.simulation_account_id).model_dump()
    owner.tick(as_of_time=NOW)
    created = owner.tick(as_of_time=NOW)["outcomes"][0]
    child_id = created["successor_replay_job_id"]
    assert child_id and created["status"] == "CREATED"
    child = repository.get_replay_job(child_id)
    assert child.start_trade_date == child.end_trade_date == date(2026, 8, 31)
    assert child.simulation_account_id == root.simulation_account_id
    owner.tick(as_of_time=NOW)
    assert repository.get_replay_job(child_id).status is LocalSimReplayStatus.READY_FOR_LIVE

    # Completion after the safe window requires another completed day, not a gap or a replay of prior fills.
    owner.historical_source_authority = _SourceAuthority(
        current_date=date(2026, 9, 2), days=(*DAYS, date(2026, 8, 31), date(2026, 9, 1)),
    )
    newest_id = owner.tick(as_of_time=NOW)["outcomes"][0]["successor_replay_job_id"]
    owner.tick(as_of_time=NOW)
    owner.safe_boundary_authority = _SafeBoundaryAuthority(eligible=True, trade_date=date(2026, 9, 2))
    owner.tick(as_of_time=NOW)
    active = repository.get_replay_job(newest_id)
    assert active.status is LocalSimReplayStatus.LIVE_ACTIVE
    assert repository.get_binding(active.live_binding_id).effective_from == date(2026, 9, 2)
    assert executed == [*DAYS, date(2026, 8, 31), date(2026, 9, 1)]
    retained = repository.get_replay_job(root.replay_job_id)
    assert (retained.replay_hash, retained.calendar_snapshot_sha256, retained.end_trade_date) == original_identity
    assert repository.get_account(root.simulation_account_id).model_dump() == original_account
    assert len(repository.accounts) == len(repository.ledger_scopes) == 1


def test_cancelled_catchup_does_not_reactivate_predecessor_or_replay_completed_days() -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    policy, control, _root = _replay(repository)
    executed: list[date] = []
    owner = _owner(repository, control, policy, eligible=False, executed=executed)
    owner.historical_source_authority = _SourceAuthority(
        current_date=date(2026, 9, 1), days=(*DAYS, date(2026, 8, 31)),
    )
    owner.tick(as_of_time=NOW)
    child_id = owner.tick(as_of_time=NOW)["outcomes"][0]["successor_replay_job_id"]
    child = repository.get_replay_job(child_id)
    repository.transition_replay_job(
        replay_job_id=child_id, expected_version=child.version,
        update={"status": LocalSimReplayStatus.CANCELLED}, updated_at=NOW,
    )
    owner.safe_boundary_authority = _SafeBoundaryAuthority(eligible=True)
    assert owner.tick(as_of_time=NOW)["eligible_job_count"] == 0
    assert executed == list(DAYS)
    assert not any(job.status is LocalSimReplayStatus.LIVE_ACTIVE for job in repository.replay_jobs.values())


def test_catchup_creation_rolls_back_and_later_retry_cannot_fork_the_lineage() -> None:
    class FailRepository(InMemoryLocalSimSuccessorRepository):
        fail = False

        def save_replay_job(self, job):
            if self.fail:
                raise RuntimeError("injected catchup insert failure")
            return super().save_replay_job(job)

    repository = FailRepository()
    policy, control, root = _replay(repository)
    executed: list[date] = []
    owner = _owner(repository, control, policy, eligible=False, executed=executed)
    owner.historical_source_authority = _SourceAuthority(
        current_date=date(2026, 9, 1), days=(*DAYS, date(2026, 8, 31)),
    )
    owner.tick(as_of_time=NOW)
    before_releases, before_bindings = set(repository.releases), set(repository.bindings)
    repository.fail = True
    failure = owner.tick(as_of_time=NOW)["outcomes"][0]
    assert failure["error"]["type"] == "RuntimeError"
    assert set(repository.releases) == before_releases and set(repository.bindings) == before_bindings
    parent = repository.get_replay_job(root.replay_job_id)
    assert parent.status is LocalSimReplayStatus.CAUGHT_UP
    repository.fail = False
    first_child = owner._create_catchup_successor(
        job=parent, latest_completed_trade_date=date(2026, 8, 31), as_of_time=NOW,
    )
    owner.historical_source_authority = _SourceAuthority(
        current_date=date(2026, 9, 2), days=(*DAYS, date(2026, 8, 31), date(2026, 9, 1)),
    )
    second_child = owner._create_catchup_successor(
        job=repository.get_replay_job(root.replay_job_id),
        latest_completed_trade_date=date(2026, 9, 1), as_of_time=NOW,
    )
    assert second_child.replay_job_id == first_child.replay_job_id
    assert len(repository.replay_jobs) == 2
    assert executed == list(DAYS)


@pytest.mark.parametrize("drift", ["calendar", "activation_date"])
def test_waiting_replay_does_not_activate_with_changed_calendar_or_wrong_live_date(drift) -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    policy, control, root = _replay(repository)
    executed: list[date] = []
    owner = _owner(repository, control, policy, eligible=False, executed=executed)
    owner.tick(as_of_time=NOW)
    owner.tick(as_of_time=NOW)
    if drift == "calendar":
        owner.historical_source_authority = _SourceAuthority(days=(DAYS[0], *DAYS[2:]))
    owner.safe_boundary_authority = _SafeBoundaryAuthority(
        eligible=True, trade_date=date(2026, 9, 1) if drift == "activation_date" else date(2026, 8, 31),
    )
    result = owner.tick(as_of_time=NOW)
    assert result["outcomes"][0]["error"]["type"] == "InvalidStateTransitionError"
    assert repository.get_replay_job(root.replay_job_id).status is LocalSimReplayStatus.READY_FOR_LIVE
    assert len(repository.releases) == len(repository.bindings) == 1
    assert executed == list(DAYS)


def test_actual_model_artifact_failure_remains_terminal_and_is_not_retried_as_timeout() -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    policy, control, root = _replay(repository)
    owner = _owner(repository, control, policy, eligible=False, executed=[])

    def broken_model(_job, _date):
        raise ArtifactGenerationFailedError(
            "model asset hash is invalid", context={"reason_code": "INVALID_MODEL_ASSET"},
        )

    owner.historical_day_executor = broken_model
    owner.tick(as_of_time=NOW)
    current = repository.get_replay_job(root.replay_job_id)
    assert current.status is LocalSimReplayStatus.FAILED_TERMINAL
    assert current.failure_context["reason_code"] == "INVALID_MODEL_ASSET"
    assert owner.tick(as_of_time=NOW)["eligible_job_count"] == 0


@pytest.mark.parametrize("status,projected,expected", [
    ("SUCCEEDED", True, "complete"), ("INTRADAY_RUNNING", False, "pending"),
    ("SIGNAL_GENERATING", False, "pending"), ("SUCCEEDED", False, "pending"), ("FAILED_TERMINAL", True, "complete"),
    ("FAILED_TERMINAL", False, "pending"),
])
def test_historical_executor_requires_completed_durable_day(status, projected, expected):
    repo = InMemoryLocalSimSuccessorRepository()
    _, _, job = _replay(repo)
    run = SimpleNamespace(status=SimulationDailyRunStatus(status), run_payload_json={
        "local_sim_persistence": {"status": "PERSISTED_WITH_RESIDUAL"},
        "local_sim_projection_outbox_v1": {"status": "PROJECTED" if projected else "PENDING"},
    })
    item = SimpleNamespace(
        strategy_id=job.simulation_account_id, binding_id=job.binding_id, run=run,
        status="LOCALSIM_EXECUTION_RESIDUAL_TERMINAL", error=None, is_success=True,
    )
    executor = LocalSimHistoricalReplayDayExecutor(
        lifecycle_scheduler=SimpleNamespace(run_once=lambda **_kwargs: SimpleNamespace(results=[item])),
    )
    if expected == "complete":
        executor(job, DAYS[0])
    else:
        with pytest.raises(DataUnavailableError, match="not completed durable"):
            executor(job, DAYS[0])


def test_historical_submit_window_failure_recovers_original_cursor():
    repo = InMemoryLocalSimSuccessorRepository()
    policy, control, job = _replay(repo)
    failed = repo.transition_replay_job(
        replay_job_id=job.replay_job_id, expected_version=job.version,
        update={"status": LocalSimReplayStatus.FAILED_TERMINAL, "failure_code": "RuntimeConfigInvalidError",
                "failure_context": {"reason_code": "SIMULATION_SUBMIT_OUTSIDE_TRADING_WINDOW",
                                    "trade_date": DAYS[0].isoformat()}}, updated_at=NOW,
    )
    executed = []
    outcome = _owner(repo, control, policy, eligible=False, executed=executed).tick(as_of_time=NOW)
    assert outcome["outcomes"][0]["replay_job_id"] == failed.replay_job_id
    assert executed == list(DAYS[:2])
    _owner(repo, control, policy, eligible=False, executed=executed).tick(as_of_time=NOW)
    assert executed == list(DAYS)
    assert repo.get_replay_job(job.replay_job_id).completed_trade_date == DAYS[-1]
    assert len(repo.accounts) == 1


@pytest.mark.parametrize("update", [
    {"status": LocalSimReplayStatus.CANCELLED}, {"failure_code": "ArtifactGenerationFailedError"},
    {"failure_context": {"reason_code": "OTHER_FAILURE", "trade_date": DAYS[0].isoformat()}},
    {"failure_context": {"reason_code": "SIMULATION_SUBMIT_OUTSIDE_TRADING_WINDOW", "trade_date": "2026-01-01"}},
])
def test_submit_window_recovery_rejects_unrelated_failures(update):
    repo = InMemoryLocalSimSuccessorRepository()
    _, _, job = _replay(repo)
    failed = job.model_copy(update={
        "status": LocalSimReplayStatus.FAILED_TERMINAL, "failure_code": "RuntimeConfigInvalidError",
        "failure_context": {"reason_code": "SIMULATION_SUBMIT_OUTSIDE_TRADING_WINDOW",
                            "trade_date": DAYS[0].isoformat()}, **update,
    })
    assert not LocalSimReplayCoordinator.is_recoverable_historical_submit_window(failed)


def test_paused_replay_waits_without_mutating_cursor_or_recovering_failure():
    repo = InMemoryLocalSimSuccessorRepository()
    policy, control, job = _replay(repo)
    account = repo.get_account(job.simulation_account_id)
    control.pause_account(account_id=account.account_id, expected_version=account.version)
    executed = []
    outcome = _owner(repo, control, policy, eligible=False, executed=executed).tick(as_of_time=NOW)
    assert outcome["eligible_job_count"] == 0
    assert executed == []
    assert repo.get_replay_job(job.replay_job_id) == job
