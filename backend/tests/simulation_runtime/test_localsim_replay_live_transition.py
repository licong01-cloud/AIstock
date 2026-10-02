from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from backend.services.simulation_data.daily_context import SimulationBrokerBackend
from backend.services.simulation_runtime.localsim_control import LocalSimControlPlaneService
from backend.services.simulation_runtime.localsim_replay import (
    LocalSimHistoricalDayRunner,
    LocalSimReplayCoordinator,
)
from backend.services.simulation_runtime.models import (
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
)
from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository
from backend.services.simulation_runtime.service import StrategyRuntimeReleaseService
from backend.services.simulation_runtime.successor_models import (
    LocalSimReplayStatus,
    LocalSimSafeBoundaryDecisionV1,
)
from backend.services.simulation_runtime.successor_repository import InMemoryLocalSimSuccessorRepository
from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_snapshot
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError


NOW = datetime(2026, 8, 31, 1, 0, tzinfo=UTC)
HISTORICAL_SOURCE_ID = "market.kline_minute_raw.completed_days"
HISTORICAL_SOURCE_SHA256 = "f" * 64


def _runner(callback) -> LocalSimHistoricalDayRunner:
    return LocalSimHistoricalDayRunner(
        historical_source_id=HISTORICAL_SOURCE_ID,
        historical_source_sha256=HISTORICAL_SOURCE_SHA256,
        run_day=callback,
    )


def _business_days(start: date, count: int) -> list[date]:
    result: list[date] = []
    cursor = start
    while len(result) < count:
        if cursor.weekday() < 5:
            result.append(cursor)
        cursor += timedelta(days=1)
    return result


def _account_bundle(
    repository: InMemoryLocalSimSuccessorRepository,
    *,
    account_name: str,
    effective_to: date | None = None,
):
    policy = local_sim_twap_only_policy_snapshot()
    service = LocalSimControlPlaneService(repository=repository, clock=lambda: NOW)
    account, _ledger_scope, release, binding = service.create_account(
        account_name=account_name,
        package_id="pkg_current_alpha",
        manifest_sha256="a" * 64,
        admission_receipt_id="admission_current_alpha",
        initial_capital=1_000_000.0,
        runtime_profile_id="localsim_runtime",
        runtime_profile_version_id="runtime_v1",
        runtime_profile_sha256="b" * 64,
        daily_strategy_profile_version_id="daily_v1",
        execution_policy_version_id=policy["policy_version_id"],
        execution_policy_sha256=policy["policy_sha256"],
        execution_policy_json=policy["policy_json"],
        tail_policy_version_id="tail_v1",
        tail_policy_sha256="c" * 64,
        effective_to=effective_to,
        created_by="test",
    )
    return account, release, binding


def _successor_release_binding(
    repository: InMemoryLocalSimSuccessorRepository, *, account_id: str, base_release_id: str
):
    account = repository.get_account(account_id)
    policy = local_sim_twap_only_policy_snapshot()
    staging = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=staging)
    release = service.create_release(
        package_id=account.package_id,
        manifest_sha256=account.manifest_sha256,
        base_release_id=base_release_id,
        runtime_profile_id="localsim_runtime",
        runtime_profile_version_id="runtime_live_v2",
        runtime_profile_sha256="d" * 64,
        daily_strategy_profile_version_id="daily_live_v2",
        execution_policy_version_id=policy["policy_version_id"],
        execution_policy_sha256=policy["policy_sha256"],
        execution_policy_json=policy["policy_json"],
        tail_policy_version_id="tail_live_v2",
        tail_policy_sha256="e" * 64,
        validation_state=RuntimeReleaseValidationState.SIM_PASSED,
        created_by="test",
    )
    binding = service.create_binding(
        strategy_id=account.account_id,
        release=release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        broker_account_id=account.account_id,
        account_group_id=account.account_id,
        capital_allocation=account.initial_capital,
        approval_state=SimulationBindingApprovalState.SIM_PASSED,
        binding_metadata={
            "localsim_account_id": account.account_id,
            "account_schema_version": "simulation_account_v1",
        },
        created_by="test",
    )
    return release, binding


def _job(
    repository: InMemoryLocalSimSuccessorRepository,
    *,
    days: list[date],
    runner,
    account_name: str = "isolated replay",
):
    account, release, binding = _account_bundle(
        repository,
        account_name=account_name,
        effective_to=days[-1],
    )
    coordinator = LocalSimReplayCoordinator(
        repository=repository,
        historical_day_runner=_runner(runner),
        clock=lambda: NOW,
    )
    job = coordinator.create_job(
        simulation_account_id=account.account_id,
        release_id=release.release_id,
        binding_id=binding.binding_id,
        start_trade_date=days[0],
        end_trade_date=days[-1],
        historical_source_id=HISTORICAL_SOURCE_ID,
        historical_source_sha256=HISTORICAL_SOURCE_SHA256,
        trading_days=days,
        created_by="test",
    )
    return coordinator, job, account, release, binding


def test_six_month_replay_resumes_from_durable_day_cursor_without_touching_current_account() -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    current_account, current_release, current_binding = _account_bundle(repository, account_name="currently running")
    current_snapshot = (
        current_account.model_dump(mode="json"),
        current_release.model_dump(mode="json"),
        current_binding.model_dump(mode="json"),
    )
    days = _business_days(date(2026, 3, 2), 126)
    executed: list[date] = []
    coordinator, job, _account, _release, _binding = _job(
        repository, days=days, runner=lambda _job, day: executed.append(day)
    )

    first = coordinator.run_next_batch(
        replay_job_id=job.replay_job_id,
        expected_version=job.version,
        trading_days=days,
        current_trading_date=days[-1] + timedelta(days=3),
        max_days=50,
    )
    assert first.completed_trade_date == days[49]
    assert first.next_trade_date == days[50]

    restarted = LocalSimReplayCoordinator(
        repository=repository,
        historical_day_runner=_runner(lambda _job, day: executed.append(day)),
        clock=lambda: NOW,
    )
    second = restarted.run_next_batch(
        replay_job_id=job.replay_job_id,
        expected_version=first.version,
        trading_days=days,
        current_trading_date=days[-1] + timedelta(days=3),
        max_days=100,
    )

    assert second.status is LocalSimReplayStatus.CAUGHT_UP
    assert second.completed_trade_date == days[-1]
    assert second.next_trade_date is None
    assert executed == days
    assert repository.get_account(current_account.account_id).model_dump(mode="json") == current_snapshot[0]
    assert repository.get_release(current_release.release_id).model_dump(mode="json") == current_snapshot[1]
    assert repository.get_binding(current_binding.binding_id).model_dump(mode="json") == current_snapshot[2]


def test_replay_failure_is_durable_and_retry_resumes_exact_failed_day() -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    days = _business_days(date(2026, 8, 24), 4)
    attempts: list[date] = []

    def fail_second(_job, trade_date: date) -> None:
        attempts.append(trade_date)
        if trade_date == days[1] and attempts.count(trade_date) == 1:
            raise DataUnavailableError("historical minute batch unavailable")

    coordinator, job, _account, _release, _binding = _job(repository, days=days, runner=fail_second)
    with pytest.raises(DataUnavailableError, match="historical minute batch unavailable"):
        coordinator.run_next_batch(
            replay_job_id=job.replay_job_id,
            expected_version=job.version,
            trading_days=days,
            current_trading_date=date(2026, 8, 31),
            max_days=4,
        )
    failed = repository.get_replay_job(job.replay_job_id)
    assert failed.status is LocalSimReplayStatus.FAILED_RETRYABLE
    assert failed.completed_trade_date == days[0]
    assert failed.next_trade_date == days[1]

    completed = coordinator.run_next_batch(
        replay_job_id=job.replay_job_id,
        expected_version=failed.version,
        trading_days=days,
        current_trading_date=date(2026, 8, 31),
        max_days=4,
    )
    assert completed.status is LocalSimReplayStatus.CAUGHT_UP
    assert attempts == [days[0], days[1], days[1], days[2], days[3]]


def test_replay_rejects_current_day_and_calendar_snapshot_drift() -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    days = _business_days(date(2026, 8, 26), 3)
    coordinator, job, _account, _release, _binding = _job(repository, days=days, runner=lambda *_args: None)

    with pytest.raises(InvalidStateTransitionError, match="current or future"):
        coordinator.run_next_batch(
            replay_job_id=job.replay_job_id,
            expected_version=job.version,
            trading_days=days,
            current_trading_date=days[-1],
            max_days=3,
        )
    with pytest.raises(InvalidStateTransitionError, match="calendar snapshot changed"):
        coordinator.run_next_batch(
            replay_job_id=job.replay_job_id,
            expected_version=job.version,
            trading_days=[days[0], days[-1]],
            current_trading_date=date(2026, 8, 31),
            max_days=3,
        )

    wrong_source = LocalSimReplayCoordinator(
        repository=repository,
        historical_day_runner=LocalSimHistoricalDayRunner(
            historical_source_id=HISTORICAL_SOURCE_ID,
            historical_source_sha256="0" * 64,
            run_day=lambda *_args: None,
        ),
        clock=lambda: NOW,
    )
    with pytest.raises(InvalidStateTransitionError, match="identity changed"):
        wrong_source.run_next_batch(
            replay_job_id=job.replay_job_id,
            expected_version=job.version,
            trading_days=days,
            current_trading_date=date(2026, 8, 31),
            max_days=3,
        )
    assert repository.get_replay_job(job.replay_job_id) == job


def test_caught_up_replay_creates_atomic_successor_then_activates_only_at_safe_boundary() -> None:
    repository = InMemoryLocalSimSuccessorRepository()
    days = _business_days(date(2026, 8, 27), 2)
    coordinator, job, account, historical_release, _binding = _job(repository, days=days, runner=lambda *_args: None)
    caught_up = coordinator.run_next_batch(
        replay_job_id=job.replay_job_id,
        expected_version=job.version,
        trading_days=days,
        current_trading_date=date(2026, 8, 31),
        max_days=2,
    )
    ready = coordinator.mark_ready_for_live(
        replay_job_id=job.replay_job_id,
        expected_version=caught_up.version,
    )
    release, binding = _successor_release_binding(
        repository,
        account_id=account.account_id,
        base_release_id=historical_release.release_id,
    )
    decision = LocalSimSafeBoundaryDecisionV1(
        eligible=True,
        evaluated_at=NOW,
        current_trading_date=date(2026, 8, 31),
        activation_trade_date=date(2026, 8, 31),
        market_phase="PRE_OPEN",
        in_flight_economic_transactions=0,
        writer_claim_available=True,
        historical_provider_closed=True,
        reason_code="LOCALSIM_PREOPEN_SAFE_BOUNDARY",
    )

    pending = coordinator.prepare_live_successor(
        replay_job_id=job.replay_job_id,
        expected_version=ready.version,
        release=release,
        binding=binding,
        decision=decision,
    )
    assert pending.status is LocalSimReplayStatus.ACTIVATION_PENDING_SAFE_BOUNDARY
    assert pending.live_release_id == release.release_id
    assert pending.live_binding_id == binding.binding_id

    active = coordinator.activate_live(
        replay_job_id=job.replay_job_id,
        expected_version=pending.version,
        decision=decision,
    )
    assert active.status is LocalSimReplayStatus.LIVE_ACTIVE
    assert active.activation_trade_date == date(2026, 8, 31)


def test_intraday_catch_up_must_target_next_safe_trading_date() -> None:
    with pytest.raises(ValueError, match="intraday catch-up"):
        LocalSimSafeBoundaryDecisionV1(
            eligible=True,
            evaluated_at=NOW,
            current_trading_date=date(2026, 8, 31),
            activation_trade_date=date(2026, 8, 31),
            market_phase="TRADING",
            in_flight_economic_transactions=0,
            writer_claim_available=True,
            historical_provider_closed=True,
            reason_code="INVALID_INTRADAY_CURRENT_DAY",
        )
