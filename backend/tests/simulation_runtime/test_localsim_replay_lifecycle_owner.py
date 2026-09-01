from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

from backend.services.simulation_runtime.localsim_control import LocalSimControlPlaneService
from backend.services.simulation_runtime.localsim_product_control import LocalSimHistoricalSourceResolutionV1
from backend.services.simulation_runtime.localsim_replay import LocalSimHistoricalDayRunner, LocalSimReplayCoordinator
from backend.services.simulation_runtime.localsim_replay_runtime import LocalSimReplayLifecycleOwner
from backend.services.simulation_runtime.successor_models import LocalSimReplayStatus, LocalSimSafeBoundaryDecisionV1
from backend.services.simulation_runtime.successor_repository import InMemoryLocalSimSuccessorRepository
from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_snapshot


NOW = datetime(2026, 8, 31, 0, 30, tzinfo=UTC)
DAYS = (date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28))
SOURCE_ID = "market.kline_minute_raw.v1"
SOURCE_SHA256 = "e" * 64


class _SourceAuthority:
    def resolve(self, *, historical_source_id: str, start_trade_date: date, end_trade_date: date):
        assert historical_source_id == SOURCE_ID
        assert (start_trade_date, end_trade_date) == (DAYS[0], DAYS[-1])
        return LocalSimHistoricalSourceResolutionV1(
            historical_source_id=SOURCE_ID,
            historical_source_sha256=SOURCE_SHA256,
            trading_days=DAYS,
            current_trading_date=date(2026, 8, 31),
            latest_completed_trade_date=DAYS[-1],
        )


class _SafeBoundaryAuthority:
    def __init__(self, *, eligible: bool) -> None:
        self.eligible = eligible

    def evaluate(self, *, as_of_time: datetime) -> LocalSimSafeBoundaryDecisionV1:
        return LocalSimSafeBoundaryDecisionV1(
            eligible=self.eligible,
            evaluated_at=as_of_time,
            current_trading_date=date(2026, 8, 31),
            activation_trade_date=date(2026, 8, 31),
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
            config_json={"daily_strategy": {"kind": "package_default"}},
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
