"""Automatic lifecycle owner for durable LocalSIM historical replays."""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .localsim_control import LocalSimControlPlaneService
from .localsim_cutover_readiness import LocalSimCutoverReadiness
from .localsim_product_authority import LocalSimProductAuthority
from .localsim_product_control import LocalSimHistoricalSourceAuthorityProtocol
from .localsim_replay import (
    LocalSimHistoricalDayRunner, LocalSimReplayCoordinator, local_sim_calendar_snapshot_sha256,
)
from .models import SimulationBrokerBackend, SimulationDailyRunStatus
from .successor_models import (
    LocalSimReplayJobV1,
    LocalSimReplayStatus,
    LocalSimSafeBoundaryDecisionV1,
    SimulationAccountStatus,
)
from .successor_repository import LocalSimSuccessorRepositoryProtocol


CHINA_TZ = ZoneInfo("Asia/Shanghai")
LOCALSIM_REPLAY_PREOPEN_CUTOFF = time(9, 10)


class LocalSimHistoricalReplayDayExecutor:
    def __init__(self, *, lifecycle_scheduler: object) -> None:
        self.lifecycle_scheduler = lifecycle_scheduler

    def __call__(self, job: LocalSimReplayJobV1, trade_date) -> None:
        as_of_time = datetime.combine(trade_date, time(15, 30), tzinfo=ZoneInfo("Asia/Shanghai"))
        result = self.lifecycle_scheduler.run_once(
            trade_date=trade_date,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            strategy_id=job.simulation_account_id,
            release_id=job.release_id,
            submit=True,
            as_of_time=as_of_time,
            created_by="localsim_replay_lifecycle_owner",
            raise_on_error=True,
        )
        matching = [
            item
            for item in result.results
            if item.strategy_id == job.simulation_account_id and item.binding_id == job.binding_id
        ]
        if len(matching) != 1:
            raise DataUnavailableError(
                "LocalSIM replay daily engine did not resolve exactly one dedicated binding",
                context={
                    "reason_code": "LOCALSIM_REPLAY_DAILY_ENGINE_BINDING_MISMATCH",
                    "replay_job_id": job.replay_job_id,
                    "trade_date": trade_date.isoformat(),
                    "matching_binding_count": len(matching),
                },
            )
        item = matching[0]
        run = item.run
        payload = run.run_payload_json if run is not None else {}
        residual_completed = (
            run is not None and run.status is SimulationDailyRunStatus.FAILED_TERMINAL
            and item.status in {"LOCALSIM_EXECUTION_RESIDUAL_TERMINAL", "LOCALSIM_CAPACITY_RESIDUAL_TERMINAL"}
            and (payload.get("local_sim_persistence") or {}).get("status")
            in {"PERSISTED_WITH_RESIDUAL", "PERSISTED_WITH_CAPACITY_RESIDUAL"}
            and (payload.get("local_sim_projection_outbox_v1") or {}).get("status") == "PROJECTED"
        )
        if not item.is_success:
            raise InvalidStateTransitionError(
                "LocalSIM replay daily engine failed",
                context={
                    "reason_code": "LOCALSIM_REPLAY_DAILY_ENGINE_FAILED",
                    "replay_job_id": job.replay_job_id,
                    "trade_date": trade_date.isoformat(),
                    "binding_id": job.binding_id,
                    "status": item.status,
                    "error": item.error,
                },
            )
        succeeded_completed = (
            run is not None and run.status is SimulationDailyRunStatus.SUCCEEDED
            and (
                payload.get("no_rebalance_required") is True
                or bool(payload.get("pre_trade_blocked_order_generation"))
                or (payload.get("local_sim_projection_outbox_v1") or {}).get("status") == "PROJECTED"
            )
        )
        if not succeeded_completed and not residual_completed:
            raise DataUnavailableError(
                "LocalSIM historical day has not completed durable execution",
                context={
                    "reason_code": "LOCALSIM_REPLAY_DAILY_ENGINE_NOT_COMPLETE",
                    "replay_job_id": job.replay_job_id, "trade_date": trade_date.isoformat(),
                    "status": item.status,
                },
            )


class LocalSimReplaySafeBoundaryAuthorityProtocol(Protocol):
    def evaluate(self, *, as_of_time: datetime) -> LocalSimSafeBoundaryDecisionV1: ...


class LocalSimReplaySafeBoundaryAuthority:
    """Resolve a technical pre-open boundary from durable runtime facts."""

    def __init__(
        self,
        *,
        readiness: LocalSimCutoverReadiness,
        lifecycle_scheduler: object,
        trading_calendar_service: object,
    ) -> None:
        self.readiness = readiness
        self.lifecycle_scheduler = lifecycle_scheduler
        self.trading_calendar_service = trading_calendar_service

    def evaluate(self, *, as_of_time: datetime) -> LocalSimSafeBoundaryDecisionV1:
        local_time = _aware_china_time(as_of_time)
        current_date = local_time.date()
        status = dict(self.trading_calendar_service.status(as_of_date=current_date))
        is_trading_day = status.get("is_trading_day")
        if not isinstance(is_trading_day, bool):
            raise DataUnavailableError(
                "LocalSIM replay safe-boundary calendar status is invalid",
                context={"reason_code": "LOCALSIM_REPLAY_SAFE_BOUNDARY_CALENDAR_INVALID"},
            )
        if not is_trading_day:
            market_phase = "NON_TRADING_DAY"
        elif local_time.timetz().replace(tzinfo=None) < LOCALSIM_REPLAY_PREOPEN_CUTOFF:
            market_phase = "PRE_OPEN"
        elif local_time.timetz().replace(tzinfo=None) < time(15, 30):
            market_phase = "TRADING"
        else:
            market_phase = "POST_CLOSE"
        if market_phase == "PRE_OPEN":
            activation_trade_date = current_date
        else:
            activation_trade_date = self.trading_calendar_service.next_trading_day(current_date)

        readiness = self.readiness.read()
        scheduler_status = dict(self.lifecycle_scheduler.status())
        watchdog = scheduler_status.get("binding_watchdog")
        binding_tick_count = int(watchdog.get("in_flight_count") or 0) if isinstance(watchdog, dict) else 0
        writer_claim_available = bool(readiness.ready and binding_tick_count == 0)
        eligible = bool(
            market_phase == "PRE_OPEN"
            and activation_trade_date == current_date
            and readiness.in_flight_economic_run_count == 0
            and writer_claim_available
        )
        if not readiness.ready:
            reason_code = "LOCALSIM_REPLAY_CUTOVER_NOT_READY"
        elif binding_tick_count:
            reason_code = "LOCALSIM_REPLAY_WRITER_BUSY"
        elif readiness.in_flight_economic_run_count:
            reason_code = "LOCALSIM_REPLAY_ECONOMIC_RUN_IN_FLIGHT"
        elif market_phase != "PRE_OPEN":
            reason_code = "LOCALSIM_REPLAY_WAITING_FOR_PREOPEN_BOUNDARY"
        else:
            reason_code = "LOCALSIM_REPLAY_PREOPEN_SAFE_BOUNDARY"
        return LocalSimSafeBoundaryDecisionV1(
            eligible=eligible,
            evaluated_at=local_time.astimezone(UTC),
            current_trading_date=current_date,
            activation_trade_date=activation_trade_date,
            market_phase=market_phase,
            in_flight_economic_transactions=readiness.in_flight_economic_run_count,
            writer_claim_available=writer_claim_available,
            historical_provider_closed=True,
            reason_code=reason_code,
        )


class LocalSimReplayLifecycleOwner:
    """Advance replay cursors and perform crash-safe replay-to-live activation."""

    _ACTIVE_STATUSES = frozenset(
        {
            LocalSimReplayStatus.CREATED,
            LocalSimReplayStatus.RUNNING_HISTORICAL,
            LocalSimReplayStatus.FAILED_RETRYABLE,
            LocalSimReplayStatus.CAUGHT_UP,
            LocalSimReplayStatus.READY_FOR_LIVE,
            LocalSimReplayStatus.ACTIVATION_PENDING_SAFE_BOUNDARY,
        }
    )

    def __init__(
        self,
        *,
        repository: LocalSimSuccessorRepositoryProtocol,
        control: LocalSimControlPlaneService,
        product_authority: LocalSimProductAuthority,
        historical_source_authority: LocalSimHistoricalSourceAuthorityProtocol,
        historical_day_executor: Callable[[LocalSimReplayJobV1, date], None],
        safe_boundary_authority: LocalSimReplaySafeBoundaryAuthorityProtocol,
        max_jobs_per_tick: int = 4,
        max_days_per_job: int = 5,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if max_jobs_per_tick <= 0 or max_days_per_job <= 0:
            raise ValueError("LocalSIM replay lifecycle bounds must be positive")
        self.repository = repository
        self.control = control
        self.product_authority = product_authority
        self.historical_source_authority = historical_source_authority
        self.historical_day_executor = historical_day_executor
        self.safe_boundary_authority = safe_boundary_authority
        self.max_jobs_per_tick = max_jobs_per_tick
        self.max_days_per_job = max_days_per_job
        self.clock = clock or (lambda: datetime.now(CHINA_TZ))

    def tick(self, *, as_of_time: datetime | None = None) -> dict[str, Any]:
        now = _aware_china_time(as_of_time or self.clock())
        all_jobs = self.repository.list_replay_jobs(limit=200)
        # A cancelled or failed catch-up child also owns the lineage: never reactivate its predecessor.
        superseded_ids = {
            (self.repository.get_release(job.release_id).release_config_json.get("metadata") or {}).get(
                "localsim_replay_predecessor_id"
            )
            for job in all_jobs
        }
        jobs = [
            job
            for job in all_jobs
            if job.replay_job_id not in superseded_ids
            and self.repository.get_account(job.simulation_account_id).status is SimulationAccountStatus.ACTIVE
            and (
                job.status in self._ACTIVE_STATUSES
                or LocalSimReplayCoordinator.is_recoverable_selection_timeout(job)
                or LocalSimReplayCoordinator.is_recoverable_historical_submit_window(job)
            )
        ][: self.max_jobs_per_tick]
        outcomes: list[dict[str, Any]] = []
        for job in jobs:
            try:
                updated = self._advance(job=job, as_of_time=now)
                outcomes.append(
                    {
                        "replay_job_id": job.replay_job_id,
                        "successor_replay_job_id": (
                            updated.replay_job_id if updated.replay_job_id != job.replay_job_id else None
                        ),
                        "before_status": job.status.value,
                        "status": updated.status.value,
                        "version": updated.version,
                        "completed_trade_date": (
                            updated.completed_trade_date.isoformat() if updated.completed_trade_date else None
                        ),
                        "next_trade_date": updated.next_trade_date.isoformat() if updated.next_trade_date else None,
                        "activation_trade_date": (
                            updated.activation_trade_date.isoformat() if updated.activation_trade_date else None
                        ),
                    }
                )
            except Exception as exc:  # every job exposes its own durable failure without starving peers
                current = self.repository.get_replay_job(job.replay_job_id)
                outcomes.append(
                    {
                        "replay_job_id": job.replay_job_id,
                        "before_status": job.status.value,
                        "status": current.status.value,
                        "version": current.version,
                        "error": {
                            "type": type(exc).__name__,
                            "message": str(exc)[:1000],
                            "context": getattr(exc, "context", None),
                        },
                    }
                )
        return {
            "schema_version": "localsim_replay_lifecycle_tick_v1",
            "evaluated_at": now.isoformat(),
            "max_jobs_per_tick": self.max_jobs_per_tick,
            "max_days_per_job": self.max_days_per_job,
            "eligible_job_count": len(jobs),
            "outcomes": outcomes,
        }

    def _advance(self, *, job: LocalSimReplayJobV1, as_of_time: datetime) -> LocalSimReplayJobV1:
        current = self.repository.get_replay_job(job.replay_job_id)
        source = self.historical_source_authority.resolve(
            historical_source_id=current.historical_source_id,
            start_trade_date=current.start_trade_date,
            end_trade_date=current.end_trade_date,
            require_latest_completed=False,
        )
        if (
            source.historical_source_sha256 != current.historical_source_sha256
            or local_sim_calendar_snapshot_sha256(source.trading_days) != current.calendar_snapshot_sha256
        ):
            raise InvalidStateTransitionError(
                "LocalSIM replay frozen source or calendar changed before continuation",
                context={"reason_code": "LOCALSIM_REPLAY_SOURCE_OR_CALENDAR_MISMATCH"},
            )
        if LocalSimReplayCoordinator.is_recoverable_selection_timeout(current):
            current = self._coordinator_for_job(current).recover_selection_timeout(
                replay_job_id=current.replay_job_id, expected_version=current.version,
            )
        elif LocalSimReplayCoordinator.is_recoverable_historical_submit_window(current):
            current = self._coordinator_for_job(current).recover_historical_submit_window(
                replay_job_id=current.replay_job_id, expected_version=current.version,
            )
        if current.status in {
            LocalSimReplayStatus.CREATED,
            LocalSimReplayStatus.RUNNING_HISTORICAL,
            LocalSimReplayStatus.FAILED_RETRYABLE,
        }:
            runner = LocalSimHistoricalDayRunner(
                historical_source_id=source.historical_source_id,
                historical_source_sha256=source.historical_source_sha256,
                run_day=self.historical_day_executor,
            )
            current = LocalSimReplayCoordinator(
                repository=self.repository,
                historical_day_runner=runner,
            ).run_next_batch(
                replay_job_id=current.replay_job_id,
                expected_version=current.version,
                trading_days=source.trading_days,
                current_trading_date=source.current_trading_date,
                max_days=self.max_days_per_job,
            )
        if current.status in {LocalSimReplayStatus.CAUGHT_UP, LocalSimReplayStatus.READY_FOR_LIVE}:
            if current.completed_trade_date != current.end_trade_date or current.next_trade_date is not None:
                raise InvalidStateTransitionError(
                    "LocalSIM replay cannot switch with an incomplete historical cursor",
                    context={"reason_code": "LOCALSIM_REPLAY_HISTORY_INCOMPLETE"},
                )
            if source.latest_completed_trade_date > current.end_trade_date:
                return self._create_catchup_successor(
                    job=current, latest_completed_trade_date=source.latest_completed_trade_date,
                    as_of_time=as_of_time,
                )
        if current.status is LocalSimReplayStatus.CAUGHT_UP:
            current = self._coordinator_for_job(current).mark_ready_for_live(
                replay_job_id=current.replay_job_id,
                expected_version=current.version,
            )
        if current.status is LocalSimReplayStatus.READY_FOR_LIVE:
            decision = self.safe_boundary_authority.evaluate(as_of_time=as_of_time)
            if not decision.eligible:
                return current
            if decision.activation_trade_date != source.current_trading_date:
                raise InvalidStateTransitionError(
                    "LocalSIM replay safe boundary is not the caught-up current trading date",
                    context={"reason_code": "LOCALSIM_REPLAY_ACTIVATION_DATE_DRIFT"},
                )
            account, _base_binding, release, binding = self._build_successor_bundle(
                job=current,
                effective_from=decision.activation_trade_date,
            )
            if account.account_id != current.simulation_account_id:
                raise InvalidStateTransitionError(
                    "LocalSIM replay live account identity drifted",
                    context={"reason_code": "LOCALSIM_REPLAY_LIVE_ACCOUNT_IDENTITY_DRIFT"},
                )
            current = self._coordinator_for_job(current).activate_live_successor_atomic(
                replay_job_id=current.replay_job_id,
                expected_version=current.version,
                release=release,
                binding=binding,
                decision=decision,
            )
        elif current.status is LocalSimReplayStatus.ACTIVATION_PENDING_SAFE_BOUNDARY:
            if source.latest_completed_trade_date != current.completed_trade_date:
                raise InvalidStateTransitionError(
                    "LocalSIM prepared live successor cannot activate with an unfilled historical gap",
                    context={"reason_code": "LOCALSIM_REPLAY_LIVE_ACTIVATION_HISTORY_GAP"},
                )
            decision = self.safe_boundary_authority.evaluate(as_of_time=as_of_time)
            if decision.eligible and current.activation_trade_date == decision.activation_trade_date:
                current = self._coordinator_for_job(current).activate_live(
                    replay_job_id=current.replay_job_id,
                    expected_version=current.version,
                    decision=decision,
                )
        return current

    def _create_catchup_successor(
        self, *, job: LocalSimReplayJobV1, latest_completed_trade_date: date, as_of_time: datetime,
    ) -> LocalSimReplayJobV1:
        source = self.historical_source_authority.resolve(
            historical_source_id=job.historical_source_id,
            start_trade_date=job.start_trade_date,
            end_trade_date=latest_completed_trade_date,
        )
        days = tuple(day for day in source.trading_days if day > job.end_trade_date)
        if (
            not days or source.historical_source_sha256 != job.historical_source_sha256
            or local_sim_calendar_snapshot_sha256(tuple(day for day in source.trading_days if day <= job.end_trade_date))
            != job.calendar_snapshot_sha256
        ):
            raise InvalidStateTransitionError(
                "LocalSIM catch-up successor source or calendar is invalid",
                context={"reason_code": "LOCALSIM_REPLAY_CATCHUP_SOURCE_MISMATCH"},
            )
        account, _base_binding, release, binding = self._build_successor_bundle(
            job=job, effective_from=days[0], effective_to=days[-1],
        )
        successor = self._coordinator_for_job(job).build_job(
            account=account, release=release, binding=binding,
            start_trade_date=days[0], end_trade_date=days[-1],
            historical_source_id=source.historical_source_id,
            historical_source_sha256=source.historical_source_sha256,
            trading_days=days, created_by="localsim_replay_lifecycle_owner",
        )
        return self.repository.create_replay_catchup_successor(
            replay_job_id=job.replay_job_id, expected_version=job.version,
            release=release, binding=binding, successor_job=successor, updated_at=as_of_time,
        )

    def _build_successor_bundle(
        self, *, job: LocalSimReplayJobV1, effective_from: date, effective_to: date | None = None,
    ):
        account = self.repository.get_account(job.simulation_account_id)
        historical_release = self.repository.get_release(job.release_id)
        resolved = self.product_authority.resolve_product(
            package_id=account.package_id,
            runtime_profile_version_id=historical_release.runtime_profile_version_id,
            execution_policy_version_id=historical_release.execution_policy_version_id,
        )
        metadata = {"localsim_runtime_profile_config": resolved.runtime_profile_version.config_json}
        if effective_to is not None:
            metadata["localsim_replay_predecessor_id"] = job.replay_job_id
        return self.control.build_successor_release_bundle(
            account_id=account.account_id,
            base_release_id=job.release_id,
            base_binding_id=job.binding_id,
            runtime_profile_id=resolved.runtime_profile.profile_id,
            runtime_profile_version_id=resolved.runtime_profile_version.profile_version_id,
            runtime_profile_sha256=resolved.runtime_profile_version.config_sha256,
            daily_strategy_profile_version_id=resolved.runtime_profile_version.daily_strategy_profile_version_id,
            execution_policy_version_id=resolved.execution_policy.policy_id,
            execution_policy_sha256=str(resolved.execution_policy.policy_sha256),
            execution_policy_json=resolved.execution_policy.policy_json,
            tail_policy_version_id=resolved.tail_policy_version_id,
            tail_policy_sha256=resolved.tail_policy_sha256,
            release_validation_evidence=resolved.release_validation_evidence(),
            release_metadata=metadata,
            effective_from=effective_from,
            effective_to=effective_to,
            created_by="localsim_replay_lifecycle_owner",
            created_reason=(
                "automatic_completed_day_catchup" if effective_to is not None
                else "automatic_replay_to_live_transition"
            ),
        )

    def _coordinator_for_job(self, job: LocalSimReplayJobV1) -> LocalSimReplayCoordinator:
        return LocalSimReplayCoordinator(
            repository=self.repository,
            historical_day_runner=LocalSimHistoricalDayRunner(
                historical_source_id=job.historical_source_id,
                historical_source_sha256=job.historical_source_sha256,
                run_day=self.historical_day_executor,
            ),
        )


def _aware_china_time(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DataUnavailableError(
            "LocalSIM replay lifecycle clock must be timezone-aware",
            context={"reason_code": "LOCALSIM_REPLAY_LIFECYCLE_CLOCK_INVALID"},
        )
    return value.astimezone(CHINA_TZ)
