"""Automatic lifecycle owner for durable LocalSIM historical replays."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime, time
import hashlib
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .localsim_control import LocalSimControlPlaneService
from .localsim_file_inputs import (
    FILE_BINDING_KEY, FILE_SOURCE_ID, frozen_file_binding, localsim_file_inputs, resolve_replay_source,
)
from .localsim_cutover_readiness import LocalSimCutoverReadiness
from .localsim_product_authority import LocalSimProductAuthority
from .localsim_product_control import LocalSimHistoricalSourceAuthorityProtocol
from .localsim_replay import (
    LocalSimHistoricalDayRunner, LocalSimReplayCoordinator, local_sim_calendar_snapshot_sha256,
)
from .models import SimulationBindingApprovalState, SimulationBrokerBackend, SimulationDailyRunStatus
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
        from .localsim_input_source_plan import SOURCE_PLAN_ID, frozen_source_plan, localsim_plan_inputs
        if job.historical_source_id == SOURCE_PLAN_ID:
            release = self.lifecycle_scheduler.repository.get_strategy_runtime_release(job.release_id)
            with localsim_plan_inputs(frozen_source_plan(release, job=job), trade_date=trade_date) as route:
                return self._run_day(job, trade_date, "DATASET_FILES" if route["source"] == "DATASET_FILES" else "DB_HISTORICAL")
        context = nullcontext()
        source = "DB_HISTORICAL"
        if job.historical_source_id == FILE_SOURCE_ID:
            release = self.lifecycle_scheduler.repository.get_strategy_runtime_release(job.release_id)
            binding = frozen_file_binding(release)
            if binding is None or binding["binding_hash"] != job.historical_source_sha256:
                raise DataUnavailableError("LocalSIM replay frozen file source mismatch")
            context = localsim_file_inputs(binding, run_id=f"{job.replay_job_id}:{trade_date.isoformat()}")
            source = "DATASET_FILES"
        with context:
            return self._run_day(job, trade_date, source)

    def _run_day(self, job, trade_date, source):
        as_of_time = datetime.combine(trade_date, time(15, 30), tzinfo=ZoneInfo("Asia/Shanghai"))
        result = self.lifecycle_scheduler.run_once(
            trade_date=trade_date,
            data_source=source,
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
            error = item.error if isinstance(item.error, dict) else {}
            error_context = error.get("context") if isinstance(error.get("context"), dict) else {}
            retry_backoff = (
                item.status == "RETRY_BACKOFF"
                and error.get("schema_version") == "simulation_binding_retry_backoff_v1"
                and error.get("reason_code") == "SIMULATION_BINDING_RETRY_BACKOFF_NOT_DUE"
            )
            if (
                run is not None and run.status is SimulationDailyRunStatus.FAILED_RETRYABLE
                and payload.get("broker_side_effect_state") != "UNKNOWN"
                and error_context.get("broker_side_effect_state") != "UNKNOWN"
                and (error.get("type") == "DataUnavailableError" or retry_backoff)
            ):
                raise DataUnavailableError(
                    "LocalSIM replay daily engine dependency is not ready",
                    context={
                        **error_context,
                        "reason_code": error_context.get("reason_code") or error.get("reason_code")
                        or "LOCALSIM_REPLAY_DAILY_DATA_UNAVAILABLE",
                        "replay_job_id": job.replay_job_id, "trade_date": trade_date.isoformat(),
                        "binding_id": job.binding_id, "status": item.status, "error": item.error,
                    },
                )
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
        self.lifecycle_scheduler.complete_localsim_replay_close(
            run_id=run.run_id, trade_date=trade_date, strategy_id=job.simulation_account_id,
            binding_id=job.binding_id, release_id=job.release_id,
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
        if self.repository.get_binding(current.binding_id).approval_state is SimulationBindingApprovalState.RETIRED:
            raise InvalidStateTransitionError("LocalSIM replay binding has been retired by an explicit successor",
                context={"reason_code": "LOCALSIM_REPLAY_BINDING_RETIRED"})
        source = resolve_replay_source(self.historical_source_authority, job=current,
                                       release=self.repository.get_release(current.release_id))
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
        from .localsim_input_source_plan import PLAN_KEY, SOURCE_PLAN_ID
        if job.historical_source_id == SOURCE_PLAN_ID:
            days = tuple(self.historical_source_authority.calendar.list_trading_days(
                job.end_trade_date, latest_completed_trade_date))[1:]
            if not days or days[-1] != latest_completed_trade_date:
                raise DataUnavailableError("LocalSIM source-plan catch-up calendar is incomplete")
            account, _base, release, binding = self._build_successor_bundle(
                job=job, effective_from=days[0], effective_to=days[-1], planned_days=days)
            plan = release.release_config_json["metadata"][PLAN_KEY]
            coordinator = LocalSimReplayCoordinator(repository=self.repository,
                historical_day_runner=LocalSimHistoricalDayRunner(historical_source_id=SOURCE_PLAN_ID,
                    historical_source_sha256=plan["plan_hash"], run_day=self.historical_day_executor))
            successor = coordinator.build_job(account=account, release=release, binding=binding,
                start_trade_date=days[0], end_trade_date=days[-1], historical_source_id=SOURCE_PLAN_ID,
                historical_source_sha256=plan["plan_hash"], trading_days=days,
                created_by="localsim_replay_lifecycle_owner")
            return self.repository.create_replay_catchup_successor(replay_job_id=job.replay_job_id,
                expected_version=job.version, release=release, binding=binding, successor_job=successor, updated_at=as_of_time)
        source = resolve_replay_source(
            self.historical_source_authority, job=job, release=self.repository.get_release(job.release_id),
            end_trade_date=latest_completed_trade_date,
            require_latest_completed=True,
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
        self, *, job: LocalSimReplayJobV1, effective_from: date, effective_to: date | None = None, planned_days=None,
    ):
        account = self.repository.get_account(job.simulation_account_id)
        historical_release = self.repository.get_release(job.release_id)
        # An explicitly migrated effective PIT policy is separate from the
        # immutable source profile/model provenance. Carry it into each native
        # catch-up/live release instead of resolving the old policy again.
        from .localsim_pit_migration import MIGRATION_KEY, POLICY_KEY, build_pit_successor

        historical_metadata = historical_release.release_config_json.get("metadata") or {}
        def source_metadata(metadata):
            from .localsim_input_source_plan import (
                PLAN_KEY, REQUEST_KEY, freeze_replay_plan, freeze_source_calendar, frozen_source_plan,
            )
            metadata = dict(metadata)
            metadata.pop(REQUEST_KEY, None)  # A catch-up is not a retry of account creation.
            if effective_to is None:
                metadata.pop(PLAN_KEY, None)
            elif planned_days is not None:
                previous = frozen_source_plan(historical_release, job=job)
                metadata[PLAN_KEY] = freeze_replay_plan(mode=previous["mode"],
                    run_id="lsinput_" + hashlib.sha256(
                        f"{job.replay_job_id}:{effective_from}:{effective_to}".encode()).hexdigest()[:32],
                    manifest=self.product_authority.package_repository.get(account.package_id).current_manifest(),
                    runtime_config=metadata["selection_runtime_config"],
                    calendar=freeze_source_calendar(self.historical_source_authority.calendar, effective_to),
                    trading_days=planned_days)
            return metadata
        if POLICY_KEY in historical_metadata:
            base_binding = self.repository.get_binding(job.binding_id)
            release, binding = build_pit_successor(
                base=historical_release, binding=base_binding, start=effective_from, end=effective_to,
                resolver=self.control.pit_authority_resolver,
                package_lifecycle_reader=getattr(self.control, "package_lifecycle_reader", None),
                evidence={"schema_version": "localsim_runtime_pit_continuation_v1",
                          "source_release_id": historical_release.release_id,
                          "origin_plan_sha256": (historical_metadata.get(MIGRATION_KEY, {}).get("plan_sha256")
                              or historical_metadata.get(MIGRATION_KEY, {}).get("origin_plan_sha256"))},
                predecessor_id=job.replay_job_id if effective_to is not None else None,
                metadata_transform=source_metadata,
            )
            return account, base_binding, release, binding
        resolved = self.product_authority.resolve_product(
            package_id=account.package_id,
            runtime_profile_version_id=historical_release.runtime_profile_version_id,
            execution_policy_version_id=historical_release.execution_policy_version_id,
        )
        metadata = {"localsim_runtime_profile_config": resolved.runtime_profile_version.config_json}
        if effective_to is not None:
            metadata["localsim_replay_predecessor_id"] = job.replay_job_id
            file_binding = frozen_file_binding(historical_release)
            if file_binding is not None:
                metadata[FILE_BINDING_KEY] = file_binding
        kwargs = dict(
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
        bundle = self.control.build_successor_release_bundle(**kwargs)
        if planned_days is not None:
            from .localsim_input_source_plan import frozen_source_plan
            kwargs["release_metadata"] = source_metadata(bundle[2].release_config_json["metadata"])
            bundle = self.control.build_successor_release_bundle(**kwargs)
            frozen_source_plan(bundle[2])
        return bundle

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
