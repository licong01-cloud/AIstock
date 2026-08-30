"""Durable isolated historical replay and safe-boundary live transition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Callable, Sequence

from backend.services.simulation_data.daily_context import SimulationBrokerBackend
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .models import SimulationReleaseBinding, StrategyRuntimeRelease, canonical_json_sha256
from .successor_models import (
    LOCALSIM_DAILY_ENGINE_CONTRACT,
    LOCALSIM_REPLAY_JOB_SCHEMA,
    LocalSimReplayJobV1,
    LocalSimReplayStatus,
    LocalSimSafeBoundaryDecisionV1,
    SimulationAccountStatus,
)
from .successor_repository import LocalSimSuccessorRepositoryProtocol


HistoricalDayCallback = Callable[[LocalSimReplayJobV1, date], None]
Clock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class LocalSimHistoricalDayRunner:
    historical_source_id: str
    historical_source_sha256: str
    run_day: HistoricalDayCallback
    day_engine_contract_id: str = LOCALSIM_DAILY_ENGINE_CONTRACT

    def __post_init__(self) -> None:
        source_id = str(self.historical_source_id or "").strip()
        source_sha256 = str(self.historical_source_sha256 or "").strip().lower()
        if not source_id:
            raise ValueError("historical_source_id is required")
        if len(source_sha256) != 64 or any(character not in "0123456789abcdef" for character in source_sha256):
            raise ValueError("historical_source_sha256 must be a lowercase SHA-256 digest")
        if self.day_engine_contract_id != LOCALSIM_DAILY_ENGINE_CONTRACT:
            raise ValueError("historical runner must use the unified simulation daily engine contract")
        object.__setattr__(self, "historical_source_id", source_id)
        object.__setattr__(self, "historical_source_sha256", source_sha256)

    def __call__(self, job: LocalSimReplayJobV1, trade_date: date) -> None:
        self.run_day(job, trade_date)


def local_sim_calendar_snapshot_sha256(trading_days: Sequence[date]) -> str:
    normalized = [value.isoformat() for value in trading_days]
    if normalized != sorted(set(normalized)):
        raise InvalidStateTransitionError(
            "LocalSIM replay calendar snapshot must be strictly increasing and unique",
            context={"reason_code": "LOCALSIM_REPLAY_CALENDAR_INVALID"},
        )
    return canonical_json_sha256(normalized)


class LocalSimReplayCoordinator:
    """Internal coordinator; product routes remain intentionally absent until SIM-LR-C."""

    def __init__(
        self,
        *,
        repository: LocalSimSuccessorRepositoryProtocol,
        historical_day_runner: LocalSimHistoricalDayRunner,
        clock: Clock = _utc_now,
    ) -> None:
        self.repository = repository
        self.historical_day_runner = historical_day_runner
        self.clock = clock

    def create_job(
        self,
        *,
        simulation_account_id: str,
        release_id: str,
        binding_id: str,
        start_trade_date: date,
        end_trade_date: date,
        historical_source_id: str,
        historical_source_sha256: str,
        trading_days: Sequence[date],
        created_by: str,
    ) -> LocalSimReplayJobV1:
        account = self.repository.get_account(simulation_account_id)
        release = self.repository.get_release(release_id)
        binding = self.repository.get_binding(binding_id)
        if (
            self.historical_day_runner.historical_source_id != str(historical_source_id).strip()
            or self.historical_day_runner.historical_source_sha256 != str(historical_source_sha256).strip().lower()
        ):
            raise InvalidStateTransitionError(
                "LocalSIM replay runner does not match the frozen historical source",
                context={"reason_code": "LOCALSIM_REPLAY_SOURCE_MISMATCH"},
            )
        if account.status is SimulationAccountStatus.RETIRED:
            raise InvalidStateTransitionError(
                "retired LocalSIM account cannot start a replay",
                context={"reason_code": "LOCALSIM_REPLAY_ACCOUNT_RETIRED"},
            )
        metadata = binding.binding_config_json.get("metadata")
        if (
            binding.broker_backend is not SimulationBrokerBackend.LOCAL_SIM
            or binding.broker_account_id != account.account_id
            or binding.release_id != release.release_id
            or binding.effective_to != end_trade_date
            or release.package_id != account.package_id
            or not isinstance(metadata, dict)
            or metadata.get("localsim_account_id") != account.account_id
        ):
            raise InvalidStateTransitionError(
                "LocalSIM replay requires its own closed historical account, release, binding, and writer scope",
                context={"reason_code": "LOCALSIM_REPLAY_SCOPE_MISMATCH"},
            )
        days = self._bounded_calendar(
            trading_days=trading_days,
            start_trade_date=start_trade_date,
            end_trade_date=end_trade_date,
        )
        calendar_sha256 = local_sim_calendar_snapshot_sha256(days)
        identity = {
            "schema_version": LOCALSIM_REPLAY_JOB_SCHEMA,
            "simulation_account_id": account.account_id,
            "release_id": release.release_id,
            "binding_id": binding.binding_id,
            "day_engine_contract_id": self.historical_day_runner.day_engine_contract_id,
            "start_trade_date": start_trade_date.isoformat(),
            "end_trade_date": end_trade_date.isoformat(),
            "historical_source_id": str(historical_source_id).strip(),
            "historical_source_sha256": str(historical_source_sha256).strip(),
            "calendar_snapshot_sha256": calendar_sha256,
        }
        replay_hash = canonical_json_sha256(identity)
        now = self._now()
        job = LocalSimReplayJobV1(
            replay_job_id=f"lsreplay_{replay_hash[:16]}",
            replay_hash=replay_hash,
            simulation_account_id=account.account_id,
            release_id=release.release_id,
            binding_id=binding.binding_id,
            day_engine_contract_id=self.historical_day_runner.day_engine_contract_id,
            start_trade_date=start_trade_date,
            end_trade_date=end_trade_date,
            historical_source_id=identity["historical_source_id"],
            historical_source_sha256=identity["historical_source_sha256"],
            calendar_snapshot_sha256=calendar_sha256,
            next_trade_date=days[0],
            created_by=created_by,
            created_at=now,
            updated_at=now,
        )
        return self.repository.save_replay_job(job)

    def run_next_batch(
        self,
        *,
        replay_job_id: str,
        expected_version: int,
        trading_days: Sequence[date],
        current_trading_date: date,
        max_days: int,
    ) -> LocalSimReplayJobV1:
        if max_days <= 0:
            raise InvalidStateTransitionError(
                "LocalSIM replay batch size must be positive",
                context={"reason_code": "LOCALSIM_REPLAY_BATCH_INVALID"},
            )
        job = self.repository.get_replay_job(replay_job_id)
        if job.version != expected_version:
            raise InvalidStateTransitionError(
                "LocalSIM replay job CAS failed",
                context={"reason_code": "LOCALSIM_REPLAY_CAS_CONFLICT"},
            )
        if job.status not in {
            LocalSimReplayStatus.CREATED,
            LocalSimReplayStatus.RUNNING_HISTORICAL,
            LocalSimReplayStatus.FAILED_RETRYABLE,
        }:
            raise InvalidStateTransitionError(
                "LocalSIM replay cannot execute historical days from its current state",
                context={
                    "reason_code": "LOCALSIM_REPLAY_TRANSITION_INVALID",
                    "status": job.status.value,
                },
            )
        days = self._bounded_calendar(
            trading_days=trading_days,
            start_trade_date=job.start_trade_date,
            end_trade_date=job.end_trade_date,
        )
        if local_sim_calendar_snapshot_sha256(days) != job.calendar_snapshot_sha256:
            raise InvalidStateTransitionError(
                "LocalSIM replay calendar snapshot changed after job creation",
                context={"reason_code": "LOCALSIM_REPLAY_CALENDAR_HASH_MISMATCH"},
            )
        if (
            self.historical_day_runner.historical_source_id != job.historical_source_id
            or self.historical_day_runner.historical_source_sha256 != job.historical_source_sha256
            or self.historical_day_runner.day_engine_contract_id != job.day_engine_contract_id
        ):
            raise InvalidStateTransitionError(
                "LocalSIM replay runner identity changed after restart",
                context={"reason_code": "LOCALSIM_REPLAY_SOURCE_MISMATCH"},
            )
        if days[-1] >= current_trading_date:
            raise InvalidStateTransitionError(
                "LocalSIM historical replay cannot consume the current or future trading date",
                context={"reason_code": "LOCALSIM_REPLAY_CURRENT_DAY_SOURCE_FORBIDDEN"},
            )
        start_index = days.index(job.next_trade_date) if job.next_trade_date is not None else len(days)
        current = job
        if current.status is not LocalSimReplayStatus.RUNNING_HISTORICAL:
            current = self.repository.transition_replay_job(
                replay_job_id=current.replay_job_id,
                expected_version=current.version,
                update={
                    "status": LocalSimReplayStatus.RUNNING_HISTORICAL,
                    "failure_code": None,
                    "failure_context": None,
                },
                updated_at=self._now(),
            )
        for trade_date in days[start_index : start_index + max_days]:
            try:
                self.historical_day_runner(current, trade_date)
            except Exception as exc:
                retryable = isinstance(exc, DataUnavailableError)
                self.repository.transition_replay_job(
                    replay_job_id=current.replay_job_id,
                    expected_version=current.version,
                    update={
                        "status": (
                            LocalSimReplayStatus.FAILED_RETRYABLE if retryable else LocalSimReplayStatus.FAILED_TERMINAL
                        ),
                        "failure_code": type(exc).__name__,
                        "failure_context": {"message": str(exc)[:1000], "trade_date": trade_date.isoformat()},
                    },
                    updated_at=self._now(),
                )
                raise
            index = days.index(trade_date)
            next_trade_date = days[index + 1] if index + 1 < len(days) else None
            current = self.repository.transition_replay_job(
                replay_job_id=current.replay_job_id,
                expected_version=current.version,
                update={
                    "status": (
                        LocalSimReplayStatus.RUNNING_HISTORICAL
                        if next_trade_date is not None
                        else LocalSimReplayStatus.CAUGHT_UP
                    ),
                    "completed_trade_date": trade_date,
                    "next_trade_date": next_trade_date,
                },
                updated_at=self._now(),
            )
        return current

    def mark_ready_for_live(self, *, replay_job_id: str, expected_version: int) -> LocalSimReplayJobV1:
        job = self.repository.get_replay_job(replay_job_id)
        if job.version != expected_version or job.status is not LocalSimReplayStatus.CAUGHT_UP:
            raise InvalidStateTransitionError(
                "LocalSIM replay is not caught up for live preparation",
                context={"reason_code": "LOCALSIM_REPLAY_READY_STATE_INVALID"},
            )
        return self.repository.transition_replay_job(
            replay_job_id=replay_job_id,
            expected_version=expected_version,
            update={"status": LocalSimReplayStatus.READY_FOR_LIVE},
            updated_at=self._now(),
        )

    def prepare_live_successor(
        self,
        *,
        replay_job_id: str,
        expected_version: int,
        release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        decision: LocalSimSafeBoundaryDecisionV1,
    ) -> LocalSimReplayJobV1:
        job = self.repository.get_replay_job(replay_job_id)
        if job.version != expected_version or job.status is not LocalSimReplayStatus.READY_FOR_LIVE:
            raise InvalidStateTransitionError(
                "LocalSIM replay is not ready for an atomic live successor",
                context={"reason_code": "LOCALSIM_REPLAY_LIVE_SUCCESSOR_STATE_INVALID"},
            )
        if not decision.eligible:
            raise InvalidStateTransitionError(
                "LocalSIM replay live successor requires an eligible safe boundary",
                context={"reason_code": decision.reason_code},
            )
        account = self.repository.get_account(job.simulation_account_id)
        if release.base_release_id != job.release_id:
            raise InvalidStateTransitionError(
                "LocalSIM replay live release must be a successor of the historical release",
                context={"reason_code": "LOCALSIM_REPLAY_LIVE_RELEASE_LINEAGE_MISMATCH"},
            )
        _release, _binding, updated = self.repository.create_replay_live_successor(
            replay_job_id=replay_job_id,
            expected_version=expected_version,
            account=account,
            release=release,
            binding=binding,
            activation_trade_date=decision.activation_trade_date,
            updated_at=self._now(),
        )
        return updated

    def activate_live(
        self,
        *,
        replay_job_id: str,
        expected_version: int,
        decision: LocalSimSafeBoundaryDecisionV1,
    ) -> LocalSimReplayJobV1:
        job = self.repository.get_replay_job(replay_job_id)
        if (
            job.version != expected_version
            or job.status is not LocalSimReplayStatus.ACTIVATION_PENDING_SAFE_BOUNDARY
            or job.activation_trade_date != decision.activation_trade_date
            or not decision.eligible
        ):
            raise InvalidStateTransitionError(
                "LocalSIM replay live activation is not at its durable safe boundary",
                context={"reason_code": "LOCALSIM_REPLAY_ACTIVATION_BOUNDARY_INVALID"},
            )
        return self.repository.transition_replay_job(
            replay_job_id=replay_job_id,
            expected_version=expected_version,
            update={"status": LocalSimReplayStatus.LIVE_ACTIVE},
            updated_at=self._now(),
        )

    def cancel(self, *, replay_job_id: str, expected_version: int) -> LocalSimReplayJobV1:
        job = self.repository.get_replay_job(replay_job_id)
        if job.version != expected_version or job.status in {
            LocalSimReplayStatus.LIVE_ACTIVE,
            LocalSimReplayStatus.FAILED_TERMINAL,
            LocalSimReplayStatus.CANCELLED,
        }:
            raise InvalidStateTransitionError(
                "LocalSIM replay cannot be cancelled from its current state",
                context={"reason_code": "LOCALSIM_REPLAY_CANCEL_STATE_INVALID"},
            )
        return self.repository.transition_replay_job(
            replay_job_id=replay_job_id,
            expected_version=expected_version,
            update={"status": LocalSimReplayStatus.CANCELLED},
            updated_at=self._now(),
        )

    @staticmethod
    def _bounded_calendar(*, trading_days: Sequence[date], start_trade_date: date, end_trade_date: date) -> list[date]:
        days = [value for value in trading_days if start_trade_date <= value <= end_trade_date]
        local_sim_calendar_snapshot_sha256(days)
        if not days or days[0] != start_trade_date or days[-1] != end_trade_date:
            raise InvalidStateTransitionError(
                "LocalSIM replay calendar does not cover the exact requested range",
                context={"reason_code": "LOCALSIM_REPLAY_CALENDAR_RANGE_INCOMPLETE"},
            )
        return days

    def _now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise DataUnavailableError(
                "LocalSIM replay clock must be timezone-aware",
                context={"reason_code": "LOCALSIM_REPLAY_CLOCK_INVALID"},
            )
        return value.astimezone(UTC)
