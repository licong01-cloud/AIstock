"""Ordered Phase 1R R3 historical-range execution without runtime side effects.

The service consumes one explicitly supplied sealed batch.  It does not start
processes, schedule work, create orders, or write to Selection/Paper/Simulation.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
import logging
from threading import Event, Lock, Thread
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from psycopg2 import OperationalError as PostgresOperationalError
from psycopg2 import errors as postgres_errors

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.candidate_producer import HistoricalRangeCandidateProducer
from backend.services.advisory_historical_range.decision_mark_provider import HistoricalRangeDecisionMarkProvider
from backend.services.advisory_historical_range.list_transition import HistoricalRangeListTransitionAdapter
from backend.services.advisory_historical_range.models import (
    DAY_ATTEMPT_RECEIPT_PAYLOAD_SCHEMA_VERSION,
    DAY_RECEIPT_PAYLOAD_SCHEMA_VERSION_V2,
    RUN_EXECUTION_RECEIPT_SCHEMA_VERSION,
    EXECUTION_OPERATION_ATTEMPT_RECEIPT_SCHEMA_VERSION,
    EXECUTION_OPERATION_RECEIPT_SCHEMA_VERSION,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeBatchStatus,
    HistoricalRangeClaimedDayV1,
    HistoricalRangeContractError,
    HistoricalRangeDayAttemptReceiptPayloadV1,
    HistoricalRangeDayAttemptV1,
    HistoricalRangeDayStatus,
    HistoricalRangeExecutionBatchV1,
    HistoricalRangeExecutionRunV1,
    HistoricalRangeExecutionOperationV1,
    HistoricalRangeExecutionOperationReceiptV1,
    HistoricalRangeExecutionOperationAttemptReceiptV1,
    HistoricalRangeOperationAttemptV1,
    HistoricalRangeOperationCancelledDayResultV1,
    HistoricalRangeOperationProgramResultV1,
    HistoricalRangeOperationRequestV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeOperationType,
    HistoricalRangeCandidateArtifactPayloadV2,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangePredecessorStateV1,
    HistoricalRangeProgramStatus,
    HistoricalRangeResolvedRequestArtifactPayloadV1,
    HistoricalRangeRunExecutionReceiptV1,
    build_candidate_input_hash,
    build_day_input_hash_v3,
    build_day_receipt_payload_v2,
    derive_prefixed_id,
)
from backend.services.advisory_historical_range.semantics import (
    HistoricalRangeListSemanticsV2,
    canonical_list_semantics_v2,
)
from backend.services.trading_core.errors import (
    DataUnavailableError,
    HMMRuntimeUnavailableError,
    MarketDataUnavailableError,
    PackageAssetInvalidError,
    RuntimeConfigInvalidError,
    StrategyPackageValidationError,
    TradingCalendarUnavailableError,
    TradingCoreError,
    UnsupportedFeatureError,
)


LOGGER = logging.getLogger(__name__)
EXECUTOR_CONTRACT_VERSION = "advisory_historical_range_r3_ordered_executor_v1"


@dataclass(frozen=True)
class HistoricalRangeDayExecutionResultV1:
    day_run_id: str
    status: HistoricalRangeDayStatus
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class HistoricalRangeBatchExecutionResultV1:
    batch_id: str
    executed_day_count: int
    successful_day_count: int
    waiting_day_count: int
    retryable_day_count: int
    failed_day_count: int
    blocking_day_run_ids: tuple[str, ...]


class HistoricalRangeExecutionRepository(Protocol):
    def load_execution_batch(self, *, batch_id: str) -> HistoricalRangeExecutionBatchV1: ...

    def list_execution_runs(
        self,
        *,
        batch_id: str,
        stable_after_research_program_id: str | None = None,
        limit: int = 500,
    ) -> tuple[HistoricalRangeExecutionRunV1, ...]: ...

    def materialize_day_plan_chunk(self, **kwargs: Any) -> Any: ...

    def transition_batch(self, **kwargs: Any) -> dict[str, Any]: ...

    def transition_run(self, **kwargs: Any) -> dict[str, Any]: ...

    def claim_next_day(self, **kwargs: Any) -> HistoricalRangeClaimedDayV1 | None: ...

    def load_expired_claimable_day(self, **kwargs: Any) -> HistoricalRangeClaimedDayV1 | None: ...

    def take_over_expired_day(self, **kwargs: Any) -> HistoricalRangeClaimedDayV1: ...

    def heartbeat_day(self, **kwargs: Any) -> HistoricalRangeClaimedDayV1: ...

    def load_reusable_candidate_ref(self, **kwargs: Any) -> HistoricalRangeArtifactRefV1 | None: ...

    def load_predecessor_state(self, *, day_run_id: str) -> HistoricalRangePredecessorStateV1: ...

    def load_episode_entry_sequences(self, *, range_run_id: str) -> dict[str, int]: ...

    def commit_successful_day(self, **kwargs: Any) -> Any: ...

    def finish_failed_day(self, **kwargs: Any) -> Any: ...

    def full_readback_successful_day(self, **kwargs: Any) -> Any: ...

    def load_run_finalization_facts(self, **kwargs: Any) -> Any: ...

    def finish_range_run(self, **kwargs: Any) -> Any: ...

    def get_or_create_operation(self, request: HistoricalRangeOperationRequestV1) -> tuple[dict[str, Any], bool]: ...

    def load_execution_operation(self, **kwargs: Any) -> HistoricalRangeExecutionOperationV1: ...

    def claim_execution_operation(self, **kwargs: Any) -> HistoricalRangeExecutionOperationV1: ...

    def finish_execution_operation(self, **kwargs: Any) -> HistoricalRangeExecutionOperationV1: ...

    def heartbeat_execution_operation(self, **kwargs: Any) -> HistoricalRangeExecutionOperationV1: ...

    def finish_execution_operation_failure(self, **kwargs: Any) -> HistoricalRangeExecutionOperationV1: ...

    def list_operation_attempt_receipt_refs(self, **kwargs: Any) -> tuple[HistoricalRangeArtifactRefV1, ...]: ...

    def load_cancellation_day_contexts(self, **kwargs: Any) -> tuple[Any, ...]: ...

    def load_cancelled_day_results(
        self, **kwargs: Any
    ) -> tuple[HistoricalRangeOperationCancelledDayResultV1, ...]: ...

    def cancel_execution_batch(self, **kwargs: Any) -> tuple[str, ...]: ...


class HistoricalRangeDayExecutor:
    """Execute a bounded set of ordered day claims from one sealed batch."""

    def __init__(
        self,
        *,
        repository: HistoricalRangeExecutionRepository,
        artifact_store: HistoricalRangeArtifactStore,
        candidate_producer: HistoricalRangeCandidateProducer,
        decision_mark_provider: HistoricalRangeDecisionMarkProvider,
        list_transition_adapter: HistoricalRangeListTransitionAdapter | None = None,
    ) -> None:
        if any(item is None for item in (repository, artifact_store, candidate_producer, decision_mark_provider)):
            raise ValueError("R3 day executor requires explicit repository, artifact, candidate, and mark dependencies")
        self._repository = repository
        self._artifact_store = artifact_store
        self._candidate_producer = candidate_producer
        self._decision_mark_provider = decision_mark_provider
        self._list_transition_adapter = list_transition_adapter or HistoricalRangeListTransitionAdapter()
        self._prefetch_lock = Lock()
        self._prefetched_candidates: dict[tuple[str, date], HistoricalRangeArtifactRefV1] = {}

    def execute_batch_slice(
        self,
        *,
        batch_id: str,
        worker_id: str,
        max_program_concurrency: int = 2,
        candidate_prefetch_per_program: int = 2,
        max_day_commits_per_slice: int = 4,
        lease_seconds: int = 3600,
    ) -> tuple[HistoricalRangeDayExecutionResultV1, ...]:
        if not 1 <= max_program_concurrency <= 32:
            raise ValueError("max_program_concurrency must be between 1 and 32")
        if not 1 <= candidate_prefetch_per_program <= 2:
            raise ValueError("candidate_prefetch_per_program must be between 1 and 2")
        if not 1 <= max_day_commits_per_slice <= 500:
            raise ValueError("max_day_commits_per_slice must be between 1 and 500")
        if not 1 <= lease_seconds <= 86_400:
            raise ValueError("lease_seconds must be between 1 and 86400")
        if not str(worker_id or "").strip():
            raise ValueError("worker_id is required")
        batch, request_payload, semantics = self._load_sealed_r3_batch(batch_id=batch_id)
        runs = _list_all_execution_runs(repository=self._repository, batch_id=batch_id)
        self._ensure_running(batch=batch, runs=runs)
        refreshed_runs = _list_all_execution_runs(repository=self._repository, batch_id=batch_id)
        eligible_runs = tuple(
            run
            for run in refreshed_runs
            if run.status in {HistoricalRangeProgramStatus.RUNNING, HistoricalRangeProgramStatus.PARTIAL}
            and run.final_receipt_ref is None
        )
        # Each run owns only its smallest non-success ordinal. This preserves
        # list ordering while allowing independent Programs to execute together.
        selected_runs = eligible_runs[:max_day_commits_per_slice]
        results: list[HistoricalRangeDayExecutionResultV1] = []
        with ThreadPoolExecutor(max_workers=min(max_program_concurrency, len(selected_runs) or 1)) as pool:
            futures = {
                pool.submit(
                    self._execute_one_run,
                    batch=batch,
                    request_payload=request_payload,
                    semantics=semantics,
                    run=run,
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                    candidate_prefetch_per_program=candidate_prefetch_per_program,
                ): run.range_run_id
                for run in selected_runs
            }
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    results.append(result)
        return tuple(sorted(results, key=lambda item: item.day_run_id))

    def _execute_one_run(
        self,
        *,
        batch: HistoricalRangeExecutionBatchV1,
        request_payload: HistoricalRangeResolvedRequestArtifactPayloadV1,
        semantics: HistoricalRangeListSemanticsV2,
        run: HistoricalRangeExecutionRunV1,
        worker_id: str,
        lease_seconds: int,
        candidate_prefetch_per_program: int,
    ) -> HistoricalRangeDayExecutionResultV1 | None:
        run = self._materialize_if_needed(run=run, request_payload=request_payload, batch=batch)
        lease_token = derive_prefixed_id(
            "ahrlease",
            {
                "batch_id": batch.batch_id,
                "range_run_id": run.range_run_id,
                "worker_id": worker_id,
                "run_row_version": run.row_version,
            },
        )
        expired = self._repository.load_expired_claimable_day(
            batch_id=batch.batch_id,
            range_run_id=run.range_run_id,
        )
        if expired is not None:
            expired_attempt = self._build_failure_attempt(
                claim=expired,
                stage="LEASE_EXPIRED",
                status=HistoricalRangeDayStatus.RETRYABLE_FAILED,
                reason_codes=("ADVISORY_HR_DAY_LEASE_EXPIRED",),
                candidate_ref=None,
                mark_ref=None,
                error=TimeoutError("historical day lease expired before takeover"),
                lease_expired_at=expired.lease_expires_at,
            )
            claim = self._repository.take_over_expired_day(
                expired_claim=expired,
                expired_attempt=expired_attempt,
                worker_id=worker_id,
                lease_token=lease_token,
                lease_seconds=lease_seconds,
            )
        else:
            claim = self._repository.claim_next_day(
                batch_id=batch.batch_id,
                range_run_id=run.range_run_id,
                expected_run_row_version=run.row_version,
                worker_id=worker_id,
                lease_token=lease_token,
                lease_seconds=lease_seconds,
            )
        if claim is None:
            return None
        return self._execute_claimed_day(
            claim=claim,
            request_payload=request_payload,
            semantics=semantics,
            lease_seconds=lease_seconds,
            candidate_prefetch_per_program=candidate_prefetch_per_program,
        )

    def _execute_claimed_day(
        self,
        *,
        claim: HistoricalRangeClaimedDayV1,
        request_payload: HistoricalRangeResolvedRequestArtifactPayloadV1,
        semantics: HistoricalRangeListSemanticsV2,
        lease_seconds: int,
        candidate_prefetch_per_program: int,
    ) -> HistoricalRangeDayExecutionResultV1:
        stage = "CLAIM_INPUT"
        candidate_ref: HistoricalRangeArtifactRefV1 | None = None
        mark_ref: HistoricalRangeArtifactRefV1 | None = None
        committed = False
        heartbeat = _DayLeaseHeartbeatSupervisor(
            repository=self._repository,
            claim=claim,
            lease_seconds=lease_seconds,
        )
        heartbeat.start()
        try:
            program = _program_for_claim(request_payload=request_payload, claim=claim)
            predecessor = self._repository.load_predecessor_state(day_run_id=claim.day_run_id)
            _validate_predecessor_against_claim(claim=claim, predecessor=predecessor)
            candidate_ref = self._repository.load_reusable_candidate_ref(day_run_id=claim.day_run_id)
            if candidate_ref is None:
                candidate_ref = self._produce_prefetched_candidates(
                    request_payload=request_payload,
                    claim=claim,
                    candidate_prefetch_per_program=candidate_prefetch_per_program,
                )
            candidate_payload = _load_candidate_payload(store=self._artifact_store, ref=candidate_ref)
            _validate_candidate_for_claim(
                claim=claim,
                request_payload=request_payload,
                program=program,
                candidate_payload=candidate_payload,
            )
            stage = "CANDIDATE_BOUND_INPUT"
            previous_marks = _previous_marks(predecessor)
            mark_result = self._decision_mark_provider.produce(
                resolved_request_hash=claim.resolved_request_hash,
                catalog=request_payload.source_revision_catalog,
                program=program,
                range_run_id=claim.range_run_id,
                day_run_id=claim.day_run_id,
                decision_trade_date=claim.decision_trade_date,
                request_ref=claim.request_ref,
                included_symbols={
                    item.symbol for item in candidate_payload.candidates if item.membership_status == "INCLUDED"
                },
                previous_marks_by_symbol=previous_marks,
                predecessor_day_receipt_ref=claim.previous_day_receipt_ref,
                decision_cutoff=_decision_cutoff(claim.decision_trade_date),
            )
            mark_ref = mark_result.artifact_ref
            stage = "DAY_INPUT"
            day_input_hash = build_day_input_hash_v3(
                candidate_input_hash=candidate_payload.candidate_input_hash,
                candidate_artifact_ref=candidate_ref,
                decision_mark_set_ref=mark_ref,
                decision_mark_policy_hash=mark_result.mark_set.mark_policy_hash,
                previous_list_hash=claim.previous_list_hash,
                previous_day_receipt_ref=claim.previous_day_receipt_ref,
                list_semantics_version=semantics.schema_version,
                list_semantics_hash=semantics.semantics_hash,
            )
            next_trade_date = _next_trade_date(request_payload, claim)
            projection = self._list_transition_adapter.build_projection(
                program=program,
                candidate_payload=candidate_payload,
                decision_mark_set=mark_result.mark_set,
                decision_mark_set_ref=mark_ref,
                previous_episodes=predecessor.active_episodes,
                entry_sequences_by_symbol=self._repository.load_episode_entry_sequences(range_run_id=claim.range_run_id),
                previous_list_version_id=claim.previous_list_version_id,
                previous_list_hash=claim.previous_list_hash,
                previous_day_receipt_hash=(
                    claim.previous_day_receipt_ref.semantic_content_hash if claim.previous_day_receipt_ref else None
                ),
                day_input_hash=day_input_hash,
                next_trade_date=next_trade_date,
                is_range_end=next_trade_date is None,
                decision_cutoff=_decision_cutoff(claim.decision_trade_date),
                semantics=semantics,
            )
            if projection.list_version is None:
                raise HistoricalRangeDayWaitingInput(
                    "ADVISORY_HR_LIST_PROJECTION_WAITING_INPUT",
                    "historical list projection lacks complete decision-day evidence",
                    reason_codes=projection.blocking_diagnostics,
                )
            terminal_status = (
                HistoricalRangeDayStatus.COMPLETE
                if candidate_payload.candidate_outcome == "CANDIDATES_AVAILABLE"
                else HistoricalRangeDayStatus.VALID_NO_CANDIDATE
            )
            receipt_payload = build_day_receipt_payload_v2(
                range_run_id=claim.range_run_id,
                day_run_id=claim.day_run_id,
                terminal_status=terminal_status,
                day_input_hash=day_input_hash,
                candidate_artifact_ref=candidate_ref,
                decision_mark_set_ref=mark_ref,
                previous_day_receipt_ref=claim.previous_day_receipt_ref,
                list_version=projection.list_version,
                items=projection.items,
                episodes=projection.episodes,
            )
            upstream = (candidate_ref, mark_ref) + (
                (claim.previous_day_receipt_ref,) if claim.previous_day_receipt_ref is not None else ()
            )
            receipt_ref = self._publish_and_readback(
                artifact_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
                payload_schema_version=DAY_RECEIPT_PAYLOAD_SCHEMA_VERSION_V2,
                resolved_request_hash=claim.resolved_request_hash,
                payload=receipt_payload,
                range_run_id=claim.range_run_id,
                day_run_id=claim.day_run_id,
                upstream_refs=upstream,
            )
            claim = heartbeat.stop()
            attempt = HistoricalRangeDayAttemptV1(
                attempt_id=derive_prefixed_id(
                    "ahrda",
                    {"day_run_id": claim.day_run_id, "attempt_no": claim.attempt_no, "fencing": claim.fencing_token},
                ),
                day_run_id=claim.day_run_id,
                attempt_no=claim.attempt_no,
                worker_id=claim.worker_id,
                lease_token=claim.lease_token,
                fencing_token=claim.fencing_token,
                status=terminal_status.value,
                input_hash=day_input_hash,
                result_hash=receipt_ref.semantic_content_hash,
                candidate_artifact_ref=candidate_ref,
                attempt_receipt_ref=receipt_ref,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
            )
            self._repository.commit_successful_day(
                day_run_id=claim.day_run_id,
                expected_row_version=claim.row_version,
                expected_fencing_token=claim.fencing_token,
                terminal_status=terminal_status,
                day_input_hash=day_input_hash,
                candidate_artifact_ref=candidate_ref,
                decision_mark_set_ref=mark_ref,
                previous_day_receipt_ref=claim.previous_day_receipt_ref,
                day_receipt_ref=receipt_ref,
                list_version=projection.list_version,
                candidates=candidate_payload.candidates,
                items=projection.items,
                episodes=projection.episodes,
                attempt=attempt,
            )
            committed = True
            readback = self._repository.full_readback_successful_day(day_run_id=claim.day_run_id)
            if readback.receipt_ref != receipt_ref:
                raise HistoricalRangeContractError(
                    "ADVISORY_HR_SUCCESS_READBACK_MISMATCH",
                    "successful day readback returned a different receipt ref",
                    context={"day_run_id": claim.day_run_id},
                )
            if next_trade_date is None:
                self.reconcile_run(range_run_id=claim.range_run_id)
            return HistoricalRangeDayExecutionResultV1(day_run_id=claim.day_run_id, status=terminal_status)
        except Exception as exc:  # The failure path persists a typed receipt and never becomes empty success.
            try:
                claim = heartbeat.stop()
            except Exception as heartbeat_exc:
                LOGGER.exception(
                    "R3 historical day heartbeat lost durable ownership "
                    "stage=%s day_run_id=%s original_error_type=%s",
                    stage,
                    claim.day_run_id,
                    type(exc).__name__,
                )
                raise heartbeat_exc from exc
            if committed:
                LOGGER.exception(
                    "R3 historical day post-commit verification failed "
                    "stage=%s day_run_id=%s error_type=%s",
                    stage,
                    claim.day_run_id,
                    type(exc).__name__,
                )
                raise
            status, reason_codes = _classify_failure(exc)
            LOGGER.exception(
                "R3 historical day execution failed stage=%s day_run_id=%s error_type=%s",
                stage,
                claim.day_run_id,
                type(exc).__name__,
            )
            failure = self._build_failure_attempt(
                claim=claim,
                stage=stage,
                status=status,
                reason_codes=reason_codes,
                candidate_ref=candidate_ref,
                mark_ref=mark_ref,
                error=exc,
            )
            self._repository.finish_failed_day(
                claimed_day=claim,
                target_status=status,
                attempt=failure,
                reason_codes=reason_codes,
                error_json=failure.error_json,
            )
            self.reconcile_run(range_run_id=claim.range_run_id)
            return HistoricalRangeDayExecutionResultV1(
                day_run_id=claim.day_run_id,
                status=status,
                reason_codes=reason_codes,
            )

    def _produce_prefetched_candidates(
        self,
        *,
        request_payload: HistoricalRangeResolvedRequestArtifactPayloadV1,
        claim: HistoricalRangeClaimedDayV1,
        candidate_prefetch_per_program: int,
    ) -> HistoricalRangeArtifactRefV1:
        dates = request_payload.resolved_request.date_plan.ordered_trade_dates[
            claim.ordinal - 1 : claim.ordinal - 1 + candidate_prefetch_per_program
        ]
        missing_dates: list[Any] = []
        with self._prefetch_lock:
            for trade_date in dates:
                if (claim.range_run_id, trade_date) not in self._prefetched_candidates:
                    missing_dates.append(trade_date)
        if missing_dates:
            with ThreadPoolExecutor(max_workers=len(missing_dates)) as pool:
                futures = {
                    pool.submit(
                        self._candidate_producer.produce,
                        request_payload=request_payload,
                        research_program_id=claim.research_program_id,
                        decision_trade_date=trade_date,
                        request_artifact_ref=claim.request_ref,
                    ): trade_date
                    for trade_date in missing_dates
                }
                produced: dict[Any, HistoricalRangeArtifactRefV1] = {}
                for future in as_completed(futures):
                    trade_date = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        if trade_date == claim.decision_trade_date:
                            raise
                        LOGGER.warning(
                            "R3 candidate prefetch failed without changing current-day execution "
                            "range_run_id=%s decision_trade_date=%s error_type=%s",
                            claim.range_run_id,
                            trade_date,
                            type(exc).__name__,
                        )
                        continue
                    produced[trade_date] = result.candidate_artifact_ref
            with self._prefetch_lock:
                for trade_date, ref in produced.items():
                    existing = self._prefetched_candidates.setdefault((claim.range_run_id, trade_date), ref)
                    if existing != ref:
                        raise HistoricalRangeContractError(
                            "ADVISORY_HR_CANDIDATE_PREFETCH_CONFLICT",
                            "same Program/day prefetch produced different exact candidate refs",
                            context={"range_run_id": claim.range_run_id, "decision_trade_date": str(trade_date)},
                        )
        with self._prefetch_lock:
            current = self._prefetched_candidates.pop((claim.range_run_id, claim.decision_trade_date), None)
        if current is None:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_CANDIDATE_PREFETCH_MISSING",
                "bounded candidate prefetch did not produce the claimed day",
                context={"day_run_id": claim.day_run_id},
            )
        return current

    def reconcile_run(self, *, range_run_id: str) -> None:
        facts = self._repository.load_run_finalization_facts(range_run_id=range_run_id)
        run = facts.run
        if run.final_receipt_ref is not None:
            return
        success_count = len(facts.successful_days)
        target_status: HistoricalRangeProgramStatus | None = None
        if success_count == facts.total_day_count:
            target_status = HistoricalRangeProgramStatus.COMPLETED
        elif facts.blocking_status is HistoricalRangeDayStatus.FAILED:
            target_status = (
                HistoricalRangeProgramStatus.FAILED
                if success_count == 0
                else HistoricalRangeProgramStatus.PARTIAL
            )
        elif facts.blocking_status is HistoricalRangeDayStatus.CANCELLED or facts.cancelled_from_ordinal is not None:
            target_status = HistoricalRangeProgramStatus.CANCELLED
        elif facts.blocking_status in {
            HistoricalRangeDayStatus.WAITING_INPUT,
            HistoricalRangeDayStatus.RETRYABLE_FAILED,
        }:
            recoverable_target = (
                HistoricalRangeProgramStatus.PARTIAL
                if success_count > 0
                else HistoricalRangeProgramStatus(facts.blocking_status.value)
            )
            if run.status is not recoverable_target:
                self._repository.transition_run(
                    range_run_id=range_run_id,
                    expected_row_version=run.row_version,
                    target_status=recoverable_target,
                    resume_trade_date=facts.blocking_trade_date,
                )
            return
        if target_status is None:
            return
        success_refs = tuple(item.receipt_ref for item in facts.successful_days)
        first_list_hash = (
            facts.successful_days[0].receipt.list_version.list_content_hash if facts.successful_days else None
        )
        latest_list_hash = (
            facts.successful_days[-1].receipt.list_version.list_content_hash if facts.successful_days else None
        )
        receipt = HistoricalRangeRunExecutionReceiptV1(
            range_run_id=run.range_run_id,
            research_program_id=run.research_program_id,
            status=target_status.value,
            resolved_request_hash=facts.resolved_request_hash,
            ordered_success_day_receipt_refs=success_refs,
            blocking_attempt_receipt_ref=facts.blocking_attempt_receipt_ref,
            first_list_hash=first_list_hash,
            latest_list_hash=latest_list_hash,
            successful_day_count=success_count,
            failed_day_count=(1 if facts.blocking_status is HistoricalRangeDayStatus.FAILED else 0),
            unexecuted_day_count=facts.unexecuted_day_count,
            blocking_day_run_id=(
                facts.blocking_day_run_id
                if facts.blocking_status in {HistoricalRangeDayStatus.FAILED, HistoricalRangeDayStatus.CANCELLED}
                else None
            ),
            blocking_ordinal=(
                facts.blocking_ordinal
                if facts.blocking_status in {HistoricalRangeDayStatus.FAILED, HistoricalRangeDayStatus.CANCELLED}
                else None
            ),
        )
        upstream = success_refs + (
            (facts.blocking_attempt_receipt_ref,) if facts.blocking_attempt_receipt_ref is not None else ()
        )
        receipt_ref = self._publish_and_readback(
            artifact_kind=HistoricalRangeArtifactKind.RANGE_RECEIPT,
            payload_schema_version=RUN_EXECUTION_RECEIPT_SCHEMA_VERSION,
            resolved_request_hash=facts.resolved_request_hash,
            payload=receipt.model_dump(mode="json"),
            range_run_id=range_run_id,
            day_run_id=None,
            upstream_refs=upstream,
        )
        self._repository.finish_range_run(
            range_run_id=range_run_id,
            expected_row_version=run.row_version,
            target_status=target_status,
            receipt=receipt,
            final_receipt_ref=receipt_ref,
        )

    def reconcile_batch(self, *, batch_id: str) -> None:
        batch = self._repository.load_execution_batch(batch_id=batch_id)
        runs = _list_all_execution_runs(repository=self._repository, batch_id=batch_id)
        if not runs:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_BATCH_HAS_NO_RUNS",
                "sealed execution batch has no Program runs",
                context={"batch_id": batch_id},
            )
        if all(run.status is HistoricalRangeProgramStatus.COMPLETED for run in runs):
            target = HistoricalRangeBatchStatus.COMPLETED
        elif all(run.status is HistoricalRangeProgramStatus.FAILED for run in runs):
            target = HistoricalRangeBatchStatus.FAILED
        elif all(
            run.final_receipt_ref is not None
            for run in runs
        ):
            target = HistoricalRangeBatchStatus.PARTIAL
        elif all(
            run.status in {
                HistoricalRangeProgramStatus.WAITING_INPUT,
                HistoricalRangeProgramStatus.RETRYABLE_FAILED,
            }
            for run in runs
        ):
            target = HistoricalRangeBatchStatus.WAITING_INPUT
        elif any(
            run.status
            in {
                HistoricalRangeProgramStatus.WAITING_INPUT,
                HistoricalRangeProgramStatus.RETRYABLE_FAILED,
                HistoricalRangeProgramStatus.PARTIAL,
                HistoricalRangeProgramStatus.FAILED,
                HistoricalRangeProgramStatus.COMPLETED,
            }
            for run in runs
        ) and not any(run.status is HistoricalRangeProgramStatus.RUNNING for run in runs):
            target = HistoricalRangeBatchStatus.PARTIAL
        else:
            return
        if batch.status is target:
            return
        self._repository.transition_batch(
            batch_id=batch_id,
            expected_row_version=batch.row_version,
            target_status=target,
        )

    def publish_range_receipt(
        self,
        *,
        payload_schema_version: str,
        resolved_request_hash: str,
        payload: dict[str, Any],
        upstream_refs: tuple[HistoricalRangeArtifactRefV1, ...],
    ) -> HistoricalRangeArtifactRefV1:
        return self._publish_and_readback(
            artifact_kind=HistoricalRangeArtifactKind.RANGE_RECEIPT,
            payload_schema_version=payload_schema_version,
            resolved_request_hash=resolved_request_hash,
            payload=payload,
            range_run_id=None,
            day_run_id=None,
            upstream_refs=upstream_refs,
        )

    def publish_day_attempt_receipt(
        self,
        *,
        resolved_request_hash: str,
        range_run_id: str,
        day_run_id: str,
        payload: HistoricalRangeDayAttemptReceiptPayloadV1,
        upstream_refs: tuple[HistoricalRangeArtifactRefV1, ...],
    ) -> HistoricalRangeArtifactRefV1:
        return self._publish_and_readback(
            artifact_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
            payload_schema_version=DAY_ATTEMPT_RECEIPT_PAYLOAD_SCHEMA_VERSION,
            resolved_request_hash=resolved_request_hash,
            payload=payload.model_dump(mode="json"),
            range_run_id=range_run_id,
            day_run_id=day_run_id,
            upstream_refs=upstream_refs,
        )

    def load_range_receipt(self, ref: HistoricalRangeArtifactRefV1) -> dict[str, Any]:
        if ref.artifact_kind is not HistoricalRangeArtifactKind.RANGE_RECEIPT:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_OPERATION_RECEIPT_KIND_INVALID",
                "execution operation result must reference RANGE_RECEIPT",
            )
        return dict(self._artifact_store.load(ref).payload)


    def _build_failure_attempt(
        self,
        *,
        claim: HistoricalRangeClaimedDayV1,
        stage: str,
        status: HistoricalRangeDayStatus,
        reason_codes: tuple[str, ...],
        candidate_ref: HistoricalRangeArtifactRefV1 | None,
        mark_ref: HistoricalRangeArtifactRefV1 | None,
        error: Exception,
        lease_expired_at: datetime | None = None,
    ) -> HistoricalRangeDayAttemptV1:
        input_hash_kind = "CLAIM_INPUT"
        if candidate_ref is not None:
            input_hash_kind = "CANDIDATE_BOUND_INPUT"
        if mark_ref is not None:
            input_hash_kind = "DAY_INPUT"
        attempt_input_hash = _failure_input_hash(
            claim=claim,
            input_hash_kind=input_hash_kind,
            candidate_ref=candidate_ref,
            mark_ref=mark_ref,
        )
        error_json = {"reason_codes": list(reason_codes), "stage": stage, "error_type": type(error).__name__}
        if isinstance(error, TradingCoreError):
            error_json["domain_error_code"] = error.error_code
        payload = HistoricalRangeDayAttemptReceiptPayloadV1(
            day_run_id=claim.day_run_id,
            attempt_no=claim.attempt_no,
            fencing_token=claim.fencing_token,
            worker_id=claim.worker_id,
            lease_token_hash=sha256(claim.lease_token.encode("utf-8")).hexdigest(),
            status=status.value,
            attempt_input_hash=attempt_input_hash,
            input_hash_kind=input_hash_kind,
            candidate_artifact_ref=candidate_ref,
            decision_mark_set_ref=mark_ref,
            previous_list_hash=claim.previous_list_hash,
            previous_day_receipt_ref=claim.previous_day_receipt_ref,
            stage=stage,
            reason_codes=reason_codes,
            sanitized_error=error_json,
            lease_expired_at=lease_expired_at,
        )
        upstream = (claim.request_ref,) + ((candidate_ref,) if candidate_ref is not None else ()) + (
            (mark_ref,) if mark_ref is not None else ()
        ) + ((claim.previous_day_receipt_ref,) if claim.previous_day_receipt_ref is not None else ())
        receipt_ref = self._publish_and_readback(
            artifact_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
            payload_schema_version=DAY_ATTEMPT_RECEIPT_PAYLOAD_SCHEMA_VERSION,
            resolved_request_hash=claim.resolved_request_hash,
            payload=payload.model_dump(mode="json"),
            range_run_id=claim.range_run_id,
            day_run_id=claim.day_run_id,
            upstream_refs=upstream,
        )
        now = datetime.now(UTC)
        return HistoricalRangeDayAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahrda",
                {"day_run_id": claim.day_run_id, "attempt_no": claim.attempt_no, "fencing": claim.fencing_token},
            ),
            day_run_id=claim.day_run_id,
            attempt_no=claim.attempt_no,
            worker_id=claim.worker_id,
            lease_token=claim.lease_token,
            fencing_token=claim.fencing_token,
            status=status.value,
            input_hash=attempt_input_hash,
            result_hash=receipt_ref.semantic_content_hash,
            candidate_artifact_ref=candidate_ref,
            attempt_receipt_ref=receipt_ref,
            reason_codes=reason_codes,
            error_json=error_json,
            started_at=now,
            finished_at=now,
        )

    def _publish_and_readback(self, **kwargs: Any) -> HistoricalRangeArtifactRefV1:
        stored = self._artifact_store.publish_payload(
            producer_contract_version=EXECUTOR_CONTRACT_VERSION,
            **kwargs,
        )
        envelope = self._artifact_store.load(stored.ref)
        expected_upstream = tuple(
            sorted((item.artifact_kind, item.semantic_content_hash, item.relative_path) for item in kwargs["upstream_refs"])
        )
        actual_upstream = tuple(
            sorted((item.artifact_kind, item.semantic_content_hash, item.relative_path) for item in envelope.upstream_refs)
        )
        if (
            envelope.payload != kwargs["payload"]
            or envelope.range_run_id != kwargs["range_run_id"]
            or envelope.day_run_id != kwargs["day_run_id"]
            or envelope.resolved_request_hash != kwargs["resolved_request_hash"]
            or actual_upstream != expected_upstream
        ):
            raise HistoricalRangeContractError(
                "ADVISORY_HR_EXECUTOR_ARTIFACT_READBACK_CONFLICT",
                "published R3 execution artifact differs from its typed request",
            )
        return stored.ref

    def _load_sealed_r3_batch(
        self,
        *,
        batch_id: str,
    ) -> tuple[HistoricalRangeExecutionBatchV1, HistoricalRangeResolvedRequestArtifactPayloadV1, HistoricalRangeListSemanticsV2]:
        batch = self._repository.load_execution_batch(batch_id=batch_id)
        if batch.artifact_root_identity_hash != self._artifact_store.root_identity_hash:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_ARTIFACT_ROOT_MISMATCH",
                "execution batch was sealed under another explicit artifact root",
                context={"batch_id": batch_id},
            )
        request_envelope = self._artifact_store.load(batch.request_ref)
        request_payload = HistoricalRangeResolvedRequestArtifactPayloadV1.model_validate(request_envelope.payload)
        semantics = canonical_list_semantics_v2()
        if (
            request_payload.resolved_request.list_semantics_version != semantics.schema_version
            or request_payload.resolved_request.list_semantics_hash != semantics.semantics_hash
            or batch.status
            not in {
                HistoricalRangeBatchStatus.QUEUED,
                HistoricalRangeBatchStatus.RUNNING,
                HistoricalRangeBatchStatus.PARTIAL,
                HistoricalRangeBatchStatus.WAITING_INPUT,
            }
        ):
            raise HistoricalRangeContractError(
                "ADVISORY_HR_R3_EXECUTION_BATCH_CONTRACT_INVALID",
                "R3 requires a sealed canonical-list batch in an executable state",
                context={"batch_id": batch_id, "status": batch.status.value},
            )
        return batch, request_payload, semantics

    def _ensure_running(self, *, batch: HistoricalRangeExecutionBatchV1, runs: tuple[HistoricalRangeExecutionRunV1, ...]) -> None:
        if batch.status is HistoricalRangeBatchStatus.QUEUED:
            self._repository.transition_batch(
                batch_id=batch.batch_id,
                expected_row_version=batch.row_version,
                target_status=HistoricalRangeBatchStatus.RUNNING,
            )
        for run in runs:
            if run.status is HistoricalRangeProgramStatus.QUEUED:
                self._repository.transition_run(
                    range_run_id=run.range_run_id,
                    expected_row_version=run.row_version,
                    target_status=HistoricalRangeProgramStatus.RUNNING,
                )

    def _materialize_if_needed(
        self,
        *,
        run: HistoricalRangeExecutionRunV1,
        request_payload: HistoricalRangeResolvedRequestArtifactPayloadV1,
        batch: HistoricalRangeExecutionBatchV1,
    ) -> HistoricalRangeExecutionRunV1:
        if run.materialized_day_count >= len(request_payload.resolved_request.date_plan.ordered_trade_dates):
            return run
        self._repository.materialize_day_plan_chunk(
            range_run_id=run.range_run_id,
            date_plan=request_payload.resolved_request.date_plan,
            date_plan_ref=batch.date_plan_ref,
            expected_cursor_ordinal=run.day_plan_cursor_ordinal,
            chunk_size=500,
        )
        refreshed = tuple(
            item
            for item in _list_all_execution_runs(repository=self._repository, batch_id=batch.batch_id)
            if item.range_run_id == run.range_run_id
        )
        if len(refreshed) != 1:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_MATERIALIZED_RUN_READBACK_MISMATCH",
                "materialized range run could not be read back exactly once",
                context={"range_run_id": run.range_run_id},
            )
        return refreshed[0]


class _DayLeaseHeartbeatSupervisor:
    def __init__(
        self,
        *,
        repository: HistoricalRangeExecutionRepository,
        claim: HistoricalRangeClaimedDayV1,
        lease_seconds: int,
    ) -> None:
        self._repository = repository
        self._claim = claim
        self._lease_seconds = lease_seconds
        self._interval_seconds = max(1.0, min(300.0, self._lease_seconds / 3.0))
        self._stop = Event()
        self._lock = Lock()
        self._error: BaseException | None = None
        self._thread = Thread(
            target=self._run,
            name=f"ahr-heartbeat-{claim.day_run_id}",
            daemon=True,
        )
        self._started = False

    def start(self) -> None:
        if self._started:
            raise RuntimeError("day lease heartbeat supervisor already started")
        self._started = True
        self._thread.start()

    def stop(self) -> HistoricalRangeClaimedDayV1:
        self._stop.set()
        if self._started and self._thread.is_alive():
            self._thread.join()
        with self._lock:
            error = self._error
            claim = self._claim
        if error is not None:
            raise error
        return claim

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                with self._lock:
                    current = self._claim
                refreshed = self._repository.heartbeat_day(
                    claimed_day=current,
                    lease_seconds=self._lease_seconds,
                )
                with self._lock:
                    self._claim = refreshed
            except BaseException as exc:
                with self._lock:
                    self._error = exc
                self._stop.set()
                return


class _OperationLeaseHeartbeatSupervisor:
    def __init__(
        self,
        *,
        repository: HistoricalRangeExecutionRepository,
        operation: HistoricalRangeExecutionOperationV1,
        lease_seconds: int,
    ) -> None:
        self._repository = repository
        self._operation = operation
        self._lease_seconds = lease_seconds
        self._interval_seconds = max(1.0, min(300.0, lease_seconds / 3.0))
        self._stop = Event()
        self._lock = Lock()
        self._error: BaseException | None = None
        self._thread = Thread(
            target=self._run,
            name=f"ahr-operation-heartbeat-{operation.operation_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> HistoricalRangeExecutionOperationV1:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join()
        with self._lock:
            error = self._error
            operation = self._operation
        if error is not None:
            raise error
        return operation

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                with self._lock:
                    current = self._operation
                refreshed = self._repository.heartbeat_execution_operation(
                    claimed_operation=current,
                    lease_seconds=self._lease_seconds,
                )
                with self._lock:
                    self._operation = refreshed
            except BaseException as exc:
                with self._lock:
                    self._error = exc
                self._stop.set()
                return


class HistoricalRangeBatchExecutionService:
    """Consume bounded slices until a real durable boundary is reached."""

    def __init__(self, *, day_executor: HistoricalRangeDayExecutor) -> None:
        if day_executor is None:
            raise ValueError("day_executor is required")
        self._day_executor = day_executor

    def execute_until_blocked(
        self,
        *,
        batch_id: str,
        worker_id: str,
        max_program_concurrency: int = 2,
        candidate_prefetch_per_program: int = 2,
        day_slice_size: int = 4,
        lease_seconds: int = 3600,
    ) -> HistoricalRangeBatchExecutionResultV1:
        all_results: list[HistoricalRangeDayExecutionResultV1] = []
        while True:
            slice_results = self._day_executor.execute_batch_slice(
                batch_id=batch_id,
                worker_id=worker_id,
                max_program_concurrency=max_program_concurrency,
                candidate_prefetch_per_program=candidate_prefetch_per_program,
                max_day_commits_per_slice=day_slice_size,
                lease_seconds=lease_seconds,
            )
            if not slice_results:
                break
            all_results.extend(slice_results)
            # A slice with only waiting/retry/failure outcomes has reached a
            # durable boundary. Successful results cause the next bounded slice.
            if not any(
                item.status in {HistoricalRangeDayStatus.COMPLETE, HistoricalRangeDayStatus.VALID_NO_CANDIDATE}
                for item in slice_results
            ):
                break
        # A worker can exit after the day commit but before its run receipt is
        # finalized. A later resume then has no day left to claim, so close
        # every recoverable run from durable day facts before aggregating the
        # batch. Completed runs are idempotent no-ops.
        for run in _list_all_execution_runs(
            repository=self._day_executor._repository,
            batch_id=batch_id,
        ):
            self._day_executor.reconcile_run(range_run_id=run.range_run_id)
        self._day_executor.reconcile_batch(batch_id=batch_id)
        return _batch_result(batch_id=batch_id, results=all_results)

    def resume_until_blocked(
        self,
        *,
        batch_id: str,
        worker_id: str,
        operation_idempotency_key: str,
        expected_batch_row_version: int,
        max_program_concurrency: int = 2,
        candidate_prefetch_per_program: int = 2,
        day_slice_size: int = 4,
        lease_seconds: int = 3600,
    ) -> HistoricalRangeBatchExecutionResultV1:
        operation_payload = {
            "schema_version": "advisory_historical_range_resume_operation_input_v1",
            "batch_id": batch_id,
            "operation_type": HistoricalRangeOperationType.RESUME.value,
            "expected_batch_row_version": expected_batch_row_version,
            "max_program_concurrency": max_program_concurrency,
            "candidate_prefetch_per_program": candidate_prefetch_per_program,
            "day_slice_size": day_slice_size,
            "lease_seconds": lease_seconds,
        }
        payload_hash = canonical_json_sha256(operation_payload)
        operation_id = derive_prefixed_id(
            "ahrop",
            {
                "batch_id": batch_id,
                "operation_type": HistoricalRangeOperationType.RESUME.value,
                "operation_idempotency_key": operation_idempotency_key,
            },
        )
        request = HistoricalRangeOperationRequestV1(
            operation_id=operation_id,
            batch_id=batch_id,
            operation_type=HistoricalRangeOperationType.RESUME,
            operation_idempotency_key=operation_idempotency_key,
            request_payload_sha256=payload_hash,
            expected_row_version=expected_batch_row_version,
        )
        self._day_executor._repository.get_or_create_operation(request)
        operation = self._day_executor._repository.load_execution_operation(operation_id=operation_id)
        if operation.status is HistoricalRangeOperationStatus.COMPLETED:
            return self._load_completed_operation_result(operation=operation, expected_payload_hash=payload_hash)
        batch = self._day_executor._repository.load_execution_batch(batch_id=batch_id)
        if batch.row_version != expected_batch_row_version and operation.attempt_no == 0:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_OPERATION_BATCH_VERSION_CONFLICT",
                "resume operation expected batch row version differs from current state",
                context={"batch_id": batch_id, "expected": expected_batch_row_version, "actual": batch.row_version},
            )
        expired_attempt = self._expired_operation_attempt(operation=operation) if (
            operation.status is HistoricalRangeOperationStatus.RUNNING
            and operation.lease_expired
        ) else None
        lease_token = derive_prefixed_id(
            "ahroplease",
            {
                "operation_id": operation_id,
                "worker_id": worker_id,
                "next_attempt": operation.attempt_no + 1,
            },
        )
        claimed = self._day_executor._repository.claim_execution_operation(
            operation_id=operation_id,
            expected_row_version=operation.row_version,
            worker_id=worker_id,
            lease_token=lease_token,
            lease_seconds=lease_seconds,
            expired_attempt=expired_attempt,
        )
        claim_started_at = datetime.now(UTC)
        heartbeat = _OperationLeaseHeartbeatSupervisor(
            repository=self._day_executor._repository,
            operation=claimed,
            lease_seconds=lease_seconds,
        )
        heartbeat.start()
        try:
            result = self._terminal_batch_result_from_run_receipts(batch_id=batch_id)
            if result is None:
                self._resume_recoverable_state(batch_id=batch_id)
                result = self.execute_until_blocked(
                    batch_id=batch_id,
                    worker_id=worker_id,
                    max_program_concurrency=max_program_concurrency,
                    candidate_prefetch_per_program=candidate_prefetch_per_program,
                    day_slice_size=day_slice_size,
                    lease_seconds=lease_seconds,
                )
            claimed = heartbeat.stop()
        except Exception as exc:
            try:
                claimed = heartbeat.stop()
            except Exception as heartbeat_exc:
                LOGGER.exception(
                    "R3 resume operation heartbeat lost durable ownership "
                    "operation_id=%s original_error_type=%s",
                    claimed.operation_id,
                    type(exc).__name__,
                )
                raise heartbeat_exc from exc
            LOGGER.exception(
                "R3 resume operation failed operation_id=%s error_type=%s",
                claimed.operation_id,
                type(exc).__name__,
            )
            self._finish_failed_operation(claimed=claimed, error=exc)
            raise
        return self._finish_operation(
            claimed=claimed,
            payload_hash=payload_hash,
            result=result,
            started_at=claim_started_at,
            cancelled_day_results=(),
        )

    def cancel_batch(
        self,
        *,
        batch_id: str,
        worker_id: str,
        operation_idempotency_key: str,
        expected_batch_row_version: int,
        lease_seconds: int = 3600,
    ) -> HistoricalRangeBatchExecutionResultV1:
        operation_payload = {
            "schema_version": "advisory_historical_range_cancel_operation_input_v1",
            "batch_id": batch_id,
            "operation_type": HistoricalRangeOperationType.CANCEL.value,
            "expected_batch_row_version": expected_batch_row_version,
            "lease_seconds": lease_seconds,
        }
        payload_hash = canonical_json_sha256(operation_payload)
        operation_id = derive_prefixed_id(
            "ahrop",
            {
                "batch_id": batch_id,
                "operation_type": HistoricalRangeOperationType.CANCEL.value,
                "operation_idempotency_key": operation_idempotency_key,
            },
        )
        request = HistoricalRangeOperationRequestV1(
            operation_id=operation_id,
            batch_id=batch_id,
            operation_type=HistoricalRangeOperationType.CANCEL,
            operation_idempotency_key=operation_idempotency_key,
            request_payload_sha256=payload_hash,
            expected_row_version=expected_batch_row_version,
        )
        self._day_executor._repository.get_or_create_operation(request)
        operation = self._day_executor._repository.load_execution_operation(operation_id=operation_id)
        if operation.status is HistoricalRangeOperationStatus.COMPLETED:
            return self._load_completed_operation_result(operation=operation, expected_payload_hash=payload_hash)
        batch = self._day_executor._repository.load_execution_batch(batch_id=batch_id)
        if batch.row_version != expected_batch_row_version and operation.attempt_no == 0:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_OPERATION_BATCH_VERSION_CONFLICT",
                "cancel operation expected batch row version differs from current state",
                context={"batch_id": batch_id, "expected": expected_batch_row_version, "actual": batch.row_version},
            )
        expired_attempt = self._expired_operation_attempt(operation=operation) if (
            operation.status is HistoricalRangeOperationStatus.RUNNING
            and operation.lease_expired
        ) else None
        lease_token = derive_prefixed_id(
            "ahroplease",
            {
                "operation_id": operation_id,
                "worker_id": worker_id,
                "next_attempt": operation.attempt_no + 1,
            },
        )
        claimed = self._day_executor._repository.claim_execution_operation(
            operation_id=operation_id,
            expected_row_version=operation.row_version,
            worker_id=worker_id,
            lease_token=lease_token,
            lease_seconds=lease_seconds,
            expired_attempt=expired_attempt,
        )
        claim_started_at = datetime.now(UTC)
        heartbeat = _OperationLeaseHeartbeatSupervisor(
            repository=self._day_executor._repository,
            operation=claimed,
            lease_seconds=lease_seconds,
        )
        heartbeat.start()
        try:
            current_batch = self._day_executor._repository.load_execution_batch(batch_id=batch_id)
            if current_batch.status in {
                HistoricalRangeBatchStatus.QUEUED,
                HistoricalRangeBatchStatus.RUNNING,
                HistoricalRangeBatchStatus.PARTIAL,
                HistoricalRangeBatchStatus.WAITING_INPUT,
            }:
                contexts = self._day_executor._repository.load_cancellation_day_contexts(
                    batch_id=batch_id
                )
                attempts = {
                    context.day_run_id: self._build_cancel_attempt(context=context, operation=claimed)
                    for context in contexts
                }
                run_ids = self._day_executor._repository.cancel_execution_batch(
                    batch_id=batch_id,
                    expected_batch_row_version=current_batch.row_version,
                    attempts=attempts,
                )
            elif current_batch.status is HistoricalRangeBatchStatus.CANCELLING:
                run_ids = tuple(
                    run.range_run_id
                    for run in _list_all_execution_runs(
                        repository=self._day_executor._repository,
                        batch_id=batch_id,
                    )
                )
            elif current_batch.status is HistoricalRangeBatchStatus.CANCELLED:
                run_ids = ()
            else:
                raise HistoricalRangeContractError(
                    "ADVISORY_HR_CANCEL_BATCH_STATE_INVALID",
                    "cancel operation cannot continue from the durable batch state",
                    context={"batch_id": batch_id, "status": current_batch.status.value},
                )
            for range_run_id in run_ids:
                self._day_executor.reconcile_run(range_run_id=range_run_id)
            cancelling = self._day_executor._repository.load_execution_batch(batch_id=batch_id)
            if cancelling.status is HistoricalRangeBatchStatus.CANCELLING:
                self._day_executor._repository.transition_batch(
                    batch_id=batch_id,
                    expected_row_version=cancelling.row_version,
                    target_status=HistoricalRangeBatchStatus.CANCELLED,
                )
            elif cancelling.status is not HistoricalRangeBatchStatus.CANCELLED:
                raise HistoricalRangeContractError(
                    "ADVISORY_HR_CANCEL_BATCH_NOT_CLOSED",
                    "cancel operation did not converge to CANCELLING or CANCELLED",
                    context={"batch_id": batch_id, "status": cancelling.status.value},
                )
            cancelled_day_results = self._day_executor._repository.load_cancelled_day_results(
                batch_id=batch_id
            )
            claimed = heartbeat.stop()
        except Exception as exc:
            try:
                claimed = heartbeat.stop()
            except Exception as heartbeat_exc:
                LOGGER.exception(
                    "R3 cancel operation heartbeat lost durable ownership "
                    "operation_id=%s original_error_type=%s",
                    claimed.operation_id,
                    type(exc).__name__,
                )
                raise heartbeat_exc from exc
            LOGGER.exception(
                "R3 cancel operation failed operation_id=%s error_type=%s",
                claimed.operation_id,
                type(exc).__name__,
            )
            self._finish_failed_operation(claimed=claimed, error=exc)
            raise
        result = HistoricalRangeBatchExecutionResultV1(
            batch_id=batch_id,
            executed_day_count=len(cancelled_day_results),
            successful_day_count=0,
            waiting_day_count=0,
            retryable_day_count=0,
            failed_day_count=0,
            blocking_day_run_ids=tuple(sorted(item.day_run_id for item in cancelled_day_results)),
        )
        return self._finish_operation(
            claimed=claimed,
            payload_hash=payload_hash,
            result=result,
            started_at=claim_started_at,
            cancelled_day_results=cancelled_day_results,
        )

    def _build_cancel_attempt(
        self,
        *,
        context: Any,
        operation: HistoricalRangeExecutionOperationV1,
    ) -> HistoricalRangeDayAttemptV1:
        running = context.status is HistoricalRangeDayStatus.RUNNING
        attempt_no = context.attempt_no if running else context.attempt_no + 1
        fencing_token = int(context.fencing_token or 0) if running else int(context.fencing_token or 0) + 1
        worker_id = context.worker_id if running else operation.worker_id
        lease_token = context.lease_token if running else operation.lease_token
        if not worker_id or not lease_token or fencing_token < 1:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_CANCEL_DAY_OWNERSHIP_MISSING",
                "cancel day attempt lacks durable worker/lease/fencing identity",
                context={"day_run_id": context.day_run_id},
            )
        input_hash = canonical_json_sha256(
            {
                "schema_version": "advisory_historical_range_cancel_day_input_v1",
                "batch_id": context.batch_id,
                "range_run_id": context.range_run_id,
                "day_run_id": context.day_run_id,
                "ordinal": context.ordinal,
                "resolved_request_hash": context.resolved_request_hash,
                "previous_list_hash": context.previous_list_hash,
                "previous_day_receipt_ref": (
                    context.previous_day_receipt_ref.model_dump(mode="json")
                    if context.previous_day_receipt_ref is not None
                    else None
                ),
            }
        )
        error_json = {
            "reason_codes": ["ADVISORY_HR_EXPLICIT_CANCEL"],
            "stage": "CANCEL",
            "error_type": "ExplicitCancellation",
        }
        payload = HistoricalRangeDayAttemptReceiptPayloadV1(
            day_run_id=context.day_run_id,
            attempt_no=attempt_no,
            fencing_token=fencing_token,
            worker_id=worker_id,
            lease_token_hash=sha256(lease_token.encode("utf-8")).hexdigest(),
            status=HistoricalRangeDayStatus.CANCELLED.value,
            attempt_input_hash=input_hash,
            input_hash_kind="CLAIM_INPUT",
            previous_list_hash=context.previous_list_hash,
            previous_day_receipt_ref=context.previous_day_receipt_ref,
            stage="CANCEL",
            reason_codes=("ADVISORY_HR_EXPLICIT_CANCEL",),
            sanitized_error=error_json,
        )
        upstream = (context.request_ref,) + (
            (context.previous_day_receipt_ref,) if context.previous_day_receipt_ref is not None else ()
        )
        ref = self._day_executor.publish_day_attempt_receipt(
            resolved_request_hash=context.resolved_request_hash,
            range_run_id=context.range_run_id,
            day_run_id=context.day_run_id,
            payload=payload,
            upstream_refs=upstream,
        )
        now = datetime.now(UTC)
        return HistoricalRangeDayAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahrda",
                {"day_run_id": context.day_run_id, "attempt_no": attempt_no, "fencing": fencing_token},
            ),
            day_run_id=context.day_run_id,
            attempt_no=attempt_no,
            worker_id=worker_id,
            lease_token=lease_token,
            fencing_token=fencing_token,
            status=HistoricalRangeDayStatus.CANCELLED.value,
            input_hash=input_hash,
            result_hash=ref.semantic_content_hash,
            attempt_receipt_ref=ref,
            reason_codes=("ADVISORY_HR_EXPLICIT_CANCEL",),
            error_json=error_json,
            started_at=now,
            finished_at=now,
        )

    def _resume_recoverable_state(self, *, batch_id: str) -> None:
        batch = self._day_executor._repository.load_execution_batch(batch_id=batch_id)
        if batch.status in {HistoricalRangeBatchStatus.WAITING_INPUT, HistoricalRangeBatchStatus.PARTIAL}:
            self._day_executor._repository.transition_batch(
                batch_id=batch_id,
                expected_row_version=batch.row_version,
                target_status=HistoricalRangeBatchStatus.RUNNING,
            )
        runs = _list_all_execution_runs(repository=self._day_executor._repository, batch_id=batch_id)
        for run in runs:
            if run.final_receipt_ref is not None:
                continue
            facts = self._day_executor._repository.load_run_finalization_facts(
                range_run_id=run.range_run_id
            )
            if facts.blocking_status is HistoricalRangeDayStatus.FAILED:
                raise HistoricalRangeContractError(
                    "ADVISORY_HR_TERMINAL_RUN_CANNOT_RESUME",
                    "terminal failed/partial Program requires a superseding batch",
                    context={"range_run_id": run.range_run_id},
                )
            if run.status in {
                HistoricalRangeProgramStatus.WAITING_INPUT,
                HistoricalRangeProgramStatus.RETRYABLE_FAILED,
                HistoricalRangeProgramStatus.PARTIAL,
            }:
                self._day_executor._repository.transition_run(
                    range_run_id=run.range_run_id,
                    expected_row_version=run.row_version,
                    target_status=HistoricalRangeProgramStatus.RUNNING,
                )

    def _terminal_batch_result_from_run_receipts(
        self,
        *,
        batch_id: str,
    ) -> HistoricalRangeBatchExecutionResultV1 | None:
        batch = self._day_executor._repository.load_execution_batch(batch_id=batch_id)
        runs = _list_all_execution_runs(
            repository=self._day_executor._repository,
            batch_id=batch_id,
        )
        if not runs or any(run.final_receipt_ref is None for run in runs):
            return None
        if batch.status not in {
            HistoricalRangeBatchStatus.COMPLETED,
            HistoricalRangeBatchStatus.FAILED,
            HistoricalRangeBatchStatus.PARTIAL,
        }:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_TERMINAL_RUN_BATCH_STATE_MISMATCH",
                "all Program runs are terminal but the batch is not a terminal execution result",
                context={"batch_id": batch_id, "status": batch.status.value},
            )
        successful = 0
        failed = 0
        blocking: list[str] = []
        for run in runs:
            receipt = HistoricalRangeRunExecutionReceiptV1.model_validate(
                self._day_executor.load_range_receipt(run.final_receipt_ref)
            )
            if (
                receipt.range_run_id != run.range_run_id
                or receipt.research_program_id != run.research_program_id
                or receipt.status != run.status.value
                or run.final_receipt_hash != run.final_receipt_ref.semantic_content_hash
            ):
                raise HistoricalRangeContractError(
                    "ADVISORY_HR_TERMINAL_RUN_RECEIPT_MISMATCH",
                    "terminal Program receipt differs from its durable run row",
                    context={"batch_id": batch_id, "range_run_id": run.range_run_id},
                )
            successful += receipt.successful_day_count
            failed += receipt.failed_day_count
            if receipt.blocking_day_run_id is not None:
                blocking.append(receipt.blocking_day_run_id)
        return HistoricalRangeBatchExecutionResultV1(
            batch_id=batch_id,
            executed_day_count=successful + failed,
            successful_day_count=successful,
            waiting_day_count=0,
            retryable_day_count=0,
            failed_day_count=failed,
            blocking_day_run_ids=tuple(sorted(blocking)),
        )

    def _expired_operation_attempt(
        self,
        *,
        operation: HistoricalRangeExecutionOperationV1,
    ) -> HistoricalRangeOperationAttemptV1:
        if (
            operation.worker_id is None
            or operation.lease_token is None
            or operation.fencing_token is None
            or operation.lease_expires_at is None
        ):
            raise HistoricalRangeContractError(
                "ADVISORY_HR_OPERATION_LEASE_IDENTITY_MISSING",
                "expired operation lacks durable lease identity",
                context={"operation_id": operation.operation_id},
            )
        error_json = {
            "reason_codes": ["ADVISORY_HR_OPERATION_LEASE_EXPIRED"],
            "stage": "OPERATION_LEASE",
            "error_type": "TimeoutError",
        }
        payload = HistoricalRangeExecutionOperationAttemptReceiptV1(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            attempt_no=operation.attempt_no,
            fencing_token=operation.fencing_token,
            worker_id=operation.worker_id,
            lease_token_hash=sha256(operation.lease_token.encode("utf-8")).hexdigest(),
            input_hash=operation.idempotency_payload_hash,
            starting_batch_row_version=operation.expected_row_version,
            stable_cursor=operation.stable_keyset_cursor_json or {},
            reason_codes=("ADVISORY_HR_OPERATION_LEASE_EXPIRED",),
            sanitized_error=error_json,
            lease_expired_at=operation.lease_expires_at,
        )
        ref = self._day_executor.publish_range_receipt(
            payload_schema_version=EXECUTION_OPERATION_ATTEMPT_RECEIPT_SCHEMA_VERSION,
            resolved_request_hash=operation.resolved_request_hash,
            payload=payload.model_dump(mode="json"),
            upstream_refs=(),
        )
        return HistoricalRangeOperationAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahroa",
                {
                    "operation_id": operation.operation_id,
                    "attempt_no": operation.attempt_no,
                    "fencing_token": operation.fencing_token,
                },
            ),
            operation_id=operation.operation_id,
            attempt_no=operation.attempt_no,
            worker_id=operation.worker_id,
            lease_token=operation.lease_token,
            fencing_token=operation.fencing_token,
            status=HistoricalRangeOperationStatus.RETRYABLE_FAILED.value,
            input_cursor_json=operation.stable_keyset_cursor_json,
            result_cursor_json=operation.stable_keyset_cursor_json,
            input_hash=operation.idempotency_payload_hash,
            result_hash=ref.semantic_content_hash,
            attempt_receipt_ref=ref,
            reason_codes=("ADVISORY_HR_OPERATION_LEASE_EXPIRED",),
            error_json=error_json,
            started_at=operation.lease_expires_at - timedelta(seconds=1),
            finished_at=datetime.now(UTC),
        )

    def _finish_failed_operation(
        self,
        *,
        claimed: HistoricalRangeExecutionOperationV1,
        error: Exception,
    ) -> None:
        if claimed.worker_id is None or claimed.lease_token is None or claimed.fencing_token is None:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_OPERATION_LEASE_IDENTITY_MISSING",
                "failed operation lacks durable lease identity",
                context={"operation_id": claimed.operation_id},
            )
        error_json = {
            "reason_codes": ["ADVISORY_HR_OPERATION_EXECUTION_FAILED"],
            "stage": "EXECUTION",
            "error_type": type(error).__name__,
        }
        payload = HistoricalRangeExecutionOperationAttemptReceiptV1(
            operation_id=claimed.operation_id,
            operation_type=claimed.operation_type,
            attempt_no=claimed.attempt_no,
            fencing_token=claimed.fencing_token,
            worker_id=claimed.worker_id,
            lease_token_hash=sha256(claimed.lease_token.encode("utf-8")).hexdigest(),
            input_hash=claimed.idempotency_payload_hash,
            starting_batch_row_version=claimed.expected_row_version,
            stable_cursor=claimed.stable_keyset_cursor_json or {},
            reason_codes=("ADVISORY_HR_OPERATION_EXECUTION_FAILED",),
            sanitized_error=error_json,
        )
        ref = self._day_executor.publish_range_receipt(
            payload_schema_version=EXECUTION_OPERATION_ATTEMPT_RECEIPT_SCHEMA_VERSION,
            resolved_request_hash=claimed.resolved_request_hash,
            payload=payload.model_dump(mode="json"),
            upstream_refs=(),
        )
        now = datetime.now(UTC)
        attempt = HistoricalRangeOperationAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahroa",
                {
                    "operation_id": claimed.operation_id,
                    "attempt_no": claimed.attempt_no,
                    "fencing_token": claimed.fencing_token,
                },
            ),
            operation_id=claimed.operation_id,
            attempt_no=claimed.attempt_no,
            worker_id=claimed.worker_id,
            lease_token=claimed.lease_token,
            fencing_token=claimed.fencing_token,
            status=HistoricalRangeOperationStatus.RETRYABLE_FAILED.value,
            input_cursor_json=claimed.stable_keyset_cursor_json,
            result_cursor_json=claimed.stable_keyset_cursor_json,
            input_hash=claimed.idempotency_payload_hash,
            result_hash=ref.semantic_content_hash,
            attempt_receipt_ref=ref,
            reason_codes=("ADVISORY_HR_OPERATION_EXECUTION_FAILED",),
            error_json=error_json,
            started_at=now,
            finished_at=now,
        )
        self._day_executor._repository.finish_execution_operation_failure(
            claimed_operation=claimed,
            attempt=attempt,
        )

    def _finish_operation(
        self,
        *,
        claimed: HistoricalRangeExecutionOperationV1,
        payload_hash: str,
        result: HistoricalRangeBatchExecutionResultV1,
        started_at: datetime,
        cancelled_day_results: tuple[HistoricalRangeOperationCancelledDayResultV1, ...],
    ) -> HistoricalRangeBatchExecutionResultV1:
        batch = self._day_executor._repository.load_execution_batch(batch_id=claimed.batch_id)
        runs = _list_all_execution_runs(
            repository=self._day_executor._repository,
            batch_id=claimed.batch_id,
        )
        prior_refs = self._day_executor._repository.list_operation_attempt_receipt_refs(
            operation_id=claimed.operation_id
        )
        program_results = tuple(
            HistoricalRangeOperationProgramResultV1(
                range_run_id=run.range_run_id,
                research_program_id=run.research_program_id,
                status=run.status,
                row_version=run.row_version,
                final_receipt_ref=run.final_receipt_ref,
            )
            for run in sorted(runs, key=lambda item: item.research_program_id)
        )
        receipt = HistoricalRangeExecutionOperationReceiptV1(
            operation_id=claimed.operation_id,
            operation_type=claimed.operation_type,
            operation_idempotency_key=claimed.operation_idempotency_key,
            idempotency_payload_hash=payload_hash,
            attempt_no=claimed.attempt_no,
            fencing_token=int(claimed.fencing_token or 0),
            starting_batch_row_version=claimed.expected_row_version,
            ending_batch_row_version=batch.row_version,
            result_status=batch.status,
            executed_day_count=result.executed_day_count,
            successful_day_count=result.successful_day_count,
            waiting_day_count=result.waiting_day_count,
            retryable_day_count=result.retryable_day_count,
            failed_day_count=result.failed_day_count,
            blocking_day_run_ids=result.blocking_day_run_ids,
            program_results=program_results,
            cancelled_day_results=cancelled_day_results,
            prior_nonterminal_attempt_receipt_refs=prior_refs,
            stable_cursor={
                "research_program_ids": [item.research_program_id for item in program_results],
                "blocking_day_run_ids": list(result.blocking_day_run_ids),
                "cancelled_day_run_ids": [item.day_run_id for item in cancelled_day_results],
            },
        )
        upstream = tuple(
            item.final_receipt_ref for item in program_results if item.final_receipt_ref is not None
        ) + tuple(item.attempt_receipt_ref for item in cancelled_day_results) + prior_refs
        ref = self._day_executor.publish_range_receipt(
            payload_schema_version=EXECUTION_OPERATION_RECEIPT_SCHEMA_VERSION,
            resolved_request_hash=claimed.resolved_request_hash,
            payload=receipt.model_dump(mode="json"),
            upstream_refs=upstream,
        )
        attempt = HistoricalRangeOperationAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahroa",
                {
                    "operation_id": claimed.operation_id,
                    "attempt_no": claimed.attempt_no,
                    "fencing_token": claimed.fencing_token,
                },
            ),
            operation_id=claimed.operation_id,
            attempt_no=claimed.attempt_no,
            worker_id=str(claimed.worker_id),
            lease_token=str(claimed.lease_token),
            fencing_token=int(claimed.fencing_token or 0),
            status=HistoricalRangeOperationStatus.COMPLETED.value,
            input_hash=payload_hash,
            result_hash=ref.semantic_content_hash,
            attempt_receipt_ref=ref,
            result_cursor_json=receipt.stable_cursor,
            started_at=started_at,
            finished_at=datetime.now(UTC),
        )
        self._day_executor._repository.finish_execution_operation(
            claimed_operation=claimed,
            receipt=receipt,
            receipt_ref=ref,
            attempt=attempt,
        )
        return result

    def _load_completed_operation_result(
        self,
        *,
        operation: HistoricalRangeExecutionOperationV1,
        expected_payload_hash: str,
    ) -> HistoricalRangeBatchExecutionResultV1:
        if operation.result_ref is None:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_OPERATION_RESULT_MISSING",
                "completed execution operation has no immutable result ref",
                context={"operation_id": operation.operation_id},
            )
        receipt = HistoricalRangeExecutionOperationReceiptV1.model_validate(
            self._day_executor.load_range_receipt(operation.result_ref)
        )
        if (
            receipt.operation_id != operation.operation_id
            or receipt.idempotency_payload_hash != expected_payload_hash
        ):
            raise HistoricalRangeContractError(
                "ADVISORY_HR_OPERATION_RESULT_CONFLICT",
                "completed execution operation result differs from the exact retry payload",
                context={"operation_id": operation.operation_id},
            )
        return HistoricalRangeBatchExecutionResultV1(
            batch_id=operation.batch_id,
            executed_day_count=receipt.executed_day_count,
            successful_day_count=receipt.successful_day_count,
            waiting_day_count=receipt.waiting_day_count,
            retryable_day_count=receipt.retryable_day_count,
            failed_day_count=receipt.failed_day_count,
            blocking_day_run_ids=receipt.blocking_day_run_ids,
        )

class HistoricalRangeDayWaitingInput(HistoricalRangeContractError):
    def __init__(self, reason_code: str, message: str, *, reason_codes: tuple[str, ...] = ()) -> None:
        super().__init__(reason_code, message)
        self.reason_codes = tuple(sorted(reason_codes or (reason_code,)))


def _list_all_execution_runs(
    *,
    repository: HistoricalRangeExecutionRepository,
    batch_id: str,
) -> tuple[HistoricalRangeExecutionRunV1, ...]:
    """Read every Program through the repository's stable keyset contract."""

    page_size = 500
    stable_after: str | None = None
    rows: list[HistoricalRangeExecutionRunV1] = []
    while True:
        page = repository.list_execution_runs(
            batch_id=batch_id,
            stable_after_research_program_id=stable_after,
            limit=page_size,
        )
        if page and stable_after is not None and page[0].research_program_id <= stable_after:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_RUN_PAGINATION_NOT_STABLE",
                "execution repository returned a non-advancing Program page",
                context={"batch_id": batch_id, "stable_after_research_program_id": stable_after},
            )
        rows.extend(page)
        if len(page) < page_size:
            return tuple(rows)
        next_stable_after = page[-1].research_program_id
        if next_stable_after == stable_after:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_RUN_PAGINATION_NOT_STABLE",
                "execution repository Program cursor did not advance",
                context={"batch_id": batch_id, "stable_after_research_program_id": stable_after},
            )
        stable_after = next_stable_after


def _program_for_claim(*, request_payload: HistoricalRangeResolvedRequestArtifactPayloadV1, claim: HistoricalRangeClaimedDayV1):
    for program in request_payload.resolved_request.frozen_programs:
        if program.research_program_id == claim.research_program_id:
            return program
    raise HistoricalRangeContractError(
        "ADVISORY_HR_FROZEN_PROGRAM_NOT_FOUND",
        "claimed Program is absent from the sealed request artifact",
        context={"research_program_id": claim.research_program_id},
    )


def _validate_candidate_for_claim(
    *,
    claim: HistoricalRangeClaimedDayV1,
    request_payload: HistoricalRangeResolvedRequestArtifactPayloadV1,
    program: HistoricalRangeFrozenProgramV1,
    candidate_payload: HistoricalRangeCandidateArtifactPayloadV2,
) -> None:
    expected_input_hash = build_candidate_input_hash(
        range_run_id=claim.range_run_id,
        research_program_id=claim.research_program_id,
        decision_trade_date=claim.decision_trade_date,
        frozen_program_hash=str(program.frozen_program_hash),
        runtime_profile_hash=candidate_payload.runtime_profile_hash,
        code_release_hash=program.code_release_hash,
        selection_semantics_hash=program.selection_semantics_hash,
        calendar_identity_hash=request_payload.source_revision_catalog.calendar_identity_hash,
        universe_identity_hash=candidate_payload.universe_identity_hash,
        source_revision_catalog_hash=str(request_payload.source_revision_catalog.catalog_hash),
        query_contract_hash=request_payload.source_revision_catalog.query_contract_hash,
    )
    identity_matches = (
        candidate_payload.day_run_id == claim.day_run_id
        and candidate_payload.range_run_id == claim.range_run_id
        and candidate_payload.research_program_id == claim.research_program_id
        and candidate_payload.decision_trade_date == claim.decision_trade_date
        and candidate_payload.package_id == program.package_id
        and candidate_payload.package_version == program.package_version
        and candidate_payload.manifest_sha256 == program.manifest_sha256
        and candidate_payload.alpha_mode == program.alpha_mode
        and candidate_payload.candidate_input_hash == expected_input_hash
    )
    if not identity_matches:
        raise HistoricalRangeContractError(
            "ADVISORY_HR_CANDIDATE_CLAIM_IDENTITY_MISMATCH",
            "candidate artifact does not close the sealed Program/day identity",
            context={"day_run_id": claim.day_run_id},
        )


def _validate_predecessor_against_claim(*, claim: HistoricalRangeClaimedDayV1, predecessor: HistoricalRangePredecessorStateV1) -> None:
    if claim.ordinal == 1:
        if predecessor.list_version is not None or predecessor.day_receipt_ref is not None:
            raise HistoricalRangeContractError("ADVISORY_HR_FIRST_DAY_PREDECESSOR_INVALID", "first day has predecessor state")
        return
    if (
        predecessor.list_version is None
        or predecessor.day_receipt_ref != claim.previous_day_receipt_ref
        or predecessor.list_version.list_version_id != claim.previous_list_version_id
        or predecessor.list_version.list_content_hash != claim.previous_list_hash
    ):
        raise HistoricalRangeContractError(
            "ADVISORY_HR_PREDECESSOR_READBACK_MISMATCH",
            "predecessor list/receipt does not equal the claimed exact chain edge",
            context={"day_run_id": claim.day_run_id},
        )


def _load_candidate_payload(*, store: HistoricalRangeArtifactStore, ref: HistoricalRangeArtifactRefV1):
    from backend.services.advisory_historical_range.models import HistoricalRangeCandidateArtifactPayloadV2

    return HistoricalRangeCandidateArtifactPayloadV2.model_validate(store.load(ref).payload)


def _previous_marks(predecessor: HistoricalRangePredecessorStateV1):
    from backend.services.advisory_historical_range.models import HistoricalRangeEpisodeMarkV2

    return {
        episode.symbol: HistoricalRangeEpisodeMarkV2.model_validate(episode.mark_json)
        for episode in predecessor.active_episodes
    }


def _next_trade_date(request_payload: HistoricalRangeResolvedRequestArtifactPayloadV1, claim: HistoricalRangeClaimedDayV1):
    dates = request_payload.resolved_request.date_plan.ordered_trade_dates
    return dates[claim.ordinal] if claim.ordinal < len(dates) else None


def _decision_cutoff(decision_trade_date):
    return datetime.combine(
        decision_trade_date,
        time(hour=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    ).astimezone(UTC)


def _failure_input_hash(
    *,
    claim: HistoricalRangeClaimedDayV1,
    input_hash_kind: str,
    candidate_ref: HistoricalRangeArtifactRefV1 | None,
    mark_ref: HistoricalRangeArtifactRefV1 | None,
) -> str:
    return canonical_json_sha256(
        {
            "schema_version": "advisory_historical_range_failure_input_v1",
            "input_hash_kind": input_hash_kind,
            "resolved_request_hash": claim.resolved_request_hash,
            "range_run_id": claim.range_run_id,
            "day_run_id": claim.day_run_id,
            "ordinal": claim.ordinal,
            "candidate_ref": candidate_ref.model_dump(mode="json") if candidate_ref else None,
            "decision_mark_ref": mark_ref.model_dump(mode="json") if mark_ref else None,
            "previous_list_hash": claim.previous_list_hash,
            "previous_day_receipt_ref": (
                claim.previous_day_receipt_ref.model_dump(mode="json") if claim.previous_day_receipt_ref else None
            ),
            "list_semantics_version": claim.list_semantics_version,
            "list_semantics_hash": claim.list_semantics_hash,
        }
    )


def _classify_failure(exc: Exception) -> tuple[HistoricalRangeDayStatus, tuple[str, ...]]:
    if isinstance(exc, HistoricalRangeDayWaitingInput):
        return HistoricalRangeDayStatus.WAITING_INPUT, exc.reason_codes
    if isinstance(exc, HistoricalRangeContractError):
        return HistoricalRangeDayStatus.FAILED, (exc.reason_code,)
    if isinstance(exc, TradingCoreError):
        context_reason = str(exc.context.get("reason_code") or "").strip()
        reason_code = context_reason or f"ADVISORY_HR_{exc.error_code}"
        if isinstance(
            exc,
            (
                DataUnavailableError,
                HMMRuntimeUnavailableError,
                MarketDataUnavailableError,
                TradingCalendarUnavailableError,
            ),
        ):
            return HistoricalRangeDayStatus.WAITING_INPUT, (reason_code,)
        if isinstance(
            exc,
            (
                PackageAssetInvalidError,
                RuntimeConfigInvalidError,
                StrategyPackageValidationError,
                UnsupportedFeatureError,
            ),
        ):
            return HistoricalRangeDayStatus.FAILED, (reason_code,)
        return HistoricalRangeDayStatus.RETRYABLE_FAILED, (reason_code,)
    if isinstance(exc, (ValidationError, ValueError, TypeError, AssertionError)):
        return HistoricalRangeDayStatus.FAILED, ("ADVISORY_HR_DETERMINISTIC_CONTRACT_FAILURE",)
    if isinstance(exc, postgres_errors.DiskFull):
        return HistoricalRangeDayStatus.RETRYABLE_FAILED, ("ADVISORY_HR_DATABASE_CAPACITY_EXHAUSTED",)
    if isinstance(exc, PostgresOperationalError):
        return HistoricalRangeDayStatus.RETRYABLE_FAILED, ("ADVISORY_HR_DATABASE_OPERATIONAL_FAILURE",)
    if isinstance(exc, (ConnectionError, OSError, TimeoutError)):
        return HistoricalRangeDayStatus.RETRYABLE_FAILED, ("ADVISORY_HR_TRANSIENT_INFRASTRUCTURE_FAILURE",)
    return HistoricalRangeDayStatus.RETRYABLE_FAILED, ("ADVISORY_HR_DAY_UNCLASSIFIED_FAILURE",)


def _batch_result(
    *,
    batch_id: str,
    results: list[HistoricalRangeDayExecutionResultV1],
) -> HistoricalRangeBatchExecutionResultV1:
    return HistoricalRangeBatchExecutionResultV1(
        batch_id=batch_id,
        executed_day_count=len(results),
        successful_day_count=sum(
            item.status in {HistoricalRangeDayStatus.COMPLETE, HistoricalRangeDayStatus.VALID_NO_CANDIDATE}
            for item in results
        ),
        waiting_day_count=sum(item.status is HistoricalRangeDayStatus.WAITING_INPUT for item in results),
        retryable_day_count=sum(item.status is HistoricalRangeDayStatus.RETRYABLE_FAILED for item in results),
        failed_day_count=sum(item.status is HistoricalRangeDayStatus.FAILED for item in results),
        blocking_day_run_ids=tuple(
            sorted(
                item.day_run_id
                for item in results
                if item.status
                in {
                    HistoricalRangeDayStatus.WAITING_INPUT,
                    HistoricalRangeDayStatus.RETRYABLE_FAILED,
                    HistoricalRangeDayStatus.FAILED,
                    HistoricalRangeDayStatus.CANCELLED,
                }
            )
        ),
    )
