"""Atomic Phase 1G G3 writer for one frozen historical advisory target."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Callable

import psycopg2
import psycopg2.extras

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.capture_foundation import (
    CaptureMembership,
    CapturePlan,
    PostgresCaptureBatchRepository,
)
from backend.services.advisory_phase1.control_binding import (
    PostgresControlBindingRepository,
)
from backend.services.advisory_phase1.observation_capture import (
    Phase1GObservationRowBundle,
    Phase1GObservationSemanticDraft,
    materialize_observation_row_bundle,
)
from backend.services.advisory_phase1.observation_capture_postgres import (
    PostgresObservationCaptureRepository,
)
from backend.services.advisory_phase1.phase1g_contract import (
    Phase1GStageEvidenceCommitRef,
    Phase1GTargetCommitProjection,
    Phase1GTransactionalWriteRequest,
    REASON_G3_BATCH_NOT_RUNNING,
    REASON_G3_BATCH_ROW_VERSION_CONFLICT,
    REASON_G3_CAPACITY_EXCEEDED,
    REASON_G3_CAPTURE_PLAN_CONFLICT,
    REASON_G3_CHILD_ROW_CONFLICT,
    REASON_G3_COMMIT_FAILED,
    REASON_G3_COMMIT_STATE_UNKNOWN,
    REASON_G3_CONTROL_BINDING_CONFLICT,
    REASON_G3_DELIVERY_CONFLICT,
    REASON_G3_FENCING_INVALID,
    REASON_G3_INPUT_INVALID,
    REASON_G3_LEASE_EXPIRED,
    REASON_G3_MEMBERSHIP_CONFLICT,
    REASON_G3_OBSERVATION_CONFLICT,
    REASON_G3_POST_COMMIT_VERIFY_FAILED,
    REASON_G3_SCHEMA_NOT_READY,
    REASON_G3_SOURCE_REVISION_CONFLICT,
    REASON_G3_TRACE_OUTBOX_CONFLICT,
    REASON_G3_UNEXPECTED_ERROR,
)
from backend.services.advisory_phase1.phase1g_historical_trace_contract import (
    Phase1GTargetProjectionSnapshot,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.source_revision_postgres import (
    PostgresSourceRevisionRepository,
)
from backend.services.advisory_phase1.stage_trace import (
    StageTraceEnvelope,
    TraceCaptureBinding,
    TraceCaptureContext,
)
from backend.services.advisory_phase1.trace_outbox import (
    PostgresTraceOutboxRepository,
    TraceDeliveryEvent,
    TraceDeliveryEventRequest,
    TraceDeliveryEventType,
    TraceOutboxRecord,
)


logger = logging.getLogger(__name__)

TransactionConnectionFactory = Callable[[], Any]


class Phase1GTransactionalWriterError(RuntimeError):
    def __init__(
        self, reason_code: str, message: str, *, context: dict[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = canonicalize(context or {})


@dataclass(frozen=True)
class Phase1GTransactionalTargetInput:
    request: Phase1GTransactionalWriteRequest
    target_snapshot: Phase1GTargetProjectionSnapshot
    capture_plan: CapturePlan
    trace_context: TraceCaptureContext
    persisted_binding: TraceCaptureBinding
    current_writer_binding: TraceCaptureBinding
    envelope: StageTraceEnvelope
    semantic_draft: Phase1GObservationSemanticDraft


@dataclass(frozen=True)
class _CommittedTargetFacts:
    source_revision_set: Any
    outbox: TraceOutboxRecord
    observation: Phase1GObservationRowBundle
    memberships: tuple[CaptureMembership, ...]
    delivery: TraceDeliveryEvent


class Phase1GTransactionalWriter:
    """Perform one bounded transaction and verify its committed facts."""

    def __init__(
        self,
        *,
        transaction_connection_factory: TransactionConnectionFactory,
        readonly_connection_factory: TransactionConnectionFactory,
    ) -> None:
        self._transaction_connection_factory = transaction_connection_factory
        self._readonly_connection_factory = readonly_connection_factory
        self._capture_repository = PostgresCaptureBatchRepository()
        self._source_repository = PostgresSourceRevisionRepository()
        self._outbox_repository = PostgresTraceOutboxRepository()
        self._observation_repository = PostgresObservationCaptureRepository()

    def write_target(
        self, target: Phase1GTransactionalTargetInput
    ) -> Phase1GTargetCommitProjection:
        self._validate_input(target)
        request = target.request
        logger.info(
            "phase1g g3 target transaction started",
            extra={
                "target_request_hash_prefix": request.target_request_hash[:12],
                "capture_batch_id": request.capture_batch_id,
                "capture_plan_hash_prefix": request.capture_plan_hash[:12],
            },
        )
        conn = self._transaction_connection_factory()
        committed = False
        commit_error: Exception | None = None
        try:
            if bool(getattr(conn, "autocommit", False)):
                raise Phase1GTransactionalWriterError(
                    REASON_G3_INPUT_INVALID,
                    "transaction connection must have autocommit disabled",
                )
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                self._write_in_transaction(cur, target)
            try:
                conn.commit()
                committed = True
            except (
                Exception
            ) as exc:  # commit state is intentionally classified on a new connection.
                commit_error = exc
        except Exception as exc:
            if not committed and commit_error is None:
                try:
                    conn.rollback()
                except Exception:
                    logger.exception(
                        "phase1g g3 pre-commit rollback failed",
                        extra={"capture_batch_id": request.capture_batch_id},
                    )
            raise self._map_error(exc) from exc
        finally:
            try:
                conn.close()
            except Exception:
                logger.exception(
                    "phase1g g3 transaction connection close failed",
                    extra={"capture_batch_id": request.capture_batch_id},
                )
        if commit_error is not None:
            return self._classify_unknown_commit(target, commit_error)
        try:
            projection = self._read_committed_projection(target)
        except Exception as exc:
            logger.error(
                "phase1g g3 post-commit verification failed",
                extra={
                    "capture_batch_id": request.capture_batch_id,
                    "reason_code": getattr(
                        exc, "reason_code", REASON_G3_POST_COMMIT_VERIFY_FAILED
                    ),
                    "exception_type": type(exc).__name__,
                },
                exc_info=True,
            )
            raise Phase1GTransactionalWriterError(
                REASON_G3_POST_COMMIT_VERIFY_FAILED,
                "committed target facts failed full readback",
                context={"capture_batch_id": request.capture_batch_id},
            ) from exc
        logger.info(
            "phase1g g3 target transaction committed",
            extra={
                "target_request_hash_prefix": request.target_request_hash[:12],
                "capture_batch_id": request.capture_batch_id,
                "observation_content_hash_prefix": projection.observation_content_hash[
                    :12
                ],
            },
        )
        return projection

    def _write_in_transaction(
        self, cur: Any, target: Phase1GTransactionalTargetInput
    ) -> _CommittedTargetFacts:
        request = target.request
        batch = self._capture_repository.lock_running_in_transaction(
            cur,
            capture_batch_id=request.capture_batch_id,
            capture_request_hash=request.capture_request_hash,
            expected_row_version=request.expected_batch_row_version,
            fencing_token=request.capture_fencing_token,
        )
        persisted_plan = self._capture_repository.read_plan_exact_in_transaction(
            cur,
            capture_batch_id=request.capture_batch_id,
            plan_hash=request.capture_plan_hash,
        )
        if persisted_plan != target.capture_plan:
            raise Phase1GTransactionalWriterError(
                REASON_G3_CAPTURE_PLAN_CONFLICT,
                "persisted capture plan differs from writer input",
            )
        binding_event = PostgresControlBindingRepository.read_exact_in_transaction(
            cur, request.control_binding_event_hash
        )
        if (
            binding_event.binding_event_hash
            != target.current_writer_binding.control_binding_event_hash
            or not binding_event.request.enabled
        ):
            raise Phase1GTransactionalWriterError(
                REASON_G3_CONTROL_BINDING_CONFLICT,
                "current trace control binding is disabled or divergent",
            )
        freeze = target.target_snapshot.source_revision_freeze_intent
        source_set = self._source_repository.freeze_in_transaction(
            cur, freeze.source_revision_set
        )
        outbox = self._outbox_repository.append_in_transaction(
            cur,
            envelope=target.envelope,
            persisted_binding=target.persisted_binding,
            current_writer_binding=target.current_writer_binding,
        )
        self._observation_repository.lock_signal_in_transaction(
            cur, target.semantic_draft.canonical_signal_id
        )
        chain = self._observation_repository.read_revision_chain_exact_in_transaction(
            cur, target.semantic_draft.canonical_signal_id
        )
        observation = self._select_or_append_observation(cur, target, chain)
        current_row_version = batch.row_version
        expected_memberships = _expected_memberships(source_set, outbox, observation)
        for membership in expected_memberships:
            batch = self._capture_repository.add_membership_in_transaction(
                cur,
                capture_batch_id=request.capture_batch_id,
                expected_row_version=current_row_version,
                fencing_token=request.capture_fencing_token,
                membership=membership,
            )
            current_row_version = batch.row_version
        delivery = self._ensure_written_delivery(cur, target, observation)
        facts = self._read_facts_in_transaction(
            cur, target, observation.semantic_observation_key
        )
        if facts.memberships != expected_memberships or facts.delivery != delivery:
            raise Phase1GTransactionalWriterError(
                REASON_G3_CHILD_ROW_CONFLICT,
                "in-transaction full readback differs from written target facts",
            )
        return facts

    def _select_or_append_observation(
        self,
        cur: Any,
        target: Phase1GTransactionalTargetInput,
        chain: tuple[dict[str, Any], ...],
    ) -> Phase1GObservationRowBundle:
        for version in chain:
            expected = materialize_observation_row_bundle(
                draft=target.semantic_draft,
                observation_revision_no=int(version["observation_revision_no"]),
                supersedes_observation_version_id=version[
                    "supersedes_observation_version_id"
                ],
                created_by_capture_batch_id=str(version["created_by_capture_batch_id"]),
            )
            if (
                expected.observation_version["observation_content_hash"]
                != version["observation_content_hash"]
            ):
                continue
            persisted = self._observation_repository.read_observation_bundle_exact_in_transaction(
                cur,
                observation_version_id=str(version["observation_version_id"]),
                semantic_observation_key=target.semantic_draft.semantic_observation_key,
            )
            if _bundle_payload(persisted) != _bundle_payload(expected):
                raise Phase1GTransactionalWriterError(
                    REASON_G3_CHILD_ROW_CONFLICT,
                    "exact observation revision has divergent child rows",
                )
            return persisted
        predecessor = str(chain[-1]["observation_version_id"]) if chain else None
        bundle = materialize_observation_row_bundle(
            draft=target.semantic_draft,
            observation_revision_no=len(chain) + 1,
            supersedes_observation_version_id=predecessor,
            created_by_capture_batch_id=target.request.capture_batch_id,
        )
        return self._observation_repository.append_materialized_bundle_in_transaction(
            cur, row_bundle=bundle
        )

    def _ensure_written_delivery(
        self,
        cur: Any,
        target: Phase1GTransactionalTargetInput,
        observation: Phase1GObservationRowBundle,
    ) -> TraceDeliveryEvent:
        outbox_id = target.envelope.trace_outbox_id
        expected_payload = {
            "trace_outbox_id": outbox_id,
            "observation_version_id": observation.observation_version[
                "observation_version_id"
            ],
            "observation_content_hash": observation.observation_version[
                "observation_content_hash"
            ],
        }
        chain = self._outbox_repository.read_delivery_chain_exact_in_transaction(
            cur, outbox_id
        )
        written = tuple(
            event
            for event in chain
            if event.request.event_type is TraceDeliveryEventType.OBSERVATION_WRITTEN
        )
        if written:
            event = written[0]
            if canonicalize(event.request.payload) != canonicalize(expected_payload):
                raise Phase1GTransactionalWriterError(
                    REASON_G3_DELIVERY_CONFLICT,
                    "terminal delivery references a different observation",
                )
            return event
        predecessor = chain[-1] if chain else None
        request = TraceDeliveryEventRequest(
            trace_outbox_id=outbox_id,
            delivery_event_no=len(chain) + 1,
            event_type=TraceDeliveryEventType.OBSERVATION_WRITTEN,
            predecessor_event_hash=(
                predecessor.delivery_event_hash if predecessor is not None else None
            ),
            writer_attempt_no=target.request.capture_attempt_no,
            payload=expected_payload,
        )
        return self._outbox_repository.append_delivery_in_transaction(cur, request)

    def _read_facts_in_transaction(
        self,
        cur: Any,
        target: Phase1GTransactionalTargetInput,
        semantic_observation_key: str,
        *,
        readonly: bool = False,
    ) -> _CommittedTargetFacts:
        freeze = target.target_snapshot.source_revision_freeze_intent
        source_read = (
            self._source_repository.read_exact_readonly
            if readonly
            else self._source_repository.read_exact_in_transaction
        )
        outbox_read = (
            self._outbox_repository.read_exact_by_hash_readonly
            if readonly
            else self._outbox_repository.read_exact_by_hash_in_transaction
        )
        observation_read = (
            self._observation_repository.read_observation_bundle_exact_readonly
            if readonly
            else self._observation_repository.read_observation_bundle_exact_in_transaction
        )
        memberships_read = (
            self._capture_repository.read_memberships_exact_readonly
            if readonly
            else self._capture_repository.read_memberships_exact_in_transaction
        )
        delivery_read = (
            self._outbox_repository.read_delivery_chain_exact_readonly
            if readonly
            else self._outbox_repository.read_delivery_chain_exact_in_transaction
        )
        source_set = source_read(
            cur, freeze.source_revision_set.source_revision_set_hash
        )
        outbox = outbox_read(cur, target.envelope.trace_content_hash)
        observation_id = self._observation_id_for_target(
            cur, target, semantic_observation_key, readonly=readonly
        )
        observation = observation_read(
            cur,
            observation_version_id=observation_id,
            semantic_observation_key=semantic_observation_key,
        )
        expected = _expected_memberships(source_set, outbox, observation)
        all_memberships = memberships_read(cur, target.request.capture_batch_id)
        indexed = {membership.content_key: membership for membership in all_memberships}
        actual = tuple(indexed.get(membership.content_key) for membership in expected)
        if any(item is None for item in actual) or tuple(actual) != expected:
            missing_count = sum(item is None for item in actual)
            raise Phase1GTransactionalWriterError(
                REASON_G3_MEMBERSHIP_CONFLICT,
                "current capture batch target memberships are missing or divergent",
                context={"missing_target_memberships": missing_count},
            )
        delivery_chain = delivery_read(cur, target.envelope.trace_outbox_id)
        delivery = next(
            (
                event
                for event in delivery_chain
                if event.request.event_type
                is TraceDeliveryEventType.OBSERVATION_WRITTEN
                and str(event.request.payload.get("observation_version_id") or "")
                == observation_id
                and str(event.request.payload.get("observation_content_hash") or "")
                == observation.observation_version["observation_content_hash"]
            ),
            None,
        )
        if delivery is None:
            raise Phase1GTransactionalWriterError(
                REASON_G3_DELIVERY_CONFLICT,
                "terminal observation delivery is missing or divergent",
            )
        return _CommittedTargetFacts(
            source_revision_set=source_set,
            outbox=outbox,
            observation=observation,
            memberships=expected,
            delivery=delivery,
        )

    @staticmethod
    def _observation_id_for_target(
        cur: Any,
        target: Phase1GTransactionalTargetInput,
        semantic_observation_key: str,
        *,
        readonly: bool = False,
    ) -> str:
        lock_clause = "" if readonly else "FOR KEY SHARE"
        cur.execute(
            f"""
            SELECT observation_version_id, observation_revision_no,
                   supersedes_observation_version_id, created_by_capture_batch_id,
                   observation_content_hash
            FROM app.advisory_signal_observation_version
            WHERE canonical_signal_id = %s
            ORDER BY observation_revision_no
            {lock_clause}
            """,
            (target.semantic_draft.canonical_signal_id,),
        )
        matches = []
        for row in cur.fetchall():
            expected = materialize_observation_row_bundle(
                draft=target.semantic_draft,
                observation_revision_no=int(row["observation_revision_no"]),
                supersedes_observation_version_id=(
                    str(row["supersedes_observation_version_id"])
                    if row["supersedes_observation_version_id"]
                    else None
                ),
                created_by_capture_batch_id=str(row["created_by_capture_batch_id"]),
            )
            if (
                expected.semantic_observation_key == semantic_observation_key
                and expected.observation_version["observation_content_hash"]
                == str(row["observation_content_hash"])
            ):
                matches.append(str(row["observation_version_id"]))
        if len(matches) != 1:
            raise Phase1GTransactionalWriterError(
                REASON_G3_OBSERVATION_CONFLICT,
                "target semantic observation does not resolve to exactly one revision",
                context={"match_count": len(matches)},
            )
        return matches[0]

    def _read_committed_projection(
        self, target: Phase1GTransactionalTargetInput
    ) -> Phase1GTargetCommitProjection:
        conn = self._readonly_connection_factory()
        try:
            if hasattr(conn, "set_session"):
                conn.set_session(readonly=True, autocommit=True)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                facts = self._read_facts_in_transaction(
                    cur,
                    target,
                    target.semantic_draft.semantic_observation_key,
                    readonly=True,
                )
            return self._projection_from_facts(target, facts)
        finally:
            conn.close()

    def _classify_unknown_commit(
        self,
        target: Phase1GTransactionalTargetInput,
        commit_error: Exception,
    ) -> Phase1GTargetCommitProjection:
        try:
            projection = self._read_committed_projection(target)
        except Exception as readback_error:
            try:
                membership_state = self._probe_target_membership_state(target)
            except Exception as probe_error:
                raise Phase1GTransactionalWriterError(
                    REASON_G3_COMMIT_STATE_UNKNOWN,
                    "database commit outcome cannot be probed from current batch facts",
                    context={"capture_batch_id": target.request.capture_batch_id},
                ) from probe_error
            if membership_state == "NOT_COMMITTED":
                raise Phase1GTransactionalWriterError(
                    REASON_G3_COMMIT_FAILED,
                    "database commit failed and current target facts were not committed",
                    context={"capture_batch_id": target.request.capture_batch_id},
                ) from commit_error
            raise Phase1GTransactionalWriterError(
                REASON_G3_COMMIT_STATE_UNKNOWN,
                "database commit outcome cannot be confirmed from complete target facts",
                context={"capture_batch_id": target.request.capture_batch_id},
            ) from readback_error
        logger.info(
            "phase1g g3 commit response lost but committed facts were verified",
            extra={"capture_batch_id": target.request.capture_batch_id},
        )
        return projection

    def _probe_target_membership_state(
        self, target: Phase1GTransactionalTargetInput
    ) -> str:
        """Classify only current-batch target residue after a lost commit response."""

        conn = self._readonly_connection_factory()
        try:
            if hasattr(conn, "set_session"):
                conn.set_session(readonly=True, autocommit=True)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                memberships = self._capture_repository.read_memberships_exact_readonly(
                    cur, target.request.capture_batch_id
                )
                expected_source = (
                    "SOURCE_REVISION_SET",
                    target.target_snapshot.source_revision_freeze_intent.source_revision_set.source_revision_set_id,
                )
                expected_outbox = ("TRACE_OUTBOX", target.envelope.trace_outbox_id)
                expected_observation_ids = self._matching_observation_ids_readonly(
                    cur, target
                )
            keys = {(item.evidence_role, item.evidence_id) for item in memberships}
            matched = int(expected_source in keys) + int(expected_outbox in keys)
            matched += sum(
                1
                for observation_id in expected_observation_ids
                if ("OBSERVATION_VERSION", observation_id) in keys
            )
            if matched == 0:
                return "NOT_COMMITTED"
            if matched == 3 and len(expected_observation_ids) == 1:
                return "COMMITTED"
            return "UNKNOWN"
        finally:
            conn.close()

    @staticmethod
    def _matching_observation_ids_readonly(
        cur: Any, target: Phase1GTransactionalTargetInput
    ) -> tuple[str, ...]:
        cur.execute(
            """
            SELECT observation_version_id, observation_revision_no,
                   supersedes_observation_version_id, created_by_capture_batch_id,
                   observation_content_hash
            FROM app.advisory_signal_observation_version
            WHERE canonical_signal_id = %s
            ORDER BY observation_revision_no
            """,
            (target.semantic_draft.canonical_signal_id,),
        )
        matches = []
        for row in cur.fetchall():
            expected = materialize_observation_row_bundle(
                draft=target.semantic_draft,
                observation_revision_no=int(row["observation_revision_no"]),
                supersedes_observation_version_id=(
                    str(row["supersedes_observation_version_id"])
                    if row["supersedes_observation_version_id"]
                    else None
                ),
                created_by_capture_batch_id=str(row["created_by_capture_batch_id"]),
            )
            if expected.observation_version["observation_content_hash"] == str(
                row["observation_content_hash"]
            ):
                matches.append(str(row["observation_version_id"]))
        return tuple(matches)

    @staticmethod
    def _projection_from_facts(
        target: Phase1GTransactionalTargetInput,
        facts: _CommittedTargetFacts,
    ) -> Phase1GTargetCommitProjection:
        freeze = target.target_snapshot.source_revision_freeze_intent
        observation = facts.observation
        candidate_hashes = tuple(
            str(row["candidate_content_hash"])
            for row in observation.candidate_payload_rows
        )
        membership_payload = [
            item.model_dump(mode="json") for item in facts.memberships
        ]
        readback_payload = {
            "source_revision_set": facts.source_revision_set.model_dump(mode="json"),
            "outbox": {
                "trace_outbox_id": facts.outbox.trace_outbox_id,
                "trace_content_hash": facts.outbox.envelope.trace_content_hash,
                "binding_hash": facts.outbox.binding.binding_hash,
            },
            "observation": _bundle_payload(observation),
            "memberships": membership_payload,
            "delivery": facts.delivery.model_dump(mode="json"),
        }
        return Phase1GTargetCommitProjection(
            target_request_hash=target.request.target_request_hash,
            target_plan_hash=target.request.capture_plan_hash,
            capture_batch_id=target.request.capture_batch_id,
            capture_request_hash=target.request.capture_request_hash,
            capture_attempt_no=target.request.capture_attempt_no,
            capture_fencing_token=target.request.capture_fencing_token,
            source_revision_set_id=facts.source_revision_set.source_revision_set_id,
            source_revision_set_hash=facts.source_revision_set.source_revision_set_hash,
            source_revision_member_count=freeze.expected_member_count,
            source_revision_member_hash=freeze.expected_member_hash,
            control_binding_event_hash=target.request.control_binding_event_hash,
            trace_outbox_id=facts.outbox.trace_outbox_id,
            trace_content_hash=facts.outbox.envelope.trace_content_hash,
            canonical_signal_id=observation.canonical_signal_header[
                "canonical_signal_id"
            ],
            observation_version_id=observation.observation_version[
                "observation_version_id"
            ],
            observation_content_hash=observation.observation_version[
                "observation_content_hash"
            ],
            observation_revision_no=observation.observation_version[
                "observation_revision_no"
            ],
            lineage_id=observation.lineage_identity["lineage_id"],
            lineage_content_hash=observation.lineage_identity["lineage_content_hash"],
            stage_evidence_refs=tuple(
                Phase1GStageEvidenceCommitRef(
                    stage=row["stage"],
                    stage_evidence_id=row["stage_evidence_id"],
                    content_hash=row["content_hash"],
                )
                for row in observation.stage_evidence_rows
            ),
            candidate_count=len(candidate_hashes),
            candidate_set_hash=canonical_json_sha256(candidate_hashes),
            target_membership_hash=canonical_json_sha256(membership_payload),
            delivery_event_id=facts.delivery.delivery_event_id,
            delivery_event_hash=facts.delivery.delivery_event_hash,
            post_commit_readback_hash=canonical_json_sha256(readback_payload),
        )

    @staticmethod
    def _validate_input(target: Phase1GTransactionalTargetInput) -> None:
        request = target.request
        snapshot = target.target_snapshot
        plan = target.capture_plan
        context = target.trace_context
        binding = target.current_writer_binding
        if (
            request.target_request_hash != snapshot.target_request_hash
            or request.g2_target_projection_snapshot_hash
            != snapshot.target_projection_snapshot_hash
            or request.phase1e_plan_id
            != snapshot.source_operation_projection.phase1e_plan_id
            or request.phase1e_plan_hash
            != snapshot.source_operation_projection.phase1e_plan_hash
            or request.capture_plan_hash != plan.plan_hash
            or request.control_binding_event_hash != binding.control_binding_event_hash
            or request.capture_batch_id != binding.capture_batch_id
            or request.capture_fencing_token != binding.capture_fencing_token
            or request.trace_capture_context_hash
            != canonical_json_sha256(context.model_dump(mode="json"))
            or request.trace_capture_binding_hash != binding.binding_hash
            or request.trace_outbox_id != target.envelope.trace_outbox_id
            or request.stage_trace_envelope_hash != target.envelope.trace_content_hash
            or request.observation_semantic_key
            != target.semantic_draft.semantic_observation_key
            or request.observation_semantic_draft_hash
            != target.semantic_draft.draft_content_hash
        ):
            raise Phase1GTransactionalWriterError(
                REASON_G3_INPUT_INVALID,
                "transactional writer identities do not close",
            )
        freeze = snapshot.source_revision_freeze_intent
        operation = snapshot.source_operation_projection
        historical = snapshot.historical_trace_projection
        source_refs = tuple(
            item
            for item in operation.expected_capture_source_sets
            if item.capture_plan_hash == request.capture_plan_hash
        )
        if len(source_refs) != 1:
            raise Phase1GTransactionalWriterError(
                REASON_G3_INPUT_INVALID,
                "capture plan does not resolve to exactly one frozen source-set reference",
                context={"matching_source_ref_count": len(source_refs)},
            )
        source_ref = source_refs[0]
        lineage = historical.dse.evidence.phase0a_candidate_lineage
        clock = historical.dse.evidence.decision_clock
        chain = historical.dse.evidence.phase0a_effective_config_chain
        if (
            plan.signal_source_revision_set_id
            != freeze.source_revision_set.source_revision_set_id
            or plan.signal_source_revision_set_hash
            != freeze.source_revision_set.source_revision_set_hash
            or source_ref.source_revision_set_id
            != freeze.source_revision_set.source_revision_set_id
            or source_ref.source_revision_set_hash
            != freeze.source_revision_set.source_revision_set_hash
            or context.binding != binding
            or context.selection_run_id != plan.selection_run_id
            or context.package_id != plan.package_id
            or context.manifest_sha256 != plan.manifest_sha256
            or context.decision_as_of_trade_date.isoformat()
            != plan.decision_as_of_trade_date
            or target.semantic_draft.canonical_signal_header["package_id"]
            != plan.package_id
            or operation.package_id != plan.package_id
            or operation.manifest_sha256 != plan.manifest_sha256
            or operation.alpha_mode != plan.alpha_mode
            or operation.decision_trade_date.isoformat()
            != plan.decision_as_of_trade_date
            or operation.program_id != plan.program_id
            or operation.admission_scope_id != plan.admission_scope_id
            or operation.admission_scope_hash != plan.admission_scope_hash
            or historical.dse.package_id != plan.package_id
            or historical.dse.manifest_sha256 != plan.manifest_sha256
            or historical.package_manifest.alpha_mode != plan.alpha_mode
            or historical.dse.evidence_id != plan.selection_evidence_id
            or historical.dse.artifact_hash != plan.selection_evidence_hash
            or historical.artifact.artifact_id != plan.selection_score_artifact_id
            or historical.artifact.artifact_payload_sha256
            != plan.selection_score_artifact_hash
            or historical.dse.runtime_profile_version_id
            != plan.runtime_profile_version_id
            or historical.dse.runtime_profile_hash != plan.runtime_profile_version_hash
            or str(lineage["selection_run_id"]) != plan.selection_run_id
            or canonical_json_sha256(lineage) != plan.selection_run_content_hash
            or clock.decision_as_of_trade_date.isoformat()
            != plan.decision_as_of_trade_date
            or clock.selection_as_of_trade_date.isoformat()
            != plan.selection_as_of_trade_date
            or clock.target_trade_date.isoformat() != plan.target_trade_date
            or clock.calendar_version != plan.calendar_version
            or clock.calendar_hash != plan.calendar_hash
            or canonical_json_sha256(
                historical.stage_trace_builder_input.runtime_config
            )
            != plan.selection_runtime_semantics_hash
            or chain.package_effective_config_hash != plan.package_effective_config_hash
            or historical.candidate_outcome
            != (
                "VALID_NO_CANDIDATE"
                if plan.valid_no_candidate
                else "CANDIDATES_PRESENT"
            )
        ):
            raise Phase1GTransactionalWriterError(
                REASON_G3_INPUT_INVALID,
                "snapshot, plan, context, source, and observation identities diverge",
            )
        minimum_rows = 4 + 5 + 2 * len(target.semantic_draft.candidate_semantic_rows)
        if request.expected_rows < minimum_rows or request.expected_bytes < max(
            snapshot.projected_bytes, target.envelope.size_bytes
        ):
            raise Phase1GTransactionalWriterError(
                REASON_G3_CAPACITY_EXCEEDED,
                "transactional writer capacity declaration is incomplete",
            )

    @staticmethod
    def _map_error(exc: Exception) -> Phase1GTransactionalWriterError:
        if isinstance(exc, Phase1GTransactionalWriterError):
            return exc
        if isinstance(
            exc,
            (
                psycopg2.errors.UndefinedColumn,
                psycopg2.errors.UndefinedObject,
                psycopg2.errors.UndefinedTable,
            ),
        ):
            return Phase1GTransactionalWriterError(
                REASON_G3_SCHEMA_NOT_READY, "Phase 1G G3 schema is incomplete"
            )
        if isinstance(exc, SourceLedgerError):
            reason = exc.reason_code
            mapped = REASON_G3_UNEXPECTED_ERROR
            conflict_kind = str(exc.context.get("conflict_kind") or "")
            if conflict_kind == "ROW_VERSION":
                mapped = REASON_G3_BATCH_ROW_VERSION_CONFLICT
            elif conflict_kind == "CAPTURE_PLAN":
                mapped = REASON_G3_CAPTURE_PLAN_CONFLICT
            elif conflict_kind == "REQUEST_HASH":
                mapped = REASON_G3_INPUT_INVALID
            elif "STATE_INVALID" in reason:
                mapped = REASON_G3_BATCH_NOT_RUNNING
            elif "FENCING" in reason:
                mapped = REASON_G3_FENCING_INVALID
            elif "LEASE" in reason:
                mapped = REASON_G3_LEASE_EXPIRED
            elif "CONTROL_BINDING" in reason:
                mapped = REASON_G3_CONTROL_BINDING_CONFLICT
            elif "MEMBERSHIP" in reason:
                mapped = REASON_G3_MEMBERSHIP_CONFLICT
            elif "SOURCE_REVISION" in reason or "REVISION_SET" in reason:
                mapped = REASON_G3_SOURCE_REVISION_CONFLICT
            elif "TRACE_OUTBOX" in reason or "OUTBOX_SCOPE" in reason:
                mapped = REASON_G3_TRACE_OUTBOX_CONFLICT
            elif "DELIVERY" in reason:
                mapped = REASON_G3_DELIVERY_CONFLICT
            elif "CHILD_ROW" in reason:
                mapped = REASON_G3_CHILD_ROW_CONFLICT
            elif "OBSERVATION" in reason:
                mapped = REASON_G3_OBSERVATION_CONFLICT
            return Phase1GTransactionalWriterError(
                mapped, str(exc), context=exc.context
            )
        logger.error(
            "phase1g g3 unexpected pre-commit failure",
            extra={
                "reason_code": REASON_G3_UNEXPECTED_ERROR,
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return Phase1GTransactionalWriterError(
            REASON_G3_UNEXPECTED_ERROR, "unexpected transactional writer failure"
        )


def _expected_memberships(
    source_set: Any,
    outbox: TraceOutboxRecord,
    observation: Phase1GObservationRowBundle,
) -> tuple[CaptureMembership, ...]:
    memberships = (
        CaptureMembership(
            evidence_role="SOURCE_REVISION_SET",
            evidence_id=source_set.source_revision_set_id,
            evidence_content_hash=source_set.source_revision_set_hash,
        ),
        CaptureMembership(
            evidence_role="TRACE_OUTBOX",
            evidence_id=outbox.trace_outbox_id,
            evidence_content_hash=outbox.envelope.trace_content_hash,
        ),
        CaptureMembership(
            evidence_role="OBSERVATION_VERSION",
            evidence_id=observation.observation_version["observation_version_id"],
            evidence_content_hash=observation.observation_version[
                "observation_content_hash"
            ],
        ),
    )
    return tuple(sorted(memberships, key=lambda item: item.content_key))


def _bundle_payload(bundle: Phase1GObservationRowBundle) -> dict[str, Any]:
    return canonicalize(
        bundle.model_dump(mode="python", exclude={"bundle_content_hash"})
    )
