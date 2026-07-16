"""Shared construction and referenced readback for Phase 1G commands."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import psycopg2
import psycopg2.extras

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatchStatus,
    PostgresCaptureBatchRepository,
)
from backend.services.advisory_phase1.observation_capture_postgres import (
    PostgresObservationCaptureRepository,
)
from backend.services.advisory_phase1.phase1g_artifact_ref import (
    Phase1GArtifactRootBinding,
    Phase1GImmutableArtifactResolver,
)
from backend.services.advisory_phase1.phase1g_contract import (
    DEFAULT_CAPTURE_POLICY_REGISTRY,
    PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY,
    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY,
    Phase1GAttemptReceipt,
    Phase1GBatchAttemptReceipt,
    Phase1GCaptureResult,
    Phase1GInputArtifactKind,
    Phase1GOutputArtifactKind,
)
from backend.services.advisory_phase1.phase1g_result_store import Phase1GResultStore
from backend.services.advisory_phase1.phase1g_schema_guard import (
    Phase1GExactTargetConnectionResolver,
)
from backend.services.advisory_phase1.phase1g_service import Phase1GService
from backend.services.advisory_phase1.release_schema_contract import TargetLabel
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    DatabaseConnectionConfig,
)
from backend.services.advisory_phase1.source_revision_postgres import (
    PostgresSourceRevisionRepository,
)
from backend.services.advisory_phase1.trace_outbox import (
    PostgresTraceOutboxRepository,
    TraceDeliveryEventType,
)


ConnectionFactory = Callable[[], Any]


@dataclass(frozen=True)
class Phase1GCommandContext:
    connection_config: DatabaseConnectionConfig
    artifact_resolver: Phase1GImmutableArtifactResolver
    result_store: Phase1GResultStore
    service: Phase1GService


@dataclass(frozen=True)
class Phase1GReferencedReadback:
    target_attempt_count: int
    capture_result_count: int
    mapping_count: int
    evidence_hash: str


def build_phase1g_command_context(
    *,
    env_file: Path,
    target_label: TargetLabel,
    release_receipt_root: Path,
    phase1e_artifact_root: Path,
    result_root: Path,
    transaction_connection_factory: ConnectionFactory | None = None,
    readonly_connection_factory: ConnectionFactory | None = None,
) -> Phase1GCommandContext:
    config = Phase1GExactTargetConnectionResolver(env_file=env_file).resolve(
        target_label=target_label
    )
    result_store = Phase1GResultStore(root=require_existing_phase1g_result_root(result_root))
    resolver = Phase1GImmutableArtifactResolver(
        bindings=(
            Phase1GArtifactRootBinding(
                artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                root=release_receipt_root,
                expected_store_policy_hash=str(
                    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash
                ),
            ),
            Phase1GArtifactRootBinding(
                artifact_kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
                root=phase1e_artifact_root,
                expected_store_policy_hash=str(
                    PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash
                ),
            ),
        )
    )

    def connect() -> Any:
        connection = psycopg2.connect(**config.connect_kwargs())
        connection.autocommit = False
        return connection

    transaction_factory = transaction_connection_factory or connect
    readonly_factory = readonly_connection_factory or connect
    service = Phase1GService(
        connection_config=config,
        transaction_connection_factory=transaction_factory,
        readonly_connection_factory=readonly_factory,
        artifact_resolver=resolver,
        result_store=result_store,
    )
    return Phase1GCommandContext(
        connection_config=config,
        artifact_resolver=resolver,
        result_store=result_store,
        service=service,
    )


def require_existing_phase1g_result_root(result_root: Path) -> Path:
    resolved = result_root.expanduser().resolve(strict=True)
    if not resolved.is_dir():
        raise ValueError("result root must be an existing external directory")
    return resolved


def verify_phase1g_attempt_database(
    *,
    receipt: Phase1GAttemptReceipt | Phase1GBatchAttemptReceipt,
    result_store: Phase1GResultStore,
    connection_config: DatabaseConnectionConfig,
) -> Phase1GReferencedReadback:
    evidence: list[dict[str, Any]] = []
    if isinstance(receipt, Phase1GBatchAttemptReceipt):
        for ref in receipt.target_attempt_refs:
            stored = result_store.load_by_identity(
                kind=Phase1GOutputArtifactKind.ATTEMPT_RECEIPT,
                identity=ref.attempt_receipt_hash,
            )
            if not isinstance(stored, Phase1GAttemptReceipt):
                raise ValueError("batch target attempt reference has wrong type")
            if (
                stored.target_request_hash != ref.target_request_hash
                or stored.target_plan_hash != ref.target_plan_hash
                or stored.operation_status is not ref.operation_status
                or stored.capture_result_hash != ref.capture_result_hash
            ):
                raise ValueError("batch target attempt reference is divergent")
            evidence.append(
                _verify_target_attempt_database(
                    receipt=stored,
                    result_store=result_store,
                    connect_kwargs=connection_config.connect_kwargs(),
                )
            )
    else:
        evidence.append(
            _verify_target_attempt_database(
                receipt=receipt,
                result_store=result_store,
                connect_kwargs=connection_config.connect_kwargs(),
            )
        )
    ordered = sorted(evidence, key=lambda item: item["attempt_receipt_hash"])
    return Phase1GReferencedReadback(
        target_attempt_count=len(ordered),
        capture_result_count=sum(item["capture_result_hash"] is not None for item in ordered),
        mapping_count=sum(int(item["mapping_count"]) for item in ordered),
        evidence_hash=canonical_json_sha256(ordered),
    )


def verify_phase1g_target_attempt_database(
    *,
    receipt: Phase1GAttemptReceipt,
    result_store: Phase1GResultStore,
    connect_kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Compatibility-preserving public delegate for one target attempt."""

    return _verify_target_attempt_database(
        receipt=receipt,
        result_store=result_store,
        connect_kwargs=connect_kwargs,
    )


def _verify_target_attempt_database(
    *,
    receipt: Phase1GAttemptReceipt,
    result_store: Phase1GResultStore,
    connect_kwargs: dict[str, Any],
) -> dict[str, Any]:
    if receipt.capture_result_ref is None:
        return {
            "attempt_receipt_hash": receipt.attempt_receipt_hash,
            "capture_result_hash": None,
            "mapping_count": 0,
            "database_evidence_hash": canonical_json_sha256(
                {
                    "target_request_hash": receipt.target_request_hash,
                    "target_plan_hash": receipt.target_plan_hash,
                    "operation_status": receipt.operation_status.value,
                }
            ),
        }
    stored = result_store.load(receipt.capture_result_ref)
    if not isinstance(stored, Phase1GCaptureResult):
        raise ValueError("target attempt result reference has wrong type")
    connection = psycopg2.connect(**connect_kwargs)
    try:
        connection.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (str(DEFAULT_CAPTURE_POLICY_REGISTRY.statement_timeout_ms),),
            )
            cur.execute(
                "SELECT set_config('lock_timeout', %s, true)",
                (str(DEFAULT_CAPTURE_POLICY_REGISTRY.lock_timeout_ms),),
            )
            chain = PostgresCaptureBatchRepository.read_request_chain_exact_readonly(
                cur, stored.capture_request_hash
            )
            matching = tuple(
                item
                for item in chain
                if item.request.capture_batch_id == stored.capture_batch_id
            )
            if len(matching) != 1:
                raise ValueError("result capture batch is absent or duplicated")
            batch = matching[0]
            if (
                batch.status is not CaptureBatchStatus.COMPLETE
                or batch.capture_attempt_no != stored.capture_attempt_no
                or batch.capture_receipt_hash != stored.capture_receipt_hash
                or batch.membership_count != stored.membership_count
                or batch.membership_hash != stored.membership_hash
                or batch.request.binding.control_binding_event_hash
                != stored.control_binding_event_hash
            ):
                raise ValueError("result differs from COMPLETE capture batch")
            memberships = PostgresCaptureBatchRepository.read_memberships_exact_readonly(
                cur, stored.capture_batch_id
            )
            membership_payload = [item.model_dump(mode="json") for item in memberships]
            if (
                len(memberships) != stored.membership_count
                or canonical_json_sha256(membership_payload) != stored.membership_hash
            ):
                raise ValueError("result capture memberships are divergent")
            source = PostgresSourceRevisionRepository.read_exact_readonly(
                cur, stored.source_revision_set_hash
            )
            if source.source_revision_set_id != stored.source_revision_set_id:
                raise ValueError("result source revision set is divergent")
            mapping_evidence: list[dict[str, Any]] = []
            for mapping in stored.selected_observation_mappings:
                outbox = PostgresTraceOutboxRepository.read_exact_by_hash_readonly(
                    cur, mapping.trace_content_hash
                )
                if outbox.trace_outbox_id != mapping.trace_outbox_id:
                    raise ValueError("result trace outbox mapping is divergent")
                bundle = PostgresObservationCaptureRepository.read_observation_rows_exact_readonly(
                    cur,
                    observation_version_id=mapping.observation_version_id,
                )
                stage_hash = canonical_json_sha256(
                    [row["content_hash"] for row in bundle.stage_evidence_rows]
                )
                if (
                    bundle.canonical_signal_header["canonical_signal_id"]
                    != mapping.canonical_signal_id
                    or bundle.observation_version["observation_content_hash"]
                    != mapping.observation_content_hash
                    or bundle.lineage_identity["lineage_id"] != mapping.lineage_id
                    or bundle.lineage_identity["lineage_content_hash"]
                    != mapping.lineage_content_hash
                    or stage_hash != mapping.stage_evidence_bundle_hash
                ):
                    raise ValueError("result observation mapping is divergent")
                delivery = PostgresTraceOutboxRepository.read_delivery_chain_exact_readonly(
                    cur, mapping.trace_outbox_id
                )
                if not any(
                    item.request.event_type
                    is TraceDeliveryEventType.OBSERVATION_WRITTEN
                    and item.request.payload.get("observation_version_id")
                    == mapping.observation_version_id
                    and item.request.payload.get("observation_content_hash")
                    == mapping.observation_content_hash
                    for item in delivery
                ):
                    raise ValueError("result observation delivery is missing")
                mapping_evidence.append(
                    {
                        "canonical_signal_id": mapping.canonical_signal_id,
                        "observation_version_id": mapping.observation_version_id,
                        "observation_content_hash": mapping.observation_content_hash,
                        "lineage_content_hash": mapping.lineage_content_hash,
                        "stage_evidence_bundle_hash": stage_hash,
                        "trace_content_hash": mapping.trace_content_hash,
                    }
                )
        connection.rollback()
    finally:
        connection.close()
    database_payload = {
        "capture_batch_id": stored.capture_batch_id,
        "capture_receipt_hash": stored.capture_receipt_hash,
        "membership_hash": stored.membership_hash,
        "source_revision_set_hash": stored.source_revision_set_hash,
        "mappings": sorted(
            mapping_evidence,
            key=lambda item: (item["canonical_signal_id"], item["observation_version_id"]),
        ),
    }
    return {
        "attempt_receipt_hash": receipt.attempt_receipt_hash,
        "capture_result_hash": stored.capture_result_hash,
        "mapping_count": len(mapping_evidence),
        "database_evidence_hash": canonical_json_sha256(database_payload),
    }
