"""Production Phase 1 capture/build/snapshot adapters for the range bridge."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, time
from typing import Any, Iterable

import psycopg2.extras

from backend.services.advisory_historical_range.artifact_store import (
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeBridgeLabelV1,
    HistoricalRangeBridgeObservationV1,
    HistoricalRangeDatasetBridgeError,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeContractError,
    HistoricalRangeDatasetBridgeRequestV1,
    HistoricalRangeOutcomePolicyBundleV1,
    REASON_DATABASE_CAPACITY_EXHAUSTED,
    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
)
from backend.services.advisory_historical_range.outcome_source import (
    PostgresHistoricalRangeOutcomeSourceProvider,
)
from backend.services.advisory_phase1.calculation_evidence import (
    LocalCalculationEvidenceStore,
)
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatch,
    CaptureBatchStatus,
    CaptureMembership,
    PostgresCaptureBatchRepository,
    RetrospectiveObservationCaptureBatchRequestV1,
    RetrospectiveObservationCaptureBinding,
    REASON_CAPTURE_BATCH_CONFLICT,
    REASON_CAPTURE_BATCH_STATE_INVALID,
    capture_request_hash,
)
from backend.services.advisory_phase1.dataset_build import (
    CompositeCapabilityRequirement,
    FrozenIdentity,
    LabelTargetIdentity,
    RetrospectiveCaptureSetMember,
    RetrospectiveDatasetBuildRequest,
    RetrospectiveSnapshotPolicyMember,
    RetrospectiveSnapshotPolicySet,
    SNAPSHOT_POLICY_SET_SCHEMA_VERSION,
    SnapshotUniversePolicySetError,
    build_snapshot_universe_policy_set_hash,
)
from backend.services.advisory_phase1.dataset_build_postgres import (
    PostgresDatasetBuildRepository,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from backend.services.advisory_phase1.label_builder import (
    LabelAppendRequest,
    LabelPolicyLineageType,
    LabelSelectionPolicy,
    LabelSelectionRequest,
    OutcomeLabelVersion,
    RetrospectiveExactLabelSelector,
    SelectedLabelMapping,
    label_key_hash,
)
from backend.services.advisory_phase1.label_builder_postgres import (
    PostgresOutcomeLabelRepository,
)
from backend.services.advisory_phase1.label_capture import (
    PlannedLabelDescriptor,
    RetrospectiveLabelCaptureBatchRequestV1,
    RetrospectiveLabelCaptureBinding,
    RetrospectiveSelectedObservationMappingReference,
)
from backend.services.advisory_phase1.observation_capture import (
    materialize_retrospective_observation_row_bundle,
)
from backend.services.advisory_phase1.observation_capture_postgres import (
    PostgresObservationCaptureRepository,
)
from backend.services.advisory_phase1.outcome_engine import (
    OUTCOME_CALCULATION_SCHEMA_VERSION,
)
from backend.services.advisory_phase1.retrospective_contracts import (
    HistoricalRangeArtifactReference,
)
from backend.services.advisory_phase1.retrospective_selector import (
    RETROSPECTIVE_SELECTOR_POLICY_HASH,
    RetrospectiveObservationSelector,
    RetrospectiveObservationVersion,
    RetrospectiveSelectionRequest,
)
from backend.services.advisory_phase1.snapshot_writer import (
    BATCH_D_BUILDER_VERSION,
    BATCH_D_WRITER_VERSION,
    RETROSPECTIVE_SNAPSHOT_SCHEMA_VERSION,
    DatasetSnapshotMaterializer,
    DatasetSnapshotPipeline,
    DeterministicParquetWriter,
    PostgresSnapshotSourceReader,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.source_revision import (
    SourceRevisionSet,
    build_source_revision_set,
)
from backend.services.advisory_phase1.source_revision_postgres import (
    PostgresSourceRevisionRepository,
)


@dataclass(frozen=True)
class _SnapshotPolicyAuthority:
    policy_bundle_id: str
    policy_bundle_hash: str
    policy_bundle_ref: HistoricalRangeArtifactRefV1
    component_hashes: dict[str, str]
    component_set_hash: str


class PostgresHistoricalRangeBridgeAdapters:
    """One exact-retry adapter used for all three bridge protocol boundaries."""

    def __init__(
        self,
        *,
        conn_factory: Any,
        artifact_store: HistoricalRangeArtifactStore,
        calculation_evidence_store: LocalCalculationEvidenceStore,
        dataset_store: LocalContentAddressedStore,
        code_commit: str,
        query_registry_version: str,
        builder_hash: str,
        writer_hash: str,
        partition_policy_id: str,
        actor: str = "advisory_phase1r_r4_bridge",
        capture_lease_seconds: int = 900,
    ) -> None:
        if conn_factory is None or not code_commit or not query_registry_version:
            raise ValueError("range bridge requires explicit DB and build identities")
        self._conn_factory = conn_factory
        self._artifact_store = artifact_store
        self._evidence_store = calculation_evidence_store
        self._dataset_store = dataset_store
        self._code_commit = code_commit
        self._query_registry_version = query_registry_version
        self._builder_hash = _sha256(builder_hash, "builder_hash")
        self._writer_hash = _sha256(writer_hash, "writer_hash")
        self._partition_policy_id = partition_policy_id
        self._actor = actor
        self._capture_lease_seconds = capture_lease_seconds
        self._capture_repository = PostgresCaptureBatchRepository(
            conn_factory=conn_factory
        )
        self._label_repository = PostgresOutcomeLabelRepository(
            evidence_reader=calculation_evidence_store,
            conn_factory=conn_factory,
        )
        self._build_repository = PostgresDatasetBuildRepository(
            conn_factory=conn_factory,
            historical_range_policy_payload_loader=lambda raw_ref: (
                self._artifact_store.load(
                    HistoricalRangeArtifactRefV1.model_validate(raw_ref)
                ).payload
            ),
        )
        writer = DeterministicParquetWriter(lineage_identity_type="HISTORICAL_RANGE")
        source_reader = PostgresSnapshotSourceReader(
            conn_factory=conn_factory,
            evidence_reader=calculation_evidence_store,
            lineage_identity_type="HISTORICAL_RANGE",
        )
        materializer = DatasetSnapshotMaterializer(
            source_reader=source_reader,
            writer=writer,
        )
        self._pipeline = DatasetSnapshotPipeline(
            repository=self._build_repository,
            materializer=materializer,
            store=dataset_store,
        )
        self._writer = writer

    def capture(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
    ) -> tuple[str, ...]:
        self._validate_bridge_request(request)
        if not observations or not labels:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "non-empty bridge requires observations and labels",
            )
        self._validate_capture_inputs(
            request=request,
            observations=observations,
            labels=labels,
        )
        observations_by_scope: dict[str, list[HistoricalRangeBridgeObservationV1]] = defaultdict(list)
        for observation in observations:
            scopes = {
                str(plan.range_scope.range_lineage_scope_hash)
                for plan in observation.capture_plan_variants
            }
            for scope_hash in scopes:
                observations_by_scope[scope_hash].append(observation)

        observation_batches: dict[str, CaptureBatch] = {}
        for scope_hash, scoped in sorted(observations_by_scope.items()):
            observation_batches[scope_hash] = self._capture_observations(
                request=request,
                scope_hash=scope_hash,
                observations=tuple(scoped),
            )

        labels_by_scope_source: dict[
            tuple[str, str, str], list[HistoricalRangeBridgeLabelV1]
        ] = defaultdict(list)
        observation_by_signal = {item.canonical_signal_id: item for item in observations}
        for label in labels:
            observation = observation_by_signal[label.canonical_signal_id]
            source_id, source_hash = _label_source_revision(label)
            scope_hashes = {
                str(plan.range_scope.range_lineage_scope_hash)
                for plan in observation.capture_plan_variants
            }
            for scope_hash in scope_hashes:
                labels_by_scope_source[(scope_hash, source_id, source_hash)].append(label)

        label_batches = []
        for (scope_hash, source_id, source_hash), scoped_labels in sorted(
            labels_by_scope_source.items()
        ):
            signals = {item.canonical_signal_id for item in scoped_labels}
            scoped_observations = tuple(
                item for item in observations_by_scope[scope_hash]
                if item.canonical_signal_id in signals
            )
            label_batches.append(
                self._capture_labels(
                    request=request,
                    source_batch=observation_batches[scope_hash],
                    observations=scoped_observations,
                    labels=tuple(scoped_labels),
                    label_source_revision_id=source_id,
                    label_source_revision_hash=source_hash,
                )
            )
        return tuple(
            sorted(
                [item.request.capture_batch_id for item in observation_batches.values()]
                + [item.request.capture_batch_id for item in label_batches]
            )
        )

    def build(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        capture_ids: tuple[str, ...],
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
    ) -> str:
        self._validate_bridge_request(request)
        batches = tuple(self._capture_repository.get(item) for item in capture_ids)
        if any(item.status is not CaptureBatchStatus.COMPLETE for item in batches):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "dataset build requires complete capture receipts",
            )
        snapshot_source = self._resolve_snapshot_source_revision(
            request=request,
            batches=batches,
            labels=labels,
        )
        build_request = self._build_request(
            request=request,
            batches=batches,
            observations=observations,
            labels=labels,
            snapshot_source=snapshot_source,
        )
        build = self._build_repository.create_or_get(
            build_request,
            actor=self._actor,
        )
        return build.build_id

    def resolve_persisted_labels(
        self, labels: tuple[HistoricalRangeBridgeLabelV1, ...]
    ) -> tuple[HistoricalRangeBridgeLabelV1, ...]:
        resolved: list[HistoricalRangeBridgeLabelV1] = []
        for label in labels:
            key = label_key_hash(
                canonical_signal_id=label.canonical_signal_id,
                symbol=label.symbol,
                label_policy_hash=label.historical_range_policy_bundle_hash,
                horizon_trading_days=label.horizon_trade_days,
                projection=label.projection,
            )
            chain = self._label_repository.chain_for(key)
            match = next(
                (
                    item
                    for item in reversed(chain)
                    if item.outcome_result == label.outcome_result
                ),
                None,
            )
            if match is None:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "persisted label readback does not match the bridge calculation",
                )
            resolved.append(
                label.model_copy(
                    update={
                        "label_version_id": match.label_version_id,
                        "label_content_hash": match.label_content_hash,
                    }
                )
            )
        return tuple(resolved)

    def seal(
        self, *, build_id: str, expected_selector_policy_hash: str
    ) -> tuple[str, str]:
        if expected_selector_policy_hash != RETROSPECTIVE_SELECTOR_POLICY_HASH:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "snapshot seal requires the retrospective selector",
            )
        build = self._pipeline.run(build_id=build_id, actor=self._actor)
        if (
            build.request.selector_policy_hash != expected_selector_policy_hash
            or build.sealed_snapshot_id is None
        ):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "sealed snapshot readback differs from the retrospective build",
            )
        return build.sealed_snapshot_id, build.request.selector_policy_hash

    def _prepare_capture_batch(
        self,
        *,
        request: RetrospectiveObservationCaptureBatchRequestV1
        | RetrospectiveLabelCaptureBatchRequestV1,
        id_prefix: str,
    ) -> CaptureBatch:
        try:
            batch = self._capture_repository.create(request)
        except SourceLedgerError as error:
            if error.reason_code != REASON_CAPTURE_BATCH_CONFLICT:
                raise
            batch = self._adopt_semantic_capture_batch(
                request=request,
                id_prefix=id_prefix,
            )
        while True:
            if batch.status in {
                CaptureBatchStatus.PLANNED,
                CaptureBatchStatus.COMPLETE,
            }:
                return batch
            if batch.status is CaptureBatchStatus.RUNNING:
                batch = self._expire_capture_batch(
                    batch=batch,
                    active_lease_detail="retrospective capture batch still has an active lease",
                )
            if batch.status not in {
                CaptureBatchStatus.FAILED,
                CaptureBatchStatus.EXPIRED,
                CaptureBatchStatus.ABORTED,
            }:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "retrospective capture chain has an invalid recovery state",
                )
            batch = self._recover_capture_successor(
                request=request,
                id_prefix=id_prefix,
                predecessor=batch,
            )

    def _adopt_semantic_capture_batch(
        self,
        *,
        request: RetrospectiveObservationCaptureBatchRequestV1
        | RetrospectiveLabelCaptureBatchRequestV1,
        id_prefix: str,
    ) -> CaptureBatch:
        """Adopt the unique exact-content recovery successor for a new operation.

        A new bridge operation derives a new deterministic capture batch id
        while the economic capture content stays identical.  The repository
        correctly refuses a second active batch for the same content, so the
        bridge must resolve the persisted same-content recovery chain and:
        reuse the completed batch, adopt the unique exact-content PLANNED
        successor, or extend the chain through the formal expire/recover
        contracts.  Every ambiguity stays fail-closed.
        """

        chain = self._capture_repository.list_by_capture_request_hash(
            capture_request_hash(request)
        )
        if not chain:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "capture content conflict has no persisted recovery chain",
            )
        for member in chain:
            self._assert_same_capture_semantics(member=member, request=request)
        self._assert_capture_chain_integrity(chain)
        completed = tuple(
            item for item in chain if item.status is CaptureBatchStatus.COMPLETE
        )
        active = tuple(
            item
            for item in chain
            if item.status
            in {CaptureBatchStatus.PLANNED, CaptureBatchStatus.RUNNING}
        )
        if completed:
            if len(completed) > 1 or active:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "capture recovery chain has conflicting completion state",
                )
            return completed[0]
        if len(active) > 1:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "capture recovery chain has multiple active batches",
            )
        if active:
            candidate = active[0]
            if candidate.status is CaptureBatchStatus.RUNNING:
                expired = self._expire_capture_batch(
                    batch=candidate,
                    active_lease_detail="active capture successor still has an active lease",
                )
                return self._recover_capture_successor(
                    request=request,
                    id_prefix=id_prefix,
                    predecessor=expired,
                )
            if (
                candidate.membership_count is not None
                or candidate.membership_hash is not None
                or candidate.capture_receipt_hash is not None
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "active capture successor already carries partial capture facts",
                )
            return candidate
        return self._recover_capture_successor(
            request=request,
            id_prefix=id_prefix,
            predecessor=chain[-1],
        )

    def _expire_capture_batch(
        self,
        *,
        batch: CaptureBatch,
        active_lease_detail: str,
    ) -> CaptureBatch:
        """Use repository time as the single lease-expiry authority."""

        try:
            return self._capture_repository.expire(
                capture_batch_id=batch.request.capture_batch_id,
                expected_row_version=batch.row_version,
                fencing_token=batch.fencing_token,
            )
        except SourceLedgerError as error:
            if error.reason_code == REASON_CAPTURE_BATCH_STATE_INVALID:
                raise HistoricalRangeContractError(
                    REASON_DATABASE_CAPACITY_EXHAUSTED,
                    active_lease_detail,
                ) from error
            raise

    def _recover_capture_successor(
        self,
        *,
        request: RetrospectiveObservationCaptureBatchRequestV1
        | RetrospectiveLabelCaptureBatchRequestV1,
        id_prefix: str,
        predecessor: CaptureBatch,
    ) -> CaptureBatch:
        successor = self._capture_recovery_request(
            request=request,
            capture_batch_id=_prefixed_id(
                id_prefix,
                {
                    "capture_request_hash": capture_request_hash(request),
                    "capture_attempt_no": predecessor.capture_attempt_no + 1,
                },
            ),
        )
        try:
            return self._capture_repository.get(successor.capture_batch_id)
        except SourceLedgerError:
            return self._capture_repository.recover(
                request=successor,
                predecessor_capture_batch_id=predecessor.request.capture_batch_id,
                expected_predecessor_row_version=predecessor.row_version,
                predecessor_fencing_token=predecessor.fencing_token,
            )

    def _assert_same_capture_semantics(
        self,
        *,
        member: CaptureBatch,
        request: RetrospectiveObservationCaptureBatchRequestV1
        | RetrospectiveLabelCaptureBatchRequestV1,
    ) -> None:
        if type(member.request) is not type(request):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "capture recovery chain mixes capture request types",
            )
        aligned = self._capture_recovery_request(
            request=request,
            capture_batch_id=member.request.capture_batch_id,
        )
        if aligned != member.request:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "capture recovery chain member payload differs from the bridge request",
            )

    @staticmethod
    def _assert_capture_chain_integrity(chain: tuple[CaptureBatch, ...]) -> None:
        attempts = tuple(item.capture_attempt_no for item in chain)
        batch_ids = tuple(item.request.capture_batch_id for item in chain)
        if len(set(attempts)) != len(attempts) or len(set(batch_ids)) != len(batch_ids):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "capture recovery chain has duplicate attempts or batch ids",
            )
        by_id = {item.request.capture_batch_id: item for item in chain}
        roots = tuple(
            item for item in chain if item.predecessor_capture_batch_id is None
        )
        if len(roots) != 1 or roots[0].capture_attempt_no != 1:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "capture recovery chain has an invalid recovery root",
            )
        for item in chain:
            if item.predecessor_capture_batch_id is None:
                continue
            predecessor = by_id.get(item.predecessor_capture_batch_id)
            if predecessor is None:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "capture recovery predecessor is outside the content chain",
                )
            if predecessor.capture_attempt_no != item.capture_attempt_no - 1:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "capture recovery chain attempt sequence is broken",
                )
            if predecessor.status not in {
                CaptureBatchStatus.FAILED,
                CaptureBatchStatus.EXPIRED,
                CaptureBatchStatus.ABORTED,
            }:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "capture recovery predecessor is not in a terminal state",
                )

    @staticmethod
    def _capture_recovery_request(
        *,
        request: RetrospectiveObservationCaptureBatchRequestV1
        | RetrospectiveLabelCaptureBatchRequestV1,
        capture_batch_id: str,
    ) -> RetrospectiveObservationCaptureBatchRequestV1 | RetrospectiveLabelCaptureBatchRequestV1:
        binding_payload = request.binding.model_dump(
            mode="python",
            exclude={"binding_hash"},
        )
        binding_payload["capture_batch_id"] = capture_batch_id
        if isinstance(request, RetrospectiveObservationCaptureBatchRequestV1):
            binding_payload["capture_fencing_token"] = 1
            binding = RetrospectiveObservationCaptureBinding.model_validate(
                binding_payload
            )
            payload = request.model_dump(
                mode="python",
                exclude={"capture_request_hash", "binding"},
            )
            payload.update(
                capture_batch_id=capture_batch_id,
                binding=binding,
            )
            successor = RetrospectiveObservationCaptureBatchRequestV1.model_validate(
                payload
            )
        else:
            binding_payload["current_fencing_token"] = 1
            binding = RetrospectiveLabelCaptureBinding.model_validate(
                binding_payload
            )
            payload = request.model_dump(
                mode="python",
                exclude={"capture_request_hash", "binding"},
            )
            payload.update(
                capture_batch_id=capture_batch_id,
                binding=binding,
            )
            successor = RetrospectiveLabelCaptureBatchRequestV1.model_validate(
                payload
            )
        if successor.capture_request_hash != request.capture_request_hash:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "retrospective capture recovery changed request semantics",
            )
        return successor

    def _capture_observations(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        scope_hash: str,
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
    ) -> CaptureBatch:
        plans_by_signal = {
            (
                observation.capture_plan.decision_as_of_trade_date,
                observation.capture_plan.canonical_signal_id,
            ): observation.capture_plan
            for observation in observations
            if str(
                observation.capture_plan.range_scope.range_lineage_scope_hash
            )
            == scope_hash
        }
        plans = tuple(
            sorted(
                plans_by_signal.values(),
                key=lambda item: (
                    item.decision_as_of_trade_date,
                    item.canonical_signal_id,
                ),
            )
        )
        if not plans:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "range scope has no observation plans",
            )
        capture_id = _prefixed_id(
            "ahr_obs_cap",
            {"bridge_request_hash": request.request_hash, "scope_hash": scope_hash},
        )
        capture_request = RetrospectiveObservationCaptureBatchRequestV1(
            capture_batch_id=capture_id,
            binding=RetrospectiveObservationCaptureBinding(
                capture_batch_id=capture_id,
                capture_fencing_token=1,
                range_scope=plans[0].range_scope,
            ),
            plans=plans,
        )
        batch = self._prepare_capture_batch(
            request=capture_request,
            id_prefix="ahr_obs_cap",
        )
        if batch.status is CaptureBatchStatus.COMPLETE:
            return batch
        capture_request = batch.request
        if not isinstance(
            capture_request, RetrospectiveObservationCaptureBatchRequestV1
        ):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "observation recovery returned another capture request type",
            )
        capture_id = capture_request.capture_batch_id
        plans = capture_request.plans
        active = self._capture_repository.acquire(
            capture_batch_id=capture_id,
            expected_row_version=batch.row_version,
            lease_seconds=self._capture_lease_seconds,
        )
        by_signal = {item.canonical_signal_id: item for item in observations}
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                current = active
                seen_observations: set[str] = set()
                seen_lineages: set[str] = set()
                for plan in plans:
                    observation = by_signal[plan.canonical_signal_id]
                    row_bundle = materialize_retrospective_observation_row_bundle(
                        plan=plan,
                        stage_payload=observation.stage_payload,
                        candidate_fact=observation.candidate_fact,
                        created_by_capture_batch_id=capture_id,
                    )
                    PostgresObservationCaptureRepository.append_materialized_bundle_in_transaction(
                        cur,
                        row_bundle=row_bundle,
                    )
                    observation_id = str(
                        row_bundle.observation_version["observation_version_id"]
                    )
                    if observation_id not in seen_observations:
                        current = self._capture_repository.add_membership_in_transaction(
                            cur,
                            capture_batch_id=capture_id,
                            expected_row_version=current.row_version,
                            fencing_token=current.fencing_token,
                            membership=CaptureMembership(
                                evidence_role="OBSERVATION_VERSION",
                                evidence_id=observation_id,
                                evidence_content_hash=str(
                                    row_bundle.observation_version[
                                        "observation_content_hash"
                                    ]
                                ),
                            ),
                        )
                        seen_observations.add(observation_id)
                    for lineage_plan in observation.capture_plan_variants:
                        lineage_hash = str(
                            lineage_plan.lineage.range_lineage_identity_hash
                        )
                        if (
                            str(
                                lineage_plan.range_scope.range_lineage_scope_hash
                            )
                            != scope_hash
                            or lineage_hash in seen_lineages
                        ):
                            continue
                        current = self._capture_repository.add_membership_in_transaction(
                            cur,
                            capture_batch_id=capture_id,
                            expected_row_version=current.row_version,
                            fencing_token=current.fencing_token,
                            membership=CaptureMembership(
                                evidence_role="OBSERVATION_LINEAGE",
                                evidence_id=lineage_hash,
                                evidence_content_hash=lineage_hash,
                            ),
                        )
                        seen_lineages.add(lineage_hash)
                return self._capture_repository.complete_in_transaction(
                    cur,
                    capture_batch_id=capture_id,
                    expected_row_version=current.row_version,
                    fencing_token=current.fencing_token,
                )

    def _capture_labels(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        source_batch: CaptureBatch,
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
        label_source_revision_id: str,
        label_source_revision_hash: str,
    ) -> CaptureBatch:
        if (
            source_batch.membership_hash is None
            or source_batch.capture_receipt_hash is None
        ):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "label capture source lacks a terminal receipt or membership closure",
            )
        source_request = source_batch.request
        if not isinstance(source_request, RetrospectiveObservationCaptureBatchRequestV1):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "label capture source is not a retrospective observation capture",
            )
        capture_policy_identities = {
            (
                item.historical_range_policy_bundle_ref,
                item.historical_range_policy_bundle_hash,
                item.policy_component_set_hash,
            )
            for item in labels
        }
        if len(capture_policy_identities) != 1:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "one label capture group requires one exact policy/component set",
            )
        mappings = self._select_observations(
            request=request,
            observations=observations,
            labels=labels,
        )
        mapping_refs = tuple(
            RetrospectiveSelectedObservationMappingReference(
                selected_mapping_id=str(mapping.selected_mapping_id),
                selected_mapping_hash=str(mapping.selected_mapping_hash),
                canonical_signal_id=mapping.canonical_signal_id,
                terminal_observation_version_id=mapping.observation_version_id,
                terminal_observation_content_hash=mapping.observation_content_hash,
                candidate_stage_evidence_id=str(
                    next(
                        item.owner.candidate_stage_evidence_id
                        for item in observations
                        if item.canonical_signal_id == mapping.canonical_signal_id
                    )
                ),
                selected_lineage_refs=mapping.selected_lineage_refs,
                selection_policy_hash=mapping.selection_policy_hash,
            )
            for mapping in mappings
        )
        policy_ref = labels[0].historical_range_policy_bundle_ref
        policy = HistoricalRangeOutcomePolicyBundleV1.model_validate(
            self._artifact_store.load(policy_ref).payload
        )
        planned = tuple(
            sorted(
                (
                    PlannedLabelDescriptor(
                        canonical_signal_id=item.canonical_signal_id,
                        observation_version_id=item.observation_version_id,
                        candidate_stage_evidence_id=str(
                            item.outcome_result.owner.candidate_stage_evidence_id
                        ),
                        symbol=item.symbol,
                        decision_as_of_trade_date=(
                            item.outcome_result.owner.decision_as_of_trade_date
                        ),
                        horizon_trading_days=item.horizon_trade_days,
                        projection=item.projection.value,
                        label_key_hash=label_key_hash(
                            canonical_signal_id=item.canonical_signal_id,
                            symbol=item.symbol,
                            label_policy_hash=item.historical_range_policy_bundle_hash,
                            horizon_trading_days=item.horizon_trade_days,
                            projection=item.projection,
                        ),
                    )
                    for item in labels
                ),
                key=lambda item: (
                    item.canonical_signal_id,
                    item.symbol,
                    item.horizon_trading_days,
                    item.projection,
                ),
            )
        )
        plan_set_hash = canonical_json_sha256(
            [item.model_dump(mode="json") for item in source_request.plans]
        )
        mapping_set_hash = canonical_json_sha256(
            [item.canonical_identity() for item in mapping_refs]
        )
        planned_hash = canonical_json_sha256(
            [item.canonical_identity() for item in planned]
        )
        label_as_of_ts = _label_as_of(labels)
        component_set_hash = labels[0].policy_component_set_hash
        capture_id = _label_capture_id(
            bridge_request_hash=str(request.request_hash),
            scope_hash=str(
                source_request.binding.range_scope.range_lineage_scope_hash
            ),
            label_source_revision_id=label_source_revision_id,
            label_source_revision_hash=label_source_revision_hash,
        )
        binding = RetrospectiveLabelCaptureBinding(
            capture_batch_id=capture_id,
            current_fencing_token=1,
            source_observation_capture_batch_id=source_request.capture_batch_id,
            source_capture_request_hash=str(source_request.capture_request_hash),
            source_capture_receipt_hash=source_batch.capture_receipt_hash,
            source_capture_membership_count=int(source_batch.membership_count or 0),
            source_capture_membership_hash=source_batch.membership_hash,
            source_capture_plan_set_count=len(source_request.plans),
            source_capture_plan_set_hash=plan_set_hash,
            range_scope=source_request.binding.range_scope,
            selected_observation_mapping_set_count=len(mapping_refs),
            selected_observation_mapping_set_hash=mapping_set_hash,
            policy_component_set_hash=component_set_hash,
            label_source_revision_set_id=label_source_revision_id,
            label_source_revision_set_hash=label_source_revision_hash,
            label_as_of_ts=label_as_of_ts,
        )
        capture_request = RetrospectiveLabelCaptureBatchRequestV1(
            capture_batch_id=capture_id,
            binding=binding,
            source_observation_capture_batch_id=source_request.capture_batch_id,
            source_capture_receipt_hash=source_batch.capture_receipt_hash,
            source_capture_membership_hash=source_batch.membership_hash,
            source_capture_plan_set_count=len(source_request.plans),
            source_capture_plan_set_hash=plan_set_hash,
            selected_observation_mappings=mapping_refs,
            label_policy_bundle_id=str(policy.policy_bundle_id),
            label_policy_bundle_hash=str(policy.policy_bundle_hash),
            historical_range_policy_bundle_ref=_phase1_ref(policy_ref),
            policy_component_set_hash=component_set_hash,
            label_source_revision_set_id=label_source_revision_id,
            label_source_revision_set_hash=label_source_revision_hash,
            label_as_of_ts=label_as_of_ts,
            planned_labels=planned,
            planned_label_count=len(planned),
            planned_label_hash=planned_hash,
            selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
        )
        batch = self._prepare_capture_batch(
            request=capture_request,
            id_prefix="ahr_lbl_cap",
        )
        if batch.status is CaptureBatchStatus.COMPLETE:
            return batch
        capture_request = batch.request
        if not isinstance(capture_request, RetrospectiveLabelCaptureBatchRequestV1):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "label recovery returned another capture request type",
            )
        capture_id = capture_request.capture_batch_id
        active = self._capture_repository.acquire(
            capture_batch_id=capture_id,
            expected_row_version=batch.row_version,
            lease_seconds=self._capture_lease_seconds,
        )
        current = active
        for label in labels:
            stored = self._evidence_store.put(label.calculation_evidence)
            append_request = self._label_append_request(label=label, stored=stored)
            version = self._label_repository.append(
                request=append_request,
                created_by_capture_batch_id=capture_id,
            )
            current = self._capture_repository.add_membership(
                capture_batch_id=capture_id,
                expected_row_version=current.row_version,
                fencing_token=current.fencing_token,
                membership=CaptureMembership(
                    evidence_role="LABEL_VERSION",
                    evidence_id=str(version.label_version_id),
                    evidence_content_hash=str(version.label_content_hash),
                ),
            )
            mapping = self._select_label_mapping(
                label=label,
                version=version,
                requested_label_as_of_ts=label_as_of_ts,
            )
            current = self._capture_repository.add_membership(
                capture_batch_id=capture_id,
                expected_row_version=current.row_version,
                fencing_token=current.fencing_token,
                membership=CaptureMembership(
                    evidence_role="SELECTED_LABEL_MAPPING",
                    evidence_id=str(mapping.selected_label_mapping_id),
                    evidence_content_hash=str(mapping.selected_label_mapping_hash),
                ),
            )
        return self._capture_repository.complete(
            capture_batch_id=capture_id,
            expected_row_version=current.row_version,
            fencing_token=current.fencing_token,
        )

    def _label_append_request(self, *, label: HistoricalRangeBridgeLabelV1, stored: Any) -> LabelAppendRequest:
        key = label_key_hash(
            canonical_signal_id=label.canonical_signal_id,
            symbol=label.symbol,
            label_policy_hash=label.historical_range_policy_bundle_hash,
            horizon_trading_days=label.horizon_trade_days,
            projection=label.projection,
        )
        chain = self._label_repository.chain_for(key)
        predecessor = chain[-1] if chain else None
        expected_id = expected_hash = expected_revision = None
        if predecessor is not None:
            same_result = (
                predecessor.outcome_result == label.outcome_result
                and predecessor.calculation_evidence_sha256 == stored.sha256
            )
            if same_result:
                expected_id = predecessor.supersedes_label_version_id
                expected_hash = predecessor.supersedes_label_version_hash
                expected_revision = (
                    predecessor.label_revision_no - 1
                    if predecessor.label_revision_no > 1
                    else None
                )
            else:
                expected_id = predecessor.label_version_id
                expected_hash = predecessor.label_content_hash
                expected_revision = predecessor.label_revision_no
        source_id, source_hash = _label_source_revision(label)
        return LabelAppendRequest(
            label_key_hash=key,
            expected_predecessor_version_id=expected_id,
            expected_predecessor_version_hash=expected_hash,
            expected_predecessor_revision_no=expected_revision,
            policy_lineage_type=LabelPolicyLineageType.HISTORICAL_RANGE_OUTCOME_POLICY,
            historical_range_policy_bundle_ref=_phase1_ref(
                label.historical_range_policy_bundle_ref
            ),
            historical_range_policy_bundle_hash=label.historical_range_policy_bundle_hash,
            policy_component_set_hash=label.policy_component_set_hash,
            label_policy_hash=label.historical_range_policy_bundle_hash,
            label_source_revision_set_id=source_id,
            label_source_revision_set_hash=source_hash,
            owner=label.outcome_result.owner,
            horizon_trading_days=label.horizon_trade_days,
            projection=label.projection,
            projection_schema_version=OUTCOME_CALCULATION_SCHEMA_VERSION,
            outcome_result=label.outcome_result,
            projection_payload_hash=str(label.outcome_result.projection_payload_hash),
            calculation_evidence_sha256=stored.sha256,
            calculation_evidence_size_bytes=stored.size_bytes,
            calculation_evidence_store_backend_hash=stored.store_backend_hash,
            calculation_evidence_uri=stored.uri,
        )

    def _select_observations(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
    ) -> tuple[Any, ...]:
        cutoff = _label_as_of(labels)
        selection_request = RetrospectiveSelectionRequest(
            range_run_ids=tuple(
                sorted(
                    {
                        lineage.range_run_id
                        for observation in observations
                        for lineage in observation.lineage_variants
                    }
                )
            ),
            candidate_artifact_refs=tuple(
                sorted(
                    {
                        lineage.candidate_artifact_ref.semantic_content_hash:
                        lineage.candidate_artifact_ref
                        for observation in observations
                        for lineage in observation.lineage_variants
                    }.values(),
                    key=lambda item: item.semantic_content_hash,
                )
            ),
            outcome_refs=tuple(
                sorted(
                    {
                        ref.semantic_content_hash: ref
                        for label in labels
                        for ref in label.accepted_outcome_refs
                    }.values(),
                    key=lambda item: item.semantic_content_hash,
                )
            ),
            requested_source_cutoff=cutoff,
        )
        versions = tuple(
            RetrospectiveObservationVersion(
                canonical_signal_id=observation.canonical_signal_id,
                observation_version_id=observation.observation_version_id,
                observation_content_hash=observation.observation_content_hash,
                evidence_available_at=observation.capture_plan.evidence_available_at,
                lineage=lineage,
                candidate_artifact_ref=lineage.candidate_artifact_ref,
                outcome_refs=observation.accepted_outcome_refs,
                observation_payload=observation.observation_payload,
            )
            for observation in observations
            for lineage in observation.lineage_variants
        )
        return RetrospectiveObservationSelector().select(
            request=selection_request,
            observations=versions,
        )

    def _replay_selected_observation_mappings(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        batches: tuple[CaptureBatch, ...],
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
    ) -> tuple[FrozenIdentity, ...]:
        """BUG-873: replay selection per frozen label-capture group.

        Retrospective label capture runs the selector once per
        (range scope, label source revision) group, so every frozen
        ``selected_mapping_hash`` binds that group's selector request
        (group range runs, group candidate/outcome refs, group cutoff,
        selector policy).  Replaying the selector once over the global
        observation/label set selects the same observations but mints
        different mapping identities, which the frozen capture evidence
        rightly rejects.  The build therefore restores each group's full
        frozen selector context from the persisted label capture request,
        replays the selector inside that group, and only accepts the replay
        when it reproduces the frozen mapping identity for every signal.
        The per-group results merge deterministically by mapping identity;
        conflicting terminals for one signal, one mapping id with different
        content, and missing or extra mappings all fail closed.  Group
        iteration order cannot affect the merged set, so an exact retry
        returns the same identities.
        """
        label_batches = tuple(
            sorted(
                (
                    item
                    for item in batches
                    if item.request.capture_purpose == "LABEL_CAPTURE_V1"
                ),
                key=lambda item: item.request.capture_batch_id,
            )
        )
        if not label_batches:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "non-empty bridge requires frozen label captures",
            )
        observations_by_scope: dict[str, list[HistoricalRangeBridgeObservationV1]] = defaultdict(list)
        observation_by_signal = {}
        for observation in observations:
            observation_by_signal[observation.canonical_signal_id] = observation
            for scope_hash in {
                str(plan.range_scope.range_lineage_scope_hash)
                for plan in observation.capture_plan_variants
            }:
                observations_by_scope[scope_hash].append(observation)
        merged: dict[str, str] = {}
        terminals_by_signal: dict[str, tuple[str, str]] = {}
        for batch in label_batches:
            frozen_request = batch.request
            if not isinstance(frozen_request, RetrospectiveLabelCaptureBatchRequestV1):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "label capture request is not retrospective",
                )
            scope_hash = str(frozen_request.binding.range_scope.range_lineage_scope_hash)
            source_pair = (
                str(frozen_request.label_source_revision_set_id),
                str(frozen_request.label_source_revision_set_hash),
            )
            frozen_by_signal = {
                str(item.canonical_signal_id): item
                for item in frozen_request.selected_observation_mappings
            }
            planned_keys = {
                (
                    str(item.canonical_signal_id),
                    int(item.horizon_trading_days),
                    str(item.projection),
                )
                for item in frozen_request.planned_labels
            }
            if not planned_keys:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "label capture freezes no planned labels",
                )
            group_labels = tuple(
                label
                for label in labels
                if (
                    label.canonical_signal_id,
                    label.horizon_trade_days,
                    label.projection.value,
                )
                in planned_keys
            )
            replay_keys = {
                (
                    label.canonical_signal_id,
                    label.horizon_trade_days,
                    label.projection.value,
                )
                for label in group_labels
            }
            if replay_keys != planned_keys or len(group_labels) != len(planned_keys):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "label capture planned labels differ from frozen build labels",
                )
            if any(
                _label_source_revision(label) != source_pair for label in group_labels
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "label source revision identity differs from frozen label capture",
                )
            group_signals = {signal for signal, _h, _p in planned_keys}
            if set(frozen_by_signal) != group_signals:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "label capture selected mappings do not cover the planned signals",
                )
            scoped_observations = tuple(
                item
                for item in observations_by_scope.get(scope_hash, ())
                if item.canonical_signal_id in group_signals
            )
            replayed = self._select_observations(
                request=request,
                observations=scoped_observations,
                labels=group_labels,
            )
            replayed_by_signal = {
                str(item.canonical_signal_id): item for item in replayed
            }
            if set(replayed_by_signal) != group_signals:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "selection replay lost or invented canonical signals",
                )
            for signal in sorted(group_signals):
                replay = replayed_by_signal[signal]
                frozen = frozen_by_signal[signal]
                if (
                    str(replay.selected_mapping_id) != str(frozen.selected_mapping_id)
                    or str(replay.selected_mapping_hash)
                    != str(frozen.selected_mapping_hash)
                    or str(replay.observation_version_id)
                    != str(frozen.terminal_observation_version_id)
                    or str(replay.observation_content_hash)
                    != str(frozen.terminal_observation_content_hash)
                    or tuple(str(ref) for ref in replay.selected_lineage_refs)
                    != tuple(str(ref) for ref in frozen.selected_lineage_refs)
                ):
                    raise HistoricalRangeDatasetBridgeError(
                        REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                        "selection replay differs from frozen label capture mapping",
                    )
                observation = observation_by_signal.get(signal)
                if observation is None or str(
                    observation.owner.candidate_stage_evidence_id
                ) != str(frozen.candidate_stage_evidence_id):
                    raise HistoricalRangeDatasetBridgeError(
                        REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                        "label capture stage evidence differs from frozen observation",
                    )
                terminal = (
                    str(frozen.terminal_observation_version_id),
                    str(frozen.terminal_observation_content_hash),
                )
                existing_terminal = terminals_by_signal.get(signal)
                if existing_terminal is not None and existing_terminal != terminal:
                    raise HistoricalRangeDatasetBridgeError(
                        REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                        "frozen label captures select conflicting terminals for one signal",
                    )
                terminals_by_signal[signal] = terminal
                mapping_id = str(frozen.selected_mapping_id)
                mapping_hash = str(frozen.selected_mapping_hash)
                existing_hash = merged.get(mapping_id)
                if existing_hash is not None and existing_hash != mapping_hash:
                    raise HistoricalRangeDatasetBridgeError(
                        REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                        "one selected mapping id carries conflicting content",
                    )
                merged[mapping_id] = mapping_hash
        return tuple(
            FrozenIdentity(identity_id=mapping_id, identity_hash=mapping_hash)
            for mapping_id, mapping_hash in sorted(merged.items())
        )

    def _resolve_snapshot_source_revision(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        batches: tuple[CaptureBatch, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
    ) -> tuple[str, str, str, datetime]:
        """BUG-866: resolve the snapshot-level composite source revision set.

        Each subject keeps its own per-subject source revision set as its PIT
        authority; the snapshot references one deterministic union of every
        selected subject's source members.  Every constituent set is
        re-derived through the standard outcome source builder from the
        subject's frozen timeline and is only accepted when its id and hash
        exactly match the immutable calculation evidence; any drift fails
        closed.  The union is built with the formal builder and persisted
        through the formal idempotent freeze contract with full readback, so
        an exact retry returns the same composite identity.

        BUG-874: the query registry identity is part of the constituent
        header, so all constituents must share exactly one registry hash;
        conflicting, missing, or malformed identities fail closed.  The
        returned third element is the union evidence's own registry hash,
        inherited from that common header and verified through the persisted
        freeze readback - the build request must declare this evidence
        identity, never a configured descriptor.

        BUG-875: the requested source cutoff is part of the same common
        header, compared as the normalized UTC timestamps the formal
        builder stores.  The returned fourth element is the union
        evidence's own cutoff; the build request projects it to a date via
        the UTC calendar, never a local timezone, never ``date_end``, and
        never a configured or current-time fallback.
        """
        label_captures = tuple(
            _capture_member(item)
            for item in batches
            if item.request.capture_purpose == "LABEL_CAPTURE_V1"
        )
        capture_pairs = {
            (item.source_revision_set_id, item.source_revision_set_hash)
            for item in label_captures
        }
        groups: dict[tuple[str, str], list[HistoricalRangeBridgeLabelV1]] = defaultdict(list)
        for label in labels:
            groups[_label_source_revision(label)].append(label)
        if set(groups) != capture_pairs:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "label capture source revision sets differ from frozen label evidence",
            )
        provider = PostgresHistoricalRangeOutcomeSourceProvider(
            conn_factory=self._conn_factory
        )
        provider.begin_operation(str(request.request_hash))
        constituents: list[SourceRevisionSet] = []
        for pair in sorted(groups):
            derivations = {
                (
                    item.symbol.upper(),
                    item.outcome_result.decision_trade_date,
                    item.outcome_result.exit_trade_date,
                    item.label_as_of_trade_date,
                )
                for item in groups[pair]
            }
            if len(derivations) != 1:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "label source revision group carries conflicting derivation inputs",
                )
            symbol, decision_date, exit_date, label_as_of_date = next(iter(derivations))
            if exit_date is None:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "label outcome result lacks the frozen exit timeline",
                )
            label_as_of_ts = datetime.combine(
                label_as_of_date, time(23, 59, 59), tzinfo=UTC
            )
            revision_set = provider.resolve_source_revision_bundle(
                symbol=symbol,
                start_trade_date=decision_date,
                end_trade_date=min(exit_date, label_as_of_date),
                label_as_of_ts=label_as_of_ts,
            ).source_revision_set
            if (
                revision_set.source_revision_set_id != pair[0]
                or revision_set.source_revision_set_hash != pair[1]
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "re-derived subject source revision set differs from frozen label evidence",
                )
            constituents.append(revision_set)
        headers = {
            (
                item.query_registry_hash,
                item.requested_source_cutoff,
                item.label_as_of_ts,
                item.research_only,
                item.schema_version,
            )
            for item in constituents
        }
        if len(headers) != 1:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "constituent source revision set header identities differ",
            )
        query_registry_hash, requested_cutoff, label_as_of_ts, research_only, _ = next(
            iter(headers)
        )
        merged: dict[str, Any] = {}
        for revision_set in constituents:
            for member in revision_set.members:
                existing = merged.get(member.member_key)
                if existing is not None:
                    if canonical_json_sha256(
                        existing.content_payload()
                    ) != canonical_json_sha256(member.content_payload()):
                        raise HistoricalRangeDatasetBridgeError(
                            REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                            "constituent source revision members conflict under one member key",
                        )
                    continue
                merged[member.member_key] = member
        union = build_source_revision_set(
            query_registry_hash=query_registry_hash,
            requested_source_cutoff=requested_cutoff,
            label_as_of_ts=label_as_of_ts,
            research_only=research_only,
            members=list(merged.values()),
        )
        union_payloads = {
            member.member_key: canonical_json_sha256(member.content_payload())
            for member in union.members
        }
        if union_payloads != {
            key: canonical_json_sha256(member.content_payload())
            for key, member in merged.items()
        }:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "snapshot source revision union lost or invented members",
            )
        for revision_set in constituents:
            for member in revision_set.members:
                if union_payloads.get(member.member_key) != canonical_json_sha256(
                    member.content_payload()
                ):
                    raise HistoricalRangeDatasetBridgeError(
                        REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                        "snapshot source revision union is not a superset of its constituents",
                    )
        PostgresSourceRevisionRepository(conn_factory=self._conn_factory).freeze(union)
        return (
            union.source_revision_set_id,
            union.source_revision_set_hash,
            union.query_registry_hash,
            union.requested_source_cutoff,
        )

    def _resolve_snapshot_policy_authority(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
    ) -> _SnapshotPolicyAuthority:
        policy_hashes = tuple(
            sorted({item.historical_range_policy_bundle_hash for item in labels})
        )
        if not policy_hashes:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "snapshot policy set cannot be empty",
            )
        refs_by_hash = {
            item.payload_sha256: item for item in request.policy_bundle_refs
        }
        members: list[RetrospectiveSnapshotPolicyMember] = []
        policy_ids: dict[str, str] = {}
        component_hashes_by_policy: dict[str, dict[str, str]] = {}
        component_set_hashes: dict[str, str] = {}
        for policy_hash in policy_hashes:
            policy_ref = refs_by_hash.get(policy_hash)
            component_hashes = request.policy_component_hashes.get(policy_hash)
            if policy_ref is None or component_hashes is None:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "snapshot label policy lies outside the exact bridge request",
                )
            policy = HistoricalRangeOutcomePolicyBundleV1.model_validate(
                self._artifact_store.load(policy_ref).payload
            )
            label_component_sets = {
                item.policy_component_set_hash
                for item in labels
                if item.historical_range_policy_bundle_hash == policy_hash
            }
            if len(label_component_sets) != 1:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "one snapshot policy requires one exact component set",
                )
            component_set_hash = next(iter(label_component_sets))
            policy_ids[policy_hash] = str(policy.policy_bundle_id)
            component_hashes_by_policy[policy_hash] = dict(component_hashes)
            component_set_hashes[policy_hash] = component_set_hash
            try:
                member = RetrospectiveSnapshotPolicyMember(
                    policy_bundle_id=policy_ids[policy_hash],
                    policy_bundle_hash=policy_hash,
                    policy_bundle_ref=policy_ref.model_dump(mode="json"),
                    policy_component_hashes=component_hashes,
                    policy_component_set_hash=component_set_hash,
                )
            except ValueError as exc:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "snapshot policy member differs from its exact component authority",
                ) from exc
            members.append(member)

        if len(members) == 1:
            policy_hash = policy_hashes[0]
            return _SnapshotPolicyAuthority(
                policy_bundle_id=policy_ids[policy_hash],
                policy_bundle_hash=policy_hash,
                policy_bundle_ref=refs_by_hash[policy_hash],
                component_hashes=component_hashes_by_policy[policy_hash],
                component_set_hash=component_set_hashes[policy_hash],
            )

        try:
            policy_set = RetrospectiveSnapshotPolicySet.from_members(members)
        except ValueError as exc:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "snapshot policy set differs from its exact members",
            ) from exc
        payload = policy_set.canonical_payload()
        upstream_refs = tuple(
            sorted(
                (refs_by_hash[policy_hash] for policy_hash in policy_hashes),
                key=lambda item: (
                    item.artifact_kind.value,
                    item.semantic_content_hash,
                    item.relative_path,
                ),
            )
        )
        stored = self._artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.REQUEST,
            producer_contract_version=SNAPSHOT_POLICY_SET_SCHEMA_VERSION,
            payload_schema_version=SNAPSHOT_POLICY_SET_SCHEMA_VERSION,
            resolved_request_hash=str(request.request_hash),
            payload=payload,
            upstream_refs=upstream_refs,
        )
        readback = self._artifact_store.load(stored.ref)
        if readback.payload != payload or readback.upstream_refs != upstream_refs:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "snapshot policy set artifact differs from its exact members",
            )
        return _SnapshotPolicyAuthority(
            policy_bundle_id=f"ahrpbs_{stored.ref.payload_sha256[:20]}",
            policy_bundle_hash=stored.ref.payload_sha256,
            policy_bundle_ref=stored.ref,
            component_hashes=policy_set.aggregate_component_hashes,
            component_set_hash=policy_set.aggregate_component_set_hash,
        )

    def _build_request(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        batches: tuple[CaptureBatch, ...],
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
        snapshot_source: tuple[str, str, str, datetime],
    ) -> RetrospectiveDatasetBuildRequest:
        policy_authority = self._resolve_snapshot_policy_authority(
            request=request,
            labels=labels,
        )
        range_scopes_by_id = {
            str(plan.range_scope.range_lineage_scope_id): FrozenIdentity(
                identity_id=str(plan.range_scope.range_lineage_scope_id),
                identity_hash=str(plan.range_scope.range_lineage_scope_hash),
            )
            for observation in observations
            for plan in observation.capture_plan_variants
        }
        captures = tuple(sorted((_capture_member(item) for item in batches), key=lambda item: item.capture_batch_id))
        source_id, source_hash, query_registry_hash, requested_source_cutoff = (
            snapshot_source
        )
        mapping_refs = self._replay_selected_observation_mappings(
            request=request,
            batches=batches,
            observations=observations,
            labels=labels,
        )
        selected_observations = mapping_refs
        selected_labels = self._select_labels(labels)
        targets = tuple(
            sorted(
                {
                    (item.horizon_trade_days, item.projection.value): LabelTargetIdentity(
                        horizon_trading_days=item.horizon_trade_days,
                        projection=item.projection.value,
                        projection_schema_version=OUTCOME_CALCULATION_SCHEMA_VERSION,
                    )
                    for item in labels
                }.values(),
                key=lambda item: (item.horizon_trading_days, item.projection),
            )
        )
        schema_fingerprint = canonical_json_sha256(
            {
                role: self._writer.schema_fingerprint(role)
                for role in sorted(self._writer.schemas)
            }
        )
        date_start = min(item.capture_plan.decision_as_of_trade_date for item in observations)
        date_end = max(item.capture_plan.decision_as_of_trade_date for item in observations)
        symbol_hashes = {
            item.capture_plan.symbol_normalization_policy_hash for item in observations
        }
        calendar_hashes = {item.capture_plan.calendar_hash for item in observations}
        if any(len(values) != 1 for values in (symbol_hashes, calendar_hashes)):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "snapshot observations have conflicting frozen policies",
            )
        # BUG-869: the universe policy hash is the per-decision-day universe
        # input membership identity, which legitimately differs across
        # trading days.  Freeze the day-bound binding set and store one
        # domain-tagged composite at snapshot level; every observation row
        # keeps its own per-day value as the row-level authority.  Same-day
        # conflicts fail closed.
        try:
            universe_policy_hash = build_snapshot_universe_policy_set_hash(
                (
                    item.capture_plan.decision_as_of_trade_date,
                    str(item.capture_plan.universe_policy_hash),
                )
                for item in observations
            )
        except SnapshotUniversePolicySetError as error:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "snapshot observations have conflicting same-day universe "
                f"policies: {error}",
            ) from error
        compatibility_hash = canonical_json_sha256(
            {
                "policy_bundle_hash": policy_authority.policy_bundle_hash,
                "policy_component_set_hash": policy_authority.component_set_hash,
                "label_targets": [item.model_dump(mode="json") for item in targets],
                "snapshot_universe_policy_set_hash": universe_policy_hash,
            }
        )
        return RetrospectiveDatasetBuildRequest(
            range_lineage_scopes=tuple(sorted(range_scopes_by_id.values(), key=lambda item: item.identity_id)),
            captures=captures,
            date_start=date_start,
            date_end=date_end,
            selected_observation_mappings=selected_observations,
            selected_label_mappings=selected_labels,
            label_policy_bundle_id=policy_authority.policy_bundle_id,
            label_policy_bundle_hash=policy_authority.policy_bundle_hash,
            historical_range_policy_bundle_ref=(
                policy_authority.policy_bundle_ref.model_dump(mode="json")
            ),
            label_targets=targets,
            universe_policy_hash=universe_policy_hash,
            benchmark_policy_hash=policy_authority.component_hashes["BENCHMARK"],
            cost_policy_hash=policy_authority.component_hashes["COST"],
            calendar_hash=next(iter(calendar_hashes)),
            symbol_normalization_policy_hash=next(iter(symbol_hashes)),
            query_registry_version=self._query_registry_version,
            query_registry_hash=query_registry_hash,
            snapshot_source_revision_set_id=source_id,
            snapshot_source_revision_set_hash=source_hash,
            required_composite_capabilities=(
                CompositeCapabilityRequirement(
                    component="labels", capability="RESEARCH_AUDIT"
                ),
                CompositeCapabilityRequirement(
                    component="observations", capability="INTERNAL_BOOTSTRAP"
                ),
            ),
            builder_version=BATCH_D_BUILDER_VERSION,
            code_commit=self._code_commit,
            writer_version=BATCH_D_WRITER_VERSION,
            snapshot_schema_version=RETROSPECTIVE_SNAPSHOT_SCHEMA_VERSION,
            schema_fingerprint=schema_fingerprint,
            partition_policy_id=self._partition_policy_id,
            partition_policy_hash=request.partition_policy_hash,
            policy_compatibility_hash=compatibility_hash,
            compression_config={"codec": "zstd", "level": 3},
            requested_source_cutoff=requested_source_cutoff.astimezone(UTC).date(),
            label_as_of_ts=_label_as_of(labels),
            selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
            selected_range_day_outcome_set_hash=canonical_json_sha256(
                [
                    item.semantic_content_hash
                    for item in sorted(
                        (*request.successful_day_refs, *request.outcome_refs),
                        key=lambda ref: ref.semantic_content_hash,
                    )
                ]
            ),
            policy_component_set_hash=policy_authority.component_set_hash,
        )

    def _select_labels(
        self,
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
    ) -> tuple[FrozenIdentity, ...]:
        label_as_of_ts = _label_as_of(labels)
        selected: list[FrozenIdentity] = []
        for label in labels:
            key = label_key_hash(
                canonical_signal_id=label.canonical_signal_id,
                symbol=label.symbol,
                label_policy_hash=label.historical_range_policy_bundle_hash,
                horizon_trading_days=label.horizon_trade_days,
                projection=label.projection,
            )
            versions = self._label_repository.chain_for(key)
            version = next(
                (
                    item
                    for item in versions
                    if item.label_version_id == label.label_version_id
                ),
                None,
            )
            if version is None:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "persisted retrospective label version is missing",
                )
            mapping = self._select_label_mapping(
                label=label,
                version=version,
                requested_label_as_of_ts=label_as_of_ts,
            )
            if (
                mapping.selected_label_mapping_id is None
                or mapping.selected_label_mapping_hash is None
                or mapping.terminal_label_version_id != label.label_version_id
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "retrospective label selector did not preserve the persisted label",
                )
            selected.append(
                FrozenIdentity(
                    identity_id=str(mapping.selected_label_mapping_id),
                    identity_hash=str(mapping.selected_label_mapping_hash),
                )
            )
        ordered = tuple(sorted(selected, key=lambda item: item.identity_id))
        if len({item.identity_id for item in ordered}) != len(ordered):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "retrospective selected label mappings are not unique",
            )
        return ordered

    @staticmethod
    def _select_label_mapping(
        *,
        label: HistoricalRangeBridgeLabelV1,
        version: OutcomeLabelVersion,
        requested_label_as_of_ts: datetime,
    ) -> SelectedLabelMapping:
        _, source_hash = _label_source_revision(label)
        return RetrospectiveExactLabelSelector().select(
            request=LabelSelectionRequest(
                selection_policy=LabelSelectionPolicy.EXACT_REVISION_V1,
                label_key_hash=label_key_hash(
                    canonical_signal_id=label.canonical_signal_id,
                    symbol=label.symbol,
                    label_policy_hash=label.historical_range_policy_bundle_hash,
                    horizon_trading_days=label.horizon_trade_days,
                    projection=label.projection,
                ),
                requested_label_as_of_ts=requested_label_as_of_ts,
                required_maturity_statuses=(
                    version.outcome_result.maturity_status,
                ),
                required_outcome_event_statuses=(
                    version.outcome_result.outcome_event_status,
                ),
                required_projection_schema_version=OUTCOME_CALCULATION_SCHEMA_VERSION,
                expected_observation_version_id=version.owner.observation_version_id,
                expected_candidate_stage_evidence_id=(
                    version.owner.candidate_stage_evidence_id
                ),
                expected_label_source_revision_set_hash=source_hash,
                explicit_label_version_id=version.label_version_id,
            ),
            label_versions=(version,),
        )

    def _validate_bridge_request(
        self, request: HistoricalRangeDatasetBridgeRequestV1
    ) -> None:
        if (
            request.artifact_root_identity_hash
            != self._artifact_store.root_identity_hash
            or request.builder_hash != self._builder_hash
            or request.writer_hash != self._writer_hash
            or request.retrospective_selector_policy_hash
            != RETROSPECTIVE_SELECTOR_POLICY_HASH
            or request.compression_config_hash
            != canonical_json_sha256({"codec": "zstd", "level": 3})
        ):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "bridge composition identities differ from the frozen request",
            )

    @staticmethod
    def _validate_capture_inputs(
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
    ) -> None:
        observation_signals = tuple(
            item.canonical_signal_id for item in observations
        )
        if len(observation_signals) != len(set(observation_signals)):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "bridge capture observations must have unique signal identities",
            )
        requested_outcomes = set(request.outcome_refs)
        requested_policies = set(request.policy_bundle_refs)
        label_signals: set[str] = set()
        policy_identity_by_signal: dict[
            str,
            tuple[HistoricalRangeArtifactRefV1, str, str],
        ] = {}
        for label in labels:
            label_signals.add(label.canonical_signal_id)
            policy_identity = (
                label.historical_range_policy_bundle_ref,
                label.historical_range_policy_bundle_hash,
                label.policy_component_set_hash,
            )
            existing_policy_identity = policy_identity_by_signal.setdefault(
                label.canonical_signal_id,
                policy_identity,
            )
            if existing_policy_identity != policy_identity:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "one canonical signal requires one exact policy/component set",
                )
            if (
                label.canonical_signal_id not in observation_signals
                or label.historical_range_policy_bundle_ref
                not in requested_policies
                or label.historical_range_policy_bundle_ref.payload_sha256
                != label.historical_range_policy_bundle_hash
                or not set(label.accepted_outcome_refs) <= requested_outcomes
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "bridge capture label lies outside the exact request",
                )
        if label_signals != set(observation_signals):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "bridge capture labels must cover every exact observation signal",
            )


def _capture_member(batch: CaptureBatch) -> RetrospectiveCaptureSetMember:
    request = batch.request
    if batch.status is not CaptureBatchStatus.COMPLETE:
        raise ValueError("capture member is not complete")
    if isinstance(request, RetrospectiveObservationCaptureBatchRequestV1):
        scope = request.binding.range_scope
        source_id = scope.signal_source_revision_set_id
        source_hash = scope.signal_source_revision_set_hash
        dates = tuple(item.decision_as_of_trade_date for item in request.plans)
    elif isinstance(request, RetrospectiveLabelCaptureBatchRequestV1):
        scope = request.binding.range_scope
        source_id = request.label_source_revision_set_id
        source_hash = request.label_source_revision_set_hash
        dates = tuple(item.decision_as_of_trade_date for item in request.planned_labels)
    else:
        raise ValueError("capture member is not retrospective")
    if batch.capture_receipt_hash is None or batch.membership_hash is None:
        raise HistoricalRangeDatasetBridgeError(
            REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
            "complete retrospective capture lacks receipt or membership closure",
        )
    return RetrospectiveCaptureSetMember(
        capture_batch_id=request.capture_batch_id,
        capture_request_hash=str(request.capture_request_hash),
        capture_receipt_hash=batch.capture_receipt_hash,
        membership_hash=batch.membership_hash,
        capture_purpose=request.capture_purpose,
        range_lineage_scope_id=str(scope.range_lineage_scope_id),
        range_lineage_scope_hash=str(scope.range_lineage_scope_hash),
        source_revision_set_id=source_id,
        source_revision_set_hash=source_hash,
        date_start=min(dates),
        date_end=max(dates),
    )


def _label_source_revision(label: HistoricalRangeBridgeLabelV1) -> tuple[str, str]:
    payload = label.calculation_evidence.evidence_payload
    source_id = str(payload.get("label_source_revision_set_id") or "")
    source_hash = str(payload.get("label_source_revision_set_hash") or "")
    if not source_id:
        raise HistoricalRangeDatasetBridgeError(
            REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
            "calculation evidence lacks label source revision id",
        )
    return source_id, _sha256(source_hash, "label_source_revision_set_hash")


def _label_as_of(labels: Iterable[HistoricalRangeBridgeLabelV1]) -> datetime:
    materialized = tuple(labels)
    if not materialized:
        raise ValueError("label collection cannot be empty")
    explicit_dates = tuple(
        item.label_as_of_trade_date for item in materialized
        if item.label_as_of_trade_date is not None
    )
    if explicit_dates:
        return datetime.combine(max(explicit_dates), time(23, 59, 59), tzinfo=UTC)
    result = max(
        item.outcome_result.source_closed_at
        or item.outcome_result.event_closed_at
        or item.outcome_result.scheduled_maturity_ts
        for item in materialized
    )
    if result.tzinfo is None or result.utcoffset() is None:
        return datetime.combine(result.date(), time(23, 59, 59), tzinfo=UTC)
    return result.astimezone(UTC)


def _phase1_ref(value: Any) -> HistoricalRangeArtifactReference:
    return HistoricalRangeArtifactReference.model_validate(value.model_dump(mode="json"))


def _prefixed_id(prefix: str, payload: Any) -> str:
    return f"{prefix}_{canonical_json_sha256(payload)[:24]}"


def _label_capture_id(
    *,
    bridge_request_hash: str,
    scope_hash: str,
    label_source_revision_id: str,
    label_source_revision_hash: str,
) -> str:
    return _prefixed_id(
        "ahr_lbl_cap",
        {
            "bridge_request_hash": bridge_request_hash,
            "scope_hash": scope_hash,
            "label_source_revision_id": label_source_revision_id,
            "label_source_revision_hash": label_source_revision_hash,
        },
    )


def _sha256(value: str, field_name: str) -> str:
    normalized = str(value)
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return normalized
