from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Protocol

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonical_json_text,
    canonicalize,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactRefV1,
    HistoricalRangeCandidateArtifactPayloadV2,
)
from backend.services.advisory_modeling.dataset_spool import RerankerDatasetSpool
from backend.services.advisory_modeling.errors import (
    AdvisoryModelingError,
    REASON_DATASET_SNAPSHOT_NOT_SEALED,
    REASON_FEATURE_CLOSURE_INCOMPLETE,
    REASON_PIT_VINTAGE_CONFLICT,
)
from backend.services.advisory_modeling.feature_builder import (
    FrozenCandidateFeatureInputV1,
    MultiAlphaLegInputV1,
    StageCandidateInputV1,
)
from backend.services.advisory_modeling.identity import FrozenModel, validated_hash
from backend.services.advisory_modeling.training_view import (
    DatasetBuildIntentV1,
    DatasetBuildRequestV1,
)
from backend.services.advisory_phase0b.snapshot_reader import (
    Phase0BSnapshotCatalogReceiptV1,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from backend.services.advisory_phase1.dataset_build import DatasetSnapshotBlobRef
from backend.services.advisory_phase1.snapshot_writer import (
    SCHEMA_DESCRIPTOR_ROLE,
    DatasetManifest,
    read_verified_snapshot_parquet_rows,
)


_ROLE_IDENTITIES: dict[str, tuple[str, ...]] = {
    "canonical_signals": ("canonical_signal_id",),
    "observation_versions": ("observation_version_id",),
    "selected_observations": ("selected_mapping_id",),
    "lineage": ("lineage_id",),
    "stage_summaries": ("stage_evidence_id",),
    "stage_candidates": ("stage_evidence_id", "symbol"),
    "selected_labels": ("selected_label_mapping_id",),
    "source_revisions": ("source_revision_set_hash", "member_key"),
    "outcome_labels": ("label_version_id",),
    "outcome_source_evidence": ("owner_type", "label_version_id"),
}


def _resolve_multi_alpha_legs(
    *,
    components: list[Any],
    raw_component_scores: dict[str, Any],
    expected_component_set_hash: str,
    symbol: str,
) -> tuple[MultiAlphaLegInputV1, ...]:
    component_payloads: dict[str, dict[str, Any]] = {}
    for item in components:
        if not isinstance(item, dict):
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "candidate artifact contains a non-object admitted component",
                context={"symbol": symbol},
            )
        component_id = str(item.get("component_id") or "")
        if not component_id or component_id in component_payloads:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "candidate artifact admitted component identities are incomplete or duplicated",
                context={"component_id": component_id, "symbol": symbol},
            )
        component_payloads[component_id] = dict(item)
    component_set_hash = canonical_json_sha256(
        tuple(canonicalize(component_payloads[key]) for key in sorted(component_payloads))
    )
    if component_set_hash != expected_component_set_hash:
        raise AdvisoryModelingError(
            REASON_PIT_VINTAGE_CONFLICT,
            "candidate component identity set differs from dataset request",
            context={"symbol": symbol},
        )
    if set(raw_component_scores) != set(component_payloads):
        raise AdvisoryModelingError(
            REASON_FEATURE_CLOSURE_INCOMPLETE,
            "multi-alpha score evidence does not close every admitted component",
            context={"symbol": symbol},
        )
    legs: list[MultiAlphaLegInputV1] = []
    for component_id, score_payload in sorted(raw_component_scores.items()):
        if not isinstance(score_payload, dict):
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "multi-alpha score/component identity mapping is incomplete",
                context={"component_id": component_id, "symbol": symbol},
            )
        try:
            score_weight = Decimal(str(score_payload["weight"]))
            component_weight = Decimal(str(component_payloads[component_id]["weight"]))
            normalized_score = Decimal(str(score_payload["normalized_score"]))
        except (KeyError, InvalidOperation) as exc:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "multi-alpha score evidence lacks a finite normalized score or weight",
                context={"component_id": component_id, "symbol": symbol},
            ) from exc
        if not all(value.is_finite() for value in (score_weight, component_weight, normalized_score)):
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "multi-alpha score evidence contains a non-finite value",
                context={"component_id": component_id, "symbol": symbol},
            )
        if score_weight != component_weight:
            raise AdvisoryModelingError(
                REASON_PIT_VINTAGE_CONFLICT,
                "multi-alpha score weight differs from the admitted component weight",
                context={"component_id": component_id, "symbol": symbol},
            )
        legs.append(
            MultiAlphaLegInputV1(
                component_id=component_id,
                score=normalized_score,
                weight=score_weight,
                model_identity_hash=canonical_json_sha256(component_payloads[component_id]),
            )
        )
    return tuple(legs)


class SnapshotCatalog(Protocol):
    def read_once(self, *, snapshot_ids: tuple[str, ...]) -> Phase0BSnapshotCatalogReceiptV1: ...


class BaseSnapshotReadReceiptV1(FrozenModel):
    schema_version: Literal["advisory_reranker_base_snapshot_read_receipt_v1"] = (
        "advisory_reranker_base_snapshot_read_receipt_v1"
    )
    snapshot_id: str = Field(min_length=1, max_length=160)
    snapshot_content_hash: str = Field(min_length=64, max_length=64)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    first_catalog_content_hash: str = Field(min_length=64, max_length=64)
    final_catalog_content_hash: str = Field(min_length=64, max_length=64)
    candidate_group_count: int = Field(ge=1)
    candidate_row_count: int = Field(ge=1)
    decision_dates: tuple[date, ...]
    base_role_partition_set_hash: str = Field(min_length=64, max_length=64)
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "snapshot_content_hash",
        "manifest_sha256",
        "first_catalog_content_hash",
        "final_catalog_content_hash",
        "base_role_partition_set_hash",
        "receipt_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "BaseSnapshotReadReceiptV1":
        if self.first_catalog_content_hash != self.final_catalog_content_hash:
            raise ValueError("snapshot catalog changed during base read")
        if tuple(sorted(set(self.decision_dates))) != self.decision_dates:
            raise ValueError("base decision dates must be unique and ascending")
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"receipt_hash"}))
        if self.receipt_hash is not None and self.receipt_hash != digest:
            raise ValueError("base read receipt hash differs from canonical payload")
        object.__setattr__(self, "receipt_hash", digest)
        return self


class HistoricalCandidateArtifactResolver:
    def __init__(self, *, artifact_store: HistoricalRangeArtifactStore) -> None:
        self._store = artifact_store
        self._cache: dict[str, HistoricalRangeCandidateArtifactPayloadV2] = {}

    def load(self, *, ref_payload: dict[str, Any], expected_hash: str) -> HistoricalRangeCandidateArtifactPayloadV2:
        ref = HistoricalRangeArtifactRefV1.model_validate(ref_payload)
        if ref.semantic_content_hash != expected_hash:
            raise AdvisoryModelingError(
                REASON_PIT_VINTAGE_CONFLICT,
                "base candidate artifact ref/hash are inconsistent",
            )
        cached = self._cache.get(expected_hash)
        if cached is not None:
            return cached
        envelope = self._store.load(ref)
        payload = HistoricalRangeCandidateArtifactPayloadV2.model_validate(envelope.payload)
        self._cache[expected_hash] = payload
        return payload


class RerankerBaseSnapshotReader:
    """Verify one explicit SEALED retrospective snapshot and normalize candidate groups."""

    def __init__(
        self,
        *,
        catalog: SnapshotCatalog,
        dataset_store: LocalContentAddressedStore,
        candidate_artifacts: HistoricalCandidateArtifactResolver,
    ) -> None:
        self._catalog = catalog
        self._dataset_store = dataset_store
        self._candidate_artifacts = candidate_artifacts

    def read(
        self,
        *,
        snapshot_id: str,
        intent: DatasetBuildIntentV1,
        spool: RerankerDatasetSpool,
    ) -> tuple[
        tuple[tuple[FrozenCandidateFeatureInputV1, ...], ...],
        BaseSnapshotReadReceiptV1,
        DatasetBuildRequestV1,
    ]:
        first = self._catalog.read_once(snapshot_ids=(snapshot_id,))
        if len(first.entries) != 1:
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "base snapshot catalog did not return exactly one explicit snapshot",
            )
        entry = first.entries[0]
        if entry.lineage_identity_type != "HISTORICAL_RANGE":
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "base snapshot is not Historical Range retrospective evidence",
            )
        header = entry.header_payload()
        manifest_sha256 = str(header.get("manifest_sha256") or "")
        snapshot_content_hash = str(header.get("snapshot_content_hash") or "")
        try:
            manifest = DatasetManifest.model_validate_json(
                self._dataset_store.read_document_bytes(kind="manifests", sha256=manifest_sha256)
            )
        except Exception as exc:
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "base snapshot manifest could not be verified",
                context={"snapshot_id": snapshot_id, "error_type": type(exc).__name__},
            ) from exc
        if (
            manifest.manifest_sha256 != manifest_sha256
            or manifest.core.manifest_core_sha256 != snapshot_content_hash
            or manifest.store_backend_hash != self._dataset_store.store_backend_hash
            or manifest.core.lineage_identity_type != "HISTORICAL_RANGE"
            or manifest.core.evidence_scope != "RETROSPECTIVE_RESEARCH_ONLY"
        ):
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "base snapshot manifest identity or research scope is inconsistent",
                context={"snapshot_id": snapshot_id},
            )
        catalog_files = tuple(item.model_dump(mode="json") for item in entry.files)
        manifest_files = tuple(
            item.model_dump(mode="json")
            for item in sorted(manifest.core.files, key=lambda value: value.logical_path)
        )
        if canonicalize(catalog_files) != canonicalize(manifest_files):
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "base snapshot manifest files differ from catalog",
            )
        catalog_observations = self._membership_payloads(entry.observation_membership_json)
        catalog_labels = self._membership_payloads(entry.label_membership_json)
        manifest_observations = tuple(
            sorted(
                canonical_json_text(item.model_dump(mode="json"))
                for item in manifest.core.selected_observations
            )
        )
        manifest_labels = tuple(
            sorted(
                canonical_json_text(item.model_dump(mode="json"))
                for item in manifest.core.selected_labels
            )
        )
        if catalog_observations != manifest_observations or catalog_labels != manifest_labels:
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "base snapshot DB membership differs from manifest",
            )
        expected_blob_refs = tuple(
            sorted(
                canonical_json_text(
                    {
                        "logical_path": ref.logical_path,
                        "logical_role": ref.logical_role,
                        "partition_key_hash": ref.partition_key_hash,
                        "ordinal": ref.ordinal,
                        "store_backend_hash": ref.blob.store_backend_hash,
                        "blob_sha256": ref.blob.blob_sha256,
                        "ref_content_hash": ref.ref_content_hash,
                    }
                )
                for ref in (
                    DatasetSnapshotBlobRef(
                        logical_path=file.logical_path,
                        logical_role=file.logical_role,
                        partition_key_hash=file.partition_key_hash,
                        ordinal=file.ordinal,
                        blob=file.blob,
                    )
                    for file in entry.files
                )
            )
        )
        if self._membership_payloads(entry.blob_ref_membership_json) != expected_blob_refs:
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "base snapshot blob membership differs from file descriptors",
            )
        required_roles = {
            "canonical_signals",
            "observation_versions",
            "stage_summaries",
            "stage_candidates",
            "outcome_labels",
            "selected_labels",
            "outcome_source_evidence",
            "source_revisions",
        }
        actual_roles = {item.logical_role for item in entry.files}
        if not required_roles.issubset(actual_roles):
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "base snapshot lacks required training roles",
                context={"missing_roles": tuple(sorted(required_roles - actual_roles))},
            )
        rows_by_role: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for file in entry.files:
            if file.blob.store_backend_hash != self._dataset_store.store_backend_hash:
                raise AdvisoryModelingError(
                    REASON_DATASET_SNAPSHOT_NOT_SEALED,
                    "base snapshot file store identity differs from configured store",
                )
            if file.logical_role == SCHEMA_DESCRIPTOR_ROLE:
                self._dataset_store.read_blob_bytes(
                    uri=file.content_uri,
                    sha256=file.sha256,
                    size_bytes=file.size_bytes,
                )
                continue
            rows = read_verified_snapshot_parquet_rows(
                file=file,
                store=self._dataset_store,
                lineage_identity_type=entry.lineage_identity_type,
            )
            if file.logical_role in {
                "canonical_signals",
                "observation_versions",
                "stage_summaries",
                "stage_candidates",
                "outcome_labels",
                "selected_labels",
                "source_revisions",
            }:
                rows_by_role[file.logical_role].extend(rows)
            identity_fields = _ROLE_IDENTITIES.get(file.logical_role)
            if identity_fields is not None:
                spool.append_partition(
                    source_kind="BASE_SNAPSHOT",
                    source_identity=snapshot_id,
                    logical_role=file.logical_role,
                    partition_key=file.partition_key_hash,
                    rows=rows,
                    identity_fields=identity_fields,
                    trade_date_field=(
                        "decision_as_of_trade_date"
                        if file.logical_role in {"canonical_signals", "outcome_labels"}
                        else None
                    ),
                    symbol_field="symbol" if file.logical_role in {"stage_candidates", "outcome_labels"} else None,
                )
        request = self._finalize_request(intent=intent, manifest=manifest, rows_by_role=rows_by_role)
        self._verify_request_identity(request=request, manifest=manifest, rows_by_role=rows_by_role)
        groups = self._candidate_groups(
            snapshot_id=snapshot_id,
            request=request,
            rows_by_role=rows_by_role,
        )
        final = self._catalog.read_once(snapshot_ids=(snapshot_id,))
        if (
            first.catalog_content_set_hash != final.catalog_content_set_hash
            or first.database_target.target_receipt_hash != final.database_target.target_receipt_hash
        ):
            raise AdvisoryModelingError(
                REASON_PIT_VINTAGE_CONFLICT,
                "base snapshot changed during verified read",
                context={"snapshot_id": snapshot_id},
            )
        receipts = spool.partition_receipts(
            source_kind="BASE_SNAPSHOT",
            source_identity=snapshot_id,
        )
        decision_dates = tuple(sorted({group[0].decision_trade_date for group in groups}))
        receipt = BaseSnapshotReadReceiptV1(
            snapshot_id=snapshot_id,
            snapshot_content_hash=snapshot_content_hash,
            manifest_sha256=manifest_sha256,
            first_catalog_content_hash=str(first.catalog_content_set_hash),
            final_catalog_content_hash=str(final.catalog_content_set_hash),
            candidate_group_count=len(groups),
            candidate_row_count=sum(len(group) for group in groups),
            decision_dates=decision_dates,
            base_role_partition_set_hash=canonical_json_sha256(receipts),
        )
        return groups, receipt, request

    @staticmethod
    def _membership_payloads(values: tuple[str, ...]) -> tuple[str, ...]:
        payloads: list[str] = []
        for value in values:
            payload = json.loads(value)
            if not isinstance(payload, dict):
                raise AdvisoryModelingError(
                    REASON_DATASET_SNAPSHOT_NOT_SEALED,
                    "base snapshot membership payload is not an object",
                )
            payload.pop("snapshot_id", None)
            payloads.append(canonical_json_text(payload))
        return tuple(sorted(payloads))

    @staticmethod
    def _finalize_request(
        *,
        intent: DatasetBuildIntentV1,
        manifest: DatasetManifest,
        rows_by_role: dict[str, list[dict[str, Any]]],
    ) -> DatasetBuildRequestV1:
        source_rows = rows_by_role["source_revisions"]
        source_identities = {
            (str(row["source_revision_set_id"]), str(row["source_revision_set_hash"]))
            for row in source_rows
        }
        if len(source_identities) != 1:
            raise AdvisoryModelingError(
                REASON_PIT_VINTAGE_CONFLICT,
                "base snapshot does not expose one composite source revision authority",
                context={"source_identity_count": len(source_identities)},
            )
        source_id, source_hash = next(iter(source_identities))
        if manifest.core.snapshot_source_revision_set_hash != source_hash:
            raise AdvisoryModelingError(
                REASON_PIT_VINTAGE_CONFLICT,
                "base manifest source authority differs from source revision rows",
            )
        universe_hashes = tuple(
            sorted({str(row["universe_policy_hash"]) for row in rows_by_role["observation_versions"]})
        )
        if not universe_hashes:
            raise AdvisoryModelingError(
                REASON_PIT_VINTAGE_CONFLICT,
                "base snapshot has no frozen universe policy identities",
            )
        universe_set_hash = canonical_json_sha256(universe_hashes)
        return intent.finalize(
            source_revision_set_id=source_id,
            source_revision_set_hash=source_hash,
            universe_policy_set_id=f"advups_{universe_set_hash[:24]}",
            universe_policy_set_hash=universe_set_hash,
        )

    @staticmethod
    def _verify_request_identity(
        *,
        request: DatasetBuildRequestV1,
        manifest: DatasetManifest,
        rows_by_role: dict[str, list[dict[str, Any]]],
    ) -> None:
        signals = rows_by_role["canonical_signals"]
        if not signals:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "base snapshot has no canonical candidate signals",
            )
        for row in signals:
            if (
                row["package_id"] != request.package_id
                or row["manifest_sha256"] != request.package_manifest_sha256
                or row["selection_runtime_semantics_hash"]
                != request.selection_runtime_semantics_hash
                or row["calendar_version"] != request.calendar_version
                or row["calendar_hash"] != request.calendar_hash
            ):
                raise AdvisoryModelingError(
                    REASON_PIT_VINTAGE_CONFLICT,
                    "base signal identity differs from dataset request",
                    context={"canonical_signal_id": row["canonical_signal_id"]},
                )
            decision = date.fromisoformat(str(row["decision_as_of_trade_date"]))
            if not request.decision_date_start <= decision <= request.decision_date_end:
                raise AdvisoryModelingError(
                    REASON_PIT_VINTAGE_CONFLICT,
                    "base signal decision date escapes dataset request",
                )
        if (
            manifest.core.snapshot_source_revision_set_hash != request.source_revision_set_hash
            or manifest.core.query_registry_hash is None
        ):
            raise AdvisoryModelingError(
                REASON_PIT_VINTAGE_CONFLICT,
                "base source revision authority differs from dataset request",
            )

    def _candidate_groups(
        self,
        *,
        snapshot_id: str,
        request: DatasetBuildRequestV1,
        rows_by_role: dict[str, list[dict[str, Any]]],
    ) -> tuple[tuple[FrozenCandidateFeatureInputV1, ...], ...]:
        signals = {row["canonical_signal_id"]: row for row in rows_by_role["canonical_signals"]}
        observations_by_signal: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows_by_role["observation_versions"]:
            observations_by_signal[str(row["canonical_signal_id"])].append(row)
        stages_by_observation: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        stage_by_id: dict[str, dict[str, Any]] = {}
        for row in rows_by_role["stage_summaries"]:
            stages_by_observation[str(row["observation_version_id"])][str(row["stage"])] = row
            stage_by_id[str(row["stage_evidence_id"])] = row
        candidates_by_stage: dict[str, dict[str, Any]] = defaultdict(dict)
        for row in rows_by_role["stage_candidates"]:
            if row["membership_status"] == "INCLUDED":
                candidates_by_stage[str(row["stage_evidence_id"])][str(row["symbol"])] = row
        grouped: dict[tuple[str, str, str], list[FrozenCandidateFeatureInputV1]] = defaultdict(list)
        expected_stages = (
            "alpha_raw",
            "hmm_adjusted",
            "risk_policy_adjusted",
            "selection_effective",
        )
        for signal_id, signal in sorted(signals.items()):
            observations = observations_by_signal.get(signal_id, [])
            if len(observations) != 1:
                raise AdvisoryModelingError(
                    REASON_FEATURE_CLOSURE_INCOMPLETE,
                    "canonical signal does not have exactly one selected observation",
                    context={"canonical_signal_id": signal_id, "observation_count": len(observations)},
                )
            observation = observations[0]
            stages = stages_by_observation[str(observation["observation_version_id"])]
            if not set(expected_stages).issubset(stages):
                raise AdvisoryModelingError(
                    REASON_FEATURE_CLOSURE_INCOMPLETE,
                    "base observation lacks four required stages",
                    context={"canonical_signal_id": signal_id},
                )
            selection_rows = candidates_by_stage[str(stages["selection_effective"]["stage_evidence_id"])]
            if len(selection_rows) != 1:
                raise AdvisoryModelingError(
                    REASON_FEATURE_CLOSURE_INCOMPLETE,
                    "base observation does not identify exactly one included candidate",
                    context={"canonical_signal_id": signal_id},
                )
            symbol = next(iter(selection_rows))
            stage_inputs: list[dict[str, Any]] = []
            for stage_name in expected_stages:
                stage = stages[stage_name]
                row = candidates_by_stage[str(stage["stage_evidence_id"])].get(symbol)
                if row is None or row.get("rank") is None or row.get("score_decimal") is None:
                    raise AdvisoryModelingError(
                        REASON_FEATURE_CLOSURE_INCOMPLETE,
                        "required candidate stage evidence is absent",
                        context={"canonical_signal_id": signal_id, "stage": stage_name},
                    )
                stage_inputs.append(
                    {
                        "stage": stage_name,
                        "rank": int(row["rank"]),
                        "score": Decimal(str(row["score_decimal"])),
                        "stage_evidence_id": str(stage["stage_evidence_id"]),
                        "candidate_content_hash": str(row["candidate_content_hash"]),
                    }
                )
            artifact = self._candidate_artifacts.load(
                ref_payload=dict(observation["candidate_artifact_ref"]),
                expected_hash=str(observation["candidate_artifact_hash"]),
            )
            facts = tuple(item for item in artifact.candidates if item.symbol == symbol)
            if len(facts) != 1:
                raise AdvisoryModelingError(
                    REASON_FEATURE_CLOSURE_INCOMPLETE,
                    "candidate artifact does not contain the exact base candidate",
                    context={"symbol": symbol, "canonical_signal_id": signal_id},
                )
            fact = facts[0]
            if (
                artifact.package_id != request.package_id
                or artifact.manifest_sha256 != request.package_manifest_sha256
                or artifact.selection_semantics_hash != request.selection_runtime_semantics_hash
                or artifact.raw_signal_semantic_header.get("asset_closure_hash")
                != request.package_asset_closure_hash
            ):
                raise AdvisoryModelingError(
                    REASON_PIT_VINTAGE_CONFLICT,
                    "candidate artifact package/runtime/asset identity differs from dataset request",
                    context={"symbol": symbol},
                )
            lineage = fact.component_lineage_json
            if (
                lineage.get("schema_version") != request.multi_alpha_parent_contract_version
                or lineage.get("package_id") != request.package_id
                or lineage.get("manifest_sha256") != request.package_manifest_sha256
                or lineage.get("alpha_mode") != "multi_alpha"
            ):
                raise AdvisoryModelingError(
                    REASON_PIT_VINTAGE_CONFLICT,
                    "candidate component lineage differs from requested multi-alpha parent",
                    context={"symbol": symbol},
                )
            raw_component_scores = lineage.get("component_scores")
            components = lineage.get("components")
            if not isinstance(raw_component_scores, dict) or not isinstance(components, list):
                raise AdvisoryModelingError(
                    REASON_FEATURE_CLOSURE_INCOMPLETE,
                    "candidate artifact lacks frozen multi-alpha component evidence",
                    context={"symbol": symbol},
                )
            legs = _resolve_multi_alpha_legs(
                components=components,
                raw_component_scores=raw_component_scores,
                expected_component_set_hash=request.multi_alpha_component_identity_set_hash,
                symbol=symbol,
            )
            exact_stage_inputs: list[StageCandidateInputV1] = []
            for current in stage_inputs:
                receipt = artifact.stage_trace[str(current["stage"])]
                receipt_candidates = receipt.get("candidates")
                if not isinstance(receipt_candidates, list):
                    raise AdvisoryModelingError(
                        REASON_FEATURE_CLOSURE_INCOMPLETE,
                        "candidate artifact stage lacks its frozen candidate rows",
                        context={"stage": current["stage"], "symbol": symbol},
                    )
                ordered = sorted(
                    (
                        (int(item["rank"]), str(item["symbol"]), Decimal(str(item["score"])))
                        for item in receipt_candidates
                        if isinstance(item, dict)
                    ),
                    key=lambda item: (item[0], item[1]),
                )
                matches = [index for index, item in enumerate(ordered) if item[1] == symbol]
                if (
                    len(matches) != 1
                    or ordered[matches[0]][0] != current["rank"]
                    or ordered[matches[0]][2] != current["score"]
                    or int(receipt.get("output_count", -1)) != len(ordered)
                ):
                    raise AdvisoryModelingError(
                        REASON_FEATURE_CLOSURE_INCOMPLETE,
                        "snapshot and candidate artifact stage evidence differ",
                        context={"stage": current["stage"], "symbol": symbol},
                    )
                index = matches[0]
                exact_stage_inputs.append(
                    StageCandidateInputV1(
                        stage=current["stage"],
                        rank=current["rank"],
                        score=current["score"],
                        stage_candidate_count=len(ordered),
                        previous_rank_score=None if index == 0 else ordered[index - 1][2],
                        next_rank_score=None if index == len(ordered) - 1 else ordered[index + 1][2],
                        stage_evidence_id=current["stage_evidence_id"],
                        candidate_content_hash=current["candidate_content_hash"],
                    )
                )
            metadata = artifact.stage_trace.get("metadata")
            metadata = metadata if isinstance(metadata, dict) else {}
            hmm = metadata.get("hmm") if isinstance(metadata.get("hmm"), dict) else {}
            risk = metadata.get("risk") if isinstance(metadata.get("risk"), dict) else {}
            hmm_receipt_candidate = next(
                (
                    item
                    for item in artifact.stage_trace["hmm_adjusted"].get("candidates", [])
                    if isinstance(item, dict) and item.get("symbol") == symbol
                ),
                None,
            )
            risk_receipt_candidate = next(
                (
                    item
                    for item in artifact.stage_trace["risk_policy_adjusted"].get("candidates", [])
                    if isinstance(item, dict) and item.get("symbol") == symbol
                ),
                None,
            )
            hmm_component = (
                (hmm_receipt_candidate.get("component_scores") or {}).get("hmm")
                if isinstance(hmm_receipt_candidate, dict)
                else None
            )
            hmm_component = hmm_component if isinstance(hmm_component, dict) else {}
            risk_component = (
                (risk_receipt_candidate.get("component_scores") or {}).get("event_risk")
                if isinstance(risk_receipt_candidate, dict)
                else None
            )
            risk_component = risk_component if isinstance(risk_component, dict) else {}
            hmm_enabled = observation.get("hmm_snapshot_status") != "NOT_APPLICABLE"
            stable_hash = str(signal["stable_signal_semantics_hash"])
            decision_text = str(signal["decision_as_of_trade_date"])
            target_text = str(signal["target_trade_date"])
            candidate = FrozenCandidateFeatureInputV1(
                base_snapshot_id=snapshot_id,
                canonical_signal_id=signal_id,
                stable_signal_semantics_hash=stable_hash,
                canonical_signal_scope_hash=str(signal["canonical_signal_scope_hash"]),
                observation_version_id=str(observation["observation_version_id"]),
                observation_content_hash=str(observation["observation_content_hash"]),
                symbol=symbol,
                decision_trade_date=date.fromisoformat(decision_text),
                decision_cutoff_ts=signal["decision_cutoff_ts"],
                target_trade_date=date.fromisoformat(target_text),
                stage_candidates=tuple(exact_stage_inputs),
                multi_alpha_legs=legs,
                component_evidence_hash=str(fact.component_lineage_hash),
                hmm_enabled=hmm_enabled,
                hmm_snapshot_id=observation.get("hmm_snapshot_id"),
                hmm_snapshot_hash=observation.get("hmm_snapshot_hash"),
                hmm_snapshot_status=str(observation["hmm_snapshot_status"]),
                hmm_freshness_trade_days=hmm.get("freshness_lag"),
                hmm_coefficient=hmm_component.get("coefficient"),
                risk_enabled=bool(risk.get("enabled")),
                risk_policy_hash=str(observation["risk_policy_hash"]),
                risk_can_buy=risk_component.get("can_buy"),
                risk_multiplier=risk_component.get("score_multiplier"),
                risk_delta=risk_component.get("score_delta"),
                risk_penalty=risk_component.get("rank_penalty"),
                universe_policy_hash=str(observation["universe_policy_hash"]),
            )
            grouped[(decision_text, target_text, stable_hash)].append(candidate)
        ordered_groups: list[tuple[FrozenCandidateFeatureInputV1, ...]] = []
        for group_key in sorted(grouped):
            group = tuple(sorted(grouped[group_key], key=lambda item: item.symbol))
            if len(group) > request.candidate_observation_top_k:
                raise AdvisoryModelingError(
                    REASON_FEATURE_CLOSURE_INCOMPLETE,
                    "base ranking group exceeds frozen candidate top-k",
                    context={"group_key_hash": canonical_json_sha256(group_key), "candidate_count": len(group)},
                )
            ordered_groups.append(group)
        if not ordered_groups:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "base snapshot produced no candidate ranking groups",
            )
        return tuple(ordered_groups)
