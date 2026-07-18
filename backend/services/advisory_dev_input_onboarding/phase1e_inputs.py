"""Phase 1E real-input contracts, immutable publication, and Program-local builders."""

from __future__ import annotations

from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
    validate_sha256,
)
from backend.services.advisory_phase1.readiness_plan_store import ContentAddressedPlanStore


StoredContractT = TypeVar("StoredContractT", bound=BaseModel)


class Phase1EInputArtifactStore:
    """O4-only typed facade over the existing atomic Phase 1E CAS."""

    def __init__(self, *, root: Path, producer_code_commit: str | None = None) -> None:
        self._store = ContentAddressedPlanStore(
            root=root,
            policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
            producer_code_commit=producer_code_commit,
        )

    @property
    def root(self) -> Path:
        return self._store.root

    def publish(
        self,
        *,
        artifact_kind: O4ArtifactKind,
        model: BaseModel,
        semantic_hash: str,
    ) -> AdvisoryImmutableArtifactRef:
        semantic_hash = validate_sha256(semantic_hash, field_name="semantic_hash")
        actual_semantic_hash = _model_semantic_hash(model=model, artifact_kind=artifact_kind)
        if actual_semantic_hash != semantic_hash:
            raise ValueError("O4 artifact semantic hash differs from the typed payload before publication")
        payload = model.model_dump(mode="json")
        document = self._store.publish(
            kind=artifact_kind.value,
            identity=semantic_hash,
            payload=payload,
            semantic_hash=semantic_hash,
        )
        ref = AdvisoryImmutableArtifactRef(
            artifact_kind=artifact_kind.value,
            store_policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
            relative_path=self._store.relative_path(
                kind=artifact_kind.value,
                identity=semantic_hash,
                semantic_hash=semantic_hash,
            ),
            semantic_hash=semantic_hash,
            file_sha256=str(document["file_sha256"]),
        )
        self.load(ref=ref, model_type=type(model))
        return ref

    def load(
        self,
        *,
        ref: AdvisoryImmutableArtifactRef,
        model_type: type[StoredContractT],
    ) -> StoredContractT:
        if ref.store_policy_hash != O4_ARTIFACT_STORE_POLICY_HASH:
            raise ValueError("O4 artifact ref store policy hash is invalid")
        kind = O4ArtifactKind(ref.artifact_kind)
        expected_path = self._store.relative_path(
            kind=kind.value,
            identity=ref.semantic_hash,
            semantic_hash=ref.semantic_hash,
        )
        if ref.relative_path != expected_path:
            raise ValueError("O4 artifact ref path differs from its semantic identity")
        document = self._store.verify(
            kind=kind.value,
            identity=ref.semantic_hash,
            semantic_hash=ref.semantic_hash,
        )
        if str(document.get("file_sha256") or "") != ref.file_sha256:
            raise ValueError("O4 artifact ref file hash differs from full readback")
        model = model_type.model_validate(document["payload"])
        if _model_semantic_hash(model=model, artifact_kind=kind) != ref.semantic_hash:
            raise ValueError("O4 artifact payload semantic hash differs from its ref")
        return model


def _model_semantic_hash(*, model: BaseModel, artifact_kind: O4ArtifactKind) -> str:
    field_by_kind = {
        O4ArtifactKind.REAL_INPUT_BUILD_REQUEST: "build_request_hash",
        O4ArtifactKind.STRATEGY_PACKAGE_INPUT_PROJECTION: "projection_hash",
        O4ArtifactKind.SOURCE_MAPPING_REGISTRY: "registry_hash",
        O4ArtifactKind.SOURCE_OBSERVATION_SCOPE_REQUEST: "observation_scope_hash",
        O4ArtifactKind.SOURCE_REQUIREMENT_REGISTRY: "registry_hash",
        O4ArtifactKind.SOURCE_REQUIREMENT_SET: "requirement_set_hash",
        O4ArtifactKind.CAPACITY_POLICY: "policy_hash",
        O4ArtifactKind.CAPACITY_REQUEST: "request_hash",
        O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD: "program_workload_hash",
        O4ArtifactKind.CAPACITY_RECEIPT: "receipt_hash",
        O4ArtifactKind.CAPACITY_PROGRAM_COVERAGE: "coverage_hash",
        O4ArtifactKind.PROGRAM_INPUT: "program_input_hash",
        O4ArtifactKind.INPUT_BUNDLE: "input_bundle_hash",
        O4ArtifactKind.PHASE1E_PROGRAM_DATE_REQUEST: "program_date_request_hash",
        O4ArtifactKind.PHASE1E_BATCH_REQUEST: "invocation_request_hash",
    }
    field_name = field_by_kind[artifact_kind]
    value = getattr(model, field_name, None)
    if value is None:
        raise ValueError(f"{artifact_kind.value} model does not expose closed {field_name}")
    return validate_sha256(str(value), field_name=field_name)
