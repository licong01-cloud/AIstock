from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Mapping

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonical_json_text,
)
from backend.services.advisory_modeling.bundle_store import (
    ArtifactCompletionReceiptV1,
    BundleFileDescriptorV1,
    ImmutableArtifactStore,
    build_file_descriptors,
)
from backend.services.advisory_modeling.errors import (
    AdvisoryModelingError,
    REASON_FEATURE_SNAPSHOT_INCOMPLETE,
)
from backend.services.advisory_modeling.identity import (
    FrozenModel,
    set_computed_hash,
    utc_datetime,
    validated_hash,
)


FEATURE_SNAPSHOT_SCHEMA_VERSION = "advisory_reranker_feature_snapshot_v1"
FEATURE_SOURCE_REVISION_SCHEMA_VERSION = "advisory_reranker_feature_source_revision_v1"


class FeatureSourceRevisionV1(FrozenModel):
    schema_version: Literal[FEATURE_SOURCE_REVISION_SCHEMA_VERSION] = (
        FEATURE_SOURCE_REVISION_SCHEMA_VERSION
    )
    query_template_id: str = Field(min_length=1, max_length=160)
    bound_parameter_hash: str = Field(min_length=64, max_length=64)
    partition_hash: str = Field(min_length=64, max_length=64)
    row_count: int = Field(ge=0)
    available_at: datetime
    source_revision_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "bound_parameter_hash", "partition_hash", "source_revision_hash"
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @field_validator("available_at")
    @classmethod
    def _available_at(cls, value: datetime) -> datetime:
        return utc_datetime(value, field_name="available_at")

    @model_validator(mode="after")
    def _identity(self) -> "FeatureSourceRevisionV1":
        set_computed_hash(self, field_name="source_revision_hash", exclude={"source_revision_hash"})
        return self


class FeaturePartitionDescriptorV1(FrozenModel):
    decision_date: date
    relative_path: str = Field(pattern=r"^feature_rows/date=\d{4}-\d{2}-\d{2}/part-00000\.parquet$")
    row_count: int = Field(ge=1)
    content_sha256: str = Field(min_length=64, max_length=64)
    row_identity_set_hash: str = Field(min_length=64, max_length=64)

    @field_validator("content_sha256", "row_identity_set_hash")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return str(validated_hash(value, field_name=info.field_name))

    @model_validator(mode="after")
    def _path_date(self) -> "FeaturePartitionDescriptorV1":
        expected = f"feature_rows/date={self.decision_date.isoformat()}/part-00000.parquet"
        if self.relative_path != expected:
            raise ValueError("feature partition path differs from decision_date")
        return self


class RerankerFeatureSnapshotV1(FrozenModel):
    schema_version: Literal[FEATURE_SNAPSHOT_SCHEMA_VERSION] = FEATURE_SNAPSHOT_SCHEMA_VERSION
    base_snapshot_id: str = Field(min_length=1, max_length=160)
    base_snapshot_content_hash: str = Field(min_length=64, max_length=64)
    request_semantic_hash: str = Field(min_length=64, max_length=64)
    feature_schema_hash: str = Field(min_length=64, max_length=64)
    formula_registry_hash: str = Field(min_length=64, max_length=64)
    query_registry_hash: str = Field(min_length=64, max_length=64)
    feature_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    builder_code_closure_hash: str = Field(min_length=64, max_length=64)
    partitions: tuple[FeaturePartitionDescriptorV1, ...]
    files: tuple[BundleFileDescriptorV1, ...]
    feature_snapshot_hash: str | None = Field(default=None, min_length=64, max_length=64)
    feature_snapshot_id: str | None = Field(default=None, min_length=20, max_length=80)

    @field_validator(
        "base_snapshot_content_hash",
        "request_semantic_hash",
        "feature_schema_hash",
        "formula_registry_hash",
        "query_registry_hash",
        "feature_source_revision_set_hash",
        "builder_code_closure_hash",
        "feature_snapshot_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "RerankerFeatureSnapshotV1":
        if not self.partitions:
            raise ValueError("feature snapshot requires at least one partition")
        dates = tuple(item.decision_date for item in self.partitions)
        if tuple(sorted(set(dates))) != dates:
            raise ValueError("feature partitions must be unique and ascending")
        file_paths = {item.relative_path for item in self.files}
        if tuple(item.relative_path for item in self.files) != tuple(
            sorted(item.relative_path for item in self.files)
        ):
            raise ValueError("feature snapshot file descriptors must use canonical path order")
        required = {
            "feature_schema.json",
            "feature_formula_registry.json",
            "feature_source_revisions.parquet",
            *(item.relative_path for item in self.partitions),
        }
        if file_paths != required:
            raise ValueError("feature snapshot file descriptors differ from the required file set")
        for partition in self.partitions:
            descriptor = next(item for item in self.files if item.relative_path == partition.relative_path)
            if descriptor.content_sha256 != partition.content_sha256:
                raise ValueError("feature partition descriptor hash differs from file descriptor")
        payload = self.model_dump(mode="python", exclude={"feature_snapshot_hash", "feature_snapshot_id"})
        digest = canonical_json_sha256(payload)
        if self.feature_snapshot_hash is not None and self.feature_snapshot_hash != digest:
            raise ValueError("feature_snapshot_hash differs from canonical manifest")
        expected_id = f"advrfs_{digest[:24]}"
        if self.feature_snapshot_id is not None and self.feature_snapshot_id != expected_id:
            raise ValueError("feature_snapshot_id differs from feature_snapshot_hash")
        object.__setattr__(self, "feature_snapshot_hash", digest)
        object.__setattr__(self, "feature_snapshot_id", expected_id)
        return self


class RerankerFeatureSnapshotStore:
    def __init__(self, *, artifact_root: Path, repository_root: Path) -> None:
        self.store = ImmutableArtifactStore(
            artifact_root=artifact_root,
            repository_root=repository_root,
            namespace="datasets",
        )

    def publish(
        self,
        *,
        manifest: RerankerFeatureSnapshotV1,
        payload_files: Mapping[str, bytes],
    ) -> ArtifactCompletionReceiptV1:
        if build_file_descriptors(payload_files) != manifest.files:
            raise AdvisoryModelingError(
                REASON_FEATURE_SNAPSHOT_INCOMPLETE,
                "feature snapshot payload differs from manifest file descriptors",
            )
        manifest_payload = (
            canonical_json_text(manifest.model_dump(mode="python")) + "\n"
        ).encode("utf-8")
        files = dict(payload_files)
        files["feature_snapshot_manifest.json"] = manifest_payload
        receipt = self.store.publish(
            artifact_id=str(manifest.feature_snapshot_id),
            semantic_hash=str(manifest.feature_snapshot_hash),
            files=files,
        )
        readback = self.read(
            feature_snapshot_id=str(manifest.feature_snapshot_id),
            expected_feature_snapshot_hash=str(manifest.feature_snapshot_hash),
        )
        if readback != manifest:
            raise AdvisoryModelingError(
                REASON_FEATURE_SNAPSHOT_INCOMPLETE,
                "feature snapshot manifest differs on exact readback",
            )
        return receipt

    def read(
        self,
        *,
        feature_snapshot_id: str,
        expected_feature_snapshot_hash: str,
    ) -> RerankerFeatureSnapshotV1:
        self.store.read_exact(
            artifact_id=feature_snapshot_id,
            expected_semantic_hash=expected_feature_snapshot_hash,
        )
        path = self.store.namespace_root / feature_snapshot_id / "feature_snapshot_manifest.json"
        try:
            manifest = RerankerFeatureSnapshotV1.model_validate_json(path.read_bytes())
        except (OSError, ValueError) as exc:
            raise AdvisoryModelingError(
                REASON_FEATURE_SNAPSHOT_INCOMPLETE,
                "feature snapshot manifest is missing or invalid",
            ) from exc
        if (
            manifest.feature_snapshot_id != feature_snapshot_id
            or manifest.feature_snapshot_hash != expected_feature_snapshot_hash
        ):
            raise AdvisoryModelingError(
                REASON_FEATURE_SNAPSHOT_INCOMPLETE,
                "feature snapshot manifest identity differs from requested snapshot",
            )
        return manifest
