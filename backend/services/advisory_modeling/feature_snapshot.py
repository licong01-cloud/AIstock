from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

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
    validated_hash,
)
from backend.services.advisory_modeling.feature_builder import BuiltFeatureRowV1
from backend.services.advisory_modeling.feature_schema import (
    FeatureFormulaRegistryV1,
    FeatureSchemaV1,
    FrozenFeatureQueryRegistryV1,
)
from backend.services.advisory_modeling.training_view import DatasetBuildRequestV1


FEATURE_SNAPSHOT_SCHEMA_VERSION = "advisory_reranker_feature_snapshot_v1"
FEATURE_SOURCE_REVISION_SCHEMA_VERSION = "advisory_reranker_feature_source_revision_v1"
FEATURE_PARQUET_WRITER_VERSION = "advisory_reranker_feature_parquet_writer_v1"


class FeatureSourceRevisionV1(FrozenModel):
    schema_version: Literal[FEATURE_SOURCE_REVISION_SCHEMA_VERSION] = (
        FEATURE_SOURCE_REVISION_SCHEMA_VERSION
    )
    query_template_id: str = Field(min_length=1, max_length=160)
    query_template_hash: str = Field(min_length=64, max_length=64)
    bound_parameter_hash: str = Field(min_length=64, max_length=64)
    partition_key: str = Field(min_length=1, max_length=160)
    partition_hash: str = Field(min_length=64, max_length=64)
    business_min_date: date
    business_max_date: date
    result_schema_hash: str = Field(min_length=64, max_length=64)
    cutoff_predicate_hash: str = Field(min_length=64, max_length=64)
    database_target_hash: str = Field(min_length=64, max_length=64)
    row_count: int = Field(ge=0)
    admissibility: Literal["RETROSPECTIVE_DB_CONTENT_HASH"] = "RETROSPECTIVE_DB_CONTENT_HASH"
    source_revision_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "query_template_hash",
        "bound_parameter_hash",
        "partition_hash",
        "result_schema_hash",
        "cutoff_predicate_hash",
        "database_target_hash",
        "source_revision_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "FeatureSourceRevisionV1":
        if self.business_min_date > self.business_max_date:
            raise ValueError("feature source business date range is reversed")
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


def _pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise AdvisoryModelingError(
            REASON_FEATURE_SNAPSHOT_INCOMPLETE,
            "pyarrow==21.0.0 is required for reranker feature snapshots",
        ) from exc
    if pa.__version__ != "21.0.0":
        raise AdvisoryModelingError(
            REASON_FEATURE_SNAPSHOT_INCOMPLETE,
            f"reranker feature snapshots require pyarrow 21.0.0, found {pa.__version__}",
        )
    return pa, pq


def _feature_arrow_schema(feature_schema: FeatureSchemaV1) -> Any:
    pa, _pq = _pyarrow()
    fields = [
        pa.field("base_snapshot_id", pa.string(), nullable=False),
        pa.field("canonical_signal_id", pa.string(), nullable=False),
        pa.field("stable_signal_semantics_hash", pa.string(), nullable=False),
        pa.field("canonical_signal_scope_hash", pa.string(), nullable=False),
        pa.field("observation_version_id", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("decision_trade_date", pa.date32(), nullable=False),
        pa.field("target_trade_date", pa.date32(), nullable=False),
        pa.field("decision_cutoff_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("row_identity_hash", pa.string(), nullable=False),
        pa.field("feature_payload_hash", pa.string(), nullable=False),
        pa.field("base_candidate_hash", pa.string(), nullable=False),
        pa.field("stage_evidence_set_hash", pa.string(), nullable=False),
        pa.field("formula_registry_hash", pa.string(), nullable=False),
        pa.field("query_registry_hash", pa.string(), nullable=False),
        pa.field("feature_source_revision_set_hash", pa.string(), nullable=False),
        pa.field("builder_code_closure_hash", pa.string(), nullable=False),
    ]
    types = {
        "float64": pa.float64(),
        "int64": pa.int64(),
        "bool": pa.bool_(),
        "string": pa.string(),
        "sha256": pa.string(),
    }
    fields.extend(
        pa.field(
            definition.name,
            types[definition.dtype],
            nullable=definition.missing_policy != "REQUIRED_FAIL_GROUP",
        )
        for definition in feature_schema.definitions
    )
    return pa.schema(fields)


def _source_arrow_schema() -> Any:
    pa, _pq = _pyarrow()
    return pa.schema(
        [
            pa.field("schema_version", pa.string(), nullable=False),
            pa.field("query_template_id", pa.string(), nullable=False),
            pa.field("query_template_hash", pa.string(), nullable=False),
            pa.field("bound_parameter_hash", pa.string(), nullable=False),
            pa.field("partition_key", pa.string(), nullable=False),
            pa.field("partition_hash", pa.string(), nullable=False),
            pa.field("business_min_date", pa.date32(), nullable=False),
            pa.field("business_max_date", pa.date32(), nullable=False),
            pa.field("result_schema_hash", pa.string(), nullable=False),
            pa.field("cutoff_predicate_hash", pa.string(), nullable=False),
            pa.field("database_target_hash", pa.string(), nullable=False),
            pa.field("row_count", pa.int64(), nullable=False),
            pa.field("admissibility", pa.string(), nullable=False),
            pa.field("source_revision_hash", pa.string(), nullable=False),
        ]
    )


def _parquet_bytes(*, schema: Any, rows: Sequence[dict[str, Any]], logical_role: str) -> bytes:
    pa, pq = _pyarrow()
    metadata = {
        b"aistock_feature_snapshot_schema_version": FEATURE_SNAPSHOT_SCHEMA_VERSION.encode("ascii"),
        b"aistock_logical_role": logical_role.encode("ascii"),
        b"aistock_writer_version": FEATURE_PARQUET_WRITER_VERSION.encode("ascii"),
        b"aistock_arrow_schema_hash": canonical_json_sha256(str(schema)).encode("ascii"),
    }
    table = pa.Table.from_pylist(list(rows), schema=schema.with_metadata(metadata))
    output = pa.BufferOutputStream()
    pq.write_table(
        table,
        output,
        compression="zstd",
        compression_level=9,
        use_dictionary=False,
        write_statistics=True,
        data_page_version="1.0",
        version="2.6",
    )
    return output.getvalue().to_pybytes()


def _feature_flat_row(row: BuiltFeatureRowV1) -> dict[str, Any]:
    identity = row.identity
    return {
        "base_snapshot_id": identity.base_snapshot_id,
        "canonical_signal_id": identity.canonical_signal_id,
        "stable_signal_semantics_hash": row.stable_signal_semantics_hash,
        "canonical_signal_scope_hash": row.canonical_signal_scope_hash,
        "observation_version_id": identity.observation_version_id,
        "symbol": identity.symbol,
        "decision_trade_date": row.decision_trade_date,
        "target_trade_date": row.target_trade_date,
        "decision_cutoff_ts": identity.decision_cutoff_ts,
        "row_identity_hash": identity.row_identity_hash,
        "feature_payload_hash": identity.feature_payload_hash,
        "base_candidate_hash": identity.base_candidate_hash,
        "stage_evidence_set_hash": identity.stage_evidence_set_hash,
        "formula_registry_hash": identity.formula_registry_hash,
        "query_registry_hash": identity.query_registry_hash,
        "feature_source_revision_set_hash": identity.feature_source_revision_set_hash,
        "builder_code_closure_hash": identity.builder_code_closure_hash,
        **row.features,
    }


def materialize_feature_snapshot(
    *,
    request: DatasetBuildRequestV1,
    base_snapshot_id: str,
    base_snapshot_content_hash: str,
    feature_schema: FeatureSchemaV1,
    formula_registry: FeatureFormulaRegistryV1,
    query_registry: FrozenFeatureQueryRegistryV1,
    source_revisions: tuple[FeatureSourceRevisionV1, ...],
    builder_code_closure_hash: str,
    rows: tuple[BuiltFeatureRowV1, ...],
) -> tuple[RerankerFeatureSnapshotV1, dict[str, bytes]]:
    if not rows:
        raise AdvisoryModelingError(
            REASON_FEATURE_SNAPSHOT_INCOMPLETE,
            "feature snapshot requires non-empty built rows",
        )
    if (
        request.feature_schema_hash != feature_schema.feature_schema_hash
        or request.feature_formula_registry_hash != formula_registry.registry_hash
        or request.feature_query_registry_hash != query_registry.registry_hash
    ):
        raise AdvisoryModelingError(
            REASON_FEATURE_SNAPSHOT_INCOMPLETE,
            "dataset request differs from frozen feature registries",
        )
    if not source_revisions:
        raise AdvisoryModelingError(
            REASON_FEATURE_SNAPSHOT_INCOMPLETE,
            "feature snapshot requires non-empty source revisions",
        )
    source_hash = canonical_json_sha256(
        tuple(str(item.source_revision_hash) for item in source_revisions)
    )
    if any(row.identity.feature_source_revision_set_hash != source_hash for row in rows):
        raise AdvisoryModelingError(
            REASON_FEATURE_SNAPSHOT_INCOMPLETE,
            "feature row source authority differs from revision set",
        )
    by_date: dict[date, list[BuiltFeatureRowV1]] = {}
    for row in rows:
        by_date.setdefault(row.decision_trade_date, []).append(row)
    payload_files: dict[str, bytes] = {
        "feature_schema.json": (
            canonical_json_text(feature_schema.model_dump(mode="python")) + "\n"
        ).encode("utf-8"),
        "feature_formula_registry.json": (
            canonical_json_text(formula_registry.model_dump(mode="python")) + "\n"
        ).encode("utf-8"),
        "feature_source_revisions.parquet": _parquet_bytes(
            schema=_source_arrow_schema(),
            rows=[item.model_dump(mode="python") for item in source_revisions],
            logical_role="feature_source_revisions",
        ),
    }
    partitions: list[FeaturePartitionDescriptorV1] = []
    feature_arrow_schema = _feature_arrow_schema(feature_schema)
    for decision_date in sorted(by_date):
        partition_rows = tuple(sorted(by_date[decision_date], key=lambda item: item.identity.symbol))
        if len({item.identity.symbol for item in partition_rows}) != len(partition_rows):
            raise AdvisoryModelingError(
                REASON_FEATURE_SNAPSHOT_INCOMPLETE,
                "feature partition contains duplicate symbols",
                context={"decision_date": decision_date.isoformat()},
            )
        relative_path = f"feature_rows/date={decision_date.isoformat()}/part-00000.parquet"
        payload = _parquet_bytes(
            schema=feature_arrow_schema,
            rows=[_feature_flat_row(item) for item in partition_rows],
            logical_role="feature_rows",
        )
        payload_files[relative_path] = payload
        partitions.append(
            FeaturePartitionDescriptorV1(
                decision_date=decision_date,
                relative_path=relative_path,
                row_count=len(partition_rows),
                content_sha256=hashlib.sha256(payload).hexdigest(),
                row_identity_set_hash=canonical_json_sha256(
                    tuple(str(item.identity.row_identity_hash) for item in partition_rows)
                ),
            )
        )
    manifest = RerankerFeatureSnapshotV1(
        base_snapshot_id=base_snapshot_id,
        base_snapshot_content_hash=base_snapshot_content_hash,
        request_semantic_hash=str(request.request_semantic_hash),
        feature_schema_hash=str(feature_schema.feature_schema_hash),
        formula_registry_hash=str(formula_registry.registry_hash),
        query_registry_hash=str(query_registry.registry_hash),
        feature_source_revision_set_hash=source_hash,
        builder_code_closure_hash=builder_code_closure_hash,
        partitions=tuple(partitions),
        files=build_file_descriptors(payload_files),
    )
    verify_feature_snapshot_payload(
        manifest=manifest,
        feature_schema=feature_schema,
        payload_files=payload_files,
    )
    return manifest, payload_files


def verify_feature_snapshot_payload(
    *,
    manifest: RerankerFeatureSnapshotV1,
    feature_schema: FeatureSchemaV1,
    payload_files: Mapping[str, bytes],
) -> None:
    pa, pq = _pyarrow()
    if build_file_descriptors(payload_files) != manifest.files:
        raise AdvisoryModelingError(
            REASON_FEATURE_SNAPSHOT_INCOMPLETE,
            "feature snapshot payload descriptors differ from manifest",
        )
    try:
        source_table = pq.read_table(
            pa.BufferReader(payload_files["feature_source_revisions.parquet"])
        )
    except Exception as exc:
        raise AdvisoryModelingError(
            REASON_FEATURE_SNAPSHOT_INCOMPLETE,
            "feature source revisions cannot be read back",
        ) from exc
    if source_table.schema.remove_metadata() != _source_arrow_schema():
        raise AdvisoryModelingError(
            REASON_FEATURE_SNAPSHOT_INCOMPLETE,
            "feature source revision schema differs on readback",
        )
    source_hashes = tuple(
        str(value) for value in source_table.column("source_revision_hash").to_pylist()
    )
    if (
        not source_hashes
        or canonical_json_sha256(source_hashes)
        != manifest.feature_source_revision_set_hash
    ):
        raise AdvisoryModelingError(
            REASON_FEATURE_SNAPSHOT_INCOMPLETE,
            "feature source revision identities differ on readback",
        )
    expected_schema = _feature_arrow_schema(feature_schema)
    for partition in manifest.partitions:
        payload = payload_files[partition.relative_path]
        try:
            table = pq.read_table(pa.BufferReader(payload))
        except Exception as exc:
            raise AdvisoryModelingError(
                REASON_FEATURE_SNAPSHOT_INCOMPLETE,
                "feature partition cannot be read back",
                context={"relative_path": partition.relative_path},
            ) from exc
        if table.schema.remove_metadata() != expected_schema or table.num_rows != partition.row_count:
            raise AdvisoryModelingError(
                REASON_FEATURE_SNAPSHOT_INCOMPLETE,
                "feature partition schema or row count differs on readback",
                context={"relative_path": partition.relative_path},
            )
        identities = tuple(str(value) for value in table.column("row_identity_hash").to_pylist())
        symbols = tuple(str(value) for value in table.column("symbol").to_pylist())
        if (
            symbols != tuple(sorted(symbols))
            or len(symbols) != len(set(symbols))
            or canonical_json_sha256(identities) != partition.row_identity_set_hash
        ):
            raise AdvisoryModelingError(
                REASON_FEATURE_SNAPSHOT_INCOMPLETE,
                "feature partition identity order differs on readback",
                context={"relative_path": partition.relative_path},
            )
