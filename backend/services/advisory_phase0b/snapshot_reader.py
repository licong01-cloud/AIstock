from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg2.extras
from pydantic import Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonical_json_text,
    canonicalize,
)
from backend.services.advisory_phase1.dataset_build import (
    DatasetBlobHeader,
    DatasetSnapshotBlobRef,
    DatasetSnapshotFile,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from backend.services.advisory_phase1.snapshot_writer import (
    SCHEMA_DESCRIPTOR_ROLE,
    DatasetManifest,
    SnapshotWriterError,
    read_verified_snapshot_parquet_rows,
)

from .contracts import Phase0BCandidateQualityAuditRequestV1, _FrozenModel
from .errors import (
    Phase0BAuditError,
    REASON_CONFIG_MISSING,
    REASON_FILE_SET_CONFLICT,
    REASON_MANIFEST_CONFLICT,
    REASON_PARQUET_VERIFY_FAILED,
    REASON_RELATION_CLOSURE_INVALID,
    REASON_SNAPSHOT_CHANGED,
    REASON_SNAPSHOT_INVALIDATED,
    REASON_SNAPSHOT_NOT_SEALED,
    REASON_STORE_IDENTITY_CONFLICT,
    REASON_TARGET_SET_CONFLICT,
)
from .spool import Phase0BBoundedSpool


REQUIRED_LOGICAL_ROLES = frozenset(
    {
        "canonical_signals",
        "observation_versions",
        "stage_summaries",
        "stage_candidates",
        "selected_observations",
        "outcome_labels",
        "selected_labels",
        "source_revisions",
    }
)

ROLE_IDENTITY_FIELDS: dict[str, tuple[str, ...]] = {
    "canonical_signals": ("canonical_signal_id",),
    "observation_versions": ("observation_version_id",),
    "selected_observations": ("selected_mapping_id",),
    "lineage": ("lineage_id",),
    "stage_summaries": ("stage_evidence_id",),
    "stage_candidates": ("stage_evidence_id", "symbol"),
    "selected_labels": ("selected_label_mapping_id",),
    "gaps": ("gap_id",),
    "source_revisions": ("source_revision_set_hash", "member_key"),
    "outcome_labels": ("label_version_id",),
    "universe_outcomes": ("label_version_id",),
    "outcome_source_evidence": ("owner_type", "label_version_id"),
}

ROLE_DECISION_DATE_FIELDS: dict[str, str] = {
    "canonical_signals": "decision_as_of_trade_date",
    "gaps": "decision_as_of_trade_date",
    "outcome_labels": "decision_as_of_trade_date",
    "universe_outcomes": "decision_as_of_trade_date",
}
READ_ONLY_STATEMENT_TIMEOUT_MS = 300_000


class Phase0BClientDatabaseTargetV1(_FrozenModel):
    env_file_path_hash: str = Field(min_length=64, max_length=64)
    configured_host_hash: str = Field(min_length=64, max_length=64)
    configured_port: int = Field(ge=1, le=65535)
    configured_database_hash: str = Field(min_length=64, max_length=64)
    configured_user_hash: str = Field(min_length=64, max_length=64)

    @field_validator(
        "env_file_path_hash",
        "configured_host_hash",
        "configured_database_hash",
        "configured_user_hash",
    )
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        from backend.services.advisory_historical_range.models import require_sha256

        return require_sha256(value, field_name=info.field_name)


class Phase0BDatabaseTargetReceiptV1(_FrozenModel):
    client_target: Phase0BClientDatabaseTargetV1
    current_database_hash: str = Field(min_length=64, max_length=64)
    server_address_hash: str | None = Field(default=None, min_length=64, max_length=64)
    server_port: int = Field(ge=1, le=65535)
    server_version_num: int = Field(ge=1)
    current_user_hash: str = Field(min_length=64, max_length=64)
    transaction_read_only: Literal[True] = True
    target_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "current_database_hash",
        "server_address_hash",
        "current_user_hash",
        "target_receipt_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        from backend.services.advisory_historical_range.models import require_sha256

        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BDatabaseTargetReceiptV1":
        if self.current_database_hash != self.client_target.configured_database_hash:
            raise ValueError("database server identity differs from configured database")
        if self.current_user_hash != self.client_target.configured_user_hash:
            raise ValueError("database server identity differs from configured user")
        digest = canonical_json_sha256(
            self.model_dump(mode="python", exclude={"target_receipt_hash"})
        )
        if self.target_receipt_hash is not None and self.target_receipt_hash != digest:
            raise ValueError("database target receipt hash differs from canonical content")
        object.__setattr__(self, "target_receipt_hash", digest)
        return self


class Phase0BSnapshotCatalogEntryV1(_FrozenModel):
    snapshot_id: str = Field(min_length=1, max_length=160)
    lineage_identity_type: Literal["PHASE0A", "HISTORICAL_RANGE"]
    header_payload_json: str = Field(min_length=2)
    files: tuple[DatasetSnapshotFile, ...] = Field(min_length=1)
    observation_membership_json: tuple[str, ...]
    label_membership_json: tuple[str, ...]
    blob_ref_membership_json: tuple[str, ...]
    header_hash: str = Field(min_length=64, max_length=64)
    file_set_hash: str = Field(min_length=64, max_length=64)
    membership_hash: str = Field(min_length=64, max_length=64)
    invalidation_query_hash: str = Field(min_length=64, max_length=64)
    catalog_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "header_hash",
        "file_set_hash",
        "membership_hash",
        "invalidation_query_hash",
        "catalog_content_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        from backend.services.advisory_historical_range.models import require_sha256

        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BSnapshotCatalogEntryV1":
        header = json.loads(self.header_payload_json)
        if not isinstance(header, dict) or canonical_json_text(header) != self.header_payload_json:
            raise ValueError("snapshot header payload must be canonical JSON")
        if str(header.get("snapshot_id") or "") != self.snapshot_id:
            raise ValueError("snapshot header id differs from catalog entry")
        normalized_lineage = header.get("lineage_identity_type")
        if normalized_lineage not in {"PHASE0A", "HISTORICAL_RANGE"}:
            raise ValueError("snapshot header requires an explicit supported lineage type")
        if normalized_lineage != self.lineage_identity_type:
            raise ValueError("snapshot header lineage type differs from catalog entry")
        files = tuple(sorted(self.files, key=lambda item: item.logical_path))
        if len({item.logical_path for item in files}) != len(files):
            raise ValueError("snapshot catalog file paths must be unique")
        for values in (
            self.observation_membership_json,
            self.label_membership_json,
            self.blob_ref_membership_json,
        ):
            if tuple(sorted(values)) != values or any(
                canonical_json_text(json.loads(value)) != value for value in values
            ):
                raise ValueError("snapshot membership payloads must be sorted canonical JSON")
        object.__setattr__(self, "files", files)
        expected_header_hash = canonical_json_sha256(header)
        expected_file_set_hash = canonical_json_sha256(
            tuple(item.model_dump(mode="json") for item in files)
        )
        expected_membership_hash = canonical_json_sha256(
            {
                "observations": self.observation_membership_json,
                "labels": self.label_membership_json,
                "blob_refs": self.blob_ref_membership_json,
            }
        )
        expected_invalidation_hash = canonical_json_sha256(())
        if (
            self.header_hash != expected_header_hash
            or self.file_set_hash != expected_file_set_hash
            or self.membership_hash != expected_membership_hash
            or self.invalidation_query_hash != expected_invalidation_hash
        ):
            raise ValueError("snapshot catalog entry hashes differ from canonical payloads")
        payload = {
            "snapshot_id": self.snapshot_id,
            "lineage_identity_type": self.lineage_identity_type,
            "header_hash": self.header_hash,
            "file_set_hash": self.file_set_hash,
            "membership_hash": self.membership_hash,
            "invalidation_query_hash": self.invalidation_query_hash,
        }
        digest = canonical_json_sha256(payload)
        if self.catalog_content_hash is not None and self.catalog_content_hash != digest:
            raise ValueError("snapshot catalog content hash differs from frozen content")
        object.__setattr__(self, "catalog_content_hash", digest)
        return self

    def header_payload(self) -> dict[str, Any]:
        payload = json.loads(self.header_payload_json)
        if not isinstance(payload, dict):
            raise ValueError("snapshot header payload must be a JSON object")
        return payload


class Phase0BSnapshotCatalogReceiptV1(_FrozenModel):
    entries: tuple[Phase0BSnapshotCatalogEntryV1, ...] = Field(min_length=1)
    database_target: Phase0BDatabaseTargetReceiptV1
    observed_at: datetime
    catalog_content_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BSnapshotCatalogReceiptV1":
        entries = tuple(sorted(self.entries, key=lambda item: item.snapshot_id))
        if len({item.snapshot_id for item in entries}) != len(entries):
            raise ValueError("snapshot catalog receipt ids must be unique")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("catalog observation timestamp must be timezone-aware")
        observed_at = self.observed_at.astimezone(UTC)
        content_hash = canonical_json_sha256(
            tuple((item.snapshot_id, item.catalog_content_hash) for item in entries)
        )
        receipt_hash = canonical_json_sha256(
            {
                "catalog_content_set_hash": content_hash,
                "database_target_receipt_hash": self.database_target.target_receipt_hash,
                "observed_at": observed_at,
            }
        )
        if self.catalog_content_set_hash is not None and self.catalog_content_set_hash != content_hash:
            raise ValueError("catalog content set hash differs from entries")
        if self.receipt_hash is not None and self.receipt_hash != receipt_hash:
            raise ValueError("catalog receipt hash differs from its payload")
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "catalog_content_set_hash", content_hash)
        object.__setattr__(self, "receipt_hash", receipt_hash)
        return self


class Phase0BTargetProgramBindingV1(_FrozenModel):
    target_hash: str = Field(min_length=64, max_length=64)
    formal_program_id: str | None = Field(default=None, min_length=1, max_length=160)
    range_program_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _one_identity(self) -> "Phase0BTargetProgramBindingV1":
        if (self.formal_program_id is None) == (self.range_program_hash is None):
            raise ValueError("target Program binding requires exactly one identity form")
        return self


class Phase0BSnapshotReadResultV1(_FrozenModel):
    first_catalog_receipt: Phase0BSnapshotCatalogReceiptV1
    target_program_bindings: tuple[Phase0BTargetProgramBindingV1, ...]

    @model_validator(mode="after")
    def _ordered(self) -> "Phase0BSnapshotReadResultV1":
        bindings = tuple(sorted(self.target_program_bindings, key=lambda item: item.target_hash))
        if len({item.target_hash for item in bindings}) != len(bindings):
            raise ValueError("target Program bindings must be unique")
        object.__setattr__(self, "target_program_bindings", bindings)
        return self


class PostgresPhase0BSnapshotCatalog:
    """Explicit-id immutable snapshot catalog reader with short read-only transactions."""

    def __init__(
        self,
        *,
        conn_factory: Callable[[], Any],
        client_target: Phase0BClientDatabaseTargetV1,
    ) -> None:
        self._conn_factory = conn_factory
        self._client_target = client_target

    def read_once(self, *, snapshot_ids: tuple[str, ...]) -> Phase0BSnapshotCatalogReceiptV1:
        if not snapshot_ids or tuple(sorted(set(snapshot_ids))) != snapshot_ids:
            raise Phase0BAuditError(
                REASON_TARGET_SET_CONFLICT,
                "snapshot catalog requires sorted explicit unique snapshot ids",
            )
        conn: Any | None = None
        observed_at: datetime | None = None
        database_target: Phase0BDatabaseTargetReceiptV1 | None = None
        try:
            conn = self._conn_factory()
            with conn:
                conn.set_session(
                    isolation_level="REPEATABLE READ",
                    readonly=True,
                    autocommit=False,
                )
                try:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(
                            "SET LOCAL statement_timeout = %s",
                            (READ_ONLY_STATEMENT_TIMEOUT_MS,),
                        )
                        cur.execute(
                            """
                            SELECT transaction_timestamp() AS observed_at,
                                   current_database() AS current_database,
                                   host(inet_server_addr()) AS server_address,
                                   inet_server_port() AS server_port,
                                   current_setting('server_version_num')::integer AS server_version_num,
                                   current_user AS current_user,
                                   current_setting('transaction_read_only') AS transaction_read_only
                            """,
                            (),
                        )
                        identity_row = cur.fetchone()
                        if identity_row is None or str(identity_row["transaction_read_only"]).lower() not in {
                            "on",
                            "true",
                        }:
                            raise Phase0BAuditError(
                                REASON_CONFIG_MISSING,
                                "catalog transaction did not prove read-only database identity",
                            )
                        observed_at = identity_row["observed_at"]
                        database_target = Phase0BDatabaseTargetReceiptV1(
                            client_target=self._client_target,
                            current_database_hash=hashlib.sha256(
                                str(identity_row["current_database"]).encode("utf-8")
                            ).hexdigest(),
                            server_address_hash=hashlib.sha256(
                                str(identity_row["server_address"]).encode("utf-8")
                            ).hexdigest()
                            if identity_row["server_address"] is not None
                            else None,
                            server_port=int(identity_row["server_port"]),
                            server_version_num=int(identity_row["server_version_num"]),
                            current_user_hash=hashlib.sha256(
                                str(identity_row["current_user"]).encode("utf-8")
                            ).hexdigest(),
                            transaction_read_only=True,
                        )
                        headers = self._headers(cur=cur, snapshot_ids=snapshot_ids)
                        files = self._rows_by_snapshot(
                            cur=cur,
                            snapshot_ids=snapshot_ids,
                            query="""
                                SELECT snapshot_id, logical_path, logical_role, partition_key_hash,
                                       ordinal, content_uri, sha256, size_bytes, row_count,
                                       schema_fingerprint, partition_content_hash,
                                       store_backend_hash, blob_sha256
                                FROM app.advisory_dataset_snapshot_file
                                WHERE snapshot_id = ANY(%s)
                                ORDER BY snapshot_id, logical_path
                            """,
                        )
                        observations = self._canonical_rows_by_snapshot(
                            cur=cur,
                            snapshot_ids=snapshot_ids,
                            table="app.advisory_dataset_snapshot_observation",
                        )
                        labels = self._canonical_rows_by_snapshot(
                            cur=cur,
                            snapshot_ids=snapshot_ids,
                            table="app.advisory_dataset_snapshot_label",
                        )
                        blob_refs = self._canonical_rows_by_snapshot(
                            cur=cur,
                            snapshot_ids=snapshot_ids,
                            table="app.advisory_dataset_snapshot_blob_ref",
                        )
                        invalidations = self._canonical_rows_by_snapshot(
                            cur=cur,
                            snapshot_ids=snapshot_ids,
                            table="app.advisory_dataset_snapshot_invalidation",
                        )
                    conn.rollback()
                except Exception:
                    conn.rollback()
                    raise
        except Phase0BAuditError:
            raise
        except Exception as error:
            raise Phase0BAuditError(
                REASON_CONFIG_MISSING,
                "configured PostgreSQL snapshot catalog could not be read",
                context={"error_type": type(error).__name__},
            ) from error
        finally:
            if conn is not None:
                conn.close()
        entries: list[Phase0BSnapshotCatalogEntryV1] = []
        if observed_at is None or database_target is None:
            raise Phase0BAuditError(
                REASON_CONFIG_MISSING,
                "catalog transaction did not return a database target receipt",
            )
        for snapshot_id in snapshot_ids:
            header = headers.get(snapshot_id)
            if header is None or header.get("snapshot_state") != "SEALED":
                raise Phase0BAuditError(
                    REASON_SNAPSHOT_NOT_SEALED,
                    "requested snapshot does not exist in SEALED state",
                    context={"snapshot_id": snapshot_id},
                )
            if invalidations.get(snapshot_id):
                raise Phase0BAuditError(
                    REASON_SNAPSHOT_INVALIDATED,
                    "requested snapshot has an append-only invalidation",
                    context={"snapshot_id": snapshot_id},
                )
            file_models = tuple(self._snapshot_file(item) for item in files.get(snapshot_id, ()))
            if not file_models:
                raise Phase0BAuditError(
                    REASON_FILE_SET_CONFLICT,
                    "SEALED snapshot has no file descriptors",
                    context={"snapshot_id": snapshot_id},
                )
            header_json = canonical_json_text(header)
            observation_rows = tuple(observations.get(snapshot_id, ()))
            label_rows = tuple(labels.get(snapshot_id, ()))
            blob_rows = tuple(blob_refs.get(snapshot_id, ()))
            entries.append(
                Phase0BSnapshotCatalogEntryV1(
                    snapshot_id=snapshot_id,
                    lineage_identity_type=header.get("lineage_identity_type"),
                    header_payload_json=header_json,
                    files=file_models,
                    observation_membership_json=observation_rows,
                    label_membership_json=label_rows,
                    blob_ref_membership_json=blob_rows,
                    header_hash=canonical_json_sha256(header),
                    file_set_hash=canonical_json_sha256(
                        tuple(item.model_dump(mode="json") for item in file_models)
                    ),
                    membership_hash=canonical_json_sha256(
                        {
                            "observations": observation_rows,
                            "labels": label_rows,
                            "blob_refs": blob_rows,
                        }
                    ),
                    invalidation_query_hash=canonical_json_sha256(()),
                )
            )
        return Phase0BSnapshotCatalogReceiptV1(
            entries=tuple(entries),
            database_target=database_target,
            observed_at=observed_at,
        )

    @staticmethod
    def _headers(*, cur: Any, snapshot_ids: tuple[str, ...]) -> dict[str, dict[str, Any]]:
        cur.execute(
            """
            SELECT snapshot.snapshot_id,
                   to_jsonb(snapshot) AS snapshot_header,
                   build.build_request_payload_jsonb
            FROM app.advisory_dataset_snapshot snapshot
            LEFT JOIN app.advisory_dataset_build build ON build.build_id = snapshot.build_id
            WHERE snapshot.snapshot_id = ANY(%s)
            ORDER BY snapshot.snapshot_id
            """,
            (list(snapshot_ids),),
        )
        result: dict[str, dict[str, Any]] = {}
        for row in cur.fetchall():
            header = dict(row["snapshot_header"])
            build_request = row.get("build_request_payload_jsonb")
            if not isinstance(build_request, Mapping):
                raise Phase0BAuditError(
                    REASON_MANIFEST_CONFLICT,
                    "snapshot build request payload is unavailable",
                    context={"snapshot_id": str(row["snapshot_id"])},
                )
            header["build_request_payload"] = dict(build_request)
            result[str(row["snapshot_id"])] = canonicalize(header)
        return result

    @staticmethod
    def _rows_by_snapshot(
        *,
        cur: Any,
        snapshot_ids: tuple[str, ...],
        query: str,
    ) -> dict[str, tuple[dict[str, Any], ...]]:
        cur.execute(query, (list(snapshot_ids),))
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in cur.fetchall():
            grouped[str(row["snapshot_id"])].append(dict(row))
        return {key: tuple(value) for key, value in grouped.items()}

    @staticmethod
    def _canonical_rows_by_snapshot(
        *,
        cur: Any,
        snapshot_ids: tuple[str, ...],
        table: str,
    ) -> dict[str, tuple[str, ...]]:
        allowed_tables = {
            "app.advisory_dataset_snapshot_observation",
            "app.advisory_dataset_snapshot_label",
            "app.advisory_dataset_snapshot_blob_ref",
            "app.advisory_dataset_snapshot_invalidation",
        }
        if table not in allowed_tables:
            raise ValueError("snapshot membership table is not allowlisted")
        cur.execute(
            f"SELECT snapshot_id, to_jsonb(member) AS payload FROM {table} member "
            "WHERE snapshot_id = ANY(%s) ORDER BY snapshot_id, payload::text",
            (list(snapshot_ids),),
        )
        grouped: dict[str, list[str]] = defaultdict(list)
        for row in cur.fetchall():
            grouped[str(row["snapshot_id"])].append(canonical_json_text(row["payload"]))
        return {key: tuple(value) for key, value in grouped.items()}

    @staticmethod
    def _snapshot_file(row: Mapping[str, Any]) -> DatasetSnapshotFile:
        return DatasetSnapshotFile(
            logical_path=str(row["logical_path"]),
            logical_role=str(row["logical_role"]),
            partition_key_hash=str(row["partition_key_hash"]),
            ordinal=int(row["ordinal"]),
            content_uri=str(row["content_uri"]),
            sha256=str(row["sha256"]),
            size_bytes=int(row["size_bytes"]),
            row_count=int(row["row_count"]),
            schema_fingerprint=str(row["schema_fingerprint"]),
            partition_content_hash=str(row["partition_content_hash"]),
            blob=DatasetBlobHeader(
                store_backend_hash=str(row["store_backend_hash"]),
                blob_sha256=str(row["blob_sha256"]),
                size_bytes=int(row["size_bytes"]),
            ),
        )


class Phase0BSnapshotReader:
    def __init__(
        self,
        *,
        catalog: PostgresPhase0BSnapshotCatalog,
        dataset_store: LocalContentAddressedStore,
    ) -> None:
        self._catalog = catalog
        self._dataset_store = dataset_store

    def read_into_spool(
        self,
        *,
        request: Phase0BCandidateQualityAuditRequestV1,
        spool: Phase0BBoundedSpool,
    ) -> Phase0BSnapshotReadResultV1:
        first_receipt = self._catalog.read_once(snapshot_ids=request.snapshot_ids)
        bindings: list[Phase0BTargetProgramBindingV1] = []
        for entry in first_receipt.entries:
            self._verify_manifest_and_files(entry=entry, spool=spool)
            spool.close_relations(snapshot_id=entry.snapshot_id)
            self._verify_source_revision_closure(entry=entry, spool=spool)
            bindings.extend(
                self._verify_target_lineage(request=request, entry=entry, spool=spool)
            )
        return Phase0BSnapshotReadResultV1(
            first_catalog_receipt=first_receipt,
            target_program_bindings=tuple(bindings),
        )

    def confirm_unchanged(
        self,
        *,
        request: Phase0BCandidateQualityAuditRequestV1,
        first_receipt: Phase0BSnapshotCatalogReceiptV1,
    ) -> Phase0BSnapshotCatalogReceiptV1:
        try:
            final_receipt = self._catalog.read_once(snapshot_ids=request.snapshot_ids)
        except Phase0BAuditError as error:
            if error.reason_code not in {
                REASON_SNAPSHOT_INVALIDATED,
                REASON_SNAPSHOT_NOT_SEALED,
            }:
                raise
            raise Phase0BAuditError(
                REASON_SNAPSHOT_CHANGED,
                "snapshot disappeared, left SEALED state, or was invalidated during CAS read",
                context={"upstream_reason": error.reason_code},
            ) from error
        if (
            final_receipt.catalog_content_set_hash != first_receipt.catalog_content_set_hash
            or final_receipt.database_target.target_receipt_hash
            != first_receipt.database_target.target_receipt_hash
        ):
            raise Phase0BAuditError(
                REASON_SNAPSHOT_CHANGED,
                "snapshot catalog changed while Phase 0B read CAS evidence",
                context={
                    "first_catalog_hash": first_receipt.catalog_content_set_hash,
                    "final_catalog_hash": final_receipt.catalog_content_set_hash,
                },
            )
        return final_receipt

    def _verify_manifest_and_files(
        self,
        *,
        entry: Phase0BSnapshotCatalogEntryV1,
        spool: Phase0BBoundedSpool,
    ) -> None:
        header = entry.header_payload()
        manifest_sha256 = str(header.get("manifest_sha256") or "")
        snapshot_content_hash = str(header.get("snapshot_content_hash") or "")
        try:
            manifest_payload = self._dataset_store.read_document_bytes(
                kind="manifests",
                sha256=manifest_sha256,
            )
            manifest = DatasetManifest.model_validate(json.loads(manifest_payload))
        except Exception as error:
            raise Phase0BAuditError(
                REASON_MANIFEST_CONFLICT,
                "snapshot manifest could not be read and validated",
                context={"snapshot_id": entry.snapshot_id, "error_type": type(error).__name__},
            ) from error
        if (
            manifest.manifest_sha256 != manifest_sha256
            or manifest.core.manifest_core_sha256 != snapshot_content_hash
            or manifest.store_backend_hash != self._dataset_store.store_backend_hash
        ):
            raise Phase0BAuditError(
                REASON_MANIFEST_CONFLICT,
                "snapshot manifest identity differs from catalog header",
                context={"snapshot_id": entry.snapshot_id},
            )
        header_to_manifest = {
            "manifest_core_sha256": manifest.core.manifest_core_sha256,
            "snapshot_source_revision_set_hash": manifest.core.snapshot_source_revision_set_hash,
            "capture_set_hash": manifest.core.capture_set_hash,
            "handoff_readiness_hash": manifest.core.handoff_readiness_hash,
            "admission_scope_set_hash": manifest.core.admission_scope_set_hash,
            "query_registry_hash": manifest.core.query_registry_hash,
            "builder_version": manifest.core.builder_version,
            "code_commit": manifest.core.code_commit,
            "writer_version": manifest.core.writer_version,
            "partition_policy_hash": manifest.core.partition_policy_hash,
            "policy_compatibility_hash": manifest.core.policy_compatibility_hash,
            "schema_fingerprint": manifest.core.schema_fingerprint,
        }
        if entry.lineage_identity_type == "HISTORICAL_RANGE":
            header_to_manifest.update(
                {
                    "lineage_identity_type": manifest.core.lineage_identity_type,
                    "execution_origin": manifest.core.execution_origin,
                    "research_scope": manifest.core.research_scope,
                    "evidence_scope": manifest.core.evidence_scope,
                    "range_lineage_scope_set_hash": manifest.core.range_lineage_scope_set_hash,
                    "selector_policy_hash": manifest.core.selector_policy_hash,
                    "selected_range_day_outcome_set_hash": manifest.core.selected_range_day_outcome_set_hash,
                    "policy_lineage_type": manifest.core.policy_lineage_type,
                    "historical_range_policy_bundle_hash": manifest.core.historical_range_policy_bundle_hash,
                    "policy_component_set_hash": manifest.core.policy_component_set_hash,
                    "selected_observation_mapping_set_hash": manifest.core.selected_observation_mapping_set_hash,
                    "selected_label_mapping_set_hash": manifest.core.selected_label_mapping_set_hash,
                    "source_revision_closure_hash": manifest.core.source_revision_closure_hash,
                    "maturity_coverage_hash": manifest.core.maturity_coverage_hash,
                }
            )
        if any(header.get(key) != value for key, value in header_to_manifest.items()):
            raise Phase0BAuditError(
                REASON_MANIFEST_CONFLICT,
                "snapshot catalog header differs from manifest core",
                context={"snapshot_id": entry.snapshot_id},
            )
        catalog_files = tuple(item.model_dump(mode="json") for item in entry.files)
        manifest_files = tuple(
            item.model_dump(mode="json")
            for item in sorted(manifest.core.files, key=lambda value: value.logical_path)
        )
        if canonicalize(catalog_files) != canonicalize(manifest_files):
            raise Phase0BAuditError(
                REASON_FILE_SET_CONFLICT,
                "snapshot manifest file set differs from catalog descriptors",
                context={"snapshot_id": entry.snapshot_id},
            )
        if (
            int(header.get("file_count", -1)) != len(entry.files)
            or int(header.get("row_count", -1)) != sum(item.row_count for item in entry.files)
            or int(header.get("total_bytes", -1)) != sum(item.size_bytes for item in entry.files)
        ):
            raise Phase0BAuditError(
                REASON_FILE_SET_CONFLICT,
                "snapshot catalog aggregate counts differ from file descriptors",
                context={"snapshot_id": entry.snapshot_id},
            )
        catalog_observations = self._membership_payloads(entry.observation_membership_json)
        catalog_labels = self._membership_payloads(entry.label_membership_json)
        manifest_observations = tuple(
            canonical_json_text(item.model_dump(mode="json"))
            for item in sorted(
                manifest.core.selected_observations,
                key=lambda value: value.canonical_signal_id,
            )
        )
        manifest_labels = tuple(
            canonical_json_text(item.model_dump(mode="json"))
            for item in sorted(
                manifest.core.selected_labels,
                key=lambda value: value.label_key_hash,
            )
        )
        if catalog_observations != manifest_observations or catalog_labels != manifest_labels:
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "snapshot catalog memberships differ from the sealed manifest",
                context={"snapshot_id": entry.snapshot_id},
            )
        catalog_blob_refs = self._membership_payloads(entry.blob_ref_membership_json)
        expected_blob_refs: list[str] = []
        for file in entry.files:
            ref = DatasetSnapshotBlobRef(
                logical_path=file.logical_path,
                logical_role=file.logical_role,
                partition_key_hash=file.partition_key_hash,
                ordinal=file.ordinal,
                blob=file.blob,
            )
            expected_blob_refs.append(
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
            )
        if catalog_blob_refs != tuple(sorted(expected_blob_refs)):
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "snapshot blob-ref membership differs from file descriptors",
                context={"snapshot_id": entry.snapshot_id},
            )
        roles = {item.logical_role for item in entry.files}
        missing_roles = tuple(sorted(REQUIRED_LOGICAL_ROLES - roles))
        if missing_roles:
            raise Phase0BAuditError(
                REASON_FILE_SET_CONFLICT,
                "snapshot lacks required Phase 0B logical roles",
                context={"snapshot_id": entry.snapshot_id, "missing_roles": missing_roles},
            )
        for file in entry.files:
            if file.blob.store_backend_hash != self._dataset_store.store_backend_hash:
                raise Phase0BAuditError(
                    REASON_STORE_IDENTITY_CONFLICT,
                    "snapshot file store backend differs from configured dataset store",
                    context={"snapshot_id": entry.snapshot_id, "logical_path": file.logical_path},
                )
            if file.logical_role == SCHEMA_DESCRIPTOR_ROLE:
                try:
                    self._dataset_store.read_blob_bytes(
                        uri=file.content_uri,
                        sha256=file.sha256,
                        size_bytes=file.size_bytes,
                    )
                except Exception as error:
                    raise Phase0BAuditError(
                        REASON_PARQUET_VERIFY_FAILED,
                        "snapshot schema descriptor failed CAS verification",
                        context={"snapshot_id": entry.snapshot_id, "logical_path": file.logical_path},
                    ) from error
                continue
            try:
                rows = read_verified_snapshot_parquet_rows(
                    file=file,
                    store=self._dataset_store,
                    lineage_identity_type=entry.lineage_identity_type,
                )
            except SnapshotWriterError as error:
                raise Phase0BAuditError(
                    REASON_PARQUET_VERIFY_FAILED,
                    "snapshot Parquet file failed full verification",
                    context={
                        "snapshot_id": entry.snapshot_id,
                        "logical_path": file.logical_path,
                        "upstream_reason": error.reason_code,
                    },
                ) from error
            identity_fields = ROLE_IDENTITY_FIELDS.get(file.logical_role)
            if identity_fields is not None:
                spool.append_rows(
                    snapshot_id=entry.snapshot_id,
                    logical_role=file.logical_role,
                    source_file_sha256=file.sha256,
                    rows=rows,
                    identity_fields=identity_fields,
                    decision_date_field=ROLE_DECISION_DATE_FIELDS.get(file.logical_role),
                )

    @staticmethod
    def _membership_payloads(values: tuple[str, ...]) -> tuple[str, ...]:
        payloads: list[str] = []
        for value in values:
            payload = json.loads(value)
            if not isinstance(payload, dict):
                raise Phase0BAuditError(
                    REASON_RELATION_CLOSURE_INVALID,
                    "snapshot catalog membership payload is not an object",
                )
            payload.pop("snapshot_id", None)
            payloads.append(canonical_json_text(payload))
        return tuple(sorted(payloads))

    @staticmethod
    def _verify_target_lineage(
        *,
        request: Phase0BCandidateQualityAuditRequestV1,
        entry: Phase0BSnapshotCatalogEntryV1,
        spool: Phase0BBoundedSpool,
    ) -> tuple[Phase0BTargetProgramBindingV1, ...]:
        header = entry.header_payload()
        build_request = header.get("build_request_payload")
        if not isinstance(build_request, dict):
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "snapshot build request is unavailable for target closure",
                context={"snapshot_id": entry.snapshot_id},
            )
        range_scopes = build_request.get("range_lineage_scopes")
        scope_by_hash: dict[str, str] = {}
        if entry.lineage_identity_type == "HISTORICAL_RANGE":
            if not isinstance(range_scopes, list):
                raise Phase0BAuditError(
                    REASON_RELATION_CLOSURE_INVALID,
                    "retrospective snapshot lacks frozen Program scopes",
                    context={"snapshot_id": entry.snapshot_id},
                )
            scope_by_hash = {
                str(item["identity_hash"]): str(item["identity_id"])
                for item in range_scopes
                if isinstance(item, dict)
                and item.get("identity_hash") is not None
                and item.get("identity_id") is not None
            }
            if (
                len(scope_by_hash) != len(range_scopes)
                or len(set(scope_by_hash.values())) != len(scope_by_hash)
            ):
                raise Phase0BAuditError(
                    REASON_RELATION_CLOSURE_INVALID,
                    "retrospective Program scope identities are incomplete or duplicated",
                    context={"snapshot_id": entry.snapshot_id},
                )
        actual: set[tuple[str, str, str, str, str]] = set()
        for package_id, manifest_sha256, alpha_mode, formal_program_id, range_program_hash in spool.distinct_target_lineages(
            snapshot_id=entry.snapshot_id
        ):
            if entry.lineage_identity_type == "HISTORICAL_RANGE":
                program_id = scope_by_hash.get(str(range_program_hash or ""))
            else:
                program_id = formal_program_id
            if program_id is None:
                raise Phase0BAuditError(
                    REASON_RELATION_CLOSURE_INVALID,
                    "snapshot signal cannot close to one frozen Program",
                    context={"snapshot_id": entry.snapshot_id},
                )
            actual.add(
                (entry.snapshot_id, program_id, package_id, manifest_sha256, alpha_mode)
            )
        expected = {
            (
                item.snapshot_id,
                item.program_id,
                item.package_id,
                item.manifest_sha256,
                item.alpha_mode,
            )
            for item in request.audit_targets
            if item.snapshot_id == entry.snapshot_id
        }
        if actual != expected:
            raise Phase0BAuditError(
                REASON_TARGET_SET_CONFLICT,
                "audit targets do not exactly match snapshot Program/package lineage",
                context={
                    "snapshot_id": entry.snapshot_id,
                    "expected_target_count": len(expected),
                    "actual_target_count": len(actual),
                },
            )
        targets_by_lineage = {
            (
                item.snapshot_id,
                item.program_id,
                item.package_id,
                item.manifest_sha256,
                item.alpha_mode,
            ): item
            for item in request.audit_targets
            if item.snapshot_id == entry.snapshot_id
        }
        scope_hash_by_program = {value: key for key, value in scope_by_hash.items()}
        return tuple(
            Phase0BTargetProgramBindingV1(
                target_hash=str(targets_by_lineage[lineage].target_hash),
                formal_program_id=lineage[1]
                if entry.lineage_identity_type != "HISTORICAL_RANGE"
                else None,
                range_program_hash=scope_hash_by_program.get(lineage[1])
                if entry.lineage_identity_type == "HISTORICAL_RANGE"
                else None,
            )
            for lineage in sorted(actual)
        )

    @staticmethod
    def _verify_source_revision_closure(
        *,
        entry: Phase0BSnapshotCatalogEntryV1,
        spool: Phase0BBoundedSpool,
    ) -> None:
        header = entry.header_payload()
        rows = tuple(
            spool.iter_rows(
                snapshot_id=entry.snapshot_id,
                logical_role="source_revisions",
            )
        )
        expected_set_hash = str(header.get("snapshot_source_revision_set_hash") or "")
        expected_query_registry_hash = str(header.get("query_registry_hash") or "")
        if (
            not rows
            or any(int(row.get("member_count", -1)) != len(rows) for row in rows)
            or {str(row.get("source_revision_set_hash") or "") for row in rows}
            != {expected_set_hash}
            or {str(row.get("query_registry_hash") or "") for row in rows}
            != {expected_query_registry_hash}
            or {bool(row.get("research_only")) for row in rows} != {True}
        ):
            raise Phase0BAuditError(
                REASON_RELATION_CLOSURE_INVALID,
                "snapshot source revision union does not close to its catalog header",
                context={"snapshot_id": entry.snapshot_id, "member_count": len(rows)},
            )
