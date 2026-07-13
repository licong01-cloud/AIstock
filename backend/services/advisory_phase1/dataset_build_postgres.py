"""PostgreSQL persistence for the immutable Batch C build/attempt state machine."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import logging
from typing import Any, Iterator, Mapping

import psycopg2
import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.dataset_build import (
    AttemptOperation,
    AttemptState,
    BuildCheckpoint,
    BuildLifecycle,
    DatasetAttemptFile,
    DatasetBuild,
    DatasetBuildAttempt,
    DatasetBuildError,
    DatasetBuildEvent,
    DatasetBuildEventType,
    DatasetBlobHeader,
    DatasetSnapshotFile,
    DatasetSnapshotInvalidation,
    SealedDatasetSnapshot,
    FixtureDatasetBuildRequest,
    REASON_ATTEMPT_FENCING_STALE,
    REASON_ATTEMPT_FILE_CONFLICT,
    REASON_ATTEMPT_LEASE_EXPIRED,
    REASON_ATTEMPT_OPERATION_INVALID,
    REASON_BUILD_ALREADY_RUNNING,
    REASON_BUILD_GENERATION_INVALID,
    REASON_BUILD_REQUEST_CONFLICT,
    REASON_BUILD_TRANSITION_INVALID,
    REASON_CHECKPOINT_CONFLICT,
    BATCH_C_FILESET_VERIFICATION_CONTRACT,
    BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT,
    build_id_for,
    file_set_hash,
    logical_build_key,
    _attempt_file_identities,
    _snapshot_file_identities,
    _verify_promoted_cas,
)


REASON_DATABASE_INVARIANT_VIOLATION = "ADVISORY_PHASE1C3_DATABASE_INVARIANT_VIOLATION"

logger = logging.getLogger(__name__)


def _same_aware_timestamp(raw: object, expected: datetime) -> bool:
    try:
        observed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return False
    if observed.tzinfo is None or observed.utcoffset() is None:
        return False
    return observed.astimezone(timezone.utc) == expected.astimezone(timezone.utc)


def _build_event_payload_hash(payload: Mapping[str, object], attempt: DatasetBuildAttempt | None) -> str:
    event_payload = dict(payload)
    if attempt is not None:
        event_payload["attempt_id"] = attempt.attempt_id
        event_payload["fencing_token"] = attempt.fencing_token
    return canonical_json_sha256(event_payload)


@contextmanager
def _transactional_conn_factory() -> Iterator[Any]:
    with get_conn(autocommit=False, manage_transaction=True) as conn:
        yield conn


class PostgresDatasetBuildRepository:
    """Short-transaction control plane; it never holds a DB transaction over IO."""

    def __init__(self, conn_factory: Any | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def create_or_get(
        self,
        request: FixtureDatasetBuildRequest,
        *,
        actor: str,
        rebuild_predecessor_build_id: str | None = None,
        expected_termination_receipt_hash: str | None = None,
    ) -> DatasetBuild:
        request = FixtureDatasetBuildRequest.model_validate(request.model_dump(mode="python"))
        key = logical_build_key(request)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
                self._require_capture_admission(cur, request)
                self._require_base_snapshot_admission(cur, request)
                cur.execute(
                    "SELECT * FROM app.advisory_dataset_build WHERE logical_build_key_sha256 = %s ORDER BY build_generation DESC FOR UPDATE",
                    (key,),
                )
                rows = [dict(row) for row in cur.fetchall()]
                if rows:
                    latest = self._build_from_row(rows[0])
                    if latest.request != request:
                        raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "logical key row has different canonical request")
                    if latest.lifecycle in {BuildLifecycle.ACTIVE, BuildLifecycle.SEALED}:
                        if latest.lifecycle is BuildLifecycle.SEALED:
                            cur.execute(
                                "SELECT 1 FROM app.advisory_dataset_snapshot_invalidation WHERE snapshot_id = %s",
                                (latest.sealed_snapshot_id,),
                            )
                            if cur.fetchone() is not None:
                                raise DatasetBuildError(REASON_BUILD_GENERATION_INVALID, "sealed build snapshot is invalidated")
                        return latest
                    if latest.lifecycle is BuildLifecycle.FAILED_TERMINAL:
                        raise DatasetBuildError(REASON_BUILD_GENERATION_INVALID, "terminal logical key requires new semantic request")
                    if (
                        rebuild_predecessor_build_id != latest.build_id
                        or expected_termination_receipt_hash is None
                        or expected_termination_receipt_hash != latest.termination_receipt_hash
                    ):
                        raise DatasetBuildError(REASON_BUILD_GENERATION_INVALID, "aborted generation requires exact predecessor termination receipt")
                    generation = latest.build_generation + 1
                else:
                    generation = 1
                build_id = build_id_for(key, generation)
                try:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_dataset_build (
                            build_id, logical_build_key_sha256, build_generation, predecessor_build_id, build_request_hash,
                            build_request_payload_jsonb, snapshot_source_revision_set_hash, capture_set_hash,
                            handoff_readiness_hash, admission_scope_set_hash, query_registry_hash,
                            date_start, date_end, base_snapshot_id, base_snapshot_content_hash, base_manifest_sha256, base_policy_compatibility_hash,
                            builder_version, code_commit, writer_version, partition_policy_hash, compression_config_hash,
                            lifecycle_status, checkpoint, current_fencing_token, row_version
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            'ACTIVE', 'REQUESTED', 1, 1
                        ) RETURNING *
                        """,
                        (
                            build_id, key, generation, latest.build_id if generation > 1 else None, request.build_request_hash,
                            psycopg2.extras.Json(canonicalize(request.model_dump(mode="json"))),
                            request.snapshot_source_revision_set_hash, request.capture_set_hash,
                            request.handoff_readiness_hash, request.admission_scope_set_hash, request.query_registry_hash,
                            request.date_start, request.date_end,
                            request.base_snapshot.snapshot_id if request.base_snapshot else None,
                            request.base_snapshot.snapshot_content_hash if request.base_snapshot else None,
                            request.base_snapshot.manifest_sha256 if request.base_snapshot else None,
                            request.base_snapshot.policy_compatibility_hash if request.base_snapshot else None,
                            request.builder_version, request.code_commit, request.writer_version,
                            request.partition_policy_hash, request.compression_config_hash,
                        ),
                    )
                except psycopg2.IntegrityError as error:
                    self._raise_integrity(error, "create dataset build")
                build = self._build_from_row(dict(cur.fetchone()))
                self._append_event(cur, build=build, attempt=None, event_type=DatasetBuildEventType.REQUESTED, actor=actor, payload={"request_hash": request.build_request_hash})
                return build

    def start_attempt(
        self,
        *,
        build_id: str,
        operation: AttemptOperation,
        expected_build_row_version: int,
        expected_checkpoint: BuildCheckpoint,
        lease_owner_id: str,
        lease_token: str,
        lease_seconds: int,
        operation_request_hash: str,
    ) -> DatasetBuildAttempt:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                build_row = self._select_build_locked(cur, build_id)
                build = self._build_from_row(build_row)
                self._require_build_state(build, expected_build_row_version, expected_checkpoint)
                expected_operation = {
                    BuildCheckpoint.REQUESTED: AttemptOperation.MATERIALIZE,
                    BuildCheckpoint.MATERIALIZED: AttemptOperation.VERIFY,
                    BuildCheckpoint.VERIFIED: AttemptOperation.PROMOTE,
                    BuildCheckpoint.PROMOTED: AttemptOperation.SEAL,
                }.get(build.checkpoint)
                if operation is not expected_operation:
                    raise DatasetBuildError(REASON_ATTEMPT_OPERATION_INVALID, "operation is not the legal next checkpoint transition")
                if build.current_attempt_id is not None:
                    cur.execute(
                        "SELECT attempt_state, expires_at, clock_timestamp() AS database_now FROM app.advisory_dataset_build_attempt WHERE attempt_id = %s FOR KEY SHARE",
                        (build.current_attempt_id,),
                    )
                    current = cur.fetchone()
                    if current is not None and current["attempt_state"] == "ACTIVE" and current["expires_at"] > current["database_now"]:
                        raise DatasetBuildError(REASON_BUILD_ALREADY_RUNNING, "an active attempt owns this build")
                    raise DatasetBuildError(
                        REASON_ATTEMPT_LEASE_EXPIRED,
                        "expired current attempt must be explicitly expired and recovered before a new attempt",
                    )
                cur.execute("SELECT count(*) AS count FROM app.advisory_dataset_build_attempt WHERE build_id = %s", (build_id,))
                attempt_no = int(cur.fetchone()["count"]) + 1
                fencing = build.current_fencing_token + 1
                attempt_id = f"advbuildatt_{canonical_json_sha256({'build_id': build_id, 'attempt_no': attempt_no, 'operation': operation.value})[:24]}"
                cur.execute(
                    """
                    INSERT INTO app.advisory_dataset_build_attempt (
                        attempt_id, build_id, attempt_no, operation, attempt_state,
                        lease_owner_id, lease_token, fencing_token, expected_build_row_version,
                        expected_checkpoint, acquired_at, heartbeat_at, expires_at, started_at,
                        operation_request_hash
                    ) VALUES (
                        %s, %s, %s, %s, 'ACTIVE', %s, %s, %s, %s, %s,
                        clock_timestamp(), clock_timestamp(), clock_timestamp() + make_interval(secs => %s),
                        clock_timestamp(), %s
                    ) RETURNING *
                    """,
                    (attempt_id, build_id, attempt_no, operation.value, lease_owner_id, lease_token, fencing,
                     expected_build_row_version, expected_checkpoint.value, lease_seconds, operation_request_hash),
                )
                attempt = self._attempt_from_row(dict(cur.fetchone()))
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build
                       SET current_fencing_token = %s, current_attempt_id = %s,
                           row_version = row_version + 1, updated_at = clock_timestamp()
                     WHERE build_id = %s AND row_version = %s
                    RETURNING *
                    """,
                    (fencing, attempt_id, build_id, expected_build_row_version),
                )
                if cur.fetchone() is None:
                    raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "build CAS changed during attempt acquisition")
                self._append_event(cur, build=build, attempt=attempt, event_type=DatasetBuildEventType.ATTEMPT_STARTED, actor=lease_owner_id, payload={"operation": operation.value})
                return attempt

    def append_file(self, *, attempt_id: str, expected_fencing_token: int, file: DatasetAttemptFile) -> DatasetAttemptFile:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                attempt, _ = self._require_active_attempt(cur, attempt_id, expected_fencing_token)
                if attempt.operation is not AttemptOperation.MATERIALIZE or file.attempt_id != attempt_id or file.fencing_token != expected_fencing_token:
                    raise DatasetBuildError(REASON_ATTEMPT_FILE_CONFLICT, "file does not belong to active materialize attempt")
                cur.execute(
                    "SELECT * FROM app.advisory_dataset_attempt_file WHERE attempt_id = %s AND logical_path = %s FOR UPDATE",
                    (attempt_id, file.logical_path),
                )
                existing = cur.fetchone()
                if existing is not None:
                    persisted = self._file_from_row(dict(existing))
                    if persisted == file:
                        return persisted
                    raise DatasetBuildError(REASON_ATTEMPT_FILE_CONFLICT, "same file path has different identity")
                try:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_dataset_attempt_file (
                            attempt_id, fencing_token, logical_path, logical_role, partition_key_hash, ordinal,
                            staging_uri, sha256, size_bytes, row_count, schema_fingerprint,
                            partition_content_hash, compression, writer_version
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (file.attempt_id, file.fencing_token, file.logical_path, file.logical_role, file.partition_key_hash,
                         file.ordinal, file.staging_uri, file.sha256, file.size_bytes, file.row_count,
                         file.schema_fingerprint, file.partition_content_hash, file.compression, file.writer_version),
                    )
                except psycopg2.IntegrityError as error:
                    self._raise_integrity(error, "append attempt file")
                return file

    def heartbeat_attempt(
        self,
        *,
        attempt_id: str,
        expected_fencing_token: int,
        lease_seconds: int,
    ) -> DatasetBuildAttempt:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._require_active_attempt(cur, attempt_id, expected_fencing_token)
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build_attempt a
                       SET heartbeat_at = clock_timestamp(),
                           expires_at = GREATEST(a.expires_at, clock_timestamp() + make_interval(secs => %s))
                      FROM app.advisory_dataset_build b
                     WHERE a.attempt_id = %s AND a.attempt_state = 'ACTIVE'
                       AND a.fencing_token = %s AND b.build_id = a.build_id
                       AND b.current_attempt_id = a.attempt_id
                       AND b.current_fencing_token = a.fencing_token
                    RETURNING a.*
                    """,
                    (lease_seconds, attempt_id, expected_fencing_token),
                )
                row = cur.fetchone()
                if row is None:
                    raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "attempt heartbeat lost current fencing")
                return self._attempt_from_row(dict(row))

    def complete_materialize(
        self,
        *,
        attempt_id: str,
        expected_fencing_token: int,
        observed_file_set_hash: str,
        actor: str,
        materialization_receipt: object | None = None,
    ) -> DatasetBuild:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                attempt, build = self._require_active_attempt(cur, attempt_id, expected_fencing_token)
                if attempt.operation is not AttemptOperation.MATERIALIZE:
                    raise DatasetBuildError(REASON_ATTEMPT_OPERATION_INVALID, "only MATERIALIZE attempt can complete materialization")
                files = self._files_for_attempt(cur, attempt_id)
                if not files or file_set_hash(files) != observed_file_set_hash:
                    raise DatasetBuildError(REASON_ATTEMPT_FILE_CONFLICT, "observed file set does not match persisted immutable files")
                if materialization_receipt is not None:
                    from backend.services.advisory_phase1.snapshot_writer import MaterializationReceipt

                    expected_source_identity = canonical_json_sha256(
                        {
                            "source_revision_set_hash": build.request.snapshot_source_revision_set_hash,
                            "query_registry_hash": build.request.query_registry_hash,
                            "requested_source_cutoff": build.request.requested_source_cutoff,
                            "label_as_of_ts": build.request.label_as_of_ts,
                        }
                    )
                    if (
                        not isinstance(materialization_receipt, MaterializationReceipt)
                        or materialization_receipt.build_id != build.build_id
                        or materialization_receipt.attempt_id != attempt_id
                        or materialization_receipt.source_identity_hash != expected_source_identity
                        or materialization_receipt.capture_set_hash != build.request.capture_set_hash
                        or materialization_receipt.source_revision_set_hash
                        != build.request.snapshot_source_revision_set_hash
                        or materialization_receipt.files != _attempt_file_identities(files)
                    ):
                        raise DatasetBuildError(
                            REASON_ATTEMPT_FILE_CONFLICT,
                            "materialization receipt differs from exact file readback",
                        )
                    receipt = str(materialization_receipt.receipt_hash)
                else:
                    receipt = canonical_json_sha256(
                        {
                            "attempt_id": attempt_id,
                            "file_set_hash": observed_file_set_hash,
                            "contract": "MATERIALIZE_V1",
                        }
                    )
                cur.execute(
                    "UPDATE app.advisory_dataset_build_attempt SET attempt_state = 'SUCCEEDED', finished_at = clock_timestamp() WHERE attempt_id = %s",
                    (attempt_id,),
                )
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build
                       SET checkpoint = 'MATERIALIZED', current_attempt_id = NULL,
                           materialized_attempt_id = %s, materialize_receipt_hash = %s,
                           materialized_file_set_hash = %s, row_version = row_version + 1,
                           updated_at = clock_timestamp()
                     WHERE build_id = %s AND current_attempt_id = %s AND current_fencing_token = %s
                    RETURNING *
                    """,
                    (attempt_id, receipt, observed_file_set_hash, build.build_id, attempt_id, expected_fencing_token),
                )
                row = cur.fetchone()
                if row is None:
                    raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "materialize completion lost current fencing")
                updated = self._build_from_row(dict(row))
                self._append_event(cur, build=updated, attempt=attempt, event_type=DatasetBuildEventType.MATERIALIZED, actor=actor, payload={"receipt": receipt})
                return updated

    def complete_verify(
        self,
        *,
        attempt_id: str,
        expected_fencing_token: int,
        observed_file_set_hash: str,
        verification_contract_version: str,
        actor: str,
    ) -> DatasetBuild:
        if verification_contract_version != BATCH_C_FILESET_VERIFICATION_CONTRACT:
            raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "Batch C cannot issue a full-Parquet verification receipt")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                attempt, build = self._require_active_attempt(cur, attempt_id, expected_fencing_token)
                if attempt.operation is not AttemptOperation.VERIFY or build.materialized_file_set_hash != observed_file_set_hash:
                    raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "verify must consume the exact materialized file set")
                receipt = canonical_json_sha256({"attempt_id": attempt_id, "file_set_hash": observed_file_set_hash, "verification_contract_version": verification_contract_version})
                cur.execute("UPDATE app.advisory_dataset_build_attempt SET attempt_state = 'SUCCEEDED', finished_at = clock_timestamp() WHERE attempt_id = %s", (attempt_id,))
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build
                       SET checkpoint = 'VERIFIED', current_attempt_id = NULL,
                           verified_attempt_id = %s, verify_receipt_hash = %s,
                           verified_file_set_hash = %s, verification_contract_version = %s,
                           row_version = row_version + 1, updated_at = clock_timestamp()
                     WHERE build_id = %s AND current_attempt_id = %s AND current_fencing_token = %s
                    RETURNING *
                    """,
                    (attempt_id, receipt, observed_file_set_hash, verification_contract_version, build.build_id, attempt_id, expected_fencing_token),
                )
                row = cur.fetchone()
                if row is None:
                    raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "verify completion lost current fencing")
                updated = self._build_from_row(dict(row))
                self._append_event(cur, build=updated, attempt=attempt, event_type=DatasetBuildEventType.VERIFIED, actor=actor, payload={"receipt": receipt})
                return updated

    def complete_full_verify(
        self,
        *,
        attempt_id: str,
        expected_fencing_token: int,
        receipt: object,
        actor: str,
    ) -> DatasetBuild:
        """Persist only a typed Batch D all-file verification receipt."""

        from backend.services.advisory_phase1.snapshot_writer import (
            BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT,
            FullParquetVerifier,
            FullParquetVerificationReceipt,
            capability_manifest_for_build,
            written_files_from_attempt,
        )

        if not isinstance(receipt, FullParquetVerificationReceipt):
            raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "Batch D verification requires a typed full-parquet receipt")
        preflight_build = self.get_build(receipt.build_id)
        if preflight_build.materialized_attempt_id is None:
            raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "Batch D build has no materialized attempt")
        preflight_files = self.files_for_attempt(preflight_build.materialized_attempt_id)
        try:
            reconstructed = FullParquetVerifier().verify_files(
                build=preflight_build,
                files=written_files_from_attempt(preflight_files),
                capability_manifest=capability_manifest_for_build(preflight_build),
            )
        except Exception as error:
            raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "full verification file readback failed") from error
        if reconstructed != receipt:
            raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "full verification receipt was not produced by exact file readback")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                attempt, build = self._require_active_attempt(cur, attempt_id, expected_fencing_token)
                if (
                    attempt.operation is not AttemptOperation.VERIFY
                    or build.materialized_file_set_hash != receipt.file_set_hash
                    or receipt.build_id != build.build_id
                    or receipt.verification_contract_version != BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT
                    or receipt.capture_set_hash != build.request.capture_set_hash
                    or receipt.source_revision_set_hash != build.request.snapshot_source_revision_set_hash
                    or receipt.selected_observation_mapping_set_hash != build.request.selected_observation_mapping_set_hash
                    or receipt.selected_label_mapping_set_hash != build.request.selected_label_mapping_set_hash
                ):
                    raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "full verification must consume the exact materialized Batch D file set")
                materialized_files = self._files_for_attempt(cur, str(build.materialized_attempt_id))
                if _attempt_file_identities(materialized_files) != tuple(
                    sorted((item.file for item in receipt.files), key=lambda item: item.logical_path)
                ):
                    raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "full verification files differ from persisted materialization")
                cur.execute(
                    "UPDATE app.advisory_dataset_build_attempt SET attempt_state = 'SUCCEEDED', finished_at = clock_timestamp() WHERE attempt_id = %s",
                    (attempt_id,),
                )
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build
                       SET checkpoint = 'VERIFIED', current_attempt_id = NULL,
                           verified_attempt_id = %s, verify_receipt_hash = %s,
                           verified_file_set_hash = %s, verification_contract_version = %s,
                           row_version = row_version + 1, updated_at = clock_timestamp()
                     WHERE build_id = %s AND current_attempt_id = %s AND current_fencing_token = %s
                    RETURNING *
                    """,
                    (
                        attempt_id, receipt.receipt_hash, receipt.file_set_hash,
                        BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT,
                        build.build_id, attempt_id, expected_fencing_token,
                    ),
                )
                row = cur.fetchone()
                if row is None:
                    raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "full verification completion lost current fencing")
                updated = self._build_from_row(dict(row))
                self._append_event(
                    cur,
                    build=updated,
                    attempt=attempt,
                    event_type=DatasetBuildEventType.VERIFIED,
                    actor=actor,
                    payload={"receipt": receipt.receipt_hash, "contract": BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT},
                )
                return updated

    def complete_promote(
        self,
        *,
        attempt_id: str,
        expected_fencing_token: int,
        receipt: object,
        manifest: object,
        store: object,
        actor: str,
    ) -> DatasetBuild:
        """Advance VERIFIED to PROMOTED after caller has reopened every CAS object."""

        from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
        from backend.services.advisory_phase1.snapshot_writer import DatasetManifest, PromotionReceipt

        if (
            not isinstance(receipt, PromotionReceipt)
            or not isinstance(manifest, DatasetManifest)
            or not isinstance(store, LocalContentAddressedStore)
        ):
            raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "promotion requires typed receipt, manifest, and CAS")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                attempt, build = self._require_active_attempt(cur, attempt_id, expected_fencing_token)
                if (
                    attempt.operation is not AttemptOperation.PROMOTE
                    or build.verification_contract_version != BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT
                    or receipt.build_id != build.build_id
                    or receipt.full_verification_receipt_hash != build.verify_receipt_hash
                    or receipt.manifest_core_sha256 != manifest.core.manifest_core_sha256
                    or receipt.manifest_sha256 != manifest.manifest_sha256
                    or receipt.store_backend_hash != manifest.store_backend_hash
                    or receipt.store_backend_hash != store.store_backend_hash
                    or tuple(receipt.blobs) != tuple(manifest.core.files)
                ):
                    raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "promotion must consume one verified full-parquet receipt")
                materialized_files = self._files_for_attempt(cur, str(build.materialized_attempt_id))
                expected_identities = _attempt_file_identities(materialized_files)
                promoted_identities = _snapshot_file_identities(receipt.blobs)
                if expected_identities != promoted_identities or receipt.verified_content_set_hash != canonical_json_sha256(
                    [item.canonical_identity() for item in expected_identities]
                ):
                    raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "promotion blobs differ from verified materialization")
                _verify_promoted_cas(store=store, manifest=manifest, receipt=receipt)
                cur.execute(
                    "UPDATE app.advisory_dataset_build_attempt SET attempt_state = 'SUCCEEDED', finished_at = clock_timestamp() WHERE attempt_id = %s",
                    (attempt_id,),
                )
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build
                       SET checkpoint = 'PROMOTED', current_attempt_id = NULL,
                           promoted_attempt_id = %s, promotion_receipt_hash = %s,
                           promoted_manifest_hash = %s, row_version = row_version + 1,
                           updated_at = clock_timestamp()
                     WHERE build_id = %s AND current_attempt_id = %s AND current_fencing_token = %s
                    RETURNING *
                    """,
                    (
                        attempt_id, receipt.receipt_sha256, receipt.manifest_sha256,
                        build.build_id, attempt_id, expected_fencing_token,
                    ),
                )
                row = cur.fetchone()
                if row is None:
                    raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "promotion completion lost current fencing")
                updated = self._build_from_row(dict(row))
                self._append_event(
                    cur,
                    build=updated,
                    attempt=attempt,
                    event_type=DatasetBuildEventType.PROMOTED,
                    actor=actor,
                    payload={"receipt": receipt.receipt_sha256, "manifest": receipt.manifest_sha256},
                )
                return updated

    def files_for_attempt(self, attempt_id: str) -> tuple[DatasetAttemptFile, ...]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._files_for_attempt(cur, attempt_id)

    def fail_attempt(self, *, attempt_id: str, expected_fencing_token: int, error_code: str, actor: str) -> DatasetBuildAttempt:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                attempt, build = self._require_active_attempt(cur, attempt_id, expected_fencing_token)
                error_hash = canonical_json_sha256({"error_code": error_code})
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build_attempt
                       SET attempt_state = 'FAILED', finished_at = clock_timestamp(), error_code = %s, error_hash = %s
                     WHERE attempt_id = %s RETURNING *
                    """,
                    (error_code, error_hash, attempt_id),
                )
                failed = self._attempt_from_row(dict(cur.fetchone()))
                cur.execute(
                    "UPDATE app.advisory_dataset_build SET current_attempt_id = NULL, row_version = row_version + 1, updated_at = clock_timestamp() WHERE build_id = %s",
                    (build.build_id,),
                )
                self._append_event(cur, build=build, attempt=failed, event_type=DatasetBuildEventType.ATTEMPT_FAILED, actor=actor, payload={"error_code": error_code}, reasons=(error_code,))
                return failed

    def expire_attempt(self, *, attempt_id: str, expected_fencing_token: int, actor: str) -> DatasetBuildAttempt:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                attempt, build = self._require_active_attempt_allow_expired(cur, attempt_id, expected_fencing_token)
                cur.execute("SELECT clock_timestamp() AS database_now")
                if attempt.expires_at > cur.fetchone()["database_now"]:
                    raise DatasetBuildError(REASON_ATTEMPT_LEASE_EXPIRED, "attempt lease has not expired")
                error_hash = canonical_json_sha256({"reason": REASON_ATTEMPT_LEASE_EXPIRED})
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build_attempt
                       SET attempt_state = 'EXPIRED', finished_at = clock_timestamp(),
                           error_code = %s, error_hash = %s
                     WHERE attempt_id = %s RETURNING *
                    """,
                    (REASON_ATTEMPT_LEASE_EXPIRED, error_hash, attempt_id),
                )
                expired = self._attempt_from_row(dict(cur.fetchone()))
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build
                       SET current_attempt_id = NULL, current_fencing_token = current_fencing_token + 1,
                           row_version = row_version + 1, updated_at = clock_timestamp()
                     WHERE build_id = %s AND current_attempt_id = %s AND current_fencing_token = %s
                    """,
                    (build.build_id, attempt_id, expected_fencing_token),
                )
                self._append_event(cur, build=build, attempt=expired, event_type=DatasetBuildEventType.ATTEMPT_EXPIRED, actor=actor, payload={"attempt_id": attempt_id}, reasons=(REASON_ATTEMPT_LEASE_EXPIRED,))
                return expired

    def recover_expired_attempt(self, *, expired_attempt_id: str, actor: str) -> DatasetBuildAttempt:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM app.advisory_dataset_build_attempt WHERE attempt_id = %s FOR UPDATE", (expired_attempt_id,))
                row = cur.fetchone()
                if row is None:
                    raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "expired attempt does not exist")
                expired = self._attempt_from_row(dict(row))
                if expired.state is not AttemptState.EXPIRED:
                    raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "recovery requires an EXPIRED attempt")
                build = self._build_from_row(self._select_build_locked(cur, expired.build_id))
                if build.current_attempt_id is not None or build.current_fencing_token <= expired.fencing_token:
                    raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "expired attempt has not been fenced off")
                cur.execute(
                    "SELECT * FROM app.advisory_dataset_build_attempt WHERE predecessor_attempt_id = %s FOR UPDATE",
                    (expired_attempt_id,),
                )
                existing_recovery = cur.fetchone()
                if existing_recovery is not None:
                    recovery = self._attempt_from_row(dict(existing_recovery))
                    if recovery.lease_owner_id != actor:
                        raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "expired attempt recovery has different actor")
                    return recovery
                cur.execute("SELECT count(*) AS count FROM app.advisory_dataset_build_attempt WHERE build_id = %s", (build.build_id,))
                attempt_no = int(cur.fetchone()["count"]) + 1
                recovery_id = f"advbuildatt_{canonical_json_sha256({'build_id': build.build_id, 'attempt_no': attempt_no, 'operation': AttemptOperation.RECOVER.value})[:24]}"
                cur.execute(
                    """
                    INSERT INTO app.advisory_dataset_build_attempt (
                        attempt_id, build_id, attempt_no, operation, attempt_state, lease_owner_id, lease_token,
                        fencing_token, expected_build_row_version, expected_checkpoint, acquired_at, heartbeat_at,
                        expires_at, started_at, finished_at, predecessor_attempt_id, operation_request_hash
                    ) VALUES (
                        %s, %s, %s, 'RECOVER', 'SUCCEEDED', %s, 'recovery-receipt', %s, %s, %s,
                        clock_timestamp(), clock_timestamp(), clock_timestamp() + make_interval(secs => 1),
                        clock_timestamp(), clock_timestamp(), %s, %s
                    ) RETURNING *
                    """,
                    (recovery_id, build.build_id, attempt_no, actor, build.current_fencing_token,
                     build.row_version, build.checkpoint.value, expired_attempt_id,
                     canonical_json_sha256({"expired_attempt_id": expired_attempt_id, "build_fencing": build.current_fencing_token})),
                )
                recovery = self._attempt_from_row(dict(cur.fetchone()))
                self._append_event(cur, build=build, attempt=recovery, event_type=DatasetBuildEventType.RECOVERY_STARTED, actor=actor, payload={"expired_attempt_id": expired_attempt_id})
                return recovery

    def terminate_build(
        self,
        *,
        build_id: str,
        expected_row_version: int,
        expected_checkpoint: BuildCheckpoint,
        expected_fencing_token: int,
        terminal: BuildLifecycle,
        reason_code: str,
        actor: str,
    ) -> DatasetBuild:
        if terminal not in {BuildLifecycle.ABORTED, BuildLifecycle.FAILED_TERMINAL}:
            raise ValueError("terminal lifecycle must be ABORTED or FAILED_TERMINAL")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                build = self._build_from_row(self._select_build_locked(cur, build_id))
                self._require_build_state(build, expected_row_version, expected_checkpoint)
                if build.current_fencing_token != expected_fencing_token:
                    raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "build termination fencing token is stale")
                receipt = canonical_json_sha256({"build_id": build_id, "checkpoint": expected_checkpoint.value, "terminal": terminal.value, "reason_code": reason_code})
                if build.current_attempt_id is not None:
                    cur.execute(
                        """
                        UPDATE app.advisory_dataset_build_attempt
                           SET attempt_state = 'ABORTED', finished_at = clock_timestamp(),
                               error_code = %s, error_hash = %s
                         WHERE attempt_id = %s AND attempt_state = 'ACTIVE'
                        """,
                        (reason_code, canonical_json_sha256({"reason_code": reason_code}), build.current_attempt_id),
                    )
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build
                       SET lifecycle_status = %s, current_attempt_id = NULL,
                           terminated_at = clock_timestamp(), termination_receipt_hash = %s,
                           terminal_reason_code = %s, terminal_payload_hash = %s,
                           row_version = row_version + 1, updated_at = clock_timestamp()
                     WHERE build_id = %s AND row_version = %s AND current_fencing_token = %s
                    RETURNING *
                    """,
                    (terminal.value, receipt, reason_code, canonical_json_sha256({"reason_code": reason_code}), build_id, expected_row_version, expected_fencing_token),
                )
                row = cur.fetchone()
                if row is None:
                    raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "build termination CAS failed")
                updated = self._build_from_row(dict(row))
                self._append_event(cur, build=updated, attempt=None, event_type=DatasetBuildEventType.BUILD_TERMINATED, actor=actor, payload={"receipt": receipt}, reasons=(reason_code,))
                return updated

    def get_build(self, build_id: str) -> DatasetBuild:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._build_from_row(self._select_build_locked(cur, build_id))

    def snapshot_files(self, snapshot_id: str) -> tuple[DatasetSnapshotFile, ...]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT logical_path, logical_role, partition_key_hash, ordinal, content_uri,
                           sha256, size_bytes, row_count, schema_fingerprint, partition_content_hash,
                           store_backend_hash, blob_sha256
                      FROM app.advisory_dataset_snapshot_file
                     WHERE snapshot_id = %s
                     ORDER BY logical_path
                    """,
                    (snapshot_id,),
                )
                return tuple(
                    DatasetSnapshotFile(
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
                    for row in cur.fetchall()
                )

    def append_snapshot_invalidation(self, invalidation: DatasetSnapshotInvalidation) -> DatasetSnapshotInvalidation:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (f"advisory_snapshot:{invalidation.snapshot_id}",),
                )
                cur.execute(
                    "SELECT manifest_sha256 FROM app.advisory_dataset_snapshot WHERE snapshot_id = %s AND snapshot_state = 'SEALED' FOR KEY SHARE",
                    (invalidation.snapshot_id,),
                )
                snapshot_row = cur.fetchone()
                if snapshot_row is None or str(snapshot_row["manifest_sha256"]) != invalidation.manifest_sha256:
                    raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "invalidation does not match one sealed snapshot manifest")
                cur.execute(
                    "SELECT * FROM app.advisory_dataset_snapshot_invalidation WHERE invalidation_request_hash = %s FOR UPDATE",
                    (invalidation.invalidation_request_hash,),
                )
                existing = cur.fetchone()
                if existing is not None:
                    persisted = DatasetSnapshotInvalidation(
                        snapshot_id=str(existing["snapshot_id"]), manifest_sha256=str(existing["manifest_sha256"]),
                        invalidated_by=str(existing["invalidated_by"]), reason_code=str(existing["reason_code"]),
                        reason_hash=str(existing["reason_hash"]), invalidation_request_hash=str(existing["invalidation_request_hash"]),
                        replacement_snapshot_id=(str(existing["replacement_snapshot_id"]) if existing["replacement_snapshot_id"] else None),
                        invalidation_content_hash=str(existing["invalidation_content_hash"]), invalidation_id=str(existing["invalidation_id"]),
                    )
                    if persisted != invalidation:
                        raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "invalidation request hash has different content")
                    return persisted
                try:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_dataset_snapshot_invalidation (
                            invalidation_id, snapshot_id, manifest_sha256, invalidated_by, reason_code,
                            reason_hash, invalidation_request_hash, replacement_snapshot_id, invalidation_content_hash
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (invalidation.invalidation_id, invalidation.snapshot_id, invalidation.manifest_sha256,
                         invalidation.invalidated_by, invalidation.reason_code, invalidation.reason_hash,
                         invalidation.invalidation_request_hash, invalidation.replacement_snapshot_id,
                         invalidation.invalidation_content_hash),
                    )
                except psycopg2.IntegrityError as error:
                    self._raise_integrity(error, "append snapshot invalidation")
                return invalidation

    def is_snapshot_invalidated(self, snapshot_id: str) -> bool:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM app.advisory_dataset_snapshot_invalidation WHERE snapshot_id = %s", (snapshot_id,))
                return cur.fetchone() is not None

    def assert_base_snapshot_reusable(self, request: FixtureDatasetBuildRequest) -> None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._require_base_snapshot_admission(cur, request)

    def save_sealed_snapshot(self, snapshot: SealedDatasetSnapshot, *, actor: str) -> SealedDatasetSnapshot:
        """Persist a Batch-D-produced aggregate in one short seal transaction.

        Batch C cannot fabricate this input: the model requires the Batch D
        full-Parquet verification contract before this method can be reached.
        """

        if snapshot.verification_contract_version != BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT:
            raise DatasetBuildError(REASON_CHECKPOINT_CONFLICT, "only full Parquet verification can seal a snapshot")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"advisory_snapshot:{snapshot.snapshot_id}",))
                cur.execute("SELECT snapshot_content_hash FROM app.advisory_dataset_snapshot WHERE snapshot_id = %s FOR UPDATE", (snapshot.snapshot_id,))
                existing = cur.fetchone()
                if existing is not None:
                    self._assert_snapshot_exact_retry(cur, snapshot)
                    return snapshot
                build = self._build_from_row(self._select_build_locked(cur, snapshot.build_id))
                if (
                    build.lifecycle is not BuildLifecycle.ACTIVE
                    or build.checkpoint is not BuildCheckpoint.PROMOTED
                    or build.verification_contract_version != BATCH_D_FULL_PARQUET_VERIFICATION_CONTRACT
                    or build.current_attempt_id != snapshot.seal_attempt_id
                    or build.promotion_receipt_hash != snapshot.promotion_receipt_hash
                    or build.promoted_manifest_hash != snapshot.manifest_sha256
                ):
                    raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "snapshot seal requires one promoted Batch D build")
                seal_attempt, _ = self._require_active_attempt(cur, snapshot.seal_attempt_id, build.current_fencing_token)
                if seal_attempt.operation is not AttemptOperation.SEAL:
                    raise DatasetBuildError(REASON_ATTEMPT_OPERATION_INVALID, "snapshot seal requires an active SEAL attempt")
                for file in snapshot.files:
                    cur.execute(
                        "INSERT INTO app.advisory_dataset_blob (store_backend_hash, blob_sha256, size_bytes) VALUES (%s, %s, %s) ON CONFLICT (store_backend_hash, blob_sha256) DO NOTHING",
                        (file.blob.store_backend_hash, file.blob.blob_sha256, file.blob.size_bytes),
                    )
                    cur.execute("SELECT size_bytes FROM app.advisory_dataset_blob WHERE store_backend_hash = %s AND blob_sha256 = %s FOR KEY SHARE", (file.blob.store_backend_hash, file.blob.blob_sha256))
                    if int(cur.fetchone()["size_bytes"]) != file.blob.size_bytes:
                        raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "snapshot blob identity has conflicting size")
                try:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_dataset_snapshot (
                            snapshot_id, snapshot_content_hash, snapshot_state, manifest_core_sha256, manifest_sha256,
                            promotion_receipt_uri, promotion_receipt_hash, build_id, snapshot_schema_version,
                            snapshot_source_revision_set_hash, capture_set_hash, base_snapshot_id,
                            base_snapshot_content_hash, base_manifest_sha256, base_policy_compatibility_hash, handoff_readiness_hash,
                            admission_scope_set_hash, query_registry_hash, builder_version, code_commit, writer_version,
                            partition_policy_hash, policy_compatibility_hash, dataset_capability_manifest, dataset_capability_manifest_hash,
                            schema_fingerprint, file_count, row_count, total_bytes, label_maturity_event_summary
                        ) VALUES (%s, %s, 'SEALED', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            snapshot.snapshot_id, snapshot.snapshot_content_hash, snapshot.manifest_core_sha256,
                            snapshot.manifest_sha256, snapshot.promotion_receipt_uri, snapshot.promotion_receipt_hash,
                            snapshot.build_id, snapshot.snapshot_schema_version, snapshot.snapshot_source_revision_set_hash,
                            snapshot.capture_set_hash, snapshot.base_snapshot.snapshot_id if snapshot.base_snapshot else None,
                            snapshot.base_snapshot.snapshot_content_hash if snapshot.base_snapshot else None,
                            snapshot.base_snapshot.manifest_sha256 if snapshot.base_snapshot else None,
                            snapshot.base_snapshot.policy_compatibility_hash if snapshot.base_snapshot else None,
                            snapshot.handoff_readiness_hash, snapshot.admission_scope_set_hash, snapshot.query_registry_hash,
                            snapshot.builder_version, snapshot.code_commit, snapshot.writer_version, snapshot.partition_policy_hash,
                            snapshot.policy_compatibility_hash,
                            psycopg2.extras.Json(snapshot.dataset_capability_manifest), snapshot.dataset_capability_manifest_hash,
                            snapshot.schema_fingerprint, len(snapshot.files), sum(item.row_count for item in snapshot.files),
                            sum(item.size_bytes for item in snapshot.files), psycopg2.extras.Json(snapshot.label_maturity_event_summary),
                        ),
                    )
                    self._insert_snapshot_membership(cur, snapshot)
                except psycopg2.IntegrityError as error:
                    self._raise_integrity(error, "seal dataset snapshot")
                cur.execute(
                    "UPDATE app.advisory_dataset_build_attempt SET attempt_state = 'SUCCEEDED', finished_at = clock_timestamp() WHERE attempt_id = %s",
                    (snapshot.seal_attempt_id,),
                )
                cur.execute(
                    """
                    UPDATE app.advisory_dataset_build
                       SET lifecycle_status = 'SEALED', checkpoint = 'SEALED', current_attempt_id = NULL,
                           sealed_attempt_id = %s, seal_receipt_hash = %s, sealed_snapshot_id = %s,
                           row_version = row_version + 1, updated_at = clock_timestamp()
                     WHERE build_id = %s AND lifecycle_status = 'ACTIVE' AND checkpoint = 'PROMOTED'
                    RETURNING *
                    """,
                    (snapshot.seal_attempt_id, snapshot.seal_receipt_hash, snapshot.snapshot_id, snapshot.build_id),
                )
                if cur.fetchone() is None:
                    raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "build changed before seal CAS")
                self._append_event(cur, build=build, attempt=None, event_type=DatasetBuildEventType.SEALED, actor=actor, payload={"snapshot_content_hash": snapshot.snapshot_content_hash})
                return snapshot

    @staticmethod
    def _insert_snapshot_membership(cur: Any, snapshot: SealedDatasetSnapshot) -> None:
        for file in snapshot.files:
            cur.execute(
                """
                INSERT INTO app.advisory_dataset_snapshot_file (
                    snapshot_id, logical_path, logical_role, partition_key_hash, ordinal, content_uri,
                    sha256, size_bytes, row_count, schema_fingerprint, partition_content_hash,
                    store_backend_hash, blob_sha256
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (snapshot.snapshot_id, file.logical_path, file.logical_role, file.partition_key_hash, file.ordinal,
                 file.content_uri, file.sha256, file.size_bytes, file.row_count, file.schema_fingerprint,
                 file.partition_content_hash, file.blob.store_backend_hash, file.blob.blob_sha256),
            )
        for item in snapshot.observations:
            cur.execute(
                "INSERT INTO app.advisory_dataset_snapshot_observation (snapshot_id, canonical_signal_id, observation_version_id, evidence_scope, oos_interval_id, selector_policy_hash) VALUES (%s, %s, %s, %s, %s, %s)",
                (snapshot.snapshot_id, item.canonical_signal_id, item.observation_version_id, item.evidence_scope, item.oos_interval_id, item.selector_policy_hash),
            )
        for item in snapshot.labels:
            cur.execute(
                """
                INSERT INTO app.advisory_dataset_snapshot_label (
                    snapshot_id, label_key_hash, label_version_id, canonical_signal_id,
                    observation_version_id, candidate_stage_evidence_id, symbol, selector_policy_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (snapshot.snapshot_id, item.label_key_hash, item.label_version_id, item.canonical_signal_id,
                 item.observation_version_id, item.candidate_stage_evidence_id, item.symbol, item.selector_policy_hash),
            )
        for ref in snapshot.blob_refs:
            cur.execute(
                """
                INSERT INTO app.advisory_dataset_snapshot_blob_ref (
                    snapshot_id, logical_path, logical_role, partition_key_hash, ordinal,
                    store_backend_hash, blob_sha256, ref_content_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (snapshot.snapshot_id, ref.logical_path, ref.logical_role, ref.partition_key_hash, ref.ordinal,
                 ref.blob.store_backend_hash, ref.blob.blob_sha256, ref.ref_content_hash),
            )

    @staticmethod
    def _assert_snapshot_exact_retry(cur: Any, snapshot: SealedDatasetSnapshot) -> None:
        cur.execute(
            """
            SELECT snapshot_id, snapshot_content_hash, snapshot_state, manifest_core_sha256, manifest_sha256,
                   promotion_receipt_uri, promotion_receipt_hash, build_id, snapshot_schema_version,
                   snapshot_source_revision_set_hash, capture_set_hash, base_snapshot_id,
                   base_snapshot_content_hash, base_manifest_sha256, base_policy_compatibility_hash,
                   handoff_readiness_hash, admission_scope_set_hash, query_registry_hash, builder_version,
                   code_commit, writer_version, partition_policy_hash, policy_compatibility_hash,
                   dataset_capability_manifest, dataset_capability_manifest_hash, schema_fingerprint,
                   file_count, row_count, total_bytes, label_maturity_event_summary
              FROM app.advisory_dataset_snapshot WHERE snapshot_id = %s
            """,
            (snapshot.snapshot_id,),
        )
        header = cur.fetchone()
        expected_header = {
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_content_hash": snapshot.snapshot_content_hash,
            "snapshot_state": "SEALED",
            "manifest_core_sha256": snapshot.manifest_core_sha256,
            "manifest_sha256": snapshot.manifest_sha256,
            "promotion_receipt_uri": snapshot.promotion_receipt_uri,
            "promotion_receipt_hash": snapshot.promotion_receipt_hash,
            "build_id": snapshot.build_id,
            "snapshot_schema_version": snapshot.snapshot_schema_version,
            "snapshot_source_revision_set_hash": snapshot.snapshot_source_revision_set_hash,
            "capture_set_hash": snapshot.capture_set_hash,
            "base_snapshot_id": snapshot.base_snapshot.snapshot_id if snapshot.base_snapshot else None,
            "base_snapshot_content_hash": snapshot.base_snapshot.snapshot_content_hash if snapshot.base_snapshot else None,
            "base_manifest_sha256": snapshot.base_snapshot.manifest_sha256 if snapshot.base_snapshot else None,
            "base_policy_compatibility_hash": snapshot.base_snapshot.policy_compatibility_hash if snapshot.base_snapshot else None,
            "handoff_readiness_hash": snapshot.handoff_readiness_hash,
            "admission_scope_set_hash": snapshot.admission_scope_set_hash,
            "query_registry_hash": snapshot.query_registry_hash,
            "builder_version": snapshot.builder_version,
            "code_commit": snapshot.code_commit,
            "writer_version": snapshot.writer_version,
            "partition_policy_hash": snapshot.partition_policy_hash,
            "policy_compatibility_hash": snapshot.policy_compatibility_hash,
            "dataset_capability_manifest": snapshot.dataset_capability_manifest,
            "dataset_capability_manifest_hash": snapshot.dataset_capability_manifest_hash,
            "schema_fingerprint": snapshot.schema_fingerprint,
            "file_count": len(snapshot.files),
            "row_count": sum(item.row_count for item in snapshot.files),
            "total_bytes": sum(item.size_bytes for item in snapshot.files),
            "label_maturity_event_summary": snapshot.label_maturity_event_summary,
        }
        queries = {
            "files": "SELECT logical_path, logical_role, partition_key_hash, ordinal, content_uri, sha256, size_bytes, row_count, schema_fingerprint, partition_content_hash, store_backend_hash, blob_sha256 FROM app.advisory_dataset_snapshot_file WHERE snapshot_id = %s ORDER BY logical_path",
            "observations": "SELECT canonical_signal_id, observation_version_id, evidence_scope, oos_interval_id, selector_policy_hash FROM app.advisory_dataset_snapshot_observation WHERE snapshot_id = %s ORDER BY canonical_signal_id",
            "labels": "SELECT label_key_hash, label_version_id, canonical_signal_id, observation_version_id, candidate_stage_evidence_id, symbol, selector_policy_hash FROM app.advisory_dataset_snapshot_label WHERE snapshot_id = %s ORDER BY label_key_hash",
            "blob_refs": "SELECT logical_path, logical_role, partition_key_hash, ordinal, store_backend_hash, blob_sha256, ref_content_hash FROM app.advisory_dataset_snapshot_blob_ref WHERE snapshot_id = %s ORDER BY logical_path",
        }
        persisted: dict[str, object] = {"header": dict(header) if header is not None else None}
        for name, query in queries.items():
            cur.execute(query, (snapshot.snapshot_id,))
            persisted[name] = [dict(row) for row in cur.fetchall()]
        expected = {
            "header": expected_header,
            "files": [
                {
                    "logical_path": item.logical_path, "logical_role": item.logical_role,
                    "partition_key_hash": item.partition_key_hash, "ordinal": item.ordinal,
                    "content_uri": item.content_uri, "sha256": item.sha256, "size_bytes": item.size_bytes,
                    "row_count": item.row_count, "schema_fingerprint": item.schema_fingerprint,
                    "partition_content_hash": item.partition_content_hash,
                    "store_backend_hash": item.blob.store_backend_hash, "blob_sha256": item.blob.blob_sha256,
                }
                for item in sorted(snapshot.files, key=lambda value: value.logical_path)
            ],
            "observations": [
                item.model_dump(mode="json") for item in sorted(snapshot.observations, key=lambda value: value.canonical_signal_id)
            ],
            "labels": [item.model_dump(mode="json") for item in sorted(snapshot.labels, key=lambda value: value.label_key_hash)],
            "blob_refs": [
                {
                    "logical_path": item.logical_path, "logical_role": item.logical_role,
                    "partition_key_hash": item.partition_key_hash, "ordinal": item.ordinal,
                    "store_backend_hash": item.blob.store_backend_hash, "blob_sha256": item.blob.blob_sha256,
                    "ref_content_hash": item.ref_content_hash,
                }
                for item in sorted(snapshot.blob_refs, key=lambda value: value.logical_path)
            ],
        }
        if canonical_json_sha256(canonicalize(persisted)) != canonical_json_sha256(canonicalize(expected)):
            raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "snapshot exact retry does not match persisted aggregate")

    @staticmethod
    def _require_build_state(build: DatasetBuild, row_version: int, checkpoint: BuildCheckpoint) -> None:
        if build.lifecycle is not BuildLifecycle.ACTIVE or build.row_version != row_version or build.checkpoint is not checkpoint:
            raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "build row version/checkpoint/lifecycle is stale")

    @staticmethod
    def _require_capture_admission(cur: Any, request: FixtureDatasetBuildRequest) -> None:
        """Revalidate every frozen capture descriptor against authority rows."""

        expected_scopes = {(item.identity_id, item.identity_hash) for item in request.admission_scopes}
        observed_scopes: set[tuple[str, str]] = set()
        observed_observation_mappings: set[tuple[str, str]] = set()
        observed_label_mappings: set[tuple[str, str]] = set()
        observed_label_targets: set[tuple[int, str]] = set()
        for member in request.captures:
            cur.execute(
                """
                SELECT capture_request_hash, request_payload_jsonb, handoff_readiness_hash,
                       admission_scope_id, admission_scope_hash, capture_status,
                       membership_hash, capture_receipt_hash, capture_purpose
                  FROM app.advisory_capture_batch
                 WHERE capture_batch_id = %s
                 FOR KEY SHARE
                """,
                (member.capture_batch_id,),
            )
            row = cur.fetchone()
            if row is None or str(row["capture_status"]) != "COMPLETE":
                raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "build capture is absent or not COMPLETE")
            expected = {
                "capture_request_hash": member.capture_request_hash,
                "capture_receipt_hash": member.capture_receipt_hash,
                "membership_hash": member.membership_hash,
                "capture_purpose": member.capture_purpose,
                "handoff_readiness_hash": member.handoff_readiness_hash,
                "admission_scope_id": member.admission_scope_id,
                "admission_scope_hash": member.admission_scope_hash,
            }
            for column, value in expected.items():
                if str(row[column] or "") != value:
                    raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, f"build capture {column} does not match authority")
            if member.handoff_readiness_hash != request.handoff_readiness_hash:
                raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "build capture handoff readiness is incompatible")
            observed_scopes.add((member.admission_scope_id, member.admission_scope_hash))
            cur.execute(
                """
                SELECT evidence_role, evidence_id, evidence_content_hash
                  FROM app.advisory_capture_batch_evidence_membership
                 WHERE capture_batch_id = %s
                 ORDER BY evidence_role, evidence_id
                 FOR KEY SHARE
                """,
                (member.capture_batch_id,),
            )
            for membership in cur.fetchall():
                identity = (str(membership["evidence_id"]), str(membership["evidence_content_hash"]))
                if membership["evidence_role"] == "selected_observation_mapping":
                    observed_observation_mappings.add(identity)
                elif membership["evidence_role"] == "selected_label_mapping":
                    observed_label_mappings.add(identity)
            payload = canonicalize(dict(row["request_payload_jsonb"]))
            if member.capture_purpose == "OBSERVATION_CAPTURE_V1":
                plans = payload.get("plans")
                if not isinstance(plans, list) or not plans:
                    raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "observation capture has no frozen plans")
                dates = {str(item.get("decision_as_of_trade_date")) for item in plans if isinstance(item, dict)}
                source_pairs = {
                    (str(item.get("signal_source_revision_set_id")), str(item.get("signal_source_revision_set_hash")))
                    for item in plans
                    if isinstance(item, dict)
                }
                if any(
                    str(item.get("phase0a_audit_id")) != request.phase0a_audit_id
                    or str(item.get("phase0a_audit_manifest_hash")) != request.phase0a_audit_hash
                    for item in plans
                    if isinstance(item, dict)
                ):
                    raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "observation capture Phase 0A audit identity is incompatible")
            else:
                planned_labels = payload.get("planned_labels")
                if not isinstance(planned_labels, list):
                    raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "label capture planned labels are malformed")
                dates = {str(item.get("decision_as_of_trade_date")) for item in planned_labels if isinstance(item, dict)}
                source_pairs = {
                    (str(payload.get("label_source_revision_set_id")), str(payload.get("label_source_revision_set_hash")))
                }
                if (
                    str(payload.get("label_policy_bundle_id")) != request.label_policy_bundle_id
                    or str(payload.get("label_policy_bundle_hash")) != request.label_policy_bundle_hash
                    or not _same_aware_timestamp(payload.get("label_as_of_ts"), request.label_as_of_ts)
                ):
                    raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "label capture policy or as-of identity is incompatible")
                observed_label_targets.update(
                    (int(item["horizon_trading_days"]), str(item["projection"]))
                    for item in planned_labels
                    if isinstance(item, dict)
                )
            expected_dates = {item.isoformat() for item in (member.date_start, member.date_end)}
            if not dates or min(dates) != min(expected_dates) or max(dates) != max(expected_dates):
                raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "build capture date range does not match frozen request")
            if source_pairs != {(member.source_revision_set_id, member.source_revision_set_hash)}:
                raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "build capture source revision set does not match authority")
        if observed_scopes != expected_scopes:
            raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "capture admission scope set does not match frozen build request")
        expected_observations = {(item.identity_id, item.identity_hash) for item in request.selected_observation_mappings}
        expected_labels = {(item.identity_id, item.identity_hash) for item in request.selected_label_mappings}
        if observed_observation_mappings != expected_observations or observed_label_mappings != expected_labels:
            raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "capture selected mapping memberships do not match frozen build request")
        expected_targets = {(item.horizon_trading_days, item.projection) for item in request.label_targets}
        if observed_label_targets != expected_targets:
            raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "capture label targets do not match frozen build request")

    @staticmethod
    def _require_base_snapshot_admission(cur: Any, request: FixtureDatasetBuildRequest) -> None:
        base = request.base_snapshot
        if base is None:
            return
        current_snapshot_id = base.snapshot_id
        visited: set[str] = set()
        row: Any | None = None
        while current_snapshot_id is not None:
            if current_snapshot_id in visited:
                raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "base snapshot chain contains a cycle")
            visited.add(current_snapshot_id)
            cur.execute(
                "SELECT pg_advisory_xact_lock_shared(hashtext(%s))",
                (f"advisory_snapshot:{current_snapshot_id}",),
            )
            cur.execute(
                """
                SELECT s.snapshot_id, s.snapshot_content_hash, s.manifest_sha256, s.policy_compatibility_hash,
                       s.base_snapshot_id
                  FROM app.advisory_dataset_snapshot s
                 WHERE s.snapshot_id = %s AND s.snapshot_state = 'SEALED'
                 FOR KEY SHARE
                """,
                (current_snapshot_id,),
            )
            candidate = cur.fetchone()
            if candidate is None:
                raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "base snapshot chain is incomplete or not sealed")
            if row is None:
                row = candidate
            cur.execute("SELECT 1 FROM app.advisory_dataset_snapshot_invalidation WHERE snapshot_id = %s", (current_snapshot_id,))
            if cur.fetchone() is not None:
                raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "base snapshot chain has an append-only invalidation")
            current_snapshot_id = candidate["base_snapshot_id"]
        if (
            row is None
            or str(row["snapshot_content_hash"]) != base.snapshot_content_hash
            or str(row["manifest_sha256"]) != base.manifest_sha256
            or str(row["policy_compatibility_hash"]) != base.policy_compatibility_hash
        ):
            raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "base snapshot identity is incomplete or does not match a sealed snapshot")

    def _require_active_attempt(self, cur: Any, attempt_id: str, fencing_token: int) -> tuple[DatasetBuildAttempt, DatasetBuild]:
        cur.execute(
            "SELECT a.*, clock_timestamp() AS database_now FROM app.advisory_dataset_build_attempt a WHERE a.attempt_id = %s FOR UPDATE",
            (attempt_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "attempt does not exist")
        attempt = self._attempt_from_row(dict(row))
        build = self._build_from_row(self._select_build_locked(cur, attempt.build_id))
        if attempt.state is not AttemptState.ACTIVE or build.current_attempt_id != attempt_id or build.current_fencing_token != fencing_token or attempt.fencing_token != fencing_token:
            raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "attempt is no longer current")
        if attempt.expires_at <= row["database_now"]:
            raise DatasetBuildError(REASON_ATTEMPT_LEASE_EXPIRED, "attempt lease has expired")
        return attempt, build

    def _require_active_attempt_allow_expired(self, cur: Any, attempt_id: str, fencing_token: int) -> tuple[DatasetBuildAttempt, DatasetBuild]:
        cur.execute(
            "SELECT * FROM app.advisory_dataset_build_attempt WHERE attempt_id = %s FOR UPDATE",
            (attempt_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise DatasetBuildError(REASON_BUILD_TRANSITION_INVALID, "attempt does not exist")
        attempt = self._attempt_from_row(dict(row))
        build = self._build_from_row(self._select_build_locked(cur, attempt.build_id))
        if attempt.state is not AttemptState.ACTIVE or build.current_attempt_id != attempt_id or build.current_fencing_token != fencing_token or attempt.fencing_token != fencing_token:
            raise DatasetBuildError(REASON_ATTEMPT_FENCING_STALE, "attempt is no longer current")
        return attempt, build

    @staticmethod
    def _select_build_locked(cur: Any, build_id: str) -> dict[str, Any]:
        cur.execute("SELECT * FROM app.advisory_dataset_build WHERE build_id = %s FOR UPDATE", (build_id,))
        row = cur.fetchone()
        if row is None:
            raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "dataset build does not exist")
        return dict(row)

    def _files_for_attempt(self, cur: Any, attempt_id: str) -> tuple[DatasetAttemptFile, ...]:
        cur.execute("SELECT * FROM app.advisory_dataset_attempt_file WHERE attempt_id = %s ORDER BY logical_path FOR KEY SHARE", (attempt_id,))
        return tuple(self._file_from_row(dict(row)) for row in cur.fetchall())

    @staticmethod
    def _build_from_row(row: Mapping[str, Any]) -> DatasetBuild:
        request = FixtureDatasetBuildRequest.model_validate(canonicalize(dict(row["build_request_payload_jsonb"])))
        return DatasetBuild(
            build_id=str(row["build_id"]), request=request, logical_build_key_sha256=str(row["logical_build_key_sha256"]),
            build_generation=int(row["build_generation"]),
            predecessor_build_id=str(row["predecessor_build_id"]) if row["predecessor_build_id"] else None,
            lifecycle=BuildLifecycle(str(row["lifecycle_status"])),
            checkpoint=BuildCheckpoint(str(row["checkpoint"])), current_fencing_token=int(row["current_fencing_token"]),
            current_attempt_id=str(row["current_attempt_id"]) if row["current_attempt_id"] else None,
            row_version=int(row["row_version"]), materialized_attempt_id=(str(row["materialized_attempt_id"]) if row["materialized_attempt_id"] else None),
            materialize_receipt_hash=(str(row["materialize_receipt_hash"]) if row["materialize_receipt_hash"] else None),
            materialized_file_set_hash=(str(row["materialized_file_set_hash"]) if row["materialized_file_set_hash"] else None),
            verified_attempt_id=(str(row["verified_attempt_id"]) if row["verified_attempt_id"] else None),
            verify_receipt_hash=(str(row["verify_receipt_hash"]) if row["verify_receipt_hash"] else None),
            verified_file_set_hash=(str(row["verified_file_set_hash"]) if row["verified_file_set_hash"] else None),
            verification_contract_version=(str(row["verification_contract_version"]) if row["verification_contract_version"] else None),
            promoted_attempt_id=(str(row["promoted_attempt_id"]) if row["promoted_attempt_id"] else None),
            promotion_receipt_hash=(str(row["promotion_receipt_hash"]) if row["promotion_receipt_hash"] else None),
            promoted_manifest_hash=(str(row["promoted_manifest_hash"]) if row["promoted_manifest_hash"] else None),
            sealed_attempt_id=(str(row["sealed_attempt_id"]) if row["sealed_attempt_id"] else None),
            seal_receipt_hash=(str(row["seal_receipt_hash"]) if row["seal_receipt_hash"] else None),
            sealed_snapshot_id=(str(row["sealed_snapshot_id"]) if row["sealed_snapshot_id"] else None),
            termination_receipt_hash=(str(row["termination_receipt_hash"]) if row["termination_receipt_hash"] else None),
            terminal_reason_code=(str(row["terminal_reason_code"]) if row["terminal_reason_code"] else None),
            created_at=row["created_at"], updated_at=row["updated_at"],
        )

    @staticmethod
    def _attempt_from_row(row: Mapping[str, Any]) -> DatasetBuildAttempt:
        return DatasetBuildAttempt(
            attempt_id=str(row["attempt_id"]), build_id=str(row["build_id"]), attempt_no=int(row["attempt_no"]),
            operation=AttemptOperation(str(row["operation"])), state=AttemptState(str(row["attempt_state"])),
            lease_owner_id=str(row["lease_owner_id"]), lease_token=str(row["lease_token"]), fencing_token=int(row["fencing_token"]),
            expected_build_row_version=int(row["expected_build_row_version"]), expected_checkpoint=BuildCheckpoint(str(row["expected_checkpoint"])),
            acquired_at=row["acquired_at"], heartbeat_at=row["heartbeat_at"], expires_at=row["expires_at"],
            predecessor_attempt_id=(str(row["predecessor_attempt_id"]) if row["predecessor_attempt_id"] else None),
            operation_request_hash=str(row["operation_request_hash"]),
            finished_at=row["finished_at"], error_code=str(row["error_code"]) if row["error_code"] else None,
            error_hash=str(row["error_hash"]) if row["error_hash"] else None,
        )

    @staticmethod
    def _file_from_row(row: Mapping[str, Any]) -> DatasetAttemptFile:
        return DatasetAttemptFile(
            attempt_id=str(row["attempt_id"]), fencing_token=int(row["fencing_token"]), logical_path=str(row["logical_path"]),
            logical_role=str(row["logical_role"]), partition_key_hash=str(row["partition_key_hash"]), ordinal=int(row["ordinal"]),
            staging_uri=str(row["staging_uri"]), sha256=str(row["sha256"]), size_bytes=int(row["size_bytes"]),
            row_count=int(row["row_count"]), schema_fingerprint=str(row["schema_fingerprint"]),
            partition_content_hash=str(row["partition_content_hash"]), compression=str(row["compression"]), writer_version=str(row["writer_version"]),
        )

    def _append_event(self, cur: Any, *, build: DatasetBuild, attempt: DatasetBuildAttempt | None, event_type: DatasetBuildEventType, actor: str, payload: dict[str, object], reasons: tuple[str, ...] = ()) -> None:
        cur.execute("SELECT clock_timestamp() AS database_now")
        event = DatasetBuildEvent(
            build_id=build.build_id, attempt_id=attempt.attempt_id if attempt else None,
            fencing_token=attempt.fencing_token if attempt else None, event_type=event_type,
            event_at=cur.fetchone()["database_now"], actor=actor,
            payload_hash=_build_event_payload_hash(payload, attempt),
            reason_codes=tuple(sorted(set(reasons))),
        )
        cur.execute(
            """
            INSERT INTO app.advisory_dataset_build_event (
                event_id, build_id, attempt_id, fencing_token, event_type, event_at, actor, payload_hash, reason_codes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (build_id, event_type, payload_hash) DO NOTHING
            RETURNING event_id
            """,
            (event.event_id, event.build_id, event.attempt_id, event.fencing_token, event.event_type.value,
             event.event_at, event.actor, event.payload_hash, psycopg2.extras.Json(list(event.reason_codes))),
        )
        if cur.fetchone() is None:
            cur.execute(
                """
                SELECT attempt_id, fencing_token, actor, reason_codes
                  FROM app.advisory_dataset_build_event
                 WHERE build_id = %s AND event_type = %s AND payload_hash = %s
                 FOR KEY SHARE
                """,
                (event.build_id, event.event_type.value, event.payload_hash),
            )
            existing = cur.fetchone()
            expected = {
                "attempt_id": event.attempt_id,
                "fencing_token": event.fencing_token,
                "actor": event.actor,
                "reason_codes": list(event.reason_codes),
            }
            if existing is None or canonicalize(dict(existing)) != canonicalize(expected):
                raise DatasetBuildError(REASON_BUILD_REQUEST_CONFLICT, "build event idempotency key has different persisted content")

    @staticmethod
    def _raise_integrity(error: psycopg2.IntegrityError, operation: str) -> None:
        constraint = getattr(error.diag, "constraint_name", None)
        logger.error(
            "advisory_phase1c3 database invariant violation operation=%s pgcode=%s constraint=%s",
            operation,
            getattr(error, "pgcode", None),
            constraint or "unknown",
        )
        raise DatasetBuildError(REASON_DATABASE_INVARIANT_VIOLATION, f"{operation} violated database invariant {constraint or 'unknown'}") from error
