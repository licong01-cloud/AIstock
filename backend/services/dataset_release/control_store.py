"""Durable SQLite repository for monthly dataset-release control state.

Initialization is an explicit operator action.  Constructing ``ControlStore``
never creates a directory, database, table, or migration.  All mutable control
records are committed through ``BEGIN IMMEDIATE`` transactions while large
payloads live in the sibling immutable CAS.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
import uuid
import ctypes
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from .canonical import ensure_sha256, normalize_root_relative_path
from .contracts import (
    CandidateIdentity,
    PitProvenanceState,
    ProducerProvenanceState,
    Scope,
)
from .errors import IdentityConflictError

# Python 3.10 compatibility: ``datetime.UTC`` exists only in 3.11+ and is the
# identical singleton ``datetime.timezone.utc`` (same tzinfo object, offset,
# and isoformat output).
UTC = timezone.utc


CONTROL_SCHEMA_VERSION = 1
CONTROL_SCHEMA_NAME = "dataset_release_control_v1"
IDENTITY_SCHEMA = "dataset_release_control_store_identity_v1"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
BUILD_CANDIDATE_REGISTRATION_NAMESPACE = uuid.UUID("4ec8a5d0-bbe7-5d8d-91a9-acde4b7228e4")


class ControlStoreError(RuntimeError):
    """Base class for durable control repository errors."""

    code = "DATASET_RELEASE_CONTROL_STORE_ERROR"
    retryable = False


class ControlStoreNotInitialized(ControlStoreError):
    """Runtime access was attempted before explicit initialization."""

    code = "DATASET_RELEASE_CONTROL_STORE_NOT_INITIALIZED"


class ControlStoreSchemaMismatch(ControlStoreError):
    """On-disk schema or root identity is incompatible with this code."""

    code = "DATASET_RELEASE_CONTROL_STORE_SCHEMA_MISMATCH"


class IdempotencyConflict(ControlStoreError):
    """The same idempotency key was rebound to a different canonical request."""

    code = "DATASET_RELEASE_IDEMPOTENCY_CONFLICT"


class StateConflict(ControlStoreError):
    """A compare-and-swap state mutation did not match exactly one row."""

    code = "DATASET_RELEASE_STATE_CONFLICT"


@dataclass(frozen=True, slots=True)
class CandidateRegistrationSpec:
    allowlisted_root_id: str
    volume_serial: str
    root_relative_path: str
    profile: str
    scope: str
    cutoff: date
    lineage_anchor: str
    artifact_root: str
    producer_provenance_state: str
    producer_provenance_digest_or_sentinel: str
    pit_provenance_state: str
    pit_provenance_digest_or_sentinel: str
    legacy_receipt_ref: str | None = None
    state: str = "CATALOGED"
    registration_id: str | None = None
    candidate_identity: str | None = None
    last_attested_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class SourceSnapshotCatalogSpec:
    observation_id: str
    profile: str
    scope: str
    cutoff: date
    source_content_root: str
    source_provenance_root: str
    stable_source_provenance_root: str
    source_content_manifest_ref: str
    source_reuse_manifest_ref: str
    source_refresh_audit_ref: str
    source_provenance_ref: str
    pit_snapshot_digest: str
    pit_snapshot_ref: str
    observed_at: datetime


NONTERMINAL_RUN_STATES = (
    "QUEUED",
    "WAITING_RESOURCE",
    "REATTESTING",
    "FINALIZING_ATTESTATION",
    "EXECUTING",
    "WAITING_PERFORMANCE_REGRESSION",
    "VALIDATING",
    "PREPARING_PUBLISH",
    "PUBLISHING",
    "WAITING_PUBLISH_RECOVERY",
    "FAILED_RETRYABLE",
    "CANCEL_REQUESTED",
    "WAITING_ORPHAN_QUIESCENCE",
)


_DDL = f"""
CREATE TABLE schema_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    applied_at TEXT NOT NULL,
    code_compat_min INTEGER NOT NULL,
    code_compat_max INTEGER NOT NULL,
    ddl_sha256 TEXT NOT NULL
);

CREATE TABLE idempotency_keys (
    principal TEXT NOT NULL,
    route TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    submission_id TEXT NOT NULL,
    response_ref TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (principal, route, idempotency_key)
);

CREATE TABLE command_idempotency_keys (
    principal TEXT NOT NULL,
    route TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    command_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (principal, route, idempotency_key)
);

CREATE TABLE reconcile_leases (
    profile TEXT PRIMARY KEY,
    fence_counter INTEGER NOT NULL DEFAULT 0 CHECK (fence_counter >= 0),
    state TEXT NOT NULL CHECK (state IN ('FREE','ACTIVE')),
    owner_identity TEXT,
    cycle_id TEXT,
    acquired_at TEXT,
    expires_at TEXT,
    updated_at TEXT NOT NULL,
    CHECK (
        (state='FREE' AND owner_identity IS NULL AND cycle_id IS NULL
         AND acquired_at IS NULL AND expires_at IS NULL)
        OR
        (state='ACTIVE' AND owner_identity IS NOT NULL AND cycle_id IS NOT NULL
         AND acquired_at IS NOT NULL AND expires_at IS NOT NULL)
    )
);

CREATE TABLE submissions (
    submission_id TEXT PRIMARY KEY,
    logical_request_key TEXT NOT NULL,
    request_ref TEXT NOT NULL,
    actor TEXT NOT NULL,
    state TEXT NOT NULL,
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    intent_id TEXT,
    run_id TEXT,
    resolution_attempt_id TEXT,
    terminal_receipt_ref TEXT,
    next_retry_at TEXT,
    deadline_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX submissions_logical_state ON submissions(logical_request_key, state, created_at);

CREATE TABLE resolution_attempts (
    resolution_attempt_id TEXT PRIMARY KEY,
    submission_id TEXT NOT NULL,
    logical_request_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    state TEXT NOT NULL,
    owner TEXT NOT NULL,
    fence INTEGER NOT NULL CHECK (fence > 0),
    source_content_root TEXT,
    source_provenance_root TEXT,
    pit_snapshot_digest TEXT,
    source_probe_ordinal INTEGER CHECK (source_probe_ordinal > 0),
    source_probe_key TEXT,
    source_probe_ref TEXT,
    source_probe_valid_until TEXT,
    error_ref TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(submission_id, ordinal)
);
CREATE UNIQUE INDEX one_active_resolution_per_logical_request
ON resolution_attempts(logical_request_key)
WHERE state IN ('CLAIMED','RUNNING','ORPHAN_HOLD');

CREATE TABLE intents (
    intent_id TEXT PRIMARY KEY,
    logical_request_key TEXT NOT NULL,
    resolved_intent_key TEXT NOT NULL UNIQUE,
    source_content_root TEXT NOT NULL,
    source_provenance_root TEXT NOT NULL,
    pit_snapshot_digest TEXT NOT NULL,
    supersedes_intent_id TEXT,
    source_revision_reason TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX intents_logical_created ON intents(logical_request_key, created_at);

CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL,
    run_generation_digest TEXT NOT NULL,
    operation_kind TEXT NOT NULL,
    lineage_root_run_id TEXT NOT NULL,
    resume_ordinal INTEGER NOT NULL DEFAULT 0 CHECK (resume_ordinal >= 0),
    state TEXT NOT NULL,
    outcome TEXT,
    plan_ref TEXT,
    terminal_receipt_ref TEXT,
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    active_attempt_id TEXT,
    resumes_run_id TEXT,
    publish_nonce TEXT,
    candidate_identity TEXT,
    artifact_root TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(intent_id, run_generation_digest)
);
CREATE UNIQUE INDEX one_nonterminal_run_per_lineage
ON runs(lineage_root_run_id)
WHERE state IN ({",".join(repr(value) for value in NONTERMINAL_RUN_STATES)});
CREATE INDEX runs_created ON runs(created_at DESC, run_id DESC);

CREATE TABLE resume_lineages (
    lineage_root_run_id TEXT PRIMARY KEY,
    latest_run_id TEXT NOT NULL,
    next_ordinal INTEGER NOT NULL CHECK (next_ordinal > 0),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0)
);

CREATE TABLE attempts (
    attempt_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    attempt_kind TEXT NOT NULL,
    state TEXT NOT NULL,
    owner TEXT NOT NULL,
    attempt_fence INTEGER NOT NULL CHECK (attempt_fence > 0),
    host_fence INTEGER,
    release_fence INTEGER,
    staging_ref TEXT,
    error_ref TEXT,
    owner_pid INTEGER,
    owner_create_time TEXT,
    worker_instance_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(run_id, ordinal)
);

CREATE TABLE events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    submission_id TEXT,
    resolution_attempt_id TEXT,
    run_id TEXT,
    attempt_id TEXT,
    type TEXT NOT NULL,
    payload_ref TEXT,
    created_at TEXT NOT NULL,
    CHECK (
        submission_id IS NOT NULL OR resolution_attempt_id IS NOT NULL
        OR run_id IS NOT NULL OR attempt_id IS NOT NULL
    )
);
CREATE INDEX events_submission ON events(submission_id, event_id);
CREATE INDEX events_run ON events(run_id, event_id);

CREATE TABLE leases (
    resource_key TEXT PRIMARY KEY,
    fence_counter INTEGER NOT NULL DEFAULT 0 CHECK (fence_counter >= 0),
    state TEXT NOT NULL,
    attempt_kind TEXT,
    attempt_id TEXT,
    run_id TEXT,
    owner_identity TEXT,
    host TEXT,
    owner_pid INTEGER,
    owner_create_time TEXT,
    worker_instance_id TEXT,
    code_sha TEXT,
    capability_digest TEXT,
    attempt_fence INTEGER,
    acquired_at TEXT,
    heartbeat_at TEXT,
    expires_at TEXT,
    requested_ram INTEGER,
    db_connections INTEGER,
    io_class TEXT,
    hybrid_wsl INTEGER NOT NULL DEFAULT 0 CHECK (hybrid_wsl IN (0,1))
);

CREATE TABLE commands (
    command_id TEXT PRIMARY KEY,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    submission_id TEXT,
    run_id TEXT,
    type TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL,
    applied_at TEXT
);
CREATE UNIQUE INDEX one_active_command_per_target
ON commands(target_type, target_id, type)
WHERE state IN ('QUEUED','PENDING','CLAIMED');

CREATE TABLE run_log_executions (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    attempt_fence INTEGER NOT NULL CHECK (attempt_fence > 0),
    execution_id TEXT NOT NULL,
    relative_log_root TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(attempt_id, attempt_fence, execution_id),
    UNIQUE(run_id, relative_log_root)
);
CREATE INDEX run_log_executions_run
ON run_log_executions(run_id, log_id);

CREATE TABLE artifacts (
    artifact_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    cas_ref TEXT NOT NULL,
    producer_attempt_id TEXT,
    committed INTEGER NOT NULL DEFAULT 0 CHECK (committed IN (0,1)),
    UNIQUE(kind, sha256)
);

CREATE TABLE attestations (
    attestation_id TEXT PRIMARY KEY,
    attestation_key TEXT NOT NULL UNIQUE,
    attestation_target_key TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_digest TEXT NOT NULL,
    candidate_identity TEXT,
    producer_provenance_state TEXT NOT NULL,
    producer_provenance_digest_or_sentinel TEXT NOT NULL,
    candidate_artifact_root TEXT NOT NULL,
    current_source_content_root TEXT NOT NULL,
    source_probe_key TEXT NOT NULL,
    source_probe_ref TEXT NOT NULL,
    pit_snapshot_digest TEXT NOT NULL,
    semantic_profile_digest TEXT NOT NULL,
    validation_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    valid_until TEXT NOT NULL,
    equivalence_mode TEXT NOT NULL,
    outcome TEXT NOT NULL,
    receipt_ref TEXT NOT NULL,
    committed INTEGER NOT NULL DEFAULT 1 CHECK (committed IN (0,1))
);
CREATE INDEX attestations_target_observed
ON attestations(attestation_target_key, observed_at DESC, attestation_id);

CREATE TABLE candidate_registrations (
    registration_id TEXT PRIMARY KEY,
    allowlisted_root_id TEXT NOT NULL,
    volume_serial TEXT NOT NULL,
    root_relative_path TEXT NOT NULL,
    profile TEXT NOT NULL,
    scope TEXT NOT NULL,
    cutoff TEXT NOT NULL,
    lineage_anchor TEXT NOT NULL,
    candidate_identity TEXT NOT NULL UNIQUE,
    artifact_root TEXT NOT NULL,
    producer_provenance_state TEXT NOT NULL,
    producer_provenance_digest_or_sentinel TEXT NOT NULL,
    pit_provenance_state TEXT NOT NULL,
    pit_provenance_digest_or_sentinel TEXT NOT NULL,
    legacy_receipt_ref TEXT,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_attested_at TEXT,
    UNIQUE(allowlisted_root_id, volume_serial, root_relative_path)
);
CREATE INDEX candidate_registrations_latest
ON candidate_registrations(
    profile, scope, cutoff DESC, last_attested_at DESC, artifact_root DESC,
    candidate_identity
);

CREATE TABLE source_snapshot_catalog (
    observation_id TEXT PRIMARY KEY,
    profile TEXT NOT NULL,
    scope TEXT NOT NULL,
    cutoff TEXT NOT NULL,
    source_content_root TEXT NOT NULL,
    source_provenance_root TEXT NOT NULL,
    stable_source_provenance_root TEXT NOT NULL,
    source_content_manifest_ref TEXT NOT NULL,
    source_reuse_manifest_ref TEXT NOT NULL,
    source_refresh_audit_ref TEXT NOT NULL,
    source_provenance_ref TEXT NOT NULL,
    pit_snapshot_digest TEXT NOT NULL,
    pit_snapshot_ref TEXT NOT NULL,
    observed_at TEXT NOT NULL
);
CREATE INDEX source_snapshot_catalog_latest
ON source_snapshot_catalog(profile,scope,cutoff DESC,observed_at DESC,observation_id);

CREATE TABLE releases (
    release_digest TEXT PRIMARY KEY,
    release_id TEXT NOT NULL UNIQUE,
    candidate_identity TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL UNIQUE,
    profile TEXT NOT NULL,
    scope TEXT NOT NULL,
    cutoff TEXT NOT NULL,
    artifact_root TEXT NOT NULL,
    pit_snapshot_digest TEXT NOT NULL,
    final_path_identity TEXT NOT NULL,
    marker_ref TEXT NOT NULL,
    attestation_id TEXT NOT NULL,
    state TEXT NOT NULL
);

CREATE TABLE publish_records (
    release_id TEXT PRIMARY KEY,
    release_digest TEXT NOT NULL,
    run_id TEXT NOT NULL UNIQUE,
    attempt_id TEXT NOT NULL,
    attempt_fence INTEGER NOT NULL,
    host_fence INTEGER NOT NULL,
    release_fence INTEGER NOT NULL,
    publish_nonce TEXT NOT NULL UNIQUE,
    published_by_attempt_id TEXT NOT NULL,
    published_by_fence INTEGER NOT NULL,
    finalized_by_attempt_id TEXT,
    finalized_by_fence INTEGER,
    state TEXT NOT NULL,
    manifest_root TEXT NOT NULL,
    artifact_root TEXT NOT NULL,
    pit_snapshot_digest TEXT NOT NULL,
    build_receipt_ref TEXT NOT NULL,
    attestation_key TEXT NOT NULL,
    attestation_ref TEXT NOT NULL,
    source_probe_key TEXT NOT NULL,
    source_probe_ref TEXT NOT NULL,
    final_path_identity TEXT NOT NULL,
    final_path TEXT NOT NULL,
    marker_ref TEXT,
    registration_id TEXT NOT NULL,
    allowlisted_root_id TEXT NOT NULL,
    volume_serial TEXT NOT NULL,
    root_relative_path TEXT NOT NULL,
    lineage_anchor TEXT NOT NULL,
    candidate_identity TEXT NOT NULL,
    producer_provenance_state TEXT NOT NULL,
    producer_provenance_digest_or_sentinel TEXT NOT NULL,
    pit_provenance_state TEXT NOT NULL,
    profile TEXT NOT NULL,
    scope TEXT NOT NULL,
    cutoff TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class ControlStore:
    """Checked handle to an already initialized control repository."""

    def __init__(
        self,
        root: str | Path,
        *,
        expected_version: int = CONTROL_SCHEMA_VERSION,
        read_only: bool = False,
    ) -> None:
        self.root = _validate_runtime_root(Path(root))
        self.read_only = bool(read_only)
        self.db_path = self.root / "control.sqlite3"
        self.identity_path = self.root / "control_store_identity.json"
        if not self.db_path.is_file() or not self.identity_path.is_file():
            raise ControlStoreNotInitialized("control store was not explicitly initialized")
        _assert_plain(self.db_path)
        _assert_plain(self.identity_path)
        self._identity = self._read_and_validate_identity(expected_version)
        self._validate_schema(expected_version)

    @classmethod
    def initialize(
        cls,
        root: str | Path,
        *,
        expected_version: int = CONTROL_SCHEMA_VERSION,
        fault_injector: Callable[[str], None] | None = None,
    ) -> "ControlStore":
        """Explicitly create a new store; never called by backend/Worker startup."""

        if expected_version != CONTROL_SCHEMA_VERSION:
            raise ControlStoreSchemaMismatch(
                f"initializer only knows schema {CONTROL_SCHEMA_VERSION}, requested {expected_version}"
            )
        fault = fault_injector or (lambda _point: None)
        target = _validate_init_target(Path(root))
        identity_path = target / "control_store_identity.json"
        db_path = target / "control.sqlite3"
        if identity_path.exists() and db_path.exists():
            # Explicit init is idempotent only for a fully valid existing store.
            return cls(target, expected_version=expected_version)
        if identity_path.exists() and not db_path.exists():
            raise ControlStoreSchemaMismatch("control store identity exists without its bound database")

        _prepare_initial_directories(target, database_exists=db_path.exists())
        ddl_digest = hashlib.sha256(_DDL.encode("utf-8")).hexdigest()
        if db_path.exists():
            # A crash after the SQLite commit but before the identity create is
            # recoverable only when the complete catalog is byte-for-byte the
            # schema this binary would create.  Any ambiguity fails closed.
            applied_at = _validate_identity_recovery_database(
                db_path,
                expected_version=expected_version,
                ddl_digest=ddl_digest,
            )
        else:
            applied_at = utc_now()
            connection = sqlite3.connect(db_path, isolation_level=None, timeout=30)
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                connection.execute("PRAGMA foreign_keys=ON")
                # ``executescript`` commits a transaction opened before it.
                # Put BEGIN inside the script so all DDL plus metadata below
                # remain one rollback-safe SQLite transaction.
                connection.executescript("BEGIN IMMEDIATE;\n" + _DDL)
                connection.execute(
                    "INSERT INTO schema_metadata VALUES (1,?,?,?,?,?,?)",
                    (
                        CONTROL_SCHEMA_NAME,
                        expected_version,
                        applied_at,
                        expected_version,
                        expected_version,
                        ddl_digest,
                    ),
                )
                connection.execute(f"PRAGMA user_version={expected_version}")
                connection.commit()
                connection.execute("PRAGMA wal_checkpoint(FULL)")
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

        fault("after_database_commit_before_identity")

        identity = {
            "schema_version": IDENTITY_SCHEMA,
            "control_store_id": f"dsc_{uuid.uuid4().hex}",
            "control_schema_name": CONTROL_SCHEMA_NAME,
            "control_schema_version": expected_version,
            "normalized_root": _normalized_path(target),
            "volume_identity": volume_identity(target),
            "ddl_sha256": ddl_digest,
            "created_at": applied_at,
        }
        _atomic_json_no_replace(identity_path, identity)
        return cls(target, expected_version=expected_version)

    @property
    def identity(self) -> Mapping[str, Any]:
        return dict(self._identity)

    def register_candidate(
        self,
        spec: CandidateRegistrationSpec,
    ) -> dict[str, Any]:
        """Bind one exact catalog path to one immutable candidate identity."""

        with self.transaction() as connection:
            return self.register_candidate_in_transaction(connection, spec)

    def register_candidate_in_transaction(
        self,
        connection: sqlite3.Connection,
        spec: CandidateRegistrationSpec,
        *,
        stamp: str | None = None,
    ) -> dict[str, Any]:
        """Register a candidate inside the caller's existing atomic transaction."""

        normalized_path = normalize_root_relative_path(spec.root_relative_path)
        allowlisted_root_id = _nonempty(spec.allowlisted_root_id, "allowlisted_root_id")
        volume_serial = _nonempty(spec.volume_serial, "volume_serial")
        profile = _nonempty(spec.profile, "profile")
        state = _candidate_registration_state(spec.state)
        try:
            scope = Scope(spec.scope)
            producer_state = ProducerProvenanceState(spec.producer_provenance_state)
            pit_state = PitProvenanceState(spec.pit_provenance_state)
        except ValueError as exc:
            raise StateConflict("candidate registration enum value is invalid") from exc
        cutoff = spec.cutoff
        if not isinstance(cutoff, date) or isinstance(cutoff, datetime):
            raise StateConflict("candidate registration cutoff must be a date")
        observed_attested = (
            _aware_utc(spec.last_attested_at, field="last_attested_at") if spec.last_attested_at is not None else None
        )
        legacy_receipt_ref = (
            _nonempty(spec.legacy_receipt_ref, "legacy_receipt_ref") if spec.legacy_receipt_ref is not None else None
        )
        if spec.lineage_anchor.startswith("LEGACY_RECEIPT:") and legacy_receipt_ref is None:
            raise StateConflict("legacy candidate registration requires legacy_receipt_ref")
        if spec.lineage_anchor.startswith("BUILD_RELEASE_DIGEST:") and legacy_receipt_ref is not None:
            raise StateConflict("build candidate registration cannot carry legacy_receipt_ref")

        existing = connection.execute(
            """
            SELECT * FROM candidate_registrations
            WHERE allowlisted_root_id=? AND volume_serial=? AND root_relative_path=?
            """,
            (allowlisted_root_id, volume_serial, normalized_path),
        ).fetchone()
        registration_id = _candidate_registration_id(
            spec.registration_id,
            existing=existing,
        )
        try:
            computed_identity = CandidateIdentity(
                registration_uuid=registration_id,
                allowlisted_root_id=allowlisted_root_id,
                volume_serial=volume_serial,
                root_relative_path=normalized_path,
                profile=profile,
                scope=scope,
                cutoff=cutoff,
                lineage_anchor=spec.lineage_anchor,
                pit_provenance_state=pit_state,
                pit_provenance_digest_or_sentinel=(spec.pit_provenance_digest_or_sentinel),
                artifact_root=spec.artifact_root,
                producer_provenance_state=producer_state,
                producer_provenance_digest_or_sentinel=(spec.producer_provenance_digest_or_sentinel),
            ).key
        except IdentityConflictError as exc:
            raise StateConflict(f"candidate registration identity is invalid: {exc}") from exc
        if spec.candidate_identity is not None and spec.candidate_identity != computed_identity:
            raise StateConflict("candidate registration supplied identity does not match fields")
        immutable = {
            "registration_id": registration_id,
            "allowlisted_root_id": allowlisted_root_id,
            "volume_serial": volume_serial,
            "root_relative_path": normalized_path,
            "profile": profile,
            "scope": scope.value,
            "cutoff": cutoff.isoformat(),
            "lineage_anchor": spec.lineage_anchor,
            "candidate_identity": computed_identity,
            "artifact_root": spec.artifact_root,
            "producer_provenance_state": producer_state.value,
            "producer_provenance_digest_or_sentinel": (spec.producer_provenance_digest_or_sentinel),
            "pit_provenance_state": pit_state.value,
            "pit_provenance_digest_or_sentinel": (spec.pit_provenance_digest_or_sentinel),
            "legacy_receipt_ref": legacy_receipt_ref,
        }
        if existing is not None:
            for field, value in immutable.items():
                if existing[field] != value:
                    raise StateConflict(f"candidate path is bound to different immutable {field}")
            return dict(existing)
        if connection.execute(
            "SELECT 1 FROM candidate_registrations WHERE registration_id=? OR candidate_identity=?",
            (registration_id, computed_identity),
        ).fetchone():
            raise StateConflict("candidate registration id or identity is already bound")
        observed_stamp = stamp or utc_now()
        connection.execute(
            """
            INSERT INTO candidate_registrations(
                registration_id,allowlisted_root_id,volume_serial,root_relative_path,
                profile,scope,cutoff,lineage_anchor,candidate_identity,artifact_root,
                producer_provenance_state,producer_provenance_digest_or_sentinel,
                pit_provenance_state,pit_provenance_digest_or_sentinel,
                legacy_receipt_ref,state,created_at,updated_at,last_attested_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                *immutable.values(),
                state,
                observed_stamp,
                observed_stamp,
                _iso_datetime(observed_attested),
            ),
        )
        return dict(
            connection.execute(
                "SELECT * FROM candidate_registrations WHERE registration_id=?",
                (registration_id,),
            ).fetchone()
        )

    def mark_candidate_attested(
        self,
        *,
        registration_id: str,
        candidate_identity: str,
        attested_at: datetime,
    ) -> dict[str, Any]:
        observed = _aware_utc(attested_at, field="attested_at")
        stamp = _iso_datetime(observed)
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM candidate_registrations WHERE registration_id=?",
                (_nonempty(registration_id, "registration_id"),),
            ).fetchone()
            if row is None or row["candidate_identity"] != candidate_identity:
                raise StateConflict("candidate attestation identity changed")
            current = row["last_attested_at"]
            if current is not None:
                current_time = _parse_utc_text(current, field="last_attested_at")
                if observed < current_time:
                    raise StateConflict("candidate attestation time cannot regress")
                if observed == current_time and row["state"] == "ATTESTED":
                    return dict(row)
            connection.execute(
                """
                UPDATE candidate_registrations
                SET state='ATTESTED',last_attested_at=?,updated_at=?
                WHERE registration_id=? AND candidate_identity=?
                """,
                (stamp, stamp, registration_id, candidate_identity),
            )
            return dict(
                connection.execute(
                    "SELECT * FROM candidate_registrations WHERE registration_id=?",
                    (registration_id,),
                ).fetchone()
            )

    def list_candidate_registrations(
        self,
        *,
        profile: str | None = None,
        scope: str | None = None,
        state: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("candidate registration list limit must be in 1..100")
        clauses: list[str] = []
        params: list[Any] = []
        for field, value in (("profile", profile), ("scope", scope), ("state", state)):
            if value is not None:
                clauses.append(f"{field}=?")
                params.append(_nonempty(value, field))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self.transaction(immediate=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM candidate_registrations {where}
                ORDER BY cutoff DESC,COALESCE(last_attested_at,created_at) DESC,
                         artifact_root DESC,candidate_identity
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def latest_candidate_registration(
        self,
        *,
        profile: str,
        scope: str,
        state: str | None = None,
    ) -> dict[str, Any] | None:
        clauses = ["profile=?", "scope=?"]
        params: list[Any] = [
            _nonempty(profile, "profile"),
            _nonempty(scope, "scope"),
        ]
        if state is not None:
            clauses.append("state=?")
            params.append(_nonempty(state, "state"))
        with self.transaction(immediate=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM candidate_registrations
                WHERE {" AND ".join(clauses)}
                ORDER BY cutoff DESC,COALESCE(last_attested_at,created_at) DESC,
                         artifact_root DESC,candidate_identity
                LIMIT 2
                """,
                tuple(params),
            ).fetchall()
        if not rows:
            return None
        if len(rows) == 2 and _candidate_latest_rank(rows[0]) == _candidate_latest_rank(rows[1]):
            raise StateConflict("latest candidate registration is ambiguous")
        return dict(rows[0])

    def register_source_snapshot(
        self,
        spec: SourceSnapshotCatalogSpec,
    ) -> dict[str, Any]:
        with self.transaction() as connection:
            return self.register_source_snapshot_in_transaction(connection, spec)

    def register_source_snapshot_in_transaction(
        self,
        connection: sqlite3.Connection,
        spec: SourceSnapshotCatalogSpec,
    ) -> dict[str, Any]:
        """Append one immutable source observation in the caller transaction."""

        try:
            scope = Scope(spec.scope).value
        except ValueError as exc:
            raise StateConflict("source snapshot catalog scope is invalid") from exc
        if not isinstance(spec.cutoff, date) or isinstance(spec.cutoff, datetime):
            raise StateConflict("source snapshot catalog cutoff must be a date")
        immutable = {
            "observation_id": ensure_sha256(spec.observation_id, field="source_snapshot_observation_id"),
            "profile": _nonempty(spec.profile, "profile"),
            "scope": scope,
            "cutoff": spec.cutoff.isoformat(),
            "source_content_root": ensure_sha256(spec.source_content_root, field="source_content_root"),
            "source_provenance_root": ensure_sha256(spec.source_provenance_root, field="source_provenance_root"),
            "stable_source_provenance_root": ensure_sha256(
                spec.stable_source_provenance_root,
                field="stable_source_provenance_root",
            ),
            "source_content_manifest_ref": ensure_sha256(
                spec.source_content_manifest_ref,
                field="source_content_manifest_ref",
            ),
            "source_reuse_manifest_ref": ensure_sha256(
                spec.source_reuse_manifest_ref, field="source_reuse_manifest_ref"
            ),
            "source_refresh_audit_ref": ensure_sha256(spec.source_refresh_audit_ref, field="source_refresh_audit_ref"),
            "source_provenance_ref": ensure_sha256(spec.source_provenance_ref, field="source_provenance_ref"),
            "pit_snapshot_digest": ensure_sha256(spec.pit_snapshot_digest, field="pit_snapshot_digest"),
            "pit_snapshot_ref": ensure_sha256(spec.pit_snapshot_ref, field="pit_snapshot_ref"),
            "observed_at": _iso_datetime(_aware_utc(spec.observed_at, field="observed_at")),
        }
        existing = connection.execute(
            "SELECT * FROM source_snapshot_catalog WHERE observation_id=?",
            (immutable["observation_id"],),
        ).fetchone()
        if existing is not None:
            for field, value in immutable.items():
                if field == "observed_at":
                    continue
                if existing[field] != value:
                    raise StateConflict(f"source snapshot observation drifted immutable {field}")
            return dict(existing)
        connection.execute(
            """
            INSERT INTO source_snapshot_catalog(
                observation_id,profile,scope,cutoff,source_content_root,
                source_provenance_root,stable_source_provenance_root,
                source_content_manifest_ref,source_reuse_manifest_ref,
                source_refresh_audit_ref,source_provenance_ref,pit_snapshot_digest,
                pit_snapshot_ref,observed_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            tuple(immutable.values()),
        )
        return dict(
            connection.execute(
                "SELECT * FROM source_snapshot_catalog WHERE observation_id=?",
                (immutable["observation_id"],),
            ).fetchone()
        )

    def latest_source_snapshot(
        self,
        *,
        profile: str,
        scope: str,
        cutoff_on_or_before: date,
    ) -> dict[str, Any] | None:
        if not isinstance(cutoff_on_or_before, date) or isinstance(cutoff_on_or_before, datetime):
            raise ValueError("source snapshot cutoff must be a date")
        try:
            normalized_scope = Scope(scope).value
        except ValueError as exc:
            raise ValueError("source snapshot scope is invalid") from exc
        with self.transaction(immediate=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM source_snapshot_catalog
                WHERE profile=? AND scope=? AND cutoff<=?
                ORDER BY cutoff DESC,observed_at DESC,observation_id
                LIMIT 2
                """,
                (
                    _nonempty(profile, "profile"),
                    normalized_scope,
                    cutoff_on_or_before.isoformat(),
                ),
            ).fetchall()
        if not rows:
            return None
        if (
            len(rows) == 2
            and rows[0]["cutoff"] == rows[1]["cutoff"]
            and rows[0]["observed_at"] == rows[1]["observed_at"]
        ):
            raise StateConflict("latest source snapshot observation is ambiguous")
        return dict(rows[0])

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        if self.read_only and immediate:
            raise ControlStoreError("read-only control store rejects write transaction")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def submit(
        self,
        *,
        principal: str,
        route: str,
        idempotency_key: str,
        request_hash: str,
        logical_request_key: str,
        request_ref: str,
        actor: str | None = None,
        response_ref: str | None = None,
        initial_event_type: str | None = None,
        submission_id: str | None = None,
    ) -> dict[str, Any]:
        """Persist or replay one submission in a single immediate transaction."""

        principal = _nonempty(principal, "principal")
        route = _nonempty(route, "route")
        idempotency_key = _nonempty(idempotency_key, "idempotency_key")
        request_hash = _nonempty(request_hash, "request_hash")
        now = utc_now()
        with self.transaction() as connection:
            existing = connection.execute(
                """
                SELECT request_hash, submission_id, response_ref
                FROM idempotency_keys
                WHERE principal=? AND route=? AND idempotency_key=?
                """,
                (principal, route, idempotency_key),
            ).fetchone()
            if existing is not None:
                if str(existing["request_hash"]) != request_hash:
                    raise IdempotencyConflict("DATASET_RELEASE_IDEMPOTENCY_CONFLICT: key is bound to another request")
                row = _required_row(
                    connection.execute(
                        "SELECT * FROM submissions WHERE submission_id=?",
                        (existing["submission_id"],),
                    ).fetchone(),
                    "idempotency row references a missing submission",
                )
                result = dict(row)
                result.update({"replayed": True, "response_ref": existing["response_ref"]})
                return result

            submission_id = submission_id or f"dss_{uuid.uuid4().hex}"
            if not re.fullmatch(r"dss_[0-9a-f]{32}", submission_id):
                raise ControlStoreError("submission identity is invalid")
            connection.execute(
                """
                INSERT INTO submissions(
                    submission_id,logical_request_key,request_ref,actor,state,
                    created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (
                    submission_id,
                    _nonempty(logical_request_key, "logical_request_key"),
                    _nonempty(request_ref, "request_ref"),
                    _nonempty(actor or principal, "actor"),
                    "QUEUED_RESOLUTION",
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO idempotency_keys(
                    principal,route,idempotency_key,request_hash,submission_id,response_ref,created_at
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (principal, route, idempotency_key, request_hash, submission_id, response_ref, now),
            )
            append_event(
                connection,
                event_type="SUBMISSION_QUEUED",
                submission_id=submission_id,
                payload_ref=request_ref,
                created_at=now,
            )
            if initial_event_type is not None:
                append_event(
                    connection,
                    event_type=initial_event_type,
                    submission_id=submission_id,
                    payload_ref=request_ref,
                    created_at=now,
                )
            row = _required_row(
                connection.execute("SELECT * FROM submissions WHERE submission_id=?", (submission_id,)).fetchone(),
                "new submission disappeared",
            )
            result = dict(row)
            result.update({"replayed": False, "response_ref": response_ref})
            return result

    def get_submission(self, submission_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM submissions WHERE submission_id=?", (submission_id,))

    def list_submissions(
        self,
        *,
        before_created_at: str | None = None,
        before_submission_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Read one bounded page ordered by ``(created_at, submission_id)`` descending."""

        bounded = max(1, min(int(limit), 201))
        if (before_created_at is None) != (before_submission_id is None):
            raise ValueError("submission cursor requires both created_at and submission_id")
        clauses: list[str] = []
        params: list[Any] = []
        if before_created_at is not None and before_submission_id is not None:
            clauses.append("(created_at < ? OR (created_at = ? AND submission_id < ?))")
            params.extend((before_created_at, before_created_at, before_submission_id))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(bounded)
        return self._many(
            f"SELECT * FROM submissions{where} ORDER BY created_at DESC, submission_id DESC LIMIT ?",
            params,
        )

    def latest_submission(self) -> dict[str, Any] | None:
        rows = self.list_submissions(limit=1)
        return rows[0] if rows else None

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM runs WHERE run_id=?", (run_id,))

    def get_attempt(self, attempt_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,))

    def get_resolution_attempt(self, attempt_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM resolution_attempts WHERE resolution_attempt_id=?", (attempt_id,))

    def get_lease(self, resource_key: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM leases WHERE resource_key=?", (resource_key,))

    def get_publish_record(self, release_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM publish_records WHERE release_id=?", (release_id,))

    def get_release_for_run(self, run_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM releases WHERE run_id=?", (run_id,))

    def get_command(self, command_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM commands WHERE command_id=?", (command_id,))

    def list_runs(
        self,
        *,
        states: Sequence[str] = (),
        before_created_at: str | None = None,
        before_run_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Read one bounded page ordered by ``(created_at, run_id)`` descending."""

        bounded = max(1, min(int(limit), 201))
        clauses: list[str] = []
        params: list[Any] = []
        normalized_states = tuple(dict.fromkeys(str(value).strip() for value in states if str(value).strip()))
        if normalized_states:
            clauses.append(f"state IN ({','.join('?' for _ in normalized_states)})")
            params.extend(normalized_states)
        if (before_created_at is None) != (before_run_id is None):
            raise ValueError("run cursor requires both created_at and run_id")
        if before_created_at is not None and before_run_id is not None:
            clauses.append("(created_at < ? OR (created_at = ? AND run_id < ?))")
            params.extend((before_created_at, before_created_at, before_run_id))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(bounded)
        return self._many(
            f"SELECT * FROM runs{where} ORDER BY created_at DESC, run_id DESC LIMIT ?",
            params,
        )

    def register_run_log_execution(
        self,
        *,
        run_id: str,
        attempt_id: str,
        attempt_fence: int,
        execution_id: str,
    ) -> dict[str, Any]:
        """Freeze the deterministic supervised log root before child launch."""

        if (
            re.fullmatch(r"dsr_[0-9a-f]{32}", str(run_id)) is None
            or re.fullmatch(r"dsa_[0-9a-f]{32}", str(attempt_id)) is None
            or type(attempt_fence) is not int
            or attempt_fence <= 0
            or re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", str(execution_id)) is None
        ):
            raise StateConflict("run log execution identity is invalid")
        relative_log_root = f"attempt_runs/{attempt_id}-{attempt_fence}/{execution_id}/logs"
        stamp = utc_now()
        with self.transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?",
                (attempt_id, run_id),
            ).fetchone()
            if (
                run is None
                or attempt is None
                or run["active_attempt_id"] != attempt_id
                or attempt["state"] not in {"CLAIMED", "RUNNING"}
                or int(attempt["attempt_fence"]) != attempt_fence
            ):
                raise StateConflict("run log execution ownership changed")
            existing = connection.execute(
                """
                SELECT * FROM run_log_executions
                WHERE attempt_id=? AND attempt_fence=? AND execution_id=?
                """,
                (attempt_id, attempt_fence, execution_id),
            ).fetchone()
            if existing is not None:
                expected = {
                    "run_id": run_id,
                    "relative_log_root": relative_log_root,
                }
                if any(existing[field] != value for field, value in expected.items()):
                    raise StateConflict("run log execution identity drifted")
                return dict(existing)
            connection.execute(
                """
                INSERT INTO run_log_executions(
                    run_id,attempt_id,attempt_fence,execution_id,
                    relative_log_root,created_at
                ) VALUES (?,?,?,?,?,?)
                """,
                (
                    run_id,
                    attempt_id,
                    attempt_fence,
                    execution_id,
                    relative_log_root,
                    stamp,
                ),
            )
            return dict(
                connection.execute(
                    """
                    SELECT * FROM run_log_executions
                    WHERE attempt_id=? AND attempt_fence=? AND execution_id=?
                    """,
                    (attempt_id, attempt_fence, execution_id),
                ).fetchone()
            )

    def list_run_log_executions(
        self,
        *,
        run_id: str,
        at_or_after_log_id: int = 1,
        limit: int = 201,
    ) -> list[dict[str, Any]]:
        if at_or_after_log_id <= 0 or not 1 <= limit <= 201:
            raise StateConflict("run log catalog bounds are invalid")
        return self._many(
            """
            SELECT * FROM run_log_executions
            WHERE run_id=? AND log_id>=?
            ORDER BY log_id LIMIT ?
            """,
            (run_id, int(at_or_after_log_id), int(limit)),
        )

    def latest_run(self) -> dict[str, Any] | None:
        rows = self.list_runs(limit=1)
        return rows[0] if rows else None

    def enqueue_command(
        self,
        *,
        target_type: str,
        target_id: str,
        command_type: str,
        principal: str,
        route: str,
        idempotency_key: str,
        request_hash: str,
        actor: str,
    ) -> dict[str, Any]:
        """Persist an operator command without executing work in the caller process."""

        target_type = _nonempty(target_type, "target_type")
        target_id = _nonempty(target_id, "target_id")
        command_type = _nonempty(command_type, "command_type")
        principal = _nonempty(principal, "principal")
        route = _nonempty(route, "route")
        idempotency_key = _nonempty(idempotency_key, "idempotency_key")
        request_hash = _nonempty(request_hash, "request_hash")
        actor = _nonempty(actor, "actor")
        if target_type not in {"submission", "run"}:
            raise ValueError("target_type must be submission or run")
        if command_type not in {"CANCEL_REQUESTED", "RESUME_REQUESTED"}:
            raise ValueError("unsupported command type")
        if target_type == "submission" and command_type != "CANCEL_REQUESTED":
            raise ValueError("submission supports cancellation only")
        now = utc_now()
        with self.transaction() as connection:
            idempotency = connection.execute(
                """
                SELECT request_hash,command_id FROM command_idempotency_keys
                WHERE principal=? AND route=? AND idempotency_key=?
                """,
                (principal, route, idempotency_key),
            ).fetchone()
            if idempotency is not None:
                if str(idempotency["request_hash"]) != request_hash:
                    raise IdempotencyConflict(
                        "DATASET_RELEASE_IDEMPOTENCY_CONFLICT: command key is bound to another request"
                    )
                row = _required_row(
                    connection.execute(
                        "SELECT * FROM commands WHERE command_id=?",
                        (idempotency["command_id"],),
                    ).fetchone(),
                    "command idempotency row references a missing command",
                )
                return _initial_command_response(row, replayed=True)
            target_table = "submissions" if target_type == "submission" else "runs"
            target_column = "submission_id" if target_type == "submission" else "run_id"
            target = connection.execute(
                f"SELECT * FROM {target_table} WHERE {target_column}=?", (target_id,)
            ).fetchone()
            if target is None:
                raise StateConflict(f"{target_type} does not exist")
            existing = connection.execute(
                """
                SELECT * FROM commands
                WHERE target_type=? AND target_id=? AND type=?
                  AND state IN ('QUEUED','PENDING','CLAIMED')
                """,
                (target_type, target_id, command_type),
            ).fetchone()
            if existing is not None:
                if existing["request_hash"] != request_hash:
                    raise StateConflict("active command request identity differs")
                connection.execute(
                    """
                    INSERT INTO command_idempotency_keys(
                        principal,route,idempotency_key,request_hash,command_id,created_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        principal,
                        route,
                        idempotency_key,
                        request_hash,
                        existing["command_id"],
                        now,
                    ),
                )
                return _initial_command_response(existing, replayed=True)
            command_id = f"dsc_{uuid.uuid4().hex}"
            submission_id = target_id if target_type == "submission" else None
            run_id = target_id if target_type == "run" else None
            connection.execute(
                """
                INSERT INTO commands(
                    command_id,target_type,target_id,submission_id,run_id,type,
                    request_hash,state,actor,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    command_id,
                    target_type,
                    target_id,
                    submission_id,
                    run_id,
                    command_type,
                    request_hash,
                    "QUEUED",
                    actor,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO command_idempotency_keys(
                    principal,route,idempotency_key,request_hash,command_id,created_at
                ) VALUES (?,?,?,?,?,?)
                """,
                (principal, route, idempotency_key, request_hash, command_id, now),
            )
            append_event(
                connection,
                event_type=command_type,
                submission_id=submission_id,
                run_id=run_id,
                payload_ref=request_hash,
                created_at=now,
            )
            row = _required_row(
                connection.execute("SELECT * FROM commands WHERE command_id=?", (command_id,)).fetchone(),
                "new command disappeared",
            )
            return _initial_command_response(row, replayed=False)

    def list_events(
        self,
        *,
        submission_id: str | None = None,
        run_id: str | None = None,
        after_event_id: int = 0,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if (submission_id is None) == (run_id is None):
            raise ValueError("exactly one of submission_id or run_id is required")
        bounded = max(1, min(int(limit), 201))
        column, value = ("submission_id", submission_id) if submission_id is not None else ("run_id", run_id)
        return self._many(
            f"SELECT * FROM events WHERE {column}=? AND event_id>? ORDER BY event_id LIMIT ?",
            (value, int(after_event_id), bounded),
        )

    def integrity_check(self) -> dict[str, Any]:
        connection = self._connect()
        try:
            quick = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            journal = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            synchronous = int(connection.execute("PRAGMA synchronous").fetchone()[0])
            foreign_keys = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
        finally:
            connection.close()
        return {
            "ok": quick.lower() == "ok" and journal == "wal" and synchronous == 2 and foreign_keys == 1,
            "quick_check": quick,
            "journal_mode": journal,
            "synchronous": synchronous,
            "foreign_keys": foreign_keys,
        }

    def _one(self, query: str, params: Sequence[Any]) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(query, params).fetchone()
            return dict(row) if row is not None else None
        finally:
            connection.close()

    def _many(self, query: str, params: Sequence[Any]) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            return [dict(row) for row in connection.execute(query, params).fetchall()]
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.is_file():
            raise ControlStoreNotInitialized("control.sqlite3 is missing")
        _assert_plain(self.db_path)
        if self.read_only:
            connection = sqlite3.connect(
                f"file:{self.db_path.as_posix()}?mode=ro&immutable=1",
                uri=True,
                isolation_level=None,
                timeout=30,
            )
        else:
            connection = sqlite3.connect(self.db_path, isolation_level=None, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        if self.read_only:
            connection.execute("PRAGMA query_only=ON")
            return connection
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=FULL")
        journal = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        if journal != "wal":
            connection.close()
            raise ControlStoreSchemaMismatch("control store journal_mode is not WAL")
        return connection

    def _read_and_validate_identity(self, expected_version: int) -> dict[str, Any]:
        try:
            identity = json.loads(self.identity_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ControlStoreSchemaMismatch("control store identity is unreadable") from exc
        expected = {
            "schema_version": IDENTITY_SCHEMA,
            "control_schema_name": CONTROL_SCHEMA_NAME,
            "control_schema_version": expected_version,
            "normalized_root": _normalized_path(self.root),
            "volume_identity": volume_identity(self.root),
            "ddl_sha256": hashlib.sha256(_DDL.encode("utf-8")).hexdigest(),
        }
        if any(identity.get(key) != value for key, value in expected.items()):
            raise ControlStoreSchemaMismatch("control store root/schema identity drifted")
        return identity

    def _validate_schema(self, expected_version: int) -> None:
        connection = self._connect()
        try:
            row = connection.execute("SELECT * FROM schema_metadata WHERE singleton=1").fetchone()
            user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            actual_catalog = _schema_catalog(connection)
            expected_catalog = _expected_schema_catalog()
        except sqlite3.DatabaseError as exc:
            raise ControlStoreSchemaMismatch("control store schema cannot be read") from exc
        finally:
            connection.close()
        if row is None or int(row["version"]) != expected_version or user_version != expected_version:
            raise ControlStoreSchemaMismatch("CONTROL_STORE_SCHEMA_MISMATCH")
        if (
            int(row["code_compat_min"]) > expected_version
            or int(row["code_compat_max"]) < expected_version
            or str(row["ddl_sha256"]) != hashlib.sha256(_DDL.encode("utf-8")).hexdigest()
            or actual_catalog != expected_catalog
        ):
            raise ControlStoreSchemaMismatch("CONTROL_STORE_SCHEMA_MISMATCH")


def append_event(
    connection: sqlite3.Connection,
    *,
    event_type: str,
    submission_id: str | None = None,
    resolution_attempt_id: str | None = None,
    run_id: str | None = None,
    attempt_id: str | None = None,
    payload_ref: str | None = None,
    created_at: str | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO events(
            submission_id,resolution_attempt_id,run_id,attempt_id,type,payload_ref,created_at
        ) VALUES (?,?,?,?,?,?,?)
        """,
        (
            submission_id,
            resolution_attempt_id,
            run_id,
            attempt_id,
            _nonempty(event_type, "event_type"),
            payload_ref,
            created_at or utc_now(),
        ),
    )
    return int(cursor.lastrowid)


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def _initial_command_response(row: Mapping[str, Any], *, replayed: bool) -> dict[str, Any]:
    """Reconstruct the immutable response frozen when a command was queued."""

    return {
        "command_id": row["command_id"],
        "target_type": row["target_type"],
        "target_id": row["target_id"],
        "submission_id": row["submission_id"],
        "run_id": row["run_id"],
        "type": row["type"],
        "request_hash": row["request_hash"],
        "state": "QUEUED",
        "actor": row["actor"],
        "created_at": row["created_at"],
        "applied_at": None,
        "replayed": replayed,
    }


_INITIAL_DIRECTORY_PATHS = (
    Path("cas"),
    Path("cas") / "sha256",
    Path("staging"),
    Path("quarantine"),
    Path("logs"),
    Path("heartbeats"),
    Path("worker_heartbeats"),
)


def _prepare_initial_directories(root: Path, *, database_exists: bool) -> None:
    """Recover only the empty directory prefix created by explicit init."""

    allowed_root_names = {
        "cas",
        "staging",
        "quarantine",
        "logs",
        "heartbeats",
        "worker_heartbeats",
    }
    if database_exists:
        allowed_root_names.update({"control.sqlite3", "control.sqlite3-wal", "control.sqlite3-shm"})
    unexpected = []
    for item in root.iterdir():
        if item.name in allowed_root_names:
            continue
        if item.name.startswith(".control_store_identity.json.") and item.name.endswith(".partial"):
            continue
        unexpected.append(item.name)
    if unexpected:
        raise ControlStoreSchemaMismatch("control init root contains unexpected entries before identity commit")
    if database_exists:
        for name in ("control.sqlite3", "control.sqlite3-wal", "control.sqlite3-shm"):
            path = root / name
            if path.exists():
                _assert_plain(path)

    for relative in _INITIAL_DIRECTORY_PATHS:
        path = root / relative
        path.mkdir(parents=True, exist_ok=True)
        _assert_plain(path)
        if not path.is_dir():
            raise ControlStoreSchemaMismatch("control init directory contract is not a directory")
    # No valid caller can write control artifacts before the identity exists.
    # Non-empty initial directories therefore indicate an ambiguous foreign or
    # partially manipulated root and must not be adopted automatically.
    for relative in (
        Path("cas") / "sha256",
        Path("staging"),
        Path("quarantine"),
        Path("logs"),
        Path("heartbeats"),
        Path("worker_heartbeats"),
    ):
        if any((root / relative).iterdir()):
            raise ControlStoreSchemaMismatch("control init recovery found pre-identity control artifacts")


def _schema_catalog(connection: sqlite3.Connection) -> tuple[tuple[str, str, str, str], ...]:
    rows = connection.execute(
        """
        SELECT type,name,tbl_name,sql FROM sqlite_master
        WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
        ORDER BY type,name,tbl_name
        """
    ).fetchall()
    return tuple(tuple(str(value) for value in row) for row in rows)


def _expected_schema_catalog() -> tuple[tuple[str, str, str, str], ...]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    try:
        connection.executescript(_DDL)
        return _schema_catalog(connection)
    finally:
        connection.close()


def _validate_identity_recovery_database(
    db_path: Path,
    *,
    expected_version: int,
    ddl_digest: str,
) -> str:
    """Validate a DB-only init crash before creating its missing identity."""

    try:
        connection = sqlite3.connect(
            f"file:{db_path.as_posix()}?mode=rw",
            uri=True,
            isolation_level=None,
            timeout=30,
        )
    except sqlite3.Error as exc:
        raise ControlStoreSchemaMismatch("pre-identity control database cannot be opened") from exc
    connection.row_factory = sqlite3.Row
    try:
        quick = str(connection.execute("PRAGMA quick_check").fetchone()[0]).lower()
        journal = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        metadata = connection.execute("SELECT * FROM schema_metadata WHERE singleton=1").fetchone()
        actual_catalog = _schema_catalog(connection)
        expected_catalog = _expected_schema_catalog()
    except sqlite3.DatabaseError as exc:
        connection.close()
        raise ControlStoreSchemaMismatch("pre-identity control database schema cannot be read") from exc
    try:
        valid = not (
            quick != "ok"
            or journal != "wal"
            or user_version != expected_version
            or metadata is None
            or actual_catalog != expected_catalog
            or str(metadata["schema_name"]) != CONTROL_SCHEMA_NAME
            or int(metadata["version"]) != expected_version
            or int(metadata["code_compat_min"]) != expected_version
            or int(metadata["code_compat_max"]) != expected_version
            or str(metadata["ddl_sha256"]) != ddl_digest
        )
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        connection.close()
        raise ControlStoreSchemaMismatch("pre-identity control database does not exactly match this schema")
    business_tables = tuple(
        row[0]
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
              AND name <> 'schema_metadata'
            ORDER BY name
            """
        ).fetchall()
    )
    try:
        if any(int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]) for table in business_tables):
            raise ControlStoreSchemaMismatch("pre-identity control database contains mutable business state")
        applied_at = str(metadata["applied_at"])
        parsed = datetime.fromisoformat(applied_at)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("naive applied_at")
        return applied_at
    except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
        if isinstance(exc, ControlStoreSchemaMismatch):
            raise
        raise ControlStoreSchemaMismatch("pre-identity control database metadata is invalid") from exc
    finally:
        connection.close()


def _validate_init_target(path: Path) -> Path:
    expanded = path.expanduser()
    normalized = str(expanded).replace("\\", "/").lower()
    if not expanded.is_absolute() or normalized.startswith("//") or normalized.startswith("/mnt/"):
        raise ControlStoreError("control root must be an absolute local fixed-volume path")
    existing = expanded
    while not existing.exists() and existing != existing.parent:
        existing = existing.parent
    _assert_existing_chain_no_reparse(existing.resolve(strict=True))
    expanded.mkdir(parents=True, exist_ok=True)
    resolved = expanded.resolve(strict=True)
    _assert_existing_chain_no_reparse(resolved)
    if not resolved.is_dir():
        raise ControlStoreError("control root must be a directory")
    _assert_supported_fixed_volume(resolved)
    return resolved


def _validate_runtime_root(path: Path) -> Path:
    expanded = path.expanduser()
    normalized = str(expanded).replace("\\", "/").lower()
    if not expanded.is_absolute() or normalized.startswith("//") or normalized.startswith("/mnt/"):
        raise ControlStoreNotInitialized("control root must be an absolute local path")
    try:
        resolved = expanded.resolve(strict=True)
    except OSError as exc:
        raise ControlStoreNotInitialized("control root does not exist") from exc
    _assert_existing_chain_no_reparse(resolved)
    _assert_supported_fixed_volume(resolved)
    return resolved


def _assert_existing_chain_no_reparse(path: Path) -> None:
    current = Path(path.anchor)
    if current.exists():
        _assert_plain(current)
    for part in path.parts[1:]:
        current = current / part
        _assert_plain(current)


def _assert_plain(path: Path) -> None:
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise ControlStoreError(f"control root path component is unavailable: {path}") from exc
    if stat.S_ISLNK(info.st_mode) or (int(getattr(info, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise ControlStoreError(f"control root traverses symlink/reparse point: {path}")


def _atomic_json_no_replace(path: Path, payload: Mapping[str, Any]) -> None:
    raw = (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".partial", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            raise ControlStoreError("control store identity already exists")
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _normalized_path(path: Path) -> str:
    return str(path.resolve(strict=True)).replace("\\", "/").casefold()


def volume_identity(path: Path) -> str:
    """Return the fixed-volume identity used to reject root/path migration."""

    resolved = path.resolve(strict=True)
    if os.name != "nt":
        return f"st_dev:{int(resolved.stat().st_dev)}"
    serial, _filesystem = _assert_supported_fixed_volume(resolved)
    return f"windows-volume-serial:{serial:08x}"


def _assert_supported_fixed_volume(path: Path) -> tuple[int, str]:
    """Require the local fixed NTFS/ReFS semantics used by SQLite WAL/CAS."""

    resolved = path.resolve(strict=True)
    if os.name != "nt":
        return int(resolved.stat().st_dev), "POSIX"
    root = Path(resolved.anchor)
    serial = ctypes.c_uint32()
    max_component = ctypes.c_uint32()
    flags = ctypes.c_uint32()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_drive_type = kernel32.GetDriveTypeW
    get_drive_type.argtypes = (ctypes.c_wchar_p,)
    get_drive_type.restype = ctypes.c_uint32
    if int(get_drive_type(str(root))) != 3:  # DRIVE_FIXED
        raise ControlStoreError("control root must be on a local fixed drive")
    get_volume_information = kernel32.GetVolumeInformationW
    get_volume_information.argtypes = (
        ctypes.c_wchar_p,
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_wchar_p,
        ctypes.c_uint32,
    )
    get_volume_information.restype = ctypes.c_int
    filesystem = ctypes.create_unicode_buffer(32)
    if not get_volume_information(
        str(root),
        None,
        0,
        ctypes.byref(serial),
        ctypes.byref(max_component),
        ctypes.byref(flags),
        filesystem,
        len(filesystem),
    ):
        raise ControlStoreError("unable to read control-root volume contract")
    filesystem_name = filesystem.value.upper()
    if filesystem_name not in {"NTFS", "REFS"}:
        raise ControlStoreError("control root filesystem must be NTFS or ReFS")
    return int(serial.value), filesystem_name


def _candidate_registration_id(
    supplied: str | None,
    *,
    existing: sqlite3.Row | None,
) -> str:
    if existing is not None:
        durable = str(existing["registration_id"])
        if supplied is not None:
            try:
                canonical = str(uuid.UUID(str(supplied)))
            except ValueError as exc:
                raise StateConflict("candidate registration_id must be a UUID") from exc
            if canonical != durable:
                raise StateConflict("candidate path is bound to another registration UUID")
        return durable
    try:
        return str(uuid.UUID(str(supplied))) if supplied is not None else str(uuid.uuid4())
    except ValueError as exc:
        raise StateConflict("candidate registration_id must be a UUID") from exc


def build_candidate_registration_id(release_digest: str) -> str:
    normalized = str(release_digest).strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise StateConflict("build candidate registration requires a SHA-256 release digest")
    return str(
        uuid.uuid5(
            BUILD_CANDIDATE_REGISTRATION_NAMESPACE,
            f"dataset-release-build:{normalized}",
        )
    )


def _candidate_registration_state(value: str) -> str:
    state = _nonempty(value, "candidate registration state").upper()
    if state not in {"CATALOGED", "ATTESTED", "RELEASED"}:
        raise StateConflict("candidate registration state is invalid")
    return state


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise StateConflict(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _iso_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _parse_utc_text(value: str, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise StateConflict(f"durable {field} is invalid") from exc
    return _aware_utc(parsed, field=field)


def _candidate_latest_rank(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row["cutoff"]),
        str(row["last_attested_at"] or row["created_at"]),
        str(row["artifact_root"]),
    )


def _nonempty(value: str, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must be non-empty")
    return normalized


def _required_row(row: sqlite3.Row | None, message: str) -> sqlite3.Row:
    if row is None:
        raise StateConflict(message)
    return row
