"""Two-phase, fence-checked publication of immutable dataset candidates."""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import stat
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from .cas_store import canonical_json_bytes
from .contracts import (
    CandidateIdentity,
    PitProvenanceState,
    ProducerProvenanceState,
    Scope,
)
from .errors import IdentityConflictError
from .control_store import (
    CandidateRegistrationSpec,
    ControlStore,
    StateConflict,
    append_event,
    build_candidate_registration_id,
    utc_now,
    volume_identity,
)
from .lease import LeaseManager


_MARKER_NAME = ".dataset_release_committed.json"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_CANCEL_COMMAND_TYPE = "CANCEL_REQUESTED"
_ACTIVE_CANCEL_COMMAND_STATES = ("QUEUED", "PENDING", "CLAIMED")


class PublishError(RuntimeError):
    """Base class for publish protocol failures."""


class PublishConflict(PublishError):
    """The final path or marker differs from the immutable prepared identity."""


@dataclass(frozen=True, slots=True)
class PublishSpec:
    run_id: str
    attempt_id: str
    attempt_fence: int
    host_fence: int
    release_fence: int
    release_id: str
    release_digest: str
    candidate_registration_id: str
    allowlisted_root_id: str
    volume_serial: str
    root_relative_path: str
    lineage_anchor: str
    candidate_identity: str
    producer_provenance_state: str
    producer_provenance_digest_or_sentinel: str
    pit_provenance_state: str
    profile: str
    scope: str
    cutoff: str
    staging_path: Path
    final_path: Path
    manifest_root: str
    artifact_root: str
    pit_snapshot_digest: str
    build_receipt_ref: str
    attestation_key: str
    attestation_ref: str
    source_probe_key: str
    source_probe_ref: str


@dataclass(frozen=True, slots=True)
class ArtifactTreeSnapshot:
    """Bounded immutable identity for one candidate artifact tree."""

    sha256: str
    file_count: int
    total_bytes: int


class DatasetPublisher:
    """Parent-only publisher; children may write staging but never final paths."""

    def __init__(
        self,
        store: ControlStore,
        *,
        candidate_root: str | Path,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.store = store
        self.leases = LeaseManager(store)
        self.candidate_root = _validate_existing_root(Path(candidate_root))
        self._fault = fault_injector or (lambda _point: None)

    def prepare(self, spec: PublishSpec) -> dict[str, Any]:
        """Persist PREPARED and the unique nonce: the publish commit point."""

        staging = _contained_path(spec.staging_path, self.candidate_root)
        final = _contained_path(spec.final_path, self.candidate_root)
        final_identity = final_path_identity(final)
        registration = _validated_registration_spec(
            spec,
            candidate_root=self.candidate_root,
            final_path=final,
        )
        existing_record = self.store.get_publish_record(spec.release_id)
        if existing_record is not None:
            if not _prepared_matches(existing_record, spec, final_identity):
                raise StateConflict("publish PREPARED identity conflict")
            return existing_record
        if staging == final or final.exists():
            raise PublishConflict("publish final path already exists before PREPARED")
        if not staging.is_dir():
            raise PublishError("publish staging path is missing")
        if volume_identity(staging) != volume_identity(final.parent):
            raise PublishError("staging and final path must be on the same volume")
        actual_root = artifact_tree_digest(staging)
        if actual_root != spec.artifact_root:
            raise PublishConflict("staging artifact root differs from prepared artifact root")

        stamp = utc_now()
        nonce = f"dsp_{uuid.uuid4().hex}"
        with self.store.transaction() as connection:
            run, attempt = _require_active_attempt(
                connection,
                run_id=spec.run_id,
                attempt_id=spec.attempt_id,
                run_states={"PREPARING_PUBLISH"},
                attempt_fence=spec.attempt_fence,
            )
            _require_lease(
                connection,
                resource_key="host:heavy-dataset",
                attempt_id=spec.attempt_id,
                fence=spec.host_fence,
            )
            _require_lease(
                connection,
                resource_key=f"release:{spec.release_id}",
                attempt_id=spec.attempt_id,
                fence=spec.release_fence,
            )
            pending_cancel = connection.execute(
                """
                SELECT 1 FROM commands
                WHERE target_type='run' AND target_id=? AND type=?
                  AND state IN (?,?,?) LIMIT 1
                """,
                (
                    spec.run_id,
                    _CANCEL_COMMAND_TYPE,
                    *_ACTIVE_CANCEL_COMMAND_STATES,
                ),
            ).fetchone()
            if pending_cancel is not None:
                raise StateConflict("cancel request won before publish commit point")
            attestation = connection.execute(
                "SELECT * FROM attestations WHERE attestation_key=? AND receipt_ref=?",
                (spec.attestation_key, spec.attestation_ref),
            ).fetchone()
            if attestation is None:
                raise StateConflict("prepared publish attestation is missing or mismatched")
            attestation_expected = {
                "candidate_identity": spec.candidate_identity,
                "candidate_artifact_root": spec.artifact_root,
                "producer_provenance_state": spec.producer_provenance_state,
                "producer_provenance_digest_or_sentinel": (spec.producer_provenance_digest_or_sentinel),
                "source_probe_key": spec.source_probe_key,
                "source_probe_ref": spec.source_probe_ref,
                "pit_snapshot_digest": spec.pit_snapshot_digest,
            }
            if any(attestation[field] != value for field, value in attestation_expected.items()):
                raise StateConflict("prepared publish attestation identity mismatched")
            try:
                connection.execute(
                    """
                    INSERT INTO publish_records(
                        release_id,release_digest,run_id,attempt_id,attempt_fence,host_fence,
                        release_fence,publish_nonce,published_by_attempt_id,published_by_fence,
                        state,manifest_root,artifact_root,pit_snapshot_digest,build_receipt_ref,
                        attestation_key,attestation_ref,source_probe_key,source_probe_ref,
                        final_path_identity,final_path,candidate_identity,profile,scope,cutoff,
                        registration_id,allowlisted_root_id,volume_serial,root_relative_path,
                        lineage_anchor,producer_provenance_state,
                        producer_provenance_digest_or_sentinel,pit_provenance_state,
                        created_at,updated_at
                    ) VALUES (
                        :release_id,:release_digest,:run_id,:attempt_id,:attempt_fence,
                        :host_fence,:release_fence,:publish_nonce,:published_by_attempt_id,
                        :published_by_fence,'PREPARED',:manifest_root,:artifact_root,
                        :pit_snapshot_digest,:build_receipt_ref,:attestation_key,
                        :attestation_ref,:source_probe_key,:source_probe_ref,
                        :final_path_identity,:final_path,:candidate_identity,:profile,:scope,
                        :cutoff,:registration_id,:allowlisted_root_id,:volume_serial,
                        :root_relative_path,:lineage_anchor,:producer_provenance_state,
                        :producer_provenance_digest_or_sentinel,:pit_provenance_state,
                        :created_at,:updated_at
                    )
                    """,
                    {
                        "release_id": spec.release_id,
                        "release_digest": spec.release_digest,
                        "run_id": spec.run_id,
                        "attempt_id": spec.attempt_id,
                        "attempt_fence": spec.attempt_fence,
                        "host_fence": spec.host_fence,
                        "release_fence": spec.release_fence,
                        "publish_nonce": nonce,
                        "published_by_attempt_id": spec.attempt_id,
                        "published_by_fence": spec.attempt_fence,
                        "manifest_root": spec.manifest_root,
                        "artifact_root": spec.artifact_root,
                        "pit_snapshot_digest": spec.pit_snapshot_digest,
                        "build_receipt_ref": spec.build_receipt_ref,
                        "attestation_key": spec.attestation_key,
                        "attestation_ref": spec.attestation_ref,
                        "source_probe_key": spec.source_probe_key,
                        "source_probe_ref": spec.source_probe_ref,
                        "final_path_identity": final_identity,
                        "final_path": str(final),
                        "candidate_identity": spec.candidate_identity,
                        "profile": spec.profile,
                        "scope": spec.scope,
                        "cutoff": spec.cutoff,
                        "registration_id": registration.registration_id,
                        "allowlisted_root_id": registration.allowlisted_root_id,
                        "volume_serial": registration.volume_serial,
                        "root_relative_path": registration.root_relative_path,
                        "lineage_anchor": registration.lineage_anchor,
                        "producer_provenance_state": registration.producer_provenance_state,
                        "producer_provenance_digest_or_sentinel": (registration.producer_provenance_digest_or_sentinel),
                        "pit_provenance_state": registration.pit_provenance_state,
                        "created_at": stamp,
                        "updated_at": stamp,
                    },
                )
            except Exception as exc:
                existing = connection.execute("SELECT * FROM publish_records WHERE run_id=?", (spec.run_id,)).fetchone()
                if existing is None or not _prepared_matches(existing, spec, final_identity):
                    raise StateConflict("publish PREPARED identity conflict") from exc
                return dict(existing)
            updated = connection.execute(
                """
                UPDATE runs SET state='PUBLISHING',publish_nonce=?,row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state='PREPARING_PUBLISH' AND active_attempt_id=?
                """,
                (nonce, stamp, spec.run_id, spec.attempt_id),
            )
            if updated.rowcount != 1:
                raise StateConflict("publish commit-point run CAS failed")
            append_event(
                connection,
                event_type="PUBLISH_PREPARED",
                run_id=spec.run_id,
                attempt_id=spec.attempt_id,
                payload_ref=spec.build_receipt_ref,
                created_at=stamp,
            )
            return dict(
                connection.execute("SELECT * FROM publish_records WHERE release_id=?", (spec.release_id,)).fetchone()
            )

    def commit_files(self, release_id: str) -> dict[str, Any]:
        """Rename staging and publish a complete marker, idempotently."""

        record = self.store.get_publish_record(release_id)
        if record is None or record["state"] not in {"PREPARED", "FILES_COMMITTED"}:
            raise StateConflict("publish record is not prepared")
        self._require_current_publisher(record)
        final = _contained_path(Path(str(record["final_path"])), self.candidate_root)
        staging = self.candidate_root / ".staging" / str(record["attempt_id"]) / str(record["attempt_fence"])
        # Tests and adapters may persist an exact staging ref on the attempt.
        attempt = self.store.get_attempt(str(record["attempt_id"]))
        if attempt is not None and attempt.get("staging_ref"):
            staging = Path(str(attempt["staging_ref"]))
        staging = _contained_path(staging, self.candidate_root)

        if record["state"] == "PREPARED":
            if not final.exists():
                if not staging.is_dir():
                    raise PublishConflict("prepared publish has neither staging nor final tree")
                if volume_identity(staging) != volume_identity(final.parent):
                    raise PublishError("staging and final path volume identity changed")
                if artifact_tree_digest(staging) != record["artifact_root"]:
                    return self._block_conflict(record, "staging artifact root drifted")
                os.replace(staging, final)
                _flush_directory(final.parent)
                self._fault("after_rename")
            elif staging.exists():
                return self._block_conflict(record, "both staging and final path exist")

        if artifact_tree_digest(final) != record["artifact_root"]:
            return self._block_conflict(record, "final artifact tree differs from PREPARED")
        marker_payload = _marker_payload(record)
        marker = final / _MARKER_NAME
        self._publish_marker(marker, marker_payload, record)
        self._fault("after_marker")
        marker_ref = str(marker)
        stamp = utc_now()
        with self.store.transaction() as connection:
            current = connection.execute("SELECT * FROM publish_records WHERE release_id=?", (release_id,)).fetchone()
            if current is None or current["state"] not in {"PREPARED", "FILES_COMMITTED"}:
                raise StateConflict("publish record changed during file commit")
            _verify_marker(marker, marker_payload)
            connection.execute(
                """
                UPDATE publish_records SET state='FILES_COMMITTED',marker_ref=?,updated_at=?
                WHERE release_id=? AND state IN ('PREPARED','FILES_COMMITTED')
                """,
                (marker_ref, stamp, release_id),
            )
            append_event(
                connection,
                event_type="PUBLISH_FILES_COMMITTED",
                run_id=str(current["run_id"]),
                attempt_id=str(current["attempt_id"]),
                payload_ref=marker_ref,
                created_at=stamp,
            )
            return dict(
                connection.execute("SELECT * FROM publish_records WHERE release_id=?", (release_id,)).fetchone()
            )

    def finalize(self, release_id: str) -> dict[str, Any]:
        """Commit catalog, attestation, run, attempt, and leases atomically."""

        record = self.store.get_publish_record(release_id)
        if record is None or record["state"] != "FILES_COMMITTED":
            raise StateConflict("publish files are not committed")
        final = _contained_path(Path(str(record["final_path"])), self.candidate_root)
        marker = final / _MARKER_NAME
        _verify_marker(marker, _marker_payload(record))
        stamp = utc_now()
        with self.store.transaction() as connection:
            current = connection.execute("SELECT * FROM publish_records WHERE release_id=?", (release_id,)).fetchone()
            if current is None or current["state"] != "FILES_COMMITTED":
                raise StateConflict("publish record changed before finalize")
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (current["run_id"],)).fetchone()
            if run is None or run["state"] != "PUBLISHING" or run["active_attempt_id"] is None:
                raise StateConflict("publishing run has no active finalizer")
            finalizer_id = str(run["active_attempt_id"])
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?",
                (finalizer_id, run["run_id"]),
            ).fetchone()
            if attempt is None or attempt["state"] not in {"RUNNING", "CLAIMED"}:
                raise StateConflict("active finalizer attempt is unavailable")
            _require_lease(
                connection,
                resource_key="host:heavy-dataset",
                attempt_id=finalizer_id,
                fence=int(attempt["host_fence"]),
            )
            _require_lease(
                connection,
                resource_key=f"release:{release_id}",
                attempt_id=finalizer_id,
                fence=int(attempt["release_fence"]),
            )
            attestation = connection.execute(
                "SELECT * FROM attestations WHERE attestation_key=? AND receipt_ref=?",
                (current["attestation_key"], current["attestation_ref"]),
            ).fetchone()
            if attestation is None:
                raise StateConflict("publish attestation disappeared")
            registration = CandidateRegistrationSpec(
                registration_id=str(current["registration_id"]),
                allowlisted_root_id=str(current["allowlisted_root_id"]),
                volume_serial=str(current["volume_serial"]),
                root_relative_path=str(current["root_relative_path"]),
                profile=str(current["profile"]),
                scope=str(current["scope"]),
                cutoff=datetime.fromisoformat(str(current["cutoff"])).date(),
                lineage_anchor=str(current["lineage_anchor"]),
                candidate_identity=str(current["candidate_identity"]),
                artifact_root=str(current["artifact_root"]),
                producer_provenance_state=str(current["producer_provenance_state"]),
                producer_provenance_digest_or_sentinel=str(current["producer_provenance_digest_or_sentinel"]),
                pit_provenance_state=str(current["pit_provenance_state"]),
                pit_provenance_digest_or_sentinel=str(current["pit_snapshot_digest"]),
                state="RELEASED",
                last_attested_at=_parse_timestamp(str(attestation["observed_at"])),
            )
            registered = self.store.register_candidate_in_transaction(
                connection,
                registration,
                stamp=stamp,
            )
            if registered["candidate_identity"] != current["candidate_identity"]:
                raise StateConflict("publish candidate registration identity changed")
            existing_release = connection.execute(
                "SELECT * FROM releases WHERE release_digest=?", (current["release_digest"],)
            ).fetchone()
            values = (
                current["release_digest"],
                current["release_id"],
                current["candidate_identity"],
                current["run_id"],
                current["profile"],
                current["scope"],
                current["cutoff"],
                current["artifact_root"],
                current["pit_snapshot_digest"],
                current["final_path_identity"],
                str(marker),
                attestation["attestation_id"],
                "COMMITTED",
            )
            if existing_release is None:
                connection.execute("INSERT INTO releases VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", values)
            elif tuple(existing_release) != values:
                raise PublishConflict("release digest is bound to another catalog identity")
            connection.execute(
                "UPDATE attestations SET committed=1 WHERE attestation_id=?",
                (attestation["attestation_id"],),
            )
            connection.execute(
                "UPDATE artifacts SET committed=1 WHERE producer_attempt_id=?",
                (str(current["attempt_id"]),),
            )
            connection.execute(
                """
                UPDATE publish_records SET state='COMMITTED',finalized_by_attempt_id=?,
                    finalized_by_fence=?,marker_ref=?,updated_at=? WHERE release_id=?
                """,
                (finalizer_id, attempt["attempt_fence"], str(marker), stamp, release_id),
            )
            connection.execute(
                "UPDATE attempts SET state='RELEASED_SUCCEEDED',updated_at=? WHERE attempt_id=?",
                (stamp, finalizer_id),
            )
            released = self.leases.release_by_attempt_in_transaction(connection, attempt_id=finalizer_id)
            if released != 2:
                raise StateConflict("publish finalizer did not release exactly host and release leases")
            updated = connection.execute(
                """
                UPDATE runs SET state='SUCCEEDED',outcome='CANDIDATE_VALIDATED',
                    terminal_receipt_ref=?,candidate_identity=?,artifact_root=?,active_attempt_id=NULL,
                    row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state='PUBLISHING' AND active_attempt_id=?
                """,
                (
                    current["build_receipt_ref"],
                    current["candidate_identity"],
                    current["artifact_root"],
                    stamp,
                    current["run_id"],
                    finalizer_id,
                ),
            )
            if updated.rowcount != 1:
                raise StateConflict("publish terminal transaction run CAS failed")
            append_event(
                connection,
                event_type="CANDIDATE_VALIDATED",
                run_id=str(current["run_id"]),
                attempt_id=finalizer_id,
                payload_ref=str(current["build_receipt_ref"]),
                created_at=stamp,
            )
            return dict(connection.execute("SELECT * FROM releases WHERE release_id=?", (release_id,)).fetchone())

    def recover_and_finalize(self, release_id: str) -> dict[str, Any]:
        """Idempotently close PREPARED/marker/DB crash windows for the current owner."""

        record = self.store.get_publish_record(release_id)
        if record is None:
            raise StateConflict("publish record does not exist")
        if record["state"] in {"PREPARED", "FILES_COMMITTED"}:
            if record["state"] == "PREPARED":
                self.commit_files(release_id)
            return self.finalize(release_id)
        if record["state"] == "COMMITTED":
            discovered = self.discover(release_id)
            if discovered is None:
                raise PublishConflict("committed publish is not discoverable")
            return discovered
        raise PublishConflict(f"publish cannot be recovered from state={record['state']}")

    def discover(self, release_id: str) -> dict[str, Any] | None:
        """Return only catalog + publish + marker-consistent releases."""

        with self.store.transaction(immediate=False) as connection:
            row = connection.execute(
                """
                SELECT r.*,p.publish_nonce,p.manifest_root,p.attestation_key,p.attestation_ref,
                       p.published_by_attempt_id,p.published_by_fence
                FROM releases r JOIN publish_records p ON p.release_id=r.release_id
                WHERE r.release_id=? AND r.state='COMMITTED' AND p.state='COMMITTED'
                """,
                (release_id,),
            ).fetchone()
        if row is None:
            return None
        # Resolve from the durable publish record; path identity itself is an
        # audit value and is never parsed as an operator-supplied path.
        publish = self.store.get_publish_record(release_id)
        assert publish is not None
        final = _contained_path(Path(str(publish["final_path"])), self.candidate_root)
        _verify_marker(final / _MARKER_NAME, _marker_payload(publish))
        if artifact_tree_digest(final) != publish["artifact_root"]:
            raise PublishConflict("committed candidate artifact root changed")
        return dict(row)

    def _publish_marker(
        self,
        marker: Path,
        payload: Mapping[str, Any],
        record: Mapping[str, Any],
    ) -> None:
        raw = canonical_json_bytes(payload)
        if marker.exists():
            _verify_marker(marker, payload)
            return
        temporary = marker.parent / (f".committed.{record['publish_nonce']}.{record['attempt_id']}.tmp")
        try:
            descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o444)
        except FileExistsError:
            # A previous crashed writer may leave a complete or partial temp;
            # it is not authority and is never overwritten automatically.
            temporary = marker.parent / (
                f".committed.{record['publish_nonce']}.{record['attempt_id']}.{uuid.uuid4().hex}.tmp"
            )
            descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o444)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        self._fault("after_marker_temp")
        published = _publish_no_replace(temporary, marker)
        if not published:
            _verify_marker(marker, payload)
        _flush_directory(marker.parent)
        _verify_marker(marker, payload)
        temporary.unlink(missing_ok=True)

    def _block_conflict(self, record: Mapping[str, Any], message: str) -> dict[str, Any]:
        stamp = utc_now()
        with self.store.transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (record["run_id"],)).fetchone()
            active_attempt_id = str(run["active_attempt_id"]) if run and run["active_attempt_id"] else None
            connection.execute(
                "UPDATE publish_records SET state='CONFLICT',updated_at=? WHERE release_id=?",
                (stamp, record["release_id"]),
            )
            if active_attempt_id is not None:
                self.leases.release_by_attempt_in_transaction(connection, attempt_id=active_attempt_id)
                connection.execute(
                    "UPDATE attempts SET state='FAILED_TERMINAL',updated_at=? WHERE attempt_id=?",
                    (stamp, active_attempt_id),
                )
            connection.execute(
                """
                UPDATE runs SET state='BLOCKED_PUBLISH_CONFLICT',outcome='BLOCKED',
                    active_attempt_id=NULL,row_version=row_version+1,updated_at=? WHERE run_id=?
                """,
                (stamp, record["run_id"]),
            )
            append_event(
                connection,
                event_type="BLOCKED_PUBLISH_CONFLICT",
                run_id=str(record["run_id"]),
                attempt_id=str(record["attempt_id"]),
                created_at=stamp,
            )
        raise PublishConflict(message)

    def _require_current_publisher(self, record: Mapping[str, Any]) -> None:
        with self.store.transaction(immediate=False) as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (record["run_id"],)).fetchone()
            if run is None or run["state"] != "PUBLISHING" or run["active_attempt_id"] is None:
                raise StateConflict("publish filesystem owner is not active")
            active_attempt_id = str(run["active_attempt_id"])
            allowed = {str(record["attempt_id"])}
            if record["finalized_by_attempt_id"]:
                allowed.add(str(record["finalized_by_attempt_id"]))
            if active_attempt_id not in allowed:
                raise StateConflict("publish filesystem owner is not the prepared/finalizer attempt")
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (active_attempt_id,)).fetchone()
            if attempt is None or attempt["state"] not in {"CLAIMED", "RUNNING"}:
                raise StateConflict("publish filesystem attempt is not active")
            _require_lease(
                connection,
                resource_key="host:heavy-dataset",
                attempt_id=active_attempt_id,
                fence=int(attempt["host_fence"]),
            )
            _require_lease(
                connection,
                resource_key=f"release:{record['release_id']}",
                attempt_id=active_attempt_id,
                fence=int(attempt["release_fence"]),
            )


def artifact_tree_snapshot(root: Path) -> ArtifactTreeSnapshot:
    """Hash immutable candidate bytes with O(one path + one read block) memory."""

    resolved = root.resolve(strict=True)
    if not resolved.is_dir():
        raise PublishConflict("candidate artifact root is not a directory")
    aggregate = hashlib.sha256()
    file_count = 0
    total_bytes = 0
    for base, directories, filenames in os.walk(resolved):
        directories.sort()
        filenames.sort()
        base_path = Path(base)
        _assert_plain(base_path)
        for directory in directories:
            _assert_plain(base_path / directory)
        for filename in filenames:
            if filename == _MARKER_NAME or filename.startswith(".committed."):
                continue
            path = base_path / filename
            _assert_plain(path)
            relative = path.relative_to(resolved).as_posix().encode("utf-8")
            digest = hashlib.sha256()
            size = 0
            with path.open("rb") as handle:
                while chunk := handle.read(8 * 1024 * 1024):
                    size += len(chunk)
                    digest.update(chunk)
            file_count += 1
            total_bytes += size
            row = len(relative).to_bytes(4, "big") + relative + size.to_bytes(8, "big") + digest.digest()
            # Preserve the v1 byte-for-byte Merkle encoding while avoiding a
            # candidate-wide list of every file row.  Catalog/re-attestation
            # memory is now O(one path + one 8 MiB read block), not O(files).
            aggregate.update(len(row).to_bytes(4, "big"))
            aggregate.update(row)
    return ArtifactTreeSnapshot(
        sha256=aggregate.hexdigest(),
        file_count=file_count,
        total_bytes=total_bytes,
    )


def artifact_tree_digest(root: Path) -> str:
    """Return the canonical v1 candidate artifact root."""

    return artifact_tree_snapshot(root).sha256


def final_path_identity(path: Path) -> str:
    resolved_parent = path.parent.resolve(strict=True)
    canonical = str(resolved_parent / path.name).replace("\\", "/").casefold()
    return f"{volume_identity(resolved_parent)}|{canonical}"


def _marker_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    body = {
        "schema_version": "dataset_release_committed_marker_v1",
        "release_id": record["release_id"],
        "release_digest": record["release_digest"],
        "publish_nonce": record["publish_nonce"],
        "manifest_root": record["manifest_root"],
        "artifact_root": record["artifact_root"],
        "pit_snapshot_digest": record["pit_snapshot_digest"],
        "candidate_identity": record["candidate_identity"],
        "registration_id": record["registration_id"],
        "allowlisted_root_id": record["allowlisted_root_id"],
        "volume_serial": record["volume_serial"],
        "root_relative_path": record["root_relative_path"],
        "lineage_anchor": record["lineage_anchor"],
        "producer_provenance_state": record["producer_provenance_state"],
        "producer_provenance_digest_or_sentinel": record["producer_provenance_digest_or_sentinel"],
        "pit_provenance_state": record["pit_provenance_state"],
        "profile": record["profile"],
        "scope": record["scope"],
        "cutoff": record["cutoff"],
        "attestation_key": record["attestation_key"],
        "attestation_receipt_digest": record["attestation_ref"],
        "published_by_attempt": record["published_by_attempt_id"],
        "published_by_fence": int(record["published_by_fence"]),
    }
    canonical_body = canonical_json_bytes(body)
    return {
        **body,
        "payload_length": len(canonical_body),
        "payload_sha256": hashlib.sha256(canonical_body).hexdigest(),
    }


def _verify_marker(path: Path, expected: Mapping[str, Any]) -> None:
    _assert_plain(path)
    try:
        raw = path.read_bytes()
        decoded = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublishConflict("committed marker is missing or invalid") from exc
    if decoded != dict(expected) or raw != canonical_json_bytes(expected):
        raise PublishConflict("committed marker identity differs from PREPARED")
    body = dict(decoded)
    supplied_length = int(body.pop("payload_length"))
    supplied_hash = str(body.pop("payload_sha256"))
    canonical_body = canonical_json_bytes(body)
    if supplied_length != len(canonical_body) or supplied_hash != hashlib.sha256(canonical_body).hexdigest():
        raise PublishConflict("committed marker length/hash envelope is invalid")


def _require_active_attempt(
    connection: Any,
    *,
    run_id: str,
    attempt_id: str,
    run_states: set[str],
    attempt_fence: int,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    attempt = connection.execute(
        "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?", (attempt_id, run_id)
    ).fetchone()
    if (
        run is None
        or attempt is None
        or run["state"] not in run_states
        or run["active_attempt_id"] != attempt_id
        or int(attempt["attempt_fence"]) != int(attempt_fence)
        or attempt["state"] not in {"CLAIMED", "RUNNING"}
    ):
        raise StateConflict("publish attempt ownership changed")
    return run, attempt


def _require_lease(
    connection: Any,
    *,
    resource_key: str,
    attempt_id: str,
    fence: int,
) -> None:
    row = connection.execute(
        """
        SELECT 1 FROM leases WHERE resource_key=? AND attempt_id=?
          AND fence_counter=? AND state='ACTIVE'
        """,
        (resource_key, attempt_id, int(fence)),
    ).fetchone()
    if row is None:
        raise StateConflict(f"publish lease/fence changed: {resource_key}")


def _validated_registration_spec(
    spec: PublishSpec,
    *,
    candidate_root: Path,
    final_path: Path,
) -> CandidateRegistrationSpec:
    expected_registration = build_candidate_registration_id(spec.release_digest)
    if spec.candidate_registration_id != expected_registration:
        raise StateConflict("build candidate registration UUID differs from release digest")
    expected_relative = final_path.relative_to(candidate_root).as_posix()
    if spec.root_relative_path.replace("\\", "/") != expected_relative:
        raise StateConflict("build candidate root-relative path differs from final path")
    expected_volume = volume_identity(candidate_root)
    if spec.volume_serial != expected_volume:
        raise StateConflict("build candidate volume identity differs from candidate root")
    expected_lineage = f"BUILD_RELEASE_DIGEST:{spec.release_digest}"
    if spec.lineage_anchor != expected_lineage:
        raise StateConflict("build candidate lineage anchor differs from release digest")
    try:
        cutoff = datetime.fromisoformat(spec.cutoff).date()
        producer_state = ProducerProvenanceState(spec.producer_provenance_state)
        pit_state = PitProvenanceState(spec.pit_provenance_state)
        scope = Scope(spec.scope)
        identity = CandidateIdentity(
            registration_uuid=spec.candidate_registration_id,
            allowlisted_root_id=spec.allowlisted_root_id,
            volume_serial=spec.volume_serial,
            root_relative_path=spec.root_relative_path,
            profile=spec.profile,
            scope=scope,
            cutoff=cutoff,
            lineage_anchor=spec.lineage_anchor,
            pit_provenance_state=pit_state,
            pit_provenance_digest_or_sentinel=spec.pit_snapshot_digest,
            artifact_root=spec.artifact_root,
            producer_provenance_state=producer_state,
            producer_provenance_digest_or_sentinel=(spec.producer_provenance_digest_or_sentinel),
        ).key
    except (IdentityConflictError, ValueError, TypeError) as exc:
        raise StateConflict("build candidate registration fields are invalid") from exc
    if pit_state is not PitProvenanceState.KNOWN:
        raise StateConflict("new build candidate requires known PIT provenance")
    if identity != spec.candidate_identity:
        raise StateConflict("build candidate identity differs from registration fields")
    return CandidateRegistrationSpec(
        registration_id=spec.candidate_registration_id,
        allowlisted_root_id=spec.allowlisted_root_id,
        volume_serial=spec.volume_serial,
        root_relative_path=spec.root_relative_path,
        profile=spec.profile,
        scope=spec.scope,
        cutoff=cutoff,
        lineage_anchor=spec.lineage_anchor,
        candidate_identity=spec.candidate_identity,
        artifact_root=spec.artifact_root,
        producer_provenance_state=spec.producer_provenance_state,
        producer_provenance_digest_or_sentinel=(spec.producer_provenance_digest_or_sentinel),
        pit_provenance_state=spec.pit_provenance_state,
        pit_provenance_digest_or_sentinel=spec.pit_snapshot_digest,
        state="RELEASED",
    )


def _parse_timestamp(value: str) -> datetime:
    try:
        observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise StateConflict("publish attestation timestamp is invalid") from exc
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise StateConflict("publish attestation timestamp must be timezone-aware")
    return observed


def _prepared_matches(row: Mapping[str, Any], spec: PublishSpec, final_identity: str) -> bool:
    expected = {
        "release_id": spec.release_id,
        "release_digest": spec.release_digest,
        "run_id": spec.run_id,
        "attempt_id": spec.attempt_id,
        "attempt_fence": spec.attempt_fence,
        "host_fence": spec.host_fence,
        "release_fence": spec.release_fence,
        "manifest_root": spec.manifest_root,
        "artifact_root": spec.artifact_root,
        "pit_snapshot_digest": spec.pit_snapshot_digest,
        "build_receipt_ref": spec.build_receipt_ref,
        "attestation_key": spec.attestation_key,
        "attestation_ref": spec.attestation_ref,
        "source_probe_key": spec.source_probe_key,
        "source_probe_ref": spec.source_probe_ref,
        "final_path_identity": final_identity,
        "registration_id": spec.candidate_registration_id,
        "allowlisted_root_id": spec.allowlisted_root_id,
        "volume_serial": spec.volume_serial,
        "root_relative_path": spec.root_relative_path,
        "lineage_anchor": spec.lineage_anchor,
        "candidate_identity": spec.candidate_identity,
        "producer_provenance_state": spec.producer_provenance_state,
        "producer_provenance_digest_or_sentinel": (spec.producer_provenance_digest_or_sentinel),
        "pit_provenance_state": spec.pit_provenance_state,
        "profile": spec.profile,
        "scope": spec.scope,
        "cutoff": spec.cutoff,
    }
    return all(row[key] == value for key, value in expected.items())


def _validate_existing_root(path: Path) -> Path:
    if not path.is_absolute():
        raise PublishError("candidate root must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise PublishError("candidate root must already exist") from exc
    if not resolved.is_dir():
        raise PublishError("candidate root must be a directory")
    _assert_existing_chain(resolved)
    return resolved


def _contained_path(path: Path, root: Path) -> Path:
    candidate = path if path.is_absolute() else root / path
    parent = candidate.parent.resolve(strict=True)
    resolved = parent / candidate.name
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise PublishError("publish path escapes candidate root") from exc
    _assert_relative_chain(parent, root)
    if resolved.exists():
        _assert_plain(resolved)
    return resolved


def _assert_relative_chain(path: Path, root: Path) -> None:
    current = root
    _assert_plain(current)
    for part in path.relative_to(root).parts:
        current = current / part
        _assert_plain(current)


def _assert_existing_chain(path: Path) -> None:
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
        raise PublishConflict(f"publish path component is unavailable: {path}") from exc
    if stat.S_ISLNK(info.st_mode) or (int(getattr(info, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise PublishConflict(f"publish path traverses symlink/reparse point: {path}")


def _publish_no_replace(source: Path, target: Path) -> bool:
    if os.name == "nt":
        move_file_ex = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
        move_file_ex.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        move_file_ex.restype = ctypes.c_int
        if move_file_ex(str(source), str(target), 0x00000008):
            return True
        error = ctypes.get_last_error()
        if error in {80, 183}:
            return False
        raise ctypes.WinError(error)
    try:
        os.link(source, target)
    except FileExistsError:
        return False
    return True


def _flush_directory(path: Path) -> None:
    if os.name == "nt":
        # MoveFileExW(..., WRITE_THROUGH) above is the Windows durability
        # primitive.  A dedicated target-platform smoke exercises directory
        # handle behavior; unit tests remain filesystem-portable.
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
