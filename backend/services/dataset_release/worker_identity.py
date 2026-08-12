"""Durable identity and health projection for the independent release Worker."""

from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import stat
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, Mapping

import psutil

from .control_store import CONTROL_SCHEMA_VERSION, ControlStore
from .errors import DatasetReleaseError


WORKER_IDENTITY_SCHEMA = "dataset_release_worker_identity_v1"
WORKER_HEARTBEAT_SCHEMA = "dataset_release_worker_heartbeat_v1"
WORKER_CAPABILITY_VERSION = "dataset_release_worker_core_v1"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_HEX_DIGEST = re.compile(r"[0-9a-f]{64}")
_CODE_SHA = re.compile(r"[0-9a-f]{40,64}")


class WorkerIdentityError(DatasetReleaseError):
    code = "BLOCKED_WORKER_IDENTITY_INVALID"


@dataclass(frozen=True, slots=True)
class WorkerIdentity:
    instance_id: str
    host: str
    pid: int
    process_create_time: str
    code_sha: str
    capability_digest: str
    capabilities: tuple[str, ...]
    profile_digests: tuple[tuple[str, str], ...]
    started_at: str

    @classmethod
    def create(
        cls,
        *,
        code_sha: str,
        profile_digests: Mapping[str, str],
        capabilities: tuple[str, ...],
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        host: str | None = None,
        pid: int | None = None,
        process_create_time: str | None = None,
        instance_id: str | None = None,
    ) -> "WorkerIdentity":
        observed = _utc(now())
        normalized_sha = str(code_sha).strip().lower()
        normalized_profiles = tuple(
            sorted((str(key).strip(), str(value).strip().lower()) for key, value in profile_digests.items())
        )
        normalized_capabilities = tuple(sorted(set(str(value).strip() for value in capabilities)))
        worker_id = instance_id or f"dsw_{uuid.uuid4().hex}"
        worker_host = str(host or socket.gethostname()).strip()
        worker_pid = int(pid if pid is not None else os.getpid())
        if process_create_time is None:
            try:
                created = datetime.fromtimestamp(psutil.Process(worker_pid).create_time(), UTC).isoformat(
                    timespec="microseconds"
                )
            except psutil.Error as exc:
                raise WorkerIdentityError("worker process create-time cannot be read") from exc
        else:
            try:
                created = _utc(datetime.fromisoformat(str(process_create_time))).isoformat(timespec="microseconds")
            except ValueError as exc:
                raise WorkerIdentityError("worker process create-time is invalid") from exc
        if not re.fullmatch(r"dsw_[0-9a-f]{32}", worker_id):
            raise WorkerIdentityError("worker instance_id is invalid")
        if not worker_host or worker_pid <= 0:
            raise WorkerIdentityError("worker host/PID identity is invalid")
        if _CODE_SHA.fullmatch(normalized_sha) is None:
            raise WorkerIdentityError("worker code_sha must be a 40-64 character hex digest")
        if not normalized_profiles or any(
            not key or _HEX_DIGEST.fullmatch(value) is None for key, value in normalized_profiles
        ):
            raise WorkerIdentityError("worker profile digests are missing or invalid")
        if not normalized_capabilities or any(not value for value in normalized_capabilities):
            raise WorkerIdentityError("worker capabilities must be non-empty")
        capability_payload = {
            "worker_capability_version": WORKER_CAPABILITY_VERSION,
            "control_schema_version": CONTROL_SCHEMA_VERSION,
            "code_sha": normalized_sha,
            "profiles": dict(normalized_profiles),
            "capabilities": list(normalized_capabilities),
        }
        capability_digest = hashlib.sha256(_json_bytes(capability_payload)).hexdigest()
        return cls(
            instance_id=worker_id,
            host=worker_host,
            pid=worker_pid,
            process_create_time=created,
            code_sha=normalized_sha,
            capability_digest=capability_digest,
            capabilities=normalized_capabilities,
            profile_digests=normalized_profiles,
            started_at=observed.isoformat(timespec="microseconds"),
        )

    @property
    def owner_identity(self) -> str:
        return f"{self.instance_id}:{self.capability_digest[:16]}"

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": WORKER_IDENTITY_SCHEMA,
            "instance_id": self.instance_id,
            "host": self.host,
            "pid": self.pid,
            "process_create_time": self.process_create_time,
            "code_sha": self.code_sha,
            "capability_digest": self.capability_digest,
            "capabilities": list(self.capabilities),
            "control_schema_version": CONTROL_SCHEMA_VERSION,
            "profile_digests": dict(self.profile_digests),
            "started_at": self.started_at,
        }


@dataclass(frozen=True, slots=True)
class WorkerHealth:
    state: str
    reason: str
    instance_id: str | None
    worker_status: str | None
    last_poll_at: str | None
    age_seconds: float | None
    capability_digest: str | None
    files_scanned: int

    def as_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "reason": self.reason,
            "instance_id": self.instance_id,
            "worker_status": self.worker_status,
            "last_poll_at": self.last_poll_at,
            "age_seconds": self.age_seconds,
            "capability_digest": self.capability_digest,
            "files_scanned": self.files_scanned,
        }


class WorkerHeartbeatStore:
    """Atomically retain every Worker instance's latest health projection."""

    def __init__(self, store: ControlStore) -> None:
        self.control_root = store.root.resolve(strict=True)
        self.root = self.control_root / "worker_heartbeats"
        self._counters: dict[str, int] = {}
        self._write_lock = threading.Lock()

    def write(
        self,
        identity: WorkerIdentity,
        *,
        status: str,
        observed_at: datetime,
        claim_kind: str | None = None,
        claim_id: str | None = None,
        stop_requested: bool = False,
    ) -> Path:
        normalized_status = str(status).strip().upper()
        if not normalized_status or (claim_kind is None) != (claim_id is None):
            raise WorkerIdentityError("worker heartbeat status/claim identity is invalid")
        with self._write_lock:
            counter = self._counters.get(identity.instance_id, 0) + 1
            self._counters[identity.instance_id] = counter
            payload = {
                "schema_version": WORKER_HEARTBEAT_SCHEMA,
                "identity": identity.as_dict(),
                "status": normalized_status,
                "last_poll_at": _utc(observed_at).isoformat(timespec="microseconds"),
                "counter": counter,
                "claim_kind": claim_kind,
                "claim_id": claim_id,
                "stop_requested": bool(stop_requested),
            }
            self.root.mkdir(parents=False, exist_ok=True)
            _assert_plain(self.root)
            destination = self.root / f"{identity.instance_id}.json"
            _atomic_replace_json(destination, payload)
            persisted = json.loads(destination.read_text(encoding="utf-8"))
            if persisted != payload:
                raise WorkerIdentityError("worker heartbeat readback mismatch")
            return destination

    def read(self, instance_id: str) -> Mapping[str, object]:
        if not re.fullmatch(r"dsw_[0-9a-f]{32}", str(instance_id)):
            raise WorkerIdentityError("worker instance_id is invalid")
        try:
            payload = json.loads((self.root / f"{instance_id}.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise WorkerIdentityError("worker heartbeat is unavailable") from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != WORKER_HEARTBEAT_SCHEMA:
            raise WorkerIdentityError("worker heartbeat schema is invalid")
        return payload

    def read_latest(
        self,
        *,
        profile: str,
        config_digest: str,
        ttl_seconds: float,
        max_files: int = 200,
        now: datetime | None = None,
    ) -> WorkerHealth:
        """Return one bounded, validated health projection without writes."""

        if not str(profile).strip() or _HEX_DIGEST.fullmatch(str(config_digest)) is None:
            raise WorkerIdentityError("worker health profile identity is invalid")
        if ttl_seconds <= 0 or not 0 < max_files <= 200:
            raise WorkerIdentityError("worker health TTL/file bound is invalid")
        if not self.root.exists():
            return WorkerHealth(
                "unavailable",
                "worker_heartbeat_root_missing",
                None,
                None,
                None,
                None,
                None,
                0,
            )
        _assert_plain(self.root)
        entries = []
        with os.scandir(self.root) as iterator:
            for item in iterator:
                if not item.name.startswith("dsw_") or not item.name.endswith(".json"):
                    continue
                entries.append(item.name)
                if len(entries) > max_files:
                    return WorkerHealth(
                        "blocked",
                        "worker_heartbeat_file_limit_exceeded",
                        None,
                        None,
                        None,
                        None,
                        None,
                        len(entries),
                    )
        observed = _utc(now or datetime.now(UTC))
        candidates: list[tuple[datetime, Mapping[str, object]]] = []
        for name in sorted(entries):
            path = self.root / name
            try:
                _assert_plain(path)
                if not 0 < path.stat().st_size <= 64 * 1024:
                    raise WorkerIdentityError("worker heartbeat file size is invalid")
                payload = json.loads(path.read_text(encoding="utf-8"))
                heartbeat_at = _validate_heartbeat_payload(
                    payload,
                    profile=profile,
                    config_digest=config_digest,
                    expected_instance_id=path.stem,
                )
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, WorkerIdentityError):
                return WorkerHealth(
                    "blocked",
                    "worker_heartbeat_validation_failed",
                    None,
                    None,
                    None,
                    None,
                    None,
                    len(entries),
                )
            if heartbeat_at is not None:
                candidates.append((heartbeat_at, payload))
        if not candidates:
            return WorkerHealth(
                "unavailable",
                "no_matching_worker_profile",
                None,
                None,
                None,
                None,
                None,
                len(entries),
            )
        heartbeat_at, latest = max(candidates, key=lambda item: item[0])
        identity = latest["identity"]
        assert isinstance(identity, dict)
        status = str(latest["status"])
        age = max(0.0, (observed - heartbeat_at).total_seconds())
        if status.startswith("BLOCKED_"):
            state, reason = "blocked", "worker_reported_blocked"
        elif status == "STOPPED" or age > ttl_seconds:
            state, reason = "stale", "worker_stopped_or_heartbeat_expired"
        else:
            state, reason = "healthy", "worker_heartbeat_fresh"
        return WorkerHealth(
            state,
            reason,
            str(identity["instance_id"]),
            status,
            heartbeat_at.isoformat(timespec="microseconds"),
            age,
            str(identity["capability_digest"]),
            len(entries),
        )


def _atomic_replace_json(path: Path, payload: Mapping[str, object]) -> None:
    raw = _json_bytes(payload)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".partial", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        _assert_plain(path)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_heartbeat_payload(
    payload: object,
    *,
    profile: str,
    config_digest: str,
    expected_instance_id: str,
) -> datetime | None:
    if not isinstance(payload, dict) or payload.get("schema_version") != WORKER_HEARTBEAT_SCHEMA:
        raise WorkerIdentityError("worker heartbeat schema is invalid")
    identity = payload.get("identity")
    if not isinstance(identity, dict) or identity.get("schema_version") != WORKER_IDENTITY_SCHEMA:
        raise WorkerIdentityError("worker heartbeat identity schema is invalid")
    instance_id = str(identity.get("instance_id", ""))
    code_sha = str(identity.get("code_sha", ""))
    capability_digest = str(identity.get("capability_digest", ""))
    capabilities_raw = identity.get("capabilities")
    profiles_raw = identity.get("profile_digests")
    if (
        instance_id != expected_instance_id
        or re.fullmatch(r"dsw_[0-9a-f]{32}", instance_id) is None
        or _CODE_SHA.fullmatch(code_sha) is None
        or _HEX_DIGEST.fullmatch(capability_digest) is None
        or identity.get("control_schema_version") != CONTROL_SCHEMA_VERSION
        or not isinstance(capabilities_raw, list)
        or not capabilities_raw
        or not isinstance(profiles_raw, dict)
        or not profiles_raw
    ):
        raise WorkerIdentityError("worker heartbeat capability identity is invalid")
    capabilities = tuple(sorted(set(str(item).strip() for item in capabilities_raw)))
    profiles = {str(key).strip(): str(value).strip().lower() for key, value in profiles_raw.items()}
    if (
        len(capabilities) != len(capabilities_raw)
        or any(not item for item in capabilities)
        or any(not key or _HEX_DIGEST.fullmatch(value) is None for key, value in profiles.items())
    ):
        raise WorkerIdentityError("worker heartbeat profile/capability list is invalid")
    expected_capability = hashlib.sha256(
        _json_bytes(
            {
                "worker_capability_version": WORKER_CAPABILITY_VERSION,
                "control_schema_version": CONTROL_SCHEMA_VERSION,
                "code_sha": code_sha,
                "profiles": dict(sorted(profiles.items())),
                "capabilities": list(capabilities),
            }
        )
    ).hexdigest()
    if expected_capability != capability_digest:
        raise WorkerIdentityError("worker heartbeat capability digest mismatched")
    try:
        heartbeat_at = _utc(datetime.fromisoformat(str(payload.get("last_poll_at"))))
        counter = int(payload.get("counter", 0))
    except (TypeError, ValueError) as exc:
        raise WorkerIdentityError("worker heartbeat time/counter is invalid") from exc
    if counter <= 0 or not str(payload.get("status", "")).strip():
        raise WorkerIdentityError("worker heartbeat status/counter is invalid")
    if profiles.get(profile) != config_digest:
        return None
    return heartbeat_at


def _json_bytes(payload: Mapping[str, object]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")


def _assert_plain(path: Path) -> None:
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise WorkerIdentityError(f"worker heartbeat path is unavailable: {path.name}") from exc
    if stat.S_ISLNK(info.st_mode) or (int(getattr(info, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise WorkerIdentityError("worker heartbeat path is a symlink/reparse point")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise WorkerIdentityError("worker timestamps must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "WORKER_CAPABILITY_VERSION",
    "WorkerHeartbeatStore",
    "WorkerHealth",
    "WorkerIdentity",
    "WorkerIdentityError",
]
