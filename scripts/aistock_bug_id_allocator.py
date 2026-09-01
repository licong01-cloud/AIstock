"""Shared BUG id allocator primitives used by CLI and MCP intake paths.

The lock has two layers:

* a process-local lock serializes threads before they touch the lock file;
* an ownership-token file lock serializes independent processes.

Keeping the layers in one module prevents the CLI and MCP implementations from
drifting and avoids a Windows race where one thread reads the lock file while
the owner tries to unlink it.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable


class BugIdLockError(RuntimeError):
    """Raised when the shared BUG id allocator lock cannot be acquired/released."""


class ExistingBugReservationError(BugIdLockError):
    """Raised when the same logical BUG is already reserved by another intake."""

    def __init__(self, record: dict[str, Any]) -> None:
        self.record = dict(record)
        bug_id = self.record.get("bug_id") or Path(str(self.record.get("reservation_path") or "unknown")).stem
        status = self.record.get("status") or "unknown"
        super().__init__(f"matching BUG reservation already exists: {bug_id} status={status}")


TERMINAL_RESERVATION_STATUSES = {
    "registered",
    "local_registry_written",
    "released",
}
DEFAULT_TERMINAL_RETENTION_SECONDS = 300.0
ALLOCATOR_STATE_SCHEMA = "aistock_bug_id_allocator_state_v1"
FINGERPRINT_INDEX_SCHEMA = "aistock_bug_fingerprint_index_v1"
FINGERPRINT_INDEX_VERSION = 2


_PROCESS_LOCKS_GUARD = threading.Lock()
_PROCESS_LOCKS: dict[str, threading.Lock] = {}


def _path_key(path: Path) -> str:
    try:
        value = str(path.resolve())
    except OSError:
        value = str(path.absolute())
    return os.path.normcase(value)


def _process_lock(path: Path) -> threading.Lock:
    key = _path_key(path)
    with _PROCESS_LOCKS_GUARD:
        return _PROCESS_LOCKS.setdefault(key, threading.Lock())


def _thread_is_alive(thread_id: int | None) -> bool | None:
    if thread_id is None:
        return None
    return any(thread.ident == thread_id and thread.is_alive() for thread in threading.enumerate())


def _parse_owner(snapshot: str) -> tuple[int | None, int | None]:
    try:
        payload = json.loads(snapshot)
    except json.JSONDecodeError:
        lines = snapshot.splitlines()
        pid = int(lines[0].strip()) if lines and lines[0].strip().isdigit() else None
        return pid, None
    try:
        pid = int(payload.get("pid"))
    except (TypeError, ValueError):
        pid = None
    try:
        thread_id = int(payload.get("thread_id"))
    except (TypeError, ValueError):
        thread_id = None
    return pid, thread_id


def read_reservations(root: Path) -> list[dict[str, Any]]:
    """Read valid reservation records, retaining their source path."""

    result: list[dict[str, Any]] = []
    if not root.exists():
        return result
    for path in sorted(root.glob("BUG-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        payload = dict(payload)
        payload["reservation_path"] = str(path)
        result.append(payload)
    return result


def read_allocator_state(path: Path) -> dict[str, Any] | None:
    """Read the single host-wide allocator state without scanning registries."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise BugIdLockError(f"invalid BUG id allocator state: {path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != ALLOCATOR_STATE_SCHEMA:
        raise BugIdLockError(f"invalid BUG id allocator state schema: {path}")
    try:
        last_allocated = int(payload.get("last_allocated") or 0)
    except (TypeError, ValueError) as exc:
        raise BugIdLockError(f"invalid BUG id allocator state counter: {path}") from exc
    if last_allocated < 0:
        raise BugIdLockError(f"invalid BUG id allocator state counter: {path}")
    try:
        fingerprint_index_version = int(payload.get("fingerprint_index_version") or 0)
    except (TypeError, ValueError) as exc:
        raise BugIdLockError(f"invalid BUG id allocator fingerprint index version: {path}") from exc
    if fingerprint_index_version < 0:
        raise BugIdLockError(f"invalid BUG id allocator fingerprint index version: {path}")
    return {
        **payload,
        "last_allocated": last_allocated,
        "fingerprint_index_version": fingerprint_index_version,
    }


def write_allocator_state(
    path: Path,
    *,
    last_allocated: int,
    updated_at: str,
    updated_by: str,
    fingerprint_index_version: int,
) -> dict[str, Any]:
    """Atomically update the host-wide allocator state while its lock is held."""

    value = int(last_allocated)
    if value < 0:
        raise BugIdLockError("BUG id allocator state cannot be negative")
    payload = {
        "schema_version": ALLOCATOR_STATE_SCHEMA,
        "last_allocated": value,
        "updated_at": str(updated_at),
        "updated_by": str(updated_by),
        "fingerprint_index_version": int(fingerprint_index_version),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp_path.replace(path)
    except OSError as exc:
        raise BugIdLockError(f"failed to update BUG id allocator state: {path}") from exc
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
    return payload


def _fingerprint_index_path(root: Path, fingerprint: str) -> Path:
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return root / ".fingerprints" / f"{digest}.json"


def write_fingerprint_index(root: Path, record: dict[str, Any]) -> Path | None:
    """Atomically index one logical BUG fingerprint for direct lookup."""

    fingerprint = str(record.get("fingerprint") or "").strip()
    if not fingerprint:
        return None
    path = _fingerprint_index_path(root, fingerprint)
    payload = {
        "schema_version": FINGERPRINT_INDEX_SCHEMA,
        "fingerprint": fingerprint,
        "bug_id": record.get("bug_id"),
        "status": record.get("status"),
        "title": record.get("title"),
        "reservation_path": record.get("reservation_path"),
        "registry_path": record.get("registry_path") or record.get("bug_json"),
        "github_issue_number": record.get("github_issue_number"),
        "github_issue_url": record.get("github_issue_url"),
        "github_sync_state": record.get("github_sync_state"),
        "updated_at": record.get("updated_at") or record.get("reserved_at"),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp_path.replace(path)
    except OSError as exc:
        raise BugIdLockError(f"failed to update BUG fingerprint index: {path}") from exc
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
    return path


def remove_fingerprint_index(root: Path, fingerprint: str | None, *, bug_id: str | None = None) -> bool:
    """Remove an exact index entry, optionally only when it belongs to ``bug_id``."""

    normalized = str(fingerprint or "").strip()
    if not normalized:
        return False
    path = _fingerprint_index_path(root, normalized)
    if bug_id is not None and path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            return False
        if str(payload.get("bug_id") or "").upper() != str(bug_id).upper():
            return False
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def bootstrap_fingerprint_index(root: Path, records: Iterable[dict[str, Any]]) -> int:
    """Build the direct index during an explicit one-time bounded migration."""

    indexed = 0
    for record in records:
        if str(record.get("status") or "") == "released":
            continue
        if write_fingerprint_index(root, record) is not None:
            indexed += 1
    return indexed


def find_matching_reservation(root: Path, fingerprint: str | None) -> dict[str, Any] | None:
    """Return an exact fingerprint match without scanning reservation files."""

    normalized = str(fingerprint or "").strip()
    if not normalized:
        return None
    path = _fingerprint_index_path(root, normalized)
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise BugIdLockError(f"invalid BUG fingerprint index: {path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != FINGERPRINT_INDEX_SCHEMA:
        raise BugIdLockError(f"invalid BUG fingerprint index schema: {path}")
    if str(payload.get("fingerprint") or "") != normalized:
        raise BugIdLockError(f"BUG fingerprint index collision or corruption: {path}")
    if str(payload.get("status") or "") == "released":
        return None
    return {**payload, "fingerprint_index_path": str(path)}


def compact_terminal_reservations(
    root: Path,
    durable_bug_ids: Iterable[str],
    *,
    min_age_seconds: float = DEFAULT_TERMINAL_RETENTION_SECONDS,
) -> list[str]:
    """Remove terminal reservations only after their BUG is durable locally.

    Fresh terminal records are retained briefly so callers that passed their
    pre-lock registry check still observe the same logical intake. Unknown
    GitHub outcomes and incomplete registrations are deliberately kept.
    Callers must invoke this while holding :class:`GlobalBugIdLock`.
    """

    durable = {str(value).strip().upper() for value in durable_bug_ids if str(value).strip()}
    minimum_age = max(0.0, float(min_age_seconds))
    now = time.time()
    removed: list[str] = []
    for record in read_reservations(root):
        bug_id = str(record.get("bug_id") or "").upper()
        status = str(record.get("status") or "")
        legacy_durable = not status and bug_id in durable
        if bug_id not in durable or (status not in TERMINAL_RESERVATION_STATUSES and not legacy_durable):
            continue
        path = Path(str(record.get("reservation_path") or ""))
        try:
            age_seconds = max(0.0, now - path.stat().st_mtime)
        except OSError:
            continue
        if age_seconds < minimum_age:
            continue
        try:
            path.unlink()
            removed.append(str(path))
        except FileNotFoundError:
            continue
    return removed


def compact_terminal_reservation(
    root: Path,
    bug_id: str,
    *,
    min_age_seconds: float = DEFAULT_TERMINAL_RETENTION_SECONDS,
) -> str | None:
    """Remove one durable terminal reservation without scanning the inventory.

    Callers must hold :class:`GlobalBugIdLock`.  The fingerprint index is kept
    because it remains the constant-time duplicate-intake guard after cleanup.
    """

    normalized = str(bug_id or "").strip().upper()
    if not normalized:
        return None
    path = root / f"{normalized}.json"
    try:
        record = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise BugIdLockError(f"invalid BUG reservation: {path}") from exc
    if not isinstance(record, dict) or str(record.get("bug_id") or "").upper() != normalized:
        raise BugIdLockError(f"BUG reservation identity mismatch: {path}")
    status = str(record.get("status") or "")
    if status and status not in TERMINAL_RESERVATION_STATUSES:
        return None
    try:
        age_seconds = max(0.0, time.time() - path.stat().st_mtime)
    except OSError:
        return None
    if age_seconds < max(0.0, float(min_age_seconds)):
        return None
    try:
        path.unlink()
    except FileNotFoundError:
        return None
    return str(path)


class GlobalBugIdLock:
    """Cross-thread and cross-process lock with token-checked release."""

    def __init__(
        self,
        path: Path,
        *,
        timeout: float,
        process_is_alive: Callable[[int], bool | None],
        invalid_metadata_max_age_seconds: float = 1800.0,
        poll_seconds: float = 0.05,
        unlink_retry_seconds: float = 1.0,
    ) -> None:
        self.path = Path(path)
        self.timeout = max(0.0, float(timeout))
        self.process_is_alive = process_is_alive
        self.invalid_metadata_max_age_seconds = max(0.0, float(invalid_metadata_max_age_seconds))
        self.poll_seconds = max(0.01, float(poll_seconds))
        self.unlink_retry_seconds = max(0.0, float(unlink_retry_seconds))
        self._fd: int | None = None
        self._local_lock = _process_lock(self.path)
        self._local_acquired = False
        self._owned_snapshot: str | None = None
        self.wait_seconds: float | None = None
        self.hold_seconds: float | None = None
        self._acquired_monotonic: float | None = None

    def _read_snapshot(self) -> str | None:
        try:
            return self.path.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            return None
        except OSError:
            return ""

    def _unlink_if_snapshot_matches(self, expected: str, *, deadline: float) -> bool:
        while True:
            current = self._read_snapshot()
            if current is None:
                return True
            if current != expected:
                return False
            try:
                self.path.unlink()
                return True
            except FileNotFoundError:
                return True
            except PermissionError:
                if time.monotonic() >= deadline:
                    return False
                time.sleep(self.poll_seconds)
            except OSError:
                return False

    def _reclaim_if_stale(self) -> bool:
        snapshot = self._read_snapshot()
        if snapshot is None:
            return True
        try:
            age_seconds = max(0.0, time.time() - self.path.stat().st_mtime)
        except OSError:
            return False
        pid, thread_id = _parse_owner(snapshot)
        process_alive = self.process_is_alive(pid) if pid is not None else None
        same_process_dead_thread = (
            pid == os.getpid() and thread_id is not None and _thread_is_alive(thread_id) is False
        )
        stale = (
            process_alive is False
            or same_process_dead_thread
            or (pid is None and age_seconds >= self.invalid_metadata_max_age_seconds)
        )
        if not stale:
            return False
        return self._unlink_if_snapshot_matches(
            snapshot,
            deadline=time.monotonic() + self.unlink_retry_seconds,
        )

    def __enter__(self) -> "GlobalBugIdLock":
        wait_started = time.monotonic()
        deadline = time.monotonic() + self.timeout
        if not self._local_lock.acquire(timeout=self.timeout):
            raise BugIdLockError(f"timed out waiting for in-process BUG id allocator lock: {self.path}")
        self._local_acquired = True
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            token = uuid.uuid4().hex
            payload = {
                "schema_version": "aistock_bug_id_lock_v2",
                "pid": os.getpid(),
                "thread_id": threading.get_ident(),
                "host": socket.gethostname(),
                "token": token,
                "created_unix": time.time(),
            }
            snapshot = json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
            while True:
                try:
                    self._fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    encoded = snapshot.encode("utf-8")
                    try:
                        written = os.write(self._fd, encoded)
                        if written != len(encoded):
                            raise OSError(f"short BUG id lock write: {written}/{len(encoded)} bytes")
                    except OSError as exc:
                        os.close(self._fd)
                        self._fd = None
                        try:
                            self.path.unlink()
                        except FileNotFoundError:
                            pass
                        except OSError as cleanup_exc:
                            raise BugIdLockError(
                                f"failed to initialize and clean BUG id allocator lock: {self.path}"
                            ) from cleanup_exc
                        raise BugIdLockError(f"failed to initialize BUG id allocator lock: {self.path}") from exc
                    self._owned_snapshot = snapshot
                    self.wait_seconds = max(0.0, time.monotonic() - wait_started)
                    self._acquired_monotonic = time.monotonic()
                    return self
                except FileExistsError as exc:
                    if self._reclaim_if_stale():
                        continue
                    if time.monotonic() >= deadline:
                        raise BugIdLockError(f"timed out waiting for global BUG id allocator lock: {self.path}") from exc
                    time.sleep(self.poll_seconds)
        except Exception:
            self._release_local()
            raise

    def _release_local(self) -> None:
        if self._local_acquired:
            self._local_acquired = False
            self._local_lock.release()

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        release_error: BugIdLockError | None = None
        try:
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None
            if self._owned_snapshot is not None:
                removed = self._unlink_if_snapshot_matches(
                    self._owned_snapshot,
                    deadline=time.monotonic() + self.unlink_retry_seconds,
                )
                if not removed:
                    release_error = BugIdLockError(
                        f"BUG id allocator lock ownership changed or could not be released: {self.path}"
                    )
        finally:
            if self._acquired_monotonic is not None:
                self.hold_seconds = max(0.0, time.monotonic() - self._acquired_monotonic)
            self._release_local()
        if release_error is not None and _exc_type is None:
            raise release_error

    def telemetry(self) -> dict[str, float | None]:
        return {
            "wait_ms": round(self.wait_seconds * 1000.0, 3) if self.wait_seconds is not None else None,
            "hold_ms": round(self.hold_seconds * 1000.0, 3) if self.hold_seconds is not None else None,
        }
