"""Read-only, identity-bound process-tree liveness for orphan recovery.

The probe deliberately has no terminate/kill capability.  It returns ``dead``
only when the durable Worker identity is exact, a complete local process
snapshot contains neither that PID/create-time nor descendants, and any
required WSL guardian reports quiescence.  Missing evidence is ``unknown``.
"""

from __future__ import annotations

import re
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable, Literal, Mapping, Sequence

import psutil

from .worker import LeaseOwnerSnapshot


LivenessState = Literal["alive", "dead", "unknown"]
WslQuiescenceState = Literal["active", "quiescent", "unknown"]
_WORKER_INSTANCE = re.compile(r"dsw_[0-9a-f]{32}")
_CODE_SHA = re.compile(r"[0-9a-f]{40,64}")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_CREATE_TIME_TOLERANCE_SECONDS = 0.01


@dataclass(frozen=True, slots=True)
class ProcessSnapshot:
    pid: int
    ppid: int | None
    create_time: float | None


class ProcessSnapshotIncomplete(RuntimeError):
    """The host process table could not be read completely enough to reclaim."""


IdentityReader = Callable[[str], Mapping[str, object]]
SnapshotReader = Callable[[], Sequence[ProcessSnapshot]]
WslQuiescenceReader = Callable[[LeaseOwnerSnapshot], WslQuiescenceState]


class LocalProcessTreeLivenessProbe:
    """Conservative psutil probe bound to one durable Worker heartbeat."""

    def __init__(
        self,
        *,
        identity_reader: IdentityReader,
        snapshot_reader: SnapshotReader | None = None,
        wsl_quiescence_reader: WslQuiescenceReader | None = None,
        local_host: str | None = None,
    ) -> None:
        self._identity_reader = identity_reader
        self._snapshot_reader = snapshot_reader or read_complete_process_snapshot
        self._wsl_quiescence_reader = wsl_quiescence_reader
        self._local_host = str(local_host or socket.gethostname()).strip().casefold()

    def __call__(self, owner: LeaseOwnerSnapshot) -> LivenessState:
        expected_created = self._validated_identity(owner)
        if expected_created is None:
            return "unknown"
        try:
            snapshots = tuple(self._snapshot_reader())
        except (OSError, RuntimeError, psutil.Error):
            return "unknown"
        if not _valid_complete_snapshot(snapshots):
            return "unknown"

        by_pid = {entry.pid: entry for entry in snapshots}
        root = by_pid.get(int(owner.owner_pid or 0))
        expected_timestamp = expected_created.timestamp()
        if root is not None:
            if root.create_time is None:
                return "unknown"
            if _same_create_time(root.create_time, expected_timestamp):
                return "alive"
            # The PID was reused.  It is unsafe to associate the replacement
            # process (or its children) with the durable Worker identity.
            return "unknown"

        descendant_state = _owned_descendant_state(
            snapshots,
            root_pid=int(owner.owner_pid or 0),
            not_before=expected_timestamp,
        )
        if descendant_state == "active":
            return "alive"
        if descendant_state == "unknown":
            return "unknown"
        if owner.hybrid_wsl:
            if self._wsl_quiescence_reader is None:
                return "unknown"
            try:
                wsl_state = self._wsl_quiescence_reader(owner)
            except (OSError, RuntimeError, psutil.Error):
                return "unknown"
            if wsl_state == "active":
                return "alive"
            if wsl_state != "quiescent":
                return "unknown"
        return "dead"

    def _validated_identity(self, owner: LeaseOwnerSnapshot) -> datetime | None:
        if (
            not self._local_host
            or not str(owner.host or "").strip()
            or str(owner.host).strip().casefold() != self._local_host
            or owner.owner_pid is None
            or owner.owner_pid <= 0
            or _WORKER_INSTANCE.fullmatch(str(owner.worker_instance_id or "")) is None
            or _CODE_SHA.fullmatch(str(owner.code_sha or "")) is None
            or _DIGEST.fullmatch(str(owner.capability_digest or "")) is None
            or owner.owner_identity != f"{owner.worker_instance_id}:{str(owner.capability_digest)[:16]}"
        ):
            return None
        try:
            expected_created = _parse_aware(str(owner.owner_create_time or ""))
            heartbeat = self._identity_reader(str(owner.worker_instance_id))
            identity = heartbeat.get("identity")
            if not isinstance(identity, Mapping):
                return None
            heartbeat_created = _parse_aware(str(identity.get("process_create_time", "")))
        except (KeyError, OSError, RuntimeError, TypeError, ValueError, psutil.Error):
            return None
        expected = {
            "instance_id": owner.worker_instance_id,
            "host": owner.host,
            "pid": owner.owner_pid,
            "code_sha": owner.code_sha,
            "capability_digest": owner.capability_digest,
        }
        if any(identity.get(name) != value for name, value in expected.items()):
            return None
        if heartbeat_created != expected_created:
            return None
        return expected_created


def read_complete_process_snapshot() -> tuple[ProcessSnapshot, ...]:
    """Read only PID/PPID/create-time; inability to read is never treated dead."""

    rows: list[ProcessSnapshot] = []
    try:
        iterator = psutil.process_iter(attrs=("pid", "ppid", "create_time"), ad_value=None)
        for process in iterator:
            try:
                info = process.info
                rows.append(
                    ProcessSnapshot(
                        pid=int(info["pid"]),
                        ppid=(int(info["ppid"]) if info.get("ppid") is not None else None),
                        create_time=(float(info["create_time"]) if info.get("create_time") is not None else None),
                    )
                )
            except psutil.NoSuchProcess:
                continue
            except psutil.AccessDenied:
                rows.append(ProcessSnapshot(pid=int(process.pid), ppid=None, create_time=None))
            except (KeyError, TypeError, ValueError) as exc:
                raise ProcessSnapshotIncomplete("process identity snapshot is incomplete") from exc
    except psutil.Error as exc:
        raise ProcessSnapshotIncomplete("process table cannot be enumerated") from exc
    if not rows:
        raise ProcessSnapshotIncomplete("process table snapshot is empty")
    return tuple(rows)


def _valid_complete_snapshot(rows: Sequence[ProcessSnapshot]) -> bool:
    seen: set[int] = set()
    for row in rows:
        if (
            not isinstance(row, ProcessSnapshot)
            or row.pid <= 0
            or (row.ppid is not None and row.ppid < 0)
            or (row.create_time is not None and row.create_time <= 0)
            or row.pid in seen
        ):
            return False
        seen.add(row.pid)
    return bool(rows)


def _owned_descendant_state(
    rows: Sequence[ProcessSnapshot],
    *,
    root_pid: int,
    not_before: float,
) -> Literal["active", "quiescent", "unknown"]:
    children: dict[int, list[ProcessSnapshot]] = {}
    opaque_possible = False
    for row in rows:
        if row.ppid is None:
            if row.create_time is None or (row.create_time + _CREATE_TIME_TOLERANCE_SECONDS >= not_before):
                opaque_possible = True
            continue
        children.setdefault(row.ppid, []).append(row)
    pending = list(children.get(root_pid, ()))
    visited: set[int] = set()
    while pending:
        child = pending.pop()
        if child.pid in visited:
            continue
        visited.add(child.pid)
        if child.create_time is None:
            return "unknown"
        if child.create_time + _CREATE_TIME_TOLERANCE_SECONDS >= not_before:
            return "active"
    return "unknown" if opaque_possible else "quiescent"


def _same_create_time(actual: float, expected: float) -> bool:
    return abs(float(actual) - float(expected)) <= _CREATE_TIME_TOLERANCE_SECONDS


def _parse_aware(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("process create-time must be timezone-aware")
    return parsed.astimezone(UTC)


__all__ = [
    "LivenessState",
    "LocalProcessTreeLivenessProbe",
    "ProcessSnapshot",
    "ProcessSnapshotIncomplete",
    "WslQuiescenceState",
    "read_complete_process_snapshot",
]
