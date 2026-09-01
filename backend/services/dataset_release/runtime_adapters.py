"""Strict runtime adapters used by the production dataset-release Worker."""

from __future__ import annotations

import json
import hashlib
import os
import re
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Literal, Mapping

from .control_store import ControlStore
from .lease import ClaimedAttempt
from .publisher import DatasetPublisher
from .resource_supervisor import WSL_GUARDIAN_STATE_SCHEMA
from .worker import LeaseOwnerSnapshot, PublishRecoveryConflict


_ATTEMPT_ID = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
_EXECUTION_ID = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
_MAX_GUARDIAN_STATE_BYTES = 64 * 1024
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_DISTRO = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
_RECOVERY_SCHEMA = "dataset_release_wsl_guardian_recovery_v1"
_CGROUP_QUIESCENCE_SCHEMA = "dataset_release_wsl_cgroup_quiescence_v1"


@dataclass(frozen=True, slots=True)
class FencedPublishRecoveryAdapter:
    """Bind publisher recovery to the finalizer attempt and both held fences."""

    store: ControlStore
    publisher: DatasetPublisher

    def recover_and_finalize(
        self,
        *,
        run: Mapping[str, Any],
        claim: ClaimedAttempt,
    ) -> Mapping[str, Any]:
        run_id = str(run.get("run_id", ""))
        if (
            not run_id
            or run.get("state") != "PUBLISHING"
            or run.get("active_attempt_id") != claim.attempt_id
            or claim.host is None
            or claim.release is None
        ):
            raise PublishRecoveryConflict("publish recovery run/finalizer identity differs")
        attempt = self.store.get_attempt(claim.attempt_id)
        records = self.store._many("SELECT * FROM publish_records WHERE run_id=? LIMIT 2", (run_id,))
        if len(records) != 1 or attempt is None:
            raise PublishRecoveryConflict("publish recovery durable record is missing or ambiguous")
        record = records[0]
        release_id = str(record.get("release_id", ""))
        expected_attempt = {
            "run_id": run_id,
            "state": "RUNNING",
            "attempt_kind": "FINALIZER_RECOVERY",
            "attempt_fence": claim.attempt_fence,
            "host_fence": claim.host.fence,
            "release_fence": claim.release.fence,
        }
        if any(attempt.get(key) != value for key, value in expected_attempt.items()):
            raise PublishRecoveryConflict("publish finalizer attempt/fence differs")
        if (
            record.get("state") not in {"PREPARED", "FILES_COMMITTED", "COMMITTED"}
            or record.get("run_id") != run_id
            or record.get("finalized_by_attempt_id") != claim.attempt_id
            or int(record.get("finalized_by_fence") or 0) != claim.attempt_fence
            or claim.host.resource_key != "host:heavy-dataset"
            or claim.release.resource_key != f"release:{release_id}"
        ):
            raise PublishRecoveryConflict("publish record/finalizer identity differs")
        for token in (claim.host, claim.release):
            lease = self.store.get_lease(token.resource_key)
            if (
                lease is None
                or lease.get("state") != "ACTIVE"
                or lease.get("attempt_id") != claim.attempt_id
                or int(lease.get("fence_counter") or 0) != token.fence
                or lease.get("owner_identity") != token.owner_identity
            ):
                raise PublishRecoveryConflict("publish finalizer lease/fence differs")
        return self.publisher.recover_and_finalize(release_id)


@dataclass(frozen=True, slots=True)
class WslSystemdQuiescenceProbe:
    """Read one exact systemd unit and its prior cgroup without process control."""

    expected_distro: str
    guardian_python: str
    guardian_script_wsl: str
    timeout_seconds: float = 10.0
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run

    def __post_init__(self) -> None:
        if (
            _DISTRO.fullmatch(self.expected_distro) is None
            or not self.guardian_python.startswith("/")
            or not self.guardian_script_wsl.startswith("/")
            or self.timeout_seconds <= 0
            or any(
                "\x00" in value
                for value in (
                    self.expected_distro,
                    self.guardian_python,
                    self.guardian_script_wsl,
                )
            )
        ):
            raise ValueError("WSL recovery probe configuration is invalid")

    def __call__(self, state: Mapping[str, Any]) -> Mapping[str, Any]:
        distro = str(state.get("distro", ""))
        unit = str(state.get("unit", ""))
        expected_group = state.get("control_group")
        if (
            distro != self.expected_distro
            or _DISTRO.fullmatch(distro) is None
            or not re.fullmatch(r"aistock-dataset-[A-Za-z0-9_.-]+-[1-9][0-9]*\.service", unit)
            or (expected_group is not None and not _valid_control_group(expected_group))
        ):
            return {"state": "unknown"}
        show = self.runner(
            [
                "wsl.exe",
                "-d",
                distro,
                "--",
                "/usr/bin/env",
                "LC_ALL=C",
                "LANG=C",
                "systemctl",
                "--user",
                "show",
                unit,
                "--property=LoadState,ActiveState,SubState,ControlGroup,MainPID",
                "--no-pager",
            ],
            check=False,
            text=True,
            capture_output=True,
            timeout=self.timeout_seconds,
        )
        if (
            show.returncode not in {0, 4}
            or len(show.stdout) > _MAX_GUARDIAN_STATE_BYTES
            or len(show.stderr) > _MAX_GUARDIAN_STATE_BYTES
        ):
            return {"state": "unknown"}
        required = {"LoadState", "ActiveState", "SubState", "ControlGroup", "MainPID"}
        strict_not_found = False
        if show.returncode == 4:
            if (
                show.stdout.strip()
                or re.fullmatch(
                    rf"Unit {re.escape(unit)} could not be found\.\r?\n?",
                    show.stderr,
                )
                is None
            ):
                return {"state": "unknown"}
            values = {
                "LoadState": "not-found",
                "ActiveState": "inactive",
                "SubState": "dead",
                "ControlGroup": "",
                "MainPID": "0",
            }
            strict_not_found = True
        else:
            if show.stderr.strip():
                return {"state": "unknown"}
            values = _parse_systemd_show(show.stdout)
            strict_not_found = values.get("LoadState") == "not-found"
        if set(values) != required:
            return {"state": "unknown"}
        try:
            main_pid = int(values["MainPID"])
        except ValueError:
            return {"state": "unknown"}
        observed_group = values["ControlGroup"] or None
        active_state = values["ActiveState"]
        if active_state in {"activating", "active", "reloading", "deactivating"}:
            if (
                main_pid <= 0
                or not _valid_control_group(observed_group)
                or (expected_group is not None and observed_group != expected_group)
            ):
                return {"state": "unknown"}
            return {
                "state": "active",
                "unit_state": active_state,
                "control_group": observed_group,
            }
        if (
            active_state not in {"inactive", "failed"}
            or main_pid != 0
            or values["LoadState"] not in {"loaded", "not-found"}
            or (observed_group is not None and observed_group != expected_group)
        ):
            return {"state": "unknown"}
        if expected_group is None:
            if not strict_not_found:
                return {"state": "unknown"}
            return {
                "state": "quiescent",
                "unit_state": "inactive",
                "load_state": "not-found",
                "sub_state": "dead",
                "control_group": None,
                "cgroup_state": "unit-not-found",
            }
        if not _valid_control_group(expected_group):
            return {"state": "unknown"}
        cgroup = self.runner(
            [
                "wsl.exe",
                "-d",
                distro,
                "--",
                self.guardian_python,
                self.guardian_script_wsl,
                "--read-quiescence",
                str(expected_group),
            ],
            check=False,
            text=True,
            capture_output=True,
            timeout=self.timeout_seconds,
        )
        if (
            cgroup.returncode != 0
            or cgroup.stderr.strip()
            or not 0 < len(cgroup.stdout) <= _MAX_GUARDIAN_STATE_BYTES
            or len(cgroup.stderr) > _MAX_GUARDIAN_STATE_BYTES
        ):
            return {"state": "unknown"}
        try:
            evidence = json.loads(cgroup.stdout)
            populated = int(evidence.get("populated", -1))
            process_count = int(evidence.get("process_count", -1))
        except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
            return {"state": "unknown"}
        if (
            not isinstance(evidence, Mapping)
            or evidence.get("schema_version") != _CGROUP_QUIESCENCE_SCHEMA
            or evidence.get("control_group") != expected_group
            or evidence.get("state") not in {"absent", "empty"}
            or populated != 0
            or process_count != 0
        ):
            return {"state": "unknown"}
        return {
            "state": "quiescent",
            "unit_state": active_state,
            "load_state": values["LoadState"],
            "sub_state": values["SubState"],
            "control_group": expected_group,
            "cgroup_state": evidence["state"],
        }


@dataclass(frozen=True, slots=True)
class DurableWslQuiescenceReader:
    """Read only the latest atomically persisted attempt/fence guardian state."""

    store: ControlStore
    active_ttl_seconds: float
    now: Callable[[], datetime] = lambda: datetime.now(UTC)
    recovery_probe: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.active_ttl_seconds < 3:
            raise ValueError("WSL guardian active TTL must be at least three seconds")

    def __call__(self, owner: LeaseOwnerSnapshot) -> Literal["active", "quiescent", "unknown"]:
        if not owner.hybrid_wsl or _ATTEMPT_ID.fullmatch(owner.attempt_id) is None:
            return "unknown"
        attempt = self.store.get_attempt(owner.attempt_id)
        if attempt is None:
            return "unknown"
        try:
            fence = int(attempt["attempt_fence"])
        except (KeyError, TypeError, ValueError):
            return "unknown"
        if fence <= 0:
            return "unknown"
        path = self.store.root / "guardian_states" / f"{owner.attempt_id}-{fence}.json"
        payload = _read_guardian_state(path, control_root=self.store.root)
        if payload is None:
            return "unknown"
        expected_unit = f"aistock-dataset-{owner.attempt_id}-{fence}.service"
        try:
            observed = datetime.fromisoformat(str(payload["observed_utc"]).replace("Z", "+00:00"))
            launch_count = int(payload["launch_count"])
            active_processes = int(payload["active_processes"])
        except (KeyError, TypeError, ValueError):
            return "unknown"
        if observed.tzinfo is None or observed.utcoffset() is None:
            return "unknown"
        if (
            payload.get("schema_version") != WSL_GUARDIAN_STATE_SCHEMA
            or payload.get("attempt_id") != owner.attempt_id
            or int(payload.get("fence") or 0) != fence
            or payload.get("unit") != expected_unit
            or launch_count < 0
        ):
            return "unknown"
        state = payload.get("state")
        execution_id = payload.get("execution_id")
        control_group = payload.get("control_group")
        distro = payload.get("distro")
        if state == "ACTIVE":
            if (
                launch_count <= 0
                or active_processes <= 0
                or not isinstance(execution_id, str)
                or _EXECUTION_ID.fullmatch(execution_id) is None
                or payload.get("systemd_wait_completed") is not False
                or not isinstance(distro, str)
                or _DISTRO.fullmatch(distro) is None
                or (control_group is not None and not _valid_control_group(control_group))
            ):
                return "unknown"
            state_digest = _mapping_digest(payload)
            recovered = _read_recovery_receipt(
                self.store.root,
                attempt_id=owner.attempt_id,
                fence=fence,
                state_digest=state_digest,
                unit=expected_unit,
                distro=distro,
                control_group=control_group,
            )
            if recovered:
                return "quiescent"
            age = (self.now().astimezone(UTC) - observed.astimezone(UTC)).total_seconds()
            if -5 <= age <= self.active_ttl_seconds:
                return "active"
            if self.recovery_probe is None:
                return "unknown"
            try:
                proof = self.recovery_probe(payload)
            except (OSError, RuntimeError, subprocess.SubprocessError):
                return "unknown"
            if not isinstance(proof, Mapping):
                return "unknown"
            if proof.get("state") == "active":
                return "active"
            if (
                proof.get("state") != "quiescent"
                or proof.get("unit_state") not in {"inactive", "failed"}
                or proof.get("cgroup_state") not in {"absent", "empty", "unit-not-found"}
                or proof.get("control_group") != control_group
                or (proof.get("cgroup_state") == "unit-not-found" and control_group is not None)
            ):
                return "unknown"
            _write_recovery_receipt(
                self.store.root,
                attempt_id=owner.attempt_id,
                fence=fence,
                state_digest=state_digest,
                unit=expected_unit,
                distro=distro,
                control_group=control_group,
                proof=proof,
                observed=self.now(),
            )
            return "quiescent"
        if state != "QUIESCENT" or active_processes != 0:
            return "unknown"
        if launch_count == 0:
            if (
                execution_id is not None
                or distro is not None
                or control_group is not None
                or payload.get("systemd_wait_completed") is not False
            ):
                return "unknown"
            return "quiescent"
        if (
            not isinstance(execution_id, str)
            or _EXECUTION_ID.fullmatch(execution_id) is None
            or not isinstance(distro, str)
            or _DISTRO.fullmatch(distro) is None
            or not _valid_control_group(control_group)
            or payload.get("systemd_wait_completed") is not True
        ):
            return "unknown"
        return "quiescent"


def _valid_control_group(value: object) -> bool:
    return isinstance(value, str) and value.startswith("/") and "\x00" not in value and ".." not in Path(value).parts


def _parse_systemd_show(value: str) -> dict[str, str]:
    output: dict[str, str] = {}
    for line in value.splitlines():
        if "=" not in line:
            continue
        key, raw = line.split("=", 1)
        key = key.strip()
        if key in output:
            return {}
        output[key] = raw.strip()
    return output


def _mapping_digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _recovery_path(root: Path, attempt_id: str, fence: int) -> Path:
    return root / "guardian_recovery_states" / f"{attempt_id}-{fence}.json"


def _read_recovery_receipt(
    root: Path,
    *,
    attempt_id: str,
    fence: int,
    state_digest: str,
    unit: str,
    distro: str,
    control_group: object,
) -> bool:
    path = _recovery_path(root, attempt_id, fence)
    value = _read_guardian_state(path, control_root=root)
    return bool(
        value is not None
        and value.get("schema_version") == _RECOVERY_SCHEMA
        and value.get("attempt_id") == attempt_id
        and int(value.get("fence") or 0) == fence
        and value.get("active_state_sha256") == state_digest
        and value.get("unit") == unit
        and value.get("distro") == distro
        and value.get("control_group") == control_group
        and value.get("state") == "QUIESCENT"
        and value.get("unit_state") in {"inactive", "failed"}
        and value.get("cgroup_state") in {"absent", "empty", "unit-not-found"}
        and not (value.get("cgroup_state") == "unit-not-found" and control_group is not None)
        and int(value.get("active_processes", -1)) == 0
    )


def _write_recovery_receipt(
    root: Path,
    *,
    attempt_id: str,
    fence: int,
    state_digest: str,
    unit: str,
    distro: str,
    control_group: str | None,
    proof: Mapping[str, Any],
    observed: datetime,
) -> None:
    parent = root / "guardian_recovery_states"
    parent.mkdir(parents=False, exist_ok=True)
    parent = parent.resolve(strict=True)
    _assert_plain(parent)
    if not parent.is_relative_to(root.resolve(strict=True)):
        raise OSError("guardian recovery receipt path escapes control root")
    payload = {
        "schema_version": _RECOVERY_SCHEMA,
        "attempt_id": attempt_id,
        "fence": fence,
        "active_state_sha256": state_digest,
        "unit": unit,
        "distro": distro,
        "control_group": control_group,
        "state": "QUIESCENT",
        "unit_state": proof["unit_state"],
        "cgroup_state": proof["cgroup_state"],
        "active_processes": 0,
        "observed_utc": observed.astimezone(UTC).isoformat(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    path = parent / f"{attempt_id}-{fence}.json"
    if path.exists():
        existing = _read_guardian_state(path, control_root=root)
        if existing != payload:
            raise OSError("guardian recovery receipt identity conflicts")
        return
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".partial", dir=parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            existing = _read_guardian_state(path, control_root=root)
            if existing != payload:
                raise OSError("guardian recovery receipt publication conflicts")
    finally:
        temporary.unlink(missing_ok=True)


def _read_guardian_state(path: Path, *, control_root: Path) -> Mapping[str, Any] | None:
    try:
        root = control_root.resolve(strict=True)
        _assert_plain(root)
        parent = path.parent.resolve(strict=True)
        _assert_plain(parent)
        resolved = path.resolve(strict=True)
        _assert_plain(resolved)
        if parent != resolved.parent or not resolved.is_relative_to(root):
            return None
        size = resolved.stat().st_size
        if not 0 < size <= _MAX_GUARDIAN_STATE_BYTES:
            return None
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, Mapping) else None


def _assert_plain(path: Path) -> None:
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or bool(getattr(info, "st_file_attributes", 0) & _REPARSE_POINT):
        raise OSError("guardian state path contains a link or reparse point")


__all__ = [
    "DurableWslQuiescenceReader",
    "FencedPublishRecoveryAdapter",
    "WslSystemdQuiescenceProbe",
]
