from __future__ import annotations

import argparse
import json
import os
import re
import select
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Protocol, Sequence


class GuardianError(RuntimeError):
    """Identity, heartbeat or cgroup contract failure."""


_IDENTITY = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
_TELEMETRY_SCHEMA = "dataset_release_wsl_resource_telemetry_v1"
_TELEMETRY_LIMIT_BYTES = 64 * 1024
_RESOURCE_CHECKPOINT_ENV = "DATASET_RESOURCE_CHECKPOINT_FILE"
_QUIESCENCE_SCHEMA = "dataset_release_wsl_cgroup_quiescence_v1"
_CHILD_ENV_ALLOWLIST = (
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "LANG",
    "LC_ALL",
    "TMPDIR",
    "SSL_CERT_FILE",
    "REQUESTS_CA_BUNDLE",
)


@dataclass(frozen=True)
class GuardianHeartbeat:
    attempt_id: str
    fence: int
    counter: int
    host_utc: datetime

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> "GuardianHeartbeat":
        try:
            observed = datetime.fromisoformat(str(raw["host_utc"]).replace("Z", "+00:00"))
            heartbeat = cls(
                attempt_id=str(raw["attempt_id"]),
                fence=int(raw["fence"]),
                counter=int(raw["counter"]),
                host_utc=observed,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise GuardianError("guardian heartbeat schema is invalid") from exc
        if heartbeat.host_utc.tzinfo is None:
            raise GuardianError("guardian heartbeat timestamp must be timezone-aware")
        if heartbeat.fence <= 0 or heartbeat.counter < 0:
            raise GuardianError("guardian heartbeat fence/counter is invalid")
        return heartbeat


class ChildProcess(Protocol):
    pid: int

    def poll(self) -> int | None: ...


HeartbeatLoader = Callable[[Path], GuardianHeartbeat]
PipeProbe = Callable[[], bool]
CgroupReader = Callable[[str], Mapping[str, object]]


def load_heartbeat(path: Path) -> GuardianHeartbeat:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GuardianError("guardian heartbeat cannot be read") from exc
    if not isinstance(raw, dict):
        raise GuardianError("guardian heartbeat must be a JSON object")
    return GuardianHeartbeat.from_mapping(raw)


def parent_pipe_alive() -> bool:
    try:
        ready, _write, _error = select.select([sys.stdin.buffer], [], [], 0)
    except (OSError, ValueError):
        return False
    if not ready:
        return True
    try:
        value = os.read(sys.stdin.fileno(), 1)
    except OSError:
        return False
    return bool(value)


class WslResourceGuardian:
    """MainPID for a transient systemd service.

    It intentionally does not terminate descendants itself. Exiting non-zero
    makes systemd KillMode=control-group fail-stop only this transient unit.
    """

    def __init__(
        self,
        *,
        attempt_id: str,
        fence: int,
        heartbeat_path: Path,
        ttl_seconds: float,
        command: Sequence[str],
        telemetry_path: Path,
        memory_high_bytes: int,
        memory_max_bytes: int,
        memory_swap_max_bytes: int,
        start_available_bytes: int,
        resource_checkpoint_path: str,
        heartbeat_loader: HeartbeatLoader = load_heartbeat,
        pipe_probe: PipeProbe = parent_pipe_alive,
        control_group_loader: Callable[[], str] = lambda: discover_self_control_group(),
        cgroup_reader: CgroupReader = lambda value: read_cgroup_files(value),
        popen_factory: Callable[..., ChildProcess] = subprocess.Popen,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not _IDENTITY.fullmatch(str(attempt_id)) or fence <= 0 or ttl_seconds < 3:
            raise GuardianError("guardian identity/TTL is invalid")
        if not command or any("\x00" in str(part) for part in command):
            raise GuardianError("guardian child command is invalid")
        if (
            not telemetry_path.is_absolute()
            or any(
                type(value) is not int or value < 0
                for value in (
                    memory_high_bytes,
                    memory_max_bytes,
                    memory_swap_max_bytes,
                    start_available_bytes,
                )
            )
            or memory_high_bytes <= 0
            or memory_max_bytes <= memory_high_bytes
            or start_available_bytes <= 0
            or not str(resource_checkpoint_path).startswith("/")
            or "\x00" in str(resource_checkpoint_path)
        ):
            raise GuardianError("guardian resource telemetry contract is invalid")
        _assert_plain_telemetry_parent(telemetry_path)
        self.attempt_id = attempt_id
        self.fence = int(fence)
        self.heartbeat_path = heartbeat_path
        self.ttl_seconds = float(ttl_seconds)
        self.command = tuple(str(part) for part in command)
        self.telemetry_path = telemetry_path
        self.memory_high_bytes = int(memory_high_bytes)
        self.memory_max_bytes = int(memory_max_bytes)
        self.memory_swap_max_bytes = int(memory_swap_max_bytes)
        self.start_available_bytes = int(start_available_bytes)
        self.resource_checkpoint_path = str(resource_checkpoint_path)
        self._load = heartbeat_loader
        self._pipe = pipe_probe
        self._control_group_loader = control_group_loader
        self._read_cgroup = cgroup_reader
        self._popen = popen_factory
        self._now = now
        self._sleep = sleep
        self._last_counter = -1
        self._telemetry_counter = 0
        self._control_group: str | None = None
        self._wrapper_pid = 0

    def _heartbeat_is_fresh(self) -> bool:
        heartbeat = self._load(self.heartbeat_path)
        if heartbeat.attempt_id != self.attempt_id or heartbeat.fence != self.fence:
            return False
        if heartbeat.counter < self._last_counter:
            return False
        age = (self._now() - heartbeat.host_utc.astimezone(timezone.utc)).total_seconds()
        if age < -5 or age > self.ttl_seconds:
            return False
        self._last_counter = heartbeat.counter
        return True

    def run(self) -> int:
        try:
            if not self._pipe() or not self._heartbeat_is_fresh():
                return 72
            available = self._sample_resources()
            if available < self.start_available_bytes:
                return 74
        except (GuardianError, OSError, TypeError, ValueError):
            return 72
        child_env = {
            key: str(os.environ[key])
            for key in _CHILD_ENV_ALLOWLIST
            if key in os.environ and "\x00" not in str(os.environ[key])
        }
        child_env[_RESOURCE_CHECKPOINT_ENV] = self.resource_checkpoint_path
        child_env["PYTHONNOUSERSITE"] = "1"
        child = self._popen(list(self.command), shell=False, env=child_env)
        try:
            self._wrapper_pid = int(child.pid)
        except (AttributeError, TypeError, ValueError):
            return 75
        if self._wrapper_pid <= 0:
            return 75
        while True:
            try:
                self._sample_resources()
                healthy = self._pipe() and self._heartbeat_is_fresh()
            except (GuardianError, OSError, TypeError, ValueError):
                healthy = False
            if not healthy:
                return 73
            status = child.poll()
            if status is not None:
                return int(status)
            self._sleep(1.0)

    def _sample_resources(self) -> int:
        control_group = self._control_group or self._control_group_loader()
        if not control_group.startswith("/") or ".." in Path(control_group).parts:
            raise GuardianError("guardian cgroup identity is invalid")
        self._control_group = control_group
        raw = self._read_cgroup(control_group)
        try:
            current = _finite_counter(raw["memory.current"], "memory.current")
            peak = _finite_counter(raw["memory.peak"], "memory.peak")
            high = _finite_counter(raw["memory.high"], "memory.high")
            maximum = _finite_counter(raw["memory.max"], "memory.max")
            swap_current = _finite_counter(raw["memory.swap.current"], "memory.swap.current")
            swap_maximum = _finite_counter(raw["memory.swap.max"], "memory.swap.max")
            oom_group = _finite_counter(raw["memory.oom.group"], "memory.oom.group")
            available = _finite_counter(raw["wsl_mem_available_bytes"], "wsl_mem_available_bytes")
            raw_events = raw["memory.events"]
        except KeyError as exc:
            raise GuardianError("guardian cgroup telemetry is incomplete") from exc
        if not isinstance(raw_events, Mapping):
            raise GuardianError("guardian memory.events telemetry is invalid")
        events = {str(key): _finite_counter(value, f"memory.events.{key}") for key, value in raw_events.items()}
        if (
            high != self.memory_high_bytes
            or maximum != self.memory_max_bytes
            or swap_maximum != self.memory_swap_max_bytes
            or oom_group != 1
            or current > maximum
            or swap_current > swap_maximum
            or peak < current
        ):
            raise GuardianError("guardian cgroup limit/readback mismatch")
        self._telemetry_counter += 1
        _atomic_json(
            self.telemetry_path,
            {
                "schema_version": _TELEMETRY_SCHEMA,
                "attempt_id": self.attempt_id,
                "fence": self.fence,
                "counter": self._telemetry_counter,
                "observed_utc": self._now().astimezone(timezone.utc).isoformat(),
                "control_group": control_group,
                "wrapper_pid": self._wrapper_pid,
                "memory_current_bytes": current,
                "memory_peak_bytes": peak,
                "memory_high_bytes": high,
                "memory_max_bytes": maximum,
                "swap_current_bytes": swap_current,
                "swap_max_bytes": swap_maximum,
                "memory_oom_group": oom_group,
                "wsl_mem_available_bytes": available,
                "memory_events": events,
            },
        )
        return available


def read_cgroup_files(control_group: str) -> dict[str, object]:
    base = Path("/sys/fs/cgroup").resolve(strict=True)
    target = (base / control_group.lstrip("/")).resolve(strict=True)
    if not target.is_relative_to(base):
        raise GuardianError("cgroup path escapes /sys/fs/cgroup")
    names = (
        "memory.current",
        "memory.peak",
        "memory.high",
        "memory.max",
        "memory.swap.current",
        "memory.swap.max",
        "memory.oom.group",
    )
    result: dict[str, object] = {"control_group": control_group}
    for name in names:
        value = (target / name).read_text(encoding="ascii").strip()
        result[name] = value if value == "max" else int(value)
    events: dict[str, int] = {}
    for line in (target / "memory.events").read_text(encoding="ascii").splitlines():
        key, value = line.split(maxsplit=1)
        events[key] = int(value)
    result["memory.events"] = events
    result["wsl_mem_available_bytes"] = read_mem_available_bytes()
    return result


def read_cgroup_quiescence(control_group: str) -> dict[str, object]:
    """Read only cgroup existence/population for exact orphan recovery."""

    if not control_group.startswith("/") or ".." in Path(control_group).parts:
        raise GuardianError("guardian quiescence cgroup identity is invalid")
    base = Path("/sys/fs/cgroup").resolve(strict=True)
    unresolved = base / control_group.lstrip("/")
    try:
        target = unresolved.resolve(strict=True)
    except FileNotFoundError:
        parent = unresolved.parent.resolve(strict=True)
        if not parent.is_relative_to(base):
            raise GuardianError("guardian quiescence cgroup escapes cgroup root")
        return {
            "schema_version": _QUIESCENCE_SCHEMA,
            "control_group": control_group,
            "state": "absent",
            "populated": 0,
            "process_count": 0,
        }
    if not target.is_relative_to(base):
        raise GuardianError("guardian quiescence cgroup escapes cgroup root")
    try:
        events_raw = (target / "cgroup.events").read_text(encoding="ascii")
        procs_raw = (target / "cgroup.procs").read_text(encoding="ascii")
    except (OSError, UnicodeDecodeError) as exc:
        raise GuardianError("guardian cgroup quiescence cannot be read") from exc
    if len(events_raw) > 64 * 1024 or len(procs_raw) > 64 * 1024:
        raise GuardianError("guardian cgroup quiescence readback is unbounded")
    events: dict[str, int] = {}
    try:
        for line in events_raw.splitlines():
            key, value = line.split(maxsplit=1)
            events[key] = int(value)
        processes = [int(value) for value in procs_raw.split()]
    except ValueError as exc:
        raise GuardianError("guardian cgroup quiescence readback is invalid") from exc
    populated = events.get("populated")
    if populated not in {0, 1} or any(value <= 0 for value in processes):
        raise GuardianError("guardian cgroup quiescence counters are invalid")
    state = "empty" if populated == 0 and not processes else "populated"
    if (state == "empty") is not (not processes and populated == 0):
        raise GuardianError("guardian cgroup population evidence conflicts")
    return {
        "schema_version": _QUIESCENCE_SCHEMA,
        "control_group": control_group,
        "state": state,
        "populated": populated,
        "process_count": len(processes),
    }


def read_mem_available_bytes(path: Path = Path("/proc/meminfo")) -> int:
    try:
        for line in path.read_text(encoding="ascii").splitlines():
            if line.startswith("MemAvailable:"):
                fields = line.split()
                if len(fields) != 3 or fields[2] != "kB":
                    break
                value = int(fields[1]) * 1024
                if value >= 0:
                    return value
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise GuardianError("WSL MemAvailable telemetry is unavailable") from exc
    raise GuardianError("WSL MemAvailable telemetry is unavailable")


def discover_self_control_group(path: Path = Path("/proc/self/cgroup")) -> str:
    try:
        matches = [
            line.split("::", 1)[1].strip()
            for line in path.read_text(encoding="ascii").splitlines()
            if line.startswith("0::")
        ]
    except (OSError, UnicodeDecodeError) as exc:
        raise GuardianError("guardian cannot discover its cgroup v2 identity") from exc
    if len(matches) != 1 or not matches[0].startswith("/") or ".." in Path(matches[0]).parts:
        raise GuardianError("guardian cgroup v2 identity is invalid")
    return matches[0]


def _finite_counter(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise GuardianError(f"{field} is not a finite integer")
    if isinstance(value, str) and (not value.isdigit() or value == "max"):
        raise GuardianError(f"{field} is not a finite integer")
    result = int(value)
    if result < 0:
        raise GuardianError(f"{field} is negative")
    return result


def _assert_plain_telemetry_parent(path: Path) -> None:
    try:
        parent = path.parent.resolve(strict=True)
    except OSError as exc:
        raise GuardianError("guardian telemetry parent is unavailable") from exc
    current = Path(parent.parts[0])
    for part in parent.parts[1:]:
        current = current / part
        metadata = current.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise GuardianError("guardian telemetry path contains a symlink")
    if path.exists() and stat.S_ISLNK(path.lstat().st_mode):
        raise GuardianError("guardian telemetry path is a symlink")


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if not 0 < len(data) <= _TELEMETRY_LIMIT_BYTES:
        raise GuardianError("guardian telemetry exceeds its bounded contract")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
    finally:
        if descriptor != -1:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Guard an identity-bound WSL dataset child.")
    parser.add_argument("--read-cgroup", default=None)
    parser.add_argument("--read-quiescence", default=None)
    parser.add_argument("--attempt-id")
    parser.add_argument("--fence", type=int)
    parser.add_argument("--heartbeat")
    parser.add_argument("--ttl-seconds", type=float, default=None)
    parser.add_argument("--telemetry")
    parser.add_argument("--memory-high-bytes", type=int)
    parser.add_argument("--memory-max-bytes", type=int)
    parser.add_argument("--memory-swap-max-bytes", type=int)
    parser.add_argument("--wsl-start-available-bytes", type=int)
    parser.add_argument("--resource-checkpoint")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.read_cgroup:
        print(json.dumps(read_cgroup_files(args.read_cgroup), sort_keys=True, separators=(",", ":")))
        return 0
    if args.read_quiescence:
        print(
            json.dumps(
                read_cgroup_quiescence(args.read_quiescence),
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if (
        not args.attempt_id
        or not args.fence
        or not args.heartbeat
        or args.ttl_seconds is None
        or not args.telemetry
        or args.memory_high_bytes is None
        or args.memory_max_bytes is None
        or args.memory_swap_max_bytes is None
        or args.wsl_start_available_bytes is None
        or not args.resource_checkpoint
        or not command
    ):
        raise SystemExit("guardian requires attempt, fence, heartbeat, TTL and command")
    guardian = WslResourceGuardian(
        attempt_id=args.attempt_id,
        fence=args.fence,
        heartbeat_path=Path(args.heartbeat),
        ttl_seconds=args.ttl_seconds,
        command=command,
        telemetry_path=Path(args.telemetry),
        memory_high_bytes=args.memory_high_bytes,
        memory_max_bytes=args.memory_max_bytes,
        memory_swap_max_bytes=args.memory_swap_max_bytes,
        start_available_bytes=args.wsl_start_available_bytes,
        resource_checkpoint_path=args.resource_checkpoint,
    )
    return guardian.run()


if __name__ == "__main__":
    raise SystemExit(main())
