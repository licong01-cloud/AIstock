from __future__ import annotations

import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.services.dataset_release.profile import ResourcePolicy
from backend.services.dataset_release.resource_budget import GIB
from backend.services.dataset_release.wsl_cgroup import (
    WslCgroupError,
    WslCgroupService,
    WslUnitIdentity,
)
from backend.services.dataset_release.wsl_resource_guardian import (
    GuardianHeartbeat,
    WslResourceGuardian,
    main as guardian_main,
)


def test_systemd_command_freezes_memory_and_control_group_lifecycle() -> None:
    identity = WslUnitIdentity.create("Ubuntu", "attempt-001", 7)
    service = WslCgroupService(identity, ResourcePolicy())
    command = service.launch_command(["python3", "guardian.py", "--", "worker"])
    assert command[:5] == ["wsl.exe", "-d", "Ubuntu", "--", "systemd-run"]
    assert "--property=MemoryHigh=6442450944" in command
    assert "--property=MemoryMax=8589934592" in command
    assert "--property=MemorySwapMax=0" in command
    assert "--property=KillMode=control-group" in command
    assert "--property=SendSIGKILL=yes" in command
    assert "--property=OOMPolicy=kill" in command
    assert "--pipe" in command
    assert "--unit=aistock-dataset-attempt-001-7.service" in command


@pytest.mark.parametrize(
    ("distro", "attempt", "fence"),
    [("bad distro", "attempt", 1), ("Ubuntu", "../escape", 1), ("Ubuntu", "ok", 0)],
)
def test_unit_identity_rejects_injection(distro: str, attempt: str, fence: int) -> None:
    with pytest.raises(WslCgroupError):
        WslUnitIdentity.create(distro, attempt, fence)


def test_readback_requires_exact_limits_and_identity() -> None:
    stdout = "\n".join(
        [
            "MainPID=321",
            "ControlGroup=/user.slice/aistock.service",
            f"MemoryHigh={6 * GIB}",
            f"MemoryMax={8 * GIB}",
            "MemorySwapMax=0",
            "ActiveState=active",
        ]
    )

    def runner(*_args, **_kwargs):
        return subprocess.CompletedProcess([], 0, stdout=stdout, stderr="")

    service = WslCgroupService(
        WslUnitIdentity.create("Ubuntu", "attempt", 1),
        ResourcePolicy(),
        runner=runner,
    )
    readback = service.readback()
    assert readback.main_pid == 321
    assert readback.memory_swap_max_bytes == 0


def test_readback_fails_closed_on_weakened_memory_max() -> None:
    stdout = "\n".join(
        [
            "MainPID=321",
            "ControlGroup=/user.slice/aistock.service",
            f"MemoryHigh={6 * GIB}",
            f"MemoryMax={9 * GIB}",
            "MemorySwapMax=0",
            "ActiveState=active",
        ]
    )

    def runner(*_args, **_kwargs):
        return subprocess.CompletedProcess([], 0, stdout=stdout, stderr="")

    service = WslCgroupService(
        WslUnitIdentity.create("Ubuntu", "attempt", 1),
        ResourcePolicy(),
        runner=runner,
    )
    with pytest.raises(WslCgroupError, match="readback mismatch"):
        service.readback()


def test_cgroup_reader_uses_only_explicit_absolute_helper_and_cwd() -> None:
    service = WslCgroupService(WslUnitIdentity.create("Ubuntu", "attempt", 1), ResourcePolicy())

    command = service.read_memory_files_command(
        "/user.slice/aistock.service",
        python_wsl="/usr/bin/python3",
        guardian_script_wsl="/opt/aistock/wsl_resource_guardian.py",
        cwd_wsl="/opt/aistock",
    )

    assert command == [
        "wsl.exe",
        "-d",
        "Ubuntu",
        "--cd",
        "/opt/aistock",
        "--",
        "/usr/bin/python3",
        "/opt/aistock/wsl_resource_guardian.py",
        "--read-cgroup",
        "/user.slice/aistock.service",
    ]
    assert "-m" not in command


class FakeChild:
    def __init__(self, statuses: list[int | None]) -> None:
        self.statuses = iter(statuses)
        self.pid = 4242

    def poll(self) -> int | None:
        return next(self.statuses)


def _resource_kwargs(tmp_path: Path, *, available: int = 20 * GIB):
    return {
        "telemetry_path": tmp_path / "wsl-resource.json",
        "memory_high_bytes": 6 * GIB,
        "memory_max_bytes": 8 * GIB,
        "memory_swap_max_bytes": 0,
        "start_available_bytes": 12 * GIB,
        "resource_checkpoint_path": "/mnt/x/control/resource-checkpoint.json",
        "control_group_loader": lambda: "/user.slice/aistock.service",
        "cgroup_reader": lambda _control_group: {
            "memory.current": 2 * GIB,
            "memory.peak": 3 * GIB,
            "memory.high": 6 * GIB,
            "memory.max": 8 * GIB,
            "memory.swap.current": 0,
            "memory.swap.max": 0,
            "memory.oom.group": 1,
            "memory.events": {"oom": 0, "oom_kill": 0},
            "wsl_mem_available_bytes": available,
        },
    }


def test_guardian_does_not_start_child_without_fresh_identity_bound_heartbeat(tmp_path: Path) -> None:
    started = False

    def popen(*_args, **_kwargs):
        nonlocal started
        started = True
        return FakeChild([0])

    now = datetime(2026, 8, 11, tzinfo=timezone.utc)
    guardian = WslResourceGuardian(
        attempt_id="attempt",
        fence=2,
        heartbeat_path=tmp_path / "heartbeat.json",
        ttl_seconds=15,
        command=["worker"],
        heartbeat_loader=lambda _path: GuardianHeartbeat("other", 2, 1, now),
        pipe_probe=lambda: True,
        popen_factory=popen,
        now=lambda: now,
        sleep=lambda _seconds: None,
        **_resource_kwargs(tmp_path),
    )
    assert guardian.run() == 72
    assert started is False


def test_guardian_exits_nonzero_on_stale_heartbeat_without_killing_child(tmp_path: Path) -> None:
    now = datetime(2026, 8, 11, tzinfo=timezone.utc)
    child = FakeChild([None])
    guardian = WslResourceGuardian(
        attempt_id="attempt",
        fence=2,
        heartbeat_path=tmp_path / "heartbeat.json",
        ttl_seconds=15,
        command=["worker"],
        heartbeat_loader=lambda _path: GuardianHeartbeat("attempt", 2, 1, now - timedelta(seconds=20)),
        pipe_probe=lambda: True,
        popen_factory=lambda *_args, **_kwargs: child,
        now=lambda: now,
        sleep=lambda _seconds: None,
        **_resource_kwargs(tmp_path),
    )
    # Initial stale heartbeat means the child is never started.
    assert guardian.run() == 72


def test_guardian_returns_child_status_when_heartbeat_and_pipe_remain_healthy(tmp_path: Path, monkeypatch) -> None:
    now = datetime(2026, 8, 11, tzinfo=timezone.utc)
    counters = iter([1, 2, 3])
    child_env = {}
    monkeypatch.setenv("SECRET_TOKEN", "must-not-reach-wsl-child")
    monkeypatch.setenv("TDX_DB_PASSWORD", "must-not-reach-build-child")

    def heartbeat(_path: Path) -> GuardianHeartbeat:
        return GuardianHeartbeat("attempt", 2, next(counters), now)

    def popen(*_args, **kwargs):
        child_env.update(kwargs["env"])
        return FakeChild([None, 0])

    guardian = WslResourceGuardian(
        attempt_id="attempt",
        fence=2,
        heartbeat_path=tmp_path / "heartbeat.json",
        ttl_seconds=15,
        command=["worker"],
        heartbeat_loader=heartbeat,
        pipe_probe=lambda: True,
        popen_factory=popen,
        now=lambda: now,
        sleep=lambda _seconds: None,
        **_resource_kwargs(tmp_path),
    )
    assert guardian.run() == 0
    assert child_env["DATASET_RESOURCE_CHECKPOINT_FILE"] == "/mnt/x/control/resource-checkpoint.json"
    assert "SECRET_TOKEN" not in child_env
    assert "TDX_DB_PASSWORD" not in child_env
    telemetry = (tmp_path / "wsl-resource.json").read_text(encoding="utf-8")
    assert '"memory_current_bytes":2147483648' in telemetry
    assert '"memory_oom_group":1' in telemetry


def test_guardian_loss_exits_for_systemd_control_group_fail_stop(tmp_path: Path) -> None:
    now = datetime(2026, 8, 11, tzinfo=timezone.utc)
    pipes = iter([True, False])
    guardian = WslResourceGuardian(
        attempt_id="attempt",
        fence=2,
        heartbeat_path=tmp_path / "heartbeat.json",
        ttl_seconds=15,
        command=["worker"],
        heartbeat_loader=lambda _path: GuardianHeartbeat("attempt", 2, 1, now),
        pipe_probe=lambda: next(pipes),
        popen_factory=lambda *_args, **_kwargs: FakeChild([None]),
        now=lambda: now,
        sleep=lambda _seconds: None,
        **_resource_kwargs(tmp_path),
    )
    assert guardian.run() == 73


def test_guardian_checks_wsl_available_before_starting_heavy_child(tmp_path: Path) -> None:
    now = datetime(2026, 8, 11, tzinfo=timezone.utc)
    started = False

    def popen(*_args, **_kwargs):
        nonlocal started
        started = True
        return FakeChild([0])

    guardian = WslResourceGuardian(
        attempt_id="attempt",
        fence=2,
        heartbeat_path=tmp_path / "heartbeat.json",
        ttl_seconds=15,
        command=["worker"],
        heartbeat_loader=lambda _path: GuardianHeartbeat("attempt", 2, 1, now),
        pipe_probe=lambda: True,
        popen_factory=popen,
        now=lambda: now,
        sleep=lambda _seconds: None,
        **_resource_kwargs(tmp_path, available=10 * GIB),
    )

    assert guardian.run() == 74
    assert started is False


def test_guardian_cli_has_no_independent_ttl_default() -> None:
    with pytest.raises(SystemExit, match="TTL"):
        guardian_main(
            [
                "--attempt-id",
                "attempt",
                "--fence",
                "2",
                "--heartbeat",
                "/tmp/heartbeat.json",
                "--",
                "worker",
            ]
        )
