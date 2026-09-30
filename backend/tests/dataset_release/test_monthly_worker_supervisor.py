from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.monthly_worker_supervisor import (
    MONTHLY_WORKER_ENABLED_ENV,
    MONTHLY_WORKER_POLL_SECONDS_ENV,
    MONTHLY_WORKER_SHUTDOWN_SECONDS_ENV,
    MonthlyWorkerProcessSupervisor,
    MonthlyWorkerSupervisorConfig,
    MonthlyWorkerSupervisorError,
)


class _Process:
    def __init__(self, *, running: bool = True, timeout_once: bool = False) -> None:
        self.pid = 321
        self.returncode = None if running else 2
        self.timeout_once = timeout_once
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        if self.timeout_once:
            self.timeout_once = False
            raise subprocess.TimeoutExpired("worker", timeout)
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


def _runtime(*, status: str = "PASS") -> SimpleNamespace:
    return SimpleNamespace(preflight_receipt=lambda: {"status": status})


def test_supervisor_config_is_explicit_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        MONTHLY_WORKER_ENABLED_ENV,
        MONTHLY_WORKER_POLL_SECONDS_ENV,
        MONTHLY_WORKER_SHUTDOWN_SECONDS_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    assert MonthlyWorkerSupervisorConfig.from_env() == (
        MonthlyWorkerSupervisorConfig()
    )

    monkeypatch.setenv(MONTHLY_WORKER_POLL_SECONDS_ENV, "invalid-but-disabled")
    assert MonthlyWorkerSupervisorConfig.from_env() == (
        MonthlyWorkerSupervisorConfig()
    )

    monkeypatch.setenv(MONTHLY_WORKER_ENABLED_ENV, "true")
    monkeypatch.setenv(MONTHLY_WORKER_POLL_SECONDS_ENV, "0.5")
    monkeypatch.setenv(MONTHLY_WORKER_SHUTDOWN_SECONDS_ENV, "10")
    assert MonthlyWorkerSupervisorConfig.from_env() == (
        MonthlyWorkerSupervisorConfig(
            enabled=True,
            poll_seconds=0.5,
            shutdown_seconds=10.0,
        )
    )

    monkeypatch.setenv(MONTHLY_WORKER_POLL_SECONDS_ENV, "nan")
    with pytest.raises(MonthlyWorkerSupervisorError, match="finite"):
        MonthlyWorkerSupervisorConfig.from_env()
    with pytest.raises(MonthlyWorkerSupervisorError, match="poll_seconds"):
        MonthlyWorkerSupervisorConfig(enabled=True, poll_seconds=True)  # type: ignore[arg-type]


def test_disabled_supervisor_does_not_load_runtime_or_start_process(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    supervisor = MonthlyWorkerProcessSupervisor(
        project_root=tmp_path,
        config=MonthlyWorkerSupervisorConfig(enabled=False),
        runtime_loader=lambda **_kwargs: calls.append("runtime"),  # type: ignore[arg-type]
        popen_factory=lambda *_args, **_kwargs: calls.append("popen"),  # type: ignore[arg-type]
    )

    assert supervisor.start() == {
        "status": "DISABLED",
        "enabled": False,
        "pid": None,
    }
    assert calls == []


def test_enabled_supervisor_preflights_and_uses_fixed_worker_command(
    tmp_path: Path,
) -> None:
    script = tmp_path / "scripts" / "monthly_unified_dataset_release_worker.py"
    script.parent.mkdir()
    script.write_text("# worker\n", encoding="utf-8")
    process = _Process()
    captured: dict[str, object] = {}

    def popen(command, **kwargs):  # type: ignore[no-untyped-def]
        captured["command"] = command
        captured["kwargs"] = kwargs
        return process

    supervisor = MonthlyWorkerProcessSupervisor(
        project_root=tmp_path,
        config=MonthlyWorkerSupervisorConfig(
            enabled=True,
            poll_seconds=2.5,
            shutdown_seconds=3.0,
        ),
        runtime_loader=lambda **_kwargs: _runtime(),  # type: ignore[arg-type]
        popen_factory=popen,
    )

    receipt = supervisor.start()

    assert receipt["status"] == "RUNNING"
    assert receipt["preflight"] == {"status": "PASS"}
    assert captured["command"] == (
        __import__("sys").executable,
        str(script),
        "--serve",
        "--poll-seconds",
        "2.5",
    )
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["cwd"] == str(tmp_path)
    assert kwargs["stdin"] == subprocess.DEVNULL
    assert "shell" not in kwargs
    assert supervisor.stop()["status"] == "STOPPED"
    assert process.terminated is True
    assert process.killed is False


def test_enabled_supervisor_fails_closed_before_process_start(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    supervisor = MonthlyWorkerProcessSupervisor(
        project_root=tmp_path,
        config=MonthlyWorkerSupervisorConfig(enabled=True),
        runtime_loader=lambda **_kwargs: _runtime(status="FAILED"),  # type: ignore[arg-type]
        popen_factory=lambda *_args, **_kwargs: calls.append("popen"),  # type: ignore[arg-type]
    )

    with pytest.raises(MonthlyWorkerSupervisorError, match="preflight"):
        supervisor.start()
    assert calls == []


def test_supervisor_kills_child_after_bounded_shutdown_timeout(
    tmp_path: Path,
) -> None:
    script = tmp_path / "scripts" / "monthly_unified_dataset_release_worker.py"
    script.parent.mkdir()
    script.write_text("# worker\n", encoding="utf-8")
    process = _Process(timeout_once=True)
    supervisor = MonthlyWorkerProcessSupervisor(
        project_root=tmp_path,
        config=MonthlyWorkerSupervisorConfig(
            enabled=True,
            shutdown_seconds=1.0,
        ),
        runtime_loader=lambda **_kwargs: _runtime(),  # type: ignore[arg-type]
        popen_factory=lambda *_args, **_kwargs: process,
    )
    supervisor.start()

    receipt = supervisor.stop()

    assert receipt["forced"] is True
    assert receipt["returncode"] == -9
    assert process.terminated is True
    assert process.killed is True
