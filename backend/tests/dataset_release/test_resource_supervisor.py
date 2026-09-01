from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.profile import ResourcePolicy
from backend.services.dataset_release.resource_budget import GIB, HostMemorySnapshot
from backend.services.dataset_release.resource_gate import DiskSpaceSnapshot, ResourceGate
from backend.services.dataset_release.resource_supervisor import (
    ResourceSupervisor,
    ResourceSupervisorError,
    WslSupervisedOptions,
    build_supervised_environment,
    _file_chunks,
)
from backend.services.dataset_release.windows_job import JobAccounting, JobChild
from backend.services.dataset_release.wsl_cgroup import WslCgroupReadback, WslCgroupError


def _gate(dataset_profile, *, low_memory: bool = False) -> ResourceGate:
    return ResourceGate(
        dataset_profile,
        host_probe=lambda: HostMemorySnapshot(
            observed_monotonic=1.0,
            available_bytes=(2 * GIB if low_memory else 32 * GIB),
            commit_total_bytes=(78 * GIB if low_memory else 40 * GIB),
            commit_limit_bytes=80 * GIB,
            pagefile_used_bytes=2 * GIB,
            pagefile_limit_bytes=32 * GIB,
            page_reads_per_second=0.0,
            low_memory_signaled=False,
        ),
        disk_probe=lambda predicted: DiskSpaceSnapshot(
            control_free_bytes=128 * GIB,
            candidate_free_bytes=128 * GIB,
            effective_free_bytes=128 * GIB,
            required_free_bytes=32 * GIB,
            predicted_remaining_new_bytes=predicted,
            same_volume=True,
        ),
        sleep=lambda _seconds: None,
    )


def test_supervised_environment_is_minimal_and_source_credentials_are_explicit() -> None:
    parent = {
        "SystemRoot": "C:/Windows",
        "PATH": "C:/safe",
        "SECRET_TOKEN": "unrelated-secret",
        "DATASET_RELEASE_OPERATOR_TOKEN_FILE": "C:/secret/operator.txt",
        "TUSHARE_TOKEN": "source-only-secret",
        "TDX_DB_PASSWORD": "source-db-secret",
    }

    build = build_supervised_environment(scope="build", source=parent)
    source = build_supervised_environment(
        scope="source",
        credential_env_keys=("TUSHARE_TOKEN",),
        source=parent,
    )

    assert build == {
        "SystemRoot": "C:/Windows",
        "PATH": "C:/safe",
        "PYTHONNOUSERSITE": "1",
    }
    assert source["TUSHARE_TOKEN"] == "source-only-secret"
    assert "TDX_DB_PASSWORD" not in source
    assert "SECRET_TOKEN" not in source
    assert "DATASET_RELEASE_OPERATOR_TOKEN_FILE" not in source
    with pytest.raises(ResourceSupervisorError, match="cannot receive"):
        build_supervised_environment(
            scope="build",
            credential_env_keys=("TUSHARE_TOKEN",),
            source=parent,
        )
    with pytest.raises(ResourceSupervisorError, match="override"):
        build_supervised_environment(
            scope="source",
            overrides={"SECRET_TOKEN": "injected"},
            source=parent,
        )


def test_log_cas_input_larger_than_segment_limit_is_never_read_as_one_blob(tmp_path: Path, monkeypatch) -> None:
    artifact = tmp_path / "oversized-fixture.log"
    block = b"x" * (1024 * 1024)
    with artifact.open("wb") as handle:
        for _ in range(17):
            handle.write(block)

    monkeypatch.setattr(
        Path,
        "read_bytes",
        lambda _self: (_ for _ in ()).throw(AssertionError("log CAS registration must stream")),
    )
    total = 0
    maximum = 0
    for chunk in _file_chunks(artifact):
        total += len(chunk)
        maximum = max(maximum, len(chunk))

    assert total == 17 * 1024 * 1024
    assert maximum == 1024 * 1024


class FakeJob:
    instances: list["FakeJob"] = []

    def __init__(self, name: str, *, policy: ResourcePolicy, hybrid_wsl: bool) -> None:
        self.name = name
        self.memory_limit_bytes = None
        self.commands: list[tuple[str, ...]] = []
        self.close_requirements: list[bool] = []
        self.active = 0
        FakeJob.instances.append(self)

    def launch(self, command, *, cwd: Path, env=None) -> JobChild:
        assert cwd.exists()
        self.commands.append(tuple(command))
        return JobChild(pid=123, process_handle="process")

    def accounting(self) -> JobAccounting:
        return JobAccounting(100, 200, self.active)

    def close(self, *, require_quiescent: bool = True) -> None:
        self.close_requirements.append(require_quiescent)


def test_supervisor_writes_identity_bound_heartbeat_and_exposes_no_launch_bypass(
    tmp_path: Path, dataset_profile
) -> None:
    FakeJob.instances.clear()
    ControlStore.initialize(tmp_path)
    supervisor = ResourceSupervisor(
        attempt_id="attempt-1",
        fence=4,
        control_root=tmp_path,
        policy=dataset_profile.resource_policy,
        hybrid_wsl=False,
        resource_gate=_gate(dataset_profile),
        job_factory=FakeJob,
    )
    try:
        payload = json.loads(supervisor.heartbeat_path.read_text(encoding="utf-8"))
        assert payload["attempt_id"] == "attempt-1"
        assert payload["fence"] == 4
        assert payload["counter"] >= 1
        assert FakeJob.instances[0].memory_limit_bytes is None
        assert not hasattr(supervisor, "launch_windows")
        assert not hasattr(supervisor, "launch_wsl")
    finally:
        supervisor.close()
    assert FakeJob.instances[0].close_requirements == [True]


@pytest.mark.parametrize("attempt_id", ("../escape", "C:/escape", "bad\\segment"))
def test_attempt_identity_rejects_path_escape_before_creating_control_artifacts(
    tmp_path: Path, attempt_id: str
) -> None:
    with pytest.raises(ResourceSupervisorError, match="attempt identity"):
        ResourceSupervisor(
            attempt_id=attempt_id,
            fence=1,
            control_root=tmp_path,
            policy=ResourcePolicy(),
            hybrid_wsl=False,
            job_factory=FakeJob,
        )

    assert not (tmp_path / "heartbeats").exists()
    assert not (tmp_path / "attempt_runs").exists()


@pytest.mark.parametrize("execution_id", ("../escape", "C:/escape", "bad\\segment"))
def test_execution_identity_rejects_path_escape_before_creating_attempt_root(
    tmp_path: Path, dataset_profile, execution_id: str
) -> None:
    ControlStore.initialize(tmp_path)
    supervisor = ResourceSupervisor(
        attempt_id="safe-attempt",
        fence=1,
        control_root=tmp_path,
        policy=dataset_profile.resource_policy,
        hybrid_wsl=False,
        resource_gate=_gate(dataset_profile),
        job_factory=FakeJob,
    )
    try:
        with pytest.raises(ResourceSupervisorError, match="execution_id"):
            supervisor.run_supervised(
                ["fixture"],
                execution_id=execution_id,
                cwd=tmp_path,
            )
    finally:
        supervisor.close()

    assert not (tmp_path / "attempt_runs").exists()


class FakeWslService:
    def __init__(self, identity, policy) -> None:
        self.identity = identity
        self.policy = policy

    def launch_command(self, guardian):
        return ["wsl.exe", "--", *guardian]

    def readback(self, *, timeout_seconds: float):
        assert timeout_seconds == 5
        return WslCgroupReadback(
            main_pid=99,
            control_group="/unit",
            memory_high_bytes=0,
            memory_max_bytes=0,
            memory_swap_max_bytes=0,
            active_state="active",
        )


def test_wsl_supervision_rejects_non_absolute_helper_before_launch(tmp_path: Path, dataset_profile) -> None:
    FakeJob.instances.clear()
    ControlStore.initialize(tmp_path)
    supervisor = ResourceSupervisor(
        attempt_id="attempt-2",
        fence=5,
        control_root=tmp_path,
        policy=dataset_profile.resource_policy,
        hybrid_wsl=True,
        resource_gate=_gate(dataset_profile),
        job_factory=FakeJob,
    )
    try:
        with pytest.raises(ResourceSupervisorError, match="paths must be absolute"):
            supervisor.run_supervised(
                ["python", "task.py"],
                execution_id="invalid-wsl-helper",
                cwd=tmp_path,
                runtime="wsl",
                wsl=WslSupervisedOptions(
                    distro="Ubuntu",
                    guardian_python="python3",
                    guardian_script_wsl="/repo/wsl_resource_guardian.py",
                    heartbeat_path_wsl="/mnt/x/control/heartbeat.json",
                    runner_python_wsl="/usr/bin/python3",
                    runner_script_wsl="/repo/subprocess_runner.py",
                    task_cwd_wsl="/repo",
                    execution_root_wsl="/mnt/x/control/attempt",
                ),
            )
        assert FakeJob.instances[0].memory_limit_bytes is None
    finally:
        supervisor.close()


class FailedWslService(FakeWslService):
    def readback(self, *, timeout_seconds: float):
        raise WslCgroupError("fixture unavailable")


def test_wsl_readback_failure_aborts_only_task_job(tmp_path: Path, dataset_profile) -> None:
    FakeJob.instances.clear()
    ControlStore.initialize(tmp_path)
    supervisor = ResourceSupervisor(
        attempt_id="attempt-3",
        fence=6,
        control_root=tmp_path,
        policy=dataset_profile.resource_policy,
        hybrid_wsl=True,
        resource_gate=_gate(dataset_profile),
        job_factory=FakeJob,
        sleep=lambda _seconds: None,
    )
    with pytest.raises(ResourceSupervisorError, match="did not pass readback"):
        supervisor._wsl_telemetry_path = tmp_path / "missing-wsl-telemetry.json"
        supervisor._launch_wsl(
            ["python", "task.py"],
            distro="Ubuntu",
            guardian_python="python3",
            guardian_script_wsl="/repo/wsl_resource_guardian.py",
            heartbeat_path_wsl="/mnt/x/control/heartbeat.json",
            telemetry_path_wsl="/mnt/x/control/wsl-resource.json",
            resource_checkpoint_path_wsl="/mnt/x/control/resource-checkpoint.json",
            cwd=tmp_path,
            service_factory=FailedWslService,
        )
    assert FakeJob.instances[0].close_requirements == [False]


def test_existing_attempt_fence_guardian_state_cannot_be_clobbered(tmp_path: Path, dataset_profile) -> None:
    FakeJob.instances.clear()
    ControlStore.initialize(tmp_path)
    first = ResourceSupervisor(
        attempt_id="attempt-existing-state",
        fence=8,
        control_root=tmp_path,
        policy=dataset_profile.resource_policy,
        hybrid_wsl=True,
        resource_gate=_gate(dataset_profile),
        job_factory=FakeJob,
    )
    state_path = tmp_path / "guardian_states" / "attempt-existing-state-8.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.update(
        {
            "state": "ACTIVE",
            "launch_count": 1,
            "execution_id": "qlib-dump",
            "distro": "Ubuntu",
            "control_group": "/user.slice/fixture.service",
            "active_processes": 1,
        }
    )
    state_path.write_text(json.dumps(state), encoding="utf-8")
    try:
        with pytest.raises(ResourceSupervisorError, match="already exists"):
            ResourceSupervisor(
                attempt_id="attempt-existing-state",
                fence=8,
                control_root=tmp_path,
                policy=dataset_profile.resource_policy,
                hybrid_wsl=True,
                resource_gate=_gate(dataset_profile),
                job_factory=FakeJob,
            )
        assert json.loads(state_path.read_text(encoding="utf-8"))["state"] == "ACTIVE"
        assert len(FakeJob.instances) == 1
    finally:
        first.close()


class ActualFixtureJob:
    instances: list["ActualFixtureJob"] = []

    def __init__(self, name: str, *, policy: ResourcePolicy, hybrid_wsl: bool) -> None:
        self.name = name
        self.policy = policy
        self.hybrid_wsl = hybrid_wsl
        self.processes: list[subprocess.Popen] = []
        self.close_requirements: list[bool] = []
        ActualFixtureJob.instances.append(self)

    def launch(self, command, *, cwd: Path, env=None) -> JobChild:
        process = subprocess.Popen(
            list(command),
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
        )
        self.processes.append(process)
        return JobChild(pid=process.pid, process_handle=process)

    def accounting(self) -> JobAccounting:
        active = sum(process.poll() is None for process in self.processes)
        return JobAccounting(123 if active else 0, 456, active)

    def close(self, *, require_quiescent: bool = True) -> None:
        self.close_requirements.append(require_quiescent)
        if require_quiescent:
            assert all(process.poll() is not None for process in self.processes)


def test_supervised_runner_streams_logs_waits_and_returns_typed_receipt(tmp_path: Path, dataset_profile) -> None:
    ActualFixtureJob.instances.clear()
    ControlStore.initialize(tmp_path)
    supervisor = ResourceSupervisor(
        attempt_id="attempt-runner",
        fence=9,
        control_root=tmp_path,
        policy=dataset_profile.resource_policy,
        hybrid_wsl=False,
        resource_gate=_gate(dataset_profile),
        job_factory=ActualFixtureJob,
    )
    try:
        receipt = supervisor.run_supervised(
            [
                sys.executable,
                "-c",
                "import sys; print('bounded-output'); sys.stderr.write('bounded-error')",
            ],
            execution_id="tiny-success",
            cwd=tmp_path,
            timeout_seconds=10,
            cooperative_grace_seconds=2,
        )
        failed = supervisor.run_supervised(
            [sys.executable, "-c", "import sys; sys.exit(7)"],
            execution_id="tiny-nonzero",
            cwd=tmp_path,
            timeout_seconds=10,
            cooperative_grace_seconds=2,
        )
    finally:
        supervisor.close()

    assert receipt.returncode == 0
    assert receipt.active_processes == 0
    assert receipt.job_peak_commit_bytes == 456
    assert receipt.segment_limit_bytes == 16 * 1024**2
    assert receipt.log_total_bytes == len(b"bounded-output\r\nbounded-error")
    assert {segment["stream"] for segment in receipt.log_segments} == {
        "stdout",
        "stderr",
    }
    cas = CASStore(tmp_path)
    assert all(cas.verify(segment["cas_ref"]).sha256 == segment["sha256"] for segment in receipt.log_segments)
    assert Path(tmp_path / receipt.result_path).is_file()
    assert failed.returncode == 7
    assert failed.active_processes == 0
    assert ActualFixtureJob.instances[0].close_requirements == [True]


def test_low_resource_telemetry_never_writes_a_checkpoint_or_stops_child(tmp_path: Path, dataset_profile) -> None:
    ActualFixtureJob.instances.clear()
    ControlStore.initialize(tmp_path)
    supervisor = ResourceSupervisor(
        attempt_id="attempt-low-telemetry",
        fence=10,
        control_root=tmp_path,
        policy=dataset_profile.resource_policy,
        hybrid_wsl=False,
        resource_gate=_gate(dataset_profile, low_memory=True),
        job_factory=ActualFixtureJob,
    )
    try:
        receipt = supervisor.run_supervised(
            [sys.executable, "-c", "print('completed-under-low-telemetry')"],
            execution_id="low-telemetry-success",
            cwd=tmp_path,
            timeout_seconds=10,
            cooperative_grace_seconds=2,
        )
    finally:
        supervisor.close()

    execution_root = tmp_path / "attempt_runs" / "attempt-low-telemetry-10" / "low-telemetry-success"
    assert receipt.returncode == 0
    assert receipt.resource_gate_receipt["checkpoint_requested"] is False
    assert not (execution_root / "resource_checkpoint.requested.json").exists()


def test_build_child_cannot_see_parent_secrets_and_source_gets_only_declared_key(
    tmp_path: Path, dataset_profile, monkeypatch
) -> None:
    sensitive_text = "fixture-sensitive-value-that-must-not-be-logged"
    monkeypatch.setenv("SECRET_TOKEN", sensitive_text)
    monkeypatch.setenv("DATASET_RELEASE_OPERATOR_TOKEN_FILE", f"{tmp_path}/operator")
    monkeypatch.setenv("TDX_DB_PASSWORD", "undeclared-db-secret")
    monkeypatch.setenv("TUSHARE_TOKEN", "declared-source-secret")
    ControlStore.initialize(tmp_path)
    supervisor = ResourceSupervisor(
        attempt_id="attempt-env",
        fence=11,
        control_root=tmp_path,
        policy=dataset_profile.resource_policy,
        hybrid_wsl=False,
        resource_gate=_gate(dataset_profile),
        job_factory=ActualFixtureJob,
    )
    try:
        build = supervisor.run_supervised(
            [
                sys.executable,
                "-c",
                (
                    "import os; "
                    "assert 'SECRET_TOKEN' not in os.environ; "
                    "assert 'DATASET_RELEASE_OPERATOR_TOKEN_FILE' not in os.environ; "
                    "assert 'TUSHARE_TOKEN' not in os.environ; print('build-env-safe')"
                ),
            ],
            execution_id="build-env",
            cwd=tmp_path,
            environment_scope="build",
        )
        source = supervisor.run_supervised(
            [
                sys.executable,
                "-c",
                (
                    "import os; "
                    "assert os.environ['TUSHARE_TOKEN']=='declared-source-secret'; "
                    "assert 'TDX_DB_PASSWORD' not in os.environ; "
                    "assert 'SECRET_TOKEN' not in os.environ; print('source-env-safe')"
                ),
            ],
            execution_id="source-env",
            cwd=tmp_path,
            environment_scope="source",
            credential_env_keys=("TUSHARE_TOKEN",),
        )
    finally:
        supervisor.close()

    serialized = json.dumps({"build": build.as_dict(), "source": source.as_dict()}, sort_keys=True)
    assert sensitive_text not in serialized
    assert "declared-source-secret" not in serialized
    for receipt in (build, source):
        log_root = tmp_path / receipt.log_root
        assert all(
            sensitive_text.encode() not in (log_root / segment["path"]).read_bytes()
            and b"declared-source-secret" not in (log_root / segment["path"]).read_bytes()
            for segment in receipt.log_segments
        )
