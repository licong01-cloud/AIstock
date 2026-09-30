from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.dataset_release.profile import ResourcePolicy
from backend.services.dataset_release.windows_job import (
    JobAccounting,
    JobChild,
    WindowsJob,
    WindowsJobError,
)


class FakeBackend:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.active = 0
        self.fail_assign = False

    def create_job(self, name: str) -> object:
        self.calls.append(("create_job", name))
        return "job-handle"

    def configure_job(self, handle: object) -> None:
        self.calls.append(("configure", handle))

    def create_suspended(self, command, *, cwd: Path, env) -> JobChild:
        self.calls.append(("create_suspended", tuple(command), cwd, dict(env)))
        return JobChild(
            pid=123,
            process_handle="process",
            thread_handle="thread",
            owner_pipe_handle="owner-pipe",
        )

    def assign(self, job_handle: object, process_handle: object) -> None:
        self.calls.append(("assign", job_handle, process_handle))
        if self.fail_assign:
            raise WindowsJobError("fixture assign failure")
        self.active = 1

    def resume(self, thread_handle: object) -> None:
        self.calls.append(("resume", thread_handle))

    def terminate(self, process_handle: object, exit_code: int) -> None:
        self.calls.append(("terminate", process_handle, exit_code))
        self.active = 0

    def query(self, job_handle: object) -> JobAccounting:
        self.calls.append(("query", job_handle))
        return JobAccounting(64, 128, self.active)

    def close(self, handle: object) -> None:
        self.calls.append(("close", handle))


def test_child_is_assigned_before_first_instruction(tmp_path: Path) -> None:
    backend = FakeBackend()
    job = WindowsJob("dataset-attempt-1", policy=ResourcePolicy(), hybrid_wsl=False, backend=backend)
    child = job.launch(["python", "fixture.py"], cwd=tmp_path, env={"SAFE": "1"})
    assert child.pid == 123
    names = [call[0] for call in backend.calls]
    assert names.index("create_suspended") < names.index("assign") < names.index("resume")
    assert ("close", "thread") in backend.calls
    assert ("close", "owner-pipe") not in backend.calls
    backend.active = 0
    job.close()
    assert ("close", "owner-pipe") in backend.calls


def test_assignment_failure_terminates_only_suspended_task_child(tmp_path: Path) -> None:
    backend = FakeBackend()
    backend.fail_assign = True
    job = WindowsJob("dataset-attempt-2", policy=ResourcePolicy(), hybrid_wsl=False, backend=backend)
    with pytest.raises(WindowsJobError, match="fixture assign failure"):
        job.launch(["python", "fixture.py"], cwd=tmp_path, env={})
    assert ("terminate", "process", 70) in backend.calls
    assert ("close", "owner-pipe") in backend.calls
    assert ("resume", "thread") not in backend.calls


def test_normal_close_refuses_active_children() -> None:
    backend = FakeBackend()
    job = WindowsJob("dataset-attempt-3", policy=ResourcePolicy(), hybrid_wsl=False, backend=backend)
    backend.active = 1
    with pytest.raises(WindowsJobError, match="children are active"):
        job.close()
    assert ("close", "job-handle") not in backend.calls
    backend.active = 0
    job.close()
    assert ("close", "job-handle") in backend.calls


@pytest.mark.parametrize("name", ["", "bad name", "../escape", "x" * 121])
def test_job_name_is_strictly_allowlisted(name: str) -> None:
    with pytest.raises(WindowsJobError, match="invalid Windows Job name"):
        WindowsJob(name, policy=ResourcePolicy(), hybrid_wsl=False, backend=FakeBackend())


def test_job_owns_processes_without_installing_aistock_memory_limits() -> None:
    windows = WindowsJob(
        "dataset-attempt-windows",
        policy=ResourcePolicy(),
        hybrid_wsl=False,
        backend=FakeBackend(),
    )
    hybrid = WindowsJob(
        "dataset-attempt-hybrid",
        policy=ResourcePolicy(),
        hybrid_wsl=True,
        backend=FakeBackend(),
    )
    assert windows.memory_limit_bytes is None
    assert hybrid.memory_limit_bytes is None
    assert ("configure", "job-handle") in windows._backend.calls
    assert ("configure", "job-handle") in hybrid._backend.calls


def test_hybrid_selector_must_be_explicit_boolean() -> None:
    with pytest.raises(WindowsJobError, match="must be a boolean"):
        WindowsJob(
            "dataset-attempt",
            policy=ResourcePolicy(),
            hybrid_wsl=1,  # type: ignore[arg-type]
            backend=FakeBackend(),
        )
