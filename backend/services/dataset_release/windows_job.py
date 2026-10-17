from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol, Sequence

import psutil

from .profile import ResourcePolicy, validate_resource_policy


class WindowsJobError(RuntimeError):
    """Fail-closed error for Windows Job creation or admission."""


_JOB_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")


@dataclass(frozen=True)
class JobAccounting:
    current_commit_bytes: int
    peak_commit_bytes: int
    active_processes: int
    current_rss_bytes: int = 0
    peak_rss_bytes: int = 0


@dataclass
class JobChild:
    pid: int
    process_handle: object
    thread_handle: object | None = None
    owner_pipe_handle: object | None = None


class JobBackend(Protocol):
    def create_job(self, name: str) -> object: ...

    def configure_job(self, handle: object) -> None: ...

    def create_suspended(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
    ) -> JobChild: ...

    def assign(self, job_handle: object, process_handle: object) -> None: ...

    def resume(self, thread_handle: object) -> None: ...

    def terminate(self, process_handle: object, exit_code: int) -> None: ...

    def query(self, job_handle: object) -> JobAccounting: ...

    def close(self, handle: object) -> None: ...


class _PyWin32Backend:
    def _modules(self):
        if os.name != "nt":
            raise WindowsJobError("Windows Job Objects are unavailable on this platform")
        try:
            import win32api
            import win32con
            import win32job
            import win32process
        except ImportError as exc:  # pragma: no cover - dependency gate
            raise WindowsJobError("pywin32 is required for Windows Job enforcement") from exc
        return win32api, win32con, win32job, win32process

    def create_job(self, name: str) -> object:
        _api, _con, win32job, _process = self._modules()
        try:
            return win32job.CreateJobObject(None, name)
        except Exception as exc:  # pragma: no cover - OS error
            raise WindowsJobError(f"CreateJobObject failed: {exc}") from exc

    def configure_job(self, handle: object) -> None:
        _api, _con, win32job, _process = self._modules()
        try:
            info = win32job.QueryInformationJobObject(
                handle,
                win32job.JobObjectExtendedLimitInformation,
            )
            flags = int(info["BasicLimitInformation"].get("LimitFlags", 0))
            flags |= int(win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
            flags |= int(win32job.JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION)
            info["BasicLimitInformation"]["LimitFlags"] = flags
            win32job.SetInformationJobObject(
                handle,
                win32job.JobObjectExtendedLimitInformation,
                info,
            )
            readback = win32job.QueryInformationJobObject(
                handle,
                win32job.JobObjectExtendedLimitInformation,
            )
        except Exception as exc:  # pragma: no cover - OS error
            raise WindowsJobError(f"SetInformationJobObject failed: {exc}") from exc
        readback_flags = int(readback["BasicLimitInformation"].get("LimitFlags", 0))
        required = (
            int(win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
            | int(win32job.JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION)
        )
        if readback_flags & required != required:
            raise WindowsJobError("Windows Job limit flags readback mismatch")

    def create_suspended(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
    ) -> JobChild:
        win32api, win32con, _job, win32process = self._modules()
        try:
            import win32pipe
            import win32security
        except ImportError as exc:  # pragma: no cover - dependency gate
            raise WindowsJobError("pywin32 pipe support is required") from exc
        if not command or any("\x00" in str(item) for item in command):
            raise WindowsJobError("invalid child command")
        command_line = subprocess.list2cmdline([str(item) for item in command])
        flags = int(win32con.CREATE_SUSPENDED) | int(win32con.CREATE_NEW_PROCESS_GROUP)
        flags |= int(getattr(win32con, "CREATE_UNICODE_ENVIRONMENT", 0x00000400))
        startup = win32process.STARTUPINFO()
        security = win32security.SECURITY_ATTRIBUTES()
        security.bInheritHandle = True
        read_handle = None
        write_handle = None
        try:
            read_handle, write_handle = win32pipe.CreatePipe(security, 0)
            win32api.SetHandleInformation(
                write_handle,
                int(win32con.HANDLE_FLAG_INHERIT),
                0,
            )
            startup.dwFlags |= int(win32con.STARTF_USESTDHANDLES)
            startup.hStdInput = read_handle
            startup.hStdOutput = win32api.GetStdHandle(win32api.STD_OUTPUT_HANDLE)
            startup.hStdError = win32api.GetStdHandle(win32api.STD_ERROR_HANDLE)
            process_handle, thread_handle, pid, _tid = win32process.CreateProcess(
                None,
                command_line,
                None,
                None,
                True,
                flags,
                dict(env),
                str(cwd),
                startup,
            )
        except Exception as exc:  # pragma: no cover - OS error
            if read_handle is not None:
                self.close(read_handle)
            if write_handle is not None:
                self.close(write_handle)
            raise WindowsJobError(f"CREATE_SUSPENDED failed: {exc}") from exc
        self.close(read_handle)
        return JobChild(
            pid=int(pid),
            process_handle=process_handle,
            thread_handle=thread_handle,
            owner_pipe_handle=write_handle,
        )

    def assign(self, job_handle: object, process_handle: object) -> None:
        _api, _con, win32job, _process = self._modules()
        try:
            win32job.AssignProcessToJobObject(job_handle, process_handle)
            if not win32job.IsProcessInJob(process_handle, job_handle):
                raise WindowsJobError("child did not enter the expected Job")
        except WindowsJobError:
            raise
        except Exception as exc:  # pragma: no cover - OS error
            raise WindowsJobError(f"AssignProcessToJobObject failed: {exc}") from exc

    def resume(self, thread_handle: object) -> None:
        _api, _con, _job, win32process = self._modules()
        try:
            previous = win32process.ResumeThread(thread_handle)
        except Exception as exc:  # pragma: no cover - OS error
            raise WindowsJobError(f"ResumeThread failed: {exc}") from exc
        if int(previous) < 1:
            raise WindowsJobError("child was not suspended before Job assignment")

    def terminate(self, process_handle: object, exit_code: int) -> None:
        win32api, _con, _job, _process = self._modules()
        try:
            win32api.TerminateProcess(process_handle, int(exit_code))
        except Exception as exc:  # pragma: no cover - OS error
            raise WindowsJobError("TerminateProcess failed for task-owned child") from exc

    def query(self, job_handle: object) -> JobAccounting:
        _api, _con, win32job, _process = self._modules()
        try:
            extended = win32job.QueryInformationJobObject(
                job_handle,
                win32job.JobObjectExtendedLimitInformation,
            )
            accounting = win32job.QueryInformationJobObject(
                job_handle,
                win32job.JobObjectBasicAccountingInformation,
            )
        except Exception as exc:  # pragma: no cover - OS error
            raise WindowsJobError(f"QueryInformationJobObject failed: {exc}") from exc
        active = int(accounting.get("ActiveProcesses", 0))
        current = int(extended.get("JobMemoryUsed", 0))
        current_rss = 0
        if active:
            try:
                pids_raw = win32job.QueryInformationJobObject(
                    job_handle,
                    win32job.JobObjectBasicProcessIdList,
                )
                pids = pids_raw.get("ProcessIdList", []) if isinstance(pids_raw, dict) else pids_raw
                fallback_private = 0
                observed_processes = 0
                for pid in pids:
                    try:
                        memory = psutil.Process(int(pid)).memory_info()
                    except (psutil.NoSuchProcess, psutil.ZombieProcess):
                        # The Job accounting snapshot and PID list are not one
                        # atomic API. A child that exited between the two is
                        # already quiescing and must not turn clean completion
                        # into a false enforcement failure.
                        continue
                    except psutil.AccessDenied as exc:
                        raise WindowsJobError("unable to read an active task-owned Job process") from exc
                    observed_processes += 1
                    current_rss += int(memory.rss)
                    fallback_private += int(getattr(memory, "private", memory.rss))
                if not current:
                    current = fallback_private
                if active and not current and not observed_processes:
                    refreshed = win32job.QueryInformationJobObject(
                        job_handle,
                        win32job.JobObjectBasicAccountingInformation,
                    )
                    if int(refreshed.get("ActiveProcesses", 0)):
                        raise WindowsJobError("active Job memory telemetry vanished during sampling")
            except WindowsJobError:
                raise
            except Exception as exc:  # pragma: no cover - OS API failure
                raise WindowsJobError("unable to read active Job memory telemetry") from exc
        return JobAccounting(
            current_commit_bytes=current,
            peak_commit_bytes=int(extended.get("PeakJobMemoryUsed", 0)),
            active_processes=active,
            current_rss_bytes=current_rss,
            peak_rss_bytes=current_rss,
        )

    def close(self, handle: object) -> None:
        win32api, _con, _job, _process = self._modules()
        try:
            win32api.CloseHandle(handle)
        except Exception as exc:  # pragma: no cover - OS error
            raise WindowsJobError("CloseHandle failed for task-owned handle") from exc


class WindowsJob:
    """Own a non-breakaway Job and admit children before their first instruction."""

    def __init__(
        self,
        name: str,
        *,
        policy: ResourcePolicy,
        hybrid_wsl: bool,
        backend: JobBackend | None = None,
    ) -> None:
        if not _JOB_NAME.fullmatch(name):
            raise WindowsJobError("invalid Windows Job name")
        if not isinstance(hybrid_wsl, bool):
            raise WindowsJobError("hybrid_wsl must be a boolean")
        validate_resource_policy(policy)
        self.name = name
        self.policy = policy
        self.hybrid_wsl = hybrid_wsl
        # The Job remains the exact process-ownership boundary. Monthly dataset
        # releases intentionally rely on the host OS for memory management and
        # do not install an AIstock-specific commit limit.
        self.memory_limit_bytes: int | None = None
        self._backend = backend or _PyWin32Backend()
        self._handle = self._backend.create_job(name)
        try:
            self._backend.configure_job(self._handle)
        except Exception:
            self._backend.close(self._handle)
            raise
        self._closed = False
        self._peak_rss_bytes = 0
        self._owner_pipe_handles: list[object] = []

    def launch(
        self,
        command: Sequence[str],
        *,
        cwd: str | Path,
        env: Mapping[str, str] | None = None,
    ) -> JobChild:
        if self._closed:
            raise WindowsJobError("Windows Job is closed")
        child = self._backend.create_suspended(
            command,
            cwd=Path(cwd).resolve(strict=True),
            env=dict(os.environ if env is None else env),
        )
        try:
            self._backend.assign(self._handle, child.process_handle)
            if child.thread_handle is None:
                raise WindowsJobError("suspended child has no thread handle")
            self._backend.resume(child.thread_handle)
        except Exception:
            self._backend.terminate(child.process_handle, 70)
            self._backend.close(child.process_handle)
            if child.thread_handle is not None:
                self._backend.close(child.thread_handle)
            if child.owner_pipe_handle is not None:
                self._backend.close(child.owner_pipe_handle)
            raise
        self._backend.close(child.thread_handle)
        child.thread_handle = None
        if child.owner_pipe_handle is not None:
            self._owner_pipe_handles.append(child.owner_pipe_handle)
        return child

    def accounting(self) -> JobAccounting:
        if self._closed:
            raise WindowsJobError("Windows Job is closed")
        sample = self._backend.query(self._handle)
        self._peak_rss_bytes = max(
            self._peak_rss_bytes,
            int(sample.current_rss_bytes),
            int(sample.peak_rss_bytes),
        )
        return JobAccounting(
            current_commit_bytes=sample.current_commit_bytes,
            peak_commit_bytes=sample.peak_commit_bytes,
            active_processes=sample.active_processes,
            current_rss_bytes=sample.current_rss_bytes,
            peak_rss_bytes=self._peak_rss_bytes,
        )

    def close(self, *, require_quiescent: bool = True) -> None:
        if self._closed:
            return
        if require_quiescent and self.accounting().active_processes:
            raise WindowsJobError("refusing normal Job close while task children are active")
        try:
            self._backend.close(self._handle)
        finally:
            for handle in self._owner_pipe_handles:
                self._backend.close(handle)
            self._owner_pipe_handles.clear()
        self._closed = True

    def __enter__(self) -> "WindowsJob":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(require_quiescent=exc_type is None)


__all__ = [
    "JobAccounting",
    "JobBackend",
    "JobChild",
    "WindowsJob",
    "WindowsJobError",
]
