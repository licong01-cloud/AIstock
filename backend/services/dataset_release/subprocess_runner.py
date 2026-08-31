from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

try:
    from .log_store import (
        LogCapacityExceeded,
        LogSegmentBudget,
        MAX_LOG_SEGMENTS,
        MAX_LOG_TOTAL_BYTES,
        RotatingLogWriter,
        manifest_segments,
    )
except ImportError:  # direct WSL script entrypoint
    import sys

    repository_root = Path(__file__).resolve().parents[3]
    if str(repository_root) not in sys.path:
        sys.path.insert(0, str(repository_root))
    from backend.services.dataset_release.log_store import (  # noqa: E402
        LogCapacityExceeded,
        LogSegmentBudget,
        MAX_LOG_SEGMENTS,
        MAX_LOG_TOTAL_BYTES,
        RotatingLogWriter,
        manifest_segments,
    )


class SubprocessRunnerError(RuntimeError):
    """Typed runner failure which never implies that the data task succeeded."""


class SubprocessStillRunning(SubprocessRunnerError):
    def __init__(self, pid: int, reason: str) -> None:
        self.pid = int(pid)
        self.reason = reason
        super().__init__(f"task-owned child still running after cooperative {reason}: pid={pid}")


@dataclass(frozen=True)
class StreamedRunResult:
    returncode: int
    pid: int
    elapsed_seconds: float
    log_segments: tuple[dict[str, object], ...]
    cooperative_reason: str | None = None


def _reader(
    stream,
    writer: RotatingLogWriter,
    errors: list[BaseException],
) -> None:
    try:
        while True:
            chunk = stream.read(64 * 1024)
            if not chunk:
                return
            writer.write(chunk)
    except BaseException as exc:
        errors.append(exc)
    finally:
        try:
            stream.close()
        except OSError:
            pass


def _cooperative_signal(process: subprocess.Popen[bytes]) -> None:
    if os.name == "nt":
        process.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        process.send_signal(signal.SIGTERM)


def run_streamed(
    command: Sequence[str],
    *,
    cwd: Path,
    log_root: Path,
    env: Mapping[str, str] | None = None,
    cancel_requested: Callable[[], bool] = lambda: False,
    timeout_seconds: float | None = None,
    cooperative_grace_seconds: float = 30.0,
    segment_limit_bytes: int = 16 * 1024**2,
    max_log_segments: int = MAX_LOG_SEGMENTS,
    max_log_bytes: int = MAX_LOG_TOTAL_BYTES,
    popen_factory: Callable[..., subprocess.Popen[bytes]] = subprocess.Popen,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> StreamedRunResult:
    if not command or any("\x00" in str(part) for part in command):
        raise SubprocessRunnerError("invalid subprocess command")
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise SubprocessRunnerError("timeout must be positive")
    if cooperative_grace_seconds <= 0:
        raise SubprocessRunnerError("cooperative grace must be positive")
    resolved_cwd = cwd.resolve(strict=True)
    log_root.mkdir(parents=True, exist_ok=True)
    segment_budget = LogSegmentBudget(
        max_log_segments,
        max_total_bytes=max_log_bytes,
    )
    stdout_writer = RotatingLogWriter(
        log_root,
        "stdout",
        segment_limit_bytes=segment_limit_bytes,
        segment_budget=segment_budget,
    )
    stderr_writer = RotatingLogWriter(
        log_root,
        "stderr",
        segment_limit_bytes=segment_limit_bytes,
        segment_budget=segment_budget,
    )
    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    started = monotonic()
    process = popen_factory(
        [str(part) for part in command],
        cwd=resolved_cwd,
        env=dict(os.environ if env is None else env),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
        creationflags=creationflags,
        start_new_session=os.name != "nt",
    )
    assert process.stdout is not None and process.stderr is not None
    reader_errors: list[BaseException] = []
    readers = [
        threading.Thread(
            target=_reader,
            args=(process.stdout, stdout_writer, reader_errors),
            daemon=True,
        ),
        threading.Thread(
            target=_reader,
            args=(process.stderr, stderr_writer, reader_errors),
            daemon=True,
        ),
    ]
    for thread in readers:
        thread.start()
    signal_reason: str | None = None
    signal_at = 0.0
    while process.poll() is None:
        elapsed = monotonic() - started
        reason = None
        if reader_errors:
            reason = (
                "log_capacity"
                if any(isinstance(error, LogCapacityExceeded) for error in reader_errors)
                else "log_failure"
            )
        elif cancel_requested():
            reason = "cancel"
        elif timeout_seconds is not None and elapsed >= timeout_seconds:
            reason = "timeout"
        if reason is not None and signal_reason is None:
            _cooperative_signal(process)
            signal_reason = reason
            signal_at = monotonic()
        if signal_reason is not None and monotonic() - signal_at >= cooperative_grace_seconds:
            # Do not force-kill here. The outer Job/cgroup guardian owns fail-stop
            # and the durable state machine must retain an orphan resource hold.
            try:
                process.stdout.close()
                process.stderr.close()
            finally:
                stdout_writer.close()
                stderr_writer.close()
            raise SubprocessStillRunning(process.pid, signal_reason)
        sleep(0.1)
    for thread in readers:
        thread.join(timeout=5)
        if thread.is_alive():
            raise SubprocessRunnerError("log stream thread did not quiesce")
    stdout_writer.close()
    stderr_writer.close()
    if reader_errors:
        if any(isinstance(error, LogCapacityExceeded) for error in reader_errors):
            signal_reason = "log_capacity"
        else:
            raise SubprocessRunnerError("log stream writer failed") from reader_errors[0]
    return StreamedRunResult(
        returncode=int(process.returncode),
        pid=int(process.pid),
        elapsed_seconds=max(0.0, monotonic() - started),
        log_segments=tuple(manifest_segments((stdout_writer, stderr_writer))),
        cooperative_reason=signal_reason,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one task child with bounded streamed logs.")
    parser.add_argument("--log-root", required=True)
    parser.add_argument("--cwd", default=None)
    parser.add_argument("--result-path", default=None)
    parser.add_argument("--cancel-file", default=None)
    parser.add_argument("--attempt-id", default=None)
    parser.add_argument("--fence", type=int, default=None)
    parser.add_argument("--timeout-seconds", type=float, default=None)
    parser.add_argument("--cooperative-grace-seconds", type=float, default=30.0)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if args.result_path is not None and (not args.attempt_id or args.fence is None or args.fence <= 0):
        raise SystemExit("supervised result requires attempt identity and positive fence")
    cancel_file = Path(args.cancel_file) if args.cancel_file else None
    result = run_streamed(
        command,
        cwd=Path(args.cwd).resolve(strict=True) if args.cwd else Path.cwd(),
        log_root=Path(args.log_root),
        cancel_requested=((lambda: cancel_file.exists()) if cancel_file is not None else (lambda: False)),
        timeout_seconds=args.timeout_seconds,
        cooperative_grace_seconds=args.cooperative_grace_seconds,
    )
    if args.result_path is not None:
        _atomic_result(
            Path(args.result_path),
            {
                "schema_version": "dataset_supervised_runner_result_v1",
                "attempt_id": args.attempt_id,
                "fence": args.fence,
                "wrapper_pid": os.getpid(),
                "child_pid": result.pid,
                "returncode": result.returncode,
                "elapsed_seconds": result.elapsed_seconds,
                "cooperative_reason": result.cooperative_reason,
                "log_segments": list(result.log_segments),
            },
        )
    return result.returncode


def _atomic_result(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if path.read_bytes() != raw:
            raise SubprocessRunnerError("supervised result readback mismatch")
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "StreamedRunResult",
    "SubprocessRunnerError",
    "SubprocessStillRunning",
    "run_streamed",
]
