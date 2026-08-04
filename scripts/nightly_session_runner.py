"""Run selected Nightly nox sessions with bounded, checkpointed receipts."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable


SessionExecutor = Callable[[str, int], dict[str, Any]]


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def write_receipts(results: list[dict[str, Any]], *, output_json: Path, output_md: Path) -> None:
    _atomic_write(output_json, json.dumps(results, ensure_ascii=False, indent=2) + "\n")
    lines = ["# Nightly selected session results", "", "| Session | Result | Failure kind | Duration (s) |", "| --- | --- | --- | ---: |"]
    for row in results:
        lines.append(
            f"| `{row['session']}` | `{row['result']}` | `{row.get('failure_kind') or '-'}` | {row.get('duration_seconds', 0):.2f} |"
        )
    _atomic_write(output_md, "\n".join(lines) + "\n")


def _terminate_process_tree(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def execute_session(session: str, timeout_seconds: int) -> dict[str, Any]:
    command = [sys.executable, "-m", "nox", "-s", session]
    started = time.monotonic()
    popen_kwargs: dict[str, Any] = {}
    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_kwargs["start_new_session"] = True
    print(
        f"NIGHTLY_SESSION_START session={session} timeout_seconds={timeout_seconds}",
        flush=True,
    )
    process = subprocess.Popen(command, **popen_kwargs)
    try:
        return_code = process.wait(timeout=timeout_seconds)
        duration = time.monotonic() - started
        result = "success" if return_code == 0 else "failure"
        failure_kind = None if return_code == 0 else "nonzero_exit"
    except subprocess.TimeoutExpired:
        _terminate_process_tree(process)
        return_code = None
        duration = time.monotonic() - started
        result = "failure"
        failure_kind = "timeout"
    row = {
        "session": session,
        "result": result,
        "failure_kind": failure_kind,
        "return_code": return_code,
        "duration_seconds": round(duration, 3),
        "timeout_seconds": timeout_seconds,
    }
    print(
        "NIGHTLY_SESSION_END "
        f"session={session} result={result} failure_kind={failure_kind or 'none'} "
        f"duration_seconds={duration:.2f}",
        flush=True,
    )
    return row


def run_sessions(
    sessions: list[str],
    *,
    output_json: Path,
    output_md: Path,
    session_timeout_seconds: int,
    total_timeout_seconds: int,
    executor: SessionExecutor = execute_session,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    deadline = time.monotonic() + total_timeout_seconds
    for session in sessions:
        remaining = int(deadline - time.monotonic())
        if remaining <= 0:
            row = {
                "session": session,
                "result": "failure",
                "failure_kind": "total_budget_exhausted",
                "return_code": None,
                "duration_seconds": 0.0,
                "timeout_seconds": 0,
            }
        else:
            row = executor(session, min(session_timeout_seconds, remaining))
        results.append(row)
        # Persist after every session so a later timeout/cancellation cannot erase
        # already observed business failures.
        write_receipts(results, output_json=output_json, output_md=output_md)
    return results


def _selected_sessions(plan_path: Path) -> list[str]:
    payload = json.loads(plan_path.read_text(encoding="utf-8-sig"))
    raw = payload.get("selected_sessions") if isinstance(payload, dict) else None
    if not isinstance(raw, list):
        raise ValueError("Nightly execution plan must contain selected_sessions")
    sessions = [str(item).strip() for item in raw if str(item).strip()]
    if len(sessions) != len(set(sessions)):
        raise ValueError("Nightly execution plan contains duplicate sessions")
    return sessions


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-md", required=True)
    parser.add_argument("--session-timeout-seconds", type=int, default=1200)
    parser.add_argument("--total-timeout-seconds", type=int, default=6300)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.session_timeout_seconds <= 0 or args.total_timeout_seconds <= 0:
        raise SystemExit("timeout values must be positive")
    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    try:
        sessions = _selected_sessions(Path(args.plan))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        failure = [{
            "session": "nightly_execution_plan",
            "result": "failure",
            "failure_kind": "invalid_plan",
            "return_code": None,
            "duration_seconds": 0.0,
            "timeout_seconds": 0,
            "error": str(exc),
        }]
        write_receipts(failure, output_json=output_json, output_md=output_md)
        print(f"NIGHTLY_RUNNER_FAILED failure_kind=invalid_plan error={exc}", file=sys.stderr)
        return 2
    results = run_sessions(
        sessions,
        output_json=output_json,
        output_md=output_md,
        session_timeout_seconds=args.session_timeout_seconds,
        total_timeout_seconds=args.total_timeout_seconds,
    )
    failures = [row for row in results if row.get("result") != "success"]
    print(
        f"NIGHTLY_RUNNER_COMPLETE sessions={len(results)} failures={len(failures)} receipt={output_json}",
        flush=True,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
