#!/usr/bin/env python
"""Run the code-owned durable worker for unified monthly dataset releases."""

from __future__ import annotations

import argparse
import math
from pathlib import Path
import signal
import sys
from threading import Event
from typing import Any, Callable, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.dataset_release.monthly_unified import (  # noqa: E402
    MonthlyReleaseError,
)
from backend.services.dataset_release.monthly_worker_runtime import (  # noqa: E402
    MonthlyWorkerRuntime,
    build_monthly_worker_runtime,
)


MAX_DRAIN_OPERATIONS = 100


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--drain", action="store_true")
    mode.add_argument("--serve", action="store_true")
    parser.add_argument("--max-operations", type=int)
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    if args.drain:
        if (
            args.max_operations is None
            or not 1 <= args.max_operations <= MAX_DRAIN_OPERATIONS
        ):
            raise ValueError(
                f"--drain requires --max-operations in 1..{MAX_DRAIN_OPERATIONS}"
            )
    elif args.max_operations is not None:
        raise ValueError("--max-operations is valid only with --drain")
    if not math.isfinite(args.poll_seconds) or not 0.1 <= args.poll_seconds <= 300:
        raise ValueError("--poll-seconds must be finite and in 0.1..300")


def _emit(value: Mapping[str, Any], *, stream=None) -> None:
    target = stream or sys.stdout.buffer
    target.write(canonical_json_bytes(value) + b"\n")
    target.flush()


def _mode(args: argparse.Namespace) -> str:
    if args.preflight:
        return "preflight"
    if args.once:
        return "once"
    if args.drain:
        return "drain"
    return "serve"


def _run_finite(runtime: MonthlyWorkerRuntime, *, limit: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for _ in range(limit):
        value = runtime.worker.run_once()
        if value is None:
            break
        results.append(dict(value))
    return results


def _install_signal_handlers(stop_event: Event) -> None:
    def request_stop(_signum, _frame) -> None:  # type: ignore[no-untyped-def]
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)


def _run_service(
    runtime: MonthlyWorkerRuntime,
    *,
    poll_seconds: float,
    stop_event: Event,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    while not stop_event.is_set():
        value = runtime.worker.run_once()
        if value is None:
            stop_event.wait(poll_seconds)
        else:
            results.append(dict(value))
    return results


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_loader: Callable[..., MonthlyWorkerRuntime] = build_monthly_worker_runtime,
) -> int:
    args = build_parser().parse_args(argv)
    try:
        _validate_args(args)
        runtime = runtime_loader(project_root=PROJECT_ROOT)
        mode = _mode(args)
        if mode == "preflight":
            _emit(runtime.preflight_receipt())
            return 0
        if mode == "serve":
            stop_event = Event()
            _install_signal_handlers(stop_event)
            results = _run_service(
                runtime,
                poll_seconds=args.poll_seconds,
                stop_event=stop_event,
            )
        else:
            limit = 1 if mode == "once" else args.max_operations
            results = _run_finite(runtime, limit=limit)
        _emit(
            {
                "schema_version": "aistock_monthly_release_worker_run_v1",
                "status": "PASS",
                "mode": mode,
                "processed_operation_count": len(results),
                "last_operation_id": (
                    None if not results else results[-1].get("operation_id")
                ),
                "last_status": None if not results else results[-1].get("status"),
            }
        )
        return 0
    except (MonthlyReleaseError, OSError, RuntimeError, ValueError) as exc:
        payload: dict[str, Any] = {
            "schema_version": "aistock_monthly_release_worker_error_v1",
            "status": "FAILED",
            "reason_code": str(
                getattr(exc, "code", "MONTHLY_RELEASE_WORKER_FAILED")
            ),
            "message": str(exc),
        }
        context = getattr(exc, "context", None)
        if isinstance(context, Mapping) and context:
            payload["context"] = dict(context)
        _emit(payload, stream=sys.stderr.buffer)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
