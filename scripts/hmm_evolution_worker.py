"""Independent worker entry point for HMM evolution evaluations.

Operators must explicitly choose finite ``--once``/``--drain`` or long-running
``--serve`` mode.  The service consumes only durable queued work; it never
starts from FastAPI, cron, or a research scheduler, and the default
``HMM_EVOLUTION_RUNTIME_MODE=disabled`` fails closed.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import signal
import socket
import sys
from pathlib import Path
from threading import Event

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.hmm_evolution.errors import HMMEvolutionError, InvalidSpecError  # noqa: E402
from backend.services.hmm_evolution.runtime import (  # noqa: E402
    build_runtime,
    require_worker_runtime,
)
from backend.services.hmm_evolution.worker import (  # noqa: E402
    HMMEvolutionWorker,
    WorkerConfig,
)
from backend.services.hmm_evolution.worker_service import (  # noqa: E402
    HMMEvolutionWorkerService,
    WorkerServiceConfig,
)

logger = logging.getLogger("hmm_evolution.worker_cli")


def _load_canonical_env(env_path: Path | None = None) -> Path:
    """Load the repository ``.env`` without overriding explicit process env."""

    path = (env_path or PROJECT_ROOT / ".env").resolve()
    if path.exists() and not path.is_file():
        raise RuntimeError(f"HMM evolution worker env path is not a file: {path}")
    load_dotenv(path, override=False)
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the isolated HMM evolution worker in explicit finite or service mode.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="Claim at most one batch item.")
    mode.add_argument(
        "--drain",
        action="store_true",
        help="Process queued work until empty or --max-jobs is reached.",
    )
    mode.add_argument(
        "--serve",
        action="store_true",
        help="Continuously consume already-queued work until SIGINT or SIGTERM.",
    )
    parser.add_argument("--max-jobs", type=int, default=50)
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=None,
        help="Idle queue poll interval for --serve (default env or 5 seconds).",
    )
    parser.add_argument("--owner-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.max_jobs < 1 or args.max_jobs > 500:
        raise SystemExit("--max-jobs must be between 1 and 500")
    env_path = _load_canonical_env()
    logging.basicConfig(
        level=os.getenv("HMM_EVOLUTION_WORKER_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if not env_path.exists():
        logger.warning(
            "HMM evolution worker canonical env file is missing path=%s; "
            "runtime mode remains fail-closed",
            env_path,
        )
    owner_id = str(args.owner_id or _default_owner_id(service=args.serve)).strip()
    try:
        mode = require_worker_runtime()
        runtime = build_runtime()
        worker = HMMEvolutionWorker(
            runtime.repository,
            owner_id=owner_id,
            config=WorkerConfig(runtime_mode=mode),
            executor=runtime.executor,
            submission_preparer=runtime.service,
        )
        if args.serve:
            stop_event = Event()
            _install_shutdown_handlers(stop_event)
            service = HMMEvolutionWorkerService(
                worker,
                config=WorkerServiceConfig(
                    poll_seconds=_resolve_poll_seconds(args.poll_seconds),
                ),
            )
            result = service.run(stop_event)
            logger.info(
                "HMM evolution worker service exit owner_id=%s cycles=%s processed_slices=%s",
                owner_id,
                result.cycles,
                result.processed_slices,
            )
            return 0
        processed = 0
        limit = 1 if args.once else args.max_jobs
        while processed < limit:
            claimed = worker.run_once()
            if not claimed:
                break
            processed += 1
        logger.info(
            "HMM evolution worker finished owner_id=%s processed=%s limit=%s",
            owner_id,
            processed,
            limit,
        )
        return 0
    except HMMEvolutionError as exc:
        logger.error(
            "HMM evolution worker stopped reason_code=%s message=%s",
            exc.reason_code,
            exc.message,
        )
        return 2
    except Exception:
        logger.exception("HMM evolution worker failed unexpectedly owner_id=%s", owner_id)
        return 1


def _resolve_poll_seconds(argument: float | None) -> float:
    raw: float | str = argument if argument is not None else os.getenv(
        "HMM_EVOLUTION_WORKER_POLL_SECONDS",
        "5",
    )
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise InvalidSpecError("HMM evolution worker poll interval is invalid") from exc
    if not math.isfinite(value) or not 0.1 <= value <= 300.0:
        raise InvalidSpecError(
            "HMM evolution worker poll interval must be between 0.1 and 300 seconds"
        )
    return value


def _install_shutdown_handlers(stop_event: Event) -> None:
    def request_shutdown(signum: int, _frame: object) -> None:
        logger.info("HMM evolution worker service shutdown requested signal=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)


def _default_owner_id(*, service: bool = False) -> str:
    prefix = "service" if service else "manual"
    return f"{prefix}-{socket.gethostname()}-{os.getpid()}"


if __name__ == "__main__":
    raise SystemExit(main())
