"""Operator-started worker entry point for HMM evolution evaluations.

The CLI is intentionally finite: operators must choose ``--once`` or a bounded
``--drain`` run.  It never starts from FastAPI, cron, or a scheduler, and the
default ``HMM_EVOLUTION_RUNTIME_MODE=disabled`` fails closed.
"""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.hmm_evolution.errors import HMMEvolutionError  # noqa: E402
from backend.services.hmm_evolution.runtime import (  # noqa: E402
    build_runtime,
    require_worker_runtime,
)
from backend.services.hmm_evolution.worker import (  # noqa: E402
    HMMEvolutionWorker,
    WorkerConfig,
)

logger = logging.getLogger("hmm_evolution.worker_cli")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the isolated HMM evolution worker with a bounded manual command.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="Claim at most one batch item.")
    mode.add_argument(
        "--drain",
        action="store_true",
        help="Process queued work until empty or --max-jobs is reached.",
    )
    parser.add_argument("--max-jobs", type=int, default=50)
    parser.add_argument("--owner-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.max_jobs < 1 or args.max_jobs > 500:
        raise SystemExit("--max-jobs must be between 1 and 500")
    logging.basicConfig(
        level=os.getenv("HMM_EVOLUTION_WORKER_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    owner_id = str(args.owner_id or _default_owner_id()).strip()
    try:
        mode = require_worker_runtime()
        runtime = build_runtime()
        worker = HMMEvolutionWorker(
            runtime.repository,
            owner_id=owner_id,
            config=WorkerConfig(runtime_mode=mode),
            executor=runtime.executor,
        )
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


def _default_owner_id() -> str:
    return f"manual-{socket.gethostname()}-{os.getpid()}"


if __name__ == "__main__":
    raise SystemExit(main())
