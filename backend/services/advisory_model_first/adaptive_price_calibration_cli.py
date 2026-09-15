from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv

from backend.services.advisory_model_first.adaptive_price_calibration import (
    AdvisoryAdaptivePriceCalibrationService,
    prepare_adaptive_price_calibration_request,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Advisory past-only adaptive daily price calibration"
    )
    parser.add_argument("command", choices=("run",))
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--source-replay-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--registry-path", required=True, type=Path)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        env_file = args.env_file.resolve()
        repository_root = args.repository_root.resolve()
        if not env_file.is_file():
            raise AdvisoryModelFirstError(
                "adaptive calibration env file is unavailable",
                reason_code="ADVISORY_ADAPTIVE_PRICE_REQUEST_INVALID",
            )
        load_dotenv(env_file, override=False)
        repository_commit = _repository_commit(repository_root)
        request = prepare_adaptive_price_calibration_request(
            source_replay_root=args.source_replay_root,
            output_root=args.output_root,
            registry_path=args.registry_path,
            repository_commit=repository_commit,
        )
        delivery = AdvisoryAdaptivePriceCalibrationService().run(request=request)
        print(
            json.dumps(
                {
                    "command": "adaptive-price-calibration",
                    **delivery.artifact.receipt.model_dump(mode="json"),
                    "artifact_path": str(delivery.artifact.path),
                    "registry_delivery": dict(delivery.registry_delivery),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    except AdvisoryModelFirstError as exc:
        print(
            json.dumps(
                {
                    "command": "adaptive-price-calibration",
                    "status": "FAILED",
                    "reason_code": exc.reason_code,
                    "message": str(exc),
                    "context": exc.context,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        print(
            json.dumps(
                {
                    "command": "adaptive-price-calibration",
                    "status": "FAILED",
                    "reason_code": "ADVISORY_ADAPTIVE_PRICE_REQUEST_INVALID",
                    "message": str(exc),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


def _repository_commit(repository_root: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    value = completed.stdout.strip()
    if len(value) != 40 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError("repository HEAD is not a canonical SHA1")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
