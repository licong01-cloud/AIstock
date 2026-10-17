from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable, Sequence

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Settle or aggregate Advisory prospective price envelopes.")
    commands = parser.add_subparsers(dest="command", required=True)

    settle = commands.add_parser("settle", help="settle one mature prospective prediction")
    settle.add_argument("--env-file", required=True, type=Path)
    settle.add_argument("--model-root", required=True, type=Path)
    settle.add_argument("--request-id", required=True)

    aggregate = commands.add_parser("aggregate", help="aggregate immutable prospective settlements")
    aggregate.add_argument("--model-root", required=True, type=Path)
    aggregate.add_argument("--price-range-bundle-id", required=True)
    aggregate.add_argument("--output", required=True, type=Path)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    service_factory: Callable[[], Any] | None = None,
    confirmation_builder: Callable[..., Any] | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
    from backend.services.advisory_model_first.prospective_price_confirmation import (
        build_price_prospective_confirmation,
        write_price_prospective_confirmation,
    )
    from backend.services.advisory_model_first.prospective_price_evaluation import (
        AdvisoryPriceProspectiveEvaluationService,
        SETTLEMENT_ROOT_NAME,
    )
    from backend.services.advisory_model_first.prospective_price_prediction import PROSPECTIVE_ROOT_NAME

    try:
        model_root = args.model_root.resolve()
        if args.command == "settle":
            env_file = args.env_file.resolve()
            if not env_file.is_file():
                raise FileNotFoundError(env_file)
            load_dotenv(env_file, override=False)
            service = service_factory() if service_factory is not None else AdvisoryPriceProspectiveEvaluationService()
            prediction_path = model_root / PROSPECTIVE_ROOT_NAME / args.request_id
            receipt = service.settle(
                prediction_artifact_path=prediction_path,
                model_root=model_root,
            )
            payload = {
                "command": "settle",
                **receipt.model_dump(mode="json"),
                "artifact_path": str(model_root / SETTLEMENT_ROOT_NAME / args.request_id),
            }
        else:
            builder = confirmation_builder or build_price_prospective_confirmation
            confirmation = builder(
                model_root=model_root,
                price_range_bundle_id=args.price_range_bundle_id,
            )
            output = write_price_prospective_confirmation(confirmation, args.output.resolve())
            payload = {
                "command": "aggregate",
                **confirmation.model_dump(mode="json"),
                "output": str(output),
            }
    except AdvisoryModelFirstError as exc:
        print(
            json.dumps(
                {
                    "command": args.command,
                    "status": "WAITING" if exc.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE" else "FAILED",
                    "reason_code": exc.reason_code,
                    "message": str(exc),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 3 if exc.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE" else 2
    except (FileNotFoundError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "command": args.command,
                    "status": "FAILED",
                    "reason_code": "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID",
                    "message": str(exc),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
