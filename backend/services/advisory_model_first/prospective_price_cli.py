from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Callable, Sequence

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze or capture a pre-open Advisory price-envelope prediction.")
    commands = parser.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser("prepare", help="freeze a prediction request")
    _common(prepare)
    prepare.add_argument("--program-id", required=True)
    prepare.add_argument("--target-trade-date", required=True, type=date.fromisoformat)
    prepare.add_argument("--parent-bundle-id", required=True)
    prepare.add_argument("--outcome-bundle-id", required=True)
    prepare.add_argument("--price-range-bundle-id", required=True)
    prepare.add_argument("--list-version-id")
    prepare.add_argument("--request-output", required=True, type=Path)

    capture = commands.add_parser("capture", help="materialize a frozen prediction")
    _common(capture)
    capture.add_argument("--request", required=True, type=Path)
    return parser


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--model-root", required=True, type=Path)


def main(
    argv: Sequence[str] | None = None,
    *,
    service_factory: Callable[[], Any] | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    env_file = args.env_file.resolve()
    if not env_file.is_file():
        raise FileNotFoundError(env_file)
    load_dotenv(env_file, override=False)

    from backend.services.advisory_model_first.prospective_price_prediction import (
        AdvisoryPriceProspectivePredictionService,
        read_prospective_request,
        write_prospective_request,
    )
    from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

    service = service_factory() if service_factory is not None else AdvisoryPriceProspectivePredictionService()
    model_root = args.model_root.resolve()
    try:
        if args.command == "prepare":
            request = service.prepare_request(
                program_id=args.program_id,
                target_trade_date=args.target_trade_date,
                model_root=model_root,
                parent_bundle_id=args.parent_bundle_id,
                outcome_bundle_id=args.outcome_bundle_id,
                price_range_bundle_id=args.price_range_bundle_id,
                list_version_id=args.list_version_id,
            )
            output = write_prospective_request(request, args.request_output.resolve())
            payload = {
                "command": "prepare",
                "schema_version": request.schema_version,
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "target_trade_date": request.target_trade_date.isoformat(),
                "target_open_at": request.target_open_at.isoformat(),
                "request_path": str(output),
                "realized_outcome_access_allowed": False,
            }
        else:
            request = read_prospective_request(args.request.resolve())
            receipt = service.capture(request=request, model_root=model_root)
            payload = {
                "command": "capture",
                **receipt.model_dump(mode="json"),
                "artifact_path": str(model_root / "price_range_prospective_predictions" / request.request_id),
            }
    except AdvisoryModelFirstError as exc:
        print(
            json.dumps(
                {
                    "command": args.command,
                    "status": "FAILED",
                    "reason_code": exc.reason_code,
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
