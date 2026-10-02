from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.advisory_model_first.financial_event_source_readiness import (  # noqa: E402
    build_financial_event_source_bundle,
    connect_readonly_from_env,
    deliver_financial_event_source_bundle,
    inspect_financial_event_source_bundle,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run target-free financial-event source readiness")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--parent-path", required=True)
    run.add_argument("--margin-receipt-path", required=True)
    run.add_argument("--repository-root", required=True)
    run.add_argument("--output-root", required=True)
    run.add_argument("--registry-path", required=True)
    run.add_argument("--route-path", required=True)
    run.add_argument("--env-file", default=str(ROOT / ".env"))

    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("--bundle-path", required=True)

    deliver = subparsers.add_parser("deliver")
    deliver.add_argument("--bundle-path", required=True)
    deliver.add_argument("--registry-path", required=True)
    deliver.add_argument("--route-path", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "run":
        load_dotenv(args.env_file, override=False)
        bundle = build_financial_event_source_bundle(
            parent_path=args.parent_path,
            margin_receipt_path=args.margin_receipt_path,
            repository_root=args.repository_root,
            output_root=args.output_root,
            registry_path=args.registry_path,
            route_path=args.route_path,
            connection_factory=connect_readonly_from_env,
        )
        payload = {"status": "COMPLETE", "bundle_path": bundle.as_posix()}
    elif args.command == "inspect":
        payload = inspect_financial_event_source_bundle(args.bundle_path)
    else:
        payload = deliver_financial_event_source_bundle(
            bundle_path=args.bundle_path,
            registry_path=args.registry_path,
            route_path=args.route_path,
        )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
