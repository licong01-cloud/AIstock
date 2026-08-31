#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError  # noqa: E402
from backend.services.advisory_model_first.research_control import (  # noqa: E402
    authorize_research_window_access,
    bootstrap_registry_from_seed,
    complete_n0,
    freeze_default_research_windows,
    generate_current_route,
    inspect_parent_prediction_extension,
    load_window_contract,
    research_policy_identity,
)
from backend.services.advisory_model_first.research_control_contracts import (  # noqa: E402
    DecisionUse,
    ObjectiveContract,
    ResearchStudyType,
    build_window_access_request,
)


class AdvisoryN0ArgumentError(ValueError):
    pass


class _TypedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise AdvisoryN0ArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _TypedArgumentParser(
        description="Offline N0 research registry, parent capability, and holdout control"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser("bootstrap-registry")
    bootstrap.add_argument("--seed", required=True)
    bootstrap.add_argument("--artifact-root", required=True)
    bootstrap.add_argument("--registry", required=True)

    parent = subparsers.add_parser("parent-spike")
    parent.add_argument("--prediction-store-root", required=True)
    parent.add_argument("--runtime-asset-root", required=True)
    parent.add_argument("--post-cutoff-evidence", required=True)
    parent.add_argument("--comparison-state", required=True)
    parent.add_argument("--target-start", required=True)
    parent.add_argument("--target-end", required=True)
    parent.add_argument("--output", required=True)
    parent.add_argument("--retrain-receipt")

    windows = subparsers.add_parser("freeze-windows")
    windows.add_argument("--output", required=True)

    access = subparsers.add_parser("check-window-access")
    access.add_argument("--contract", required=True)
    access.add_argument("--study-type", choices=[item.value for item in ResearchStudyType], required=True)
    access.add_argument(
        "--objective-contract", choices=[item.value for item in ObjectiveContract], required=True
    )
    access.add_argument("--decision-use", choices=[item.value for item in DecisionUse], required=True)
    access.add_argument("--dataset-identity", required=True)
    access.add_argument("--policy-identity")
    access.add_argument("--start-date", required=True)
    access.add_argument("--end-date", required=True)
    access.add_argument("--frontier-id")
    access.add_argument("--candidate-id")
    access.add_argument("--consume-receipt")

    route = subparsers.add_parser("generate-route")
    route.add_argument("--registry", required=True)
    route.add_argument("--parent-spike", required=True)
    route.add_argument("--window-contract", required=True)
    route.add_argument("--output", required=True)

    complete = subparsers.add_parser("complete-n0")
    complete.add_argument("--registry", required=True)
    complete.add_argument("--parent-spike", required=True)
    complete.add_argument("--window-contract", required=True)
    complete.add_argument("--route", required=True)
    complete.add_argument("--output", required=True)
    return parser


def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "bootstrap-registry":
        return bootstrap_registry_from_seed(
            seed_path=args.seed,
            artifact_root=args.artifact_root,
            registry_path=args.registry,
        )
    if args.command == "parent-spike":
        receipt = inspect_parent_prediction_extension(
            prediction_store_root=args.prediction_store_root,
            runtime_asset_root=args.runtime_asset_root,
            post_cutoff_evidence_path=args.post_cutoff_evidence,
            comparison_state_path=args.comparison_state,
            target_extension_start=args.target_start,
            target_extension_end=args.target_end,
            output_path=args.output,
            retrain_receipt_path=args.retrain_receipt,
        )
        return {
            "status": "ok",
            "parent_prediction_status": receipt.status.value,
            "receipt_id": receipt.receipt_id,
            "receipt_sha256": receipt.receipt_sha256,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "freeze-windows":
        contract = freeze_default_research_windows(output_path=args.output)
        sealed = next(item for item in contract.windows if item.state.value == "SEALED_UNCONSUMED")
        return {
            "status": "ok",
            "contract_id": contract.contract_id,
            "contract_sha256": contract.contract_sha256,
            "sealed_window_id": sealed.window_id,
            "sealed_dataset_identity": sealed.dataset_identity,
            "research_policy_identity": research_policy_identity(),
            "output_path": Path(args.output).resolve().as_posix(),
        }
    if args.command == "check-window-access":
        contract = load_window_contract(args.contract)
        default_policy_identity = research_policy_identity(
            baseline_policy_sha256=contract.baseline_policy_sha256,
            shadow_policy_sha256=contract.shadow_policy_sha256,
            cost_policy_sha256=contract.cost_policy_sha256,
        )
        request = build_window_access_request(
            contract_sha256=contract.contract_sha256,
            study_type=args.study_type,
            objective_contract=args.objective_contract,
            decision_use=args.decision_use,
            dataset_identity=args.dataset_identity,
            policy_identity=args.policy_identity or default_policy_identity,
            start_date=args.start_date,
            end_date=args.end_date,
            frontier_id=args.frontier_id,
            candidate_id=args.candidate_id,
        )
        return authorize_research_window_access(
            contract=contract,
            request=request,
            consume_receipt_path=args.consume_receipt,
        )
    if args.command == "generate-route":
        return generate_current_route(
            registry_path=args.registry,
            parent_spike_path=args.parent_spike,
            window_contract_path=args.window_contract,
            output_path=args.output,
        )
    if args.command == "complete-n0":
        receipt = complete_n0(
            registry_path=args.registry,
            parent_spike_path=args.parent_spike,
            window_contract_path=args.window_contract,
            route_path=args.route,
            output_path=args.output,
        )
        return {
            "status": "ok",
            "n0_status": receipt.status,
            "receipt_id": receipt.receipt_id,
            "receipt_sha256": receipt.receipt_sha256,
            "next_task": receipt.next_task,
            "output_path": Path(args.output).resolve().as_posix(),
        }
    raise AssertionError(f"unsupported command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        result = _run(args)
    except AdvisoryModelFirstError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        return 1
    except (ValidationError, ValueError, KeyError) as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": "ADVISORY_N0_REQUEST_INVALID",
                    "message": str(exc),
                    "context": {"error_type": type(exc).__name__},
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 1
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": "ADVISORY_N0_UNEXPECTED_FAILURE",
                    "message": str(exc),
                    "context": {"error_type": type(exc).__name__},
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
