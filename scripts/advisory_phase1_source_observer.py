"""Standalone Phase 1D source observer and capacity planning entry point."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.advisory_phase0a.policy import canonical_json_text
from backend.services.advisory_phase1.source_capacity import (
    CapacityPlanningReceipt,
    CapacityPlanningRequest,
    AdvisoryPhase1CapacityProbe,
)
from backend.services.advisory_phase1.source_observer import (
    SOURCE_QUERY_TEMPLATES,
    SourceObserverError,
    registered_source_observer_configs,
)
from backend.services.advisory_phase1.source_observer_postgres import PostgresSourceObserverRepository


REASON_OBSERVER_DISABLED = "ADVISORY_PHASE1_SOURCE_OBSERVER_DISABLED"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    observe = subparsers.add_parser("observe-once", help="Run one default-off observer pass.")
    _add_config_args(observe)
    capacity = subparsers.add_parser("capacity-plan", help="Create a read-only capacity receipt.")
    _add_config_args(capacity)
    capacity.add_argument("--request", required=True, type=Path, help="CapacityPlanningRequest JSON path.")
    capacity.add_argument("--output", required=True, type=Path, help="External capacity receipt JSON path.")
    verify = subparsers.add_parser("verify-receipt", help="Verify a capacity receipt hash.")
    verify.add_argument("--receipt", required=True, type=Path, help="Capacity receipt JSON path.")
    return parser


def _add_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config-id", default="phase1d_market_daily_dev_v1")
    parser.add_argument("--config-version", default="v1")


def _config(args: argparse.Namespace):
    config = registered_source_observer_configs().get((args.config_id, args.config_version))
    if config is None:
        raise SourceObserverError(
            "ADVISORY_PHASE1_SOURCE_OBSERVER_CONFIG_INVALID",
            "unknown compiled observer config",
            context={"config_id": args.config_id, "config_version": args.config_version},
        )
    return config


def _external_output(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    repo_root = Path(__file__).resolve().parents[1]
    try:
        resolved.relative_to(repo_root)
    except ValueError:
        return resolved
    raise SourceObserverError(
        "ADVISORY_PHASE1_CAPACITY_REQUEST_INVALID",
        "capacity receipts must be written outside the repository",
        context={"path": str(resolved)},
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SourceObserverError("ADVISORY_PHASE1_CAPACITY_REQUEST_INVALID", "JSON input path does not exist", context={"path": str(path)}) from exc
    except json.JSONDecodeError as exc:
        raise SourceObserverError("ADVISORY_PHASE1_CAPACITY_REQUEST_INVALID", "JSON input is invalid", context={"path": str(path)}) from exc
    if not isinstance(payload, dict):
        raise SourceObserverError("ADVISORY_PHASE1_CAPACITY_REQUEST_INVALID", "JSON input must be an object", context={"path": str(path)})
    return payload


def _receipt_payload(receipt: CapacityPlanningReceipt) -> dict[str, Any]:
    return {**receipt.model_dump(mode="json"), "receipt_hash": receipt.receipt_hash}


def _run_observe_once(args: argparse.Namespace) -> int:
    if os.getenv("AISTOCK_ADVISORY_PHASE1_SOURCE_OBSERVER_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
        raise SourceObserverError(REASON_OBSERVER_DISABLED, "standalone observer is disabled by runtime configuration")
    config = _config(args)
    summary = PostgresSourceObserverRepository().observe_once(config=config, registry=SOURCE_QUERY_TEMPLATES)
    print(canonical_json_text(summary.as_dict()))
    return 0 if summary.succeeded else 1


def _run_capacity_plan(args: argparse.Namespace) -> int:
    config = _config(args)
    request = CapacityPlanningRequest.model_validate(_read_json(args.request))
    receipt = AdvisoryPhase1CapacityProbe().probe(request=request, config=config, registry=SOURCE_QUERY_TEMPLATES)
    output = _external_output(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(canonical_json_text(_receipt_payload(receipt)) + "\n", encoding="utf-8")
    logging.getLogger(__name__).info(
        "advisory_phase1_capacity_finished request_hash=%s receipt_hash=%s status=%s missing=%s max_required_free_bytes=%s",
        receipt.request_hash,
        receipt.receipt_hash,
        receipt.status.value,
        receipt.missing_measurements,
        receipt.staging_store_summary.get("max", {}).get("required_free_bytes"),
    )
    print(canonical_json_text({"output": str(output), "receipt_hash": receipt.receipt_hash, "status": receipt.status.value}))
    return 0


def _run_verify_receipt(args: argparse.Namespace) -> int:
    payload = _read_json(args.receipt)
    claimed_hash = payload.pop("receipt_hash", None)
    receipt = CapacityPlanningReceipt.model_validate(payload)
    if claimed_hash != receipt.receipt_hash:
        raise SourceObserverError(
            "ADVISORY_PHASE1_CAPACITY_RECEIPT_CONFLICT",
            "receipt content does not match its claimed hash",
            context={"path": str(args.receipt)},
        )
    print(canonical_json_text({"receipt_hash": receipt.receipt_hash, "status": receipt.status.value, "verified": True}))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=os.getenv("AISTOCK_LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = _parser().parse_args(argv)
    try:
        if args.command == "observe-once":
            return _run_observe_once(args)
        if args.command == "capacity-plan":
            return _run_capacity_plan(args)
        return _run_verify_receipt(args)
    except SourceObserverError as exc:
        _logger = logging.getLogger(__name__)
        _logger.exception("advisory_phase1_source_observer_failed reason_code=%s context=%s", exc.reason_code, exc.context)
        print(canonical_json_text({"reason_code": exc.reason_code, "message": str(exc), "context": exc.context}), file=sys.stderr)
        return 2
    except Exception as exc:  # Preserve traceback in logs; never turn an unexpected failure into success.
        _logger = logging.getLogger(__name__)
        _logger.exception("advisory_phase1_source_observer_failed reason_code=%s", "ADVISORY_PHASE1_SOURCE_OBSERVER_UNEXPECTED")
        print(canonical_json_text({"reason_code": "ADVISORY_PHASE1_SOURCE_OBSERVER_UNEXPECTED", "message": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
