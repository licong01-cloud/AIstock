#!/usr/bin/env python3
"""Standalone Advisory real DEV onboarding CLI (historical research only)."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys
import traceback
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.advisory_phase0a.policy import canonical_json_text
from backend.services.advisory_dev_input_onboarding.contracts import (
    EvidenceKind,
    InventoryClassification,
    OnboardingArtifactRef,
    PortableAdvisoryEvidenceBundle,
    RealDevOnboardingError,
    RealDevOnboardingInventoryReceipt,
    RealDevOnboardingInventoryQuery,
    RealDevOnboardingRequest,
    REASON_UNEXPECTED_ERROR,
    database_identity_hash,
)
from backend.services.advisory_dev_input_onboarding.production_projection import (
    RealDevOnboardingInventoryService,
    load_exact_release_receipt,
)
from backend.services.advisory_dev_input_onboarding.store import RealDevOnboardingEvidenceStore


LOGGER = logging.getLogger("advisory_real_dev_onboarding")
EXIT_SUCCESS = 0
EXIT_INVALID = 2
EXIT_INPUT_PENDING = 3
EXIT_VERIFICATION_FAILED = 4
EXIT_STATE_UNKNOWN = 5
EXIT_INTERNAL = 70


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Advisory real DEV input onboarding (historical research only)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory = subparsers.add_parser("inventory", help="run exact production/DEV read-only inventory")
    inventory_input = inventory.add_mutually_exclusive_group(required=True)
    inventory_input.add_argument("--request", type=Path)
    inventory_input.add_argument("--inventory-query", type=Path)
    inventory.add_argument("--env-file", required=True, type=Path)
    inventory.add_argument("--release-receipt-root", required=True, type=Path)
    inventory.add_argument("--evidence-root", required=True, type=Path)

    verify = subparsers.add_parser("verify-evidence", help="offline full readback of an immutable evidence ref")
    verify.add_argument("--evidence-ref", required=True, type=Path)
    verify.add_argument("--evidence-root", required=True, type=Path)
    verify.add_argument("--release-receipt-root", required=True, type=Path)

    verify_bundle = subparsers.add_parser("verify-bundle", help="offline bundle and dependency closure verification")
    verify_bundle.add_argument("--bundle-ref", required=True, type=Path)
    verify_bundle.add_argument("--evidence-root", required=True, type=Path)
    verify_bundle.add_argument("--release-receipt-root", required=True, type=Path)
    return parser


def _read_json(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    if not resolved.is_file():
        raise ValueError("input JSON path must be a regular file")
    try:
        document = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("input path is not valid UTF-8 JSON") from exc
    if not isinstance(document, dict):
        raise ValueError("input JSON must contain one object")
    return document


def _read_ref(path: Path, *, expected: EvidenceKind | None = None) -> OnboardingArtifactRef:
    ref = OnboardingArtifactRef.model_validate(_read_json(path))
    if expected is not None and ref.evidence_kind is not expected:
        raise ValueError(f"evidence ref must have kind {expected.value}")
    return ref


def _emit(value: Any) -> None:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    print(canonical_json_text(value))


def _inventory(args: argparse.Namespace) -> int:
    input_contract = (
        RealDevOnboardingRequest.model_validate(_read_json(args.request))
        if args.request is not None
        else RealDevOnboardingInventoryQuery.model_validate(_read_json(args.inventory_query))
    )
    store = RealDevOnboardingEvidenceStore(root=args.evidence_root)
    stored_input = store.publish(input_contract)
    receipt = RealDevOnboardingInventoryService().inventory(
        input_contract=input_contract,
        selected_input_ref=stored_input.ref,
        env_file=args.env_file,
        release_receipt_root=args.release_receipt_root,
    )
    stored_receipt = store.publish(receipt)
    readback = store.load(stored_receipt.ref)
    if not isinstance(readback, RealDevOnboardingInventoryReceipt) or readback.inventory_hash != receipt.inventory_hash:
        raise RealDevOnboardingError("ADVISORY_REAL_DEV_INVENTORY_READBACK_FAILED", "inventory full readback differs")
    _emit(
        {
            "ok": receipt.classification is InventoryClassification.DUAL_TRACK_AVAILABLE,
            "command": "inventory",
            "classification": receipt.classification.value,
            "reason_codes": receipt.reason_codes,
            "program_candidates": [
                {
                    "package_id": item.package_id,
                    "manifest_sha256": item.manifest_sha256,
                    "alpha_mode": item.alpha_mode.value,
                    "package_status": item.package_status,
                    "package_asset_count": item.package_asset_count,
                    "package_eligible": item.package_eligible,
                    "closure_status": item.closure_status.value,
                }
                for item in receipt.program_candidates
            ],
            "selected_input_ref": stored_input.ref,
            "inventory_ref": stored_receipt.ref,
            "source_database_identity_hash": database_identity_hash(receipt.source_database_identity),
            "target_database_identity_hash": database_identity_hash(receipt.target_database_identity),
        }
    )
    if receipt.classification is InventoryClassification.DUAL_TRACK_AVAILABLE:
        return EXIT_SUCCESS
    if receipt.classification is InventoryClassification.TARGET_CONFLICT:
        return EXIT_INVALID
    return EXIT_INPUT_PENDING


def _verify(args: argparse.Namespace, *, expected: EvidenceKind | None = None) -> int:
    store = RealDevOnboardingEvidenceStore(root=args.evidence_root)
    ref_path = args.bundle_ref if expected is EvidenceKind.BUNDLE else args.evidence_ref
    ref = _read_ref(ref_path, expected=expected)
    evidence = store.load(ref)
    store.verify_reference_closure(evidence)
    if expected is EvidenceKind.BUNDLE and not isinstance(evidence, PortableAdvisoryEvidenceBundle):
        raise ValueError("bundle ref resolved to the wrong evidence model")
    if isinstance(evidence, PortableAdvisoryEvidenceBundle):
        release_ref = evidence.request.release_receipt_ref
    elif isinstance(evidence, RealDevOnboardingInventoryReceipt):
        selected = store.load(evidence.selected_input_ref)
        if not isinstance(selected, (RealDevOnboardingRequest, RealDevOnboardingInventoryQuery)):
            raise ValueError("inventory selected input resolves to the wrong evidence model")
        release_ref = selected.release_receipt_ref
    elif isinstance(evidence, (RealDevOnboardingRequest, RealDevOnboardingInventoryQuery)):
        release_ref = evidence.release_receipt_ref
    else:
        raise ValueError("unsupported evidence model")
    load_exact_release_receipt(ref=release_ref, root=args.release_receipt_root)
    _emit(
        {
            "ok": True,
            "command": args.command,
            "evidence_kind": ref.evidence_kind.value,
            "semantic_content_hash": ref.semantic_content_hash,
            "file_sha256": ref.file_sha256,
        }
    )
    return EXIT_SUCCESS


def _log_sanitized_exception(message: str, exc: Exception) -> None:
    frames = " > ".join(
        f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}"
        for frame in traceback.extract_tb(exc.__traceback__)
    )
    LOGGER.error("%s exception_type=%s redacted_traceback=%s", message, type(exc).__name__, frames)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = _parser().parse_args(argv)
    try:
        if args.command == "inventory":
            return _inventory(args)
        if args.command == "verify-evidence":
            return _verify(args)
        if args.command == "verify-bundle":
            return _verify(args, expected=EvidenceKind.BUNDLE)
        raise ValueError("unsupported command")
    except ValueError as exc:
        reason_code = "ADVISORY_REAL_DEV_CONTRACT_INVALID"
        LOGGER.error("advisory_onboarding_command_failed command=%s reason_code=%s", args.command, reason_code)
        _emit({"ok": False, "command": args.command, "reason_code": reason_code, "message": str(exc)})
        return EXIT_INVALID
    except RealDevOnboardingError as exc:
        reason_code = getattr(exc, "reason_code", "ADVISORY_REAL_DEV_CONTRACT_INVALID")
        LOGGER.error("advisory_onboarding_command_failed command=%s reason_code=%s", args.command, reason_code)
        _emit({"ok": False, "command": args.command, "reason_code": reason_code, "message": str(exc)})
        return EXIT_VERIFICATION_FAILED if args.command in {"verify-evidence", "verify-bundle"} else EXIT_INVALID
    except Exception as exc:  # pragma: no cover - process boundary.
        _log_sanitized_exception("unexpected onboarding failure", exc)
        _emit({"ok": False, "command": args.command, "reason_code": REASON_UNEXPECTED_ERROR, "message": "unexpected internal error"})
        return EXIT_INTERNAL


if __name__ == "__main__":
    raise SystemExit(main())
