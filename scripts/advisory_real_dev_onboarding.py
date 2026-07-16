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

from pydantic import ValidationError

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.advisory_phase0a.policy import canonical_json_text
from backend.services.advisory_dev_input_onboarding.contracts import (
    EvidenceKind,
    InventoryClassification,
    ImportCommitOutcome,
    ImportPlanStatus,
    OnboardingArtifactRef,
    PortableAdvisoryEvidenceBundle,
    RealDevImportPlan,
    RealDevImportReceipt,
    RealDevOnboardingError,
    RealDevOnboardingInventoryReceipt,
    RealDevOnboardingInventoryQuery,
    RealDevOnboardingRequest,
    REASON_UNEXPECTED_ERROR,
    database_identity_hash,
)
from backend.services.advisory_dev_input_onboarding.production_projection import (
    RealDevOnboardingInventoryService,
    RealDevProductionPackageExporter,
    load_exact_release_receipt,
)
from backend.services.advisory_dev_input_onboarding.dev_importer import RealDevPackageImporter
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

    export_bundle = subparsers.add_parser("export-bundle", help="export the exact production package closure")
    export_bundle.add_argument("--request", required=True, type=Path)
    export_bundle.add_argument("--inventory-ref", required=True, type=Path)
    export_bundle.add_argument("--env-file", required=True, type=Path)
    export_bundle.add_argument("--evidence-root", required=True, type=Path)
    export_bundle.add_argument("--source-package-asset-root", required=True, type=Path)
    export_bundle.add_argument("--target-package-asset-root", required=True, type=Path)

    plan_import = subparsers.add_parser("plan-import", help="classify exact DEV rows without DML")
    plan_import.add_argument("--bundle-ref", required=True, type=Path)
    plan_import.add_argument("--env-file", required=True, type=Path)
    plan_import.add_argument("--release-receipt-root", required=True, type=Path)
    plan_import.add_argument("--evidence-root", required=True, type=Path)

    import_dev = subparsers.add_parser("import-dev", help="execute the fixed DEV package INSERT-or-compare protocol")
    import_dev.add_argument("--bundle-ref", required=True, type=Path)
    import_dev.add_argument("--plan", required=True, type=Path)
    import_dev.add_argument("--env-file", required=True, type=Path)
    import_dev.add_argument("--release-receipt-root", required=True, type=Path)
    import_dev.add_argument("--evidence-root", required=True, type=Path)
    import_dev.add_argument("--source-package-asset-root", required=True, type=Path)
    import_dev.add_argument("--target-package-asset-root", required=True, type=Path)

    verify_import = subparsers.add_parser("verify-import", help="freshly verify a completed DEV package import")
    verify_import.add_argument("--bundle-ref", required=True, type=Path)
    verify_import.add_argument("--receipt", required=True, type=Path)
    verify_import.add_argument("--plan", required=True, type=Path)
    verify_import.add_argument("--env-file", required=True, type=Path)
    verify_import.add_argument("--release-receipt-root", required=True, type=Path)
    verify_import.add_argument("--evidence-root", required=True, type=Path)
    verify_import.add_argument("--source-package-asset-root", required=True, type=Path)
    verify_import.add_argument("--target-package-asset-root", required=True, type=Path)
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


def _load_bundle(*, store: RealDevOnboardingEvidenceStore, ref_path: Path) -> tuple[PortableAdvisoryEvidenceBundle, OnboardingArtifactRef]:
    ref = _read_ref(ref_path, expected=EvidenceKind.BUNDLE)
    bundle = store.load(ref)
    store.verify_reference_closure(bundle)
    if not isinstance(bundle, PortableAdvisoryEvidenceBundle):
        raise ValueError("bundle ref resolved to the wrong evidence model")
    return bundle, ref


def _export_bundle(args: argparse.Namespace) -> int:
    request = RealDevOnboardingRequest.model_validate(_read_json(args.request))
    store = RealDevOnboardingEvidenceStore(root=args.evidence_root)
    request_ref = store.publish(request).ref
    inventory_ref = _read_ref(args.inventory_ref, expected=EvidenceKind.INVENTORY)
    inventory = store.load(inventory_ref)
    store.verify_reference_closure(inventory)
    if not isinstance(inventory, RealDevOnboardingInventoryReceipt):
        raise ValueError("inventory ref resolved to the wrong evidence model")
    result = RealDevProductionPackageExporter().export(
        request=request,
        request_ref=request_ref,
        inventory=inventory,
        env_file=args.env_file,
        evidence_store=store,
        source_package_asset_root=args.source_package_asset_root,
        target_package_asset_root=args.target_package_asset_root,
    )
    _emit(
        {
            "ok": True,
            "command": "export-bundle",
            "bundle_ref": result.bundle_ref,
            "bundle_hash": result.bundle.bundle_content_hash,
            "dependency_closure_hash": result.bundle.dependency_closure_hash,
            "idempotent": result.idempotent,
        }
    )
    return EXIT_SUCCESS


def _plan_import(args: argparse.Namespace) -> int:
    store = RealDevOnboardingEvidenceStore(root=args.evidence_root)
    bundle, bundle_ref = _load_bundle(store=store, ref_path=args.bundle_ref)
    plan = RealDevPackageImporter().plan(
        bundle=bundle,
        bundle_ref=bundle_ref,
        evidence_store=store,
        env_file=args.env_file,
        release_receipt_root=args.release_receipt_root,
    )
    _emit(plan)
    return EXIT_INVALID if plan.status is ImportPlanStatus.CONFLICT else EXIT_SUCCESS


def _import_dev(args: argparse.Namespace) -> int:
    store = RealDevOnboardingEvidenceStore(root=args.evidence_root)
    bundle, bundle_ref = _load_bundle(store=store, ref_path=args.bundle_ref)
    supplied_plan = RealDevImportPlan.model_validate(_read_json(args.plan))
    receipt = RealDevPackageImporter().import_dev(
        bundle=bundle,
        bundle_ref=bundle_ref,
        supplied_plan=supplied_plan,
        evidence_store=store,
        env_file=args.env_file,
        release_receipt_root=args.release_receipt_root,
        source_package_asset_root=args.source_package_asset_root,
        target_package_asset_root=args.target_package_asset_root,
    )
    _emit(receipt)
    return EXIT_STATE_UNKNOWN if receipt.commit_outcome is ImportCommitOutcome.STATE_UNKNOWN else EXIT_SUCCESS


def _verify_import(args: argparse.Namespace) -> int:
    store = RealDevOnboardingEvidenceStore(root=args.evidence_root)
    bundle, bundle_ref = _load_bundle(store=store, ref_path=args.bundle_ref)
    receipt = RealDevImportReceipt.model_validate(_read_json(args.receipt))
    supplied_plan = RealDevImportPlan.model_validate(_read_json(args.plan))
    RealDevPackageImporter().verify_import(
        bundle=bundle,
        bundle_ref=bundle_ref,
        receipt=receipt,
        supplied_plan=supplied_plan,
        evidence_store=store,
        env_file=args.env_file,
        release_receipt_root=args.release_receipt_root,
        source_package_asset_root=args.source_package_asset_root,
        target_package_asset_root=args.target_package_asset_root,
    )
    _emit(
        {
            "ok": True,
            "command": "verify-import",
            "receipt_hash": receipt.receipt_hash,
            "commit_outcome": receipt.commit_outcome.value,
            "bundle_hash": receipt.bundle_hash,
        }
    )
    return EXIT_SUCCESS


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
        if args.command == "export-bundle":
            return _export_bundle(args)
        if args.command == "plan-import":
            return _plan_import(args)
        if args.command == "import-dev":
            return _import_dev(args)
        if args.command == "verify-import":
            return _verify_import(args)
        if args.command == "verify-evidence":
            return _verify(args)
        if args.command == "verify-bundle":
            return _verify(args, expected=EvidenceKind.BUNDLE)
        raise ValueError("unsupported command")
    except ValidationError as exc:
        reason_code = "ADVISORY_REAL_DEV_CONTRACT_INVALID"
        LOGGER.error(
            "advisory_onboarding_contract_validation_failed command=%s reason_code=%s error_count=%s",
            args.command,
            reason_code,
            exc.error_count(),
        )
        fields = sorted(
            {
                ".".join(str(part) for part in error.get("loc", ())) or "contract"
                for error in exc.errors(include_url=False, include_context=False, include_input=False)
            }
        )
        _emit(
            {
                "ok": False,
                "command": args.command,
                "reason_code": reason_code,
                "message": "input contract validation failed",
                "invalid_fields": fields,
                "error_count": exc.error_count(),
            }
        )
        return EXIT_INVALID
    except ValueError as exc:
        reason_code = "ADVISORY_REAL_DEV_CONTRACT_INVALID"
        LOGGER.error("advisory_onboarding_command_failed command=%s reason_code=%s", args.command, reason_code)
        _emit({"ok": False, "command": args.command, "reason_code": reason_code, "message": str(exc)})
        return EXIT_INVALID
    except RealDevOnboardingError as exc:
        reason_code = getattr(exc, "reason_code", "ADVISORY_REAL_DEV_CONTRACT_INVALID")
        LOGGER.error("advisory_onboarding_command_failed command=%s reason_code=%s", args.command, reason_code)
        _emit({"ok": False, "command": args.command, "reason_code": reason_code, "message": str(exc)})
        if reason_code == "ADVISORY_REAL_DEV_IMPORT_COMMIT_STATE_UNKNOWN":
            return EXIT_STATE_UNKNOWN
        if args.command in {"verify-evidence", "verify-bundle", "verify-import"} or reason_code in {
            "ADVISORY_REAL_DEV_IMPORT_COMMIT_NOT_OBSERVED",
            "ADVISORY_REAL_DEV_IMPORT_READBACK_FAILED",
            "ADVISORY_REAL_DEV_IMPORT_TRANSACTION_FAILED",
        }:
            return EXIT_VERIFICATION_FAILED
        return EXIT_INVALID
    except Exception as exc:  # pragma: no cover - process boundary.
        _log_sanitized_exception("unexpected onboarding failure", exc)
        _emit({"ok": False, "command": args.command, "reason_code": REASON_UNEXPECTED_ERROR, "message": "unexpected internal error"})
        return EXIT_INTERNAL


if __name__ == "__main__":
    raise SystemExit(main())
