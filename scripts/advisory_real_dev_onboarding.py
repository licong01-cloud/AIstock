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

from backend.services.advisory_phase0a.policy import POLICY_REGISTRY_ROOT, canonical_json_text
from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    EvidenceKind,
    InventoryClassification,
    ImportCommitOutcome,
    ImportPlanStatus,
    OnboardingArtifactRef,
    PortableAdvisoryEvidenceBundle,
    RealDevHistoricalRunRequest,
    RealDevImportPlan,
    RealDevImportReceipt,
    RealDevOnboardingError,
    RealDevOnboardingInventoryReceipt,
    RealDevOnboardingInventoryQuery,
    RealDevOnboardingRequest,
    REASON_UNEXPECTED_ERROR,
    REASON_HISTORICAL_INPUT_PENDING,
    database_identity_hash,
)
from backend.services.advisory_dev_input_onboarding.phase1e_orchestration import (
    AdvisoryPhase1EOrchestrationService,
)
from backend.services.advisory_dev_input_onboarding.production_projection import (
    RealDevOnboardingInventoryService,
    RealDevProductionPackageExporter,
    load_exact_release_receipt,
)
from backend.services.advisory_dev_input_onboarding.dev_importer import RealDevPackageImporter
from backend.services.advisory_dev_input_onboarding.historical_onboarding import RealDevHistoricalOnboardingService
from backend.services.advisory_dev_input_onboarding.store import RealDevOnboardingEvidenceStore
from backend.services.strategy_package.advisory_input_projection import project_advisory_inputs
from backend.services.strategy_package.repository import StrategyPackageRepository


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

    rollback_dev = subparsers.add_parser(
        "validate-dev-rollback",
        help="execute the exact DEV importer and prove physical rollback with fresh readback",
    )
    rollback_dev.add_argument("--bundle-ref", required=True, type=Path)
    rollback_dev.add_argument("--plan", required=True, type=Path)
    rollback_dev.add_argument("--env-file", required=True, type=Path)
    rollback_dev.add_argument("--release-receipt-root", required=True, type=Path)
    rollback_dev.add_argument("--evidence-root", required=True, type=Path)
    rollback_dev.add_argument("--source-package-asset-root", required=True, type=Path)
    rollback_dev.add_argument("--target-package-asset-root", required=True, type=Path)

    verify_import = subparsers.add_parser("verify-import", help="freshly verify a completed DEV package import")
    verify_import.add_argument("--bundle-ref", required=True, type=Path)
    verify_import.add_argument("--receipt", required=True, type=Path)
    verify_import.add_argument("--plan", required=True, type=Path)
    verify_import.add_argument("--env-file", required=True, type=Path)
    verify_import.add_argument("--release-receipt-root", required=True, type=Path)
    verify_import.add_argument("--evidence-root", required=True, type=Path)
    verify_import.add_argument("--source-package-asset-root", required=True, type=Path)
    verify_import.add_argument("--target-package-asset-root", required=True, type=Path)

    run_historical = subparsers.add_parser(
        "run-historical",
        help="create exact DEV Programs, produce prospective DSE v2 and run formal historical research",
    )
    run_historical.add_argument("--historical-request", required=True, type=Path)
    run_historical.add_argument("--env-file", required=True, type=Path)
    run_historical.add_argument("--evidence-root", required=True, type=Path)
    run_historical.add_argument("--target-package-asset-root", required=True, type=Path)

    observe_source = subparsers.add_parser("observe-source", help="append exact DEV source facts for O4 Program scopes")
    observe_source.add_argument("--historical-request-ref", required=True, type=Path)
    observe_source.add_argument("--env-file", required=True, type=Path)
    observe_source.add_argument("--evidence-root", required=True, type=Path)
    observe_source.add_argument("--artifact-root", required=True, type=Path)
    _add_o4_config_args(observe_source)

    build_phase1e = subparsers.add_parser("build-phase1e-inputs", help="build exact pre-capacity O4 Program inputs")
    build_phase1e.add_argument("--historical-request-ref", required=True, type=Path)
    build_phase1e.add_argument("--historical-receipt-ref", required=True, type=Path)
    build_phase1e.add_argument("--observation-scope-ref", required=True, action="append", type=Path)
    build_phase1e.add_argument("--source-mapping-registry-ref", required=True, type=Path)
    build_phase1e.add_argument("--capacity-policy-ref", required=True, type=Path)
    build_phase1e.add_argument("--env-file", required=True, type=Path)
    build_phase1e.add_argument("--evidence-root", required=True, type=Path)
    build_phase1e.add_argument("--artifact-root", required=True, type=Path)
    build_phase1e.add_argument("--policy-registry-root", type=Path, default=POLICY_REGISTRY_ROOT)
    _add_o4_config_args(build_phase1e)

    plan_capacity = subparsers.add_parser("plan-capacity", help="probe exact DEV O4 workloads and build post-capacity inputs")
    plan_capacity.add_argument("--input-bundle-ref", required=True, type=Path)
    plan_capacity.add_argument("--env-file", required=True, type=Path)
    plan_capacity.add_argument("--artifact-root", required=True, type=Path)
    plan_capacity.add_argument("--advisory-store-root", type=Path)
    _add_o4_config_args(plan_capacity)

    compile_phase1e = subparsers.add_parser("compile-phase1e", help="compile independent single-Program Phase1E batches")
    compile_phase1e.add_argument("--input-bundle-ref", required=True, type=Path)
    compile_phase1e.add_argument("--env-file", required=True, type=Path)
    compile_phase1e.add_argument("--artifact-root", required=True, type=Path)
    return parser


def _add_o4_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config-id", default="phase1e_advisory_inputs_dev_v2")
    parser.add_argument("--config-version", default="v2")


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


def _read_o4_ref(path: Path) -> AdvisoryImmutableArtifactRef:
    return AdvisoryImmutableArtifactRef.model_validate(_read_json(path))


def _emit(value: Any) -> None:
    def jsonable(item: Any) -> Any:
        if hasattr(item, "model_dump"):
            return jsonable(item.model_dump(mode="json"))
        if isinstance(item, dict):
            return {str(key): jsonable(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [jsonable(child) for child in item]
        return item

    print(canonical_json_text(jsonable(value)))


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


def _validate_dev_rollback(args: argparse.Namespace) -> int:
    store = RealDevOnboardingEvidenceStore(root=args.evidence_root)
    bundle, bundle_ref = _load_bundle(store=store, ref_path=args.bundle_ref)
    supplied_plan = RealDevImportPlan.model_validate(_read_json(args.plan))
    receipt = RealDevPackageImporter().validate_rollback(
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
    return EXIT_SUCCESS


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


def _run_historical(args: argparse.Namespace) -> int:
    request = RealDevHistoricalRunRequest.model_validate(_read_json(args.historical_request))
    receipt, stored = RealDevHistoricalOnboardingService().run(
        request=request,
        env_file=args.env_file,
        evidence_root=args.evidence_root,
        target_package_asset_root=args.target_package_asset_root,
        repository_root=REPOSITORY_ROOT,
    )
    _emit(
        {
            "ok": receipt.batch_status == "COMPLETE",
            "command": "run-historical",
            "historical_request_hash": receipt.historical_request_hash,
            "historical_receipt_hash": receipt.receipt_hash,
            "formal_batch_receipt_hash": receipt.formal_batch_receipt_hash,
            "batch_id": receipt.batch_id,
            "batch_key": receipt.batch_key,
            "batch_status": receipt.batch_status,
            "program_results": [
                {
                    "program_id": item.program_id,
                    "package_id": item.package_id,
                    "alpha_mode": item.alpha_mode.value,
                    "status": item.status.value,
                    "reason_codes": item.reason_codes,
                }
                for item in receipt.program_results
            ],
            "receipt_relative_path": stored.relative_path,
            "receipt_store_policy_hash": stored.store_policy_hash,
            "receipt_file_sha256": stored.file_sha256,
            "receipt_idempotent": stored.idempotent,
        }
    )
    if receipt.batch_status == "COMPLETE":
        return EXIT_SUCCESS
    if receipt.batch_status == "WAITING_INPUT":
        return EXIT_INPUT_PENDING
    return EXIT_VERIFICATION_FAILED


def _project_admitted_package_inputs(*, conn_factory: Any, package_id: str) -> dict[str, Any]:
    package = StrategyPackageRepository(conn_factory=conn_factory).get(package_id)
    projected = project_advisory_inputs(package.manifest)
    return projected.model_dump(mode="json")


def _o4_service() -> AdvisoryPhase1EOrchestrationService:
    return AdvisoryPhase1EOrchestrationService(
        repository_root=REPOSITORY_ROOT,
        package_projection_provider=_project_admitted_package_inputs,
    )


def _observe_source(args: argparse.Namespace) -> int:
    result = _o4_service().observe_source(
        historical_request_ref=_read_o4_ref(args.historical_request_ref),
        env_file=args.env_file,
        evidence_root=args.evidence_root,
        artifact_root=args.artifact_root,
        config_id=args.config_id,
        config_version=args.config_version,
    )
    _emit(result)
    if result["aggregate_status"] == "COMPLETE":
        return EXIT_SUCCESS
    return EXIT_INPUT_PENDING if result["aggregate_status"] == "PENDING" else EXIT_VERIFICATION_FAILED


def _build_phase1e_inputs(args: argparse.Namespace) -> int:
    result = _o4_service().build_phase1e_inputs(
        historical_request_ref=_read_o4_ref(args.historical_request_ref),
        historical_receipt_ref=_read_o4_ref(args.historical_receipt_ref),
        observation_scope_refs=tuple(_read_o4_ref(path) for path in args.observation_scope_ref),
        source_mapping_registry_ref=_read_o4_ref(args.source_mapping_registry_ref),
        capacity_policy_ref=_read_o4_ref(args.capacity_policy_ref),
        env_file=args.env_file,
        evidence_root=args.evidence_root,
        artifact_root=args.artifact_root,
        policy_registry_root=args.policy_registry_root,
        config_id=args.config_id,
        config_version=args.config_version,
    )
    _emit(result)
    aggregate = result["bundle"].aggregate_readiness.value
    if aggregate == "ALL_FULL_READY":
        return EXIT_SUCCESS
    return EXIT_VERIFICATION_FAILED if aggregate == "BLOCKED" else EXIT_INPUT_PENDING


def _plan_capacity(args: argparse.Namespace) -> int:
    result = _o4_service().plan_capacity(
        input_bundle_ref=_read_o4_ref(args.input_bundle_ref),
        env_file=args.env_file,
        artifact_root=args.artifact_root,
        advisory_store_root=args.advisory_store_root,
        config_id=args.config_id,
        config_version=args.config_version,
    )
    _emit(result)
    aggregate = result["bundle"].aggregate_readiness.value
    if aggregate == "ALL_FULL_READY":
        return EXIT_SUCCESS
    return EXIT_VERIFICATION_FAILED if aggregate == "BLOCKED" else EXIT_INPUT_PENDING


def _compile_phase1e(args: argparse.Namespace) -> int:
    result = _o4_service().compile_phase1e(
        input_bundle_ref=_read_o4_ref(args.input_bundle_ref),
        env_file=args.env_file,
        artifact_root=args.artifact_root,
    )
    _emit(result)
    aggregate = result["compile_receipt"].aggregate_status.value
    if aggregate == "COMPLETE":
        return EXIT_SUCCESS
    return EXIT_VERIFICATION_FAILED if aggregate == "FAILED" else EXIT_INPUT_PENDING


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
        if args.command == "validate-dev-rollback":
            return _validate_dev_rollback(args)
        if args.command == "verify-import":
            return _verify_import(args)
        if args.command == "run-historical":
            return _run_historical(args)
        if args.command == "observe-source":
            return _observe_source(args)
        if args.command == "build-phase1e-inputs":
            return _build_phase1e_inputs(args)
        if args.command == "plan-capacity":
            return _plan_capacity(args)
        if args.command == "compile-phase1e":
            return _compile_phase1e(args)
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
        _emit(
            {
                "ok": False,
                "command": args.command,
                "reason_code": reason_code,
                "message": str(exc),
                "context": getattr(exc, "context", None),
            }
        )
        return EXIT_INVALID
    except RealDevOnboardingError as exc:
        reason_code = getattr(exc, "reason_code", "ADVISORY_REAL_DEV_CONTRACT_INVALID")
        LOGGER.error("advisory_onboarding_command_failed command=%s reason_code=%s", args.command, reason_code)
        _emit(
            {
                "ok": False,
                "command": args.command,
                "reason_code": reason_code,
                "message": str(exc),
                "context": exc.context,
            }
        )
        if reason_code == "ADVISORY_REAL_DEV_IMPORT_COMMIT_STATE_UNKNOWN":
            return EXIT_STATE_UNKNOWN
        if reason_code == REASON_HISTORICAL_INPUT_PENDING:
            return EXIT_INPUT_PENDING
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
