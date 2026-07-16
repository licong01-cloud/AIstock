from __future__ import annotations

import ast
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_text
from backend.services.advisory_dev_input_onboarding.store import RealDevOnboardingEvidenceStore
from backend.services.advisory_dev_input_onboarding.contracts import (
    AlphaComponentEvidence,
    AlphaMode,
    InventoryClassification,
    PackageInventoryCandidate,
    PackageClosureStatus,
    RealDevOnboardingInventoryReceipt,
    RealDevOnboardingError,
    SourceFactEligibility,
)
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel
import scripts.advisory_real_dev_onboarding as cli_module
from scripts.advisory_real_dev_onboarding import EXIT_SUCCESS, EXIT_VERIFICATION_FAILED, main


FORBIDDEN_IMPORT_PREFIXES = (
    "backend.services.selection_center",
    "backend.services.strategy_package",
    "backend.services.simulation_runtime",
    "backend.services.paper_trading",
    "backend.services.quantevolver",
    "backend.services.rdagent",
    "backend.qlib_exporter",
    "backend.infra.qmt",
)


def test_onboarding_service_has_no_shared_runtime_imports() -> None:
    service_root = Path(__file__).resolve().parents[2] / "services" / "advisory_dev_input_onboarding"
    for path in service_root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
        assert not [name for name in imported if name.startswith(FORBIDDEN_IMPORT_PREFIXES)], path.name


def test_verify_evidence_cli_full_readback_and_tamper_exit(
    tmp_path: Path, onboarding_request, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(cli_module, "load_exact_release_receipt", lambda **_kwargs: object())
    store = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    stored = store.publish(onboarding_request)
    ref_path = tmp_path / "request-ref.json"
    ref_path.write_text(canonical_json_text(stored.ref.model_dump(mode="json")), encoding="utf-8")
    args = [
        "verify-evidence",
        "--evidence-ref",
        str(ref_path),
        "--evidence-root",
        str(store.root),
        "--release-receipt-root",
        str(tmp_path / "release"),
    ]
    assert main(args) == EXIT_SUCCESS
    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    assert output["semantic_content_hash"] == onboarding_request.request_hash

    stored.path.write_text("{}", encoding="utf-8")
    assert main(args) == EXIT_VERIFICATION_FAILED
    error = json.loads(capsys.readouterr().out)
    assert error["ok"] is False
    assert error["reason_code"] == "ADVISORY_REAL_DEV_EVIDENCE_STORE_FAILED"


def _candidate(package_id: str, manifest_hash: str, alpha_mode: AlphaMode) -> PackageInventoryCandidate:
    component_count = 1 if alpha_mode is AlphaMode.SINGLE else 2
    return PackageInventoryCandidate(
        package_id=package_id,
        manifest_sha256=manifest_hash,
        alpha_mode=alpha_mode,
        package_status="SELECTION_ENABLED",
        components=tuple(
            AlphaComponentEvidence(
                alpha_id=f"alpha_{index}",
                alpha_name=f"Alpha {index}",
                component_weight=1.0 / component_count,
                holding_period="5d",
                rebalance_frequency="1d",
                score_direction="higher_better",
                score_normalization="rank",
                factor_ids=(f"factor_{index}",),
            )
            for index in range(component_count)
        ),
        package_asset_count=1,
        has_runtime_assets=True,
        has_source_evidence=True,
        closure_status=PackageClosureStatus.O2_EXPORT_VERIFICATION_REQUIRED,
        binding_fact_eligibility=SourceFactEligibility.LEGACY_BINDING_INELIGIBLE,
        dse_fact_eligibility=SourceFactEligibility.DSE_V1_INELIGIBLE,
        package_eligible=True,
    )


def test_inventory_cli_publishes_request_and_receipt_without_extra_confirmation(
    tmp_path: Path,
    onboarding_request,
    onboarding_request_ref,
    onboarding_inventory_query,
    monkeypatch,
    capsys,
) -> None:
    source_identity = DatabaseIdentity(
        target_label=TargetLabel.PRODUCTION,
        current_database="aistock",
        server_address="10.0.0.1",
        server_port=5432,
        server_version_num=160000,
        current_user_hash="a" * 64,
        environment_contract_hash="b" * 64,
    )
    target_identity = DatabaseIdentity(
        target_label=TargetLabel.DEV,
        current_database="aistock_dev",
        server_address="10.0.0.2",
        server_port=5432,
        server_version_num=160000,
        current_user_hash="a" * 64,
        environment_contract_hash="c" * 64,
    )
    receipt = RealDevOnboardingInventoryReceipt(
        inventory_invocation_id="inventory_test",
        source_database_identity=source_identity,
        target_database_identity=target_identity,
        release_receipt_ref=onboarding_request.release_receipt_ref,
        release_catalog_fingerprint="c" * 64,
        program_candidates=(
            _candidate("pkg_single", "a" * 64, AlphaMode.SINGLE),
            _candidate("pkg_multi", "b" * 64, AlphaMode.MULTI),
        ),
        selected_input_ref=onboarding_request_ref,
        selected_request_hash=onboarding_request.request_hash,
        relation_row_counts={"source.strategy_pkg.package": 2},
        dependency_closure_hash=None,
        classification=InventoryClassification.DUAL_TRACK_AVAILABLE,
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )

    class Service:
        def inventory(self, **_kwargs):
            payload = receipt.model_dump(mode="python", exclude={"inventory_hash"})
            payload["selected_input_ref"] = _kwargs["selected_input_ref"]
            input_contract = _kwargs["input_contract"]
            payload["selected_request_hash"] = getattr(input_contract, "request_hash", None)
            payload["selected_inventory_query_hash"] = getattr(input_contract, "inventory_query_hash", None)
            return RealDevOnboardingInventoryReceipt.model_validate(payload)

    monkeypatch.setattr(cli_module, "RealDevOnboardingInventoryService", Service)
    request_path = tmp_path / "request.json"
    request_path.write_text(
        canonical_json_text(onboarding_request.model_dump(mode="json")),
        encoding="utf-8",
    )
    code = main(
        [
            "inventory",
            "--request",
            str(request_path),
            "--env-file",
            str(tmp_path / ".env"),
            "--release-receipt-root",
            str(tmp_path / "release"),
            "--evidence-root",
            str(tmp_path / "evidence"),
        ]
    )
    assert code == EXIT_SUCCESS
    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    assert output["classification"] == "DUAL_TRACK_AVAILABLE"
    assert output["selected_input_ref"]["evidence_kind"] == "request"
    assert output["inventory_ref"]["evidence_kind"] == "inventory"
    store = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    inventory_ref = cli_module.OnboardingArtifactRef.model_validate(output["inventory_ref"])
    inventory = store.load(inventory_ref)
    store.verify_reference_closure(inventory)
    selected_path = store.root / inventory.selected_input_ref.relative_path
    selected_path.unlink()
    with pytest.raises(RealDevOnboardingError, match="unavailable"):
        store.verify_reference_closure(inventory)

    query_path = tmp_path / "inventory-query.json"
    query_path.write_text(
        canonical_json_text(onboarding_inventory_query.model_dump(mode="json")),
        encoding="utf-8",
    )
    query_code = main(
        [
            "inventory",
            "--inventory-query",
            str(query_path),
            "--env-file",
            str(tmp_path / ".env"),
            "--release-receipt-root",
            str(tmp_path / "release"),
            "--evidence-root",
            str(tmp_path / "query-evidence"),
        ]
    )
    assert query_code == EXIT_SUCCESS
    query_output = json.loads(capsys.readouterr().out)
    assert query_output["selected_input_ref"]["evidence_kind"] == "inventory_query"
    assert {item["manifest_sha256"] for item in query_output["program_candidates"]} == {"a" * 64, "b" * 64}


def test_verify_contract_error_uses_exit_2_not_verification_exit(tmp_path: Path, capsys) -> None:
    invalid_ref = tmp_path / "invalid-ref.json"
    invalid_ref.write_text("{}", encoding="utf-8")
    code = main(
        [
            "verify-evidence",
            "--evidence-ref",
            str(invalid_ref),
            "--evidence-root",
            str(tmp_path / "evidence"),
            "--release-receipt-root",
            str(tmp_path / "release"),
        ]
    )
    assert code == 2
    output = json.loads(capsys.readouterr().out)
    assert output["reason_code"] == "ADVISORY_REAL_DEV_CONTRACT_INVALID"
