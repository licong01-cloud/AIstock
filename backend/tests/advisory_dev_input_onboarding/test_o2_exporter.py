from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
from pathlib import Path

import pytest

from backend.services.advisory_dev_input_onboarding.contracts import (
    AlphaComponentEvidence,
    AlphaMode,
    InventoryClassification,
    PackageClosureStatus,
    PackageInventoryCandidate,
    RealDevOnboardingError,
    RealDevOnboardingInventoryReceipt,
    RealDevOnboardingRequest,
    SourceFactEligibility,
    deserialize_postgres_value,
)
from backend.services.advisory_dev_input_onboarding.dev_importer import (
    _materialize_target_blobs,
    _verify_target_blobs,
)
from backend.services.advisory_dev_input_onboarding.production_projection import (
    PACKAGE_ASSET_PROVENANCE_COLUMNS,
    PACKAGE_ASSET_SEMANTIC_COLUMNS,
    PACKAGE_EXCLUDED_SOURCE_COLUMNS,
    PACKAGE_PROVENANCE_COLUMNS,
    PACKAGE_SEMANTIC_COLUMNS,
    SQL,
    RealDevProductionPackageExporter,
    _build_portable_bundle,
    _compute_manifest_json_sha256,
    _manifest_runtime_asset_refs,
)
from backend.services.advisory_dev_input_onboarding.store import RealDevOnboardingEvidenceStore
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel
from backend.services.advisory_phase1.release_schema_verify_postgres import DatabaseConnectionConfig
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
import backend.services.advisory_dev_input_onboarding.production_projection as projection_module

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def _identity() -> DatabaseIdentity:
    return DatabaseIdentity(
        target_label=TargetLabel.PRODUCTION,
        current_database="aistock",
        server_address="prod.invalid",
        server_port=5432,
        server_version_num=160000,
        current_user_hash=SHA_A,
        environment_contract_hash=SHA_B,
    )


def _component(alpha_id: str, weight: float) -> dict[str, object]:
    return {
        "alpha_id": alpha_id,
        "alpha_name": alpha_id,
        "component_weight": weight,
        "model_id": f"model_{alpha_id}",
        "holding_period": "5d" if alpha_id == "fast" else "20d",
        "rebalance_frequency": "1d" if alpha_id == "fast" else "5d",
        "score_direction": "higher_better",
        "score_normalization": "rank",
        "factor_ids": [f"factor_{alpha_id}"],
    }


def _package_row(
    package_id: str,
    manifest_sha: str,
    alpha_mode: str,
    components: list[dict[str, object]],
    *,
    asset_ref: str | None = None,
    asset_sha: str | None = None,
) -> dict[str, object]:
    manifest: dict[str, object] = {
        "package_id": package_id,
        "manifest_sha256": None,
        "alpha_mode": alpha_mode,
        "alpha_components": components,
        "runtime_assets": {},
    }
    if asset_ref is not None:
        manifest["model_asset"] = {"asset_ref": asset_ref, "sha256": asset_sha}
    manifest_sha = _compute_manifest_json_sha256(manifest)
    manifest["manifest_sha256"] = manifest_sha
    values: dict[str, object] = {
        "package_id": package_id,
        "package_name": package_id,
        "package_version": "1.0",
        "source_type": "candidate_strategy_package",
        "source_id": f"source_{package_id}",
        "loop_id": None,
        "run_id": None,
        "package_status": "SELECTION_ENABLED",
        "manifest_json": manifest,
        "manifest_sha256": manifest_sha,
        "alpha_mode": alpha_mode,
        "signal_domain": "daily",
        "display_name": package_id,
        "legacy_name": None,
        "data_vintage": None,
        "prediction_ref_uri": None,
        "prediction_ref_sha256": None,
        "model_artifact_uri": None,
        "model_artifact_sha256": None,
        "seed_policy": "fixed",
        "master_seed": 7,
        "seed_sequence": [7],
        "seed_contract": {"master_seed": 7},
        "seed_contract_sha256": SHA_C,
        "reproducibility_level": "artifact_only",
        "nondeterministic_flags": [],
        "paper_portfolio_count": 0,
        "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
    }
    assert set(values) == (
        set(PACKAGE_SEMANTIC_COLUMNS)
        | set(PACKAGE_PROVENANCE_COLUMNS)
        | set(PACKAGE_EXCLUDED_SOURCE_COLUMNS)
    )
    return values


def _request_for_packages(
    request: RealDevOnboardingRequest,
    package_rows: list[dict[str, object]],
) -> RealDevOnboardingRequest:
    payload = request.model_dump(mode="python", exclude={"request_hash"})
    payload["expected_package_manifest_sha256s"] = {
        str(row["package_id"]): str(row["manifest_sha256"]) for row in package_rows
    }
    return RealDevOnboardingRequest.model_validate(payload)


def _refreeze_package_row(row: dict[str, object]) -> None:
    manifest = row["manifest_json"]
    assert isinstance(manifest, dict)
    digest = _compute_manifest_json_sha256(manifest)
    manifest["manifest_sha256"] = digest
    row["manifest_sha256"] = digest


def test_fixed_export_registry_and_column_contract_are_closed() -> None:
    for name in ("export_snapshot_identity", "export_packages", "export_package_assets"):
        normalized = " ".join(SQL[name].split()).upper()
        assert normalized.startswith("SELECT ")
        assert all(token not in f" {normalized} " for token in (" INSERT ", " UPDATE ", " DELETE ", " TRUNCATE "))
    assert set(PACKAGE_SEMANTIC_COLUMNS).isdisjoint(PACKAGE_PROVENANCE_COLUMNS)
    assert set(PACKAGE_SEMANTIC_COLUMNS).isdisjoint(PACKAGE_EXCLUDED_SOURCE_COLUMNS)
    assert set(PACKAGE_ASSET_SEMANTIC_COLUMNS).isdisjoint(PACKAGE_ASSET_PROVENANCE_COLUMNS)


def test_projection_manifest_hash_matches_strategy_package_authority() -> None:
    row = _package_row("pkg_single", SHA_A, "single_alpha", [_component("trend", 1.0)])
    manifest = row["manifest_json"]
    assert isinstance(manifest, dict)
    assert _compute_manifest_json_sha256(manifest) == compute_manifest_json_sha256(manifest)
    assert row["manifest_sha256"] == compute_manifest_json_sha256(manifest)


def test_runtime_asset_projection_excludes_unrelated_historical_source_evidence() -> None:
    manifest = {
        "model_asset": {"asset_ref": "models/live.bin", "sha256": SHA_A},
        "source_evidence": {
            "historical_backtest_output": {
                "asset_ref": "history/backtest.parquet",
                "sha256": SHA_B,
            }
        },
    }
    assert _manifest_runtime_asset_refs(manifest) == {"models/live.bin": SHA_A}


def test_export_builds_real_blob_and_component_closure(
    tmp_path: Path,
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    raw = b"real-package-model-blob"
    digest = hashlib.sha256(raw).hexdigest()
    asset_ref = f"aistock-package-asset://blobs/{digest}?kind=model_weight&logical_name=params.pkl"
    source_root = tmp_path / "source-assets"
    source_path = source_root / "blobs" / digest[:2] / digest
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes(raw)
    store = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    packages = [
        _package_row(
            "pkg_single",
            SHA_A,
            "single_alpha",
            [_component("trend", 1.0)],
            asset_ref=asset_ref,
            asset_sha=digest,
        ),
        _package_row(
            "pkg_multi",
            SHA_B,
            "multi_alpha",
            [_component("fast", 0.6), _component("slow", 0.4)],
            asset_ref=asset_ref,
            asset_sha=digest,
        ),
    ]
    packages[1]["manifest_json"]["backtest_context"] = {  # type: ignore[index]
        "raw_backtest_config": {
            "runtime_template_dir": "F:/Dev/AIstock_worktrees/legacy/template_runtime",
        }
    }
    packages[1]["manifest_json"]["source"] = {  # type: ignore[index]
        "source_type": "multi_alpha_combine_run",
        "source_id": "source_pkg_multi",
    }
    packages[1]["source_type"] = "multi_alpha_combine_run"
    packages[1]["prediction_ref_uri"] = "F:/historical/predictions.parquet"
    packages[1]["prediction_ref_sha256"] = SHA_C
    packages[1]["model_artifact_uri"] = "F:/historical/model.pkl"
    packages[1]["model_artifact_sha256"] = SHA_C
    _refreeze_package_row(packages[1])
    onboarding_request = _request_for_packages(onboarding_request, packages)
    assets = [
        {
            "asset_id": index,
            "package_id": package_id,
            "asset_type": "model_weight",
            "asset_ref": asset_ref,
            "asset_sha256": digest,
            "metadata": {"logical_name": "params.pkl"},
            "asset_role": "governed_asset",
            "asset_size_bytes": len(raw),
            "protected_asset": True,
            "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        }
        for index, package_id in enumerate(("pkg_single", "pkg_multi"), start=8)
    ]
    bundle = _build_portable_bundle(
        request=onboarding_request,
        source_identity=_identity(),
        export_snapshot_identity="1:10:",
        package_rows=packages,
        asset_rows=assets,
        source_package_asset_root=source_root,
        evidence_store=store,
    )
    assert bundle.source_database_identity_hash
    assert len(bundle.native_multi_component_refs) == 2
    assert len(bundle.artifact_blob_refs) == 2
    multi_ref = next(item for item in bundle.package_refs if item.package_id == "pkg_multi")
    assert multi_ref.source_manifest_sha256 != multi_ref.manifest_sha256
    package_row_set = next(item for item in bundle.relation_row_sets if item.relation_name == "strategy_pkg.package")
    multi_row = next(row for row in package_row_set.sorted_rows if row["package_id"] == "pkg_multi")
    portable_manifest = deserialize_postgres_value(multi_row["manifest_json"])
    assert "backtest_context" not in portable_manifest
    assert "F:/Dev/AIstock_worktrees" not in str(portable_manifest)
    assert portable_manifest["source"]["source_type"] == "candidate_strategy_package"
    assert portable_manifest["alpha_mode"] == packages[1]["alpha_mode"]
    assert multi_row["source_type"] == "candidate_strategy_package"
    assert all(name not in multi_row for name in PACKAGE_EXCLUDED_SOURCE_COLUMNS)
    assert multi_ref.projection.runtime_asset_closure_hash
    assert multi_ref.projection.alpha_component_closure_hash
    asset_row_set = next(item for item in bundle.relation_row_sets if item.relation_name == "strategy_pkg.package_asset")
    assert len(asset_row_set.sorted_rows) == 2
    assert store.load_blob(bundle.artifact_blob_refs[0].blob_ref) == raw
    store.verify_reference_closure(bundle)
    target_root = tmp_path / "target-assets"
    _materialize_target_blobs(bundle=bundle, evidence_store=store, target_root=target_root)
    _materialize_target_blobs(bundle=bundle, evidence_store=store, target_root=target_root)
    target_path = target_root / "blobs" / digest[:2] / digest
    assert target_path.read_bytes() == raw
    _verify_target_blobs(bundle=bundle, target_root=target_root)
    target_path.write_bytes(b"tampered")
    with pytest.raises(RealDevOnboardingError, match="full readback"):
        _verify_target_blobs(bundle=bundle, target_root=target_root)


def test_export_rejects_absolute_workstation_path_without_echoing_it(
    tmp_path: Path,
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    packages = [
        _package_row("pkg_single", SHA_A, "single_alpha", [_component("trend", 1.0)]),
        _package_row("pkg_multi", SHA_B, "multi_alpha", [_component("fast", 0.6), _component("slow", 0.4)]),
    ]
    packages[0]["manifest_json"]["source_uri"] = "F:/private/model.bin"  # type: ignore[index]
    _refreeze_package_row(packages[0])
    onboarding_request = _request_for_packages(onboarding_request, packages)
    with pytest.raises(RealDevOnboardingError) as captured:
        _build_portable_bundle(
            request=onboarding_request,
            source_identity=_identity(),
            export_snapshot_identity="1:10:",
            package_rows=packages,
            asset_rows=[],
            source_package_asset_root=tmp_path / "missing-source",
            evidence_store=RealDevOnboardingEvidenceStore(root=tmp_path / "evidence"),
        )
    assert "absolute workstation path" in str(captured.value)
    assert "F:/private" not in str(captured.value)


def test_export_rejects_retired_package_lifecycle(
    tmp_path: Path,
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    packages = [
        _package_row("pkg_single", SHA_A, "single_alpha", [_component("trend", 1.0)]),
        _package_row("pkg_multi", SHA_B, "multi_alpha", [_component("fast", 0.6), _component("slow", 0.4)]),
    ]
    packages[0]["package_status"] = "RETIRED"
    onboarding_request = _request_for_packages(onboarding_request, packages)
    with pytest.raises(RealDevOnboardingError, match="manifest identity is inconsistent"):
        _build_portable_bundle(
            request=onboarding_request,
            source_identity=_identity(),
            export_snapshot_identity="1:10:",
            package_rows=packages,
            asset_rows=[],
            source_package_asset_root=tmp_path / "source",
            evidence_store=RealDevOnboardingEvidenceStore(root=tmp_path / "evidence"),
        )


def test_export_service_rechecks_same_snapshot_program_and_asset_authority(
    monkeypatch,
    tmp_path: Path,
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    raw = b"service-export-model"
    digest = hashlib.sha256(raw).hexdigest()
    runtime_ref = f"aistock-package-asset://blobs/{digest}?kind=model_weight"
    package_rows = [
        _package_row(
            "pkg_single",
            SHA_A,
            "single_alpha",
            [_component("trend", 1.0)],
            asset_ref=runtime_ref,
            asset_sha=digest,
        ),
        _package_row(
            "pkg_multi",
            SHA_B,
            "multi_alpha",
            [_component("fast", 0.6), _component("slow", 0.4)],
            asset_ref=runtime_ref,
            asset_sha=digest,
        ),
    ]
    onboarding_request = _request_for_packages(onboarding_request, package_rows)
    source_root = tmp_path / "source-assets"
    source_path = source_root / "blobs" / digest[:2] / digest
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes(raw)
    target_root = tmp_path / "target-assets"
    store = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request_ref = store.publish(onboarding_request).ref
    source_identity = _identity()
    target_identity = DatabaseIdentity(
        target_label=TargetLabel.DEV,
        current_database="aistock_dev",
        server_address="dev.invalid",
        server_port=5432,
        server_version_num=160000,
        current_user_hash=SHA_A,
        environment_contract_hash=SHA_C,
    )

    def candidate(package_id: str, digest_value: str, mode: AlphaMode) -> PackageInventoryCandidate:
        if mode is AlphaMode.SINGLE:
            components = (AlphaComponentEvidence.model_validate(_component("trend", 1.0)),)
        else:
            components = (
                AlphaComponentEvidence.model_validate(_component("fast", 0.6)),
                AlphaComponentEvidence.model_validate(_component("slow", 0.4)),
            )
        return PackageInventoryCandidate(
            package_id=package_id,
            manifest_sha256=digest_value,
            alpha_mode=mode,
            package_status="SELECTION_ENABLED",
            components=components,
            package_asset_count=2 if mode is AlphaMode.SINGLE else 1,
            has_runtime_assets=True,
            has_source_evidence=True,
            closure_status=PackageClosureStatus.O2_EXPORT_VERIFICATION_REQUIRED,
            source_program_refs=("prod_single",) if mode is AlphaMode.SINGLE else ("prod_multi",),
            dse_schema_counts={"daily_selection_evidence_v1": 1},
            binding_fact_eligibility=SourceFactEligibility.LEGACY_BINDING_INELIGIBLE,
            dse_fact_eligibility=SourceFactEligibility.DSE_V1_INELIGIBLE,
            package_eligible=True,
        )

    inventory = RealDevOnboardingInventoryReceipt(
        inventory_invocation_id="inventory_export_service",
        source_database_identity=source_identity,
        target_database_identity=target_identity,
        release_receipt_ref=onboarding_request.release_receipt_ref,
        release_catalog_fingerprint=SHA_C,
        program_candidates=(
            candidate(
                "pkg_single",
                onboarding_request.expected_package_manifest_sha256s["pkg_single"],
                AlphaMode.SINGLE,
            ),
            candidate(
                "pkg_multi",
                onboarding_request.expected_package_manifest_sha256s["pkg_multi"],
                AlphaMode.MULTI,
            ),
        ),
        selected_input_ref=request_ref,
        selected_request_hash=onboarding_request.request_hash,
        relation_row_counts={"source.strategy_pkg.package": 2},
        classification=InventoryClassification.DUAL_TRACK_AVAILABLE,
        observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    asset_rows = [
        {
            "asset_id": 1,
            "package_id": "pkg_single",
            "asset_type": "model_weight",
            "asset_ref": runtime_ref,
            "asset_sha256": digest,
            "metadata": {},
            "asset_role": "governed_asset",
            "asset_size_bytes": len(raw),
            "protected_asset": True,
            "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        },
        {
            "asset_id": 3,
            "package_id": "pkg_single",
            "asset_type": "validation_report",
            "asset_ref": "reports/single.json",
            "asset_sha256": SHA_C,
            "metadata": {},
            "asset_role": "governed_asset",
            "asset_size_bytes": None,
            "protected_asset": True,
            "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        },
        {
            "asset_id": 2,
            "package_id": "pkg_multi",
            "asset_type": "model_weight",
            "asset_ref": runtime_ref,
            "asset_sha256": digest,
            "metadata": {},
            "asset_role": "governed_asset",
            "asset_size_bytes": len(raw),
            "protected_asset": True,
            "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        },
    ]
    rows = {
        "export_packages": package_rows,
        "export_package_assets": asset_rows,
        "source_programs": [{"program_id": "prod_single"}, {"program_id": "prod_multi"}],
        "source_bindings": [
            {"program_id": "prod_single", "package_ids": ["pkg_single"]},
            {"program_id": "prod_multi", "package_ids": ["pkg_multi"]},
        ],
    }

    class Projection:
        write_query_count = 0

        def identity(self):
            return source_identity

        def one(self, name, _params):
            assert name == "export_snapshot_identity"
            return {"snapshot_identity": "1:20:"}

        def all(self, name, _params):
            return list(rows[name])

    @contextmanager
    def readonly(_config, *, connector):
        yield object()

    config = DatabaseConnectionConfig(
        target_label=TargetLabel.PRODUCTION,
        host="prod.invalid",
        port=5432,
        database="aistock",
        user="readonly",
        password="fixture",
        environment_contract_hash=source_identity.environment_contract_hash,
    )
    monkeypatch.setattr(projection_module, "resolve_database_connection", lambda **_kwargs: config)
    monkeypatch.setattr(projection_module, "readonly_onboarding_connection", readonly)
    monkeypatch.setattr(projection_module, "FixedReadOnlyProjection", lambda *_args: Projection())
    result = RealDevProductionPackageExporter(connector=lambda **_kwargs: object()).export(
        request=onboarding_request,
        request_ref=request_ref,
        inventory=inventory,
        env_file=tmp_path / ".env",
        evidence_store=store,
        source_package_asset_root=source_root,
        target_package_asset_root=target_root,
    )
    assert result.bundle_ref.semantic_content_hash == result.bundle.bundle_content_hash
    assert {item.package_id for item in result.bundle.package_refs} == {"pkg_single", "pkg_multi"}
    asset_row_set = next(
        item for item in result.bundle.relation_row_sets if item.relation_name == "strategy_pkg.package_asset"
    )
    assert {(row["package_id"], row["asset_type"]) for row in asset_row_set.sorted_rows} == {
        ("pkg_single", "model_weight"),
        ("pkg_multi", "model_weight"),
    }
