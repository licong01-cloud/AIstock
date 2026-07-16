from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_dev_input_onboarding.contracts import (
    AlphaComponentEvidence,
    AlphaMode,
    BundleBlobRef,
    BundlePackageRef,
    DependencyEdge,
    ImportCommitOutcome,
    ImportPlanStatus,
    NativeMultiComponentRef,
    OnboardingArtifactRef,
    OnboardingBlobRef,
    PortableAdvisoryEvidenceBundle,
    PortableRelationRowSet,
    RealDevImportReceipt,
    RealDevOnboardingError,
    RealDevOnboardingRequest,
    REASON_IMPORT_COMMIT_NOT_OBSERVED,
    compute_portable_manifest_json_sha256,
    database_identity_hash,
    deserialize_postgres_value,
    serialize_postgres_value,
    TargetDevProgramSpec,
)
from backend.services.advisory_dev_input_onboarding.dev_importer import (
    PACKAGE_ASSET_SEMANTIC_COLUMNS,
    PACKAGE_SEMANTIC_COLUMNS,
    RealDevPackageImporter,
    _validate_receipt_authority,
    _verify_target_blobs,
    build_import_plan,
)
from backend.services.advisory_dev_input_onboarding.store import RealDevOnboardingEvidenceStore
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def _identity() -> DatabaseIdentity:
    return DatabaseIdentity(
        target_label=TargetLabel.DEV,
        current_database="aistock_dev",
        server_address="dev.invalid",
        server_port=5432,
        server_version_num=160000,
        current_user_hash=SHA_A,
        environment_contract_hash=SHA_C,
    )


def _component(alpha_id: str, weight: float) -> AlphaComponentEvidence:
    return AlphaComponentEvidence(
        alpha_id=alpha_id,
        alpha_name=alpha_id,
        component_weight=weight,
        model_id=f"model_{alpha_id}",
        holding_period="5d" if alpha_id == "fast" else "20d",
        rebalance_frequency="1d" if alpha_id == "fast" else "5d",
        score_direction="higher_better",
        score_normalization="rank",
        factor_ids=(f"factor_{alpha_id}",),
    )


def _package_row(package_id: str, digest: str, alpha_mode: str) -> dict[str, object]:
    asset_ref = f"assets/{package_id}/model.bin"
    manifest: dict[str, object] = {
        "package_id": package_id,
        "manifest_sha256": None,
        "alpha_mode": alpha_mode,
        "model_asset": {"asset_ref": asset_ref, "sha256": SHA_C},
    }
    digest = compute_portable_manifest_json_sha256(manifest)
    manifest["manifest_sha256"] = digest
    return {
        "package_id": package_id,
        "package_name": package_id,
        "package_version": "1.0",
        "source_type": "candidate_strategy_package",
        "source_id": f"source_{package_id}",
        "loop_id": None,
        "run_id": None,
        "package_status": "SELECTION_ENABLED",
        "manifest_json": manifest,
        "manifest_sha256": digest,
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
        "paper_portfolio_count": 3,
        "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 7, 2, tzinfo=timezone.utc),
    }


def _asset_row(package_id: str, asset_id: int) -> dict[str, object]:
    return {
        "asset_id": asset_id,
        "package_id": package_id,
        "asset_type": "model_weight",
        "asset_ref": f"assets/{package_id}/model.bin",
        "asset_sha256": SHA_C,
        "metadata": {"logical_name": "params.pkl"},
        "asset_role": "governed_asset",
        "asset_size_bytes": 32,
        "protected_asset": True,
        "created_at": datetime(2026, 7, 2, tzinfo=timezone.utc),
    }


def _bundle(onboarding_request: RealDevOnboardingRequest) -> tuple[PortableAdvisoryEvidenceBundle, OnboardingArtifactRef]:
    package_rows = (
        _package_row("pkg_single", SHA_A, "single_alpha"),
        _package_row("pkg_multi", SHA_B, "multi_alpha"),
    )
    request_payload = onboarding_request.model_dump(mode="python", exclude={"request_hash"})
    request_payload["expected_package_manifest_sha256s"] = {
        str(row["package_id"]): str(row["manifest_sha256"]) for row in package_rows
    }
    onboarding_request = RealDevOnboardingRequest.model_validate(request_payload)
    asset_rows = (_asset_row("pkg_single", 40), _asset_row("pkg_multi", 41))
    package_set = PortableRelationRowSet(
        relation_name="strategy_pkg.package",
        primary_or_natural_key_fields=("package_id",),
        semantic_column_names=PACKAGE_SEMANTIC_COLUMNS,
        source_provenance_column_names=("paper_portfolio_count", "created_at", "updated_at"),
        sorted_rows=package_rows,
    )
    asset_set = PortableRelationRowSet(
        relation_name="strategy_pkg.package_asset",
        primary_or_natural_key_fields=("package_id", "asset_type", "asset_ref"),
        semantic_column_names=PACKAGE_ASSET_SEMANTIC_COLUMNS,
        source_provenance_column_names=("asset_id", "created_at"),
        sorted_rows=asset_rows,
    )
    asset_identities = {
        str(row["package_id"]): f"package_asset:{row['package_id']}:model_weight:{row['asset_ref']}"
        for row in asset_rows
    }
    bundle = PortableAdvisoryEvidenceBundle(
        request=onboarding_request,
        source_database_identity_hash=SHA_B,
        export_snapshot_identity="snapshot:O2",
        package_refs=(
            BundlePackageRef(
                package_id="pkg_single",
                manifest_sha256=str(package_rows[0]["manifest_sha256"]),
                alpha_mode=AlphaMode.SINGLE,
            ),
            BundlePackageRef(
                package_id="pkg_multi",
                manifest_sha256=str(package_rows[1]["manifest_sha256"]),
                alpha_mode=AlphaMode.MULTI,
            ),
        ),
        native_multi_component_refs=(
            NativeMultiComponentRef(parent_package_id="pkg_multi", component=_component("fast", 0.6)),
            NativeMultiComponentRef(parent_package_id="pkg_multi", component=_component("slow", 0.4)),
        ),
        relation_row_sets=(package_set, asset_set),
        artifact_blob_refs=(
            BundleBlobRef(
                package_id="pkg_single",
                asset_type="model_weight",
                asset_ref=str(asset_rows[0]["asset_ref"]),
                blob_ref=OnboardingBlobRef(
                    relative_path=f"blobs/{SHA_C[:2]}/{SHA_C}.blob",
                    blob_sha256=SHA_C,
                    size_bytes=32,
                ),
            ),
            BundleBlobRef(
                package_id="pkg_multi",
                asset_type="model_weight",
                asset_ref=str(asset_rows[1]["asset_ref"]),
                blob_ref=OnboardingBlobRef(
                    relative_path=f"blobs/{SHA_C[:2]}/{SHA_C}.blob",
                    blob_sha256=SHA_C,
                    size_bytes=32,
                ),
            ),
        ),
        dependency_edges=(
            DependencyEdge(
                parent_identity="pkg_multi",
                child_identity="alpha_component:pkg_multi:fast",
                relation="PACKAGE_COMPONENT",
            ),
            DependencyEdge(
                parent_identity="pkg_multi",
                child_identity="alpha_component:pkg_multi:slow",
                relation="PACKAGE_COMPONENT",
            ),
            DependencyEdge(
                parent_identity="pkg_single",
                child_identity=asset_identities["pkg_single"],
                relation="PACKAGE_ASSET",
            ),
            DependencyEdge(
                parent_identity=asset_identities["pkg_single"],
                child_identity=f"sha256:{SHA_C}",
                relation="ASSET_BLOB",
            ),
            DependencyEdge(
                parent_identity="pkg_multi",
                child_identity=asset_identities["pkg_multi"],
                relation="PACKAGE_ASSET",
            ),
            DependencyEdge(
                parent_identity=asset_identities["pkg_multi"],
                child_identity=f"sha256:{SHA_C}",
                relation="ASSET_BLOB",
            ),
        ),
    )
    return bundle, OnboardingArtifactRef(
        evidence_kind="bundle",
        relative_path=f"bundles/{bundle.bundle_content_hash[:2]}/{bundle.bundle_content_hash}.json",
        semantic_content_hash=str(bundle.bundle_content_hash),
        file_sha256=SHA_A,
    )


def _raw_target_rows(bundle: PortableAdvisoryEvidenceBundle) -> dict[str, list[dict[str, object]]]:
    return {
        row_set.relation_name: [
            {name: deserialize_postgres_value(row[name]) for name in row_set.semantic_column_names}
            for row in row_set.sorted_rows
        ]
        for row_set in bundle.relation_row_sets
    }


def test_postgres_serializer_inverse_round_trip() -> None:
    values = [7, "value", [7], {"nested": ["x"]}, datetime(2026, 7, 16, tzinfo=timezone.utc)]
    assert [deserialize_postgres_value(serialize_postgres_value(value)) for value in values] == values


def test_request_supports_multiple_independent_packages_per_alpha_mode(
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    payload = onboarding_request.model_dump(mode="python", exclude={"request_hash"})
    payload["source_package_ids"] = (*payload["source_package_ids"], "pkg_single_2")
    payload["target_dev_program_specs"] = (
        *payload["target_dev_program_specs"],
        TargetDevProgramSpec(
            program_id="dev_single_2",
            package_id="pkg_single_2",
            alpha_mode=AlphaMode.SINGLE,
            target_count=5,
            review_policy={"mode": "manual_research_review"},
            style="trend_following",
        ),
    )
    payload["expected_program_packages"] = {
        **payload["expected_program_packages"],
        "dev_single_2": "pkg_single_2",
    }
    payload["expected_package_manifest_sha256s"] = {
        **payload["expected_package_manifest_sha256s"],
        "pkg_single_2": "d" * 64,
    }
    expanded = RealDevOnboardingRequest.model_validate(payload)
    assert len(expanded.target_dev_program_specs) == 3
    assert {item.package_id for item in expanded.target_dev_program_specs} == set(expanded.source_package_ids)


def test_controlled_package_asset_uri_is_portable_and_arbitrary_scheme_is_rejected() -> None:
    BundleBlobRef(
        package_id="pkg",
        asset_type="model_weight",
        asset_ref=f"aistock-package-asset://blobs/{SHA_C}?kind=model_weight",
        blob_ref=OnboardingBlobRef(
            relative_path=f"blobs/{SHA_C[:2]}/{SHA_C}.blob",
            blob_sha256=SHA_C,
            size_bytes=1,
        ),
    )
    with pytest.raises(ValidationError, match="controlled package asset scheme"):
        BundleBlobRef(
            package_id="pkg",
            asset_type="model_weight",
            asset_ref=f"file://blobs/{SHA_C}",
            blob_ref=OnboardingBlobRef(
                relative_path=f"blobs/{SHA_C[:2]}/{SHA_C}.blob",
                blob_sha256=SHA_C,
                size_bytes=1,
            ),
        )
    with pytest.raises(ValidationError, match="URI digest differs"):
        BundleBlobRef(
            package_id="pkg",
            asset_type="model_weight",
            asset_ref=f"aistock-package-asset://blobs/{SHA_B}",
            blob_ref=OnboardingBlobRef(
                relative_path=f"blobs/{SHA_C[:2]}/{SHA_C}.blob",
                blob_sha256=SHA_C,
                size_bytes=1,
            ),
        )


def test_bundle_rejects_package_asset_row_without_exact_blob_closure(
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    bundle, _ref = _bundle(onboarding_request)
    payload = bundle.model_dump(mode="python")
    payload["artifact_blob_refs"] = ()
    with pytest.raises(ValidationError, match="every package asset row must close"):
        PortableAdvisoryEvidenceBundle.model_validate(payload)


def test_plan_classifies_insert_exact_and_conflict_without_conflict_dml(
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    bundle, ref = _bundle(onboarding_request)
    empty = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation={"strategy_pkg.package": [], "strategy_pkg.package_asset": []},
    )
    assert empty.status is ImportPlanStatus.EXECUTABLE
    assert [item.relation_name for item in empty.ordered_write_operations] == [
        "strategy_pkg.package",
        "strategy_pkg.package",
        "strategy_pkg.package_asset",
        "strategy_pkg.package_asset",
    ]

    target = _raw_target_rows(bundle)
    exact = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation=target,
    )
    assert exact.status is ImportPlanStatus.ALREADY_PRESENT
    assert exact.ordered_write_operations == ()

    target["strategy_pkg.package"][0]["package_status"] = "RETIRED"
    target["strategy_pkg.package_asset"] = []
    conflict = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation=target,
    )
    assert conflict.status is ImportPlanStatus.CONFLICT
    assert conflict.insert_rows_by_relation["strategy_pkg.package_asset"]
    assert conflict.conflict_rows_by_relation["strategy_pkg.package"]
    assert conflict.ordered_write_operations == ()
    assert conflict.planned_write_relation_set == ()


def test_import_receipt_outcome_invariants(onboarding_request: RealDevOnboardingRequest) -> None:
    bundle, ref = _bundle(onboarding_request)
    plan = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation={"strategy_pkg.package": [], "strategy_pkg.package_asset": []},
    )
    now = datetime.now(timezone.utc)
    receipt = RealDevImportReceipt(
        import_invocation_id="import_1",
        bundle_ref=ref,
        request_hash=str(bundle.request.request_hash),
        bundle_hash=str(bundle.bundle_content_hash),
        plan_hash=str(plan.plan_hash),
        source_database_identity_hash=bundle.source_database_identity_hash,
        target_database_identity_hash=SHA_C,
        transaction_id="tx1",
        inserted_row_counts={"strategy_pkg.package": 2, "strategy_pkg.package_asset": 2},
        matched_row_counts={"strategy_pkg.package": 2, "strategy_pkg.package_asset": 2},
        write_relation_set=("strategy_pkg.package", "strategy_pkg.package_asset"),
        post_readback_row_hashes={
            item.relation_name: tuple(sorted(item.row_content_hashes)) for item in bundle.relation_row_sets
        },
        post_dependency_closure_hash=str(bundle.dependency_closure_hash),
        physical_commit_count=1,
        commit_outcome=ImportCommitOutcome.COMMITTED,
        started_at=now,
        finished_at=now,
    )
    assert receipt.receipt_hash is not None
    with pytest.raises(ValidationError, match="one physical commit"):
        RealDevImportReceipt.model_validate(
            {**receipt.model_dump(mode="python", exclude={"receipt_hash"}), "physical_commit_count": 0}
        )


def test_commit_uncertainty_uses_stable_natural_keys_for_all_three_outcomes(
    tmp_path,
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    bundle, ref = _bundle(onboarding_request)
    empty_rows = {"strategy_pkg.package": [], "strategy_pkg.package_asset": []}
    execution_plan = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation=empty_rows,
    )
    target = _raw_target_rows(bundle)
    exact = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation=target,
    )

    class Importer(RealDevPackageImporter):
        def __init__(self, result):
            self.result = result

        def plan(self, **_kwargs):
            return self.result

    kwargs = {
        "bundle": bundle,
        "bundle_ref": ref,
        "execution_plan": execution_plan,
        "transaction_id": "tx1",
        "evidence_store": RealDevOnboardingEvidenceStore(root=tmp_path / "evidence"),
        "env_file": tmp_path / ".env",
        "release_receipt_root": tmp_path / "release",
        "started_at": datetime.now(timezone.utc),
    }
    committed = Importer(exact)._resolve_commit_uncertainty(**kwargs)
    assert committed.commit_outcome is ImportCommitOutcome.COMMITTED

    with pytest.raises(RealDevOnboardingError) as not_observed:
        Importer(execution_plan)._resolve_commit_uncertainty(**kwargs)
    assert not_observed.value.reason_code == REASON_IMPORT_COMMIT_NOT_OBSERVED

    target["strategy_pkg.package_asset"] = []
    mixed = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation=target,
    )
    unknown = Importer(mixed)._resolve_commit_uncertainty(**kwargs)
    assert unknown.commit_outcome is ImportCommitOutcome.STATE_UNKNOWN
    assert unknown.physical_commit_count is None

    class Unavailable(RealDevPackageImporter):
        def plan(self, **_kwargs):
            raise RealDevOnboardingError("ADVISORY_REAL_DEV_IMPORT_READBACK_FAILED", "unavailable")

    unavailable = Unavailable()._resolve_commit_uncertainty(**kwargs)
    assert unavailable.commit_outcome is ImportCommitOutcome.STATE_UNKNOWN
    assert unavailable.post_readback_row_hashes == {}


def test_commit_uncertainty_checks_preexisting_exact_rows_before_committed(
    tmp_path,
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    bundle, ref = _bundle(onboarding_request)
    target = _raw_target_rows(bundle)
    execution_rows = {
        "strategy_pkg.package": [target["strategy_pkg.package"][0].copy()],
        "strategy_pkg.package_asset": [],
    }
    execution_plan = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation=execution_rows,
    )
    target["strategy_pkg.package"][0]["package_status"] = "RETIRED"
    fresh_conflict = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation=target,
    )

    class Importer(RealDevPackageImporter):
        def plan(self, **_kwargs):
            return fresh_conflict

    unknown = Importer()._resolve_commit_uncertainty(
        bundle=bundle,
        bundle_ref=ref,
        execution_plan=execution_plan,
        transaction_id="tx1",
        evidence_store=RealDevOnboardingEvidenceStore(root=tmp_path / "evidence"),
        env_file=tmp_path / ".env",
        release_receipt_root=tmp_path / "release",
        started_at=datetime.now(timezone.utc),
    )
    assert fresh_conflict.status is ImportPlanStatus.CONFLICT
    assert unknown.commit_outcome is ImportCommitOutcome.STATE_UNKNOWN


def test_receipt_authority_rejects_forged_request_source_plan_and_counts(
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    bundle, ref = _bundle(onboarding_request)
    target = _raw_target_rows(bundle)
    supplied_plan = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation={"strategy_pkg.package": [], "strategy_pkg.package_asset": []},
    )
    readback = build_import_plan(
        bundle=bundle,
        bundle_ref=ref,
        target_database_identity=_identity(),
        target_rows_by_relation=target,
    )
    now = datetime.now(timezone.utc)
    forged = RealDevImportReceipt(
        import_invocation_id="forged",
        bundle_ref=ref,
        request_hash="d" * 64,
        bundle_hash=str(bundle.bundle_content_hash),
        plan_hash="e" * 64,
        source_database_identity_hash="f" * 64,
        target_database_identity_hash=database_identity_hash(_identity()),
        transaction_id="tx-forged",
        inserted_row_counts={"strategy_pkg.package": 2, "strategy_pkg.package_asset": 2},
        matched_row_counts={"strategy_pkg.package": 99, "strategy_pkg.package_asset": 2},
        write_relation_set=("strategy_pkg.package", "strategy_pkg.package_asset"),
        post_readback_row_hashes={
            item.relation_name: tuple(sorted(item.row_content_hashes)) for item in bundle.relation_row_sets
        },
        post_dependency_closure_hash=str(bundle.dependency_closure_hash),
        physical_commit_count=1,
        commit_outcome=ImportCommitOutcome.COMMITTED,
        started_at=now,
        finished_at=now,
    )
    with pytest.raises(RealDevOnboardingError, match="receipt authority differs"):
        _validate_receipt_authority(
            bundle=bundle,
            bundle_ref=ref,
            receipt=forged,
            supplied_plan=supplied_plan,
            readback=readback,
        )


def test_missing_target_blob_uses_stable_readback_error(tmp_path, onboarding_request: RealDevOnboardingRequest) -> None:
    bundle, _ = _bundle(onboarding_request)
    with pytest.raises(RealDevOnboardingError) as captured:
        _verify_target_blobs(bundle=bundle, target_root=tmp_path / "missing")
    assert captured.value.reason_code == "ADVISORY_REAL_DEV_IMPORT_READBACK_FAILED"
