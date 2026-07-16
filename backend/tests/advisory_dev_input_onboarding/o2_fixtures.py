from __future__ import annotations

from datetime import datetime, timezone
import hashlib

from backend.services.advisory_dev_input_onboarding.contracts import (
    AlphaComponentEvidence,
    AlphaMode,
    BundleBlobRef,
    BundlePackageRef,
    DependencyEdge,
    NativeMultiComponentRef,
    OnboardingArtifactRef,
    OnboardingBlobRef,
    PortableAdvisoryEvidenceBundle,
    PortableRelationRowSet,
    RealDevOnboardingRequest,
    compute_portable_manifest_json_sha256,
)
from backend.services.advisory_dev_input_onboarding.production_projection import (
    PACKAGE_ASSET_SEMANTIC_COLUMNS,
    PACKAGE_SEMANTIC_COLUMNS,
)

DATABASE_BUNDLE_BLOB_RAW = b"advisory-o2-disposable-postgres-model"
DATABASE_BUNDLE_BLOB_SHA256 = hashlib.sha256(DATABASE_BUNDLE_BLOB_RAW).hexdigest()


def build_database_bundle(
    request: RealDevOnboardingRequest,
    *,
    asset_metadata: dict[str, object] | None = None,
    multi_package_status: str = "SELECTION_ENABLED",
) -> tuple[PortableAdvisoryEvidenceBundle, OnboardingArtifactRef]:
    def package_row(package_id: str, digest: str, alpha_mode: str) -> dict[str, object]:
        asset_ref = f"assets/{package_id}/model.bin"
        alpha_components = (
            [
                {
                    "alpha_id": "single",
                    "alpha_name": "single",
                    "component_weight": 1.0,
                    "holding_period": "5d",
                    "rebalance_frequency": "1d",
                    "score_direction": "higher_better",
                    "score_normalization": "rank",
                    "factor_ids": ["factor_single"],
                }
            ]
            if alpha_mode == "single_alpha"
            else [
                {
                    "alpha_id": "fast",
                    "alpha_name": "fast",
                    "component_weight": 0.6,
                    "holding_period": "5d",
                    "rebalance_frequency": "1d",
                    "score_direction": "higher_better",
                    "score_normalization": "rank",
                    "factor_ids": ["factor_fast"],
                },
                {
                    "alpha_id": "slow",
                    "alpha_name": "slow",
                    "component_weight": 0.4,
                    "holding_period": "20d",
                    "rebalance_frequency": "5d",
                    "score_direction": "higher_better",
                    "score_normalization": "rank",
                    "factor_ids": ["factor_slow"],
                },
            ]
        )
        manifest: dict[str, object] = {
            "package_id": package_id,
            "manifest_sha256": None,
            "alpha_mode": alpha_mode,
            "alpha_components": alpha_components,
            "model_asset": {"asset_ref": asset_ref, "sha256": DATABASE_BUNDLE_BLOB_SHA256},
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
            "package_status": multi_package_status if package_id == "pkg_multi" else "SELECTION_ENABLED",
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
            "seed_contract_sha256": "c" * 64,
            "reproducibility_level": "artifact_only",
            "nondeterministic_flags": [],
            "paper_portfolio_count": 0,
            "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        }

    package_rows = (
        package_row("pkg_single", "a" * 64, "single_alpha"),
        package_row("pkg_multi", "b" * 64, "multi_alpha"),
    )
    request_payload = request.model_dump(mode="python", exclude={"request_hash"})
    request_payload["expected_package_manifest_sha256s"] = {
        str(row["package_id"]): str(row["manifest_sha256"]) for row in package_rows
    }
    request = RealDevOnboardingRequest.model_validate(request_payload)
    package_set = PortableRelationRowSet(
        relation_name="strategy_pkg.package",
        primary_or_natural_key_fields=("package_id",),
        semantic_column_names=PACKAGE_SEMANTIC_COLUMNS,
        source_provenance_column_names=("paper_portfolio_count", "created_at", "updated_at"),
        sorted_rows=package_rows,
    )
    asset_rows = tuple(
        {
            "asset_id": index,
            "package_id": package_id,
            "asset_type": "model_weight",
            "asset_ref": f"assets/{package_id}/model.bin",
            "asset_sha256": DATABASE_BUNDLE_BLOB_SHA256,
            "metadata": asset_metadata or {"logical_name": "model.bin"},
            "asset_role": "governed_asset",
            "asset_size_bytes": len(DATABASE_BUNDLE_BLOB_RAW),
            "protected_asset": True,
            "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        }
        for index, package_id in enumerate(("pkg_single", "pkg_multi"), start=1)
    )
    asset_set = PortableRelationRowSet(
        relation_name="strategy_pkg.package_asset",
        primary_or_natural_key_fields=("package_id", "asset_type", "asset_ref"),
        semantic_column_names=PACKAGE_ASSET_SEMANTIC_COLUMNS,
        source_provenance_column_names=("asset_id", "created_at"),
        sorted_rows=asset_rows,
    )
    components = (
        NativeMultiComponentRef(
            parent_package_id="pkg_multi",
            component=AlphaComponentEvidence(
                alpha_id="fast",
                alpha_name="fast",
                component_weight=0.6,
                holding_period="5d",
                rebalance_frequency="1d",
                score_direction="higher_better",
                score_normalization="rank",
                factor_ids=("factor_fast",),
            ),
        ),
        NativeMultiComponentRef(
            parent_package_id="pkg_multi",
            component=AlphaComponentEvidence(
                alpha_id="slow",
                alpha_name="slow",
                component_weight=0.4,
                holding_period="20d",
                rebalance_frequency="5d",
                score_direction="higher_better",
                score_normalization="rank",
                factor_ids=("factor_slow",),
            ),
        ),
    )
    asset_identities = {
        package_id: f"package_asset:{package_id}:model_weight:assets/{package_id}/model.bin"
        for package_id in ("pkg_single", "pkg_multi")
    }
    bundle = PortableAdvisoryEvidenceBundle(
        request=request,
        source_database_identity_hash="b" * 64,
        export_snapshot_identity="db-integration",
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
        native_multi_component_refs=components,
        relation_row_sets=(package_set, asset_set),
        artifact_blob_refs=(
            BundleBlobRef(
                package_id="pkg_single",
                asset_type="model_weight",
                asset_ref="assets/pkg_single/model.bin",
                blob_ref=OnboardingBlobRef(
                    relative_path=(
                        f"blobs/{DATABASE_BUNDLE_BLOB_SHA256[:2]}/{DATABASE_BUNDLE_BLOB_SHA256}.blob"
                    ),
                    blob_sha256=DATABASE_BUNDLE_BLOB_SHA256,
                    size_bytes=len(DATABASE_BUNDLE_BLOB_RAW),
                ),
            ),
            BundleBlobRef(
                package_id="pkg_multi",
                asset_type="model_weight",
                asset_ref="assets/pkg_multi/model.bin",
                blob_ref=OnboardingBlobRef(
                    relative_path=(
                        f"blobs/{DATABASE_BUNDLE_BLOB_SHA256[:2]}/{DATABASE_BUNDLE_BLOB_SHA256}.blob"
                    ),
                    blob_sha256=DATABASE_BUNDLE_BLOB_SHA256,
                    size_bytes=len(DATABASE_BUNDLE_BLOB_RAW),
                ),
            ),
        ),
        dependency_edges=(
            DependencyEdge(parent_identity="pkg_multi", child_identity="alpha_component:pkg_multi:fast", relation="PACKAGE_COMPONENT"),
            DependencyEdge(parent_identity="pkg_multi", child_identity="alpha_component:pkg_multi:slow", relation="PACKAGE_COMPONENT"),
            DependencyEdge(
                parent_identity="pkg_single",
                child_identity=asset_identities["pkg_single"],
                relation="PACKAGE_ASSET",
            ),
            DependencyEdge(
                parent_identity=asset_identities["pkg_single"],
                child_identity=f"sha256:{DATABASE_BUNDLE_BLOB_SHA256}",
                relation="ASSET_BLOB",
            ),
            DependencyEdge(
                parent_identity="pkg_multi",
                child_identity=asset_identities["pkg_multi"],
                relation="PACKAGE_ASSET",
            ),
            DependencyEdge(
                parent_identity=asset_identities["pkg_multi"],
                child_identity=f"sha256:{DATABASE_BUNDLE_BLOB_SHA256}",
                relation="ASSET_BLOB",
            ),
        ),
    )
    ref = OnboardingArtifactRef(
        evidence_kind="bundle",
        relative_path=f"bundles/{bundle.bundle_content_hash[:2]}/{bundle.bundle_content_hash}.json",
        semantic_content_hash=str(bundle.bundle_content_hash),
        file_sha256="a" * 64,
    )
    return bundle, ref
