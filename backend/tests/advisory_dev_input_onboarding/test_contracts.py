from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from backend.services.advisory_dev_input_onboarding.contracts import (
    AlphaComponentEvidence,
    AlphaMode,
    BundleBlobRef,
    BundlePackageRef,
    DependencyEdge,
    NativeMultiComponentRef,
    OnboardingBlobRef,
    PortableAdvisoryEvidenceBundle,
    PortableRelationRowSet,
    RealDevOnboardingRequest,
    RealDevOnboardingError,
    serialize_postgres_value,
)
from backend.services.advisory_dev_input_onboarding.store import RealDevOnboardingEvidenceStore

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def _component(alpha_id: str, *, window: str) -> AlphaComponentEvidence:
    return AlphaComponentEvidence(
        alpha_id=alpha_id,
        alpha_name=f"Alpha {alpha_id}",
        component_weight=0.5,
        model_id=f"model_{alpha_id}",
        holding_period=window,
        rebalance_frequency="1d" if window == "60d" else "5d",
        score_direction="higher_better",
        score_normalization="rank",
        factor_ids=(f"factor_{alpha_id}",),
    )


def _row_sets(asset_id: int = 11, asset_sha256: str = SHA_C) -> tuple[PortableRelationRowSet, PortableRelationRowSet]:
    packages = PortableRelationRowSet(
        relation_name="strategy_pkg.package",
        primary_or_natural_key_fields=("package_id",),
        semantic_column_names=("manifest_sha256", "package_id"),
        sorted_rows=(
            {"package_id": "pkg_multi", "manifest_sha256": SHA_B},
            {"package_id": "pkg_single", "manifest_sha256": SHA_A},
        ),
    )
    assets = PortableRelationRowSet(
        relation_name="strategy_pkg.package_asset",
        primary_or_natural_key_fields=("asset_ref", "asset_type", "package_id"),
        semantic_column_names=("asset_ref", "asset_sha256", "asset_type", "package_id"),
        source_provenance_column_names=("asset_id",),
        sorted_rows=(
            {
                "asset_id": asset_id,
                "package_id": "pkg_multi",
                "asset_type": "MODEL",
                "asset_ref": "models/multi.bin",
                "asset_sha256": asset_sha256,
            },
        ),
    )
    return packages, assets


def test_request_is_explicit_sorted_and_hash_closed(onboarding_request: RealDevOnboardingRequest) -> None:
    assert onboarding_request.source_package_ids == ("pkg_multi", "pkg_single")
    assert tuple(item.program_id for item in onboarding_request.target_dev_program_specs) == (
        "dev_multi",
        "dev_single",
    )
    assert onboarding_request.request_hash is not None
    reloaded = RealDevOnboardingRequest.model_validate(onboarding_request.model_dump(mode="json"))
    assert reloaded.request_hash == onboarding_request.request_hash


def test_request_rejects_backdated_decision(onboarding_request: RealDevOnboardingRequest) -> None:
    payload = onboarding_request.model_dump(mode="python", exclude={"request_hash"})
    payload["decision_trade_date"] = date(2026, 7, 19)
    with pytest.raises(ValidationError, match="inside the new binding interval"):
        RealDevOnboardingRequest.model_validate(payload)


def test_postgres_serializer_is_typed_and_round_trip_stable() -> None:
    values = (
        Decimal("1.2300"),
        datetime(2026, 7, 16, 8, 30, tzinfo=timezone.utc),
        date(2026, 7, 16),
        UUID("00000000-0000-0000-0000-000000000001"),
        b"abc",
        [Decimal("2.0")],
        {"z": Decimal("3.0")},
    )
    serialized = [serialize_postgres_value(value) for value in values]
    assert serialized[0] == {"type": "numeric", "value": "1.2300"}
    assert [serialize_postgres_value(value) for value in serialized] == serialized
    with pytest.raises(ValueError, match="timezone-aware"):
        serialize_postgres_value(datetime(2026, 7, 16, 8, 30))


def test_package_asset_surrogate_key_does_not_change_semantic_row_set_hash() -> None:
    first = _row_sets(asset_id=11)[1]
    second = _row_sets(asset_id=999)[1]
    assert first.row_content_hashes == second.row_content_hashes
    assert first.row_set_hash == second.row_set_hash
    assert first.sorted_rows != second.sorted_rows


def test_bundle_accepts_distinct_native_component_windows_and_exact_graph(
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    c1 = NativeMultiComponentRef(parent_package_id="pkg_multi", component=_component("lstm", window="60d"))
    c2 = NativeMultiComponentRef(parent_package_id="pkg_multi", component=_component("fundamental", window="8q"))
    blob = BundleBlobRef(
        package_id="pkg_multi",
        asset_type="MODEL",
        asset_ref="models/multi.bin",
        blob_ref=OnboardingBlobRef(
            relative_path=f"blobs/{SHA_C[:2]}/{SHA_C}.blob",
            blob_sha256=SHA_C,
            size_bytes=20,
        ),
    )
    asset_identity = "package_asset:pkg_multi:MODEL:models/multi.bin"
    bundle = PortableAdvisoryEvidenceBundle(
        request=onboarding_request,
        source_database_identity_hash=SHA_C,
        export_snapshot_identity="snapshot:1",
        package_refs=(
            BundlePackageRef(package_id="pkg_single", manifest_sha256=SHA_A, alpha_mode=AlphaMode.SINGLE),
            BundlePackageRef(package_id="pkg_multi", manifest_sha256=SHA_B, alpha_mode=AlphaMode.MULTI),
        ),
        native_multi_component_refs=(c1, c2),
        relation_row_sets=_row_sets(),
        artifact_blob_refs=(blob,),
        dependency_edges=(
            DependencyEdge(parent_identity="pkg_multi", child_identity="alpha_component:pkg_multi:lstm", relation="PACKAGE_COMPONENT"),
            DependencyEdge(parent_identity="pkg_multi", child_identity="alpha_component:pkg_multi:fundamental", relation="PACKAGE_COMPONENT"),
            DependencyEdge(parent_identity="pkg_multi", child_identity=asset_identity, relation="PACKAGE_ASSET"),
            DependencyEdge(parent_identity=asset_identity, child_identity=f"sha256:{SHA_C}", relation="ASSET_BLOB"),
        ),
    )
    assert {item.component.holding_period for item in bundle.native_multi_component_refs} == {"60d", "8q"}
    assert {item.component.window_evidence_status for item in bundle.native_multi_component_refs} == {
        "PROSPECTIVE_DSE_V2_REQUIRED"
    }
    assert bundle.dependency_closure_hash is not None
    assert bundle.bundle_content_hash is not None


def test_bundle_rejects_missing_dependency_edge(onboarding_request: RealDevOnboardingRequest) -> None:
    with pytest.raises(ValidationError, match="dependency graph"):
        PortableAdvisoryEvidenceBundle(
            request=onboarding_request,
            source_database_identity_hash=SHA_C,
            export_snapshot_identity="snapshot:1",
            package_refs=(
                BundlePackageRef(package_id="pkg_single", manifest_sha256=SHA_A, alpha_mode=AlphaMode.SINGLE),
                BundlePackageRef(package_id="pkg_multi", manifest_sha256=SHA_B, alpha_mode=AlphaMode.MULTI),
            ),
            native_multi_component_refs=(
                NativeMultiComponentRef(parent_package_id="pkg_multi", component=_component("a", window="20d")),
                NativeMultiComponentRef(parent_package_id="pkg_multi", component=_component("b", window="80d")),
            ),
            relation_row_sets=_row_sets(),
            artifact_blob_refs=(),
            dependency_edges=(),
        )


def test_bundle_offline_verification_reads_every_blob_and_detects_tamper(
    tmp_path: Path,
    onboarding_request: RealDevOnboardingRequest,
) -> None:
    store = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    raw = b"real-model-bytes"
    digest = hashlib.sha256(raw).hexdigest()
    stored_blob = store.publish_blob(raw=raw, expected_sha256=digest)
    repeated_blob = store.publish_blob(raw=raw, expected_sha256=digest)
    assert repeated_blob.idempotent is True
    with pytest.raises(RealDevOnboardingError, match="differs from its package asset authority"):
        store.publish_blob(raw=raw, expected_sha256=SHA_C)
    with pytest.raises(RealDevOnboardingError, match="must be bytes"):
        store.publish_blob(raw=bytearray(raw))  # type: ignore[arg-type]
    asset_identity = "package_asset:pkg_multi:MODEL:models/multi.bin"
    bundle = PortableAdvisoryEvidenceBundle(
        request=onboarding_request,
        source_database_identity_hash=SHA_C,
        export_snapshot_identity="snapshot:blob-readback",
        package_refs=(
            BundlePackageRef(package_id="pkg_single", manifest_sha256=SHA_A, alpha_mode=AlphaMode.SINGLE),
            BundlePackageRef(package_id="pkg_multi", manifest_sha256=SHA_B, alpha_mode=AlphaMode.MULTI),
        ),
        native_multi_component_refs=(
            NativeMultiComponentRef(parent_package_id="pkg_multi", component=_component("a", window="5d")),
            NativeMultiComponentRef(parent_package_id="pkg_multi", component=_component("b", window="20d")),
        ),
        relation_row_sets=_row_sets(asset_sha256=digest),
        artifact_blob_refs=(
            BundleBlobRef(
                package_id="pkg_multi",
                asset_type="MODEL",
                asset_ref="models/multi.bin",
                blob_ref=stored_blob.ref,
            ),
        ),
        dependency_edges=(
            DependencyEdge(parent_identity="pkg_multi", child_identity="alpha_component:pkg_multi:a", relation="PACKAGE_COMPONENT"),
            DependencyEdge(parent_identity="pkg_multi", child_identity="alpha_component:pkg_multi:b", relation="PACKAGE_COMPONENT"),
            DependencyEdge(parent_identity="pkg_multi", child_identity=asset_identity, relation="PACKAGE_ASSET"),
            DependencyEdge(parent_identity=asset_identity, child_identity=f"sha256:{digest}", relation="ASSET_BLOB"),
        ),
    )
    stored_bundle = store.publish(bundle)
    readback = store.load(stored_bundle.ref)
    store.verify_reference_closure(readback)
    assert store.load_blob(stored_blob.ref) == raw

    stored_blob.path.write_bytes(b"tampered-model")
    with pytest.raises(RealDevOnboardingError, match="full readback"):
        store.verify_reference_closure(readback)
