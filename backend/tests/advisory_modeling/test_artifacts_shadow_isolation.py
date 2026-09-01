from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.advisory_modeling.bundle_store import (
    ImmutableArtifactStore,
    ImmutableModelBundleStore,
    ImmutableModelBundleV1,
    build_file_descriptors,
)
from backend.services.advisory_modeling.errors import (
    AdvisoryModelingError,
    REASON_BUNDLE_HASH_MISMATCH,
    REASON_BUNDLE_INCOMPLETE,
    REASON_EXACT_RETRY_CONFLICT,
)
from backend.services.advisory_modeling.feature_snapshot import (
    FeaturePartitionDescriptorV1,
    RerankerFeatureSnapshotStore,
    RerankerFeatureSnapshotV1,
)
from backend.services.advisory_modeling.shadow_inference import (
    ShadowCandidateScoreV1,
    build_shadow_result,
)


_HASH = "a" * 64


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def test_artifact_store_receipt_last_exact_retry_and_tamper_visible(tmp_path: Path) -> None:
    root = (tmp_path / "artifact-root").resolve()
    root.mkdir()
    store = ImmutableArtifactStore(
        artifact_root=root,
        repository_root=_repository_root(),
        namespace="reports",
    )
    files = {"report.json": b'{"status":"complete"}\n'}

    first = store.publish(artifact_id="artifact-1", semantic_hash=_HASH, files=files)
    second = store.publish(artifact_id="artifact-1", semantic_hash=_HASH, files=files)

    assert first == second
    assert (root / "reports" / "artifact-1" / "completion_receipt.json").exists()
    (root / "reports" / "artifact-1" / "report.json").write_bytes(b"tampered")
    with pytest.raises(AdvisoryModelingError) as error:
        store.read_exact(artifact_id="artifact-1", expected_semantic_hash=_HASH)
    assert error.value.reason_code == REASON_BUNDLE_HASH_MISMATCH


def test_artifact_store_rejects_retry_identity_conflict(tmp_path: Path) -> None:
    root = (tmp_path / "artifact-root").resolve()
    root.mkdir()
    store = ImmutableArtifactStore(
        artifact_root=root,
        repository_root=_repository_root(),
        namespace="reports",
    )
    store.publish(artifact_id="artifact-1", semantic_hash=_HASH, files={"one": b"1"})
    with pytest.raises(AdvisoryModelingError) as error:
        store.publish(artifact_id="artifact-1", semantic_hash="b" * 64, files={"one": b"1"})
    assert error.value.reason_code == REASON_EXACT_RETRY_CONFLICT


def test_artifact_store_requires_existing_repo_external_non_wsl_root(tmp_path: Path) -> None:
    with pytest.raises(AdvisoryModelingError) as error:
        ImmutableArtifactStore(
            artifact_root=_repository_root() / "tmp" / "modeling-artifacts",
            repository_root=_repository_root(),
            namespace="reports",
        )
    assert error.value.reason_code == REASON_BUNDLE_INCOMPLETE


def _bundle_files() -> dict[str, bytes]:
    paths = {
        "models/final_model.txt",
        *(f"models/selected_folds/fold-{index}/model.txt" for index in range(5)),
        "style_profile.json",
        "feature_schema.json",
        "feature_formula_registry.json",
        "feature_snapshot_ref.json",
        "market_regime_policy_template.json",
        *(f"fitted_market_regimes/fold-{index}.json" for index in range(5)),
        "fitted_market_regimes/final.json",
        "label_policy.json",
        "dataset_snapshot_ref.json",
        "training_views.json",
        "split_plan.json",
        "experiment_registry.json",
        "training_config.json",
        "environment_lock.json",
        "oos_metrics.json",
        "baseline_comparison.json",
        "feature_importance.json",
        "model_selection_receipt.json",
        "model_card.md",
    }
    return {path: f"payload:{path}".encode() for path in paths}


def test_model_bundle_represents_complete_artifact_with_unavailable_capability(
    tmp_path: Path,
) -> None:
    payloads = _bundle_files()
    manifest = ImmutableModelBundleV1(
        request_semantic_hash=_HASH,
        style_profile_hash="b" * 64,
        feature_snapshot_hash="c" * 64,
        split_plan_hash="d" * 64,
        experiment_registry_hash="e" * 64,
        model_selection_receipt_hash="f" * 64,
        environment_lock_hash="1" * 64,
        files=build_file_descriptors(payloads),
        capability_status="MODEL_UNAVAILABLE",
        unavailable_reason_codes=("MODEL_NO_FORMAL_OOS",),
    )
    root = (tmp_path / "artifact-root").resolve()
    root.mkdir()
    receipt = ImmutableModelBundleStore(
        artifact_root=root,
        repository_root=_repository_root(),
    ).publish(manifest=manifest, payload_files=payloads)

    assert manifest.bundle_status == "RESEARCH_BUNDLE_COMPLETE"
    assert receipt.status == "COMPLETE"
    assert (root / "model_bundles" / str(manifest.bundle_id) / "bundle_manifest.json").exists()


def test_feature_snapshot_manifest_and_store_close_exact_partition_file_set(tmp_path: Path) -> None:
    partition_path = "feature_rows/date=2026-07-01/part-00000.parquet"
    payloads = {
        "feature_schema.json": b"{}\n",
        "feature_formula_registry.json": b"{}\n",
        "feature_source_revisions.parquet": b"source-revisions",
        partition_path: b"feature-rows",
    }
    descriptors = build_file_descriptors(payloads)
    partition_descriptor = next(item for item in descriptors if item.relative_path == partition_path)
    manifest = RerankerFeatureSnapshotV1(
        base_snapshot_id="snapshot-1",
        base_snapshot_content_hash=_HASH,
        request_semantic_hash="b" * 64,
        feature_schema_hash="c" * 64,
        formula_registry_hash="d" * 64,
        query_registry_hash="e" * 64,
        feature_source_revision_set_hash="f" * 64,
        builder_code_closure_hash="1" * 64,
        partitions=(
            FeaturePartitionDescriptorV1(
                decision_date="2026-07-01",
                relative_path=partition_path,
                row_count=20,
                content_sha256=partition_descriptor.content_sha256,
                row_identity_set_hash="2" * 64,
            ),
        ),
        files=descriptors,
    )
    root = (tmp_path / "artifact-root").resolve()
    root.mkdir()
    store = RerankerFeatureSnapshotStore(
        artifact_root=root,
        repository_root=_repository_root(),
    )
    store.publish(manifest=manifest, payload_files=payloads)

    assert store.read(
        feature_snapshot_id=str(manifest.feature_snapshot_id),
        expected_feature_snapshot_hash=str(manifest.feature_snapshot_hash),
    ) == manifest


def test_shadow_ties_use_symbol_only_and_do_not_use_baseline_rank() -> None:
    result = build_shadow_result(
        candidate_group_hash=_HASH,
        bundle_id="advrerank_bundle",
        bundle_hash="b" * 64,
        feature_closure_hash="c" * 64,
        candidates=(
            ShadowCandidateScoreV1(symbol="000002.SZ", baseline_rank=1, model_score=Decimal("1")),
            ShadowCandidateScoreV1(symbol="000001.SZ", baseline_rank=2, model_score=Decimal("1")),
            ShadowCandidateScoreV1(symbol="000003.SZ", baseline_rank=3, model_score=Decimal("0")),
        ),
    )

    assert tuple(item.symbol for item in result.candidates) == (
        "000001.SZ",
        "000002.SZ",
        "000003.SZ",
    )
    assert result.candidates[0].normalized_model_score == result.candidates[1].normalized_model_score


def test_shared_selection_and_trading_modules_do_not_import_advisory_modeling() -> None:
    repository_root = _repository_root()
    protected_roots = (
        repository_root / "backend" / "services" / "selection_center",
        repository_root / "backend" / "services" / "simulation_runtime",
        repository_root / "backend" / "services" / "paper_trading",
        repository_root / "backend" / "services" / "strategy_package",
        repository_root / "backend" / "services" / "quantevolver",
    )
    offenders = []
    for root in protected_roots:
        for path in root.rglob("*.py"):
            if "advisory_modeling" in path.read_text(encoding="utf-8"):
                offenders.append(path.relative_to(repository_root).as_posix())
    assert offenders == []
