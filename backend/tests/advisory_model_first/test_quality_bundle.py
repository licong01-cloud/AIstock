from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.services.advisory_model_first.model_bundle import (
    load_exact_shadow_bundle,
    publish_shadow_binding,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    FEATURE_SCHEMA_HASH,
    FEATURE_SCHEMA_PAYLOAD,
)
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.quality_bundle import publish_quality_model_bundle
from backend.services.advisory_model_first.quality_contracts import (
    QUALITY_SEEDS,
    QUALITY_BASELINE_NAMES,
    ParentArtifactDescriptor,
    QualityProjectionDescriptor,
    QualityWinnerCandidate,
    build_quality_test_request,
    build_winner_receipt,
)
from backend.services.advisory_model_first.quality_pipeline import create_quality_train_request


PARENT_BUNDLE_ID = "9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629"


class _Booster:
    def __init__(self, path: Path) -> None:
        self.path = path


def test_v2_bundle_publishes_and_exact_loader_reads_all_five_members(tmp_path: Path) -> None:
    parent_root = _parent_root(tmp_path)
    projection = QualityProjectionDescriptor(
        path="/projection/train_validation.parquet",
        sha256="a" * 64,
        row_count=10,
        date_start="2024-07-04",
        date_end="2026-03-10",
        split_names=("train", "validation"),
    )
    train = create_quality_train_request(
        model_root=parent_root,
        parent_bundle_id=PARENT_BUNDLE_ID,
        train_validation_projection=projection,
        output_root=tmp_path,
        repository_root="/repo",
        repository_commit="1" * 40,
        lightgbm_version="4.6.0",
    )
    run = tmp_path / "run"
    run.mkdir()
    model_paths = []
    model_hashes = []
    for seed in QUALITY_SEEDS:
        path = run / f"model_seed_{seed}.txt"
        path.write_text(f"model-{seed}", encoding="utf-8")
        model_paths.append(str(path))
        model_hashes.append(sha256_file(path))
    parent_schema = json.loads(
        (parent_root / "bundles" / PARENT_BUNDLE_ID / "feature_schema.json").read_text(encoding="utf-8")
    )
    vocabulary = run / "categorical_vocabulary.json"
    vocabulary.write_text(
        json.dumps(
            {
                "schema_version": "advisory_reranker_quality_vocabulary_v1",
                "categorical_vocabulary": parent_schema["categorical_vocabulary"],
            }
        ),
        encoding="utf-8",
    )
    tournament = run / "tournament_report.json"
    tournament.write_text(json.dumps({"status": "MODEL_WINNER_SELECTED"}), encoding="utf-8")
    candidate = QualityWinnerCandidate(
        candidate_id="EXPANDING_ALL__LAMBDARANK_NDCG5__MW_0.50",
        window_id="EXPANDING_ALL",
        family_id="LAMBDARANK_NDCG5",
        model_weight=0.5,
        seeds=QUALITY_SEEDS,
        member_model_paths=tuple(model_paths),
        member_model_sha256=tuple(model_hashes),
        categorical_vocabulary_path=str(vocabulary),
        categorical_vocabulary_sha256=sha256_file(vocabulary),
        validation_metrics={
            "mean_daily_top5_excess_return_5": 0.01,
            "median_daily_top5_excess_return_5": 0.01,
            "excess_hit_rate": 0.6,
            "shortlist_turnover": 0.5,
        },
    )
    winner = build_winner_receipt(
        train_request_id=train.request_id,
        train_request_sha256=train.request_sha256,
        status="MODEL_WINNER_SELECTED",
        winner=candidate,
        tournament_report_path=str(tournament),
        tournament_report_sha256=sha256_file(tournament),
    )
    winner_path = run / "winner_receipt.json"
    winner.write_json(winner_path)
    parent_predictions = run / "parent_test_predictions.parquet"
    parent_predictions.write_bytes(b"parent-predictions")
    test_request = build_quality_test_request(
        output_root=str(tmp_path),
        train_request_id=train.request_id,
        train_request_sha256=train.request_sha256,
        parent_bundle_id=train.parent_bundle_id,
        parent_split_sha256=train.parent_split_sha256,
        winner_receipt_path=str(winner_path),
        winner_receipt_sha256=sha256_file(winner_path),
        winner_receipt_id=winner.receipt_id,
        test_projection=QualityProjectionDescriptor(
            path="/projection/test.parquet",
            sha256="b" * 64,
            row_count=5,
            date_start="2026-03-11",
            date_end="2026-06-30",
            split_names=("test",),
        ),
        parent_test_predictions=ParentArtifactDescriptor(
            path=str(parent_predictions),
            sha256=sha256_file(parent_predictions),
        ),
    )
    test_report = run / "test_report.json"
    test_report.write_text(
        json.dumps(
            {
                "schema_version": "advisory_reranker_quality_test_report_v1",
                "evaluation_id": test_request.evaluation_id,
                "test_request_sha256": test_request.request_sha256,
                "winner_receipt_id": winner.receipt_id,
                "winner_receipt_sha256": winner.receipt_sha256,
                "winner_metrics": {
                    "mean_daily_top5_excess_return_5": 0.01,
                    "mean_excess_return_5": 0.01,
                },
                "baselines": {
                    name: {"mean_daily_top5_excess_return_5": 0.0}
                    for name in QUALITY_BASELINE_NAMES
                },
            }
        ),
        encoding="utf-8",
    )

    bundle_id, _path, manifest = publish_quality_model_bundle(
        model_root=tmp_path / "published",
        train_request=train,
        test_request=test_request,
        winner_receipt=winner,
        test_report_path=test_report,
    )
    assert manifest["schema_version"] == "advisory_model_bundle_v2"
    assert len(manifest["ensemble_members"]) == 5
    publish_shadow_binding(model_root=tmp_path / "published", bundle_id=bundle_id)
    loaded = load_exact_shadow_bundle(
        model_root=tmp_path / "published",
        package_id=train.package_id,
        manifest_sha256=train.manifest_sha256,
        style_profile_hash=train.style_profile_hash,
        booster_factory=_Booster,
    )
    assert loaded.booster is None
    assert len(loaded.boosters) == 5
    assert loaded.baselines["model_top5"]["mean_daily_top5_excess_return_5"] == 0.01
    assert loaded.baselines["model_top5"]["mean_excess_return_5"] == 0.01
    assert "selection_rank_top5" in loaded.baselines
    tampered_report = json.loads(test_report.read_text(encoding="utf-8"))
    tampered_report["evaluation_id"] = "different-evaluation"
    test_report.write_text(json.dumps(tampered_report), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as report_error:
        publish_quality_model_bundle(
            model_root=tmp_path / "published",
            train_request=train,
            test_request=test_request,
            winner_receipt=winner,
            test_report_path=test_report,
        )
    assert report_error.value.reason_code == "ADVISORY_M5_INPUT_IDENTITY_MISMATCH"
    member_path = loaded.bundle_path / f"model_seed_{QUALITY_SEEDS[0]}.txt"
    member_path.write_text("tampered", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as raised:
        load_exact_shadow_bundle(
            model_root=tmp_path / "published",
            package_id=train.package_id,
            manifest_sha256=train.manifest_sha256,
            style_profile_hash=train.style_profile_hash,
            booster_factory=_Booster,
        )
    assert raised.value.reason_code == "ADVISORY_MODEL_BUNDLE_INVALID"


def _parent_root(tmp_path: Path) -> Path:
    root = tmp_path / "parent"
    bundle = root / "bundles" / PARENT_BUNDLE_ID
    bundle.mkdir(parents=True)
    manifest = {
        "schema_version": "advisory_model_bundle_v1",
        "bundle_id": PARENT_BUNDLE_ID,
        "request_id": "parent-request",
        "package_id": "pkg",
        "manifest_sha256": "1" * 64,
        "package_asset_closure_hash": "2" * 64,
        "style_profile_id": "style",
        "style_profile_hash": "3" * 64,
        "selection_runtime_semantics_id": "runtime",
        "selection_runtime_semantics_hash": "4" * 64,
        "selection_runtime_semantics": {"version": 1},
        "terminal_weights": {"leg": 1.0},
        "continuation_cutoff": "2026-03-10",
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
        "label_policy_version": "advisory_label_policy_v1",
        "decision_clock_version": "decision-clock-v1",
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    for name in ("label_policy.json", "split.json"):
        (bundle / name).write_text(json.dumps({"name": name}), encoding="utf-8")
    (bundle / "training_request.json").write_text(
        json.dumps({"program_id": "program", "binding_version_id": "binding"}),
        encoding="utf-8",
    )
    (bundle / "feature_schema.json").write_text(
        json.dumps(
            {
                **FEATURE_SCHEMA_PAYLOAD,
                "feature_schema_hash": FEATURE_SCHEMA_HASH,
                "categorical_vocabulary": {"l2_code_id": [1]},
                "trained_feature_names": FEATURE_SCHEMA_PAYLOAD["model_feature_columns"],
            }
        ),
        encoding="utf-8",
    )
    (bundle / "fresh_hmm_models.json").write_text(json.dumps({}), encoding="utf-8")
    return root
