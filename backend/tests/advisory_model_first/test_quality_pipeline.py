from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.quality_contracts import (
    M5A_PARENT_BUNDLE_ID,
    QUALITY_SEEDS,
    ParentArtifactDescriptor,
    QualityProjectionDescriptor,
    build_quality_train_request,
)
from backend.services.advisory_model_first.quality_pipeline import (
    _moving_block_bootstrap_interval,
    _test_baselines,
    _validate_existing_test_receipt,
    load_quality_test_projection,
    prepare_quality_projections,
    run_quality_stage_a,
)
from backend.services.advisory_model_first.quality_tournament import (
    TournamentResult,
    TrainedFamilyCandidate,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def test_projection_builder_physically_separates_test_rows(tmp_path: Path) -> None:
    model_root = tmp_path / "models"
    bundle_id = "a" * 64
    request_id = "parent-request"
    bundle = model_root / "bundles" / bundle_id
    run = model_root / "runs" / request_id
    bundle.mkdir(parents=True)
    run.mkdir(parents=True)
    (bundle / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "advisory_model_bundle_v1",
                "bundle_id": bundle_id,
                "request_id": request_id,
            }
        ),
        encoding="utf-8",
    )
    dates = pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-06"])
    features = pd.DataFrame({column: [0.1] * 3 for column in MODEL_FEATURE_COLUMNS})
    features["l2_code_id"] = 1
    features["decision_as_of_trade_date"] = dates
    features["target_trade_date"] = dates + pd.offsets.BDay(1)
    features["instrument"] = ["000001.SZ"] * 3
    features["program_id"] = "program"
    features["binding_version_id"] = "binding"
    features["package_id"] = "pkg"
    features["manifest_sha256"] = "1" * 64
    features["selection_runtime_semantics_hash"] = "2" * 64
    features["selection_effective_rank"] = 1
    features["candidate_group_size"] = 1
    features["parent_combined_score"] = 1.0
    labels = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "target_trade_date": dates + pd.offsets.BDay(1),
            "instrument": ["000001.SZ"] * 3,
            "group_label_status": ["AVAILABLE"] * 3,
            "relevance": [4, 4, 4],
            "utility_5": [0.1, 0.1, 0.1],
            "stock_net_return_5": [0.1, 0.1, 0.1],
            "excess_return_5": [0.1, 0.1, 0.1],
            "path_mfe_5": [0.2, 0.2, 0.2],
            "path_mae_loss_5": [0.01, 0.01, 0.01],
            "split": ["train", "validation", "test"],
        }
    )
    features.to_parquet(run / "features.parquet", index=False)
    labels.to_parquet(run / "labels.parquet", index=False)

    receipt = prepare_quality_projections(
        model_root=model_root,
        parent_bundle_id=bundle_id,
        projection_root=tmp_path / "projections",
        projection_root_for_request="/mnt/f/model/projections",
    )
    train_validation = pd.read_parquet(tmp_path / "projections" / "train_validation.parquet")
    test = pd.read_parquet(tmp_path / "projections" / "test.parquet")
    assert set(train_validation["split"]) == {"train", "validation"}
    assert set(test["split"]) == {"test"}
    assert receipt["train_validation_projection"]["split_names"] == ["train", "validation"]
    assert receipt["train_validation_projection"]["path"] == "/mnt/f/model/projections/train_validation.parquet"
    train_request = SimpleNamespace(
        parent_bundle_id=bundle_id,
        parent_request_id=request_id,
        train_validation_projection=QualityProjectionDescriptor.model_validate(receipt["train_validation_projection"]),
    )
    test_projection = load_quality_test_projection(
        projection_receipt_path=tmp_path / "projections" / "projection_receipt.json",
        train_request=train_request,
    )
    assert test_projection.split_names == ("test",)

    tampered = dict(receipt)
    tampered["test_projection"] = {**tampered["test_projection"], "row_count": 2}
    (tmp_path / "projections" / "projection_receipt.json").write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as raised:
        load_quality_test_projection(
            projection_receipt_path=tmp_path / "projections" / "projection_receipt.json",
            train_request=train_request,
        )
    assert raised.value.reason_code == "ADVISORY_M5_INPUT_IDENTITY_MISMATCH"


def test_moving_block_bootstrap_is_deterministic_for_frozen_seed() -> None:
    values = np.asarray([0.01, -0.02, 0.03, 0.0, 0.01, 0.02], dtype=float)
    first = _moving_block_bootstrap_interval(values, seed=20260812)
    second = _moving_block_bootstrap_interval(values, seed=20260812)
    assert first == second
    assert first["lower"] <= first["upper"]


def test_test_once_receipt_rejects_self_hash_tampering(tmp_path: Path) -> None:
    report = tmp_path / "test_report.json"
    report.write_text(
        json.dumps(
            {
                "schema_version": "advisory_reranker_quality_test_report_v1",
                "evaluation_id": "evaluation",
                "test_request_sha256": "1" * 64,
                "winner_receipt_sha256": "3" * 64,
            }
        ),
        encoding="utf-8",
    )
    receipt = {
        "schema_version": "advisory_reranker_quality_test_once_receipt_v1",
        "evaluation_id": "evaluation",
        "test_request_sha256": "1" * 64,
        "test_input_sha256": "2" * 64,
        "winner_receipt_sha256": "3" * 64,
        "started_at": "2026-08-12T00:00:00Z",
        "finished_at": "2026-08-12T00:01:00Z",
        "status": "SUCCEEDED",
        "test_report_sha256": sha256_file(report),
    }
    receipt["receipt_sha256"] = canonical_json_sha256(receipt)
    _validate_existing_test_receipt(
        receipt,
        evaluation_id="evaluation",
        test_request_sha256="1" * 64,
        test_input_sha256="2" * 64,
        winner_receipt_sha256="3" * 64,
        report_path=report,
    )
    receipt["started_at"] = "2026-08-12T00:00:01Z"
    with pytest.raises(AdvisoryModelFirstError):
        _validate_existing_test_receipt(
            receipt,
            evaluation_id="evaluation",
            test_request_sha256="1" * 64,
            test_input_sha256="2" * 64,
            winner_receipt_sha256="3" * 64,
            report_path=report,
        )


def test_test_baselines_build_explicit_scores_for_every_policy(tmp_path: Path) -> None:
    rows = []
    for decision in pd.to_datetime(["2026-01-02", "2026-01-05"]):
        for rank in range(1, 7):
            rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": decision + pd.offsets.BDay(1),
                    "instrument": f"{rank:06d}.SZ",
                    "selection_effective_rank": rank,
                    "hmm_bull_posterior": 1.0 - rank / 10.0,
                    "relevance": 5 - min(rank, 5),
                    "stock_net_return_5": rank / 100.0,
                    "excess_return_5": rank / 200.0,
                    "path_mfe_5": rank / 80.0,
                    "path_mae_loss_5": rank / 300.0,
                }
            )
    test = pd.DataFrame(rows)
    parent = test.copy()
    parent["excess_return_5"] = 99.0
    parent["advisory_model_score"] = -parent["selection_effective_rank"].astype(float)
    parent["advisory_model_rank"] = parent["selection_effective_rank"]
    parent_path = tmp_path / "parent_test_predictions.parquet"
    parent.to_parquet(parent_path, index=False)
    baselines = _test_baselines(
        test,
        seed=20260812,
        parent_test_predictions=ParentArtifactDescriptor(
            path=str(parent_path),
            sha256=sha256_file(parent_path),
        ),
    )
    assert set(baselines) == {
        "current_m1_model_top5",
        "selection_rank_top5",
        "hmm_top5",
        "random_top5",
        "candidate_top20_equal",
    }
    assert baselines["random_top5"]["row_count"] == 10
    assert baselines["candidate_top20_equal"]["row_count"] == 12
    assert "mean_excess_return_5" in baselines["current_m1_model_top5"]
    assert baselines["current_m1_model_top5"]["mean_excess_return_5"] != 99.0


def test_selection_rank_baseline_does_not_backfill_missing_original_top5(tmp_path: Path) -> None:
    test = pd.DataFrame(
        {
            "decision_as_of_trade_date": pd.to_datetime(["2026-01-02"] * 5),
            "target_trade_date": pd.to_datetime(["2026-01-05"] * 5),
            "instrument": [f"{rank:06d}.SZ" for rank in (1, 2, 3, 4, 6)],
            "selection_effective_rank": [1, 2, 3, 4, 6],
            "hmm_bull_posterior": [0.9, 0.8, 0.7, 0.6, 0.5],
            "relevance": [4, 3, 2, 1, 0],
            "stock_net_return_5": [0.01] * 5,
            "excess_return_5": [0.01] * 5,
            "path_mfe_5": [0.02] * 5,
            "path_mae_loss_5": [0.01] * 5,
        }
    )
    parent = test.copy()
    parent["advisory_model_score"] = -parent["selection_effective_rank"].astype(float)
    parent["advisory_model_rank"] = parent["selection_effective_rank"]
    parent_path = tmp_path / "parent_test_predictions.parquet"
    parent.to_parquet(parent_path, index=False)

    baselines = _test_baselines(
        test,
        seed=20260812,
        parent_test_predictions=ParentArtifactDescriptor(
            path=str(parent_path),
            sha256=sha256_file(parent_path),
        ),
    )

    assert baselines["selection_rank_top5"]["row_count"] == 4


class _SavedBooster:
    best_iteration = 3

    def save_model(self, path: str, num_iteration: int) -> None:
        assert num_iteration == 3
        Path(path).write_text("complete-model", encoding="utf-8")


def test_stage_a_exact_retry_reuses_frozen_winner_without_retraining(tmp_path: Path, monkeypatch) -> None:
    parent_artifacts = {}
    for name in (
        "manifest.json",
        "training_request.json",
        "feature_schema.json",
        "fresh_hmm_models.json",
        "label_policy.json",
        "split.json",
    ):
        path = tmp_path / "parent" / M5A_PARENT_BUNDLE_ID / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if name == "manifest.json":
            content = json.dumps(
                {
                    "bundle_id": M5A_PARENT_BUNDLE_ID,
                    "request_id": "parent-request",
                    "package_id": "pkg",
                    "manifest_sha256": "1" * 64,
                    "style_profile_id": "style",
                    "style_profile_hash": "2" * 64,
                    "selection_runtime_semantics_hash": "3" * 64,
                }
            )
        elif name == "training_request.json":
            content = json.dumps({"program_id": "program", "binding_version_id": "binding"})
        else:
            content = name
        path.write_text(content, encoding="utf-8")
        parent_artifacts[name] = ParentArtifactDescriptor(path=str(path), sha256=sha256_file(path))
    projection_path = tmp_path / "train_validation.parquet"
    projection_path.write_bytes(b"projection")
    projection = QualityProjectionDescriptor(
        path=str(projection_path),
        sha256=sha256_file(projection_path),
        row_count=2,
        date_start="2025-01-02",
        date_end="2025-01-03",
        split_names=("train", "validation"),
    )
    request = build_quality_train_request(
        output_root=str(tmp_path / "output"),
        parent_bundle_id=M5A_PARENT_BUNDLE_ID,
        parent_request_id="parent-request",
        parent_artifacts=parent_artifacts,
        parent_split_sha256=parent_artifacts["split.json"].sha256,
        train_validation_projection=projection,
        program_id="program",
        binding_version_id="binding",
        package_id="pkg",
        manifest_sha256="1" * 64,
        style_profile_id="style",
        style_profile_hash="2" * 64,
        selection_runtime_semantics_hash="3" * 64,
        repository_root="/repo",
        repository_commit="4" * 40,
        lightgbm_version="4.6.0",
    )
    request_path = tmp_path / "request.json"
    request.write_json(request_path)
    frozen_projection = pd.DataFrame(
        {
            "split": ["train", "validation"],
            "decision_as_of_trade_date": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "program_id": ["program", "program"],
            "binding_version_id": ["binding", "binding"],
            "package_id": ["pkg", "pkg"],
            "manifest_sha256": ["1" * 64, "1" * 64],
            "selection_runtime_semantics_hash": ["3" * 64, "3" * 64],
        }
    )
    calls = 0

    def tournament(_frame: pd.DataFrame, *, progress_callback=None) -> TournamentResult:
        nonlocal calls
        calls += 1
        family = TrainedFamilyCandidate(
            window_id="EXPANDING_ALL",
            family_id="LAMBDARANK_NDCG5",
            seeds=QUALITY_SEEDS,
            boosters=tuple(_SavedBooster() for _ in QUALITY_SEEDS),
            categorical_vocabulary={"l2_code_id": (1,)},
            validation_predictions=pd.DataFrame(),
            evaluation_history=tuple({"seed": seed, "best_iteration": 3, "history": {}} for seed in QUALITY_SEEDS),
        )
        winner = {
            "candidate_id": "EXPANDING_ALL__LAMBDARANK_NDCG5__MW_0.50",
            "window_id": "EXPANDING_ALL",
            "family_id": "LAMBDARANK_NDCG5",
            "model_weight": 0.5,
            "seeds": list(QUALITY_SEEDS),
            "mean_daily_top5_excess_return_5": 0.01,
            "median_daily_top5_excess_return_5": 0.01,
            "excess_hit_rate": 0.6,
            "shortlist_turnover": 0.5,
        }
        return TournamentResult(
            winner_row=winner,
            winning_family=family,
            report={"status": "MODEL_WINNER_SELECTED", "winner": winner},
        )

    monkeypatch.setattr(
        "backend.services.advisory_model_first.quality_pipeline._verify_training_environment",
        lambda _request: None,
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.quality_pipeline._read_projection",
        lambda _descriptor: frozen_projection,
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.quality_pipeline.run_quality_tournament",
        tournament,
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.quality_pipeline._peak_rss_bytes",
        lambda: 1024,
    )
    first = run_quality_stage_a(request_path)
    second = run_quality_stage_a(request_path)
    assert calls == 1
    assert first == second
    assert len(first.winner.member_model_paths) == 5
