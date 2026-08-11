from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

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
    prepare_quality_projections,
    run_quality_stage_a,
)
from backend.services.advisory_model_first.quality_tournament import (
    TournamentResult,
    TrainedFamilyCandidate,
)


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
    features["selection_effective_rank"] = 1
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


def test_moving_block_bootstrap_is_deterministic_for_frozen_seed() -> None:
    values = np.asarray([0.01, -0.02, 0.03, 0.0, 0.01, 0.02], dtype=float)
    first = _moving_block_bootstrap_interval(values, seed=20260812)
    second = _moving_block_bootstrap_interval(values, seed=20260812)
    assert first == second
    assert first["lower"] <= first["upper"]


class _SavedBooster:
    best_iteration = 3

    def save_model(self, path: str, num_iteration: int) -> None:
        assert num_iteration == 3
        Path(path).write_text("complete-model", encoding="utf-8")


def test_stage_a_exact_retry_reuses_frozen_winner_without_retraining(tmp_path: Path, monkeypatch) -> None:
    parent_artifacts = {}
    for name in ("training_request.json", "feature_schema.json", "label_policy.json", "split.json"):
        path = tmp_path / "parent" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(name, encoding="utf-8")
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
        }
    )
    calls = 0

    def tournament(_frame: pd.DataFrame) -> TournamentResult:
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
