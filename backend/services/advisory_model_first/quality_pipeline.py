from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.quality_contracts import (
    QUALITY_SEEDS,
    AdvisoryRerankerQualityTestRequestV1,
    AdvisoryRerankerQualityTrainRequestV1,
    ParentArtifactDescriptor,
    QualityProjectionDescriptor,
    QualityWinnerCandidate,
    QualityWinnerReceiptV1,
    build_quality_test_request,
    build_quality_train_request,
    build_winner_receipt,
)
from backend.services.advisory_model_first.quality_tournament import (
    apply_ensemble_scores,
    evaluate_shortlist,
    prepare_model_matrix,
    run_quality_tournament,
    validate_quality_projection,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


PARENT_ARTIFACT_NAMES = (
    "training_request.json",
    "feature_schema.json",
    "label_policy.json",
    "split.json",
)


def prepare_quality_projections(
    *,
    model_root: str | Path,
    parent_bundle_id: str,
    projection_root: str | Path,
    projection_root_for_request: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(model_root).resolve()
    bundle_path = root / "bundles" / parent_bundle_id
    manifest = _read_json(bundle_path / "manifest.json")
    if manifest.get("bundle_id") != parent_bundle_id or manifest.get("schema_version") != "advisory_model_bundle_v1":
        raise AdvisoryModelFirstError(
            "M5A parent bundle identity is invalid",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
            context={"parent_bundle_id": parent_bundle_id},
        )
    parent_request_id = str(manifest.get("request_id") or "")
    run_root = root / "runs" / parent_request_id
    features_path = run_root / "features.parquet"
    labels_path = run_root / "labels.parquet"
    if not features_path.is_file() or not labels_path.is_file():
        raise AdvisoryModelFirstError(
            "M5A parent run does not contain frozen feature and label files",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
            context={"parent_request_id": parent_request_id},
        )
    features = pd.read_parquet(features_path)
    labels = pd.read_parquet(labels_path)
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    merged = features.merge(labels, on=keys, how="inner", validate="one_to_one")
    merged = merged[
        (merged["group_label_status"] == "AVAILABLE")
        & merged["relevance"].notna()
        & merged["utility_5"].notna()
        & merged["split"].isin(["train", "validation", "test"])
    ].copy()
    required_features = set(MODEL_FEATURE_COLUMNS)
    if merged.empty or not required_features.issubset(merged.columns):
        raise AdvisoryModelFirstError(
            "M5A parent projection has no complete modelable rows",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    merged = merged.sort_values(["decision_as_of_trade_date", "selection_effective_rank", "instrument"]).reset_index(
        drop=True
    )
    output_root = Path(projection_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    request_root = (
        PurePosixPath(str(projection_root_for_request)) if projection_root_for_request is not None else output_root
    )
    descriptors: dict[str, QualityProjectionDescriptor] = {}
    for name, splits in (
        ("train_validation", ("train", "validation")),
        ("test", ("test",)),
    ):
        projection = merged[merged["split"].isin(splits)].copy()
        validate_quality_projection(projection, allowed_splits=splits)
        local_path = output_root / f"{name}.parquet"
        _write_parquet_atomic(projection, local_path)
        request_path = request_root / local_path.name
        descriptors[name] = QualityProjectionDescriptor(
            path=str(request_path),
            sha256=sha256_file(local_path),
            row_count=len(projection),
            date_start=pd.Timestamp(projection["decision_as_of_trade_date"].min()).date().isoformat(),
            date_end=pd.Timestamp(projection["decision_as_of_trade_date"].max()).date().isoformat(),
            split_names=splits,
        )
    receipt = {
        "schema_version": "advisory_reranker_quality_projection_receipt_v1",
        "parent_bundle_id": parent_bundle_id,
        "parent_request_id": parent_request_id,
        "source_feature_sha256": sha256_file(features_path),
        "source_label_sha256": sha256_file(labels_path),
        "train_validation_projection": descriptors["train_validation"].model_dump(mode="json"),
        "test_projection": descriptors["test"].model_dump(mode="json"),
    }
    receipt["receipt_sha256"] = canonical_json_sha256(receipt)
    _write_json_atomic(receipt, output_root / "projection_receipt.json")
    return receipt


def create_quality_train_request(
    *,
    model_root: str | Path,
    parent_bundle_id: str,
    train_validation_projection: QualityProjectionDescriptor,
    output_root: str | Path,
    repository_root: str | Path,
    repository_commit: str,
    lightgbm_version: str,
    parent_bundle_root_for_request: str | Path | None = None,
) -> AdvisoryRerankerQualityTrainRequestV1:
    root = Path(model_root).resolve()
    bundle_path = root / "bundles" / parent_bundle_id
    manifest = _read_json(bundle_path / "manifest.json")
    artifacts = {
        name: ParentArtifactDescriptor(
            path=(
                str(PurePosixPath(str(parent_bundle_root_for_request)) / name)
                if parent_bundle_root_for_request is not None
                else str(bundle_path / name)
            ),
            sha256=sha256_file(bundle_path / name),
        )
        for name in PARENT_ARTIFACT_NAMES
    }
    return build_quality_train_request(
        output_root=str(output_root),
        parent_bundle_id=parent_bundle_id,
        parent_request_id=str(manifest["request_id"]),
        parent_artifacts=artifacts,
        parent_split_sha256=artifacts["split.json"].sha256,
        train_validation_projection=train_validation_projection,
        package_id=str(manifest["package_id"]),
        manifest_sha256=str(manifest["manifest_sha256"]),
        style_profile_id=str(manifest["style_profile_id"]),
        style_profile_hash=str(manifest["style_profile_hash"]),
        selection_runtime_semantics_hash=str(manifest["selection_runtime_semantics_hash"]),
        repository_root=str(repository_root),
        repository_commit=repository_commit,
        lightgbm_version=lightgbm_version,
    )


def run_quality_stage_a(request_path: str | Path) -> QualityWinnerReceiptV1:
    started = time.monotonic()
    request = AdvisoryRerankerQualityTrainRequestV1.model_validate_json(Path(request_path).read_text(encoding="utf-8"))
    _verify_training_environment(request)
    _validate_parent_artifacts(request)
    projection = _read_projection(request.train_validation_projection)
    run_root = Path(request.output_root).resolve() / "quality_runs" / request.request_id
    run_root.mkdir(parents=True, exist_ok=True)
    existing_receipt_path = run_root / "winner_receipt.json"
    if existing_receipt_path.is_file():
        existing = QualityWinnerReceiptV1.model_validate_json(existing_receipt_path.read_text(encoding="utf-8"))
        _validate_existing_winner(existing, request=request)
        return existing
    result = run_quality_tournament(projection)
    peak_rss_bytes = _peak_rss_bytes()
    if peak_rss_bytes > request.resource_max_rss_bytes:
        raise AdvisoryModelFirstError(
            "M5A tournament exceeded the frozen RSS limit",
            reason_code="ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED",
            context={"peak_rss_bytes": peak_rss_bytes, "limit_bytes": request.resource_max_rss_bytes},
        )
    report = {
        **result.report,
        "train_request_id": request.request_id,
        "train_request_sha256": request.request_sha256,
        "wall_seconds": round(time.monotonic() - started, 3),
        "peak_rss_bytes": peak_rss_bytes,
        "cpu_threads": 4,
        "split_summary": {
            split_name: _split_summary(projection[projection["split"] == split_name])
            for split_name in ("train", "validation")
        },
    }
    report_path = run_root / "tournament_report.json"
    _write_json_atomic(report, report_path)
    model_paths: list[str] = []
    model_hashes: list[str] = []
    vocabulary_path: Path | None = None
    if result.winning_family is not None:
        member_root = run_root / "winner_models"
        member_root.mkdir(parents=True, exist_ok=True)
        for seed, booster in zip(QUALITY_SEEDS, result.winning_family.boosters, strict=True):
            path = member_root / f"model_seed_{seed}.txt"
            booster.save_model(str(path), num_iteration=booster.best_iteration)
            if not path.is_file() or path.stat().st_size <= 0:
                raise AdvisoryModelFirstError(
                    "M5A winning booster file is missing or empty",
                    reason_code="ADVISORY_M5_ENSEMBLE_INCOMPLETE",
                    context={"seed": seed},
                )
            model_paths.append(str(path))
            model_hashes.append(sha256_file(path))
        vocabulary_path = run_root / "categorical_vocabulary.json"
        _write_json_atomic(
            {
                "schema_version": "advisory_reranker_quality_vocabulary_v1",
                "categorical_vocabulary": result.winning_family.categorical_vocabulary,
            },
            vocabulary_path,
        )
    winner = QualityWinnerCandidate(
        candidate_id=str(result.winner_row["candidate_id"]),
        window_id=str(result.winner_row["window_id"]),
        family_id=str(result.winner_row["family_id"]),
        model_weight=float(result.winner_row["model_weight"]),
        seeds=QUALITY_SEEDS if result.winning_family is not None else (),
        member_model_paths=tuple(model_paths),
        member_model_sha256=tuple(model_hashes),
        categorical_vocabulary_path=str(vocabulary_path) if vocabulary_path is not None else None,
        categorical_vocabulary_sha256=sha256_file(vocabulary_path) if vocabulary_path is not None else None,
        validation_metrics={
            key: value
            for key, value in result.winner_row.items()
            if key not in {"candidate_id", "window_id", "family_id", "model_weight", "seeds"}
        },
    )
    receipt = build_winner_receipt(
        train_request_id=request.request_id,
        train_request_sha256=request.request_sha256,
        status=report["status"],
        winner=winner,
        tournament_report_path=str(report_path),
        tournament_report_sha256=sha256_file(report_path),
    )
    receipt.write_json(run_root / "winner_receipt.json")
    return receipt


def create_quality_test_request(
    *,
    train_request: AdvisoryRerankerQualityTrainRequestV1,
    winner_receipt_path: str | Path,
    test_projection: QualityProjectionDescriptor,
    output_root: str | Path,
) -> AdvisoryRerankerQualityTestRequestV1:
    receipt_path = Path(winner_receipt_path)
    if not receipt_path.is_file():
        raise AdvisoryModelFirstError(
            "M5A winner receipt must exist before creating the test request",
            reason_code="ADVISORY_M5_TEST_ACCESSED_BEFORE_WINNER_FREEZE",
        )
    winner = QualityWinnerReceiptV1.model_validate_json(receipt_path.read_text(encoding="utf-8"))
    if (
        winner.train_request_id != train_request.request_id
        or winner.train_request_sha256 != train_request.request_sha256
    ):
        raise AdvisoryModelFirstError(
            "M5A winner receipt does not belong to the train request",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    parent_bundle_path = Path(train_request.parent_artifacts["training_request.json"].path).parent
    parent_test_predictions_path = parent_bundle_path / "test_predictions.parquet"
    if not parent_test_predictions_path.is_file():
        raise AdvisoryModelFirstError(
            "M5A parent test predictions are unavailable for the frozen baseline",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    return build_quality_test_request(
        output_root=str(output_root),
        train_request_id=train_request.request_id,
        train_request_sha256=train_request.request_sha256,
        parent_bundle_id=train_request.parent_bundle_id,
        parent_split_sha256=train_request.parent_split_sha256,
        winner_receipt_path=str(receipt_path),
        winner_receipt_sha256=sha256_file(receipt_path),
        winner_receipt_id=winner.receipt_id,
        test_projection=test_projection,
        parent_test_predictions=ParentArtifactDescriptor(
            path=str(parent_test_predictions_path),
            sha256=sha256_file(parent_test_predictions_path),
        ),
    )


def run_quality_stage_b(
    request_path: str | Path,
    *,
    train_request_path: str | Path,
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    started = time.monotonic()
    request = AdvisoryRerankerQualityTestRequestV1.model_validate_json(Path(request_path).read_text(encoding="utf-8"))
    train_request = AdvisoryRerankerQualityTrainRequestV1.model_validate_json(
        Path(train_request_path).read_text(encoding="utf-8")
    )
    _verify_training_environment(train_request)
    if (
        request.train_request_id != train_request.request_id
        or request.train_request_sha256 != train_request.request_sha256
        or request.parent_bundle_id != train_request.parent_bundle_id
        or request.parent_split_sha256 != train_request.parent_split_sha256
    ):
        raise AdvisoryModelFirstError(
            "M5A test request differs from its frozen train request",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    _validate_parent_artifacts(train_request)
    winner_path = Path(request.winner_receipt_path)
    if not winner_path.is_file() or sha256_file(winner_path) != request.winner_receipt_sha256:
        raise AdvisoryModelFirstError(
            "M5A winner receipt changed before test evaluation",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    winner = QualityWinnerReceiptV1.model_validate_json(winner_path.read_text(encoding="utf-8"))
    if winner.receipt_id != request.winner_receipt_id:
        raise AdvisoryModelFirstError(
            "M5A test request binds a different winner identity",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    run_root = Path(request.output_root).resolve() / "quality_evaluations" / request.evaluation_id
    receipt_path = run_root / "test_once_receipt.json"
    report_path = run_root / "test_report.json"
    if receipt_path.is_file():
        existing = _read_json(receipt_path)
        if (
            existing.get("test_request_sha256") != request.request_sha256
            or existing.get("winner_receipt_sha256") != winner.receipt_sha256
            or existing.get("status") != "SUCCEEDED"
            or not report_path.is_file()
            or sha256_file(report_path) != existing.get("test_report_sha256")
        ):
            raise AdvisoryModelFirstError(
                "M5A test-once receipt conflicts with the requested evaluation",
                reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
            )
        return {"report": _read_json(report_path), "receipt": existing, "idempotent": True}
    run_root.mkdir(parents=True, exist_ok=True)

    test = _read_projection(request.test_projection)
    if winner.winner.model_weight == 0.0:
        scored = apply_ensemble_scores(test, score_columns=(), model_weight=0.0)
    else:
        scored = _score_frozen_winner(test, winner)
    winner_metrics = evaluate_shortlist(scored)
    baselines = _test_baselines(
        test,
        seed=int(request.request_sha256[:16], 16),
        parent_test_predictions=request.parent_test_predictions,
    )
    top5 = scored[scored["advisory_model_rank"] <= 5].copy()
    daily_model = top5.groupby("decision_as_of_trade_date", sort=True)["excess_return_5"].mean()
    daily_selection = (
        test[test["selection_effective_rank"] <= 5]
        .groupby("decision_as_of_trade_date", sort=True)["excess_return_5"]
        .mean()
    )
    lift = daily_model.to_frame("model").join(daily_selection.rename("selection"), how="inner")
    lift_values = (lift["model"] - lift["selection"]).to_numpy(dtype=float)
    interval = _moving_block_bootstrap_interval(
        lift_values,
        seed=int(request.request_sha256[:16], 16),
    )
    report = {
        "schema_version": "advisory_reranker_quality_test_report_v1",
        "evaluation_id": request.evaluation_id,
        "test_request_sha256": request.request_sha256,
        "winner_receipt_id": winner.receipt_id,
        "winner_receipt_sha256": winner.receipt_sha256,
        "winner": winner.winner.model_dump(mode="json"),
        "winner_metrics": winner_metrics,
        "baselines": baselines,
        "lift_vs_selection_rank": {
            "mean": float(np.mean(lift_values)),
            "median": float(np.median(lift_values)),
            "moving_block_bootstrap_95": interval,
            "replicates": 1000,
            "block_length": 5,
        },
        "test_date_count": int(test["decision_as_of_trade_date"].nunique()),
        "test_row_count": int(len(test)),
        "split_summary": {
            **dict(_read_json(Path(winner.tournament_report_path)).get("split_summary") or {}),
            "test": _split_summary(test),
        },
        "top5_by_date": _top5_records(top5),
        "wall_seconds": round(time.monotonic() - started, 3),
        "peak_rss_bytes": _peak_rss_bytes(),
        "cpu_threads": 4,
        "winner_model_size_bytes": sum(Path(path).stat().st_size for path in winner.winner.member_model_paths),
        "test_projection_size_bytes": Path(request.test_projection.path).stat().st_size,
    }
    _write_json_atomic(report, report_path)
    finished_at = datetime.now(UTC)
    receipt = {
        "schema_version": "advisory_reranker_quality_test_once_receipt_v1",
        "evaluation_id": request.evaluation_id,
        "test_request_sha256": request.request_sha256,
        "test_input_sha256": request.test_projection.sha256,
        "winner_receipt_sha256": winner.receipt_sha256,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "status": "SUCCEEDED",
        "test_report_sha256": sha256_file(report_path),
    }
    receipt["receipt_sha256"] = canonical_json_sha256(receipt)
    _write_json_atomic(receipt, receipt_path)
    return {"report": report, "receipt": receipt, "idempotent": False}


def _score_frozen_winner(test: pd.DataFrame, winner: QualityWinnerReceiptV1) -> pd.DataFrame:
    candidate = winner.winner
    if len(candidate.member_model_paths) != len(QUALITY_SEEDS):
        raise AdvisoryModelFirstError(
            "M5A winner is missing one or more booster files",
            reason_code="ADVISORY_M5_ENSEMBLE_INCOMPLETE",
        )
    vocabulary_path = Path(str(candidate.categorical_vocabulary_path))
    if not vocabulary_path.is_file() or sha256_file(vocabulary_path) != candidate.categorical_vocabulary_sha256:
        raise AdvisoryModelFirstError(
            "M5A categorical vocabulary changed after winner freeze",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    vocabulary_payload = _read_json(vocabulary_path)
    vocabulary = {
        key: tuple(int(value) for value in values)
        for key, values in dict(vocabulary_payload.get("categorical_vocabulary") or {}).items()
    }
    matrix, _ = prepare_model_matrix(
        test,
        train_mask=pd.Series(True, index=test.index),
        categorical_vocabulary=vocabulary,
        validate_all_null_train=False,
    )
    try:
        import lightgbm as lgb
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "LightGBM is unavailable for M5A test evaluation",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
        ) from exc
    scored = test.copy().reset_index(drop=True)
    for seed, model_path_value, expected_hash in zip(
        QUALITY_SEEDS,
        candidate.member_model_paths,
        candidate.member_model_sha256,
        strict=True,
    ):
        model_path = Path(model_path_value)
        if not model_path.is_file() or sha256_file(model_path) != expected_hash:
            raise AdvisoryModelFirstError(
                "M5A winner booster changed before test evaluation",
                reason_code="ADVISORY_M5_ENSEMBLE_INCOMPLETE",
                context={"seed": seed},
            )
        booster = lgb.Booster(model_file=str(model_path))
        if tuple(booster.feature_name()) != tuple(MODEL_FEATURE_COLUMNS):
            raise AdvisoryModelFirstError(
                "M5A winner booster feature order differs from the frozen schema",
                reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
                context={"seed": seed},
            )
        raw = np.asarray(booster.predict(matrix), dtype=float)
        if len(raw) != len(scored) or not np.isfinite(raw).all():
            raise AdvisoryModelFirstError(
                "M5A winner booster produced invalid test scores",
                reason_code="ADVISORY_M5_ENSEMBLE_INCOMPLETE",
                context={"seed": seed},
            )
        scored[f"raw_score_{seed}"] = raw
    return apply_ensemble_scores(
        scored,
        score_columns=tuple(f"raw_score_{seed}" for seed in QUALITY_SEEDS),
        model_weight=candidate.model_weight,
    )


def _test_baselines(
    test: pd.DataFrame,
    *,
    seed: int,
    parent_test_predictions: ParentArtifactDescriptor,
) -> dict[str, Any]:
    baseline_frames: dict[str, pd.DataFrame] = {}
    parent_path = Path(parent_test_predictions.path)
    if not parent_path.is_file() or sha256_file(parent_path) != parent_test_predictions.sha256:
        raise AdvisoryModelFirstError(
            "M5A parent M1 test predictions changed after request freeze",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    parent = pd.read_parquet(parent_path)
    expected_keys = set(
        zip(
            pd.to_datetime(test["decision_as_of_trade_date"]).dt.normalize(),
            test["instrument"].astype(str),
            strict=True,
        )
    )
    parent_keys = set(
        zip(
            pd.to_datetime(parent["decision_as_of_trade_date"]).dt.normalize(),
            parent["instrument"].astype(str),
            strict=True,
        )
    )
    if (
        len(parent) != len(test)
        or parent.duplicated(["decision_as_of_trade_date", "instrument"]).any()
        or parent_keys != expected_keys
    ):
        raise AdvisoryModelFirstError(
            "M5A parent M1 baseline candidate identity differs from frozen test",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    baseline_frames["current_m1_model_top5"] = parent
    selection = test.sort_values(["decision_as_of_trade_date", "selection_effective_rank", "instrument"]).copy()
    selection["advisory_model_rank"] = selection.groupby("decision_as_of_trade_date").cumcount().add(1)
    baseline_frames["selection_rank_top5"] = selection
    if "hmm_bull_posterior" in test:
        hmm = (
            test[test["hmm_bull_posterior"].notna()]
            .sort_values(
                ["decision_as_of_trade_date", "hmm_bull_posterior", "instrument"],
                ascending=[True, False, True],
            )
            .copy()
        )
        hmm["advisory_model_rank"] = hmm.groupby("decision_as_of_trade_date").cumcount().add(1)
        baseline_frames["hmm_top5"] = hmm
    random_groups = []
    for decision, group in test.groupby("decision_as_of_trade_date", sort=True):
        digest = hashlib.sha256(f"{seed}:{pd.Timestamp(decision).date().isoformat()}".encode("ascii")).digest()
        rng = np.random.default_rng(int.from_bytes(digest[:8], "big"))
        positions = set(rng.choice(len(group), size=min(5, len(group)), replace=False).tolist())
        item = group.copy()
        item["advisory_model_rank"] = [1 if index in positions else 6 for index in range(len(group))]
        random_groups.append(item)
    baseline_frames["random_top5"] = pd.concat(random_groups, ignore_index=True)
    top20 = test.copy()
    top20["advisory_model_rank"] = 1
    baseline_frames["candidate_top20_equal"] = top20
    return {name: evaluate_shortlist(frame, selection_reference=test) for name, frame in baseline_frames.items()}


def _split_summary(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        raise AdvisoryModelFirstError(
            "M5A split summary cannot be built from an empty projection",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    dates = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    return {
        "row_count": int(len(frame)),
        "date_count": int(dates.nunique()),
        "date_start": pd.Timestamp(dates.min()).date().isoformat(),
        "date_end": pd.Timestamp(dates.max()).date().isoformat(),
    }


def _moving_block_bootstrap_interval(values: np.ndarray, *, seed: int) -> dict[str, float]:
    if values.size == 0:
        raise AdvisoryModelFirstError(
            "M5A test lift series is empty",
            reason_code="ADVISORY_M5_TRIAL_FAILED",
        )
    rng = np.random.default_rng(seed)
    block_length = min(5, len(values))
    block_starts = np.arange(len(values))
    estimates = np.empty(1000, dtype=float)
    for replicate in range(1000):
        sample: list[float] = []
        while len(sample) < len(values):
            start = int(rng.choice(block_starts))
            sample.extend(values[(start + offset) % len(values)] for offset in range(block_length))
        estimates[replicate] = float(np.mean(sample[: len(values)]))
    lower, upper = np.quantile(estimates, [0.025, 0.975])
    return {"lower": float(lower), "upper": float(upper)}


def _top5_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    columns = [
        "decision_as_of_trade_date",
        "instrument",
        "advisory_model_score",
        "advisory_model_rank",
        "stock_net_return_5",
        "excess_return_5",
    ]
    result = frame.loc[:, columns].copy()
    result["decision_as_of_trade_date"] = result["decision_as_of_trade_date"].dt.strftime("%Y-%m-%d")
    return result.sort_values(["decision_as_of_trade_date", "advisory_model_rank"]).to_dict("records")


def _validate_parent_artifacts(request: AdvisoryRerankerQualityTrainRequestV1) -> None:
    for name, descriptor in request.parent_artifacts.items():
        path = Path(descriptor.path)
        if not path.is_file() or sha256_file(path) != descriptor.sha256:
            raise AdvisoryModelFirstError(
                "M5A parent artifact changed after request freeze",
                reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
                context={"artifact": name},
            )


def _validate_existing_winner(
    winner: QualityWinnerReceiptV1,
    *,
    request: AdvisoryRerankerQualityTrainRequestV1,
) -> None:
    report_path = Path(winner.tournament_report_path)
    if (
        winner.train_request_id != request.request_id
        or winner.train_request_sha256 != request.request_sha256
        or not report_path.is_file()
        or sha256_file(report_path) != winner.tournament_report_sha256
    ):
        raise AdvisoryModelFirstError(
            "existing M5A winner receipt differs from the frozen train request",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    for path_value, expected_hash in zip(
        winner.winner.member_model_paths,
        winner.winner.member_model_sha256,
        strict=True,
    ):
        path = Path(path_value)
        if not path.is_file() or sha256_file(path) != expected_hash:
            raise AdvisoryModelFirstError(
                "existing M5A winner model changed after freeze",
                reason_code="ADVISORY_M5_ENSEMBLE_INCOMPLETE",
            )
    if winner.winner.model_weight > 0.0:
        vocabulary_path = Path(str(winner.winner.categorical_vocabulary_path))
        if not vocabulary_path.is_file() or sha256_file(vocabulary_path) != winner.winner.categorical_vocabulary_sha256:
            raise AdvisoryModelFirstError(
                "existing M5A winner vocabulary changed after freeze",
                reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
            )


def _verify_training_environment(request: AdvisoryRerankerQualityTrainRequestV1) -> None:
    actual_environment = os.getenv("CONDA_DEFAULT_ENV", "").strip()
    try:
        actual_lightgbm = importlib.metadata.version("lightgbm")
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(request.repository_root),
            check=True,
            capture_output=True,
            text=True,
        )
    except (importlib.metadata.PackageNotFoundError, OSError, subprocess.SubprocessError) as exc:
        raise AdvisoryModelFirstError(
            "M5A training environment identity cannot be resolved",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"error_type": type(exc).__name__},
        ) from exc
    actual_commit = completed.stdout.strip().lower()
    if (
        platform.system() != "Linux"
        or actual_environment != request.conda_environment
        or actual_lightgbm != request.lightgbm_version
        or actual_commit != request.repository_commit
    ):
        raise AdvisoryModelFirstError(
            "M5A training environment differs from the frozen request",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={
                "platform": platform.system(),
                "conda_environment": actual_environment,
                "lightgbm_version": actual_lightgbm,
                "repository_commit": actual_commit,
            },
        )


def _read_projection(descriptor: QualityProjectionDescriptor) -> pd.DataFrame:
    path = Path(descriptor.path)
    if not path.is_file() or sha256_file(path) != descriptor.sha256:
        raise AdvisoryModelFirstError(
            "M5A projection changed after request freeze",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
            context={"path": str(path)},
        )
    frame = pd.read_parquet(path)
    if len(frame) != descriptor.row_count:
        raise AdvisoryModelFirstError(
            "M5A projection row count differs from its descriptor",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    validated = validate_quality_projection(frame, allowed_splits=descriptor.split_names)
    actual_start = pd.Timestamp(validated["decision_as_of_trade_date"].min()).date().isoformat()
    actual_end = pd.Timestamp(validated["decision_as_of_trade_date"].max()).date().isoformat()
    if (actual_start, actual_end) != (descriptor.date_start, descriptor.date_end):
        raise AdvisoryModelFirstError(
            "M5A projection date range differs from its descriptor",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    return validated


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "M5A authority JSON cannot be read",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
            context={"path": str(path), "error_type": type(exc).__name__},
        ) from exc
    if not isinstance(payload, dict):
        raise AdvisoryModelFirstError(
            "M5A authority JSON is not an object",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
            context={"path": str(path)},
        )
    return payload


def _write_json_atomic(payload: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".parquet", dir=path.parent)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        frame.to_parquet(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _peak_rss_bytes() -> int:
    try:
        import resource
    except ModuleNotFoundError as exc:
        raise AdvisoryModelFirstError(
            "M5A resource accounting requires the frozen WSL training environment",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
        ) from exc
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value * 1024
