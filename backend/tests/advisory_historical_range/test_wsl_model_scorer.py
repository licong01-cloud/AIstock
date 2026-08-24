from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.model_bundle import LoadedAdvisoryModelBundle
from backend.services.advisory_model_first.quality_contracts import QUALITY_SEEDS
from backend.services.advisory_historical_range.wsl_model_scorer import (
    DeferredLightgbmBooster,
    REASON_WSL_OUTPUT_INVALID,
    WSL_SCORE_RESULT_SCHEMA,
    WslFrozenFeatureMatrixScorer,
    WslMetaLabelFeatureMatrixScorer,
)
from backend.tests.advisory_historical_range.conftest import digest
from scripts.wsl.advisory_historical_model_predict import validate_request_identity


def test_wsl_scorer_keeps_ensemble_and_rank_semantics_in_host(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    features = _features()

    def runner(**kwargs: object) -> SimpleNamespace:
        request = kwargs["request"]
        assert isinstance(request, dict)
        assert request["bundle_id"] == bundle.bundle_id
        assert len(request["model_paths"]) == len(QUALITY_SEEDS)
        output_path = kwargs["output_path"]
        assert isinstance(output_path, Path)
        row_count = len(request["matrix_records"])
        feature_count = len(MODEL_FEATURE_COLUMNS)
        output_path.write_text(
            json.dumps(
                {
                    "schema_version": WSL_SCORE_RESULT_SCHEMA,
                    "request_hash": request["request_hash"],
                    "bundle_id": bundle.bundle_id,
                    "raw_scores": [[0.1, 0.9] for _ in QUALITY_SEEDS],
                    "raw_contributions": [
                        [[0.01] * (feature_count + 1) for _ in range(row_count)]
                        for _ in QUALITY_SEEDS
                    ],
                    "booster_feature_names": [
                        list(MODEL_FEATURE_COLUMNS) for _ in QUALITY_SEEDS
                    ],
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    scored = WslFrozenFeatureMatrixScorer(
        repo_root=Path(__file__).resolve().parents[3],
        runner=runner,
        path_converter=lambda value: value.replace("\\", "/"),
    )(bundle, features)

    assert [item["symbol"] for item in scored] == ["000002.SZ", "000001.SZ"]
    assert [item["advisory_model_rank"] for item in scored] == [1, 2]
    assert all(item["score_components"]["model_weight"] == 0.75 for item in scored)


def test_wsl_scorer_rejects_output_identity_drift(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)

    def runner(**kwargs: object) -> SimpleNamespace:
        output_path = kwargs["output_path"]
        assert isinstance(output_path, Path)
        output_path.write_text(
            json.dumps(
                {
                    "schema_version": WSL_SCORE_RESULT_SCHEMA,
                    "request_hash": digest("wrong-request"),
                    "bundle_id": bundle.bundle_id,
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with pytest.raises(AdvisoryModelFirstError) as raised:
        WslFrozenFeatureMatrixScorer(
            repo_root=Path(__file__).resolve().parents[3],
            runner=runner,
            path_converter=lambda value: value.replace("\\", "/"),
        )(bundle, _features())

    assert raised.value.reason_code == REASON_WSL_OUTPUT_INVALID


def test_wsl_meta_label_scorer_returns_production_probability_ranks(
    tmp_path: Path,
) -> None:
    bundle_path = tmp_path / "meta-bundle"
    bundle_path.mkdir()
    (bundle_path / "model.txt").write_text(
        "frozen meta model placeholder", encoding="utf-8"
    )
    bundle = {
        "bundle_path": bundle_path,
        "manifest": {"bundle_id": digest("meta-bundle")},
        "manifest_file_sha256": digest("meta-manifest"),
        "feature_schema": {
            "trained_feature_names": list(MODEL_FEATURE_COLUMNS),
            "categorical_vocabulary": {
                column: [0, 1] for column in CATEGORICAL_FEATURE_COLUMNS
            },
        },
    }
    features = _features()
    features["decision_as_of_trade_date"] = pd.Timestamp("2026-05-15")
    features["target_trade_date"] = pd.Timestamp("2026-05-18")

    def runner(**kwargs: object) -> SimpleNamespace:
        request = kwargs["request"]
        assert isinstance(request, dict)
        output_path = kwargs["output_path"]
        assert isinstance(output_path, Path)
        output_path.write_text(
            json.dumps(
                {
                    "schema_version": WSL_SCORE_RESULT_SCHEMA,
                    "request_hash": request["request_hash"],
                    "bundle_id": digest("meta-bundle"),
                    "raw_scores": [[0.1, 0.9]],
                    "raw_contributions": [],
                    "booster_feature_names": [list(MODEL_FEATURE_COLUMNS)],
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    scored = WslMetaLabelFeatureMatrixScorer(
        repo_root=Path(__file__).resolve().parents[3],
        runner=runner,
        path_converter=lambda value: value.replace("\\", "/"),
    )(bundle, features)

    assert scored["instrument"].tolist() == ["000002.SZ", "000001.SZ"]
    assert scored["entry_priority_rank"].tolist() == [1, 2]
    assert scored["take_probability"].tolist() == [0.9, 0.1]


def test_wsl_helper_recomputes_request_hash_and_binds_model_paths(
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def runner(**kwargs: object) -> SimpleNamespace:
        request = kwargs["request"]
        assert isinstance(request, dict)
        captured.update(request)
        output_path = kwargs["output_path"]
        assert isinstance(output_path, Path)
        output_path.write_text("{}", encoding="utf-8")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with pytest.raises(AdvisoryModelFirstError):
        WslFrozenFeatureMatrixScorer(
            repo_root=Path(__file__).resolve().parents[3],
            runner=runner,
            path_converter=lambda value: value.replace("\\", "/"),
        )(_bundle(tmp_path), _features())

    assert validate_request_identity(captured) == captured["request_hash"]
    tampered = {**captured, "matrix_records": [*captured["matrix_records"]]}
    tampered["matrix_records"][0] = {
        **tampered["matrix_records"][0],
        MODEL_FEATURE_COLUMNS[0]: "999.000000000000",
    }
    with pytest.raises(ValueError, match="request hash"):
        validate_request_identity(tampered)
    wrong_path = {**captured, "model_paths": [*captured["model_paths"]]}
    wrong_path["model_paths"][0] = "/tmp/unbound-model.txt"
    with pytest.raises(ValueError, match="model path differs"):
        validate_request_identity(wrong_path)
    first_model = Path(str(captured["model_paths"][0]))
    first_model.write_text("changed after host validation", encoding="utf-8")
    with pytest.raises(ValueError, match="model bytes differ"):
        validate_request_identity(captured)


def _bundle(tmp_path: Path) -> LoadedAdvisoryModelBundle:
    bundle_path = tmp_path / "bundle"
    bundle_path.mkdir()
    members = []
    for seed in QUALITY_SEEDS:
        member = bundle_path / f"model_{seed}.txt"
        member.write_text("frozen model placeholder", encoding="utf-8")
        members.append(DeferredLightgbmBooster(path=member))
    return LoadedAdvisoryModelBundle(
        bundle_id=digest("bundle"),
        bundle_path=bundle_path,
        manifest={
            "schema_version": "advisory_model_bundle_v2",
            "model_weight": 0.75,
            "explanation_policy": "MODEL_MEMBER_RAW_CONTRIBUTION_MEAN_V1",
        },
        feature_schema={
            "trained_feature_names": list(MODEL_FEATURE_COLUMNS),
            "categorical_vocabulary": {
                column: [0, 1] for column in CATEGORICAL_FEATURE_COLUMNS
            },
        },
        hmm_models={},
        baselines={},
        booster=None,
        boosters=tuple(members),
        manifest_file_sha256=digest("manifest"),
    )


def _features() -> pd.DataFrame:
    frame = pd.DataFrame(
        {column: [0.0, 0.0] for column in MODEL_FEATURE_COLUMNS},
        index=[0, 1],
    )
    for column in CATEGORICAL_FEATURE_COLUMNS:
        frame[column] = [0, 0]
    frame["instrument"] = ["000001.SZ", "000002.SZ"]
    frame["selection_effective_rank"] = [1, 2]
    frame["parent_combined_score"] = [0.7, 0.6]
    frame["candidate_group_size"] = [20, 20]
    return frame
