from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.model_bundle import LoadedAdvisoryModelBundle
from backend.services.advisory_model_first.model_inference import _score
from backend.services.advisory_model_first.quality_contracts import QUALITY_SEEDS
from backend.services.advisory_model_first.quality_tournament import (
    apply_ensemble_scores,
    prepare_model_matrix,
)


class _Booster:
    def __init__(self, values: list[float]) -> None:
        self.values = np.asarray(values, dtype=float)

    def feature_name(self) -> list[str]:
        return list(MODEL_FEATURE_COLUMNS)

    def predict(self, matrix: pd.DataFrame, pred_contrib: bool = False):
        if pred_contrib:
            result = np.zeros((len(matrix), len(MODEL_FEATURE_COLUMNS) + 1), dtype=float)
            result[:, 0] = self.values
            return result
        return self.values


def _features() -> pd.DataFrame:
    frame = pd.DataFrame({column: [0.0, 0.0, 0.0] for column in MODEL_FEATURE_COLUMNS})
    frame["l2_code_id"] = [1, 1, 1]
    frame["instrument"] = ["000001.SZ", "000002.SZ", "000003.SZ"]
    frame["selection_effective_rank"] = [1, 2, 3]
    frame["candidate_group_size"] = [3, 3, 3]
    frame["parent_combined_score"] = [0.9, 0.8, 0.7]
    return frame


def test_percentile_ensemble_and_selection_prior_follow_frozen_formula() -> None:
    frame = pd.DataFrame(
        {
            "decision_as_of_trade_date": pd.to_datetime(["2026-01-01"] * 3),
            "instrument": ["A", "B", "C"],
            "selection_effective_rank": [1, 2, 3],
            "candidate_group_size": [3, 3, 3],
        }
    )
    for seed in QUALITY_SEEDS:
        frame[f"raw_score_{seed}"] = [0.0, 2.0, 1.0]
    scored = apply_ensemble_scores(
        frame,
        score_columns=tuple(f"raw_score_{seed}" for seed in QUALITY_SEEDS),
        model_weight=0.5,
    ).set_index("instrument")
    assert scored.loc["A", "advisory_model_score"] == pytest.approx(0.5)
    assert scored.loc["B", "advisory_model_score"] == pytest.approx(0.75)
    assert scored.loc["C", "advisory_model_score"] == pytest.approx(0.25)


def test_ensemble_rejects_missing_seed_instead_of_using_subset() -> None:
    frame = pd.DataFrame(
        {
            "decision_as_of_trade_date": pd.to_datetime(["2026-01-01"]),
            "instrument": ["A"],
            "selection_effective_rank": [1],
            "candidate_group_size": [1],
            "raw_score_20260808": [1.0],
        }
    )
    with pytest.raises(AdvisoryModelFirstError) as raised:
        apply_ensemble_scores(frame, score_columns=("raw_score_20260808",), model_weight=1.0)
    assert raised.value.reason_code == "ADVISORY_M5_ENSEMBLE_INCOMPLETE"


def test_selection_prior_uses_frozen_group_size_when_one_modelable_row_is_missing() -> None:
    frame = pd.DataFrame(
        {
            "decision_as_of_trade_date": pd.to_datetime(["2026-01-01", "2026-01-01"]),
            "instrument": ["A", "C"],
            "selection_effective_rank": [1, 3],
            "candidate_group_size": [3, 3],
        }
    )
    scored = apply_ensemble_scores(frame, score_columns=(), model_weight=0.0).set_index("instrument")
    assert scored.loc["A", "selection_prior"] == pytest.approx(1.0)
    assert scored.loc["C", "selection_prior"] == pytest.approx(0.0)


def test_unseen_categorical_value_sets_missing_indicator_like_runtime() -> None:
    frame = pd.DataFrame({column: [0.0, 0.0] for column in MODEL_FEATURE_COLUMNS})
    frame["l2_code_id"] = [1, 2]
    frame["l2_code_id__missing"] = [0, 0]
    matrix, vocabulary = prepare_model_matrix(
        frame,
        train_mask=pd.Series([True, False]),
    )
    assert vocabulary["l2_code_id"] == (1,)
    assert pd.isna(matrix.loc[1, "l2_code_id"])
    assert matrix.loc[1, "l2_code_id__missing"] == 1


def test_runtime_v2_uses_five_member_percentile_ensemble_and_prior() -> None:
    bundle = LoadedAdvisoryModelBundle(
        bundle_id="b" * 64,
        bundle_path=Path("/bundle"),
        manifest={
            "schema_version": "advisory_model_bundle_v2",
            "model_weight": 0.5,
            "explanation_policy": "MODEL_MEMBER_RAW_CONTRIBUTION_MEAN_V1",
        },
        feature_schema={"categorical_vocabulary": {"l2_code_id": [1]}},
        hmm_models={},
        baselines={},
        booster=None,
        boosters=tuple(_Booster([0.0, 2.0, 1.0]) for _ in QUALITY_SEEDS),
    )
    scored = {item["symbol"]: item for item in _score(bundle, _features())}
    assert scored["000002.SZ"]["advisory_model_rank"] == 1
    assert scored["000002.SZ"]["advisory_model_score"] == pytest.approx(0.75)
    assert scored["000001.SZ"]["score_components"] == {
        "ensemble_score": 0.0,
        "selection_prior": 1.0,
        "model_weight": 0.5,
    }
