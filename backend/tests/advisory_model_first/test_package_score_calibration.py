from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.score_hmm_admission_contracts import SCORE_FEATURE_COLUMNS
from backend.services.advisory_model_first.score_hmm_admission_pipeline import build_package_score_features
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID


def _top50(*, tie_first_two: bool = False) -> pd.DataFrame:
    instruments = [f"{index:06d}.SZ" for index in range(1, 51)]
    parent = np.linspace(5.0, -5.0, 50)
    if tie_first_two:
        parent[1] = parent[0]
    return pd.DataFrame(
        {
            "decision_as_of_trade_date": pd.Timestamp("2025-01-02"),
            "target_trade_date": pd.Timestamp("2025-01-03"),
            "instrument": instruments,
            "combined_score": parent,
            "selection_effective_rank": np.arange(1, 51),
            f"norm__{LSTM_LEG_ID}": np.linspace(3.0, -2.0, 50),
            f"norm__{FUND_LEG_ID}": np.linspace(-1.0, 4.0, 50),
            f"rank__{LSTM_LEG_ID}": np.arange(1, 51),
            f"rank__{FUND_LEG_ID}": np.arange(50, 0, -1),
        }
    )


def test_same_day_score_features_are_positive_affine_invariant_and_drop_raw_values() -> None:
    rankings = _top50()
    transformed = rankings.copy()
    transformed["combined_score"] = transformed["combined_score"] * 7.0 + 19.0
    transformed[f"norm__{LSTM_LEG_ID}"] = transformed[f"norm__{LSTM_LEG_ID}"] * 3.0 - 11.0
    transformed[f"norm__{FUND_LEG_ID}"] = transformed[f"norm__{FUND_LEG_ID}"] * 5.0 + 2.0

    baseline = build_package_score_features(rankings, expected_row_count=None)
    rebuilt = build_package_score_features(transformed, expected_row_count=None)

    pd.testing.assert_frame_equal(
        baseline[["instrument", *SCORE_FEATURE_COLUMNS]],
        rebuilt[["instrument", *SCORE_FEATURE_COLUMNS]],
        check_exact=False,
        rtol=1e-12,
        atol=1e-12,
    )
    assert "combined_score" not in baseline
    assert not any(column.startswith("norm__") or column.startswith("rank__") for column in baseline.columns)


def test_parent_tie_break_is_instrument_ascending_and_stable_under_input_shuffle() -> None:
    rankings = _top50(tie_first_two=True).sample(frac=1.0, random_state=42)
    result = build_package_score_features(rankings, expected_row_count=None)
    assert result.iloc[:2]["instrument"].tolist() == ["000001.SZ", "000002.SZ"]
    assert result.iloc[:2]["selection_effective_rank"].tolist() == [1, 2]


def test_zero_iqr_score_distribution_fails_closed() -> None:
    rankings = _top50()
    rankings["combined_score"] = 1.0
    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_package_score_features(rankings, expected_row_count=None)
    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_SCORE_TRANSFORM_INVALID"


def test_negative_affine_transform_is_rejected_by_rank_identity() -> None:
    rankings = _top50()
    rankings["combined_score"] *= -1.0
    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_package_score_features(rankings, expected_row_count=None)
    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH"
