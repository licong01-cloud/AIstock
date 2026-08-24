from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.meta_label_contracts import (
    approved_meta_label_families,
    approved_meta_label_outcome_weighting,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_training import (
    HMM_FEATURES,
    apply_meta_label_outcome_weights,
    fit_meta_label_outcome_weights,
    meta_label_feature_names,
    train_meta_label_trial,
)


def _matrix() -> tuple[pd.DataFrame, pd.DataFrame, pd.DatetimeIndex]:
    dates = pd.bdate_range("2026-01-02", periods=12)
    rows = []
    labels = []
    rng = np.random.default_rng(42)
    for day_index, day in enumerate(dates):
        for rank in range(1, 21):
            symbol = f"{rank:06d}.SZ"
            row = {
                "decision_as_of_trade_date": day,
                "target_trade_date": day + pd.offsets.BDay(1),
                "instrument": symbol,
                "selection_effective_rank": rank,
            }
            for column in MODEL_FEATURE_COLUMNS:
                row[column] = float(rng.normal())
            row["l2_code_id"] = rank % 3
            rows.append(row)
            take = int((rank + day_index) % 2 == 0)
            labels.append(
                {
                    "decision_as_of_trade_date": day,
                    "target_trade_date": day + pd.offsets.BDay(1),
                    "instrument": symbol,
                    "label_status": "MATURED",
                    "take_label": take,
                    "net_excess_return_bps": 100.0 if take else -100.0,
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(labels), dates


def test_core_family_excludes_hmm_values_and_missing_indicators_and_fails_loudly_without_lightgbm() -> None:
    features, labels, dates = _matrix()
    family = approved_meta_label_families()[0]
    assert not (set(meta_label_feature_names(family)) & HMM_FEATURES)
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        train_meta_label_trial(
            features=features,
            labels=labels,
            train_dates=dates[:8],
            validation_dates=dates[8:],
            family=family,
            seed=20260813,
        )
    assert excinfo.value.reason_code == "ADVISORY_MODEL_TRAINING_REQUIRES_WSL"


def test_outcome_weights_are_train_local_capped_and_mean_normalized() -> None:
    specification = approved_meta_label_outcome_weighting()
    train = pd.Series([-100.0, 100.0, 200.0, 10_000.0])
    weights, fit = fit_meta_label_outcome_weights(
        train,
        specification=specification,
    )
    assert fit.scale_bps == 150.0
    assert weights.mean() == pytest.approx(1.0)
    assert weights[-1] == pytest.approx(weights.max())
    assert weights[-1] == pytest.approx(
        (specification.base_weight + specification.relative_cap) / fit.normalization_divisor
    )
    validation = apply_meta_label_outcome_weights(
        pd.Series([1.0, 1_000_000.0]),
        specification=specification,
        fit=fit,
    )
    assert validation[-1] == pytest.approx(weights[-1])
    assert fit.scale_bps == 150.0


@pytest.mark.parametrize(
    "values",
    [pd.Series([0.0, 0.0]), pd.Series([1.0, np.nan]), pd.Series([1.0, np.inf])],
)
def test_outcome_weights_fail_closed_for_invalid_training_returns(values) -> None:
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        fit_meta_label_outcome_weights(
            values,
            specification=approved_meta_label_outcome_weighting(),
        )
    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_OUTCOME_WEIGHTING_INVALID"
