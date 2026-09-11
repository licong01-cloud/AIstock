from datetime import date, timedelta
import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError, FEATURE_ORDER, cutoff_on
from backend.services.position_timing.pattern_model import (
    CORE_SET,
    ENHANCED_SET,
    ENTRY_HEAD,
    EXIT_HEAD,
    FEATURE_ORDERS,
    MODEL_ROW_COLUMNS,
    PATTERN_COLUMNS,
    fit_pattern_model,
)


def _training_rows() -> pd.DataFrame:
    rows = []
    base = date(2023, 1, 3)
    for index in range(12):
        for head in (ENTRY_HEAD, EXIT_HEAD):
            values = {name: float(index + 1) for name in FEATURE_ORDER}
            if head == ENTRY_HEAD:
                values["holding_age"] = np.nan
                values["holding_age_missing"] = 1.0
                values["unrealized_return_bps"] = np.nan
                values["entry_cost_missing"] = 1.0
            else:
                values["holding_age_missing"] = 0.0
                values["entry_cost_missing"] = 0.0
            pattern = {name: float(index) / 10.0 for name in PATTERN_COLUMNS}
            day = base + timedelta(days=index)
            rows.append(
                {
                    "decision_as_of": cutoff_on(day),
                    "label_available_at": cutoff_on(day + timedelta(days=2)),
                    "objective": head,
                    "net_action_value_bps": float(index - 5),
                    **values,
                    **pattern,
                }
            )
    # A future label must not enter the fit even though its decision is old.
    future = dict(rows[0])
    future["label_available_at"] = cutoff_on(date(2025, 1, 1))
    future["net_action_value_bps"] = 999999.0
    rows.append(future)
    return pd.DataFrame(rows)


@pytest.mark.parametrize("feature_set", [CORE_SET, ENHANCED_SET])
def test_fixed_pattern_model_uses_only_mature_labels_and_predicts(feature_set):
    rows = _training_rows()
    cutoff = cutoff_on(date(2024, 1, 31))
    available = cutoff_on(date(2024, 2, 1))
    model = fit_pattern_model(
        rows,
        feature_set=feature_set,
        cutoff=cutoff,
        available_at=available,
        source_sha256="a" * 64,
        request_sha256="b" * 64,
        source_commit="c" * 40,
    )
    assert model.metadata["parameters"]
    assert model.metadata["heads"][ENTRY_HEAD]["training_rows"] == 12
    assert model.metadata["heads"][EXIT_HEAD]["training_rows"] == 12
    order = FEATURE_ORDERS[feature_set]
    sample = rows.iloc[0]
    inputs = {name: sample[name] for name in order}
    value = model.predict_one(inputs, head=ENTRY_HEAD, decision_as_of=available)
    assert np.isfinite(value)
    with pytest.raises(ActionValueError, match="NOT_AVAILABLE"):
        model.predict_one(inputs, head=ENTRY_HEAD, decision_as_of=cutoff)


def test_missing_head_is_typed_not_estimable():
    rows = _training_rows()
    rows = rows.loc[rows.objective.eq(ENTRY_HEAD)]
    with pytest.raises(ActionValueError, match="NOT_ESTIMABLE"):
        fit_pattern_model(
            rows,
            feature_set=CORE_SET,
            cutoff=cutoff_on(date(2024, 1, 31)),
            available_at=cutoff_on(date(2024, 2, 1)),
            source_sha256="a" * 64,
            request_sha256="b" * 64,
            source_commit="c" * 40,
        )


def test_empty_but_well_formed_training_population_is_typed_not_estimable():
    rows = pd.DataFrame(columns=MODEL_ROW_COLUMNS)
    with pytest.raises(ActionValueError, match="NOT_ESTIMABLE"):
        fit_pattern_model(
            rows,
            feature_set=CORE_SET,
            cutoff=cutoff_on(date(2024, 1, 31)),
            available_at=cutoff_on(date(2024, 2, 1)),
            source_sha256="a" * 64,
            request_sha256="b" * 64,
            source_commit="c" * 40,
        )


def test_naive_training_time_is_rejected_not_assumed_utc():
    rows = _training_rows()
    rows["label_available_at"] = rows["label_available_at"].astype(object)
    rows.loc[0, "label_available_at"] = pd.Timestamp("2023-01-05 20:00:00")
    with pytest.raises(ActionValueError, match="LABEL_TIME_INVALID"):
        fit_pattern_model(
            rows,
            feature_set=CORE_SET,
            cutoff=cutoff_on(date(2024, 1, 31)),
            available_at=cutoff_on(date(2024, 2, 1)),
            source_sha256="a" * 64,
            request_sha256="b" * 64,
            source_commit="c" * 40,
        )
