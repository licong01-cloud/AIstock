from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.causal_timing_contracts import FEATURE_ORDER
from backend.services.position_timing.causal_timing_model import (
    LABEL, fit_gbdt, fit_ridge, predict, training_split,
)


def rows() -> pd.DataFrame:
    records = []
    for offset in range(12):
        train = offset < 8
        decision = pd.Timestamp("2022-10-03") + pd.offsets.BDay(offset) if train else pd.Timestamp("2024-03-01") + pd.offsets.BDay(offset)
        available = decision + pd.offsets.BDay(21)
        values = {name: float(offset + index / 10) for index, name in enumerate(FEATURE_ORDER)}
        records.append({
            "symbol": f"000{offset:03d}.SZ", "decision_date": decision.date().isoformat(),
            "label_available_at": available.date().isoformat(), LABEL: float(offset * 2 - 5), **values,
        })
    return pd.DataFrame(records)


def test_temporal_split_and_train_only_preprocessing() -> None:
    frame = rows()
    train, valid, audit = training_split(frame)
    assert audit == {"input_rows": 12, "train_rows": 8, "validation_rows": 4, "immature_or_outside_rows": 0}
    first = fit_ridge(frame, source_sha256="1" * 64, request_sha256="2" * 64)
    changed = frame.copy()
    changed.loc[changed.index[-4:], LABEL] += 10_000
    second = fit_ridge(changed, source_sha256="1" * 64, request_sha256="2" * 64)
    assert first["coefficients"] == second["coefficients"]
    assert first["intercept"] == second["intercept"]
    assert first["validation"] != second["validation"]
    assert len(predict(first, valid)) == 4


def test_future_feature_perturbation_does_not_change_trained_coefficients() -> None:
    frame = rows()
    first = fit_ridge(frame, source_sha256="3" * 64, request_sha256="4" * 64)
    shifted = frame.copy()
    shifted.loc[shifted.index[-4:], list(FEATURE_ORDER)] += 1_000_000
    second = fit_ridge(shifted, source_sha256="3" * 64, request_sha256="4" * 64)
    assert np.allclose(first["coefficients"], second["coefficients"])
    assert first["mean"] == second["mean"]


def test_gbdt_uses_the_frozen_single_head_contract() -> None:
    model = fit_gbdt(rows(), source_sha256="5" * 64, request_sha256="6" * 64)
    _, valid, _ = training_split(rows())
    assert model["spec"]["num_boost_round"] == 100
    assert model["spec"]["num_threads"] == 1
    assert len(predict(model, valid)) == 4


def test_oracle_columns_cannot_enter_frozen_feature_whitelist() -> None:
    frame = rows()
    frame["restricted_oracle_future_value"] = np.arange(len(frame)) * 1_000_000.0
    first = fit_ridge(frame, source_sha256="7" * 64, request_sha256="8" * 64)
    frame["restricted_oracle_future_value"] *= -100
    second = fit_ridge(frame, source_sha256="7" * 64, request_sha256="8" * 64)
    assert first["coefficients"] == second["coefficients"]
    broken = frame.rename(columns={FEATURE_ORDER[0]: "return_1d_bps_shift_minus1"})
    with pytest.raises(ActionValueError, match="CAUSAL_MODEL_COLUMNS_MISSING"):
        fit_ridge(broken, source_sha256="7" * 64, request_sha256="8" * 64)
