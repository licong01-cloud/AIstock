from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.dual_head_output_constraint_pipeline import (
    _attach_labels,
    _train_p0d_oof,
    _verify_cpcv_identity,
    _verify_label_status_identity,
    _verify_prediction_dates,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_dual_head_output_constraint_contracts import _request


def _priorities(dates) -> pd.DataFrame:
    rows = []
    for decision in pd.to_datetime(list(dates)):
        for rank in range(1, 21):
            rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": decision + pd.offsets.BDay(1),
                    "instrument": f"S{rank:02d}",
                    "selection_effective_rank": rank,
                    "entry_priority_rank": rank,
                }
            )
    return pd.DataFrame(rows)


def test_prediction_date_identity_requires_exact_top20_and_exact_date_set() -> None:
    dates = pd.to_datetime(["2026-01-05", "2026-01-06"])
    _verify_prediction_dates(_priorities(dates), dates)
    with pytest.raises(AdvisoryModelFirstError, match="differ from exact calibration dates"):
        _verify_prediction_dates(_priorities(dates).iloc[:-1], dates)


def test_p0d_oof_uses_each_fold_train_and_score_dates_once(monkeypatch) -> None:
    calls = []

    def fake_train(**kwargs):
        calls.append((tuple(kwargs["train_dates"]), tuple(kwargs["score_dates"])))
        return _priorities(kwargs["score_dates"])

    monkeypatch.setattr(
        "backend.services.advisory_model_first.dual_head_output_constraint_pipeline."
        "train_fixed_p0d_reference_predictions",
        fake_train,
    )
    folds = [
        SimpleNamespace(
            train_dates=(pd.Timestamp("2026-01-05"),),
            score_dates=(pd.Timestamp("2026-01-06"),),
        ),
        SimpleNamespace(
            train_dates=(pd.Timestamp("2026-01-06"),),
            score_dates=(pd.Timestamp("2026-01-07"),),
        ),
    ]
    result = _train_p0d_oof(
        features=pd.DataFrame(),
        labels=pd.DataFrame(),
        folds=folds,
        family=object(),
        seed=20260813,
        boost_rounds=2,
    )
    assert len(calls) == 2
    assert len(result) == 40
    assert result["decision_as_of_trade_date"].nunique() == 2


def test_attach_labels_leaves_non_matured_liability_missing() -> None:
    predictions = _priorities(pd.to_datetime(["2026-01-05"]))
    labels = predictions[
        ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    ].copy()
    labels["label_status"] = "MATURED"
    labels["net_excess_return_bps"] = 1.0
    labels["holding_trading_days"] = 5.0
    labels.loc[0, "label_status"] = "NOT_ENTERED_LIMIT_UP"
    labels.loc[0, "holding_trading_days"] = None
    attached = _attach_labels(predictions, labels)
    assert attached.loc[0, "turnover_liability_fraction_per_day"] != attached.loc[
        0, "turnover_liability_fraction_per_day"
    ]
    assert attached.loc[1, "turnover_liability_fraction_per_day"] == pytest.approx(2 / 25)


def test_frozen_label_and_cpcv_identity_fail_closed() -> None:
    request = _request()
    labels = pd.DataFrame(
        {
            "label_status": [
                *(["MATURED"] * 7716),
                *(["NOT_ENTERED_LIMIT_UP"] * 3),
                "CENSORED_RIGHT_BOUNDARY",
            ]
        }
    )
    _verify_label_status_identity(request, labels)
    labels.loc[0, "label_status"] = "CENSORED_RIGHT_BOUNDARY"
    with pytest.raises(AdvisoryModelFirstError, match="differs from frozen P0-C"):
        _verify_label_status_identity(request, labels)

    paths = [
        {"path_id": f"path_{index:02d}", "validation_blocks": [0, 1]}
        for index in range(28)
    ]
    block_by_date = {f"2026-01-{index + 1:02d}": index for index in range(8)}
    _verify_cpcv_identity(request, paths, block_by_date)
    paths[0]["validation_blocks"] = [0]
    with pytest.raises(AdvisoryModelFirstError, match="CPCV identity"):
        _verify_cpcv_identity(request, paths, block_by_date)
