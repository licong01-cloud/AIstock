from __future__ import annotations

import pandas as pd
import pytest

from backend.services.multi_alpha.combiner import MultiAlphaCombiner
from backend.services.multi_alpha.panels import MultiAlphaPanelBuilder, MultiAlphaPanelError, normalize_label_frame


DATES = [pd.Timestamp("2026-01-02").date(), pd.Timestamp("2026-01-03").date(), pd.Timestamp("2026-01-04").date()]
INSTRUMENTS = ["A", "B", "C"]


def _pred(multiplier: float = 1.0) -> pd.DataFrame:
    rows = []
    for date_index, trade_date in enumerate(DATES):
        for rank, instrument in enumerate(INSTRUMENTS, start=1):
            rows.append({"trade_date": trade_date, "instrument": instrument, "score": multiplier * (rank + date_index)})
    return pd.DataFrame(rows)


def _label() -> pd.DataFrame:
    rows = []
    for date_index, trade_date in enumerate(DATES):
        for rank, instrument in enumerate(INSTRUMENTS, start=1):
            rows.append({"trade_date": trade_date, "instrument": instrument, "forward_return": 0.01 * (rank + date_index)})
    return pd.DataFrame(rows)


def test_panel_builder_computes_ic_and_topk_returns_for_seed_ensemble() -> None:
    preds = {
        "run_a1": _pred(1.0),
        "run_a2": _pred(2.0),
        "run_b1": _pred(-1.0),
        "run_b2": _pred(-2.0),
    }
    labels = {run_id: _label() for run_id in preds}
    builder = MultiAlphaPanelBuilder(
        prediction_loader=lambda run_id: preds[run_id],
        label_loader=lambda run_id: labels[run_id],
    )

    legs = builder.build_combiner_legs(
        legs=[
            {"leg_id": "positive", "seed_run_ids": ["run_a1", "run_a2"]},
            {"leg_id": "negative", "seed_run_ids": ["run_b1", "run_b2"]},
        ],
        oos_start=DATES[0],
        oos_end=DATES[-1],
        topk=1,
        min_date_coverage=1.0,
    )

    assert legs[0].metric_by_date[DATES[0]] == pytest.approx(1.0)
    assert legs[0].returns_by_date[DATES[0]] == pytest.approx(0.03)
    assert legs[1].metric_by_date[DATES[0]] == pytest.approx(-1.0)
    assert legs[1].returns_by_date[DATES[0]] == pytest.approx(0.01)


def test_walk_forward_weights_use_only_past_panel_dates() -> None:
    builder = MultiAlphaPanelBuilder(
        prediction_loader=lambda run_id: _pred(1.0 if run_id.startswith("a") else -1.0),
        label_loader=lambda _run_id: _label(),
    )
    legs = builder.build_combiner_legs(
        legs=[
            {"leg_id": "positive", "seed_run_ids": ["a1"]},
            {"leg_id": "negative", "seed_run_ids": ["b1"]},
        ],
        oos_start=DATES[0],
        oos_end=DATES[-1],
        topk=1,
        min_date_coverage=1.0,
    )

    result = MultiAlphaCombiner().combine(
        legs=legs,
        weighting_scheme="ic_weighted",
        normalize_method="rank",
        walk_forward={"enabled": True, "window": 1, "min_periods": 1},
    )

    assert result.per_window_weights[0]["apply_date"] == "2026-01-03"
    assert result.per_window_weights[0]["train_start"] == "2026-01-02"
    assert result.per_window_weights[0]["train_end"] == "2026-01-02"
    assert result.per_window_weights[0]["weights"] == {"positive": 1.0, "negative": 0.0}
    assert set(result.combined_score_frame["trade_date"].unique()) == {DATES[1], DATES[2]}


def test_missing_label_fails_loud_with_leg_and_reason_code() -> None:
    builder = MultiAlphaPanelBuilder(
        prediction_loader=lambda _run_id: _pred(),
        label_loader=lambda _run_id: (_ for _ in ()).throw(FileNotFoundError("label.pkl")),
    )

    with pytest.raises(MultiAlphaPanelError) as excinfo:
        builder.build_combiner_legs(
            legs=[
                {"leg_id": "leg_a", "seed_run_ids": ["run_missing_label"]},
                {"leg_id": "leg_b", "seed_run_ids": ["run_b"]},
            ],
            oos_start=DATES[0],
            oos_end=DATES[-1],
        )

    assert excinfo.value.leg_id == "leg_a"
    assert excinfo.value.reason_code == "label_missing_or_invalid"
    assert "run_missing_label" in str(excinfo.value)


def test_label_normalizer_requires_valid_shape() -> None:
    with pytest.raises(MultiAlphaPanelError) as excinfo:
        normalize_label_frame(pd.DataFrame({"not_label": ["x"]}), run_id="bad")
    assert excinfo.value.reason_code in {"label_column_missing", "label_columns_invalid"}
