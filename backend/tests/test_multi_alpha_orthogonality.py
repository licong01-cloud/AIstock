from __future__ import annotations

import pandas as pd
import pytest

from backend.services.multi_alpha.orthogonality import (
    MultiAlphaOrthogonalityError,
    MultiAlphaOrthogonalityService,
    PredictionLeg,
)


def _leg(run_id: str, scores: dict[str, list[float]]) -> PredictionLeg:
    dates = [pd.Timestamp("2026-01-02").date(), pd.Timestamp("2026-01-03").date()]
    instruments = ["A", "B", "C", "D"]
    rows = []
    for date_index, trade_date in enumerate(dates):
        for inst_index, instrument in enumerate(instruments):
            rows.append(
                {
                    "trade_date": trade_date,
                    "instrument": instrument,
                    "score": scores[instrument][date_index],
                }
            )
    return PredictionLeg(run_id=run_id, frame=pd.DataFrame(rows), model=f"model-{run_id}", factor_set=f"fs-{run_id}")


def test_multi_alpha_orthogonality_known_corr_and_jaccard() -> None:
    legs = {
        "run_a": _leg("run_a", {"A": [4, 4], "B": [3, 3], "C": [2, 2], "D": [1, 1]}),
        "run_b": _leg("run_b", {"A": [4, 4], "B": [3, 3], "C": [2, 2], "D": [1, 1]}),
        "run_c": _leg("run_c", {"A": [1, 1], "B": [2, 2], "C": [3, 3], "D": [4, 4]}),
    }
    service = MultiAlphaOrthogonalityService(prediction_loader=lambda run_id: legs[run_id])

    result = service.compute(run_ids=["run_a", "run_b", "run_c"], k=2)

    assert result["legs"] == ["run_a", "run_b", "run_c"]
    assert result["n_common_dates"] == 2
    assert result["pred_corr_matrix"][0][1] == 1.0
    assert result["pred_corr_matrix"][0][2] == -1.0
    assert result["jaccard_matrix"][0][1] == 1.0
    assert result["jaccard_matrix"][0][2] == 0.0
    assert result["per_leg"]["run_a"]["model"] == "model-run_a"


def test_multi_alpha_orthogonality_uses_common_dates_only() -> None:
    left = _leg("left", {"A": [2, 5], "B": [1, 4], "C": [4, 3], "D": [3, 2]})
    right = _leg("right", {"A": [2, 5], "B": [1, 4], "C": [4, 3], "D": [3, 2]})
    right = PredictionLeg(
        run_id="right",
        frame=right.frame[right.frame["trade_date"] == pd.Timestamp("2026-01-03").date()].copy(),
    )
    service = MultiAlphaOrthogonalityService(prediction_loader=lambda run_id: left if run_id == "left" else right)

    result = service.compute(run_ids=["left", "right"], k=2)

    assert result["n_common_dates"] == 1
    assert result["common_date_start"] == "2026-01-03"
    assert result["pred_corr_matrix"][0][1] == 1.0


def test_multi_alpha_orthogonality_rejects_missing_common_dates() -> None:
    left = PredictionLeg(
        run_id="left",
        frame=pd.DataFrame([{"trade_date": pd.Timestamp("2026-01-02").date(), "instrument": "A", "score": 1.0}]),
    )
    right = PredictionLeg(
        run_id="right",
        frame=pd.DataFrame([{"trade_date": pd.Timestamp("2026-01-03").date(), "instrument": "A", "score": 1.0}]),
    )
    service = MultiAlphaOrthogonalityService(prediction_loader=lambda run_id: left if run_id == "left" else right)

    with pytest.raises(MultiAlphaOrthogonalityError, match="no common trade_date"):
        service.compute(run_ids=["left", "right"], k=2)
