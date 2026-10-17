import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.cross_package_validation_exit_v1 import (
    choose_frozen_exit_fold, frozen_linear_exit_prediction,
)
from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES


def test_exit_fold_selection_uses_only_original_past_metadata():
    schedules = {
        "1": dict(status="FITTED", latest_training_e="2025-04-01", first_evaluation_s="2025-04-03"),
        "2": dict(status="FITTED", latest_training_e="2026-03-12", first_evaluation_s="2026-03-13"),
    }
    assert choose_frozen_exit_fold(schedules=schedules, first_s="2026-03-12") == "1"
    with pytest.raises(ValueError, match="no original"):
        choose_frozen_exit_fold(schedules=schedules, first_s="2025-04-01")


@pytest.mark.parametrize("held_path", [False, True])
def test_exact_frozen_exit_encoding_preserves_unknown_and_excludes_labels(held_path):
    extras = ("held_mark_return_bps", "held_peak_drawdown_fraction", "held_range_fraction") if held_path else ()
    columns = (*FEATURES, *extras, "remaining_session_fraction")
    values = {name: [np.nan if i == 0 else 2.] for i, name in enumerate(columns)}
    values.update(episode_id=["original"], s_date=["2026-03-12"])
    frame = pd.DataFrame(values)
    names = ("median", "mean", "scale") if held_path else ("medians", "means", "scales")
    recipe = dict(zip(names, ([1.]*len(columns), [0.]*len(columns), [2.]*len(columns)), strict=True))
    coefficients = [1.]*(2*len(columns)-1)
    fitted = dict(status="FITTED", recipe=recipe, coefficients=coefficients, intercept=3.)
    result = frozen_linear_exit_prediction(s_inputs=frame, fitted=fitted, held_path=held_path)
    assert result.predicted_y_hold_bps.iloc[0] == pytest.approx(.5+len(columns)-1+1+3)
    for forbidden in ("y_hold_bps", "sell_scenario_bps", "endpoint_close"):
        with pytest.raises(ValueError, match="only original S"):
            frozen_linear_exit_prediction(s_inputs=frame.assign(**{forbidden: -999.}), fitted=fitted, held_path=held_path)
    fitted["recipe"][names[2]][0] = 0.
    with pytest.raises(ValueError, match="numeric recipe"):
        frozen_linear_exit_prediction(s_inputs=frame, fitted=fitted, held_path=held_path)
