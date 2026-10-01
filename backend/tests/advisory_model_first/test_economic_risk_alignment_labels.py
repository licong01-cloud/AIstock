from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_labels import build_economic_entry_labels
from backend.services.advisory_model_first.economic_risk_alignment_labels import build_entry_loss_labels_v2
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_labels"]


def _args(inputs):
    inputs["prices"]["tradability_unknown"] = False
    inputs["prices"]["up_limit"], inputs["prices"]["down_limit"] = 15., 5.
    return {"original_labels": build_economic_entry_labels(**inputs),
            **{name: inputs[name] for name in ("episodes", "prices", "trading_calendar", "identity")}}


def test_cost_once_entry_loss_not_peak_loss_and_no_future_high_low(inputs):
    args = _args(inputs)
    before = args["original_labels"][0].label_sha256
    value, = build_entry_loss_labels_v2(**args)
    assert value.entry_net_max_loss_bps == pytest.approx((1 - 9 * .998 / (10 * 1.001)) * 10000)
    assert value.episode_peak_to_trough_drawdown_bps == pytest.approx(2500)
    args["prices"]["raw_low_cny"] = .001
    args["prices"].loc[3, "raw_close_cny"] = 100000
    assert build_entry_loss_labels_v2(**args)[0] == value
    assert value.original.label_sha256 == before and not value.deployable


@pytest.mark.parametrize("defect", ["entry_limit", "exit_limit", "missing_mark", "ambiguous_mark", "limit_unknown"])
def test_unknown_execution_or_mark_preserves_original_without_numeric_risk(inputs, defect):
    args = _args(inputs)
    if defect == "entry_limit":
        args["prices"].loc[1, "up_limit"] = 10.
    elif defect == "exit_limit":
        args["prices"].loc[3, "down_limit"] = 11.
    elif defect == "missing_mark":
        args["prices"] = args["prices"].drop(index=2)
    elif defect == "ambiguous_mark":
        args["prices"].loc[2, "tradability_unknown"] = True
    else:
        args["prices"].loc[1, "up_limit"] = None
    value, = build_entry_loss_labels_v2(**args)
    assert value.status == "UNAVAILABLE" and value.original.status == "AVAILABLE"
    assert value.entry_net_max_loss_bps is None and value.original_label_sha256 == args["original_labels"][0].label_sha256


def test_suspension_carries_adjusted_mark_without_inventing_quote(inputs):
    args = _args(inputs)
    args["prices"].loc[2, "suspended"] = True
    args["prices"].loc[2, ["raw_open_cny", "raw_close_cny"]] = None
    args["original_labels"] = build_economic_entry_labels(**{**inputs, "prices": args["prices"]})
    value, = build_entry_loss_labels_v2(**args)
    assert value.status == "AVAILABLE"
    assert value.entry_net_max_loss_bps == pytest.approx((1 - .998 / 1.001) * 10000)


@pytest.mark.parametrize("defect", ["source", "endpoint", "duplicate", "clock", "calendar"])
def test_provenance_and_structural_contradictions_fail_closed(inputs, defect):
    args = _args(inputs)
    if defect == "source":
        args["prices"].loc[0, "source_sha256"] = "f" * 64
    elif defect == "endpoint":
        args["episodes"].loc[0, "entry_price"] = 20.
    elif defect == "duplicate":
        args["prices"] = pd.concat([args["prices"], args["prices"].iloc[[0]]])
    elif defect == "clock":
        args["episodes"].loc[0, "selection_rank"] = 2
    else:
        args["trading_calendar"] = list(reversed(args["trading_calendar"]))
    with pytest.raises(AdvisoryModelFirstError):
        build_entry_loss_labels_v2(**args)
