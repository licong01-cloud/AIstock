from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_aligned_evaluation import (
    aligned_intervention_attribution_v3, fixed_block_interval_v3, query_aligned_actual_opens_v3,
    shadow_endpoint_execution_audit_v3, shadow_portfolio_intervention_support_v3,
)
from backend.services.advisory_model_first.economic_entry_aligned_training import train_aligned_entry_model_v3
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_entry_aligned_training import _inputs

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def _query(study):
    args = _inputs(study)
    fitted = train_aligned_entry_model_v3(**args)
    source = study["request"]
    frozen = study["features"].loc[study["features"].decision_as_of_trade_date.between(pd.Timestamp(source.test_start), pd.Timestamp(source.test_end))].copy()
    roster = frozen.loc[:, ["decision_as_of_trade_date", "target_trade_date", "instrument"]].copy()
    roster["selection_effective_rank"] = [int(value[:6]) for value in roster.instrument]
    refs = roster.assign(target_reference_raw_cny=10., reference_visible_through=roster.decision_as_of_trade_date,
                         source_sha256=study["identity"].reference_source_sha256)
    quotes = roster.rename(columns={"target_trade_date": "trade_date"}).assign(raw_open_cny=10.02, suspended=False,
                                                                                tradability_unknown=False, up_limit=11., down_limit=9.,
                                                                                source_sha256=study["identity"].price_source_sha256,
                                                                                price_coordinate_sha256=study["identity"].price_coordinate_sha256)
    return dict(fitted=fitted, identity=study["identity"], candidates=roster, features=frozen, references=refs, prices=quotes)


def test_future_episode_status_and_ohlc_never_change_actual_query(study):
    args = _query(study)
    before = query_aligned_actual_opens_v3(**args)
    args["prices"]["raw_high_cny"], args["prices"]["raw_close_cny"] = 1e12, .001
    args["features"]["label_status"], args["features"]["future_exit_return"] = "UNKNOWN", -9999
    pd.testing.assert_frame_equal(query_aligned_actual_opens_v3(**args), before)
    args["prices"].loc[args["prices"].index[0], "up_limit"] = 10.02
    after = query_aligned_actual_opens_v3(**args)
    assert after.model_action.iloc[0] == "UNAVAILABLE" and len(after) == len(before)
    args["candidates"].loc[args["candidates"].index[0], "selection_effective_rank"] = 0
    with pytest.raises(AdvisoryModelFirstError, match="invalid or duplicate Top20"):
        query_aligned_actual_opens_v3(**args)


def test_control_profit_is_not_actual_model_take_and_fixed_block_not_tuned():
    decision = pd.Timestamp("2025-10-09")
    rows = pd.DataFrame({"decision_as_of_trade_date": [decision] * 3, "instrument": ["000001.SZ", "000002.SZ", "000003.SZ"],
                         "selection_effective_rank": [1, 2, 3], "model_action": ["UNAVAILABLE", "SKIP", "TAKE"]})
    baseline = pd.DataFrame({"entry_signal_date": [decision] * 3, "instrument": rows.instrument, "net_return_bps": [100, 200, -100]})
    model = baseline.iloc[[0, 2]].copy()
    result = aligned_intervention_attribution_v3(rows, baseline, model)
    assert result["actual_model_take_episodes"] == 1 and result["research_unknown_control_episodes"] == 1
    assert result["skipped_baseline_profitable"] == 1
    empty = aligned_intervention_attribution_v3(rows, baseline, pd.DataFrame())
    assert empty["actual_model_take_episodes"] == empty["research_unknown_control_episodes"] == 0
    assert fixed_block_interval_v3([0., 1., -1.]) == fixed_block_interval_v3([0., 1., -1.])
    with pytest.raises(AdvisoryModelFirstError, match="one fixed block"):
        fixed_block_interval_v3([0., 1.], block_days=1)


def test_nominal_holding_changes_and_execution_restrictions_are_separate():
    days = pd.date_range("2025-10-09", periods=3)
    baseline = pd.DataFrame({"episode_id": ["episode1"], "instrument": ["000001.SZ"],
                             "entry_trade_date": [days[0]], "exit_trade_date": [days[-1]], "entry_price": [10.], "exit_price": [9.]})
    assert shadow_portfolio_intervention_support_v3(baseline, baseline, days)["holding_difference_day_count"] == 0
    changed = shadow_portfolio_intervention_support_v3(baseline, pd.DataFrame(), days)
    assert changed["holding_difference_day_count"] == 2 and changed["entry_action_difference_day_count"] == 1
    prices = pd.DataFrame({"trade_date": [days[0], days[-1]], "instrument": ["000001.SZ"] * 2,
                           "raw_open_cny": [10., 9.], "policy_price_per_raw_cny": [1., 1.], "up_limit": [11., 11.],
                           "down_limit": [9., 9.], "tradability_unknown": [False, False], "suspended": [False, False]})
    before = baseline.copy(deep=True)
    audit = shadow_endpoint_execution_audit_v3(baseline, prices)
    assert audit["restricted_episode_count"] == 1
    assert audit["limitations"][0]["reason_codes"] == ["EXIT_OPEN_LIMIT_EXECUTION_UNPROVEN"]
    pd.testing.assert_frame_equal(baseline, before)
