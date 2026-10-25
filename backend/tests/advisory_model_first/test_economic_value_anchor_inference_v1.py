import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, ValueAnchorEstimateV1, ValueAnchorGapSupportV1
from backend.services.advisory_model_first.economic_value_anchor_inference_v1 import (
    build_value_anchor_gap_support_v1, build_value_anchor_price_set_v1, evaluate_value_anchor_price_v1,
)


def test_cost_once_and_lower_price_is_only_a_supported_valuation_not_a_fill():
    args = dict(estimate=ValueAnchorEstimateV1(1.03, .95), reference_cny=10., support=ValueAnchorGapSupportV1(((-1000., 1000.),)))
    low = evaluate_value_anchor_price_v1(price_cny=10., **args)
    high = evaluate_value_anchor_price_v1(price_cny=10.5, **args)
    assert low.expected_net_bps == pytest.approx((1.03*(1-COST.sell_cost_bps/10000)/(1+COST.buy_cost_bps/10000)-1)*10000)
    assert low.downside_q90_bps == pytest.approx((1-.95*(1-COST.sell_cost_bps/10000)/(1+COST.buy_cost_bps/10000))*10000)
    assert low.status == "ACCEPTABLE" and high.status == "AVOID"
    assert high.downside_q90_bps > low.downside_q90_bps
    assert evaluate_value_anchor_price_v1(price_cny=8., **args).status == "UNKNOWN_OUT_OF_SUPPORT"


def test_strict_profit_root_ticks_and_support_holes():
    args = dict(estimate=ValueAnchorEstimateV1(1., 1.), reference_cny=10.,
        support=ValueAnchorGapSupportV1(((-200., -150.), (-100., 100.))), legal_low_cny=9.8, legal_high_cny=10.1)
    result = build_value_anchor_price_set_v1(**args)
    assert result.intervals_cny == ((9.8, 9.85), (9.9, 9.99))
    assert not result.deployable and result.evidence_use == "NAVIGATION_ONLY"
    missing = build_value_anchor_price_set_v1(**{**args, "estimate": None})
    assert missing.status == "UNKNOWN_MODEL_VALUE" and not missing.intervals_cny
    with pytest.raises(ValueError):
        build_value_anchor_price_set_v1(**{**args, "tick_cny": .000001})


def test_support_is_unlabelled_train_only_with_fixed_tail_trim_and_population():
    records = []
    for day in pd.bdate_range("2025-01-01", periods=5):
        for index in range(12):
            records.append({"decision_as_of_trade_date": day, "target_trade_date": day+pd.Timedelta(days=1),
                "instrument": str(index), "split": "train", "actual_gap_bps": -90.+index if index < 6 else 110.+index,
                **dict.fromkeys(D_FEATURES, 1.), "future_profit": 1e6})
    frame = pd.DataFrame(records)
    support = build_value_anchor_gap_support_v1(frame)
    assert len(support.intervals_bps) == 2 and not support.contains(0.)
    frame["future_profit"] = -1e6
    assert build_value_anchor_gap_support_v1(frame) == support
    frame.loc[0, "split"] = "test"
    with pytest.raises(ValueError):
        build_value_anchor_gap_support_v1(frame)
