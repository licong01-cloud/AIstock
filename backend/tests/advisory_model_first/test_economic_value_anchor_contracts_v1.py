from dataclasses import replace

import pytest

from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import (
    ValueAnchorEstimateV1, ValueAnchorGapSupportV1, value_anchor_policy_sha256_v1, value_anchor_policy_v1,
)


def test_scenario_is_independent_and_immutable_not_a_production_policy_edit():
    policy = value_anchor_policy_v1()
    assert (policy.stop_loss_bps, policy.take_profit_bps, policy.trailing_stop_bps) == (0, 0, 0)
    assert (policy.rank_exit_threshold, policy.rank_exit_confirm_days, policy.time_stop_days) == (40, 2, 5)
    assert replace(policy, time_stop_days=20) != value_anchor_policy_v1()
    assert len(value_anchor_policy_sha256_v1()) == 64


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True, 0., -1.])
def test_invalid_estimate_is_not_an_unknown_success(value):
    with pytest.raises(ValueError):
        ValueAnchorEstimateV1(value, 1.)


def test_support_holes_are_immutable_and_not_merged():
    support = ValueAnchorGapSupportV1(((-200., -100.), (100., 200.)))
    assert support.contains(-150.) and not support.contains(0.)
    with pytest.raises(ValueError):
        ValueAnchorGapSupportV1(((-200., 100.), (0., 200.)))
