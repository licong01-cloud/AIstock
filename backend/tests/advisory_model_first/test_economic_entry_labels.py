from __future__ import annotations

import copy

import pandas as pd
import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1
from backend.services.advisory_model_first.economic_entry_labels import (
    build_economic_entry_labels,
    candidate_roster_sha256,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


@pytest.fixture
def inputs():
    days = pd.bdate_range("2026-01-05", periods=4)
    policy = {
        "target_count": 5, "rank_enter_threshold": 5, "rank_exit_threshold": 40,
        "rank_exit_confirm_days": 2, "daily_replacement_budget": 5,
        "stop_loss_bps": 800, "take_profit_bps": 1800, "trailing_stop_bps": 700,
        "time_stop_days": 20, "take_profit_mode": "trailing",
        "entry_price_basis": "next_open_executable", "exit_price_basis": "next_open_executable",
    }
    candidates = pd.DataFrame([{
        "decision_as_of_trade_date": days[0], "target_trade_date": days[1],
        "instrument": "000001.SZ", "selection_effective_rank": 1,
    }])
    cost = AdvisoryPolicyCostV1(buy_cost_bps=10, sell_cost_bps=20)
    identity = EconomicEntryInputIdentityV1(
        dataset_manifest_sha256="a" * 64, request_id="frozen_request", request_sha256="b" * 64,
        program_id="program", binding_version_id="binding", package_id="package",
        manifest_sha256="c" * 64, selection_runtime_semantics_hash="d" * 64,
        universe_identity_sha256="e" * 64, candidate_roster_sha256=candidate_roster_sha256(candidates),
        price_coordinate_sha256="f" * 64, price_source_sha256="1" * 64,
        reference_source_sha256="2" * 64, shadow_policy=policy,
        shadow_policy_sha256=canonical_json_sha256(policy), cost_policy=cost,
        cost_policy_sha256=cost.policy_sha256,
        source_evidence="RECOVERED_LIMITED", evidence_limitations=("not a native receipt",),
    )
    net = (11 * .998 / (10 * 1.001) - 1) * 10000
    episodes = pd.DataFrame([{
        **identity.episode_identity(), **candidates.iloc[0].to_dict(),
        "selection_rank": 1, "episode_label_id": "episode_one", "label_status": "MATURED",
        "label_information_start": days[0], "label_information_end": days[3],
        "entry_trade_date": days[1], "effective_exit_date": days[3],
        "entry_price": 10, "exit_price": 11, "net_return_bps": net,
    }])
    prices = pd.DataFrame([{
        "trade_date": day, "instrument": "000001.SZ", "raw_open_cny": price,
        "raw_close_cny": 12 if index == 1 else price, "policy_price_per_raw_cny": 1,
        "suspended": False, "price_coordinate_sha256": identity.price_coordinate_sha256,
        "source_sha256": identity.price_source_sha256,
    } for index, (day, price) in enumerate(zip(days, [10., 10., 9., 11.], strict=True))])
    references = pd.DataFrame([{
        **candidates.iloc[0].to_dict(), "decision_raw_close_cny": 10, "target_reference_raw_cny": 10,
        "reference_visible_through": days[0], "source_sha256": identity.reference_source_sha256,
    }])
    return dict(candidates=candidates, episodes=episodes, prices=prices, references=references,
                trading_calendar=days, label_cutoff=days[-1], identity=identity)


def test_cost_once_true_cash_advantage_and_chronological_daily_risk(inputs):
    source = inputs["episodes"].copy(deep=True)
    label, = build_economic_entry_labels(**inputs)
    assert label.status == "AVAILABLE"
    assert label.entry_advantage_bps == pytest.approx((11 * .998 / (10 * 1.001) - 1) * 10000)
    assert label.skip_net_value_bps == 0
    assert label.daily_mark_max_drawdown_bps == pytest.approx(2500)  # 12 -> 9, not 10 -> 9.
    assert label.actual_gap_bps == 0
    assert not label.deployable
    pd.testing.assert_frame_equal(source, inputs["episodes"])
    # High/low and the exit day's later close are not observable policy risk marks.
    inputs["prices"]["raw_high_cny"] = 1e8
    inputs["prices"].loc[3, "raw_close_cny"] = .01
    assert build_economic_entry_labels(**inputs)[0].label_sha256 == label.label_sha256


@pytest.mark.parametrize("status", ["MATURED", "NOT_ENTERED_SUSPENDED"])
def test_ambiguous_execution_state_preserves_candidate_without_fake_skip(inputs, status):
    inputs["episodes"].loc[0, "label_status"] = status
    inputs["prices"]["tradability_unknown"] = False
    inputs["prices"].loc[1, "tradability_unknown"] = True
    label, = build_economic_entry_labels(**inputs)
    assert label.status == "DATA_UNAVAILABLE" and label.entry_advantage_bps is None
    assert label.original_label_status == status
    assert "TRADABILITY_UNKNOWN" in label.reason_code


def test_corporate_action_reference_and_raw_to_policy_parity(inputs):
    inputs["prices"].loc[1:, ["raw_open_cny", "raw_close_cny"]] /= 2
    inputs["prices"].loc[1:, "policy_price_per_raw_cny"] = 2
    inputs["references"].loc[0, "target_reference_raw_cny"] = 5
    label, = build_economic_entry_labels(**inputs)
    assert label.actual_open_raw_cny == 5
    assert label.actual_gap_bps == 0  # Not a spurious 50% bearish opening gap.
    assert label.daily_mark_max_drawdown_bps == pytest.approx(2500)


@pytest.mark.parametrize("status", ["NOT_ENTERED_SUSPENDED", "NOT_ENTERED_LIMIT_UP",
                                   "NOT_ENTERED_MISSING_OPEN", "DATA_UNAVAILABLE", "CENSORED_RIGHT_BOUNDARY"])
def test_normal_non_entered_or_unmature_candidates_are_retained_without_targets(inputs, status):
    inputs["episodes"].loc[0, "label_status"] = status
    inputs["episodes"].loc[0, "net_return_bps"] = None
    inputs["prices"] = inputs["prices"].iloc[:0]
    label, = build_economic_entry_labels(**inputs)
    assert label.original_label_status == status
    if status == "NOT_ENTERED_MISSING_OPEN":
        assert label.status == "DATA_UNAVAILABLE"
    assert label.enter_net_value_bps is None and label.daily_mark_max_drawdown_bps is None
    assert label.instrument == "000001.SZ"


@pytest.mark.parametrize("defect", ["reference_missing", "mark_missing", "mark_invalid", "cutoff"])
def test_missing_or_post_cutoff_label_is_not_zero_loss(inputs, defect):
    if defect == "reference_missing":
        inputs["references"] = inputs["references"].iloc[:0]
    elif defect == "mark_missing":
        inputs["prices"] = inputs["prices"].drop(index=2)
    elif defect == "mark_invalid":
        inputs["prices"].loc[2, "raw_close_cny"] = float("nan")
    else:
        inputs["label_cutoff"] = inputs["trading_calendar"][2]
    label, = build_economic_entry_labels(**inputs)
    assert label.status in {"DATA_UNAVAILABLE", "CENSORED_RIGHT_BOUNDARY"}
    assert label.entry_advantage_bps is None and label.daily_mark_max_drawdown_bps is None


def test_normal_suspension_uses_explicit_carried_adjusted_mark(inputs):
    inputs["prices"].loc[2, "suspended"] = True
    inputs["prices"].loc[2, ["raw_open_cny", "raw_close_cny", "policy_price_per_raw_cny"]] = float("nan")
    label, = build_economic_entry_labels(**inputs)
    assert label.status == "AVAILABLE"
    assert label.suspension_carried_mark_count == 1
    assert label.daily_mark_max_drawdown_bps == pytest.approx((1 - 11 / 12) * 10000)


@pytest.mark.parametrize("defect, reason", [
    ("identity", "ADVISORY_ECONOMIC_LABEL_INPUT_MISMATCH"),
    ("roster", "ADVISORY_ECONOMIC_LABEL_INPUT_MISMATCH"),
    ("price_source", "ADVISORY_ECONOMIC_LABEL_INPUT_MISMATCH"),
    ("reference_future", "ADVISORY_ECONOMIC_REFERENCE_PIT"),
    ("price_parity", "ADVISORY_ECONOMIC_PRICE_PARITY"),
    ("cost_twice", "ADVISORY_ECONOMIC_COST_PARITY"),
    ("suspended_entry", "ADVISORY_ECONOMIC_LABEL_INPUT_MISMATCH"),
    ("unknown_status", "ADVISORY_ECONOMIC_LABEL_INPUT_MISMATCH"),
    ("clock", "ADVISORY_ECONOMIC_LABEL_INPUT_MISMATCH"),
    ("duplicate", "ADVISORY_ECONOMIC_LABEL_INPUT_MISMATCH"),
    ("suspension_contradiction", "ADVISORY_ECONOMIC_LABEL_INPUT_MISMATCH"),
])
def test_contradictions_fail_closed(inputs, defect, reason):
    if defect == "identity":
        inputs["episodes"].loc[0, "package_id"] = "another_package"
    elif defect == "roster":
        inputs["candidates"].loc[0, "selection_effective_rank"] = 2
    elif defect == "price_source":
        inputs["prices"].loc[1, "source_sha256"] = "9" * 64
    elif defect == "reference_future":
        inputs["references"].loc[0, "reference_visible_through"] = inputs["trading_calendar"][1]
    elif defect == "price_parity":
        inputs["prices"].loc[1, "policy_price_per_raw_cny"] = 2
    elif defect == "cost_twice":
        inputs["episodes"].loc[0, "net_return_bps"] -= 30
    elif defect == "suspended_entry":
        inputs["prices"].loc[1, "suspended"] = True
    elif defect == "unknown_status":
        inputs["episodes"].loc[0, "label_status"] = "SUCCESS_OR_WHATEVER"
    elif defect == "clock":
        inputs["episodes"].loc[0, "effective_exit_date"] = inputs["trading_calendar"][1]
    elif defect == "suspension_contradiction":
        inputs["episodes"].loc[0, "label_status"] = "NOT_ENTERED_SUSPENDED"
    else:
        inputs["prices"] = pd.concat([inputs["prices"], inputs["prices"].iloc[[0]]])
    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_economic_entry_labels(**inputs)
    assert caught.value.reason_code == reason


def test_unverified_cost_policy_and_recovered_identity_do_not_validate(inputs):
    payload = inputs["identity"].model_dump()
    for changes in ({"cost_policy_sha256": "9" * 64}, {"evidence_limitations": ()},
                    {"shadow_policy_sha256": "9" * 64}):
        with pytest.raises(ValidationError):
            EconomicEntryInputIdentityV1(**{**copy.deepcopy(payload), **changes})


def test_duplicate_rank_or_nested_policy_mutation_cannot_bypass_identity(inputs):
    candidates = pd.concat([inputs["candidates"], inputs["candidates"].assign(instrument="000002.SZ")])
    with pytest.raises(AdvisoryModelFirstError, match="ranks are not unique"):
        candidate_roster_sha256(candidates)
    inputs["identity"].shadow_policy["stop_loss_bps"] = 999
    with pytest.raises(ValidationError, match="shadow policy hash mismatch"):
        build_economic_entry_labels(**inputs)
