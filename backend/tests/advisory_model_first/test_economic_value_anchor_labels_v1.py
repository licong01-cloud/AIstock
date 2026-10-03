import copy

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1
from backend.services.advisory_model_first.economic_entry_labels import candidate_roster_sha256
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1, value_anchor_policy_v1
from backend.services.advisory_model_first.economic_value_anchor_labels_v1 import build_value_anchor_labels_v1, value_anchor_shadow_inputs_v1
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


@pytest.fixture
def scene():
    days = pd.bdate_range("2025-01-02", periods=12)
    rankings = pd.DataFrame([{"decision_as_of_trade_date": day, "target_trade_date": days[index+1],
        "instrument": f"{rank:06d}.SZ", "selection_effective_rank": rank, "combined_score": 100.-rank}
        for index, day in enumerate(days[:-1]) for rank in range(1, 41)])
    candidates = rankings.loc[rankings.decision_as_of_trade_date.eq(days[0]) & rankings.selection_effective_rank.le(20)].copy()
    policy = {"target_count": 5, "rank_enter_threshold": 5, "rank_exit_threshold": 40, "rank_exit_confirm_days": 2,
        "daily_replacement_budget": 5, "stop_loss_bps": 800, "take_profit_bps": 1800, "trailing_stop_bps": 700,
        "time_stop_days": 20, "take_profit_mode": "trailing", "entry_price_basis": "next_open_executable", "exit_price_basis": "next_open_executable"}
    identity = EconomicEntryInputIdentityV1(dataset_manifest_sha256="a"*64, request_id="original_frozen", request_sha256="b"*64,
        program_id="program", binding_version_id="binding", package_id="package", manifest_sha256="c"*64,
        selection_runtime_semantics_hash="d"*64, universe_identity_sha256="e"*64, candidate_roster_sha256=candidate_roster_sha256(candidates),
        price_coordinate_sha256="f"*64, price_source_sha256="1"*64, reference_source_sha256="2"*64,
        shadow_policy=policy, shadow_policy_sha256=canonical_json_sha256(policy), cost_policy=COST, cost_policy_sha256=COST.policy_sha256,
        source_evidence="RECOVERED_LIMITED", evidence_limitations=("original members not native",))
    prices = pd.DataFrame([{"trade_date": day, "instrument": f"{rank:06d}.SZ",
        "raw_open_cny": 10.+index*.1, "raw_close_cny": 10.+index*.1, "raw_high_cny": 10.2+index*.1, "raw_low_cny": 9.8+index*.1,
        "policy_price_per_raw_cny": 1., "up_limit": 15., "down_limit": 5., "suspended": False, "tradability_unknown": False,
        "source_sha256": identity.price_source_sha256, "price_coordinate_sha256": identity.price_coordinate_sha256}
        for index, day in enumerate(days) for rank in range(1, 41)])
    refs = candidates.copy()
    refs["target_reference_raw_cny"], refs["reference_visible_through"], refs["source_sha256"] = 10., days[0], identity.reference_source_sha256
    return dict(candidates=candidates, rankings=rankings, prices=prices, references=refs, calendar=days, parent_identity=identity)


def _one(labels):
    return labels.loc[labels.instrument.eq("000001.SZ")].iloc[0]


@pytest.mark.parametrize("exit_kind", ["time", "rank"])
def test_new_value_label_and_exit_do_not_depend_on_hypothetical_buy_price(scene, exit_kind):
    exit_index = 6 if exit_kind == "time" else 3
    if exit_kind == "rank":
        ranked = scene["rankings"]
        ranked.loc[ranked.decision_as_of_trade_date.ge(scene["calendar"][1]) & ranked.selection_effective_rank.eq(1), "instrument"] = "000041.SZ"
    before = _one(build_value_anchor_labels_v1(**scene))
    changed = copy.deepcopy(scene)
    entry = changed["prices"].trade_date.eq(scene["calendar"][1]) & changed["prices"].instrument.eq("000001.SZ")
    changed["prices"].loc[entry, "raw_open_cny"] = 12.
    changed["prices"].loc[entry, "raw_high_cny"] = 12.2
    after = _one(build_value_anchor_labels_v1(**changed))
    assert before.effective_exit_date == after.effective_exit_date == scene["calendar"][exit_index]
    assert before.gross_value_ratio == after.gross_value_ratio == pytest.approx(1.+exit_index*.01)
    assert before.path_min_value_ratio == after.path_min_value_ratio == pytest.approx(1.01)
    assert before.net_return_bps != after.net_return_bps
    assert after.parent_input_identity_sha256 == scene["parent_identity"].identity_sha256
    changed["prices"].loc[changed["prices"].trade_date.ge(scene["calendar"][exit_index]), "raw_close_cny"] = .001
    assert _one(build_value_anchor_labels_v1(**changed)).path_min_value_ratio == after.path_min_value_ratio


def test_equivalent_corporate_action_coordinate_preserves_value_and_path(scene):
    before = build_value_anchor_labels_v1(**scene)
    scaled = copy.deepcopy(scene)
    names = ["raw_open_cny", "raw_close_cny", "raw_high_cny", "raw_low_cny", "up_limit", "down_limit"]
    scaled["prices"][names] /= 2
    scaled["prices"]["policy_price_per_raw_cny"] = 2.
    scaled["references"]["target_reference_raw_cny"] /= 2
    after = build_value_anchor_labels_v1(**scaled)
    pd.testing.assert_frame_equal(before[["gross_value_ratio", "path_min_value_ratio", "effective_exit_date"]],
        after[["gross_value_ratio", "path_min_value_ratio", "effective_exit_date"]])


def test_suspension_and_limit_deferral_match_the_existing_portfolio(scene):
    price = scene["prices"]
    symbol = price.instrument.eq("000001.SZ")
    halt = symbol & price.trade_date.eq(scene["calendar"][3])
    price.loc[halt, "suspended"] = True
    price.loc[halt, ["raw_open_cny", "raw_close_cny"]] = None
    limited = symbol & price.trade_date.eq(scene["calendar"][7])
    price.loc[limited, ["raw_open_cny", "raw_close_cny", "raw_high_cny", "raw_low_cny", "down_limit"]] = 8.
    labels = build_value_anchor_labels_v1(**scene)
    row = _one(labels)
    assert len(labels) == 20 and row.value_label_status == "AVAILABLE"
    assert row.effective_exit_date == scene["calendar"][8] and row.path_min_value_ratio == pytest.approx(.8)
    market, cash, suspend, days = value_anchor_shadow_inputs_v1(prices=price, calendar=scene["calendar"])
    replay = replay_shadow_portfolio(rankings=scene["rankings"], daily=market, benchmark_daily=cash,
        suspend_rows=suspend, trading_calendar=days, policy=value_anchor_policy_v1(), policy_sha256=value_anchor_policy_sha256_v1(),
        cost_policy=COST, request_id="value_scene_parity", candidate_decision_dates=[days[0]])
    episode = replay.episodes.loc[replay.episodes.instrument.eq("000001.SZ")].iloc[0]
    assert episode.exit_trade_date == row.effective_exit_date and episode.exit_price == row.exit_price


@pytest.mark.parametrize("defect", ["ambiguous", "missing", "source", "roster", "future_reference"])
def test_normal_unknown_keeps_candidates_and_identity_contradictions_fail(scene, defect):
    price = scene["prices"]
    affected = price.instrument.eq("000001.SZ") & price.trade_date.eq(scene["calendar"][2])
    if defect == "ambiguous":
        price.loc[affected, "tradability_unknown"] = True
    elif defect == "missing":
        scene["prices"] = price.loc[~affected]
    elif defect == "source":
        price.loc[affected, "source_sha256"] = "3"*64
    elif defect == "roster":
        scene["candidates"].loc[scene["candidates"].instrument.eq("000001.SZ"), "instrument"] = "999999.SZ"
    else:
        scene["references"]["reference_visible_through"] = scene["calendar"][1]
    if defect in ("ambiguous", "missing"):
        labels = build_value_anchor_labels_v1(**scene)
        assert len(labels) == 20 and _one(labels).value_label_status == "UNKNOWN"
        assert pd.isna(_one(labels).gross_value_ratio)
    else:
        with pytest.raises(ValueError):
            build_value_anchor_labels_v1(**scene)
