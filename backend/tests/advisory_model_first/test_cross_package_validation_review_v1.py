from datetime import date
from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.cross_package_validation_review_v1 import audit_original_review_episodes, original_review_cohorts, project_original_review_scores
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_v1, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.economic_value_anchor_labels_v1 import value_anchor_shadow_inputs_v1
from backend.services.advisory_model_first.policy_episode_labels import build_policy_episode_labels


def test_original_review_projection_does_not_invent_top40_or_leg_ranks():
    roles, weights = {"lstm": "original_lstm", "fund": "original_fund"}, {"original_lstm": .7, "original_fund": .3}
    scores = [dict(rank=i+1, symbol=f"{i:06d}.SZ", score=.7*i+.3*2,
        component_scores={"original_lstm": dict(weight=.7, raw_score=i, normalized_score=i),
                          "original_fund": dict(weight=.3, raw_score=2, normalized_score=2)}) for i in range(25)]
    kwargs = dict(scores=scores, decision=date(2026,7,20), target=date(2026,7,21), component_roles=roles, terminal_weights=weights)
    candidates, reviews, raw = project_original_review_scores(**kwargs)
    assert len(candidates) == 20 and reviews.empty and len(raw) == 20
    assert candidates.selection_effective_rank.tolist() == list(range(1,21))
    assert candidates.candidate_group_size.eq(20).all()
    assert not any("leg_rank" in name for name in candidates.columns)
    scores[0]["component_scores"]["original_fund"]["weight"] = .4
    with pytest.raises(ValueError, match="weight"):
        project_original_review_scores(**kwargs)


def review_fixture():
    days = pd.bdate_range("2026-03-11",periods=9)
    prices = pd.DataFrame(dict(trade_date=days,instrument="000001.SZ",raw_open_cny=100.,raw_high_cny=102.,
        raw_low_cny=99.,raw_close_cny=101.,policy_price_per_raw_cny=2.,up_limit=110.,down_limit=90.,
        suspended=False,tradability_unknown=False))
    rankings = pd.DataFrame([{KEY[0]:d,KEY[1]:t,KEY[2]:f"{i:06d}.SZ", "selection_effective_rank":i,
                             "combined_score":1/i} for d,t in zip(days[:-1],days[1:]) for i in range(1,41)])
    market,cash,suspend,calendar = value_anchor_shadow_inputs_v1(prices=prices,calendar=days)
    kwargs = dict(rankings=rankings,daily=market,benchmark_daily=cash,suspend_rows=suspend,trading_calendar=calendar,
        policy=value_anchor_policy_v1(),policy_sha256=value_anchor_policy_sha256_v1(),cost_policy=COST,
        request_identity={"request_id":"test_original_policy"},candidate_decision_dates=[days[0]],candidate_depth=1)
    return days,prices,kwargs


def test_original_review_clocks_and_missing_rank_are_not_fixed5_proxies():
    days,prices,kwargs = review_fixture()
    episodes = build_policy_episode_labels(**kwargs).labels
    assert episodes.label_status.tolist() == ["MATURED"]
    longer = build_policy_episode_labels(**{**kwargs,"policy":replace(kwargs["policy"],time_stop_days=20)}).labels
    assert longer.label_status.tolist() == ["CENSORED_RIGHT_BOUNDARY"]
    gap = kwargs["rankings"].loc[kwargs["rankings"][KEY[0]].ne(days[2])]
    unknown = build_policy_episode_labels(**{**kwargs,"rankings":gap}).labels
    assert unknown.label_status.tolist() == ["DATA_UNAVAILABLE"]
    assert unknown.label_information_end.iloc[0] == days[3]
    settled = audit_original_review_episodes(episodes=episodes,prices=prices,calendar=days,cost_policy=COST)
    assert settled.settlement_status.tolist() == ["AVAILABLE"]
    assert np.isclose(settled.baseline_net_bps.iloc[0],(1-COST.sell_cost_bps/10000)/(1+COST.buy_cost_bps/10000)*10000-10000)


def test_original_review_exit_close_is_not_read_and_unknown_path_is_preserved():
    days,prices,kwargs = review_fixture()
    episodes = build_policy_episode_labels(**kwargs).labels
    endpoint = episodes.effective_exit_date.iloc[0]
    prices.loc[prices.trade_date.ge(endpoint),"raw_close_cny"] = np.nan
    assert audit_original_review_episodes(episodes=episodes,prices=prices,calendar=days,cost_policy=COST).settlement_status.tolist() == ["AVAILABLE"]
    prices.loc[prices.trade_date.eq(days[2]),"raw_close_cny"] = np.nan
    unavailable = audit_original_review_episodes(episodes=episodes,prices=prices,calendar=days,cost_policy=COST)
    assert unavailable.settlement_status.tolist() == ["UNKNOWN"]
    assert unavailable.settlement_reason.tolist() == ["HOLDING_CLOSE_UNKNOWN"]
    prices["suspended"] = prices.suspended.astype(object)
    prices.loc[0,"suspended"] = "false"
    with pytest.raises(ValueError,match="explicit booleans"):
        audit_original_review_episodes(episodes=episodes,prices=prices,calendar=days,cost_policy=COST)


def test_original_review_cohort_preserves_fixed_slots_unknowns_and_date_holes():
    candidates = pd.DataFrame([{KEY[0]:pd.Timestamp("2026-03-11"),KEY[1]:pd.Timestamp("2026-03-12"),KEY[2]:f"{i:06d}.SZ",
        "selection_effective_rank":i} for i in range(1,6)])
    labels = candidates.loc[:,KEY].assign(settlement_status="AVAILABLE",baseline_net_bps=[-100.,300.,100.,-200.,500.])
    predictions = candidates.loc[:,KEY].assign(status=["AVOID","AVOID","ACCEPTABLE","ACCEPTABLE","ACCEPTABLE"])
    _,daily = original_review_cohorts(predictions=predictions,labels=labels,candidates=candidates,original_days=["2026-03-11","2026-03-12"])
    assert daily.decision_date.tolist() == ["2026-03-11","2026-03-12"]
    assert daily.paired_known.tolist() == [True,False]
    assert daily.loc[0,["avoided_loss_bps","missed_profit_bps","increment_bps"]].tolist() == [20.,60.,-40.]
    assert daily.loc[0,"baseline_bps"] == 120. and daily.loc[0,"candidate_bps"] == 80.
    predictions.loc[4,"status"] = "UNKNOWN_LOCAL_SUPPORT"
    nodes,unknown = original_review_cohorts(predictions=predictions,labels=labels,candidates=candidates,original_days=["2026-03-11","2026-03-12"])
    assert not unknown.paired_known.any() and unknown.increment_bps.isna().all()
    assert np.isnan(nodes.loc[4,"candidate_net_bps"])  # Never cash or renormalized four slots.
    assert unknown.loc[0,"known_interventions"] == 0
    # A genuine short original list keeps unused slots as cash. It is not
    # dropped, supplemented by Top6, or renormalized to three positions.
    short_nodes,short_daily = original_review_cohorts(predictions=predictions.iloc[:3],labels=labels.iloc[:3],
        candidates=candidates.iloc[:3],original_days=["2026-03-11","2026-03-12"])
    assert len(short_nodes) == 3 and short_daily.paired_known.tolist() == [True,False]
    assert short_daily.loc[0,["baseline_bps","candidate_bps","increment_bps"]].tolist() == [60.,20.,-40.]


def test_review_parent_mutation_cannot_be_hidden_by_a_valid_child_receipt(tmp_path, monkeypatch):
    from backend.services.advisory_model_first import cross_package_validation_review_v1 as consumer
    from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import file_sha, publish_json
    package = "original"
    base = tmp_path/"review_transfer_v1"/package
    base.mkdir(parents=True)
    plan = dict(inventory_filename="inventory.json", decision_dates=["2026-03-11"])
    publish_json(tmp_path/"plan.json", plan)
    publish_json(tmp_path/"inventory.json", dict(packages=[dict(package_id=package,disposition="IN_MATRIX",manifest_sha256="a"*64)]))
    publish_json(tmp_path/"selection_source_catalog_v2.json", {"original":"source_catalog"})
    spec = dict(plan_sha256=file_sha(tmp_path/"plan.json"),package_id=package,package_manifest_sha256="a"*64,
        source_catalog_sha256=file_sha(tmp_path/"selection_source_catalog_v2.json"),
        original_D_axis=plan["decision_dates"],physical_fit_count=0,sealed_read=False)
    publish_json(base/"input_spec.json", spec)
    names = ("d_features.parquet", "candidates.parquet", "rankings.parquet", "raw_legs.parquet", "original_sources.json")
    for name in names:
        (base/name).write_bytes(b"frozen test member")
    receipt = dict(input_spec_sha256=file_sha(base/"input_spec.json"),outcomes_read=False,physical_fit_count=0,database_written=False,
        files={name:file_sha(base/name) for name in names})
    publish_json(base/"inputs_receipt.json",receipt)
    monkeypatch.setattr(consumer,"checked_plan",lambda root:plan)
    assert consumer.checked_review_inputs(tmp_path,package_id=package) == receipt
    (base/"raw_legs.parquet").write_bytes(b"changed after child receipt")
    with pytest.raises(ValueError,match="immutable parent"):
        consumer.checked_review_inputs(tmp_path,package_id=package)


def test_economic_transfer_point_support_and_action_match_original_boundaries():
    from backend.services.advisory_model_first.cross_package_validation_review_models_v1 import query_original_economic_nodes
    class FrozenHead:
        def __init__(self, values):
            self.values = values
            self.calls = 0
        def predict(self, rows, **kwargs):
            self.calls += 1
            return np.array(self.values)
    request = SimpleNamespace(feature_names=("D_feature","query_gap_bps"),gap_bin_width_bps=100.,
        minimum_bin_observations=20,minimum_bin_days=5,minimum_expected_net_value_bps=0.,downside_budget_bps=800.)
    fitted = SimpleNamespace(request=request, feature_bounds={"D_feature":(-1.,1.),"query_gap_bps":(-100.,100.)},
        price_support={0:dict(observation_count=20,decision_day_count=5,observed_min_gap_bps=0.,observed_max_gap_bps=90.)},
        return_model=FrozenHead([1.,0.,1.]),risk_model=FrozenHead([800.,0.,801.]))
    rows = pd.DataFrame(dict(D_feature=[0.,0.,0.,0.,np.nan,True],query_gap_bps=[0.,20.,80.,99.,0.,0.]))
    result = query_original_economic_nodes(fitted=fitted,rows=rows)
    assert result.status.tolist() == ["ACCEPTABLE","AVOID","AVOID","UNKNOWN_INPUT_OR_SUPPORT","UNKNOWN_INPUT_OR_SUPPORT","UNKNOWN_INPUT_OR_SUPPORT"]
    assert fitted.return_model.calls == fitted.risk_model.calls == 1
    assert result.loc[3:,"expected_net_bps"].isna().all()
    fitted.price_support[0]["decision_day_count"] = 4
    with pytest.raises(ValueError,match="price support"):
        query_original_economic_nodes(fitted=fitted,rows=rows)
