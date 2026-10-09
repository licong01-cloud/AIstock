import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import build_shared_daily_features
from backend.services.advisory_model_first.generic_daily_price_input_v1 import KEY


def sample():
    days = pd.bdate_range("2026-03-01", periods=25).date.tolist()
    d, t = days[19:21]
    rosters = pd.DataFrame([{KEY[0]: pd.Timestamp(d), KEY[1]: pd.Timestamp(t), KEY[2]: "000001.SZ",
        "selection_effective_rank": 1, "candidate_group_size": 1, "package_id": package}
        for package in ("a", "b")])
    rows = [{"trade_date": pd.Timestamp(day), "instrument": "000001.SZ", "adj_factor": 2.,
        "raw_open_cny": 10.+i, "raw_high_cny": 11.+i, "raw_low_cny": 9.+i,
        "raw_close_cny": 10.+i, "volume_hand": 100.}
        for i, day in enumerate(days)]
    index = pd.DataFrame({"trade_date": pd.to_datetime(days), "instrument": "000300.SH", "close": np.arange(100., 125.)})
    return rosters, days, pd.DataFrame(rows), index


def test_shared_daily_exact_original_roster_and_future_isolation():
    rosters, days, daily, index = sample()
    first, refs = build_shared_daily_features(rosters=rosters, calendar=days, daily=daily, index_daily=index)
    future = daily.trade_date.gt(pd.Timestamp(days[19]))
    daily.loc[future, ["raw_close_cny", "raw_low_cny", "adj_factor"]] = -999.
    index.loc[index.trade_date.gt(pd.Timestamp(days[19])), "close"] = -999.
    second, other_refs = build_shared_daily_features(rosters=rosters, calendar=days, daily=daily, index_daily=index)
    pd.testing.assert_frame_equal(first, second)
    pd.testing.assert_frame_equal(refs, other_refs)
    assert len(refs) == 1 and len(first) == 2
    assert first.ret_5.tolist() == pytest.approx([29./24.-1]*2)
    assert first.market_up_ratio.isna().all()
    assert first.package_id.tolist() == ["a", "b"]


def test_missing_daily_remains_unknown_and_duplicate_is_rejected():
    rosters, days, daily, index = sample()
    missing = daily.loc[daily.trade_date.ne(pd.Timestamp(days[17]))]
    result, _ = build_shared_daily_features(rosters=rosters, calendar=days, daily=missing, index_daily=index)
    assert len(result) == 2 and result.ret_5.isna().all()
    with pytest.raises(ValueError, match="duplicate"):
        build_shared_daily_features(rosters=rosters, calendar=days,
            daily=pd.concat([daily, daily.iloc[:1]]), index_daily=index)


def test_auxiliary_budget_is_exact_original_visible_requests_not_shared_superset():
    from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import original_key_daily_slice
    rosters, days, daily, _ = sample()
    original = rosters.loc[:, KEY].drop_duplicates()
    selected = original_key_daily_slice(roster=original, calendar=days, daily=daily)
    assert len(selected) == 20 and selected.trade_date.max() == pd.Timestamp(days[19])
    unrelated = daily.copy()
    unrelated.instrument = "000002.SZ"
    extended = original_key_daily_slice(roster=original, calendar=days, daily=pd.concat((daily, unrelated), ignore_index=True))
    pd.testing.assert_frame_equal(selected, extended)


def test_fixed5_slot_cash_unknown_attribution_and_single_cost():
    from backend.services.advisory_model_first.cross_package_validation_evaluation_v1 import account_original_fixed5_slots
    from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY, POLICY_SHA256
    d, t = pd.Timestamp("2026-03-11"), pd.Timestamp("2026-03-12")
    rows, predictions = [], []
    for i, gross in enumerate((.99, 1.01, 1.02, 1.03, 1.04), 1):
        identity = {KEY[0]: d, KEY[1]: t, KEY[2]: f'{i:06}.SZ', "package_id": "p", "policy_sha256": POLICY_SHA256}
        rows.append({**identity, "selection_effective_rank": i, "candidate_group_size": 50,
            "label_status": "AVAILABLE", "observed_gap_bps": 0., "gross_terminal_ratio": gross})
        state = "AVOID" if i <= 2 else "UNKNOWN_INPUT_OR_SUPPORT" if i == 3 else "ACCEPTABLE"
        predictions.append({**identity, "status": state, "expected_net_bps": -1. if i <= 2 else 10., "downside_q90_bps": 10.})
    labels, pred = pd.DataFrame(rows), pd.DataFrame(predictions)
    axis, episodes, attribution = account_original_fixed5_slots(labels=labels, predictions=pred,
        decision_dates=[str(d.date())], regimes=[dict(decision_date=str(d.date()), regime="UP_LOW_VOL")])
    net = np.array([10000*(gross*(1-POLICY['sell_bps']/10000)/(1+POLICY['buy_bps']/10000)-1) for gross in (.99, 1.01, 1.02, 1.03, 1.04)])
    assert axis.baseline_bps.iloc[0] == pytest.approx(net.mean())
    assert axis.model_bps.iloc[0] == pytest.approx(net[3:].sum()/5)
    assert attribution['known_avoided_loss_bps'] == pytest.approx(-net[0])
    assert attribution['known_missed_profit_bps'] == pytest.approx(net[1])
    assert attribution['unknown_cash_increment_bps'] == pytest.approx(-net[2])
    # Four known individual outcomes on an incomplete second D must not enter
    # the complete-cohort explanation or intervention support denominator.
    extra_labels,extra_pred = labels.copy(),pred.copy()
    for frame in (extra_labels,extra_pred):
        frame[KEY[0]],frame[KEY[1]] = d+pd.Timedelta(days=1),t+pd.Timedelta(days=1)
    extra_labels.loc[extra_labels.selection_effective_rank.eq(5),'label_status'] = 'UNKNOWN'
    two_axis,all_episodes,paired = account_original_fixed5_slots(labels=pd.concat((labels,extra_labels),ignore_index=True),
        predictions=pd.concat((pred,extra_pred),ignore_index=True),decision_dates=[str(d.date()),str((d+pd.Timedelta(days=1)).date())],
        regimes=[dict(decision_date=str(day.date()),regime='UP_LOW_VOL') for day in (d,d+pd.Timedelta(days=1))])
    assert len(two_axis) == 2 and len(all_episodes) == 10 and all_episodes.paired_cohort_known.sum() == 5
    assert paired['paired_count'] == 5 and paired['unpaired_known_episode_count'] == 4
    assert paired['total_increment_bps'] == pytest.approx(attribution['total_increment_bps'])
    assert paired['total_increment_bps'] == pytest.approx(two_axis.increment_bps.sum()*5)
    labels.loc[labels.selection_effective_rank.eq(5), 'label_status'] = 'UNKNOWN'
    unknown_axis, _, missing = account_original_fixed5_slots(labels=labels, predictions=pred,
        decision_dates=[str(d.date())], regimes=[dict(decision_date=str(d.date()), regime="UP_LOW_VOL")])
    assert unknown_axis.baseline_bps.iloc[0] is None and unknown_axis.increment_bps.iloc[0] is None
    assert missing['paired_count'] == 0 and missing['unpaired_known_episode_count'] == 4
    assert len(episodes) == 5


def test_settlement_checks_supplement_before_any_H_label_read(tmp_path,monkeypatch):
    from backend.services.advisory_model_first import cross_package_validation_evaluation_v1 as consumer
    from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import publish_json,file_sha
    from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY_SHA256
    publish_json(tmp_path/"plan.json",dict(study="original"))
    spec = dict(plan_sha256=file_sha(tmp_path/"plan.json"),policy_sha256=POLICY_SHA256,
        physical_fit_count=0,outcomes_read=False,units=[])
    publish_json(tmp_path/"fixed5_query_spec.json",spec)
    publish_json(tmp_path/"fixed5_query_spec_selection_context.json",{**spec,
        "units":[dict(model_id="supplement",arm="original",model_sha256="a"*64)]})
    monkeypatch.setattr(consumer,"checked_plan",lambda root:dict(settlement_cutoff="2026-09-04"))
    calls = []
    monkeypatch.setattr(consumer,"_extend_quotes",lambda *a,**kw:calls.append("H_READ_FORBIDDEN"))
    with pytest.raises(FileNotFoundError):
        consumer.settle_fixed5_transfer(tmp_path)
    assert calls == []


def test_canary_resume_runs_base_and_supplement_before_settlement_then_exit(tmp_path,monkeypatch):
    import json
    from backend.services.advisory_model_first import cross_package_validation_inputs_v1 as consumer
    from backend.services.advisory_model_first import cross_package_validation_evaluation_v1 as evaluation
    from backend.services.advisory_model_first import cross_package_validation_exit_v1 as exit_role
    from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import publish_json,file_sha
    parent,child = tmp_path/"parent",tmp_path/"parent"/"child"
    publish_json(parent/"plan.json",dict(study="original"))
    publish_json(parent/"fixed5_query_spec_selection_context.json",dict(original_source={"role":"entry"}))
    publish_json(parent/"exit_query_spec_v1.json",dict(original_source={"role":"exit"}))
    parent_plan = dict(decision_dates=["2026-03-11"])
    child_plan = {**parent_plan,"parent_plan_sha256":file_sha(parent/"plan.json"),
        "historical_original_receipt_created":False,"parent_other_role_outcomes_already_seen":True}
    monkeypatch.setattr(consumer,"checked_plan",lambda path:parent_plan if path == parent else child_plan)
    events = []
    monkeypatch.setattr(consumer,"prepare_daily_source",lambda **kw:events.append("D_DAILY"))
    monkeypatch.setattr(consumer,"prepare_auxiliary_features",lambda **kw:events.append("D_AUX"))
    monkeypatch.setattr(evaluation,"predict_fixed5_transfer",lambda *a,**kw:events.append("D_SUPPLEMENT" if kw.get("original_source") else "D_BASE"))
    monkeypatch.setattr(evaluation,"settle_fixed5_transfer",lambda *a,**kw:events.append("H_SETTLEMENT"))
    def evaluate(*a,**kw):
        events.append("EVALUATE")
        return {"status":"INSUFFICIENT"}
    monkeypatch.setattr(evaluation,"evaluate_fixed5_transfer",evaluate)
    monkeypatch.setattr(exit_role,"run_exit_transfer",lambda *a,**kw:events.append("ORIGINAL_EXIT"))
    result = consumer.run_current_canary_fixed5(parent,child_root=child,active_profile_path="readonly_profile")
    assert result == {"status":"INSUFFICIENT"}
    assert events == ["D_DAILY","D_AUX","D_BASE","D_SUPPLEMENT","H_SETTLEMENT","EVALUATE","ORIGINAL_EXIT"]
    assert json.loads((parent/"plan.json").read_text(encoding="utf-8")) == {"study":"original"}


def test_current_source_recovery_keeps_declared_dates_and_profile_and_no_early_labels(tmp_path,monkeypatch):
    from backend.services.advisory_model_first import cross_package_validation_inputs_v1 as consumer
    from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import publish_json,file_sha,CANARY_DATES
    parent = tmp_path/"study"
    publish_json(parent/"plan.json",dict(identity="original"))
    publish_json(parent/"inventory.json",dict(packages=[dict(package_id="p",disposition="IN_MATRIX")]))
    publish_json(tmp_path/"profile.json",dict(generation="unchanged"))
    plan = dict(inventory_filename="inventory.json",inventory_sha256=file_sha(parent/"inventory.json"))
    monkeypatch.setattr(consumer,"checked_plan",lambda root:plan)
    spec_path = parent/"canary"/"declared_attempt"/"spec.json"
    spec = dict(package_ids=["p"],parent_plan_sha256=file_sha(parent/"plan.json"),inventory_sha256=plan["inventory_sha256"],
        profile_path=str(tmp_path/"profile.json"),profile_sha256=file_sha(tmp_path/"profile.json"),
        decision_dates=[str(d) for d in CANARY_DATES],physical_fit_count=0,all_original_D_axis_retained=True,
        consumer_runtime="LOCAL_WSL_CPU_1_NO_GPU",other_role_outcomes_already_seen=True,
        source="CURRENT_DATABASE_NON_VINTAGE_NOT_HISTORICAL_CAPTURE")
    publish_json(spec_path,spec)
    assert consumer.checked_current_canary_spec(parent,spec_path=spec_path,active_profile_path=tmp_path/"profile.json")[0] == spec
    with pytest.raises(ValueError,match="no H labels"):
        consumer.checked_current_canary_spec(parent,spec_path=spec_path,active_profile_path=tmp_path/"profile.json",require_finished=True)
    (tmp_path/"profile.json").write_bytes(b'changed_generation')
    with pytest.raises(ValueError,match="identity"):
        consumer.checked_current_canary_spec(parent,spec_path=spec_path,active_profile_path=tmp_path/"profile.json")
