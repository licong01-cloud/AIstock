import copy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import group_centered_rank_5td_study_v1 as m


@pytest.fixture
def packet():
    calendar = pd.bdate_range("2024-01-01", periods=28)
    sources = [dict(package_id=p, manifest_sha256=p*64, run_id=p+"-run") for p in ("a", "b")]
    rows, days = [], []
    for s in sources:
        for i,d in enumerate(calendar[:22]):
            t = calendar[i+1]
            h = calendar[i+5]
            days.append(dict(package_id=s["package_id"], decision_date=d, target_date=t, horizon_end=h, original_candidates=20))
            for k in range(1,21):
                y = float(k*10 + i*2 - 120)
                terminal = (1+y/10000)*(1+m.POLICY["buy_bps"]/10000)/(1-m.POLICY["sell_bps"]/10000)
                rows.append(dict(s, instrument=f"{k:06d}.SZ", decision_as_of_trade_date=d, target_trade_date=t,
                    selection_effective_rank=k, candidate_group_size=20,
                    **{n:(k+1)/(j+2)+i*.1 for j,n in enumerate(m.FEATURES)},
                    valuation_status="AVAILABLE", hypothetical_liquidation_net_bps=y,
                    valuation_gross_terminal_ratio=terminal, observed_gap_bps=0., exit_execution_status="EXECUTABLE",
                    valuation_policy_sha256=m.VALUATION_POLICY_SHA256, label_information_end=h))
    return pd.DataFrame(rows),pd.DataFrame(days),calendar,sources


def fit(rows):
    return m.train_models(rows, train_start="2024-01-01", train_end="2024-01-19")


def test_training_clock_dedup_centering_and_future_poison(packet):
    rows,_,_,_ = packet
    base = fit(rows)
    unique = rows.drop_duplicates(list(m.KEY))
    assert fit(unique) == base
    future = rows.copy()
    future.loc[future.label_information_end.gt("2024-01-19"), [*m.FEATURES,"hypothetical_liquidation_net_bps"]] = "poison"
    assert fit(future) == base
    shifted = rows.copy()
    shift = shifted[m.KEY[0]].rank(method="dense")*100
    shifted["hypothetical_liquidation_net_bps"] += shift
    shifted["valuation_gross_terminal_ratio"] = (1+shifted.hypothetical_liquidation_net_bps/10000)*(1+m.POLICY["buy_bps"]/10000)/(1-m.POLICY["sell_bps"]/10000)
    alternate = fit(shifted)
    np.testing.assert_allclose(alternate["models"]["centered"]["coef"],base["models"]["centered"]["coef"],atol=1e-10)
    assert not np.allclose(alternate["models"]["absolute"]["coef"],base["models"]["absolute"]["coef"])
    bad = rows.copy()
    bad.loc[0,"ret_1"] += 1
    with pytest.raises(ValueError,match="contradictory"):
        fit(bad)


def test_prediction_keeps_unknown_slots_and_never_uses_outcomes(packet):
    rows,_,_,_ = packet
    fitted = fit(rows)
    inputs = rows.loc[:,m.INPUTS].copy()
    inputs.loc[inputs.selection_effective_rank.eq(3), m.FEATURES] = np.nan
    prediction = m.predict_ranks(inputs,fitted)
    pinned = prediction.status.eq("NO_RANK_ADVICE")
    assert prediction.loc[pinned,"centered_rank"].eq(3).all()
    assert prediction.loc[pinned,"centered_score"].isna().all()
    assert prediction.groupby(["package_id",m.KEY[0]]).size().eq(20).all()
    poison = inputs.assign(hypothetical_liquidation_net_bps="bad",valuation_status="FUTURE")
    pd.testing.assert_frame_equal(m.predict_ranks(poison,fitted),prediction)
    tied = copy.deepcopy(fitted)
    for model in tied["models"].values():
        model["coef"] = [0.]*18
    pp = m.predict_ranks(inputs,tied)
    assert pp.centered_rank.equals(pp.baseline_rank)


def test_native_roster_and_date_identity(packet):
    rows,days,calendar,sources = packet
    m.validate_roster(rows,days,calendar,sources,"2024-01-01","2024-01-30")
    with pytest.raises(ValueError,match="missing original"):
        m.validate_roster(rows,days.iloc[1:],calendar,sources,"2024-01-01","2024-01-30")
    bad = rows.copy()
    bad.loc[0,"run_id"] = "wrong"
    with pytest.raises(ValueError,match="manifest/run"):
        m.validate_roster(bad,days,calendar,sources,"2024-01-01","2024-01-30")
    bad = days.copy()
    bad.loc[0,"horizon_end"] = calendar[6]
    with pytest.raises(ValueError,match="D/T/H"):
        m.validate_roster(rows,bad,calendar,sources,"2024-01-01","2024-01-30")


def test_evaluation_cash_unknown_costs_and_attribution(packet):
    rows,days,_,_ = packet
    predictions = m.predict_ranks(rows,fit(rows))
    result = m.evaluate_ranks(rows,days,predictions,evaluation_end="2024-02-07")
    assert result["statistics"]["baseline"]["mean_increment_bps"] > 0
    assert result["attribution"]["baseline"]["net_increment_bps"] == pytest.approx(result["statistics"]["baseline"]["mean_increment_bps"])
    changed = rows.copy()
    hit = changed[m.KEY[0]].eq(pd.Timestamp("2024-01-01")) & changed.selection_effective_rank.eq(20)
    changed.loc[hit,"valuation_status"] = "CASH_ENTRY_NOT_EXECUTABLE"
    cash = m.evaluate_ranks(changed,days,predictions,evaluation_end="2024-02-07")
    assert cash["slots"]["centered"]["unavailable_entry"] == 2
    changed.loc[hit,"valuation_status"] = "UNKNOWN_PRICE"
    unknown = m.evaluate_ranks(changed,days,predictions,evaluation_end="2024-02-07")
    assert unknown["panels"]["centered"][0] is None
    assert unknown["statistics"]["baseline"]["simultaneous_95_bonferroni_bps"] is None
    assert unknown["fixed_ten_slot_mean_bps"]["centered"] is None
    bad = rows.copy()
    bad.loc[0,"hypothetical_liquidation_net_bps"] += 1
    with pytest.raises(ValueError,match="cost"):
        m.evaluate_ranks(bad,days,predictions,evaluation_end="2024-02-07")
    immature = m.evaluate_ranks(rows,days,predictions,evaluation_end="2024-01-30")
    assert immature["mature_days"] < immature["original_days"]
    assert immature["slots"]["baseline"]["mature_selected"] < immature["slots"]["baseline"]["original_selected"]


def test_unknown_training_feature_or_nonfinite_input_fails(packet):
    rows,_,_,_ = packet
    bad = rows.copy()
    bad["ret_1"] = np.nan
    with pytest.raises(ValueError,match="entire training"):
        fit(bad)
    bad["ret_1"] = np.inf
    with pytest.raises(ValueError,match="nonfinite"):
        fit(bad)


def test_native_stages_exact_retry_and_source_drift(packet,tmp_path,monkeypatch):
    original_train = m.train_models
    rows,days,calendar,sources = packet
    source,output = tmp_path/"source",tmp_path/"result"
    plan = dict(sources=sources,train_start="2024-01-01",train_end="2024-01-19",
                evaluation_start="2024-01-22",evaluation_end="2024-01-30")
    digest = m.sha(plan)
    m.publish_stage(study_root=source,stage="preregistered",plan_sha256=digest,parent_sha256=None,
                    artifacts={"plan.json":m._json_bytes(plan)})
    parent = m.load_json(source/"preregistered/manifest.json")["stage_sha256"]
    m.publish_stage(study_root=source,stage="prepared",plan_sha256=digest,parent_sha256=parent,
        artifacts={"rows.parquet":m.parquet_bytes(rows),"days.parquet":m.parquet_bytes(days),
                   "calendar.json":m._json_bytes([d.date().isoformat() for d in calendar])})
    prepared = m.load_json(source/"prepared/manifest.json")["stage_sha256"]
    args = dict(source_root=source,source_plan_sha256=digest,source_prepared_sha256=prepared,
                output_root=output,source_commit="a"*40)
    first = m.run_study(**args)
    assert not first["exact_retry"]
    def no_fit(*args,**kwargs):
        raise AssertionError("exact retry must not refit")
    monkeypatch.setattr(m,"train_models",no_fit)
    retry = m.run_study(**args)
    assert retry["exact_retry"] and retry["plan_sha256"] == first["plan_sha256"]
    assert retry["physical_fit_count"] == 2
    monkeypatch.setattr(m,"train_models",original_train)
    def interrupted_fit(*args,**kwargs):
        raise RuntimeError("simulated fit interruption")
    monkeypatch.setattr(m.Ridge,"fit",interrupted_fit)
    interrupted = dict(args,output_root=tmp_path/"interrupted")
    with pytest.raises(RuntimeError,match="interruption"):
        m.run_study(**interrupted)
    with pytest.raises(ValueError,match="partial fit"):
        m.run_study(**interrupted)
    with (source/"prepared/rows.parquet").open("ab") as stream:
        stream.write(b"drift")
    with pytest.raises(Exception,match="hash mismatch"):
        m.run_study(**args)
