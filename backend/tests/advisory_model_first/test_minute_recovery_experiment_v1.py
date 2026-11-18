"""Paired learning, financial clock projection, full-axis statistics and fit accounting."""
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import minute_recovery_experiment_v1 as core
from backend.services.advisory_model_first import minute_recovery_pipeline_v1 as pipe
from backend.services.advisory_model_first import minute_recovery_inputs_v1 as inp
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE


def population():
    rows = []
    for j,d in enumerate(pd.to_datetime(["2024-07-04", "2024-07-05", "2025-04-01"])):
        for rank in range(1, 51):
            y = float(100-rank*2+j)
            f = {n:np.sin(rank*(i+1)/50)+j*.01 for i,n in enumerate(core.features.FEATURES)}
            rows.append(dict(package_id=inp.PACKAGE, manifest_sha256=inp.MANIFEST, run_id=inp.RUN,
                source_id=inp.SOURCE_ID, decision_as_of_trade_date=d,
                target_trade_date=d+pd.offsets.BDay(1), instrument=f"{rank:06d}.SZ",
                selection_effective_rank=rank, candidate_group_size=50, parent_score=float(51-rank),
                **f, model_support=True, recovery_status="RECOVERED", label_information_end=d+pd.offsets.BDay(5),
                valuation_status="AVAILABLE", hypothetical_liquidation_net_bps=y,
                valuation_gross_terminal_ratio=(1+y/10000)*(1+.95/10000)/(1-5.95/10000),
                observed_gap_bps=0., exit_execution_status="NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF",
                valuation_policy_sha256=core.VALUATION_POLICY_SHA256))
    return pd.DataFrame(rows)


def test_time_projection_paired_encoding_and_preserved_unknown_slots():
    f = population()
    f.loc[f.selection_effective_rank.eq(3), "ret_1"] = np.nan
    a = core.train(f, "2025-03-31", "matched")
    b = core.train(f, "2025-03-31", "candidate")
    pipe.verify_common_packets(a,b)
    poison = f.copy()
    poison.loc[poison[inp.DAY].gt("2025-03-31"), ["hypothetical_liquidation_net_bps", "ret_1"]] = np.inf
    assert a == core.train(poison, "2025-03-31", "matched")
    immature = poison[inp.DAY].eq("2024-07-04") & poison.selection_effective_rank.eq(1)
    poison.loc[immature, "label_information_end"] = pd.Timestamp("2025-04-09")
    poison.loc[immature, "hypothetical_liquidation_net_bps"] = np.inf
    assert core.train(poison, "2025-03-31", "matched") == core.train(f.loc[~immature], "2025-03-31", "matched")
    target = f.loc[f[inp.DAY].eq("2025-04-01")].reset_index(drop=True)
    target.loc[0, list(core.features.NEW)] = np.nan
    target.loc[0,"model_support"] = False
    p,q = core.predict(target,a),core.predict(target,b)
    assert p.no_rank_advice.equals(q.no_rank_advice)
    assert p.loc[0,"advisory_rank"] == 1 and np.isnan(p.loc[0,"predicted_relative_net_bps"])
    assert p.selected.sum() == 10 and sorted(p.advisory_rank) == list(range(1,51))
    assert p[list(core.IDENTITY)].equals(target[list(core.IDENTITY)])
    bad = dict(a, scale=[0.]*len(a["fields"]))
    with pytest.raises(ValueError):
        core.predict(target,bad)


def test_all_missing_columns_train_only_and_same_base_params():
    f = population()
    f[core.features.MATCHED[10]] = np.nan
    a,b = (core.train(f,"2025-03-31",arm) for arm in ("matched","candidate"))
    pipe.verify_common_packets(a,b)
    assert core.features.MATCHED[10] in a["inactive_fields"]
    b["median"][0] += 1
    with pytest.raises(ValueError,match="encoding"):
        pipe.verify_common_packets(a,b)


def test_claims_are_bounded_durable_and_not_retried(tmp_path):
    s = pipe.Study(tmp_path, {"fixed":True}, inp.Budget())
    s.claim("fold1_matched")
    with pytest.raises(ValueError,match="claimed"):
        s.claim("fold1_matched")
    s.claim("fold1_matched","a"*64)
    s.claim("fold1_matched","a"*64)
    with pytest.raises(ValueError,match="identity"):
        s.claim("fold1_matched","b"*64)
    for i in range(1,10):
        for arm in ("matched","candidate"):
            if (i,arm)!=(1,"matched"):
                s.claim(f"fold{i}_{arm}")
    assert len(list((tmp_path/"fit_claims").glob("*_started.json"))) == 18
    with pytest.raises(ValueError):
        s.claim("final_candidate")


def test_financial_projection_reuses_labels_repairs_only_stale_mature(tmp_path, monkeypatch):
    f = population().loc[lambda x:x[inp.DAY].eq("2024-07-04")].reset_index(drop=True)
    cal = pd.bdate_range("2024-07-04",periods=9)
    finance = f[[*inp.KEY,*FINANCE]].copy()
    finance.loc[0,["valuation_status","exit_execution_status"]] = "IMMATURE"
    finance.loc[0,"hypothetical_liquidation_net_bps"] = np.nan
    path = tmp_path/"finance.parquet"
    finance.to_parquet(path,index=False)
    empty = tmp_path/"empty.parquet"
    finance.iloc[:0].to_parquet(empty,index=False)
    calls = []
    def repair(dataset,rows,cutoff,budget):
        calls.append(rows.instrument.tolist())
        return f.loc[f.instrument.isin(rows.instrument),[*inp.KEY,*FINANCE]], {"sql_count":0}
    monkeypatch.setattr(pipe,"finance_inputs",repair)
    reader = pipe.FinanceReader(SimpleNamespace(calendar=cal),{"finance_files":{str(path):{},str(empty):{}}},inp.Budget())
    r,receipt = reader.read(f,cal[5])
    assert calls == [[f.instrument.iloc[0]]] and receipt["stale_immature_repaired"] == 1
    assert r.valuation_status.eq("AVAILABLE").all()
    poison = finance.copy()
    poison["hypothetical_liquidation_net_bps"] = np.inf
    poison.to_parquet(path,index=False)
    r,receipt = reader.read(f,cal[3])
    assert r.valuation_status.eq("IMMATURE").all() and r.hypothetical_liquidation_net_bps.isna().all()
    assert receipt["decoded_mature_rows"] == 0
    with pytest.raises(ValueError,match="cutoff"):
        reader.read(f,"2026-09-01")


def panel():
    days = pd.bdate_range("2025-04-01","2026-08-28")
    rows = []
    for d in days:
        fold = next(i for i,(_,first,last) in enumerate(core.FOLDS,1) if pd.Timestamp(first)<=d<=pd.Timestamp(last))
        rows.append(dict(decision_date=d,fold=fold,mature=True,candidate_net_bps=200.,native_net_bps=0.,
            matched_net_bps=50.,delta_native_bps=200.,delta_matched_bps=150.,intervention_count=3,
            candidate_unknown=0,baseline_unknown=0,matched_unknown=0,
            avoided_loss=0.,missed_profit=0.,added_profit=200.,added_loss=0.))
    return pd.DataFrame(rows)


def test_date_axis_bootstrap_and_all_nine_fold_verdict():
    p = panel()
    p.loc[5,["candidate_net_bps","delta_native_bps","delta_matched_bps"]] = np.nan
    r = core.summarize(p)
    assert r["classification"] == "SUPPORTED_FOR_NEW_CONFIRMATION_DESIGN"
    assert r["overall"]["versus_native"]["original_days"] == len(p)
    assert r["overall"]["versus_native"]["paired_days"] == len(p)-1
    assert r["windows"]["development"]["versus_native"]["ci95_bps"] == [200.,200.]
    p.loc[p.decision_date.ge("2025-10-01"),["candidate_net_bps","delta_native_bps"]] = -10.
    assert core.summarize(p)["classification"] == "CLOSED_NEGATIVE_EXACT_CANDIDATE"
    p.loc[:,["candidate_net_bps","delta_native_bps"]] = 20.
    assert core.summarize(p)["classification"] == "INCONCLUSIVE_NO_PROMOTION_SUPPORT"
    assert core.paired_interval([np.nan])["ci95_bps"] is None
    with pytest.raises(ValueError):
        core.summarize(p[p.fold.ne(9)])


def test_comparison_preserves_unknown_and_cash_and_fee_contract():
    f=population()
    a,b=(core.train(f,"2025-03-31",arm) for arm in ("matched","candidate"))
    target=f[f[inp.DAY].eq("2025-04-01")].reset_index(drop=True)
    p,q=core.predict(target,a),core.predict(target,b)
    finance=target[[*inp.KEY,*FINANCE]].copy()
    cash=finance.index[finance.instrument.eq(target.instrument.iloc[0])]
    finance.loc[cash,"valuation_status"]="CASH_ENTRY_NOT_EXECUTABLE"
    finance.loc[cash,"hypothetical_liquidation_net_bps"]=0.
    finance.loc[cash,"valuation_gross_terminal_ratio"]=np.nan
    finance.loc[cash,"observed_gap_bps"]=np.nan
    finance.loc[cash,"exit_execution_status"]="CASH_NO_ENTRY"
    result=core.comparison(q,p,finance)
    assert result.mature.all()
    finance.loc[1,"valuation_status"]="UNKNOWN_PRICE"
    finance.loc[1,"hypothetical_liquidation_net_bps"]=np.nan
    # Unknown value must never be silently converted to zero.
    assert not core.comparison(q,p,finance).candidate_net_bps.fillna(0).equals(result.candidate_net_bps)


def test_cli_is_routed_and_has_no_extra_trials():
    from scripts.ci_change_classifier import classify_changed_files
    r=classify_changed_files(["scripts/advisory_minute_recovery_experiment.py"], repo_root=Path(__file__).resolve().parents[3])
    assert r["backend_sessions"] == ["advisory_modeling_backend"] and not r["unmapped_code_files"]
    assert core.CONTRACT["max_fits"] == len(core.FOLDS)*2 == 18
    json.loads(json.dumps(core.CONTRACT,allow_nan=False))


def test_all_forecasts_published_before_eval_and_exact_retry_never_refits(tmp_path,monkeypatch):
    template=population().iloc[:50].copy()
    calendar=pd.bdate_range("2024-07-01","2026-08-31")
    dates=[pd.Timestamp("2024-07-04"),pd.Timestamp("2024-07-05")]
    dates += [calendar[calendar>=first][0] for _,first,_ in core.FOLDS]
    frames=[]
    for d in dates:
        f=template.copy()
        f[inp.DAY]=d
        f[inp.TARGET]=calendar[calendar.get_loc(d)+1]
        f["label_information_end"]=calendar[calendar.get_loc(d)+5]
        frames.append(f)
    all_rows=pd.concat(frames,ignore_index=True)
    frame=all_rows.drop(columns=list(FINANCE))
    class Reader:
        def __init__(self):
            self.calendar=calendar
            self.calls=[]
        def read(self,rows,cutoff):
            self.calls.append(cutoff)
            assert rows[inp.DAY].le(cutoff).all()
            finance=all_rows.merge(rows[inp.KEY],on=inp.KEY,validate="one_to_one")[[*inp.KEY,*FINANCE]]
            future=finance.label_information_end.gt(cutoff)
            finance.loc[future,["valuation_status","exit_execution_status"]]="IMMATURE"
            finance.loc[future,["hypothetical_liquidation_net_bps","valuation_gross_terminal_ratio","observed_gap_bps"]]=np.nan
            return finance,{"cutoff":str(cutoff)}
    reader=Reader()
    study=pipe.Study(tmp_path,{"fixed":True},inp.Budget())
    parent=study.stage("preregistered",None,{"plan.json":b'{"fixed":true}'})
    predictions,digest=pipe.forecasts(study,frame,reader,parent,progress=lambda e:None)
    assert len(reader.calls)==9 and len(predictions["candidate"])==450
    assert len(list((tmp_path/"fit_claims").glob("*_finished.json")))==18
    assert (tmp_path/"forecasts/trained/manifest.json").is_file()
    assert not (tmp_path/"evaluation_finance").exists()
    monkeypatch.setattr(core,"train",lambda *a,**kw:pytest.fail("completed fit repeated"))
    again,restored=pipe.forecasts(study,frame,reader,parent,progress=lambda e:None)
    assert restored==digest and len(reader.calls)==9
    assert again["candidate"].equals(predictions["candidate"])
    # A started but unpublished fit must block recovery, even with budget remaining.
    interrupted=pipe.Study(tmp_path/"interrupted",{"fixed":True},inp.Budget())
    p=interrupted.stage("preregistered",None,{"plan.json":b'{"fixed":true}'})
    interrupted.claim("fold1_matched")
    with pytest.raises(ValueError,match="interrupted unpublished fit"):
        pipe.forecasts(interrupted,frame,Reader(),p,progress=lambda e:None)
