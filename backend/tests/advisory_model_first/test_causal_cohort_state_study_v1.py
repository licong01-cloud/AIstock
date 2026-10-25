"""Direct scientific contracts, not snapshots of research output."""
from copy import deepcopy
import json
import socket

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import causal_cohort_state_study_v1 as study


def ledger():
    dates = pd.bdate_range("2025-01-01", periods=36)
    return pd.DataFrame(dict(decision_date=dates[:30], horizon_end=dates[5:35],
        known=True, cohort_net_bps=np.arange(30, dtype=float)-10))


def test_projection_before_future_finance_and_exact_horizon():
    frame = ledger()
    cutoff = frame.horizon_end.iloc[22]
    expected = study.predict_state(frame, as_of=cutoff, window=20)
    changed = frame.copy()
    changed.loc[changed.horizon_end.gt(cutoff), "cohort_net_bps"] = np.inf
    assert study.predict_state(changed, as_of=cutoff, window=20) == expected
    assert expected["history_days"] == 20
    assert expected["max_label_end"] == cutoff.date().isoformat()
    assert expected["expected_net_bps"] == pytest.approx(frame.cohort_net_bps.iloc[3:23].mean())
    with pytest.raises(ValueError, match="search windows"):
        study.predict_state(frame, as_of=cutoff, window=19)


def test_unknown_not_skipped_static_freezes_and_source_untouched():
    frame = ledger()
    cutoff = frame.horizon_end.iloc[22]
    frame.loc[10, "known"] = False
    before = frame.copy(deep=True)
    assert study.predict_state(frame, as_of=cutoff, window=20)["action"] == "NO_ADVICE"
    forecasts = study.freeze_forecasts(frame, train_end=frame.horizon_end.iloc[19],
        evaluation_dates=[cutoff, cutoff+pd.Timedelta(days=1)])
    assert forecasts[cutoff.date().isoformat()]["static"] == forecasts[(cutoff+pd.Timedelta(days=1)).date().isoformat()]["static"]
    pd.testing.assert_frame_equal(before, frame)
    assert study.predict_state(frame, as_of=frame.horizon_end.iloc[5], window=20)["action"] == "NO_ADVICE"
    with pytest.raises(ValueError, match="overlaps"):
        study.freeze_forecasts(frame, train_end=cutoff, evaluation_dates=[cutoff])


def source_rows():
    calendar = pd.bdate_range("2025-01-01", periods=10)
    rows, days = [], []
    for i in range(3):
        for package in ("one", "two"):
            days.append(dict(package_id=package, decision_date=calendar[i], target_date=calendar[i+1],
                horizon_end=calendar[i+5], original_candidates=1))
            rows.append(dict(package_id=package, decision_as_of_trade_date=calendar[i],
                target_trade_date=calendar[i+1], label_information_end=calendar[i+5], instrument="stock",
                selection_effective_rank=1, valuation_status="AVAILABLE", hypothetical_liquidation_net_bps=100.))
    return pd.DataFrame(rows), pd.DataFrame(days), [d.date().isoformat() for d in calendar]


def test_cohort_fixed_slots_cash_unknown_and_empty_original_day():
    rows, days, calendar = source_rows()
    assert study.build_cohorts(rows, days, calendar).cohort_net_bps.tolist() == [20.,20.,20.]
    rows.loc[0, ["valuation_status", "hypothetical_liquidation_net_bps"]] = ["CASH_ENTRY_NOT_EXECUTABLE", 0.]
    assert study.build_cohorts(rows, days, calendar).cohort_net_bps.iloc[0] == 10.
    rows.loc[0, "valuation_status"] = "UNKNOWN"
    assert pd.isna(study.build_cohorts(rows, days, calendar).cohort_net_bps.iloc[0])
    rows = rows.loc[rows[study.D].ne(days.decision_date.iloc[0])]
    days.loc[days.decision_date.eq(days.decision_date.iloc[0]), "original_candidates"] = 0
    assert study.build_cohorts(rows, days, calendar).cohort_net_bps.iloc[0] == 0.


@pytest.mark.parametrize("mutation", ["missing_date", "rank", "horizon", "duplicate", "cash", "nan", "package"])
def test_original_identity_rejects_mutations(mutation):
    rows, days, calendar = source_rows()
    if mutation == "missing_date":
        missing = days.decision_date.iloc[2]
        rows = rows.loc[rows[study.D].ne(missing)]
        days = days.loc[days.decision_date.ne(missing)]
    elif mutation == "rank":
        rows.loc[0, "selection_effective_rank"] = 2
    elif mutation == "horizon":
        rows.loc[0, study.H] += pd.Timedelta(days=1)
    elif mutation == "duplicate":
        rows = pd.concat([rows, rows.iloc[:1]])
    elif mutation == "cash":
        rows.loc[0, "valuation_status"] = "CASH_ENTRY_NOT_EXECUTABLE"
    elif mutation == "nan":
        rows.loc[0, "hypothetical_liquidation_net_bps"] = np.nan
    else:
        rows.loc[0, "package_id"] = "foreign"
    with pytest.raises(ValueError):
        study.build_cohorts(rows, days, calendar)


def evaluation_input():
    dates = [d.date().isoformat() for d in pd.bdate_range("2025-04-01", periods=12)]
    episodes, forecasts = [], {}
    for d in dates:
        state = dict(status="PREDICTED", action="TAKE", as_of=d, max_label_end=d,
            policy_sha256=study.POLICY_SHA256, expected_net_bps=1., p_profit=.5)
        forecasts[d] = dict(static=state, rolling20=dict(state, action="SKIP"))
        episodes.extend(dict(decision_date=d, instrument=str(k), label_cluster=f"{d}:{k}", mature=True,
            financial_known=True, valuation_status="AVAILABLE", baseline_net_bps=-100.,package_id=p)
            for p in ("one","two") for k in range(5))
    source = dict(original_decision_days=12, episodes=episodes, aggregate=dict(original_mature_days=dates,
        packages=["one","two"], fixed_slot_panels_bps=dict(baseline=[-100.]*12,rule_300bps=[-100.]*12)))
    return source, forecasts


def test_native_statistics_attribution_cluster_denominator_and_no_advice():
    source, forecasts = evaluation_input()
    result = study.evaluate_states(source, forecasts)
    assert result["statistics"]["static"]["mean_increment_bps"] == 100.
    assert result["support"]["baseline"]["unique_stock_days"] == 60  # not 120 business slots
    assert result["attribution"]["baseline"]["avoided_loss_bps"] == pytest.approx(100.)
    assert result["status"] == "EXACT_CANDIDATE_NOT_SUPPORTED"  # fewer than 20 original days
    before = deepcopy(source)
    for f in forecasts.values():
        f["rolling20"].update(action="NO_ADVICE", status="NO_ADVICE")
    result = study.evaluate_states(source, forecasts)
    assert result["fixed_slot_mean_bps"]["rolling20"] == -100.
    assert result["predictive_skill"]["rolling20"]["rmse_bps"] is None
    assert result["support"]["baseline"]["days"] == 0
    assert source == before


@pytest.mark.parametrize("mutation", ["future", "policy", "action", "date"])
def test_forecast_identity_clock_and_original_dates(mutation):
    source, forecasts = evaluation_input()
    first = next(iter(forecasts))
    if mutation == "date":
        del forecasts[first]
    else:
        field, value = {"future":("max_label_end","2099-01-01"), "policy":("policy_sha256","fake"),
            "action":("action","fallback")}[mutation]
        forecasts[first]["rolling20"][field] = value
    with pytest.raises(ValueError):
        study.evaluate_states(source, forecasts)


def test_unknown_finance_prevents_confirmation_and_remains_unknown():
    source, forecasts = evaluation_input()
    source["aggregate"]["fixed_slot_panels_bps"]["baseline"][0] = None
    for ep in source["episodes"][:10]:
        ep.update(financial_known=False, valuation_status="UNKNOWN",baseline_net_bps=None)
    result = study.evaluate_states(source, forecasts)
    assert result["fixed_slot_mean_bps"]["rolling20"] is None
    assert result["statistics"]["baseline"]["simultaneous_95_bonferroni_bps"] is None
    assert result["deployable"] is False


def test_real_stage_chain_exact_retry_and_identity(tmp_path, monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("offline study attempted network access")
    monkeypatch.setattr(socket.socket, "connect", denied)
    def publish(**kwargs):
        path = study.publish_stage(**kwargs)
        return json.loads((path/"manifest.json").read_bytes())
    rows, days, calendar = source_rows()
    rows["manifest_sha256"], rows["run_id"] = "a"*64, "original_run"
    plan = dict(train_start=calendar[0], train_end=calendar[0], evaluation_start=calendar[1], evaluation_end=calendar[2],
        sources=[dict(package_id=p,manifest_sha256="a"*64,run_id="original_run") for p in ("one","two")])
    digest = study.sha(plan)
    root = tmp_path/"source"
    parent = publish(study_root=root,stage="preregistered",plan_sha256=digest,parent_sha256=None,
        artifacts={"plan.json":study._json_bytes(plan)})
    parent = publish(study_root=root,stage="prepared",plan_sha256=digest,parent_sha256=parent["stage_sha256"],
        artifacts={"domain.parquet":rows.loc[rows[study.D].eq(calendar[0])].to_parquet(index=False),
            "days.parquet":days.to_parquet(index=False), "calendar.json":study._json_bytes(calendar)})
    parent = publish(study_root=root,stage="trained",plan_sha256=digest,parent_sha256=parent["stage_sha256"],
        artifacts={"bundle.json":study._json_bytes({"original":True})})
    forecast = publish(study_root=root/"forecasts",stage="trained",plan_sha256=digest,
        parent_sha256=parent["stage_sha256"],artifacts={"receipt.json":study._json_bytes({"original":True})})
    evaluation = dict(original_decision_days=2, forecast_stage_sha256=forecast["stage_sha256"],policy_sha256="b"*64,
        episodes=[dict(decision_date=d,package_id=p,instrument="stock",label_cluster=d,mature=True,
            financial_known=True,valuation_status="AVAILABLE",baseline_net_bps=100.) for d in calendar[1:3] for p in ("one","two")],
        aggregate=dict(packages=["one","two"],original_mature_days=calendar[1:3],fixed_slot_panels_bps=dict(baseline=[20.,20.],rule_300bps=[20.,20.])))
    evaluation["by_package"] = {p:dict(evaluation["aggregate"],packages=[p]) for p in ("one","two")}
    study.publish_stage(study_root=root,stage="evaluated",plan_sha256=digest,parent_sha256=parent["stage_sha256"],
        artifacts={"evaluation.json":study._json_bytes(evaluation),
            "measured_rows.parquet":rows.loc[rows[study.D].ne(calendar[0])].to_parquet(index=False)})
    kwargs = dict(source_root=str(root),source_plan_sha256=digest,forecast_stage_sha256=forecast["stage_sha256"],
        output_root=str(tmp_path/"study"))
    with pytest.raises(ValueError, match="forecast identity"):
        study.run_study(**dict(kwargs, forecast_stage_sha256="c"*64))
    result = study.run_study(**kwargs)
    assert result == study.run_study(**kwargs)
    assert result["physical_fit_count"] == 0 and result["preparation_original_slots"] == 6
    assert result["original_slots"] == 4
    assert result["fixed_slot_mean_bps"]["rolling20"] == 20.
    registered = [json.loads(line) for line in (tmp_path/"study/trial_registry.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(registered) == 4
