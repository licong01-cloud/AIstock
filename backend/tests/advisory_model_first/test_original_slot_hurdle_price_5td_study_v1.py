"""Synthetic stages/accounting only; fit mocks are not real research evidence."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
import sys
import tempfile

import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_original_slot_hurdle_price_5td_inputs_v1 import prepared_study as prepared_study
from backend.tests.advisory_model_first.test_original_slot_hurdle_price_5td_model_v1 import (
    constant_bundle, fitted_study as fitted_study,
)
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_contracts_v1 import ARMS, KEY, VALUE_FIELDS
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_evaluation_v1 import evaluate_hurdle_cohorts_v1
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_model_v1 import query_hurdle_nodes_v1
from backend.services.advisory_model_first import original_slot_hurdle_price_5td_pipeline_v1 as pipeline


def idle():
    return dict(captured_at=datetime.now(timezone.utc).isoformat(), active_counts=dict(single=0, custom_evo=0, multi_alpha=0))


def evaluation_case(prepared_study, fitted_study):
    plan, p, original = prepared_study
    rows = original.loc[original[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))
        & original.selection_effective_rank.le(5)].copy()
    immature = rows.label_information_end.gt(pd.Timestamp(plan.evaluation_end))
    rows.loc[immature, ["valuation_gross_terminal_ratio", "valuation_path_min_ratio", "hypothetical_liquidation_net_bps", "mark_to_market_net_bps"]] = np.nan
    rows.loc[immature, ["valuation_status", "label_status", "exit_execution_status"]] = "IMMATURE"
    rows.loc[rows[KEY[1]].gt(pd.Timestamp(plan.evaluation_end)), "observed_gap_bps"] = np.nan
    predictions = {a: query_hurdle_nodes_v1(bundle=constant_bundle(fitted_study), features=rows,
        scenario_gap_bps=rows.observed_gap_bps, arm=a) for a in ARMS}
    return rows, predictions


def test_four_original_five_slot_cohorts_and_unknown_attribution(prepared_study, fitted_study):
    plan, p, _ = prepared_study
    rows, predictions = evaluation_case(prepared_study, fitted_study)
    first = rows[KEY[0]].min()
    missing = rows[KEY[0]].eq(first) & rows.instrument.eq("stock_1")
    rows.loc[missing, "valuation_status"] = "UNKNOWN"
    rows.loc[missing, ["valuation_gross_terminal_ratio", "valuation_path_min_ratio", "hypothetical_liquidation_net_bps", "mark_to_market_net_bps"]] = np.nan
    candidate = predictions[ARMS[1]]
    known = candidate.status.isin(["AVOID", "ACCEPTABLE_VALUE_PREDICTED"])
    candidate.loc[known, "status"] = "UNKNOWN_STOCK_INPUT"
    candidate.loc[known, list(VALUE_FIELDS)] = np.nan
    result = evaluate_hurdle_cohorts_v1(rows=rows, days=p["days"], predictions=predictions,
        original_roster=p["roster"], plan=plan, calendar=p["calendar"])
    assert result["fixed_slot_budget"] == result["original_package_days"]*5
    assert sum(not g["horizon_mature"] for g in result["original_groups"]) == 10
    assert all(e["rank"] <= 5 and e["realized_return_bps"] is None for e in result["original_episodes"])
    for pair in result["primary"]["pairs"].values():
        assert pair["inference_status"] == "UNAVAILABLE_ORIGINAL_PANEL_HOLE_OR_TOO_SHORT"
        assert pair["settled_intervention_unique_stock_days"] == 0
        assert pair["mean_increment_bps"] == pytest.approx(pair["attribution"]["unknown_cash_contribution_bps"])
    assert result["status"] != "EXPLORATORY_WORTH_CONFIRMING" and not result["activation_evidence"]


@pytest.mark.parametrize("limited", [False, True])
def test_known_increment_and_intervention_support_can_route_only_navigation(prepared_study, fitted_study, limited):
    plan, p, _ = prepared_study
    rows, predictions = evaluation_case(prepared_study, fitted_study)
    # Protocol fixture with true action differences; not a claim of fitted profitability.
    avoid = query_hurdle_nodes_v1(bundle=constant_bundle(fitted_study, probability=.8, gain=100, loss=1000),
        features=rows, scenario_gap_bps=rows.observed_gap_bps, arm=ARMS[1])
    mask = rows.instrument.isin(["stock_4", "stock_5"]).to_numpy()
    if limited:
        mask &= rows[KEY[0]].isin(sorted(rows[KEY[0]].unique())[:5]).to_numpy()
    for name in (*VALUE_FIELDS, "status"):
        predictions[ARMS[1]].loc[mask, name] = avoid.loc[mask, name].to_numpy()
    result = evaluate_hurdle_cohorts_v1(rows=rows, days=p["days"], predictions=predictions,
        original_roster=p["roster"], plan=plan, calendar=p["calendar"])
    assert result["status"] == ("EXPLORATORY_INSUFFICIENT_SUPPORT" if limited else "EXPLORATORY_WORTH_CONFIRMING")
    for pair in result["primary"]["pairs"].values():
        assert pair["mean_increment_bps"] > 0 and pair["utility_increment_bps"] >= 0
        assert pair["intervention_support_met"] is not limited
        assert pair["settled_intervention_days"] == 5 if limited else pair["settled_intervention_days"] >= 20
        assert pair["attribution"]["known_action_contribution_bps"] == pytest.approx(
            pair["attribution"]["avoided_loss_bps"]-pair["attribution"]["missed_profit_bps"])
    assert result["evidence_use"] == "NAVIGATION_ONLY" and not result["deployable"]


def test_prediction_or_future_finance_corruption_fails(prepared_study, fitted_study):
    plan, p, _ = prepared_study
    rows, predictions = evaluation_case(prepared_study, fitted_study)
    predictions[ARMS[1]].loc[0, "p_positive"] = 2.
    with pytest.raises(ValueError, match="coordinates"):
        evaluate_hurdle_cohorts_v1(rows=rows, days=p["days"], predictions=predictions,
            original_roster=p["roster"], plan=plan, calendar=p["calendar"])
    rows, predictions = evaluation_case(prepared_study, fitted_study)
    rows.loc[rows.valuation_status.eq("IMMATURE"), "hypothetical_liquidation_net_bps"] = 9999.
    with pytest.raises(ValueError, match="future"):
        evaluate_hurdle_cohorts_v1(rows=rows, days=p["days"], predictions=predictions,
            original_roster=p["roster"], plan=plan, calendar=p["calendar"])


def setup_stages(plan, tmp_path, monkeypatch, fitted_study):
    pipeline.preregister_hurdle_study_v1(plan=plan, output_root=str(tmp_path))
    pipeline.prepare_hurdle_study_v1(plan=plan, output_root=str(tmp_path))
    monkeypatch.setattr(pipeline, "_node", lambda p: dict(unit_fixture=True, real_research_fit=False))
    calls = []
    def synthetic_fit(**kwargs):
        calls.append((kwargs["arm"], kwargs["component"]))
        return deepcopy(fitted_study["models"][kwargs["arm"]][kwargs["component"]])
    monkeypatch.setattr(pipeline, "fit_component_v1", synthetic_fit)
    return tmp_path/plan.experiment_id, calls


def test_six_components_exact_resume_and_prediction_freeze(prepared_study, fitted_study, tmp_path, monkeypatch):
    plan, _, _ = prepared_study
    root, fits = setup_stages(plan, tmp_path, monkeypatch, fitted_study)
    observations = []
    def probe():
        observations.append(idle())
        return observations[-1]
    path = pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=probe)
    assert len(fits) == 6 and len(observations) == 12 and len(set(fits)) == 6
    receipt = json.loads((path/"receipt.json").read_bytes())
    assert receipt["physical_fit_count"] == 6 and receipt["optimizer_fit_count"] == 2
    assert pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=probe) == path
    assert len(fits) == 6 and len(observations) == 12
    original_read = pipeline.read_projection_v1
    def projection(**kwargs):
        if "valuation_gross_terminal_ratio" in kwargs["columns"]:
            assert (root/"forecasts"/"trained"/"manifest.json").exists()
            assert kwargs["top5"]
        return original_read(**kwargs)
    monkeypatch.setattr(pipeline, "read_projection_v1", projection)
    result_path = pipeline.evaluate_hurdle_study_v1(plan=plan, output_root=str(tmp_path))
    result = json.loads((result_path/"evaluation.json").read_bytes())
    assert result["original_top5_rows"]*6/5 == result["original_top50_rows"]
    assert result["prediction_freeze_stage_sha256"] and not result["independent_oos_evidence"]
    assert pipeline.evaluate_hurdle_study_v1(plan=plan, output_root=str(tmp_path)) == result_path
    assert len(fits) == 6


def test_QE_overlap_saves_completed_component_and_pauses_next(prepared_study, fitted_study, tmp_path, monkeypatch):
    plan, _, _ = prepared_study
    root, fits = setup_stages(plan, tmp_path, monkeypatch, fitted_study)
    active = idle()
    active["active_counts"]["custom_evo"] = 1
    with pytest.raises(ValueError, match="running/pending"):
        pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=lambda: active)
    assert len(fits) == 0 and not (root/"fits").exists()
    count = 0
    def overlap():
        nonlocal count
        count += 1
        observed = idle()
        if count == 2:
            observed["active_counts"]["single"] = 1
        return observed
    with pytest.raises(ValueError, match="RESOURCE_OVERLAP_DETECTED"):
        pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=overlap)
    assert len(fits) == 1 and (root/"fits"/(ARMS[0]+"_sign")/"trained").exists()
    assert not (root/"trained").exists()
    pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=idle)
    assert len(fits) == 6  # The completed component was not implicitly fitted again.


def test_failed_started_component_never_implicitly_retries(prepared_study, fitted_study, tmp_path, monkeypatch):
    plan, _, _ = prepared_study
    root, _ = setup_stages(plan, tmp_path, monkeypatch, fitted_study)
    calls = []
    def broken(**kwargs):
        calls.append(1)
        raise RuntimeError("synthetic optimizer interruption")
    monkeypatch.setattr(pipeline, "fit_component_v1", broken)
    with pytest.raises(RuntimeError):
        pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=idle)
    with pytest.raises(ValueError, match="TECHNICAL_RECOVERY_REQUIRED"):
        pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=idle)
    assert len(calls) == 1 and not (root/"trained").exists()
    retry = plan.model_validate({**plan.model_dump(mode="json"), "attempt_id": "technical_retry_2", "exact_retry_of": str(root)})
    assert retry.economic_contract_sha256 == plan.economic_contract_sha256 and retry.plan_sha256 != plan.plan_sha256
    assert pipeline._prior_fit_attempts(retry) == 1


def test_nonexecution_and_normal_unknown_keep_all_dates(prepared_study, fitted_study):
    plan, p, _ = prepared_study
    rows, predictions = evaluation_case(prepared_study, fitted_study)
    day = rows[KEY[0]].min()
    cash = rows[KEY[0]].eq(day) & rows.instrument.eq("stock_1")
    missing = rows[KEY[0]].eq(day) & rows.instrument.eq("stock_2")
    fields = ["valuation_gross_terminal_ratio", "valuation_path_min_ratio", "hypothetical_liquidation_net_bps", "mark_to_market_net_bps"]
    rows.loc[cash | missing, fields] = np.nan
    rows.loc[cash, "valuation_status"] = "CASH_ENTRY_NOT_EXECUTABLE"
    rows.loc[cash, ["hypothetical_liquidation_net_bps", "mark_to_market_net_bps"]] = 0.
    rows.loc[cash, "exit_execution_status"] = "NOT_HELD"
    rows.loc[missing, "valuation_status"] = "UNKNOWN"
    result = evaluate_hurdle_cohorts_v1(rows=rows, days=p["days"], predictions=predictions,
        original_roster=p["roster"], plan=plan, calendar=p["calendar"])
    records = [e for e in result["original_episodes"] if e["decision_date"] == day.date().isoformat()]
    assert all(set(e["contributions_bps"].values()) == {0.} for e in records if e["instrument"] == "stock_1")
    assert all(set(e["contributions_bps"].values()) == {None} for e in records if e["instrument"] == "stock_2")
    assert result["primary"]["pairs"]["baseline"]["paired_days"] == result["primary"]["original_mature_days"]-1


def test_empty_original_evaluation_has_cash_slots_not_fake_candidates(prepared_study, fitted_study):
    plan, p, _ = prepared_study
    rows, predictions = evaluation_case(prepared_study, fitted_study)
    rows = rows.iloc[:0]
    predictions = {a: f.iloc[:0] for a, f in predictions.items()}
    roster, days = p["roster"].copy(), p["days"].copy()
    roster = roster.loc[roster[KEY[0]].lt(pd.Timestamp(plan.evaluation_start))]
    days.loc[days.decision_date.ge(pd.Timestamp(plan.evaluation_start)), "original_candidates"] = 0
    result = evaluate_hurdle_cohorts_v1(rows=rows, days=days, predictions=predictions,
        original_roster=roster, plan=plan, calendar=p["calendar"])
    assert result["original_top5_rows"] == 0 and result["fixed_slot_budget"] == result["original_package_days"]*5
    assert result["primary"]["cohort_means"]["baseline"]["mean_bps"] == 0.


def test_X_environment_is_restored_even_when_operation_raises(prepared_study):
    plan, _, _ = prepared_study
    prior = {n: os.environ.get(n) for n in ("TEMP", "TMP", "TMPDIR", "JOBLIB_TEMP_FOLDER")}
    temp, bytecode = tempfile.tempdir, sys.dont_write_bytecode
    @pipeline._x_temporary_files
    def failure(**kwargs):
        assert tempfile.tempdir.replace("\\", "/").lower().startswith(("x:/", "/mnt/x/"))
        raise RuntimeError("synthetic failure")
    with pytest.raises(RuntimeError):
        failure(plan=plan)
    assert prior == {n: os.environ.get(n) for n in prior}
    assert tempfile.tempdir == temp and sys.dont_write_bytecode == bytecode

