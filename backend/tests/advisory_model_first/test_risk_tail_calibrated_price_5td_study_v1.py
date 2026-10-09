"""Atomic stages, fit accounting, immutable cohorts and unknown attribution."""
from datetime import datetime, timezone
import json

import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_risk_tail_calibrated_price_5td_inputs_v1 import synthetic_study as synthetic_study
from backend.tests.advisory_model_first.test_risk_tail_calibrated_price_5td_model_v1 import fitted_study as fitted_study
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY
from backend.services.advisory_model_first.generic_population_price_5td_cli_v1 import public_qe_observation_v1
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_contracts_v1 import ARMS, ROSTER_KEY
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_evaluation_v1 import evaluate_risk_tail_cohorts_v1
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_model_v1 import bundle_payload_v1, core_payload_v1, query_risk_tail_nodes_v1
from backend.services.advisory_model_first import risk_tail_calibrated_price_5td_pipeline_v1 as pipeline


def idle():
    return dict(captured_at=datetime.now(timezone.utc).isoformat(), active_counts=dict(single=0, custom_evo=0, multi_alpha=0))


def predictions_for(rows, fitted_study):
    return {a: query_risk_tail_nodes_v1(bundle=bundle_payload_v1(*fitted_study[:2]),
        features=rows[[*ROSTER_KEY, *FEATURES]], scenario_gap_bps=rows.observed_gap_bps, arm=a) for a in ARMS}


def test_four_original_cohorts_and_unknown_outcomes(synthetic_study, fitted_study):
    plan, prepared, rows = synthetic_study
    rows = rows.loc[rows[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy()
    forecasts = predictions_for(rows, fitted_study)
    first = rows[KEY[0]].min()
    # Entire stock/date observation missing in both packages, not a model abstention.
    missing = rows[KEY[0]].eq(first) & rows.instrument.eq("stock_1")
    rows.loc[missing, "valuation_status"] = "UNKNOWN"
    rows.loc[missing, ["valuation_gross_terminal_ratio", "valuation_path_min_ratio", "hypothetical_liquidation_net_bps", "mark_to_market_net_bps"]] = np.nan
    result = evaluate_risk_tail_cohorts_v1(rows=rows, days=prepared["days"], original_roster=prepared["roster"],
        predictions=forecasts, plan=plan, calendar=prepared["calendar"])
    assert result["fixed_slot_budget"] == result["original_package_days"]*5
    assert all(e["rank"] <= 5 for e in result["original_episodes"])
    assert sum(not g["horizon_mature"] for g in result["original_groups"]) == 10
    for opponent in ("baseline", ARMS[0]):
        endpoint = result["primary"]["pairs"][opponent]
        assert endpoint["paired_days"] == endpoint["original_mature_days"]-1
        assert endpoint["simultaneous_95_bonferroni_bps"] is None and endpoint["mde_80_bps"] is None
        attr = endpoint["attribution"]
        assert endpoint["mean_increment_bps"] == pytest.approx(attr["known_action_contribution_bps"]+attr["unknown_cash_contribution_bps"])
    assert result["status"] == "NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE"
    assert not result["activation_evidence"] and not result["nav_or_annualized_return_claimed"]
    assert all(all(v is None for v in e["contributions_bps"].values()) for e in result["original_episodes"]
        if e["decision_date"] == first.date().isoformat() and e["instrument"] == "stock_1")


def test_model_unknown_cash_is_not_a_known_intervention(synthetic_study, fitted_study):
    plan, prepared, rows = synthetic_study
    rows = rows.loc[rows[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy()
    forecasts = predictions_for(rows, fitted_study)
    for frame in forecasts.values():
        frame.loc[:, "status"] = "UNKNOWN_MODEL_DISTRIBUTION"
        frame.loc[:, ["expected_net_bps", "profit_probability", "downside_q90_bps", "uncalibrated_downside_q90_bps"]] = np.nan
    result = evaluate_risk_tail_cohorts_v1(rows=rows, days=prepared["days"], original_roster=prepared["roster"],
        predictions=forecasts, plan=plan, calendar=prepared["calendar"])
    pair = result["primary"]["pairs"]["baseline"]
    assert pair["mean_increment_bps"] > 0 and pair["settled_known_interventions"] == 0
    assert pair["attribution"]["unknown_cash_contribution_bps"] == pytest.approx(pair["mean_increment_bps"])
    assert result["status"] == "NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE"


def test_eval_cannot_change_ranks_or_policy(synthetic_study, fitted_study):
    plan, prepared, rows = synthetic_study
    rows = rows.loc[rows[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))].copy()
    forecasts = predictions_for(rows, fitted_study)
    rows.loc[rows.index[0], "selection_effective_rank"] = 6
    with pytest.raises(ValueError, match="ranks"):
        evaluate_risk_tail_cohorts_v1(rows=rows, days=prepared["days"], original_roster=prepared["roster"],
            predictions=forecasts, plan=plan, calendar=prepared["calendar"])


def test_completed_core_resumes_calibration_without_refit(tmp_path, synthetic_study, fitted_study, monkeypatch):
    plan, inputs, _ = synthetic_study
    output = tmp_path/"new_run"
    pipeline.preregister_risk_tail_study_v1(plan=plan, output_root=output)
    prepared = pipeline.prepare_risk_tail_study_v1(plan=plan, output_root=output)
    parent = json.loads((prepared/"manifest.json").read_bytes())
    core = fitted_study[0]
    root = prepared.parent
    publish_stage(study_root=root/"fits"/"shared_core", stage="trained", plan_sha256=plan.plan_sha256,
        parent_sha256=parent["stage_sha256"], artifacts={"model.json": _json_bytes(core_payload_v1(core))})
    def unexpected(*args, **kwargs):
        raise AssertionError("resume cannot fit, query QE or require a new training node")
    monkeypatch.setattr(pipeline, "fit_shared_core_v1", unexpected)
    monkeypatch.setattr(pipeline, "_node", unexpected)
    calls = []
    original_projection = pipeline.read_projection_v1
    original_query = pipeline.query_risk_tail_nodes_v1
    def projection(**kwargs):
        calls.append(("project", kwargs["columns"], kwargs["dates"]))
        return original_projection(**kwargs)
    def query(**kwargs):
        calls.append(("forecast", kwargs["arm"]))
        return original_query(**kwargs)
    monkeypatch.setattr(pipeline, "read_projection_v1", projection)
    monkeypatch.setattr(pipeline, "query_risk_tail_nodes_v1", query)
    trained = pipeline.train_risk_tail_study_v1(plan=plan, output_root=output, qe_idle_probe=unexpected)
    assert json.loads((trained/"receipt.json").read_bytes())["physical_fit_count"] == 1
    assert pipeline.train_risk_tail_study_v1(plan=plan, output_root=output, qe_idle_probe=unexpected) == trained
    evaluated = pipeline.evaluate_risk_tail_study_v1(plan=plan, output_root=output)
    assert pipeline.evaluate_risk_tail_study_v1(plan=plan, output_root=output) == evaluated
    records = [json.loads(v) for v in (output/"trial_registry.jsonl").read_text(encoding="utf8").splitlines()]
    assert all(r["planned_trial_count"] == 2 for r in records)
    assert records[-1]["evaluated_trial_count"] == 2 and records[-1]["selected_trial_count"] == 0
    last = calls[-1]
    assert last[0] == "project" and "hypothetical_liquidation_net_bps" in last[1]
    assert all(str(d)[:10] <= str(plan.evaluation_end) for d in last[2])
    assert [v[1] for v in calls if v[0] == "forecast"] == list(ARMS)
    assert calls.index(next(v for v in calls if v[0] == "forecast")) < len(calls)-1
    calendar = pd.to_datetime(inputs["calendar"])
    assert all(calendar[calendar.get_loc(pd.Timestamp(d))+5] <= pd.Timestamp(plan.evaluation_end) for d in last[2])
    result = json.loads((evaluated/"evaluation.json").read_bytes())
    assert result["primary"]["pairs"]["baseline"]["simultaneous_95_bonferroni_bps"] is not None
    assert result["regime_support"] == "UNKNOWN"


def test_explicit_exact_retry_preserves_economics_and_never_replaces_completion(tmp_path, synthetic_study):
    plan, _, _ = synthetic_study
    first = pipeline.preregister_risk_tail_study_v1(plan=plan, output_root=tmp_path/"original").parent
    retry = plan.model_copy(update={"attempt_id": "technical_retry_2", "exact_retry_of": str(first)})
    assert retry.plan_sha256 != plan.plan_sha256 and retry.economic_contract_sha256 == plan.economic_contract_sha256
    assert pipeline.preregister_risk_tail_study_v1(plan=retry, output_root=tmp_path/"retry").exists()
    changed = retry.model_copy(update={"source_prepared": retry.source_prepared.model_copy(update={"stage_sha256": "0"*64})})
    with pytest.raises(ValueError, match="identity"):
        pipeline.preregister_risk_tail_study_v1(plan=changed, output_root=tmp_path/"changed")
    publish_stage(study_root=first, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256="e"*64,
        artifacts={"receipt.json": _json_bytes({"unit_complete": True})})
    with pytest.raises(ValueError, match="completed study"):
        pipeline.preregister_risk_tail_study_v1(plan=retry, output_root=tmp_path/"later")


def test_c_drive_and_non_frozen_implementation_are_rejected(tmp_path, synthetic_study):
    plan, _, _ = synthetic_study
    with pytest.raises(ValueError, match="C|forbidden"):
        pipeline.preregister_risk_tail_study_v1(plan=plan, output_root="C:/Temp/forbidden")
    with pytest.raises(ValueError, match="implementation"):
        pipeline.preregister_risk_tail_study_v1(plan=plan.model_copy(update={"implementation_sha256": "0"*64}), output_root=tmp_path)


@pytest.mark.parametrize("busy_after", [False, True])
def test_fit_qe_before_after_and_failed_attempt_never_refits(tmp_path, synthetic_study, monkeypatch, busy_after):
    plan, _, _ = synthetic_study
    output = tmp_path/"new_run"
    pipeline.preregister_risk_tail_study_v1(plan=plan, output_root=output)
    prepared = pipeline.prepare_risk_tail_study_v1(plan=plan, output_root=output)
    monkeypatch.setattr(pipeline, "_node", lambda p: {"unit_only": True})
    seen = []
    def probe():
        seen.append("observe")
        result = idle()
        result["active_counts"]["single"] = int(not busy_after or len(seen) == 2)
        return result
    def attempt(*, before_fit, after_fit, **kwargs):
        before_fit(ARMS[0])
        try:
            seen.append("fit")
            raise RuntimeError("unit fit failure")
        finally:
            after_fit(ARMS[0])
    monkeypatch.setattr(pipeline, "fit_shared_core_v1", attempt)
    with pytest.raises((ValueError, RuntimeError)):
        pipeline.train_risk_tail_study_v1(plan=plan, output_root=output, qe_idle_probe=probe)
    assert seen == (["observe"] if not busy_after else ["observe", "fit", "observe"])
    if busy_after:
        with pytest.raises(ValueError, match="implicit refit"):
            pipeline.train_risk_tail_study_v1(plan=plan, output_root=output, qe_idle_probe=idle)
        assert not (prepared.parent/"trained").exists()
        assert (prepared.parent/"fit_attempt.json").exists()


def test_six_public_qe_reads_are_running_and_pending_only():
    calls = []
    def get(path, *, params):
        calls.append((path, params["status"]))
        if path.endswith("experiments"):
            return dict(ok=True, total=0, items=[], has_more=False)
        if path.endswith("tasks"):
            return dict(status="success", data=[])
        return dict(status="success", data=dict(count=0, runs=[]))
    assert not any(public_qe_observation_v1("http://127.0.0.1:8001", get=get)["active_counts"].values())
    assert len(calls) == 6 and {v for _, v in calls} == {"running", "pending"}
