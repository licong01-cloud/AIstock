"""Synthetic workflow/causal-read ordering; no DB, QE or research run."""
from copy import deepcopy
from datetime import datetime, timezone
import json

import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_market_open_hurdle_price_5td_inputs_v1 import (
    MarketSession, prepared_study as prepared_study,
)
from backend.tests.advisory_model_first.test_market_open_hurdle_price_5td_model_v1 import (
    fitted_study as fitted_study, query,
)
from backend.services.advisory_model_first import market_open_hurdle_price_5td_pipeline_v1 as pipeline
from backend.services.advisory_model_first.market_open_hurdle_price_5td_contracts_v1 import ARMS, CLOCK, KEY, MARKET_COLUMN, sha
from backend.services.advisory_model_first.market_open_hurdle_price_5td_evaluation_v1 import evaluate_hurdle_cohorts_v1
from backend.services.advisory_model_first.market_open_hurdle_price_5td_inputs_v1 import join_market_open_v1, read_market_open_v1


def idle():
    return dict(captured_at=datetime.now(timezone.utc).isoformat(), active_counts=dict(single=0, custom_evo=0, multi_alpha=0))


def setup_stages(plan, tmp_path, monkeypatch, bundle):
    pipeline.preregister_hurdle_study_v1(plan=plan, output_root=str(tmp_path))
    pipeline.prepare_hurdle_study_v1(plan=plan, output_root=str(tmp_path), session_factory=MarketSession)
    monkeypatch.setattr(pipeline, "_node", lambda p: dict(synthetic_fixture_only=True))
    calls = []
    def fit(**kw):
        calls.append((kw["arm"], kw["component"]))
        value = deepcopy(bundle["models"][kw["arm"]][kw["component"]])
        # Same synthetic numbers, a new readonly capture time: explicitly bind this toy head to its stage.
        value["recipe"]["encoding_sha256"] = sha(kw["encoding"])
        value["component_sha256"] = sha({n: v for n, v in value.items() if n != "component_sha256"})
        return value
    monkeypatch.setattr(pipeline, "fit_component_v1", fit)
    return tmp_path/plan.experiment_id, calls


def test_six_components_and_freeze_before_outcomes_with_no_database_resume(prepared_study, fitted_study, tmp_path, monkeypatch):
    plan, _, _ = prepared_study
    root, fits = setup_stages(plan, tmp_path, monkeypatch, fitted_study)
    observations = []
    def probe():
        observations.append(idle())
        return observations[-1]
    trained = pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=probe)
    receipt = json.loads((trained/"receipt.json").read_bytes())
    assert len(fits) == 6 and len(observations) == 12 and len(set(fits)) == 6
    assert receipt["previous_physical_fit_count"] == 142 and receipt["cumulative_physical_fit_attempts"] == 148
    pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=probe)
    assert len(fits) == 6 and len(observations) == 12
    read = pipeline.read_projection_v1
    finance_attempts = 0
    def ordered_projection(**kw):
        nonlocal finance_attempts
        if "valuation_gross_terminal_ratio" in kw["columns"]:
            assert (root/"forecasts"/"trained"/"inputs.parquet").exists()
            assert (root/"forecasts"/"trained"/"predictions.parquet").exists()
            assert (root/"forecasts"/"trained"/"market_source.json").exists()
            finance_attempts += 1
            if finance_attempts == 1:
                raise RuntimeError("synthetic outcome-reader failure after prediction freeze")
        return read(**kw)
    monkeypatch.setattr(pipeline, "read_projection_v1", ordered_projection)
    sessions = []
    def factory():
        s = MarketSession()
        sessions.append(s)
        return s
    with pytest.raises(RuntimeError, match="after prediction freeze"):
        pipeline.evaluate_hurdle_study_v1(plan=plan, output_root=str(tmp_path), session_factory=factory)
    assert len(sessions) == 1 and len(sessions[0].calls) == 2
    monkeypatch.setattr(pipeline, "read_market_open_v1", lambda **kw: pytest.fail("market source re-read"))
    path = pipeline.evaluate_hurdle_study_v1(plan=plan, output_root=str(tmp_path))
    assert finance_attempts == 2 and len(fits) == 6
    result = json.loads((path/"evaluation.json").read_bytes())
    assert result["information_clock"] == CLOCK and not result["actual_fill_proven"]
    assert result["fixed_slot_budget"] == result["original_package_days"]*5
    assert sum(not g["horizon_mature"] for g in result["original_groups"]) == 10
    # A completed/resumed run must not touch now-changing market source.
    monkeypatch.setattr(pipeline, "read_market_open_v1", lambda **kw: pytest.fail("market source re-read"))
    assert pipeline.evaluate_hurdle_study_v1(plan=plan, output_root=str(tmp_path)) == path
    assert not result["deployable"] and not result["activation_evidence"]


def test_incomplete_started_component_requires_explicit_technical_identity(prepared_study, fitted_study, tmp_path, monkeypatch):
    plan, _, _ = prepared_study
    root, fits = setup_stages(plan, tmp_path, monkeypatch, fitted_study)
    def failure(**kw):
        fits.append((kw["arm"], kw["component"]))
        raise RuntimeError("synthetic interrupted optimizer")
    monkeypatch.setattr(pipeline, "fit_component_v1", failure)
    with pytest.raises(RuntimeError):
        pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=idle)
    with pytest.raises(ValueError, match="TECHNICAL_RECOVERY_REQUIRED"):
        pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=idle)
    assert len(fits) == 1 and (root/"fits"/(ARMS[0]+"_sign")/"attempt.json").exists()


def test_QE_overlap_retains_completed_model_no_hidden_second_fit(prepared_study, fitted_study, tmp_path, monkeypatch):
    plan, _, _ = prepared_study
    root, fits = setup_stages(plan, tmp_path, monkeypatch, fitted_study)
    count = 0
    def observation():
        nonlocal count
        count += 1
        value = idle()
        if count == 2:
            value["active_counts"]["single"] = 1
        return value
    with pytest.raises(ValueError, match="OVERLAP"):
        pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=observation)
    assert len(fits) == 1 and (root/"fits"/(ARMS[0]+"_sign")/"trained").exists()
    pipeline.train_hurdle_study_v1(plan=plan, output_root=str(tmp_path), qe_idle_probe=idle)
    assert len(fits) == 6


def test_false_market_identity_and_future_finance_are_rejected(prepared_study, fitted_study):
    plan, p, original = prepared_study
    rows = original.loc[original[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))
        & original.selection_effective_rank.le(5)].copy()
    market, _ = read_market_open_v1(plan=plan, dates=p["encoding"]["clocks"]["gap_evaluation_dates"],
        calendar=p["calendar"], session_factory=MarketSession)
    rows = join_market_open_v1(rows, market)
    immature = rows.label_information_end.gt(pd.Timestamp(plan.evaluation_end))
    outcomes = ["valuation_gross_terminal_ratio", "valuation_path_min_ratio", "hypothetical_liquidation_net_bps", "mark_to_market_net_bps"]
    rows.loc[immature, outcomes] = np.nan
    rows.loc[immature, ["valuation_status", "label_status", "exit_execution_status"]] = "IMMATURE"
    rows.loc[rows[KEY[1]].gt(pd.Timestamp(plan.evaluation_end)), "observed_gap_bps"] = np.nan
    forecasts = {a: query(fitted_study, rows, a, kind="OBSERVED_POST_OPEN_VALUATION") for a in ARMS}
    forecasts[ARMS[1]].loc[0, MARKET_COLUMN] += 1
    with pytest.raises(ValueError, match="market condition"):
        evaluate_hurdle_cohorts_v1(rows=rows, days=p["days"], predictions=forecasts, original_roster=p["roster"],
            plan=plan, calendar=p["calendar"])
    forecasts[ARMS[1]].loc[0, MARKET_COLUMN] -= 1
    rows.loc[immature, "hypothetical_liquidation_net_bps"] = 9999.
    with pytest.raises(ValueError, match="future"):
        evaluate_hurdle_cohorts_v1(rows=rows, days=p["days"], predictions=forecasts, original_roster=p["roster"],
            plan=plan, calendar=p["calendar"])
