"""Source ordering and fixed-slot accounting; no live services or research runs."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_sector_relative_open_price_5td_inputs_v1 import file_binding
from backend.tests.advisory_model_first.test_sector_relative_open_price_5td_model_v1 import (
    bundle as bundle, fitted_native as fitted_native, synthetic_domain, query,
)
from backend.services.advisory_model_first import sector_relative_open_price_5td_pipeline_v1 as pipeline
from backend.services.advisory_model_first.sector_relative_open_price_5td_contracts_v1 import (
    ARMS, CONTEXT_COLUMNS, FEATURES, KEY, META, SectorRelativeOpenStudyPlanV1, sha,
)
from backend.services.advisory_model_first.sector_relative_open_price_5td_evaluation_v1 import evaluate_sector_relative_open_v1


def evaluation_packet(bundle):
    calendar = pd.bdate_range("2024-07-04", "2025-10-10")
    dates = calendar[(calendar >= "2025-06-03") & (calendar <= "2025-09-30")]
    domain, _ = synthetic_domain()
    records, schedule = [], []
    for p in ("p1", "p2"):
        for d in dates:
            i = calendar.get_loc(d)
            schedule.append(dict(package_id=p, decision_date=d, target_date=calendar[i+1], horizon_end=calendar[i+5],
                original_candidates=50, source_status="AVAILABLE"))
            for k in range(5):
                row = domain.iloc[2*k].to_dict()
                row.update(package_id=p, run_id=p, source_id=p, **{KEY[0]: d, KEY[1]: calendar[i+1]},
                    label_information_end=calendar[i+5], label_cluster=sha([str(d), k]), observed_gap_bps=50.,
                    market_open_gap_bps=10., peer_open_median_bps=30., peer_up_fraction=.6, peer_quote_coverage=1.,
                    valuation_status="AVAILABLE", valuation_reason=None, exit_execution_status="NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF",
                    hypothetical_liquidation_net_bps=100. if k%2 == 0 else -200., mark_to_market_net_bps=106. if k%2 == 0 else -194.,
                    valuation_gross_terminal_ratio=1.02, valuation_path_min_ratio=.95)
                records.append(row)
    rows = pd.DataFrame(records)
    predictions = {a: query(bundle, rows, arm=a) for a in ARMS}
    sources = tuple(SimpleNamespace(package_id=p, manifest_sha256="a"*64, run_id=p, source_id=p) for p in ("p1", "p2"))
    plan = SimpleNamespace(sources=sources, evaluation_start=pd.Timestamp("2025-06-03").date(), evaluation_end=pd.Timestamp("2025-09-30").date())
    return rows, pd.DataFrame(schedule), predictions, plan, [str(d.date()) for d in calendar]


def no_advice(predictions):
    for frame in predictions.values():
        frame["status"], frame["action"] = "UNKNOWN_PEER_SUPPORT", "NO_ADVICE"
        frame[["expected_net_bps", "expected_utility_bps", "expected_terminal_excess_loss_bps"]] = np.nan


def test_all_no_advice_equals_baseline_not_cash_and_cannot_pass_navigation(bundle):
    rows, days, predictions, plan, calendar = evaluation_packet(bundle)
    no_advice(predictions)
    result = evaluate_sector_relative_open_v1(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    aggregate = result["aggregate"]
    assert result["status"] == "EXACT_CANDIDATE_NOT_SUPPORTED"
    for arm in ARMS:
        assert aggregate["cohorts"][arm]["complete_fixed_slot_mean_bps"] == aggregate["cohorts"]["baseline"]["complete_fixed_slot_mean_bps"]
    for attr in aggregate["attribution"].values():
        assert attr["net_increment_bps"] == attr["intervened_unique_stock_days"] == attr["intervention_days"] == 0
    assert aggregate["cohorts"][ARMS[1]]["composition"]["BASELINE_UNCHANGED_NO_ADVICE"] > 0
    assert aggregate["cohorts"][ARMS[1]]["no_advice_fraction"] == 1.
    assert aggregate["cohorts"][ARMS[1]]["known_cash_fraction"] == 0.
    assert not result["deployable"] and not result["independent_oos_evidence"]
    for weekly in result["by_week"].values():
        count = sum(weekly["cohorts"][ARMS[1]]["actions"].values())
        assert count == 10*len(weekly["original_mature_days"])


def test_financial_unknown_is_not_deleted_or_zero_even_if_overlay_skips(bundle):
    rows, days, predictions, plan, calendar = evaluation_packet(bundle)
    no_advice(predictions)
    rows.loc[0, ["hypothetical_liquidation_net_bps", "valuation_gross_terminal_ratio"]] = np.nan
    rows.loc[0, "valuation_status"] = "UNKNOWN"
    result = evaluate_sector_relative_open_v1(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    for cohort in result["aggregate"]["cohorts"].values():
        assert cohort["unknown_group_days"] == 1 and cohort["complete_fixed_slot_mean_bps"] is None
    assert result["evaluation_original_rows"] == len(rows)
    assert result["aggregate"]["paired_statistics"]["baseline"]["simultaneous_95_bonferroni_bps"] is None
    predictions[ARMS[0]].loc[0, "run_id"] = "wrong"
    with pytest.raises(ValueError, match="original slot"):
        evaluate_sector_relative_open_v1(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)


def test_known_intervention_counts_do_not_depend_on_profit_or_nonzero_return(bundle):
    rows, days, predictions, plan, calendar = evaluation_packet(bundle)
    for arm, frame in predictions.items():
        take = arm == ARMS[0]
        frame["status"], frame["action"] = ("ACCEPTABLE_VALUE_PREDICTED", "TAKE") if take else ("AVOID", "SKIP")
        frame["expected_net_bps"] = 10. if take else -10.
        frame["expected_terminal_excess_loss_bps"] = 1.
        frame["expected_utility_bps"] = 9. if take else -11.
    rows.loc[:, "hypothetical_liquidation_net_bps"] = 0.
    result = evaluate_sector_relative_open_v1(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    for attr in result["aggregate"]["attribution"].values():
        assert attr["net_increment_bps"] == 0. and attr["intervention_days"] == result["mature_decision_days"]
        assert attr["intervened_unique_stock_days"] == 5*result["mature_decision_days"]


def stage_fixture(tmp_path, monkeypatch, bundle):
    binding, _, _ = file_binding(tmp_path/"asset")
    sources = []
    for i, p in enumerate(("p1", "p2")):
        sources.append(dict(package_id=p, manifest_sha256="a"*64, run_id=p, source_id=p, context_bit=i,
            prediction_source=dict(package_id=p, manifest_sha256="a"*64, run_id=p, status="FROZEN_SCORE_AVAILABLE",
                descriptor=dict(sha256="a"*64, size_bytes=1))))
    plan = SectorRelativeOpenStudyPlanV1(sources=sources, source_prepared=dict(artifact_uri=str(tmp_path/"original"),
        plan_sha256="a"*64, stage_sha256="a"*64, parent_sha256="a"*64,
        files={n: dict(sha256="a"*64, size_bytes=1) for n in ("rows.parquet", "days.parquet", "calendar.json")}),
        file_source_binding=binding, train_start="2024-07-04", train_end="2025-05-30",
        evaluation_start="2025-06-03", evaluation_end="2025-09-30", implementation_sha256=pipeline.implementation_sha256(),
        source_commit="a"*40, node_python_uri="/existing/python", qe_api_base="http://127.0.0.1:8001/api/v1",
        library_versions={n: "synthetic" for n in ("numpy", "pandas", "scikit-learn", "scipy", "pyarrow", "pydantic")})
    domain, _ = synthetic_domain()
    domain["peer_status"] = "KNOWN"
    eval_rows, eval_days, _, _, calendar = evaluation_packet(bundle)
    monkeypatch.setattr(pipeline, "prepare_sector_inputs_v1", lambda **kw: dict(roster=eval_rows[list(META)], days=eval_days,
        domain=domain, peer_panel=domain.iloc[:0], calendar=calendar,
        encoding=bundle["encoding"], source_receipt=dict(synthetic_only=True), source_stage_sha256="a"*64))
    pipeline.preregister_sector_study_v1(plan=plan, output_root=str(tmp_path/"studies"))
    pipeline.prepare_sector_study_v1(plan=plan, output_root=str(tmp_path/"studies"))
    monkeypatch.setattr(pipeline, "_node", lambda p: dict(synthetic_only=True))
    calls = []
    def fit(**kw):
        kw["before_fit"]()
        calls.append((kw["arm"], kw["component"]))
        value = deepcopy(bundle["models"][kw["arm"]][kw["component"]])
        value["recipe"]["plan_sha256"] = kw["plan_sha256"]
        value["component_sha256"] = sha({k: v for k, v in value.items() if k != "component_sha256"})
        return value
    monkeypatch.setattr(pipeline, "fit_component_v1", fit)
    return plan, tmp_path/"studies", calls


def observation():
    return dict(captured_at=datetime.now(timezone.utc).isoformat(), active_counts=dict(single=1, custom_evo=0, multi_alpha=0))


def test_six_fit_callback_journal_and_exact_resume_do_not_require_QE_idle(bundle, tmp_path, monkeypatch):
    plan, output, calls = stage_fixture(tmp_path, monkeypatch, bundle)
    path = pipeline.train_sector_study_v1(plan=plan, output_root=str(output), qe_observation_probe=observation)
    assert len(calls) == 6 and len(set(calls)) == 6
    assert json.loads((path/"receipt.json").read_bytes())["cumulative_physical_fit_attempts"] == 158
    journal = (path.parent/"fit_journal.jsonl").read_text().splitlines()
    assert len([r for r in journal if json.loads(r)["kind"] == "PHYSICAL_FIT_STARTED"]) == 6
    pipeline.train_sector_study_v1(plan=plan, output_root=str(output), qe_observation_probe=lambda: pytest.fail("no new fit"))
    assert len(calls) == 6


def test_partial_started_is_preserved_and_not_implicitly_refitted(bundle, tmp_path, monkeypatch):
    plan, output, calls = stage_fixture(tmp_path, monkeypatch, bundle)
    def fail(**kw):
        kw["before_fit"]()
        calls.append((kw["arm"], kw["component"]))
        raise RuntimeError("synthetic optimizer interrupted")
    monkeypatch.setattr(pipeline, "fit_component_v1", fail)
    with pytest.raises(RuntimeError, match="interrupted"):
        pipeline.train_sector_study_v1(plan=plan, output_root=str(output), qe_observation_probe=observation)
    with pytest.raises(ValueError, match="TECHNICAL_RECOVERY_REQUIRED"):
        pipeline.train_sector_study_v1(plan=plan, output_root=str(output), qe_observation_probe=observation)
    assert len(calls) == 1


def test_forecast_is_published_before_finance_and_resume_never_refits_or_reloads_context(bundle, tmp_path, monkeypatch):
    plan, output, calls = stage_fixture(tmp_path, monkeypatch, bundle)
    pipeline.train_sector_study_v1(plan=plan, output_root=str(output), qe_observation_probe=observation)
    eval_rows, _, _, _, _ = evaluation_packet(bundle)
    reader = SimpleNamespace(binding_hash=plan.file_source_binding["binding_hash"], assert_unchanged=lambda: None,
        receipt=lambda: dict(synthetic_only=True))
    safe = [*META, *FEATURES, *CONTEXT_COLUMNS, "reference_cny", "observed_gap_bps"]
    monkeypatch.setattr(pipeline, "build_sector_context_inputs_v1", lambda **kw: (eval_rows[safe].copy(), eval_rows[list(KEY)]))
    attempts = []
    def finance(**kw):
        root = output/plan.experiment_id
        assert (root/"forecasts"/"trained"/"contexts.parquet").is_file()
        assert (root/"forecasts"/"trained"/"predictions.parquet").is_file()
        assert not set(kw["inputs"]) & {"hypothetical_liquidation_net_bps", "valuation_status", "label_information_end"}
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("synthetic finance read interruption")
        return eval_rows.copy()
    monkeypatch.setattr(pipeline, "measure_sector_finance_v1", finance)
    with pytest.raises(RuntimeError, match="interruption"):
        pipeline.evaluate_sector_study_v1(plan=plan, output_root=str(output), reader=reader)
    monkeypatch.setattr(pipeline, "build_sector_context_inputs_v1", lambda **kw: pytest.fail("frozen input reread"))
    target = pipeline.evaluate_sector_study_v1(plan=plan, output_root=str(output), reader=reader)
    assert len(calls) == 6 and len(attempts) == 2
    result = json.loads((target/"evaluation.json").read_bytes())
    assert result["evaluation_original_rows"] == len(eval_rows) and not result["test_or_sealed_finance_consumed"]
    monkeypatch.setattr(pipeline, "measure_sector_finance_v1", lambda **kw: pytest.fail("completed outcome reread"))
    assert pipeline.evaluate_sector_study_v1(plan=plan, output_root=str(output)) == target
