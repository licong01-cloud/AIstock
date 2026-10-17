"""Complete original populations, once-cost attribution and no implicit physical refit."""
from datetime import date, datetime, timezone
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.d_clock_population_price_5td_contracts_v1 import ARMS, FEATURES, KEY, MARK_POLICY_SHA256, SCHEMA_SHA256
from backend.services.advisory_model_first.d_clock_population_price_5td_evaluation_v1 import evaluate_d_clock_population_cohorts_v1
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes


def cohort_inputs():
    calendar = list(pd.bdate_range("2025-06-03", periods=30))
    plan = SimpleNamespace(evaluation_start=calendar[0].date(), evaluation_end=calendar[24].date(),
        sources=[SimpleNamespace(package_id=p, manifest_sha256=p*64, run_id=p, source_id=p) for p in ("a", "b")],
        objective_contract="RISK_MANAGED_ADVISORY", study_type="EXPLORATORY_SCREEN")
    rows, days = [], []
    for package in ("a", "b"):
        for i, d in enumerate(calendar[:25]):
            t, h = calendar[i+1], calendar[i+5]
            days.append(dict(package_id=package, decision_date=d, target_date=t, horizon_end=h, original_candidates=5, source_status="KNOWN_ROSTER"))
            for rank in range(1, 6):
                rows.append(dict(package_id=package, manifest_sha256=package*64, run_id=package, source_id=package,
                    decision_as_of_trade_date=d, target_trade_date=t, instrument=f"{rank:06d}.SZ", selection_effective_rank=rank,
                    candidate_group_size=5, reference_cny=10., label_information_end=h,
                    terminal_mark_ratio=.8 if rank == 1 else 1.1, path_mark_min_ratio=.7 if rank == 1 else .97,
                    mark_label_status="AVAILABLE" if i < 20 else "IMMATURE", mark_label_reason=None,
                    mark_policy_sha256=MARK_POLICY_SHA256, entry_execution_status="NOMINAL_ENTRY_ELIGIBLE_NOT_FILL_PROOF",
                    entry_price_cny=10., observed_gap_bps=0., endpoint_execution_status="NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF",
                    carried_mark_sessions=0, **{n: .02 for n in FEATURES}))
    rows, days = pd.DataFrame(rows), pd.DataFrame(days)
    predictions = {}
    for arm in ARMS:
        frame = rows.loc[:, ["package_id", "manifest_sha256", "run_id", "source_id", *KEY]].copy()
        skip = rows.selection_effective_rank.eq(1) if arm == ARMS[0] else np.zeros(len(rows), dtype=bool)
        frame["status"] = np.where(skip, "AVOID", "ACCEPTABLE")
        frame["expected_net_bps"], frame["profit_probability"], frame["downside_q90_bps"] = np.where(skip, -10., 100.), .6, 100.
        frame["mark_policy_sha256"], frame["schema_sha256"] = MARK_POLICY_SHA256, SCHEMA_SHA256
        predictions[arm] = frame
    return dict(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)


def test_full_fixed_slot_cohorts_have_once_cost_attribution_and_not_nav():
    args = cohort_inputs()
    result = evaluate_d_clock_population_cohorts_v1(**args)
    assert result["original_package_days"] == 50 and result["primary"]["original_mature_days"] == 20
    assert sum(not g["horizon_mature"] for g in result["original_groups"]) == 10
    endpoint = result["primary"]["pairs"]["baseline"]
    expected_avoided_loss = -10000*(.8*(1-5.95/10000)/(1+.95/10000)-1)/5
    assert endpoint["mean_increment_bps"] == pytest.approx(expected_avoided_loss)
    assert endpoint["attribution"]["avoided_loss_bps"] == pytest.approx(expected_avoided_loss)
    assert endpoint["intervention_unique_stock_days"] == 20  # Two package copies are not forty independent interventions.
    assert result["status"] == "EXPLORATORY_PROMISING_NOT_ACTIVATION"
    assert result["activation_evidence"] is False and result["nav_or_annualized_return_claimed"] is False


def test_unknown_entry_is_not_cash_and_original_inference_holes_are_not_compressed():
    args = cohort_inputs()
    args["rows"].loc[args["rows"][KEY[0]].eq(args["rows"][KEY[0]].iloc[0]), "entry_execution_status"] = "UNKNOWN"
    immature = args["rows"].mark_label_status.eq("IMMATURE")
    args["rows"][["terminal_mark_ratio", "path_mark_min_ratio"]] = args["rows"][["terminal_mark_ratio", "path_mark_min_ratio"]].astype(object)
    args["rows"].loc[immature, ["terminal_mark_ratio", "path_mark_min_ratio"]] = "opaque immature outcome"
    result = evaluate_d_clock_population_cohorts_v1(**args)
    panel = result["primary"]["original_daily_panel"]
    assert all(v is None for v in panel[0]["values_bps"].values())
    endpoint = result["primary"]["pairs"]["baseline"]
    assert endpoint["original_mature_days"] == 20 and endpoint["paired_days"] == 19
    assert endpoint["simultaneous_95_bonferroni_bps"] is None and endpoint["mde_80_bps"] is None


def test_model_unknown_cash_is_separately_attributed_without_claiming_known_loss_avoidance():
    args = cohort_inputs()
    forecast = args["predictions"][ARMS[0]]
    selected = args["rows"].selection_effective_rank.eq(2)
    forecast.loc[selected, "status"] = "UNKNOWN_STOCK_INPUT"
    forecast.loc[selected, ["expected_net_bps", "profit_probability", "downside_q90_bps"]] = np.nan
    result = evaluate_d_clock_population_cohorts_v1(**args)
    parts = result["primary"]["pairs"]["baseline"]["attribution"]
    assert parts["unknown_cash_contribution_bps"] < 0
    assert parts["known_action_contribution_bps"] == pytest.approx(parts["avoided_loss_bps"])
    assert parts["missed_profit_bps"] == 0


@pytest.mark.parametrize("conflict", ["duplicate", "policy", "horizon"])
def test_known_roster_and_policy_conflicts_fail_closed(conflict):
    args = cohort_inputs()
    if conflict == "duplicate":
        args["rows"] = pd.concat([args["rows"], args["rows"].iloc[:1]], ignore_index=True)
    elif conflict == "policy":
        args["predictions"][ARMS[0]].loc[0, "schema_sha256"] = "f"*64
    else:
        args["days"].loc[0, "horizon_end"] += pd.offsets.BDay(1)
    with pytest.raises(ValueError):
        evaluate_d_clock_population_cohorts_v1(**args)


def _prepared_stage(root, plan, encoding):
    first = publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None,
        artifacts={"plan.json": _json_bytes({"test": "stage behavior, no DB or market"})})
    header = read_stage(first, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    rows = pd.DataFrame({"pool_"+a: ["STRUCTURE", "ESTIMATION"] for a in ARMS})
    rows[KEY[0]], rows["label_information_end"] = pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-09")
    publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=header["stage_sha256"],
        artifacts={"encoding.json": _json_bytes(encoding), "rows.parquet": parquet_bytes(rows)})


def test_atomic_fit_journal_preserves_completed_arm_and_blocks_implicit_retry(tmp_path, monkeypatch):
    from backend.services.advisory_model_first import d_clock_population_price_5td_model_v1 as model
    from backend.services.advisory_model_first import d_clock_population_price_5td_pipeline_v1 as pipeline
    root = tmp_path/"study"
    plan = SimpleNamespace(plan_sha256="a"*64, train_start=date(2024, 7, 4), train_end=date(2025, 5, 30))
    _prepared_stage(root, plan, {"contrast_status": "PREPARED_IDENTIFIABLE_NO_FIT"})
    monkeypatch.setattr(pipeline, "_checked", lambda p, r: (p, root))
    monkeypatch.setattr(pipeline, "_register", lambda *a, **k: None)
    monkeypatch.setattr(pipeline, "_node", lambda p: {"fixture": "no formal node fit"})
    calls = []
    def fit(**kwargs):
        arm = kwargs["arm"]
        calls.append(arm)
        kwargs["before_fit"](arm)
        kwargs["after_fit"](arm)
        if arm == ARMS[1]:
            raise RuntimeError("technical interruption fixture")
        return SimpleNamespace(model_sha256="b"*64)
    monkeypatch.setattr(model, "train_d_clock_population_model_v1", fit)
    monkeypatch.setattr(model, "d_clock_model_payload_v1", lambda f: {"fixture_model": "not an economic model"})
    def probe():
        return dict(captured_at=datetime.now(timezone.utc).isoformat(), active_counts=dict(single=0, custom_evo=0, multi_alpha=0))
    with pytest.raises(RuntimeError, match="technical interruption"):
        pipeline.train_d_clock_population_study_v1(plan=plan, output_root=tmp_path, qe_idle_probe=probe)
    assert (root/"fits"/ARMS[0]/"trained"/"model.json").is_file()
    journal = [json.loads(line) for line in (root/"fit_journal.jsonl").read_text().splitlines()]
    assert sum(v["kind"] == "PHYSICAL_FIT_STARTED" for v in journal) == 2
    assert sum(v["kind"] == "PHYSICAL_FIT_COMPLETE" for v in journal) == 1
    with pytest.raises(ValueError, match="implicitly refit"):
        pipeline.train_d_clock_population_study_v1(plan=plan, output_root=tmp_path, qe_idle_probe=probe)
    assert calls == list(ARMS)


def test_unidentifiable_contrast_publishes_zero_fit_without_qe_or_outcome_access(tmp_path, monkeypatch):
    from backend.services.advisory_model_first import d_clock_population_price_5td_pipeline_v1 as pipeline
    root = tmp_path/"no_fit"
    plan = SimpleNamespace(plan_sha256="a"*64)
    _prepared_stage(root, plan, {"contrast_status": "NOT_IDENTIFIABLE_SAMPLING_CONTRAST"})
    monkeypatch.setattr(pipeline, "_checked", lambda p, r: (p, root))
    monkeypatch.setattr(pipeline, "_register", lambda *a, **k: None)
    def forbidden(*a, **k):
        raise AssertionError("zero-fit path must not inspect QE, node or evaluation finance")
    monkeypatch.setattr(pipeline, "_node", forbidden)
    monkeypatch.setattr(pipeline, "_public_evaluation_inputs", forbidden)
    pipeline.train_d_clock_population_study_v1(plan=plan, output_root=tmp_path, qe_idle_probe=forbidden)
    stage = pipeline.evaluate_d_clock_population_study_v1(plan=plan, output_root=tmp_path)
    result = json.loads((stage/"evaluation.json").read_bytes())
    assert result["physical_fit_count"] == 0 and result["financial_evaluation_performed"] is False
