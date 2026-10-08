"""Immutable stages, fit ownership and four-arm economic attribution."""
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import io
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.generic_moneyflow_price_5td_contracts_v1 import FEATURES, KEY, GenericMoneyflowPrice5TDPlanV1
from backend.services.advisory_model_first.generic_moneyflow_price_5td_models_v1 import fitted_identity
from backend.services.advisory_model_first import generic_moneyflow_price_5td_pipeline_v1 as pipeline
from backend.services.advisory_model_first.generic_price_5td_pipeline_v1 import GenericPrice5TDPlanV1
from backend.services.advisory_model_first.research_control import evidence_reference_for_file as ref
from backend.tests.advisory_model_first.test_generic_moneyflow_price_5td_models_v1 import frozen_models
from backend.tests.advisory_model_first.test_generic_moneyflow_price_5td_source_v1 import development_frames, projected_development


def idle():
    return dict(captured_at=datetime.now(timezone.utc).isoformat(), running_counts=dict(single=0, custom_evo=0, multi_alpha=0))


def study(tmp_path):
    original, funding, config, calendar = development_frames()
    clean, _, dates, _ = projected_development(tmp_path)
    control, fitted = frozen_models(clean, config)
    cal = tmp_path/"calendar.json"
    cal.write_bytes(_json_bytes(calendar))
    parent = GenericPrice5TDPlanV1(configuration=config, inputs={name: ref(cal, role=name)
        for name in ("calendar", "roster", "raw_daily", "volume_daily", "index_daily")}, dataset_identity="frozen_dataset",
        parent_lineage=("economic_parent",), decision_dates=tuple(pd.Timestamp(day).date() for day in calendar[:61]),
        implementation_sha256="a"*64, source_evidence="RECOVERED_LIMITED_NON_VINTAGE")
    root = tmp_path/"parent"/parent.experiment_id
    digest = None
    for stage, artifacts in (("preregistered", {"plan.json": _json_bytes(parent.model_dump(mode="json"))}),
                             ("prepared", {"rows.parquet": _parquet(original)}),
                             ("trained", {"model.json": _json_bytes(asdict(control))})):
        publish_stage(study_root=root, stage=stage, plan_sha256=parent.plan_sha256, parent_sha256=digest, artifacts=artifacts)
        digest = read_stage(root/stage, stage=stage, plan_sha256=parent.plan_sha256, parent_sha256=digest)["stage_sha256"]
    funding_root = tmp_path/"funding"
    publish_stage(study_root=funding_root, stage="prepared", plan_sha256="b"*64, parent_sha256="c"*64,
                  artifacts={"rows.parquet": _parquet(funding)})
    plan = GenericMoneyflowPrice5TDPlanV1(configuration=config, gp5_plan_ref=ref(root/"preregistered"/"plan.json", role="gp5_plan"),
        gp5_prepared_ref=ref(root/"prepared"/"manifest.json", role="gp5_prepared"),
        gp5_trained_ref=ref(root/"trained"/"manifest.json", role="gp5_trained"),
        moneyflow_prepared_ref=ref(funding_root/"prepared"/"manifest.json", role="moneyflow_prepared"),
        dataset_identity=parent.dataset_identity, parent_lineage=(parent.experiment_id,), decision_dates=dates,
        implementation_sha256=pipeline.implementation_sha256())
    output = tmp_path/"own"
    pipeline.preregister_moneyflow_price_5td_v1(plan=plan, output_root=output)
    pipeline.prepare_moneyflow_price_5td_v1(plan=plan, output_root=output)
    return plan, output, fitted


def _parquet(frame):
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def fake_train(fitted):
    def train(**kwargs):
        for head in ("mean", "path"):
            kwargs["before_fit"](head)
            kwargs["after_fit"](head)
        return fitted
    return train


def test_stage_chain_read_only_parent_and_exact_completed_resume(tmp_path, monkeypatch):
    plan, output, fitted = study(tmp_path)
    monkeypatch.setattr(pipeline, "train_moneyflow_price_5td_v1", fake_train(fitted))
    trained = pipeline.train_moneyflow_price_5td_study_v1(plan=plan, output_root=output, qe_idle_probe=idle)
    journal = (trained.parent/"fit_journal.jsonl").read_bytes()
    records = [json.loads(line) for line in journal.splitlines()]
    assert [record["kind"] for record in records] == ["PHYSICAL_FIT_STARTED", "PHYSICAL_FIT_COMPLETED"]*2
    assert all(set(record["qe_observation"]["running_counts"]) == set(pipeline.QE_PATHS) for record in records)
    pipeline.train_moneyflow_price_5td_study_v1(plan=plan, output_root=output,
        qe_idle_probe=lambda: pytest.fail("completed stage must not probe or fit again"))
    assert (trained.parent/"fit_journal.jsonl").read_bytes() == journal
    evaluated = pipeline.evaluate_moneyflow_price_5td_study_v1(plan=plan, output_root=output)
    result = json.loads((evaluated/"evaluation.json").read_bytes())
    assert result["test"] == "EXCLUDED_NOT_CONSUMED" and result["status"] == "NEGATIVE_STOP_THIS_CANDIDATE"
    records = [json.loads(line) for line in (output/"trial_registry.jsonl").read_bytes().splitlines()]
    assert len(records) == 4 and all(record["consumed_windows"][0]["end_date"] == plan.configuration.validation_end.isoformat() for record in records)
    assert pipeline.evaluate_moneyflow_price_5td_study_v1(plan=plan, output_root=output) == evaluated
    assert pipeline.prepare_moneyflow_price_5td_v1(plan=plan, output_root=output) == trained.parent/"prepared"


def test_busy_zero_fit_and_incomplete_attempt_never_refits(tmp_path, monkeypatch):
    plan, output, fitted = study(tmp_path)
    monkeypatch.setattr(pipeline, "train_moneyflow_price_5td_v1", fake_train(fitted))
    def busy():
        result = idle()
        result["running_counts"]["single"] = 1
        return result
    with pytest.raises(ValueError, match="waits for idle QE"):
        pipeline.train_moneyflow_price_5td_study_v1(plan=plan, output_root=output, qe_idle_probe=busy)
    root = output/plan.experiment_id
    assert not (root/"fit_attempt.json").exists()
    def interrupt(**kwargs):
        kwargs["before_fit"]("mean")
        raise ValueError("simulated interruption")
    monkeypatch.setattr(pipeline, "train_moneyflow_price_5td_v1", interrupt)
    with pytest.raises(ValueError, match="simulated interruption"):
        pipeline.train_moneyflow_price_5td_study_v1(plan=plan, output_root=output, qe_idle_probe=idle)
    with pytest.raises(ValueError, match="incomplete STARTED"):
        pipeline.train_moneyflow_price_5td_study_v1(plan=plan, output_root=output, qe_idle_probe=idle)
    assert len((root/"fit_journal.jsonl").read_bytes().splitlines()) == 1


def test_stale_qe_code_source_and_resource_fail_closed(tmp_path, monkeypatch):
    result = idle()
    result["captured_at"] = (datetime.now(timezone.utc)-timedelta(seconds=61)).isoformat()
    with pytest.raises(ValueError, match="not fresh"):
        pipeline._fresh_qe(lambda: result)
    plan, output, _ = study(tmp_path)
    with pytest.raises(ValueError, match="identity differs"):
        pipeline.prepare_moneyflow_price_5td_v1(plan=plan.model_copy(update={"implementation_sha256": "0"*64}), output_root=output)
    monkeypatch.setattr(pipeline, "RESOURCE_LIMIT_BYTES", 1)
    with pytest.raises(ValueError, match="time/RSS budget"):
        pipeline._budget(pipeline.time.monotonic())
    with pytest.raises(ValueError, match="artifact budget"):
        pipeline._publish(plan, output/plan.experiment_id, "trained", "0"*64, {"model.json": b"{}"})


def test_four_arms_cash_attribution_boundaries_and_no_replacement(tmp_path):
    rows, config, dates, calendar = projected_development(tmp_path)
    control, fitted = frozen_models(rows, config)
    validation = rows.loc[rows[KEY[0]].ge(pd.Timestamp(config.validation_start))].copy()
    # One UNKNOWN stock remains in the Top5; a normal non-executable stock is a separate bucket.
    first = validation.index[0]
    validation.loc[first, list(FEATURES)] = np.nan
    validation.loc[first+1, "label_status"] = "ENTRY_NOT_EXECUTABLE"
    validation.loc[first+1, ["observed_gap_bps", "gross_terminal_ratio", "path_min_ratio"]] = np.nan
    result = pipeline.evaluate_moneyflow_price_5td_cohorts_v1(rows=validation, fitted=fitted, frozen_control=control,
        decision_dates=dates[40:], calendar=calendar, validation_end=config.validation_end)
    assert set(result["arms"]) == {"baseline", "rule_300bps", "frozen_gp5", "moneyflow"}
    assert result["arms"]["moneyflow"]["unknown_cash"] > 0 and result["arms"]["moneyflow"]["not_executable"] == 1
    assert result["arms"]["baseline"]["unsettled_takes"] == 25
    assert len(result["episodes"]) == 100 and len(result["cohorts"]) == 20
    increment = result["increments"]["baseline"]
    assert increment["unknown_cash_difference_sum_bps"] < 0 and increment["known_action_increment_sum_bps"] == 0
    assert result["is_nav"] is False and result["maximum_drawdown"] is None and result["deployable"] is False
    assert pipeline._statistics([0.]*12)["ci95_bps"] == [0., 0.]
    assert pipeline._statistics([0., None]+[0.]*12)["ci95_bps"] is None
    # AVOID can miss profit: higher win rate or fewer trades alone is not a positive result.
    fitted.models["mean"]["initial"] = .8
    avoid = replace(fitted, model_sha256=fitted_identity(fitted))
    result = pipeline.evaluate_moneyflow_price_5td_cohorts_v1(rows=validation, fitted=avoid, frozen_control=control,
        decision_dates=dates[40:], calendar=calendar, validation_end=config.validation_end)
    assert result["increments"]["baseline"]["missed_profit_sum_bps"] > 0
    assert result["status"] == "NEGATIVE_STOP_THIS_CANDIDATE"
