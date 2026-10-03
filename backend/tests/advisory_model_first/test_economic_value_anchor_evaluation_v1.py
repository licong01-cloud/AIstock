import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorEstimateV1, ValueAnchorGapSupportV1
from backend.services.advisory_model_first import economic_value_anchor_evaluation_v1 as evaluation
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_value_anchor_evaluation_v1 import _holding_audit, value_anchor_actual_decisions_v1
from backend.tests.advisory_model_first.test_economic_value_anchor_training_v1 import UnitModel, training_fixture

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_value_anchor_labels_v1"]


def args(scene):
    inputs = scene["candidates"].copy()
    for name in D_FEATURES:
        inputs[name] = .5
    inputs["feature_visible_through"] = inputs.decision_as_of_trade_date
    fitted = SimpleNamespace(mean_model=UnitModel(1.04), path_model=UnitModel(.97), constant=ValueAnchorEstimateV1(1.03, .96),
        gap_support=ValueAnchorGapSupportV1(((-500., 500.),)))
    return dict(fitted=fitted, candidates=scene["candidates"], inputs=inputs, prices=scene["prices"], references=scene["references"], identity=scene["parent_identity"], arm="model")


def test_actual_open_decision_has_no_later_OHLC_label_or_maturity_dependency(scene):
    value = args(scene)
    before = value_anchor_actual_decisions_v1(**value)
    value["prices"].loc[:, ["raw_high_cny", "raw_low_cny", "raw_close_cny"]] = .001
    value["inputs"]["gross_value_ratio"], value["inputs"]["label_information_end"] = 999., scene["calendar"][-1]
    after = value_anchor_actual_decisions_v1(**value)
    pd.testing.assert_frame_equal(before, after)
    assert before.market_admissible.all() and before.model_action.eq("TAKE").all()


def test_market_unproven_is_never_research_unknown_baseline_buy(scene):
    value = args(scene)
    affected = value["prices"].trade_date.eq(scene["calendar"][1]) & value["prices"].instrument.eq("000001.SZ")
    value["prices"].loc[affected, "tradability_unknown"] = True
    decision = value_anchor_actual_decisions_v1(**value).iloc[0]
    assert not decision.market_admissible and decision.model_action == "UNAVAILABLE"
    episode = pd.DataFrame([{"episode_id": "held", "instrument": "000001.SZ", "entry_trade_date": scene["calendar"][1], "exit_trade_date": scene["calendar"][6]}])
    assert _holding_audit(episode, value["prices"], scene["calendar"])[0]["reason"] == "HELD_MARK_UNPROVEN"


@pytest.mark.parametrize("defect", ["source", "future_reference", "illegal_quote", "foreign_candidate"])
def test_observed_contradictions_fail_closed(scene, defect):
    value = args(scene)
    if defect == "source":
        value["prices"].loc[0, "source_sha256"] = "9"*64
    elif defect == "future_reference":
        value["references"]["reference_visible_through"] = scene["calendar"][1]
    elif defect == "illegal_quote":
        value["prices"].loc[value["prices"].trade_date.eq(scene["calendar"][1]), "raw_open_cny"] = 20.
    else:
        value["candidates"] = value["candidates"].copy()
        value["candidates"].iloc[0, value["candidates"].columns.get_loc("instrument")] = "999999.SZ"
    with pytest.raises(ValueError):
        value_anchor_actual_decisions_v1(**value)


@pytest.mark.parametrize("blocked", [False, True])
def test_whole_three_arm_evaluation_is_atomic_and_unproven_holding_blocks_comparison(scene, tmp_path, monkeypatch, blocked):
    _, plan = training_fixture()
    root, frozen = tmp_path/plan.experiment_id, tmp_path/"frozen"
    frozen.mkdir()
    rankings = scene["rankings"].copy()
    rankings["is_candidate_decision"] = rankings.decision_as_of_trade_date.eq(scene["calendar"][0])
    if blocked:
        affected = scene["prices"].instrument.eq("000001.SZ") & scene["prices"].trade_date.eq(scene["calendar"][2])
        scene["prices"].loc[affected, "tradability_unknown"] = True
    for name, frame in (("frozen_rankings", rankings), ("prices", scene["prices"]), ("references", scene["references"])):
        (frozen/(name+".parquet")).write_bytes(_parquet_bytes(frame))
    (frozen/"calendar.json").write_bytes(_json_bytes([day.isoformat() for day in scene["calendar"]]))
    registered_path = publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json"))})
    registered = read_stage(registered_path, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    data = args(scene)
    prepared_path = publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"], artifacts={"rows.parquet": _parquet_bytes(data["inputs"])})
    prepared = read_stage(prepared_path, stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
    publish_stage(study_root=root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"], artifacts={"metadata.json": _json_bytes({"unit_fixture_not_real_model": True})})
    parent = SimpleNamespace(configuration=SimpleNamespace(train_start=scene["calendar"][0], label_cutoff=scene["calendar"][-1], test_start=scene["calendar"][0], test_end=scene["calendar"][0]))
    monkeypatch.setattr(evaluation, "load_value_anchor_v1", lambda **kwargs: (plan, root, registered, (parent, frozen, scene["parent_identity"], None)))
    monkeypatch.setattr(evaluation, "load_value_anchor_fit_v1", lambda **kwargs: data["fitted"])
    monkeypatch.setattr(evaluation, "_ledger", lambda *a: None)
    monkeypatch.setattr(evaluation, "_record", lambda *a, **k: None)
    target = evaluation.evaluate_value_anchor_v1(plan_path=registered_path/"plan.json", output_root=tmp_path)
    result = json.loads((target/"evaluation.json").read_text(encoding="utf-8"))
    assert not result["deployable"] and not result["sealed_accessed"]
    if blocked:
        assert result["navigation"] == "BLOCKED_EXECUTION_OR_MARK_UNPROVEN" and result["metrics"] is None and result["increment"] is None
    else:
        assert result["navigation"] == "STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION" and len(result["metrics"]) == 3
        assert result["increment"]["model_minus_baseline"]["mean_bps"] == 0
