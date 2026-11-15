from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.routers.multi_alpha import CombineBacktestRunRequest
from backend.services.multi_alpha.combine_backtest import (
    DEFAULT_WEIGHTING_SCHEMES,
    MultiAlphaCombineBacktestError,
    MultiAlphaCombineBacktestService,
    _PredictionTask,
    _PredictionTaskOutcome,
    build_prediction_only_legs,
    parse_request,
    persisted_backtest_config_for,
    request_snapshot_for,
    roster_hash_for,
)
from backend.services.multi_alpha.durable_execution_adapter import QEWorkspacePredBacktestAdapter
from backend.services.multi_alpha.durable_models import DurableRunSpec, durable_run_request_payload, make_attempt_id, request_hash_for
from backend.services.multi_alpha.durable_orchestrator import _parent_status
from backend.services.multi_alpha.durable_plan import DeterministicChildPlanner


def payload():
    return {
        "roster": [{"leg_id": "AF", "seed_run_ids": ["seed42", "seed2024", "seed2026"]}],
        "weighting_schemes": [],
        "prediction_task_selection": {"include_baseline": True, "include_loo": False, "baseline_only": True},
        "walk_forward": {"enabled": False},
        "normalize_method": "rank",  # A baseline consumes raw scores; no combiner is invoked.
        "oos_start": "2026-04-01", "oos_end": "2026-04-02",
        "backtest_config": {"node_id": "wsl2-5080", "node_parallelism": {"wsl2-5080": 4}},
    }


def frames():
    index = pd.MultiIndex.from_product([pd.to_datetime(["2026-04-01", "2026-04-02"]), ["A", "B", "C"]], names=["datetime", "instrument"])
    return {key: pd.DataFrame({"score": np.array(values, dtype="float32")}, index=index) for key, values in {
        "seed42": [100, -3, 7, 20, 30, 40], "seed2024": [-50, 9, 7, 40, 90, 80], "seed2026": [1, 6, -5, 60, 60, 60],
    }.items()}


def make_run(request):
    config = persisted_backtest_config_for(request)
    identity = dict(roster_hash=roster_hash_for(request.roster),
        roster=request_snapshot_for(request)["roster"], oos_start=request.oos_start, oos_end=request.oos_end,
        normalize_method=request.normalize_method, walk_forward=request.walk_forward, backtest_config=config,
        baseline_leg_id=request.baseline_leg_id, node_parallelism={"wsl2-5080": 4})
    return DurableRunSpec(run_id="macb_baseline_fixture", task_id="mact_baseline_fixture",
        request_hash=request_hash_for(durable_run_request_payload(**identity)), **identity)


def test_rest_snapshot_recovery_and_child_identity_preserve_exact_single_account(tmp_path):
    request = parse_request(CombineBacktestRunRequest(**payload()).model_dump())
    assert request.weighting_schemes == () and request.baseline_leg_id == "AF"
    recovered = QEWorkspacePredBacktestAdapter._request_from_run({"backtest_config_json": persisted_backtest_config_for(request)})
    assert request_snapshot_for(recovered) == request_snapshot_for(request)
    run = make_run(request)
    children = DeterministicChildPlanner.build_child_specs(run_spec=run, request=request)
    assert len(children) == 1 and children[0].child_key == "baseline:AF"
    assert children[0].weighting_scheme is None
    assert children[0].input_manifest["prediction_task_selection"]["baseline_only"] is True
    assert children[0].input_manifest["prediction_source_refs"][0]["seed_run_ids"] == ["seed42", "seed2024", "seed2026"]
    row = {"child_id": children[0].child_id, "child_key": children[0].child_key, "run_id": run.run_id,
           "child_kind": "baseline", "weighting_scheme": None, "input_manifest_json": children[0].input_manifest,
           "input_manifest_hash": children[0].input_manifest_hash}
    repository = SimpleNamespace(get_run=lambda _: {"id": run.run_id, "backtest_config_json": run.backtest_config},
                                 get_child=lambda _: row, get_attempt=lambda _: None)
    sources = frames()
    def no_metrics(**_):
        pytest.fail("single raw baseline must not read labels/IC panels")
    adapter = QEWorkspacePredBacktestAdapter(repository=repository, prediction_loader=sources.__getitem__,
        panel_builder=SimpleNamespace(build_combiner_legs=no_metrics), workspace_root=tmp_path)
    material = adapter.materialize_child_input(run_id=run.run_id, child_id=row["child_id"], attempt_id=make_attempt_id(row["child_id"], 1))
    expected = pd.concat([value.score for value in sources.values()], axis=1).mean(axis=1, skipna=False)
    assert material.prediction_frame.score.dtype == np.dtype("float32")
    np.testing.assert_array_equal(material.prediction_frame.score.to_numpy(), expected.to_numpy())
    assert material.prediction_frame.score.iloc[0] == 17  # raw arithmetic, far outside rank[-1,1]
    assert material.weights == {} and material.per_window_weights == ()
    changed = payload()
    changed["roster"][0]["seed_run_ids"] = ["seed42"]
    assert request_hash_for(request_snapshot_for(parse_request(changed))) != request_hash_for(request_snapshot_for(request))


@pytest.mark.parametrize("mutation", ["second_leg", "no_leg", "scheme", "implicit_schemes", "baseline_off", "loo_on", "rolling", "implicit_rolling", "wrong_baseline", "rank_fusion", "frozen_weights"])
def test_ambiguous_or_mixed_baseline_only_contract_is_rejected(mutation):
    value = payload()
    if mutation == "second_leg":
        value["roster"].append({"leg_id": "other", "seed_run_ids": ["other"]})
    elif mutation == "no_leg":
        value["roster"] = []
    elif mutation == "scheme":
        value["weighting_schemes"] = ["equal"]
    elif mutation == "implicit_schemes":
        value.pop("weighting_schemes")
    elif mutation == "baseline_off":
        value["prediction_task_selection"]["include_baseline"] = False
    elif mutation == "loo_on":
        value["prediction_task_selection"]["include_loo"] = True
    elif mutation == "rolling":
        value["walk_forward"]["enabled"] = True
    elif mutation == "implicit_rolling":
        value.pop("walk_forward")
    elif mutation == "wrong_baseline":
        value["baseline_leg_id"] = "other"
    elif mutation == "rank_fusion":
        value["rank_fusion"] = {"rrf_k": 60}
    elif mutation == "frozen_weights":
        value["frozen_weights"] = {"weights": {"AF": 1}}
    with pytest.raises(MultiAlphaCombineBacktestError, match="baseline_only requires"):
        parse_request(value)


@pytest.mark.parametrize("failure", ["missing_key", "duplicate_key", "nan", "inf", "label_only", "duplicate_seed"])
def test_raw_seed_mean_never_repairs_invalid_samples(failure):
    value = payload()
    sources = frames()
    if failure == "missing_key":
        sources["seed2024"] = sources["seed2024"].iloc[1:]
    elif failure == "duplicate_key":
        sources["seed2024"] = pd.concat([sources["seed2024"], sources["seed2024"].iloc[:1]])
    elif failure in {"nan", "inf"}:
        sources["seed2024"].iloc[0, 0] = float(failure)
    elif failure == "label_only":
        sources["seed2024"] = sources["seed2024"].rename(columns={"score": "label"})
    elif failure == "duplicate_seed":
        value["roster"][0]["seed_run_ids"].append("seed42")
    with pytest.raises(MultiAlphaCombineBacktestError):
        build_prediction_only_legs(parse_request(value), prediction_loader=sources.__getitem__)


def test_old_default_request_shape_and_failed_missing_scheme_still_hold():
    old = payload()
    old["roster"].append({"leg_id": "other", "seed_run_ids": ["other"]})
    old["prediction_task_selection"] = {"include_baseline": True, "include_loo": False}
    request = parse_request(old)
    assert request.weighting_schemes == DEFAULT_WEIGHTING_SCHEMES
    assert "baseline_only" not in request_snapshot_for(request)["prediction_task_selection"]
    old.pop("prediction_task_selection")
    assert "prediction_task_selection" not in request_snapshot_for(parse_request(old))
    old["roster"] = old["roster"][:1]
    with pytest.raises(MultiAlphaCombineBacktestError, match="at least two"):
        parse_request(old)
    child = {"child_id": "macbc_baseline", "child_key": "baseline:AF", "child_kind": "baseline", "status": "succeeded"}
    assert _parent_status(children=[child], run={"status": "running", "baseline_leg_id": "AF"})[0] == "failed"


@pytest.mark.parametrize("status", ["succeeded", "failed", "not_computable", "cancelled"])
def test_durable_parent_requires_real_successful_exact_single_baseline(status):
    request = parse_request(payload())
    run = {"status": "running", "baseline_leg_id": "AF", "backtest_config_json": persisted_backtest_config_for(request)}
    child = {"child_id": "macbc_baseline", "child_key": "baseline:AF", "child_kind": "baseline", "status": status}
    result = _parent_status(children=[child], run=run)[0]
    assert result == ("succeeded" if status == "succeeded" else "failed")
    if status == "succeeded":
        assert _parent_status(children=[{**child, "child_key": "baseline:other"}], run=run)[0] == "failed"
        assert _parent_status(children=[], run=run)[0] == "failed"
        assert _parent_status(children=[child, {**child, "child_id": "extra"}], run=run)[0] == "failed"


def test_legacy_result_requires_explicit_mode_and_real_baseline_metrics():
    service = MultiAlphaCombineBacktestService(repository=SimpleNamespace(), legacy_execution_mode_for_tests=True)
    baseline = _PredictionTask(name="baseline_AF", kind="baseline", frame=pd.DataFrame(), critical=True)
    success = _PredictionTaskOutcome(task=baseline, metrics={"cagr": .1})
    assert service._persist_task_outcomes(run_id="macb_fixture", outcomes=[success], baseline_only=True)["status"] == "succeeded"
    assert service._persist_task_outcomes(run_id="macb_fixture", outcomes=[success])["status"] == "failed"
    failure = _PredictionTaskOutcome(task=baseline, error={"reason_code": "actual_backtest_failed"})
    assert service._persist_task_outcomes(run_id="macb_fixture", outcomes=[failure], baseline_only=True)["status"] == "failed"
    assert service._persist_task_outcomes(run_id="macb_fixture", outcomes=[], baseline_only=True)["status"] == "failed"
