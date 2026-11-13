from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.routers.multi_alpha import (
    CombineBacktestRunRequest,
    CombinePreviewRequest,
    preview_multi_alpha_combination,
)
from backend.services.multi_alpha.combine_backtest import (
    MultiAlphaCombineBacktestError,
    build_prediction_only_legs,
    combine_legs,
    parse_request,
    persisted_backtest_config_for,
    request_snapshot_for,
    roster_hash_for,
)
from backend.services.multi_alpha.combiner import CombinerLeg, MultiAlphaCombiner, MultiAlphaCombinerError
from backend.services.multi_alpha.durable_execution_adapter import QEWorkspacePredBacktestAdapter
from backend.services.multi_alpha.durable_models import (
    DurableRunSpec,
    durable_run_request_payload,
    make_attempt_id,
    request_hash_for,
)
from backend.services.multi_alpha.durable_plan import DeterministicChildPlanner


WEIGHTS = {"LSTM": 0.6966591521, "FUND": 0.3033408479}


def _contract():
    return {
        "schema_version": "qe_frozen_weights_v1",
        "weights": dict(WEIGHTS),
        "source_ref": "approved-contract-test-fixture",
        "source_sha256": "a" * 64,
    }


def _frames():
    return {
        "lstm_source": pd.DataFrame(
            {"trade_date": ["2026-04-01"] * 3, "instrument": ["A", "B", "C"], "score": [1.0, 2.0, 3.0]}
        ),
        "fund_source": pd.DataFrame(
            {"trade_date": ["2026-04-01"] * 3, "instrument": ["A", "B", "C"], "score": [2.0, 1.0, 3.0]}
        ),
    }


def _payload():
    return {
        "roster": [
            {"leg_id": "LSTM", "seed_run_ids": ["lstm_source"]},
            {"leg_id": "FUND", "seed_run_ids": ["fund_source"]},
        ],
        "weighting_schemes": ["frozen_weighted"],
        "normalize_method": "rank",
        "walk_forward": {"enabled": False},
        "frozen_weights": _contract(),
        "prediction_task_selection": {"include_baseline": False, "include_loo": False},
        "oos_start": "2026-04-01",
        "oos_end": "2026-09-30",
        "backtest_config": {"node_id": "rdagent-node1", "node_parallelism": {"rdagent-node1": 4}},
    }


def _legs():
    frames = _frames()
    return [CombinerLeg("LSTM", frames["lstm_source"], ic=-100.0), CombinerLeg("FUND", frames["fund_source"], ic=100.0)]


def test_known_rank_arithmetic_and_preview_ignore_oos_metrics():
    result = MultiAlphaCombiner().combine(
        legs=_legs(),
        weighting_scheme="frozen_weighted",
        normalize_method="rank",
        frozen_weights=_contract(),
    )
    assert dict(zip(result.combined_score_frame.instrument, result.combined_score_frame.combined_score)) == {
        "A": -WEIGHTS["LSTM"],
        "B": -WEIGHTS["FUND"],
        "C": 1.0,
    }
    assert result.weights == WEIGHTS
    request = CombinePreviewRequest(
        legs=[{"id": leg.leg_id, "pred_frame": leg.pred_frame.to_dict("records"), "ic": leg.ic} for leg in _legs()],
        weighting_scheme="frozen_weighted",
        normalize_method="rank",
        frozen_weights=_contract(),
    )
    data = preview_multi_alpha_combination(request)["data"]
    assert data["weights"] == WEIGHTS
    assert data["summary"]["frozen_weights"] == _contract()


def test_daily_ties_single_stock_and_row_order_use_original_scale():
    left = pd.DataFrame(
        {
            "trade_date": ["2026-04-01"] * 3 + ["2026-04-02"],
            "instrument": ["A", "B", "C", "A"],
            "score": [3.0, 3.0, 1.0, 9.0],
        }
    )
    right = left.copy()
    right["score"] = [1.0, 2.0, 3.0, -8.0]
    result = MultiAlphaCombiner().combine(
        legs=[CombinerLeg("LSTM", left.iloc[::-1]), CombinerLeg("FUND", right)],
        weighting_scheme="frozen_weighted",
        normalize_method="rank",
        frozen_weights=_contract(),
    )
    assert result.combined_score_frame.combined_score.tolist() == pytest.approx(
        [
            0.5 * WEIGHTS["LSTM"] - WEIGHTS["FUND"],
            0.5 * WEIGHTS["LSTM"],
            -WEIGHTS["LSTM"] + WEIGHTS["FUND"],
            0.0,
        ]
    )


@pytest.mark.parametrize(
    "weights",
    [
        {"LSTM": 0.6},
        {"LSTM": 0.6, "FUND": 0.4, "extra": 0.0},
        {"LSTM": -0.1, "FUND": 1.1},
        {"LSTM": True, "FUND": 0.0},
        {"LSTM": float("nan"), "FUND": 0.3},
        {"LSTM": float("inf"), "FUND": 0.3},
        {"LSTM": 10**400, "FUND": 0.3},
        {"LSTM": 0.6, "FUND": 0.3},
        {"LSTM": "0.6", "FUND": 0.4},
    ],
)
def test_invalid_weights_fail_before_submission(weights):
    payload = _payload()
    payload["frozen_weights"]["weights"] = weights
    with pytest.raises(MultiAlphaCombineBacktestError, match="frozen_weights"):
        parse_request(payload)


@pytest.mark.parametrize(
    "update",
    [
        {"normalize_method": "zscore"},
        {"walk_forward": {"enabled": True}},
        {"walk_forward": {}},
        {"weighting_schemes": ["equal", "frozen_weighted"]},
        {"weighting_schemes": ["ic_weighted"]},
        {"rank_fusion": {"rrf_k": 60}},
    ],
)
def test_incompatible_contract_is_rejected(update):
    payload = _payload()
    payload.update(update)
    with pytest.raises(MultiAlphaCombineBacktestError):
        parse_request(payload)


@pytest.mark.parametrize("key,value", [("source_sha256", "unknown"), ("source_ref", ""), ("schema_version", "future")])
def test_missing_provenance_is_not_a_default(key, value):
    payload = _payload()
    payload["frozen_weights"][key] = value
    with pytest.raises(MultiAlphaCombineBacktestError):
        parse_request(payload)


@pytest.mark.parametrize("kind", ["missing", "duplicate", "nan", "inf", "label", "null_instrument"])
def test_prediction_corruption_is_not_cropped_or_filled(kind):
    legs = _legs()
    frame = legs[1].pred_frame.copy()
    if kind == "missing":
        frame = frame.iloc[1:]
    elif kind == "duplicate":
        frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
    elif kind in {"nan", "inf"}:
        frame.loc[0, "score"] = float(kind)
    elif kind == "label":
        frame = frame.rename(columns={"score": "label"})
    else:
        frame.loc[0, "instrument"] = None
    legs[1] = CombinerLeg("FUND", frame)
    with pytest.raises(MultiAlphaCombinerError):
        MultiAlphaCombiner().combine(
            legs=legs, weighting_scheme="frozen_weighted", normalize_method="rank", frozen_weights=_contract()
        )


def test_snapshot_retry_and_request_hash_preserve_exact_contract():
    request = parse_request(CombineBacktestRunRequest(**_payload()).model_dump())
    snapshot = request_snapshot_for(request)
    assert snapshot["frozen_weights"] == _contract()
    recovered = QEWorkspacePredBacktestAdapter._request_from_run(
        {"backtest_config_json": persisted_backtest_config_for(request)}
    )
    assert request_snapshot_for(recovered) == snapshot
    changed = deepcopy(snapshot)
    changed["frozen_weights"]["weights"] = {"LSTM": 0.5, "FUND": 0.5}
    assert request_hash_for(request_snapshot_for(parse_request(changed))) != request_hash_for(snapshot)
    legacy = _payload()
    legacy.pop("frozen_weights")
    legacy["weighting_schemes"] = ["equal"]
    assert "frozen_weights" not in request_snapshot_for(parse_request(legacy))


def test_real_durable_materializer_uses_saved_predictions_without_metrics(tmp_path):
    calls = []
    frames = _frames()

    def load(run_id):
        calls.append(run_id)
        return frames[run_id]

    def no_metrics(**kwargs):
        pytest.fail("frozen weights must not read OOS label/IC panels")

    adapter = QEWorkspacePredBacktestAdapter(
        prediction_loader=load,
        panel_builder=SimpleNamespace(build_combiner_legs=no_metrics),
        workspace_root=tmp_path,
    )
    request = parse_request(_payload())
    first, weights, evidence = adapter._materialize_prediction(
        child={"child_kind": "scheme", "weighting_scheme": "frozen_weighted"},
        request=request,
    )
    retry, retry_weights, retry_evidence = adapter._materialize_prediction(
        child={"child_kind": "scheme", "weighting_scheme": "frozen_weighted"},
        request=request,
    )
    pd.testing.assert_frame_equal(first, retry)
    assert first.score.tolist() == [-WEIGHTS["LSTM"], -WEIGHTS["FUND"], 1.0]
    assert calls == ["lstm_source", "fund_source"] * 2
    assert weights == retry_weights == WEIGHTS
    assert evidence == retry_evidence
    assert evidence[0]["frozen_weights"] == _contract()


def test_loader_refuses_nan_before_seed_aggregation():
    frames = _frames()
    frames["fund_source"].loc[0, "score"] = float("nan")
    with pytest.raises(MultiAlphaCombineBacktestError, match="invalid or duplicate"):
        build_prediction_only_legs(parse_request(_payload()), prediction_loader=frames.__getitem__)


def test_seed_mean_preserved_but_missing_seed_keys_rejected():
    payload = _payload()
    payload["roster"][0]["seed_run_ids"].append("lstm_source2")
    frames = _frames()
    frames["lstm_source2"] = frames["lstm_source"].copy()
    frames["lstm_source2"]["score"] *= 3
    request = parse_request(payload)
    legs = build_prediction_only_legs(request, prediction_loader=frames.__getitem__)
    assert legs[0].pred_frame.score.tolist() == [2.0, 4.0, 6.0]
    frames["lstm_source2"] = frames["lstm_source2"].iloc[1:]
    with pytest.raises(MultiAlphaCombineBacktestError, match="keys differ"):
        build_prediction_only_legs(request, prediction_loader=frames.__getitem__)


def test_old_ic_weighting_still_uses_original_metric_rule():
    result = MultiAlphaCombiner().combine(legs=_legs(), weighting_scheme="ic_weighted", normalize_method="rank")
    assert result.weights == {"LSTM": 0.0, "FUND": 1.0}
    assert combine_legs(legs=_legs(), scheme="frozen_weighted", request=parse_request(_payload())).weights == WEIGHTS


def test_public_materializer_and_planner_produce_exactly_one_account(tmp_path):
    request = parse_request(_payload())
    config = persisted_backtest_config_for(request)
    run_id = "macb_frozen_fixture"
    run = {"id": run_id, "backtest_config_json": config}
    identity = durable_run_request_payload(
        roster_hash=roster_hash_for(request.roster),
        roster=request_snapshot_for(request)["roster"],
        oos_start=request.oos_start,
        oos_end=request.oos_end,
        normalize_method=request.normalize_method,
        walk_forward=request.walk_forward,
        backtest_config=config,
    )
    spec = DurableRunSpec(
        run_id=run_id,
        task_id="mact_frozen_fixture",
        request_hash=request_hash_for(identity),
        roster_hash=roster_hash_for(request.roster),
        roster=request_snapshot_for(request)["roster"],
        oos_start=request.oos_start,
        oos_end=request.oos_end,
        normalize_method=request.normalize_method,
        walk_forward=request.walk_forward,
        backtest_config=config,
    )
    children = DeterministicChildPlanner.build_child_specs(run_spec=spec, request=request)
    assert len(children) == 1
    child = children[0]
    row = {
        "child_id": child.child_id,
        "run_id": run_id,
        "child_kind": child.child_kind,
        "weighting_scheme": child.weighting_scheme,
        "input_manifest_json": child.input_manifest,
        "input_manifest_hash": child.input_manifest_hash,
    }
    repository = SimpleNamespace(get_run=lambda key: run, get_child=lambda key: row, get_attempt=lambda key: None)
    frames = _frames()
    adapter = QEWorkspacePredBacktestAdapter(
        repository=repository, prediction_loader=frames.__getitem__, workspace_root=tmp_path
    )
    material = adapter.materialize_child_input(
        run_id=run_id,
        child_id=child.child_id,
        attempt_id=make_attempt_id(child.child_id, 1),
    )
    assert material.prediction_frame.score.tolist() == [-WEIGHTS["LSTM"], -WEIGHTS["FUND"], 1.0]
    assert material.per_window_weights[0]["frozen_weights"] == _contract()
    assert material.request.frozen_weights == request.frozen_weights


def test_three_leg_loo_cannot_renormalize_frozen_weights():
    payload = _payload()
    payload["roster"].append({"leg_id": "third", "seed_run_ids": ["third_source"]})
    payload["frozen_weights"]["weights"] = {"LSTM": 0.4, "FUND": 0.3, "third": 0.3}
    payload["prediction_task_selection"]["include_loo"] = True
    with pytest.raises(MultiAlphaCombineBacktestError, match="no multi-leg LOO"):
        parse_request(payload)
