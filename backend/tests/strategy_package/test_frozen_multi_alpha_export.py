from __future__ import annotations

import base64
from copy import deepcopy
from datetime import date
import hashlib
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.multi_alpha.combine_backtest import REQUEST_SNAPSHOT_KEY, parse_request, persisted_backtest_config_for
from backend.services.strategy_package.frozen_multi_alpha_contract import prediction_contract
from backend.services.strategy_package.multi_alpha_live import (
    MultiAlphaWeightService, _align_component_frames, _combine_aligned,
    _ensemble_seed_frames, _live_result_to_frame, _normalization_method, _normalize_leg_frame,
)
from backend.services.strategy_package.multi_alpha_promotion import MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION
from backend.services.trading_core.errors import TradingCoreError
from backend.tests.strategy_package.test_multi_alpha_promotion import A1_LEG, FUND_LEG, RUN_ID, _asset_records_from_manifest, _seed_repos, _service


def fixture(tmp_path, monkeypatch, *, zero_leg=True):
    combine, packages, _, _ = _seed_repos()
    run = combine.runs[RUN_ID]
    roster = deepcopy(run["roster_json"])
    weights = {A1_LEG: 0.6966591521, FUND_LEG: 0.3033408479}
    if zero_leg:
        roster.insert(0, {"leg_id": "AF", "seed_run_ids": ["af_reference"]})
        weights = {"AF": 0.0, **weights}
    source = tmp_path / "approved_weights.json"
    source.write_text('{"approved":true,"no_reestimate":true}', encoding="utf-8")
    contract = {"schema_version": "qe_frozen_weights_v1", "weights": weights,
                "source_ref": str(source), "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
    request = parse_request({"roster": roster, "weighting_schemes": ["frozen_weighted"],
        "normalize_method": "rank", "walk_forward": {"enabled": False}, "frozen_weights": contract,
        "prediction_task_selection": {"include_baseline": False, "include_loo": False},
        "oos_start": run["oos_start"], "oos_end": run["oos_end"], "topk": 50, "backtest_config": run["backtest_config_json"]})
    run.update(roster_json=roster, normalize_method="rank", walk_forward_json={"enabled": False}, backtest_config_json=persisted_backtest_config_for(request))
    run["execution_identity_json"] = {"prediction_sources": [{"seed_run_id": seed, "artifact_sha256": hashlib.sha256(seed.encode()).hexdigest()}
        for leg in roster for seed in leg["seed_run_ids"]]}
    result = combine.scheme_results[0]
    combined_path = tmp_path / "original_combined.pkl"
    pd.DataFrame({"score": np.array([.1, .2], dtype="float32")}, index=pd.MultiIndex.from_product([[pd.Timestamp("2026-04-01")], ["A", "B"]], names=["datetime", "instrument"])).to_pickle(combined_path)
    prediction_ref = {"uri": combined_path.as_uri(), "sha256": hashlib.sha256(combined_path.read_bytes()).hexdigest()}
    result.update(id="scheme_frozen", weighting_scheme="frozen_weighted", weights_json={**weights, "combined_prediction_ref": prediction_ref},
                  per_window_weights_json=[{"frozen_weights": contract}])
    dtype_calls = []
    def dtype_contract(seed):
        dtype_calls.append(seed)
        return {"score_dtype": "float32" if "a1" in seed else "float64", "source_prediction_sha256": hashlib.sha256(seed.encode()).hexdigest()}
    monkeypatch.setattr("backend.services.strategy_package.multi_alpha_promotion.prediction_contract", dtype_contract)
    service = _service(combine, packages)
    original_freeze = service.asset_freezer.freeze_manifest_assets
    def freeze(manifest):
        # Preserve already frozen CAS bytes for the assembled parent, as the
        # real freezer does. The older synthetic fixture rehashes every weight
        # by its display label and cannot represent per-seed CAS references.
        if manifest.alpha_mode.value == "multi_alpha":
            return SimpleNamespace(manifest=manifest, assets=_asset_records_from_manifest(manifest))
        return original_freeze(manifest)
    service.asset_freezer.freeze_manifest_assets = freeze
    return combine, packages, service, contract, dtype_calls


def promote(service):
    return service.promote_from_combine_run(combine_backtest_run_id=RUN_ID, weighting_scheme="frozen_weighted",
        scheme_result_id="scheme_frozen", topk=50, weight_policy={"mode": "frozen_backtest_terminal_weights"},
        confirmation=MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION)


def test_new_parent_keeps_contract_seed_bindings_precision_and_zero_leg_provenance(tmp_path, monkeypatch):
    combine, packages, service, contract, calls = fixture(tmp_path, monkeypatch)
    first = promote(service).package
    second = promote(service).package
    manifest = first.current_manifest()
    assert first.package_id == second.package_id and first.manifest_sha256 == second.manifest_sha256
    assert manifest.alpha_combination_policy.method == "frozen_weighted"
    assert set(manifest.alpha_combination_policy.weights) == {A1_LEG, FUND_LEG}
    evidence = manifest.source_evidence["multi_alpha"]
    frozen = evidence["frozen_weight_contract"]
    assert frozen["contract"] == contract and frozen["zero_weight_legs"] == ["AF"]
    assert len(frozen["source_roster"]) == 3
    assert hashlib.sha256(base64.b64decode(frozen["source_document"]["content"])).hexdigest() == contract["source_sha256"]
    assert len(calls) == 4 and "af_reference" not in calls
    for leg in evidence["legs"]:
        assert len(leg["seed_assets"]) == len(leg["seed_run_ids"]) == 1
        assert leg["seed_assets"][0]["score_dtype"] in {"float32", "float64"}
        assert len(leg["seed_assets"][0]["source_prediction_sha256"]) == 64
    assert _normalization_method(manifest) == "rank"
    artifact = MultiAlphaWeightService().weights_for_apply_date(manifest=manifest, apply_date=date(2026, 4, 1), leg_ids=[A1_LEG, FUND_LEG])
    assert artifact.weights == {key: value for key, value in contract["weights"].items() if value > 0}
    assert len(manifest.model_asset) == 2  # AF0 is not an operational model or fabricated runtime leg.


@pytest.mark.parametrize("failure", ["snapshot", "normalization", "actual_weight", "window_provenance", "source_sha", "duplicate_seed", "partial_run", "retired_algo"])
def test_new_export_refuses_contract_or_authority_drift(tmp_path, monkeypatch, failure):
    combine, _, service, _, _ = fixture(tmp_path, monkeypatch)
    run, result = combine.runs[RUN_ID], combine.scheme_results[0]
    if failure == "snapshot":
        run["backtest_config_json"] = {}
    elif failure == "normalization":
        run["normalize_method"] = "zscore"
    elif failure == "actual_weight":
        result["weights_json"][A1_LEG] = 0.5
    elif failure == "window_provenance":
        result["per_window_weights_json"] = []
    elif failure == "source_sha":
        source = run["backtest_config_json"][REQUEST_SNAPSHOT_KEY]["frozen_weights"]["source_ref"]
        from pathlib import Path
        Path(source).write_text("changed", encoding="utf-8")
    elif failure == "duplicate_seed":
        run["roster_json"][1]["seed_run_ids"].append(run["roster_json"][1]["seed_run_ids"][0])
    elif failure == "partial_run":
        run["status"] = "partial_failed"
    elif failure == "retired_algo":
        run["backtest_config_json"]["execution_algo"] = "V25_1_SMALL_CAP"
        run["backtest_config_json"][REQUEST_SNAPSHOT_KEY]["backtest_config"]["execution_algo"] = "V25_1_SMALL_CAP"
    with pytest.raises(TradingCoreError):
        promote(service)


def test_typed_mean_rank_and_fixed_sum_match_independent_oracle():
    day = date(2026, 4, 1)
    values = [[1 + 2**-25, 1, 3], [1 + 2**-25, 1, 5]]
    seeds = {}
    for index, scores in enumerate(values):
        seeds[str(index)] = _live_result_to_frame([{"symbol": s, "score": value} for s, value in zip(["A", "B", "C"], scores)],
            package_id="parent", leg_id="LSTM", model_id="m", seed_run_id=str(index), trade_date=day, strict=True, score_dtype="float32")
    ensemble = _ensemble_seed_frames(seeds, leg_id="LSTM", model_id="m", package_id="parent", trade_date=day, strict=True)
    assert ensemble.score.dtype == np.dtype("float32")
    assert ensemble.score.iloc[0] == ensemble.score.iloc[1]  # Original float32 tie must survive Python float rows.
    lstm = _normalize_leg_frame(ensemble, package_id="parent", leg_id="LSTM", model_id="m", method="rank")
    fund = _normalize_leg_frame(pd.DataFrame({"trade_date": [day] * 3, "instrument": ["A", "B", "C"], "score": [3., 2., 1.]}),
        package_id="parent", leg_id="FUND", model_id="f", method="rank")
    aligned = _align_component_frames({"LSTM": lstm, "FUND": fund}, strict=True)
    weights = {"LSTM": .6966591521, "FUND": .3033408479}
    actual = _combine_aligned(aligned, weights=weights, leg_ids=["LSTM", "FUND"])
    raw_mean = pd.DataFrame(np.asarray(values, dtype="float32").T).mean(axis=1, skipna=False)
    oracle_lstm = 2 * (raw_mean.rank(method="average") - 1) / 2 - 1
    oracle = weights["LSTM"] * oracle_lstm + weights["FUND"] * pd.Series([1., 0., -1.])
    np.testing.assert_array_equal(actual.combined_score.to_numpy(), oracle.to_numpy())
    serialized = _combine_aligned(aligned, weights=weights, leg_ids=["LSTM", "FUND"], score_dtype="float32")
    assert serialized.combined_score.dtype == np.dtype("float32")
    np.testing.assert_array_equal(serialized.combined_score.to_numpy(), oracle.to_numpy().astype("float32"))
    with pytest.raises(TradingCoreError, match="intersection is forbidden"):
        _align_component_frames({"LSTM": lstm, "FUND": fund.iloc[1:]}, strict=True)
    with pytest.raises(TradingCoreError, match="keys differ"):
        _ensemble_seed_frames({"one": seeds["0"], "two": seeds["1"].iloc[1:]}, leg_id="LSTM", model_id="m", package_id="p", trade_date=day, strict=True)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), None])
def test_frozen_live_rows_do_not_skip_invalid_scores(bad):
    with pytest.raises(TradingCoreError):
        _live_result_to_frame([{"symbol": "A", "score": 1.}, {"symbol": "B", "score": bad}],
            package_id="p", leg_id="l", model_id="m", seed_run_id="s", trade_date=date(2026, 4, 1), strict=True, score_dtype="float32")


def test_actual_prediction_dtype_contract_reads_hash_without_model_calls(tmp_path, monkeypatch):
    index = pd.MultiIndex.from_product([[pd.Timestamp("2026-04-01")], ["A", "B"]], names=["datetime", "instrument"])
    path = tmp_path / "pred.pkl"
    pd.DataFrame({"score": np.array([1, 2], dtype="float32")}, index=index).to_pickle(path)
    monkeypatch.setattr("backend.services.model_store.ModelStoreService.prediction_path", lambda self, **_: path)
    result = prediction_contract("real_saved_fixture")
    assert result == {"score_dtype": "float32", "source_prediction_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
