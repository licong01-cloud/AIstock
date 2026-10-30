"""Money-budget invariants, strict real schema and causal reference validation."""
# ruff: noqa: E402 -- worker imports use the same scripts path as the node.
from copy import deepcopy
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
import qe_l2_relative_admission as admission
from qe_l2_relative_consumer import (
    RelativeInputs, relative_targets, target_reference, validate_reference, PRODUCER, TARGET,
)
from backend.services.quantevolver.l2_relative_consumer import extract_relative, apply_relative
from backend.tests.quantevolver.qe_l2_continuous_fixture import make_data, _write, _seal


@pytest.fixture
def data(tmp_path, monkeypatch):
    old = make_data(tmp_path, monkeypatch)
    risk = deepcopy(old.risks["M1"])
    risk.pop("arm")
    risk["schema_version"] = PRODUCER + "_predictions"
    risk["policy_applied"] = False
    contract = {"version": PRODUCER, "target": TARGET, "prediction_projection": "clip_raw_to_0_1"}
    risk["contract"] = contract
    for i, b in enumerate(risk["model_bundles"]):
        b.update(schema_version=PRODUCER + "_model", contract=contract)
        risk["model_bundles"][i] = _seal({k: v for k, v in b.items() if k != "receipt_sha256"})
    for row in risk["predictions"]:
        p = row.pop("probability")
        row.pop("warning")
        row.update(expected_loss=p, raw_expected_loss=p)
    risk = _seal({k: v for k, v in risk.items() if k != "receipt_sha256"})
    m = old.manifest
    del m["assets"]["M0"], m["assets"]["M1"], m["models"]["M0"]
    m.update(contract=admission.CONTRACT, schema_version=admission.VERSION, arm="BSTAR", kappa=0.10,
             max_weight=0.05, base_source_sha256=admission.BASE_SOURCE_SHA)
    m["assets"]["loss"] = _write(tmp_path / "loss.json", risk)
    # Synthetic fixture lacks the production AF projection; explicit test-only
    # registry entry preserves all other complete reader checks.
    m["prediction_projection"] = None
    monkeypatch.setattr(admission, "validate_projection", lambda value, **kwargs: value)
    record = {**admission.identity(m), "AF_predictions_by_seed": {"42": m["assets"]["prediction"]["sha256"]},
              "AF_projection_by_seed": {"42": None}}
    monkeypatch.setattr(admission, "ADMISSIONS", {m["asset_admission_version"]: record})
    _write(tmp_path / "relative.json", m)
    old.relative_risk = risk
    old.relative_record = record
    return old


def test_exact_budget_oracle_and_neutral():
    # Two two-stock groups with unequal starting weights and opposite risks.
    base = {"a": 0.03, "b": 0.01, "c": 0.02, "d": 0.04}
    order = ["b", "a", "c", "d"]
    r = {"a": 0.0, "b": 1.0, "c": 1.0, "d": 0.0}
    target, groups = relative_targets(base, order, r, apply_risk=True)
    assert target == pytest.approx({"a": 0.03075, "b": 0.00925, "c": 0.018666666666666668, "d": 0.04133333333333333})
    assert [g["base_budget"] for g in groups] == pytest.approx([0.04, 0.06])
    assert relative_targets(base, order, r, apply_risk=False)[0] == base
    assert relative_targets(base, order, dict.fromkeys(base, 0.5), apply_risk=True)[0] == base
    assert relative_targets({}, [], {}, apply_risk=True)[0] == {}
    assert relative_targets({"a": .05}, ["a"], {"a": 1.0}, apply_risk=True)[0] == {"a": .05}


def test_unequal_group_property_and_cap_fail_closed():
    rng = np.random.default_rng(17)
    for n in range(2, 52):
        x = rng.uniform(.001, .025, n)
        base = {str(i): float(v) for i, v in enumerate(x)}
        order = list(base)[::-1]
        risk = {s: float(rng.uniform()) for s in order}
        target, groups = relative_targets(base, order, risk, apply_risk=True)
        assert set(target) == set(base)
        for group in groups:
            assert sum(target[s] for s in group["stocks"]) == pytest.approx(group["base_budget"], abs=1e-12)
        assert all(.9 <= target[s] / base[s] <= 1.1 for s in base)
    with pytest.raises(ValueError, match="TARGET_CAP"):
        relative_targets({"a": .05, "b": .01, "c": .01, "d": .01}, list("abcd"),
                         {"a": 0., "b": 1., "c": 0., "d": 0.}, apply_risk=True)


def test_complete_schema_reader_and_no_extra_lag(data):
    inputs = RelativeInputs(data.root / "relative.json")
    assert inputs.receipt["rows"] == 16375 and inputs.receipt["industries"] == 131
    assert inputs.as_of["2026-04-01"] == "2026-03-31"
    targets, groups, rows = inputs.targets("2026-04-01", {"000001.SZ": .03}, ["000001.SZ"])
    assert targets == {"000001.SZ": .03} and len(groups) == 2
    assert rows["000001.SZ"]["availability"] == "available"
    with pytest.raises(ValueError, match="MAPPING_MISSING"):
        inputs.targets("2026-04-01", {"000002.SZ": .03}, ["000002.SZ"])


@pytest.mark.parametrize("change", ["sha", "missing", "duplicate", "clock", "negative", "na", "model", "training"])
def test_reject_structural_or_numeric_risk_fault(data, monkeypatch, change):
    risk = deepcopy(data.relative_risk)
    if change == "sha":
        Path(data.manifest["assets"]["loss"]["path"]).write_text("{}")
        with pytest.raises(ValueError, match="HASH"):
            RelativeInputs(data.root / "relative.json")
        return
    if change == "missing":
        risk["predictions"].pop()
    elif change == "duplicate":
        risk["predictions"].append(risk["predictions"][0])
    elif change == "clock":
        risk["predictions"][0]["as_of_date"] = "2026-04-01"
    elif change == "negative":
        risk["predictions"][0]["expected_loss"] = -0.1
    elif change == "na":
        risk["predictions"][0].update(availability="unavailable", expected_loss=None, reason_code="NO_MEMBERS")
    elif change == "model":
        risk["predictions"][0]["model_sha256"] = "0" * 64
    elif change == "training":
        b = risk["model_bundles"][0]
        b["training_input_identity"] = {"wrong": True}
        risk["model_bundles"][0] = _seal({k: v for k, v in b.items() if k != "receipt_sha256"})
    risk = _seal({k: v for k, v in risk.items() if k != "receipt_sha256"})
    data.manifest["assets"]["loss"] = _write(data.root / "loss.json", risk)
    record = {**data.relative_record, "loss_sha256": data.manifest["assets"]["loss"]["sha256"]}
    monkeypatch.setattr(admission, "ADMISSIONS", {data.manifest["asset_admission_version"]: record})
    _write(data.root / "relative.json", data.manifest)
    with pytest.raises(ValueError):
        RelativeInputs(data.root / "relative.json")


def test_causal_reference_grid_seed_and_no_outcome_fields(data):
    inputs = RelativeInputs(data.root / "relative.json")
    data.manifest["execution_sha256"] = "b" * 64
    actions = [{"contract": admission.CONTRACT, "arm": "BSTAR", "seed": 42,
                "base_contract_sha256": data.manifest["base_contract_sha256"],
                "execution_sha256": "b" * 64,
                "trade_date": day, "as_of_date": prior, "base_targets": {"000001.SZ": .03},
                "target_weights": {"000001.SZ": .03}, "af_order": ["000001.SZ"]}
               for day, prior in inputs.as_of.items()]
    ref = target_reference(actions, data.manifest, source_task_id="qe_test", source_loop_index=1,
                           source_actions_sha256="a" * 64, execution_sha256="b" * 64)
    m = {**data.manifest, "target_source": {k: ref[k] for k in ["source_task_id", "source_loop_index",
         "source_actions_sha256", "execution_sha256"]}}
    assert len(validate_reference(ref, m, inputs.as_of)) == 125
    with pytest.raises(ValueError, match="IDENTITY"):
        validate_reference({**ref, "seed": 2024}, m, inputs.as_of)
    with pytest.raises(ValueError, match="EXECUTION_DRIFT"):
        validate_reference(ref, {**m, "execution_sha256": "c" * 64}, inputs.as_of)
    actions[0]["base_contract_sha256"] = "wrong"
    with pytest.raises(ValueError, match="SOURCE_INVALID"):
        target_reference(actions, data.manifest, source_task_id="qe_test", source_loop_index=1,
                         source_actions_sha256="a" * 64, execution_sha256="b" * 64)
    ref["rows"][0]["actual_fill"] = 1
    with pytest.raises(ValueError, match="CLOCK"):
        validate_reference(ref, m, inputs.as_of)


def test_noop_and_overlay_rejection(data):
    source = {"unchanged": True}
    assert apply_relative(source, None, scripts_dir=ROOT / "scripts") is source
    assert extract_relative({"x": 1}) == ({"x": 1}, None)
    with pytest.raises(ValueError, match="OTHER_OVERLAY"):
        extract_relative({"qe_l2_relative_manifest": data.manifest, "qe_l2_continuous_manifest": {"x": 1}})
    with pytest.raises(ValueError, match="AF_IDENTITY"):
        admission.validate_admission({**data.manifest, "seed": 2024})


def test_original_af_rank_not_target_dictionary_order():
    from types import SimpleNamespace
    from qe_l2_relative_strategy import RelativeMixin
    obj = RelativeMixin.__new__(RelativeMixin)
    obj._relative_inputs = SimpleNamespace(manifest={"arm": "BSTAR"})
    weights = {"b": .02, "a": .03, "c": .04, "zero": 0.}
    actual_rank = pd.Series([.9, .7, .6, .2], index=["c", "a", "b", "zero"])
    assert obj._adjust_target_weight_map(weights, "2026-04-01", ranked=actual_rank) == weights
    assert obj._relative_base == ({"b": .02, "a": .03, "c": .04}, ["c", "a", "b"])


def test_relative_never_trains_or_rebinds_prediction():
    import asyncio
    from backend.tests.unified_engine.test_qe_prediction_replay import _config, _context, _executor, _source
    from backend.services.quantevolver.executors.backtest import BacktestMode
    cfg = _config()
    cfg.model_params_base = {"qe_l2_relative_manifest": {"assets": {"prediction": {"sha256": "wrong"}}}}
    for mode, message in [(BacktestMode.FULL_TRAIN, "REQUIRES_PREDICTION_REPLAY"),
                          (BacktestMode.PREDICTION_REPLAY, "SOURCE_PREDICTION_MISMATCH")]:
        executor, coordinator = _executor()
        with pytest.raises(ValueError, match=message):
            asyncio.run(executor.submit(cfg, _context(_source()), mode=mode))
        executor.composer.compose_experiment_in_memory.assert_not_called()
        assert coordinator.payload is None


def test_fresh_backend_package_import_without_worker_path():
    import subprocess
    code = (f"import sys; sys.path.insert(0, {str(ROOT)!r}); "
            "from backend.services.quantevolver.l2_relative_consumer import extract_relative; "
            "assert extract_relative({'existing': 7}) == ({'existing': 7}, None)")
    result = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
