"""Continuous consumer protocol, money/position oracles, and complete reader IO."""

# ruff: noqa: E402 -- isolated worker modules live under scripts, as in existing L2 tests.
from copy import deepcopy
from backend.tests.quantevolver.qe_l2_continuous_fixture import make_data, validate_native, _seal, _write
from pathlib import Path
import sys
import types

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
import qe_l2_continuous_admission as admission
from qe_l2_continuous_consumer import (
    ContinuousInputs,
    ContinuousMixin,
    adjust_targets,
    validate_reference,
    project_prediction,
    prediction_panel_sha256,
)
from qe_l2_continuous_plan import build_plan, exposure_reference, baseline_contract
from backend.services.quantevolver.l2_continuous_consumer import (
    extract_continuous, apply_continuous, bind_base_target_hooks,
)
from backend.services.quantevolver.l2_continuous_evaluation import evaluate


@pytest.fixture
def data(tmp_path, monkeypatch):
    return make_data(tmp_path, monkeypatch)


def test_full_reader_and_previous_day_no_double_lag(data):
    inputs = ContinuousInputs(data.root / "manifest.json")
    assert len(inputs.risk["M1"]) == 16375 and inputs.receipt["days"] == 125
    p, row = inputs.probability("SZ000001", "2026-04-01")
    assert p == 0.25 and row["as_of_date"] == "2026-03-31"


@pytest.mark.parametrize(
    "change", ["file", "model", "asof", "missing", "duplicate", "negative", "vintage", "authority", "training"]
)
def test_reader_rejects_tamper_and_incomplete_population(data, change, monkeypatch):
    value = deepcopy(data.risks["M1"])
    if change == "file":
        Path(data.manifest["assets"]["M1"]["path"]).write_text("{}")
        with pytest.raises(ValueError, match="HASH_MISMATCH"):
            ContinuousInputs(data.root / "manifest.json")
        return
    if change == "model":
        value["model_bundles"][0]["model_sha256"] = "0" * 64
    if change == "asof":
        value["predictions"][0]["as_of_date"] = "2026-04-01"
    if change == "missing":
        value["predictions"].pop()
    if change == "duplicate":
        value["predictions"].append(value["predictions"][0])
    if change == "negative":
        value["predictions"][0]["probability"] = -0.1
    if change == "vintage":
        value["predictions"][0]["vintage_id"] = "2026Q3"
    if change == "authority":
        value["input_identity"]["industry_authority_sha256"] = "0" * 64
    if change == "training":
        bundle = value["model_bundles"][0]
        bundle["training_input_identity"] = {"wrong": True}
        value["model_bundles"][0] = _seal({k: v for k, v in bundle.items() if k != "receipt_sha256"})
    value = _seal({k: v for k, v in value.items() if k != "receipt_sha256"})
    ref = _write(data.root / "M1.json", value)
    data.manifest["assets"]["M1"] = ref
    record = {**data.record, "M1_sha256": ref["sha256"]}
    monkeypatch.setattr(admission, "ADMISSIONS", {"TEST_ONLY": record})
    _write(data.root / "manifest.json", data.manifest)
    with pytest.raises(ValueError):
        ContinuousInputs(data.root / "manifest.json")


def test_typed_na_and_classification_na_remain_explicit(data, monkeypatch):
    value = deepcopy(data.risks["M0"])
    row = value["predictions"][0]
    row.update(probability=None, warning=None, availability="unavailable", reason_code="NO_MEMBERS")
    value = _seal({k: v for k, v in value.items() if k != "receipt_sha256"})
    ref = _write(data.root / "M0.json", value)
    data.manifest["assets"]["M0"] = ref
    monkeypatch.setattr(admission, "ADMISSIONS", {"TEST_ONLY": {**data.record, "M0_sha256": ref["sha256"]}})
    _write(data.root / "manifest.json", data.manifest)
    inputs = ContinuousInputs(data.root / "manifest.json")
    p, row = inputs.probability("000001.SZ", "2026-04-01")
    assert p is None and row["reason_code"] == "NO_MEMBERS"
    with pytest.raises(ValueError, match="MAPPING_MISSING"):
        inputs.probability("000002.SZ", "2026-04-01")


def test_unreviewed_admission_and_cross_seed_rejected(data, monkeypatch):
    with pytest.raises(ValueError, match="ADMISSION_UNKNOWN"):
        admission.validate_admission({**data.manifest, "asset_admission_version": "unreviewed"})
    with pytest.raises(ValueError, match="AF_IDENTITY"):
        admission.validate_admission({**data.manifest, "seed": 7})


def test_missing_whole_AF_signal_date_never_shrinks_window(data):
    path = Path(data.manifest["assets"]["prediction"]["path"])
    pred = pd.read_pickle(path)
    pred = pred.loc[pred.index.get_level_values("datetime") != "2026-03-31"]
    pred.to_pickle(path)
    import hashlib

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    data.manifest["assets"]["prediction"]["sha256"] = digest
    data.record["AF_predictions_by_seed"] = {"42": digest}
    _write(data.root / "manifest.json", data.manifest)
    with pytest.raises(ValueError, match="PREDICTION_DATE_GRID"):
        ContinuousInputs(data.root / "manifest.json")


def _projection_case():
    calendar = pd.bdate_range("2026-01-05", "2026-09-30")
    index = pd.MultiIndex.from_product([calendar, ["SH600000", "SZ000001"]], names=["datetime", "instrument"])
    source = pd.DataFrame({"score": np.arange(len(index), dtype=float) / 1000}, index=index)
    target = source.loc[source.index.get_level_values("datetime") >= "2026-03-31"]
    projection = dict(schema_version="qe_l2_af_projection_v1", source_prediction_sha256="a" * 64,
                      source_start="2026-01-05", source_end="2026-09-30", start="2026-03-31", end="2026-09-30",
                      panel_sha256=prediction_panel_sha256(target))
    return source, target, projection, calendar


def test_projection_keeps_every_source_value_and_security_in_declared_window():
    source, expected, projection, calendar = _projection_case()
    before = source.copy(deep=True)
    actual = project_prediction(source, projection, source_sha256="a" * 64, calendar=calendar)
    pd.testing.assert_frame_equal(actual, expected)
    pd.testing.assert_frame_equal(source, before)


@pytest.mark.parametrize("change", ["missing_day", "missing_stock", "nan", "reordered", "values", "pin", "shortened"])
def test_projection_cannot_hide_missing_data_or_change_the_plan(change):
    source, expected, projection, calendar = _projection_case()
    if change == "missing_day":
        source = source.loc[source.index.get_level_values("datetime") != "2026-08-03"]
    elif change == "missing_stock":
        source = source.drop(source.index[-1])
    elif change == "nan":
        source.iloc[-1, 0] = np.nan
    elif change == "reordered":
        source = source.iloc[::-1]
    elif change == "values":
        source.iloc[-1, 0] += 0.1
    elif change == "pin":
        projection["source_prediction_sha256"] = "b" * 64
    else:
        projection["end"] = "2026-09-29"
    with pytest.raises(ValueError, match="QE_L2_CONTINUOUS_"):
        project_prediction(source, projection, source_sha256="a" * 64, calendar=calendar)


def test_projection_must_be_reviewed_for_this_seed(data, monkeypatch):
    _, _, projection, _ = _projection_case()
    projection["source_prediction_sha256"] = data.manifest["assets"]["prediction"]["sha256"]
    manifest = {**data.manifest, "prediction_projection": projection}
    with pytest.raises(ValueError, match="PROJECTION_ADMISSION_MISMATCH"):
        admission.validate_admission(manifest)
    record = {**data.record, "AF_projection_by_seed": {"42": projection}}
    monkeypatch.setattr(admission, "ADMISSIONS", {"TEST_ONLY": record})
    admission.validate_admission(manifest)
    changed = deepcopy(manifest)
    changed["prediction_projection"]["panel_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="PROJECTION_ADMISSION_MISMATCH"):
        admission.validate_admission(changed)


def test_legacy_reader_still_rejects_unapproved_extra_source_dates(data):
    import hashlib

    path = Path(data.manifest["assets"]["prediction"]["path"])
    pred = pd.read_pickle(path)
    extra = pred.iloc[:1].copy()
    extra.index = pd.MultiIndex.from_tuples([(pd.Timestamp("2026-01-05"), "000001.SZ")], names=pred.index.names)
    pd.concat([extra, pred]).sort_index().to_pickle(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    data.manifest["assets"]["prediction"]["sha256"] = digest
    data.record["AF_predictions_by_seed"] = {"42": digest}
    _write(data.root / "manifest.json", data.manifest)
    with pytest.raises(ValueError, match="PREDICTION_DATE_GRID"):
        ContinuousInputs(data.root / "manifest.json")


def test_complete_reader_explicit_projection_preserves_full_parent_asset(data, monkeypatch):
    import hashlib

    m = data.manifest
    calendar_path = Path(m["assets"]["calendar"]["path"])
    prefix = pd.bdate_range("2026-01-05", "2026-03-30")
    calendar = prefix.append(data.calendar)
    calendar_path.write_text("\n".join(str(d.date()) for d in calendar) + "\n", encoding="utf-8")
    calendar_sha = hashlib.sha256(calendar_path.read_bytes()).hexdigest()
    m["assets"]["calendar"]["sha256"] = calendar_sha
    m["dataset_binding"]["day_pins"]["calendar_sha256"] = calendar_sha
    m["release_identity"]["metadata_sha256"]["calendar"] = calendar_sha
    record = deepcopy(data.record)
    record["dataset_binding_sha256"] = admission.canonical_sha(admission.physical_binding(m["dataset_binding"]))
    record["release_identity_sha256"] = admission.canonical_sha(m["release_identity"])
    for arm in ("M0", "M1"):
        risk = deepcopy(data.risks[arm])
        risk["input_identity"]["calendar_sha256"] = calendar_sha
        for index, bundle in enumerate(risk["model_bundles"]):
            bundle["training_input_identity"]["calendar_sha256"] = calendar_sha
            risk["model_bundles"][index] = _seal({k: v for k, v in bundle.items() if k != "receipt_sha256"})
        ref = _write(data.root / f"{arm}.json", _seal({k: v for k, v in risk.items() if k != "receipt_sha256"}))
        m["assets"][arm] = ref
        record[arm + "_sha256"] = ref["sha256"]
    path = Path(m["assets"]["prediction"]["path"])
    target = pd.read_pickle(path)
    extra_index = pd.MultiIndex.from_product([prefix, ["000001.SZ"]], names=target.index.names)
    extra = pd.DataFrame({"score": np.full(len(extra_index), 0.1)}, index=extra_index)
    pd.concat([extra, target]).sort_index().to_pickle(path)
    parent_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    m["assets"]["prediction"]["sha256"] = parent_sha
    projection = dict(schema_version="qe_l2_af_projection_v1", source_prediction_sha256=parent_sha,
                      source_start="2026-01-05", source_end="2026-09-30", start="2026-03-31", end="2026-09-30",
                      panel_sha256=prediction_panel_sha256(target))
    record["AF_predictions_by_seed"] = {"42": parent_sha}
    record["AF_projection_by_seed"] = {"42": projection}
    m["prediction_projection"] = projection
    monkeypatch.setattr(admission, "ADMISSIONS", {"TEST_ONLY": record})
    _write(data.root / "manifest.json", m)
    inputs = ContinuousInputs(data.root / "manifest.json")
    assert len(inputs.mapping) == len(target) == 126
    assert inputs.receipt["days"] == 125 and inputs.receipt["catalog_count"] == 131
    assert inputs.receipt["prediction_projection"] == projection
    assert hashlib.sha256(path.read_bytes()).hexdigest() == parent_sha
    assert len(pd.read_pickle(path)) > 126


def test_absolute_targets_cash_no_normalization_or_compounding():
    base = {"A": 0.6, "B": 0.35}
    prob = {"A": 0.5, "B": 0.2}
    target = adjust_targets(base, prob)
    assert target == pytest.approx({"A": 0.3, "B": 0.28})
    assert 1 - sum(target.values()) == pytest.approx(0.42)
    assert adjust_targets(base, prob) == target
    assert adjust_targets(base, {"A": 0.0, "B": 0.0}) == base
    assert adjust_targets(base, {"A": 1.0, "B": 1.0}) == {"A": 0.0, "B": 0.0}


def test_reference_uses_only_contemporaneous_targets(data):
    m = data.manifest
    actions = [
        {
            "arm": "M0",
            "seed": 42,
            "trade_date": str(day.date()),
            "as_of_date": str(data.calendar[i].date()),
            "target_exposure": 0.5,
            "target_weights": {"A": 0.5},
            "base_contract_sha256": m["base_contract_sha256"],
            "future_return": 999,
        }
        for i, day in enumerate(data.days)
    ]
    ref = exposure_reference(actions, m)
    xm = {**m, "arm": "X0"}
    assert (
        len(validate_reference(ref, xm, {str(d.date()): str(data.calendar[i].date()) for i, d in enumerate(data.days)}))
        == 125
    )
    assert all(set(row) == {"trade_date", "as_of_date", "target_exposure"} for row in ref["rows"])
    ref["rows"][0]["future_return"] = 1
    with pytest.raises(ValueError, match="REFERENCE_CLOCK"):
        validate_reference(ref, xm, {str(d.date()): str(data.calendar[i].date()) for i, d in enumerate(data.days)})


def test_reference_target_feasibility_and_identity(data):
    assert sum(adjust_targets({"A": 0.6, "B": 0.35}, {}, reference_exposure=0.5).values()) == pytest.approx(0.5)
    with pytest.raises(ValueError, match="REFERENCE_INFEASIBLE"):
        adjust_targets({"A": 0.4}, {}, reference_exposure=0.5)
    with pytest.raises(ValueError, match="NUMBER_INVALID"):
        adjust_targets({"A": 0.4}, {"A": float("nan")})


def _conf(m):
    binding = m["dataset_binding"]
    return {
        "qlib_init": {"provider_uri": {"day": binding["provider_uri_day"], "1min": binding["provider_uri_1min"]}},
        "market": "stock_universe",
        "task": {"dataset": {"kwargs": {"segments": {"test": ["2026-03-31", "2026-09-30"]}}}},
        "port_analysis_config": {
            "strategy": {
                "class": "SuspendFilterScoreWeightedTopkStrategyV2",
                "module_path": "qe_suspend_filter_score_weighted_strategy",
                "kwargs": {},
            },
            "backtest": {
                "start_time": "2026-03-31",
                "end_time": "2026-09-30",
                "account": 100_000_000,
                "exchange_kwargs": {"freq": "1min"},
            },
            "executor": {"class": "NestedExecutor", "kwargs": {"inner_executor": {"class": "SimulatorExecutor"}}},
        },
    }


def test_compose_all_five_arms_and_default_identity(data):
    import yaml

    for arm in ["B", "M0", "M1", "X0", "X1"]:
        manifest = {**deepcopy(data.manifest), "arm": arm}
        if arm in ("X0", "X1"):
            source = "M0" if arm == "X0" else "M1"
            actions = [
                {
                    "arm": source,
                    "seed": 42,
                    "trade_date": str(day.date()),
                    "as_of_date": str(data.calendar[i].date()),
                    "target_exposure": 0.5,
                    "target_weights": {"A": 0.5},
                    "base_contract_sha256": manifest["base_contract_sha256"],
                }
                for i, day in enumerate(data.days)
            ]
            manifest["assets"]["reference"] = _write(
                data.root / f"{arm}_reference.json", exposure_reference(actions, {**manifest, "arm": source})
            )
        conf = _conf(manifest)
        original = {
            "experiment_files": {
                "conf.yaml": yaml.safe_dump(conf),
                "custom_strategy.py": (ROOT / "scripts/score_weighted_strategy_v2.py").read_text(encoding="utf-8"),
            },
            "direct_v2_dataset_binding": deepcopy(manifest["dataset_binding"]),
            "wsl_command": "python qrun_limit_minute.py conf.yaml",
        }
        _, study = extract_continuous({"qe_l2_continuous_manifest": manifest})
        result = apply_continuous(original, study, scripts_dir=ROOT / "scripts")
        final = yaml.safe_load(result["experiment_files"]["conf.yaml"])
        assert final["port_analysis_config"]["backtest"]["start_time"] == "2026-04-01"
        assert final["port_analysis_config"]["backtest"]["end_time"] == "2026-09-30"
        assert final["port_analysis_config"]["executor"]["class"] == "BoundedTerminalNestedExecutor"
        assert "qe_l2_continuous_consumer.py" in result["wsl_command"]
    result = {"unchanged": True}
    params, study = extract_continuous({"another": 7})
    assert params == {"another": 7} and apply_continuous(result, study, scripts_dir="missing") is result


def test_compose_rejects_other_overlay_and_rebinding(data):
    with pytest.raises(ValueError, match="OTHER_OVERLAY"):
        extract_continuous({"qe_l2_continuous_manifest": data.manifest, "qe_l2_risk_enabled": True})
    with pytest.raises(ValueError, match="EXECUTION_BINDING"):
        apply_continuous({"direct_v2_dataset_binding": {}}, data.manifest, scripts_dir=ROOT / "scripts")


def test_asset_transport_rejects_unpinned_calendar_before_file_io(data):
    manifest = deepcopy(data.manifest)
    manifest["assets"]["calendar"] = {"path": "SHOULD_NEVER_BE_READ", "sha256": "0" * 64}
    with pytest.raises(ValueError, match="TRANSPORT_PIN_MISMATCH"):
        apply_continuous(
            {"direct_v2_dataset_binding": manifest["dataset_binding"], "experiment_files": {}},
            manifest,
            scripts_dir=ROOT / "scripts",
        )


def test_five_arm_plan_reuses_exact_AF_and_does_not_fabricate_reference(data):
    bases = {}
    manifests = {}
    for seed in [42, 2024, 2026]:
        manifests[seed] = {**data.manifest, "seed": seed}
        bases[seed] = {
            "prediction_replay": True,
            "prediction_source_sha256": data.manifest["assets"]["prediction"]["sha256"],
            "runtime_flags": {"random_seed": seed},
            "data_split": {"test_start": "2026-03-31", "test_end": "2026-09-30", "backtest_end": "2026-09-30"},
            "feature_processing": None,
            "model_params": {},
            "factor_keys": ["AF"],
            "model_id": "frozen_AF",
            "strategy_id": "AF_V2",
            "strategy_params": {"initial_cash": 100_000_000},
            "execution_algo": "TWAP",
            "execution_algo_params": {},
            "label_horizon": 20,
            "universe_selection": {"mode": "stock_universe"},
            "suspend_filter_strict": True,
            "filter_suspended_on_signal": True,
            "disable_alpha158": True,
            "sector_blacklist": [],
            "unfilled_handler": "TAIL_BOOST",
            "unfilled_handler_params": {},
        }
        manifests[seed]["base_contract_sha256"] = baseline_contract(bases[seed])
    data.record["base_contract_sha256"] = manifests[42]["base_contract_sha256"]
    plan = build_plan(bases, manifests)
    assert len(plan["stages"]["models"]) == 9 and plan["stages"]["references"] == [] and plan["new_fits"] == 0
    refs = {(seed, arm): {"path": "TEST_ONLY", "sha256": "0" * 64} for seed in bases for arm in ["X0", "X1"]}
    plan = build_plan(bases, manifests, refs)
    assert [x["loop_index"] for x in plan["stages"]["references"]] == list(range(10, 16))
    assert plan["max_accounts"] == 15 and plan["node_allocation"] == "explicit"
    assert all(x["prediction_replay"] for rows in plan["stages"].values() for x in rows)


def test_actual_account_net_and_exposure_pairing_no_double_fees(data):
    dates = data.days
    accounts = {}
    actions = {}
    for seed in [42, 2024, 2026]:
        for arm in ["B", "M0", "M1", "X0", "X1"]:
            nav = 100_000_000 * np.cumprod(np.repeat(1.001, len(dates)))
            accounts[seed, arm] = pd.DataFrame(
                {"account": nav, "cash": nav * 0.5, "trade_cost": 100.0, "trade_notional": 1000.0}, index=dates
            )
            exposure = 0.95 if arm == "B" else 0.5
            actions[seed, arm] = [
                {
                    "trade_date": str(d.date()),
                    "as_of_date": str(data.calendar[i].date()),
                    "target_exposure": exposure,
                    "target_weights": {"A": exposure},
                    "seed": seed,
                    "arm": arm,
                }
                for i, d in enumerate(dates)
            ]
    benchmarks = {symbol: {str(day.date()): 0.0 for day in dates} for symbol in ["000300.SH", "000985.CSI"]}
    result = evaluate(accounts, actions, dates, benchmarks)
    assert result["account_count"] == 15 and result["new_backtests"] == 0
    row = result["accounts"][0]["periods"]["full"]
    assert row["net_return"] == pytest.approx(1.001**125 - 1)
    assert row["fees_currency"] == 12500.0
    july_august = result["accounts"][0]["periods"]["2026-07_08"]
    expected_days = int(((dates >= "2026-07-01") & (dates <= "2026-08-31")).sum())
    assert july_august["days"] == expected_days
    assert july_august["net_return"] == pytest.approx(1.001**expected_days - 1)
    assert july_august["fees_currency"] == 100.0 * expected_days
    actions[42, "X0"][0]["target_exposure"] = 0.4
    actions[42, "X0"][0]["target_weights"] = {"A": 0.4}
    with pytest.raises(ValueError, match="REFERENCE_EXPOSURE_MISMATCH"):
        evaluate(accounts, actions, dates, benchmarks)


@pytest.fixture
def strategy(monkeypatch):
    class OrderDir:
        BUY = 1
        SELL = 0

    class Order:
        def __init__(self, stock_id, amount, direction, start_time, end_time):
            self.stock_id = stock_id
            self.amount = amount
            self.direction = direction

    decision = types.ModuleType("qlib.backtest.decision")
    decision.Order = Order
    decision.OrderDir = OrderDir
    monkeypatch.setitem(sys.modules, "qlib.backtest.decision", decision)

    class Parent:
        def _adjust_target_weight_map(self, weights, time):
            return dict(weights)

    class Strategy(ContinuousMixin, Parent):
        pass

    obj = object.__new__(Strategy)
    obj._continuous_inputs = types.SimpleNamespace(
        manifest={"arm": "M0", "seed": 42, "base_contract_sha256": "b" * 64}, as_of={"2026-04-01": "2026-03-31"}
    )
    obj._continuous_inputs.probability = lambda stock, day: (0.5, {"availability": "available", "probability": 0.5})
    obj._continuous_reduced = set()
    obj._continuous_day = None
    obj.trade_position = types.SimpleNamespace(get_stock_amount=lambda stock: 1000.0, get_cash=lambda: 10000.0)
    obj.trade_exchange = types.SimpleNamespace(
        get_amount_of_trade_unit=lambda **kwargs: 50.0, open_cost=0.000095, min_cost=5.0
    )
    obj._get_current_price = lambda *args: 10.0
    obj._get_current_factor = lambda *args: 2.0
    obj._is_orderable_without_warning = lambda *args: True
    obj._can_sell_under_hold_thresh = lambda *args: True
    obj.max_single_order_value = 100000.0
    obj.min_trade_price = 0.0
    obj.max_trade_price = 1000.0
    return obj


def _orders(obj, weights, planned=()):
    return obj._build_additional_rebalance_orders(
        weight_map=weights,
        current_holdings=["A"],
        existing_sell_ids=set(),
        planned_buy_orders=list(planned),
        total_account_value=20000.0,
        trade_step=0,
        trade_start_time=pd.Timestamp("2026-04-01"),
        trade_end_time=pd.Timestamp("2026-04-01"),
    )


def test_real_target_delta_amount_not_divided_by_factor_twice(strategy):
    weights = strategy._adjust_target_weight_map({"A": 0.5}, "2026-04-01")
    orders = _orders(strategy, weights)
    assert len(orders) == 1 and orders[0].direction == 0 and orders[0].amount == 500.0
    assert 1000.0 * 10 - orders[0].amount * 10 == weights["A"] * 20000.0


def test_existing_hold_constraints_and_no_duplicate_orders(strategy):
    weights = strategy._adjust_target_weight_map({"A": 0.5}, "2026-04-01")
    strategy._can_sell_under_hold_thresh = lambda *args: False
    assert _orders(strategy, weights) == []
    assert strategy._continuous_day["rebalance"][-1]["reason"] == "HOLD_THRESH"
    strategy._can_sell_under_hold_thresh = lambda *args: True
    planned = types.SimpleNamespace(stock_id="A", amount=1.0)
    assert _orders(strategy, weights, [planned]) == []


def test_na_attempts_exit_with_reason_and_neutral_restoration(strategy):
    strategy._continuous_inputs.probability = lambda *args: (
        None,
        {"availability": "unavailable", "reason_code": "NO_MEMBERS"},
    )
    weights = strategy._adjust_target_weight_map({"A": 0.5}, "2026-04-01")
    orders = _orders(strategy, weights)
    assert weights["A"] == 0 and orders[0].amount == 1000.0
    strategy.trade_position.get_stock_amount = lambda stock: 100.0
    strategy._continuous_inputs.probability = lambda *args: (0.0, {"availability": "available", "probability": 0.0})
    weights = strategy._adjust_target_weight_map({"A": 0.5}, "2026-04-01")
    orders = _orders(strategy, weights)
    assert orders[0].direction == 1 and orders[0].amount == 900.0


def test_neutral_never_changes_legacy_orders_without_prior_reduction(strategy):
    strategy._continuous_inputs.probability = lambda *args: (0.0, {"availability": "available", "probability": 0.0})
    weights = strategy._adjust_target_weight_map({"A": 0.5}, "2026-04-01")
    assert weights == {"A": 0.5} and _orders(strategy, weights) == []


def test_zero_AF_target_and_typed_NA_cannot_keep_risky_position(strategy):
    strategy._continuous_inputs.probability = lambda *args: (
        None,
        {"availability": "unavailable", "reason_code": "NO_MEMBERS"},
    )
    weights = strategy._adjust_target_weight_map({"A": 0.0}, "2026-04-01")
    orders = _orders(strategy, weights)
    assert len(orders) == 1 and orders[0].amount == 1000.0


def test_restoration_reserves_fees_and_never_spends_fictional_sell_cash(strategy):
    strategy._continuous_reduced = {"A"}
    strategy._continuous_inputs.probability = lambda *args: (0.0, {"availability": "available", "probability": 0.0})
    strategy.trade_position.get_stock_amount = lambda stock: 0.0
    strategy.trade_position.get_cash = lambda: 500.0
    weights = strategy._adjust_target_weight_map({"A": 0.5}, "2026-04-01")
    assert _orders(strategy, weights) == []  # One adjusted lot costs 500 plus fee; cash is only 500.
    strategy.trade_position.get_cash = lambda: 505.0
    orders = _orders(strategy, weights)
    assert len(orders) == 1 and orders[0].amount == 50.0


def test_untradable_or_missing_quote_is_audited_not_filled(strategy):
    weights = strategy._adjust_target_weight_map({"A": 0.5}, "2026-04-01")
    strategy._get_current_price = lambda *args: None
    assert _orders(strategy, weights) == []
    assert strategy._continuous_day["rebalance"][-1]["reason"] == "QUOTE_UNAVAILABLE"
    assert strategy._continuous_day["rebalance"][-1]["actual_fill_claimed"] is False


def test_fresh_process_real_qlib_strategy_and_fee_cash_closure(data, monkeypatch):
    pytest.importorskip("qlib")
    validate_native(data, monkeypatch)


LEGACY_BASE_SOURCE = '''
class ScoreWeightedTopkStrategyV2:
    def generate_trade_decision(self):
        trade_step = self.day
        trade_start_time, trade_end_time = self.day, self.day
        current_holdings = list(self.holdings)
        final_holdings = ['A', 'B']
        weights = [0.6, 0.35]
        weight_map = {}
        for i, sid in enumerate(final_holdings):
            weight_map[sid] = weights[i]
        sell_orders = []
        total_account_value = 1000.0
        buy_orders = [Order(sid, total_account_value * weight_map[sid], OrderDir.BUY)
                      for sid in final_holdings if sid not in current_holdings]
        self.backup_candidates = ['C', 'D']
        return TradeDecisionWO(sell_orders + buy_orders, self)
'''


def _load_legacy_base(source):
    from collections import namedtuple

    namespace = {
        "Order": namedtuple("Order", "stock_id amount direction"),
        "OrderDir": types.SimpleNamespace(BUY=1, SELL=0),
        "TradeDecisionWO": lambda orders, _strategy: orders,
    }
    exec(compile(source, "selected_strategy.py", "exec"), namespace)
    return namespace["ScoreWeightedTopkStrategyV2"]


def test_selected_legacy_base_orders_and_backups_preserved_across_held_day():
    adapted, receipt = bind_base_target_hooks(LEGACY_BASE_SOURCE)
    assert receipt["mode"] == "legacy_source_instrumented"
    assert receipt["original_AST_preserved_after_removing_hooks"] is True
    old, new = _load_legacy_base(LEGACY_BASE_SOURCE)(), _load_legacy_base(adapted)()
    for day, holdings in [(0, []), (1, ["A", "B"]), (2, ["A"])]:
        old.day = new.day = day
        old.holdings = new.holdings = holdings
        assert old.generate_trade_decision() == new.generate_trade_decision()
        assert old.backup_candidates == new.backup_candidates == ["C", "D"]


def test_selected_legacy_source_invokes_both_hooks_even_without_new_buys():
    source, _ = bind_base_target_hooks(LEGACY_BASE_SOURCE)

    class Consumer(_load_legacy_base(source)):
        def _adjust_target_weight_map(self, weights, day):
            self.target_days.append(day)
            return {stock: weight * 0.5 for stock, weight in weights.items()}

        def _build_additional_rebalance_orders(self, **kwargs):
            self.rebalance_days.append(kwargs["trade_step"])
            assert sum(kwargs["weight_map"].values()) == pytest.approx(0.475)
            assert kwargs["current_holdings"] == self.holdings
            return []

    obj = Consumer()
    obj.target_days, obj.rebalance_days = [], []
    for obj.day, obj.holdings in [(0, []), (1, ["A", "B"])]:
        orders = obj.generate_trade_decision()
        assert sum(order.amount for order in orders) == (475.0 if obj.day == 0 else 0.0)
    assert obj.target_days == obj.rebalance_days == [0, 1]


@pytest.mark.parametrize("source", [
    "class Other: pass",
    LEGACY_BASE_SOURCE.replace("return TradeDecisionWO(sell_orders + buy_orders, self)", "return None"),
    LEGACY_BASE_SOURCE + "\n    def _adjust_target_weight_map(self, weights, day): return weights\n",
])
def test_unknown_or_partial_base_strategy_rejected(source):
    with pytest.raises(ValueError, match="QE_L2_CONTINUOUS_BASE_"):
        bind_base_target_hooks(source)


def test_selected_repository_base_with_existing_hooks_is_not_replaced():
    source = (ROOT / "scripts/score_weighted_strategy_v2.py").read_text(encoding="utf-8")
    adapted, receipt = bind_base_target_hooks(source)
    assert adapted == source and receipt["mode"] == "existing_hooks"
