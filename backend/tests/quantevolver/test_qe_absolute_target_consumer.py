from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "backend/services/quantevolver"))
sys.path.insert(1, str(Path(__file__).resolve().parents[3] / "scripts"))
from qe_absolute_target_strategy import target_orders
from qe_absolute_targets import AbsoluteTargets, SCHEMA, file_sha256, normalize_contract, validate_mode


@pytest.fixture
def targets(tmp_path):
    rows = []
    calendar = ["2026-04-01", "2026-04-02", "2026-04-03"]
    for signal, target in zip(calendar[:-1], calendar[1:]):
        for symbol, sector, state, wt, wb in [("000001.SZ", 1, 1, .1, .05),
                                             ("600001.SH", 2, 0, 0, .05),
                                             ("000002.SZ", 3, -1, 0, 0)]:
            rows.append(dict(signal_date=pd.Timestamp(signal), target_date=pd.Timestamp(target),
                             instrument=symbol, sector=sector, state=state, member_count=1,
                             weight_T=wt, weight_B=wb))
    frame = pd.DataFrame(rows)
    path = tmp_path / "targets.parquet"
    frame.to_parquet(path, index=False, row_group_size=3)
    contract = dict(schema_version=SCHEMA, arm="T", targets=dict(path=str(path), sha256=file_sha256(path)),
                    dataset=dict(release_id="release", manifest_sha256="a" * 64, profile_sha256="b" * 64),
                    window=dict(signal_start=calendar[0], signal_end=calendar[1], account_start=calendar[1],
                                account_end=calendar[2], account_days=2), initial_cash=10000000,
                    execution_reference=dict(path="original.yaml", sha256="c" * 64), sector_ids=[1, 2, 3, 4],
                    known_at="UNVERIFIED")
    return contract, calendar, frame


def rewrite(contract, frame):
    frame.to_parquet(contract["targets"]["path"], index=False, row_group_size=3)
    contract["targets"]["sha256"] = file_sha256(contract["targets"]["path"])


def test_full_window_absolute_values_unknown_and_no_second_lag(targets):
    contract, calendar, _ = targets
    reader = AbsoluteTargets(contract, calendar=calendar)
    target, receipt = reader.day("2026-04-02")
    assert target == {"000001.SZ": .1}
    assert receipt["signal_date"] == "2026-04-01"
    assert len(receipt["sectors"]) == 4
    assert receipt["sectors"][2]["reason"] == "SOURCE_UNKNOWN"
    assert receipt["sectors"][3]["reason"] == "NO_ELIGIBLE_MEMBERS"
    assert receipt["sectors"][2]["availability"] is False
    assert len(reader.coverage) == 2


@pytest.mark.parametrize("defect,reason", [
    ("sha", "HASH_MISMATCH"), ("missing", "MISSING_DAY"), ("lag", "CLOCK_NOT_NEXT_SESSION"),
    ("duplicate", "INSTRUMENT_KEYS"), ("nan", "NULL_OR_EMPTY"), ("unknown", "UNKNOWN_BUDGET"),
    ("exposure", "PAIR_EXPOSURE"), ("leverage", "LEVERAGE"), ("state", "STATE"),
])
def test_corrupt_inputs_fail_closed(targets, defect, reason):
    contract, calendar, frame = targets
    if defect == "sha":
        contract["targets"]["sha256"] = "f" * 64
    else:
        if defect == "missing":
            frame = frame.iloc[:3]
        if defect == "lag":
            frame.loc[:2, "signal_date"] = pd.Timestamp(calendar[1])
        if defect == "duplicate":
            frame.loc[1, "instrument"] = frame.loc[0, "instrument"]
        if defect == "nan":
            frame.loc[0, "weight_T"] = float("nan")
        if defect == "unknown":
            frame.loc[2, "weight_T"] = .01
        if defect == "exposure":
            frame.loc[0, "weight_T"] = .2
        if defect == "leverage":
            frame.loc[0, "weight_T"] = 2
        if defect == "state":
            frame.loc[0, "state"] = 2
        rewrite(contract, frame)
    with pytest.raises(ValueError, match=reason):
        AbsoluteTargets(contract, calendar=calendar)


def test_zero_exposure_keeps_each_account_day(targets):
    contract, calendar, frame = targets
    frame.loc[:, ["weight_T", "weight_B"]] = 0.0
    rewrite(contract, frame)
    reader = AbsoluteTargets(contract, calendar=calendar)
    assert reader.day(calendar[1])[0] == {}
    assert len(reader.coverage) == 2


@pytest.mark.parametrize("key,value", [("model_id", "fake"), ("factor_keys", ["fake"]),
                                      ("prediction_replay", True), ("backtest_only", True),
                                      ("enable_sector_hmm", True), ("ensemble", {"seeds": [42]})])
def test_mode_forbids_fake_model_or_unapproved_combination(targets, key, value):
    contract, _, _ = targets
    with pytest.raises(ValueError):
        validate_mode({"absolute_target_replay": contract, key: value})


def orders(**overrides):
    args = dict(targets={"A": .1}, held={}, nav=100000, cash=100000,
                quote=lambda s: (10.0, 100.0), buy_allowed=lambda s: None, sell_allowed=lambda s: None,
                open_cost=.000095, close_cost=.000595, min_cost=5.0)
    args.update(overrides)
    return target_orders(**args)


def test_absolute_nav_target_without_095_softmax_or_minimum_weight():
    result, events, cash = orders(targets={"A": .001, "B": .2}, quote=lambda s: (1.0, 100.0))
    assert result == [("A", 100.0, 1), ("B", 20000.0, 1)]
    assert events[0]["target_value"] == 100
    assert cash == pytest.approx(100000 - 20100 - 10)


def test_sells_first_exit_oddlots_trim_add_cash_fee_and_buy_block():
    result, events, cash = orders(targets={"A": .1, "B": .2}, held={"B": 3000.0, "C": 153.0}, cash=1000)
    assert result[:2] == [("B", 1000.0, 0), ("C", 153.0, 0)]
    assert result[2] == ("A", 1000.0, 1)
    assert cash >= 0
    assert all(e["actual_fill_claimed"] is False for e in events)
    assert orders(buy_allowed=lambda s: "EVENT_BLOCK")[0] == []
    assert orders(quote=lambda s: None)[0] == []


def test_full_exposure_budget_deducts_fee_and_preserves_residual_cash():
    result, events, cash = orders(targets={"A": 1.0}, nav=10000, cash=10000)
    assert result == [("A", 900.0, 1)]
    assert cash == 995
    assert events[0]["requested_weight"] == 1


def test_model_free_experiment_configuration_and_capacity(targets):
    from backend.services.quantevolver.experiment_config import ExperimentConfig
    from backend.services.quantevolver.qe_batch_execution_policy import (
        bind_batch_model_nodes, batch_node_parallelism, strategy_node_parallelism,
        POLICY_KEY, QE_BATCH_EXECUTION_POLICY,
    )
    from backend.services.quantevolver.seed_contract import ensure_loop_fixed_seed

    contract, _, _ = targets
    cfg = ExperimentConfig(factor_names=[], model_id=None, absolute_target_replay=contract)
    assert cfg.label_horizon is None and cfg.build_custom_params()["absolute_target_replay"] == contract
    assert ensure_loop_fixed_seed({"absolute_target_replay": contract}, context="target") is None
    loops = bind_batch_model_nodes(
        [{"absolute_target_replay": contract, "node_id": "rdagent-node1", "model_id": None}],
        available_nodes=["rdagent-node1"], balanced=False,
        model_lookup=lambda _: pytest.fail("target replay must not read model catalog"))
    assert batch_node_parallelism(loops) == {"rdagent-node1": 4}
    loops[0][POLICY_KEY] = {"version": QE_BATCH_EXECUTION_POLICY}
    assert strategy_node_parallelism(loops, None) == {"rdagent-node1": 4}


def test_manifest_discloses_retrospective_classification(targets):
    contract, _, _ = targets
    bad = copy.deepcopy(contract)
    bad["known_at"] = "STRICT_AS_KNOWN"
    with pytest.raises(ValueError, match="DISCLOSURE"):
        normalize_contract(bad)
    assert json.loads(json.dumps(normalize_contract(contract))) == contract


def result_for(contract):
    return dict(schema_version=SCHEMA, status="COMPLETE_ACCOUNT_AND_TARGET_CONSUMPTION", arm=contract["arm"],
                targets_sha256=contract["targets"]["sha256"], dataset=contract["dataset"], window=contract["window"],
                account_days=2, target_days_consumed=2, new_fit_calls=0, new_predict_calls=0,
                IC=None, RankIC=None, label_status="NOT_APPLICABLE", known_at="UNVERIFIED", actions_sha256="d" * 64)


def test_complete_receipt_required_and_model_free_archive_identity(targets):
    from backend.services.quantevolver.absolute_target_consumer import validate_absolute_result
    from backend.services.qe_archive.asset_lifecycle import _identity_complete

    contract, _, _ = targets
    receipt = result_for(contract)
    assert validate_absolute_result(receipt, contract) == receipt
    for key, value in [("account_days", 1), ("new_predict_calls", 1), ("targets_sha256", "e" * 64)]:
        with pytest.raises(ValueError, match="RESULT_MISMATCH"):
            validate_absolute_result({**receipt, key: value}, contract)
    payload = {"source_id": "task", "config": {"absolute_target_replay": contract}}
    assert _identity_complete(payload, {"dataset_release_id": "release"})
    assert not _identity_complete({"source_id": "task", "config": {"absolute_target_replay": {}}}, {})


def test_results_only_recovery_never_submits_another_account(targets):
    import asyncio
    from backend.services.quantevolver.absolute_target_consumer import recover_absolute_target_results

    contract, _, _ = targets
    result = result_for(contract)
    class Client:
        async def inspect_loop_submission(self, *args):
            return SimpleNamespace(task_id="task", loop_id="Loop1", status="completed",
                                   request_digest="a" * 64, submission_intent_hash="b" * 64)

        async def get_workspace_file(self, task, loop, name):
            return result if name == "qe_absolute_target_result.json" else {"recorder_id": "original_recorder"}

        async def get_loop_metrics(self, *args):
            return {"annualized_return": -.1}

        async def get_enhanced_metrics(self, *args):
            return {"absolute_target_result": result}

    calls = []
    scheduler = SimpleNamespace(
        _register_results_only_loop=lambda **kw: calls.append(kw) or "task_L1",
        _archive_completed_loop_best_effort=lambda *a: None,
        _record_research_backtest_best_effort=lambda *a, **kw: None,
        recompute_custom_evo_task_status=lambda *a: "completed")
    output = asyncio.run(recover_absolute_target_results(scheduler, task_id="task", loop_index=1, task={}, client=Client(),
        effective_node_id="rdagent-node1", config={"absolute_target_replay": contract}))
    assert output["new_fit_predict_backtest"] == 0 and len(calls) == 1
    assert calls[0]["metrics"]["IC"] is None


def test_public_request_is_explicitly_model_free(targets):
    from backend.routers.quantevolver_evolution import CustomEvoLoopConfig

    contract, _, _ = targets
    request = CustomEvoLoopConfig(absolute_target_replay=contract)
    assert request.model_id is None and request.factor_keys == []
    with pytest.raises(ValueError, match="SEED_NOT_APPLICABLE"):
        CustomEvoLoopConfig(absolute_target_replay=contract, runtime_flags={"random_seed": 42})


def record_case(targets, monkeypatch, tmp_path, indicator_frame, fail_analysis=False):
    """Exercise the recorder boundary, including a disabled inner executor."""
    from types import ModuleType
    from qe_absolute_target_record import run_absolute_account

    contract, calendar, _ = targets
    monkeypatch.chdir(tmp_path)
    Path("absolute_targets.parquet").write_bytes(Path(contract["targets"]["path"]).read_bytes())
    Path("qe_absolute_target_manifest.json").write_text(json.dumps(contract), encoding="utf-8")
    actions = "".join(json.dumps({"trade_date": day}) + "\n" for day in calendar[1:])
    Path("qe_absolute_target_actions.jsonl").write_text(actions, encoding="utf-8")
    report = pd.DataFrame({"return": [.01, -.01], "bench": [.001, -.001], "cost": [.0001, .0001]},
                          index=pd.to_datetime(calendar[1:]))
    saved, calls = [], []

    def analyze(frame):
        calls.append(frame)
        if fail_analysis:
            raise ValueError("observed indicator failure")
        assert not frame.empty
        return pd.DataFrame({"value": [.8]}, index=["ffr"])

    def flatten(value):
        return {f"{key}.{name}": metric for key, row in value.items() for name, metric in row.items()}

    modules = {name: ModuleType(name) for name in [
        "qlib", "qlib.backtest", "qlib.contrib", "qlib.contrib.evaluate", "qlib.utils"]}
    modules["qlib.backtest"].backtest = lambda **kwargs: (
        {"1day": (report, {})}, {"1day": (indicator_frame, {}), "1min": (pd.DataFrame(), {})})
    modules["qlib.contrib.evaluate"].indicator_analysis = analyze
    modules["qlib.contrib.evaluate"].risk_analysis = lambda values, freq: pd.DataFrame(
        {"risk": [float(values.sum())]}, index=["mean"])
    modules["qlib.utils"].flatten_dict = flatten
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    recorder = SimpleNamespace(save_objects=lambda **values: saved.append(values), log_metrics=lambda **values: None)
    config = {"absolute_target_replay": contract, "task": {},
              "port_analysis_config": {"executor": {}, "strategy": {}, "backtest": {}}}
    return lambda: run_absolute_account(config, recorder), saved, calls, report


def test_recorder_preserves_empty_inner_indicators_as_unavailable(targets, monkeypatch, tmp_path):
    frame = pd.DataFrame({"count": [1], "deal_amount": [100.], "value": [1000.],
                          "ffr": [.8], "pa": [.01], "pos": [1.]})
    run, saved, calls, report = record_case(targets, monkeypatch, tmp_path, frame)
    result = run()
    assert len(calls) == 1 and calls[0] is frame
    assert saved[0]["report_normal_1day.pkl"] is report
    assert saved[0]["indicators_normal_1min.pkl"].empty
    assert result["indicator_availability"]["1min"] == {
        "available": False, "reason": "NO_ORDER_OBSERVATIONS"}
    assert result["indicator_availability"]["1day"]["available"] is True
    assert result["account_days"] == 2 and result["new_predict_calls"] == 0


def test_recorder_saves_raw_account_before_analysis_failure(targets, monkeypatch, tmp_path):
    frame = pd.DataFrame({"count": [1], "deal_amount": [100.], "value": [1000.],
                          "ffr": [.8], "pa": [.01], "pos": [1.]})
    run, saved, _, report = record_case(targets, monkeypatch, tmp_path, frame, fail_analysis=True)
    with pytest.raises(ValueError, match="observed indicator failure"):
        run()
    assert saved[0]["report_normal_1day.pkl"] is report
    assert not Path("qe_absolute_target_result.json").exists()


def test_recorder_retains_empty_daily_indicator_na_without_fake_count(targets, monkeypatch, tmp_path):
    run, saved, calls, _ = record_case(targets, monkeypatch, tmp_path, pd.DataFrame())
    result = run()
    assert calls == []
    assert result["indicator_availability"]["1day"]["available"] is False
    assert saved[0]["indicators_normal_1day.pkl"].empty


def test_recorder_nonempty_malformed_indicators_fail_without_fake_success(targets, monkeypatch, tmp_path):
    run, saved, _, _ = record_case(targets, monkeypatch, tmp_path, pd.DataFrame({"ffr": [.8]}))
    with pytest.raises(ValueError, match="QE_ABSOLUTE_TARGET_INDICATOR_COLUMNS"):
        run()
    assert "report_normal_1day.pkl" in saved[0]
    assert not Path("qe_absolute_target_result.json").exists()
