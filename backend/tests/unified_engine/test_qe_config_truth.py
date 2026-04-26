
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.routers.quantevolver_evolution import _merge_strategy_runtime_flags, _reject_nested_runtime_flags
from backend.execution_algos.v25_two_stage_algo import V25TwoStageAlgo, V25TwoStageUnavailableError
from backend.services.quantevolver.config_composer import ConfigComposer
from backend.services.quantevolver.stock_pool_sync import sync_stock_pool_to_remote_node

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from qe_suspend_filter import QESuspendFilter  # noqa: E402


DATA_SPLIT = {
    "train_start": "2020-01-01",
    "train_end": "2020-12-31",
    "valid_start": "2021-01-01",
    "valid_end": "2021-06-30",
    "test_start": "2021-07-01",
    "test_end": "2021-12-31",
    "backtest_end": "2021-12-31",
}


def _base_yaml(**kwargs):
    params = {
        "factors_info": [],
        "model_info": None,
        "strategy_info": None,
        "data_split": DATA_SPLIT,
        "custom_params": {},
        "has_custom_factors": False,
        "has_alpha158": False,
        "backtest_freq": "1min",
    }
    params.update(kwargs)
    return ConfigComposer()._compose_conf_yaml(**params)


def test_v25_execution_algo_generates_v25_inner_strategy():
    yaml_text = _base_yaml(
        execution_algo="V25_TWO_STAGE",
        execution_algo_params={"device": "cpu"},
    )

    assert "class: TailTWAPWithV25TwoStageStrategy" in yaml_text
    assert "module_path: tail_twap_v25_strategy" in yaml_text
    assert "effective_algo: V25_TWO_STAGE" in yaml_text
    assert "early_model_path:" in yaml_text
    assert "late_model_path:" in yaml_text


def test_v25_backend_algo_fails_before_optional_runtime_fallback():
    with pytest.raises(V25TwoStageUnavailableError, match="early_model_path"):
        V25TwoStageAlgo({})


def test_execution_algo_unknown_and_vwap_fail_fast():
    with pytest.raises(ValueError, match="unsupported for QE"):
        ConfigComposer._normalize_execution_algo("POV")
    with pytest.raises(ValueError, match="VWAP"):
        ConfigComposer._normalize_execution_algo("VWAP")


def test_backtest_freq_must_match_execution_algo():
    assert ConfigComposer._resolve_backtest_freq("CLOSE_PRICE", {}) == "day"
    assert ConfigComposer._resolve_backtest_freq("V25_TWO_STAGE", {}) == "1min"
    with pytest.raises(ValueError, match="requires backtest_freq=1min"):
        ConfigComposer._resolve_backtest_freq("V25_TWO_STAGE", {"backtest_freq": "day"})
    with pytest.raises(ValueError, match="requires backtest_freq=day"):
        ConfigComposer._resolve_backtest_freq("CLOSE_PRICE", {"backtest_freq": "1min"})
    with pytest.raises(ValueError, match="requires backtest_freq=1min"):
        ConfigComposer._resolve_backtest_freq(None, {"backtest_freq": "day"})


def test_suspend_filter_wraps_topk_strategy():
    yaml_text = _base_yaml(
        custom_params={
            "topk": 20,
            "n_drop": 2,
            "filter_suspended_on_signal": True,
            "suspend_filter_file": "qe_suspend_filter.json",
            "suspend_filter_strict": True,
        },
    )

    assert "class: SuspendFilterTopkDropoutStrategy" in yaml_text
    assert "module_path: qe_suspend_filter_strategy" in yaml_text
    assert "filter_suspended_on_signal: true" in yaml_text
    assert "suspend_filter_file: qe_suspend_filter.json" in yaml_text


def test_suspend_filter_wraps_score_weighted_v2_strategy():
    yaml_text = _base_yaml(
        strategy_info={
            "strategy_id": "score_weighted_v2",
            "source_code": "class ScoreWeightedTopkStrategyV2(object):\n    pass\n",
            "portfolio_config": {"class": "ScoreWeightedTopkStrategyV2", "kwargs": {}},
        },
        custom_params={
            "topk": 20,
            "n_drop": 2,
            "filter_suspended_on_signal": True,
            "suspend_filter_file": "qe_suspend_filter.json",
        },
    )

    assert "class: SuspendFilterScoreWeightedTopkStrategyV2" in yaml_text
    assert "module_path: qe_suspend_filter_score_weighted_strategy" in yaml_text


def test_suspend_runtime_flags_reject_nested_conflicts():
    merged = _merge_strategy_runtime_flags({"topk": 10}, True, False)
    assert merged["filter_suspended_on_signal"] is True
    assert merged["suspend_filter_strict"] is False

    with pytest.raises(HTTPException, match="top-level request fields"):
        _merge_strategy_runtime_flags({"filter_suspended_on_signal": True}, False, True)
    with pytest.raises(HTTPException, match="strategy_loop"):
        _reject_nested_runtime_flags({"suspend_filter_strict": False}, "strategy_loop[1].strategy_params")


def test_qe_suspend_filter_symbol_aliases_and_strict_missing_date(tmp_path):
    artifact = tmp_path / "qe_suspend_filter.json"
    artifact.write_text(
        '{"enabled": true, "suspended_by_date": {"2024-01-02": ["000001.SZ", "600000.SH"]}}',
        encoding="utf-8",
    )
    filt = QESuspendFilter(True, str(artifact), strict=True)

    suspended = filt.suspended_symbols("2024-01-02")
    assert "000001.SZ" in suspended
    assert "SZ000001" in suspended
    assert "600000.SH" in suspended
    assert "SH600000" in suspended
    with pytest.raises(RuntimeError, match="no entry"):
        filt.suspended_symbols("2024-01-03")


def test_remote_stock_pool_sync_local_node_checks_file_and_checksum(monkeypatch):
    calls = []

    class Result:
        returncode = 0
        stderr = b""
        stdout = b"abc123\n"

    def fake_run(cmd, timeout, check, capture_output):
        calls.append(cmd)
        return Result()

    monkeypatch.setattr("backend.services.quantevolver.stock_pool_sync.subprocess.run", fake_run)
    result = sync_stock_pool_to_remote_node(
        "/home/lc999/data/qlib_bin/instruments/filtered_pool_x.txt",
        {"node_id": "wsl2-5080", "api_base_url": "http://127.0.0.1:9000"},
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "local_node"
    assert result["sha256"] == "abc123"
    assert any("test -f" in part for cmd in calls for part in cmd)


def test_remote_stock_pool_sync_resolves_instrument_name(monkeypatch):
    calls = []

    class Result:
        returncode = 0
        stderr = b""
        stdout = b"abc123\n"

    def fake_run(cmd, timeout, check, capture_output):
        calls.append(cmd)
        return Result()

    monkeypatch.setenv("QLIB_DATA_PATH_WSL", "/home/lc999/data/qlib_bin")
    monkeypatch.setattr("backend.services.quantevolver.stock_pool_sync.subprocess.run", fake_run)
    result = sync_stock_pool_to_remote_node(
        "filtered_pool_20260426",
        {"node_id": "wsl2-5080", "api_base_url": "http://127.0.0.1:9000"},
    )

    assert result["status"] == "skipped"
    assert result["local_path"] == "/home/lc999/data/qlib_bin/instruments/filtered_pool_20260426.txt"
    assert any("filtered_pool_20260426.txt" in part for cmd in calls for part in cmd)
