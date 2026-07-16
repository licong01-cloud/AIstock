
import json
import sys
import asyncio
import base64
import importlib.util
import inspect
import pickle
import types
from contextlib import contextmanager
from pathlib import Path
import yaml

import pytest
import numpy as np
import pandas as pd
from fastapi import HTTPException
from unittest.mock import AsyncMock

from backend.routers.quantevolver_evolution import (
    CustomEvoLoopConfig,
    EvolutionLoopRetryRequest,
    _hoist_runtime_metadata_from_strategy_params,
    _merge_strategy_runtime_flags,
    _model_to_dict,
    _reject_nested_runtime_flags,
)
from backend.services.quantevolver import qe_evolution_service as qes
from backend.execution_algos.v25_two_stage_algo import V25TwoStageAlgo, V25TwoStageUnavailableError
from backend.services.quantevolver.config_composer import (
    PRECOMPUTED_HMM_COEFF_JSON_PARAM,
    ConfigComposer,
    QE_DEFAULT_BACKTEST_END,
    QE_DEFAULT_SIGNAL_END,
    RISK_POLICY_FILE,
    RDAGENT_DEFAULT_DATA_SPLIT,
)
from backend.services.quantevolver.qe_evolution_service import (
    AutoEvolutionScheduler,
    QE_LOOP_RETRY_MODE_AUTO,
    QE_LOOP_RETRY_MODE_BACKTEST_ONLY,
    QE_LOOP_RETRY_MODE_FULL_TRAIN,
    QE_LOOP_RETRY_MODE_RESULTS_ONLY,
    normalize_qe_loop_retry_mode,
)
from backend.services.quantevolver.experiment_config_builders import (
    build_config_from_custom_evo_loop,
    build_config_from_retry_loop,
)
from backend.services.quantevolver.stock_pool_sync import (
    inject_stock_pool_install_command,
    prepare_stock_pool_loop_payload,
    sync_stock_pool_to_remote_node,
)
from backend.services.quantevolver.qe_dataset_contract import (
    QE_DATASET_SIGNAL_END_DATE,
    QE_DATASET_START_DATE,
    QE_ST_PIT_UNIVERSE_KEY,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from qe_suspend_filter import QESuspendFilter  # noqa: E402
from qe_event_risk_policy import QEEventRiskPolicy  # noqa: E402


DATA_SPLIT = {
    "train_start": "2020-01-01",
    "train_end": "2020-12-31",
    "valid_start": "2021-01-01",
    "valid_end": "2021-06-30",
    "test_start": "2021-07-01",
    "test_end": "2021-12-31",
    "backtest_end": "2021-12-31",
}


def _load_qrun_minute_module(monkeypatch):
    qlib = types.ModuleType("qlib")
    qlib_model = types.ModuleType("qlib.model")
    qlib_model_trainer = types.ModuleType("qlib.model.trainer")
    qlib_model_trainer.task_train = lambda *args, **kwargs: None
    qlib_workflow = types.ModuleType("qlib.workflow")
    qlib_workflow_cli = types.ModuleType("qlib.workflow.cli")
    qlib_workflow_cli.sys_config = lambda *args, **kwargs: None
    qlib_config = types.ModuleType("qlib.config")
    qlib_config.C = {"exp_manager": {"kwargs": {}}}
    for name, module in {
        "qlib": qlib,
        "qlib.model": qlib_model,
        "qlib.model.trainer": qlib_model_trainer,
        "qlib.workflow": qlib_workflow,
        "qlib.workflow.cli": qlib_workflow_cli,
        "qlib.config": qlib_config,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    module_path = SCRIPTS_DIR / "qrun_limit_minute.py"
    spec = importlib.util.spec_from_file_location("qrun_limit_minute_seed_ensemble_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def _stub_execution_algo_catalog(monkeypatch):
    """Keep config-truth tests independent from the local DB catalog."""

    monkeypatch.setattr(
        ConfigComposer,
        "_execution_algo_catalog_entry",
        classmethod(
            lambda cls, algo: {
                "default_config": {
                    "early_model_path": "early.pt",
                    "late_model_path": "late.pt",
                    "device": "cpu",
                    "min_cost": 5.0,
                    "commission_rate": 0.000595,
                    "tolerance_bps": 10.0,
                    "max_buckets": 30,
                },
                "is_enabled": True,
            }
        ),
    )


def test_remote_stock_pool_sync_has_no_direct_worker_directory_commands():
    import backend.services.quantevolver.stock_pool_sync as stock_pool_sync_module

    source = Path(stock_pool_sync_module.__file__).read_text(encoding="utf-8")
    assert "subprocess" not in source
    assert "wsl" not in source.lower()
    assert "ssh" not in source.lower()
    assert "scp" not in source.lower()
    assert "_run_checked" not in source


def test_hmm_coefficients_read_local_artifact_without_legacy_wsl_fallback(monkeypatch, tmp_path):
    import backend.services.quantevolver.config_composer as composer_module
    from unittest.mock import patch

    project_root = tmp_path / "project"
    model_path = project_root / "backend" / "data" / "hmm_models" / "snap" / "models.json"
    model_path.parent.mkdir(parents=True)
    model_path.write_text("{}", encoding="utf-8")
    coeff_path = model_path.parent / "coefficients_preset_A_2024-07-01_2026-04-27.json"
    coeff_path.write_text(
        json.dumps(
            {
                "sector_count": 1,
                "daily_coefficients": {"2024-07-01": {"801010.SI": 1.0}},
                "stock_sector_map": {"000001.SZ": "801010.SI"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(composer_module, "AISTOCK_PROJECT_ROOT", project_root)

    with patch("subprocess.run", side_effect=AssertionError("legacy fallback must not run")):
        result = ConfigComposer()._precompute_hmm_coefficients(
            {
                "sector_hmm_model_path": str(model_path),
                "hmm_signal_preset": "preset_A",
            },
            {"test_start": "2024-07-01", "backtest_end": "2026-04-27"},
        )

    payload = json.loads(result)
    assert payload["daily_coefficients"]["2024-07-01"]["801010.SI"] == 1.0


def test_hmm_linux_worker_model_path_is_not_converted_to_windows(monkeypatch):
    from unittest.mock import patch

    linux_worker_model_path = "/".join(
        ["", "mnt", "worker", "AIstock", "backend", "data", "hmm_models", "snap", "models.json"]
    )
    with patch("subprocess.run") as mock_run:
        with pytest.raises(RuntimeError, match="must not invoke WSL"):
            ConfigComposer()._precompute_hmm_coefficients(
                {
                    "sector_hmm_model_path": linux_worker_model_path,
                    "hmm_signal_preset": "preset_A",
                },
                {"test_start": "2024-07-01", "backtest_end": "2026-04-27"},
            )
        mock_run.assert_not_called()


def test_hmm_precompute_resolves_hidden_config_for_strict_window():
    from unittest.mock import MagicMock, patch

    svc = MagicMock()
    svc.get_snapshot.return_value = {"snapshot_id": "snap_hidden", "config_id": "cfg_hidden"}
    svc.get_config.return_value = {
        "config_id": "cfg_hidden",
        "model_type": "sector_hmm_experimental_stacking_20260504",
        "config_json": {
            "strict_no_leakage": True,
            "coefficient_windows": [
                {
                    "preset": "preset_A",
                    "test_start": "2024-07-01",
                    "backtest_end": "2026-04-27",
                    "strict_no_leakage": True,
                }
            ],
        },
    }

    with patch("backend.services.hmm_training_service.HMMTrainingService", return_value=svc):
        with pytest.raises(ValueError, match="strict_no_leakage"):
            ConfigComposer()._precompute_hmm_coefficients(
                {
                    "sector_hmm_model_path": "F:/tmp/models.json",
                    "hmm_signal_preset": "preset_A",
                    "hmm_model_version_id": "snap_hidden",
                },
                {"test_start": "2024-06-28", "backtest_end": "2026-04-27"},
            )

    svc.get_config.assert_called_once_with("cfg_hidden")


def test_qe_generation_code_has_no_windows_worker_workspace_direct_access():
    import inspect
    import backend.services.quantevolver.config_composer as composer_module

    source = Path(composer_module.__file__).read_text(encoding="utf-8")
    assert "QE_WORKSPACE_WIN" not in source
    assert "QE_WORKSPACE_WIN.parent" not in source
    for fn in (
        ConfigComposer.compose_experiment,
        ConfigComposer.regenerate_experiment,
        ConfigComposer._build_strategy_py_content,
        ConfigComposer._write_custom_strategy,
        ConfigComposer._precompute_hmm_coefficients,
        ConfigComposer._api_sync_experiment_files,
        ConfigComposer._get_read_exp_res_content,
    ):
        fn_source = inspect.getsource(fn)
        assert "QE_WORKSPACE_WIN" not in fn_source
        assert "subprocess.run" not in fn_source
        assert '["wsl"' not in fn_source


def test_qe_new_tasks_require_bound_recorder_before_result_extraction():
    import backend.services.quantevolver.config_composer as composer_module

    source = Path(composer_module.__file__).read_text(encoding="utf-8")
    assert "QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py" in source
    assert "\npython read_exp_res.py\n" not in source


def test_qe_read_exp_res_keeps_legacy_fallback_but_supports_strict_binding():
    template = Path("backend/services/quantevolver/templates/read_exp_res.py").read_text(encoding="utf-8")

    assert "qe_current_recorder.json" in template
    assert "QE_REQUIRE_RECORDER_ID=1" in template
    assert "using legacy latest-recorder fallback for old experiments" in template
    assert "refusing to extract another recorder" in template


def test_strategy_dependency_env_rejects_linux_worker_root(monkeypatch):
    monkeypatch.setenv("RDAGENT_FACTOR_TEMPLATE_WIN", "/home/lc999/RD-Agent-main/rdagent/scenarios/qlib")

    with pytest.raises(ValueError, match="Linux/WSL paths are forbidden"):
        ConfigComposer._strategy_dependency_roots()


def test_strategy_dependency_can_be_loaded_from_strategy_catalog(monkeypatch):
    import backend.services.quantevolver.config_composer as composer_module

    class FakeCursor:
        description = [("source_code",)]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _sql, params):
            assert params == ("score_weighted_strategy.py",)

        def fetchone(self):
            return ("class ScoreWeightedTopkStrategy(object):\n    pass\n",)

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    monkeypatch.setattr(
        ConfigComposer,
        "_resolve_strategy_dependency_path",
        classmethod(lambda cls, module_name, allowed_external_modules: None),
    )
    monkeypatch.setattr(composer_module, "get_conn", lambda: FakeConn())

    strategy_content, deps = ConfigComposer()._build_strategy_py_content(
        {
            "strategy_id": "score_weighted_topk_v2",
            "source_code": (
                "from score_weighted_strategy import ScoreWeightedTopkStrategy\n\n"
                "class ScoreWeightedTopkStrategyV2(ScoreWeightedTopkStrategy):\n"
                "    pass\n"
            ),
        }
    )

    assert "from score_weighted_strategy import ScoreWeightedTopkStrategy" in strategy_content
    assert deps["score_weighted_strategy.py"].startswith("class ScoreWeightedTopkStrategy")


def test_qe_template_root_rejects_linux_worker_path(monkeypatch):
    import backend.services.quantevolver.config_composer as composer_module

    linux_worker_root = Path("/".join(["", "mnt", "worker", "qe_programs"]))
    monkeypatch.setattr(composer_module, "QE_PROGRAMS_WIN", linux_worker_root)

    with pytest.raises(Exception, match="direct worker workspace path access is forbidden"):
        ConfigComposer()._get_read_exp_res_content()


def test_qe_api_sync_rejects_worker_experiment_dir_before_http():
    linux_worker_dir = Path("/".join(["", "mnt", "worker", "qe_workspace", "exp-a"]))

    with pytest.raises(Exception, match="direct worker workspace path access is forbidden"):
        ConfigComposer()._api_sync_experiment_files("exp-a", linux_worker_dir)


def test_qe_default_split_uses_safe_backtest_end_20260629():
    split = dict(RDAGENT_DEFAULT_DATA_SPLIT)

    ConfigComposer._validate_data_split(split)
    ConfigComposer._ensure_backtest_end(split)

    assert split["test_end"] == QE_DEFAULT_SIGNAL_END
    assert split["backtest_end"] == QE_DEFAULT_BACKTEST_END


def test_current_signal_end_without_backtest_end_derives_20260629():
    split = {
        **DATA_SPLIT,
        "test_start": "2024-07-01",
        "test_end": QE_DEFAULT_SIGNAL_END,
    }
    split.pop("backtest_end")

    ConfigComposer._ensure_backtest_end(split)

    assert split["backtest_end"] == QE_DEFAULT_BACKTEST_END


def test_legacy_qe_default_split_is_upgraded_to_current_safe_window():
    split = {
        "train_start": "2018-08-01",
        "train_end": "2022-12-31",
        "valid_start": "2023-01-01",
        "valid_end": "2024-06-30",
        "test_start": "2024-07-01",
        "test_end": "2026-03-10",
    }

    ConfigComposer._ensure_backtest_end(split)

    assert split["test_end"] == QE_DEFAULT_SIGNAL_END
    assert split["backtest_end"] == QE_DEFAULT_BACKTEST_END


def test_previous_20260428_default_split_is_upgraded_to_current_safe_window():
    split = {
        "train_start": "2018-08-01",
        "train_end": "2022-12-31",
        "valid_start": "2023-01-01",
        "valid_end": "2024-06-30",
        "test_start": "2024-07-01",
        "test_end": "2026-04-28",
        "backtest_end": "2026-04-27",
    }

    ConfigComposer._ensure_backtest_end(split)

    assert split["test_end"] == "2026-06-30"
    assert split["backtest_end"] == "2026-06-29"


def test_non_latest_user_window_uses_test_end_as_safe_backtest_end():
    split = dict(DATA_SPLIT)
    split.pop("backtest_end")

    ConfigComposer._ensure_backtest_end(split)

    assert split["backtest_end"] == "2021-12-31"


def test_historical_stock_pool_date_must_not_exceed_test_end():
    with pytest.raises(ValueError, match="QE_STOCK_POOL_DATE_OUT_OF_WINDOW"):
        ConfigComposer._validate_historical_stock_pool_window(
            {"stock_pool": "filtered_pool_20260701"},
            {
                **RDAGENT_DEFAULT_DATA_SPLIT,
                "test_end": QE_DEFAULT_SIGNAL_END,
                "backtest_end": QE_DEFAULT_BACKTEST_END,
            },
        )


def test_historical_stock_pool_date_allows_pit_pool_at_test_end():
    ConfigComposer._validate_historical_stock_pool_window(
        {"stock_pool": "filtered_pool_20260630"},
        {
            **RDAGENT_DEFAULT_DATA_SPLIT,
            "test_end": QE_DEFAULT_SIGNAL_END,
            "backtest_end": QE_DEFAULT_BACKTEST_END,
        },
    )


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


def _slice_yaml_between(yaml_text: str, start_marker: str, end_marker: str) -> str:
    start = yaml_text.index(start_marker)
    end = yaml_text.index(end_marker, start)
    return yaml_text[start:end]


def _parse_conf_yaml_with_jinja_placeholders(yaml_text: str):
    rendered_for_assert = (
        yaml_text.replace("{{ num_features }}", "44")
        .replace("{{ num_timesteps }}", "20")
    )
    return yaml.safe_load(rendered_for_assert)


def test_v25_execution_algo_generates_v25_inner_strategy():
    yaml_text = _base_yaml(
        execution_algo="V25_TWO_STAGE",
        execution_algo_params={"device": "cpu"},
    )
    inner_strategy = _slice_yaml_between(
        yaml_text,
        "            inner_strategy:",
        "            # qe_execution_trace:",
    )
    outer_strategy = _slice_yaml_between(
        yaml_text,
        "    strategy:",
        "    model:",
    )

    assert "class: TailTWAPWithV25TwoStageStrategy" in inner_strategy
    assert "module_path: tail_twap_v25_strategy" in inner_strategy
    assert "filter_suspended_on_signal: true" in inner_strategy
    assert "suspend_filter_file: qe_suspend_filter.json" in inner_strategy
    assert "suspend_filter_strict: true" in inner_strategy
    assert "class: SuspendFilterTopkDropoutStrategy" not in outer_strategy
    assert "filter_suspended_on_signal: true" not in outer_strategy
    assert "effective_algo: V25_TWO_STAGE" in yaml_text
    assert "early_model_path:" in yaml_text
    assert "late_model_path:" in yaml_text


def test_v25_1_execution_algo_generates_v25_1_inner_strategy():
    yaml_text = _base_yaml(
        execution_algo="V25_1_SMALL_CAP",
        execution_algo_params={"device": "cpu"},
    )
    inner_strategy = _slice_yaml_between(
        yaml_text,
        "            inner_strategy:",
        "            # qe_execution_trace:",
    )
    outer_strategy = _slice_yaml_between(
        yaml_text,
        "    strategy:",
        "    model:",
    )

    assert "class: TailTWAPWithV25_1SmallCapStrategy" in inner_strategy
    assert "module_path: tail_twap_v25_1_strategy" in inner_strategy
    assert "filter_suspended_on_signal: true" in inner_strategy
    assert "suspend_filter_file: qe_suspend_filter.json" in inner_strategy
    assert "suspend_filter_strict: true" in inner_strategy
    assert "class: SuspendFilterTopkDropoutStrategy" not in outer_strategy
    assert "effective_algo: V25_1_SMALL_CAP" in yaml_text
    assert "early_model_path:" in yaml_text
    assert "late_model_path:" in yaml_text
    assert "min_cost:" in yaml_text
    assert "commission_rate:" in yaml_text
    assert "tolerance_bps:" in yaml_text
    assert "max_buckets:" in yaml_text
    assert "trade_unit: ~" in yaml_text
    assert "board_lot_trade_unit: true" in yaml_text


def test_v25_1_execution_algo_yaml_preserves_ui_cost_params_for_wrapper_aliases():
    yaml_text = _base_yaml(
        execution_algo="V25_1_SMALL_CAP",
        execution_algo_params={
            "device": "cpu",
            "min_cost": 5.0,
            "commission_rate": 0.00025,
            "tolerance_bps": 10.0,
            "max_buckets": 12,
        },
    )
    inner_strategy = _slice_yaml_between(
        yaml_text,
        "            inner_strategy:",
        "            # qe_execution_trace:",
    )

    assert "min_cost: 5.0" in inner_strategy
    assert "commission_rate: 0.00025" in inner_strategy
    assert "tolerance_bps: 10.0" in inner_strategy
    assert "max_buckets: 12" in inner_strategy
    assert "v25_1_min_cost:" not in inner_strategy
    assert "v25_1_commission_rate:" not in inner_strategy
    assert "v25_1_tolerance_bps:" not in inner_strategy
    assert "v25_1_max_buckets:" not in inner_strategy


def test_v25_1_execution_algo_receives_suspend_artifact_when_signal_filter_enabled():
    yaml_text = _base_yaml(
        execution_algo="V25_1_SMALL_CAP",
        execution_algo_params={"device": "cpu"},
        custom_params={
            "filter_suspended_on_signal": True,
            "suspend_filter_file": "qe_suspend_filter.json",
            "suspend_filter_strict": True,
        },
    )

    inner_strategy = _slice_yaml_between(
        yaml_text,
        "            inner_strategy:",
        "            # qe_execution_trace:",
    )
    outer_strategy = _slice_yaml_between(
        yaml_text,
        "    strategy:",
        "    model:",
    )

    assert "class: TailTWAPWithV25_1SmallCapStrategy" in inner_strategy
    assert "filter_suspended_on_signal: true" in inner_strategy
    assert "class: SuspendFilterTopkDropoutStrategy" in outer_strategy
    assert "filter_suspended_on_signal: true" in outer_strategy


def test_qe_exchange_can_receive_wider_quote_universe_codes_for_forced_exit():
    yaml_text = _base_yaml(
        execution_algo="V25_TWO_STAGE",
        execution_algo_params={"device": "cpu"},
        custom_params={
            "quote_universe_codes": ["000001.SZ", "600000.SH"],
            "risk_policy": {
                "enabled": True,
                "providers": ["st_pit"],
                "hard_actions": ["block_buy", "force_exit"],
            },
        },
    )
    exchange = _slice_yaml_between(
        yaml_text,
        "        exchange_kwargs:",
        "task:",
    )

    assert "codes:" in exchange
    assert "- 000001.SZ" in exchange
    assert "- 600000.SH" in exchange
    assert "contract: stock_event_risk_policy_v1" in yaml_text


def test_qe_risk_policy_wraps_outer_strategy_and_emits_runtime_kwargs():
    yaml_text = _base_yaml(
        custom_params={
            "risk_policy": {
                "enabled": True,
                "providers": ["st_pit"],
                "hard_actions": ["block_buy", "force_exit"],
            },
            "risk_policy_file": RISK_POLICY_FILE,
            "risk_policy_strict": True,
        },
    )

    outer_strategy = _slice_yaml_between(
        yaml_text,
        "    strategy:",
        "    model:",
    )

    assert "class: SuspendFilterTopkDropoutStrategy" in outer_strategy
    assert "module_path: qe_suspend_filter_strategy" in outer_strategy
    assert "risk_policy_enabled: true" in outer_strategy
    assert f"risk_policy_file: {RISK_POLICY_FILE}" in outer_strategy
    assert "risk_policy_strict: true" in outer_strategy
    assert "filter_suspended_on_signal: true" in outer_strategy
    assert "suspend_filter_file: qe_suspend_filter.json" in outer_strategy
    assert "suspend_filter_strict: true" in outer_strategy


def test_qe_risk_policy_wraps_score_weighted_v2_strategy():
    yaml_text = _base_yaml(
        strategy_info={
            "strategy_id": "score_weighted_v2",
            "source_code": "class ScoreWeightedTopkStrategyV2(object):\n    pass\n",
            "portfolio_config": {"class": "ScoreWeightedTopkStrategyV2", "kwargs": {}},
        },
        custom_params={
            "risk_policy": {
                "enabled": True,
                "providers": ["st_pit"],
                "hard_actions": ["block_buy", "force_exit"],
            },
            "risk_policy_file": RISK_POLICY_FILE,
        },
    )

    assert "class: SuspendFilterScoreWeightedTopkStrategyV2" in yaml_text
    assert "module_path: qe_suspend_filter_score_weighted_strategy" in yaml_text
    assert "risk_policy_enabled: true" in yaml_text
    assert "filter_suspended_on_signal: true" in yaml_text


def test_qe_risk_policy_runtime_prepares_local_artifact(monkeypatch):
    composer = ConfigComposer()
    calls = []

    def fake_build_risk_policy_artifact(data_split, custom_params):
        calls.append((data_split, custom_params["risk_policy"]["st_universe_key"]))
        return json.dumps(
            {
                "enabled": True,
                "contract": "stock_event_risk_policy_v1",
                "active_spans": [
                    {"ts_code": "600000.SH", "eligible_start": "2021-01-01", "eligible_end": "2021-12-31"},
                    {"ts_code": "000001.SZ", "eligible_start": "2021-01-01", "eligible_end": "2021-12-31"},
                ],
            }
        )

    monkeypatch.setattr(composer, "_build_qe_risk_policy_artifact", fake_build_risk_policy_artifact)
    custom_params, artifact = composer._prepare_risk_policy_runtime(
        custom_params={
            "risk_policy": {
                "enabled": True,
                "providers": ["st_pit"],
                "st_universe_key": "shsz_st_pit_active_v1",
                "hard_actions": ["block_buy", "force_exit"],
            }
        },
        data_split=DATA_SPLIT,
    )

    assert artifact.startswith('{"enabled": true')
    assert calls == [(DATA_SPLIT, QE_ST_PIT_UNIVERSE_KEY)]
    assert custom_params["risk_policy_enabled"] is True
    assert custom_params["risk_policy_file"] == RISK_POLICY_FILE
    assert custom_params["risk_policy_strict"] is True
    assert custom_params["quote_universe_codes"] == ["000001.SZ", "600000.SH"]


def test_qe_risk_policy_uses_immutable_dataset_pit_snapshot(monkeypatch):
    import backend.services.quantevolver.config_composer as composer_module
    from backend.services.stock_universe_pit_service import StockUniversePitService

    ensure_calls = []

    def fake_ensure(_self, **kwargs):
        ensure_calls.append(kwargs)
        return {"status": "ready", "rebuilt": False}

    class FakeCursor:
        sql = ""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, _params):
            self.sql = sql

        def fetchall(self):
            if "FROM market.trading_calendar" in self.sql:
                return [(pd.Timestamp("2021-07-01").date(),), (pd.Timestamp("2021-12-31").date(),)]
            if "FROM market.stock_universe_pit_spans" in self.sql:
                return []
            raise AssertionError(f"unexpected fetchall SQL: {self.sql}")

        def fetchone(self):
            if "FROM market.stock_universe_pit_state" in self.sql:
                return (
                    QE_ST_PIT_UNIVERSE_KEY,
                    "st_pub_next_trade_restore_active_l_v1",
                    "st_only_active",
                    "ready",
                    False,
                    "fingerprint",
                    None,
                )
            raise AssertionError(f"unexpected fetchone SQL: {self.sql}")

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    monkeypatch.setattr(StockUniversePitService, "ensure_immutable_dataset_snapshot", fake_ensure)
    monkeypatch.setattr(composer_module, "get_conn", lambda: FakeConn())

    artifact = ConfigComposer()._build_qe_risk_policy_artifact(
        DATA_SPLIT,
        {"risk_policy": {"enabled": True, "providers": ["st_pit"]}},
    )

    assert json.loads(artifact)["end_date"] == DATA_SPLIT["backtest_end"]
    assert ensure_calls == [
        {
            "universe_key": QE_ST_PIT_UNIVERSE_KEY,
            "start_date": QE_DATASET_START_DATE,
            "end_date": QE_DATASET_SIGNAL_END_DATE,
            "bootstrap_if_missing": True,
        }
    ]


def test_qe_risk_policy_runtime_defaults_and_overwrites_stale_quote_universe(monkeypatch):
    composer = ConfigComposer()

    def fake_build_risk_policy_artifact(data_split, custom_params):
        assert custom_params["risk_policy"]["enabled"] is True
        assert custom_params["risk_policy"]["providers"] == ["st_pit"]
        return json.dumps(
            {
                "enabled": True,
                "contract": "stock_event_risk_policy_v1",
                "active_spans": [
                    {"ts_code": "000001.SZ", "eligible_start": "2021-01-01", "eligible_end": "2021-12-31"},
                ],
            }
        )

    monkeypatch.setattr(composer, "_build_qe_risk_policy_artifact", fake_build_risk_policy_artifact)
    custom_params, artifact = composer._prepare_risk_policy_runtime(
        custom_params={"topk": 20, "quote_universe_codes": ["STALE.SH"]},
        data_split=DATA_SPLIT,
    )

    assert json.loads(artifact)["enabled"] is True
    assert custom_params["risk_policy"]["st_universe_key"] == QE_ST_PIT_UNIVERSE_KEY
    assert custom_params["quote_universe_codes"] == ["000001.SZ"]


def test_qe_risk_policy_runtime_rejects_disabled_policy():
    with pytest.raises(ValueError, match="risk_policy.enabled=false"):
        ConfigComposer()._prepare_risk_policy_runtime(
            custom_params={"risk_policy": {"enabled": False}},
            data_split=DATA_SPLIT,
        )


def test_v25_execution_prepares_suspend_artifact_without_signal_filter(monkeypatch):
    composer = ConfigComposer()
    calls = []

    def fake_build_suspend_filter_artifact(data_split, *, strict_audit=True):
        calls.append((data_split, strict_audit))
        return '{"enabled": true, "suspended_by_date": {}}'

    monkeypatch.setattr(composer, "_build_suspend_filter_artifact", fake_build_suspend_filter_artifact)
    custom_params, artifact = composer._prepare_suspend_filter_runtime(
        custom_params={},
        data_split=DATA_SPLIT,
        strategy_info=None,
        execution_algo="V25_TWO_STAGE",
    )

    assert artifact == '{"enabled": true, "suspended_by_date": {}}'
    assert calls == [(DATA_SPLIT, True)]
    assert custom_params["suspend_filter_file"] == "qe_suspend_filter.json"
    assert custom_params["suspend_filter_strict"] is True
    assert "filter_suspended_on_signal" not in custom_params


def test_qe_risk_policy_prepares_suspend_artifact_for_signal_filter(monkeypatch):
    composer = ConfigComposer()
    calls = []

    def fake_build_suspend_filter_artifact(data_split, *, strict_audit=True):
        calls.append((data_split, strict_audit))
        return '{"enabled": true, "suspended_by_date": {}}'

    monkeypatch.setattr(composer, "_build_suspend_filter_artifact", fake_build_suspend_filter_artifact)
    custom_params, artifact = composer._prepare_suspend_filter_runtime(
        custom_params={
            "risk_policy": {
                "enabled": True,
                "providers": ["st_pit"],
                "hard_actions": ["block_buy", "force_exit"],
            }
        },
        data_split=DATA_SPLIT,
        strategy_info=None,
        execution_algo=None,
    )

    assert artifact == '{"enabled": true, "suspended_by_date": {}}'
    assert calls == [(DATA_SPLIT, True)]
    assert custom_params["filter_suspended_on_signal"] is True
    assert custom_params["suspend_filter_file"] == "qe_suspend_filter.json"
    assert custom_params["suspend_filter_strict"] is True


def test_hmm_precomputed_coefficients_skip_runtime_precompute(monkeypatch):
    composer = ConfigComposer()
    coeff_json = (
        '{"daily_coefficients": {"2024-07-01": {"801010.SI": 1.05}}, '
        '"stock_sector_map": {"000001.SZ": "801010.SI"}}'
    )

    def fail_precompute(*args, **kwargs):
        raise AssertionError("runtime precompute should not be called")

    monkeypatch.setattr(composer, "_precompute_hmm_coefficients", fail_precompute)

    resolved = composer._resolve_hmm_coefficients_json(
        {PRECOMPUTED_HMM_COEFF_JSON_PARAM: coeff_json},
        DATA_SPLIT,
    )

    assert resolved == coeff_json


def test_v25_execution_algo_receives_suspend_artifact_when_signal_filter_enabled():
    yaml_text = _base_yaml(
        execution_algo="V25_TWO_STAGE",
        execution_algo_params={"device": "cpu"},
        custom_params={
            "filter_suspended_on_signal": True,
            "suspend_filter_file": "qe_suspend_filter.json",
            "suspend_filter_strict": True,
        },
    )

    inner_strategy = _slice_yaml_between(
        yaml_text,
        "            inner_strategy:",
        "            # qe_execution_trace:",
    )
    outer_strategy = _slice_yaml_between(
        yaml_text,
        "    strategy:",
        "    model:",
    )

    assert "class: TailTWAPWithV25TwoStageStrategy" in inner_strategy
    assert "filter_suspended_on_signal: true" in inner_strategy
    assert "suspend_filter_file: qe_suspend_filter.json" in inner_strategy
    assert "suspend_filter_strict: true" in inner_strategy
    assert "class: SuspendFilterTopkDropoutStrategy" in outer_strategy
    assert "filter_suspended_on_signal: true" in outer_strategy
    assert "suspend_filter_file: qe_suspend_filter.json" in outer_strategy
    assert "suspend_filter_strict: true" in outer_strategy


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
    assert ConfigComposer._resolve_backtest_freq("V25_1_SMALL_CAP", {}) == "1min"
    with pytest.raises(ValueError, match="requires backtest_freq=1min"):
        ConfigComposer._resolve_backtest_freq("V25_TWO_STAGE", {"backtest_freq": "day"})
    with pytest.raises(ValueError, match="requires backtest_freq=1min"):
        ConfigComposer._resolve_backtest_freq("V25_1_SMALL_CAP", {"backtest_freq": "day"})
    with pytest.raises(ValueError, match="requires backtest_freq=day"):
        ConfigComposer._resolve_backtest_freq("CLOSE_PRICE", {"backtest_freq": "1min"})
    with pytest.raises(ValueError, match="requires backtest_freq=1min"):
        ConfigComposer._resolve_backtest_freq(None, {"backtest_freq": "day"})


def test_retry_loop_preserves_loop_specific_execution_and_hold_config():
    cfg = build_config_from_retry_loop(
        {
            "factor_list": ["f1"],
            "model_id": "lgbm",
            "strategy_id": "score_weighted_topk_v2",
            "strategy_params": {"hold_thresh": 5},
            "execution_algo": "V25_TWO_STAGE",
            "execution_algo_params": {"device": "cpu"},
            "stock_pool": "filtered_pool_20260426",
            "label_type": "raw_return",
            "label_horizon": 5,
            "model_params": {"label_horizon": 5},
            "data_split": DATA_SPLIT,
        },
        {
            "strategy_id": "task_default_strategy",
            "strategy_params": {"hold_thresh": 2},
            "execution_algo": "TWAP",
            "execution_algo_params": {"unexpected": True},
            "stock_pool": "all",
            "label_type": "task_default_label",
            "label_horizon": 1,
        },
        experiment_name="retry-test",
    )

    assert cfg.execution_algo == "V25_TWO_STAGE"
    assert cfg.execution_algo_params == {"device": "cpu"}
    assert cfg.strategy_id == "score_weighted_topk_v2"
    assert cfg.strategy_params == {"hold_thresh": 5}
    assert cfg.stock_pool == "filtered_pool_20260426"
    assert cfg.label_type == "raw_return"
    assert cfg.label_horizon == 5


def test_custom_evo_builder_accepts_persisted_factor_list():
    cfg = build_config_from_custom_evo_loop(
        {
            "factor_list": ["alpha_a", "alpha_b"],
            "model_id": "model_lgbm_v1",
            "strategy_id": "score_weighted_topk_v2",
            "data_split": {},
            "stock_pool": "filtered_pool_20260502",
            "label_horizon": 10,
            "source_label_horizon": 10,
            "backtest_only": True,
            "model_source_task_id": "qe_source",
            "model_source_loop_index": 26,
        },
        {"node_id": "wsl2-5080"},
        experiment_name="custom-retry",
    )

    assert cfg.factor_names == ["alpha_a", "alpha_b"]
    assert cfg.backtest_only is True
    assert cfg.model_source_task_id == "qe_source"
    assert cfg.model_source_loop_index == 26


def test_custom_evo_general_ptnn_ltr_model_params_reaches_composed_conf(monkeypatch):
    composer = ConfigComposer()
    monkeypatch.setattr(composer, "_get_factors_info", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        composer,
        "_get_model_info",
        lambda *_args, **_kwargs: _custom_timeseries_lstm_model_info(),
    )
    monkeypatch.setattr(composer, "_get_strategy_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        composer,
        "_prepare_risk_policy_runtime",
        lambda **_kwargs: (_kwargs["custom_params"], None),
    )
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **_kwargs: (_kwargs["custom_params"], None),
    )
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read_exp_res")

    loop_payload = _model_to_dict(
        CustomEvoLoopConfig(
            factor_keys=["alpha_a||catalog"],
            model_id="__seed_LSTM_10D_hs64_d02__",
            strategy_id="score_weighted_topk_v2",
            data_split=DATA_SPLIT,
            label_horizon=20,
            runtime_flags={"random_seed": 42},
            model_params={
                "ltr_loss_mode": "approx_ndcg_at_k",
                "topk_train_k": 25,
                "ltr_temperature": 0.8,
            },
        )
    )

    cfg = build_config_from_custom_evo_loop(
        loop_payload,
        {},
        experiment_name="custom-evo-ltr",
    )

    result = composer.compose_experiment_in_memory(
        factor_names=cfg.factor_names,
        model_id=cfg.model_id,
        strategy_id=cfg.strategy_id,
        data_split=cfg.data_split,
        custom_params=cfg.build_custom_params(),
        experiment_name=cfg.experiment_name,
        skip_db_save=True,
        execution_algo=cfg.execution_algo,
        execution_algo_params=cfg.execution_algo_params,
        strategy_params=cfg.build_strategy_params(),
        node_id=cfg.node_id,
        callback_url="http://127.0.0.1:8001",
        task_id="qe_custom_ltr",
        loop_index=1,
    )

    yaml_text = result["experiment_files"]["conf.yaml"]
    parsed = _parse_conf_yaml_with_jinja_placeholders(yaml_text)
    model_cfg = parsed["task"]["model"]
    model_kwargs = model_cfg["kwargs"]
    assert model_cfg["class"] == "AIStockGeneralPTNNLTR"
    assert model_cfg["module_path"] == "aistock_models.general_ptnn_ltr"
    assert model_kwargs["loss"] == "approx_ndcg_at_k"
    assert model_kwargs["ltr_loss_mode"] == "approx_ndcg_at_k"
    assert model_kwargs["topk_train_k"] == 25
    assert model_kwargs["ltr_temperature"] == 0.8
    assert "aistock_models/general_ptnn_ltr.py" in result["experiment_files"]
    strategy_kwargs = parsed["port_analysis_config"]["strategy"]["kwargs"]
    assert "ltr_loss_mode" not in strategy_kwargs
    assert "topk_train_k" not in strategy_kwargs


@pytest.mark.parametrize(
    "bad_patch",
    [
        {"custom_params": {"ltr_loss_mode": "approx_ndcg_at_k"}},
        {"ltr_loss_mode": "approx_ndcg_at_k"},
        {"strategy_params": {"ltr_loss_mode": "approx_ndcg_at_k"}},
    ],
)
def test_custom_evo_general_ptnn_ltr_misplaced_request_fails_loud(bad_patch):
    loop = {
        "factor_list": ["alpha_a"],
        "model_id": "__seed_LSTM_10D_hs64_d02__",
        "strategy_id": "score_weighted_topk_v2",
        "data_split": DATA_SPLIT,
        "label_horizon": 20,
    }
    loop.update(bad_patch)

    with pytest.raises(ValueError, match="reason_code=ltr_loss_mode_injection_failed"):
        build_config_from_custom_evo_loop(loop, {"node_id": "wsl2-5080"}, experiment_name="bad-ltr")


def test_qe_loop_retry_mode_normalization():
    assert EvolutionLoopRetryRequest().retry_mode == QE_LOOP_RETRY_MODE_AUTO
    assert normalize_qe_loop_retry_mode(None) == QE_LOOP_RETRY_MODE_AUTO
    assert normalize_qe_loop_retry_mode("auto") == QE_LOOP_RETRY_MODE_AUTO
    assert normalize_qe_loop_retry_mode("backtest-only") == QE_LOOP_RETRY_MODE_BACKTEST_ONLY
    assert normalize_qe_loop_retry_mode("backtest") == QE_LOOP_RETRY_MODE_BACKTEST_ONLY
    assert normalize_qe_loop_retry_mode("full-train") == QE_LOOP_RETRY_MODE_FULL_TRAIN
    assert normalize_qe_loop_retry_mode("train") == QE_LOOP_RETRY_MODE_FULL_TRAIN
    assert normalize_qe_loop_retry_mode("results-only") == QE_LOOP_RETRY_MODE_RESULTS_ONLY
    assert normalize_qe_loop_retry_mode("register-only") == QE_LOOP_RETRY_MODE_RESULTS_ONLY

    with pytest.raises(ValueError, match="Invalid retry mode"):
        normalize_qe_loop_retry_mode("invalid")


def test_backtest_only_model_payload_uses_node_api_archive():
    client = AsyncMock()
    client.download_mlruns_params.return_value = b"tar-bytes"
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    model_source, extra_files = asyncio.get_event_loop().run_until_complete(
        scheduler._build_backtest_only_model_payload(
            client,
            "qe_source",
            26,
            reason="unit-test backtest-only",
        )
    )

    assert model_source == {
        "source_task_id": "qe_source",
        "source_loop": "Loop26",
        "cross_node": True,
        "source_transport": "mlruns_params_tar",
    }
    assert base64.b64decode(extra_files["mlruns_params.tar.gz.b64"]) == b"tar-bytes"
    source_ref = json.loads(extra_files["qe_backtest_source_ref.json"])
    assert source_ref["source_task_id"] == "qe_source"
    assert source_ref["source_loop"] == "Loop26"
    assert source_ref["source_transport"] == "mlruns_params_tar"
    client.download_mlruns_params.assert_awaited_once_with("qe_source", "Loop26")


def test_backtest_only_model_payload_fails_without_params():
    client = AsyncMock()
    client.download_mlruns_params.return_value = b""
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    with pytest.raises(RuntimeError, match="missing source model params"):
        asyncio.get_event_loop().run_until_complete(
            scheduler._build_backtest_only_model_payload(
                client,
                "qe_source",
                1,
                reason="unit-test backtest-only",
            )
        )


def test_backtest_retry_requires_isolation_passed():
    client = AsyncMock()
    client.get_workspace_file.return_value = {"recorder_isolation_status": "failed"}
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    with pytest.raises(ValueError, match="QE_BACKTEST_RETRY_REQUIRES_ISOLATION_PASSED"):
        asyncio.get_event_loop().run_until_complete(
            scheduler._require_backtest_retry_isolation_passed(
                client,
                "qe_task",
                "Loop4",
                "rdagent-node1",
            )
        )


def test_backtest_retry_accepts_isolation_passed_manifest_json():
    client = AsyncMock()
    client.get_workspace_file.return_value = json.dumps({"recorder_isolation_status": "passed"})
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    payload = asyncio.get_event_loop().run_until_complete(
        scheduler._require_backtest_retry_isolation_passed(
            client,
            "qe_task",
            "Loop4",
            "rdagent-node1",
        )
    )

    assert payload["recorder_isolation_status"] == "passed"


def test_backtest_retry_isolation_gate_precedes_auto_fallback_try():
    source = inspect.getsource(AutoEvolutionScheduler.retry_loop)
    retry_block_idx = source.index("if requested_retry_mode in (")
    require_idx = source.index("await self._require_backtest_retry_isolation_passed", retry_block_idx)
    fallback_try_idx = source.index("try:", retry_block_idx)
    payload_idx = source.index("retry_model_source, retry_extra_experiment_files", fallback_try_idx)

    assert require_idx < fallback_try_idx < payload_idx


class _CustomEvoParallelismCursor:
    def __init__(self, active_count: int):
        self.active_count = active_count
        self.sql: list[str] = []
        self.params: list[object] = []

    def execute(self, sql, params=None):
        self.sql.append(" ".join(str(sql).split()))
        self.params.append(params)

    def fetchone(self):
        if "COUNT(*) AS active_count" in self.sql[-1]:
            return {"active_count": self.active_count}
        return None


class _CustomEvoQueueCursor:
    def __init__(self, state: dict):
        self.state = state
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, sql, params=None):
        self.sql = " ".join(str(sql).split())
        self.state["sql"].append(self.sql)
        self.state["params"].append(params)

    def fetchone(self):
        if "RETURNING status" in self.sql and self.state.get("transitioned", True):
            return ("running",)
        return None


class _CustomEvoQueueConn:
    def __init__(self, state: dict):
        self.state = state

    def __enter__(self):
        self.state["enters"] += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.state["exits"] += 1
        return False

    def cursor(self, *args, **kwargs):
        return _CustomEvoQueueCursor(self.state)

    def commit(self):
        self.state["commits"] += 1


def _custom_evo_task_for_parallelism(limit: int = 1):
    return {
        "task_id": "qe_parallel_task",
        "task_type": "custom_evo",
        "node_id": "node-a",
        "strategy_evo_config": {
            "loops": [
                {"loop_index": 1, "node_id": "node-a", "factor_keys": ["Alpha001"], "model_id": "model_a"},
                {"loop_index": 2, "node_id": "node-a", "factor_keys": ["Alpha002"], "model_id": "model_a"},
            ],
            "node_parallelism": {"node-a": limit},
            "engine_mode": "unified",
        },
    }


def test_custom_evo_parallelism_slot_queues_active_retry_on_same_node():
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    cursor = _CustomEvoParallelismCursor(active_count=1)

    slot = scheduler._enforce_custom_evo_node_parallelism_slot(
        cursor,
        task=_custom_evo_task_for_parallelism(limit=1),
        loop_index=2,
        target_node_id="node-a",
        loop_db_id="qe_parallel_task_Loop2",
    )

    assert slot is not None
    assert slot["available"] is False
    assert slot["active_count"] == 1
    assert slot["limit"] == 1
    assert any("pg_advisory_xact_lock" in sql for sql in cursor.sql)
    assert any("COUNT(*) AS active_count" in sql for sql in cursor.sql)


def test_custom_evo_parallelism_slot_allows_when_node_has_capacity():
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    cursor = _CustomEvoParallelismCursor(active_count=1)

    slot = scheduler._enforce_custom_evo_node_parallelism_slot(
        cursor,
        task=_custom_evo_task_for_parallelism(limit=2),
        loop_index=2,
        target_node_id="node-a",
        loop_db_id="qe_parallel_task_Loop2",
    )

    assert slot is not None
    assert slot["node_id"] == "node-a"
    assert slot["limit"] == 2
    assert slot["active_count"] == 1
    assert slot["available"] is True


def test_retry_loop_queues_until_custom_evo_parallelism_before_executor_submit():
    source = inspect.getsource(AutoEvolutionScheduler.retry_loop)
    queue_idx = source.index("self._mark_custom_evo_loop_running_when_slot_available")
    submit_idx = source.index("result = await executor.submit")

    assert queue_idx < submit_idx


def test_custom_evo_parallelism_queue_helper_marks_pending_then_running():
    source = inspect.getsource(AutoEvolutionScheduler._mark_custom_evo_loop_running_when_slot_available)

    assert "ELSE 'pending'" in source
    assert "status = 'running'" in source
    assert "await asyncio.sleep(poll_seconds)" in source


def test_custom_evo_parallelism_queue_helper_waits_then_runs(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    monkeypatch.setattr(scheduler, "_get_task_status", lambda task_id: "running")
    state = {"sql": [], "params": [], "commits": 0, "enters": 0, "exits": 0, "sleeps": []}
    slots = [
        {
            "task_id": "qe_parallel_task",
            "loop_index": 2,
            "node_id": "node-a",
            "limit": 1,
            "active_count": 1,
            "available": False,
        },
        {
            "task_id": "qe_parallel_task",
            "loop_index": 2,
            "node_id": "node-a",
            "limit": 1,
            "active_count": 0,
            "available": True,
        },
    ]

    def fake_enforce(cur, *, task, loop_index, target_node_id, loop_db_id):
        return slots.pop(0)

    async def fake_sleep(seconds):
        state["sleeps"].append(seconds)

    monkeypatch.setattr(scheduler, "_enforce_custom_evo_node_parallelism_slot", fake_enforce)
    monkeypatch.setattr(qes, "get_conn", lambda: _CustomEvoQueueConn(state))
    monkeypatch.setattr(qes.asyncio, "sleep", fake_sleep)

    slot = asyncio.run(
        scheduler._mark_custom_evo_loop_running_when_slot_available(
            task=_custom_evo_task_for_parallelism(limit=1),
            loop_index=2,
            target_node_id="node-a",
            loop_db_id="qe_parallel_task_Loop2",
            action_type="retry",
            insert_if_missing=True,
            poll_seconds=15,
        )
    )

    assert slot is not None
    assert slot["available"] is True
    assert state["sleeps"] == [15]
    assert state["enters"] == state["exits"] == 2
    assert any("VALUES (%s, %s, %s, 'pending', %s, %s)" in sql for sql in state["sql"])
    assert any("VALUES (%s, %s, %s, 'running', %s, %s)" in sql for sql in state["sql"])


def test_custom_evo_parallelism_queue_helper_does_not_resubmit_active_loop(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    monkeypatch.setattr(scheduler, "_get_task_status", lambda task_id: "running")
    state = {
        "sql": [],
        "params": [],
        "commits": 0,
        "enters": 0,
        "exits": 0,
        "transitioned": False,
    }

    def fake_enforce(cur, *, task, loop_index, target_node_id, loop_db_id):
        return {
            "task_id": "qe_parallel_task",
            "loop_index": 2,
            "node_id": "node-a",
            "limit": 1,
            "active_count": 0,
            "available": True,
        }

    monkeypatch.setattr(scheduler, "_enforce_custom_evo_node_parallelism_slot", fake_enforce)
    monkeypatch.setattr(qes, "get_conn", lambda: _CustomEvoQueueConn(state))

    slot = asyncio.run(
        scheduler._mark_custom_evo_loop_running_when_slot_available(
            task=_custom_evo_task_for_parallelism(limit=1),
            loop_index=2,
            target_node_id="node-a",
            loop_db_id="qe_parallel_task_Loop2",
            action_type="retry",
        )
    )

    assert slot is None
    assert any("RETURNING status" in sql for sql in state["sql"])
    assert not any("UPDATE qe_evolution_tasks SET status = 'running'" in sql for sql in state["sql"])


def test_gpu_phase_waiter_releases_before_loop_terminal_after_lifecycle_event(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    class FakeLease:
        policy = "exclusive"
        active = True

        async def release(self):
            self.active = False

    lease = FakeLease()
    observations = []

    class FakeResourceService:
        def get_session_state(self, _session_id):
            return {"current_phase": "gpu_phase_released"}

        def mark_session_terminal(self, session_id, *, status, reason_code=None):
            observations.append(("terminal", session_id, status, reason_code))

    statuses = iter(["running", "completed"])

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            observations.append(("loop_poll", lease.active))
            return (next(statuses),)

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, *_args, **_kwargs):
            return Cursor()

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(qes, "QEResourcePhaseService", lambda: FakeResourceService())
    monkeypatch.setattr(qes, "get_conn", lambda: Conn())
    monkeypatch.setattr(qes.asyncio, "sleep", no_sleep)

    asyncio.run(
        scheduler._wait_resource_session_until_safe_release(
            session_id="qers_1",
            task_id="qe_task",
            loop_index=1,
            gpu_lease=lease,
        )
    )

    assert observations[0] == ("loop_poll", False)
    assert observations[-1] == ("terminal", "qers_1", "completed", None)


def test_scheduler_resolves_model_aware_gpu_training_policy(monkeypatch):
    model_rows = {
        "gat-model": {"model_id": "gat-model", "model_config": {"class": "EfficientGATs"}},
        "lstm-model": {"model_id": "lstm-model", "model_name": "LSTM"},
    }
    monkeypatch.setattr(ConfigComposer, "_get_model_info", lambda _self, model_id: model_rows[model_id])

    assert AutoEvolutionScheduler._resolve_model_gpu_training_policy("gat-model") == "exclusive"
    assert AutoEvolutionScheduler._resolve_model_gpu_training_policy("lstm-model") == "parallel"
    assert AutoEvolutionScheduler._resolve_model_gpu_training_policy(None) == "parallel"


def test_scheduler_atomically_reserves_policy_specific_gpu_phase_sessions(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    requested = []

    class FakeResourceService:
        def create_session(self, **kwargs):
            requested.append(dict(kwargs))
            return types.SimpleNamespace(session_id=f"session-{kwargs['gpu_training_policy']}")

    async def scenario():
        parallel, parallel_session = await scheduler._reserve_gpu_phase_session(
            service=FakeResourceService(),
            task_id="qe_task",
            loop_index=1,
            node_id="node-a",
            policy="parallel",
        )
        assert parallel_session.session_id == "session-parallel"
        await parallel.release()
        exclusive, exclusive_session = await scheduler._reserve_gpu_phase_session(
            service=FakeResourceService(),
            task_id="qe_task",
            loop_index=2,
            node_id="node-a",
            policy="exclusive",
        )
        assert exclusive_session.session_id == "session-exclusive"
        await exclusive.release()

    asyncio.run(scenario())

    assert [(item["node_id"], item["gpu_training_policy"]) for item in requested] == [
        ("node-a", "parallel"),
        ("node-a", "exclusive"),
    ]
    assert all(item["reserve_gpu_phase"] is True for item in requested)


def test_scheduler_instances_share_one_process_gpu_gate():
    first = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    second = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    async def scenario():
        lease = await first._acquire_gpu_phase_lease("node-shared", "exclusive")
        waiter = asyncio.create_task(second._acquire_gpu_phase_lease("node-shared", "parallel"))
        await asyncio.sleep(0)
        assert waiter.done() is False
        await lease.release()
        shared = await asyncio.wait_for(waiter, timeout=1)
        await shared.release()

    asyncio.run(scenario())


def test_scheduler_releases_local_gate_before_retrying_durable_busy_session(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    attempts = 0

    class FakeResourceService:
        def create_session(self, **_kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise qes.QEResourcePhaseError(qes.GPU_LEASE_BUSY_REASON, "occupied")
            return types.SimpleNamespace(session_id="session-after-retry")

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(qes.asyncio, "sleep", no_sleep)

    async def scenario():
        lease, session = await scheduler._reserve_gpu_phase_session(
            service=FakeResourceService(),
            task_id="qe_task",
            loop_index=6,
            node_id="node-retry",
            policy="exclusive",
        )
        assert session.session_id == "session-after-retry"
        await lease.release()

    asyncio.run(scenario())
    assert attempts == 2


def test_gpu_phase_waiter_keeps_slot_for_release_rejected_until_terminal(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    class FakeLease:
        policy = "exclusive"
        active = True

        async def release(self):
            self.active = False

    lease = FakeLease()
    observations = []

    class FakeResourceService:
        def get_session_state(self, _session_id):
            return {"current_phase": "release_rejected"}

        def mark_session_terminal(self, session_id, *, status, reason_code=None):
            observations.append(("terminal", session_id, status, reason_code))

    statuses = iter(["running", "completed"])

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            observations.append(("loop_poll", lease.active))
            return (next(statuses),)

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, *_args, **_kwargs):
            return Cursor()

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(qes, "QEResourcePhaseService", lambda: FakeResourceService())
    monkeypatch.setattr(qes, "get_conn", lambda: Conn())
    monkeypatch.setattr(qes.asyncio, "sleep", no_sleep)

    asyncio.run(
        scheduler._wait_resource_session_until_safe_release(
            session_id="qers_2",
            task_id="qe_task",
            loop_index=2,
            gpu_lease=lease,
        )
    )

    assert observations[0] == ("loop_poll", True)
    assert observations[1] == ("loop_poll", True)
    assert observations[-1] == (
        "terminal",
        "qers_2",
        "completed",
        "QE_GPU_PHASE_LIFECYCLE_INCOMPLETE",
    )


def test_gpu_phase_waiter_releases_local_slot_when_terminal_session_write_fails(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    class FakeLease:
        policy = "exclusive"
        active = True

        async def release(self):
            self.active = False

    lease = FakeLease()

    class FakeResourceService:
        def get_session_state(self, _session_id):
            return {"current_phase": "release_rejected"}

        def mark_session_terminal(self, *_args, **_kwargs):
            raise RuntimeError("database restart window")

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return ("completed",)

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, *_args, **_kwargs):
            return Cursor()

    monkeypatch.setattr(qes, "QEResourcePhaseService", lambda: FakeResourceService())
    monkeypatch.setattr(qes, "get_conn", lambda: Conn())

    asyncio.run(
        scheduler._wait_resource_session_until_safe_release(
            session_id="qers_terminal_db_error",
            task_id="qe_task",
            loop_index=5,
            gpu_lease=lease,
        )
    )

    assert lease.active is False


def test_backend_lifespan_shutdown_never_kills_remote_qe_loops():
    backend_main = Path(qes.__file__).resolve().parents[2] / "main.py"
    source = backend_main.read_text(encoding="utf-8")
    shutdown = source[source.index("    finally:\n        # ── SHUTDOWN ──") :]

    assert "shutdown_event.set()" in shutdown
    assert "kill_loop(" not in shutdown


def test_gpu_phase_waiter_releases_parallel_lease_at_terminal_without_unsupported_reason(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    class FakeLease:
        policy = "parallel"
        active = True

        async def release(self):
            self.active = False

    lease = FakeLease()
    observations = []

    class FakeResourceService:
        def get_session_state(self, _session_id):
            return {"current_phase": "bootstrap"}

        def mark_session_terminal(self, session_id, *, status, reason_code=None):
            observations.append(("terminal", session_id, status, reason_code))

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return ("completed",)

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, *_args, **_kwargs):
            return Cursor()

    monkeypatch.setattr(qes, "QEResourcePhaseService", lambda: FakeResourceService())
    monkeypatch.setattr(qes, "get_conn", lambda: Conn())

    asyncio.run(
        scheduler._wait_resource_session_until_safe_release(
            session_id="qers_parallel",
            task_id="qe_task",
            loop_index=3,
            gpu_lease=lease,
        )
    )

    assert lease.active is False
    assert observations == [
        (
            "terminal",
            "qers_parallel",
            "completed",
            "QE_GPU_PARALLEL_LEASE_TERMINAL_RELEASE",
        )
    ]


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


def test_score_weighted_capacity_strategy_config_exposes_explicit_capacity_params():
    yaml_text = _base_yaml(
        strategy_info={
            "strategy_id": "score_weighted_topk_v2_capacity_v1",
            "source_code": (
                "from score_weighted_strategy_v2 import ScoreWeightedTopkStrategyV2\n\n"
                "class ScoreWeightedTopkStrategyV2CapacityV1(ScoreWeightedTopkStrategyV2):\n"
                "    pass\n"
            ),
            "portfolio_config": {"class": "ScoreWeightedTopkStrategyV2CapacityV1", "kwargs": {}},
            "default_kwargs": {
                "max_single_order_value": 1_000_000_000.0,
                "max_weight": 0.05,
                "max_position_ratio": 0.95,
            },
        },
        custom_params={
            "topk": 20,
            "n_drop": 2,
            "max_single_order_value": 1_000_000_000.0,
            "max_weight": 0.05,
            "max_position_ratio": 0.95,
        },
    )

    assert "class: ScoreWeightedTopkStrategyV2CapacityV1" in yaml_text
    assert "max_single_order_value: 1000000000.0" in yaml_text
    assert "max_weight: 0.05" in yaml_text
    assert "max_position_ratio: 0.95" in yaml_text


def test_score_weighted_capacity_strategy_suspend_filter_uses_capacity_wrapper():
    yaml_text = _base_yaml(
        strategy_info={
            "strategy_id": "score_weighted_topk_v2_capacity_v1",
            "source_code": (
                "from score_weighted_strategy_v2 import ScoreWeightedTopkStrategyV2\n\n"
                "class ScoreWeightedTopkStrategyV2CapacityV1(ScoreWeightedTopkStrategyV2):\n"
                "    pass\n"
            ),
            "portfolio_config": {"class": "ScoreWeightedTopkStrategyV2CapacityV1", "kwargs": {}},
            "default_kwargs": {"max_single_order_value": 1_000_000_000.0},
        },
        custom_params={
            "filter_suspended_on_signal": True,
            "suspend_filter_file": "qe_suspend_filter.json",
            "max_single_order_value": 1_000_000_000.0,
        },
    )

    assert "class: SuspendFilterScoreWeightedTopkStrategyV2CapacityV1" in yaml_text
    assert "module_path: qe_suspend_filter_score_weighted_strategy" in yaml_text
    assert "max_single_order_value: 1000000000.0" in yaml_text


def test_score_weighted_v2_filters_archive_seed_metadata_from_strategy_kwargs():
    yaml_text = _base_yaml(
        strategy_info={
            "strategy_id": "score_weighted_topk_v2",
            "source_code": "class ScoreWeightedTopkStrategyV2:\\n    pass\\n",
            "portfolio_config": {"class": "ScoreWeightedTopkStrategyV2", "kwargs": {}},
        },
        custom_params={
            "topk": 20,
            "archive_policy": "AUTO",
            "archive_reason": "unit",
            "archive_allow_override": True,
            "random_seed": 42,
        },
    )

    assert "class: ScoreWeightedTopkStrategyV2" in yaml_text
    assert "topk: 20" in yaml_text
    assert "archive_policy" not in yaml_text
    assert "archive_reason" not in yaml_text
    assert "archive_allow_override" not in yaml_text
    parsed = yaml.safe_load(yaml_text)
    strategy_kwargs = parsed["port_analysis_config"]["strategy"]["kwargs"]
    assert "random_seed" not in strategy_kwargs
    assert parsed["qe_runtime"]["random_seed"] == 42
    assert parsed["qe_runtime"]["seed_policy"] == "fixed"


def _custom_timeseries_lstm_model_info(training_hp=None):
    return {
        "model_id": "__seed_LSTM_10D_hs64_d02__",
        "model_name": "LSTM_10D_hs64_d02",
        "model_type": "TimeSeries",
        "model_hyperparameters": {"hidden_size": 64, "num_layers": 1, "dropout": 0.2},
        "model_training_hyperparameters": training_hp or {
            "n_epochs": 200,
            "lr": 0.001,
            "batch_size": 4096,
            "early_stop": 20,
            "weight_decay": 0.001,
        },
        "code_text": "class LSTMModel: pass\nmodel_cls = LSTMModel\n",
    }


def _builtin_transformer_model_info():
    # Built-in qlib Transformer referenced via pt_model_uri (no custom code_text).
    # Reaches the GeneralPTNN "no source code" branch in _compose_conf_yaml.
    return {
        "model_id": "__seed_Transformer_small_v1__",
        "model_name": "Transformer",
        "model_type": "PTNN",
        "model_hyperparameters": {
            "pt_model_uri": "qlib.contrib.model.pytorch_transformer_ts.Transformer",
            "d_feat": 20,
            "d_model": 64,
            "nhead": 4,
            "num_layers": 2,
            "dropout": 0.1,
        },
        "model_training_hyperparameters": {
            "n_epochs": 100,
            "lr": 0.0002,
            "batch_size": 2048,
            "early_stop": 15,
            "weight_decay": 0.0001,
            "GPU": 0,
        },
    }


def _gats_model_info():
    return {
        "model_id": "__seed_GATs_default_v1__",
        "model_name": "GATs",
        "model_type": "GATS",
        "model_config": {
            "class": "GATs",
            "module_path": "qlib.contrib.model.pytorch_gats_ts",
            "dataset_type": "TSDatasetH",
        },
        "default_dataset_type": "TSDatasetH",
        "model_hyperparameters": {
            "d_feat": 20,
            "hidden_size": 64,
            "num_layers": 2,
            "dropout": 0.1,
            "base_model": "GRU",
            "n_epochs": 80,
            "lr": 0.0003,
            "metric": "loss",
            "early_stop": 10,
            "batch_size": 4096,
            "GPU": 0,
            "n_jobs": 2,
            "seed": 20260708,
        },
        "model_training_hyperparameters": {
            "n_epochs": 80,
            "lr": 0.0003,
            "batch_size": 4096,
            "early_stop": 10,
        },
    }


def _efficient_gats_model_info():
    model_info = _gats_model_info()
    model_info["model_id"] = "__seed_EfficientGATs_default_v1__"
    model_info["model_name"] = "EfficientGATs"
    model_info["model_config"] = {
        "class": "EfficientGATs",
        "module_path": "aistock_models.efficient_gats",
        "dataset_type": "TSDatasetH",
    }
    return model_info


_MODEL_CATALOG_COLUMNS = [
    "model_id",
    "model_name",
    "model_type",
    "model_hyperparameters",
    "model_training_hyperparameters",
    "model_config",
    "ic",
    "annualized_return",
    "is_sota",
    "code_text",
]


class _FakeModelCatalogCursor:
    description = [(col,) for col in _MODEL_CATALOG_COLUMNS]

    def __init__(self, row):
        self._row = row
        self._params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        assert "model_config" in sql
        self._params = params

    def fetchone(self):
        if not self._params or self._params[0] != self._row["model_id"]:
            return None
        return tuple(self._row.get(col) for col in _MODEL_CATALOG_COLUMNS)


class _FakeModelCatalogConn:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeModelCatalogCursor(self._row)


def _catalog_row_from_model_info(model_info):
    return {
        "model_id": model_info["model_id"],
        "model_name": model_info["model_name"],
        "model_type": model_info["model_type"],
        "model_hyperparameters": model_info.get("model_hyperparameters"),
        "model_training_hyperparameters": model_info.get("model_training_hyperparameters"),
        "model_config": model_info.get("model_config"),
        "ic": None,
        "annualized_return": None,
        "is_sota": False,
        "code_text": None,
    }


def _compose_in_memory_with_catalog_model(monkeypatch, catalog_row, *, model_id):
    import backend.services.quantevolver.config_composer as composer_module

    composer = ConfigComposer()
    monkeypatch.setattr(composer_module, "get_conn", lambda: _FakeModelCatalogConn(catalog_row))
    monkeypatch.setattr(composer, "_get_factors_info", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(composer, "_get_strategy_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        composer,
        "_prepare_risk_policy_runtime",
        lambda **_kwargs: (_kwargs["custom_params"], None),
    )
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **_kwargs: (_kwargs["custom_params"], None),
    )
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read_exp_res")

    return composer.compose_experiment_in_memory(
        factor_names=["Alpha001"],
        model_id=model_id,
        strategy_id="score_weighted_topk_v2",
        data_split=DATA_SPLIT,
        custom_params={},
        skip_db_save=True,
        execution_algo="CLOSE_PRICE",
        execution_algo_params={},
    )


class _MiniGatsSegment:
    def __init__(self, index, values, industry_ids=None):
        self._index = index
        self._values = values
        self._industry_ids = industry_ids
        self.empty = len(index) == 0

    def get_index(self):
        return self._index

    def get_industry_ids(self):
        return self._industry_ids

    def config(self, **_kwargs):
        return None

    def __len__(self):
        return len(self._index)

    def __getitem__(self, index):
        import torch

        if isinstance(index, (list, tuple)) and len(index) == 1:
            index = index[0]
        if hasattr(index, "detach"):
            index = index.detach().cpu().numpy()
        return torch.as_tensor(self._values[index], dtype=torch.float32)


class _MiniGatsDataset:
    def __init__(self, d_feat=2, step_len=3, stocks=6, include_industry=False, missing_industry_rows=None):
        self.d_feat = d_feat
        self.step_len = step_len
        self.stocks = stocks
        self.include_industry = include_industry
        self.missing_industry_rows = set(missing_industry_rows or [])
        self._segments = {
            "train": self._make_segment("2021-01-04", 4),
            "valid": self._make_segment("2021-01-08", 3),
            "test": self._make_segment("2021-01-13", 3),
        }

    def _make_segment(self, start, days):
        dates = pd.bdate_range(start, periods=days)
        rows = []
        values = []
        industry_ids = []
        for day_idx, date in enumerate(dates):
            for stock_idx in range(self.stocks):
                instrument = f"STK{stock_idx:03d}"
                stock_signal = (stock_idx - (self.stocks - 1) / 2.0) / self.stocks
                day_signal = day_idx / max(days - 1, 1)
                seq = np.zeros((self.step_len, self.d_feat + 1), dtype="float32")
                for step_idx in range(self.step_len):
                    seq[step_idx, 0] = stock_signal + 0.05 * step_idx + 0.1 * day_signal
                    seq[step_idx, 1] = np.sin(stock_idx + day_idx + step_idx)
                    seq[step_idx, -1] = 0.7 * stock_signal + 0.2 * day_signal
                rows.append((date, instrument))
                values.append(seq)
                if self.include_industry:
                    row_offset = day_idx * self.stocks + stock_idx
                    industry_ids.append(None if row_offset in self.missing_industry_rows else f"SW2_{stock_idx // 2}")
        index = pd.MultiIndex.from_tuples(rows, names=["datetime", "instrument"])
        return _MiniGatsSegment(
            index,
            np.stack(values),
            industry_ids=np.asarray(industry_ids, dtype=object) if self.include_industry else None,
        )

    def prepare(self, segment, **_kwargs):
        return self._segments[segment]


class _FakeSwMemberCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []
        self._result = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        symbols, trade_date, asof_date = params
        assert "ROW_NUMBER() OVER" in sql
        assert "in_date <= %s" in sql
        assert "(out_date IS NULL OR out_date >= %s)" in sql
        assert trade_date == asof_date
        self.executed.append({"sql": sql, "params": params})
        selected = []
        for row in self._rows:
            ts_code, l2_code, in_date, out_date = row[:4]
            if ts_code not in symbols:
                continue
            if pd.Timestamp(in_date).date() > trade_date:
                continue
            if out_date is not None and pd.Timestamp(out_date).date() < trade_date:
                continue
            selected.append((ts_code, l2_code, pd.Timestamp(in_date).date(), out_date))
        selected.sort(
            key=lambda item: (
                item[0],
                pd.Timestamp(item[2]),
                pd.Timestamp(item[3]) if item[3] is not None else pd.Timestamp.max,
            ),
            reverse=True,
        )
        result = {}
        for row in selected:
            result.setdefault(row[0], row)
        self._result = sorted(result.values(), key=lambda item: item[0])

    def fetchall(self):
        return list(self._result)


class _FakeSwMemberConn:
    def __init__(self, rows):
        self.cursor_obj = _FakeSwMemberCursor(rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_obj


def _fake_sw_member_conn_factory(rows):
    conns = []

    @contextmanager
    def _factory():
        conn = _FakeSwMemberConn(rows)
        conns.append(conn)
        yield conn

    _factory.conns = conns
    return _factory


def test_gats_seed_composes_direct_qlib_model_with_tsdataseth():
    yaml_text = _base_yaml(model_info=_gats_model_info())

    assert "class: GATs" in yaml_text
    assert "module_path: qlib.contrib.model.pytorch_gats_ts" in yaml_text
    assert "class: GeneralPTNN" not in yaml_text
    assert "pt_model_kwargs" not in yaml_text
    assert "class: TSDatasetH" in yaml_text
    assert "step_len: 20" in yaml_text
    assert "d_feat: {{ num_features }}" in yaml_text
    assert "dropout: 0.1" in yaml_text
    assert "base_model: GRU" in yaml_text


def test_efficient_gats_seed_respects_model_config_module_path():
    yaml_text = _base_yaml(model_info=_efficient_gats_model_info())

    assert "class: EfficientGATs" in yaml_text
    assert "module_path: aistock_models.efficient_gats" in yaml_text
    assert "class: GeneralPTNN" not in yaml_text
    assert "pt_model_kwargs" not in yaml_text
    assert "class: TSDatasetH" in yaml_text
    assert "step_len: 20" in yaml_text
    assert "d_feat: {{ num_features }}" in yaml_text


def test_catalog_get_model_info_preserves_efficient_gats_model_config(monkeypatch):
    model_info = _efficient_gats_model_info()
    catalog_row = _catalog_row_from_model_info(model_info)

    result = _compose_in_memory_with_catalog_model(
        monkeypatch,
        catalog_row,
        model_id="__seed_EfficientGATs_default_v1__",
    )

    conf_yaml = result["experiment_files"]["conf.yaml"]
    assert "class: EfficientGATs" in conf_yaml
    assert "module_path: aistock_models.efficient_gats" in conf_yaml
    assert "class: GATs" not in conf_yaml
    assert "module_path: qlib.contrib.model.pytorch_gats_ts" not in conf_yaml


@pytest.mark.parametrize(
    "model_config",
    [
        {"class": "GATs", "module_path": "qlib.contrib.model.pytorch_gats_ts", "dataset_type": "TSDatasetH"},
        None,
    ],
)
def test_catalog_get_model_info_keeps_original_gats_default(monkeypatch, model_config):
    model_info = _gats_model_info()
    model_info["model_config"] = model_config
    catalog_row = _catalog_row_from_model_info(model_info)

    result = _compose_in_memory_with_catalog_model(
        monkeypatch,
        catalog_row,
        model_id="__seed_GATs_default_v1__",
    )

    conf_yaml = result["experiment_files"]["conf.yaml"]
    assert "class: GATs" in conf_yaml
    assert "module_path: qlib.contrib.model.pytorch_gats_ts" in conf_yaml
    assert "class: EfficientGATs" not in conf_yaml
    assert "module_path: aistock_models.efficient_gats" not in conf_yaml


def test_catalog_get_model_info_invalid_model_config_fails_loud(monkeypatch):
    import backend.services.quantevolver.config_composer as composer_module

    model_info = _efficient_gats_model_info()
    catalog_row = _catalog_row_from_model_info(model_info)
    catalog_row["model_config"] = "{invalid-json"
    monkeypatch.setattr(composer_module, "get_conn", lambda: _FakeModelCatalogConn(catalog_row))

    with pytest.raises(ValueError, match="reason_code=model_config_invalid"):
        ConfigComposer()._get_model_info("__seed_EfficientGATs_default_v1__")


def test_gats_custom_params_route_to_model_kwargs_not_strategy_or_pt_model_kwargs():
    yaml_text = _base_yaml(
        model_info=_gats_model_info(),
        custom_params={
            "dropout": 0.25,
            "base_model": "LSTM",
            "hidden_size": 32,
            "num_layers": 1,
            "lr": "0.0005",
            "batch_size": 128,
            "gpu_resident": True,
            "gats_adjacency_mode": "industry_bias",
            "gats_industry_gamma_init": 0.0,
            "gats_industry_embedding": "on",
            "gats_industry_embedding_dim": 8,
        },
    )
    conf = _parse_conf_yaml_with_jinja_placeholders(yaml_text)

    model = conf["task"]["model"]
    strategy_kwargs = conf["port_analysis_config"]["strategy"]["kwargs"]
    assert model["class"] == "GATs"
    assert model["module_path"] == "qlib.contrib.model.pytorch_gats_ts"
    assert model["kwargs"]["dropout"] == 0.25
    assert model["kwargs"]["base_model"] == "LSTM"
    assert model["kwargs"]["hidden_size"] == 32
    assert model["kwargs"]["num_layers"] == 1
    assert model["kwargs"]["lr"] == 0.0005
    assert model["kwargs"]["batch_size"] == 128
    assert model["kwargs"]["gpu_resident"] is True
    assert model["kwargs"]["gats_adjacency_mode"] == "industry_bias"
    assert model["kwargs"]["gats_industry_gamma_init"] == 0.0
    assert model["kwargs"]["gats_industry_embedding"] in ("on", True)
    assert model["kwargs"]["gats_industry_embedding_dim"] == 8
    assert "pt_model_kwargs" not in model["kwargs"]
    for key in (
        "dropout",
        "base_model",
        "hidden_size",
        "num_layers",
        "lr",
        "batch_size",
        "gpu_resident",
        "gats_adjacency_mode",
        "gats_industry_gamma_init",
        "gats_industry_embedding",
        "gats_industry_embedding_dim",
    ):
        assert key not in strategy_kwargs


def test_gats_v25_minute_execution_config_remains_daily_signal_nested_executor():
    yaml_text = _base_yaml(
        model_info=_gats_model_info(),
        custom_params={"execution_algo": "V25_TWO_STAGE"},
        execution_algo="V25_TWO_STAGE",
    )
    conf = _parse_conf_yaml_with_jinja_placeholders(yaml_text)

    assert conf["task"]["model"]["class"] == "GATs"
    assert conf["port_analysis_config"]["executor"]["class"] == "NestedExecutor"
    assert conf["port_analysis_config"]["executor"]["kwargs"]["time_per_step"] == "day"
    assert (
        conf["port_analysis_config"]["executor"]["kwargs"]["inner_executor"]["kwargs"]["time_per_step"]
        == "1min"
    )
    assert conf["port_analysis_config"]["strategy"]["kwargs"]["signal"] == "<PRED>"
    assert conf["port_analysis_config"]["backtest"]["exchange_kwargs"]["freq"] == "1min"


def test_qlib_gats_minimal_fit_predict_non_degenerate_rank_ic(tmp_path):
    gats_module = pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    pytest.importorskip("torch")

    dataset = _MiniGatsDataset()
    sampler = gats_module.DailyBatchSampler(dataset.prepare("train"))
    first_batch = next(iter(sampler))
    train_index = dataset.prepare("train").get_index()
    assert len(first_batch) == dataset.stocks
    assert train_index[first_batch].get_level_values("datetime").nunique() == 1

    model = gats_module.GATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
    )
    evals_result = {}
    model.fit(dataset, evals_result=evals_result, save_path=str(tmp_path / "gats.pt"))
    preds = model.predict(dataset)

    assert not preds.empty
    assert preds.index.equals(dataset.prepare("test").get_index())
    assert preds.notna().any()
    assert np.isfinite(preds.to_numpy()).any()
    assert not np.allclose(preds.to_numpy(), preds.iloc[0])
    assert evals_result["train"] and evals_result["valid"]

    labels = pd.Series(
        dataset.prepare("test")._values[:, -1, -1],
        index=dataset.prepare("test").get_index(),
        name="label",
    )
    rank_ic = (
        pd.DataFrame({"pred": preds, "label": labels})
        .groupby(level="datetime")
        .apply(lambda frame: frame["pred"].rank().corr(frame["label"].rank()))
    )
    finite_rank_ic = rank_ic[np.isfinite(rank_ic)]
    assert not finite_rank_ic.empty


def _import_efficient_gats_module():
    models_root = PROJECT_ROOT / "aistock_models"
    if str(models_root) not in sys.path:
        sys.path.insert(0, str(models_root))
    import aistock_models.efficient_gats as efficient_gats

    return efficient_gats


def _import_gats_industry_provider_module():
    models_root = PROJECT_ROOT / "aistock_models"
    if str(models_root) not in sys.path:
        sys.path.insert(0, str(models_root))
    import aistock_models.gats_industry_provider as provider_mod

    return provider_mod


def test_efficient_gats_attention_is_numerically_identical_to_naive():
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    for seed, sample_num, hidden_size in ((1, 3, 4), (7, 8, 16), (13, 17, 32)):
        torch.manual_seed(seed)
        hidden = torch.randn(sample_num, hidden_size, dtype=torch.float32)
        attention_vector = torch.randn(hidden_size * 2, 1, dtype=torch.float32)

        # Match qlib 0.9.7 GATModel.cal_attention exactly:
        # e_x = x.expand(N, N, H); e_y = transpose(e_x, 0, 1);
        # cat((e_x, e_y), dim=2).view(-1, 2H).
        e_x = hidden.expand(sample_num, sample_num, hidden_size)
        e_y = torch.transpose(e_x, 0, 1)
        naive_input = torch.cat((e_x, e_y), dim=2).view(-1, hidden_size * 2)
        naive_raw = attention_vector.t().mm(naive_input.t()).view(sample_num, sample_num)

        efficient_raw = efficient_gats.additive_gats_attention_logits(
            hidden,
            hidden,
            attention_vector,
        )
        assert torch.allclose(naive_raw, efficient_raw, atol=1e-5)

        naive_leaky = torch.nn.functional.leaky_relu(naive_raw)
        efficient_leaky = torch.nn.functional.leaky_relu(efficient_raw)
        assert torch.allclose(naive_leaky, efficient_leaky, atol=1e-5)

        naive_softmax = torch.softmax(naive_leaky, dim=1)
        efficient_softmax = torch.softmax(efficient_leaky, dim=1)
        assert torch.allclose(naive_softmax, efficient_softmax, atol=1e-5)

        naive_model = efficient_gats.QlibGATModel(
            d_feat=2,
            hidden_size=hidden_size,
            num_layers=1,
            dropout=0.0,
        )
        efficient_model = efficient_gats.EfficientGATModel(
            d_feat=2,
            hidden_size=hidden_size,
            num_layers=1,
            dropout=0.0,
        )
        efficient_model.load_state_dict(naive_model.state_dict())
        assert torch.allclose(
            naive_model.cal_attention(hidden, hidden),
            efficient_model.cal_attention(hidden, hidden),
            atol=1e-5,
        )


def test_efficient_gats_industry_bias_increases_same_industry_attention_without_masking_cross_industry():
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    torch.manual_seed(11)
    model = efficient_gats.EfficientGATModel(
        d_feat=2,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        gats_adjacency_mode="industry_bias",
        gats_industry_gamma_init=0.0,
    )
    hidden = torch.randn(6, 4)
    industry_ids = torch.tensor([0, 0, 1, 1, 2, 2], dtype=torch.long)

    with torch.no_grad():
        att_off = model.cal_attention(hidden, hidden, industry_ids=None)
        model.industry_bias_gamma.fill_(2.0)
        att_bias = model.cal_attention(hidden, hidden, industry_ids=industry_ids)

    same = efficient_gats.EfficientGATModel.industry_same_matrix(
        industry_ids,
        device=att_bias.device,
        dtype=torch.bool,
    )
    same_mass_off = (att_off * same).sum(dim=1)
    same_mass_bias = (att_bias * same).sum(dim=1)
    cross_weight = att_bias.masked_select(~same)

    assert torch.all(same_mass_bias > same_mass_off)
    assert torch.all(cross_weight > 0)


def test_efficient_gats_industry_bias_gamma_zero_matches_off_attention():
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    torch.manual_seed(17)
    off_model = efficient_gats.EfficientGATModel(
        d_feat=2,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        gats_adjacency_mode="off",
    )
    bias_model = efficient_gats.EfficientGATModel(
        d_feat=2,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        gats_adjacency_mode="industry_bias",
        gats_industry_gamma_init=0.0,
    )
    bias_model.load_state_dict(off_model.state_dict(), strict=False)
    hidden = torch.randn(6, 4)
    industry_ids = torch.tensor([0, 0, 1, 1, 2, 2], dtype=torch.long)

    assert torch.allclose(
        off_model.cal_attention(hidden, hidden),
        bias_model.cal_attention(hidden, hidden, industry_ids=industry_ids),
        atol=1e-6,
    )


def test_efficient_gats_full_pool_attention_smoke_no_n2_hidden_materialization(monkeypatch):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    sample_num = 5000
    hidden_size = 64

    if torch.cuda.is_available():
        device = torch.device("cuda")
        torch.cuda.empty_cache()
        model = efficient_gats.EfficientGATModel(
            d_feat=4,
            hidden_size=hidden_size,
            num_layers=1,
            dropout=0.0,
        ).to(device)
        batch = torch.randn(sample_num, 2, 4, device=device)
        pred = model(batch)
        loss = pred.float().pow(2).mean()
        loss.backward()
        assert pred.shape == (sample_num,)
        assert torch.isfinite(loss)
    else:
        model = efficient_gats.EfficientGATModel(
            d_feat=4,
            hidden_size=hidden_size,
            num_layers=1,
            dropout=0.0,
        )
        hidden = torch.randn(sample_num, hidden_size, requires_grad=True)
        original_cat = torch.cat

        def _reject_n2_hidden_cat(tensors, *args, **kwargs):
            result = original_cat(tensors, *args, **kwargs)
            assert result.shape != (sample_num, sample_num, hidden_size * 2)
            return result

        with monkeypatch.context() as mp:
            mp.setattr(torch, "cat", _reject_n2_hidden_cat)
            att = model.cal_attention(hidden, hidden)
            assert att.shape == (sample_num, sample_num)
            loss = att[:16, :16].sum()
            loss.backward()
        assert hidden.grad is not None
        assert torch.isfinite(hidden.grad).any()


def test_efficient_gats_minimal_fit_predict_non_degenerate_rank_ic(tmp_path):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset()
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
    )
    evals_result = {}
    model.fit(dataset, evals_result=evals_result, save_path=str(tmp_path / "efficient_gats.pt"))
    preds = model.predict(dataset)

    assert not preds.empty
    assert preds.index.equals(dataset.prepare("test").get_index())
    assert preds.notna().any()
    assert np.isfinite(preds.to_numpy()).any()
    assert not np.allclose(preds.to_numpy(), preds.iloc[0])
    assert evals_result["train"] and evals_result["valid"]

    labels = pd.Series(
        dataset.prepare("test")._values[:, -1, -1],
        index=dataset.prepare("test").get_index(),
        name="label",
    )
    rank_ic = (
        pd.DataFrame({"pred": preds, "label": labels})
        .groupby(level="datetime")
        .apply(lambda frame: frame["pred"].rank().corr(frame["label"].rank()))
    )
    assert not rank_ic[np.isfinite(rank_ic)].empty


def test_efficient_gats_adjacency_off_matches_default_fit_predict(tmp_path):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset(include_industry=True)
    common_kwargs = dict(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
    )
    default_model = efficient_gats.EfficientGATs(**common_kwargs)
    off_model = efficient_gats.EfficientGATs(**common_kwargs, gats_adjacency_mode="off")

    default_model.fit(dataset, evals_result={}, save_path=str(tmp_path / "default.pt"))
    off_model.fit(dataset, evals_result={}, save_path=str(tmp_path / "off.pt"))
    default_preds = default_model.predict(dataset)
    off_preds = off_model.predict(dataset)

    assert default_preds.index.equals(off_preds.index)
    assert np.allclose(default_preds.to_numpy(), off_preds.to_numpy(), atol=1e-6)


def test_efficient_gats_industry_embedding_off_matches_default_fit_predict(tmp_path):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset(include_industry=True)
    common_kwargs = dict(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=19,
    )
    default_model = efficient_gats.EfficientGATs(**common_kwargs)
    embedding_off_model = efficient_gats.EfficientGATs(
        **common_kwargs,
        gats_adjacency_mode="off",
        gats_industry_embedding="off",
        gats_industry_embedding_dim=8,
    )

    default_model.fit(dataset, evals_result={}, save_path=str(tmp_path / "default.pt"))
    embedding_off_model.fit(dataset, evals_result={}, save_path=str(tmp_path / "embedding_off.pt"))
    default_preds = default_model.predict(dataset)
    off_preds = embedding_off_model.predict(dataset)

    assert default_preds.index.equals(off_preds.index)
    assert np.allclose(default_preds.to_numpy(), off_preds.to_numpy(), atol=1e-6)


def _resident_segment_for_test(model, segment, torch):
    cpu_segment = model._preload_segment_to_cpu(segment, segment_name="train")
    return {
        "segment_name": cpu_segment["segment_name"],
        "tensor": cpu_segment["tensor"],
        "daily_indices": [
            torch.as_tensor(index, dtype=torch.long)
            for index in cpu_segment["daily_indices"]
        ],
        "index": cpu_segment["index"],
        "industry_ids": cpu_segment.get("industry_ids"),
    }


def test_efficient_gats_gpu_resident_daily_batch_data_equivalent_to_streaming():
    gats_module = pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()
    from torch.utils.data import DataLoader

    dataset = _MiniGatsDataset()
    segment = dataset.prepare("train")
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gpu_resident=True,
    )
    resident = _resident_segment_for_test(model, segment, torch)

    stream_loader = DataLoader(
        segment,
        sampler=gats_module.DailyBatchSampler(segment),
        num_workers=0,
        drop_last=True,
    )
    stream_batches = [batch.squeeze() for batch in stream_loader]
    resident_batches = list(model._iter_resident_batches(resident, shuffle=False))

    assert len(stream_batches) == len(resident_batches)
    for stream_batch, resident_batch in zip(stream_batches, resident_batches):
        assert torch.allclose(resident_batch.cpu(), stream_batch.cpu())


def test_efficient_gats_industry_ids_resident_and_streaming_side_channel():
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset(include_industry=True)
    segment = dataset.prepare("train")
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gpu_resident=True,
        gats_adjacency_mode="industry_bias",
    )
    resident_cpu = model._preload_segment_to_cpu(segment, segment_name="train")
    resident = {
        "segment_name": resident_cpu["segment_name"],
        "tensor": resident_cpu["tensor"],
        "daily_indices": [torch.as_tensor(index, dtype=torch.long) for index in resident_cpu["daily_indices"]],
        "index": resident_cpu["index"],
        "industry_ids": resident_cpu["industry_ids"],
    }
    streaming = model._preload_streaming_segment_metadata(segment, segment_name="train")

    resident_data, resident_ids = next(model._iter_resident_batches(resident, shuffle=False, include_industry=True))
    streaming_data, streaming_ids = next(model._iter_streaming_batches(streaming, shuffle=False))

    assert torch.allclose(resident_data.cpu(), streaming_data.cpu())
    assert torch.equal(resident_ids.cpu(), streaming_ids.cpu())
    assert resident_ids.shape == (dataset.stocks,)


def test_efficient_gats_industry_missing_ids_are_loud_and_unbiased(capsys):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset(include_industry=True, missing_industry_rows={1})
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gats_adjacency_mode="industry_bias",
    )

    resident_cpu = model._preload_segment_to_cpu(dataset.prepare("train"), segment_name="train")
    captured = capsys.readouterr()
    assert "reason_code=efficient_gats_industry_id_missing" in captured.out
    assert model.gats_adjacency_last_event["reason_code"] == "efficient_gats_industry_id_missing"

    ids = resident_cpu["industry_ids"][: dataset.stocks]
    hidden = torch.randn(dataset.stocks, 4)
    with torch.no_grad():
        model.GAT_model.industry_bias_gamma.fill_(2.0)
        att_off = model.GAT_model.cal_attention(hidden, hidden, industry_ids=None)
        att_bias = model.GAT_model.cal_attention(hidden, hidden, industry_ids=ids)

    assert ids[1].item() == efficient_gats._MISSING_INDUSTRY_ID
    assert torch.allclose(att_bias[1], att_off[1], atol=1e-6)
    assert torch.all(att_bias[:, 1] > 0)


def test_efficient_gats_gpu_resident_train_epoch_has_no_per_batch_to(monkeypatch):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset()
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gpu_resident=True,
    )
    resident = _resident_segment_for_test(model, dataset.prepare("train"), torch)
    calls = []
    original_to = torch.Tensor.to

    def _count_to(tensor, *args, **kwargs):
        calls.append((tuple(tensor.shape), args, kwargs))
        return original_to(tensor, *args, **kwargs)

    monkeypatch.setattr(torch.Tensor, "to", _count_to)
    model.train_epoch(resident)

    assert calls == []


def test_efficient_gats_gpu_resident_activation_performs_no_vram_query(monkeypatch):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset()
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gpu_resident=True,
    )
    resident_cpu = model._preload_segment_to_cpu(dataset.prepare("train"), segment_name="train")

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("GPU/VRAM resource query must not run")

    model.device = torch.device("cuda:0")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "mem_get_info", _forbidden)
    monkeypatch.setattr(torch.cuda, "memory_reserved", _forbidden)
    monkeypatch.setattr(torch.cuda, "memory_allocated", _forbidden)
    assert model._can_activate_gpu_resident([resident_cpu]) is True
    assert model.gpu_resident_last_fallback is None


def test_efficient_gats_legacy_resource_options_are_explicitly_ignored(capsys):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    efficient_gats = _import_efficient_gats_module()
    dataset = _MiniGatsDataset()
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gpu_resident=True,
        gpu_resident_vram_margin_bytes=123,
        gpu_resident_working_memory_multiplier=9,
        gpu_phase_release_tolerance_bytes=128,
    )
    output = capsys.readouterr().out
    assert "reason_code=QE_GPU_RESOURCE_OPTIONS_REMOVED" in output
    assert not hasattr(model, "gpu_phase_release_tolerance_bytes")
    assert not hasattr(model, "gpu_resident_vram_margin_bytes")


def test_efficient_gats_gpu_resident_predict_uses_lifecycle_without_resource_queries(monkeypatch):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset()
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gpu_resident=True,
    )

    def _cpu_resident_move(segment):
        return {
            "segment_name": segment["segment_name"],
            "tensor": segment["tensor"],
            "daily_indices": [
                torch.as_tensor(index, dtype=torch.long)
                for index in segment["daily_indices"]
            ],
            "index": segment["index"],
            "industry_ids": segment.get("industry_ids"),
        }

    monkeypatch.setattr(model, "_move_segment_to_gpu", _cpu_resident_move)
    model.fitted = True

    calls = []
    model.device = torch.device("cuda:0")
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "empty_cache", lambda: calls.append("empty_cache"))
    monkeypatch.setattr(
        efficient_gats,
        "_finalize_gpu_phase_lifecycle",
        lambda *, predict_error=None, next_phase="backtest": calls.append("lifecycle"),
    )

    original_preload = model._preload_segment_to_cpu

    def _assert_empty_cache_before_preload(segment, *, segment_name):
        assert calls == ["empty_cache"]
        return original_preload(segment, segment_name=segment_name)

    original_can_activate = efficient_gats.EfficientGATs._can_activate_gpu_resident.__get__(
        model,
        type(model),
    )

    def _assert_empty_cache_already_called(segments):
        assert calls == ["empty_cache"]
        return original_can_activate(segments)

    monkeypatch.setattr(model, "_preload_segment_to_cpu", _assert_empty_cache_before_preload)
    monkeypatch.setattr(model, "_can_activate_gpu_resident", _assert_empty_cache_already_called)
    preds = model.predict(dataset)

    assert calls == ["empty_cache", "lifecycle"]
    assert not preds.empty


def _cpu_resident_move_for_test(torch):
    def _move(segment):
        return {
            "segment_name": segment["segment_name"],
            "tensor": segment["tensor"],
            "daily_indices": [
                torch.as_tensor(index, dtype=torch.long)
                for index in segment["daily_indices"]
            ],
            "index": segment["index"],
            "industry_ids": segment.get("industry_ids"),
        }

    return _move


def test_efficient_gats_gpu_resident_predict_matches_streaming_when_not_shuffled(monkeypatch, tmp_path):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset()
    common_kwargs = dict(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
    )
    streaming_model = efficient_gats.EfficientGATs(**common_kwargs, gpu_resident=False)
    resident_model = efficient_gats.EfficientGATs(
        **common_kwargs,
        gpu_resident=True,
        gpu_resident_shuffle_days=False,
    )
    monkeypatch.setattr(resident_model, "_can_activate_gpu_resident", lambda _segments: True)
    monkeypatch.setattr(resident_model, "_move_segment_to_gpu", _cpu_resident_move_for_test(torch))

    streaming_model.fit(
        dataset,
        evals_result={},
        save_path=str(tmp_path / "efficient_gats_streaming.pt"),
    )
    resident_model.fit(
        dataset,
        evals_result={},
        save_path=str(tmp_path / "efficient_gats_resident.pt"),
    )
    streaming_preds = streaming_model.predict(dataset)
    resident_preds = resident_model.predict(dataset)

    assert streaming_preds.index.equals(resident_preds.index)
    assert np.allclose(streaming_preds.to_numpy(), resident_preds.to_numpy(), atol=1e-6)

    labels = pd.Series(
        dataset.prepare("test")._values[:, -1, -1],
        index=dataset.prepare("test").get_index(),
        name="label",
    )
    streaming_rank_ic = (
        pd.DataFrame({"pred": streaming_preds, "label": labels})
        .groupby(level="datetime")
        .apply(lambda frame: frame["pred"].rank().corr(frame["label"].rank()))
    )
    resident_rank_ic = (
        pd.DataFrame({"pred": resident_preds, "label": labels})
        .groupby(level="datetime")
        .apply(lambda frame: frame["pred"].rank().corr(frame["label"].rank()))
    )
    assert np.allclose(
        streaming_rank_ic.dropna().to_numpy(),
        resident_rank_ic.dropna().to_numpy(),
        atol=1e-6,
    )


def test_efficient_gats_gpu_resident_test_epoch_item_sync_is_constant(monkeypatch):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset()
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gpu_resident=True,
    )
    resident = _resident_segment_for_test(model, dataset.prepare("train"), torch)
    calls = []
    original_item = torch.Tensor.item

    def _count_item(tensor, *args, **kwargs):
        calls.append(tuple(tensor.shape))
        return original_item(tensor, *args, **kwargs)

    monkeypatch.setattr(torch.Tensor, "item", _count_item)
    model.test_epoch(resident)

    assert len(calls) <= 2


def test_efficient_gats_gpu_resident_preload_float32_and_daily_paths_do_not_float(monkeypatch):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset()
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gpu_resident=True,
    )
    resident_cpu = model._preload_segment_to_cpu(dataset.prepare("train"), segment_name="train")
    assert resident_cpu["tensor"].dtype == torch.float32
    resident = {
        "segment_name": resident_cpu["segment_name"],
        "tensor": resident_cpu["tensor"],
        "daily_indices": [
            torch.as_tensor(index, dtype=torch.long)
            for index in resident_cpu["daily_indices"]
        ],
        "index": resident_cpu["index"],
    }
    calls = []
    original_float = torch.Tensor.float

    def _count_float(tensor, *args, **kwargs):
        calls.append(tuple(tensor.shape))
        return original_float(tensor, *args, **kwargs)

    monkeypatch.setattr(torch.Tensor, "float", _count_float)
    model.train_epoch(resident)
    model.test_epoch(resident)

    monkeypatch.setattr(model, "_can_activate_gpu_resident", lambda _segments: True)
    monkeypatch.setattr(model, "_move_segment_to_gpu", _cpu_resident_move_for_test(torch))
    model.fitted = True
    preds = model.predict(dataset)

    assert calls == []
    assert not preds.empty


def test_efficient_gats_gpu_resident_fit_predict_non_degenerate_rank_ic(monkeypatch, tmp_path):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset()
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gpu_resident=True,
    )

    def _cpu_resident_move(segment):
        return {
            "segment_name": segment["segment_name"],
            "tensor": segment["tensor"],
            "daily_indices": [
                torch.as_tensor(index, dtype=torch.long)
                for index in segment["daily_indices"]
            ],
            "index": segment["index"],
            "industry_ids": segment.get("industry_ids"),
        }

    monkeypatch.setattr(model, "_can_activate_gpu_resident", lambda _segments: True)
    monkeypatch.setattr(model, "_move_segment_to_gpu", _cpu_resident_move)

    evals_result = {}
    model.fit(dataset, evals_result=evals_result, save_path=str(tmp_path / "efficient_gats_gpu_resident.pt"))
    preds = model.predict(dataset)

    assert model.gpu_resident_active is True
    assert not preds.empty
    assert preds.index.equals(dataset.prepare("test").get_index())
    assert preds.notna().any()
    assert np.isfinite(preds.to_numpy()).any()
    assert not np.allclose(preds.to_numpy(), preds.iloc[0])
    assert evals_result["train"] and evals_result["valid"]

    labels = pd.Series(
        dataset.prepare("test")._values[:, -1, -1],
        index=dataset.prepare("test").get_index(),
        name="label",
    )
    rank_ic = (
        pd.DataFrame({"pred": preds, "label": labels})
        .groupby(level="datetime")
        .apply(lambda frame: frame["pred"].rank().corr(frame["label"].rank()))
    )
    assert not rank_ic[np.isfinite(rank_ic)].empty


def test_efficient_gats_industry_bias_fit_predict_non_degenerate_rank_ic(tmp_path):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset(include_industry=True)
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gats_adjacency_mode="industry_bias",
    )

    evals_result = {}
    model.fit(dataset, evals_result=evals_result, save_path=str(tmp_path / "efficient_gats_industry_bias.pt"))
    preds = model.predict(dataset)

    assert not preds.empty
    assert preds.index.equals(dataset.prepare("test").get_index())
    assert preds.notna().any()
    assert np.isfinite(preds.to_numpy()).any()
    assert not np.allclose(preds.to_numpy(), preds.iloc[0])
    assert evals_result["train"] and evals_result["valid"]

    labels = pd.Series(
        dataset.prepare("test")._values[:, -1, -1],
        index=dataset.prepare("test").get_index(),
        name="label",
    )
    rank_ic = (
        pd.DataFrame({"pred": preds, "label": labels})
        .groupby(level="datetime")
        .apply(lambda frame: frame["pred"].rank().corr(frame["label"].rank()))
    )
    assert not rank_ic[np.isfinite(rank_ic)].empty


def test_efficient_gats_industry_embedding_updates_weights_changes_predictions_and_unknown_works(tmp_path, capsys):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    provider_mod = _import_gats_industry_provider_module()
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset(include_industry=False)
    rows = [(f"STK{i:03d}", f"8010{i // 2:02d}.SI", "2020-01-01", None) for i in range(1, dataset.stocks)]
    provider = provider_mod.SwIndexMemberIndustryIdProvider(
        min_coverage=0.80,
        conn_factory=_fake_sw_member_conn_factory(rows),
    )
    common_kwargs = dict(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=2,
        lr=0.01,
        metric="loss",
        early_stop=2,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=23,
    )
    off_model = efficient_gats.EfficientGATs(**common_kwargs)
    embedding_model = efficient_gats.EfficientGATs(
        **common_kwargs,
        gats_industry_embedding="on",
        gats_industry_embedding_dim=8,
        gats_industry_id_provider=provider,
    )
    initial_embedding = embedding_model.GAT_model.industry_embedding.weight.detach().clone()

    off_model.fit(dataset, evals_result={}, save_path=str(tmp_path / "off.pt"))
    evals_result = {}
    embedding_model.fit(dataset, evals_result=evals_result, save_path=str(tmp_path / "embedding.pt"))
    off_preds = off_model.predict(dataset)
    embedding_preds = embedding_model.predict(dataset)
    captured = capsys.readouterr()

    assert "reason_code=efficient_gats_industry_id_missing" in captured.out
    assert not torch.allclose(
        initial_embedding,
        embedding_model.GAT_model.industry_embedding.weight.detach(),
    )
    assert embedding_model.GAT_model.industry_embedding.weight.grad is not None
    assert embedding_preds.index.equals(off_preds.index)
    assert not np.allclose(embedding_preds.to_numpy(), off_preds.to_numpy(), atol=1e-6)
    assert embedding_model._preload_segment_to_cpu(dataset.prepare("train"), segment_name="train")[
        "industry_ids"
    ][0].item() == efficient_gats._MISSING_INDUSTRY_ID
    assert provider.coverage_history
    assert evals_result["train"] and evals_result["valid"]

    labels = pd.Series(
        dataset.prepare("test")._values[:, -1, -1],
        index=dataset.prepare("test").get_index(),
        name="label",
    )
    rank_ic = (
        pd.DataFrame({"pred": embedding_preds, "label": labels})
        .groupby(level="datetime")
        .apply(lambda frame: frame["pred"].rank().corr(frame["label"].rank()))
    )
    assert not rank_ic[np.isfinite(rank_ic)].empty


def test_efficient_gats_industry_embedding_resident_and_streaming_side_channel():
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    torch = pytest.importorskip("torch")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset(include_industry=True)
    segment = dataset.prepare("train")
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gpu_resident=True,
        gats_industry_embedding="on",
    )
    resident_cpu = model._preload_segment_to_cpu(segment, segment_name="train")
    resident = {
        "segment_name": resident_cpu["segment_name"],
        "tensor": resident_cpu["tensor"],
        "daily_indices": [torch.as_tensor(index, dtype=torch.long) for index in resident_cpu["daily_indices"]],
        "index": resident_cpu["index"],
        "industry_ids": resident_cpu["industry_ids"],
    }
    streaming = model._preload_streaming_segment_metadata(segment, segment_name="train")

    resident_data, resident_ids = next(model._iter_resident_batches(resident, shuffle=False, include_industry=True))
    streaming_data, streaming_ids = next(model._iter_streaming_batches(streaming, shuffle=False))

    assert torch.allclose(resident_data.cpu(), streaming_data.cpu())
    assert torch.equal(resident_ids.cpu(), streaming_ids.cpu())
    assert resident_ids.shape == (dataset.stocks,)

def test_gats_industry_provider_db_pit_asof_migration_and_no_future_leakage():
    provider_mod = _import_gats_industry_provider_module()

    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2021-01-04"), "SZ000001"),
            (pd.Timestamp("2021-01-05"), "000001.SZ"),
            (pd.Timestamp("2021-01-05"), "000002.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    rows = [
        ("000001.SZ", "801010.SI", "2021-01-01", "2021-01-04"),
        ("000001.SZ", "801020.SI", "2021-01-05", None),
        ("000002.SZ", "801030.SI", "2021-01-06", None),
    ]
    conn_factory = _fake_sw_member_conn_factory(rows)

    provider = provider_mod.SwIndexMemberIndustryIdProvider(
        min_coverage=0.50,
        conn_factory=conn_factory,
    )
    values = provider(index)

    assert values.loc[(pd.Timestamp("2021-01-04"), "SZ000001")] == "801010.SI"
    assert values.loc[(pd.Timestamp("2021-01-05"), "000001.SZ")] == "801020.SI"
    assert pd.isna(values.loc[(pd.Timestamp("2021-01-05"), "000002.SZ")])
    assert provider.last_coverage["covered_rows"] == 2
    assert provider.last_coverage["source"] == "market.sw_index_member"
    executed_sql = "\n".join(conn.cursor_obj.executed[0]["sql"] for conn in conn_factory.conns)
    assert "ROW_NUMBER() OVER" in executed_sql
    assert "in_date <= %s" in executed_sql
    assert "(out_date IS NULL OR out_date >= %s)" in executed_sql


def test_gats_industry_provider_normalises_qlib_and_ts_code_instruments():
    provider_mod = _import_gats_industry_provider_module()

    conn_factory = _fake_sw_member_conn_factory([("000001.SZ", "801010.SI", "2021-01-01", None)])
    target_index = pd.MultiIndex.from_tuples([(pd.Timestamp("2021-01-04"), "SZ000001")], names=["datetime", "instrument"])

    provider = provider_mod.SwIndexMemberIndustryIdProvider(
        min_coverage=1.0,
        conn_factory=conn_factory,
    )
    values = provider(target_index)

    assert values.loc[(pd.Timestamp("2021-01-04"), "SZ000001")] == "801010.SI"
    assert provider.last_coverage["coverage"] == 1.0


def test_gats_industry_provider_lookup_failure_and_coverage_zero_are_loud():
    provider_mod = _import_gats_industry_provider_module()

    @contextmanager
    def failing_factory():
        raise RuntimeError("db offline")
        yield

    index = pd.MultiIndex.from_tuples([(pd.Timestamp("2021-01-04"), "MISSING")], names=["datetime", "instrument"])

    provider = provider_mod.SwIndexMemberIndustryIdProvider(min_coverage=0.90, conn_factory=failing_factory)
    with pytest.raises(provider_mod.GatsIndustryProviderError, match="reason_code=qe_gats_industry_lookup_failed"):
        provider(index)

    provider = provider_mod.SwIndexMemberIndustryIdProvider(
        min_coverage=0.90,
        conn_factory=_fake_sw_member_conn_factory([("STK000", "801010.SI", "2021-01-01", None)]),
    )
    with pytest.raises(provider_mod.GatsIndustryProviderError, match="reason_code=qe_gats_industry_coverage_below_threshold"):
        provider(index)


def test_gats_industry_provider_missing_db_password_fails_loud(monkeypatch):
    provider_mod = _import_gats_industry_provider_module()
    for key in ("TDX_DB_PASSWORD", "POSTGRES_PASSWORD", "PG_PASSWORD"):
        monkeypatch.delenv(key, raising=False)

    def _unexpected_connect(**_kwargs):
        raise AssertionError("psycopg2.connect must not run without a password")

    monkeypatch.setitem(sys.modules, "psycopg2", types.SimpleNamespace(connect=_unexpected_connect))

    with pytest.raises(provider_mod.GatsIndustryProviderError) as exc_info:
        with provider_mod._default_conn_factory():
            pass

    assert str(exc_info.value) == (
        "reason_code=qe_gats_industry_db_password_missing: "
        "set one of TDX_DB_PASSWORD/POSTGRES_PASSWORD/PG_PASSWORD"
    )


def test_gats_industry_provider_off_mode_does_not_resolve_source(tmp_path, monkeypatch):
    provider_mod = _import_gats_industry_provider_module()

    def _boom(*_args, **_kwargs):
        raise AssertionError("DB should not be resolved in off mode")

    monkeypatch.setattr(provider_mod, "_default_conn_factory", _boom)
    result = provider_mod.inject_gats_industry_provider_if_needed(
        {"task": {"model": {"kwargs": {"gats_adjacency_mode": "off"}}}},
        cwd=tmp_path,
        print_fn=lambda *_args, **_kwargs: None,
    )

    assert result is None


def test_gats_industry_provider_injects_for_embedding_on_without_db_connect(tmp_path, monkeypatch):
    provider_mod = _import_gats_industry_provider_module()

    def _boom(*_args, **_kwargs):
        raise AssertionError("inject should not connect before fit/predict")

    monkeypatch.setattr(provider_mod, "_default_conn_factory", _boom)
    config = {"task": {"model": {"kwargs": {"gats_adjacency_mode": "off", "gats_industry_embedding": "on"}}}}

    provider = provider_mod.inject_gats_industry_provider_if_needed(
        config,
        cwd=tmp_path,
        print_fn=lambda *_args, **_kwargs: None,
    )

    assert isinstance(provider, provider_mod.SwIndexMemberIndustryIdProvider)
    assert config["task"]["model"]["kwargs"]["gats_industry_id_provider"] is provider


def test_gats_industry_provider_pickle_does_not_embed_source_rows_or_connection():
    provider_mod = _import_gats_industry_provider_module()
    conn_factory = _fake_sw_member_conn_factory([("STK000", "801010.SI", "2021-01-01", None)])
    provider = provider_mod.SwIndexMemberIndustryIdProvider(min_coverage=1.0, conn_factory=conn_factory)
    index = pd.MultiIndex.from_tuples([(pd.Timestamp("2021-01-04"), "STK000")], names=["datetime", "instrument"])

    assert provider(index).iloc[0] == "801010.SI"
    assert provider._daily_cache
    blob = pickle.dumps(provider)
    restored = pickle.loads(blob)

    assert b"801010.SI" not in blob
    assert restored.conn_factory is None
    assert restored._daily_cache == {}
    assert restored.coverage_history


def test_efficient_gats_industry_bias_runner_injection_fit_predict(tmp_path):
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    pytest.importorskip("torch")
    provider_mod = _import_gats_industry_provider_module()
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset(include_industry=False)
    rows = [(f"STK{i:03d}", f"8010{i // 2:02d}.SI", "2020-01-01", None) for i in range(dataset.stocks)]
    provider = provider_mod.SwIndexMemberIndustryIdProvider(
        min_coverage=0.90,
        conn_factory=_fake_sw_member_conn_factory(rows),
    )

    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gats_adjacency_mode="industry_bias",
        gats_industry_id_provider=provider,
    )

    evals_result = {}
    model.fit(dataset, evals_result=evals_result, save_path=str(tmp_path / "efficient_gats_industry_provider.pt"))
    preds = model.predict(dataset)

    assert not preds.empty
    if model.gats_adjacency_last_event is not None:
        assert model.gats_adjacency_last_event["reason_code"] != "efficient_gats_industry_ids_missing"
    assert provider.coverage_history
    assert min(item["coverage"] for item in provider.coverage_history) > 0.90


def test_efficient_gats_industry_bias_missing_provider_fails_loud():
    pytest.importorskip("qlib.contrib.model.pytorch_gats_ts")
    efficient_gats = _import_efficient_gats_module()

    dataset = _MiniGatsDataset(include_industry=False)
    model = efficient_gats.EfficientGATs(
        d_feat=dataset.d_feat,
        hidden_size=4,
        num_layers=1,
        dropout=0.0,
        n_epochs=1,
        lr=0.01,
        metric="loss",
        early_stop=1,
        base_model="GRU",
        GPU=-1,
        n_jobs=0,
        seed=7,
        gats_adjacency_mode="industry_bias",
    )

    with pytest.raises(ValueError, match="reason_code=efficient_gats_industry_ids_missing"):
        model._preload_segment_to_cpu(dataset.prepare("train"), segment_name="train")

def test_general_ptnn_builtin_transformer_arch_params_route_into_pt_model_kwargs():
    # Regression for BUG-592: a built-in qlib Transformer seed carries arch params
    # d_model/nhead that GeneralPTNN.__init__ does NOT accept. They must be routed
    # through pt_model_kwargs to the nn.Module, never leaked as GeneralPTNN
    # top-level kwargs (which previously raised TypeError -> qrun return code 1).
    yaml_text = _base_yaml(model_info=_builtin_transformer_model_info())

    assert "class: GeneralPTNN" in yaml_text
    assert (
        "pt_model_uri: qlib.contrib.model.pytorch_transformer_ts.Transformer"
        in yaml_text
    )
    # Arch params routed into pt_model_kwargs (dict-literal form, quoted keys).
    assert '"d_model": 64' in yaml_text
    assert '"nhead": 4' in yaml_text
    # And NOT emitted as GeneralPTNN top-level kwargs (12-space indented yaml key).
    assert "\n            d_model:" not in yaml_text
    assert "\n            nhead:" not in yaml_text


def test_builtin_models_registry_has_transformer_tsdataseth():
    # Regression for BUG-593: the built-in qlib Transformer
    # (pytorch_transformer_ts) is a time-series model and MUST resolve to
    # TSDatasetH, exactly like its sibling ALSTM/GRU2 built-ins. It was missing
    # from _BUILTIN_MODELS, so a model_name="Transformer" seed matched no
    # built-in, _get_model_info never set default_dataset_type, and the PTNN
    # branch fell back to flat DatasetH -> built-in Transformer got [N, F]
    # instead of windowed [N, T, F] -> Epoch0 RuntimeError (shape mismatch).
    entry = ConfigComposer._BUILTIN_MODELS.get("__builtin_Transformer__")
    assert entry is not None
    assert entry["model_name"] == "Transformer"
    assert entry["model_type"] == "PTNN"
    assert entry["default_dataset_type"] == "TSDatasetH"
    assert (
        entry["default_hyperparameters"]["pt_model_uri"]
        == "qlib.contrib.model.pytorch_transformer_ts.Transformer"
    )


def test_builtin_transformer_seed_composes_tsdataseth_and_templated_dfeat():
    # BUG-593 behavior: once _get_model_info resolves default_dataset_type from
    # the built-in catalog, the Transformer seed composes onto TSDatasetH
    # (windowed [N, T, F]) and its d_feat is templated to the real feature count
    # rather than the hardcoded seed value, so the nn.Module input dim matches.
    model_info = _builtin_transformer_model_info()
    model_info["default_dataset_type"] = "TSDatasetH"
    yaml_text = _base_yaml(model_info=model_info)

    assert "class: GeneralPTNN" in yaml_text
    assert "class: TSDatasetH" in yaml_text
    # d_feat inside pt_model_kwargs is templated, not the hardcoded seed value 20.
    assert "{{ num_features }}" in yaml_text
    assert '"d_feat": 20' not in yaml_text


def test_general_ptnn_default_mse_path_stays_on_qlib_adapter():
    yaml_text = _base_yaml(model_info=_custom_timeseries_lstm_model_info())

    assert "class: GeneralPTNN" in yaml_text
    assert "module_path: qlib.contrib.model.pytorch_general_nn" in yaml_text
    assert "loss: mse" in yaml_text
    assert "ltr_loss_mode" not in yaml_text
    assert "class: TSDatasetH" in yaml_text


def test_general_ptnn_ltr_custom_params_selects_adapter_and_passes_hp():
    yaml_text = _base_yaml(
        model_info=_custom_timeseries_lstm_model_info(),
        custom_params={
            "ltr_loss_mode": "approx_ndcg_at_k",
            "topk_train_k": "25",
            "ltr_temperature": "0.8",
            "ltr_gate_temperature": "1.2",
            "ltr_relevance_bins": "7",
            "ltr_min_group_size": "25",
        },
    )

    assert "class: AIStockGeneralPTNNLTR" in yaml_text
    assert "module_path: aistock_models.general_ptnn_ltr" in yaml_text
    assert "loss: approx_ndcg_at_k" in yaml_text
    assert "ltr_loss_mode: approx_ndcg_at_k" in yaml_text
    assert "topk_train_k: 25" in yaml_text
    assert "ltr_temperature: 0.8" in yaml_text
    assert "ltr_gate_temperature: 1.2" in yaml_text
    assert "ltr_relevance_bins: 7" in yaml_text
    assert "ltr_min_group_size: 25" in yaml_text
    assert "class: TSDatasetH" in yaml_text
    strategy_section = _slice_yaml_between(yaml_text, "    strategy:", "    model:")
    assert "ltr_loss_mode" not in strategy_section
    assert "topk_train_k" not in strategy_section


def test_general_ptnn_ltr_loss_alias_selects_adapter():
    yaml_text = _base_yaml(
        model_info=_custom_timeseries_lstm_model_info(),
        custom_params={"loss": "approx_ndcg_at_k", "topk_train_k": 25},
    )

    assert "class: AIStockGeneralPTNNLTR" in yaml_text
    assert "module_path: aistock_models.general_ptnn_ltr" in yaml_text
    assert "loss: approx_ndcg_at_k" in yaml_text
    strategy_section = _slice_yaml_between(yaml_text, "    strategy:", "    model:")
    assert "loss: approx_ndcg_at_k" not in strategy_section


def test_general_ptnn_ltr_invalid_config_fails_fast():
    with pytest.raises(ValueError, match="general_ptnn_ltr_invalid_loss_mode"):
        _base_yaml(
            model_info=_custom_timeseries_lstm_model_info(),
            custom_params={"ltr_loss_mode": "listmle"},
        )

    with pytest.raises(ValueError, match="general_ptnn_ltr_invalid_topk_train_k"):
        _base_yaml(
            model_info=_custom_timeseries_lstm_model_info(),
            custom_params={"ltr_loss_mode": "approx_ndcg_at_k", "topk_train_k": 0},
        )

    with pytest.raises(ValueError, match="general_ptnn_ltr_requires_timeseries_dataset"):
        _base_yaml(
            model_info={**_custom_timeseries_lstm_model_info(), "model_type": "Tabular"},
            custom_params={"ltr_loss_mode": "approx_ndcg_at_k"},
        )


def test_general_ptnn_ltr_request_on_non_ptnn_model_fails_loud_not_mse():
    with pytest.raises(ValueError, match="reason_code=ltr_loss_mode_injection_failed"):
        _base_yaml(
            model_info={
                "model_id": "lgbm",
                "model_name": "lgbm",
                "model_type": "LGBM",
            },
            custom_params={"ltr_loss_mode": "approx_ndcg_at_k"},
        )


def test_general_ptnn_ltr_in_memory_payload_includes_adapter_files(monkeypatch):
    composer = ConfigComposer()
    monkeypatch.setattr(composer, "_get_factors_info", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        composer,
        "_get_model_info",
        lambda *_args, **_kwargs: _custom_timeseries_lstm_model_info(),
    )
    monkeypatch.setattr(composer, "_get_strategy_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        composer,
        "_prepare_risk_policy_runtime",
        lambda **_kwargs: (_kwargs["custom_params"], None),
    )
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **_kwargs: (_kwargs["custom_params"], None),
    )
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read_exp_res")

    result = composer.compose_experiment_in_memory(
        factor_names=["Alpha001"],
        model_id="__seed_LSTM_10D_hs64_d02__",
        strategy_id="score_weighted_topk_v2",
        data_split=DATA_SPLIT,
        custom_params={"ltr_loss_mode": "approx_ndcg_at_k", "topk_train_k": 25},
        skip_db_save=True,
        execution_algo="CLOSE_PRICE",
        execution_algo_params={},
    )

    files = result["experiment_files"]
    assert "aistock_models/__init__.py" in files
    assert "aistock_models/general_ptnn_ltr.py" in files
    assert "class AIStockGeneralPTNNLTR" in files["aistock_models/general_ptnn_ltr.py"]
    assert "export PYTHONPATH=" in result["wsl_command"]


def test_efficient_gats_in_memory_payload_includes_adapter_files(monkeypatch):
    composer = ConfigComposer()
    monkeypatch.setattr(composer, "_get_factors_info", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        composer,
        "_get_model_info",
        lambda *_args, **_kwargs: _efficient_gats_model_info(),
    )
    monkeypatch.setattr(composer, "_get_strategy_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        composer,
        "_prepare_risk_policy_runtime",
        lambda **_kwargs: (_kwargs["custom_params"], None),
    )
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **_kwargs: (_kwargs["custom_params"], None),
    )
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read_exp_res")

    result = composer.compose_experiment_in_memory(
        factor_names=["Alpha001"],
        model_id="__seed_EfficientGATs_default_v1__",
        strategy_id="score_weighted_topk_v2",
        data_split=DATA_SPLIT,
        custom_params={},
        skip_db_save=True,
        execution_algo="CLOSE_PRICE",
        execution_algo_params={},
        task_id="qe_task",
        loop_index=1,
        resource_session_id="qers_1",
        resource_source_run_key="qe_task_L1",
        resource_session_token="scoped-secret-token",
        phase_pipeline_enabled=True,
    )

    files = result["experiment_files"]
    conf_yaml = files["conf.yaml"]
    assert "class: EfficientGATs" in conf_yaml
    assert "module_path: aistock_models.efficient_gats" in conf_yaml
    assert "aistock_models/__init__.py" in files
    assert "aistock_models/efficient_gats.py" in files
    assert "class EfficientGATModel" in files["aistock_models/efficient_gats.py"]
    assert "qe_runtime_resource.py" in files
    assert json.loads(files["qe_resource_session_secret.json"])["token"] == "scoped-secret-token"
    assert "scoped-secret-token" not in result["wsl_command"]
    assert "export PYTHONPATH=" in result["wsl_command"]


def test_qe_resource_session_env_is_opt_in_and_complete(monkeypatch):
    composer = ConfigComposer()
    default_env, _ = composer._build_auto_wsl_command_parts("/tmp/qe-default")
    default_text = "\n".join(default_env)
    assert "QE_RESOURCE_SESSION_ID" not in default_text
    assert "QE_PHASE_PIPELINE_ENABLED" not in default_text

    env_lines, core_parts = composer._build_auto_wsl_command_parts(
        "/tmp/qe-phase",
        node_id="wsl2-5080",
        task_id="qe_task",
        loop_index=2,
        resource_session_id="qers_123",
        resource_source_run_key="qe_task_L2",
        resource_session_token="token-value",
        phase_pipeline_enabled=True,
    )
    env_text = "\n".join(env_lines)
    assert "export QE_RESOURCE_SESSION_ID=qers_123" in env_text
    assert "export QE_RESOURCE_SOURCE_RUN_KEY=qe_task_L2" in env_text
    assert "token-value" not in env_text
    assert "export QE_PHASE_PIPELINE_ENABLED=1" in env_text
    assert "chmod 600 qe_resource_session_secret.json" in core_parts

    with pytest.raises(ValueError, match="requires id, source_run_key, and token together"):
        composer._build_auto_wsl_command_parts(
            "/tmp/qe-invalid",
            resource_session_id="qers_incomplete",
        )


def test_seed_ensemble_day_backtest_packages_minute_runner(monkeypatch):
    composer = ConfigComposer()
    monkeypatch.setattr(composer, "_get_factors_info", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(composer, "_get_model_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(composer, "_get_strategy_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        composer,
        "_prepare_risk_policy_runtime",
        lambda **_kwargs: (_kwargs["custom_params"], None),
    )
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **_kwargs: (_kwargs["custom_params"], None),
    )
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read_exp_res")

    result = composer.compose_experiment_in_memory(
        factor_names=["Alpha001"],
        model_id="__seed_LSTM_10D_hs64_d02__",
        strategy_id="score_weighted_topk_v2",
        data_split=DATA_SPLIT,
        custom_params={
            "random_seed": 42,
            "_seed_ensemble_config": {
                "enabled": True,
                "seeds": [42, 2026],
                "level": "score",
                "agg": "mean",
            },
        },
        skip_db_save=True,
        execution_algo="CLOSE_PRICE",
        execution_algo_params={},
    )

    assert result["experiment_files"]["conf.yaml"].find("ensemble:") >= 0
    assert "qrun_limit_minute.py" in result["experiment_files"]
    assert "python qrun_limit_minute.py conf.yaml" in result["wsl_command"]


def test_seed_ensemble_runtime_metadata_stays_out_of_strategy_kwargs():
    yaml_text = _base_yaml(
        strategy_info={
            "strategy_id": "score_weighted_topk_v2",
            "source_code": "class ScoreWeightedTopkStrategyV2:\n    pass\n",
            "portfolio_config": {"class": "ScoreWeightedTopkStrategyV2", "kwargs": {}},
        },
        custom_params={
            "topk": 12,
            "random_seed": 42,
            "_seed_ensemble_config": {
                "enabled": True,
                "seeds": [42, 2026, 12345],
                "level": "score",
                "agg": "mean",
            },
        },
    )

    parsed = yaml.safe_load(yaml_text)
    strategy_kwargs = parsed["port_analysis_config"]["strategy"]["kwargs"]
    assert "_seed_ensemble_config" not in strategy_kwargs
    assert parsed["qe_runtime"]["random_seed"] == 42
    assert parsed["qe_runtime"]["ensemble"] == {
        "enabled": True,
        "level": "score",
        "agg": "mean",
        "seeds": [42, 2026, 12345],
    }


def test_suspend_runtime_flags_reject_nested_conflicts():
    merged = _merge_strategy_runtime_flags({"topk": 10}, True, False)
    assert merged["filter_suspended_on_signal"] is True
    assert merged["suspend_filter_strict"] is False

    seeded = _merge_strategy_runtime_flags({}, False, True, 123)
    assert seeded["random_seed"] == 123

    with pytest.raises(HTTPException, match="top-level request fields"):
        _merge_strategy_runtime_flags({"filter_suspended_on_signal": True}, False, True)
    with pytest.raises(HTTPException, match="strategy_loop"):
        _reject_nested_runtime_flags({"suspend_filter_strict": False}, "strategy_loop[1].strategy_params")


def test_strategy_params_runtime_metadata_is_hoisted_to_runtime_flags():
    cfg = {
        "strategy_params": {"topk": 10, "archive_policy": "SKIP", "random_seed": 2024},
        "runtime_flags": {"archive_policy": "AUTO"},
    }

    _hoist_runtime_metadata_from_strategy_params(cfg)

    assert cfg["strategy_params"] == {"topk": 10}
    assert cfg["runtime_flags"]["archive_policy"] == "AUTO"
    assert cfg["runtime_flags"]["random_seed"] == 2024


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
    assert filt.is_suspended("SZ000001", "2024-01-02") is True
    assert filt.is_suspended("000002.SZ", "2024-01-02") is False
    with pytest.raises(RuntimeError, match="no entry"):
        filt.suspended_symbols("2024-01-03")


def test_score_weighted_suspend_wrapper_guards_orders_and_prices():
    source = (SCRIPTS_DIR / "qe_suspend_filter_score_weighted_strategy.py").read_text(encoding="utf-8")

    assert "def _get_current_price" in source
    assert "def _filter_scores_without_close" in source
    assert "def _filter_untradable_orders" in source
    assert "def _is_orderable_without_warning" in source
    assert "self.trade_exchange.get_close" in source
    assert "self._qe_suspend_filter.is_suspended" in source


def test_qe_event_risk_policy_filters_buys_and_marks_forced_exits(tmp_path):
    artifact = tmp_path / "qe_event_risk_policy.json"
    artifact.write_text(
        json.dumps(
            {
                "enabled": True,
                "contract": "stock_event_risk_policy_v1",
                "providers": ["st_pit"],
                "hard_actions": ["block_buy", "force_exit"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "active_spans": [
                    {
                        "ts_code": "000001.SZ",
                        "eligible_start": "2024-01-01",
                        "eligible_end": "2024-01-31",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    policy = QEEventRiskPolicy(True, str(artifact), strict=True)
    scores = policy.filter_scores(pd.Series([1.0, 2.0], index=["000001.SZ", "600000.SH"]), "2024-01-02")

    assert list(scores.index) == ["000001.SZ"]
    assert policy.force_exit_symbols(["000001.SZ", "600000.SH"], "2024-01-02") == {"600000.SH"}
    with pytest.raises(RuntimeError, match="does not cover trade date"):
        policy.force_exit_symbols(["000001.SZ"], "2024-02-01")


def _write_stock_pool(root: Path, filename: str = "filtered_pool_x.txt") -> tuple[Path, str]:
    root.mkdir(parents=True, exist_ok=True)
    path = root / filename
    path.write_text("000001.SZ\t2018-01-01\t2026-05-02\n", encoding="utf-8")
    import hashlib

    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_remote_stock_pool_sync_uses_local_cache_and_loop_payload_for_localhost(monkeypatch, tmp_path):
    pool_root = tmp_path / "stock_pools"
    path, digest = _write_stock_pool(pool_root)
    monkeypatch.setenv("STOCK_POOL_OUTPUT_DIR", str(pool_root))
    monkeypatch.setenv("AISTOCK_SAFE_ARTIFACT_ROOTS", str(pool_root))

    result = sync_stock_pool_to_remote_node(
        "/home/lc999/data/qlib_bin/instruments/filtered_pool_x.txt",
        {
            "node_id": "wsl2-5080",
            "api_base_url": "http://127.0.0.1:9000",
            "qlib_data_path": "/home/lc999/data/qlib_bin",
        },
    )

    assert result["status"] == "packaged"
    assert result["sync_transport"] == "loop_payload_api"
    assert result["sha256"] == digest
    assert result["local_path"] == str(path)
    assert result["remote_path"] == "/home/lc999/data/qlib_bin/instruments/filtered_pool_x.txt"


def test_remote_stock_pool_sync_resolves_instrument_name_from_local_cache(monkeypatch, tmp_path):
    pool_root = tmp_path / "stock_pools"
    path, digest = _write_stock_pool(pool_root, "filtered_pool_20260426.txt")
    monkeypatch.setenv("STOCK_POOL_OUTPUT_DIR", str(pool_root))
    monkeypatch.setenv("AISTOCK_SAFE_ARTIFACT_ROOTS", str(pool_root))

    result = sync_stock_pool_to_remote_node(
        "filtered_pool_20260426",
        {
            "node_id": "rdagent-node1",
            "api_base_url": "http://192.168.50.215:9000",
            "qlib_data_path": "/home/lc999/data/qlib_bin",
        },
    )

    assert result["filename"] == "filtered_pool_20260426.txt"
    assert result["local_path"] == str(path)
    assert result["sha256"] == digest


def test_remote_stock_pool_prepare_payload_includes_file_and_install_command(monkeypatch, tmp_path):
    pool_root = tmp_path / "stock_pools"
    _path, digest = _write_stock_pool(pool_root)
    monkeypatch.setenv("STOCK_POOL_OUTPUT_DIR", str(pool_root))
    monkeypatch.setenv("AISTOCK_SAFE_ARTIFACT_ROOTS", str(pool_root))

    payload = prepare_stock_pool_loop_payload(
        "filtered_pool_x",
        {
            "node_id": "rdagent-node1",
            "api_base_url": "http://192.168.50.215:9000",
            "workspace_config": {"qlib_data_path": "/home/lc999/data/qlib_bin"},
        },
    )

    assert payload is not None
    assert payload["experiment_files"]["filtered_pool_x.txt"].startswith("000001.SZ")
    assert "mkdir -p /home/lc999/data/qlib_bin/instruments" in payload["install_command"]
    assert "filtered_pool_x.txt" in payload["install_command"]
    assert digest in payload["install_command"]


def test_remote_stock_pool_install_command_is_injected_after_cd():
    command = inject_stock_pool_install_command(
        "cd /home/node/qe_workspace/task/Loop1 && conda activate env && python qrun_limit_minute.py conf.yaml",
        "test -f filtered_pool_x.txt",
    )

    assert command.startswith("cd /home/node/qe_workspace/task/Loop1 && test -f filtered_pool_x.txt &&")
    assert "conda activate env && python qrun_limit_minute.py conf.yaml" in command


def test_remote_stock_pool_sync_fails_fast_when_local_cache_missing(monkeypatch, tmp_path):
    pool_root = tmp_path / "stock_pools"
    monkeypatch.setenv("STOCK_POOL_OUTPUT_DIR", str(pool_root))
    monkeypatch.setenv("AISTOCK_SAFE_ARTIFACT_ROOTS", str(pool_root))

    with pytest.raises(RuntimeError, match="local filtered stock_pool cache file is missing"):
        sync_stock_pool_to_remote_node(
            "filtered_pool_missing",
            {
                "node_id": "rdagent-node1",
                "api_base_url": "http://192.168.50.215:9000",
                "qlib_data_path": "/home/lc999/data/qlib_bin",
            },
        )


def test_qrun_seed_prediction_aggregation_is_deterministic(monkeypatch):
    runner = _load_qrun_minute_module(monkeypatch)
    idx = pd.MultiIndex.from_product(
        [pd.to_datetime(["2024-01-02", "2024-01-03"]), ["000001.SZ", "000002.SZ"]],
        names=["datetime", "instrument"],
    )
    seed_a = pd.Series([0.1, 0.4, 0.8, 0.2], index=idx)
    seed_b = pd.Series([0.3, 0.2, 0.4, 0.6], index=idx)

    mean_df = runner._aggregate_seed_predictions([(42, seed_a), (2026, seed_b)], "mean")
    rank_df = runner._aggregate_seed_predictions([(42, seed_a), (2026, seed_b)], "rank_mean")

    assert mean_df["score"].tolist() == pytest.approx([0.2, 0.3, 0.6, 0.4])
    assert rank_df.loc[(pd.Timestamp("2024-01-02"), "000002.SZ"), "score"] == pytest.approx(0.75)
    assert rank_df.loc[(pd.Timestamp("2024-01-03"), "000001.SZ"), "score"] == pytest.approx(0.75)


def test_qrun_seed_ensemble_config_rejects_duplicate_seeds(monkeypatch):
    runner = _load_qrun_minute_module(monkeypatch)

    with pytest.raises(ValueError, match="duplicate seeds"):
        runner._get_seed_ensemble_config(
            {
                "qe_runtime": {
                    "ensemble": {
                        "enabled": True,
                        "seeds": [42, 42],
                        "level": "score",
                        "agg": "mean",
                    }
                }
            }
        )


def test_qrun_optional_recorder_artifact_load_fails_fast_on_corruption(monkeypatch):
    runner = _load_qrun_minute_module(monkeypatch)

    class BrokenRecorder:
        def load_object(self, name):
            raise pickle.UnpicklingError(f"corrupt optional artifact: {name}")

    with pytest.raises(RuntimeError, match="optional recorder artifact load failed"):
        runner._load_recorder_object(
            BrokenRecorder(),
            "portfolio_analysis/indicators_normal_1day.pkl",
            seed=42,
            required=False,
        )


def test_qrun_portfolio_seed_aggregation_matches_offline_equal_weight_proxy(monkeypatch):
    runner = _load_qrun_minute_module(monkeypatch)
    dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
    seed_1_positions = {
        dates[0]: {
            "cash": 400.0,
            "now_account_value": 1000.0,
            "000001.SZ": {"amount": 60.0, "price": 10.0},
        },
        dates[1]: {
            "cash": 450.0,
            "now_account_value": 1100.0,
            "000001.SZ": {"amount": 65.0, "price": 10.0},
        },
    }
    seed_2_positions = {
        dates[0]: {
            "cash": 700.0,
            "now_account_value": 1000.0,
            "000002.SZ": {"amount": 15.0, "price": 20.0},
        },
        dates[1]: {
            "cash": 610.0,
            "now_account_value": 1000.0,
            "000002.SZ": {"amount": 19.5, "price": 20.0},
        },
    }
    report_1 = pd.DataFrame({"account": [1000.0, 1100.0], "cash": [400.0, 450.0], "value": [600.0, 650.0], "bench": [0, 0]}, index=dates)
    report_2 = pd.DataFrame({"account": [1000.0, 1000.0], "cash": [700.0, 610.0], "value": [300.0, 390.0], "bench": [0, 0]}, index=dates)

    merged_positions = runner._aggregate_seed_positions(
        [(1, seed_1_positions), (2, seed_2_positions)],
        [(1, report_1), (2, report_2)],
    )
    merged_report = runner._aggregate_seed_reports([(1, report_1), (2, report_2)], merged_positions)

    first = merged_positions[dates[0]]
    second = merged_positions[dates[1]]
    assert first["now_account_value"] == pytest.approx(1000.0)
    assert first["cash"] == pytest.approx(550.0)
    assert first["000001.SZ"]["weight"] == pytest.approx(0.30)
    assert first["000002.SZ"]["weight"] == pytest.approx(0.15)
    assert second["now_account_value"] == pytest.approx(1050.0)
    assert second["cash"] == pytest.approx(535.022727)
    assert second["000001.SZ"]["weight"] == pytest.approx(0.2954545)
    assert second["000002.SZ"]["weight"] == pytest.approx(0.195)
    assert merged_report.loc[dates[1], "account"] == pytest.approx(1050.0)
    assert merged_report.loc[dates[1], "return"] == pytest.approx(0.05)
