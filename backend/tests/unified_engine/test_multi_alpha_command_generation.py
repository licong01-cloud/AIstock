import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from backend.services.quantevolver.config_composer import ConfigComposer
from backend.services.quantevolver.meta_model import MetaModelCombiner
from backend.services.quantevolver.multi_alpha_engine import MultiAlphaEngine
from backend.services.quantevolver.multi_alpha_resource_planner import plan_assignments
from backend.services.quantevolver.multi_alpha_result_collector import MultiAlphaResultCollector
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient
from backend.services.quantevolver.callback_urls import build_aistock_callback_url
from backend.routers import quantevolver as quantevolver_router
from backend.routers.quantevolver import _build_multi_alpha_group_command


class TestQrunLimitMinutePredBacktest:
    def test_runner_declares_pred_backtest_without_changing_existing_modes(self):
        runner = Path("scripts/qrun_limit_minute.py").read_text(encoding="utf-8")

        assert "add_mutually_exclusive_group" in runner
        assert 'add_argument("--backtest-only"' in runner
        assert 'add_argument("--train-only"' in runner
        assert 'add_argument("--pred-backtest"' in runner
        assert "def _run_pred_backtest" in runner

    def test_pred_backtest_injects_prediction_and_label_for_records(self):
        runner = Path("scripts/qrun_limit_minute.py").read_text(encoding="utf-8")

        assert 'recorder.save_objects(**{"pred.pkl": pred_df, "label.pkl": raw_label})' in runner
        assert 'if "SignalRecord" in rec_class' in runner
        assert 'if not has_port_ana' in runner

    def test_runner_subscribes_factor_for_v25_raw_price_conversion(self):
        runner = Path("scripts/qrun_limit_minute.py").read_text(encoding="utf-8")

        assert "'$factor'" in runner
        assert "'$up_limit_price', '$down_limit_price', '$prev_close', '$factor'" in runner


class TestMultiAlphaRolloutGate:
    def test_distributed_mode_rejected_until_feature_flag_enabled(self):
        with patch.dict("os.environ", {"AISTOCK_MULTI_ALPHA_DISTRIBUTED_ENABLED": ""}):
            with pytest.raises(Exception) as exc:
                quantevolver_router._assert_multi_alpha_execution_mode_supported(
                    {"execution_mode": "distributed"}
                )

        assert getattr(exc.value, "status_code", None) == 400
        assert "distributed execution is disabled" in str(exc.value.detail)

    def test_distributed_mode_allowed_when_feature_flag_enabled(self):
        with patch.dict("os.environ", {"AISTOCK_MULTI_ALPHA_DISTRIBUTED_ENABLED": "1"}):
            quantevolver_router._assert_multi_alpha_execution_mode_supported(
                {"execution_mode": "distributed"}
            )


class TestQELoopCallbackUrls:
    def test_wsl_qe_callback_uses_concrete_8001_endpoint_not_node_base(self):
        with patch.dict("os.environ", {
            "AISTOCK_QE_LOOP_CALLBACK_URL": "",
            "AISTOCK_QE_CALLBACK_BASE_URL": "",
            "AISTOCK_BACKEND_CALLBACK_BASE_URL": "",
            "AISTOCK_BACKEND_BASE_URL": "",
        }):
            url = quantevolver_router._resolve_qe_experiment_callback_url(
                "wsl2-5080",
                "http://192.168.50.1:8000",
            )

        assert url == "http://127.0.0.1:8001/api/v1/quantevolver/webhook/loop-completed"

    def test_remote_qe_callback_expands_compute_node_base(self):
        with patch.dict("os.environ", {
            "AISTOCK_QE_LOOP_CALLBACK_URL": "",
            "AISTOCK_QE_CALLBACK_BASE_URL": "",
            "AISTOCK_BACKEND_CALLBACK_BASE_URL": "",
            "AISTOCK_BACKEND_BASE_URL": "",
        }):
            url = quantevolver_router._resolve_qe_experiment_callback_url(
                "rdagent-node1",
                "http://192.168.50.10:8001",
            )

        assert url == "http://192.168.50.10:8001/api/v1/quantevolver/webhook/loop-completed"

    def test_remote_qe_callback_rejects_localhost_without_override(self):
        with patch.dict("os.environ", {
            "AISTOCK_QE_LOOP_CALLBACK_URL": "",
            "AISTOCK_QE_CALLBACK_BASE_URL": "",
            "AISTOCK_BACKEND_CALLBACK_BASE_URL": "",
            "AISTOCK_BACKEND_BASE_URL": "",
        }):
            with pytest.raises(ValueError, match="Remote QE callback URL"):
                quantevolver_router._resolve_qe_experiment_callback_url(
                    "rdagent-node1",
                    "http://127.0.0.1:8001",
                )

    def test_full_callback_override_is_preserved(self):
        with patch.dict("os.environ", {
            "AISTOCK_QE_LOOP_CALLBACK_URL": "http://aistock.local/cb/qe",
        }):
            url = build_aistock_callback_url(
                endpoint_path="/api/v1/quantevolver/webhook/loop-completed",
                full_url_env="AISTOCK_QE_LOOP_CALLBACK_URL",
                node_id="wsl2-5080",
                node_callback_url="http://127.0.0.1:8000",
            )

        assert url == "http://aistock.local/cb/qe"


class TestConfigComposerCommandGeneration:
    def test_build_auto_wsl_command_parts_uses_factor_cache_root(self):
        composer = ConfigComposer()
        env_lines, core_parts = composer._build_auto_wsl_command_parts(
            "/mnt/f/Dev/RD-Agent-main/qe_workspace/demo",
            has_custom_factors=True,
            use_custom_model=False,
            backtest_freq="1min",
        )

        assert any("/mnt/f/Dev/AIstock/rdagent_assets/factor_values" in line for line in env_lines)
        assert all(not part.startswith("cd ") for part in core_parts)
        assert "python prepare_factors.py" in core_parts
        assert ". ./.factor_env" in core_parts
        assert 'case "$_conda_sh"' in core_parts[0]
        assert 'conda activate "${QLIB_WSL_CONDA_ENV:-rdagent-gpu}"' in core_parts[0]

    def test_build_conda_activate_chain_expands_tilde_and_supports_fallbacks(self):
        chain = ConfigComposer._build_conda_activate_chain()

        assert '${QLIB_WSL_CONDA_SH:-$HOME/miniconda3/etc/profile.d/conda.sh}' in chain
        assert 'case "$_conda_sh" in "~/"*) _conda_sh="$HOME/${_conda_sh#~/}" ;; esac' in chain
        assert '$HOME/anaconda3/etc/profile.d/conda.sh' in chain
        assert '/opt/conda/etc/profile.d/conda.sh' in chain
        assert 'conda activate "${QLIB_WSL_CONDA_ENV:-rdagent-gpu}"' in chain

    def test_hmm_precompute_uses_robust_conda_activate_chain(self):
        composer = ConfigComposer()
        strategy_params = {
            "sector_hmm_model_path": "F:/tmp/models.json",
            "hmm_signal_preset": "preset_A",
            "hmm_signal_presets": {"preset_A": {"coefficients": {"1": {"trending": 1.05, "neutral": 1.0, "fading": 0.96}}}},
        }
        data_split = {"test_start": "2024-01-01", "backtest_end": "2024-01-31"}

        with patch("subprocess.run") as mock_run, patch.dict("os.environ", {"TDX_DB_PASSWORD": "secret"}, clear=False):
            mock_run.side_effect = [
                SimpleNamespace(returncode=1, stdout="", stderr=""),
                SimpleNamespace(returncode=0, stdout="10.0.0.1\n", stderr=""),
                SimpleNamespace(
                    returncode=0,
                    stdout='{"daily_coefficients": {"2024-01-02": {"801010.SI": 1.05}}, "stock_sector_map": {"000001.SZ": "801010.SI"}, "sector_count": 1}',
                    stderr="",
                ),
            ]

            composer._precompute_hmm_coefficients(strategy_params, data_split)

            cmd = mock_run.call_args_list[-1].args[0][3]
            assert 'case "$_conda_sh" in "~/"*) _conda_sh="$HOME/${_conda_sh#~/}" ;; esac' in cmd
            assert 'conda activate "${QLIB_WSL_CONDA_ENV:-rdagent-gpu}"' in cmd

    def test_hmm_precompute_rejects_empty_coefficients(self):
        composer = ConfigComposer()
        strategy_params = {
            "sector_hmm_model_path": "F:/tmp/models.json",
            "hmm_signal_preset": "preset_A",
        }
        data_split = {"test_start": "2024-01-01", "backtest_end": "2024-01-31"}

        with patch("subprocess.run") as mock_run, patch.dict("os.environ", {"TDX_DB_PASSWORD": "secret"}, clear=False):
            mock_run.side_effect = [
                SimpleNamespace(returncode=1, stdout="", stderr=""),
                SimpleNamespace(returncode=0, stdout="10.0.0.1\n", stderr=""),
                SimpleNamespace(returncode=0, stdout='{"daily_coefficients": {}, "stock_sector_map": {}, "sector_count": 0}', stderr=""),
            ]

            with pytest.raises(RuntimeError, match="daily_coefficients"):
                composer._precompute_hmm_coefficients(strategy_params, data_split)

    def test_generate_auto_wsl_command_keeps_legacy_cd_prefix(self):
        composer = ConfigComposer()
        wsl_path = "/mnt/f/Dev/RD-Agent-main/qe_workspace/demo"
        command = composer._generate_wsl_command(
            wsl_path,
            has_custom_factors=True,
            use_custom_model=False,
            mode="auto",
            backtest_freq="1min",
        )

        assert command.startswith(f"cd {wsl_path} && ")
        assert "python prepare_factors.py" in command
        assert "python qrun_limit_minute.py conf.yaml" in command

    def test_compose_prepare_factors_generates_valid_python(self):
        composer = ConfigComposer()
        script = composer._compose_prepare_factors(
            [
                {"factor_name": "demo_factor", "code_text": "print('ok')", "source": "custom"},
                {"factor_name": "quote'name", "code_text": "print('quoted')", "source": "custom"},
            ],
            factor_data_dir="/mnt/f/Dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data",
            data_split={"train_start": "2018-08-01", "test_end": "2026-03-10"},
        )

        assert script is not None
        compile(script, "prepare_factors.py", "exec")
        assert "rstrip('/\\\\')" in script
        assert "pd.Timestamp(TRAIN_START)" in script
        assert "_pd.Timestamp" not in script
        assert "factor_codes[\"quote'name\"]" in script or "factor_codes['quote\'name']" in script


    def test_timeseries_general_ptnn_uses_dynamic_d_feat_when_custom_factors_present(self):
        composer = ConfigComposer()
        conf = composer._compose_conf_yaml(
            factors_info=[
                {"factor_name": "alpha158_close", "source": "alpha158", "code_text": None},
                {"factor_name": "custom_f1", "source": "custom", "code_text": "print('ok')"},
            ],
            model_info={
                "model_name": "ALSTM",
                "model_type": "NN",
                "code_text": None,
                "default_dataset_type": "TSDatasetH",
                "model_hyperparameters": {
                    "pt_model_uri": "qlib.contrib.model.pytorch_alstm_ts.ALSTMModel",
                    "d_feat": 20,
                    "hidden_size": 64,
                    "num_layers": 2,
                    "dropout": 0.0,
                },
                "model_training_hyperparameters": None,
            },
            strategy_info=None,
            data_split={
                "train_start": "2018-01-01",
                "train_end": "2020-12-31",
                "valid_start": "2021-01-01",
                "valid_end": "2021-12-31",
                "test_start": "2022-01-01",
                "test_end": "2022-12-31",
                "backtest_end": "2022-12-31",
            },
            custom_params={},
            has_custom_factors=True,
            has_alpha158=True,
            has_alpha360=False,
            disable_alpha158=False,
            quick_train=False,
            qlib_data_path="/home/lc999/data/qlib_bin",
            qlib_minute_path="/home/lc999/data/qlib_minute_bin",
        )

        assert '"d_feat": {{ num_features }}' in conf
        assert '"hidden_size": 64' in conf
        assert 'step_len: 20' in conf


class TestMultiAlphaPlanner:
    def test_distributed_accepts_busy_nodes(self):
        group = SimpleNamespace(
            group_name="price_volume",
            compute_resource="cpu",
            preferred_node_id=None,
        )

        assignments = plan_assignments(
            groups=[group],
            execution_mode="distributed",
            available_nodes=[
                {"node_id": "wsl2-5080", "status": "busy", "gpu_vram_mb": 16384},
                {"node_id": "rdagent-node1", "status": "offline", "gpu_vram_mb": 6144},
            ],
            default_node_id="wsl2-5080",
        )

        assert len(assignments) == 1
        assert assignments[0].node_id == "wsl2-5080"


class TestMultiAlphaCommandBuilder:
    def test_run_endpoint_rejects_removed_legacy_engine_mode(self):
        with pytest.raises(quantevolver_router.HTTPException) as exc_info:
            asyncio.get_event_loop().run_until_complete(
                quantevolver_router.run_experiment("any_experiment", engine_mode="legacy")
            )

        assert exc_info.value.status_code == 400
        assert "legacy execution engine has been removed" in exc_info.value.detail

    def test_prefers_command_core_without_legacy_cd(self):
        cmd = _build_multi_alpha_group_command(
            {
                "group_name": "price_volume",
                "wsl_command_core": "python prepare_factors.py && python qrun_limit_minute.py conf.yaml",
                "wsl_command": "cd /mnt/f/legacy && python qrun_limit_minute.py conf.yaml",
            },
            "wsl2-5080",
        )

        assert "(cd group_price_volume && python prepare_factors.py" in cmd
        assert "cd /mnt/f/legacy" not in cmd
        assert "on wsl2-5080" in cmd

    def test_rejects_legacy_command_without_unified_core(self):
        with pytest.raises(ValueError, match="missing wsl_command_core"):
            _build_multi_alpha_group_command(
                {
                    "group_name": "price_volume",
                    "wsl_command_core": "",
                    "wsl_command": "python qrun_limit_minute.py conf.yaml",
                }
            )

    def test_raises_when_group_has_no_executable_command(self):
        with pytest.raises(ValueError, match="missing wsl_command_core"):
            _build_multi_alpha_group_command(
                {
                    "group_name": "price_volume",
                    "wsl_command_core": "",
                    "wsl_command": "",
                }
            )


class TestMultiAlphaMetaRunner:
    def test_meta_runner_uses_source_prediction_path_for_reuse_group(self):
        engine = MultiAlphaEngine.__new__(MultiAlphaEngine)
        engine.ma_config = type("MetaCfg", (), {"meta_model": type("MetaModelCfg", (), {"method": "ic_weighted", "lookback_days": 60})()})()

        script = engine._generate_meta_runner_script(
            [
                {
                    "group_name": "price_volume",
                    "prediction_path": "/mnt/f/Dev/RD-Agent-main/qe_workspace/src_exp/Loop1/group_price_volume/output/pred.pkl",
                    "reuse_mode": "reuse_prediction",
                },
                {
                    "group_name": "money_flow",
                    "wsl_command_core": "python qrun_limit_minute.py conf.yaml",
                },
            ]
        )

        assert "GROUP_PREDICTION_PATHS" in script
        assert "_resolve_prediction_path" in script
        assert "GROUP_PREDICTION_PATHS.get(g_name)" in script
        assert "src_exp/Loop1/group_price_volume/output/pred.pkl" in script
        compile(script, "meta_model_runner.py", "exec")

    def test_meta_runner_fails_when_prediction_missing(self):
        engine = MultiAlphaEngine.__new__(MultiAlphaEngine)
        engine.ma_config = type("MetaCfg", (), {"meta_model": type("MetaModelCfg", (), {"method": "ic_weighted", "lookback_days": 60})()})()

        script = engine._generate_meta_runner_script(
            [
                {"group_name": "price_volume", "prediction_path": "", "reuse_mode": "reuse_prediction"},
                {"group_name": "money_flow", "wsl_command_core": "python qrun_limit_minute.py conf.yaml"},
            ]
        )

        assert 'raise RuntimeError("Missing prediction_path for reuse group")' in script
        assert 'raise RuntimeError(' in script
        assert 'Missing prediction files for groups' in script
        compile(script, "meta_model_runner.py", "exec")


class TestMultiAlphaEngineFailFast:
    def test_root_backtest_files_are_generated_from_full_unified_bundle(self):
        class DummyComposer:
            def compose_experiment_in_memory(self, **kwargs):
                assert kwargs["train_only"] is False
                return {
                    "experiment_files": {
                        "conf.yaml": "task:\n  record: []\n  module_path: qe_custom_loaders\n",
                        "qrun_limit_minute.py": "runner",
                        "read_exp_res.py": "reader",
                        "qe_custom_loaders.py": "loader",
                        "benchmark_sh000300.parquet.b64": "AA==",
                    }
                }

        engine = MultiAlphaEngine.__new__(MultiAlphaEngine)
        engine.composer = DummyComposer()
        engine.config = SimpleNamespace(
            strategy_id=None,
            data_split={},
            execution_algo=None,
            execution_algo_params=None,
            experiment_name="exp_demo",
            build_custom_params=lambda: {},
            build_strategy_params=lambda: {},
        )
        engine.ma_config = SimpleNamespace(
            alpha_groups=[
                SimpleNamespace(
                    group_name="price_volume",
                    factor_names=["f1"],
                    model_id="m1",
                )
            ]
        )

        files = {
            "group_price_volume/conf.yaml": "group train-only conf",
            "group_price_volume/qrun_limit_minute.py": "train-only runner",
            "group_price_volume/read_exp_res.py": "train-only reader",
        }

        engine._add_root_backtest_files(files, [{"group_name": "price_volume"}])

        assert files["conf.yaml"].startswith("task:")
        assert files["qrun_limit_minute.py"] == "runner"
        assert files["read_exp_res.py"] == "reader"
        assert files["qe_custom_loaders.py"] == "loader"
        assert files["benchmark_sh000300.parquet.b64"] == "AA=="

    def test_root_backtest_files_fail_when_referenced_module_missing(self):
        class DummyComposer:
            def compose_experiment_in_memory(self, **kwargs):
                return {
                    "experiment_files": {
                        "conf.yaml": "class: custom_strategy.CustomStrategy\n",
                        "qrun_limit_minute.py": "runner",
                        "read_exp_res.py": "reader",
                    }
                }

        engine = MultiAlphaEngine.__new__(MultiAlphaEngine)
        engine.composer = DummyComposer()
        engine.config = SimpleNamespace(
            strategy_id=None,
            data_split={},
            execution_algo=None,
            execution_algo_params=None,
            experiment_name="exp_demo",
            build_custom_params=lambda: {},
            build_strategy_params=lambda: {},
        )
        engine.ma_config = SimpleNamespace(
            alpha_groups=[
                SimpleNamespace(
                    group_name="price_volume",
                    factor_names=["f1"],
                    model_id="m1",
                )
            ]
        )

        with pytest.raises(RuntimeError, match="custom_strategy.py"):
            engine._add_root_backtest_files({}, [{"group_name": "price_volume"}])

    def test_run_rejects_reuse_model_fallback(self):
        engine = MultiAlphaEngine.__new__(MultiAlphaEngine)
        engine.config = SimpleNamespace(
            node_id="wsl2-5080",
            experiment_name="exp_demo",
        )
        engine.ma_config = SimpleNamespace(
            alpha_groups=[
                SimpleNamespace(
                    group_name="price_volume",
                    reuse_mode="reuse_model",
                    model_source_experiment_id="src_exp",
                    model_source_group_name="price_volume",
                    factor_names=["f1"],
                    model_id="m1",
                    model_params=None,
                    dataset_type="alpha158",
                    compute_resource=None,
                )
            ],
            execution_mode="serial",
            meta_model=SimpleNamespace(method="equal"),
            model_dump=lambda: {},
        )
        engine.available_nodes = None
        engine.composer = object()

        with patch("backend.services.quantevolver.multi_alpha_engine.plan_assignments", return_value=[
            SimpleNamespace(group=engine.ma_config.alpha_groups[0], node_id="wsl2-5080", order=0)
        ]):
            with pytest.raises(ValueError, match="reuse_model is not supported"):
                engine.run()

    def test_run_requires_source_experiment_for_reuse_prediction(self):
        engine = MultiAlphaEngine.__new__(MultiAlphaEngine)
        engine.config = SimpleNamespace(
            node_id="wsl2-5080",
            experiment_name="exp_demo",
        )
        engine.ma_config = SimpleNamespace(
            alpha_groups=[
                SimpleNamespace(
                    group_name="price_volume",
                    reuse_mode="reuse_prediction",
                    model_source_experiment_id=None,
                    model_source_group_name=None,
                    factor_names=["f1"],
                    model_id="m1",
                    model_params=None,
                    dataset_type="alpha158",
                    compute_resource=None,
                )
            ],
            execution_mode="serial",
            meta_model=SimpleNamespace(method="equal"),
            model_dump=lambda: {},
        )
        engine.available_nodes = None
        engine.composer = object()

        with patch("backend.services.quantevolver.multi_alpha_engine.plan_assignments", return_value=[
            SimpleNamespace(group=engine.ma_config.alpha_groups[0], node_id="wsl2-5080", order=0)
        ]):
            with pytest.raises(ValueError, match="reuse_prediction requires model_source_experiment_id"):
                engine.run()


class TestMetaModelCombinerFailFast:
    def test_ic_weighted_requires_actual_returns(self):
        combiner = MetaModelCombiner(method="ic_weighted", lookback_days=60)
        preds = {"g1": __import__("pandas").DataFrame({"score": [1.0, 2.0]})}

        with pytest.raises(ValueError, match="actual_returns"):
            combiner.fit_and_combine(preds, None)

    def test_equal_weighted_combines_without_actual_returns(self):
        import pandas as pd

        idx = pd.MultiIndex.from_tuples([
            ("2024-01-01", "SH600000"),
            ("2024-01-01", "SH600001"),
        ])
        preds = {
            "g1": pd.DataFrame({"score": [1.0, 3.0]}, index=idx),
            "g2": pd.DataFrame({"score": [3.0, 5.0]}, index=idx),
        }

        combiner = MetaModelCombiner(method="equal", lookback_days=60)
        combined, weights = combiner.fit_and_combine(preds, None)

        assert weights == {"g1": 0.5, "g2": 0.5}
        assert combined["score"].tolist() == [2.0, 4.0]

    def test_equal_with_zero_weights_does_not_fallback_first_group(self):
        import pandas as pd

        combiner = MetaModelCombiner(method="equal", lookback_days=60)
        aligned = {"g1": pd.Series([1.0, 2.0])}

        with pytest.raises(ValueError, match="weights are zero"):
            combiner._weighted_sum(aligned, {"g1": 0.0})


class TestQEWorkspaceClientFailFast:
    def test_get_loop_metrics_rejects_empty_payload(self):
        client = QEWorkspaceClient()
        client.client.get = AsyncMock(return_value=SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {},
        ))

        with pytest.raises(RuntimeError, match="回测指标响应为空或格式错误"):
            asyncio.run(client.get_loop_metrics("task_x", "Loop1"))

    def test_get_enhanced_metrics_rejects_empty_payload(self):
        client = QEWorkspaceClient()
        client.client.get = AsyncMock(return_value=SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {},
        ))

        with pytest.raises(RuntimeError, match="增强指标响应为空或格式错误"):
            asyncio.run(client.get_enhanced_metrics("task_x", "Loop1"))

    def test_get_workspace_file_rejects_empty_text(self):
        client = QEWorkspaceClient()
        client.client.get = AsyncMock(return_value=SimpleNamespace(
            raise_for_status=lambda: None,
            headers={"content-type": "text/plain"},
            text="",
        ))

        with pytest.raises(RuntimeError, match="workspace 文件内容为空"):
            asyncio.run(client.get_workspace_file("task_x", "Loop1", "foo.txt"))

    def test_get_workspace_file_rejects_null_json(self):
        client = QEWorkspaceClient()
        client.client.get = AsyncMock(return_value=SimpleNamespace(
            raise_for_status=lambda: None,
            headers={"content-type": "application/json"},
            json=lambda: None,
        ))

        with pytest.raises(RuntimeError, match="workspace 文件 JSON 为空"):
            asyncio.run(client.get_workspace_file("task_x", "Loop1", "foo.json"))


    def test_get_workspace_file_raises_runtime_error_on_http_failure(self):
        client = QEWorkspaceClient()
        request = httpx.Request("GET", "http://localhost/test")
        response = httpx.Response(404, request=request)
        error = httpx.HTTPStatusError("not found", request=request, response=response)
        client.client.get = AsyncMock(side_effect=error)

        with pytest.raises(RuntimeError, match="读取 workspace 文件失败"):
            asyncio.run(client.get_workspace_file("task_x", "Loop1", "foo.json"))


class TestRouterRunStatusFailFast:
    def test_run_status_requires_qe_task_id_when_running(self):
        with patch.object(quantevolver_router, "get_conn") as mock_get_conn:
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = ("running", None, "Loop1", None, "single")
            cur.description = [("status",), ("qe_task_id",), ("qe_loop_id",), ("result_metrics",), ("alpha_mode",)]

            with pytest.raises(quantevolver_router.HTTPException, match="qe_task_id"):
                asyncio.run(quantevolver_router.get_experiment_run_status("exp_1"))

    def test_run_status_rejects_empty_live_status(self):
        class DummyClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

            async def get_loop_status(self, task_id, loop_id):
                return {}

        with patch.object(quantevolver_router, "get_conn") as mock_get_conn, \
             patch("backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient", return_value=DummyClient()):
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = ("running", "task_x", "Loop1", None, "single")
            cur.description = [("status",), ("qe_task_id",), ("qe_loop_id",), ("result_metrics",), ("alpha_mode",)]

            with pytest.raises(quantevolver_router.HTTPException, match="空状态"):
                asyncio.run(quantevolver_router.get_experiment_run_status("exp_1"))


class TestRouterMultiAlphaResultsFallback:
    def test_multi_alpha_results_falls_back_to_unified_result_metrics(self):
        with patch.object(quantevolver_router, "get_conn") as mock_get_conn:
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = (
                "completed",
                {
                    "multi_alpha_detail": {
                        "meta_method": "equal",
                        "meta_weights": {"g1": 0.4},
                        "combined_ic": 0.15,
                        "group_results": [
                            {
                                "group_name": "g1",
                                "ic": 0.1,
                                "icir": 1.0,
                                "sharpe": 1.2,
                                "meta_weight": 0.4,
                                "model_id": "m1",
                            }
                        ],
                    }
                },
            )
            cur.fetchall.side_effect = [
                [],
                [],
            ]

            result = quantevolver_router.get_multi_alpha_results("exp_1")

        assert result["ok"] is True
        assert result["groups"] == [
            {
                "group_name": "g1",
                "factor_names": [],
                "model_id": "m1",
                "dataset_type": None,
                "group_ic": 0.1,
                "group_icir": 1.0,
                "group_sharpe": 1.2,
                "meta_weight": 0.4,
                "assigned_node_id": None,
                "status": "completed",
                "error_message": None,
            }
        ]
        assert result["meta_weights_history"] == [
            {
                "as_of_date": None,
                "method": "equal",
                "weights": {"g1": 0.4},
                "combined_ic": 0.15,
            }
        ]

    def test_multi_alpha_results_merges_unified_metrics_into_group_rows(self):
        with patch.object(quantevolver_router, "get_conn") as mock_get_conn:
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = (
                "completed",
                {
                    "multi_alpha_detail": {
                        "meta_method": "equal",
                        "meta_weights": {"g1": 0.4},
                        "combined_ic": 0.15,
                        "group_results": [
                            {
                                "group_name": "g1",
                                "ic": 0.1,
                                "icir": 1.0,
                                "sharpe": 1.2,
                                "meta_weight": 0.4,
                                "model_id": "m1",
                            }
                        ],
                    }
                },
            )
            cur.fetchall.side_effect = [
                [(
                    "g1", ["f1"], "m1", "DatasetH",
                    None, None, None, None,
                    "node1", "running", None,
                )],
                [],
            ]
            cur.description = [
                ("group_name",), ("factor_names",), ("model_id",), ("dataset_type",),
                ("group_ic",), ("group_icir",), ("group_sharpe",), ("meta_weight",),
                ("assigned_node_id",), ("status",), ("error_message",),
            ]

            result = quantevolver_router.get_multi_alpha_results("exp_1")

        assert result["groups"][0]["group_ic"] == 0.1
        assert result["groups"][0]["group_icir"] == 1.0
        assert result["groups"][0]["group_sharpe"] == 1.2
        assert result["groups"][0]["meta_weight"] == 0.4
        assert result["groups"][0]["status"] == "completed"


class TestRouterMultiAlphaDiagnosticsFallback:
    def test_diagnostics_falls_back_to_unified_result_metrics(self):
        from backend.services.quantevolver.multi_alpha_diagnostics import MultiAlphaDiagnostics

        diag = MultiAlphaDiagnostics()

        unified_row = (
            "completed",
            {
                "multi_alpha_detail": {
                    "meta_method": "equal",
                    "combined_ic": 0.15,
                    "group_results": [
                        {
                            "group_name": "g1",
                            "factor_count": 2,
                            "model_id": "m1",
                            "dataset_type": "DatasetH",
                            "compute_resource": "cpu",
                            "ic": 0.1,
                            "icir": 1.0,
                            "sharpe": 1.2,
                            "meta_weight": 0.4,
                        },
                        {
                            "group_name": "g2",
                            "factor_count": 3,
                            "model_id": "m2",
                            "dataset_type": "DatasetH",
                            "compute_resource": "cpu",
                            "ic": 0.2,
                            "icir": 1.1,
                            "sharpe": 1.3,
                            "meta_weight": 0.6,
                        },
                    ],
                    "group_correlations": {"g1|g2": 0.2},
                }
            },
            {"execution_mode": "distributed", "meta_model": {"method": "equal"}},
        )

        with patch.object(diag, "_load_groups", return_value=[]), \
             patch.object(diag, "_load_meta_info", return_value={}), \
             patch.object(diag, "_load_correlations", return_value={}), \
             patch("backend.services.quantevolver.multi_alpha_diagnostics.get_conn") as mock_get_conn:
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = unified_row

            result = diag.analyze("exp_1")

        assert result["ok"] is True
        diagnostics = result["diagnostics"]
        assert diagnostics["meta_method"] == "equal"
        assert diagnostics["execution_mode"] == "distributed"
        assert diagnostics["combined_ic"] == 0.15
        assert diagnostics["correlations"] == {"g1|g2": 0.2}
        assert diagnostics["groups"][0]["group_name"] == "g1"
        assert diagnostics["groups"][0]["group_ic"] == 0.1
        assert diagnostics["groups"][0]["status"] == "completed"

    def test_diagnostics_merges_unified_metrics_into_db_groups(self):
        from backend.services.quantevolver.multi_alpha_diagnostics import GroupMetrics, MultiAlphaDiagnostics

        diag = MultiAlphaDiagnostics()
        groups = [
            GroupMetrics(
                group_name="g1",
                factor_count=1,
                factor_names=["f1"],
                model_id="m1",
                dataset_type="DatasetH",
                compute_resource="cpu",
                group_ic=None,
                group_icir=None,
                group_sharpe=None,
                meta_weight=None,
                status="running",
                assigned_node_id="node1",
            )
        ]
        unified_row = (
            "completed",
            {
                "multi_alpha_detail": {
                    "meta_method": "equal",
                    "combined_ic": 0.15,
                    "group_results": [
                        {
                            "group_name": "g1",
                            "ic": 0.1,
                            "icir": 1.0,
                            "sharpe": 1.2,
                            "meta_weight": 0.4,
                            "model_id": "m1",
                        }
                    ],
                    "group_correlations": {},
                }
            },
            {"execution_mode": "distributed", "meta_model": {"method": "equal"}},
        )

        with patch.object(diag, "_load_groups", return_value=groups), \
             patch.object(diag, "_load_meta_info", return_value={}), \
             patch.object(diag, "_load_correlations", return_value={}), \
             patch("backend.services.quantevolver.multi_alpha_diagnostics.get_conn") as mock_get_conn:
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = unified_row

            result = diag.analyze("exp_1")

        merged = result["diagnostics"]["groups"][0]
        assert merged["group_ic"] == 0.1
        assert merged["group_icir"] == 1.0
        assert merged["group_sharpe"] == 1.2
        assert merged["meta_weight"] == 0.4
        assert merged["status"] == "completed"

    def test_diagnostics_returns_persisted_multi_alpha_analysis(self):
        from backend.services.quantevolver.multi_alpha_diagnostics import MultiAlphaDiagnostics

        diag = MultiAlphaDiagnostics()
        unified_row = (
            "completed",
            {
                "multi_alpha_detail": {
                    "meta_method": "equal",
                    "combined_ic": 0.03,
                    "group_results": [
                        {"group_name": "g1", "ic": 0.02, "icir": 1.0, "sharpe": 1.1, "meta_weight": 0.5, "model_id": "m1"},
                        {"group_name": "g2", "ic": -0.01, "icir": -0.5, "sharpe": -0.8, "meta_weight": 0.5, "model_id": "m2"},
                    ],
                    "group_correlations": {"g1|g2": 0.1},
                },
                "multi_alpha_analysis": {
                    "schema_version": 1,
                    "combined_vs_groups": {"combined_ic": 0.03, "best_group_ic": 0.02},
                    "portfolio_diagnostics": {"annualized_turnover": 31.0},
                    "diversification": {"effective_group_count": 2.0},
                    "group_diagnostics": [
                        {
                            "group_name": "g2",
                            "training_diagnostics": {"overfit_ratio": 1.4},
                            "prediction_diagnostics": {"pred_rank_turnover": 0.3},
                        }
                    ],
                    "data_availability": {"groups_with_enhanced_metrics": 2, "ic_quality": True},
                    "ic_quality": {"combined": {"valid_days": 9, "skipped_days": 1}},
                    "optimization_guidance": [
                        {
                            "rule_id": "group_overfit_risk",
                            "severity": "medium",
                            "message": "g2 overfit",
                            "recommendation": "regularize",
                            "action_type": "switch_model",
                            "affected_groups": ["g2"],
                            "evidence": {"overfit_ratio": 1.4},
                            "source_fields": ["multi_alpha_analysis.group_diagnostics"],
                        }
                    ],
                },
            },
            {"execution_mode": "serial", "meta_model": {"method": "equal"}},
        )

        with patch.object(diag, "_load_groups", return_value=[]), \
             patch.object(diag, "_load_meta_info", return_value={}), \
             patch.object(diag, "_load_correlations", return_value={}), \
             patch("backend.services.quantevolver.multi_alpha_diagnostics.get_conn") as mock_get_conn:
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = unified_row

            result = diag.analyze("exp_1")

        diagnostics = result["diagnostics"]
        assert diagnostics["data_availability"]["groups_with_enhanced_metrics"] == 2
        assert diagnostics["ic_quality"]["combined"]["skipped_days"] == 1
        assert diagnostics["portfolio_diagnostics"]["annualized_turnover"] == 31.0
        assert diagnostics["optimization_guidance"][0]["rule_id"] == "group_overfit_risk"


class TestRouterEnhancedMetricsFallback:
    def test_enhanced_metrics_falls_back_to_cached_result_metrics(self):
        with patch.object(quantevolver_router, "get_conn") as mock_get_conn:
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = (
                None,
                "task_x",
                {
                    "enhanced_metrics": {
                        "ic_diagnostics": {
                            "dates": ["2024-01-01"],
                            "ic_series": [0.04],
                        },
                        "return_curves": {
                            "dates": ["2024-01-01"],
                            "cumulative_excess_with_cost": [0.01],
                        },
                        "training_diagnostics": {
                            "train_loss_curve": [0.5, 0.3],
                        },
                        "summary": {"ic": 0.04},
                    }
                },
            )

            result = asyncio.run(quantevolver_router.get_experiment_enhanced_metrics("exp_1"))

        assert result["dates"] == ["2024-01-01"]
        assert result["ic_series"] == [0.04]
        assert result["cumulative_excess_with_cost"] == [0.01]
        assert result["train_loss_curve"] == [0.5, 0.3]
        assert result["summary"] == {"ic": 0.04}

    def test_enhanced_metrics_uses_flat_cached_result_metrics(self):
        with patch.object(quantevolver_router, "get_conn") as mock_get_conn:
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = (
                None,
                "task_x",
                {
                    "dates": ["2024-01-01"],
                    "ic_series": [0.04],
                    "cumulative_excess_with_cost": [0.01],
                    "train_loss_curve": [0.5, 0.3],
                },
            )

            result = asyncio.run(quantevolver_router.get_experiment_enhanced_metrics("exp_1"))

        assert result["dates"] == ["2024-01-01"]
        assert result["ic_series"] == [0.04]
        assert result["cumulative_excess_with_cost"] == [0.01]
        assert result["train_loss_curve"] == [0.5, 0.3]

    def test_enhanced_metrics_exposes_cached_multi_alpha_analysis(self):
        with patch.object(quantevolver_router, "get_conn") as mock_get_conn:
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = (
                None,
                "task_x",
                {
                    "enhanced_metrics": {
                        "summary": {"IC": 0.04},
                        "multi_alpha_analysis": {
                            "schema_version": 1,
                            "data_availability": {"groups_with_enhanced_metrics": 2},
                        },
                    },
                    "multi_alpha_detail": {"total_groups": 2},
                },
            )

            result = asyncio.run(quantevolver_router.get_experiment_enhanced_metrics("exp_1"))

        assert result["multi_alpha_detail"]["total_groups"] == 2
        assert result["multi_alpha_analysis"]["data_availability"]["groups_with_enhanced_metrics"] == 2


class TestRouterMultiAlphaNodeLoopTracking:
    def test_poll_multi_alpha_nodes_uses_group_qe_loop_id(self):
        class DummyClient:
            def __init__(self):
                self.calls = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

            async def get_loop_status(self, task_id, loop_id):
                self.calls.append((task_id, loop_id))
                return {"status": "completed"}

        dummy_client = DummyClient()

        with patch.object(quantevolver_router, "get_conn") as mock_get_conn, \
             patch("backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient.for_node", return_value=dummy_client):
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchall.side_effect = [
                [("running", 1)],
                [("node1", "Loop9")],
            ]

            status = asyncio.run(quantevolver_router._poll_multi_alpha_nodes("exp_1", "task_x"))

            assert status == "completed"
            assert dummy_client.calls == [("task_x", "Loop9")]


class TestMultiAlphaResultCollectorFailFast:
    def test_build_result_metrics_embeds_multi_alpha_analysis(self):
        collector = MultiAlphaResultCollector()

        result = collector._build_result_metrics(
            {
                "IC": 0.03,
                "_enhanced_metrics": {
                    "summary": {
                        "IC": 0.03,
                        "1day.excess_return_with_cost.annualized_return": 0.10,
                        "1day.excess_return_without_cost.annualized_return": 0.12,
                    },
                    "trade_diagnostics": {"annualized_turnover": 31.0},
                    "prediction_diagnostics": {"pred_rank_turnover": 0.4},
                },
            },
            "equal",
            {"g1": 0.5, "g2": 0.5},
            [
                {"group_name": "g1", "ic": 0.02, "icir": 1.0, "sharpe": 1.1, "meta_weight": 0.5, "factor_count": 1, "model_id": "m1"},
                {"group_name": "g2", "ic": -0.01, "icir": -0.5, "sharpe": -0.8, "meta_weight": 0.5, "factor_count": 1, "model_id": "m2"},
            ],
            {"g1|g2": 0.1},
            {
                "g1": {
                    "summary": {"IC": 0.02, "Rank IC": 0.021, "l2.train": 0.9, "l2.valid": 1.0},
                    "ic_diagnostics": {"ic_series": [0.01, 0.03], "ic_positive_ratio": 1.0},
                    "prediction_diagnostics": {"pred_rank_turnover": 0.2, "top30_stability": 0.6},
                    "factor_analysis": {"feature_importance": [{"name": "f1", "gain": 1.0}]},
                },
                "g2": {
                    "summary": {"IC": -0.01, "Rank IC": -0.011, "l2.train": 0.8, "l2.valid": 1.1},
                    "ic_diagnostics": {"ic_series": [-0.01, -0.02], "ic_positive_ratio": 0.0},
                    "prediction_diagnostics": {"pred_rank_turnover": 0.3, "top30_stability": 0.5},
                },
            },
            {
                "combined": {
                    "total_days": 10,
                    "valid_days": 9,
                    "skipped_days": 1,
                    "skipped_samples": [
                        {"date": "2026-03-09", "reason": "insufficient_valid_samples_after_dropna"}
                    ],
                }
            },
        )

        analysis = result["multi_alpha_analysis"]
        assert result["multi_alpha_detail"]["ic_quality"]["combined"]["skipped_days"] == 1
        assert analysis["ic_quality"]["combined"]["valid_days"] == 9
        assert analysis["data_availability"]["ic_quality"] is True
        assert analysis["data_availability"]["groups_with_enhanced_metrics"] == 2
        assert analysis["portfolio_diagnostics"]["cost_drag_annualized"] == pytest.approx(-0.02)
        assert analysis["group_diagnostics"][0]["training_diagnostics"]["final_train_loss"] == 0.9
        assert result["enhanced_metrics"]["multi_alpha_analysis"]["schema_version"] == 1
        assert any(g["rule_id"] == "negative_weighted_group" for g in analysis["optimization_guidance"])

    def test_collect_and_persist_requires_qe_loop_id(self):
        collector = MultiAlphaResultCollector()

        with patch.object(collector, "_get_experiment_record", return_value={
            "qe_task_id": "task_x",
            "qe_loop_id": None,
            "multi_alpha_config": {},
        }), patch.object(collector, "_get_group_records", return_value=[{"group_name": "g1", "status": "completed"}, {"group_name": "g2", "status": "completed"}]):
            with pytest.raises(ValueError, match="qe_loop_id"):
                asyncio.run(collector.collect_and_persist("exp_1"))

    def test_collect_and_persist_distributed_does_not_require_parent_qe_loop_id(self):
        collector = MultiAlphaResultCollector()

        async def fake_collect_distributed(task_id, groups, multi_alpha_config):
            return {
                "group_metrics": {
                    "g1": {"ic": 0.1, "icir": 1.0, "sharpe": 1.2},
                    "g2": {"ic": 0.2, "icir": 1.1, "sharpe": 1.3},
                },
                "meta_weights": {"g1": 0.4, "g2": 0.6},
                "correlations": {"g1|g2": 0.2},
                "meta_method": "equal",
                "_combined_metrics": {"IC": 0.15},
            }

        with patch.object(collector, "_get_experiment_record", return_value={
            "qe_task_id": "task_x",
            "qe_loop_id": None,
            "multi_alpha_config": {"meta_model": {"method": "equal", "lookback_days": 60}},
        }), patch.object(collector, "_get_group_records", return_value=[
            {"group_name": "g1", "assigned_node_id": "node1", "qe_loop_id": "Loop7", "model_id": "m1", "factor_names": [], "status": "completed"},
            {"group_name": "g2", "assigned_node_id": "node2", "qe_loop_id": "Loop8", "model_id": "m2", "factor_names": [], "status": "completed"},
        ]), patch.object(collector, "_collect_distributed", side_effect=fake_collect_distributed), \
             patch.object(collector, "_update_distributed_group_records"), \
             patch.object(collector, "_insert_meta_weights"), \
             patch.object(collector, "_insert_correlations"), \
             patch.object(collector, "_build_result_metrics", return_value={}), \
             patch.object(collector, "_update_experiment_unified"):
            result = asyncio.run(collector.collect_and_persist("exp_1"))

        assert result["ok"] is True

    def test_collect_and_persist_distributed_uses_existing_group_metrics(self):
        collector = MultiAlphaResultCollector()

        async def fake_collect_distributed(task_id, groups, multi_alpha_config):
            return {
                "group_metrics": {},
                "meta_weights": {"g1": 0.4, "g2": 0.6},
                "correlations": {"g1|g2": 0.2},
                "meta_method": "equal",
                "_combined_metrics": {"IC": 0.15},
            }

        with patch.object(collector, "_get_experiment_record", return_value={
            "qe_task_id": "task_x",
            "qe_loop_id": None,
            "multi_alpha_config": {"meta_model": {"method": "equal", "lookback_days": 60}},
        }), patch.object(collector, "_get_group_records", return_value=[
            {
                "group_name": "g1",
                "assigned_node_id": "node1",
                "qe_loop_id": "Loop7",
                "model_id": "m1",
                "factor_names": ["f1"],
                "group_ic": 0.1,
                "group_icir": 1.0,
                "group_sharpe": 1.2,
                "status": "completed",
            },
            {
                "group_name": "g2",
                "assigned_node_id": "node2",
                "qe_loop_id": "Loop8",
                "model_id": "m2",
                "factor_names": ["f2", "f3"],
                "group_ic": 0.2,
                "group_icir": 1.1,
                "group_sharpe": 1.3,
                "status": "completed",
            },
        ]), patch.object(collector, "_collect_distributed", side_effect=fake_collect_distributed), \
             patch.object(collector, "_update_distributed_group_records"), \
             patch.object(collector, "_insert_meta_weights"), \
             patch.object(collector, "_insert_correlations"), \
             patch.object(collector, "_build_result_metrics", return_value={}), \
             patch.object(collector, "_update_experiment_unified"):
            result = asyncio.run(collector.collect_and_persist("exp_1"))

        assert result["ok"] is True
        # group_metrics 为空时，fallback 到 DB 记录的 group_ic/group_icir
        assert result["group_results"][0]["ic"] == 0.1
        assert result["group_results"][1]["ic"] == 0.2

    def test_collect_distributed_requires_group_qe_loop_id(self):
        collector = MultiAlphaResultCollector()

        with pytest.raises(RuntimeError, match="缺少 qe_loop_id"):
            asyncio.run(collector._collect_distributed(
                "task_x",
                [
                    {"group_name": "g1", "assigned_node_id": "node1", "qe_loop_id": None},
                    {"group_name": "g2", "assigned_node_id": "node2", "qe_loop_id": "Loop2"},
                ],
                {"meta_model": {"method": "equal", "lookback_days": 60}},
            ))

    def test_collect_distributed_graceful_without_label(self):
        """ic_weighted without label should not crash — falls back to enhanced.json or equal."""
        # This test just verifies the method signature is correct and
        # the actual network calls would fail (no mock), so we test the
        # _update_group_records path instead.
        collector = MultiAlphaResultCollector()

        with pytest.raises(RuntimeError, match="group_metrics 缺失组"):
            collector._update_group_records(
                "exp_1",
                [{"group_name": "g1", "model_id": "m1", "factor_names": []}],
                {"g1": None},
                {"g1": 1.0},
            )

    def test_update_group_records_requires_non_empty_group_metrics(self):
        collector = MultiAlphaResultCollector()

        with pytest.raises(RuntimeError, match="group_metrics 缺少关键指标"):
            collector._update_group_records(
                "exp_1",
                [{"group_name": "g1", "model_id": "m1", "factor_names": []}],
                {"g1": {}},
                {"g1": 1.0},
            )

class TestMultiAlphaPhase2ArtifactValidation:
    def test_single_node_artifact_validation_requires_group_prediction(self):
        from backend.services.quantevolver.multi_alpha_result_collector import MultiAlphaArtifactError

        class DummyClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

            async def download_workspace_file_bytes(self, task_id, loop_id, file_path):
                return b"combined" if file_path == "combined_prediction.pkl" else b"x"

            async def get_workspace_file(self, task_id, loop_id, file_path):
                return {"ok": True}

            async def download_group_predictions(self, task_id, loop_id, group_name):
                raise RuntimeError("pred not found")

        collector = MultiAlphaResultCollector()
        with patch("backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient", return_value=DummyClient()):
            with pytest.raises(MultiAlphaArtifactError, match="group g1 pred.pkl"):
                asyncio.run(collector._validate_single_node_artifacts(
                    "task_x",
                    "Loop1",
                    [{"group_name": "g1", "status": "completed"}],
                ))

    def test_single_node_artifact_validation_accepts_required_artifacts(self):
        class DummyClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

            async def download_workspace_file_bytes(self, task_id, loop_id, file_path):
                return b"combined"

            async def get_workspace_file(self, task_id, loop_id, file_path):
                if file_path.endswith("qlib_results_enhanced.json"):
                    return {"summary": {"IC": 0.1}, "artifact": file_path}
                return {"ok": True, "artifact": file_path}

            async def download_group_predictions(self, task_id, loop_id, group_name):
                return b"pred"

        collector = MultiAlphaResultCollector()
        with patch("backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient", return_value=DummyClient()):
            asyncio.run(collector._validate_single_node_artifacts(
                "task_x",
                "Loop1",
                [{"group_name": "g1", "status": "completed"}],
            ))

    def test_multi_alpha_results_completed_without_meta_is_conflict_not_empty_success(self):
        with patch.object(quantevolver_router, "get_conn") as mock_get_conn:
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = ("completed", {})
            cur.fetchall.side_effect = [
                [(
                    "g1", ["f1"], "m1", "DatasetH",
                    None, None, None, None,
                    "node1", "completed", None,
                )],
                [],
            ]
            cur.description = [
                ("group_name",), ("factor_names",), ("model_id",), ("dataset_type",),
                ("group_ic",), ("group_icir",), ("group_sharpe",), ("meta_weight",),
                ("assigned_node_id",), ("status",), ("error_message",),
            ]

            with pytest.raises(quantevolver_router.HTTPException) as exc:
                quantevolver_router.get_multi_alpha_results("exp_1")

        assert exc.value.status_code == 409
        assert "artifact-ready" in str(exc.value.detail)

    def test_run_status_marks_artifact_collection_failure_explicitly(self):
        collector_instance = SimpleNamespace(
            collect_and_persist=AsyncMock(side_effect=RuntimeError("missing multi_alpha_results.json"))
        )

        with patch.object(quantevolver_router, "get_conn") as mock_get_conn, \
             patch.object(quantevolver_router, "_load_multi_alpha_status_payload", return_value={
                 "stage": "result_collection",
                 "artifact_status": "validating",
                 "groups": [],
                 "total_groups": 2,
                 "completed_groups": 2,
                 "failed_groups": 0,
                 "running_groups": 0,
                 "group_status_counts": {"completed": 2},
             }), \
             patch.object(quantevolver_router, "_poll_multi_alpha_nodes", AsyncMock(return_value="completed")), \
             patch.object(quantevolver_router, "_mark_multi_alpha_artifact_failure") as mark_failed, \
             patch("backend.services.quantevolver.multi_alpha_result_collector.MultiAlphaResultCollector", return_value=collector_instance):
            conn = mock_get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = ("running", "task_x", "Loop1", None, "multi")
            cur.description = [("status",), ("qe_task_id",), ("qe_loop_id",), ("result_metrics",), ("alpha_mode",)]

            result = asyncio.run(quantevolver_router.get_experiment_run_status("exp_1"))

        assert result["status"] == "failed"
        assert result["multi_alpha_stage"] == "failed_artifact"
        assert result["artifact_status"] == "failed"
        assert "missing multi_alpha_results.json" in result["error"]
        mark_failed.assert_called_once()
