"""
Level 2 集成测试 — BacktestExecutor

验证 BacktestExecutor 传给 ConfigComposer 和 QEWorkspaceClient 的参数
与现有 4 条路径完全一致。使用 Mock 替代真实外部依赖。
"""
import pytest
import asyncio
import threading
import time
from unittest.mock import MagicMock, AsyncMock, patch

from backend.services.quantevolver.experiment_config import ExperimentConfig
from backend.services.quantevolver.experiment_config_builders import (
    build_config_from_evolution_loop,
    build_config_from_strategy_evo_loop,
    build_config_from_custom_evo_loop,
)
from backend.services.quantevolver.executors.base import ExecutionContext
from backend.services.quantevolver.executors.backtest import BacktestExecutor, BacktestMode
from tests.fixtures.sample_configs import (
    EVOLUTION_CONFIG_MINIMAL,
    EVOLUTION_TASK_MINIMAL,
    EVOLUTION_TASK_WITH_UNFILLED,
    EVOLUTION_TASK_WITH_HMM,
    STRATEGY_EVO_BASE_CONFIG,
    STRATEGY_EVO_LOOP_NO_HMM,
    STRATEGY_EVO_LOOP_WITH_HMM,
    STRATEGY_EVO_TASK,
    CUSTOM_EVO_LOOP_MINIMAL,
    CUSTOM_EVO_LOOP_FULL,
    CUSTOM_EVO_TASK,
    HMM_SNAPSHOT,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

MOCK_WSL_COMMAND = "cd /mnt/f && python qrun_limit_minute.py conf.yaml"
MOCK_EXPERIMENT_FILES = {"conf.yaml": "mock_yaml_content", "factor.py": "mock_factor"}


def make_mock_composer(wsl_command: str = MOCK_WSL_COMMAND) -> MagicMock:
    composer = MagicMock()
    composer.compose_experiment_in_memory.return_value = {
        "experiment_files": dict(MOCK_EXPERIMENT_FILES),
        "wsl_command": wsl_command,
    }
    return composer


def make_mock_client(job_id: str = "Loop1") -> AsyncMock:
    client = AsyncMock()
    client.create_and_run_loop.return_value = job_id
    return client


def make_ctx(
    task_id: str = "task_test",
    loop_index: int = 1,
    experiment_name: str = "task_test/Loop1",
    node_id: str | None = None,
    callback_url: str | None = None,
    model_source: dict | None = None,
    extra_experiment_files: dict | None = None,
    require_fixed_seed: bool = False,
) -> ExecutionContext:
    return ExecutionContext(
        task_id=task_id,
        loop_index=loop_index,
        experiment_name=experiment_name,
        node_id=node_id,
        callback_url=callback_url,
        model_source=model_source,
        extra_experiment_files=extra_experiment_files,
        require_fixed_seed=require_fixed_seed,
    )


# ── BacktestExecutor 基础测试 ──────────────────────────────────────────────────

class TestBacktestExecutorBasic:
    def test_submit_returns_execution_result(self):
        composer = make_mock_composer()
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        cfg = ExperimentConfig(factor_names=["f1", "f2"], model_id="lgbm")
        ctx = make_ctx()

        result = asyncio.get_event_loop().run_until_complete(
            executor.submit(cfg, ctx)
        )

        assert result.job_id == "Loop1"
        assert result.status == "submitted"
        assert result.experiment_files == MOCK_EXPERIMENT_FILES
        assert result.wsl_command == MOCK_WSL_COMMAND

    def test_backtest_only_requires_model_source(self):
        executor = BacktestExecutor(make_mock_composer(), make_mock_client())
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")
        ctx = make_ctx()  # no model_source

        with pytest.raises(ValueError, match="model_source"):
            asyncio.get_event_loop().run_until_complete(
                executor.submit(cfg, ctx, mode=BacktestMode.BACKTEST_ONLY)
            )

    def test_backtest_only_injects_flag(self):
        wsl_cmd = "cd /mnt/f && python qrun_limit_minute.py /path/conf.yaml"
        composer = make_mock_composer(wsl_command=wsl_cmd)
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")
        ctx = make_ctx(model_source={"source_task_id": "t1", "source_loop": "Loop1"})

        result = asyncio.get_event_loop().run_until_complete(
            executor.submit(cfg, ctx, mode=BacktestMode.BACKTEST_ONLY)
        )

        assert "--backtest-only" in result.wsl_command

    def test_full_train_does_not_inject_backtest_only(self):
        executor = BacktestExecutor(make_mock_composer(), make_mock_client())
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")
        ctx = make_ctx()

        result = asyncio.get_event_loop().run_until_complete(
            executor.submit(cfg, ctx, mode=BacktestMode.FULL_TRAIN)
        )

        assert "--backtest-only" not in (result.wsl_command or "")

    def test_seed_ensemble_reaches_composer_but_not_model_params(self):
        composer = make_mock_composer()
        composer.compose_experiment_in_memory.return_value["experiment_files"]["conf.yaml"] = """
qe_runtime:
  seed_policy: fixed
  random_seed: 42
  ensemble:
    enabled: true
    level: score
    agg: median
    seeds: [42, 2026, 12345]
task:
  model:
    class: LGBModel
    kwargs:
      seed: 42
      random_state: 42
  dataset:
    class: DatasetH
    kwargs:
      handler:
        class: DataHandlerLP
port_analysis_config:
  executor:
    class: SimulatorExecutor
  strategy:
    class: TopkDropoutStrategy
    kwargs: {}
  backtest:
    exchange_kwargs: {}
"""
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            runtime_flags={"random_seed": 42},
            seed_ensemble={
                "enabled": True,
                "seeds": [42, 2026, 12345],
                "level": "score",
                "agg": "median",
            },
        )
        ctx = make_ctx(require_fixed_seed=True)

        asyncio.get_event_loop().run_until_complete(
            executor.submit(cfg, ctx, mode=BacktestMode.FULL_TRAIN)
        )

        compose_kwargs = composer.compose_experiment_in_memory.call_args.kwargs
        assert compose_kwargs["custom_params"]["_seed_ensemble_config"]["agg"] == "median"
        rdagent_config = client.create_and_run_loop.call_args.args[2]
        assert rdagent_config["runtime_flags"]["ensemble"]["seeds"] == [42, 2026, 12345]
        assert "_seed_ensemble_config" not in rdagent_config["model_params"]

    def test_full_train_strict_seed_contract_rejects_missing_seed(self):
        executor = BacktestExecutor(make_mock_composer(), make_mock_client())
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")
        ctx = make_ctx(require_fixed_seed=True)

        with pytest.raises(ValueError, match="runtime_flags.random_seed"):
            asyncio.get_event_loop().run_until_complete(
                executor.submit(cfg, ctx, mode=BacktestMode.FULL_TRAIN)
            )

    def test_submit_keeps_event_loop_responsive_during_blocking_compose_work(self):
        loop_thread_id: int | None = None
        stock_pool_thread_id: int | None = None
        compose_thread_id: int | None = None
        blocking_started = threading.Event()
        blocking_done = threading.Event()
        ticks: list[float] = []
        blocking_stages: list[str] = []

        def blocking_stock_pool(_node_id, _stock_pool):
            nonlocal stock_pool_thread_id
            stock_pool_thread_id = threading.get_ident()
            blocking_started.set()
            blocking_stages.append("stock_pool")
            time.sleep(0.07)
            return {
                "experiment_files": {"filtered_pool_test.txt": "000001.SZ\n"},
                "install_command": "echo install-filtered-pool",
            }

        def blocking_compose(**_kwargs):
            nonlocal compose_thread_id
            compose_thread_id = threading.get_ident()
            blocking_started.set()
            for stage in ("compose", "st_pit_payload", "fingerprint"):
                blocking_stages.append(stage)
                time.sleep(0.07)
            blocking_done.set()
            return {
                "experiment_files": MOCK_EXPERIMENT_FILES,
                "wsl_command": MOCK_WSL_COMMAND,
            }

        async def ticker_while_composing():
            while not blocking_started.is_set():
                await asyncio.sleep(0)
            while not blocking_done.is_set():
                ticks.append(time.perf_counter())
                await asyncio.sleep(0.01)

        async def run_submit_and_ticker():
            nonlocal loop_thread_id
            loop_thread_id = threading.get_ident()
            composer = MagicMock()
            composer.compose_experiment_in_memory.side_effect = blocking_compose
            client = make_mock_client()
            executor = BacktestExecutor(composer, client)
            cfg = ExperimentConfig(
                factor_names=["f1"],
                model_id="lgbm",
                stock_pool="filtered_pool_test.txt",
            )
            ctx = make_ctx(node_id="node-1")

            with patch(
                "backend.services.quantevolver.stock_pool_sync."
                "prepare_stock_pool_loop_payload_for_compute_node_by_id",
                side_effect=blocking_stock_pool,
            ):
                submit_task = asyncio.create_task(executor.submit(cfg, ctx))
                ticker_task = asyncio.create_task(ticker_while_composing())
                result = await asyncio.wait_for(submit_task, timeout=2)
                await asyncio.wait_for(ticker_task, timeout=1)
                return result

        result = asyncio.run(run_submit_and_ticker())

        assert result.job_id == "Loop1"
        assert len(ticks) >= 3
        assert blocking_stages == ["stock_pool", "compose", "st_pit_payload", "fingerprint"]
        assert stock_pool_thread_id is not None
        assert compose_thread_id is not None
        assert stock_pool_thread_id == compose_thread_id
        assert compose_thread_id != loop_thread_id


# ── compose_experiment_in_memory 参数验证 ──────────────────────────────────────

    def test_empty_wsl_command_raises(self):
        """compose_experiment_in_memory 返回空 wsl_command 时应 raise，不能静默继续"""
        composer = MagicMock()
        composer.compose_experiment_in_memory.return_value = {
            "experiment_files": {"conf.yaml": "mock"},
            "wsl_command": "",  # 空命令
        }
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")
        ctx = make_ctx()

        with pytest.raises(ValueError, match="empty wsl_command"):
            asyncio.get_event_loop().run_until_complete(executor.submit(cfg, ctx))

    def test_extra_experiment_files_are_merged(self):
        composer = make_mock_composer()
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")
        ctx = make_ctx(
            extra_experiment_files={"mlruns_params.tar.gz.b64": "encoded"}
        )

        result = asyncio.get_event_loop().run_until_complete(executor.submit(cfg, ctx))

        assert result.experiment_files["mlruns_params.tar.gz.b64"] == "encoded"
        args, _ = client.create_and_run_loop.call_args
        assert args[3]["mlruns_params.tar.gz.b64"] == "encoded"

    def test_extra_experiment_files_do_not_overwrite_generated_files(self):
        composer = make_mock_composer()
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")
        ctx = make_ctx(extra_experiment_files={"conf.yaml": "overwrite"})

        with pytest.raises(ValueError, match="overwrite generated files"):
            asyncio.get_event_loop().run_until_complete(executor.submit(cfg, ctx))


    """验证 BacktestExecutor 传给 compose_experiment_in_memory 的参数
    与现有各路径代码完全一致。"""

    def _run_and_get_compose_call(self, cfg, ctx, mode=BacktestMode.FULL_TRAIN):
        composer = make_mock_composer()
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        asyncio.get_event_loop().run_until_complete(
            executor.submit(cfg, ctx, mode=mode)
        )
        return composer.compose_experiment_in_memory.call_args

    def test_path2_compose_params(self):
        """Path 2: submit_next_loop — 验证 compose 参数与 qe_evolution_service.py:1097-1109 一致"""
        cfg = build_config_from_evolution_loop(
            EVOLUTION_CONFIG_MINIMAL, EVOLUTION_TASK_MINIMAL,
            experiment_name="task_001/Loop1"
        )
        ctx = make_ctx(task_id="task_001", loop_index=1, experiment_name="task_001/Loop1")
        compose_call = self._run_and_get_compose_call(cfg, ctx)
        kwargs = compose_call.kwargs

        assert kwargs["factor_names"] == ["Alpha001", "Alpha003"]
        assert kwargs["model_id"] == "model_lgbm_v1"
        assert kwargs["skip_db_save"] is True
        assert kwargs["experiment_name"] == "task_001/Loop1"
        # stock_pool and label_type go into custom_params
        assert kwargs["custom_params"]["stock_pool"] == "csi300"
        assert kwargs["custom_params"]["label_type"] == "Ref($close, -2)/Ref($close, -1) - 1"
        assert kwargs["custom_params"]["risk_policy"]["enabled"] is True
        assert kwargs["custom_params"]["risk_policy"]["providers"] == ["st_pit"]
        assert "force_exit" in kwargs["custom_params"]["risk_policy"]["hard_actions"]

    def test_path2_unfilled_handler_in_custom_params(self):
        """Path 2: unfilled_handler 参数展开到 custom_params"""
        cfg = build_config_from_evolution_loop(
            EVOLUTION_CONFIG_MINIMAL, EVOLUTION_TASK_WITH_UNFILLED,
            experiment_name="task_001/Loop1"
        )
        ctx = make_ctx()
        compose_call = self._run_and_get_compose_call(cfg, ctx)
        cp = compose_call.kwargs["custom_params"]

        assert cp["unfilled_handler"] == "cancel_and_resubmit"
        assert cp["unfilled_trigger_minute"] == 145
        assert cp["unfilled_backup_depth"] == 3

    def test_path2_hmm_in_custom_params_for_all_auto_loops(self):
        with patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
            return_value=HMM_SNAPSHOT["model_path"],
        ), patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_config_json",
            return_value=None,
        ):
            cfg = build_config_from_evolution_loop(
                EVOLUTION_CONFIG_MINIMAL, EVOLUTION_TASK_WITH_HMM,
                experiment_name="task_001/Loop2",
            )
        ctx = make_ctx(task_id="task_001", loop_index=2, experiment_name="task_001/Loop2")
        compose_call = self._run_and_get_compose_call(cfg, ctx)
        cp = compose_call.kwargs["custom_params"]

        assert cp["enable_sector_hmm"] is True
        assert cp["hmm_model_version_id"] == "hmm_snap_001"
        assert cp["sector_hmm_model_path"] == HMM_SNAPSHOT["model_path"]
        assert cp["hmm_signal_preset"] == "preset_B"

    def test_path3_compose_params_no_hmm(self):
        """Path 3: submit_strategy_evo_loop — 验证 compose 参数与 qe_evolution_service.py:2820-2860 一致"""
        cfg = build_config_from_strategy_evo_loop(
            STRATEGY_EVO_BASE_CONFIG, STRATEGY_EVO_LOOP_NO_HMM, STRATEGY_EVO_TASK,
            experiment_name="task_strat/Loop1"
        )
        ctx = make_ctx(
            task_id="task_strat", loop_index=1,
            experiment_name="task_strat/Loop1",
            model_source={"source_task_id": "src_task", "source_loop": "Loop3"},
        )
        compose_call = self._run_and_get_compose_call(cfg, ctx, mode=BacktestMode.BACKTEST_ONLY)
        kwargs = compose_call.kwargs
        cp = kwargs["custom_params"]

        assert kwargs["factor_names"] == ["Alpha001", "Alpha002"]
        assert kwargs["skip_db_save"] is True
        assert cp["sector_blacklist"] == ["SW_Coal", "SW_Steel"]
        # strategy_params override: topk=30 wins over model_params_base topk=50
        assert cp["topk"] == 30

    def test_path3_with_hmm(self):
        """Path 3 + HMM: sector_hmm_model_path 注入到 custom_params"""
        with patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
            return_value=HMM_SNAPSHOT["model_path"],
        ), patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_config_json",
            return_value=None,
        ):
            cfg = build_config_from_strategy_evo_loop(
                STRATEGY_EVO_BASE_CONFIG, STRATEGY_EVO_LOOP_WITH_HMM, STRATEGY_EVO_TASK,
            )
        ctx = make_ctx(model_source={"source_task_id": "t", "source_loop": "Loop1"})
        compose_call = self._run_and_get_compose_call(cfg, ctx, mode=BacktestMode.BACKTEST_ONLY)
        cp = compose_call.kwargs["custom_params"]

        assert cp["enable_sector_hmm"] is True
        assert cp["sector_hmm_model_path"] == HMM_SNAPSHOT["model_path"]
        assert cp["hmm_model_version_id"] == "hmm_snap_001"
        assert cp["hmm_signal_preset"] == "aggressive"

    def test_path4_compose_params_minimal(self):
        """Path 4: submit_custom_evo_loop — 最小配置"""
        cfg = build_config_from_custom_evo_loop(
            CUSTOM_EVO_LOOP_MINIMAL, CUSTOM_EVO_TASK,
            experiment_name="task_custom/Loop1"
        )
        ctx = make_ctx(task_id="task_custom", loop_index=1, experiment_name="task_custom/Loop1")
        compose_call = self._run_and_get_compose_call(cfg, ctx)
        kwargs = compose_call.kwargs

        assert kwargs["factor_names"] == ["Alpha001", "Alpha002"]
        assert kwargs["model_id"] == "model_lgbm_v1"
        assert kwargs["strategy_id"] == "TopkDropoutStrategy"
        assert kwargs["skip_db_save"] is True

    def test_path4_compose_params_full(self):
        """Path 4: submit_custom_evo_loop — 完整配置，验证所有参数注入"""
        with patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
            return_value=HMM_SNAPSHOT["model_path"],
        ), patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_config_json",
            return_value=None,
        ):
            cfg = build_config_from_custom_evo_loop(
                CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK,
                experiment_name="task_custom/Loop1"
            )
        ctx = make_ctx(task_id="task_custom", loop_index=1, experiment_name="task_custom/Loop1")
        compose_call = self._run_and_get_compose_call(cfg, ctx)
        kwargs = compose_call.kwargs
        cp = kwargs["custom_params"]

        # execution_algo 直接传参
        assert kwargs["execution_algo"] == "twap"
        assert kwargs["execution_algo_params"] == {"interval": 5}

        # custom_params 完整性
        assert cp["enable_sector_hmm"] is True
        assert cp["sector_hmm_model_path"] == HMM_SNAPSHOT["model_path"]
        assert cp["sector_blacklist"] == ["SW_Coal", "SW_Steel"]
        assert cp["stock_pool"] == "/mnt/f/data/stock_pools/csi300.txt"
        assert cp["label_type"] == "Ref($close, -2)/Ref($close, -1) - 1"
        assert cp["unfilled_handler"] == "cancel_and_resubmit"
        assert cp["unfilled_trigger_minute"] == 145
        assert cp["unfilled_backup_depth"] == 3
        assert "initial_cash" not in cp

    def test_initial_cash_not_in_custom_params_but_in_strategy_params(self):
        """initial_cash 必须从 custom_params 中移除，但保留在 strategy_params"""
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            strategy_params={"topk": 50, "initial_cash": 5_000_000},
        )
        ctx = make_ctx()
        compose_call = self._run_and_get_compose_call(cfg, ctx)
        kwargs = compose_call.kwargs

        assert "initial_cash" not in kwargs["custom_params"]
        assert kwargs["strategy_params"]["initial_cash"] == 5_000_000


# ── create_and_run_loop 参数验证 ───────────────────────────────────────────────

class TestWorkspaceClientParams:
    """验证 BacktestExecutor 传给 create_and_run_loop 的参数正确。"""

    def test_callback_url_passed(self):
        composer = make_mock_composer()
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")
        ctx = make_ctx(callback_url="http://aistock/callback/task_001")

        asyncio.get_event_loop().run_until_complete(executor.submit(cfg, ctx))

        _, kwargs = client.create_and_run_loop.call_args
        assert kwargs.get("callback_url") == "http://aistock/callback/task_001"
        compose_kwargs = composer.compose_experiment_in_memory.call_args.kwargs
        assert compose_kwargs["callback_url"] == "http://aistock/callback/task_001"
        assert compose_kwargs["task_id"] == "task_test"
        assert compose_kwargs["loop_index"] == 1

    def test_model_source_passed_in_backtest_only(self):
        composer = make_mock_composer()
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")
        model_src = {"source_task_id": "src_task", "source_loop": "Loop3"}
        ctx = make_ctx(model_source=model_src)

        asyncio.get_event_loop().run_until_complete(
            executor.submit(cfg, ctx, mode=BacktestMode.BACKTEST_ONLY)
        )

        _, kwargs = client.create_and_run_loop.call_args
        assert kwargs.get("model_source") == model_src

    def test_node_id_passed_to_composer(self):
        composer = make_mock_composer()
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm", node_id="node_215")
        ctx = make_ctx(node_id="node_215")

        asyncio.get_event_loop().run_until_complete(executor.submit(cfg, ctx))

        compose_kwargs = composer.compose_experiment_in_memory.call_args.kwargs
        assert compose_kwargs["node_id"] == "node_215"

    def test_stock_pool_file_packaged_through_loop_payload(self):
        composer = make_mock_composer(
            "cd /home/node/qe_workspace/task/Loop1 && python qrun_limit_minute.py conf.yaml"
        )
        client = make_mock_client()
        executor = BacktestExecutor(composer, client)
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            stock_pool="filtered_pool_x",
        )
        ctx = make_ctx(node_id="rdagent-node1")

        with patch(
            "backend.services.quantevolver.stock_pool_sync.prepare_stock_pool_loop_payload_for_compute_node_by_id",
            return_value={
                "experiment_files": {"filtered_pool_x.txt": "000001.SZ\t2018-01-01\t2026-05-02\n"},
                "install_command": "test -f filtered_pool_x.txt",
            },
        ):
            result = asyncio.get_event_loop().run_until_complete(executor.submit(cfg, ctx))

        args, _kwargs = client.create_and_run_loop.call_args
        assert args[3]["filtered_pool_x.txt"].startswith("000001.SZ")
        assert args[4].startswith(
            "cd /home/node/qe_workspace/task/Loop1 && test -f filtered_pool_x.txt &&"
        )
        assert result.experiment_files["filtered_pool_x.txt"].startswith("000001.SZ")
        assert result.wsl_command == args[4]
