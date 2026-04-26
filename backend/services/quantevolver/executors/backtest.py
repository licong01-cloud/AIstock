"""
QE Unified Engine — BacktestExecutor

封装 ConfigComposer + QEWorkspaceClient 的两步调用，
替代当前 4 套重复的参数组装代码。
"""
from __future__ import annotations

import re
import logging
from enum import Enum
from typing import Any

from ..experiment_config import ExperimentConfig
from .base import BaseExecutor, ExecutionContext, ExecutionResult

logger = logging.getLogger("aistock.quantevolver.executors.backtest")


class BacktestMode(str, Enum):
    FULL_TRAIN = "full_train"        # 完整训练 + 回测（Path 1/2/4）
    BACKTEST_ONLY = "backtest_only"  # 复用已训练模型，仅回测（Path 3）


class BacktestExecutor(BaseExecutor):
    """
    RDAgent WSL 回测执行器。

    封装 ConfigComposer + QEWorkspaceClient 的两步调用。
    无状态设计，依赖通过构造函数注入，方便测试。
    """

    def __init__(self, composer: Any, client: Any):
        """
        Args:
            composer: ConfigComposer 实例
            client:   QEWorkspaceClient 实例
        """
        self.composer = composer
        self.client = client

    async def submit(
        self,
        config: ExperimentConfig,
        ctx: ExecutionContext,
        mode: BacktestMode = BacktestMode.FULL_TRAIN,
        **kwargs,
    ) -> ExecutionResult:
        """
        提交回测任务到 RDAgent。

        Args:
            config: 实验配置（配置层产出）
            ctx:    执行上下文（task_id, loop_index 等）
            mode:   FULL_TRAIN 或 BACKTEST_ONLY
        """
        if mode == BacktestMode.BACKTEST_ONLY and not ctx.model_source:
            raise ValueError(
                "BACKTEST_ONLY requires ctx.model_source; refusing to assume an "
                f"implicit workspace model for task={ctx.task_id} loop={ctx.loop_index}"
            )

        # 1. 构建 custom_params（配置层唯一注入点）
        custom_params = config.build_custom_params()
        strategy_params = config.build_strategy_params()
        if ctx.node_id:
            from ..stock_pool_sync import sync_stock_pool_to_compute_node_by_id

            sync_stock_pool_to_compute_node_by_id(
                ctx.node_id,
                custom_params.get("stock_pool"),
            )

        # 2. 调用 ConfigComposer（已有统一层，不改）
        compose_res = self.composer.compose_experiment_in_memory(
            factor_names=config.factor_names,
            model_id=config.model_id,
            strategy_id=config.strategy_id,
            data_split=config.data_split,
            custom_params=custom_params,
            experiment_name=ctx.experiment_name,
            skip_db_save=True,
            execution_algo=config.execution_algo,
            execution_algo_params=config.execution_algo_params,
            strategy_params=strategy_params if strategy_params else None,
            node_id=ctx.node_id,
        )

        experiment_files: dict[str, str] = compose_res.get("experiment_files", {})
        wsl_command: str = compose_res.get("wsl_command", "")
        if not wsl_command:
            raise ValueError(
                f"compose_experiment_in_memory returned empty wsl_command for "
                f"task={ctx.task_id} loop={ctx.loop_index}"
            )

        if ctx.extra_experiment_files:
            duplicate_keys = set(experiment_files).intersection(ctx.extra_experiment_files)
            if duplicate_keys:
                raise ValueError(
                    "extra_experiment_files would overwrite generated files: "
                    f"{sorted(duplicate_keys)}"
                )
            experiment_files.update(ctx.extra_experiment_files)

        # 3. 注入 --backtest-only（Path 3 专用）
        if mode == BacktestMode.BACKTEST_ONLY:
            wsl_command = re.sub(
                r"(python\s+qrun_limit_minute\.py\s+\S+\.yaml)",
                r"\1 --backtest-only",
                wsl_command,
            )

        # 4. 构建传给 RDAgent 的 config 记录
        rdagent_config = {
            "factor_list": config.factor_names,
            "model_id": config.model_id,
            "strategy_id": config.strategy_id,
            "data_split": config.data_split,
            "model_params": custom_params,
        }

        # 5. 提交到 RDAgent
        job_id = await self.client.create_and_run_loop(
            ctx.task_id,
            ctx.loop_index,
            rdagent_config,
            experiment_files,
            wsl_command,
            model_source=ctx.model_source,
            callback_url=ctx.callback_url,
        )

        return ExecutionResult(
            job_id=job_id,
            status="submitted",
            experiment_files=experiment_files,
            wsl_command=wsl_command,
        )
