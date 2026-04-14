"""
QE Unified Engine — BacktestResultAnalyzer

统一的回测结果分析器，替代 process_completed_loop /
process_strategy_evo_completed_loop / _update_experiment_with_metrics
中分散的指标提取逻辑。
"""
from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel

from .metrics_normalizer import normalize_metrics
from .metrics_store import MetricsStore

logger = logging.getLogger("aistock.quantevolver.analysis.backtest_analyzer")


class BacktestResult(BaseModel):
    """回测结果 — 与前端 ExperimentDetailPage 数据结构对齐"""

    experiment_id: str | None = None

    # 核心指标（归一化后）
    metrics: dict[str, Any]

    # 增强指标（用于图表展示，fetch_enhanced=True 时填充）
    enhanced: dict[str, Any] | None = None


class BacktestResultAnalyzer:
    """
    统一的回测结果分析器。

    替代当前分散在以下函数中的指标提取逻辑：
    - process_completed_loop (qe_evolution_service.py:1207-1244)
    - process_strategy_evo_completed_loop (qe_evolution_service.py:3109-3174)

    Agent 分析（run_analyst / run_evaluator）不在此层 —
    那是自动演进的决策逻辑，保留在调用方。
    """

    def __init__(
        self,
        workspace_client: Any,
        metrics_store: MetricsStore,
    ):
        self.client = workspace_client
        self.store = metrics_store

    async def analyze(
        self,
        task_id: str,
        loop_id: str,
        config: "ExperimentConfig",  # noqa: F821
        loop_index: int,
        save_to_db: bool = True,
        fetch_enhanced: bool = True,
        is_sota: bool = False,
        experiment_name_suffix: str | None = None,
    ) -> BacktestResult:
        """完整的回测结果分析流程。

        Args:
            task_id:   任务 ID
            loop_id:   RDAgent 侧 loop_id（如 "Loop1"）
            config:    实验配置（用于写入 DB）
            loop_index: Loop 序号
            save_to_db: 是否写入 DB（False 用于 dry-run / 测试）
            fetch_enhanced: 是否获取增强指标（IC 时序、收益曲线等）
            is_sota:   是否标记为 SOTA
            experiment_name_suffix: qe_experiments.experiment_name 后缀

        Returns:
            BacktestResult — 包含所有前端展示需要的数据
        """
        # 1. 获取基础指标并归一化
        raw_metrics = await self.client.get_loop_metrics(task_id, loop_id)
        metrics = normalize_metrics(raw_metrics)

        # 2. 获取增强指标（可选）
        enhanced: dict[str, Any] | None = None
        if fetch_enhanced:
            enhanced = await self.client.get_enhanced_metrics(task_id, loop_id)

        result = BacktestResult(metrics=metrics, enhanced=enhanced)

        # 3. 写入 DB（可选）
        if save_to_db:
            evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
            experiment_id = self.store.save_experiment_record(
                task_id=task_id,
                loop_id=evolution_loop_db_id,
                loop_index=loop_index,
                config=config,
                metrics=metrics,
                is_sota=is_sota,
                experiment_name_suffix=experiment_name_suffix,
            )
            self.store.save_loop_metrics(
                loop_id=evolution_loop_db_id,
                metrics=metrics,
                experiment_id=experiment_id,
                status="completed",
            )
            if is_sota:
                self.store.save_sota_registry(evolution_loop_db_id)
            result.experiment_id = experiment_id

        return result
