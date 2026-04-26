"""
QE Unified Engine — BaseExecutor + ExecutionContext + ExecutionResult
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class ExecutionContext(BaseModel):
    """执行上下文 — 与实验配置正交的执行环境参数"""
    task_id: str
    loop_index: int
    experiment_name: str
    node_id: str | None = None
    callback_url: str | None = None
    model_source: dict[str, Any] | None = None  # backtest-only 模式下的模型来源
    extra_experiment_files: dict[str, str] | None = None


class ExecutionResult(BaseModel):
    """执行结果"""
    job_id: str
    status: str                                   # submitted / running / completed / failed
    experiment_files: dict[str, str] | None = None
    wsl_command: str | None = None
    detail: dict[str, Any] | None = None


class BaseExecutor(ABC):
    """执行器抽象接口 — 所有执行目标实现此接口"""

    @abstractmethod
    async def submit(
        self,
        config: "ExperimentConfig",  # noqa: F821
        ctx: ExecutionContext,
        **kwargs,
    ) -> ExecutionResult:
        """提交实验到执行目标"""
        ...
