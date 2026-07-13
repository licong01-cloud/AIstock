"""
QE Unified Engine - BaseExecutor + ExecutionContext + ExecutionResult.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class ExecutionContext(BaseModel):
    """Execution environment parameters orthogonal to ExperimentConfig."""
    task_id: str
    loop_index: int
    experiment_name: str
    node_id: str | None = None
    callback_url: str | None = None
    model_source: dict[str, Any] | None = None
    extra_experiment_files: dict[str, str] | None = None
    require_fixed_seed: bool = False
    resource_session_id: str | None = None
    resource_source_run_key: str | None = None
    resource_session_token: str | None = None
    phase_pipeline_enabled: bool = False


class ExecutionResult(BaseModel):
    """Executor submission result."""
    job_id: str
    status: str
    experiment_files: dict[str, str] | None = None
    wsl_command: str | None = None
    detail: dict[str, Any] | None = None


class BaseExecutor(ABC):
    """Abstract executor interface for QE execution targets."""

    @abstractmethod
    async def submit(
        self,
        config: "ExperimentConfig",  # noqa: F821
        ctx: ExecutionContext,
        **kwargs,
    ) -> ExecutionResult:
        """Submit an experiment to the execution target."""
        ...
