"""Retired Paper v2 MiniQMT adapter compatibility gate.

The product execution path is MiniQMTExecutionRuntimeClient.  This module is
kept only to fail fast for stale imports until the final post-L5 deletion chore.
"""

from __future__ import annotations

from typing import Any

from backend.services.simulation_runtime.models import ExecutionPathNotCanonicalError


class MiniQMTLiveAlgoAdapter:
    """Legacy adapter name retained as a fail-fast compatibility gate."""

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        raise ExecutionPathNotCanonicalError(
            "Paper v2 MiniQMT legacy live algo adapter is retired; use MiniQMTExecutionRuntimeClient",
            context={
                "legacy_path": "backend.services.paper_trading_v2.execution.minqmt_live_algo_adapter.MiniQMTLiveAlgoAdapter",
                "required_runtime_owner": "MiniQMTExecutionRuntime",
                "replacement": "backend.services.miniqmt_execution_runtime.MiniQMTExecutionRuntimeClient",
            },
        )


__all__ = ["MiniQMTLiveAlgoAdapter"]
