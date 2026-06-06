"""Compatibility adapter exposing vn.py-style assets through ALGO_REGISTRY.

Paper v2 MiniQMT uses the event/action cores directly. StrategyPackage policy
validation still expects every selectable algo_code in the legacy registry, so
these classes provide a fail-fast bridge without pretending to be historical
minute-bar replay algorithms.
"""

from __future__ import annotations

from typing import Any

from backend.execution_algos.base_algo import BaseExecutionAlgo, OrderState, StepResult
from backend.execution_algos.registry import register

from .base import VnpyStyleConfigError
from .registry import validate_vnpy_style_config


class VnpyStyleRegistryAlgo(BaseExecutionAlgo):
    ALGO_CODE = "VNPY_STYLE_REGISTRY"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config=config)
        validate_vnpy_style_config(self.ALGO_CODE, self.config)

    def compute_step(
        self,
        state: OrderState,
        bar_data: dict[str, Any],
        market_context: dict[str, Any],
    ) -> StepResult | None:
        raise VnpyStyleConfigError(
            f"{self.ALGO_CODE} is a live MiniQMT event-driven execution asset; "
            "use backend.execution_algos.vnpy_style core or Paper v2 MiniQMT adapter"
        )


@register
class SniperMiniQMTRegistryAlgo(VnpyStyleRegistryAlgo):
    ALGO_CODE = "SNIPER_MINIQMT"


@register
class BestLimitMiniQMTRegistryAlgo(VnpyStyleRegistryAlgo):
    ALGO_CODE = "BEST_LIMIT_MINIQMT"


@register
class TwapLiteMiniQMTRegistryAlgo(VnpyStyleRegistryAlgo):
    ALGO_CODE = "TWAP_LITE_MINIQMT"
