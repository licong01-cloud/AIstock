"""策略注册表 — @register 装饰器模式（仿 execution_algos/registry.py）."""
from __future__ import annotations

from typing import Any, Dict, Type

from .base_strategy import BaseRebalanceStrategy

STRATEGY_REGISTRY: Dict[str, Type[BaseRebalanceStrategy]] = {}


def register(cls: Type[BaseRebalanceStrategy]) -> Type[BaseRebalanceStrategy]:
    """@register 装饰器 — 将策略类注册到全局字典."""
    code = getattr(cls, "STRATEGY_CODE", None)
    if code is None:
        raise ValueError(f"{cls.__name__} 缺少 STRATEGY_CODE 类属性")
    STRATEGY_REGISTRY[code] = cls
    return cls


def get_strategy(
    strategy_code: str,
    config: Dict[str, Any] | None = None,
) -> BaseRebalanceStrategy:
    """根据策略代码获取策略实例."""
    # 延迟导入，确保所有策略模块已被加载
    _ensure_strategies_loaded()
    cls = STRATEGY_REGISTRY.get(strategy_code)
    if cls is None:
        available = list(STRATEGY_REGISTRY.keys())
        raise ValueError(
            f"未注册的再平衡策略: {strategy_code}，可用策略: {available}"
        )
    return cls(config=config)


def _ensure_strategies_loaded() -> None:
    """确保所有内置策略已注册（延迟导入避免循环依赖）."""
    if not STRATEGY_REGISTRY:
        from . import topk_dropout  # noqa: F401
        from . import topk_dropout_rc  # noqa: F401
