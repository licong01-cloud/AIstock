"""执行算法注册表 — 装饰器模式自动注册."""
from __future__ import annotations

from typing import Any, Dict, Type

from .base_algo import BaseExecutionAlgo

ALGO_REGISTRY: Dict[str, Type[BaseExecutionAlgo]] = {}


def register(cls: Type[BaseExecutionAlgo]) -> Type[BaseExecutionAlgo]:
    """@register 装饰器 — 将算法类注册到全局字典."""
    code = getattr(cls, "ALGO_CODE", None)
    if code is None:
        raise ValueError(f"{cls.__name__} 缺少 ALGO_CODE 类属性")
    ALGO_REGISTRY[code] = cls
    return cls


def get_algo(algo_code: str, config: Dict[str, Any] | None = None) -> BaseExecutionAlgo:
    """根据算法代码获取实例."""
    cls = ALGO_REGISTRY.get(algo_code)
    if cls is None:
        raise ValueError(f"未注册的执行算法: {algo_code}，可用: {list(ALGO_REGISTRY.keys())}")
    return cls(config=config)
