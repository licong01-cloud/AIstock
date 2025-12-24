from __future__ import annotations

"""SmartMonitorEngine wrapper for AI盯盘模块.

本模块提供一个与旧接口兼容的统一入口：
- analyze_stock
- start_monitor
- stop_monitor

当前后端不再依赖任何旧目录代码，统一回退到安全的占位实现，
避免影响 FastAPI 后端以及其他功能模块。
"""

from typing import Any, Dict


class _StubEngine:
    """安全占位版智能盯盘引擎.

    - 始终返回明确错误提示，不做任何数据库 / 外部请求
    - start_monitor / stop_monitor 为 no-op
    """

    def analyze_stock(self, stock_code: str, **_: Any) -> Dict[str, Any]:
        return {
            "success": False,
            "error": "AI盯盘引擎依赖尚未完全迁移或加载失败，请稍后再试",
            "stock_code": stock_code,
        }

    def start_monitor(self, *args: Any, **kwargs: Any) -> None:  # noqa: ARG002
        return None

    def stop_monitor(self, *args: Any, **kwargs: Any) -> None:  # noqa: ARG002
        return None


class SmartMonitorEngine:
    """包装旧 SmartMonitorEngine 的统一入口.

    - 如果 legacy 引擎加载成功，则内部委托给它
    - 否则使用 _StubEngine，保证后端可正常启动
    """

    def __init__(self) -> None:
        self._impl: Any = _StubEngine()

    def _ensure_impl(self) -> Any:
        return self._impl

    def analyze_stock(self, stock_code: str, **kwargs: Any) -> Dict[str, Any]:
        return self._ensure_impl().analyze_stock(stock_code, **kwargs)

    def start_monitor(self, *args: Any, **kwargs: Any) -> None:
        return self._ensure_impl().start_monitor(*args, **kwargs)

    def stop_monitor(self, *args: Any, **kwargs: Any) -> None:
        return self._ensure_impl().stop_monitor(*args, **kwargs)


engine = SmartMonitorEngine()
