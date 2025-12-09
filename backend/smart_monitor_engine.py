from __future__ import annotations

"""SmartMonitorEngine wrapper for AI盯盘模块.

只在本模块内部动态加载旧项目 aiagents-stock-main 下的
``smart_monitor_engine.SmartMonitorEngine``，并对外提供统一接口：
- analyze_stock
- start_monitor
- stop_monitor

如果旧引擎或其依赖无法正确加载，则回退到安全的占位实现，
避免影响整个 FastAPI 后端以及其他功能模块。
"""

from typing import Any, Dict, Optional
import importlib
import os
import sys
from pathlib import Path


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


def _load_legacy_engine() -> Optional[Any]:
    """尝试从 aiagents-stock-main 动态加载原始 SmartMonitorEngine.

    仅在本模块内部修改 sys.path，其他模块无需感知。
    加载失败时返回 None，由调用方决定是否回退到 stub。
    """

    try:
        backend_dir = Path(__file__).resolve().parent
        project_root = backend_dir.parent
        legacy_dir = project_root / "aiagents-stock-main"

        if legacy_dir.is_dir():
            legacy_path = str(legacy_dir)
            if legacy_path not in sys.path:
                # 只追加路径，不移除原有路径，避免影响其他模块
                sys.path.insert(0, legacy_path)

        # 旧项目中的模块名就是 smart_monitor_engine
        legacy_mod = importlib.import_module("smart_monitor_engine")
        legacy_cls = getattr(legacy_mod, "SmartMonitorEngine", None)
        if legacy_cls is None:
            return None
        return legacy_cls
    except Exception:
        # 任何导入错误都静默处理，统一由上层回退到 stub
        return None


class SmartMonitorEngine:
    """包装旧 SmartMonitorEngine 的统一入口.

    - 如果 legacy 引擎加载成功，则内部委托给它
    - 否则使用 _StubEngine，保证后端可正常启动
    """

    def __init__(self) -> None:
        legacy_cls = _load_legacy_engine()
        if legacy_cls is not None:
            try:
                # 不传任何参数，沿用旧引擎的默认配置读取逻辑
                self._impl = legacy_cls()
            except Exception:
                self._impl = _StubEngine()
        else:
            self._impl = _StubEngine()

    def analyze_stock(self, stock_code: str, **kwargs: Any) -> Dict[str, Any]:
        return self._impl.analyze_stock(stock_code, **kwargs)

    def start_monitor(self, *args: Any, **kwargs: Any) -> None:
        return self._impl.start_monitor(*args, **kwargs)

    def stop_monitor(self, *args: Any, **kwargs: Any) -> None:
        return self._impl.stop_monitor(*args, **kwargs)


engine = SmartMonitorEngine()
