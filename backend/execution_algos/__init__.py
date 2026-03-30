"""执行算法模块 — 自动注册所有算法，导出 ALGO_REGISTRY 和 get_algo."""
from .registry import ALGO_REGISTRY, get_algo  # noqa: F401

# 导入触发 @register 装饰器注册
from . import close_price_algo  # noqa: F401
from . import twap_algo  # noqa: F401
from . import vwap_algo  # noqa: F401
from . import sbb_ema_algo  # noqa: F401
from . import ac_optimal_algo  # noqa: F401
from . import pov_algo  # noqa: F401
