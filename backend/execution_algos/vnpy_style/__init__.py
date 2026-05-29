"""vn.py-style execution strategy assets for AIstock.

This package ports selected vnpy_algotrading algorithm semantics into an
adapter-independent core. It intentionally does not import vn.py runtime,
FastAPI, DB repositories, MiniQMT broker adapters, or xtquant.
"""

from .attribution import AISTOCK_ASSET_VERSION, SOURCE_FILE_MAP, source_attribution
from .base import VnpyAlgoTemplate, VnpyStyleConfigError
from .best_limit_core import BestLimitMiniQMTCore
from .models import (
    VnpyAction,
    VnpyActionType,
    VnpyAlgoConfig,
    VnpyAlgoSnapshot,
    VnpyAlgoStatus,
    VnpyDirection,
    VnpyOrderUpdate,
    VnpyTick,
    VnpyTradeUpdate,
)
from .registry import (
    VNPY_STYLE_ASSETS,
    VnpyStyleAssetSpec,
    create_vnpy_style_core,
    get_vnpy_style_asset,
    is_vnpy_style_algo,
    validate_vnpy_style_config,
)
from .sniper_core import SniperMiniQMTCore
from .twap_lite_core import TwapLiteMiniQMTCore

__all__ = [
    "AISTOCK_ASSET_VERSION",
    "SOURCE_FILE_MAP",
    "source_attribution",
    "VNPY_STYLE_ASSETS",
    "VnpyStyleAssetSpec",
    "VnpyAlgoTemplate",
    "VnpyStyleConfigError",
    "SniperMiniQMTCore",
    "BestLimitMiniQMTCore",
    "TwapLiteMiniQMTCore",
    "create_vnpy_style_core",
    "get_vnpy_style_asset",
    "is_vnpy_style_algo",
    "validate_vnpy_style_config",
    "VnpyAction",
    "VnpyActionType",
    "VnpyAlgoConfig",
    "VnpyAlgoSnapshot",
    "VnpyAlgoStatus",
    "VnpyDirection",
    "VnpyOrderUpdate",
    "VnpyTick",
    "VnpyTradeUpdate",
]
