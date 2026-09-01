from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_registry import PluginProcessBindingsV2

from .hot_best_limit_plugin import BestLimitMiniQMTPluginV4
from .hot_plugin_manifests import (
    current_three_hot_manifests_v4,
    validate_current_three_hot_config_v4,
    validate_current_three_hot_state_v4,
)
from .hot_sniper_plugin import SniperMiniQMTPluginV4
from .hot_twap_lite_plugin import TwapLiteMiniQMTPluginV4


def _create(algo_code: str, config: Mapping[str, Any]):
    matches = [item for item in current_three_hot_manifests_v4() if item.algo_code == algo_code]
    if len(matches) != 1:
        raise ValueError("hot current-three catalog requires one exact manifest")
    manifest = matches[0]
    canonical = thaw_json_v1(validate_current_three_hot_config_v4(manifest, config))
    cls = {
        "SNIPER_MINIQMT": SniperMiniQMTPluginV4,
        "BEST_LIMIT_MINIQMT": BestLimitMiniQMTPluginV4,
        "TWAP_LITE_MINIQMT": TwapLiteMiniQMTPluginV4,
    }[algo_code]
    return cls(manifest=manifest, canonical_config=canonical)


def create_sniper_miniqmt_plugin_v4(canonical_plugin_config: Mapping[str, Any]) -> SniperMiniQMTPluginV4:
    return _create("SNIPER_MINIQMT", canonical_plugin_config)


def create_best_limit_miniqmt_plugin_v4(canonical_plugin_config: Mapping[str, Any]) -> BestLimitMiniQMTPluginV4:
    return _create("BEST_LIMIT_MINIQMT", canonical_plugin_config)


def create_twap_lite_miniqmt_plugin_v4(canonical_plugin_config: Mapping[str, Any]) -> TwapLiteMiniQMTPluginV4:
    return _create("TWAP_LITE_MINIQMT", canonical_plugin_config)


def current_three_hot_process_bindings_v4() -> PluginProcessBindingsV2:
    bindings = {
        "aistock.vnpy.sniper.v4.factory": create_sniper_miniqmt_plugin_v4,
        "aistock.vnpy.best_limit.v4.factory": create_best_limit_miniqmt_plugin_v4,
        "aistock.vnpy.twap_lite.v4.factory": create_twap_lite_miniqmt_plugin_v4,
    }
    for manifest in current_three_hot_manifests_v4():
        bindings[f"{manifest.plugin_id}.v4.config_validator"] = validate_current_three_hot_config_v4
        bindings[f"{manifest.plugin_id}.v4.state_codec"] = validate_current_three_hot_state_v4
    return PluginProcessBindingsV2(bindings)


__all__ = [
    "create_best_limit_miniqmt_plugin_v4",
    "create_sniper_miniqmt_plugin_v4",
    "create_twap_lite_miniqmt_plugin_v4",
    "current_three_hot_process_bindings_v4",
]
