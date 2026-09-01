"""Live process bindings for the exact current-three v3 pure plugins."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_registry import PluginProcessBindingsV2

from .best_limit_plugin import BestLimitMiniQMTPluginV3
from .plugin_manifests import (
    current_three_manifests_v3,
    validate_current_three_config_v2,
    validate_current_three_state_v3,
)
from .sniper_plugin import SniperMiniQMTPluginV3
from .twap_lite_plugin import TwapLiteMiniQMTPluginV3


def _manifest(algo_code: str):
    matches = [item for item in current_three_manifests_v3() if item.algo_code == algo_code]
    if len(matches) != 1:
        raise ValueError("current-three manifest catalog must contain one exact algorithm entry")
    return matches[0]


def _config(algo_code: str, canonical_plugin_config: Mapping[str, Any]) -> tuple[Any, dict[str, Any]]:
    manifest = _manifest(algo_code)
    validated = validate_current_three_config_v2(manifest, canonical_plugin_config)
    return manifest, thaw_json_v1(validated)


def create_sniper_miniqmt_plugin_v3(canonical_plugin_config: Mapping[str, Any]) -> SniperMiniQMTPluginV3:
    manifest, config = _config("SNIPER_MINIQMT", canonical_plugin_config)
    return SniperMiniQMTPluginV3(manifest=manifest, canonical_config=config)


def create_best_limit_miniqmt_plugin_v3(canonical_plugin_config: Mapping[str, Any]) -> BestLimitMiniQMTPluginV3:
    manifest, config = _config("BEST_LIMIT_MINIQMT", canonical_plugin_config)
    return BestLimitMiniQMTPluginV3(manifest=manifest, canonical_config=config)


def create_twap_lite_miniqmt_plugin_v3(canonical_plugin_config: Mapping[str, Any]) -> TwapLiteMiniQMTPluginV3:
    manifest, config = _config("TWAP_LITE_MINIQMT", canonical_plugin_config)
    return TwapLiteMiniQMTPluginV3(manifest=manifest, canonical_config=config)


def current_three_process_bindings_v3() -> PluginProcessBindingsV2:
    bindings: dict[str, Any] = {
        "aistock.vnpy.sniper.factory": create_sniper_miniqmt_plugin_v3,
        "aistock.vnpy.best_limit.factory": create_best_limit_miniqmt_plugin_v3,
        "aistock.vnpy.twap_lite.factory": create_twap_lite_miniqmt_plugin_v3,
    }
    for manifest in current_three_manifests_v3():
        bindings[f"{manifest.plugin_id}.config_validator"] = validate_current_three_config_v2
        bindings[f"{manifest.plugin_id}.state_codec"] = validate_current_three_state_v3
    return PluginProcessBindingsV2(bindings)


__all__ = [
    "create_best_limit_miniqmt_plugin_v3",
    "create_sniper_miniqmt_plugin_v3",
    "create_twap_lite_miniqmt_plugin_v3",
    "current_three_process_bindings_v3",
]
