"""Process-local K5 Iceberg/Stop adapter factories.

Factories own no durable state and never publish a catalog.  The K5 shadow
composition root performs fresh K4-authority equality before accepting their
descriptors or bindings.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    MiniQMTPluginContractError,
    MiniQMTPluginReasonCode,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import PluginProcessBindingsV2

from .facade_adapter import VnpyFacadeBackedPluginAdapterV1
from .facade_characterization import (
    build_vnpy_facade_state_mappings_v1,
    build_vnpy_facade_terminal_mappings_v1,
    load_pinned_vnpy_algorithm_classes_v1,
)
from .k5_binding_authority import k5_binding_for_algo_v2
from .k5_plugin_manifests import (
    k5_manifests_v1,
    validate_iceberg_config_v1,
    validate_iceberg_state_v1,
    validate_stop_config_v1,
    validate_stop_state_v1,
)


def _manifest(algo_code: str):
    matches = tuple(item for item in k5_manifests_v1() if item.algo_code == algo_code)
    if len(matches) != 1:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.BINDING_INVALID,
            "K5 factory requires exactly one matching manifest",
            context={"algo_code": algo_code, "match_count": len(matches)},
        )
    return matches[0]


def _stop_source_setting_v1(canonical_config: Mapping[str, Any]) -> dict[str, Any]:
    """Bridge Stop's canonical decimal string into its pinned float setting.

    This is a representation boundary only: no default, rounding, clipping,
    or business decision is introduced.  The canonical durable config remains
    the source of truth and is revalidated for every initialization attempt.
    """

    config = thaw_json_v1(validate_stop_config_v1(_manifest("STOP"), canonical_config))
    return {"price_add": float(Decimal(config["price_add"]))}


def _iceberg_source_setting_v1(canonical_config: Mapping[str, Any]) -> dict[str, Any]:
    """Bridge the exact durable numeric carrier into pinned float state."""

    config = thaw_json_v1(validate_iceberg_config_v1(_manifest("ICEBERG"), canonical_config))
    display_volume = config["display_volume"]
    return {
        "display_volume": float(Decimal(str(display_volume))),
        "interval": config["interval"],
    }


def _create(algo_code: str, canonical_plugin_config: Mapping[str, Any]) -> VnpyFacadeBackedPluginAdapterV1:
    manifest = _manifest(algo_code)
    validator = validate_iceberg_config_v1 if algo_code == "ICEBERG" else validate_stop_config_v1
    # The factory SPI receives the canonical config only.  Validation is a
    # mandatory readback; the resulting object is not retained by the factory.
    validated = validator(manifest, canonical_plugin_config)
    if thaw_json_v1(validated) != dict(canonical_plugin_config):
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.BINDING_INVALID,
            "K5 factory config validator did not preserve its canonical payload",
            context={"algo_code": algo_code},
        )
    binding = k5_binding_for_algo_v2(algo_code)
    classes = load_pinned_vnpy_algorithm_classes_v1()
    algorithm_class = classes.get(algo_code)
    if algorithm_class is None:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.BINDING_INVALID,
            "K5 pinned class loader did not return the requested algorithm",
            context={"algo_code": algo_code, "available_algo_codes": tuple(sorted(classes))},
        )
    state_mappings = tuple(item for item in build_vnpy_facade_state_mappings_v1() if item.algo_code == algo_code)
    terminal_mappings = tuple(item for item in build_vnpy_facade_terminal_mappings_v1() if item.algo_code == algo_code)
    adapter = VnpyFacadeBackedPluginAdapterV1(
        manifest=manifest,
        algorithm_class=algorithm_class,
        algorithm_binding=binding,
        state_mappings=state_mappings,
        terminal_mappings=terminal_mappings,
        source_setting_builder=(_iceberg_source_setting_v1 if algo_code == "ICEBERG" else _stop_source_setting_v1),
    )
    if adapter.manifest != manifest:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.BINDING_INVALID,
            "K5 facade adapter manifest readback drifted",
            context={
                "algo_code": algo_code,
                "expected_manifest_sha256": manifest.manifest_sha256,
                "actual_manifest_sha256": adapter.manifest.manifest_sha256,
            },
        )
    return adapter


def create_iceberg_plugin_v1(canonical_plugin_config: Mapping[str, Any]) -> VnpyFacadeBackedPluginAdapterV1:
    return _create("ICEBERG", canonical_plugin_config)


def create_stop_plugin_v1(canonical_plugin_config: Mapping[str, Any]) -> VnpyFacadeBackedPluginAdapterV1:
    return _create("STOP", canonical_plugin_config)


def k5_process_bindings_v1() -> PluginProcessBindingsV2:
    return PluginProcessBindingsV2(
        {
            "aistock.vnpy.iceberg.factory": create_iceberg_plugin_v1,
            "aistock.vnpy.iceberg.config_validator": validate_iceberg_config_v1,
            "aistock.vnpy.iceberg.state_codec": validate_iceberg_state_v1,
            "aistock.vnpy.stop.factory": create_stop_plugin_v1,
            "aistock.vnpy.stop.config_validator": validate_stop_config_v1,
            "aistock.vnpy.stop.state_codec": validate_stop_state_v1,
        }
    )


__all__ = ["create_iceberg_plugin_v1", "create_stop_plugin_v1", "k5_process_bindings_v1"]
