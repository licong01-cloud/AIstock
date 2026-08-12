"""Source-pinned Iceberg/Stop V4 manifests without durable market carriers."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

from backend.execution_algos.vnpy_style.hot_plugin_manifests import hot_active_order_schema_v4
from backend.services.miniqmt_execution_runtime.plugin_canonical import freeze_json_v1, hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    EventTypeV2,
    ExecutionAlgoPluginManifestV2,
    FileHashV1,
    GatewayCapabilityCatalogV1,
    SourceAttributionV1,
    validate_json_schema_instance_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    PluginCatalogRuntimeV2,
    PluginCreationBindingV1,
    PluginRegistrationDescriptorV2,
    callable_signature_sha256_v1,
)
from .facade_characterization import (
    VnpyFacadeCharacterizationAuthorityV2,
    _build_vnpy_facade_conformance_set_v2,
    _validate_vnpy_facade_conformance_set_against_authority_v2,
)
from .facade_contracts import (
    VnpyFacadeAlgorithmBindingV2,
    VnpyFacadeConformanceAuthorityV2,
    VnpyFacadeConformanceSetV2,
    VnpyFacadeContractV1,
    VnpyFacadeSourceManifestV1,
)

from .k5_plugin_manifests import k5_manifests_v1, validate_iceberg_config_v1, validate_stop_config_v1

_ROOT = Path(__file__).resolve().parents[3]
_FACTS = {
    "ICEBERG": (
        "backend.execution_algos.vnpy_compat.hot_facade_adapter:create_iceberg_hot_plugin_v4",
        "iceberg_hot_state_v4",
        "vnpy_algotrading/algos/iceberg_algo.py",
    ),
    "STOP": (
        "backend.execution_algos.vnpy_compat.hot_facade_adapter:create_stop_hot_plugin_v4",
        "stop_hot_state_v4",
        "vnpy_algotrading/algos/stop_algo.py",
    ),
}
_FULL_FIVE_ALGO_CODES = ("BEST_LIMIT_MINIQMT", "ICEBERG", "SNIPER_MINIQMT", "STOP", "TWAP_LITE_MINIQMT")


def build_hot_product_conformance_set_v4(
    *,
    catalog_runtime: PluginCatalogRuntimeV2,
    gateway_catalog: GatewayCapabilityCatalogV1,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    characterization_authority_v2: VnpyFacadeCharacterizationAuthorityV2,
    algorithm_bindings_v2: tuple[VnpyFacadeAlgorithmBindingV2, ...],
) -> VnpyFacadeConformanceSetV2:
    """Build the exact all-pure V4 product conformance set."""

    return _build_vnpy_facade_conformance_set_v2(
        catalog_runtime=catalog_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization_authority_v2,
        algorithm_bindings_v2=algorithm_bindings_v2,
        expected_algo_codes=_FULL_FIVE_ALGO_CODES,
        facade_backed_algo_codes=frozenset(),
    )


def validate_hot_product_conformance_set_against_authority_v4(
    *,
    conformance_set: VnpyFacadeConformanceSetV2,
    catalog_runtime: PluginCatalogRuntimeV2,
    gateway_catalog: GatewayCapabilityCatalogV1,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    characterization_authority_v2: VnpyFacadeCharacterizationAuthorityV2,
) -> VnpyFacadeConformanceAuthorityV2:
    """Rebuild and read back the exact all-pure V4 product authority."""

    return _validate_vnpy_facade_conformance_set_against_authority_v2(
        conformance_set=conformance_set,
        catalog_runtime=catalog_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization_authority_v2,
        expected_algo_codes=_FULL_FIVE_ALGO_CODES,
        facade_backed_algo_codes=frozenset(),
    )


def _object(properties: dict[str, Any], required: tuple[str, ...]) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": list(required),
    }


def _state_schema(algo_code: str) -> dict[str, Any]:
    common = {
        "algo_name": {"type": "string", "minLength": 1},
        "algo_code": {"const": algo_code},
        "parent_intent_id": {"type": "string", "minLength": 1},
        "symbol": {"type": "string", "pattern": "^[0-9]{6}\\.(?:SH|SZ|BJ)$"},
        "side": {"enum": ["BUY", "SELL"]},
        "offset": {"const": "NONE"},
        "limit_price_decimal": {"type": "string"},
        "parent_quantity": {"type": "integer", "minimum": 1},
        "min_volume": {"type": "integer", "minimum": 1},
        "volume_increment": {"type": "integer", "minimum": 1},
        "status": {"enum": ["PAUSED", "RUNNING", "STOPPED", "FINISHED"]},
        "traded_quantity": {"type": "integer", "minimum": 0},
        "traded_price_decimal": {"type": "string"},
        "active_orders": {"type": "array", "items": hot_active_order_schema_v4()},
        "finished_reason": {"type": ["string", "null"]},
    }
    if algo_code == "ICEBERG":
        extra = {
            "display_volume": {"type": "integer", "minimum": 1},
            "interval": {"type": "integer", "minimum": 1},
            "timer_count": {"type": "integer", "minimum": 0},
            "slice_ready": {"type": "boolean"},
            "vt_orderid": {"type": ["string", "null"]},
            "parameters": _object(
                {"display_volume": {"type": "integer", "minimum": 1}, "interval": {"type": "integer", "minimum": 1}},
                ("display_volume", "interval"),
            ),
            "variables": _object(
                {
                    "timer_count": {"type": "integer", "minimum": 0},
                    "slice_ready": {"type": "boolean"},
                    "vt_orderid": {"type": ["string", "null"]},
                },
                ("timer_count", "slice_ready", "vt_orderid"),
            ),
        }
    else:
        extra = {
            "price_add_decimal": {"type": "string"},
            "triggered": {"type": "boolean"},
            "vt_orderid": {"type": ["string", "null"]},
            "order_status": {"type": ["string", "null"]},
            "parameters": _object({"price_add": {"type": "string"}}, ("price_add",)),
            "variables": _object(
                {
                    "triggered": {"type": "boolean"},
                    "vt_orderid": {"type": ["string", "null"]},
                    "order_status": {"type": ["string", "null"]},
                },
                ("triggered", "vt_orderid", "order_status"),
            ),
        }
    return _object({**common, **extra}, tuple((*common, *extra)))


def _file(path: str) -> FileHashV1:
    data = (_ROOT / path).read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return FileHashV1(path=path, sha256=hashlib.sha256(data).hexdigest())


def _manifest(base: ExecutionAlgoPluginManifestV2) -> ExecutionAlgoPluginManifestV2:
    ref, state_version, _upstream = _FACTS[base.algo_code]
    paths = (
        "backend/execution_algos/vnpy_compat/hot_facade_adapter.py",
        "backend/execution_algos/vnpy_compat/hot_facade_contracts.py",
        "backend/execution_algos/vnpy_style/hot_plugin_base.py",
        "backend/execution_algos/vnpy_style/plugin_base.py",
        "backend/execution_algos/hot_market_contracts.py",
        "backend/services/miniqmt_execution_runtime/plugin_contracts.py",
    )
    files = tuple(_file(path) for path in sorted(paths))
    source_plain = {
        "schema_version": "source_attribution_v1",
        "upstream_repo": base.source_attribution.upstream_repo,
        "upstream_commit": base.source_attribution.upstream_commit,
        "upstream_files": [item.canonical_payload_v1() for item in base.source_attribution.upstream_files],
        "upstream_license": base.source_attribution.upstream_license,
        "upstream_copyright": base.source_attribution.upstream_copyright,
        "aistock_asset_version": "2026.08.12-miniqmt-hot-facade-v4",
        "aistock_files": [item.canonical_payload_v1() for item in files],
        "derivation_summary": "pinned Iceberg/Stop behavior with process-local market view and durable economic effect only",
    }
    source = SourceAttributionV1(
        **{**source_plain, "upstream_files": base.source_attribution.upstream_files, "aistock_files": files},
        attribution_sha256=hash_hex_v1("miniqmt_source_attribution_v1", source_plain),
    )
    schema = _state_schema(base.algo_code)
    subscriptions = tuple(
        sorted(
            {
                *base.subscribed_event_types,
                EventTypeV2.COMMAND_OUTCOME,
                EventTypeV2.EOD,
                EventTypeV2.OPERATOR,
                EventTypeV2.RECONCILE,
                EventTypeV2.SESSION,
                EventTypeV2.TICK,
            },
            key=lambda item: item.value,
        )
    )
    canonical = base.canonical_payload_v1(exclude={"manifest_sha256", "behavior_contract_sha256"})
    canonical.update(
        plugin_version="4.0.0",
        implementation_ref=ref,
        state_schema_version=state_version,
        state_schema=schema,
        state_schema_sha256=hash_hex_v1("miniqmt_plugin_state_schema_v1", schema),
        subscribed_event_types=[item.value for item in subscriptions],
        source_attribution=source.canonical_payload_v1(),
        behavior_characterization_sha256=hash_hex_v1(
            "miniqmt_plugin_behavior_characterization_v4",
            {"algo_code": base.algo_code, "pinned_behavior": True, "market_plane": "PROCESS_LOCAL"},
        ),
    )
    keys = (
        "plugin_id",
        "algo_code",
        "plugin_version",
        "provider",
        "implementation_ref",
        "config_schema_version",
        "config_schema_sha256",
        "state_schema_version",
        "state_schema_sha256",
        "subscribed_event_types",
        "market_data_requirements",
        "required_facade_methods",
        "required_facade_object_fields",
        "supported_sides",
        "supported_order_types",
        "supported_broker_backends",
        "restart_policy",
        "source_attribution",
        "compatibility_requirement",
        "behavior_characterization_sha256",
    )
    behavior = hash_hex_v1("miniqmt_plugin_behavior_contract_v2", {key: canonical[key] for key in keys})
    canonical["behavior_contract_sha256"] = behavior
    manifest_hash = hash_hex_v1("execution_algo_plugin_manifest_v2", canonical)
    model = base.model_dump(mode="python", exclude={"manifest_sha256", "behavior_contract_sha256"})
    model.update(
        plugin_version="4.0.0",
        implementation_ref=ref,
        state_schema_version=state_version,
        state_schema=schema,
        state_schema_sha256=canonical["state_schema_sha256"],
        subscribed_event_types=subscriptions,
        source_attribution=source,
        behavior_characterization_sha256=canonical["behavior_characterization_sha256"],
        behavior_contract_sha256=behavior,
        manifest_sha256=manifest_hash,
    )
    return ExecutionAlgoPluginManifestV2.model_validate(model, strict=True)


def hot_facade_manifests_v4() -> tuple[ExecutionAlgoPluginManifestV2, ...]:
    return tuple(_manifest(item) for item in k5_manifests_v1())


def validate_hot_facade_config_v4(manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]):
    old = next(item for item in k5_manifests_v1() if item.algo_code == manifest.algo_code)
    return (validate_iceberg_config_v1 if manifest.algo_code == "ICEBERG" else validate_stop_config_v1)(old, value)


def validate_hot_facade_state_v4(manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]):
    frozen = freeze_json_v1(dict(value))
    validate_json_schema_instance_v1(
        schema=manifest.state_schema, instance=frozen, contract_name=f"{manifest.algo_code} hot facade state"
    )
    return frozen


def hot_facade_descriptors_v4() -> tuple[PluginRegistrationDescriptorV2, ...]:
    from . import hot_facade_adapter

    result = []
    for manifest in hot_facade_manifests_v4():
        factory = getattr(
            hot_facade_adapter,
            "create_iceberg_hot_plugin_v4" if manifest.algo_code == "ICEBERG" else "create_stop_hot_plugin_v4",
        )
        result.append(
            PluginRegistrationDescriptorV2(
                schema_version="plugin_registration_descriptor_v2",
                manifest=manifest,
                factory_binding_id=f"{manifest.plugin_id}.v4.factory",
                factory_callable_ref=_FACTS[manifest.algo_code][0],
                factory_signature_sha256=callable_signature_sha256_v1(factory),
                config_validator_binding_id=f"{manifest.plugin_id}.v4.config_validator",
                config_validator_callable_ref=f"{__name__}:validate_hot_facade_config_v4",
                config_validator_signature_sha256=callable_signature_sha256_v1(validate_hot_facade_config_v4),
                state_codec_binding_id=f"{manifest.plugin_id}.v4.state_codec",
                state_codec_callable_ref=f"{__name__}:validate_hot_facade_state_v4",
                state_codec_signature_sha256=callable_signature_sha256_v1(validate_hot_facade_state_v4),
            )
        )
    return tuple(result)


def hot_facade_creation_bindings_v4() -> tuple[PluginCreationBindingV1, ...]:
    return tuple(
        PluginCreationBindingV1(algo_code=item.manifest.algo_code, plugin_key=item.plugin_key)
        for item in hot_facade_descriptors_v4()
    )


__all__ = [
    "build_hot_product_conformance_set_v4",
    "hot_facade_creation_bindings_v4",
    "hot_facade_descriptors_v4",
    "hot_facade_manifests_v4",
    "validate_hot_facade_config_v4",
    "validate_hot_facade_state_v4",
    "validate_hot_product_conformance_set_against_authority_v4",
]
