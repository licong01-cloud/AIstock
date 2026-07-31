"""K5 shadow-only manifests and strict codecs for Iceberg and Stop.

K1 owns the shared vn.py source/interface compatibility surface; K4 owns the
pinned Iceberg/Stop source characterization.  This module joins those two
authorities without introducing a second DTO, product registration, broker
route, or execution fallback.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from backend.execution_algos.vnpy_style.attribution import (
    UPSTREAM_COMMIT,
    UPSTREAM_COPYRIGHT,
    UPSTREAM_LICENSE,
    UPSTREAM_REPO,
)
from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v3
from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    freeze_json_v1,
    hash_hex_v1,
    json_safe_evidence_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AbsenceDispositionV1,
    EventTypeV2,
    ExecutionAlgoPluginManifestV2,
    FileHashV1,
    MarketDataCapabilityV1,
    MarketDataRequirementV1,
    MiniQMTPluginContractError,
    MiniQMTPluginReasonCode,
    OrderTypeV1,
    PluginProviderV2,
    SessionPhaseV1,
    SideV1,
    SourceAttributionV1,
    VnpyCompatibilityRequirementV2,
    validate_json_schema_instance_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    PluginCreationBindingV1,
    PluginRegistrationDescriptorV2,
    callable_signature_sha256_v1,
)

from .facade_contracts import VnpyFacadeStateEnvelopeV1
from .k5_binding_authority import k5_binding_for_algo_v2

_REPO_ROOT = Path(__file__).resolve().parents[3]
_K5_ALGOS = ("ICEBERG", "STOP")
_SIDES = (SideV1.BUY, SideV1.SELL)
_CONTINUOUS_PHASES = (SessionPhaseV1.CONTINUOUS_AM, SessionPhaseV1.CONTINUOUS_PM)
_FACTORY_SIGNATURE_SHA256 = "1b92d686d7a1f9d7d0e99b495e7da99fd4555946e505959b2a7b15b394e91d65"
_VALIDATOR_SIGNATURE_SHA256 = "487a83b73a7d94a2d0ee6e43fb00b0337ab928fdf7bddcb54098db8c626daa58"
_UPSTREAM_HASHES = {
    "vnpy_algotrading/algos/iceberg_algo.py": "9019cd20740f706f70f8db6ee9d051405de0d63849f1acfae894fa1b796a2c21",
    "vnpy_algotrading/algos/stop_algo.py": "18a758b24491f1cedd391e0f5378013ece8e1117ec14c1089046db344e8db090",
    "vnpy_algotrading/base.py": "8416653d8cf61ab45e26b593eea06417dd6fa21b331bba6c60a2bbb8bccf8f93",
    "vnpy_algotrading/engine.py": "2c73e1c093cabcd5768954f1129451877a82afd204790fb07e4f305b64c5e68d",
    "vnpy_algotrading/template.py": "b21fa36a8a2c347ab92379df1cd9f81ec69bc922233ec4096d75dbbade7454b8",
}
_FACTS = {
    "ICEBERG": {
        "plugin_id": "aistock.vnpy.iceberg",
        "implementation_ref": "backend.execution_algos.vnpy_compat.k5_plugin_factories:create_iceberg_plugin_v1",
        "factory_name": "create_iceberg_plugin_v1",
        "config_validator_ref": "backend.execution_algos.vnpy_compat.k5_plugin_manifests:validate_iceberg_config_v1",
        "state_validator_ref": "backend.execution_algos.vnpy_compat.k5_plugin_manifests:validate_iceberg_state_v1",
        "config_schema_version": "iceberg_facade_config_v1",
        "state_schema_version": "iceberg_facade_state_v1",
        "upstream_path": "vnpy_algotrading/algos/iceberg_algo.py",
        "methods": (
            "cancel_order",
            "get_tick",
            "put_algo_event",
            "send_order",
            "update_order",
            "update_timer",
            "update_trade",
            "write_log",
        ),
        "events": (EventTypeV2.ALGO_START, EventTypeV2.ORDER, EventTypeV2.TIMER, EventTypeV2.TRADE),
    },
    "STOP": {
        "plugin_id": "aistock.vnpy.stop",
        "implementation_ref": "backend.execution_algos.vnpy_compat.k5_plugin_factories:create_stop_plugin_v1",
        "factory_name": "create_stop_plugin_v1",
        "config_validator_ref": "backend.execution_algos.vnpy_compat.k5_plugin_manifests:validate_stop_config_v1",
        "state_validator_ref": "backend.execution_algos.vnpy_compat.k5_plugin_manifests:validate_stop_state_v1",
        "config_schema_version": "stop_facade_config_v1",
        "state_schema_version": "stop_facade_state_v1",
        "upstream_path": "vnpy_algotrading/algos/stop_algo.py",
        "methods": ("put_algo_event", "send_order", "update_order", "update_tick", "update_trade", "write_log"),
        "events": (EventTypeV2.ALGO_START, EventTypeV2.ORDER, EventTypeV2.TICK, EventTypeV2.TRADE),
    },
}


def _canonical_lf_sha256(path: str) -> FileHashV1:
    payload = (_REPO_ROOT / path).read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return FileHashV1(path=path, sha256=hashlib.sha256(payload).hexdigest())


def _source_attribution(algo_code: str) -> SourceAttributionV1:
    facts = _FACTS[algo_code]
    upstream_files = tuple(
        FileHashV1(path=path, sha256=_UPSTREAM_HASHES[path])
        for path in sorted(
            (
                facts["upstream_path"],
                "vnpy_algotrading/base.py",
                "vnpy_algotrading/engine.py",
                "vnpy_algotrading/template.py",
            )
        )
    )
    aistock_paths = (
        "backend/execution_algos/vnpy_compat/facade_adapter.py",
        "backend/execution_algos/vnpy_compat/facade_characterization.py",
        "backend/execution_algos/vnpy_compat/facade_contracts.py",
        "backend/execution_algos/vnpy_compat/k5_binding_authority.py",
        "backend/execution_algos/vnpy_compat/k5_plugin_factories.py",
        "backend/execution_algos/vnpy_compat/k5_plugin_manifests.py",
        "backend/services/miniqmt_execution_runtime/k5_shadow_catalog.py",
        "backend/services/miniqmt_execution_runtime/kernel_delivery.py",
        "backend/services/miniqmt_execution_runtime/plugin_contracts.py",
        "backend/services/miniqmt_execution_runtime/plugin_registry.py",
    )
    aistock_files = tuple(_canonical_lf_sha256(path) for path in aistock_paths)
    plain = {
        "schema_version": "source_attribution_v1",
        "upstream_repo": UPSTREAM_REPO,
        "upstream_commit": UPSTREAM_COMMIT,
        "upstream_files": [item.canonical_payload_v1() for item in upstream_files],
        "upstream_license": UPSTREAM_LICENSE,
        "upstream_copyright": UPSTREAM_COPYRIGHT,
        "aistock_asset_version": "2026.07.31-miniqmt-k5-facade-adapter-v1",
        "aistock_files": [item.canonical_payload_v1() for item in aistock_files],
        "derivation_summary": "K5 shadow-only Iceberg/Stop adapter bridge; K1 compatibility, K4 source characterization and K2 kernel ownership remain separate",
    }
    return SourceAttributionV1(
        **{**plain, "upstream_files": upstream_files, "aistock_files": aistock_files},
        attribution_sha256=hash_hex_v1("miniqmt_source_attribution_v1", plain),
    )


def _config_schema(algo_code: str) -> dict[str, Any]:
    if algo_code == "ICEBERG":
        return {
            "type": "object",
            "additionalProperties": False,
            "required": ["display_volume", "interval"],
            "properties": {
                "display_volume": {
                    "oneOf": [
                        {"type": "integer", "minimum": 0},
                        {
                            "type": "string",
                            "pattern": r"^(?:0|[1-9][0-9]*)\.[0-9]*[1-9]$",
                        },
                    ]
                },
                "interval": {"type": "integer", "minimum": 0},
            },
        }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["price_add"],
        "properties": {"price_add": {"type": "string", "pattern": r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$"}},
    }


def _state_schema() -> dict[str, Any]:
    """Return the real existing K4 envelope schema, not a parallel K5 DTO."""

    return VnpyFacadeStateEnvelopeV1.model_json_schema(mode="validation")


def _canonical_signed_decimal(value: Any, *, field_name: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty trim-stable decimal string")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal string") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    normalized = format(parsed, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    normalized = "0" if normalized in ("", "-0") else normalized
    if value != normalized:
        raise ValueError(f"{field_name} must be canonical without redundant zeros")
    return normalized


def _canonical_nonnegative_source_number(value: Any, *, field_name: str) -> int | str:
    """Keep integers exact and fractions as one canonical decimal string.

    K1 durable JSON deliberately rejects binary floats.  The pinned Iceberg
    source still accepts fractional display volume, so a fractional value uses
    a canonical decimal string until the process-local source-setting bridge.
    Integral values have exactly one carrier: strict ``int``.
    """

    if type(value) is int:
        return value
    return _canonical_signed_decimal(value, field_name=field_name)


def _validate_schema(schema: Mapping[str, Any], value: Mapping[str, Any], *, contract_name: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.CONFIG_SCHEMA_INVALID
            if "config" in contract_name
            else MiniQMTPluginReasonCode.STATE_SCHEMA_INVALID,
            f"{contract_name} must be a strict object",
            context={"contract_name": contract_name, "actual_type": type(value).__name__},
        )
    try:
        frozen = freeze_json_v1(value)
    except (TypeError, ValueError) as exc:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.CONFIG_SCHEMA_INVALID
            if "config" in contract_name
            else MiniQMTPluginReasonCode.STATE_SCHEMA_INVALID,
            f"{contract_name} contains a non-canonical JSON carrier",
            context={"contract_name": contract_name, "error": json_safe_evidence_v1(exc)},
        ) from exc
    validate_json_schema_instance_v1(schema=schema, instance=frozen, contract_name=contract_name)
    return dict(value)


def _compatibility_requirement(characterization_sha256: str) -> VnpyCompatibilityRequirementV2:
    """Reuse K1's exact source/interface lock; K4 owns the extra source files."""

    base = current_three_manifests_v3()[0].compatibility_requirement
    plain = base.canonical_payload_v1(exclude={"characterization_sha256", "requirement_sha256"})
    plain["characterization_sha256"] = characterization_sha256
    return VnpyCompatibilityRequirementV2(
        schema_version=base.schema_version,
        mode=base.mode,
        upstream_sources=base.upstream_sources,
        source_files_and_hashes=base.source_files_and_hashes,
        required_method_signatures=base.required_method_signatures,
        required_object_fields=base.required_object_fields,
        required_enum_values=base.required_enum_values,
        characterization_sha256=characterization_sha256,
        requirement_sha256=hash_hex_v1("miniqmt_vnpy_compatibility_requirement_v2", plain),
    )


def _market_requirements(algo_code: str) -> tuple[MarketDataRequirementV1, ...]:
    wait = AbsenceDispositionV1.WAIT_FOR_NEXT_VALID_EVENT
    if algo_code == "ICEBERG":
        values = (
            MarketDataRequirementV1.create(
                capability=MarketDataCapabilityV1.L1_ASK,
                required_fields=("price",),
                applicable_sides=(SideV1.BUY,),
                event_types=(EventTypeV2.TIMER,),
                session_phases=_CONTINUOUS_PHASES,
                absence_disposition=wait,
            ),
            MarketDataRequirementV1.create(
                capability=MarketDataCapabilityV1.L1_BID,
                required_fields=("price",),
                applicable_sides=(SideV1.SELL,),
                event_types=(EventTypeV2.TIMER,),
                session_phases=_CONTINUOUS_PHASES,
                absence_disposition=wait,
            ),
        )
    else:
        values = (
            MarketDataRequirementV1.create(
                capability=MarketDataCapabilityV1.LAST_PRICE,
                required_fields=("price",),
                applicable_sides=_SIDES,
                event_types=(EventTypeV2.TICK,),
                session_phases=_CONTINUOUS_PHASES,
                absence_disposition=wait,
            ),
            MarketDataRequirementV1.create(
                capability=MarketDataCapabilityV1.LIMIT_UP_DOWN,
                required_fields=("limit_down", "limit_up"),
                applicable_sides=_SIDES,
                event_types=(EventTypeV2.TICK,),
                session_phases=_CONTINUOUS_PHASES,
                absence_disposition=wait,
            ),
        )
    return tuple(sorted(values, key=lambda item: item.requirement_sha256))


def _manifest(algo_code: str) -> ExecutionAlgoPluginManifestV2:
    facts = _FACTS[algo_code]
    binding = k5_binding_for_algo_v2(algo_code)
    config_schema = _config_schema(algo_code)
    state_schema = _state_schema()
    source = _source_attribution(algo_code)
    compatibility = _compatibility_requirement(binding.characterization_receipt_sha256)
    requirements = _market_requirements(algo_code)
    plain: dict[str, Any] = {
        "schema_version": "execution_algo_plugin_manifest_v2",
        "plugin_id": facts["plugin_id"],
        "algo_code": algo_code,
        "plugin_version": "1.0.0",
        "provider": PluginProviderV2.VNPY_COMPAT.value,
        "implementation_ref": facts["implementation_ref"],
        "config_schema_version": facts["config_schema_version"],
        "config_schema": config_schema,
        "config_schema_sha256": hash_hex_v1("miniqmt_plugin_config_schema_v1", config_schema),
        "state_schema_version": facts["state_schema_version"],
        "state_schema": state_schema,
        "state_schema_sha256": hash_hex_v1("miniqmt_plugin_state_schema_v1", state_schema),
        "subscribed_event_types": [item.value for item in facts["events"]],
        "market_data_requirements": [item.canonical_payload_v1() for item in requirements],
        "required_facade_methods": list(facts["methods"]),
        "required_facade_object_fields": [item.canonical_payload_v1() for item in compatibility.required_object_fields],
        "supported_sides": [item.value for item in _SIDES],
        "supported_order_types": [OrderTypeV1.LIMIT.value],
        "supported_broker_backends": ["minqmt_sim"],
        "restart_policy": "DURABLE_RESTORE",
        "source_attribution": source.canonical_payload_v1(),
        "compatibility_requirement": compatibility.canonical_payload_v1(),
        "behavior_characterization_sha256": binding.characterization_receipt_sha256,
    }
    behavior_keys = (
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
    plain["behavior_contract_sha256"] = hash_hex_v1(
        "miniqmt_plugin_behavior_contract_v2", {key: plain[key] for key in behavior_keys}
    )
    plain["manifest_sha256"] = hash_hex_v1("execution_algo_plugin_manifest_v2", plain)
    return ExecutionAlgoPluginManifestV2(
        **{
            **plain,
            "provider": PluginProviderV2.VNPY_COMPAT,
            "subscribed_event_types": facts["events"],
            "market_data_requirements": requirements,
            "required_facade_methods": facts["methods"],
            "required_facade_object_fields": compatibility.required_object_fields,
            "supported_sides": _SIDES,
            "supported_order_types": (OrderTypeV1.LIMIT,),
            "supported_broker_backends": ("minqmt_sim",),
            "source_attribution": source,
            "compatibility_requirement": compatibility,
        }
    )


def k5_manifests_v1() -> tuple[ExecutionAlgoPluginManifestV2, ...]:
    """Build exactly the two K5 candidates; no runtime catalog is published."""

    return tuple(_manifest(algo_code) for algo_code in _K5_ALGOS)


def _manifest_for_algo(algo_code: str) -> ExecutionAlgoPluginManifestV2:
    matches = tuple(item for item in k5_manifests_v1() if item.algo_code == algo_code)
    if len(matches) != 1:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.MANIFEST_SCHEMA_INVALID,
            "K5 manifest builder must contain exactly one requested algorithm",
            context={"algo_code": algo_code, "match_count": len(matches)},
        )
    return matches[0]


def validate_iceberg_config_v1(manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]) -> Any:
    try:
        if manifest != _manifest_for_algo("ICEBERG"):
            raise ValueError("Iceberg config validator requires the exact K5 Iceberg manifest")
        config = _validate_schema(manifest.config_schema, value, contract_name="ICEBERG config")
        config["display_volume"] = _canonical_nonnegative_source_number(
            config["display_volume"], field_name="display_volume"
        )
        return freeze_json_v1(config)
    except MiniQMTPluginContractError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.CONFIG_SCHEMA_INVALID,
            "Iceberg config violates the exact K5 source contract",
            context={"algo_code": "ICEBERG", "error": json_safe_evidence_v1(exc)},
        ) from exc


def validate_stop_config_v1(manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]) -> Any:
    try:
        if manifest != _manifest_for_algo("STOP"):
            raise ValueError("Stop config validator requires the exact K5 Stop manifest")
        config = _validate_schema(manifest.config_schema, value, contract_name="STOP config")
        config["price_add"] = _canonical_signed_decimal(config["price_add"], field_name="price_add")
        if not math.isfinite(float(Decimal(config["price_add"]))):
            raise ValueError("price_add must remain finite in the exact pinned float source representation")
        return freeze_json_v1(config)
    except MiniQMTPluginContractError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.CONFIG_SCHEMA_INVALID,
            "Stop config violates the exact K5 source contract",
            context={"algo_code": "STOP", "error": json_safe_evidence_v1(exc)},
        ) from exc


def _strict_envelope(manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]) -> VnpyFacadeStateEnvelopeV1:
    state = _validate_schema(manifest.state_schema, value, contract_name=f"{manifest.algo_code} state")
    envelope = VnpyFacadeStateEnvelopeV1.model_validate_json(
        json.dumps(state, sort_keys=True, separators=(",", ":")), strict=True
    )
    binding = k5_binding_for_algo_v2(manifest.algo_code)
    expected = (
        manifest.plugin_id,
        manifest.plugin_version,
        manifest.manifest_sha256,
        binding.binding_sha256,
    )
    actual = (
        envelope.plugin_id,
        envelope.plugin_version,
        envelope.plugin_manifest_sha256,
        envelope.algorithm_binding_sha256,
    )
    if actual != expected:
        raise ValueError("facade envelope plugin/manifest/binding identity does not close over the K5 manifest")
    if Decimal(envelope.traded_volume_decimal) > Decimal(envelope.target_volume_decimal):
        raise ValueError("K5 state traded volume exceeds target volume")
    expected_side = {"LONG": "BUY", "SHORT": "SELL"}.get(envelope.direction_member)
    if expected_side is None or any(
        item.symbol != envelope.symbol or item.side != expected_side for item in envelope.ordered_active_orders
    ):
        raise ValueError("K5 active mappings must retain the envelope symbol and source direction ownership")
    return envelope


def _state_values(envelope: VnpyFacadeStateEnvelopeV1, *, role: str) -> dict[str, Any]:
    values = envelope.ordered_parameters if role == "parameter" else envelope.ordered_variables
    return {item.name: thaw_json_v1(item.value) for item in values}


def _validate_pointer(envelope: VnpyFacadeStateEnvelopeV1, pointer: Any) -> None:
    if type(pointer) is not str:
        raise ValueError("vt_orderid must be a strict string")
    if pointer and pointer not in {item.local_vt_orderid for item in envelope.ordered_active_orders}:
        raise ValueError("non-empty vt_orderid must identify one retained active mapping")


def validate_iceberg_state_v1(manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]) -> Any:
    try:
        if manifest != _manifest_for_algo("ICEBERG"):
            raise ValueError("Iceberg state validator requires the exact K5 Iceberg manifest")
        envelope = _strict_envelope(manifest, value)
        parameters = _state_values(envelope, role="parameter")
        variables = _state_values(envelope, role="variable")
        if set(parameters) != {"display_volume", "interval"} or set(variables) != {"timer_count", "vt_orderid"}:
            raise ValueError("Iceberg durable state must retain its exact parameter and variable sets")
        display_volume = parameters["display_volume"]
        if type(display_volume) is int:
            valid_display_volume = display_volume >= 0
        elif type(display_volume) is str:
            valid_display_volume = Decimal(_canonical_signed_decimal(display_volume, field_name="display_volume")) >= 0
        else:
            valid_display_volume = False
        if not valid_display_volume:
            raise ValueError("Iceberg display_volume must retain an exact non-negative source numeric carrier")
        if type(parameters["interval"]) is not int or parameters["interval"] < 0:
            raise ValueError("Iceberg interval must be a non-negative strict integer")
        if type(variables["timer_count"]) is not int or variables["timer_count"] < 0:
            raise ValueError("Iceberg timer_count must be a non-negative strict integer")
        interval = parameters["interval"]
        if (interval in (0, 1) and variables["timer_count"] != 0) or (
            interval > 1 and variables["timer_count"] >= interval
        ):
            raise ValueError("Iceberg timer_count violates exact source interval reset semantics")
        _validate_pointer(envelope, variables["vt_orderid"])
        return freeze_json_v1(envelope.canonical_payload_v1())
    except MiniQMTPluginContractError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.STATE_SCHEMA_INVALID,
            "Iceberg state violates the exact K5 source contract",
            context={"algo_code": "ICEBERG", "error": json_safe_evidence_v1(exc)},
        ) from exc


def validate_stop_state_v1(manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]) -> Any:
    try:
        if manifest != _manifest_for_algo("STOP"):
            raise ValueError("Stop state validator requires the exact K5 Stop manifest")
        envelope = _strict_envelope(manifest, value)
        parameters = _state_values(envelope, role="parameter")
        variables = _state_values(envelope, role="variable")
        if set(parameters) != {"price_add"} or set(variables) != {"order_status", "vt_orderid"}:
            raise ValueError("Stop durable state must retain its exact parameter and variable sets")
        _canonical_signed_decimal(parameters["price_add"], field_name="price_add")
        _validate_pointer(envelope, variables["vt_orderid"])
        if len(envelope.ordered_active_orders) > 1:
            raise ValueError("Stop source state permits at most one retained active mapping")
        order_status = variables["order_status"]
        if order_status != "" and not (
            type(order_status) is dict
            and set(order_status) == {"enum_owner", "member", "pinned_value"}
            and order_status["enum_owner"] == "Status"
            and all(type(order_status[field]) is str and order_status[field] for field in ("member", "pinned_value"))
        ):
            raise ValueError("Stop order_status must be the exact source empty value or Status enum carrier")
        return freeze_json_v1(envelope.canonical_payload_v1())
    except MiniQMTPluginContractError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.STATE_SCHEMA_INVALID,
            "Stop state violates the exact K5 source contract",
            context={"algo_code": "STOP", "error": json_safe_evidence_v1(exc)},
        ) from exc


def _assert_callable_signature_v1(value: Any, *, expected: str, binding_id: str) -> None:
    actual = callable_signature_sha256_v1(value)
    if actual != expected:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.BINDING_INVALID,
            "K5 code-owned callable signature drifted",
            context={"binding_id": binding_id, "expected_sha256": expected, "actual_sha256": actual},
        )


def k5_descriptors_v1() -> tuple[PluginRegistrationDescriptorV2, ...]:
    descriptors = []
    for manifest in k5_manifests_v1():
        facts = _FACTS[manifest.algo_code]
        prefix = manifest.plugin_id
        from . import k5_plugin_factories

        factory = getattr(k5_plugin_factories, facts["factory_name"])
        config_validator = validate_iceberg_config_v1 if manifest.algo_code == "ICEBERG" else validate_stop_config_v1
        state_validator = validate_iceberg_state_v1 if manifest.algo_code == "ICEBERG" else validate_stop_state_v1
        _assert_callable_signature_v1(factory, expected=_FACTORY_SIGNATURE_SHA256, binding_id=f"{prefix}.factory")
        _assert_callable_signature_v1(
            config_validator, expected=_VALIDATOR_SIGNATURE_SHA256, binding_id=f"{prefix}.config_validator"
        )
        _assert_callable_signature_v1(
            state_validator, expected=_VALIDATOR_SIGNATURE_SHA256, binding_id=f"{prefix}.state_codec"
        )
        descriptors.append(
            PluginRegistrationDescriptorV2(
                schema_version="plugin_registration_descriptor_v2",
                manifest=manifest,
                factory_binding_id=f"{prefix}.factory",
                factory_callable_ref=facts["implementation_ref"],
                factory_signature_sha256=_FACTORY_SIGNATURE_SHA256,
                config_validator_binding_id=f"{prefix}.config_validator",
                config_validator_callable_ref=facts["config_validator_ref"],
                config_validator_signature_sha256=_VALIDATOR_SIGNATURE_SHA256,
                state_codec_binding_id=f"{prefix}.state_codec",
                state_codec_callable_ref=facts["state_validator_ref"],
                state_codec_signature_sha256=_VALIDATOR_SIGNATURE_SHA256,
            )
        )
    return tuple(descriptors)


def k5_creation_bindings_v1() -> tuple[PluginCreationBindingV1, ...]:
    return tuple(
        PluginCreationBindingV1(algo_code=manifest.algo_code, plugin_key=descriptor.plugin_key)
        for manifest, descriptor in zip(k5_manifests_v1(), k5_descriptors_v1(), strict=True)
    )


__all__ = [
    "k5_creation_bindings_v1",
    "k5_descriptors_v1",
    "k5_manifests_v1",
    "validate_iceberg_config_v1",
    "validate_iceberg_state_v1",
    "validate_stop_config_v1",
    "validate_stop_state_v1",
]
