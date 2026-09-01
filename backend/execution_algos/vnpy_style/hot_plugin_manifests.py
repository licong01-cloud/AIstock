"""Versioned current-three manifests for the process-local hot-data plane.

V3 descriptors remain registered for exact historical restore.  These V4
descriptors are the sole creation bindings and their durable state contains
economic/order facts only: no quote identity, payload, source-view hash or
market-data lineage.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import re
from typing import Any, Mapping, Self

from pydantic import model_validator

from backend.services.miniqmt_execution_runtime.plugin_canonical import freeze_json_v1, hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    EventTypeV2,
    ExecutionAlgoPluginManifestV2,
    FileHashV1,
    FrozenStrictModel,
    IdentityV1,
    Sha256V1,
    SideV1,
    BrokerCommandTypeV2,
    CurrentThreeActiveOrderStatusV3,
    NormalizedOrderStatusV1,
    PositiveCanonicalDecimalV1,
    PositiveIntV1,
    NonNegativeIntV1,
    FrozenJsonObjectV1,
    SourceAttributionV1,
    validate_json_schema_instance_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    PluginCreationBindingV1,
    PluginRegistrationDescriptorV2,
    callable_signature_sha256_v1,
)

from .plugin_manifests import current_three_manifests_v3, validate_current_three_config_v2

_ROOT = Path(__file__).resolve().parents[3]
_VERSION = "4.0.0"
_FACTS = {
    "BEST_LIMIT_MINIQMT": (
        "backend.execution_algos.vnpy_style.hot_plugin_factories:create_best_limit_miniqmt_plugin_v4",
        "best_limit_state_v4",
        "backend/execution_algos/vnpy_style/hot_best_limit_plugin.py",
    ),
    "SNIPER_MINIQMT": (
        "backend.execution_algos.vnpy_style.hot_plugin_factories:create_sniper_miniqmt_plugin_v4",
        "sniper_state_v4",
        "backend/execution_algos/vnpy_style/hot_sniper_plugin.py",
    ),
    "TWAP_LITE_MINIQMT": (
        "backend.execution_algos.vnpy_style.hot_plugin_factories:create_twap_lite_miniqmt_plugin_v4",
        "twap_lite_state_v4",
        "backend/execution_algos/vnpy_style/hot_twap_lite_plugin.py",
    ),
}


class CurrentThreeHotActiveOrderStateV4(FrozenStrictModel):
    local_vt_orderid: IdentityV1
    submit_command_id: IdentityV1
    broker_order_id: IdentityV1 | None
    symbol: IdentityV1
    side: SideV1
    status: CurrentThreeActiveOrderStatusV3
    pending_command_type: BrokerCommandTypeV2 | None
    pending_command_id: IdentityV1 | None
    requested_price_decimal: PositiveCanonicalDecimalV1
    requested_quantity: PositiveIntV1
    cumulative_filled_quantity: NonNegativeIntV1
    remaining_quantity: NonNegativeIntV1
    last_order_event_id: IdentityV1 | None
    last_trade_event_id: IdentityV1 | None
    last_command_outcome_event_id: IdentityV1 | None
    last_oms_reconcile_event_id: IdentityV1 | None
    terminal_order_status: NormalizedOrderStatusV1 | None
    terminal_observed_cumulative_filled_quantity: NonNegativeIntV1 | None
    active_order_state_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {key: value for key, value in values.items() if key != "active_order_state_sha256"}
        model = {
            **payload,
            "side": SideV1(payload["side"]),
            "status": CurrentThreeActiveOrderStatusV3(payload["status"]),
            "pending_command_type": None
            if payload.get("pending_command_type") is None
            else BrokerCommandTypeV2(payload["pending_command_type"]),
            "terminal_order_status": None
            if payload.get("terminal_order_status") is None
            else NormalizedOrderStatusV1(payload["terminal_order_status"]),
        }
        hash_payload = {
            **model,
            "side": model["side"].value,
            "status": model["status"].value,
            "pending_command_type": None
            if model["pending_command_type"] is None
            else model["pending_command_type"].value,
            "terminal_order_status": None
            if model["terminal_order_status"] is None
            else model["terminal_order_status"].value,
        }
        return cls(**model, active_order_state_sha256=hash_hex_v1("miniqmt_plugin_active_order_state_v4", hash_payload))

    @model_validator(mode="after")
    def _validate_order(self) -> Self:
        if re.fullmatch(r"[0-9]{6}\.(?:SH|SZ|BJ)", self.symbol) is None:
            raise ValueError("symbol must be a canonical A-share symbol")
        if self.cumulative_filled_quantity + self.remaining_quantity != self.requested_quantity:
            raise ValueError("active child quantity closure is invalid")
        if (self.pending_command_type is None) != (self.pending_command_id is None):
            raise ValueError("pending command owner must be complete")
        if self.status is CurrentThreeActiveOrderStatusV3.COMMAND_PENDING and (
            self.pending_command_type is not BrokerCommandTypeV2.SUBMIT_LIMIT
            or self.pending_command_id != self.submit_command_id
            or self.broker_order_id is not None
            or self.cumulative_filled_quantity != 0
            or self.remaining_quantity != self.requested_quantity
        ):
            raise ValueError("COMMAND_PENDING owner closure is invalid")
        if self.status in {
            CurrentThreeActiveOrderStatusV3.SUBMITTED,
            CurrentThreeActiveOrderStatusV3.PARTIALLY_FILLED,
        } and (self.pending_command_id is not None or self.broker_order_id is None):
            raise ValueError("broker-active order owner closure is invalid")
        if self.status is CurrentThreeActiveOrderStatusV3.CANCEL_PENDING and (
            self.pending_command_type is not BrokerCommandTypeV2.CANCEL_ORDER or self.broker_order_id is None
        ):
            raise ValueError("CANCEL_PENDING owner closure is invalid")
        if self.status is CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN:
            if self.pending_command_type is None:
                raise ValueError("OUTCOME_UNKNOWN requires the exact pending command")
            if self.pending_command_type is BrokerCommandTypeV2.CANCEL_ORDER and self.broker_order_id is None:
                raise ValueError("unknown CANCEL outcome requires the owned broker identity")
        if self.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING and (
            self.pending_command_id is not None
            or self.broker_order_id is None
            or self.last_order_event_id is None
            or self.terminal_order_status
            not in {
                NormalizedOrderStatusV1.FILLED,
                NormalizedOrderStatusV1.CANCELLED,
                NormalizedOrderStatusV1.REJECTED,
            }
        ):
            raise ValueError("terminal-trade-pending owner closure is invalid")
        if self.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING:
            observed = self.terminal_observed_cumulative_filled_quantity
            if observed is not None and observed <= self.cumulative_filled_quantity:
                raise ValueError("terminal observed cumulative must be missing or ahead of trade facts")
        if self.status is not CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING and (
            self.terminal_order_status is not None or self.terminal_observed_cumulative_filled_quantity is not None
        ):
            raise ValueError("nonterminal active child cannot carry terminal facts")
        if self.active_order_state_sha256 != hash_hex_v1(
            "miniqmt_plugin_active_order_state_v4", self.canonical_payload_v1(exclude={"active_order_state_sha256"})
        ):
            raise ValueError("active child hash closure is invalid")
        return self


def _object(properties: dict[str, Any], required: tuple[str, ...]) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": list(required),
    }


def hot_active_order_schema_v4() -> dict[str, Any]:
    optional_identity = {"type": ["string", "null"]}
    return _object(
        {
            "local_vt_orderid": {"type": "string", "minLength": 1},
            "submit_command_id": {"type": "string", "minLength": 1},
            "broker_order_id": optional_identity,
            "symbol": {"type": "string", "pattern": "^[0-9]{6}\\.(?:SH|SZ|BJ)$"},
            "side": {"enum": ["BUY", "SELL"]},
            "status": {
                "enum": [
                    "COMMAND_PENDING",
                    "SUBMITTED",
                    "PARTIALLY_FILLED",
                    "CANCEL_PENDING",
                    "OUTCOME_UNKNOWN",
                    "TERMINAL_TRADE_PENDING",
                ]
            },
            "pending_command_type": {"type": ["string", "null"], "enum": ["SUBMIT_LIMIT", "CANCEL_ORDER", None]},
            "pending_command_id": optional_identity,
            "requested_price_decimal": {"type": "string", "pattern": "^(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?$"},
            "requested_quantity": {"type": "integer", "minimum": 1},
            "cumulative_filled_quantity": {"type": "integer", "minimum": 0},
            "remaining_quantity": {"type": "integer", "minimum": 0},
            "last_order_event_id": optional_identity,
            "last_trade_event_id": optional_identity,
            "last_command_outcome_event_id": optional_identity,
            "last_oms_reconcile_event_id": optional_identity,
            "terminal_order_status": {"type": ["string", "null"], "enum": ["FILLED", "CANCELLED", "REJECTED", None]},
            "terminal_observed_cumulative_filled_quantity": {"type": ["integer", "null"], "minimum": 0},
            "active_order_state_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        },
        (
            "local_vt_orderid",
            "submit_command_id",
            "broker_order_id",
            "symbol",
            "side",
            "status",
            "pending_command_type",
            "pending_command_id",
            "requested_price_decimal",
            "requested_quantity",
            "cumulative_filled_quantity",
            "remaining_quantity",
            "last_order_event_id",
            "last_trade_event_id",
            "last_command_outcome_event_id",
            "last_oms_reconcile_event_id",
            "terminal_order_status",
            "terminal_observed_cumulative_filled_quantity",
            "active_order_state_sha256",
        ),
    )


def _state_schema(algo_code: str) -> dict[str, Any]:
    parameter_properties: dict[str, Any] = {}
    variable_properties: dict[str, Any] = {}
    specific: dict[str, Any] = {}
    if algo_code == "SNIPER_MINIQMT":
        variable_properties = {"vt_orderid": {"type": ["string", "null"]}}
        specific = dict(variable_properties)
    elif algo_code == "BEST_LIMIT_MINIQMT":
        parameter_properties = {
            "min_volume": {"type": "integer", "minimum": 1},
            "max_volume": {"type": "integer", "minimum": 1},
        }
        variable_properties = {
            "vt_orderid": {"type": ["string", "null"]},
            "order_price_decimal": {"type": ["string", "null"]},
            "next_draw_ordinal": {"type": "integer", "minimum": 0},
        }
        specific = dict(variable_properties)
    else:
        parameter_properties = {
            "time": {"type": "integer", "minimum": 1},
            "interval": {"type": "integer", "minimum": 1},
        }
        variable_properties = {
            "order_volume": {"type": "integer", "minimum": 0},
            "active_elapsed_seconds": {"type": "integer", "minimum": 0},
            "interval_elapsed_seconds": {"type": "integer", "minimum": 0},
            "last_timer_occurrence_id": {"type": ["string", "null"]},
            "slice_ready": {"type": "boolean"},
        }
        specific = {
            "duration_seconds": {"type": "integer", "minimum": 1},
            "interval_seconds": {"type": "integer", "minimum": 1},
            **variable_properties,
        }
    properties = {
        "algo_name": {"type": "string", "minLength": 1},
        "algo_code": {"const": algo_code},
        "parent_intent_id": {"type": "string", "minLength": 1},
        "symbol": {"type": "string", "pattern": "^[0-9]{6}\\.(?:SH|SZ|BJ)$"},
        "side": {"enum": ["BUY", "SELL"]},
        "offset": {"const": "NONE"},
        "limit_price_decimal": {"type": "string", "pattern": "^(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?$"},
        "parent_quantity": {"type": "integer", "minimum": 1},
        "min_volume": {"type": "integer", "minimum": 1},
        "volume_increment": {"type": "integer", "minimum": 1},
        "status": {"enum": ["PAUSED", "RUNNING", "STOPPED", "FINISHED"]},
        "traded_quantity": {"type": "integer", "minimum": 0},
        "traded_price_decimal": {"type": "string", "pattern": "^(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?$"},
        "active_orders": {"type": "array", "items": hot_active_order_schema_v4()},
        "parameters": _object(parameter_properties, tuple(parameter_properties)),
        "variables": _object(variable_properties, tuple(variable_properties)),
        "finished_reason": {"type": ["string", "null"]},
        **specific,
    }
    return _object(properties, tuple(properties))


def _file(path: str) -> FileHashV1:
    payload = (_ROOT / path).read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return FileHashV1(path=path, sha256=hashlib.sha256(payload).hexdigest())


def _source(base: ExecutionAlgoPluginManifestV2, algo_path: str) -> SourceAttributionV1:
    paths = (
        "backend/execution_algos/vnpy_style/hot_plugin_base.py",
        "backend/execution_algos/vnpy_style/hot_plugin_factories.py",
        "backend/execution_algos/vnpy_style/hot_plugin_manifests.py",
        algo_path,
        "backend/execution_algos/vnpy_style/plugin_base.py",
        "backend/execution_algos/hot_market_contracts.py",
        "backend/services/miniqmt_execution_runtime/plugin_contracts.py",
    )
    plain = {
        "schema_version": "source_attribution_v1",
        "upstream_repo": base.source_attribution.upstream_repo,
        "upstream_commit": base.source_attribution.upstream_commit,
        "upstream_files": [item.canonical_payload_v1() for item in base.source_attribution.upstream_files],
        "upstream_license": base.source_attribution.upstream_license,
        "upstream_copyright": base.source_attribution.upstream_copyright,
        "aistock_asset_version": "2026.08.12-miniqmt-hot-market-data-v4",
        "aistock_files": [item.canonical_payload_v1() for item in tuple(_file(path) for path in sorted(paths))],
        "derivation_summary": "process-local B0 market view with durable economic effect only; V3 remains restore-only",
    }
    return SourceAttributionV1(
        **{
            **plain,
            "upstream_files": base.source_attribution.upstream_files,
            "aistock_files": tuple(_file(path) for path in sorted(paths)),
        },
        attribution_sha256=hash_hex_v1("miniqmt_source_attribution_v1", plain),
    )


def _manifest(base: ExecutionAlgoPluginManifestV2) -> ExecutionAlgoPluginManifestV2:
    implementation_ref, state_version, algo_path = _FACTS[base.algo_code]
    state_schema = _state_schema(base.algo_code)
    source = _source(base, algo_path)
    canonical = base.canonical_payload_v1(exclude={"manifest_sha256", "behavior_contract_sha256"})
    # TICK remains a declarative process-local capability subscription.  The
    # durable ingress rejects TICK before repository access; OPERATOR carries
    # the rare committed economic effect.
    subscriptions = tuple(sorted(({*base.subscribed_event_types, EventTypeV2.OPERATOR}), key=lambda item: item.value))
    canonical.update(
        plugin_version=_VERSION,
        implementation_ref=implementation_ref,
        state_schema_version=state_version,
        state_schema=state_schema,
        state_schema_sha256=hash_hex_v1("miniqmt_plugin_state_schema_v1", state_schema),
        subscribed_event_types=[item.value for item in subscriptions],
        source_attribution=source.canonical_payload_v1(),
        behavior_characterization_sha256=hash_hex_v1(
            "miniqmt_plugin_behavior_characterization_v4",
            {"algo_code": base.algo_code, "market_plane": "PROCESS_LOCAL", "durable_effect": "ECONOMIC_ONLY"},
        ),
    )
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
    behavior_hash = hash_hex_v1("miniqmt_plugin_behavior_contract_v2", {key: canonical[key] for key in behavior_keys})
    canonical["behavior_contract_sha256"] = behavior_hash
    manifest_hash = hash_hex_v1("execution_algo_plugin_manifest_v2", canonical)
    model_payload = base.model_dump(mode="python", exclude={"manifest_sha256", "behavior_contract_sha256"})
    model_payload.update(
        plugin_version=_VERSION,
        implementation_ref=implementation_ref,
        state_schema_version=state_version,
        state_schema=state_schema,
        state_schema_sha256=canonical["state_schema_sha256"],
        subscribed_event_types=subscriptions,
        source_attribution=source,
        behavior_characterization_sha256=canonical["behavior_characterization_sha256"],
        behavior_contract_sha256=behavior_hash,
        manifest_sha256=manifest_hash,
    )
    return ExecutionAlgoPluginManifestV2.model_validate(model_payload, strict=True)


def current_three_hot_manifests_v4() -> tuple[ExecutionAlgoPluginManifestV2, ...]:
    return tuple(_manifest(item) for item in current_three_manifests_v3())


def validate_current_three_hot_config_v4(
    manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]
) -> FrozenJsonObjectV1:
    return validate_current_three_config_v2(manifest, value)


def validate_current_three_hot_state_v4(
    manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]
) -> FrozenJsonObjectV1:
    if manifest.plugin_version != _VERSION:
        raise ValueError("hot state codec requires exact V4 manifest")
    frozen = freeze_json_v1(dict(value))
    validate_json_schema_instance_v1(
        schema=manifest.state_schema, instance=frozen, contract_name=f"{manifest.algo_code} hot state"
    )
    return frozen


def current_three_hot_descriptors_v4() -> tuple[PluginRegistrationDescriptorV2, ...]:
    from . import hot_plugin_factories

    descriptors = []
    for manifest in current_three_hot_manifests_v4():
        factory_name = _FACTS[manifest.algo_code][0].split(":", 1)[1]
        factory = getattr(hot_plugin_factories, factory_name)
        descriptors.append(
            PluginRegistrationDescriptorV2(
                schema_version="plugin_registration_descriptor_v2",
                manifest=manifest,
                factory_binding_id=f"{manifest.plugin_id}.v4.factory",
                factory_callable_ref=_FACTS[manifest.algo_code][0],
                factory_signature_sha256=callable_signature_sha256_v1(factory),
                config_validator_binding_id=f"{manifest.plugin_id}.v4.config_validator",
                config_validator_callable_ref=f"{__name__}:validate_current_three_hot_config_v4",
                config_validator_signature_sha256=callable_signature_sha256_v1(validate_current_three_hot_config_v4),
                state_codec_binding_id=f"{manifest.plugin_id}.v4.state_codec",
                state_codec_callable_ref=f"{__name__}:validate_current_three_hot_state_v4",
                state_codec_signature_sha256=callable_signature_sha256_v1(validate_current_three_hot_state_v4),
            )
        )
    return tuple(descriptors)


def current_three_hot_creation_bindings_v4() -> tuple[PluginCreationBindingV1, ...]:
    return tuple(
        PluginCreationBindingV1(algo_code=item.manifest.algo_code, plugin_key=item.plugin_key)
        for item in current_three_hot_descriptors_v4()
    )


__all__ = [
    "current_three_hot_creation_bindings_v4",
    "current_three_hot_descriptors_v4",
    "current_three_hot_manifests_v4",
    "hot_active_order_schema_v4",
    "validate_current_three_hot_config_v4",
    "validate_current_three_hot_state_v4",
]
