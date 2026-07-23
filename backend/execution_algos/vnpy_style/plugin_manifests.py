"""K1-B code-owned manifests for the three current vn.py-style algorithms.

This module is shadow-only.  It describes the future execution-kernel plugin
boundary without importing repositories, OMS, gateways, broker SDKs, or the
current product runtime.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from jsonschema import Draft202012Validator
from pydantic import model_validator

from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    canonical_utc_datetime_v1,
    freeze_json_v1,
    hash_hex_v1,
    json_safe_evidence_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AbsenceDispositionV1,
    EnumValueRequirementV1,
    EventTypeV2,
    ExecutionAlgoPluginManifestV2,
    FileHashV1,
    FrozenJsonFieldV1,
    FrozenStrictModel,
    MarketDataCapabilityV1,
    MarketDataRequirementV1,
    ObjectFieldRequirementV1,
    OrderTypeV1,
    PluginProviderV2,
    SessionPhaseV1,
    Sha256V1,
    SideV1,
    SourceAttributionV1,
    VnpyCompatibilityRequirementV1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    PluginCreationBindingV1,
    PluginProcessBindingsV2,
    PluginRegistrationDescriptorV2,
    callable_ref_v1,
    callable_signature_sha256_v1,
)

from .attribution import (
    AISTOCK_ASSET_VERSION,
    UPSTREAM_COMMIT,
    UPSTREAM_COPYRIGHT,
    UPSTREAM_LICENSE,
    UPSTREAM_REPO,
)
from .best_limit_core import BestLimitMiniQMTCore
from .sniper_core import SniperMiniQMTCore
from .twap_lite_core import TwapLiteMiniQMTCore

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PLUGIN_VERSION = "2.0.0"
_CONTINUOUS_PHASES = (SessionPhaseV1.CONTINUOUS_AM, SessionPhaseV1.CONTINUOUS_PM)
_SIDES = (SideV1.BUY, SideV1.SELL)
_COMMON_EVENTS = (
    EventTypeV2.ALGO_START,
    EventTypeV2.EOD,
    EventTypeV2.ORDER,
    EventTypeV2.SESSION,
    EventTypeV2.TICK,
    EventTypeV2.TRADE,
)
_UPSTREAM_HASHES = {
    "vnpy_algotrading/algos/best_limit_algo.py": "b35227b932a160c2f786d3202283b61656d9f16631fb42f596a9d376765617e9",
    "vnpy_algotrading/algos/sniper_algo.py": "fbf84d2c61f8200079fe1f8da3b3412a036e5a7ffb6c601f9e4614ad110c8c76",
    "vnpy_algotrading/algos/twap_algo.py": "aeabb067ef79d48182f357b8d4736f8a90f6a4ecb77bc82506a3244575a6cd0f",
    "vnpy_algotrading/base.py": "8416653d8cf61ab45e26b593eea06417dd6fa21b331bba6c60a2bbb8bccf8f93",
    "vnpy_algotrading/engine.py": "2c73e1c093cabcd5768954f1129451877a82afd204790fb07e4f305b64c5e68d",
    "vnpy_algotrading/template.py": "b21fa36a8a2c347ab92379df1cd9f81ec69bc922233ec4096d75dbbade7454b8",
}
_ALGO_FACTS = {
    "BEST_LIMIT_MINIQMT": (
        "aistock.vnpy.best_limit",
        BestLimitMiniQMTCore,
        "best_limit_state_v2",
        "vnpy_algotrading/algos/best_limit_algo.py",
        "backend/execution_algos/vnpy_style/best_limit_core.py",
    ),
    "SNIPER_MINIQMT": (
        "aistock.vnpy.sniper",
        SniperMiniQMTCore,
        "sniper_state_v2",
        "vnpy_algotrading/algos/sniper_algo.py",
        "backend/execution_algos/vnpy_style/sniper_core.py",
    ),
    "TWAP_LITE_MINIQMT": (
        "aistock.vnpy.twap_lite",
        TwapLiteMiniQMTCore,
        "twap_lite_state_v2",
        "vnpy_algotrading/algos/twap_algo.py",
        "backend/execution_algos/vnpy_style/twap_lite_core.py",
    ),
}

CURRENT_THREE_BEHAVIOR_CHARACTERIZATIONS_V2: dict[str, dict[str, Any]] = {
    "SNIPER_MINIQMT": {
        "schema_version": "current_three_behavior_characterization_v2",
        "algo_code": "SNIPER_MINIQMT",
        "trace_vectors": [
            {
                "vector_id": "legacy_core_primary_submit",
                "expected": {
                    "action_type": "SUBMIT",
                    "price_decimal": "10",
                    "quantity": 200,
                    "reason": "sniper_ask_crossed_limit",
                },
            }
        ],
        "active_child_identity": "EXACT_LOCAL_VT_ORDERID_TO_DURABLE_COMMAND_CHILD_OMS_JOIN",
        "buy_quote": "L1_ASK_PRICE_AND_VOLUME",
        "sell_quote": "L1_BID_PRICE_AND_VOLUME",
        "replace_policy": "CANCEL_ACTIVE_CHILD_BEFORE_REQUOTE",
        "restart_source": "FROZEN_PLUGIN_KEY_AND_DURABLE_STATE_SNAPSHOT",
        "auction_native_synthesis": False,
    },
    "BEST_LIMIT_MINIQMT": {
        "schema_version": "current_three_behavior_characterization_v2",
        "algo_code": "BEST_LIMIT_MINIQMT",
        "trace_vectors": [
            {
                "vector_id": "legacy_core_primary_submit",
                "expected": {
                    "action_type": "SUBMIT",
                    "price_decimal": "9.88",
                    "quantity": 300,
                    "reason": "best_limit_buy_at_bid_price_1",
                },
            }
        ],
        "active_child_identity": "EXACT_LOCAL_VT_ORDERID_TO_DURABLE_COMMAND_CHILD_OMS_JOIN",
        "buy_quote": "L1_BID_PRICE_ONLY",
        "sell_quote": "L1_ASK_PRICE_ONLY",
        "draw_formula": "RAW_DIGEST_U53",
        "draw_ordinal": "STRICT_CONTIGUOUS_DURABLE_STATE",
        "restart_source": "FROZEN_PLUGIN_KEY_AND_DURABLE_STATE_SNAPSHOT",
        "auction_native_synthesis": False,
    },
    "TWAP_LITE_MINIQMT": {
        "schema_version": "current_three_behavior_characterization_v2",
        "algo_code": "TWAP_LITE_MINIQMT",
        "trace_vectors": [
            {
                "vector_id": "legacy_core_primary_submit",
                "expected": {
                    "action_type": "SUBMIT",
                    "price_decimal": "10",
                    "quantity": 500,
                    "reason": "twap_lite_interval_buy",
                },
            }
        ],
        "active_child_identity": "EXACT_LOCAL_VT_ORDERID_TO_DURABLE_COMMAND_CHILD_OMS_JOIN",
        "duration_unit": "EXCHANGE_ACTIVE_SECONDS",
        "interval_unit": "EXCHANGE_ACTIVE_SECONDS",
        "counted_session_phases": ["CONTINUOUS_AM", "CONTINUOUS_PM"],
        "non_counted_session_phases": ["OPEN_AUCTION", "LUNCH_BREAK", "CLOSE_AUCTION", "CLOSED"],
        "timer_reads_wall_clock": False,
        "timer_market_data_source": "DURABLE_LATEST_MARKET_DATA_VIEW",
        "timer_market_data_fallbacks": [],
        "catch_up_burst": False,
        "restart_replays_consumed_timer": False,
        "eod_outcome": "EXPLICIT_TERMINAL_OR_RESIDUAL_EVIDENCE",
        "zero_slice_diagnostic_reason": "TWAP_SLICE_VOLUME_ROUNDED_ZERO",
        "auction_native_synthesis": False,
    },
}


def _object_schema(properties: dict[str, Any], required: tuple[str, ...]) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": list(required),
    }


def _config_schema(algo_code: str) -> dict[str, Any]:
    if algo_code == "SNIPER_MINIQMT":
        return _object_schema(
            {
                "price_mode": {
                    "const": "LIMIT_TRIGGER_BY_BEST_QUOTE",
                    "type": "string",
                    "default": "LIMIT_TRIGGER_BY_BEST_QUOTE",
                }
            },
            ("price_mode",),
        )
    if algo_code == "BEST_LIMIT_MINIQMT":
        return _object_schema(
            {
                "min_volume": {"type": "integer", "minimum": 1},
                "max_volume": {"type": "integer", "minimum": 1},
            },
            ("min_volume", "max_volume"),
        )
    return _object_schema(
        {
            "time": {"type": "integer", "minimum": 1},
            "interval": {"type": "integer", "minimum": 1},
        },
        ("time", "interval"),
    )


def _lineage_schema() -> dict[str, Any]:
    return _object_schema(
        {
            "market_data_id": {"type": "string", "minLength": 1, "pattern": "^(?:\\S|\\S.*\\S)$"},
            "event_id": {"type": "string", "minLength": 1, "pattern": "^(?:\\S|\\S.*\\S)$"},
            "payload_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "generation": {"type": "integer", "minimum": 0},
            "sequence": {"type": "integer", "minimum": 0},
            "exchange_time_utc": {"type": "string", "minLength": 1},
            "session_phase": {"enum": [item.value for item in SessionPhaseV1]},
        },
        (
            "market_data_id",
            "event_id",
            "payload_sha256",
            "generation",
            "sequence",
            "exchange_time_utc",
            "session_phase",
        ),
    )


def _active_order_schema() -> dict[str, Any]:
    return _object_schema(
        {
            "local_vt_orderid": {"type": "string", "minLength": 1, "pattern": "^(?:\\S|\\S.*\\S)$"},
            "submit_command_id": {"type": "string", "minLength": 1, "pattern": "^(?:\\S|\\S.*\\S)$"},
            "broker_order_id": {
                "anyOf": [
                    {"type": "string", "minLength": 1, "pattern": "^(?:\\S|\\S.*\\S)$"},
                    {"type": "null"},
                ]
            },
            "symbol": {"type": "string", "pattern": "^[0-9]{6}\\.(?:SH|SZ|BJ)$"},
            "side": {"enum": ["BUY", "SELL"]},
            "status": {
                "enum": [
                    "PENDING_DISPATCH",
                    "SUBMITTED",
                    "PARTIALLY_FILLED",
                    "CANCEL_PENDING",
                    "CANCELLED",
                    "FILLED",
                    "REJECTED",
                    "OUTCOME_UNKNOWN",
                ]
            },
            "requested_price_decimal": {"type": "string", "pattern": "^(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?$"},
            "requested_quantity": {"type": "integer", "minimum": 1},
            "cumulative_filled_quantity": {"type": "integer", "minimum": 0},
            "remaining_quantity": {"type": "integer", "minimum": 0},
            "last_order_event_id": {
                "anyOf": [
                    {"type": "string", "minLength": 1, "pattern": "^(?:\\S|\\S.*\\S)$"},
                    {"type": "null"},
                ]
            },
            "last_trade_event_id": {
                "anyOf": [
                    {"type": "string", "minLength": 1, "pattern": "^(?:\\S|\\S.*\\S)$"},
                    {"type": "null"},
                ]
            },
            "mapping_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        },
        (
            "local_vt_orderid",
            "submit_command_id",
            "broker_order_id",
            "symbol",
            "side",
            "status",
            "requested_price_decimal",
            "requested_quantity",
            "cumulative_filled_quantity",
            "remaining_quantity",
            "last_order_event_id",
            "last_trade_event_id",
            "mapping_sha256",
        ),
    )


def _state_schema(algo_code: str) -> dict[str, Any]:
    parameter_properties: dict[str, Any] = {}
    variable_properties: dict[str, Any] = {}
    specific: dict[str, Any] = {}
    required_specific: tuple[str, ...] = ()
    if algo_code == "SNIPER_MINIQMT":
        variable_properties = {"vt_orderid": {"type": ["string", "null"]}}
        specific = {"vt_orderid": {"type": ["string", "null"]}}
        required_specific = ("vt_orderid",)
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
        required_specific = ("vt_orderid", "order_price_decimal", "next_draw_ordinal")
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
            "last_market_data_lineage": {"anyOf": [_lineage_schema(), {"type": "null"}]},
        }
        specific = {
            "duration_seconds": {"type": "integer", "minimum": 1},
            "interval_seconds": {"type": "integer", "minimum": 1},
            **variable_properties,
        }
        required_specific = tuple(specific)
    properties = {
        "algo_name": {"type": "string", "minLength": 1},
        "algo_code": {"const": algo_code},
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
        "active_orders": {"type": "array", "items": _active_order_schema()},
        "parameters": _object_schema(parameter_properties, tuple(parameter_properties)),
        "variables": _object_schema(variable_properties, tuple(variable_properties)),
        "last_tick_lineage": {"anyOf": [_lineage_schema(), {"type": "null"}]},
        "finished_reason": {"type": ["string", "null"]},
        **specific,
    }
    required = (
        "algo_name",
        "algo_code",
        "symbol",
        "side",
        "offset",
        "limit_price_decimal",
        "parent_quantity",
        "min_volume",
        "volume_increment",
        "status",
        "traded_quantity",
        "traded_price_decimal",
        "active_orders",
        "parameters",
        "variables",
        "last_tick_lineage",
        "finished_reason",
        *required_specific,
    )
    return _object_schema(properties, required)


def _file_hash(path: str) -> FileHashV1:
    return FileHashV1(path=path, sha256=hashlib.sha256((_REPO_ROOT / path).read_bytes()).hexdigest())


def _source_attribution(algo_code: str) -> SourceAttributionV1:
    _, _, _, upstream_algo_path, core_path = _ALGO_FACTS[algo_code]
    upstream_files = tuple(
        FileHashV1(path=path, sha256=_UPSTREAM_HASHES[path])
        for path in sorted(
            (
                upstream_algo_path,
                "vnpy_algotrading/base.py",
                "vnpy_algotrading/engine.py",
                "vnpy_algotrading/template.py",
            )
        )
    )
    aistock_paths = (
        "backend/execution_algos/vnpy_style/base.py",
        core_path,
        "backend/execution_algos/vnpy_style/models.py",
        "backend/execution_algos/vnpy_style/plugin_manifests.py",
    ) + (
        ("backend/services/miniqmt_execution_runtime/deterministic_context.py",)
        if algo_code == "BEST_LIMIT_MINIQMT"
        else ()
    )
    aistock_files = tuple(_file_hash(path) for path in sorted(aistock_paths))
    plain = {
        "schema_version": "source_attribution_v1",
        "upstream_repo": UPSTREAM_REPO,
        "upstream_commit": UPSTREAM_COMMIT,
        "upstream_files": [item.canonical_payload_v1() for item in upstream_files],
        "upstream_license": UPSTREAM_LICENSE,
        "upstream_copyright": UPSTREAM_COPYRIGHT,
        "aistock_asset_version": AISTOCK_ASSET_VERSION,
        "aistock_files": [item.canonical_payload_v1() for item in aistock_files],
        "derivation_summary": "vn.py algorithm semantics adapted to strict DTO/action boundaries; broker, persistence, OMS, risk and timing remain kernel-owned",
    }
    return SourceAttributionV1(
        **{**plain, "upstream_files": upstream_files, "aistock_files": aistock_files},
        attribution_sha256=hash_hex_v1("miniqmt_source_attribution_v1", plain),
    )


def _compatibility_requirement(algo_code: str, characterization_sha256: str) -> VnpyCompatibilityRequirementV1:
    _, _, _, upstream_algo_path, _ = _ALGO_FACTS[algo_code]
    source_files = tuple(
        FileHashV1(path=path, sha256=_UPSTREAM_HASHES[path])
        for path in sorted(
            (
                upstream_algo_path,
                "vnpy_algotrading/base.py",
                "vnpy_algotrading/engine.py",
                "vnpy_algotrading/template.py",
            )
        )
    )
    method_signatures = tuple(
        sorted(
            (
                "cancel_order(algo,vt_orderid)->None",
                "get_contract(algo)->ContractData|None",
                "get_tick(algo)->TickData|None",
                "put_algo_event(algo,data)->None",
                "send_order(algo,direction,price,volume,order_type,offset)->str",
                "update_order(algo,order)->None",
                "update_tick(algo,tick)->None",
                "update_timer(algo)->None",
                "update_trade(algo,trade)->None",
                "write_log(msg,algo=None)->None",
            )
        )
    )
    object_fields = tuple(
        sorted(
            (
                ObjectFieldRequirementV1(
                    object_name="ContractData",
                    fields=("exchange", "gateway_name", "min_volume", "pricetick", "symbol"),
                ),
                ObjectFieldRequirementV1(
                    object_name="OrderData",
                    fields=("is_active", "price", "status", "traded", "vt_orderid"),
                ),
                ObjectFieldRequirementV1(
                    object_name="TickData",
                    fields=("ask_price_1", "ask_volume_1", "bid_price_1", "bid_volume_1", "datetime", "vt_symbol"),
                ),
                ObjectFieldRequirementV1(
                    object_name="TradeData",
                    fields=("datetime", "price", "volume", "vt_orderid", "vt_tradeid"),
                ),
            ),
            key=lambda item: item.object_name,
        )
    )
    enum_values = tuple(
        sorted(
            (
                EnumValueRequirementV1(enum_name="AlgoStatus", values=("FINISHED", "PAUSED", "RUNNING", "STOPPED")),
                EnumValueRequirementV1(enum_name="Direction", values=("LONG", "SHORT")),
                EnumValueRequirementV1(enum_name="Offset", values=("NONE",)),
                EnumValueRequirementV1(enum_name="OrderType", values=("LIMIT",)),
            ),
            key=lambda item: item.enum_name,
        )
    )
    plain = {
        "schema_version": "vnpy_compatibility_requirement_v1",
        "mode": "DERIVED_SOURCE_EXACT_CHARACTERIZATION",
        "upstream_repo": UPSTREAM_REPO,
        "upstream_commit": UPSTREAM_COMMIT,
        "source_files_and_hashes": [item.canonical_payload_v1() for item in source_files],
        "required_method_signatures": list(method_signatures),
        "required_object_fields": [item.canonical_payload_v1() for item in object_fields],
        "required_enum_values": [item.canonical_payload_v1() for item in enum_values],
        "characterization_sha256": characterization_sha256,
    }
    return VnpyCompatibilityRequirementV1(
        **{
            **plain,
            "source_files_and_hashes": source_files,
            "required_method_signatures": method_signatures,
            "required_object_fields": object_fields,
            "required_enum_values": enum_values,
        },
        requirement_sha256=hash_hex_v1("miniqmt_vnpy_compatibility_requirement_v1", plain),
    )


def _market_requirement(
    capability: MarketDataCapabilityV1,
    fields: tuple[str, ...],
    side: SideV1,
    *,
    absence: AbsenceDispositionV1,
) -> MarketDataRequirementV1:
    return MarketDataRequirementV1.create(
        capability=capability,
        required_fields=fields,
        applicable_sides=(side,),
        event_types=(EventTypeV2.TICK,),
        session_phases=_CONTINUOUS_PHASES,
        absence_disposition=absence,
    )


def _market_requirements(algo_code: str) -> tuple[MarketDataRequirementV1, ...]:
    wait = AbsenceDispositionV1.WAIT_FOR_NEXT_VALID_EVENT
    if algo_code == "SNIPER_MINIQMT":
        return (
            _market_requirement(MarketDataCapabilityV1.L1_ASK, ("price", "volume"), SideV1.BUY, absence=wait),
            _market_requirement(MarketDataCapabilityV1.L1_BID, ("price", "volume"), SideV1.SELL, absence=wait),
        )
    if algo_code == "BEST_LIMIT_MINIQMT":
        return (
            _market_requirement(MarketDataCapabilityV1.L1_BID, ("price",), SideV1.BUY, absence=wait),
            _market_requirement(MarketDataCapabilityV1.L1_ASK, ("price",), SideV1.SELL, absence=wait),
        )
    return (
        _market_requirement(MarketDataCapabilityV1.L1_ASK, ("price",), SideV1.BUY, absence=wait),
        _market_requirement(MarketDataCapabilityV1.L1_BID, ("price",), SideV1.SELL, absence=wait),
    )


def _manifest(algo_code: str) -> ExecutionAlgoPluginManifestV2:
    plugin_id, factory, state_version, _, _ = _ALGO_FACTS[algo_code]
    config_schema = _config_schema(algo_code)
    state_schema = _state_schema(algo_code)
    characterization = CURRENT_THREE_BEHAVIOR_CHARACTERIZATIONS_V2[algo_code]
    characterization_sha256 = hash_hex_v1("miniqmt_plugin_behavior_characterization_v2", characterization)
    source = _source_attribution(algo_code)
    compatibility = _compatibility_requirement(algo_code, characterization_sha256)
    subscriptions = tuple(
        sorted(
            (*_COMMON_EVENTS, EventTypeV2.TIMER) if algo_code == "TWAP_LITE_MINIQMT" else _COMMON_EVENTS,
            key=lambda item: item.value,
        )
    )
    requirements = tuple(sorted(_market_requirements(algo_code), key=lambda item: item.requirement_sha256))
    facade_methods = tuple(
        sorted(
            (
                "cancel_order",
                "get_contract",
                "get_tick",
                "put_algo_event",
                "send_order",
                "update_order",
                "update_tick",
                "update_timer",
                "update_trade",
                "write_log",
            )
        )
    )
    facade_fields = (
        ObjectFieldRequirementV1(
            object_name="ContractData",
            fields=("exchange", "gateway_name", "min_volume", "pricetick", "symbol"),
        ),
        ObjectFieldRequirementV1(
            object_name="OrderData",
            fields=("is_active", "price", "status", "traded", "vt_orderid"),
        ),
        ObjectFieldRequirementV1(
            object_name="TickData",
            fields=("ask_price_1", "ask_volume_1", "bid_price_1", "bid_volume_1", "datetime", "vt_symbol"),
        ),
        ObjectFieldRequirementV1(
            object_name="TradeData",
            fields=("datetime", "price", "volume", "vt_orderid", "vt_tradeid"),
        ),
    )
    plain: dict[str, Any] = {
        "schema_version": "execution_algo_plugin_manifest_v2",
        "plugin_id": plugin_id,
        "algo_code": algo_code,
        "plugin_version": _PLUGIN_VERSION,
        "provider": PluginProviderV2.AISTOCK_DERIVED.value,
        "implementation_ref": callable_ref_v1(factory),
        "config_schema_version": f"{algo_code.lower()}_config_v2",
        "config_schema": config_schema,
        "config_schema_sha256": hash_hex_v1("miniqmt_plugin_config_schema_v1", config_schema),
        "state_schema_version": state_version,
        "state_schema": state_schema,
        "state_schema_sha256": hash_hex_v1("miniqmt_plugin_state_schema_v1", state_schema),
        "subscribed_event_types": [item.value for item in subscriptions],
        "market_data_requirements": [item.canonical_payload_v1() for item in requirements],
        "required_facade_methods": list(facade_methods),
        "required_facade_object_fields": [item.canonical_payload_v1() for item in facade_fields],
        "supported_sides": [item.value for item in _SIDES],
        "supported_order_types": [OrderTypeV1.LIMIT.value],
        "supported_broker_backends": ["minqmt_sim"],
        "restart_policy": "DURABLE_RESTORE",
        "source_attribution": source.canonical_payload_v1(),
        "compatibility_requirement": compatibility.canonical_payload_v1(),
        "behavior_characterization_sha256": characterization_sha256,
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
            "provider": PluginProviderV2.AISTOCK_DERIVED,
            "subscribed_event_types": subscriptions,
            "market_data_requirements": requirements,
            "required_facade_methods": facade_methods,
            "required_facade_object_fields": facade_fields,
            "supported_sides": _SIDES,
            "supported_order_types": (OrderTypeV1.LIMIT,),
            "supported_broker_backends": ("minqmt_sim",),
            "source_attribution": source,
            "compatibility_requirement": compatibility,
        }
    )


def current_three_manifests_v2() -> tuple[ExecutionAlgoPluginManifestV2, ...]:
    return tuple(_manifest(algo_code) for algo_code in sorted(_ALGO_FACTS))


def _validate_schema(schema: Any, value: Mapping[str, Any], contract: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise ValueError(f"{contract} must be a strict object")
    errors = sorted(
        Draft202012Validator(thaw_json_v1(schema)).iter_errors(value),
        key=lambda item: (tuple(str(part) for part in item.absolute_path), item.message),
    )
    if errors:
        rendered = str(
            [{"path": [str(part) for part in item.absolute_path], "message": item.message} for item in errors[:32]]
        )
        raise ValueError(f"{contract} validation failed: {rendered}")
    return dict(value)


def validate_current_three_config_v2(manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]) -> Any:
    config = _validate_schema(manifest.config_schema, value, f"{manifest.algo_code} config")
    if manifest.algo_code in ("BEST_LIMIT_MINIQMT", "TWAP_LITE_MINIQMT") and any(
        type(item) is not int for item in config.values()
    ):
        raise ValueError("numeric plugin config fields must be strict integers")
    if manifest.algo_code == "BEST_LIMIT_MINIQMT" and config["max_volume"] < config["min_volume"]:
        raise ValueError("BEST_LIMIT_MINIQMT requires max_volume >= min_volume")
    if manifest.algo_code == "TWAP_LITE_MINIQMT" and config["time"] < config["interval"]:
        raise ValueError("TWAP_LITE_MINIQMT requires time >= interval")
    return freeze_json_v1(config)


def _canonical_decimal(value: Any, *, positive: bool) -> bool:
    if type(value) is not str or not value or value != value.strip():
        return False
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return False
    if not parsed.is_finite() or parsed < 0 or (positive and parsed == 0):
        return False
    normalized = format(parsed, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return value == ("0" if normalized in ("", "-0") else normalized)


def _trim_stable_identity(value: Any) -> bool:
    return type(value) is str and bool(value) and value == value.strip()


def _validate_lineage(lineage: Any, *, field_name: str) -> None:
    if lineage is None:
        return
    for identity_field in ("market_data_id", "event_id"):
        if not _trim_stable_identity(lineage[identity_field]):
            raise ValueError(f"{field_name}.{identity_field} must be a trim-stable identity")
    if type(lineage["generation"]) is not int or type(lineage["sequence"]) is not int:
        raise ValueError(f"{field_name} generation and sequence must be strict integers")
    try:
        canonical_time = canonical_utc_datetime_v1(
            lineage["exchange_time_utc"], field_name=f"{field_name}.exchange_time_utc"
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name}.exchange_time_utc is invalid") from exc
    if lineage["exchange_time_utc"] != canonical_time:
        raise ValueError(f"{field_name}.exchange_time_utc must be canonical UTC")


def validate_current_three_state_v2(manifest: ExecutionAlgoPluginManifestV2, value: Mapping[str, Any]) -> Any:
    state = _validate_schema(manifest.state_schema, value, f"{manifest.algo_code} state")
    common_integer_fields = (
        "parent_quantity",
        "min_volume",
        "volume_increment",
        "traded_quantity",
    )
    if any(type(state[field]) is not int for field in common_integer_fields):
        raise ValueError("durable share fields must be strict integers")
    if not _canonical_decimal(state["limit_price_decimal"], positive=True):
        raise ValueError("limit_price_decimal must be a positive canonical decimal")
    if not _canonical_decimal(state["traded_price_decimal"], positive=False):
        raise ValueError("traded_price_decimal must be a non-negative canonical decimal")
    if state["traded_quantity"] > state["parent_quantity"]:
        raise ValueError("traded_quantity exceeds parent_quantity")
    if (state["traded_quantity"] == 0) != (state["traded_price_decimal"] == "0"):
        raise ValueError("traded price and quantity zero closure failed")
    if (state["status"] == "FINISHED") != bool(state["finished_reason"]):
        raise ValueError("FINISHED status requires and exclusively owns finished_reason")
    if not _trim_stable_identity(state["algo_name"]):
        raise ValueError("algo_name must be a trim-stable identity")
    if state["finished_reason"] is not None and not _trim_stable_identity(state["finished_reason"]):
        raise ValueError("finished_reason must be a trim-stable identity")
    _validate_lineage(state["last_tick_lineage"], field_name="last_tick_lineage")
    active_ids: list[str] = []
    active_remaining = 0
    for active in state["active_orders"]:
        if active["status"] in {"CANCELLED", "FILLED", "REJECTED"}:
            raise ValueError("inactive child must not remain in active_orders")
        if (
            type(active["requested_quantity"]) is not int
            or type(active["cumulative_filled_quantity"]) is not int
            or type(active["remaining_quantity"]) is not int
        ):
            raise ValueError("active child quantity fields must be strict integers")
        if active["cumulative_filled_quantity"] > active["requested_quantity"]:
            raise ValueError("active child cumulative quantity exceeds requested quantity")
        if active["remaining_quantity"] != active["requested_quantity"] - active["cumulative_filled_quantity"]:
            raise ValueError("active child remaining quantity does not close over requested and filled quantity")
        if active["symbol"] != state["symbol"] or active["side"] != state["side"]:
            raise ValueError("active child symbol and side must equal frozen algo state")
        if not _canonical_decimal(active["requested_price_decimal"], positive=True):
            raise ValueError("active child requested price must be a positive canonical decimal")
        for identity_field in (
            "local_vt_orderid",
            "submit_command_id",
            "broker_order_id",
            "last_order_event_id",
            "last_trade_event_id",
        ):
            if active[identity_field] is not None and not _trim_stable_identity(active[identity_field]):
                raise ValueError(f"active child {identity_field} must be a trim-stable identity")
        expected = hash_hex_v1(
            "miniqmt_plugin_active_order_state_v1",
            {key: item for key, item in active.items() if key != "mapping_sha256"},
        )
        if active["mapping_sha256"] != expected:
            raise ValueError("active child mapping_sha256 mismatch")
        active_ids.append(active["local_vt_orderid"])
        active_remaining += active["remaining_quantity"]
    if active_ids != sorted(set(active_ids)):
        raise ValueError("active_orders must have unique ascending local_vt_orderid")
    if active_remaining > state["parent_quantity"] - state["traded_quantity"]:
        raise ValueError("active child remaining quantity exceeds parent remaining quantity")
    if manifest.algo_code in ("SNIPER_MINIQMT", "BEST_LIMIT_MINIQMT"):
        vt_orderid = state["vt_orderid"]
        if (vt_orderid is None and active_ids) or (vt_orderid is not None and active_ids != [vt_orderid]):
            raise ValueError("vt_orderid must exactly identify the sole active child")
    if manifest.algo_code in ("SNIPER_MINIQMT", "TWAP_LITE_MINIQMT") and any(
        active["requested_price_decimal"] != state["limit_price_decimal"] for active in state["active_orders"]
    ):
        raise ValueError(f"{manifest.algo_code} active child price must equal the frozen limit price")
    if manifest.algo_code == "BEST_LIMIT_MINIQMT":
        if type(state["next_draw_ordinal"]) is not int:
            raise ValueError("next_draw_ordinal must be a strict integer")
        if state["parameters"]["max_volume"] < state["parameters"]["min_volume"]:
            raise ValueError("BestLimit parameter range is invalid")
        if state["variables"] != {
            "vt_orderid": state["vt_orderid"],
            "order_price_decimal": state["order_price_decimal"],
            "next_draw_ordinal": state["next_draw_ordinal"],
        }:
            raise ValueError("BestLimit durable variables do not close over exact state")
        if (state["vt_orderid"] is None) != (state["order_price_decimal"] is None):
            raise ValueError("BestLimit active order price identity is incomplete")
        if state["order_price_decimal"] is not None:
            if not _canonical_decimal(state["order_price_decimal"], positive=True):
                raise ValueError("BestLimit active order price must be a positive canonical decimal")
            if state["order_price_decimal"] != state["active_orders"][0]["requested_price_decimal"]:
                raise ValueError("BestLimit state price must equal the exact active child price")
    if manifest.algo_code == "TWAP_LITE_MINIQMT":
        twap_integer_fields = (
            "duration_seconds",
            "interval_seconds",
            "order_volume",
            "active_elapsed_seconds",
            "interval_elapsed_seconds",
        )
        if any(type(state[field]) is not int for field in twap_integer_fields):
            raise ValueError("TWAP durable counters must be strict integers")
        duration = state["duration_seconds"]
        interval = state["interval_seconds"]
        if duration < interval or state["active_elapsed_seconds"] > duration:
            raise ValueError("TWAP duration closure is invalid")
        if state["interval_elapsed_seconds"] >= interval:
            raise ValueError("TWAP interval elapsed must remain below interval")
        if state["order_volume"] > state["parent_quantity"]:
            raise ValueError("TWAP order_volume exceeds parent quantity")
        if state["order_volume"] > 0:
            board_lot_valid = (
                state["order_volume"] >= state["min_volume"] and state["order_volume"] % state["volume_increment"] == 0
            )
            whole_sell_residual = state["side"] == "SELL" and state["order_volume"] == state["parent_quantity"]
            if not board_lot_valid and not whole_sell_residual:
                raise ValueError("TWAP order_volume violates frozen board-lot closure")
        if (state["active_elapsed_seconds"] == 0) != (state["last_timer_occurrence_id"] is None):
            raise ValueError("TWAP timer occurrence identity does not close over elapsed state")
        if state["active_elapsed_seconds"] == duration and state["status"] != "FINISHED":
            raise ValueError("TWAP duration exhaustion requires explicit FINISHED terminal state")
        _validate_lineage(state["last_market_data_lineage"], field_name="last_market_data_lineage")
        if state["last_market_data_lineage"] != state["last_tick_lineage"]:
            raise ValueError("TWAP durable latest market data lineage must equal last tick lineage")
        if state["parameters"] != {"time": duration, "interval": interval}:
            raise ValueError("TWAP parameters do not close over duration and interval")
        expected_variables = {
            "order_volume": state["order_volume"],
            "active_elapsed_seconds": state["active_elapsed_seconds"],
            "interval_elapsed_seconds": state["interval_elapsed_seconds"],
            "last_timer_occurrence_id": state["last_timer_occurrence_id"],
            "last_market_data_lineage": state["last_market_data_lineage"],
        }
        if state["variables"] != expected_variables:
            raise ValueError("TWAP durable variables do not close over exact state")
    return freeze_json_v1(state)


def current_three_descriptors_v2() -> tuple[PluginRegistrationDescriptorV2, ...]:
    descriptors = []
    for manifest in current_three_manifests_v2():
        _, factory, _, _, _ = _ALGO_FACTS[manifest.algo_code]
        prefix = manifest.plugin_id
        descriptors.append(
            PluginRegistrationDescriptorV2(
                schema_version="plugin_registration_descriptor_v2",
                manifest=manifest,
                factory_binding_id=f"{prefix}.factory",
                factory_callable_ref=callable_ref_v1(factory),
                factory_signature_sha256=callable_signature_sha256_v1(factory),
                config_validator_binding_id=f"{prefix}.config_validator",
                config_validator_callable_ref=callable_ref_v1(validate_current_three_config_v2),
                config_validator_signature_sha256=callable_signature_sha256_v1(validate_current_three_config_v2),
                state_codec_binding_id=f"{prefix}.state_codec",
                state_codec_callable_ref=callable_ref_v1(validate_current_three_state_v2),
                state_codec_signature_sha256=callable_signature_sha256_v1(validate_current_three_state_v2),
            )
        )
    return tuple(descriptors)


def current_three_creation_bindings_v1() -> tuple[PluginCreationBindingV1, ...]:
    return tuple(
        PluginCreationBindingV1(algo_code=manifest.algo_code, plugin_key=descriptor.plugin_key)
        for manifest, descriptor in zip(current_three_manifests_v2(), current_three_descriptors_v2(), strict=True)
    )


def current_three_process_bindings_v2() -> PluginProcessBindingsV2:
    bindings: dict[str, Any] = {}
    for manifest in current_three_manifests_v2():
        _, factory, _, _, _ = _ALGO_FACTS[manifest.algo_code]
        prefix = manifest.plugin_id
        bindings[f"{prefix}.factory"] = factory
        bindings[f"{prefix}.config_validator"] = validate_current_three_config_v2
        bindings[f"{prefix}.state_codec"] = validate_current_three_state_v2
    return PluginProcessBindingsV2(bindings)


class LegacyProjectionDriftV1(StrEnum):
    NO_DRIFT = "NO_DRIFT"
    ALIAS_EQUIVALENT = "ALIAS_EQUIVALENT"
    DRIFT_REQUIRES_EXPLICIT_POLICY_MIGRATION = "DRIFT_REQUIRES_EXPLICIT_POLICY_MIGRATION"
    CONFLICT = "CONFLICT"
    INVALID_INPUT_VISIBLE = "INVALID_INPUT_VISIBLE"


class LegacyProjectionObservationV1(FrozenStrictModel):
    field: str
    kind: str
    value: FrozenJsonFieldV1
    value_sha256: Sha256V1

    @classmethod
    def create(cls, *, field: str, kind: str, value: Any) -> Self:
        safe = json_safe_evidence_v1(value)
        return cls(
            field=field,
            kind=kind,
            value=safe,
            value_sha256=hash_hex_v1("miniqmt_legacy_projection_observation_value_v1", safe),
        )

    def sort_key_v1(self) -> tuple[str, str, str]:
        return (self.field, self.kind, self.value_sha256)


class LegacyVnpyPolicyProjectionV1(FrozenStrictModel):
    schema_version: Literal["legacy_vnpy_policy_projection_v1"]
    algo_code: str
    raw_legacy_config: FrozenJsonFieldV1
    raw_config_sha256: Sha256V1
    legacy_effective_config: FrozenJsonFieldV1
    candidate_canonical_config: FrozenJsonFieldV1
    adapter_runtime_controls: FrozenJsonFieldV1
    unknown_fields: tuple[LegacyProjectionObservationV1, ...]
    alias_observations: tuple[LegacyProjectionObservationV1, ...]
    invalid_fields: tuple[LegacyProjectionObservationV1, ...]
    drift_classification: LegacyProjectionDriftV1
    observation_only: Literal[True]
    runtime_effect_applied: Literal[False]
    projection_sha256: Sha256V1
    receipt_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_hashes(self) -> Self:
        for name in ("unknown_fields", "alias_observations", "invalid_fields"):
            ordered = tuple(sorted(getattr(self, name), key=lambda item: item.sort_key_v1()))
            if getattr(self, name) != ordered:
                raise ValueError(f"{name} must be stable sorted")
        payload = self.canonical_payload_v1(
            exclude={"projection_sha256", "receipt_sha256", "observation_only", "runtime_effect_applied"}
        )
        if self.projection_sha256 != hash_hex_v1("miniqmt_legacy_policy_projection_v1", payload):
            raise ValueError("legacy projection hash mismatch")
        receipt = self.canonical_payload_v1(exclude={"receipt_sha256"})
        if self.receipt_sha256 != hash_hex_v1("miniqmt_legacy_policy_projection_receipt_v1", receipt):
            raise ValueError("legacy projection receipt hash mismatch")
        return self


_CONTROL_FIELDS = {
    "timer_iterations",
    "time_in_force_seconds",
    "max_cancel_replace",
    "marketable_limit_cross_ticks",
    "marketable_limit_protection_band_pct",
    "price_tick",
}
_PLUGIN_FIELDS = {
    "SNIPER_MINIQMT": {"price_mode"},
    "BEST_LIMIT_MINIQMT": {"min_volume", "max_volume"},
    "TWAP_LITE_MINIQMT": {"time", "interval"},
}
_LEGACY_DEFAULTS = {
    "SNIPER_MINIQMT": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
    "BEST_LIMIT_MINIQMT": {"min_volume": 100, "max_volume": 1000},
    "TWAP_LITE_MINIQMT": {"time": 600, "interval": 60},
}


def _invalid_value(value: Any) -> bool:
    if type(value) is bool:
        return True
    if isinstance(value, float) and not math.isfinite(value):
        return True
    return type(value) is str and not value.strip()


def _legacy_effective_config_v1(
    *,
    algo_code: str,
    raw_config: dict[Any, Any],
    invalid: list[LegacyProjectionObservationV1],
) -> dict[Any, Any]:
    effective: dict[Any, Any] = dict(_LEGACY_DEFAULTS[algo_code])
    effective.update(raw_config)
    numeric_fields: tuple[str, ...]
    if algo_code == "BEST_LIMIT_MINIQMT":
        numeric_fields = ("min_volume", "max_volume")
    elif algo_code == "TWAP_LITE_MINIQMT":
        if "time" not in effective and "duration_seconds" in effective:
            effective["time"] = effective["duration_seconds"]
        if "interval" not in effective and "interval_seconds" in effective:
            effective["interval"] = effective["interval_seconds"]
        numeric_fields = ("time", "interval")
    else:
        numeric_fields = ()
    for field in numeric_fields:
        try:
            effective[field] = int(effective[field])
        except (TypeError, ValueError, OverflowError) as exc:
            invalid.append(
                LegacyProjectionObservationV1.create(
                    field=field,
                    kind="LEGACY_NORMALIZATION_ERROR",
                    value={"value": effective.get(field), "error_type": type(exc).__name__},
                )
            )
    return effective


def project_legacy_vnpy_policy_v1(algo_code: str, raw_legacy_config: Mapping[Any, Any]) -> LegacyVnpyPolicyProjectionV1:
    if algo_code not in _ALGO_FACTS or not isinstance(raw_legacy_config, Mapping):
        raise ValueError("legacy projection requires a current-three algo and mapping")
    raw_dict = dict(raw_legacy_config)
    safe_raw = json_safe_evidence_v1(raw_dict)
    known = _PLUGIN_FIELDS[algo_code]
    aliases = {"duration_seconds": "time", "interval_seconds": "interval"} if algo_code == "TWAP_LITE_MINIQMT" else {}
    unknown: list[LegacyProjectionObservationV1] = []
    alias_observations: list[LegacyProjectionObservationV1] = []
    invalid: list[LegacyProjectionObservationV1] = []
    controls: dict[str, Any] = {}
    for field, value in raw_dict.items():
        field_name = (
            field
            if type(field) is str
            else "__non_string_key__:" + hash_hex_v1("miniqmt_legacy_non_string_key_v1", json_safe_evidence_v1(field))
        )
        if field in _CONTROL_FIELDS:
            controls[str(field)] = value
        elif field not in known and field not in aliases:
            unknown.append(LegacyProjectionObservationV1.create(field=field_name, kind="UNKNOWN_FIELD", value=value))
        if _invalid_value(value):
            invalid.append(LegacyProjectionObservationV1.create(field=field_name, kind="INVALID_VALUE", value=value))
    conflict = False
    alias_only_drift = False
    alias_equivalent = False
    candidate_input = dict(_LEGACY_DEFAULTS[algo_code])
    candidate_input.update({field: raw_dict[field] for field in known if field in raw_dict})
    legacy_effective = _legacy_effective_config_v1(
        algo_code=algo_code,
        raw_config=raw_dict,
        invalid=invalid,
    )
    for alias, canonical in aliases.items():
        if alias not in raw_dict:
            continue
        if canonical in raw_dict:
            same = raw_dict[alias] == raw_dict[canonical] and type(raw_dict[alias]) is type(raw_dict[canonical])
            kind = "ALIAS_CANONICAL_EQUIVALENT" if same else "ALIAS_CANONICAL_CONFLICT"
            conflict |= not same
            alias_equivalent |= same
        else:
            kind = "ALIAS_ONLY"
            candidate_input[canonical] = raw_dict[alias]
            same_as_legacy = type(raw_dict[alias]) is type(legacy_effective.get(canonical)) and raw_dict[
                alias
            ] == legacy_effective.get(canonical)
            alias_equivalent |= same_as_legacy
            alias_only_drift |= not same_as_legacy
        alias_observations.append(
            LegacyProjectionObservationV1.create(
                field=alias,
                kind=kind,
                value={
                    "alias_value": raw_dict[alias],
                    "canonical_field": canonical,
                    "canonical_value": raw_dict.get(canonical),
                },
            )
        )
    candidate: Any = None
    manifest = next(item for item in current_three_manifests_v2() if item.algo_code == algo_code)
    if not conflict and not invalid:
        try:
            candidate = thaw_json_v1(validate_current_three_config_v2(manifest, candidate_input))
        except ValueError as exc:
            invalid.append(
                LegacyProjectionObservationV1.create(
                    field="__candidate__", kind="STRICT_CONFIG_INVALID", value=str(exc)
                )
            )
    if conflict:
        drift = LegacyProjectionDriftV1.CONFLICT
    elif invalid or unknown:
        drift = LegacyProjectionDriftV1.INVALID_INPUT_VISIBLE
    elif alias_only_drift:
        drift = LegacyProjectionDriftV1.DRIFT_REQUIRES_EXPLICIT_POLICY_MIGRATION
    elif alias_equivalent:
        drift = LegacyProjectionDriftV1.ALIAS_EQUIVALENT
    else:
        drift = LegacyProjectionDriftV1.NO_DRIFT
    ordered_unknown = tuple(sorted(unknown, key=lambda item: item.sort_key_v1()))
    ordered_aliases = tuple(sorted(alias_observations, key=lambda item: item.sort_key_v1()))
    ordered_invalid = tuple(sorted(invalid, key=lambda item: item.sort_key_v1()))
    projection_payload = {
        "schema_version": "legacy_vnpy_policy_projection_v1",
        "algo_code": algo_code,
        "raw_legacy_config": safe_raw,
        "raw_config_sha256": hash_hex_v1("miniqmt_legacy_policy_raw_config_v1", safe_raw),
        "legacy_effective_config": json_safe_evidence_v1(legacy_effective),
        "candidate_canonical_config": candidate,
        "adapter_runtime_controls": json_safe_evidence_v1(controls),
        "unknown_fields": [item.canonical_payload_v1() for item in ordered_unknown],
        "alias_observations": [item.canonical_payload_v1() for item in ordered_aliases],
        "invalid_fields": [item.canonical_payload_v1() for item in ordered_invalid],
        "drift_classification": drift.value,
    }
    projection_sha256 = hash_hex_v1("miniqmt_legacy_policy_projection_v1", projection_payload)
    receipt_payload = {
        **projection_payload,
        "observation_only": True,
        "runtime_effect_applied": False,
        "projection_sha256": projection_sha256,
    }
    return LegacyVnpyPolicyProjectionV1(
        **{
            **receipt_payload,
            "drift_classification": drift,
            "unknown_fields": ordered_unknown,
            "alias_observations": ordered_aliases,
            "invalid_fields": ordered_invalid,
        },
        receipt_sha256=hash_hex_v1("miniqmt_legacy_policy_projection_receipt_v1", receipt_payload),
    )


__all__ = [
    "CURRENT_THREE_BEHAVIOR_CHARACTERIZATIONS_V2",
    "LegacyProjectionDriftV1",
    "LegacyVnpyPolicyProjectionV1",
    "current_three_creation_bindings_v1",
    "current_three_descriptors_v2",
    "current_three_manifests_v2",
    "current_three_process_bindings_v2",
    "project_legacy_vnpy_policy_v1",
    "validate_current_three_config_v2",
    "validate_current_three_state_v2",
]
