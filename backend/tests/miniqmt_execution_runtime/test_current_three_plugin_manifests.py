from __future__ import annotations

import hashlib
import inspect
import json
import math
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType

import pytest

from backend.execution_algos.vnpy_style import registry as legacy_registry
from backend.execution_algos.vnpy_style import plugin_manifests as manifest_module
from backend.execution_algos.vnpy_style.models import VnpyActionType, VnpyTick
from backend.execution_algos.vnpy_style.plugin_manifests import (
    CURRENT_THREE_BEHAVIOR_CHARACTERIZATIONS_V2,
    LegacyProjectionDriftV1,
    current_three_creation_bindings_v1,
    current_three_descriptors_v2,
    current_three_manifests_v2,
    project_legacy_vnpy_policy_v1,
    validate_current_three_config_v2,
    validate_current_three_state_v2,
)
from backend.services.miniqmt_execution_runtime.deterministic_context import best_limit_quantity_v1
from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    FrozenJsonArrayV1,
    FrozenJsonObjectV1,
    canonical_json_bytes_v1,
    hash_hex_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AbsenceDispositionV1,
    DeterministicExecutionContextV1,
    EventTypeV2,
    MarketDataCapabilityV1,
    PluginProviderV2,
    SessionPhaseV1,
    SideV1,
)

EXPECTED = {
    "SNIPER_MINIQMT": "aistock.vnpy.sniper",
    "BEST_LIMIT_MINIQMT": "aistock.vnpy.best_limit",
    "TWAP_LITE_MINIQMT": "aistock.vnpy.twap_lite",
}


def _manifest(algo_code: str):
    return next(item for item in current_three_manifests_v2() if item.algo_code == algo_code)


def _behavior_characterization(algo_code: str) -> dict[str, object]:
    return thaw_json_v1(CURRENT_THREE_BEHAVIOR_CHARACTERIZATIONS_V2)[algo_code]


def _lineage() -> dict[str, object]:
    return {
        "market_data_id": "md_1",
        "event_id": "mqrtevt_1",
        "payload_sha256": "a" * 64,
        "generation": 1,
        "sequence": 2,
        "exchange_time_utc": "2026-07-23T02:00:00.000000Z",
        "session_phase": "CONTINUOUS_AM",
    }


def _active_order(
    *,
    symbol: str = "600000.SH",
    side: str = "BUY",
    price: str = "10",
    requested_quantity: int = 300,
    cumulative_filled_quantity: int = 100,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "parent_intent_id": "parent_1",
        "local_vt_orderid": "vord_1",
        "submit_command_id": "mqcommand_1",
        "broker_order_id": "broker_1",
        "symbol": symbol,
        "side": side,
        "status": "PARTIALLY_FILLED",
        "requested_price_decimal": price,
        "requested_quantity": requested_quantity,
        "cumulative_filled_quantity": cumulative_filled_quantity,
        "remaining_quantity": requested_quantity - cumulative_filled_quantity,
        "last_order_event_id": "mqrtevt_order_1",
        "last_trade_event_id": "mqrtevt_trade_1",
        "market_data_lineage": _lineage(),
    }
    payload["mapping_sha256"] = hash_hex_v1("miniqmt_plugin_active_order_state_v1", payload)
    return payload


def _state(algo_code: str) -> dict[str, object]:
    state: dict[str, object] = {
        "algo_name": f"{algo_code}_stable",
        "algo_code": algo_code,
        "parent_intent_id": "parent_1",
        "symbol": "600000.SH",
        "side": "BUY",
        "offset": "NONE",
        "limit_price_decimal": "10",
        "parent_quantity": 1000,
        "min_volume": 100,
        "volume_increment": 100,
        "status": "RUNNING",
        "traded_quantity": 0,
        "traded_price_decimal": "0",
        "active_orders": [],
        "parameters": {},
        "variables": {},
        "last_tick_lineage": _lineage(),
        "finished_reason": None,
    }
    if algo_code == "SNIPER_MINIQMT":
        state.update({"vt_orderid": None, "variables": {"vt_orderid": None}})
    elif algo_code == "BEST_LIMIT_MINIQMT":
        state.update(
            {
                "vt_orderid": None,
                "order_price_decimal": None,
                "next_draw_ordinal": 0,
                "parameters": {"max_volume": 1000, "min_volume": 100},
                "variables": {"next_draw_ordinal": 0, "order_price_decimal": None, "vt_orderid": None},
            }
        )
    else:
        state.update(
            {
                "duration_seconds": 600,
                "interval_seconds": 60,
                "order_volume": 100,
                "active_elapsed_seconds": 120,
                "interval_elapsed_seconds": 0,
                "last_timer_occurrence_id": "mqtimerocc_2",
                "last_market_data_lineage": _lineage(),
                "parameters": {"interval": 60, "time": 600},
                "variables": {
                    "active_elapsed_seconds": 120,
                    "interval_elapsed_seconds": 0,
                    "last_market_data_lineage": _lineage(),
                    "last_timer_occurrence_id": "mqtimerocc_2",
                    "order_volume": 100,
                },
            }
        )
    return state


def test_current_three_identity_hash_and_creation_binding_closure() -> None:
    manifests = current_three_manifests_v2()
    assert {item.algo_code: item.plugin_id for item in manifests} == EXPECTED
    assert {item.plugin_version for item in manifests} == {"2.0.0"}
    assert {item.algo_code for item in manifests} == set(legacy_registry.VNPY_STYLE_ASSETS)
    for manifest in manifests:
        assert manifest.config_schema_sha256 == hash_hex_v1(
            "miniqmt_plugin_config_schema_v1", thaw_json_v1(manifest.config_schema)
        )
        assert manifest.state_schema_sha256 == hash_hex_v1(
            "miniqmt_plugin_state_schema_v1", thaw_json_v1(manifest.state_schema)
        )
        assert manifest.manifest_sha256 == manifest.model_validate_json(manifest.model_dump_json()).manifest_sha256
    assert {item.algo_code: item.plugin_key.manifest_sha256 for item in current_three_creation_bindings_v1()} == {
        item.algo_code: item.manifest_sha256 for item in manifests
    }


def test_code_owned_behavior_characterizations_are_recursively_immutable() -> None:
    before = canonical_json_bytes_v1([item.canonical_payload_v1() for item in current_three_manifests_v2()])
    authority = CURRENT_THREE_BEHAVIOR_CHARACTERIZATIONS_V2
    assert isinstance(authority, FrozenJsonObjectV1)

    with pytest.raises(TypeError):
        authority[0] = authority[0]  # type: ignore[index]
    twap = next(member.value for member in authority if member.key == "TWAP_LITE_MINIQMT")
    assert isinstance(twap, FrozenJsonObjectV1)
    with pytest.raises(TypeError):
        twap[0] = twap[0]  # type: ignore[index]
    counted = next(member.value for member in twap if member.key == "counted_session_phases")
    assert isinstance(counted, FrozenJsonArrayV1)
    with pytest.raises(AttributeError):
        counted.append("CLOSED")  # type: ignore[attr-defined]

    after = canonical_json_bytes_v1([item.canonical_payload_v1() for item in current_three_manifests_v2()])
    assert after == before

    script = """
from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v2
from backend.services.miniqmt_execution_runtime.plugin_canonical import canonical_json_bytes_v1
import sys
sys.stdout.buffer.write(canonical_json_bytes_v1([item.canonical_payload_v1() for item in current_three_manifests_v2()]))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[3],
        check=True,
        capture_output=True,
    )
    assert result.stdout == after


def test_all_code_owned_manifest_build_tables_are_immutable() -> None:
    assert isinstance(manifest_module._UPSTREAM_HASHES, MappingProxyType)
    assert isinstance(manifest_module._ALGO_FACTS, MappingProxyType)
    assert isinstance(manifest_module._PROCESS_VALIDATOR_FACTS, MappingProxyType)
    assert isinstance(manifest_module._PLUGIN_FIELDS, MappingProxyType)
    assert isinstance(manifest_module._LEGACY_DEFAULTS, FrozenJsonObjectV1)
    assert isinstance(manifest_module._CONTROL_FIELDS, frozenset)
    assert all(isinstance(fields, frozenset) for fields in manifest_module._PLUGIN_FIELDS.values())
    assert not any(callable(value) for facts in manifest_module._ALGO_FACTS.values() for value in facts)

    with pytest.raises(TypeError):
        manifest_module._UPSTREAM_HASHES["unexpected"] = "f" * 64  # type: ignore[index]
    with pytest.raises(TypeError):
        manifest_module._ALGO_FACTS["unexpected"] = ()  # type: ignore[index]
    with pytest.raises(TypeError):
        manifest_module._PROCESS_VALIDATOR_FACTS["unexpected"] = "forged"  # type: ignore[index]
    with pytest.raises(TypeError):
        manifest_module._PLUGIN_FIELDS["SNIPER_MINIQMT"] = frozenset()  # type: ignore[index]
    with pytest.raises(AttributeError):
        manifest_module._PLUGIN_FIELDS["SNIPER_MINIQMT"].add("unexpected")  # type: ignore[attr-defined]


def _state_with_schema_violations(count: int) -> dict[str, object]:
    state = _state("TWAP_LITE_MINIQMT")
    active_orders: list[dict[str, object]] = []
    for index in range(count):
        active = _active_order()
        active["local_vt_orderid"] = f"vord_{index:04d}"
        active["submit_command_id"] = f"mqcommand_{index:04d}"
        active["broker_order_id"] = f"broker_{index:04d}"
        active["last_order_event_id"] = f"mqrtevt_order_{index:04d}"
        active["last_trade_event_id"] = f"mqrtevt_trade_{index:04d}"
        active["mapping_sha256"] = hash_hex_v1(
            "miniqmt_plugin_active_order_state_v1",
            {key: value for key, value in active.items() if key != "mapping_sha256"},
        )
        active["unexpected_schema_field"] = index
        active_orders.append(active)
    state["active_orders"] = active_orders
    return state


def _schema_failure_context(count: int) -> dict[str, object]:
    with pytest.raises(ValueError) as error:
        validate_current_three_state_v2(
            _manifest("TWAP_LITE_MINIQMT"),
            _state_with_schema_violations(count),
        )
    context = getattr(error.value, "context", None)
    assert isinstance(context, dict)
    json.dumps(context, allow_nan=False)
    return context


def test_current_three_schema_failures_are_bounded_explicit_and_stable() -> None:
    below = _schema_failure_context(31)
    assert below["violations_truncated"] is False
    assert below["retained_violation_count"] == 31
    assert below["observed_violation_count_lower_bound"] == 31

    exact = _schema_failure_context(32)
    assert exact["violations_truncated"] is False
    assert exact["retained_violation_count"] == 32
    assert exact["observed_violation_count_lower_bound"] == 32

    over = _schema_failure_context(500)
    assert over["violations_truncated"] is True
    assert over["retained_violation_count"] == 32
    assert over["observed_violation_count_lower_bound"] == 33
    assert len(over["ordered_violations"]) == 32  # type: ignore[arg-type]
    assert len(json.dumps(over, sort_keys=True)) < 20_000
    assert over == _schema_failure_context(500)

    valid = _state("TWAP_LITE_MINIQMT")
    assert thaw_json_v1(validate_current_three_state_v2(_manifest("TWAP_LITE_MINIQMT"), valid)) == valid


def test_current_three_are_aistock_derived_and_require_exact_delivery_callbacks() -> None:
    required_callbacks = {"update_tick", "update_timer", "update_order", "update_trade"}
    for manifest in current_three_manifests_v2():
        assert manifest.provider is PluginProviderV2.AISTOCK_DERIVED
        assert required_callbacks.issubset(manifest.required_facade_methods)
        assert manifest.compatibility_requirement.mode == "DERIVED_SOURCE_EXACT_CHARACTERIZATION"


@pytest.mark.parametrize(
    ("algo_code", "config"),
    [
        ("SNIPER_MINIQMT", {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}),
        ("BEST_LIMIT_MINIQMT", {"min_volume": 100, "max_volume": 1000}),
        ("TWAP_LITE_MINIQMT", {"time": 600, "interval": 60}),
    ],
)
def test_strict_configs_accept_canonical_fields(algo_code: str, config: dict[str, object]) -> None:
    assert thaw_json_v1(validate_current_three_config_v2(_manifest(algo_code), config)) == config


@pytest.mark.parametrize(
    ("algo_code", "config"),
    [
        ("SNIPER_MINIQMT", {"price_mode": "OTHER"}),
        ("SNIPER_MINIQMT", {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE", "unknown": 1}),
        ("BEST_LIMIT_MINIQMT", {"min_volume": True, "max_volume": 1000}),
        ("BEST_LIMIT_MINIQMT", {"min_volume": 100.0, "max_volume": 1000}),
        ("BEST_LIMIT_MINIQMT", {"min_volume": 1000, "max_volume": 100}),
        ("TWAP_LITE_MINIQMT", {"time": 60, "interval": 600}),
        ("TWAP_LITE_MINIQMT", {"duration_seconds": 600, "interval_seconds": 60}),
        ("TWAP_LITE_MINIQMT", {"time": True, "interval": 60}),
    ],
)
def test_strict_configs_fail_without_alias_or_coercion(algo_code: str, config: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        validate_current_three_config_v2(_manifest(algo_code), config)


def test_market_requirements_match_actual_side_consumption() -> None:
    sniper = _manifest("SNIPER_MINIQMT")
    required = {
        (item.capability, item.applicable_sides): item.required_fields for item in sniper.market_data_requirements
    }
    assert required[(MarketDataCapabilityV1.L1_ASK, (SideV1.BUY,))] == ("price", "volume")
    assert required[(MarketDataCapabilityV1.L1_BID, (SideV1.SELL,))] == ("price", "volume")
    for algo_code in ("BEST_LIMIT_MINIQMT", "TWAP_LITE_MINIQMT"):
        assert all(item.required_fields == ("price",) for item in _manifest(algo_code).market_data_requirements)
    assert all(
        item.absence_disposition is AbsenceDispositionV1.WAIT_FOR_NEXT_VALID_EVENT
        for item in _manifest("TWAP_LITE_MINIQMT").market_data_requirements
    )


@pytest.mark.parametrize("algo_code", tuple(EXPECTED))
def test_state_codec_requires_restart_and_active_order_lineage(algo_code: str) -> None:
    state = _state(algo_code)
    assert thaw_json_v1(validate_current_three_state_v2(_manifest(algo_code), state)) == state
    state.pop("active_orders")
    with pytest.raises(ValueError):
        validate_current_three_state_v2(_manifest(algo_code), state)


def test_active_order_schema_requires_parent_and_market_data_lineage() -> None:
    state_schema = thaw_json_v1(_manifest("BEST_LIMIT_MINIQMT").state_schema)
    active_order_schema = state_schema["properties"]["active_orders"]["items"]

    assert {"parent_intent_id", "market_data_lineage"}.issubset(active_order_schema["required"])


def test_state_codec_rejects_cross_field_restart_conflicts() -> None:
    malformed_states: list[tuple[str, dict[str, object]]] = []

    noncanonical = _state("SNIPER_MINIQMT")
    noncanonical["limit_price_decimal"] = "10.00"
    malformed_states.append(("SNIPER_MINIQMT", noncanonical))

    overfilled = _state("SNIPER_MINIQMT")
    overfilled["traded_quantity"] = 1001
    overfilled["traded_price_decimal"] = "10"
    malformed_states.append(("SNIPER_MINIQMT", overfilled))

    terminal_child = _state("SNIPER_MINIQMT")
    terminal_child["vt_orderid"] = "vord_1"
    terminal_child["variables"] = {"vt_orderid": "vord_1"}
    terminal_child["active_orders"] = [
        {
            "local_vt_orderid": "vord_1",
            "submit_command_id": "command_1",
            "broker_order_id": None,
            "status": "FILLED",
            "requested_price_decimal": "10",
            "requested_quantity": 100,
            "cumulative_filled_quantity": 100,
            "last_order_event_id": "event_1",
            "last_trade_event_id": "event_2",
            "mapping_sha256": "a" * 64,
        }
    ]
    malformed_states.append(("SNIPER_MINIQMT", terminal_child))

    best_variables = _state("BEST_LIMIT_MINIQMT")
    best_variables["variables"] = {"next_draw_ordinal": 1, "order_price_decimal": None, "vt_orderid": None}
    malformed_states.append(("BEST_LIMIT_MINIQMT", best_variables))

    twap_interval = _state("TWAP_LITE_MINIQMT")
    twap_interval["interval_elapsed_seconds"] = 60
    twap_interval["variables"] = {
        **twap_interval["variables"],  # type: ignore[arg-type]
        "interval_elapsed_seconds": 60,
    }
    malformed_states.append(("TWAP_LITE_MINIQMT", twap_interval))

    for algo_code, state in malformed_states:
        with pytest.raises(ValueError):
            validate_current_three_state_v2(_manifest(algo_code), state)


def test_best_limit_active_order_closes_price_quantity_symbol_side_and_lineage() -> None:
    state = _state("BEST_LIMIT_MINIQMT")
    active = _active_order()
    state.update(
        {
            "active_orders": [active],
            "traded_quantity": active["cumulative_filled_quantity"],
            "traded_price_decimal": active["requested_price_decimal"],
            "vt_orderid": active["local_vt_orderid"],
            "order_price_decimal": active["requested_price_decimal"],
            "variables": {
                "next_draw_ordinal": 0,
                "order_price_decimal": active["requested_price_decimal"],
                "vt_orderid": active["local_vt_orderid"],
            },
        }
    )
    assert thaw_json_v1(validate_current_three_state_v2(_manifest("BEST_LIMIT_MINIQMT"), state)) == state

    for field, value in (
        ("requested_price_decimal", "10.00"),
        ("remaining_quantity", 201),
        ("parent_intent_id", "parent_other"),
        ("symbol", "000001.SZ"),
        ("side", "SELL"),
    ):
        malformed = _state("BEST_LIMIT_MINIQMT")
        malformed_active = _active_order()
        malformed_active[field] = value
        malformed_active["mapping_sha256"] = hash_hex_v1(
            "miniqmt_plugin_active_order_state_v1",
            {key: item for key, item in malformed_active.items() if key != "mapping_sha256"},
        )
        malformed.update(
            {
                "active_orders": [malformed_active],
                "traded_quantity": malformed_active["cumulative_filled_quantity"],
                "traded_price_decimal": "10",
                "vt_orderid": malformed_active["local_vt_orderid"],
                "order_price_decimal": "10",
                "variables": {
                    "next_draw_ordinal": 0,
                    "order_price_decimal": "10",
                    "vt_orderid": malformed_active["local_vt_orderid"],
                },
            }
        )
        with pytest.raises(ValueError):
            validate_current_three_state_v2(_manifest("BEST_LIMIT_MINIQMT"), malformed)

    mismatched_price = {**state, "order_price_decimal": "11"}
    mismatched_price["variables"] = {
        "next_draw_ordinal": 0,
        "order_price_decimal": "11",
        "vt_orderid": active["local_vt_orderid"],
    }
    with pytest.raises(ValueError, match="price"):
        validate_current_three_state_v2(_manifest("BEST_LIMIT_MINIQMT"), mismatched_price)


def test_active_child_cumulative_fill_must_close_over_parent_traded_quantity() -> None:
    state = _state("BEST_LIMIT_MINIQMT")
    active = _active_order(cumulative_filled_quantity=100)
    state.update(
        {
            "active_orders": [active],
            "vt_orderid": active["local_vt_orderid"],
            "order_price_decimal": active["requested_price_decimal"],
            "variables": {
                "next_draw_ordinal": 0,
                "order_price_decimal": active["requested_price_decimal"],
                "vt_orderid": active["local_vt_orderid"],
            },
        }
    )

    with pytest.raises(ValueError, match="cumulative.*traded|parent traded"):
        validate_current_three_state_v2(_manifest("BEST_LIMIT_MINIQMT"), state)


def test_active_order_same_hash_cannot_cover_different_identity_or_market_data_payload() -> None:
    for field, value in (
        ("broker_order_id", "broker_other"),
        ("market_data_lineage", {**_lineage(), "market_data_id": "md_other"}),
    ):
        state = _state("BEST_LIMIT_MINIQMT")
        active = _active_order(cumulative_filled_quantity=100)
        active[field] = value
        state.update(
            {
                "active_orders": [active],
                "traded_quantity": 100,
                "traded_price_decimal": "10",
                "vt_orderid": active["local_vt_orderid"],
                "order_price_decimal": active["requested_price_decimal"],
                "variables": {
                    "next_draw_ordinal": 0,
                    "order_price_decimal": active["requested_price_decimal"],
                    "vt_orderid": active["local_vt_orderid"],
                },
            }
        )

        with pytest.raises(ValueError, match="mapping_sha256"):
            validate_current_three_state_v2(_manifest("BEST_LIMIT_MINIQMT"), state)


def test_sniper_active_order_price_must_equal_frozen_limit_price() -> None:
    state = _state("SNIPER_MINIQMT")
    active = _active_order(price="11")
    state.update(
        {
            "active_orders": [active],
            "traded_quantity": active["cumulative_filled_quantity"],
            "traded_price_decimal": active["requested_price_decimal"],
            "vt_orderid": active["local_vt_orderid"],
            "variables": {"vt_orderid": active["local_vt_orderid"]},
        }
    )

    with pytest.raises(ValueError, match="Sniper|limit price"):
        validate_current_three_state_v2(_manifest("SNIPER_MINIQMT"), state)


def test_state_codec_rejects_invalid_market_data_time_and_twap_terminal_drift() -> None:
    invalid_time = _state("SNIPER_MINIQMT")
    invalid_time["last_tick_lineage"] = {**_lineage(), "exchange_time_utc": "not-a-time"}
    with pytest.raises(ValueError, match="exchange_time_utc"):
        validate_current_three_state_v2(_manifest("SNIPER_MINIQMT"), invalid_time)

    exhausted = _state("TWAP_LITE_MINIQMT")
    exhausted["active_elapsed_seconds"] = exhausted["duration_seconds"]
    exhausted["variables"] = {
        **exhausted["variables"],  # type: ignore[arg-type]
        "active_elapsed_seconds": exhausted["duration_seconds"],
    }
    with pytest.raises(ValueError, match="duration|terminal|FINISHED"):
        validate_current_three_state_v2(_manifest("TWAP_LITE_MINIQMT"), exhausted)

    divergent_view = _state("TWAP_LITE_MINIQMT")
    divergent_view["last_market_data_lineage"] = {**_lineage(), "market_data_id": "md_other"}
    divergent_view["variables"] = {
        **divergent_view["variables"],  # type: ignore[arg-type]
        "last_market_data_lineage": divergent_view["last_market_data_lineage"],
    }
    with pytest.raises(ValueError, match="market data|lineage"):
        validate_current_three_state_v2(_manifest("TWAP_LITE_MINIQMT"), divergent_view)


def test_twap_behavior_is_exchange_active_timer_only_and_restart_safe() -> None:
    behavior = _behavior_characterization("TWAP_LITE_MINIQMT")
    assert behavior["duration_unit"] == behavior["interval_unit"] == "EXCHANGE_ACTIVE_SECONDS"
    assert behavior["counted_session_phases"] == ["CONTINUOUS_AM", "CONTINUOUS_PM"]
    assert behavior["non_counted_session_phases"] == ["OPEN_AUCTION", "LUNCH_BREAK", "CLOSE_AUCTION", "CLOSED"]
    assert behavior["timer_reads_wall_clock"] is False
    assert behavior["timer_market_data_source"] == "DURABLE_LATEST_MARKET_DATA_VIEW"
    assert behavior["catch_up_burst"] is False
    assert behavior["restart_replays_consumed_timer"] is False
    assert behavior["eod_outcome"] == "EXPLICIT_TERMINAL_OR_RESIDUAL_EVIDENCE"
    assert behavior["auction_native_synthesis"] is False
    assert behavior["zero_slice_diagnostic_reason"] == "TWAP_SLICE_VOLUME_ROUNDED_ZERO"


def test_behavior_characterization_vectors_match_current_core_traces() -> None:
    tick = VnpyTick(
        symbol="600000.SH",
        datetime=datetime(2026, 7, 23, 9, 30, tzinfo=UTC),
        bid_price_1=9.88,
        bid_volume_1=500,
        ask_price_1=9.99,
        ask_volume_1=250,
    )
    actual: dict[str, dict[str, object]] = {}

    sniper = legacy_registry.create_vnpy_style_core(
        algo_code="SNIPER_MINIQMT",
        symbol="600000.SH",
        side="BUY",
        price=10,
        volume=1000,
        algo_name="characterization_sniper",
    )
    sniper.start()
    sniper_submit = next(item for item in sniper.update_tick(tick) if item.action_type is VnpyActionType.SUBMIT)
    actual["SNIPER_MINIQMT"] = {
        "action_type": sniper_submit.action_type.value,
        "price_decimal": format(Decimal(str(sniper_submit.price)).normalize(), "f"),
        "quantity": sniper_submit.volume,
        "reason": sniper_submit.reason,
    }

    best = legacy_registry.create_vnpy_style_core(
        algo_code="BEST_LIMIT_MINIQMT",
        symbol="600000.SH",
        side="BUY",
        price=10,
        volume=1000,
        algo_config={"min_volume": 100, "max_volume": 500},
        algo_name="characterization_best_limit",
        random_volume_provider=lambda _minimum, _maximum: 350,
    )
    best.start()
    best_submit = next(item for item in best.update_tick(tick) if item.action_type is VnpyActionType.SUBMIT)
    actual["BEST_LIMIT_MINIQMT"] = {
        "action_type": best_submit.action_type.value,
        "price_decimal": format(Decimal(str(best_submit.price)).normalize(), "f"),
        "quantity": best_submit.volume,
        "reason": best_submit.reason,
    }

    twap = legacy_registry.create_vnpy_style_core(
        algo_code="TWAP_LITE_MINIQMT",
        symbol="600000.SH",
        side="BUY",
        price=10,
        volume=1000,
        algo_config={"time": 4, "interval": 2},
        algo_name="characterization_twap",
    )
    twap.start()
    twap.update_tick(tick)
    assert not [item for item in twap.update_timer() if item.action_type is VnpyActionType.SUBMIT]
    twap_submit = next(item for item in twap.update_timer() if item.action_type is VnpyActionType.SUBMIT)
    actual["TWAP_LITE_MINIQMT"] = {
        "action_type": twap_submit.action_type.value,
        "price_decimal": format(Decimal(str(twap_submit.price)).normalize(), "f"),
        "quantity": twap_submit.volume,
        "reason": twap_submit.reason,
    }

    for algo_code, observed in actual.items():
        characterization = _behavior_characterization(algo_code)
        assert characterization["schema_version"] == "current_three_behavior_characterization_v2"
        assert characterization["algo_code"] == algo_code
        assert characterization["trace_vectors"] == [{"vector_id": "legacy_core_primary_submit", "expected": observed}]


def test_best_limit_binds_raw_digest_u53_and_strict_ordinal() -> None:
    behavior = _behavior_characterization("BEST_LIMIT_MINIQMT")
    assert behavior["draw_formula"] == "RAW_DIGEST_U53"
    assert behavior["draw_ordinal"] == "STRICT_CONTIGUOUS_DURABLE_STATE"
    context = DeterministicExecutionContextV1.create(
        runtime_id="runtime_1",
        algo_instance_id="algo_1",
        event_id="event_1",
        delivery_id="delivery_1",
        plugin_manifest_sha256=_manifest("BEST_LIMIT_MINIQMT").manifest_sha256,
        transition_sequence=1,
        logical_time_utc="2026-07-23T02:00:00.000000Z",
        exchange_trade_date="2026-07-23",
        session_epoch="session_1",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="b" * 64,
    )
    assert isinstance(best_limit_quantity_v1(context=context, draw_ordinal=0, min_volume=100, max_volume=1000), int)


def test_source_attribution_closes_actual_files() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    for descriptor in current_three_descriptors_v2():
        assert descriptor.factory_callable_ref == descriptor.manifest.implementation_ref
        for item in descriptor.manifest.source_attribution.aistock_files:
            assert hashlib.sha256((repo_root / item.path).read_bytes()).hexdigest() == item.sha256


def test_legacy_projection_preserves_conflict_drift_unknown_and_invalid_values() -> None:
    conflict = project_legacy_vnpy_policy_v1(
        "TWAP_LITE_MINIQMT",
        {"time": 600, "duration_seconds": 1200, "interval": 60, "interval_seconds": 60},
    )
    alias_only = project_legacy_vnpy_policy_v1("TWAP_LITE_MINIQMT", {"duration_seconds": 1200, "interval_seconds": 120})
    malformed = project_legacy_vnpy_policy_v1(
        "BEST_LIMIT_MINIQMT",
        {"min_volume": True, "max_volume": math.inf, "mystery": "  ", "timer_iterations": 3},
    )
    assert conflict.drift_classification is LegacyProjectionDriftV1.CONFLICT
    assert alias_only.drift_classification is LegacyProjectionDriftV1.DRIFT_REQUIRES_EXPLICIT_POLICY_MIGRATION
    assert malformed.drift_classification is LegacyProjectionDriftV1.INVALID_INPUT_VISIBLE
    assert {item.field for item in malformed.unknown_fields} == {"mystery"}
    assert {item.field for item in malformed.invalid_fields}.issuperset({"min_volume", "max_volume", "mystery"})
    assert thaw_json_v1(malformed.adapter_runtime_controls)["timer_iterations"] == 3
    assert malformed.observation_only is True and malformed.runtime_effect_applied is False
    assert malformed.projection_sha256 != malformed.receipt_sha256


def test_legacy_projection_uses_defaults_but_never_hides_unknown_fields() -> None:
    defaulted = project_legacy_vnpy_policy_v1("SNIPER_MINIQMT", {})
    unknown = project_legacy_vnpy_policy_v1("SNIPER_MINIQMT", {"unowned_control": 1})
    assert defaulted.drift_classification is LegacyProjectionDriftV1.NO_DRIFT
    assert thaw_json_v1(defaulted.candidate_canonical_config) == {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    assert unknown.drift_classification is LegacyProjectionDriftV1.INVALID_INPUT_VISIBLE
    assert thaw_json_v1(unknown.candidate_canonical_config) == {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    assert {item.field for item in unknown.unknown_fields} == {"unowned_control"}


def test_legacy_projection_covers_equivalent_alias_and_candidate_range_failure() -> None:
    equivalent = project_legacy_vnpy_policy_v1(
        "TWAP_LITE_MINIQMT",
        {"time": 600, "duration_seconds": 600, "interval": 60, "interval_seconds": 60},
    )
    invalid_range = project_legacy_vnpy_policy_v1(
        "BEST_LIMIT_MINIQMT",
        {"min_volume": 1000, "max_volume": 100},
    )
    assert equivalent.drift_classification is LegacyProjectionDriftV1.ALIAS_EQUIVALENT
    assert invalid_range.drift_classification is LegacyProjectionDriftV1.INVALID_INPUT_VISIBLE
    assert invalid_range.candidate_canonical_config is None
    assert {item.field for item in invalid_range.invalid_fields} == {"__candidate__"}


@pytest.mark.parametrize(
    ("algo_code", "raw_config"),
    [
        ("SNIPER_MINIQMT", {"mystery": 7, "timer_iterations": 3}),
        ("BEST_LIMIT_MINIQMT", {"min_volume": "100", "max_volume": "1000", "timer_iterations": 2}),
        ("TWAP_LITE_MINIQMT", {"duration_seconds": 600, "interval_seconds": 60}),
    ],
)
def test_legacy_projection_preserves_actual_legacy_effective_config(
    algo_code: str, raw_config: dict[str, object]
) -> None:
    projection = project_legacy_vnpy_policy_v1(algo_code, raw_config)
    assert thaw_json_v1(projection.legacy_effective_config) == legacy_registry.validate_vnpy_style_config(
        algo_code, raw_config
    )
    assert projection.observation_only is True
    assert projection.runtime_effect_applied is False


def test_alias_only_default_equivalence_and_conflict_priority_are_explicit() -> None:
    equivalent = project_legacy_vnpy_policy_v1("TWAP_LITE_MINIQMT", {"duration_seconds": 600, "interval_seconds": 60})
    conflict = project_legacy_vnpy_policy_v1(
        "TWAP_LITE_MINIQMT",
        {"time": 600, "duration_seconds": 1200, "interval": 60, "unknown": 1},
    )
    assert equivalent.drift_classification is LegacyProjectionDriftV1.ALIAS_EQUIVALENT
    assert conflict.drift_classification is LegacyProjectionDriftV1.CONFLICT
    assert {item.field for item in conflict.unknown_fields} == {"unknown"}


def test_k1b_remains_shadow_only_without_runtime_wiring() -> None:
    runtime_source = inspect.getsource(
        __import__("backend.services.miniqmt_execution_runtime.runtime", fromlist=["MiniQMTExecutionRuntime"])
    )
    assert "plugin_manifests" not in runtime_source
    assert EventTypeV2.TIMER in _manifest("TWAP_LITE_MINIQMT").subscribed_event_types
