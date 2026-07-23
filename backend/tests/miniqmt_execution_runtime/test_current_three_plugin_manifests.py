from __future__ import annotations

import hashlib
import inspect
import math
from pathlib import Path

import pytest

from backend.execution_algos.vnpy_style import registry as legacy_registry
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
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AbsenceDispositionV1,
    DeterministicExecutionContextV1,
    EventTypeV2,
    MarketDataCapabilityV1,
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


def _state(algo_code: str) -> dict[str, object]:
    state: dict[str, object] = {
        "algo_name": f"{algo_code}_stable",
        "algo_code": algo_code,
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
    required = {(item.capability, item.applicable_sides): item.required_fields for item in sniper.market_data_requirements}
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


def test_twap_behavior_is_exchange_active_timer_only_and_restart_safe() -> None:
    behavior = CURRENT_THREE_BEHAVIOR_CHARACTERIZATIONS_V2["TWAP_LITE_MINIQMT"]
    assert behavior["duration_unit"] == behavior["interval_unit"] == "EXCHANGE_ACTIVE_SECONDS"
    assert behavior["counted_session_phases"] == ["CONTINUOUS_AM", "CONTINUOUS_PM"]
    assert behavior["non_counted_session_phases"] == ["OPEN_AUCTION", "LUNCH_BREAK", "CLOSE_AUCTION", "CLOSED"]
    assert behavior["timer_reads_wall_clock"] is False
    assert behavior["timer_market_data_source"] == "DURABLE_LATEST_MARKET_DATA_VIEW"
    assert behavior["catch_up_burst"] is False
    assert behavior["restart_replays_consumed_timer"] is False
    assert behavior["eod_outcome"] == "EXPLICIT_TERMINAL_OR_RESIDUAL_EVIDENCE"
    assert behavior["auction_native_synthesis"] is False


def test_best_limit_binds_raw_digest_u53_and_strict_ordinal() -> None:
    behavior = CURRENT_THREE_BEHAVIOR_CHARACTERIZATIONS_V2["BEST_LIMIT_MINIQMT"]
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
    alias_only = project_legacy_vnpy_policy_v1(
        "TWAP_LITE_MINIQMT", {"duration_seconds": 1200, "interval_seconds": 120}
    )
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
    assert thaw_json_v1(defaulted.candidate_canonical_config) == {
        "price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"
    }
    assert unknown.drift_classification is LegacyProjectionDriftV1.INVALID_INPUT_VISIBLE
    assert unknown.candidate_canonical_config is None
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


def test_k1b_remains_shadow_only_without_runtime_wiring() -> None:
    runtime_source = inspect.getsource(
        __import__("backend.services.miniqmt_execution_runtime.runtime", fromlist=["MiniQMTExecutionRuntime"])
    )
    assert "plugin_manifests" not in runtime_source
    assert EventTypeV2.TIMER in _manifest("TWAP_LITE_MINIQMT").subscribed_event_types
