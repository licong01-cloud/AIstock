from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v2
from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    FrozenJsonArrayV1,
    FrozenJsonMemberV1,
    FrozenJsonObjectV1,
    canonical_decimal_string_v1,
    canonical_json_bytes_v1,
    canonical_utc_datetime_v1,
    digest_bytes_v1,
    freeze_json_v1,
    hash_hex_v1,
    json_safe_evidence_v1,
    require_identity_v1,
    require_sha256_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
    build_kernel_command_outcome_event_payload_v1,
    build_kernel_order_event_payload_v1,
    build_kernel_order_reconcile_event_payload_v1,
    build_kernel_trade_event_payload_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AbsenceDispositionV1,
    AlgoEventDeliveryV1,
    AlgoInitializationV1,
    AlgoStartContextV1,
    AlgoStateSnapshotV2,
    AlgoTransitionV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    DeliveryStatusV1,
    DeterministicExecutionContextV1,
    DiagnosticObservationV1,
    DiagnosticSeverityV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionAlgoPluginManifestV2,
    FileHashV1,
    GatewayCapabilityCatalogV1,
    MarketDataCapabilityV1,
    MarketDataRequirementV1,
    MiniQMTPluginContractError,
    MiniQMTPluginReasonCode,
    OrderTypeV1,
    PluginProviderV2,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
    SideV1,
    SourceAttributionV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    VnpyCompatibilityRequirementV2,
)


def test_frozen_json_is_recursively_immutable_and_thaw_returns_fresh_views() -> None:
    caller = {"z": [1, {"nested": "original"}], "a": True}
    frozen = freeze_json_v1(caller)
    initial = canonical_json_bytes_v1(frozen)

    caller["z"][1]["nested"] = "caller-mutated"
    caller["z"].append(2)
    first_view = thaw_json_v1(frozen)
    first_view["z"][1]["nested"] = "view-mutated"
    first_view["z"].append(3)

    second_view = thaw_json_v1(frozen)
    assert second_view == {"a": True, "z": [1, {"nested": "original"}]}
    assert canonical_json_bytes_v1(frozen) == initial
    assert first_view is not second_view


@pytest.mark.parametrize(
    "malformed",
    [
        {1: "non-string-key"},
        {"float": 1.0},
        {"nan": float("nan")},
        {"bytes": b"x"},
        {"set": {1}},
        ("external", "tuple"),
    ],
)
def test_frozen_json_rejects_non_json_or_ambiguous_carriers(malformed: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        freeze_json_v1(malformed)


def test_canonical_json_and_domain_hash_have_exact_raw_digest_semantics() -> None:
    left = {"汉": [2, 1], "a": {"y": False, "x": None}}
    right = {"a": {"x": None, "y": False}, "汉": [2, 1]}

    assert canonical_json_bytes_v1(left) == canonical_json_bytes_v1(right)
    assert canonical_json_bytes_v1(left) == '{"a":{"x":null,"y":false},"汉":[2,1]}'.encode()

    raw = digest_bytes_v1("domain.v1", {"a": 1})
    digest_hex = hash_hex_v1("domain.v1", {"a": 1})
    assert len(raw) == 32
    assert len(digest_hex) == 64
    assert raw.hex() == digest_hex
    assert raw != digest_hex.encode()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1.2300", "1.23"),
        ("0.000", "0"),
        (Decimal("100.500"), "100.5"),
    ],
)
def test_decimal_codec_is_canonical_and_never_uses_binary_float(value: object, expected: str) -> None:
    assert canonical_decimal_string_v1(value) == expected


@pytest.mark.parametrize("value", [True, 1, 1.5, "NaN", Decimal("Infinity")])
def test_decimal_codec_rejects_coercion_and_nonfinite_values(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        canonical_decimal_string_v1(value)


def test_datetime_codec_requires_authoritative_timezone_and_normalizes_utc() -> None:
    assert canonical_utc_datetime_v1(datetime(2026, 7, 22, 9, 30, tzinfo=UTC)) == "2026-07-22T09:30:00.000000Z"
    assert canonical_utc_datetime_v1("2026-07-22T17:30:00+08:00") == "2026-07-22T09:30:00.000000Z"
    with pytest.raises(ValueError, match="timezone"):
        canonical_utc_datetime_v1(datetime(2026, 7, 22, 9, 30))


def test_error_evidence_is_json_safe_for_malformed_unhashable_values() -> None:
    malformed = {
        "dict": {"nested": [1, {"bad": object()}]},
        "list": [1, {"x": {1, 2}}],
        "bytes": b"secret-payload",
        "nan": float("nan"),
    }
    safe = json_safe_evidence_v1(malformed)

    json.dumps(safe, ensure_ascii=False)
    error = MiniQMTPluginContractError(
        MiniQMTPluginReasonCode.MANIFEST_SCHEMA_INVALID,
        "malformed contract",
        context=malformed,
    )
    json.dumps(error.context, ensure_ascii=False)
    assert error.reason_code is MiniQMTPluginReasonCode.MANIFEST_SCHEMA_INVALID


def test_canonical_codec_failure_branches_remain_loud_and_typed() -> None:
    frozen = freeze_json_v1({"array": [1]})
    assert freeze_json_v1(frozen) is frozen
    assert canonical_json_bytes_v1([1, {"a": "b"}]) == b'[1,{"a":"b"}]'

    for invalid in ((1, 2), 1.0, float("inf"), Decimal("1.0"), object()):
        with pytest.raises((TypeError, ValueError)):
            canonical_json_bytes_v1(invalid)
    with pytest.raises(TypeError, match="FrozenJsonValueV1"):
        thaw_json_v1(1.0)
    for domain in ("", " padded ", 1):
        with pytest.raises((TypeError, ValueError)):
            digest_bytes_v1(domain, {})
    for identity in (None, "", " padded ", 1):
        with pytest.raises(ValueError):
            require_identity_v1(identity, field_name="id")
    for sha in (None, "A" * 64, "a" * 63, 1):
        with pytest.raises(ValueError):
            require_sha256_v1(sha, field_name="hash")


def test_decimal_and_datetime_codec_cover_scale_parse_and_type_failures() -> None:
    assert canonical_decimal_string_v1("1.230", max_scale=2) == "1.23"
    with pytest.raises(ValueError, match="scale"):
        canonical_decimal_string_v1("1.234", max_scale=2)
    for invalid in ("", " padded ", "not-decimal", "-1", Decimal("-1")):
        with pytest.raises((TypeError, ValueError)):
            canonical_decimal_string_v1(invalid)
    with pytest.raises(ValueError, match="positive"):
        canonical_decimal_string_v1("0", allow_zero=False)

    for invalid in ("", " padded ", "not-a-time", 1, datetime(2026, 7, 22, 1, 30)):
        with pytest.raises((TypeError, ValueError)):
            canonical_utc_datetime_v1(invalid)


def test_error_evidence_codec_is_bounded_and_stable_across_json_types() -> None:
    aware = datetime(2026, 7, 22, 1, 30, tzinfo=UTC)
    naive = datetime(2026, 7, 22, 1, 30)
    model = FileHashV1(path="source.py", sha256="a" * 64)
    error = ValueError("explicit failure")
    carrier = {
        "finite": 1.5,
        "decimal": Decimal("1.20"),
        "aware": aware,
        "naive": naive,
        "model": model,
        "tuple": (1, 2),
        "error": error,
        "long": "x" * 3_000,
        "mixed_keys": {1: "one", "two": 2},
    }
    safe = json_safe_evidence_v1(carrier)
    encoded = json.dumps(safe, sort_keys=True, ensure_ascii=False)
    assert "explicit failure" in encoded
    assert "max_depth" in json.dumps(json_safe_evidence_v1([[[[[[[[[[[[[1]]]]]]]]]]]]]))
    assert len(safe["long"]) < 3_000

    oversized = json_safe_evidence_v1(list(range(200)))
    assert oversized[-1] == {"__truncated_items__": 72}


def _market_requirement() -> MarketDataRequirementV1:
    return MarketDataRequirementV1.create(
        capability=MarketDataCapabilityV1.L1_ASK,
        required_fields=("price", "volume"),
        applicable_sides=(SideV1.BUY,),
        event_types=(EventTypeV2.TICK,),
        session_phases=(SessionPhaseV1.CONTINUOUS_AM, SessionPhaseV1.CONTINUOUS_PM),
        absence_disposition=AbsenceDispositionV1.WAIT_FOR_NEXT_VALID_EVENT,
    )


def test_market_requirement_is_sorted_hashed_and_strict() -> None:
    requirement = _market_requirement()
    assert requirement.session_phases == (SessionPhaseV1.CONTINUOUS_AM, SessionPhaseV1.CONTINUOUS_PM)
    assert requirement.requirement_sha256 == hash_hex_v1(
        "miniqmt_market_data_requirement_v1",
        requirement.hash_payload_v1(),
    )
    with pytest.raises(ValidationError):
        MarketDataRequirementV1.model_validate(
            {
                **requirement.model_dump(mode="python"),
                "required_fields": ("price", "price"),
            }
        )
    with pytest.raises(ValidationError):
        MarketDataRequirementV1.model_validate({**requirement.model_dump(mode="python"), "extra_gate": True})


def test_set_semantics_are_permutation_invariant_but_duplicates_fail_loud() -> None:
    left = MarketDataRequirementV1.create(
        capability=MarketDataCapabilityV1.L1_ASK,
        required_fields=("volume", "price"),
        applicable_sides=(SideV1.SELL, SideV1.BUY),
        event_types=(EventTypeV2.TICK,),
        session_phases=(SessionPhaseV1.CONTINUOUS_PM, SessionPhaseV1.CONTINUOUS_AM),
        absence_disposition=AbsenceDispositionV1.WAIT_FOR_NEXT_VALID_EVENT,
    )
    right = MarketDataRequirementV1.create(
        capability=MarketDataCapabilityV1.L1_ASK,
        required_fields=("price", "volume"),
        applicable_sides=(SideV1.BUY, SideV1.SELL),
        event_types=(EventTypeV2.TICK,),
        session_phases=(SessionPhaseV1.CONTINUOUS_AM, SessionPhaseV1.CONTINUOUS_PM),
        absence_disposition=AbsenceDispositionV1.WAIT_FOR_NEXT_VALID_EVENT,
    )
    assert left == right
    assert left.requirement_sha256 == right.requirement_sha256
    assert left.required_fields == ("price", "volume")

    with pytest.raises(ValueError, match="duplicates"):
        MarketDataRequirementV1.create(
            capability=MarketDataCapabilityV1.L1_ASK,
            required_fields=("price", "price"),
            applicable_sides=(SideV1.BUY,),
            event_types=(EventTypeV2.TICK,),
            session_phases=(SessionPhaseV1.CONTINUOUS_AM,),
            absence_disposition=AbsenceDispositionV1.WAIT_FOR_NEXT_VALID_EVENT,
        )
    with pytest.raises(TypeError, match="strict strings") as exc_info:
        MarketDataRequirementV1.create(
            capability=MarketDataCapabilityV1.L1_ASK,
            required_fields=({"not": "hashable"},),
            applicable_sides=(SideV1.BUY,),
            event_types=(EventTypeV2.TICK,),
            session_phases=(SessionPhaseV1.CONTINUOUS_AM,),
            absence_disposition=AbsenceDispositionV1.WAIT_FOR_NEXT_VALID_EVENT,
        )
    assert "unhashable" not in str(exc_info.value)


def _tick_event() -> RuntimeEventEnvelopeV2:
    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_20260722_a",
        sequence=7,
        event_type=EventTypeV2.TICK,
        event_time_utc="2026-07-22T09:30:00+08:00",
        monotonic_ns=None,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol="600000.SH",
        payload_schema_version="miniqmt_market_data_view_v2",
        payload={"market_data_id": "md_abc", "ask": {"price": "10.01", "volume": 100}},
        source_identity={"market_data_id": "md_abc"},
        correlation={"parent_intent_id": "intent_abc"},
    )


def test_runtime_event_enforces_composite_source_schema_identity_and_hash_closure() -> None:
    event = _tick_event()
    assert event.event_id == f"mqrtevt_{event.event_key_sha256}"
    assert event.payload_sha256 == hash_hex_v1("miniqmt_runtime_event_payload_v2", thaw_json_v1(event.payload))
    assert event.event_time_utc == "2026-07-22T01:30:00.000000Z"

    raw = event.model_dump(mode="python")
    with pytest.raises(ValidationError, match="event/source/payload"):
        RuntimeEventEnvelopeV2.model_validate(
            {
                **raw,
                "source": EventSourceV2.EXCHANGE_SESSION_CLOCK,
            }
        )
    with pytest.raises(ValidationError, match="source_identity"):
        RuntimeEventEnvelopeV2.create(
            runtime_id="runtime_20260722_a",
            sequence=8,
            event_type=EventTypeV2.TICK,
            event_time_utc="2026-07-22T09:30:01+08:00",
            monotonic_ns=None,
            source=EventSourceV2.B0_QUOTE_V2,
            symbol="600000.SH",
            payload_schema_version="miniqmt_market_data_view_v2",
            payload={"market_data_id": "md_abc"},
            source_identity={},
            correlation={},
        )
    with pytest.raises(ValidationError, match="strict identity"):
        RuntimeEventEnvelopeV2.create(
            runtime_id="runtime_20260722_a",
            sequence=8,
            event_type=EventTypeV2.TICK,
            event_time_utc="2026-07-22T09:30:01+08:00",
            monotonic_ns=None,
            source=EventSourceV2.B0_QUOTE_V2,
            symbol="600000.SH",
            payload_schema_version="miniqmt_market_data_view_v2",
            payload={"market_data_id": "md_abc"},
            source_identity={"market_data_id": {"not": "an identity"}},
            correlation={},
        )


def test_delivery_predecessor_contract_is_gap_free_at_dto_boundary() -> None:
    first = AlgoEventDeliveryV1.create(
        event=_tick_event(),
        algo_instance_id="mqalgo_a",
        plugin_manifest_sha256="a" * 64,
        algo_delivery_sequence=1,
        previous_delivery_id=None,
        status=DeliveryStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=None,
        last_error_json=None,
        created_at_utc="2026-07-22T01:30:00Z",
        updated_at_utc="2026-07-22T01:30:00Z",
    )
    assert first.previous_delivery_id is None

    raw = first.model_dump(mode="python")
    with pytest.raises(ValidationError, match="predecessor"):
        AlgoEventDeliveryV1.model_validate({**raw, "algo_delivery_sequence": 2})
    with pytest.raises(ValidationError):
        AlgoEventDeliveryV1.model_validate({**raw, "attempt_count": True})

    failed = AlgoEventDeliveryV1.model_validate(
        {
            **raw,
            "status": DeliveryStatusV1.FAILED_TERMINAL,
            "last_error_json": {
                "reason_code": "MINIQMT_PLUGIN_STATE_SCHEMA_INVALID",
                "message": "state schema conflict",
                "context": {"field": "state"},
            },
        }
    )
    assert thaw_json_v1(failed.last_error_json)["context"] == {"field": "state"}
    with pytest.raises(ValidationError, match="structured fields"):
        AlgoEventDeliveryV1.model_validate(
            {
                **raw,
                "status": DeliveryStatusV1.FAILED_TERMINAL,
                "last_error_json": {},
            }
        )


def test_state_snapshot_hashes_deep_frozen_state_and_rejects_hash_drift() -> None:
    caller_state = {"status": "RUNNING", "orders": [{"id": "local_1", "filled": 0}]}
    snapshot = _state_snapshot(
        state=caller_state,
        event_id="mqrtevt_3",
        delivery_id="mqdelivery_3",
        transition_sequence=3,
    )
    before = snapshot.state_sha256
    caller_state["orders"][0]["filled"] = 100
    assert snapshot.state_sha256 == before
    assert thaw_json_v1(snapshot.state)["orders"][0]["filled"] == 0

    with pytest.raises(ValidationError, match="state_sha256"):
        AlgoStateSnapshotV2.model_validate({**snapshot.model_dump(mode="python"), "state_sha256": "b" * 64})
    with pytest.raises(ValidationError):
        AlgoStateSnapshotV2.model_validate(
            {
                **snapshot.model_dump(mode="python"),
                "transition_sequence": 0,
                "last_applied_delivery_sequence": 0,
            }
        )


def test_broker_command_cross_field_contract_does_not_fake_broker_acceptance() -> None:
    submit = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="runtime_a",
        algo_instance_id="mqalgo_a",
        parent_intent_id="intent_a",
        transition_id="mqtransition_a",
        ordinal=0,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.01",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="SNIPER_TRIGGER",
        metadata={},
    )
    assert submit.owned_broker_order_id is None
    assert submit.local_vt_orderid.startswith("mqlocalorder_")
    assert submit.command_id.startswith("mqcommand_")
    with pytest.raises(ValueError, match="local_vt_orderid"):
        BrokerCommandV2.create(
            command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
            runtime_id="runtime_a",
            algo_instance_id="mqalgo_a",
            parent_intent_id="intent_a",
            transition_id="mqtransition_a",
            ordinal=0,
            local_vt_orderid="caller_alias",
            symbol="600000.SH",
            side=SideV1.BUY,
            order_type=OrderTypeV1.LIMIT,
            price_decimal="10.01",
            quantity=100,
            owned_broker_order_id=None,
            reason_code="SNIPER_TRIGGER",
            metadata={},
        )
    with pytest.raises(ValidationError, match="must not carry broker"):
        BrokerCommandV2.model_validate({**submit.model_dump(mode="python"), "owned_broker_order_id": "broker_1"})

    cancel = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id="runtime_a",
        algo_instance_id="mqalgo_a",
        parent_intent_id="intent_a",
        transition_id="mqtransition_cancel",
        ordinal=0,
        local_vt_orderid=submit.local_vt_orderid,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.01",
        quantity=100,
        owned_broker_order_id="broker_1",
        reason_code="CANCEL_REQUESTED",
        metadata={},
    )
    assert cancel.owned_broker_order_id == "broker_1"
    with pytest.raises(ValidationError, match="requires exact durable-owned"):
        BrokerCommandV2.model_validate({**cancel.model_dump(mode="python"), "owned_broker_order_id": None})


def test_diagnostic_context_is_deep_frozen_and_hash_verified() -> None:
    deterministic = _deterministic_context_for_manifest(_manifest())
    diagnostic = DiagnosticObservationV1.create(
        deterministic_context=deterministic,
        transition_id="mqtransition_a",
        ordinal=0,
        severity="ERROR",
        reason_code="MINIQMT_PLUGIN_CONFIG_SCHEMA_INVALID",
        message="invalid config",
        context={"field": "min_volume", "actual": {"bad": [1]}},
    )
    json.dumps(thaw_json_v1(diagnostic.context))
    assert diagnostic.context_sha256 == hash_hex_v1(
        "miniqmt_diagnostic_context_v1",
        thaw_json_v1(diagnostic.context),
    )


def _source_attribution() -> SourceAttributionV1:
    upstream_files = (
        FileHashV1(path="vnpy_algotrading/base.py", sha256="1" * 64),
        FileHashV1(path="vnpy_algotrading/template.py", sha256="2" * 64),
    )
    aistock_files = (FileHashV1(path="backend/execution_algos/vnpy_style/sniper_core.py", sha256="3" * 64),)
    payload = {
        "schema_version": "source_attribution_v1",
        "upstream_repo": "vnpy/vnpy_algotrading",
        "upstream_commit": "4" * 40,
        "upstream_files": [item.model_dump(mode="json") for item in upstream_files],
        "upstream_license": "MIT",
        "upstream_copyright": "VeighNa authors",
        "aistock_asset_version": "2.0.0",
        "aistock_files": [item.model_dump(mode="json") for item in aistock_files],
        "derivation_summary": "AIstock broker-neutral exact characterization",
    }
    model_payload = {**payload, "upstream_files": upstream_files, "aistock_files": aistock_files}
    return SourceAttributionV1(
        **model_payload,
        attribution_sha256=hash_hex_v1("miniqmt_source_attribution_v1", payload),
    )


def _compatibility_requirement() -> VnpyCompatibilityRequirementV2:
    return next(
        manifest.compatibility_requirement
        for manifest in current_three_manifests_v2()
        if manifest.algo_code == "SNIPER_MINIQMT"
    )


def _manifest(config_schema: dict[str, object] | None = None) -> ExecutionAlgoPluginManifestV2:
    config_schema = config_schema or {
        "type": "object",
        "additionalProperties": False,
        "required": ["price_mode"],
        "properties": {"price_mode": {"const": "LIMIT_TRIGGER_BY_BEST_QUOTE"}},
    }
    state_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["status"],
        "properties": {
            "status": {"type": "string"},
            "orders": {"type": "array"},
        },
    }
    requirement = _market_requirement()
    attribution = _source_attribution()
    compatibility = _compatibility_requirement()
    facade_fields = compatibility.required_object_fields
    payload: dict[str, object] = {
        "schema_version": "execution_algo_plugin_manifest_v2",
        "plugin_id": "aistock.vnpy.sniper",
        "algo_code": "SNIPER_MINIQMT",
        "plugin_version": "2.0.0",
        "provider": PluginProviderV2.AISTOCK_DERIVED.value,
        "implementation_ref": "backend.execution_algos.vnpy_style.sniper_core:SniperAlgoCore",
        "config_schema_version": "sniper_config_v2",
        "config_schema": config_schema,
        "config_schema_sha256": hash_hex_v1("miniqmt_plugin_config_schema_v1", config_schema),
        "state_schema_version": "sniper_state_v2",
        "state_schema": state_schema,
        "state_schema_sha256": hash_hex_v1("miniqmt_plugin_state_schema_v1", state_schema),
        "subscribed_event_types": ["ALGO_START", "TICK"],
        "market_data_requirements": [requirement.model_dump(mode="json")],
        "required_facade_methods": ["get_tick", "send_order"],
        "required_facade_object_fields": [item.model_dump(mode="json") for item in facade_fields],
        "supported_sides": ["BUY", "SELL"],
        "supported_order_types": ["LIMIT"],
        "supported_broker_backends": ["minqmt_sim"],
        "restart_policy": "DURABLE_RESTORE",
        "source_attribution": attribution.model_dump(mode="json"),
        "compatibility_requirement": compatibility.model_dump(mode="json"),
        "behavior_characterization_sha256": "6" * 64,
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
    payload["behavior_contract_sha256"] = hash_hex_v1(
        "miniqmt_plugin_behavior_contract_v2",
        {key: payload[key] for key in behavior_keys},
    )
    payload["manifest_sha256"] = hash_hex_v1("execution_algo_plugin_manifest_v2", payload)
    model_payload = {
        **payload,
        "provider": PluginProviderV2.AISTOCK_DERIVED,
        "config_schema": config_schema,
        "state_schema": state_schema,
        "subscribed_event_types": (EventTypeV2.ALGO_START, EventTypeV2.TICK),
        "market_data_requirements": (requirement,),
        "required_facade_methods": ("get_tick", "send_order"),
        "required_facade_object_fields": facade_fields,
        "supported_sides": (SideV1.BUY, SideV1.SELL),
        "supported_order_types": (OrderTypeV1.LIMIT,),
        "supported_broker_backends": ("minqmt_sim",),
        "source_attribution": attribution,
        "compatibility_requirement": compatibility,
    }
    return ExecutionAlgoPluginManifestV2(**model_payload)


def _deterministic_context_for_manifest(
    manifest: ExecutionAlgoPluginManifestV2,
    *,
    event_id: str = "mqrtevt_a",
    delivery_id: str = "mqdelivery_a",
    transition_sequence: int = 1,
    logical_time_utc: str = "2026-07-22T01:30:00Z",
) -> DeterministicExecutionContextV1:
    return DeterministicExecutionContextV1.create(
        runtime_id="runtime_a",
        algo_instance_id="mqalgo_a",
        event_id=event_id,
        delivery_id=delivery_id,
        plugin_manifest_sha256=manifest.manifest_sha256,
        transition_sequence=transition_sequence,
        logical_time_utc=logical_time_utc,
        exchange_trade_date="2026-07-22",
        session_epoch="session_am",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="9" * 64,
    )


def _algo_instance_id_for(
    *,
    runtime_id: str,
    parent_intent_id: str,
    strategy_slot_id: str,
    algo_code: str,
    plugin_id: str,
    plugin_version: str,
    plugin_manifest_sha256: str,
    plugin_config_sha256: str,
) -> str:
    return "mqalgo_" + hash_hex_v1(
        "miniqmt_algo_instance_v2",
        {
            "runtime_id": runtime_id,
            "parent_intent_id": parent_intent_id,
            "strategy_slot_id": strategy_slot_id,
            "algo_code": algo_code,
            "plugin_id": plugin_id,
            "plugin_version": plugin_version,
            "plugin_manifest_sha256": plugin_manifest_sha256,
            "plugin_config_sha256": plugin_config_sha256,
        },
    )


def _state_snapshot(
    *,
    state: dict[str, object],
    event_id: str = "mqrtevt_a",
    delivery_id: str = "mqdelivery_a",
    transition_sequence: int = 1,
) -> AlgoStateSnapshotV2:
    manifest = _manifest()
    context = _deterministic_context_for_manifest(
        manifest,
        event_id=event_id,
        delivery_id=delivery_id,
        transition_sequence=transition_sequence,
    )
    return AlgoStateSnapshotV2.create(
        plugin_manifest=manifest,
        deterministic_context=context,
        transition_sequence=transition_sequence,
        last_applied_delivery_sequence=transition_sequence,
        last_applied_delivery_id=delivery_id,
        last_closed_delivery_sequence=transition_sequence,
        state=state,
        last_applied_event_id=event_id,
    )


def test_manifest_has_complete_hash_closure_deep_immutability_and_json_readback() -> None:
    caller_schema: dict[str, object] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"price_mode": {"enum": ["LIMIT_TRIGGER_BY_BEST_QUOTE"]}},
    }
    manifest = _manifest(caller_schema)
    initial_hash = manifest.manifest_sha256
    caller_schema["properties"]["price_mode"]["enum"].append("MUTATED")

    assert manifest.manifest_sha256 == initial_hash
    assert thaw_json_v1(manifest.config_schema)["properties"]["price_mode"]["enum"] == ["LIMIT_TRIGGER_BY_BEST_QUOTE"]
    readback = ExecutionAlgoPluginManifestV2.model_validate_json(manifest.model_dump_json())
    assert readback == manifest
    assert readback.manifest_sha256 == initial_hash

    with pytest.raises(ValidationError, match="manifest_sha256"):
        ExecutionAlgoPluginManifestV2.model_validate(
            {**manifest.model_dump(mode="python"), "manifest_sha256": "f" * 64}
        )

    duplicate_key_json = manifest.model_dump_json().replace(
        "{",
        '{"plugin_id":"aistock.vnpy.conflict",',
        1,
    )
    with pytest.raises(ValueError, match="duplicate key: plugin_id"):
        ExecutionAlgoPluginManifestV2.model_validate_json(duplicate_key_json)
    with pytest.raises(ValueError, match="BOM"):
        ExecutionAlgoPluginManifestV2.model_validate_json("\ufeff" + manifest.model_dump_json())


def test_gateway_capability_catalog_is_a_hashed_technical_fact_not_a_gate() -> None:
    payload = {
        "schema_version": "miniqmt_gateway_capability_catalog_v1",
        "route_id": "miniqmt_sim_b0",
        "quote_source": "B0_QUOTE_V2",
        "gateway_backend": "minqmt_sim",
        "order_types": ["LIMIT"],
        "market_data_capabilities": ["L1_ASK", "L1_BID"],
        "session_phases": ["CONTINUOUS_AM", "CONTINUOUS_PM"],
        "idempotent_submit_by_client_ref": False,
        "exact_order_id_cancel": True,
    }
    model_payload = {
        **payload,
        "order_types": (OrderTypeV1.LIMIT,),
        "market_data_capabilities": (MarketDataCapabilityV1.L1_ASK, MarketDataCapabilityV1.L1_BID),
        "session_phases": (SessionPhaseV1.CONTINUOUS_AM, SessionPhaseV1.CONTINUOUS_PM),
    }
    catalog = GatewayCapabilityCatalogV1(
        **model_payload,
        catalog_sha256=hash_hex_v1("miniqmt_gateway_capability_catalog_v1", payload),
    )
    assert catalog.idempotent_submit_by_client_ref is False
    with pytest.raises(ValidationError):
        GatewayCapabilityCatalogV1.model_validate({**catalog.model_dump(mode="python"), "exact_order_id_cancel": 1})


@pytest.mark.parametrize(
    ("event_type", "source", "schema", "source_identity", "symbol", "monotonic_ns"),
    [
        (
            EventTypeV2.ALGO_START,
            EventSourceV2.MINIQMT_EXECUTION_KERNEL,
            "miniqmt_algo_start_v1",
            {
                "algo_instance_id": _algo_instance_id_for(
                    runtime_id="runtime_a",
                    parent_intent_id="intent_a",
                    strategy_slot_id="slot_a",
                    algo_code="SNIPER_MINIQMT",
                    plugin_id="aistock.vnpy.sniper",
                    plugin_version="2.0.0",
                    plugin_manifest_sha256="a" * 64,
                    plugin_config_sha256="b" * 64,
                ),
                "runtime_id": "runtime_a",
                "strategy_slot_id": "slot_a",
                "algo_code": "SNIPER_MINIQMT",
                "plugin_id": "aistock.vnpy.sniper",
                "plugin_version": "2.0.0",
                "plugin_manifest_sha256": "a" * 64,
                "plugin_config_sha256": "b" * 64,
                "parent_intent_id": "intent_a",
            },
            "600000.SH",
            None,
        ),
        (
            EventTypeV2.COMMAND_OUTCOME,
            EventSourceV2.MINIQMT_EXECUTION_KERNEL,
            "miniqmt_command_outcome_v1",
            {"receipt_id": "outcome_receipt_a", "receipt_sha256": "e" * 64},
            "600000.SH",
            None,
        ),
        (
            EventTypeV2.TICK,
            EventSourceV2.B0_QUOTE_V2,
            "miniqmt_market_data_view_v2",
            {"market_data_id": "md_a"},
            "600000.SH",
            None,
        ),
        (
            EventTypeV2.TIMER,
            EventSourceV2.EXCHANGE_SESSION_CLOCK,
            "miniqmt_timer_due_v1",
            {"timer_occurrence_id": "occ_a"},
            None,
            10,
        ),
        (
            EventTypeV2.SESSION,
            EventSourceV2.EXCHANGE_SESSION_CLOCK,
            "miniqmt_session_event_v1",
            {"session_event_id": "session_a"},
            None,
            None,
        ),
        (
            EventTypeV2.EOD,
            EventSourceV2.EXCHANGE_SESSION_CLOCK,
            "miniqmt_eod_event_v1",
            {"runtime_id": "runtime_a", "trade_date": "2026-07-22", "session_epoch": "epoch_a"},
            None,
            None,
        ),
        (
            EventTypeV2.ORDER,
            EventSourceV2.QMT_GATEWAY_CALLBACK,
            "miniqmt_order_event_v1",
            {"order_event_id": "order_evt_a"},
            "600000.SH",
            None,
        ),
        (
            EventTypeV2.TRADE,
            EventSourceV2.QMT_GATEWAY_CALLBACK,
            "miniqmt_trade_fact_v1",
            {"trade_id": "trade_a"},
            "600000.SH",
            None,
        ),
        (
            EventTypeV2.ACCOUNT,
            EventSourceV2.QMT_OMS_PROJECTION,
            "miniqmt_account_projection_v1",
            {"projection_version": "v1", "projection_sha256": "c" * 64},
            None,
            None,
        ),
        (
            EventTypeV2.RECONCILE,
            EventSourceV2.QMT_OMS_RECONCILIATION,
            "miniqmt_reconciliation_receipt_v1",
            {"receipt_id": "receipt_a", "receipt_sha256": "d" * 64},
            "600000.SH",
            None,
        ),
        (
            EventTypeV2.OPERATOR,
            EventSourceV2.SIMULATION_RUNTIME_OPERATOR,
            "miniqmt_operator_command_v1",
            {"operator_command_id": "operator_a"},
            None,
            None,
        ),
    ],
)
def test_all_runtime_event_composite_rows_are_explicit_and_roundtrip(
    event_type: EventTypeV2,
    source: EventSourceV2,
    schema: str,
    source_identity: dict[str, object],
    symbol: str | None,
    monotonic_ns: int | None,
) -> None:
    payload: dict[str, object] = {"row": event_type.value}
    common = {
        "runtime_id": "runtime_a",
        "algo_instance_id": "mqalgo_a",
        "parent_intent_id": "intent_a",
        "strategy_slot_id": "slot_a",
        "mapping_id": "mapping_a",
        "command_id": "command_a",
        "local_vt_orderid": "local_order_a",
        "broker_order_id": "broker_order_a",
    }
    if event_type is EventTypeV2.ORDER:
        payload = build_kernel_order_event_payload_v1(
            raw_payload={"order_status": 48},
            order_event_id="order_evt_a",
            symbol="600000.SH",
            side="BUY",
            requested_quantity=100,
            **common,
        ).model_dump(mode="json")
    elif event_type is EventTypeV2.TRADE:
        payload = build_kernel_trade_event_payload_v1(
            raw_payload={"trade_id": "trade_a"},
            symbol="600000.SH",
            side="BUY",
            trade_quantity=100,
            trade_price_decimal="10",
            **common,
        ).model_dump(mode="json")
    elif event_type is EventTypeV2.COMMAND_OUTCOME:
        payload = build_kernel_command_outcome_event_payload_v1(
            receipt_id="outcome_receipt_a",
            receipt_sha256="e" * 64,
            command_type="SUBMIT_LIMIT",
            outcome="ACCEPTED",
            outbox_status="ACKED",
            outbox_row_version=2,
            outcome_receipt_sha256="f" * 64,
            outbox_terminal=True,
            order_terminal=False,
            **common,
        ).model_dump(mode="json")
    elif event_type is EventTypeV2.RECONCILE:
        reconcile_common = {key: value for key, value in common.items() if key != "command_id"}
        payload = build_kernel_order_reconcile_event_payload_v1(
            ordered_trade_refs=(),
            requested_quantity=100,
            receipt_id="receipt_a",
            receipt_sha256="d" * 64,
            symbol="600000.SH",
            side="BUY",
            normalized_order_status="ACCEPTED",
            authoritative_cumulative_filled_quantity=0,
            authoritative_remaining_quantity=100,
            callback_watermark="watermark_a",
            snapshot_sha256="a" * 64,
            **reconcile_common,
        ).model_dump(mode="json")
    event = RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_a",
        sequence=1,
        event_type=event_type,
        event_time_utc="2026-07-22T01:30:00Z",
        monotonic_ns=monotonic_ns,
        source=source,
        symbol=symbol,
        payload_schema_version=schema,
        payload=payload,
        source_identity=source_identity,
        correlation={},
    )
    assert RuntimeEventEnvelopeV2.model_validate_json(event.model_dump_json()) == event


@pytest.mark.parametrize("identity", [None, "", " padded ", 1, True, [], {}])
def test_identity_fields_reject_null_empty_coercion_and_containers(identity: object) -> None:
    raw = AlgoEventDeliveryV1.create(
        event=_tick_event(),
        algo_instance_id="mqalgo_a",
        plugin_manifest_sha256="a" * 64,
        algo_delivery_sequence=1,
        previous_delivery_id=None,
        status=DeliveryStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=None,
        last_error_json=None,
        created_at_utc="2026-07-22T01:30:00Z",
        updated_at_utc="2026-07-22T01:30:00Z",
    ).model_dump(mode="python")
    with pytest.raises(ValidationError):
        AlgoEventDeliveryV1.model_validate({**raw, "delivery_id": identity})


def _timer_mutation(*, ordinal: int) -> TimerMutationV1:
    payload = {"timer_name": "twap_second", "slice": ordinal}
    return TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id="mqalgo_a",
        transition_id="mqtransition_a",
        ordinal=ordinal,
        timer_name="twap_second",
        schedule_epoch="epoch_1",
        due_at_exchange_utc="2026-07-22T01:30:01Z",
        catch_up_policy="NO_CATCH_UP_BURST",
        payload=payload,
    )


def test_transition_effect_set_preserves_order_and_rejects_duplicate_or_skipped_ordinals() -> None:
    state = _state_snapshot(state={"status": "RUNNING"})
    timer = _timer_mutation(ordinal=0)
    diagnostic = DiagnosticObservationV1.create(
        deterministic_context=_deterministic_context_for_manifest(_manifest()),
        transition_id="mqtransition_a",
        ordinal=1,
        severity=DiagnosticSeverityV1.INFO,
        reason_code="TWAP_TIMER_SCHEDULED",
        message="timer scheduled",
        context={"occurrence": "mqtimerocc_0"},
    )
    effect_payload = {
        "next_state_sha256": state.state_sha256,
        "ordered_command_ids": [],
        "ordered_timer_mutation_ids": [timer.mutation_identity_v1()],
        "ordered_diagnostic_observation_ids": [diagnostic.observation_id],
        "terminal_outcome": None,
    }
    transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=state,
        broker_commands=(),
        timer_mutations=(timer,),
        diagnostic_observations=(diagnostic,),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", effect_payload),
    )
    assert transition.effect_hash_payload_v1() == effect_payload
    assert AlgoTransitionV1.model_validate_json(transition.model_dump_json()) == transition

    duplicate_diagnostic = DiagnosticObservationV1.create(
        deterministic_context=_deterministic_context_for_manifest(_manifest()),
        transition_id="mqtransition_a",
        ordinal=0,
        severity=DiagnosticSeverityV1.INFO,
        reason_code="TWAP_TIMER_SCHEDULED",
        message="timer scheduled",
        context={"occurrence": "mqtimerocc_0"},
    )
    with pytest.raises(ValidationError, match="duplicate"):
        AlgoTransitionV1.model_validate(
            {
                **transition.model_dump(mode="python"),
                "diagnostic_observations": (duplicate_diagnostic,),
            }
        )
    skipped_diagnostic = DiagnosticObservationV1.create(
        deterministic_context=_deterministic_context_for_manifest(_manifest()),
        transition_id="mqtransition_a",
        ordinal=2,
        severity=DiagnosticSeverityV1.INFO,
        reason_code="TWAP_TIMER_SCHEDULED",
        message="timer scheduled",
        context={"occurrence": "mqtimerocc_0"},
    )
    with pytest.raises(ValidationError, match="contiguous"):
        AlgoTransitionV1.model_validate(
            {
                **transition.model_dump(mode="python"),
                "diagnostic_observations": (skipped_diagnostic,),
            }
        )

    timer_second = _timer_mutation(ordinal=1)
    timer_first = _timer_mutation(ordinal=0)
    reverse_effect_payload = {
        "next_state_sha256": state.state_sha256,
        "ordered_command_ids": [],
        "ordered_timer_mutation_ids": [
            timer_second.mutation_identity_v1(),
            timer_first.mutation_identity_v1(),
        ],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": None,
    }
    with pytest.raises(ValidationError, match="ascending"):
        AlgoTransitionV1(
            schema_version="miniqmt_algo_transition_v1",
            next_state=state,
            broker_commands=(),
            timer_mutations=(timer_second, timer_first),
            diagnostic_observations=(),
            terminal_outcome=None,
            effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", reverse_effect_payload),
        )


def test_initialization_closes_exact_start_delivery_event_and_first_state() -> None:
    state = _state_snapshot(
        state={"status": "RUNNING"},
        event_id="mqrtevt_start",
        delivery_id="mqdelivery_start",
    )
    effect_payload = {
        "next_state_sha256": state.state_sha256,
        "ordered_command_ids": [],
        "ordered_timer_mutation_ids": [],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": None,
    }
    initialization = AlgoInitializationV1(
        schema_version="miniqmt_algo_initialization_v1",
        start_event_id="mqrtevt_start",
        start_delivery_id="mqdelivery_start",
        next_state=state,
        broker_commands=(),
        timer_mutations=(),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", effect_payload),
    )
    assert AlgoInitializationV1.model_validate_json(initialization.model_dump_json()) == initialization
    with pytest.raises(ValidationError, match="start_event_id"):
        AlgoInitializationV1.model_validate(
            {
                **initialization.model_dump(mode="python"),
                "start_event_id": "mqrtevt_other",
            }
        )


def _start_context_for_manifest(
    manifest: ExecutionAlgoPluginManifestV2,
    plugin_config: dict[str, object],
) -> AlgoStartContextV1:
    plugin_config_sha256 = hash_hex_v1("miniqmt_plugin_config_v2", plugin_config)
    algo_instance_id = _algo_instance_id_for(
        runtime_id="runtime_a",
        parent_intent_id="intent_a",
        strategy_slot_id="slot_a",
        algo_code=manifest.algo_code,
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        plugin_manifest_sha256=manifest.manifest_sha256,
        plugin_config_sha256=plugin_config_sha256,
    )
    deterministic = DeterministicExecutionContextV1.create(
        runtime_id="runtime_a",
        algo_instance_id=algo_instance_id,
        event_id="mqrtevt_start",
        delivery_id="mqdelivery_start",
        plugin_manifest_sha256=manifest.manifest_sha256,
        transition_sequence=0,
        logical_time_utc="2026-07-22T01:30:00Z",
        exchange_trade_date="2026-07-22",
        session_epoch="session_am",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="9" * 64,
    )
    contract_projection = {"pricetick_decimal": "0.01", "min_volume": 100}
    account_projection = {"account_group_id": "sim_account"}
    capability_projection = {"route_id": "miniqmt_sim_b0", "capabilities": ["L1_ASK"]}
    return AlgoStartContextV1(
        schema_version="miniqmt_algo_start_context_v1",
        runtime_id="runtime_a",
        algo_instance_id=algo_instance_id,
        parent_intent_id="intent_a",
        strategy_slot_id="slot_a",
        symbol="600000.SH",
        side=SideV1.BUY,
        limit_price_decimal="10.01",
        parent_quantity=100,
        min_volume=100,
        volume_increment=100,
        plugin_manifest=manifest,
        plugin_config=plugin_config,
        plugin_config_sha256=plugin_config_sha256,
        start_event_id="mqrtevt_start",
        start_delivery_id="mqdelivery_start",
        deterministic_context=deterministic,
        contract_projection=contract_projection,
        contract_projection_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract_projection),
        account_projection=account_projection,
        account_projection_sha256=hash_hex_v1("miniqmt_account_projection_v1", account_projection),
        market_capability_projection=capability_projection,
        market_capability_projection_sha256=hash_hex_v1(
            "miniqmt_market_capability_projection_v1", capability_projection
        ),
        execution_plan_id="plan_a",
        execution_plan_sha256="a" * 64,
        release_id="release_a",
        release_sha256="b" * 64,
        policy_id="policy_a",
        policy_sha256="c" * 64,
    )


def _start_context() -> AlgoStartContextV1:
    return _start_context_for_manifest(
        _manifest(),
        {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
    )


def test_algo_start_context_closes_manifest_context_projections_and_quantity() -> None:
    start = _start_context()
    assert AlgoStartContextV1.model_validate_json(start.model_dump_json()) == start

    raw = start.model_dump(mode="python")
    with pytest.raises(ValidationError, match="volume_increment"):
        AlgoStartContextV1.model_validate({**raw, "parent_quantity": 150})
    with pytest.raises(ValidationError, match="runtime_id"):
        AlgoStartContextV1.model_validate({**raw, "runtime_id": "runtime_other"})
    with pytest.raises(ValidationError, match="projection"):
        AlgoStartContextV1.model_validate({**raw, "account_projection_sha256": "f" * 64})


def test_public_frozen_json_markers_cannot_retain_caller_owned_mutability() -> None:
    caller_owned: list[object] = []
    caller_owned_object: dict[str, object] = {}
    forged_marker = FrozenJsonArrayV1([caller_owned])
    forged_object = FrozenJsonObjectV1([FrozenJsonMemberV1(key="nested", value=caller_owned_object)])
    frozen = freeze_json_v1(forged_marker)
    frozen_object = freeze_json_v1(forged_object)

    caller_owned.append("mutated-after-freeze")
    caller_owned_object["mutated"] = True

    assert thaw_json_v1(frozen) == [[]]
    assert thaw_json_v1(frozen_object) == {"nested": {}}
    assert canonical_json_bytes_v1(frozen) == b"[[]]"


def test_error_evidence_never_raises_a_secondary_exception() -> None:
    class BrokenMessageError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("secondary diagnostic failure")

    evidence = json_safe_evidence_v1(BrokenMessageError())

    json.dumps(evidence, ensure_ascii=False)
    assert evidence["__type__"].endswith("BrokenMessageError")
    assert evidence["message_render_error_type"].endswith("RuntimeError")

    class BrokenMapping(Mapping[str, object]):
        def __getitem__(self, key: str) -> object:
            raise KeyError(key)

        def __iter__(self) -> Iterator[str]:
            raise RuntimeError("broken mapping iterator")

        def __len__(self) -> int:
            return 1

    mapping_evidence = json_safe_evidence_v1(BrokenMapping())
    json.dumps(mapping_evidence, ensure_ascii=False)
    assert mapping_evidence["__evidence_render_error_type__"].endswith("RuntimeError")


def test_public_frozen_json_markers_reject_malformed_direct_construction() -> None:
    with pytest.raises(TypeError, match="iterable"):
        FrozenJsonArrayV1("not-an-array-carrier")
    with pytest.raises(TypeError, match="FrozenJsonMemberV1"):
        FrozenJsonObjectV1(["not-a-member"])
    with pytest.raises(TypeError, match="key must be str"):
        FrozenJsonObjectV1([FrozenJsonMemberV1(key=1, value=None)])
    with pytest.raises(ValueError, match="duplicate key"):
        FrozenJsonObjectV1(
            [
                FrozenJsonMemberV1(key="same", value=1),
                FrozenJsonMemberV1(key="same", value=2),
            ]
        )


def test_runtime_event_rejects_unregistered_source_identity_components() -> None:
    with pytest.raises(ValidationError, match="exact registered fields"):
        RuntimeEventEnvelopeV2.create(
            runtime_id="runtime_a",
            sequence=1,
            event_type=EventTypeV2.TICK,
            event_time_utc="2026-07-22T01:30:00Z",
            monotonic_ns=None,
            source=EventSourceV2.B0_QUOTE_V2,
            symbol="600000.SH",
            payload_schema_version="miniqmt_market_data_view_v2",
            payload={"market_data_id": "md_a"},
            source_identity={"market_data_id": "md_a", "unregistered_component": "x"},
            correlation={},
        )


def _algo_start_event() -> RuntimeEventEnvelopeV2:
    identity = {
        "runtime_id": "runtime_a",
        "parent_intent_id": "intent_a",
        "strategy_slot_id": "slot_a",
        "algo_code": "SNIPER_MINIQMT",
        "plugin_id": "aistock.vnpy.sniper",
        "plugin_version": "2.0.0",
        "plugin_manifest_sha256": "a" * 64,
        "plugin_config_sha256": "b" * 64,
    }
    identity["algo_instance_id"] = _algo_instance_id_for(**identity)
    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_a",
        sequence=1,
        event_type=EventTypeV2.ALGO_START,
        event_time_utc="2026-07-22T01:30:00Z",
        monotonic_ns=None,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        symbol="600000.SH",
        payload_schema_version="miniqmt_algo_start_v1",
        payload={"algo_instance_id": identity["algo_instance_id"]},
        source_identity=identity,
        correlation={"parent_intent_id": "intent_a"},
    )


def test_algo_start_identity_and_first_delivery_are_exact() -> None:
    event = _algo_start_event()
    invalid_identity = event.model_dump(mode="json")["source_identity"]
    invalid_identity["algo_instance_id"] = "mqalgo_wrong"
    with pytest.raises(ValidationError, match="complete source identity closure"):
        RuntimeEventEnvelopeV2.create(
            runtime_id="runtime_a",
            sequence=1,
            event_type=EventTypeV2.ALGO_START,
            event_time_utc="2026-07-22T01:30:00Z",
            monotonic_ns=None,
            source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
            symbol="600000.SH",
            payload_schema_version="miniqmt_algo_start_v1",
            payload={"algo_instance_id": "mqalgo_wrong"},
            source_identity=invalid_identity,
            correlation={"parent_intent_id": "intent_a"},
        )
    with pytest.raises(ValueError, match="delivery sequence 1"):
        AlgoEventDeliveryV1.create(
            event=event,
            algo_instance_id=event.model_dump(mode="json")["source_identity"]["algo_instance_id"],
            plugin_manifest_sha256="a" * 64,
            algo_delivery_sequence=2,
            previous_delivery_id="mqdelivery_previous",
            status=DeliveryStatusV1.PENDING,
            attempt_count=0,
            lease_owner=None,
            lease_expires_at=None,
            transition_id=None,
            last_error_json=None,
            created_at_utc="2026-07-22T01:30:00Z",
            updated_at_utc="2026-07-22T01:30:00Z",
        )


def test_command_identity_and_effect_hash_reject_same_id_with_payload_drift() -> None:
    state = _state_snapshot(state={"status": "RUNNING"})
    first = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="runtime_a",
        algo_instance_id="mqalgo_a",
        parent_intent_id="intent_a",
        transition_id="mqtransition_a",
        ordinal=0,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.01",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="TEST",
        metadata={},
    )
    second = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="runtime_a",
        algo_instance_id="mqalgo_a",
        parent_intent_id="intent_a",
        transition_id="mqtransition_a",
        ordinal=0,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.01",
        quantity=200,
        owned_broker_order_id=None,
        reason_code="TEST",
        metadata={},
    )
    assert first.command_id != second.command_id

    first_effect_payload = {
        "next_state_sha256": state.state_sha256,
        "ordered_command_ids": [first.command_id],
        "ordered_timer_mutation_ids": [],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": None,
    }
    first_transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=state,
        broker_commands=(first,),
        timer_mutations=(),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", first_effect_payload),
    )
    second_effect_payload = {**first_effect_payload, "ordered_command_ids": [second.command_id]}
    second_transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=state,
        broker_commands=(second,),
        timer_mutations=(),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", second_effect_payload),
    )
    assert first_transition.effect_set_sha256 != second_transition.effect_set_sha256

    drifted = second.model_dump(mode="python")
    with pytest.raises(ValidationError, match="command_id"):
        BrokerCommandV2.model_validate(
            {
                **drifted,
                "command_id": first.command_id,
            }
        )


def test_state_and_config_must_validate_against_manifest_schema_and_logical_time() -> None:
    manifest = _manifest(
        {
            "type": "object",
            "additionalProperties": False,
            "required": ["price_mode"],
            "properties": {"price_mode": {"const": "LIMIT_TRIGGER_BY_BEST_QUOTE"}},
        }
    )
    deterministic = DeterministicExecutionContextV1.create(
        runtime_id="runtime_a",
        algo_instance_id="mqalgo_a",
        event_id="mqrtevt_a",
        delivery_id="mqdelivery_a",
        plugin_manifest_sha256=manifest.manifest_sha256,
        transition_sequence=1,
        logical_time_utc="2026-07-22T01:30:00Z",
        exchange_trade_date="2026-07-22",
        session_epoch="session_am",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="9" * 64,
    )

    with pytest.raises((TypeError, ValidationError, ValueError), match="state schema"):
        AlgoStateSnapshotV2.create(
            plugin_manifest=manifest,
            deterministic_context=deterministic,
            transition_sequence=1,
            last_applied_delivery_sequence=1,
            last_applied_delivery_id="mqdelivery_a",
            last_closed_delivery_sequence=1,
            state={},
            last_applied_event_id="mqrtevt_a",
        )
    with pytest.raises(ValueError, match="last event identity"):
        AlgoStateSnapshotV2.create(
            plugin_manifest=manifest,
            deterministic_context=deterministic,
            transition_sequence=1,
            last_applied_delivery_sequence=1,
            last_applied_delivery_id="mqdelivery_a",
            last_closed_delivery_sequence=1,
            state={"status": "RUNNING"},
            last_applied_event_id="mqrtevt_other",
        )

    valid_snapshot = AlgoStateSnapshotV2.create(
        plugin_manifest=manifest,
        deterministic_context=deterministic,
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id="mqdelivery_a",
        last_closed_delivery_sequence=1,
        state={"status": "RUNNING"},
        last_applied_event_id="mqrtevt_a",
    )
    drifted_time = AlgoStateSnapshotV2.model_validate(
        {
            **valid_snapshot.model_dump(mode="python"),
            "updated_at_utc": "2026-07-22T02:30:00Z",
        }
    )
    with pytest.raises(ValueError, match="logical time"):
        drifted_time.validate_against_authority_v1(
            plugin_manifest=manifest,
            deterministic_context=deterministic,
        )

    start = _start_context()
    raw = start.model_dump(mode="python")
    invalid_config = {"unexpected": True}
    with pytest.raises(ValidationError, match="config schema"):
        AlgoStartContextV1.model_validate(
            {
                **raw,
                "plugin_config": invalid_config,
                "plugin_config_sha256": hash_hex_v1("miniqmt_plugin_config_v2", invalid_config),
            }
        )


def test_delivery_timer_and_diagnostic_identities_are_recomputed_on_readback() -> None:
    event = _tick_event()
    delivery = AlgoEventDeliveryV1.create(
        event=event,
        algo_instance_id="mqalgo_a",
        plugin_manifest_sha256="a" * 64,
        algo_delivery_sequence=1,
        previous_delivery_id=None,
        status=DeliveryStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=None,
        last_error_json=None,
        created_at_utc="2026-07-22T01:30:00Z",
        updated_at_utc="2026-07-22T01:30:00Z",
    )
    with pytest.raises(ValidationError, match="delivery_id"):
        AlgoEventDeliveryV1.model_validate({**delivery.model_dump(mode="python"), "delivery_id": "mqdelivery_wrong"})

    timer = _timer_mutation(ordinal=0)
    with pytest.raises(ValidationError, match="schedule_id"):
        TimerMutationV1.model_validate({**timer.model_dump(mode="python"), "schedule_id": "mqtimersched_wrong"})
    with pytest.raises(ValidationError, match="timer_occurrence_id"):
        TimerMutationV1.model_validate({**timer.model_dump(mode="python"), "timer_occurrence_id": "mqtimerocc_wrong"})

    cancelled_timer = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.CANCEL,
        algo_instance_id="mqalgo_a",
        transition_id="mqtransition_a",
        ordinal=0,
        timer_name="twap_second",
        schedule_epoch="epoch_1",
        due_at_exchange_utc=None,
        catch_up_policy="NO_CATCH_UP_BURST",
        payload={"reason": "terminal"},
    )
    assert cancelled_timer.timer_occurrence_id is None
    with pytest.raises(ValidationError, match="must not fabricate"):
        TimerMutationV1.model_validate(
            {
                **cancelled_timer.model_dump(mode="python"),
                "due_at_exchange_utc": "2026-07-22T01:30:01Z",
            }
        )

    deterministic = _deterministic_context_for_manifest(_manifest())
    diagnostic = DiagnosticObservationV1.create(
        deterministic_context=deterministic,
        transition_id="mqtransition_a",
        ordinal=0,
        severity=DiagnosticSeverityV1.ERROR,
        reason_code="MINIQMT_PLUGIN_STATE_SCHEMA_INVALID",
        message="state schema invalid",
        context={"field": "state"},
    )
    assert diagnostic.observed_at_logical_utc == deterministic.logical_time_utc
    with pytest.raises(ValidationError, match="observation_id"):
        DiagnosticObservationV1.model_validate(
            {**diagnostic.model_dump(mode="python"), "observation_id": "mqdiag_wrong"}
        )
    drifted_diagnostic_payload = diagnostic.model_dump(mode="json")
    drifted_diagnostic_payload["observed_at_logical_utc"] = "2026-07-22T02:30:00.000000Z"
    drifted_identity_payload = {
        key: value for key, value in drifted_diagnostic_payload.items() if key != "observation_id"
    }
    drifted_diagnostic_payload["observation_id"] = "mqdiag_" + hash_hex_v1(
        "miniqmt_diagnostic_observation_identity_v1",
        drifted_identity_payload,
    )
    drifted_diagnostic = DiagnosticObservationV1.model_validate_json(json.dumps(drifted_diagnostic_payload))
    with pytest.raises(ValueError, match="logical time"):
        drifted_diagnostic.validate_against_context_v1(deterministic)


def test_manifest_rejects_invalid_json_schema_definitions() -> None:
    assert (
        _manifest(
            {
                "$defs": {"mode": {"const": "LIMIT_TRIGGER_BY_BEST_QUOTE"}},
                "type": "object",
                "properties": {"price_mode": {"$ref": "#/$defs/mode"}},
            }
        ).config_schema_version
        == "sniper_config_v2"
    )
    with pytest.raises(ValidationError, match="valid JSON schema"):
        _manifest({"type": "not-a-json-schema-type"})
    with pytest.raises(ValidationError, match="external schema reference is forbidden"):
        _manifest({"$ref": "https://example.invalid/external-schema.json"})
    with pytest.raises(ValidationError, match="reference target does not exist"):
        _manifest({"$ref": "#/$defs/missing", "$defs": {}})


def test_schema_violation_evidence_is_bounded_without_hiding_truncation() -> None:
    required_fields = [f"required_{index}" for index in range(40)]
    manifest = _manifest(
        {
            "type": "object",
            "additionalProperties": False,
            "required": required_fields,
            "properties": {field: {"type": "string"} for field in required_fields},
        }
    )
    with pytest.raises(ValidationError, match="additional violations omitted after limit=32"):
        _start_context_for_manifest(manifest, {})
