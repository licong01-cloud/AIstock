from __future__ import annotations

import pytest

from backend.services.miniqmt_execution_runtime.kernel_creation import KernelAlgoCreationCoordinatorV1
from backend.services.miniqmt_execution_runtime.kernel_current_three_contracts import CurrentThreeContractError
from backend.services.miniqmt_execution_runtime.kernel_current_three_shadow_orchestration import (
    build_current_three_shadow_creation_request_v1,
    build_current_three_shadow_delivery_input_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_current_three_shadow_runner import (
    build_current_three_parity_input_from_shadow_v1,
    build_current_three_shadow_event_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_delivery import build_command_lifecycle_projection_v1
from backend.services.miniqmt_execution_runtime.kernel_delivery import (
    invoke_plugin_transition_v1,
    resolve_plugin_for_restore_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_materializer import materialize_applied_transition_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    DeliveryStatusV1,
    KernelProjectionTypeV1,
    kernel_lease_fence_token_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_registry import PluginRouteCompatibilityReceiptV1
from backend.services.miniqmt_execution_runtime.repository import InMemoryMiniQMTExecutionRuntimeRepository
from backend.tests.miniqmt_execution_runtime.test_current_three_shadow_source import _algo, _child, _events, _runtime
from backend.tests.miniqmt_execution_runtime.test_kernel_creation import _CapturingRepository, _catalog, _gateway


def _read_and_input():
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    repo.upsert_algo_instance(
        _algo().model_copy(
            update={
                "metadata": {
                    "config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
                    "limit_price_decimal": "10",
                    "pricetick_decimal": "0.01",
                    "min_volume": 100,
                    "volume_increment": 100,
                }
            }
        )
    )
    repo.upsert_child_order(_child())
    tick = _events()[0].model_copy(
        update={
            "payload": {
                **_events()[0].payload,
                "generation": 1,
                "exchange_trade_date": "2026-07-29",
                "session_epoch": "session_shadow_am",
                "session_phase": "CONTINUOUS_AM",
            }
        }
    )
    repo.append_event(tick)
    read = repo.read_current_three_shadow_snapshot("runtime_shadow")
    parity_input, raw_events = build_current_three_parity_input_from_shadow_v1(
        read, legacy_algo_instance_id="legacy_algo_1"
    )
    return read, parity_input, raw_events[0]


def test_shadow_creation_and_delivery_inputs_use_real_k2_contracts_without_broker_route() -> None:
    read, parity_input, raw = _read_and_input()
    gateway = _gateway()
    request = build_current_three_shadow_creation_request_v1(
        read=read,
        parity_input=parity_input,
        gateway_catalog=gateway,
    )
    capture = _CapturingRepository()
    created = KernelAlgoCreationCoordinatorV1(
        repository=capture,
        catalog_runtime=_catalog(),
        gateway_catalog=gateway,
    ).create(request)
    assert created["algo"].status.value == "ACTIVE", created["receipt"].model_dump(mode="json")
    assert capture.bundle.transition_bundle.command_outboxes == ()

    event = build_current_three_shadow_event_v1(
        parity_input=parity_input,
        raw=raw,
        sequence=2,
        association=None,
    )
    delivery = AlgoDeliveryPersistenceV1.create(
        delivery=AlgoEventDeliveryV1.create(
            event=event,
            algo_instance_id=created["algo"].algo_instance_id,
            plugin_manifest_sha256=created["algo"].plugin_manifest_sha256,
            algo_delivery_sequence=2,
            previous_delivery_id=created["delivery"].delivery_id,
            status=DeliveryStatusV1.PENDING,
            attempt_count=0,
            lease_owner=None,
            lease_expires_at=None,
            transition_id=None,
            last_error_json=None,
            created_at_utc=event.event_time_utc,
            updated_at_utc=event.event_time_utc,
        ),
        lease_epoch=0,
        lease_fence_token=None,
        row_version=1,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=None,
        closed_at_utc=None,
    )
    owner = "worker_shadow:incarnation_shadow"
    claimed_payload = delivery.model_dump(mode="python")
    claimed_payload.update(
        status=DeliveryStatusV1.CLAIMED,
        attempt_count=1,
        lease_owner=owner,
        lease_expires_at="2026-07-29T01:31:00Z",
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="DELIVERY", owner_id=delivery.delivery_id, lease_epoch=1, lease_owner=owner
        ),
        row_version=2,
    )
    claimed = AlgoDeliveryPersistenceV1.model_validate(claimed_payload).validate_successor_v1(delivery)
    lifecycle = build_command_lifecycle_projection_v1(
        event=event,
        delivery=claimed,
        previous_state=capture.bundle.transition_bundle.after_state,
        mappings=(),
        outboxes=(),
    )
    route = PluginRouteCompatibilityReceiptV1.create(
        catalog_snapshot=_catalog().snapshot,
        plugin_key=_catalog().plugin_key_for_new_instance(parity_input.algo_code),
        gateway_catalog=gateway,
    ).validate_against_authority_v1(catalog_snapshot=_catalog().snapshot, gateway_catalog=gateway)
    inputs = build_current_three_shadow_delivery_input_v1(
        read=read,
        parity_input=parity_input,
        event=event,
        delivery=claimed,
        algo=created["algo"],
        previous_state=capture.bundle.transition_bundle.after_state,
        lifecycle_projection=lifecycle,
        route_receipt=route,
        expected_legacy_child_order_ids=("legacy_child_1",),
    )
    projection_types = {
        item.projection_type for item in inputs.services.execution_projection_set.ordered_projection_refs
    }
    assert projection_types == {
        KernelProjectionTypeV1.MARKET_DATA,
        KernelProjectionTypeV1.OMS_PREFLIGHT,
        KernelProjectionTypeV1.RISK_DECISION,
        KernelProjectionTypeV1.KILL_SWITCH_STATE,
        KernelProjectionTypeV1.ROUTE_COMPATIBILITY,
    }
    assert inputs.command_lifecycle_projection == lifecycle
    assert inputs.services.market_data_projection_id == "md_1"
    resolved = resolve_plugin_for_restore_v1(
        catalog_runtime=_catalog(),
        plugin_id=created["algo"].plugin_id,
        plugin_version=created["algo"].plugin_version,
        plugin_manifest_sha256=created["algo"].plugin_manifest_sha256,
        canonical_plugin_config=thaw_json_v1(created["algo"].plugin_config_json),
        plugin_config_sha256=created["algo"].plugin_config_sha256,
    )
    transition = invoke_plugin_transition_v1(
        plugin=resolved.plugin,
        expected_manifest=resolved.descriptor.manifest,
        state_codec=resolved.state_codec,
        state=capture.bundle.transition_bundle.after_state,
        event=event,
        services=inputs.services,
        deterministic_context=inputs.deterministic_context,
    )
    materialized = materialize_applied_transition_v1(
        event=event,
        predecessor_delivery=claimed,
        previous_algo=created["algo"],
        transition=transition,
        projection_set=inputs.services.execution_projection_set,
        consumed_lineage_refs=inputs.consumed_lineage_refs,
        strategy_slot_id=created["algo"].strategy_slot_id,
        parent_intent_id=created["algo"].parent_intent_id,
        compatibility_receipt_sha256=created["algo"].compatibility_receipt_sha256,
        plugin_config=thaw_json_v1(created["algo"].plugin_config_json),
        plugin_config_sha256=created["algo"].plugin_config_sha256,
        target_quantity=created["algo"].target_quantity,
        algo_code=created["algo"].algo_code,
        symbol=created["algo"].symbol,
        side=created["algo"].side,
        command_lifecycle_projection=lifecycle,
        existing_mappings_by_local_vt_orderid={},
        existing_timer_schedules={},
        initialization=False,
    )
    assert len(materialized.new_child_mappings) == 1
    assert len(materialized.command_outboxes) == 1
    assert materialized.command_outboxes[0].status.value == "PENDING"
    assert materialized.command_outboxes[0].attempt_count == 0
    assert materialized.command_outboxes[0].broker_called is None


def test_shadow_delivery_input_rejects_ambiguous_or_missing_historical_child() -> None:
    read, parity_input, raw = _read_and_input()
    request = build_current_three_shadow_creation_request_v1(
        read=read, parity_input=parity_input, gateway_catalog=_gateway()
    )
    capture = _CapturingRepository()
    created = KernelAlgoCreationCoordinatorV1(
        repository=capture, catalog_runtime=_catalog(), gateway_catalog=_gateway()
    ).create(request)
    event = build_current_three_shadow_event_v1(parity_input=parity_input, raw=raw, sequence=2, association=None)
    delivery = created["delivery"]
    lifecycle = build_command_lifecycle_projection_v1(
        event=event,
        delivery=delivery.model_copy(update={"event_id": event.event_id}),
        previous_state=capture.bundle.transition_bundle.after_state,
        mappings=(),
        outboxes=(),
    )
    route = PluginRouteCompatibilityReceiptV1.create(
        catalog_snapshot=_catalog().snapshot,
        plugin_key=_catalog().plugin_key_for_new_instance(parity_input.algo_code),
        gateway_catalog=_gateway(),
    ).validate_against_authority_v1(catalog_snapshot=_catalog().snapshot, gateway_catalog=_gateway())
    common = dict(
        read=read,
        parity_input=parity_input,
        event=event,
        delivery=delivery,
        algo=created["algo"],
        previous_state=capture.bundle.transition_bundle.after_state,
        lifecycle_projection=lifecycle,
        route_receipt=route,
    )
    with pytest.raises(CurrentThreeContractError):
        build_current_three_shadow_delivery_input_v1(
            **common,
            expected_legacy_child_order_ids=("legacy_child_1", "legacy_child_1"),
        )
    with pytest.raises(CurrentThreeContractError):
        build_current_three_shadow_delivery_input_v1(
            **common,
            expected_legacy_child_order_ids=("missing_child",),
        )


def test_shadow_creation_requires_strict_parity_and_gateway_carriers() -> None:
    read, parity_input, _raw = _read_and_input()
    with pytest.raises(TypeError, match="parity_input"):
        build_current_three_shadow_creation_request_v1(
            read=read,
            parity_input=object(),  # type: ignore[arg-type]
            gateway_catalog=_gateway(),
        )
    with pytest.raises(TypeError, match="gateway_catalog"):
        build_current_three_shadow_creation_request_v1(
            read=read,
            parity_input=parity_input,
            gateway_catalog=object(),  # type: ignore[arg-type]
        )
