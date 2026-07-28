from __future__ import annotations

from functools import lru_cache
from types import SimpleNamespace

import pytest

import backend.services.miniqmt_execution_runtime.kernel_creation as kernel_creation
from backend.execution_algos.vnpy_compat.receipts import build_current_three_compatibility_receipts_v1
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v1,
    current_three_descriptors_v2,
    current_three_process_bindings_v2,
)
from backend.services.miniqmt_execution_runtime.kernel_creation import KernelAlgoCreationCoordinatorV1
from backend.services.miniqmt_execution_runtime.kernel_delivery import KernelAlgoCreationRequestV1
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ExecutionProjectionRefV1,
    AlgoInitializationV1,
    AlgoStateSnapshotV2,
    GatewayCapabilityCatalogV1,
    KernelProjectionTypeV1,
    MarketDataCapabilityV1,
    OrderTypeV1,
    SessionPhaseV1,
    SideV1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import build_plugin_catalog_v2
from backend.tests.miniqmt_execution_runtime.test_current_three_plugin_manifests import _state


class _CapturingRepository:
    def __init__(self) -> None:
        self.bundle = None

    def initialize_algo_atomic(self, *, runtime_id, event_key_sha256, creation_authority, bundle_builder):
        assert creation_authority.runtime_id == runtime_id
        self.bundle = bundle_builder(1)
        assert self.bundle.event.runtime_id == runtime_id
        assert self.bundle.event.event_key_sha256 == event_key_sha256
        return {
            "event": self.bundle.event,
            "receipt": self.bundle.transition_bundle.receipt,
            "algo": self.bundle.transition_bundle.algo_instance,
            "delivery": self.bundle.transition_bundle.delivery,
        }


@lru_cache(maxsize=1)
def _catalog():
    return build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v2(),
        creation_bindings=current_three_creation_bindings_v1(),
        process_bindings=current_three_process_bindings_v2(),
        pinned_compatibility_receipts=build_current_three_compatibility_receipts_v1(),
    )


def _gateway(*, exact_order_id_cancel: bool = True) -> GatewayCapabilityCatalogV1:
    values = {
        "schema_version": "miniqmt_gateway_capability_catalog_v1",
        "route_id": "route.sim.k2b",
        "quote_source": "B0_QUOTE_V2",
        "gateway_backend": "minqmt_sim",
        "order_types": (OrderTypeV1.LIMIT,),
        "market_data_capabilities": tuple(sorted(MarketDataCapabilityV1, key=lambda item: item.value)),
        "session_phases": tuple(sorted(SessionPhaseV1, key=lambda item: item.value)),
        "idempotent_submit_by_client_ref": False,
        "exact_order_id_cancel": exact_order_id_cancel,
    }
    payload = {
        **values,
        "order_types": [item.value for item in values["order_types"]],
        "market_data_capabilities": [item.value for item in values["market_data_capabilities"]],
        "session_phases": [item.value for item in values["session_phases"]],
    }
    return GatewayCapabilityCatalogV1(
        **values,
        catalog_sha256=hash_hex_v1("miniqmt_gateway_capability_catalog_v1", payload),
    )


def _request() -> KernelAlgoCreationRequestV1:
    config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    contract = {"symbol": "600000.SH", "min_volume": 100, "volume_increment": 100}
    account = {"account_projection_id": "account_k2b", "available_cash_decimal": "100000.000000"}
    capability = _gateway().model_dump(mode="json")
    refs = tuple(
        sorted(
            (
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.CONTRACT,
                    projection_id="contract_k2b",
                    projection_version="contract_v1",
                    payload_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
                    source_event_id=None,
                    logical_at_utc="2026-07-26T01:20:00Z",
                ),
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.ACCOUNT,
                    projection_id="account_k2b",
                    projection_version="account_v1",
                    payload_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
                    source_event_id=None,
                    logical_at_utc="2026-07-26T01:20:00Z",
                ),
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.MARKET_CAPABILITY,
                    projection_id="gateway_k2b",
                    projection_version="gateway_v1",
                    payload_sha256=hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
                    source_event_id=None,
                    logical_at_utc="2026-07-26T01:20:00Z",
                ),
            ),
            key=lambda item: (item.projection_type.value, item.projection_id),
        )
    )
    return KernelAlgoCreationRequestV1(
        runtime_id="runtime_creation_k2b",
        parent_intent_id="intent_creation_k2b",
        strategy_slot_id="slot_creation_k2b",
        symbol="600000.SH",
        side=SideV1.BUY,
        limit_price_decimal="10.000000",
        parent_quantity=100,
        min_volume=100,
        volume_increment=100,
        algo_code="SNIPER_MINIQMT",
        plugin_config=config,
        plugin_config_sha256=hash_hex_v1("miniqmt_plugin_config_v2", config),
        contract_projection=contract,
        contract_projection_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
        account_projection=account,
        account_projection_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
        market_capability_projection=capability,
        market_capability_projection_sha256=hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
        projection_refs=refs,
        execution_plan_id="plan_creation_k2b",
        execution_plan_sha256="a" * 64,
        release_id="release_creation_k2b",
        release_sha256="b" * 64,
        policy_id="policy_creation_k2b",
        policy_sha256="c" * 64,
        logical_time_utc="2026-07-26T01:20:00Z",
        exchange_trade_date="2026-07-26",
        session_epoch="session_creation_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
    )


def test_creation_coordinator_persists_pre_k4_binding_failure_without_legacy_fallback() -> None:
    config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    contract = {"symbol": "600000.SH", "min_volume": 100, "volume_increment": 100}
    account = {"account_projection_id": "account_k2b", "available_cash_decimal": "100000.000000"}
    capability = _gateway().model_dump(mode="json")
    refs = tuple(
        sorted(
            (
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.CONTRACT,
                    projection_id="contract_k2b",
                    projection_version="contract_v1",
                    payload_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
                    source_event_id=None,
                    logical_at_utc="2026-07-26T01:20:00Z",
                ),
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.ACCOUNT,
                    projection_id="account_k2b",
                    projection_version="account_v1",
                    payload_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
                    source_event_id=None,
                    logical_at_utc="2026-07-26T01:20:00Z",
                ),
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.MARKET_CAPABILITY,
                    projection_id="gateway_k2b",
                    projection_version="gateway_v1",
                    payload_sha256=hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
                    source_event_id=None,
                    logical_at_utc="2026-07-26T01:20:00Z",
                ),
            ),
            key=lambda item: (item.projection_type.value, item.projection_id),
        )
    )
    request = KernelAlgoCreationRequestV1(
        runtime_id="runtime_creation_k2b",
        parent_intent_id="intent_creation_k2b",
        strategy_slot_id="slot_creation_k2b",
        symbol="600000.SH",
        side=SideV1.BUY,
        limit_price_decimal="10.000000",
        parent_quantity=100,
        min_volume=100,
        volume_increment=100,
        algo_code="SNIPER_MINIQMT",
        plugin_config=config,
        plugin_config_sha256=hash_hex_v1("miniqmt_plugin_config_v2", config),
        contract_projection=contract,
        contract_projection_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
        account_projection=account,
        account_projection_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
        market_capability_projection=capability,
        market_capability_projection_sha256=hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
        projection_refs=refs,
        execution_plan_id="plan_creation_k2b",
        execution_plan_sha256="a" * 64,
        release_id="release_creation_k2b",
        release_sha256="b" * 64,
        policy_id="policy_creation_k2b",
        policy_sha256="c" * 64,
        logical_time_utc="2026-07-26T01:20:00Z",
        exchange_trade_date="2026-07-26",
        session_epoch="session_creation_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
    )
    repository = _CapturingRepository()
    result = KernelAlgoCreationCoordinatorV1(
        repository=repository,
        catalog_runtime=_catalog(),
        gateway_catalog=_gateway(),
    ).create(request)

    assert result["algo"].status.value == "FAILED"
    assert result["algo"].state_json is None
    assert result["delivery"].status.value == "FAILED_TERMINAL"
    assert result["receipt"].stable_reason_code == "MINIQMT_ALGO_PLUGIN_BINDING_INVALID"
    assert repository.bundle.transition_bundle.command_outboxes == ()
    projection_types = {
        item.projection_type for item in repository.bundle.transition_bundle.projection_set.ordered_projection_refs
    }
    assert KernelProjectionTypeV1.ROUTE_COMPATIBILITY in projection_types

    missing_account_ref = request.model_copy(
        update={
            "projection_refs": tuple(
                item for item in request.projection_refs if item.projection_type is not KernelProjectionTypeV1.ACCOUNT
            )
        }
    )
    with pytest.raises(ValueError, match="ACCOUNT ref"):
        KernelAlgoCreationCoordinatorV1(
            repository=_CapturingRepository(),
            catalog_runtime=_catalog(),
            gateway_catalog=_gateway(),
        ).create(missing_account_ref)


def test_creation_coordinator_success_path_and_pre_route_failures_are_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    runtime = _catalog()
    descriptor = next(
        item for item in runtime.snapshot.registration_descriptors if item.manifest.algo_code == request.algo_code
    )

    def initialize(*, start_context, **_values):
        state_payload = _state("SNIPER_MINIQMT")
        state_payload["parent_quantity"] = start_context.parent_quantity
        state = AlgoStateSnapshotV2.create(
            plugin_manifest=descriptor.manifest,
            deterministic_context=start_context.deterministic_context,
            transition_sequence=1,
            last_applied_delivery_sequence=1,
            last_applied_delivery_id=start_context.start_delivery_id,
            last_closed_delivery_sequence=1,
            state=state_payload,
            last_applied_event_id=start_context.start_event_id,
        )
        effect = {
            "next_state_sha256": state.state_sha256,
            "ordered_command_ids": [],
            "ordered_timer_mutation_ids": [],
            "ordered_diagnostic_observation_ids": [],
            "terminal_outcome": None,
        }
        return AlgoInitializationV1(
            schema_version="miniqmt_algo_initialization_v1",
            start_event_id=start_context.start_event_id,
            start_delivery_id=start_context.start_delivery_id,
            next_state=state,
            broker_commands=(),
            timer_mutations=(),
            diagnostic_observations=(),
            terminal_outcome=None,
            effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", effect),
        )

    monkeypatch.setattr(
        kernel_creation,
        "resolve_plugin_for_restore_v1",
        lambda **_values: SimpleNamespace(plugin=object()),
    )
    monkeypatch.setattr(kernel_creation, "invoke_plugin_initialize_v1", initialize)
    result = KernelAlgoCreationCoordinatorV1(
        repository=_CapturingRepository(),
        catalog_runtime=runtime,
        gateway_catalog=_gateway(),
    ).create(request)
    assert result["algo"].status.value == "ACTIVE", result["receipt"].model_dump(mode="json")
    assert result["delivery"].status.value == "APPLIED"

    with pytest.raises(TypeError, match="request"):
        KernelAlgoCreationCoordinatorV1(
            repository=_CapturingRepository(), catalog_runtime=runtime, gateway_catalog=_gateway()
        ).create(object())
    with pytest.raises(kernel_creation.KernelPluginInvocationError) as missing:
        KernelAlgoCreationCoordinatorV1(
            repository=_CapturingRepository(), catalog_runtime=runtime, gateway_catalog=_gateway()
        ).create(request.model_copy(update={"algo_code": "UNKNOWN_ALGO_K2B"}))
    assert missing.value.reason_code == "MINIQMT_ALGO_PLUGIN_BINDING_INVALID"
    with pytest.raises(kernel_creation.KernelPluginInvocationError) as unsupported:
        KernelAlgoCreationCoordinatorV1(
            repository=_CapturingRepository(),
            catalog_runtime=runtime,
            gateway_catalog=_gateway(exact_order_id_cancel=False),
        ).create(request)
    assert unsupported.value.reason_code == "MINIQMT_ALGO_ROUTE_COMPATIBILITY_FAILED"
