from __future__ import annotations

from datetime import date, datetime, timezone
from functools import lru_cache

import pytest

from backend.execution_algos.vnpy_compat.receipts import build_current_three_compatibility_receipts_v1
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v3,
    current_three_descriptors_v3,
)
from backend.execution_algos.vnpy_style.plugin_factories import current_three_process_bindings_v3
from backend.services.miniqmt_execution_runtime.full_five_catalog_authority import (
    build_full_five_catalog_authority_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_creation import (
    KernelAlgoCreationCoordinatorV1,
    KernelAlgoCreationCoordinatorV2,
)
from backend.services.miniqmt_execution_runtime.kernel_delivery import (
    KernelAlgoCreationRequestV1,
    KernelAlgoCreationRequestV2,
    KernelPluginInvocationError,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import build_plugin_catalog_v2
from backend.services.miniqmt_execution_runtime.kernel_product_contracts import (
    KernelProductContractError,
    ProductRouteCutoverReceiptV1,
    ProductRouteOwnerKindV1,
    ProductRouteOwnerV1,
)
from backend.services.miniqmt_execution_runtime.kernel_product_cutover import (
    KernelProductCutoverCoordinator,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ExecutionProjectionRefV1,
    GatewayCapabilityCatalogV1,
    KernelProjectionTypeV1,
    MarketDataCapabilityV1,
    OrderTypeV1,
    SessionPhaseV1,
    SideV1,
)


def _request_v2() -> KernelAlgoCreationRequestV2:
    return KernelAlgoCreationRequestV2.from_v1(
        _request(),
        binding_id="binding_k6d",
        product_route_cutover_receipt_sha256="d" * 64,
        product_route_owner_sha256="e" * 64,
        product_route_epoch=1,
        effective_new_instance_sequence=7,
    )


@lru_cache(maxsize=1)
def _catalog():
    return build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v3(),
        creation_bindings=current_three_creation_bindings_v3(),
        process_bindings=current_three_process_bindings_v3(),
        pinned_compatibility_receipts=build_current_three_compatibility_receipts_v1(),
    )


@lru_cache(maxsize=1)
def _full_five_authority():
    return build_full_five_catalog_authority_v1(gateway_catalog=_gateway())


def _gateway() -> GatewayCapabilityCatalogV1:
    gateway_payload = {
        "schema_version": "miniqmt_gateway_capability_catalog_v1",
        "route_id": "route.sim.k6d",
        "quote_source": "B0_QUOTE_V2",
        "gateway_backend": "minqmt_sim",
        "order_types": [OrderTypeV1.LIMIT.value],
        "market_data_capabilities": [
            item.value for item in sorted(MarketDataCapabilityV1, key=lambda item: item.value)
        ],
        "session_phases": [item.value for item in sorted(SessionPhaseV1, key=lambda item: item.value)],
        "idempotent_submit_by_client_ref": False,
        "exact_order_id_cancel": True,
    }
    return GatewayCapabilityCatalogV1(
        schema_version="miniqmt_gateway_capability_catalog_v1",
        route_id="route.sim.k6d",
        quote_source="B0_QUOTE_V2",
        gateway_backend="minqmt_sim",
        order_types=(OrderTypeV1.LIMIT,),
        market_data_capabilities=tuple(sorted(MarketDataCapabilityV1, key=lambda item: item.value)),
        session_phases=tuple(sorted(SessionPhaseV1, key=lambda item: item.value)),
        idempotent_submit_by_client_ref=False,
        exact_order_id_cancel=True,
        catalog_sha256=hash_hex_v1("miniqmt_gateway_capability_catalog_v1", gateway_payload),
    )


def _request() -> KernelAlgoCreationRequestV1:
    config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    contract = {"symbol": "600000.SH", "min_volume": 100, "volume_increment": 100}
    account = {"account_projection_id": "account_k6d", "available_cash_decimal": "100000.000000"}
    gateway = _gateway()
    refs = tuple(
        sorted(
            (
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.CONTRACT,
                    projection_id="contract_k6d",
                    projection_version="contract_v1",
                    payload_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
                    source_event_id=None,
                    logical_at_utc="2026-08-04T01:20:00Z",
                ),
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.ACCOUNT,
                    projection_id="account_k6d",
                    projection_version="account_v1",
                    payload_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
                    source_event_id=None,
                    logical_at_utc="2026-08-04T01:20:00Z",
                ),
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.MARKET_CAPABILITY,
                    projection_id="gateway_k6d",
                    projection_version="gateway_v1",
                    payload_sha256=hash_hex_v1(
                        "miniqmt_market_capability_projection_v1", gateway.model_dump(mode="json")
                    ),
                    source_event_id=None,
                    logical_at_utc="2026-08-04T01:20:00Z",
                ),
            ),
            key=lambda item: (item.projection_type.value, item.projection_id),
        )
    )
    return KernelAlgoCreationRequestV1(
        runtime_id="runtime_k6d",
        parent_intent_id="intent_k6d",
        strategy_slot_id="slot_k6d",
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
        market_capability_projection=gateway.model_dump(mode="json"),
        market_capability_projection_sha256=hash_hex_v1(
            "miniqmt_market_capability_projection_v1", gateway.model_dump(mode="json")
        ),
        projection_refs=refs,
        execution_plan_id="plan_k6d",
        execution_plan_sha256="a" * 64,
        release_id="release_k6d",
        release_sha256="b" * 64,
        policy_id="policy_k6d",
        policy_sha256="c" * 64,
        logical_time_utc="2026-08-04T01:20:00Z",
        exchange_trade_date="2026-08-04",
        session_epoch="session_k6d",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
    )


def test_product_creation_request_v2_hash_closes_exact_binding_and_route_lineage() -> None:
    request = _request_v2()

    assert request.validate_hashes_v2() is request
    assert request.binding_id == "binding_k6d"
    assert request.product_route_epoch == 1
    assert request.effective_new_instance_sequence == 7


@pytest.mark.parametrize(
    "field_name,value",
    (
        ("binding_id", "binding_other"),
        ("product_route_cutover_receipt_sha256", "f" * 64),
        ("product_route_owner_sha256", "a" * 64),
        ("product_route_epoch", 2),
        ("effective_new_instance_sequence", 8),
    ),
)
def test_product_creation_request_v2_rejects_hash_correct_shape_with_lineage_drift(
    field_name: str, value: object
) -> None:
    request = _request_v2().model_copy(update={field_name: value})

    with pytest.raises(ValueError, match="product creation request hash differs"):
        request.validate_hashes_v2()


def _route_owner(
    *,
    route_owner: ProductRouteOwnerKindV1 = ProductRouteOwnerKindV1.KERNEL_V2,
    runtime_id: str = "runtime_k6d",
    binding_id: str = "binding_k6d",
) -> ProductRouteOwnerV1:
    receipt = ProductRouteCutoverReceiptV1.create(
        runtime_id=runtime_id,
        binding_id=binding_id,
        trade_date=date(2026, 8, 4),
        route_epoch=1,
        route_owner=route_owner,
        effective_new_instance_sequence=7,
        legacy_active_instance_count=0,
        kernel_active_instance_count=0,
        catalog_sha256="a" * 64,
        gateway_capability_catalog_sha256="b" * 64,
        exchange_session_authority_sha256="c" * 64,
        migration_readback_sha256="d" * 64,
        product_authority_schema_sha256="e" * 64,
        previous_receipt_sha256=None,
        created_at_utc=datetime(2026, 8, 4, 1, 20, tzinfo=timezone.utc),
    )
    return ProductRouteOwnerV1.create(receipt=receipt, row_version=1)


class _CapturingCutoverRepository:
    def __init__(self, owner: ProductRouteOwnerV1) -> None:
        self.owner = owner
        self.calls: list[tuple[str, str, date, str]] = []

    def activate_kernel_v2_route_v1(
        self, *, runtime_id: str, binding_id: str, trade_date: date, worker_incarnation_id: str
    ) -> ProductRouteOwnerV1:
        self.calls.append((runtime_id, binding_id, trade_date, worker_incarnation_id))
        return self.owner


def test_product_cutover_coordinator_only_accepts_stable_identity_and_returns_kernel_owner() -> None:
    repository = _CapturingCutoverRepository(_route_owner())
    coordinator = KernelProductCutoverCoordinator(repository=repository)

    owner = coordinator.activate_kernel_v2_route_v1(
        runtime_id="runtime_k6d",
        binding_id="binding_k6d",
        trade_date=date(2026, 8, 4),
        worker_incarnation_id="worker_k6d",
    )

    assert owner == _route_owner()
    assert repository.calls == [("runtime_k6d", "binding_k6d", date(2026, 8, 4), "worker_k6d")]


def test_product_cutover_coordinator_rejects_legacy_owner_without_fallback() -> None:
    coordinator = KernelProductCutoverCoordinator(
        repository=_CapturingCutoverRepository(_route_owner(route_owner=ProductRouteOwnerKindV1.LEGACY_DRAIN_ONLY))
    )

    with pytest.raises(KernelProductContractError, match="legacy") as raised:
        coordinator.activate_kernel_v2_route_v1(
            runtime_id="runtime_k6d",
            binding_id="binding_k6d",
            trade_date=date(2026, 8, 4),
            worker_incarnation_id="worker_k6d",
        )

    assert raised.value.reason_code == "MINIQMT_K6_ROUTE_LEGACY_OWNER_PRESENT"


def test_product_cutover_coordinator_converts_malformed_owner_readback_to_typed_error() -> None:
    malformed_owner = _route_owner().model_copy(update={"owner_sha256": "a" * 64})
    coordinator = KernelProductCutoverCoordinator(repository=_CapturingCutoverRepository(malformed_owner))

    with pytest.raises(KernelProductContractError, match="malformed") as raised:
        coordinator.activate_kernel_v2_route_v1(
            runtime_id="runtime_k6d",
            binding_id="binding_k6d",
            trade_date=date(2026, 8, 4),
            worker_incarnation_id="worker_k6d",
        )

    assert raised.value.reason_code == "MINIQMT_K6_ROUTE_OWNER_READBACK_INVALID"
    assert raised.value.context["broker_called"] is False


def test_product_cutover_coordinator_rejects_non_callable_repository_seam() -> None:
    class _InvalidRepository:
        activate_kernel_v2_route_v1 = None

    with pytest.raises(TypeError, match="must implement"):
        KernelProductCutoverCoordinator(repository=_InvalidRepository())  # type: ignore[arg-type]


def test_product_cutover_coordinator_rejects_non_owner_readback_with_typed_evidence() -> None:
    coordinator = KernelProductCutoverCoordinator(repository=_CapturingCutoverRepository(object()))  # type: ignore[arg-type]

    with pytest.raises(KernelProductContractError, match="strict product route owner") as raised:
        coordinator.activate_kernel_v2_route_v1(
            runtime_id="runtime_k6d",
            binding_id="binding_k6d",
            trade_date=date(2026, 8, 4),
            worker_incarnation_id="worker_k6d",
        )

    assert raised.value.reason_code == "MINIQMT_K6_ROUTE_OWNER_READBACK_INVALID"
    assert raised.value.context["actual_type"] == "object"


def test_product_cutover_coordinator_rejects_owner_identity_drift() -> None:
    coordinator = KernelProductCutoverCoordinator(
        repository=_CapturingCutoverRepository(_route_owner(binding_id="binding_other"))
    )

    with pytest.raises(KernelProductContractError, match="does not close") as raised:
        coordinator.activate_kernel_v2_route_v1(
            runtime_id="runtime_k6d",
            binding_id="binding_k6d",
            trade_date=date(2026, 8, 4),
            worker_incarnation_id="worker_k6d",
        )

    assert raised.value.reason_code == "MINIQMT_K6_ROUTE_OWNER_IDENTITY_DRIFT"


def test_product_cutover_coordinator_rejects_invalid_stable_identity_before_repository_call() -> None:
    repository = _CapturingCutoverRepository(_route_owner())
    coordinator = KernelProductCutoverCoordinator(repository=repository)

    with pytest.raises(TypeError, match="runtime_id"):
        coordinator.activate_kernel_v2_route_v1(
            runtime_id=" runtime_k6d",
            binding_id="binding_k6d",
            trade_date=date(2026, 8, 4),
            worker_incarnation_id="worker_k6d",
        )

    assert repository.calls == []


class _CapturingCreationRepositoryV2:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.bundle = None

    def initialize_product_algo_atomic_v3(
        self,
        *,
        runtime_id,
        worker_incarnation_id,
        event_key_sha256,
        creation_authority,
        creation_binding,
        bundle_builder,
    ):
        self.calls.append(
            {
                "runtime_id": runtime_id,
                "worker_incarnation_id": worker_incarnation_id,
                "event_key_sha256": event_key_sha256,
                "creation_authority": creation_authority,
                "creation_binding": creation_binding,
            }
        )
        self.bundle = bundle_builder(7)
        assert self.bundle.event.event_key_sha256 == event_key_sha256
        return {
            "event": self.bundle.event,
            "ingress_receipt": self.bundle.transition_bundle.receipt,
            "algo": self.bundle.transition_bundle.algo_instance,
            "delivery": self.bundle.transition_bundle.delivery,
        }


def test_final_product_creation_uses_v2_repository_and_unambiguous_route_lineage() -> None:
    repository = _CapturingCreationRepositoryV2()
    request = _request_v2()
    full_authority = _full_five_authority()

    result = KernelAlgoCreationCoordinatorV2(
        repository=repository,
        catalog_runtime=full_authority.catalog_runtime,
        gateway_catalog=_gateway(),
        worker_incarnation_id="worker_k6d",
        facade_authority=full_authority.conformance_authority,
    ).create(request)

    assert len(repository.calls) == 1
    assert repository.calls[0]["worker_incarnation_id"] == "worker_k6d"
    assert repository.calls[0]["creation_authority"] is request
    assert result["event"].payload_schema_version == "miniqmt_algo_start_v2"
    payload = thaw_json_v1(result["event"].payload)
    assert payload["plugin_route_compatibility_receipt_sha256"]
    assert payload["product_route_cutover_receipt_sha256"] == request.product_route_cutover_receipt_sha256
    assert payload["product_route_owner_sha256"] == request.product_route_owner_sha256
    assert payload["product_route_epoch"] == request.product_route_epoch
    assert payload["effective_new_instance_sequence"] == request.effective_new_instance_sequence
    assert payload["binding_id"] == request.binding_id
    assert "route_receipt_sha256" not in payload
    assert "route_compatibility_receipt" not in payload


def test_final_product_creation_rejects_current_three_or_unsealed_facade_authority() -> None:
    full_authority = _full_five_authority()
    with pytest.raises(KernelPluginInvocationError, match="full-five plugin catalog"):
        KernelAlgoCreationCoordinatorV2(
            repository=_CapturingCreationRepositoryV2(),
            catalog_runtime=_catalog(),
            gateway_catalog=_gateway(),
            worker_incarnation_id="worker_k6d",
            facade_authority=full_authority.conformance_authority,
        )
    with pytest.raises(TypeError, match="requires VnpyFacadeConformanceAuthorityV2"):
        KernelAlgoCreationCoordinatorV2(
            repository=_CapturingCreationRepositoryV2(),
            catalog_runtime=full_authority.catalog_runtime,
            gateway_catalog=_gateway(),
            worker_incarnation_id="worker_k6d",
            facade_authority=None,  # type: ignore[arg-type]
        )


def test_shadow_creation_rejects_v2_product_request_instead_of_downgrading_route_authority() -> None:
    with pytest.raises(TypeError, match="KernelAlgoCreationRequestV1"):
        KernelAlgoCreationCoordinatorV1(
            repository=object(),  # type: ignore[arg-type]
            catalog_runtime=_catalog(),
            gateway_catalog=_gateway(),
        ).create(_request_v2())
