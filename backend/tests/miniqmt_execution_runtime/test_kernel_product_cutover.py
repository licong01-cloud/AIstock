from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from backend.services.miniqmt_execution_runtime.kernel_delivery import (
    KernelAlgoCreationRequestV2,
)
from backend.services.miniqmt_execution_runtime.kernel_product_contracts import (
    KernelProductContractError,
    ProductRouteCutoverReceiptV1,
    ProductRouteOwnerKindV1,
    ProductRouteOwnerV1,
)
from backend.services.miniqmt_execution_runtime.kernel_product_cutover import (
    KernelProductCutoverCoordinator,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_creation import _request


def _request_v2() -> KernelAlgoCreationRequestV2:
    return KernelAlgoCreationRequestV2.from_v1(
        _request(),
        binding_id="binding_k6d",
        product_route_cutover_receipt_sha256="d" * 64,
        product_route_owner_sha256="e" * 64,
        product_route_epoch=1,
        effective_new_instance_sequence=7,
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


def _route_owner(*, route_owner: ProductRouteOwnerKindV1 = ProductRouteOwnerKindV1.KERNEL_V2) -> ProductRouteOwnerV1:
    receipt = ProductRouteCutoverReceiptV1.create(
        runtime_id="runtime_k6d",
        binding_id="binding_k6d",
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
