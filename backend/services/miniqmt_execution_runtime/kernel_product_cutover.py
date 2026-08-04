"""K6-D final product-route coordinator.

The coordinator deliberately accepts only stable runtime identity.  Durable
binding, catalog, session, migration and fence authority remain repository
owned so callers cannot synthesize a route receipt or select an owner.
"""

from __future__ import annotations

from datetime import date
from typing import Protocol

from .kernel_product_contracts import (
    KernelProductContractError,
    ProductRouteOwnerKindV1,
    ProductRouteOwnerV1,
)


class KernelProductCutoverRepositoryV1(Protocol):
    """Repository-owned transaction seam for final KERNEL_V2 route selection."""

    def activate_kernel_v2_route_v1(
        self,
        *,
        runtime_id: str,
        binding_id: str,
        trade_date: date,
        worker_incarnation_id: str,
    ) -> ProductRouteOwnerV1: ...


class KernelProductCutoverCoordinator:
    """Expose the sole product entry for K6-D route activation."""

    def __init__(self, *, repository: KernelProductCutoverRepositoryV1) -> None:
        if not hasattr(repository, "activate_kernel_v2_route_v1"):
            raise TypeError("repository must implement activate_kernel_v2_route_v1")
        self._repository = repository

    def activate_kernel_v2_route_v1(
        self,
        runtime_id: str,
        binding_id: str,
        trade_date: date,
        worker_incarnation_id: str,
    ) -> ProductRouteOwnerV1:
        self._validate_identity(runtime_id, field_name="runtime_id")
        self._validate_identity(binding_id, field_name="binding_id")
        self._validate_identity(worker_incarnation_id, field_name="worker_incarnation_id")
        if type(trade_date) is not date:
            raise TypeError("trade_date must be an exact date")

        owner = self._repository.activate_kernel_v2_route_v1(
            runtime_id=runtime_id,
            binding_id=binding_id,
            trade_date=trade_date,
            worker_incarnation_id=worker_incarnation_id,
        )
        if not isinstance(owner, ProductRouteOwnerV1):
            raise KernelProductContractError(
                "MINIQMT_K6_ROUTE_OWNER_READBACK_INVALID",
                "route activation did not return a strict product route owner",
                context={
                    "runtime_id": runtime_id,
                    "binding_id": binding_id,
                    "trade_date": trade_date.isoformat(),
                    "actual_type": type(owner).__name__,
                    "broker_called": False,
                },
            )
        strict_owner = ProductRouteOwnerV1.model_validate(owner.model_dump(mode="python"), strict=True)
        if (
            strict_owner.runtime_id,
            strict_owner.binding_id,
            strict_owner.trade_date,
        ) != (runtime_id, binding_id, trade_date):
            raise KernelProductContractError(
                "MINIQMT_K6_ROUTE_OWNER_IDENTITY_DRIFT",
                "route activation owner does not close to requested stable identity",
                context={
                    "runtime_id": runtime_id,
                    "binding_id": binding_id,
                    "trade_date": trade_date.isoformat(),
                    "actual_runtime_id": strict_owner.runtime_id,
                    "actual_binding_id": strict_owner.binding_id,
                    "actual_trade_date": strict_owner.trade_date.isoformat(),
                    "broker_called": False,
                },
            )
        if strict_owner.route_owner is ProductRouteOwnerKindV1.LEGACY_DRAIN_ONLY:
            raise KernelProductContractError(
                "MINIQMT_K6_ROUTE_LEGACY_OWNER_PRESENT",
                "current route owner is legacy drain-only and cannot create KERNEL_V2 instances",
                context={
                    "runtime_id": runtime_id,
                    "binding_id": binding_id,
                    "trade_date": trade_date.isoformat(),
                    "route_epoch": strict_owner.current_route_epoch,
                    "receipt_sha256": strict_owner.current_receipt_sha256,
                    "owner_sha256": strict_owner.owner_sha256,
                    "broker_called": False,
                },
            )
        if strict_owner.route_owner is not ProductRouteOwnerKindV1.KERNEL_V2:
            raise KernelProductContractError(
                "MINIQMT_K6_ROUTE_OWNER_INVALID",
                "route activation returned an unsupported route owner",
                context={
                    "runtime_id": runtime_id,
                    "binding_id": binding_id,
                    "trade_date": trade_date.isoformat(),
                    "route_owner": strict_owner.route_owner.value,
                    "broker_called": False,
                },
            )
        return strict_owner

    @staticmethod
    def _validate_identity(value: str, *, field_name: str) -> None:
        if type(value) is not str or not value or value != value.strip():
            raise TypeError(f"{field_name} must be a non-empty canonical strict string")


__all__ = ["KernelProductCutoverCoordinator", "KernelProductCutoverRepositoryV1"]
