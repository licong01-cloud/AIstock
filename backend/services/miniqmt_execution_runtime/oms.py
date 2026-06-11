"""Durable OMS projection helpers for MiniQMTExecutionRuntime."""

from __future__ import annotations

from dataclasses import dataclass

from .models import MiniQMTChildOrder, MiniQMTExecutionAlgoInstance
from .repository import MiniQMTExecutionRuntimeRepository


@dataclass(frozen=True)
class MiniQMTOmsProjection:
    runtime_id: str
    active_algo_instances: tuple[MiniQMTExecutionAlgoInstance, ...]
    active_child_orders: tuple[MiniQMTChildOrder, ...]


class MiniQMTOmsLedger:
    """Read/write facade over durable algo and child-order projections."""

    def __init__(self, repository: MiniQMTExecutionRuntimeRepository) -> None:
        self._repository = repository

    def record_algo_instance(self, instance: MiniQMTExecutionAlgoInstance) -> MiniQMTExecutionAlgoInstance:
        return self._repository.upsert_algo_instance(instance)

    def record_child_order(self, order: MiniQMTChildOrder) -> MiniQMTChildOrder:
        return self._repository.upsert_child_order(order)

    def active_projection(self, runtime_id: str) -> MiniQMTOmsProjection:
        return MiniQMTOmsProjection(
            runtime_id=runtime_id,
            active_algo_instances=tuple(self._repository.list_algo_instances(runtime_id, active_only=True)),
            active_child_orders=tuple(self._repository.list_child_orders(runtime_id, active_only=True)),
        )
