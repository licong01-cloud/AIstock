"""Single-writer LocalSIM economic transaction coordinator."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from backend.services.simulation_execution.localsim.models import (
    LocalSimEconomicReceiptV1,
    LocalSimExecutionStateV1,
    LocalSimProjectionOutboxV1,
)


class LocalSimRuntimeEconomicRepository(Protocol):
    def local_sim_economic_transaction_scope(self) -> Any: ...

    def stage_local_sim_economic_commit(self, **kwargs: Any) -> tuple[
        LocalSimEconomicReceiptV1,
        LocalSimProjectionOutboxV1,
        bool,
    ]: ...

    def readback_local_sim_economic_commit(
        self,
        *,
        run_id: str,
        receipt: LocalSimEconomicReceiptV1,
        outbox: LocalSimProjectionOutboxV1,
    ) -> Any: ...


class LocalSimPaperEconomicRepository(Protocol):
    def local_sim_economic_transaction(self, run_id: str) -> Any: ...

    def save_order(self, run_id: str, order: Any) -> None: ...

    def save_fill(self, run_id: str, fill: Any) -> None: ...

    def save_order_event(self, run_id: str, event: Any) -> None: ...

    def save_cash_entry(self, run_id: str, entry: Any) -> None: ...

    def save_run_event(
        self,
        *,
        run_id: str,
        event_type: str,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> None: ...

    def readback_local_sim_economic_facts(self, **kwargs: Any) -> dict[str, int]: ...


@dataclass(frozen=True)
class LocalSimEconomicCommitRequest:
    run_id: str
    binding_id: str
    trade_date: date
    plan_id: str
    states: tuple[LocalSimExecutionStateV1, ...]
    expected_versions: dict[str, tuple[int, str] | None]
    economic_facts: dict[str, Any]
    projection_payload: dict[str, Any]
    status: Any
    payload_patch: dict[str, Any]
    payload_unset: tuple[str, ...]
    orders: tuple[Any, ...] = ()
    fills: tuple[Any, ...] = ()
    events: tuple[Any, ...] = ()
    cash_entries: tuple[Any, ...] = ()
    event_type: str = "RUN_ECONOMIC_COMMITTED"
    event_message: str = "LocalSIM economic facts committed; projection outbox pending"
    event_context: dict[str, Any] | None = None
    on_created: Callable[[LocalSimEconomicReceiptV1, LocalSimProjectionOutboxV1], None] | None = None


@dataclass(frozen=True)
class LocalSimEconomicCommitResult:
    receipt: LocalSimEconomicReceiptV1
    outbox: LocalSimProjectionOutboxV1
    created: bool


class LocalSimEconomicCoordinator:
    """Own exactly one atomic write path for a LocalSIM economic generation."""

    def __init__(
        self,
        *,
        runtime_repository: LocalSimRuntimeEconomicRepository,
        paper_repository: LocalSimPaperEconomicRepository,
        ensure_paper_run: Callable[[], None],
    ) -> None:
        self._runtime_repository = runtime_repository
        self._paper_repository = paper_repository
        self._ensure_paper_run = ensure_paper_run

    def commit(self, request: LocalSimEconomicCommitRequest) -> LocalSimEconomicCommitResult:
        with self._runtime_repository.local_sim_economic_transaction_scope():
            with self._paper_repository.local_sim_economic_transaction(request.run_id) as connection:
                self._ensure_paper_run()
                self._write_economic_facts(request)
                receipt, outbox, created = self._runtime_repository.stage_local_sim_economic_commit(
                    connection=connection,
                    run_id=request.run_id,
                    binding_id=request.binding_id,
                    trade_date=request.trade_date,
                    plan_id=request.plan_id,
                    states=request.states,
                    expected_versions=request.expected_versions,
                    economic_facts=request.economic_facts,
                    projection_payload=request.projection_payload,
                    status=request.status,
                    payload_patch=request.payload_patch,
                    payload_unset=request.payload_unset,
                )
                if created:
                    event_context = {
                        **dict(request.event_context or {}),
                        "receipt_id": receipt.receipt_id,
                        "outbox_id": outbox.outbox_id,
                        "generation": receipt.generation,
                        "economic_hash": receipt.economic_hash,
                    }
                    self._paper_repository.save_run_event(
                        run_id=request.run_id,
                        event_type=request.event_type,
                        message=request.event_message,
                        context=event_context,
                    )
                    if request.on_created is not None:
                        request.on_created(receipt, outbox)

        self._runtime_repository.readback_local_sim_economic_commit(
            run_id=request.run_id,
            receipt=receipt,
            outbox=outbox,
        )
        self._paper_repository.readback_local_sim_economic_facts(
            run_id=request.run_id,
            order_ids=self._identity_set(request.orders, "order_id"),
            fill_ids=self._identity_set(request.fills, "fill_id"),
            order_event_ids=self._identity_set(request.events, "event_id"),
            cash_fill_ids=self._identity_set(request.cash_entries, "fill_id"),
        )
        return LocalSimEconomicCommitResult(receipt=receipt, outbox=outbox, created=created)

    def _write_economic_facts(self, request: LocalSimEconomicCommitRequest) -> None:
        writers: tuple[tuple[Iterable[Any], Callable[[str, Any], None]], ...] = (
            (request.orders, self._paper_repository.save_order),
            (request.fills, self._paper_repository.save_fill),
            (request.events, self._paper_repository.save_order_event),
            (request.cash_entries, self._paper_repository.save_cash_entry),
        )
        for facts, writer in writers:
            for fact in facts:
                writer(request.run_id, fact)

    @staticmethod
    def _identity_set(values: Iterable[Any], field: str) -> set[str]:
        return {str(getattr(item, field)) for item in values}


__all__ = [
    "LocalSimEconomicCommitRequest",
    "LocalSimEconomicCommitResult",
    "LocalSimEconomicCoordinator",
    "LocalSimPaperEconomicRepository",
    "LocalSimRuntimeEconomicRepository",
]
