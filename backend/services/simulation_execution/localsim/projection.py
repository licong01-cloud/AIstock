"""Outbox-only LocalSIM projection transaction owner."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import psycopg2

from backend.services.simulation_execution.localsim.models import LocalSimProjectionReceiptV1
from backend.services.trading_core.errors import SessionLockTimeoutError


LOCAL_SIM_PROJECTION_RETRYABLE_PG_CODES = frozenset({"40001", "40P01", "55P03"})


def local_sim_projection_error_is_retryable(exc: BaseException) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (SessionLockTimeoutError, psycopg2.OperationalError, psycopg2.InterfaceError)):
            return True
        if str(getattr(current, "pgcode", "") or "") in LOCAL_SIM_PROJECTION_RETRYABLE_PG_CODES:
            return True
        current = current.__cause__ or current.__context__
    return False


class LocalSimRuntimeProjectionRepository(Protocol):
    def local_sim_economic_transaction_scope(self) -> Any: ...

    def stage_local_sim_projection_commit(self, **kwargs: Any) -> LocalSimProjectionReceiptV1: ...


class LocalSimPaperProjectionRepository(Protocol):
    def local_sim_economic_transaction(self, run_id: str) -> Any: ...


@dataclass(frozen=True)
class LocalSimProjectionCommitRequest:
    run_id: str
    outbox_id: str
    generation: int
    final_status: Any
    projection_result: dict[str, Any]
    payload_patch: dict[str, Any]
    payload_unset: tuple[str, ...]
    apply_paper_projection: Callable[[], None]
    readback: Callable[[LocalSimProjectionReceiptV1], Any]
    on_staged: Callable[[], None] | None = None


@dataclass(frozen=True)
class LocalSimProjectionCommitResult:
    receipt: LocalSimProjectionReceiptV1
    projected: Any


class LocalSimProjector:
    """Consume one durable outbox generation without broker or signal access."""

    def __init__(
        self,
        *,
        runtime_repository: LocalSimRuntimeProjectionRepository,
        paper_repository: LocalSimPaperProjectionRepository,
    ) -> None:
        self._runtime_repository = runtime_repository
        self._paper_repository = paper_repository

    def commit(self, request: LocalSimProjectionCommitRequest) -> LocalSimProjectionCommitResult:
        with self._runtime_repository.local_sim_economic_transaction_scope():
            with self._paper_repository.local_sim_economic_transaction(request.run_id) as connection:
                request.apply_paper_projection()
                receipt = self._runtime_repository.stage_local_sim_projection_commit(
                    connection=connection,
                    run_id=request.run_id,
                    outbox_id=request.outbox_id,
                    generation=request.generation,
                    final_status=request.final_status,
                    projection_result=request.projection_result,
                    payload_patch=request.payload_patch,
                    payload_unset=request.payload_unset,
                )
        if request.on_staged is not None:
            request.on_staged()
        projected = request.readback(receipt)
        return LocalSimProjectionCommitResult(receipt=receipt, projected=projected)


__all__ = [
    "LOCAL_SIM_PROJECTION_RETRYABLE_PG_CODES",
    "LocalSimPaperProjectionRepository",
    "LocalSimProjectionCommitRequest",
    "LocalSimProjectionCommitResult",
    "LocalSimProjector",
    "LocalSimRuntimeProjectionRepository",
    "local_sim_projection_error_is_retryable",
]
