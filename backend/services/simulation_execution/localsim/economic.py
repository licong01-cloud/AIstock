"""Single-writer LocalSIM economic transaction coordinator."""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, fields as dataclass_fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol

from backend.services.simulation_execution.localsim.models import (
    LocalSimEconomicReceiptV1,
    LocalSimExecutionStateV1,
    LocalSimProjectionOutboxV1,
)
from backend.services.simulation_data.daily_context import canonical_json_sha256
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


def canonical_local_sim_json_value(value: Any, *, path: str = "$") -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise RuntimeConfigInvalidError(
                "LocalSIM durable fact contains a non-finite Decimal",
                context={
                    "reason_code": "LOCALSIM_FACT_JSON_NUMBER_INVALID",
                    "stage": "LOCALSIM_FACT_JSON_NORMALIZE",
                    "path": path,
                    "value": str(value),
                },
            )
        return str(value)
    if isinstance(value, Enum):
        return canonical_local_sim_json_value(value.value, path=path)
    if isinstance(value, Mapping):
        invalid_keys = [key for key in value if not isinstance(key, str)]
        if invalid_keys:
            invalid_key = invalid_keys[0]
            raise RuntimeConfigInvalidError(
                "LocalSIM durable fact JSON object keys must be strings",
                context={
                    "reason_code": "LOCALSIM_FACT_JSON_KEY_INVALID",
                    "stage": "LOCALSIM_FACT_JSON_NORMALIZE",
                    "path": path,
                    "key_type": type(invalid_key).__name__,
                    "key_repr": repr(invalid_key),
                },
            )
        return {
            key: canonical_local_sim_json_value(item, path=f"{path}.{key}")
            for key, item in sorted(value.items())
        }
    if isinstance(value, (list, tuple)):
        return [canonical_local_sim_json_value(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RuntimeConfigInvalidError(
                "LocalSIM durable fact contains a non-finite float",
                context={
                    "reason_code": "LOCALSIM_FACT_JSON_NUMBER_INVALID",
                    "stage": "LOCALSIM_FACT_JSON_NORMALIZE",
                    "path": path,
                    "value": repr(value),
                },
            )
        return value
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise RuntimeConfigInvalidError(
        "LocalSIM durable fact contains a value that is not JSON serializable",
        context={
            "reason_code": "LOCALSIM_FACT_JSON_TYPE_INVALID",
            "stage": "LOCALSIM_FACT_JSON_NORMALIZE",
            "path": path,
            "value_type": type(value).__name__,
        },
    )


def local_sim_fact_payload(item: Any, *, fact_type: str) -> dict[str, Any]:
    dump = getattr(item, "model_dump", None)
    if callable(dump):
        raw = dump(mode="python", exclude={"created_at", "updated_at"})
    elif is_dataclass(item):
        raw = {
            field_info.name: getattr(item, field_info.name)
            for field_info in dataclass_fields(item)
            if field_info.name not in {"created_at", "updated_at"}
        }
    else:
        raise DataUnavailableError(
            "LocalSim economic fact cannot be serialized canonically",
            context={
                "reason_code": "LOCALSIM_ECONOMIC_FACT_SCHEMA_INVALID",
                "fact_type": fact_type,
                "python_type": type(item).__name__,
            },
        )
    payload = canonical_local_sim_json_value(raw)
    if not isinstance(payload, dict):
        raise DataUnavailableError(
            "LocalSim economic fact canonical payload must be an object",
            context={"reason_code": "LOCALSIM_ECONOMIC_FACT_SCHEMA_INVALID", "fact_type": fact_type},
        )
    return payload


def validate_local_sim_duplicate_account_truth(
    *,
    run_id: str,
    projected_positions: Mapping[str, Any],
    projected_cash: float,
    observed_positions: Mapping[str, Any],
    observed_account: Any,
) -> None:
    raw_observed_cash = getattr(observed_account, "cash", None)
    if isinstance(raw_observed_cash, bool) or not isinstance(raw_observed_cash, (int, float, Decimal)):
        raise DataUnavailableError(
            "LocalSim duplicate generation has no exact observed cash",
            context={
                "reason_code": "LOCALSIM_DUPLICATE_ECONOMIC_STATE_CONFLICT",
                "run_id": run_id,
                "observed_cash_type": type(raw_observed_cash).__name__,
            },
        )
    observed_cash = float(raw_observed_cash)
    projected_hashes = {
        symbol: canonical_json_sha256(local_sim_fact_payload(position, fact_type="position"))
        for symbol, position in sorted(projected_positions.items())
    }
    observed_hashes = {
        symbol: canonical_json_sha256(local_sim_fact_payload(position, fact_type="position"))
        for symbol, position in sorted(observed_positions.items())
    }
    if not math.isfinite(observed_cash) or observed_cash != projected_cash or observed_hashes != projected_hashes:
        raise DataUnavailableError(
            "LocalSim duplicate generation account truth changed without a new economic generation",
            context={
                "reason_code": "LOCALSIM_DUPLICATE_ECONOMIC_STATE_CONFLICT",
                "run_id": run_id,
                "projected_cash": projected_cash,
                "observed_cash": observed_cash,
                "projected_position_hashes": projected_hashes,
                "observed_position_hashes": observed_hashes,
            },
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
    "canonical_local_sim_json_value",
    "local_sim_fact_payload",
    "validate_local_sim_duplicate_account_truth",
    "LocalSimEconomicCommitRequest",
    "LocalSimEconomicCommitResult",
    "LocalSimEconomicCoordinator",
    "LocalSimPaperEconomicRepository",
    "LocalSimRuntimeEconomicRepository",
]
