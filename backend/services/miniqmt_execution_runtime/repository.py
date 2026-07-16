"""Repository interfaces for the durable MiniQMT execution runtime."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import threading
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Protocol

import psycopg2
import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.execution_algos.adaptive_is.contracts import MarketDataEvidenceV1, canonical_sha256
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.models import OrderSide

from .models import (
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrder,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionAlgoInstance,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeMode,
    MiniQMTExecutionRuntimeRecord,
    MiniQMTExecutionRuntimeState,
    MiniQMTGatewayState,
    MiniQMTOmsState,
)


_PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MINIQMT_EXECUTION_RUNTIME_STORE_PATH = (
    _PROJECT_ROOT / "tmp" / "miniqmt_execution_runtime" / "runtime-state.json"
)
MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV = "MINIQMT_EXECUTION_RUNTIME_STORE_PATH"
MINIQMT_EXECUTION_RUNTIME_REPOSITORY_ENV = "MINIQMT_EXECUTION_RUNTIME_REPOSITORY"
MINIQMT_EXECUTION_RUNTIME_JSONFILE_TEST_ONLY_ENV = "AISTOCK_MINIQMT_RUNTIME_JSONFILE_TEST_ONLY"
MINIQMT_EXECUTION_RUNTIME_STORE_MAX_BYTES_ENV = "MINIQMT_EXECUTION_RUNTIME_STORE_MAX_BYTES"
MINIQMT_EXECUTION_RUNTIME_COMPACT_EVERY_WRITES_ENV = "MINIQMT_EXECUTION_RUNTIME_COMPACT_EVERY_WRITES"
MINIQMT_EXECUTION_RUNTIME_MAX_EVENTS_PER_RUNTIME_ENV = "MINIQMT_EXECUTION_RUNTIME_MAX_EVENTS_PER_RUNTIME"
MINIQMT_EXECUTION_RUNTIME_RETAIN_EVENTS_PER_RUNTIME_ENV = "MINIQMT_EXECUTION_RUNTIME_RETAIN_EVENTS_PER_RUNTIME"
MINIQMT_EXECUTION_RUNTIME_MAX_CHILD_ORDERS_PER_RUNTIME_ENV = "MINIQMT_EXECUTION_RUNTIME_MAX_CHILD_ORDERS_PER_RUNTIME"
MINIQMT_EXECUTION_RUNTIME_RETAIN_CHILD_ORDERS_PER_RUNTIME_ENV = (
    "MINIQMT_EXECUTION_RUNTIME_RETAIN_CHILD_ORDERS_PER_RUNTIME"
)
MINIQMT_EXECUTION_RUNTIME_MAX_ALGO_INSTANCES_PER_RUNTIME_ENV = (
    "MINIQMT_EXECUTION_RUNTIME_MAX_ALGO_INSTANCES_PER_RUNTIME"
)
MINIQMT_EXECUTION_RUNTIME_RETAIN_ALGO_INSTANCES_PER_RUNTIME_ENV = (
    "MINIQMT_EXECUTION_RUNTIME_RETAIN_ALGO_INSTANCES_PER_RUNTIME"
)
DEFAULT_MINIQMT_EXECUTION_RUNTIME_STORE_MAX_BYTES = 32 * 1024 * 1024
DEFAULT_MINIQMT_EXECUTION_RUNTIME_COMPACT_EVERY_WRITES = 1000
DEFAULT_MINIQMT_EXECUTION_RUNTIME_MAX_EVENTS_PER_RUNTIME = 3000
DEFAULT_MINIQMT_EXECUTION_RUNTIME_RETAIN_EVENTS_PER_RUNTIME = 2000
DEFAULT_MINIQMT_EXECUTION_RUNTIME_MAX_CHILD_ORDERS_PER_RUNTIME = 3000
DEFAULT_MINIQMT_EXECUTION_RUNTIME_RETAIN_CHILD_ORDERS_PER_RUNTIME = 2000
DEFAULT_MINIQMT_EXECUTION_RUNTIME_MAX_ALGO_INSTANCES_PER_RUNTIME = 2000
DEFAULT_MINIQMT_EXECUTION_RUNTIME_RETAIN_ALGO_INSTANCES_PER_RUNTIME = 1200
DEFAULT_MINIQMT_EXECUTION_RUNTIME_REPOSITORY = "postgres"
MINIQMT_EXECUTION_RUNTIME_PRUNE_EVERY_WRITES_ENV = "MINIQMT_EXECUTION_RUNTIME_PRUNE_EVERY_WRITES"
DEFAULT_MINIQMT_EXECUTION_RUNTIME_PRUNE_EVERY_WRITES = 100
_TERMINAL_CHILD_ORDER_STATUSES = frozenset(
    {
        MiniQMTChildOrderStatus.FILLED.value,
        MiniQMTChildOrderStatus.CANCELLED.value,
        MiniQMTChildOrderStatus.REJECTED.value,
    }
)
_QUOTE_EVIDENCE_EVENT_TYPES = frozenset(
    {
        MiniQMTExecutionEventType.QUOTE_OBSERVED,
        MiniQMTExecutionEventType.QUOTE_REJECTED,
        MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED,
        MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED,
        MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH,
    }
)


class QuoteEvidenceIdempotencyConflict(ValueError):
    """A deterministic evidence event id was reused with different contents."""


@dataclass(frozen=True)
class QuoteEvidenceEventCandidate:
    """Sequence-free evidence append request; the repository owns ordering."""

    event_id: str
    runtime_id: str
    event_type: MiniQMTExecutionEventType
    event_time: datetime
    payload: dict[str, Any]
    evidence_sha256: str
    evidence_contract: MarketDataEvidenceV1 | None = None

    def __post_init__(self) -> None:
        if not self.event_id or not self.runtime_id or not self.evidence_sha256:
            raise ValueError("quote evidence candidate requires event_id, runtime_id, and evidence_sha256")
        if self.event_type not in _QUOTE_EVIDENCE_EVENT_TYPES:
            raise ValueError(f"event type is not a quote-evidence carrier: {self.event_type}")
        if self.event_time.tzinfo is None or self.event_time.utcoffset() is None:
            raise ValueError("quote evidence candidate event_time must be timezone-aware")
        if not isinstance(self.payload, dict):
            raise ValueError("quote evidence candidate payload must be a dict")
        if self.event_type == MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH:
            health = self.payload.get("health_or_aggregate")
            if (
                self.payload.get("schema_version") != "miniqmt_quote_ingress_health_payload_v1"
                or not isinstance(health, dict)
                or str(health.get("health_sha256") or "") != self.evidence_sha256
            ):
                raise ValueError("quote ingress health candidate payload is invalid")
            return
        if not isinstance(self.evidence_contract, MarketDataEvidenceV1):
            raise ValueError("quote evidence candidate requires its validated MarketDataEvidenceV1 contract")
        if self.payload.get("schema_version") != "miniqmt_quote_runtime_event_payload_v1":
            raise ValueError("quote evidence candidate payload schema is invalid")
        evidence = self.payload.get("evidence")
        if not isinstance(evidence, dict):
            raise ValueError("quote evidence candidate requires a typed evidence payload")
        if str(evidence.get("evidence_sha256") or "") != self.evidence_sha256:
            raise ValueError("quote evidence candidate evidence hash does not match payload")
        capture_type = str(evidence.get("capture_type") or "")
        expected_event_type = {
            "ACTION_INPUT": MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED,
            "ACTION_REJECT": MiniQMTExecutionEventType.QUOTE_REJECTED,
            "CHILD_RECEIPT": MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED,
            "PROTECTION_BAND_TRIGGER": MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED,
            "MARKOUT_60S": MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED,
            "MARKOUT_300S": MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED,
            "MARKOUT_900S": MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED,
            "CADENCE_AGGREGATE": MiniQMTExecutionEventType.QUOTE_OBSERVED,
        }.get(capture_type)
        if expected_event_type is None or self.event_type != expected_event_type:
            raise ValueError("quote evidence capture type and runtime event type do not match the registered mapping")
        expected_event_id = "mqrtevt_" + canonical_sha256(
            {"schema": "miniqmt_quote_event_v1", "evidence_id": self.evidence_contract.evidence_id}
        )
        if (
            self.event_id != expected_event_id
            or self.runtime_id != self.evidence_contract.runtime_id
            or self.event_time != self.evidence_contract.event_time_utc
            or self.evidence_sha256 != self.evidence_contract.evidence_sha256
            or self.payload != self.evidence_contract.runtime_payload()
        ):
            raise ValueError("quote evidence candidate does not exactly match its validated evidence contract")


@dataclass(frozen=True)
class DurableEvidenceReceipt:
    event: MiniQMTExecutionEvent
    persisted_at_utc: datetime
    durable_ack: bool
    readback_verified: bool


class MiniQMTExecutionRuntimeRepository(Protocol):
    def upsert_runtime(self, runtime: MiniQMTExecutionRuntimeRecord) -> MiniQMTExecutionRuntimeRecord: ...

    def get_runtime(self, runtime_id: str) -> MiniQMTExecutionRuntimeRecord | None: ...

    def list_runtimes(self) -> list[MiniQMTExecutionRuntimeRecord]: ...

    def list_runtimes_for_account(
        self, *, account_group_id: str, limit: int
    ) -> list[MiniQMTExecutionRuntimeRecord]: ...

    def append_event(self, event: MiniQMTExecutionEvent) -> MiniQMTExecutionEvent: ...

    def append_evidence_event_idempotent(self, candidate: QuoteEvidenceEventCandidate) -> DurableEvidenceReceipt: ...

    def list_events(self, runtime_id: str, *, include_archived: bool = False) -> list[MiniQMTExecutionEvent]: ...

    def list_evidence_receipts(
        self,
        runtime_id: str,
        *,
        market_data_id: str | None = None,
        evidence_id: str | None = None,
        include_archived: bool = False,
        after_sequence: int = 0,
        after_event_id: str = "",
        limit: int = 501,
    ) -> list[DurableEvidenceReceipt]: ...

    def list_quote_events_page(
        self,
        runtime_id: str,
        *,
        symbol: str | None,
        after_sequence: int,
        after_event_id: str,
        limit: int,
    ) -> list[MiniQMTExecutionEvent]: ...

    def list_events_by_ids(
        self,
        runtime_id: str,
        *,
        event_ids: tuple[str, ...],
        include_archived: bool = False,
    ) -> list[MiniQMTExecutionEvent]: ...

    def quote_diagnostics_summary(self, runtime_id: str, *, symbol: str | None) -> dict[str, Any]: ...

    def quote_event_schema_gate(self) -> str: ...

    def existing_evidence_ids(
        self,
        runtime_id: str,
        *,
        evidence_ids: tuple[str, ...],
        include_archived: bool = False,
    ) -> set[str]: ...

    def next_event_sequence(self, runtime_id: str) -> int: ...

    def read_quote_control_snapshot(
        self,
        *,
        cursor: Any,
        runtime_ids: tuple[str, ...],
        include_archived: bool = True,
    ) -> tuple[list[MiniQMTExecutionRuntimeRecord], list[MiniQMTExecutionEvent]]: ...

    def upsert_algo_instance(self, instance: MiniQMTExecutionAlgoInstance) -> MiniQMTExecutionAlgoInstance: ...

    def list_algo_instances(
        self,
        runtime_id: str,
        *,
        active_only: bool = False,
    ) -> list[MiniQMTExecutionAlgoInstance]: ...

    def upsert_child_order(self, order: MiniQMTChildOrder) -> MiniQMTChildOrder: ...

    def list_child_orders(
        self,
        runtime_id: str,
        *,
        active_only: bool = False,
    ) -> list[MiniQMTChildOrder]: ...


class InMemoryMiniQMTExecutionRuntimeRepository:
    """Deterministic in-memory repository for unit tests."""

    def __init__(self) -> None:
        self._runtimes: dict[str, MiniQMTExecutionRuntimeRecord] = {}
        self._events: dict[str, list[MiniQMTExecutionEvent]] = {}
        self._evidence_receipts: dict[str, DurableEvidenceReceipt] = {}
        self._algo_instances: dict[str, MiniQMTExecutionAlgoInstance] = {}
        self._child_orders: dict[str, MiniQMTChildOrder] = {}

    def upsert_runtime(self, runtime: MiniQMTExecutionRuntimeRecord) -> MiniQMTExecutionRuntimeRecord:
        existing = self._runtimes.get(runtime.runtime_id)
        events = self._events.get(runtime.runtime_id) or []
        last_event_sequence = max(
            int(runtime.last_event_sequence or 0),
            int(existing.last_event_sequence or 0) if existing is not None else 0,
            int(events[-1].sequence) if events else 0,
        )
        stored = runtime.model_copy(
            update={
                "last_event_sequence": last_event_sequence,
                "updated_at": datetime.now(UTC),
            }
        )
        self._runtimes[stored.runtime_id] = stored
        return stored

    def get_runtime(self, runtime_id: str) -> MiniQMTExecutionRuntimeRecord | None:
        return self._runtimes.get(runtime_id)

    def list_runtimes(self) -> list[MiniQMTExecutionRuntimeRecord]:
        return sorted(self._runtimes.values(), key=lambda item: item.updated_at, reverse=True)

    def list_runtimes_for_account(
        self, *, account_group_id: str, limit: int
    ) -> list[MiniQMTExecutionRuntimeRecord]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        exact_account_group_id = str(account_group_id or "").strip()
        if not exact_account_group_id:
            raise ValueError("account_group_id is required")
        rows = [
            runtime
            for runtime in self._runtimes.values()
            if runtime.account_group_id == exact_account_group_id
            and runtime.event_loop_state
            not in {
                MiniQMTExecutionRuntimeState.STOPPED,
                MiniQMTExecutionRuntimeState.FAILED,
            }
        ]
        rows.sort(key=lambda item: (item.updated_at, item.runtime_id), reverse=True)
        return rows[:limit]

    def append_event(self, event: MiniQMTExecutionEvent) -> MiniQMTExecutionEvent:
        existing = self._events.setdefault(event.runtime_id, [])
        runtime = self._runtimes.get(event.runtime_id)
        last_sequence = max(
            int(runtime.last_event_sequence or 0) if runtime is not None else 0,
            int(existing[-1].sequence) if existing else 0,
        )
        expected_sequence = int(last_sequence or 0) + 1
        if event.sequence != expected_sequence:
            raise ValueError(
                f"event sequence must be monotonic for runtime {event.runtime_id}: "
                f"expected {expected_sequence}, got {event.sequence}"
            )
        existing.append(event)
        if runtime is not None:
            self._runtimes[event.runtime_id] = runtime.model_copy(
                update={"last_event_sequence": event.sequence, "updated_at": datetime.now(UTC)}
            )
        return event

    def append_evidence_event_idempotent(self, candidate: QuoteEvidenceEventCandidate) -> DurableEvidenceReceipt:
        existing = self._evidence_receipts.get(candidate.event_id)
        if existing is not None:
            event = existing.event
            if not _evidence_event_matches_candidate(event, candidate):
                raise QuoteEvidenceIdempotencyConflict(f"quote evidence event id conflicts: {candidate.event_id}")
            return existing
        runtime = self._runtimes.get(candidate.runtime_id)
        if runtime is None:
            raise ValueError(f"quote evidence runtime does not exist: {candidate.runtime_id}")
        sequence = int(runtime.last_event_sequence or 0) + 1
        event = MiniQMTExecutionEvent(
            event_id=candidate.event_id,
            runtime_id=candidate.runtime_id,
            sequence=sequence,
            event_type=candidate.event_type,
            event_time=candidate.event_time,
            source="quote_ingress",
            payload=dict(candidate.payload),
        )
        self._events.setdefault(event.runtime_id, []).append(event)
        self._runtimes[event.runtime_id] = runtime.model_copy(
            update={"last_event_sequence": event.sequence, "updated_at": event.event_time}
        )
        receipt = DurableEvidenceReceipt(
            event=event,
            persisted_at_utc=datetime.now(UTC),
            durable_ack=True,
            readback_verified=True,
        )
        self._evidence_receipts[candidate.event_id] = receipt
        return receipt

    def list_events(self, runtime_id: str, *, include_archived: bool = False) -> list[MiniQMTExecutionEvent]:
        return list(self._events.get(runtime_id, ()))

    def list_evidence_receipts(
        self,
        runtime_id: str,
        *,
        market_data_id: str | None = None,
        evidence_id: str | None = None,
        include_archived: bool = False,
        after_sequence: int = 0,
        after_event_id: str = "",
        limit: int = 501,
    ) -> list[DurableEvidenceReceipt]:
        if (market_data_id is None) == (evidence_id is None):
            raise ValueError("exactly one of market_data_id or evidence_id is required")
        if limit < 1 or limit > 501:
            raise ValueError("evidence receipt limit must be between 1 and 501")
        receipts = [
            receipt
            for receipt in self._evidence_receipts.values()
            if receipt.event.runtime_id == runtime_id and isinstance(receipt.event.payload.get("evidence"), dict)
        ]
        connected: set[str] = {market_data_id or evidence_id or ""}
        selected: dict[str, DurableEvidenceReceipt] = {}
        changed = True
        while changed:
            changed = False
            for receipt in receipts:
                evidence = receipt.event.payload["evidence"]
                tokens = _evidence_link_tokens(evidence, event_id=receipt.event.event_id)
                if connected.intersection(tokens) and receipt.event.event_id not in selected:
                    selected[receipt.event.event_id] = receipt
                    connected.update(tokens)
                    changed = True
        return [
            receipt
            for receipt in sorted(selected.values(), key=lambda item: (item.event.sequence, item.event.event_id))
            if (receipt.event.sequence, receipt.event.event_id) > (after_sequence, after_event_id)
        ][:limit]

    def list_quote_events_page(
        self,
        runtime_id: str,
        *,
        symbol: str | None,
        after_sequence: int,
        after_event_id: str,
        limit: int,
    ) -> list[MiniQMTExecutionEvent]:
        quote_events = [
            event
            for event in self._events.get(runtime_id, ())
            if event.event_type.value.startswith("QUOTE_")
            and (symbol is None or (event.payload.get("evidence") or {}).get("symbol") == symbol)
            and (event.sequence, event.event_id) > (after_sequence, after_event_id)
        ]
        return sorted(quote_events, key=lambda item: (item.sequence, item.event_id))[:limit]

    def list_events_by_ids(
        self,
        runtime_id: str,
        *,
        event_ids: tuple[str, ...],
        include_archived: bool = False,
    ) -> list[MiniQMTExecutionEvent]:
        wanted = set(event_ids)
        return [event for event in self._events.get(runtime_id, ()) if event.event_id in wanted]

    def quote_diagnostics_summary(self, runtime_id: str, *, symbol: str | None) -> dict[str, Any]:
        quote_events = [
            event
            for event in self._events.get(runtime_id, ())
            if event.event_type.value.startswith("QUOTE_")
            and (
                symbol is None
                or event.event_type == MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH
                or (event.payload.get("evidence") or {}).get("symbol") == symbol
            )
        ]
        return _quote_diagnostics_summary_from_events(quote_events)

    def quote_event_schema_gate(self) -> str:
        return "test_only_unverified"

    def existing_evidence_ids(
        self,
        runtime_id: str,
        *,
        evidence_ids: tuple[str, ...],
        include_archived: bool = False,
    ) -> set[str]:
        wanted = set(evidence_ids)
        return {
            str(evidence["evidence_id"])
            for event in self._events.get(runtime_id, ())
            if isinstance((evidence := event.payload.get("evidence")), dict) and evidence.get("evidence_id") in wanted
        }

    def next_event_sequence(self, runtime_id: str) -> int:
        runtime = self._runtimes.get(runtime_id)
        if runtime is not None and runtime.last_event_sequence:
            return int(runtime.last_event_sequence) + 1
        events = self._events.get(runtime_id, ())
        return (int(events[-1].sequence) if events else 0) + 1

    def read_quote_control_snapshot(
        self,
        *,
        cursor: Any,  # noqa: ARG002 - in-memory repository has no DB cursor
        runtime_ids: tuple[str, ...],
        include_archived: bool = True,  # noqa: ARG002 - in-memory rows are not archived
    ) -> tuple[list[MiniQMTExecutionRuntimeRecord], list[MiniQMTExecutionEvent]]:
        requested = set(runtime_ids)
        runtimes = [runtime for runtime_id, runtime in self._runtimes.items() if runtime_id in requested]
        events = [event for runtime_id in sorted(requested) for event in self._events.get(runtime_id, ())]
        return (
            sorted(runtimes, key=lambda item: item.runtime_id),
            sorted(events, key=lambda item: (item.runtime_id, item.sequence, item.event_id)),
        )

    def upsert_algo_instance(self, instance: MiniQMTExecutionAlgoInstance) -> MiniQMTExecutionAlgoInstance:
        stored = instance.model_copy(update={"updated_at": datetime.now(UTC)})
        self._algo_instances[stored.algo_instance_id] = stored
        return stored

    def list_algo_instances(
        self,
        runtime_id: str,
        *,
        active_only: bool = False,
    ) -> list[MiniQMTExecutionAlgoInstance]:
        items = [item for item in self._algo_instances.values() if item.runtime_id == runtime_id]
        if active_only:
            items = [item for item in items if item.status == MiniQMTAlgoInstanceStatus.ACTIVE]
        return sorted(items, key=lambda item: item.created_at)

    def upsert_child_order(self, order: MiniQMTChildOrder) -> MiniQMTChildOrder:
        stored = order.model_copy(update={"updated_at": datetime.now(UTC)})
        self._child_orders[stored.child_order_id] = stored
        return stored

    def list_child_orders(
        self,
        runtime_id: str,
        *,
        active_only: bool = False,
    ) -> list[MiniQMTChildOrder]:
        items = [item for item in self._child_orders.values() if item.runtime_id == runtime_id]
        if active_only:
            terminal = {
                MiniQMTChildOrderStatus.FILLED,
                MiniQMTChildOrderStatus.CANCELLED,
                MiniQMTChildOrderStatus.REJECTED,
            }
            items = [item for item in items if item.status not in terminal]
        return sorted(items, key=lambda item: item.updated_at)

    def mark_runtime_state(
        self,
        runtime_id: str,
        state: MiniQMTExecutionRuntimeState,
    ) -> MiniQMTExecutionRuntimeRecord:
        runtime = self._runtimes[runtime_id]
        return self.upsert_runtime(runtime.model_copy(update={"event_loop_state": state}))


class PostgresMiniQMTExecutionRuntimeRepository:
    """Production MiniQMT runtime store with incremental per-row DB writes."""

    def __init__(self, conn_factory: Any = get_conn) -> None:
        self._conn_factory = conn_factory
        self._conn_factory_accepts_autocommit = _supports_conn_factory_kw(conn_factory, "autocommit")
        self._conn_factory_accepts_manage_transaction = _supports_conn_factory_kw(conn_factory, "manage_transaction")
        self._prune_write_count_by_runtime: dict[str, int] = {}
        self._prune_lock = threading.Lock()

    def upsert_runtime(self, runtime: MiniQMTExecutionRuntimeRecord) -> MiniQMTExecutionRuntimeRecord:
        stored = runtime.model_copy(update={"updated_at": datetime.now(UTC)})
        self._with_runtime_db_error(
            "upsert_runtime",
            "MINIQMT_RUNTIME_DB_UPSERT_RUNTIME_FAILED",
            {"runtime_id": stored.runtime_id, "trade_date": stored.trade_date.isoformat()},
            lambda: self._upsert_runtime_row(stored),
        )
        return stored

    def get_runtime(self, runtime_id: str) -> MiniQMTExecutionRuntimeRecord | None:
        return self._with_runtime_db_error(
            "get_runtime",
            "MINIQMT_RUNTIME_DB_GET_RUNTIME_FAILED",
            {"runtime_id": runtime_id},
            lambda: self._get_runtime_row(runtime_id),
        )

    def list_runtimes(self) -> list[MiniQMTExecutionRuntimeRecord]:
        return self._with_runtime_db_error(
            "list_runtimes",
            "MINIQMT_RUNTIME_DB_LIST_RUNTIMES_FAILED",
            {},
            self._list_runtime_rows,
        )

    def list_runtimes_for_account(
        self, *, account_group_id: str, limit: int
    ) -> list[MiniQMTExecutionRuntimeRecord]:
        exact_account_group_id = str(account_group_id or "").strip()
        if not exact_account_group_id:
            raise ValueError("account_group_id is required")
        if limit <= 0:
            raise ValueError("limit must be positive")
        return self._with_runtime_db_error(
            "list_runtimes_for_account",
            "MINIQMT_RUNTIME_DB_LIST_ACCOUNT_RUNTIMES_FAILED",
            {"account_group_id": exact_account_group_id, "limit": limit},
            lambda: self._list_runtime_rows_for_account(
                account_group_id=exact_account_group_id,
                limit=limit,
            ),
        )

    def append_event(self, event: MiniQMTExecutionEvent) -> MiniQMTExecutionEvent:
        existing = self.get_runtime(event.runtime_id)
        expected_sequence = int(existing.last_event_sequence or 0) + 1 if existing is not None else event.sequence
        if event.sequence != expected_sequence:
            raise ValueError(
                f"event sequence must be monotonic for runtime {event.runtime_id}: "
                f"expected {expected_sequence}, got {event.sequence}"
            )
        self._with_runtime_db_error(
            "append_event",
            "MINIQMT_RUNTIME_DB_APPEND_EVENT_FAILED",
            {"runtime_id": event.runtime_id, "event_id": event.event_id, "sequence": event.sequence},
            lambda: self._append_event_row(event),
        )
        self._prune_runtime_if_due(runtime_id=event.runtime_id, reason="append_event")
        return event

    def append_evidence_event_idempotent(self, candidate: QuoteEvidenceEventCandidate) -> DurableEvidenceReceipt:
        receipt = self._with_runtime_db_error(
            "append_evidence_event",
            "ADAPTIVE_IS_MARKET_DATA_EVIDENCE_PERSIST_FAILED",
            {
                "runtime_id": candidate.runtime_id,
                "event_id": candidate.event_id,
                "evidence_sha256": candidate.evidence_sha256,
            },
            lambda: self._append_evidence_event_row(candidate),
        )
        if not receipt.durable_ack or not receipt.readback_verified:
            raise RuntimeConfigInvalidError(
                "quote evidence persistence did not produce a durable verified receipt",
                context={
                    "reason_code": "ADAPTIVE_IS_MARKET_DATA_EVIDENCE_PERSIST_FAILED",
                    "stage": "PERSIST",
                    "runtime_id": candidate.runtime_id,
                    "event_id": candidate.event_id,
                    "durable_ack": receipt.durable_ack,
                    "readback_verified": receipt.readback_verified,
                },
            )
        self._prune_runtime_if_due(runtime_id=candidate.runtime_id, reason="append_quote_evidence")
        return receipt

    def list_events(self, runtime_id: str, *, include_archived: bool = False) -> list[MiniQMTExecutionEvent]:
        return self._with_runtime_db_error(
            "list_events",
            "MINIQMT_RUNTIME_DB_LIST_EVENTS_FAILED",
            {"runtime_id": runtime_id, "include_archived": include_archived},
            lambda: self._list_event_rows(runtime_id, include_archived=include_archived),
        )

    def list_evidence_receipts(
        self,
        runtime_id: str,
        *,
        market_data_id: str | None = None,
        evidence_id: str | None = None,
        include_archived: bool = False,
        after_sequence: int = 0,
        after_event_id: str = "",
        limit: int = 501,
    ) -> list[DurableEvidenceReceipt]:
        if (market_data_id is None) == (evidence_id is None):
            raise ValueError("exactly one of market_data_id or evidence_id is required")
        return self._with_runtime_db_error(
            "list_evidence_receipts",
            "MINIQMT_RUNTIME_DB_LIST_EVIDENCE_RECEIPTS_FAILED",
            {
                "runtime_id": runtime_id,
                "market_data_id": market_data_id,
                "evidence_id": evidence_id,
                "include_archived": include_archived,
                "after_sequence": after_sequence,
                "after_event_id": after_event_id,
                "limit": limit,
            },
            lambda: self._list_evidence_receipt_rows(
                runtime_id,
                market_data_id=market_data_id,
                evidence_id=evidence_id,
                include_archived=include_archived,
                after_sequence=after_sequence,
                after_event_id=after_event_id,
                limit=limit,
            ),
        )

    def list_quote_events_page(
        self,
        runtime_id: str,
        *,
        symbol: str | None,
        after_sequence: int,
        after_event_id: str,
        limit: int,
    ) -> list[MiniQMTExecutionEvent]:
        return self._with_runtime_db_error(
            "list_quote_events_page",
            "MINIQMT_RUNTIME_DB_LIST_QUOTE_EVENTS_FAILED",
            {"runtime_id": runtime_id, "symbol": symbol, "limit": limit},
            lambda: self._list_quote_event_rows(
                runtime_id,
                symbol=symbol,
                after_sequence=after_sequence,
                after_event_id=after_event_id,
                limit=limit,
            ),
        )

    def list_events_by_ids(
        self,
        runtime_id: str,
        *,
        event_ids: tuple[str, ...],
        include_archived: bool = False,
    ) -> list[MiniQMTExecutionEvent]:
        if not event_ids:
            return []
        return self._with_runtime_db_error(
            "list_events_by_ids",
            "MINIQMT_RUNTIME_DB_LIST_LINKED_EVENTS_FAILED",
            {"runtime_id": runtime_id, "event_count": len(event_ids), "include_archived": include_archived},
            lambda: self._list_event_rows_by_ids(runtime_id, event_ids=event_ids, include_archived=include_archived),
        )

    def quote_diagnostics_summary(self, runtime_id: str, *, symbol: str | None) -> dict[str, Any]:
        return self._with_runtime_db_error(
            "quote_diagnostics_summary",
            "MINIQMT_RUNTIME_DB_QUOTE_DIAGNOSTICS_SUMMARY_FAILED",
            {"runtime_id": runtime_id, "symbol": symbol},
            lambda: self._quote_diagnostics_summary_rows(runtime_id, symbol=symbol),
        )

    def quote_event_schema_gate(self) -> str:
        from .quote_event_schema import read_quote_event_schema

        return self._with_runtime_db_error(
            "quote_event_schema_gate",
            "MINIQMT_RUNTIME_DB_QUOTE_EVENT_SCHEMA_READBACK_FAILED",
            {},
            lambda: self._read_quote_event_schema_gate(read_quote_event_schema),
        )

    def existing_evidence_ids(
        self,
        runtime_id: str,
        *,
        evidence_ids: tuple[str, ...],
        include_archived: bool = False,
    ) -> set[str]:
        if not evidence_ids:
            return set()
        return self._with_runtime_db_error(
            "existing_evidence_ids",
            "MINIQMT_RUNTIME_DB_LIST_LINKED_EVIDENCE_FAILED",
            {"runtime_id": runtime_id, "evidence_count": len(evidence_ids), "include_archived": include_archived},
            lambda: self._existing_evidence_id_rows(
                runtime_id,
                evidence_ids=evidence_ids,
                include_archived=include_archived,
            ),
        )

    def _read_quote_event_schema_gate(self, reader: Callable[[Any], Any]) -> str:
        with self._conn() as conn:
            return str(reader(conn).production_ddl_gate)

    def next_event_sequence(self, runtime_id: str) -> int:
        runtime = self.get_runtime(runtime_id)
        if runtime is not None and runtime.last_event_sequence:
            return int(runtime.last_event_sequence) + 1
        return (
            self._with_runtime_db_error(
                "next_event_sequence",
                "MINIQMT_RUNTIME_DB_NEXT_EVENT_SEQUENCE_FAILED",
                {"runtime_id": runtime_id},
                lambda: self._last_event_sequence(runtime_id),
            )
            + 1
        )

    def read_quote_control_snapshot(
        self,
        *,
        cursor: Any,
        runtime_ids: tuple[str, ...],
        include_archived: bool = True,
    ) -> tuple[list[MiniQMTExecutionRuntimeRecord], list[MiniQMTExecutionEvent]]:
        if cursor is None:
            raise ValueError("quote-control export requires the TCA-owned external read cursor")
        normalized_ids = tuple(
            sorted({str(runtime_id or "").strip() for runtime_id in runtime_ids if str(runtime_id or "").strip()})
        )
        if not normalized_ids:
            return [], []
        cursor.execute(
            """
            SELECT *
            FROM qmt_strategy.execution_runtime
            WHERE runtime_id = ANY(%s)
            ORDER BY runtime_id
            """,
            (list(normalized_ids),),
        )
        runtimes = [_row_to_runtime(row) for row in cursor.fetchall()]
        archived_clause = "" if include_archived else "AND archived_at IS NULL"
        cursor.execute(
            f"""
            SELECT *
            FROM qmt_strategy.execution_runtime_event
            WHERE runtime_id = ANY(%s)
              {archived_clause}
            ORDER BY runtime_id, sequence, event_id
            """,
            (list(normalized_ids),),
        )
        events = [_row_to_event(row) for row in cursor.fetchall()]
        return runtimes, events

    def upsert_algo_instance(self, instance: MiniQMTExecutionAlgoInstance) -> MiniQMTExecutionAlgoInstance:
        stored = instance.model_copy(update={"updated_at": datetime.now(UTC)})
        self._with_runtime_db_error(
            "upsert_algo_instance",
            "MINIQMT_RUNTIME_DB_UPSERT_ALGO_FAILED",
            {
                "runtime_id": stored.runtime_id,
                "algo_instance_id": stored.algo_instance_id,
                "status": stored.status.value,
            },
            lambda: self._upsert_algo_instance_row(stored),
        )
        self._prune_runtime_if_due(runtime_id=stored.runtime_id, reason="upsert_algo_instance")
        return stored

    def list_algo_instances(
        self,
        runtime_id: str,
        *,
        active_only: bool = False,
    ) -> list[MiniQMTExecutionAlgoInstance]:
        return self._with_runtime_db_error(
            "list_algo_instances",
            "MINIQMT_RUNTIME_DB_LIST_ALGO_FAILED",
            {"runtime_id": runtime_id, "active_only": active_only},
            lambda: self._list_algo_instance_rows(runtime_id, active_only=active_only),
        )

    def upsert_child_order(self, order: MiniQMTChildOrder) -> MiniQMTChildOrder:
        stored = order.model_copy(update={"updated_at": datetime.now(UTC)})
        self._with_runtime_db_error(
            "upsert_child_order",
            "MINIQMT_RUNTIME_DB_UPSERT_CHILD_ORDER_FAILED",
            {
                "runtime_id": stored.runtime_id,
                "child_order_id": stored.child_order_id,
                "status": stored.status.value,
            },
            lambda: self._upsert_child_order_row(stored),
        )
        self._prune_runtime_if_due(runtime_id=stored.runtime_id, reason="upsert_child_order")
        return stored

    def list_child_orders(
        self,
        runtime_id: str,
        *,
        active_only: bool = False,
    ) -> list[MiniQMTChildOrder]:
        return self._with_runtime_db_error(
            "list_child_orders",
            "MINIQMT_RUNTIME_DB_LIST_CHILD_ORDER_FAILED",
            {"runtime_id": runtime_id, "active_only": active_only},
            lambda: self._list_child_order_rows(runtime_id, active_only=active_only),
        )

    def mark_runtime_state(
        self,
        runtime_id: str,
        state: MiniQMTExecutionRuntimeState,
    ) -> MiniQMTExecutionRuntimeRecord:
        runtime = self.get_runtime(runtime_id)
        if runtime is None:
            raise RuntimeConfigInvalidError(
                "MiniQMT runtime DB state update found no runtime",
                context={
                    "reason_code": "MINIQMT_RUNTIME_DB_RUNTIME_MISSING",
                    "stage": "MINIQMT_RUNTIME_DB_MARK_STATE",
                    "runtime_id": runtime_id,
                },
            )
        return self.upsert_runtime(runtime.model_copy(update={"event_loop_state": state}))

    def prune_runtime(self, *, runtime_id: str, reason: str = "runtime_store_prune") -> dict[str, Any]:
        return self._with_runtime_db_error(
            "prune_runtime",
            "MINIQMT_RUNTIME_DB_PRUNE_FAILED",
            {"runtime_id": runtime_id, "reason": reason},
            lambda: self._prune_runtime_rows(runtime_id=runtime_id, reason=reason),
        )

    def maintenance_status(self) -> dict[str, Any]:
        return self._with_runtime_db_error(
            "maintenance_status",
            "MINIQMT_RUNTIME_DB_MAINTENANCE_STATUS_FAILED",
            {},
            self._maintenance_status_rows,
        )

    def _prune_runtime_if_due(self, *, runtime_id: str, reason: str) -> dict[str, Any] | None:
        prune_every_writes = _positive_int_env(
            MINIQMT_EXECUTION_RUNTIME_PRUNE_EVERY_WRITES_ENV,
            DEFAULT_MINIQMT_EXECUTION_RUNTIME_PRUNE_EVERY_WRITES,
        )
        with self._prune_lock:
            write_count = self._prune_write_count_by_runtime.get(runtime_id, 0) + 1
            if write_count < prune_every_writes:
                self._prune_write_count_by_runtime[runtime_id] = write_count
                return None
            self._prune_write_count_by_runtime[runtime_id] = 0
        return self.prune_runtime(runtime_id=runtime_id, reason=reason)

    def _upsert_runtime_row(self, runtime: MiniQMTExecutionRuntimeRecord) -> None:
        with self._conn(manage_transaction=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_runtime (
                        runtime_id, account_group_id, trade_date, mode, event_loop_state,
                        gateway_state, oms_state, runtime_config_hash, last_event_sequence,
                        metadata, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (runtime_id) DO UPDATE SET
                        account_group_id = EXCLUDED.account_group_id,
                        trade_date = EXCLUDED.trade_date,
                        mode = EXCLUDED.mode,
                        event_loop_state = EXCLUDED.event_loop_state,
                        gateway_state = EXCLUDED.gateway_state,
                        oms_state = EXCLUDED.oms_state,
                        runtime_config_hash = EXCLUDED.runtime_config_hash,
                        last_event_sequence = GREATEST(
                            qmt_strategy.execution_runtime.last_event_sequence,
                            EXCLUDED.last_event_sequence
                        ),
                        metadata = EXCLUDED.metadata,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        runtime.runtime_id,
                        runtime.account_group_id,
                        runtime.trade_date,
                        runtime.mode.value,
                        runtime.event_loop_state.value,
                        runtime.gateway_state.value,
                        runtime.oms_state.value,
                        runtime.runtime_config_hash,
                        runtime.last_event_sequence,
                        _json(runtime.metadata),
                        runtime.created_at,
                        runtime.updated_at,
                    ),
                )

    def _get_runtime_row(self, runtime_id: str) -> MiniQMTExecutionRuntimeRecord | None:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.execution_runtime
                    WHERE runtime_id = %s AND archived_at IS NULL
                    """,
                    (runtime_id,),
                )
                row = cur.fetchone()
        return _row_to_runtime(row) if row else None

    def _list_runtime_rows(self) -> list[MiniQMTExecutionRuntimeRecord]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.execution_runtime
                    WHERE archived_at IS NULL
                    ORDER BY updated_at DESC, runtime_id
                    """
                )
                rows = cur.fetchall()
        return [_row_to_runtime(row) for row in rows]

    def _list_runtime_rows_for_account(
        self, *, account_group_id: str, limit: int
    ) -> list[MiniQMTExecutionRuntimeRecord]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.execution_runtime
                    WHERE archived_at IS NULL
                      AND account_group_id = %s
                      AND event_loop_state NOT IN ('STOPPED', 'FAILED')
                    ORDER BY updated_at DESC, runtime_id
                    LIMIT %s
                    """,
                    (account_group_id, limit),
                )
                rows = cur.fetchall()
        return [_row_to_runtime(row) for row in rows]

    def _append_event_row(self, event: MiniQMTExecutionEvent) -> None:
        with self._conn(manage_transaction=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_runtime_event (
                        event_id, runtime_id, sequence, event_type, event_time, source, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        event.event_id,
                        event.runtime_id,
                        event.sequence,
                        event.event_type.value,
                        event.event_time,
                        event.source,
                        _json(event.payload),
                    ),
                )
                cur.execute(
                    """
                    UPDATE qmt_strategy.execution_runtime
                    SET last_event_sequence = GREATEST(last_event_sequence, %s),
                        updated_at = %s
                    WHERE runtime_id = %s
                    """,
                    (event.sequence, event.event_time, event.runtime_id),
                )

    def _append_evidence_event_row(self, candidate: QuoteEvidenceEventCandidate) -> DurableEvidenceReceipt:
        with self._conn(manage_transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT runtime_id, last_event_sequence
                    FROM qmt_strategy.execution_runtime
                    WHERE runtime_id = %s
                    FOR UPDATE
                    """,
                    (candidate.runtime_id,),
                )
                runtime = cur.fetchone()
                if runtime is None:
                    raise ValueError(f"quote evidence runtime does not exist: {candidate.runtime_id}")
                cur.execute(
                    """
                    SELECT event_id, runtime_id, sequence, event_type, event_time, source, payload, created_at
                    FROM qmt_strategy.execution_runtime_event
                    WHERE event_id = %s
                    """,
                    (candidate.event_id,),
                )
                existing = cur.fetchone()
                if existing is not None:
                    event = _row_to_event(existing)
                    if not _evidence_event_matches_candidate(event, candidate):
                        raise QuoteEvidenceIdempotencyConflict(
                            f"quote evidence event id conflicts: {candidate.event_id}"
                        )
                    persisted_at = existing["created_at"]
                else:
                    sequence = int(runtime["last_event_sequence"] or 0) + 1
                    cur.execute(
                        """
                        INSERT INTO qmt_strategy.execution_runtime_event (
                            event_id, runtime_id, sequence, event_type, event_time, source, payload
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING created_at
                        """,
                        (
                            candidate.event_id,
                            candidate.runtime_id,
                            sequence,
                            candidate.event_type.value,
                            candidate.event_time,
                            "quote_ingress",
                            _json(candidate.payload),
                        ),
                    )
                    inserted = cur.fetchone()
                    if inserted is None:
                        raise RuntimeError("quote evidence insert did not return created_at")
                    persisted_at = inserted["created_at"]
                    cur.execute(
                        """
                        UPDATE qmt_strategy.execution_runtime
                        SET last_event_sequence = %s,
                            updated_at = %s
                        WHERE runtime_id = %s
                        """,
                        (sequence, candidate.event_time, candidate.runtime_id),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError("quote evidence runtime sequence update did not affect exactly one row")
                    event = MiniQMTExecutionEvent(
                        event_id=candidate.event_id,
                        runtime_id=candidate.runtime_id,
                        sequence=sequence,
                        event_type=candidate.event_type,
                        event_time=candidate.event_time,
                        source="quote_ingress",
                        payload=dict(candidate.payload),
                    )
        readback_event, readback_persisted_at = self._read_evidence_event(candidate.event_id)
        if (
            not _evidence_event_matches_candidate(readback_event, candidate)
            or readback_event.sequence != event.sequence
            or readback_persisted_at != persisted_at
        ):
            raise RuntimeError(f"quote evidence post-commit readback mismatch: {candidate.event_id}")
        return DurableEvidenceReceipt(
            event=readback_event,
            persisted_at_utc=readback_persisted_at,
            durable_ack=True,
            readback_verified=True,
        )

    def _read_evidence_event(self, event_id: str) -> tuple[MiniQMTExecutionEvent, datetime]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT event_id, runtime_id, sequence, event_type, event_time, source, payload, created_at
                    FROM qmt_strategy.execution_runtime_event
                    WHERE event_id = %s
                    """,
                    (event_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"quote evidence post-commit readback is missing: {event_id}")
        return _row_to_event(row), row["created_at"]

    def _list_event_rows(self, runtime_id: str, *, include_archived: bool = False) -> list[MiniQMTExecutionEvent]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                archive_clause = "" if include_archived else "AND archived_at IS NULL"
                cur.execute(
                    f"""
                    SELECT *
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s {archive_clause}
                    ORDER BY sequence, event_time, event_id
                    """,
                    (runtime_id,),
                )
                rows = cur.fetchall()
        return [_row_to_event(row) for row in rows]

    def _list_evidence_receipt_rows(
        self,
        runtime_id: str,
        *,
        market_data_id: str | None,
        evidence_id: str | None,
        include_archived: bool,
        after_sequence: int,
        after_event_id: str,
        limit: int,
    ) -> list[DurableEvidenceReceipt]:
        if limit < 1 or limit > 501:
            raise ValueError("evidence receipt limit must be between 1 and 501")
        archive_clause = "" if include_archived else "AND archived_at IS NULL"
        expected = market_data_id if market_data_id is not None else evidence_id
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    WITH RECURSIVE evidence_rows AS (
                        SELECT event_id, runtime_id, sequence, event_type, event_time, source, payload, created_at,
                               array_remove(ARRAY[
                                   event_id,
                                   payload -> 'evidence' ->> 'evidence_id',
                                   payload -> 'evidence' ->> 'market_data_id',
                                   payload -> 'evidence' ->> 'anchor_market_data_id',
                                   payload -> 'evidence' ->> 'action_evidence_id',
                                   payload -> 'evidence' ->> 'child_receipt_evidence_id',
                                   payload -> 'evidence' ->> 'supersedes_evidence_id',
                                   payload -> 'evidence' ->> 'source_child_event_id',
                                   payload -> 'evidence' ->> 'anchor_trade_event_id',
                                   payload -> 'evidence' ->> 'action_id',
                                   payload -> 'evidence' ->> 'child_order_id',
                                   payload -> 'evidence' ->> 'broker_order_id',
                                   payload -> 'evidence' ->> 'trade_id',
                                   payload -> 'evidence' ->> 'mark_series_key'
                               ], NULL) AS link_tokens
                        FROM qmt_strategy.execution_runtime_event
                        WHERE runtime_id = %s
                          {archive_clause}
                          AND source = 'quote_ingress'
                          AND jsonb_typeof(payload -> 'evidence') = 'object'
                    ), chain AS (
                        SELECT * FROM evidence_rows WHERE %s = ANY(link_tokens)
                        UNION
                        SELECT candidate.*
                        FROM evidence_rows AS candidate
                        JOIN chain AS linked ON candidate.link_tokens && linked.link_tokens
                    )
                    SELECT DISTINCT event_id, runtime_id, sequence, event_type, event_time, source, payload, created_at
                    FROM chain
                    WHERE (sequence, event_id) > (%s, %s)
                    ORDER BY sequence, event_id
                    LIMIT %s
                    """,
                    (runtime_id, expected, after_sequence, after_event_id, limit),
                )
                rows = cur.fetchall()
        receipts: list[DurableEvidenceReceipt] = []
        for row in rows:
            event = _row_to_event(row)
            evidence = event.payload.get("evidence") if isinstance(event.payload, dict) else None
            if event.source != "quote_ingress" or not isinstance(evidence, dict) or not evidence.get("evidence_sha256"):
                raise RuntimeError(f"quote evidence readback row is structurally invalid: {event.event_id}")
            receipts.append(
                DurableEvidenceReceipt(
                    event=event,
                    persisted_at_utc=row["created_at"],
                    durable_ack=True,
                    readback_verified=True,
                )
            )
        return receipts

    def _list_quote_event_rows(
        self,
        runtime_id: str,
        *,
        symbol: str | None,
        after_sequence: int,
        after_event_id: str,
        limit: int,
    ) -> list[MiniQMTExecutionEvent]:
        if limit < 1 or limit > 501:
            raise ValueError("quote event page limit must be between 1 and 501")
        quote_types = [item.value for item in MiniQMTExecutionEventType if item.value.startswith("QUOTE_")]
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT event_id, runtime_id, sequence, event_type, event_time, source, payload, created_at
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s
                      AND archived_at IS NULL
                      AND event_type = ANY(%s)
                      AND (%s IS NULL OR payload -> 'evidence' ->> 'symbol' = %s)
                      AND (sequence, event_id) > (%s, %s)
                    ORDER BY sequence, event_id
                    LIMIT %s
                    """,
                    (runtime_id, quote_types, symbol, symbol, after_sequence, after_event_id, limit),
                )
                rows = cur.fetchall()
        return [_row_to_event(row) for row in rows]

    def _list_event_rows_by_ids(
        self,
        runtime_id: str,
        *,
        event_ids: tuple[str, ...],
        include_archived: bool,
    ) -> list[MiniQMTExecutionEvent]:
        archive_clause = "" if include_archived else "AND archived_at IS NULL"
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT event_id, runtime_id, sequence, event_type, event_time, source, payload, created_at
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s {archive_clause} AND event_id = ANY(%s)
                    ORDER BY sequence, event_id
                    """,
                    (runtime_id, list(event_ids)),
                )
                rows = cur.fetchall()
        return [_row_to_event(row) for row in rows]

    def _existing_evidence_id_rows(
        self,
        runtime_id: str,
        *,
        evidence_ids: tuple[str, ...],
        include_archived: bool,
    ) -> set[str]:
        archive_clause = "" if include_archived else "AND archived_at IS NULL"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT payload -> 'evidence' ->> 'evidence_id'
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s {archive_clause}
                      AND payload -> 'evidence' ->> 'evidence_id' = ANY(%s)
                    """,
                    (runtime_id, list(evidence_ids)),
                )
                rows = cur.fetchall()
        return {str(row[0]) for row in rows}

    def _quote_diagnostics_summary_rows(self, runtime_id: str, *, symbol: str | None) -> dict[str, Any]:
        symbol_clause = "" if symbol is None else "AND payload -> 'evidence' ->> 'symbol' = %s"
        symbol_params: tuple[Any, ...] = () if symbol is None else (symbol,)
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT payload -> 'evidence' ->> 'symbol' AS symbol,
                           COUNT(*) AS capture_count,
                           MAX(event_time) AS last_event_time
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s AND archived_at IS NULL
                      AND event_type = ANY(%s)
                      AND jsonb_typeof(payload -> 'evidence') = 'object'
                      {symbol_clause}
                    GROUP BY payload -> 'evidence' ->> 'symbol'
                    ORDER BY symbol
                    """,
                    (
                        runtime_id,
                        [item.value for item in MiniQMTExecutionEventType if item.value.startswith("QUOTE_")],
                        *symbol_params,
                    ),
                )
                per_symbol = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    f"""
                    SELECT COALESCE(payload -> 'evidence' ->> 'quality_reason_code',
                                    payload -> 'evidence' ->> 'unavailable_reason') AS reason_code,
                           COUNT(*) AS occurrence_count
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s AND archived_at IS NULL
                      AND jsonb_typeof(payload -> 'evidence') = 'object'
                      {symbol_clause}
                      AND COALESCE(payload -> 'evidence' ->> 'quality_reason_code',
                                   payload -> 'evidence' ->> 'unavailable_reason') IS NOT NULL
                    GROUP BY reason_code
                    ORDER BY reason_code
                    """,
                    (runtime_id, *symbol_params),
                )
                reason_counts = {str(row["reason_code"]): int(row["occurrence_count"]) for row in cur.fetchall()}
                cur.execute(
                    f"""
                    SELECT COALESCE(payload -> 'evidence' ->> 'quality_reason_code',
                                    payload -> 'evidence' ->> 'unavailable_reason') AS reason_code,
                           payload -> 'evidence' ->> 'stage' AS stage,
                           event_id, event_time
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s AND archived_at IS NULL
                      AND jsonb_typeof(payload -> 'evidence') = 'object'
                      {symbol_clause}
                      AND COALESCE(payload -> 'evidence' ->> 'quality_reason_code',
                                   payload -> 'evidence' ->> 'unavailable_reason') IS NOT NULL
                    ORDER BY sequence DESC, event_id DESC
                    LIMIT 1
                    """,
                    (runtime_id, *symbol_params),
                )
                last_reason = cur.fetchone()
                cur.execute(
                    f"""
                    SELECT COUNT(*) AS terminal_due_count,
                           COUNT(*) FILTER (WHERE payload -> 'evidence' ->> 'mark_status' = 'CAPTURED') AS captured_count,
                           COUNT(*) FILTER (WHERE payload -> 'evidence' ->> 'mark_status' = 'UNAVAILABLE') AS unavailable_count
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s AND archived_at IS NULL
                      AND payload -> 'evidence' ->> 'capture_type' LIKE 'MARKOUT_%%'
                      {symbol_clause}
                    """,
                    (runtime_id, *symbol_params),
                )
                markout = dict(cur.fetchone())
                cur.execute(
                    """
                    SELECT payload -> 'health_or_aggregate' AS health,
                           event_id, sequence, event_time
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s AND archived_at IS NULL AND event_type = 'QUOTE_INGRESS_HEALTH'
                    ORDER BY sequence DESC, event_id DESC
                    LIMIT 1
                    """,
                    (runtime_id,),
                )
                health_row = cur.fetchone()
        return {
            "per_symbol": per_symbol,
            "reason_counts": reason_counts,
            "last_reason": dict(last_reason) if last_reason else None,
            "markout": markout,
            "health": dict(health_row["health"]) if health_row and isinstance(health_row["health"], dict) else None,
            "health_event": (
                {
                    "event_id": str(health_row["event_id"]),
                    "sequence": int(health_row["sequence"]),
                    "event_time": health_row["event_time"],
                }
                if health_row and isinstance(health_row["health"], dict)
                else None
            ),
        }

    def _last_event_sequence(self, runtime_id: str) -> int:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(MAX(sequence), 0)
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s
                    """,
                    (runtime_id,),
                )
                row = cur.fetchone()
        return int(row[0] or 0) if row else 0

    def _upsert_algo_instance_row(self, instance: MiniQMTExecutionAlgoInstance) -> None:
        with self._conn(manage_transaction=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_algo_instance (
                        algo_instance_id, runtime_id, parent_intent_id, strategy_slot_id,
                        symbol, side, target_quantity, remaining_quantity, algo_code,
                        status, metadata, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (algo_instance_id) DO UPDATE SET
                        runtime_id = EXCLUDED.runtime_id,
                        parent_intent_id = EXCLUDED.parent_intent_id,
                        strategy_slot_id = EXCLUDED.strategy_slot_id,
                        symbol = EXCLUDED.symbol,
                        side = EXCLUDED.side,
                        target_quantity = EXCLUDED.target_quantity,
                        remaining_quantity = EXCLUDED.remaining_quantity,
                        algo_code = EXCLUDED.algo_code,
                        status = EXCLUDED.status,
                        metadata = EXCLUDED.metadata,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        instance.algo_instance_id,
                        instance.runtime_id,
                        instance.parent_intent_id,
                        instance.strategy_slot_id,
                        instance.symbol,
                        instance.side.value,
                        instance.target_quantity,
                        instance.remaining_quantity,
                        instance.algo_code,
                        instance.status.value,
                        _json(instance.metadata),
                        instance.created_at,
                        instance.updated_at,
                    ),
                )

    def _list_algo_instance_rows(self, runtime_id: str, *, active_only: bool) -> list[MiniQMTExecutionAlgoInstance]:
        filters = ["runtime_id = %s", "archived_at IS NULL"]
        params: list[Any] = [runtime_id]
        if active_only:
            filters.append("status = %s")
            params.append(MiniQMTAlgoInstanceStatus.ACTIVE.value)
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM qmt_strategy.execution_algo_instance
                    WHERE {" AND ".join(filters)}
                    ORDER BY created_at, algo_instance_id
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [_row_to_algo_instance(row) for row in rows]

    def _upsert_child_order_row(self, order: MiniQMTChildOrder) -> None:
        with self._conn(manage_transaction=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_child_order (
                        child_order_id, runtime_id, algo_instance_id, parent_intent_id,
                        strategy_slot_id, symbol, side, quantity, price, price_type,
                        status, broker_order_id, submitted_at, metadata, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (child_order_id) DO UPDATE SET
                        runtime_id = EXCLUDED.runtime_id,
                        algo_instance_id = EXCLUDED.algo_instance_id,
                        parent_intent_id = EXCLUDED.parent_intent_id,
                        strategy_slot_id = EXCLUDED.strategy_slot_id,
                        symbol = EXCLUDED.symbol,
                        side = EXCLUDED.side,
                        quantity = EXCLUDED.quantity,
                        price = EXCLUDED.price,
                        price_type = EXCLUDED.price_type,
                        status = EXCLUDED.status,
                        broker_order_id = EXCLUDED.broker_order_id,
                        submitted_at = EXCLUDED.submitted_at,
                        metadata = EXCLUDED.metadata,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        order.child_order_id,
                        order.runtime_id,
                        order.algo_instance_id,
                        order.parent_intent_id,
                        order.strategy_slot_id,
                        order.symbol,
                        order.side.value,
                        order.quantity,
                        order.price,
                        order.price_type,
                        order.status.value,
                        order.broker_order_id,
                        order.submitted_at,
                        _json(order.metadata),
                        order.updated_at,
                    ),
                )

    def _list_child_order_rows(self, runtime_id: str, *, active_only: bool) -> list[MiniQMTChildOrder]:
        filters = ["runtime_id = %s", "archived_at IS NULL"]
        params: list[Any] = [runtime_id]
        if active_only:
            filters.append("status <> ALL(%s)")
            params.append(list(_TERMINAL_CHILD_ORDER_STATUSES))
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM qmt_strategy.execution_child_order
                    WHERE {" AND ".join(filters)}
                    ORDER BY updated_at, child_order_id
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [_row_to_child_order(row) for row in rows]

    def _prune_runtime_rows(self, *, runtime_id: str, reason: str) -> dict[str, Any]:
        with self._conn(manage_transaction=True) as conn:
            with conn.cursor() as cur:
                pruned_counts = {
                    "events": self._archive_events_for_runtime(cur, runtime_id=runtime_id, reason=reason),
                    "algo_instances": self._archive_algo_instances_for_runtime(
                        cur, runtime_id=runtime_id, reason=reason
                    ),
                    "child_orders": self._archive_child_orders_for_runtime(cur, runtime_id=runtime_id, reason=reason),
                }
        return {
            "schema_version": "miniqmt_runtime_db_prune_v1",
            "runtime_id": runtime_id,
            "reason": reason,
            "pruned_counts": pruned_counts,
            "pruned_total": sum(pruned_counts.values()),
            "completed_at": datetime.now(UTC).isoformat(),
        }

    def _archive_events_for_runtime(self, cur: Any, *, runtime_id: str, reason: str) -> int:
        max_events = _positive_int_env(
            MINIQMT_EXECUTION_RUNTIME_MAX_EVENTS_PER_RUNTIME_ENV,
            DEFAULT_MINIQMT_EXECUTION_RUNTIME_MAX_EVENTS_PER_RUNTIME,
        )
        retain_events = min(
            max_events,
            _positive_int_env(
                MINIQMT_EXECUTION_RUNTIME_RETAIN_EVENTS_PER_RUNTIME_ENV,
                DEFAULT_MINIQMT_EXECUTION_RUNTIME_RETAIN_EVENTS_PER_RUNTIME,
            ),
        )
        cur.execute(
            """
            WITH active AS (
                SELECT event_id, event_type, event_time, sequence, payload
                FROM qmt_strategy.execution_runtime_event
                WHERE runtime_id = %s AND archived_at IS NULL
            ), ordinary AS (
                SELECT event_id,
                       row_number() OVER (ORDER BY sequence DESC, event_time DESC, event_id DESC) AS rn,
                       COUNT(*) OVER () AS ordinary_count
                FROM active
                WHERE event_type NOT IN (
                    'QUOTE_OBSERVED', 'QUOTE_INGRESS_HEALTH', 'QUOTE_REJECTED',
                    'QUOTE_ELIGIBILITY_EVALUATED', 'QUOTE_MARK_CAPTURED', 'TRADE_EVENT',
                    'CHILD_ORDER_SUBMITTED', 'CHILD_ORDER_REJECTED', 'CHILD_ORDER_CANCEL_REQUESTED'
                )
            ), eligible AS (
                SELECT event_id FROM active
                WHERE event_type IN ('QUOTE_OBSERVED', 'QUOTE_INGRESS_HEALTH')
                  AND event_time < NOW() - INTERVAL '14 days'
                UNION
                SELECT candidate.event_id
                FROM active AS candidate
                WHERE candidate.event_type IN (
                    'QUOTE_REJECTED', 'QUOTE_ELIGIBILITY_EVALUATED', 'QUOTE_MARK_CAPTURED',
                    'TRADE_EVENT', 'CHILD_ORDER_SUBMITTED', 'CHILD_ORDER_REJECTED',
                    'CHILD_ORDER_CANCEL_REQUESTED'
                )
                  AND candidate.event_time < NOW() - INTERVAL '90 days'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM active AS trade
                      WHERE trade.event_type = 'TRADE_EVENT'
                        AND (
                            trade.event_id = candidate.event_id
                            OR trade.payload -> 'quote_evidence_markout_anchor_v1' ->> 'action_evidence_id'
                               = candidate.payload -> 'evidence' ->> 'evidence_id'
                            OR trade.payload -> 'quote_evidence_markout_anchor_v1' ->> 'child_order_id'
                               = COALESCE(
                                   candidate.payload -> 'evidence' ->> 'child_order_id',
                                   candidate.payload ->> 'child_order_id'
                               )
                        )
                        AND (
                            SELECT COUNT(DISTINCT mark.payload -> 'evidence' ->> 'horizon_seconds')
                            FROM active AS mark
                            WHERE mark.event_type = 'QUOTE_MARK_CAPTURED'
                              AND mark.payload -> 'evidence' ->> 'trade_id'
                                  = trade.payload -> 'quote_evidence_markout_anchor_v1' ->> 'trade_id'
                              AND mark.payload -> 'evidence' ->> 'mark_status' IN ('CAPTURED', 'UNAVAILABLE')
                        ) < 3
                  )
                UNION
                SELECT event_id FROM ordinary WHERE rn > %s AND ordinary_count > %s
            )
            UPDATE qmt_strategy.execution_runtime_event AS event
            SET archived_at = NOW(),
                archive_reason = %s
            FROM eligible
            WHERE event.event_id = eligible.event_id
            """,
            (runtime_id, retain_events, max_events, reason),
        )
        return int(cur.rowcount or 0)

    def _archive_algo_instances_for_runtime(self, cur: Any, *, runtime_id: str, reason: str) -> int:
        max_items = _positive_int_env(
            MINIQMT_EXECUTION_RUNTIME_MAX_ALGO_INSTANCES_PER_RUNTIME_ENV,
            DEFAULT_MINIQMT_EXECUTION_RUNTIME_MAX_ALGO_INSTANCES_PER_RUNTIME,
        )
        retain_items = min(
            max_items,
            _positive_int_env(
                MINIQMT_EXECUTION_RUNTIME_RETAIN_ALGO_INSTANCES_PER_RUNTIME_ENV,
                DEFAULT_MINIQMT_EXECUTION_RUNTIME_RETAIN_ALGO_INSTANCES_PER_RUNTIME,
            ),
        )
        cur.execute(
            """
            WITH ranked AS (
                SELECT algo_instance_id,
                       row_number() OVER (
                           ORDER BY CASE WHEN status = 'ACTIVE' THEN 0 ELSE 1 END,
                                    updated_at DESC,
                                    algo_instance_id DESC
                       ) AS rn
                FROM qmt_strategy.execution_algo_instance
                WHERE runtime_id = %s AND archived_at IS NULL
            )
            UPDATE qmt_strategy.execution_algo_instance AS algo
            SET archived_at = NOW(),
                archive_reason = %s
            FROM ranked
            WHERE algo.algo_instance_id = ranked.algo_instance_id
              AND ranked.rn > %s
              AND (SELECT COUNT(*) FROM ranked) > %s
            """,
            (runtime_id, reason, retain_items, max_items),
        )
        return int(cur.rowcount or 0)

    def _archive_child_orders_for_runtime(self, cur: Any, *, runtime_id: str, reason: str) -> int:
        max_items = _positive_int_env(
            MINIQMT_EXECUTION_RUNTIME_MAX_CHILD_ORDERS_PER_RUNTIME_ENV,
            DEFAULT_MINIQMT_EXECUTION_RUNTIME_MAX_CHILD_ORDERS_PER_RUNTIME,
        )
        retain_items = min(
            max_items,
            _positive_int_env(
                MINIQMT_EXECUTION_RUNTIME_RETAIN_CHILD_ORDERS_PER_RUNTIME_ENV,
                DEFAULT_MINIQMT_EXECUTION_RUNTIME_RETAIN_CHILD_ORDERS_PER_RUNTIME,
            ),
        )
        cur.execute(
            """
            WITH ranked AS (
                SELECT child_order_id,
                       row_number() OVER (
                           ORDER BY CASE WHEN status IN ('FILLED', 'CANCELLED', 'REJECTED') THEN 1 ELSE 0 END,
                                    updated_at DESC,
                                    child_order_id DESC
                       ) AS rn
                FROM qmt_strategy.execution_child_order
                WHERE runtime_id = %s AND archived_at IS NULL
            )
            UPDATE qmt_strategy.execution_child_order AS child
            SET archived_at = NOW(),
                archive_reason = %s
            FROM ranked
            WHERE child.child_order_id = ranked.child_order_id
              AND ranked.rn > %s
              AND (SELECT COUNT(*) FROM ranked) > %s
            """,
            (runtime_id, reason, retain_items, max_items),
        )
        return int(cur.rowcount or 0)

    def _maintenance_status_rows(self) -> dict[str, Any]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                counts: dict[str, int] = {}
                for key, table in {
                    "runtime_count": "qmt_strategy.execution_runtime",
                    "event_count": "qmt_strategy.execution_runtime_event",
                    "algo_instance_count": "qmt_strategy.execution_algo_instance",
                    "child_order_count": "qmt_strategy.execution_child_order",
                }.items():
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE archived_at IS NULL")
                    row = cur.fetchone()
                    counts[key] = int(row[0] or 0) if row else 0
        return {
            "schema_version": "miniqmt_runtime_store_maintenance_status_v1",
            "repository": "postgres",
            "write_mode": "incremental_postgres_row_writes",
            "jsonfile_production_fallback": False,
            **counts,
        }

    @contextmanager
    def _conn(self, *, autocommit: bool | None = None, manage_transaction: bool = False) -> Iterator[Any]:
        effective_autocommit = (not manage_transaction) if autocommit is None else autocommit
        kwargs: dict[str, Any] = {}
        if self._conn_factory_accepts_autocommit:
            kwargs["autocommit"] = effective_autocommit
        if self._conn_factory_accepts_manage_transaction:
            kwargs["manage_transaction"] = manage_transaction
        with self._conn_factory(**kwargs) as conn:
            yield conn

    @staticmethod
    def _with_runtime_db_error(
        stage: str,
        reason_code: str,
        context: dict[str, Any],
        func: Callable[[], Any],
    ) -> Any:
        try:
            return func()
        except RuntimeConfigInvalidError:
            raise
        except QuoteEvidenceIdempotencyConflict:
            # This is an invariant violation with its own registered reason;
            # callers must never collapse it into a retryable DB outage.
            raise
        except psycopg2.Error as exc:
            schema_gate_context = (
                {"ddl_required": True, "production_ddl_gate": "pending"}
                if getattr(exc, "pgcode", None) in {"42P01", "42703", "42704", "23514"}
                else {}
            )
            raise RuntimeConfigInvalidError(
                "MiniQMT runtime Postgres repository operation failed loudly",
                context={
                    **context,
                    "reason_code": reason_code,
                    "stage": f"MINIQMT_RUNTIME_DB_{stage.upper()}",
                    "error_type": type(exc).__name__,
                    "pgcode": getattr(exc, "pgcode", None),
                    "message": str(exc),
                    **schema_gate_context,
                },
            ) from exc
        except Exception as exc:
            raise RuntimeConfigInvalidError(
                "MiniQMT runtime Postgres repository operation failed loudly",
                context={
                    **context,
                    "reason_code": reason_code,
                    "stage": f"MINIQMT_RUNTIME_DB_{stage.upper()}",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
            ) from exc


class JsonFileMiniQMTExecutionRuntimeRepository(InMemoryMiniQMTExecutionRuntimeRepository):
    """Test/offline JSONL-backed store; production uses Postgres."""

    _SNAPSHOT_SCHEMA_VERSION = "miniqmt_execution_runtime_json_repository_v1"
    _OPLOG_SCHEMA_VERSION = "miniqmt_execution_runtime_jsonl_op_v1"

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._oplog_path = self._path.with_suffix(".jsonl")
        self._archive_dir = self._path.parent / "archive"
        self._writes_since_compaction = 0
        self._last_maintenance: dict[str, Any] | None = None
        super().__init__()
        self._reset_oversized_store_if_needed()
        self._load()
        self._prune_and_compact_if_needed(reason="load")

    def upsert_runtime(self, runtime: MiniQMTExecutionRuntimeRecord) -> MiniQMTExecutionRuntimeRecord:
        stored = super().upsert_runtime(runtime)
        self._append_operation("upsert_runtime", stored.model_dump(mode="json"))
        self._after_incremental_write(runtime_id=stored.runtime_id, reason="upsert_runtime")
        return stored

    def append_event(self, event: MiniQMTExecutionEvent) -> MiniQMTExecutionEvent:
        stored = super().append_event(event)
        self._append_operation("append_event", stored.model_dump(mode="json"))
        self._after_incremental_write(runtime_id=stored.runtime_id, reason="append_event")
        return stored

    def append_evidence_event_idempotent(self, candidate: QuoteEvidenceEventCandidate) -> DurableEvidenceReceipt:
        existing = self._evidence_receipts.get(candidate.event_id)
        receipt = super().append_evidence_event_idempotent(candidate)
        if existing is not None:
            return receipt
        self._append_operation(
            "append_evidence_event",
            {
                "event": receipt.event.model_dump(mode="json"),
                "persisted_at_utc": receipt.persisted_at_utc.isoformat(),
                "durable_ack": receipt.durable_ack,
                "readback_verified": receipt.readback_verified,
            },
        )
        self._after_incremental_write(runtime_id=receipt.event.runtime_id, reason="append_evidence_event")
        return receipt

    def upsert_algo_instance(self, instance: MiniQMTExecutionAlgoInstance) -> MiniQMTExecutionAlgoInstance:
        stored = super().upsert_algo_instance(instance)
        self._append_operation("upsert_algo_instance", stored.model_dump(mode="json"))
        self._after_incremental_write(runtime_id=stored.runtime_id, reason="upsert_algo_instance")
        return stored

    def upsert_child_order(self, order: MiniQMTChildOrder) -> MiniQMTChildOrder:
        stored = super().upsert_child_order(order)
        self._append_operation("upsert_child_order", stored.model_dump(mode="json"))
        self._after_incremental_write(runtime_id=stored.runtime_id, reason="upsert_child_order")
        return stored

    def reset_store_for_tmp_rebuild(self, *, reason: str = "manual_tmp_store_reset") -> dict[str, Any]:
        """Archive current tmp state and start a clean bounded runtime store."""

        archived = self._archive_existing_store(reason=reason)
        self._runtimes = {}
        self._events = {}
        self._evidence_receipts = {}
        self._algo_instances = {}
        self._child_orders = {}
        self._writes_since_compaction = 0
        self._write_snapshot(reason=reason)
        self._last_maintenance = {
            "schema_version": "miniqmt_runtime_store_maintenance_v1",
            "action": "reset_store_for_tmp_rebuild",
            "reason": reason,
            "archived": archived,
            "store_path": str(self._path),
            "oplog_path": str(self._oplog_path),
            "completed_at": datetime.now(UTC).isoformat(),
        }
        return dict(self._last_maintenance)

    def maintenance_status(self) -> dict[str, Any]:
        return {
            "schema_version": "miniqmt_runtime_store_maintenance_status_v1",
            "store_path": str(self._path),
            "oplog_path": str(self._oplog_path),
            "snapshot_exists": self._path.exists(),
            "oplog_exists": self._oplog_path.exists(),
            "snapshot_bytes": self._path.stat().st_size if self._path.exists() else 0,
            "oplog_bytes": self._oplog_path.stat().st_size if self._oplog_path.exists() else 0,
            "runtime_count": len(self._runtimes),
            "event_count": sum(len(events) for events in self._events.values()),
            "algo_instance_count": len(self._algo_instances),
            "child_order_count": len(self._child_orders),
            "writes_since_compaction": self._writes_since_compaction,
            "last_maintenance": self._last_maintenance,
            "write_mode": "incremental_jsonl_with_bounded_compaction",
        }

    def _append_operation(self, operation: str, item: dict[str, Any]) -> None:
        self._oplog_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "schema_version": self._OPLOG_SCHEMA_VERSION,
            "operation": operation,
            "item": item,
            "written_at": datetime.now(UTC).isoformat(),
        }
        with self._oplog_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            handle.write("\n")
        self._writes_since_compaction += 1

    def _after_incremental_write(self, *, runtime_id: str, reason: str) -> None:
        if self._prune_and_compact_if_needed(reason=reason, runtime_id=runtime_id):
            return
        compact_every = _positive_int_env(
            MINIQMT_EXECUTION_RUNTIME_COMPACT_EVERY_WRITES_ENV,
            DEFAULT_MINIQMT_EXECUTION_RUNTIME_COMPACT_EVERY_WRITES,
        )
        if self._writes_since_compaction >= compact_every:
            self._write_snapshot(reason=f"periodic_{reason}")

    def _write_snapshot(self, *, reason: str) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(
                self._snapshot_payload(reason=reason), ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ),
            encoding="utf-8",
        )
        if self._oplog_path.exists():
            self._oplog_path.unlink()
        self._writes_since_compaction = 0

    def _snapshot_payload(self, *, reason: str) -> dict[str, Any]:
        return {
            "schema_version": self._SNAPSHOT_SCHEMA_VERSION,
            "write_mode": "incremental_jsonl_with_bounded_compaction",
            "compacted_at": datetime.now(UTC).isoformat(),
            "compaction_reason": reason,
            "runtimes": [item.model_dump(mode="json") for item in self._runtimes.values()],
            "events": {
                runtime_id: [event.model_dump(mode="json") for event in events]
                for runtime_id, events in self._events.items()
            },
            "evidence_receipts": [
                {
                    "event": receipt.event.model_dump(mode="json"),
                    "persisted_at_utc": receipt.persisted_at_utc.isoformat(),
                    "durable_ack": receipt.durable_ack,
                    "readback_verified": receipt.readback_verified,
                }
                for receipt in self._evidence_receipts.values()
            ],
            "algo_instances": [item.model_dump(mode="json") for item in self._algo_instances.values()],
            "child_orders": [item.model_dump(mode="json") for item in self._child_orders.values()],
        }

    def _load(self) -> None:
        if self._path.exists():
            payload = json.loads(self._path.read_text(encoding="utf-8"))
            if payload.get("schema_version") != self._SNAPSHOT_SCHEMA_VERSION:
                raise ValueError("unsupported MiniQMT runtime repository schema")
            self._runtimes = {
                item["runtime_id"]: MiniQMTExecutionRuntimeRecord.model_validate(item)
                for item in payload.get("runtimes", [])
            }
            self._events = {
                str(runtime_id): [MiniQMTExecutionEvent.model_validate(item) for item in events]
                for runtime_id, events in (payload.get("events") or {}).items()
            }
            self._evidence_receipts = {}
            for item in payload.get("evidence_receipts", []):
                receipt = _durable_receipt_from_json(item)
                self._evidence_receipts[receipt.event.event_id] = receipt
            self._algo_instances = {
                item["algo_instance_id"]: MiniQMTExecutionAlgoInstance.model_validate(item)
                for item in payload.get("algo_instances", [])
            }
            self._child_orders = {
                item["child_order_id"]: MiniQMTChildOrder.model_validate(item)
                for item in payload.get("child_orders", [])
            }
        self._replay_oplog()

    def _replay_oplog(self) -> None:
        if not self._oplog_path.exists():
            return
        with self._oplog_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                record = json.loads(stripped)
                if record.get("schema_version") != self._OPLOG_SCHEMA_VERSION:
                    raise ValueError(f"unsupported MiniQMT runtime repository op schema at line {line_number}")
                self._apply_operation(str(record.get("operation") or ""), record.get("item") or {})

    def _apply_operation(self, operation: str, item: dict[str, Any]) -> None:
        if operation == "upsert_runtime":
            runtime = MiniQMTExecutionRuntimeRecord.model_validate(item)
            self._runtimes[runtime.runtime_id] = runtime
            return
        if operation == "append_event":
            event = MiniQMTExecutionEvent.model_validate(item)
            events = self._events.setdefault(event.runtime_id, [])
            if not any(existing.event_id == event.event_id for existing in events):
                events.append(event)
            runtime = self._runtimes.get(event.runtime_id)
            if runtime is not None and event.sequence > runtime.last_event_sequence:
                self._runtimes[event.runtime_id] = runtime.model_copy(
                    update={"last_event_sequence": event.sequence, "updated_at": event.event_time}
                )
            return
        if operation == "append_evidence_event":
            receipt = _durable_receipt_from_json(item)
            event = receipt.event
            events = self._events.setdefault(event.runtime_id, [])
            if not any(existing.event_id == event.event_id for existing in events):
                events.append(event)
            self._evidence_receipts[event.event_id] = receipt
            runtime = self._runtimes.get(event.runtime_id)
            if runtime is not None and event.sequence > runtime.last_event_sequence:
                self._runtimes[event.runtime_id] = runtime.model_copy(
                    update={"last_event_sequence": event.sequence, "updated_at": event.event_time}
                )
            return
        if operation == "upsert_algo_instance":
            instance = MiniQMTExecutionAlgoInstance.model_validate(item)
            self._algo_instances[instance.algo_instance_id] = instance
            return
        if operation == "upsert_child_order":
            order = MiniQMTChildOrder.model_validate(item)
            self._child_orders[order.child_order_id] = order
            return
        raise ValueError(f"unsupported MiniQMT runtime repository operation: {operation}")

    def _prune_and_compact_if_needed(self, *, reason: str, runtime_id: str | None = None) -> bool:
        pruned = {
            "events": self._prune_events(runtime_id=runtime_id),
            "algo_instances": self._prune_algo_instances(runtime_id=runtime_id),
            "child_orders": self._prune_child_orders(runtime_id=runtime_id),
        }
        pruned_total = sum(len(items) for items in pruned.values())
        if pruned_total <= 0:
            return False
        archive_path = self._archive_pruned_records(pruned=pruned, reason=reason)
        self._last_maintenance = {
            "schema_version": "miniqmt_runtime_store_maintenance_v1",
            "action": "prune_and_compact",
            "reason": reason,
            "runtime_id": runtime_id,
            "pruned_total": pruned_total,
            "pruned_counts": {key: len(value) for key, value in pruned.items()},
            "archive_path": str(archive_path) if archive_path is not None else None,
            "completed_at": datetime.now(UTC).isoformat(),
        }
        self._write_snapshot(reason=f"prune_{reason}")
        return True

    def _prune_events(self, *, runtime_id: str | None) -> list[dict[str, Any]]:
        max_events = _positive_int_env(
            MINIQMT_EXECUTION_RUNTIME_MAX_EVENTS_PER_RUNTIME_ENV,
            DEFAULT_MINIQMT_EXECUTION_RUNTIME_MAX_EVENTS_PER_RUNTIME,
        )
        retain_events = min(
            max_events,
            _positive_int_env(
                MINIQMT_EXECUTION_RUNTIME_RETAIN_EVENTS_PER_RUNTIME_ENV,
                DEFAULT_MINIQMT_EXECUTION_RUNTIME_RETAIN_EVENTS_PER_RUNTIME,
            ),
        )
        runtime_ids = [runtime_id] if runtime_id else list(self._events)
        pruned: list[dict[str, Any]] = []
        for current_runtime_id in runtime_ids:
            if current_runtime_id is None:
                continue
            events = self._events.get(current_runtime_id, [])
            if len(events) <= max_events:
                continue
            ordered = sorted(events, key=lambda item: item.sequence)
            dropped = ordered[: max(0, len(ordered) - retain_events)]
            self._events[current_runtime_id] = ordered[len(dropped) :]
            runtime = self._runtimes.get(current_runtime_id)
            trade_date = runtime.trade_date.isoformat() if runtime is not None else None
            pruned.extend(
                {
                    "kind": "event",
                    "runtime_id": current_runtime_id,
                    "trade_date": trade_date,
                    "item": item.model_dump(mode="json"),
                }
                for item in dropped
            )
        return pruned

    def _prune_algo_instances(self, *, runtime_id: str | None) -> list[dict[str, Any]]:
        max_items = _positive_int_env(
            MINIQMT_EXECUTION_RUNTIME_MAX_ALGO_INSTANCES_PER_RUNTIME_ENV,
            DEFAULT_MINIQMT_EXECUTION_RUNTIME_MAX_ALGO_INSTANCES_PER_RUNTIME,
        )
        retain_items = min(
            max_items,
            _positive_int_env(
                MINIQMT_EXECUTION_RUNTIME_RETAIN_ALGO_INSTANCES_PER_RUNTIME_ENV,
                DEFAULT_MINIQMT_EXECUTION_RUNTIME_RETAIN_ALGO_INSTANCES_PER_RUNTIME,
            ),
        )
        pruned: list[dict[str, Any]] = []
        for current_runtime_id in self._runtime_ids_for_prune(runtime_id=runtime_id, source="algo"):
            items = [item for item in self._algo_instances.values() if item.runtime_id == current_runtime_id]
            if len(items) <= max_items:
                continue
            active = [item for item in items if item.status == MiniQMTAlgoInstanceStatus.ACTIVE]
            terminal = sorted(
                (item for item in items if item.status != MiniQMTAlgoInstanceStatus.ACTIVE),
                key=lambda item: item.updated_at,
            )
            dropped = terminal[: max(0, len(terminal) - max(0, retain_items - len(active)))]
            for item in dropped:
                self._algo_instances.pop(item.algo_instance_id, None)
            runtime = self._runtimes.get(current_runtime_id)
            trade_date = runtime.trade_date.isoformat() if runtime is not None else None
            pruned.extend(
                {
                    "kind": "algo_instance",
                    "runtime_id": current_runtime_id,
                    "trade_date": trade_date,
                    "item": item.model_dump(mode="json"),
                }
                for item in dropped
            )
        return pruned

    def _prune_child_orders(self, *, runtime_id: str | None) -> list[dict[str, Any]]:
        max_items = _positive_int_env(
            MINIQMT_EXECUTION_RUNTIME_MAX_CHILD_ORDERS_PER_RUNTIME_ENV,
            DEFAULT_MINIQMT_EXECUTION_RUNTIME_MAX_CHILD_ORDERS_PER_RUNTIME,
        )
        retain_items = min(
            max_items,
            _positive_int_env(
                MINIQMT_EXECUTION_RUNTIME_RETAIN_CHILD_ORDERS_PER_RUNTIME_ENV,
                DEFAULT_MINIQMT_EXECUTION_RUNTIME_RETAIN_CHILD_ORDERS_PER_RUNTIME,
            ),
        )
        terminal_statuses = {
            MiniQMTChildOrderStatus.FILLED,
            MiniQMTChildOrderStatus.CANCELLED,
            MiniQMTChildOrderStatus.REJECTED,
        }
        pruned: list[dict[str, Any]] = []
        for current_runtime_id in self._runtime_ids_for_prune(runtime_id=runtime_id, source="child"):
            items = [item for item in self._child_orders.values() if item.runtime_id == current_runtime_id]
            if len(items) <= max_items:
                continue
            active = [item for item in items if item.status not in terminal_statuses]
            terminal = sorted(
                (item for item in items if item.status in terminal_statuses), key=lambda item: item.updated_at
            )
            dropped = terminal[: max(0, len(terminal) - max(0, retain_items - len(active)))]
            for item in dropped:
                self._child_orders.pop(item.child_order_id, None)
            runtime = self._runtimes.get(current_runtime_id)
            trade_date = runtime.trade_date.isoformat() if runtime is not None else None
            pruned.extend(
                {
                    "kind": "child_order",
                    "runtime_id": current_runtime_id,
                    "trade_date": trade_date,
                    "item": item.model_dump(mode="json"),
                }
                for item in dropped
            )
        return pruned

    def _runtime_ids_for_prune(self, *, runtime_id: str | None, source: str) -> list[str]:
        if runtime_id:
            return [runtime_id]
        if source == "algo":
            return sorted({item.runtime_id for item in self._algo_instances.values()})
        if source == "child":
            return sorted({item.runtime_id for item in self._child_orders.values()})
        return []

    def _archive_pruned_records(self, *, pruned: dict[str, list[dict[str, Any]]], reason: str) -> Path | None:
        records = [record for records in pruned.values() for record in records]
        if not records:
            return None
        self._archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = self._archive_dir / f"runtime-state-pruned-{_utc_stamp()}-{_safe_slug(reason)}.jsonl"
        with archive_path.open("a", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
                handle.write("\n")
        return archive_path

    def _reset_oversized_store_if_needed(self) -> None:
        max_bytes = _positive_int_env(
            MINIQMT_EXECUTION_RUNTIME_STORE_MAX_BYTES_ENV,
            DEFAULT_MINIQMT_EXECUTION_RUNTIME_STORE_MAX_BYTES,
        )
        snapshot_bytes = self._path.stat().st_size if self._path.exists() else 0
        oplog_bytes = self._oplog_path.stat().st_size if self._oplog_path.exists() else 0
        if snapshot_bytes + oplog_bytes <= max_bytes:
            return
        archived = self._archive_existing_store(reason="oversized_tmp_store_auto_reset")
        self._write_snapshot(reason="oversized_tmp_store_auto_reset")
        self._last_maintenance = {
            "schema_version": "miniqmt_runtime_store_maintenance_v1",
            "action": "oversized_tmp_store_auto_reset",
            "max_bytes": max_bytes,
            "snapshot_bytes": snapshot_bytes,
            "oplog_bytes": oplog_bytes,
            "archived": archived,
            "completed_at": datetime.now(UTC).isoformat(),
        }

    def _archive_existing_store(self, *, reason: str) -> dict[str, str]:
        archived: dict[str, str] = {}
        self._archive_dir.mkdir(parents=True, exist_ok=True)
        for label, source_path in (("snapshot", self._path), ("oplog", self._oplog_path)):
            if not source_path.exists():
                continue
            archive_path = self._archive_dir / f"{source_path.name}.{_utc_stamp()}.{_safe_slug(reason)}.bak"
            source_path.replace(archive_path)
            archived[label] = str(archive_path)
        return archived


def _positive_int_env(env_var: str, default_value: int) -> int:
    raw = str(os.getenv(env_var) or "").strip()
    if not raw:
        return int(default_value)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{env_var} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{env_var} must be positive")
    return value


def _supports_conn_factory_kw(factory: Any, parameter_name: str) -> bool:
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        return False
    return parameter_name in signature.parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()
    )


def _json(value: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(value if value is not None else {}, dumps=_json_dumps)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    return dict(row)


def _row_to_runtime(row: Any) -> MiniQMTExecutionRuntimeRecord:
    data = _row_dict(row)
    return MiniQMTExecutionRuntimeRecord(
        runtime_id=data["runtime_id"],
        account_group_id=data["account_group_id"],
        trade_date=data["trade_date"],
        mode=MiniQMTExecutionRuntimeMode(data["mode"]),
        event_loop_state=MiniQMTExecutionRuntimeState(data["event_loop_state"]),
        gateway_state=MiniQMTGatewayState(data["gateway_state"]),
        oms_state=MiniQMTOmsState(data["oms_state"]),
        runtime_config_hash=data["runtime_config_hash"],
        last_event_sequence=int(data.get("last_event_sequence") or 0),
        created_at=data["created_at"],
        updated_at=data["updated_at"],
        metadata=data.get("metadata") or {},
    )


def _row_to_event(row: Any) -> MiniQMTExecutionEvent:
    data = _row_dict(row)
    return MiniQMTExecutionEvent(
        event_id=data["event_id"],
        runtime_id=data["runtime_id"],
        sequence=int(data["sequence"]),
        event_type=MiniQMTExecutionEventType(data["event_type"]),
        event_time=data["event_time"],
        source=data["source"],
        payload=data.get("payload") or {},
    )


def _evidence_event_matches_candidate(event: MiniQMTExecutionEvent, candidate: QuoteEvidenceEventCandidate) -> bool:
    evidence = event.payload.get("evidence") if isinstance(event.payload, dict) else None
    health = event.payload.get("health_or_aggregate") if isinstance(event.payload, dict) else None
    content_hash = (
        str(health.get("health_sha256") or "")
        if candidate.event_type == MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH and isinstance(health, dict)
        else str(evidence.get("evidence_sha256") or "")
        if isinstance(evidence, dict)
        else ""
    )
    return (
        event.event_id == candidate.event_id
        and event.runtime_id == candidate.runtime_id
        and event.event_type == candidate.event_type
        and event.source == "quote_ingress"
        and event.event_time == candidate.event_time
        and event.payload == candidate.payload
        and content_hash == candidate.evidence_sha256
    )


def _evidence_link_tokens(evidence: dict[str, Any], *, event_id: str) -> set[str]:
    link_fields = (
        "evidence_id",
        "market_data_id",
        "anchor_market_data_id",
        "action_evidence_id",
        "child_receipt_evidence_id",
        "supersedes_evidence_id",
        "source_child_event_id",
        "anchor_trade_event_id",
        "action_id",
        "child_order_id",
        "broker_order_id",
        "trade_id",
        "mark_series_key",
    )
    return {event_id, *(str(evidence[field]) for field in link_fields if evidence.get(field))}


def _durable_receipt_from_json(item: dict[str, Any]) -> DurableEvidenceReceipt:
    event = MiniQMTExecutionEvent.model_validate(item.get("event"))
    persisted_at = datetime.fromisoformat(str(item.get("persisted_at_utc")))
    if persisted_at.tzinfo is None or persisted_at.utcoffset() is None:
        raise ValueError("JSON evidence receipt persisted_at_utc must be timezone-aware")
    durable_ack = item.get("durable_ack")
    readback_verified = item.get("readback_verified")
    if durable_ack is not True or readback_verified is not True:
        raise ValueError("JSON evidence receipt cannot replay an unverified durable success")
    return DurableEvidenceReceipt(
        event=event,
        persisted_at_utc=persisted_at.astimezone(UTC),
        durable_ack=True,
        readback_verified=True,
    )


def _quote_diagnostics_summary_from_events(events: list[MiniQMTExecutionEvent]) -> dict[str, Any]:
    per_symbol: dict[str, dict[str, Any]] = {}
    reason_counts: Counter[str] = Counter()
    last_reason: dict[str, Any] | None = None
    markout = {"terminal_due_count": 0, "captured_count": 0, "unavailable_count": 0}
    health: dict[str, Any] | None = None
    health_event: dict[str, Any] | None = None
    for event in sorted(events, key=lambda item: (item.sequence, item.event_id)):
        if event.event_type == MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH:
            payload = event.payload.get("health_or_aggregate")
            health = dict(payload) if isinstance(payload, dict) else None
            health_event = (
                {
                    "event_id": event.event_id,
                    "sequence": event.sequence,
                    "event_time": event.event_time,
                }
                if health is not None
                else None
            )
        evidence = event.payload.get("evidence")
        if not isinstance(evidence, dict):
            continue
        symbol = str(evidence.get("symbol") or "")
        if symbol:
            entry = per_symbol.setdefault(symbol, {"symbol": symbol, "capture_count": 0, "last_event_time": None})
            entry["capture_count"] += 1
            entry["last_event_time"] = event.event_time
        reason = evidence.get("quality_reason_code") or evidence.get("unavailable_reason")
        if reason:
            reason_counts[str(reason)] += 1
            last_reason = {
                "reason_code": str(reason),
                "stage": evidence.get("stage"),
                "event_id": event.event_id,
                "event_time": event.event_time,
            }
        if str(evidence.get("capture_type") or "").startswith("MARKOUT_"):
            markout["terminal_due_count"] += 1
            if evidence.get("mark_status") == "CAPTURED":
                markout["captured_count"] += 1
            elif evidence.get("mark_status") == "UNAVAILABLE":
                markout["unavailable_count"] += 1
    return {
        "per_symbol": list(per_symbol.values()),
        "reason_counts": dict(reason_counts),
        "last_reason": last_reason,
        "markout": markout,
        "health": health,
        "health_event": health_event,
    }


def _row_to_algo_instance(row: Any) -> MiniQMTExecutionAlgoInstance:
    data = _row_dict(row)
    return MiniQMTExecutionAlgoInstance(
        algo_instance_id=data["algo_instance_id"],
        runtime_id=data["runtime_id"],
        parent_intent_id=data["parent_intent_id"],
        strategy_slot_id=data["strategy_slot_id"],
        symbol=data["symbol"],
        side=OrderSide(data["side"]),
        target_quantity=int(data["target_quantity"]),
        remaining_quantity=int(data["remaining_quantity"]),
        algo_code=data["algo_code"],
        status=MiniQMTAlgoInstanceStatus(data["status"]),
        created_at=data["created_at"],
        updated_at=data["updated_at"],
        metadata=data.get("metadata") or {},
    )


def _row_to_child_order(row: Any) -> MiniQMTChildOrder:
    data = _row_dict(row)
    return MiniQMTChildOrder(
        child_order_id=data["child_order_id"],
        runtime_id=data["runtime_id"],
        algo_instance_id=data["algo_instance_id"],
        parent_intent_id=data["parent_intent_id"],
        strategy_slot_id=data["strategy_slot_id"],
        symbol=data["symbol"],
        side=OrderSide(data["side"]),
        quantity=int(data["quantity"]),
        price=float(data["price"]),
        price_type=int(data["price_type"]),
        status=MiniQMTChildOrderStatus(data["status"]),
        broker_order_id=data.get("broker_order_id"),
        submitted_at=data.get("submitted_at"),
        updated_at=data["updated_at"],
        metadata=data.get("metadata") or {},
    )


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def _safe_slug(value: str) -> str:
    slug = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in str(value or "reset").lower())
    return slug.strip("-")[:48] or "reset"


def default_miniqmt_execution_runtime_repository_path() -> Path:
    """Return the explicit test/offline JsonFile store path."""

    configured = os.getenv(MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV)
    if configured:
        return Path(configured)
    pytest_current_test = os.getenv("PYTEST_CURRENT_TEST")
    if pytest_current_test:
        # Keep the product default durable while preventing unit tests from
        # reusing another test's runtime/event store across repeated runs.
        test_id = hashlib.sha256(pytest_current_test.encode("utf-8")).hexdigest()[:16]
        return (
            _PROJECT_ROOT
            / "tmp"
            / "miniqmt_execution_runtime"
            / "pytest"
            / f"runtime-state-{os.getpid()}-{test_id}.json"
        )
    return DEFAULT_MINIQMT_EXECUTION_RUNTIME_STORE_PATH


def default_miniqmt_execution_runtime_repository() -> MiniQMTExecutionRuntimeRepository:
    """Build the default durable repository for product runtime clients."""

    requested = (
        str(os.getenv(MINIQMT_EXECUTION_RUNTIME_REPOSITORY_ENV) or DEFAULT_MINIQMT_EXECUTION_RUNTIME_REPOSITORY)
        .strip()
        .lower()
    )
    if requested in {"", "postgres", "pg", "db"}:
        return PostgresMiniQMTExecutionRuntimeRepository()
    if requested in {"json", "jsonfile", "file"}:
        if not _env_truthy(MINIQMT_EXECUTION_RUNTIME_JSONFILE_TEST_ONLY_ENV):
            raise RuntimeConfigInvalidError(
                "MiniQMT JsonFile runtime repository is retired for production",
                context={
                    "reason_code": "MINIQMT_RUNTIME_JSONFILE_REPOSITORY_TEST_ONLY",
                    "stage": "MINIQMT_RUNTIME_REPOSITORY_FACTORY",
                    "repository": requested,
                    "required_env_var": MINIQMT_EXECUTION_RUNTIME_JSONFILE_TEST_ONLY_ENV,
                    "production_default": DEFAULT_MINIQMT_EXECUTION_RUNTIME_REPOSITORY,
                    "jsonfile_production_fallback": False,
                },
            )
        return JsonFileMiniQMTExecutionRuntimeRepository(default_miniqmt_execution_runtime_repository_path())
    raise RuntimeConfigInvalidError(
        "unsupported MiniQMT runtime repository",
        context={
            "reason_code": "MINIQMT_RUNTIME_REPOSITORY_UNSUPPORTED",
            "stage": "MINIQMT_RUNTIME_REPOSITORY_FACTORY",
            "repository": requested,
            "supported": ["postgres", "jsonfile(test_only)"],
        },
    )


def _env_truthy(env_var: str) -> bool:
    raw = str(os.getenv(env_var) or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}
