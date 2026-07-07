"""Repository interfaces for the durable MiniQMT execution runtime."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Protocol

import psycopg2
import psycopg2.extras

from backend.db.pg_pool import get_conn
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
DEFAULT_MINIQMT_EXECUTION_RUNTIME_STORE_PATH = _PROJECT_ROOT / "tmp" / "miniqmt_execution_runtime" / "runtime-state.json"
MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV = "MINIQMT_EXECUTION_RUNTIME_STORE_PATH"
MINIQMT_EXECUTION_RUNTIME_REPOSITORY_ENV = "MINIQMT_EXECUTION_RUNTIME_REPOSITORY"
MINIQMT_EXECUTION_RUNTIME_JSONFILE_TEST_ONLY_ENV = "AISTOCK_MINIQMT_RUNTIME_JSONFILE_TEST_ONLY"
MINIQMT_EXECUTION_RUNTIME_STORE_MAX_BYTES_ENV = "MINIQMT_EXECUTION_RUNTIME_STORE_MAX_BYTES"
MINIQMT_EXECUTION_RUNTIME_COMPACT_EVERY_WRITES_ENV = "MINIQMT_EXECUTION_RUNTIME_COMPACT_EVERY_WRITES"
MINIQMT_EXECUTION_RUNTIME_MAX_EVENTS_PER_RUNTIME_ENV = "MINIQMT_EXECUTION_RUNTIME_MAX_EVENTS_PER_RUNTIME"
MINIQMT_EXECUTION_RUNTIME_RETAIN_EVENTS_PER_RUNTIME_ENV = "MINIQMT_EXECUTION_RUNTIME_RETAIN_EVENTS_PER_RUNTIME"
MINIQMT_EXECUTION_RUNTIME_MAX_CHILD_ORDERS_PER_RUNTIME_ENV = "MINIQMT_EXECUTION_RUNTIME_MAX_CHILD_ORDERS_PER_RUNTIME"
MINIQMT_EXECUTION_RUNTIME_RETAIN_CHILD_ORDERS_PER_RUNTIME_ENV = "MINIQMT_EXECUTION_RUNTIME_RETAIN_CHILD_ORDERS_PER_RUNTIME"
MINIQMT_EXECUTION_RUNTIME_MAX_ALGO_INSTANCES_PER_RUNTIME_ENV = "MINIQMT_EXECUTION_RUNTIME_MAX_ALGO_INSTANCES_PER_RUNTIME"
MINIQMT_EXECUTION_RUNTIME_RETAIN_ALGO_INSTANCES_PER_RUNTIME_ENV = "MINIQMT_EXECUTION_RUNTIME_RETAIN_ALGO_INSTANCES_PER_RUNTIME"
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


class MiniQMTExecutionRuntimeRepository(Protocol):
    def upsert_runtime(self, runtime: MiniQMTExecutionRuntimeRecord) -> MiniQMTExecutionRuntimeRecord:
        ...

    def get_runtime(self, runtime_id: str) -> MiniQMTExecutionRuntimeRecord | None:
        ...

    def list_runtimes(self) -> list[MiniQMTExecutionRuntimeRecord]:
        ...

    def append_event(self, event: MiniQMTExecutionEvent) -> MiniQMTExecutionEvent:
        ...

    def list_events(self, runtime_id: str) -> list[MiniQMTExecutionEvent]:
        ...

    def next_event_sequence(self, runtime_id: str) -> int:
        ...

    def upsert_algo_instance(self, instance: MiniQMTExecutionAlgoInstance) -> MiniQMTExecutionAlgoInstance:
        ...

    def list_algo_instances(
        self,
        runtime_id: str,
        *,
        active_only: bool = False,
    ) -> list[MiniQMTExecutionAlgoInstance]:
        ...

    def upsert_child_order(self, order: MiniQMTChildOrder) -> MiniQMTChildOrder:
        ...

    def list_child_orders(
        self,
        runtime_id: str,
        *,
        active_only: bool = False,
    ) -> list[MiniQMTChildOrder]:
        ...


class InMemoryMiniQMTExecutionRuntimeRepository:
    """Deterministic in-memory repository for unit tests."""

    def __init__(self) -> None:
        self._runtimes: dict[str, MiniQMTExecutionRuntimeRecord] = {}
        self._events: dict[str, list[MiniQMTExecutionEvent]] = {}
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

    def list_events(self, runtime_id: str) -> list[MiniQMTExecutionEvent]:
        return list(self._events.get(runtime_id, ()))

    def next_event_sequence(self, runtime_id: str) -> int:
        runtime = self._runtimes.get(runtime_id)
        if runtime is not None and runtime.last_event_sequence:
            return int(runtime.last_event_sequence) + 1
        events = self._events.get(runtime_id, ())
        return (int(events[-1].sequence) if events else 0) + 1

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

    def list_events(self, runtime_id: str) -> list[MiniQMTExecutionEvent]:
        return self._with_runtime_db_error(
            "list_events",
            "MINIQMT_RUNTIME_DB_LIST_EVENTS_FAILED",
            {"runtime_id": runtime_id},
            lambda: self._list_event_rows(runtime_id),
        )

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

    def _list_event_rows(self, runtime_id: str) -> list[MiniQMTExecutionEvent]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.execution_runtime_event
                    WHERE runtime_id = %s AND archived_at IS NULL
                    ORDER BY sequence, event_time, event_id
                    """,
                    (runtime_id,),
                )
                rows = cur.fetchall()
        return [_row_to_event(row) for row in rows]

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
                    WHERE {' AND '.join(filters)}
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
                    WHERE {' AND '.join(filters)}
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
                    "algo_instances": self._archive_algo_instances_for_runtime(cur, runtime_id=runtime_id, reason=reason),
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
            WITH ranked AS (
                SELECT event_id,
                       row_number() OVER (ORDER BY sequence DESC, event_time DESC, event_id DESC) AS rn
                FROM qmt_strategy.execution_runtime_event
                WHERE runtime_id = %s AND archived_at IS NULL
            )
            UPDATE qmt_strategy.execution_runtime_event AS event
            SET archived_at = NOW(),
                archive_reason = %s
            FROM ranked
            WHERE event.event_id = ranked.event_id
              AND ranked.rn > %s
              AND (SELECT COUNT(*) FROM ranked) > %s
            """,
            (runtime_id, reason, retain_events, max_events),
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
        except psycopg2.Error as exc:
            raise RuntimeConfigInvalidError(
                "MiniQMT runtime Postgres repository operation failed loudly",
                context={
                    **context,
                    "reason_code": reason_code,
                    "stage": f"MINIQMT_RUNTIME_DB_{stage.upper()}",
                    "error_type": type(exc).__name__,
                    "pgcode": getattr(exc, "pgcode", None),
                    "message": str(exc),
                    "ddl_required": True,
                    "production_ddl_gate": "pending",
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
            json.dumps(self._snapshot_payload(reason=reason), ensure_ascii=False, sort_keys=True, separators=(",", ":")),
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
            self._events[current_runtime_id] = ordered[len(dropped):]
            runtime = self._runtimes.get(current_runtime_id)
            trade_date = runtime.trade_date.isoformat() if runtime is not None else None
            pruned.extend(
                {"kind": "event", "runtime_id": current_runtime_id, "trade_date": trade_date, "item": item.model_dump(mode="json")}
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
            terminal = sorted((item for item in items if item.status != MiniQMTAlgoInstanceStatus.ACTIVE), key=lambda item: item.updated_at)
            dropped = terminal[: max(0, len(terminal) - max(0, retain_items - len(active)))]
            for item in dropped:
                self._algo_instances.pop(item.algo_instance_id, None)
            runtime = self._runtimes.get(current_runtime_id)
            trade_date = runtime.trade_date.isoformat() if runtime is not None else None
            pruned.extend(
                {"kind": "algo_instance", "runtime_id": current_runtime_id, "trade_date": trade_date, "item": item.model_dump(mode="json")}
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
            terminal = sorted((item for item in items if item.status in terminal_statuses), key=lambda item: item.updated_at)
            dropped = terminal[: max(0, len(terminal) - max(0, retain_items - len(active)))]
            for item in dropped:
                self._child_orders.pop(item.child_order_id, None)
            runtime = self._runtimes.get(current_runtime_id)
            trade_date = runtime.trade_date.isoformat() if runtime is not None else None
            pruned.extend(
                {"kind": "child_order", "runtime_id": current_runtime_id, "trade_date": trade_date, "item": item.model_dump(mode="json")}
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
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
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

    requested = str(
        os.getenv(MINIQMT_EXECUTION_RUNTIME_REPOSITORY_ENV)
        or DEFAULT_MINIQMT_EXECUTION_RUNTIME_REPOSITORY
    ).strip().lower()
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
