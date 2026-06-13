"""Repository interfaces for the durable MiniQMT execution runtime."""

from __future__ import annotations

import json
import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from .models import (
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrder,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionAlgoInstance,
    MiniQMTExecutionEvent,
    MiniQMTExecutionRuntimeRecord,
    MiniQMTExecutionRuntimeState,
)


_PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MINIQMT_EXECUTION_RUNTIME_STORE_PATH = _PROJECT_ROOT / "tmp" / "miniqmt_execution_runtime" / "runtime-state.json"
MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV = "MINIQMT_EXECUTION_RUNTIME_STORE_PATH"


class MiniQMTExecutionRuntimeRepository(Protocol):
    def upsert_runtime(self, runtime: MiniQMTExecutionRuntimeRecord) -> MiniQMTExecutionRuntimeRecord:
        ...

    def get_runtime(self, runtime_id: str) -> MiniQMTExecutionRuntimeRecord | None:
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
    """Deterministic repository stand-in for Phase 2 L2/L3 validation."""

    def __init__(self) -> None:
        self._runtimes: dict[str, MiniQMTExecutionRuntimeRecord] = {}
        self._events: dict[str, list[MiniQMTExecutionEvent]] = {}
        self._algo_instances: dict[str, MiniQMTExecutionAlgoInstance] = {}
        self._child_orders: dict[str, MiniQMTChildOrder] = {}

    def upsert_runtime(self, runtime: MiniQMTExecutionRuntimeRecord) -> MiniQMTExecutionRuntimeRecord:
        stored = runtime.model_copy(update={"updated_at": datetime.now(UTC)})
        self._runtimes[stored.runtime_id] = stored
        return stored

    def get_runtime(self, runtime_id: str) -> MiniQMTExecutionRuntimeRecord | None:
        return self._runtimes.get(runtime_id)

    def append_event(self, event: MiniQMTExecutionEvent) -> MiniQMTExecutionEvent:
        existing = self._events.setdefault(event.runtime_id, [])
        expected_sequence = len(existing) + 1
        if event.sequence != expected_sequence:
            raise ValueError(
                f"event sequence must be monotonic for runtime {event.runtime_id}: "
                f"expected {expected_sequence}, got {event.sequence}"
            )
        existing.append(event)
        runtime = self._runtimes.get(event.runtime_id)
        if runtime is not None:
            self._runtimes[event.runtime_id] = runtime.model_copy(
                update={"last_event_sequence": event.sequence, "updated_at": datetime.now(UTC)}
            )
        return event

    def list_events(self, runtime_id: str) -> list[MiniQMTExecutionEvent]:
        return list(self._events.get(runtime_id, ()))

    def next_event_sequence(self, runtime_id: str) -> int:
        return len(self._events.get(runtime_id, ())) + 1

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


class JsonFileMiniQMTExecutionRuntimeRepository(InMemoryMiniQMTExecutionRuntimeRepository):
    """Append-state JSON repository used to prove restart recovery without DDL.

    Production Postgres/Timescale DDL is intentionally not introduced in Phase
    2.1. This repository provides durable semantics for fake-broker validation
    and keeps the same interface that a later DB repository will implement.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        super().__init__()
        self._load()

    def upsert_runtime(self, runtime: MiniQMTExecutionRuntimeRecord) -> MiniQMTExecutionRuntimeRecord:
        stored = super().upsert_runtime(runtime)
        self._save()
        return stored

    def append_event(self, event: MiniQMTExecutionEvent) -> MiniQMTExecutionEvent:
        stored = super().append_event(event)
        self._save()
        return stored

    def upsert_algo_instance(self, instance: MiniQMTExecutionAlgoInstance) -> MiniQMTExecutionAlgoInstance:
        stored = super().upsert_algo_instance(instance)
        self._save()
        return stored

    def upsert_child_order(self, order: MiniQMTChildOrder) -> MiniQMTChildOrder:
        stored = super().upsert_child_order(order)
        self._save()
        return stored

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": "miniqmt_execution_runtime_json_repository_v1",
            "runtimes": [item.model_dump(mode="json") for item in self._runtimes.values()],
            "events": {
                runtime_id: [event.model_dump(mode="json") for event in events]
                for runtime_id, events in self._events.items()
            },
            "algo_instances": [item.model_dump(mode="json") for item in self._algo_instances.values()],
            "child_orders": [item.model_dump(mode="json") for item in self._child_orders.values()],
        }
        self._path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")

    def _load(self) -> None:
        if not self._path.exists():
            return
        payload = json.loads(self._path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != "miniqmt_execution_runtime_json_repository_v1":
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


def default_miniqmt_execution_runtime_repository_path() -> Path:
    """Return the durable store used by unattended product MiniQMT paths."""

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

    return JsonFileMiniQMTExecutionRuntimeRepository(default_miniqmt_execution_runtime_repository_path())
