from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Awaitable, Callable, Mapping, Sequence

from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn


logger = logging.getLogger("aistock.qe_reconciliation")


class QEReconciliationScope(StrEnum):
    RESERVATION = "reservation"
    EXPERIMENT = "experiment"
    EVOLUTION = "evolution"
    LONG_TREND = "long_trend"
    RESOURCE_SESSION = "resource_session"


_TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "y", "on"})
_MIN_SCAN_INTERVAL_SECONDS = 60.0


def _env_disabled(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in _TRUE_ENV_VALUES


def _env_scan_interval(name: str, default: int) -> float:
    raw = str(os.getenv(name) or str(default)).strip() or str(default)
    return max(_MIN_SCAN_INTERVAL_SECONDS, float(raw))


@dataclass(frozen=True)
class QEReconciliationConfig:
    """Runtime configuration preserving the pre-coordinator environment API."""

    enabled_scopes: frozenset[QEReconciliationScope]
    family_intervals: Mapping[QEReconciliationScope, float]
    experiment_batch_size: int

    @classmethod
    def from_environment(cls) -> "QEReconciliationConfig":
        enabled = set(QEReconciliationScope)
        if _env_disabled("DISABLE_EVOLUTION_SCANNER"):
            enabled.discard(QEReconciliationScope.EVOLUTION)
        if _env_disabled("DISABLE_QE_EXPERIMENT_SCANNER"):
            enabled.discard(QEReconciliationScope.EXPERIMENT)
        intervals = {
            QEReconciliationScope.RESERVATION: _env_scan_interval(
                "QE_EXECUTION_RESERVATION_SCAN_INTERVAL_SEC", 15
            ),
            QEReconciliationScope.EXPERIMENT: _env_scan_interval(
                "QE_EXPERIMENT_SCAN_INTERVAL_SEC", 30
            ),
            QEReconciliationScope.EVOLUTION: _env_scan_interval(
                "QE_EVOLUTION_SCAN_INTERVAL_SEC", 60
            ),
            QEReconciliationScope.LONG_TREND: _MIN_SCAN_INTERVAL_SECONDS,
            QEReconciliationScope.RESOURCE_SESSION: _MIN_SCAN_INTERVAL_SECONDS,
        }
        batch_raw = str(os.getenv("QE_EXPERIMENT_SCAN_BATCH_SIZE") or "50").strip() or "50"
        return cls(
            enabled_scopes=frozenset(enabled),
            family_intervals=intervals,
            experiment_batch_size=max(1, int(batch_raw)),
        )


@dataclass(frozen=True)
class QEReconciliationDue:
    reservation: bool = False
    experiment: bool = False
    evolution: bool = False
    long_trend: bool = False
    terminal_resource_session: bool = False
    resource_states: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    loop_states: Mapping[str, str] = field(default_factory=dict)

    @property
    def any_due(self) -> bool:
        return any(
            (
                self.reservation,
                self.experiment,
                self.evolution,
                self.long_trend,
                self.terminal_resource_session,
            )
        )


class QEReconciliationWakeBus:
    """Thread-safe process-local wake and waiter registry.

    PostgreSQL remains the durable cross-process authority.  Notifications are
    only latency hints; the coordinator's 60-second aggregate sweep provides
    restart/cross-process recovery.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending_work: dict[QEReconciliationScope, dict[str, bool]] = {}
        self._pending_order: OrderedDict[
            tuple[QEReconciliationScope, str], None
        ] = OrderedDict()
        self._snapshot_requests: dict[QEReconciliationScope, set[str]] = {}
        self._coordinator_subscribers: set[
            tuple[asyncio.AbstractEventLoop, asyncio.Event]
        ] = set()
        self._waiters: dict[
            tuple[QEReconciliationScope, str],
            dict[tuple[asyncio.AbstractEventLoop, asyncio.Event], int],
        ] = {}
        self._state_values: OrderedDict[
            tuple[QEReconciliationScope, str], object
        ] = OrderedDict()
        self._state_generations: dict[tuple[QEReconciliationScope, str], int] = {}
        self._generation_clock = -1

    _MISSING = object()
    _STATE_CACHE_LIMIT = 10_000
    _PENDING_WORK_LIMIT = 10_000

    def register_coordinator(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        event: asyncio.Event,
    ) -> None:
        with self._lock:
            self._coordinator_subscribers.add((loop, event))

    def unregister_coordinator(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        event: asyncio.Event,
    ) -> None:
        with self._lock:
            self._coordinator_subscribers.discard((loop, event))

    def notify(
        self,
        scope: QEReconciliationScope | str,
        *,
        key: str | None = None,
        force: bool = False,
    ) -> None:
        """Coalesce business work and wake the single coordinator.

        ``force`` is reserved for a real completion callback whose new durable
        revision must be observed immediately.  Ordinary repeated hints for the
        same key remain subject to the coordinator's one-minute remote throttle.
        """
        normalized = QEReconciliationScope(str(scope))
        normalized_key = str(key or "").strip()
        with self._lock:
            work = self._pending_work.setdefault(normalized, {})
            work[normalized_key] = bool(work.get(normalized_key)) or bool(force)
            pending_key = (normalized, normalized_key)
            self._pending_order[pending_key] = None
            self._pending_order.move_to_end(pending_key)
            while len(self._pending_order) > self._PENDING_WORK_LIMIT:
                evicted, _ = self._pending_order.popitem(last=False)
                evicted_scope, evicted_key = evicted
                evicted_work = self._pending_work.get(evicted_scope)
                if evicted_work is not None:
                    evicted_work.pop(evicted_key, None)
                    if not evicted_work:
                        self._pending_work.pop(evicted_scope, None)
            subscribers = tuple(self._coordinator_subscribers)
        self._schedule(subscribers)

    def wake_coordinators(self) -> None:
        """Wake coordinators without manufacturing external business work."""
        with self._lock:
            subscribers = tuple(self._coordinator_subscribers)
        self._schedule(subscribers)

    def request_state_snapshot(
        self,
        scope: QEReconciliationScope | str,
        *,
        key: str,
    ) -> None:
        """Request one batched read without scheduling any business scanner."""
        normalized = QEReconciliationScope(str(scope))
        normalized_key = str(key or "").strip()
        if not normalized_key:
            raise ValueError("QE reconciliation snapshot key is required")
        if normalized not in {
            QEReconciliationScope.RESOURCE_SESSION,
            QEReconciliationScope.EVOLUTION,
        }:
            raise ValueError(f"QE reconciliation scope has no state snapshot: {normalized}")
        with self._lock:
            self._snapshot_requests.setdefault(normalized, set()).add(normalized_key)
            subscribers = tuple(self._coordinator_subscribers)
        self._schedule(subscribers)

    def drain_pending_work(
        self,
    ) -> dict[QEReconciliationScope, dict[str, bool]]:
        with self._lock:
            pending = {
                scope: dict(sorted(work.items()))
                for scope, work in self._pending_work.items()
            }
            self._pending_work.clear()
            self._pending_order.clear()
        return pending

    def drain_snapshot_requests(
        self,
    ) -> dict[QEReconciliationScope, tuple[str, ...]]:
        with self._lock:
            pending = {
                scope: tuple(sorted(keys))
                for scope, keys in self._snapshot_requests.items()
            }
            self._snapshot_requests.clear()
        return pending

    def active_waiter_keys(
        self,
        scope: QEReconciliationScope,
    ) -> tuple[str, ...]:
        with self._lock:
            return tuple(
                sorted(
                    key
                    for waiter_scope, key in self._waiters
                    if waiter_scope == scope and key
                )
            )

    async def wait(
        self,
        scope: QEReconciliationScope | str,
        *,
        key: str,
        timeout_seconds: float = 60.0,
        observed_generation: int | None = None,
    ) -> int:
        normalized = QEReconciliationScope(str(scope))
        normalized_key = str(key or "").strip()
        if not normalized_key:
            raise ValueError("QE reconciliation wait key is required")
        loop = asyncio.get_running_loop()
        event = asyncio.Event()
        subscriber = (loop, event)
        waiter_key = (normalized, normalized_key)
        with self._lock:
            current_generation = self._state_generations.get(waiter_key, -1)
            observed = -1 if observed_generation is None else int(observed_generation)
            if current_generation > observed:
                return current_generation
            self._waiters.setdefault(waiter_key, {})[subscriber] = observed
            snapshot_missing = waiter_key not in self._state_values
        # A first registration asks only for a batched state snapshot.  It is
        # deliberately not business work and cannot run a remote scanner.
        if snapshot_missing:
            self.request_state_snapshot(normalized, key=normalized_key)
        try:
            await asyncio.wait_for(event.wait(), timeout=max(60.0, timeout_seconds))
        except asyncio.TimeoutError:
            pass
        finally:
            with self._lock:
                subscribers = self._waiters.get(waiter_key)
                if subscribers is not None:
                    subscribers.pop(subscriber, None)
                    if not subscribers:
                        self._waiters.pop(waiter_key, None)
        with self._lock:
            return self._state_generations.get(waiter_key, observed)

    def publish_states(
        self,
        due: QEReconciliationDue,
        *,
        resource_session_ids: Sequence[str] = (),
        loop_ids: Sequence[str] = (),
    ) -> None:
        changed_subscribers: list[
            tuple[asyncio.AbstractEventLoop, asyncio.Event]
        ] = []
        with self._lock:
            for scope, keys, values in (
                (
                    QEReconciliationScope.RESOURCE_SESSION,
                    resource_session_ids,
                    due.resource_states,
                ),
                (QEReconciliationScope.EVOLUTION, loop_ids, due.loop_states),
            ):
                for key in keys:
                    normalized_key = str(key)
                    waiter_key = (scope, normalized_key)
                    if normalized_key in values:
                        raw_value = values[normalized_key]
                        value: object = (
                            dict(raw_value)
                            if scope == QEReconciliationScope.RESOURCE_SESSION
                            else str(raw_value)
                        )
                    else:
                        value = self._MISSING
                    previous_exists = waiter_key in self._state_values
                    previous = self._state_values.get(waiter_key)
                    if previous_exists and previous == value:
                        continue
                    self._generation_clock += 1
                    generation = self._generation_clock
                    self._state_values[waiter_key] = value
                    self._state_values.move_to_end(waiter_key)
                    self._state_generations[waiter_key] = generation
                    for subscriber, observed in self._waiters.get(
                        waiter_key, {}
                    ).items():
                        if generation > observed:
                            changed_subscribers.append(subscriber)
                    if len(self._state_values) > self._STATE_CACHE_LIMIT:
                        for evicted_key in tuple(self._state_values):
                            if len(self._state_values) <= self._STATE_CACHE_LIMIT:
                                break
                            if evicted_key in self._waiters:
                                continue
                            self._state_values.pop(evicted_key, None)
                            self._state_generations.pop(evicted_key, None)
        self._schedule(tuple(changed_subscribers))

    def resource_state(self, session_id: str) -> Mapping[str, Any] | None:
        with self._lock:
            state = self._state_values.get(
                (QEReconciliationScope.RESOURCE_SESSION, str(session_id)),
                self._MISSING,
            )
            return dict(state) if isinstance(state, Mapping) else None

    def loop_state(self, loop_id: str) -> str | None:
        with self._lock:
            state = self._state_values.get(
                (QEReconciliationScope.EVOLUTION, str(loop_id)),
                self._MISSING,
            )
            return str(state) if state is not self._MISSING else None

    def state_generation(
        self,
        scope: QEReconciliationScope | str,
        *,
        key: str,
    ) -> int:
        normalized = QEReconciliationScope(str(scope))
        with self._lock:
            return self._state_generations.get((normalized, str(key)), -1)

    def _schedule(
        self,
        subscribers: Sequence[tuple[asyncio.AbstractEventLoop, asyncio.Event]],
    ) -> None:
        for loop, event in subscribers:
            if loop.is_closed():
                continue
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                continue


qe_reconciliation_wakeup = QEReconciliationWakeBus()


def notify_qe_reconciliation(
    scope: QEReconciliationScope | str,
    *,
    key: str | None = None,
    force: bool = False,
) -> None:
    qe_reconciliation_wakeup.notify(scope, key=key, force=force)


async def wait_for_qe_reconciliation(
    scope: QEReconciliationScope | str,
    *,
    key: str,
    timeout_seconds: float = 60.0,
    observed_generation: int | None = None,
) -> int:
    return await qe_reconciliation_wakeup.wait(
        scope,
        key=key,
        timeout_seconds=timeout_seconds,
        observed_generation=observed_generation,
    )


DueProbe = Callable[..., QEReconciliationDue]
AsyncScanner = Callable[[], Awaitable[Any]]


class QEReconciliationCoordinator:
    _THROTTLE_CACHE_LIMIT = 10_000

    def __init__(
        self,
        *,
        safety_sweep_seconds: float = 60.0,
        config: QEReconciliationConfig | None = None,
        due_probe: DueProbe | None = None,
        reservation_scanner: AsyncScanner | None = None,
        experiment_scanner: AsyncScanner | None = None,
        evolution_scanner: AsyncScanner | None = None,
        long_trend_scanner: AsyncScanner | None = None,
        resource_scanner: Callable[[], Any] | None = None,
        wake_bus: QEReconciliationWakeBus = qe_reconciliation_wakeup,
    ) -> None:
        self._safety_sweep_seconds = max(60.0, float(safety_sweep_seconds))
        self._config = config or QEReconciliationConfig.from_environment()
        self._due_probe = due_probe or self._load_due_work
        self._reservation_scanner = reservation_scanner
        self._experiment_scanner = experiment_scanner
        self._evolution_scanner = evolution_scanner
        self._long_trend_scanner = long_trend_scanner
        self._resource_scanner = resource_scanner
        self._wake_bus = wake_bus
        self._initialized = False
        self._last_business_scan_at: OrderedDict[
            tuple[QEReconciliationScope, str], float
        ] = OrderedDict()
        self._last_forced_scan_at: OrderedDict[
            tuple[QEReconciliationScope, str], float
        ] = OrderedDict()
        self._last_family_scan_at: dict[QEReconciliationScope, float] = {}
        self._last_safety_probe_at: float | None = None
        self._inflight: dict[QEReconciliationScope, asyncio.Task[None]] = {}
        self._inflight_dirty: set[QEReconciliationScope] = set()
        self._replay_ready: set[QEReconciliationScope] = set()
        self._shutting_down = False

    @property
    def config(self) -> QEReconciliationConfig:
        return self._config

    def _scope_enabled(self, scope: QEReconciliationScope) -> bool:
        return scope in self._config.enabled_scopes

    def _family_interval(self, scope: QEReconciliationScope) -> float:
        return max(
            _MIN_SCAN_INTERVAL_SECONDS,
            float(self._config.family_intervals.get(scope, _MIN_SCAN_INTERVAL_SECONDS)),
        )

    def _initialize_default_scanners(self) -> None:
        if self._initialized:
            return
        if self._scope_enabled(QEReconciliationScope.RESERVATION) and self._reservation_scanner is None:
            from .qe_active_execution_capacity import QEExecutionReservationReconciler

            reconciler = QEExecutionReservationReconciler()
            self._reservation_scanner = reconciler.scan_once
        if self._scope_enabled(QEReconciliationScope.EXPERIMENT) and self._experiment_scanner is None:
            from .qe_experiment_status_scanner import QEExperimentStatusScanner

            scanner = QEExperimentStatusScanner(
                batch_size=self._config.experiment_batch_size
            )
            self._experiment_scanner = scanner.scan_once
        if self._scope_enabled(QEReconciliationScope.EVOLUTION) and self._evolution_scanner is None:
            from .qe_evolution_service import AutoEvolutionScheduler

            scheduler = AutoEvolutionScheduler()
            self._evolution_scanner = scheduler.scan_running_loops
        if self._scope_enabled(QEReconciliationScope.LONG_TREND) and self._long_trend_scanner is None:
            from backend.services.qe_archive.long_trend_repository import (
                QELongTrendEvaluationResultRepository,
            )

            from .long_trend_evaluation_phase2 import QELongTrendPhase2Service

            service = QELongTrendPhase2Service(
                result_repository=QELongTrendEvaluationResultRepository()
            )
            self._long_trend_scanner = lambda: service.reconcile_nonterminal(limit=100)
        if self._scope_enabled(QEReconciliationScope.RESOURCE_SESSION) and self._resource_scanner is None:
            from .qe_resource_phase_service import QEResourcePhaseService

            resource_service = QEResourcePhaseService()
            self._resource_scanner = resource_service.reconcile_terminal_sessions
        self._initialized = True

    async def run(self, stop_event: asyncio.Event) -> None:
        self._initialize_default_scanners()
        self._shutting_down = False
        loop = asyncio.get_running_loop()
        wake_event = asyncio.Event()
        self._wake_bus.register_coordinator(loop=loop, event=wake_event)
        next_safety_sweep_at = time.monotonic()
        try:
            while not stop_event.is_set():
                timeout = max(0.0, next_safety_sweep_at - time.monotonic())
                wake_task = asyncio.create_task(wake_event.wait())
                stop_task = asyncio.create_task(stop_event.wait())
                tasks = {wake_task, stop_task}
                try:
                    await asyncio.wait(
                        tasks,
                        timeout=timeout,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                if stop_event.is_set():
                    break
                now = time.monotonic()
                is_safety_sweep = now >= next_safety_sweep_at
                if is_safety_sweep:
                    next_safety_sweep_at = now + self._safety_sweep_seconds
                wake_event.clear()
                pending = self._wake_bus.drain_pending_work()
                snapshot_requests = self._wake_bus.drain_snapshot_requests()
                replay_scopes = frozenset(self._replay_ready)
                self._replay_ready.difference_update(replay_scopes)
                try:
                    await self.reconcile_once(
                        pending=pending,
                        snapshot_requests=snapshot_requests,
                        is_safety_sweep=is_safety_sweep,
                        dispatch_background=True,
                        replay_scopes=replay_scopes,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("QE reconciliation coordinator cycle failed")
        finally:
            self._shutting_down = True
            self._wake_bus.unregister_coordinator(loop=loop, event=wake_event)
            inflight = tuple(self._inflight.values())
            self._inflight.clear()
            for task in inflight:
                if not task.done():
                    task.cancel()
            if inflight:
                await asyncio.gather(*inflight, return_exceptions=True)

    async def reconcile_once(
        self,
        *,
        pending: Mapping[QEReconciliationScope, Mapping[str, bool]] | None = None,
        snapshot_requests: Mapping[QEReconciliationScope, Sequence[str]] | None = None,
        is_safety_sweep: bool = False,
        dispatch_background: bool = False,
        replay_scopes: frozenset[QEReconciliationScope] = frozenset(),
    ) -> QEReconciliationDue:
        """Execute one deterministic aggregate-read reconciliation cycle."""
        now = time.monotonic()
        pending = self._filter_pending_before_probe(
            pending or {},
            now=now,
            replay_scopes=replay_scopes,
        )
        snapshot_requests = snapshot_requests or {}
        has_snapshots = any(snapshot_requests.values())
        has_replay = any(
            self._scope_enabled(scope) for scope in replay_scopes
        )
        safety_probe_due = (
            self._last_safety_probe_at is None
            or now - self._last_safety_probe_at >= self._safety_sweep_seconds
        )
        force_probe_due = any(
            force for work in pending.values() for force in work.values()
        ) or has_replay
        if not (
            has_snapshots
            or force_probe_due
            or (is_safety_sweep and safety_probe_due)
            or bool(pending)
        ):
            return QEReconciliationDue()
        resource_ids = tuple(
            sorted(
                set(
                    self._wake_bus.active_waiter_keys(
                        QEReconciliationScope.RESOURCE_SESSION
                    )
                )
                | set(snapshot_requests.get(QEReconciliationScope.RESOURCE_SESSION, ()))
                | set(pending.get(QEReconciliationScope.RESOURCE_SESSION, {}))
            )
        )
        loop_ids = tuple(
            sorted(
                set(
                    self._wake_bus.active_waiter_keys(QEReconciliationScope.EVOLUTION)
                )
                | set(snapshot_requests.get(QEReconciliationScope.EVOLUTION, ()))
                | set(pending.get(QEReconciliationScope.EVOLUTION, {}))
            )
        )
        due = await self._run_blocking_shutdown_safe(
            self._due_probe,
            resource_session_ids=resource_ids,
            loop_ids=loop_ids,
        )
        completed_at = time.monotonic()
        if is_safety_sweep and safety_probe_due:
            self._last_safety_probe_at = completed_at
        self._wake_bus.publish_states(
            due,
            resource_session_ids=resource_ids,
            loop_ids=loop_ids,
        )
        await self._run_due_scanners(
            due,
            pending=pending,
            is_safety_sweep=is_safety_sweep,
            dispatch_background=dispatch_background,
            replay_scopes=replay_scopes,
        )
        return due

    async def _run_blocking_shutdown_safe(
        self,
        call: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Wait for a worker thread before propagating task cancellation.

        ``asyncio.to_thread`` cancellation does not stop the underlying thread.
        Waiting here prevents lifespan shutdown from closing the PostgreSQL pool
        while an aggregate probe or synchronous scanner is still using it.
        """

        task = asyncio.create_task(asyncio.to_thread(call, *args, **kwargs))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await task
            except Exception:
                logger.exception("QE blocking reconciliation failed during shutdown")
            raise

    def _remember_throttle(
        self,
        cache: OrderedDict[tuple[QEReconciliationScope, str], float],
        key: tuple[QEReconciliationScope, str],
        value: float,
    ) -> None:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > self._THROTTLE_CACHE_LIMIT:
            cache.popitem(last=False)

    def _filter_pending_before_probe(
        self,
        pending: Mapping[QEReconciliationScope, Mapping[str, bool]],
        *,
        now: float,
        replay_scopes: frozenset[QEReconciliationScope],
    ) -> dict[QEReconciliationScope, dict[str, bool]]:
        """Drop duplicate wakes before they can execute the aggregate SELECT."""

        filtered: dict[QEReconciliationScope, dict[str, bool]] = {}
        for raw_scope, work in pending.items():
            scope = QEReconciliationScope(str(raw_scope))
            if not self._scope_enabled(scope) or not work:
                continue
            allowed: dict[str, bool] = {}
            for raw_key, force in work.items():
                key = str(raw_key or "*")
                throttle_key = (scope, key)
                previous = self._last_business_scan_at.get(throttle_key)
                forced_previous = self._last_forced_scan_at.get(throttle_key)
                if force:
                    if (
                        forced_previous is not None
                        and now - forced_previous < self._safety_sweep_seconds
                    ):
                        continue
                    self._remember_throttle(
                        self._last_forced_scan_at,
                        throttle_key,
                        now,
                    )
                elif (
                    previous is not None
                    and now - previous < self._family_interval(scope)
                ):
                    continue
                self._remember_throttle(
                    self._last_business_scan_at,
                    throttle_key,
                    now,
                )
                allowed[key] = bool(force)
            if allowed:
                filtered[scope] = allowed
        for scope in replay_scopes:
            if self._scope_enabled(scope):
                filtered.setdefault(scope, {})["__inflight_replay__"] = True
        return filtered

    async def _run_due_scanners(
        self,
        due: QEReconciliationDue,
        *,
        pending: Mapping[QEReconciliationScope, Mapping[str, bool]],
        is_safety_sweep: bool,
        dispatch_background: bool,
        replay_scopes: frozenset[QEReconciliationScope],
    ) -> None:
        if self._scanner_is_due(
            QEReconciliationScope.RESERVATION,
            due=due.reservation,
            pending=pending,
            is_safety_sweep=is_safety_sweep,
            replay_scopes=replay_scopes,
        ) and self._reservation_scanner is not None:
            await self._dispatch_async_scanner(
                QEReconciliationScope.RESERVATION,
                "reservation",
                self._reservation_scanner,
                background=dispatch_background,
            )
        if self._scanner_is_due(
            QEReconciliationScope.EXPERIMENT,
            due=due.experiment,
            pending=pending,
            is_safety_sweep=is_safety_sweep,
            replay_scopes=replay_scopes,
        ) and self._experiment_scanner is not None:
            await self._dispatch_async_scanner(
                QEReconciliationScope.EXPERIMENT,
                "experiment",
                self._experiment_scanner,
                background=dispatch_background,
            )
        if self._scanner_is_due(
            QEReconciliationScope.EVOLUTION,
            due=due.evolution,
            pending=pending,
            is_safety_sweep=is_safety_sweep,
            replay_scopes=replay_scopes,
        ) and self._evolution_scanner is not None:
            await self._dispatch_async_scanner(
                QEReconciliationScope.EVOLUTION,
                "evolution",
                self._evolution_scanner,
                background=dispatch_background,
            )
        if self._scanner_is_due(
            QEReconciliationScope.LONG_TREND,
            due=due.long_trend,
            pending=pending,
            is_safety_sweep=is_safety_sweep,
            replay_scopes=replay_scopes,
        ) and self._long_trend_scanner is not None:
            await self._dispatch_async_scanner(
                QEReconciliationScope.LONG_TREND,
                "long_trend",
                self._long_trend_scanner,
                background=dispatch_background,
            )
        if self._scanner_is_due(
            QEReconciliationScope.RESOURCE_SESSION,
            due=due.terminal_resource_session,
            pending=pending,
            is_safety_sweep=is_safety_sweep,
            replay_scopes=replay_scopes,
        ) and self._resource_scanner is not None:
            async def resource_scanner() -> None:
                try:
                    await self._run_blocking_shutdown_safe(self._resource_scanner)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("QE resource-session reconciliation failed")

            await self._dispatch_task(
                QEReconciliationScope.RESOURCE_SESSION,
                resource_scanner(),
                background=dispatch_background,
            )

    async def _dispatch_async_scanner(
        self,
        scope: QEReconciliationScope,
        name: str,
        scanner: AsyncScanner,
        *,
        background: bool,
    ) -> None:
        await self._dispatch_task(
            scope,
            self._invoke_async_scanner(name, scanner),
            background=background,
        )

    async def _dispatch_task(
        self,
        scope: QEReconciliationScope,
        coroutine: Awaitable[None],
        *,
        background: bool,
    ) -> None:
        if not background:
            await coroutine
            return
        existing = self._inflight.get(scope)
        if existing is not None and not existing.done():
            coroutine.close()  # type: ignore[attr-defined]
            self._inflight_dirty.add(scope)
            return
        task = asyncio.create_task(coroutine, name=f"qe-reconcile-{scope.value}")
        self._inflight[scope] = task

        def clear(done: asyncio.Task[None]) -> None:
            if self._inflight.get(scope) is done:
                self._inflight.pop(scope, None)
            if scope in self._inflight_dirty:
                self._inflight_dirty.discard(scope)
                if not self._shutting_down:
                    self._replay_ready.add(scope)
                    self._wake_bus.wake_coordinators()

        task.add_done_callback(clear)

    @staticmethod
    async def _invoke_async_scanner(name: str, scanner: AsyncScanner) -> None:
        try:
            await scanner()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("QE %s reconciliation failed", name)

    def _scanner_is_due(
        self,
        scope: QEReconciliationScope,
        *,
        due: bool,
        pending: Mapping[QEReconciliationScope, Mapping[str, bool]],
        is_safety_sweep: bool,
        replay_scopes: frozenset[QEReconciliationScope],
    ) -> bool:
        if not self._scope_enabled(scope) or not due:
            return False
        if not is_safety_sweep and scope not in pending and scope not in replay_scopes:
            return False
        now = time.monotonic()
        event_driven = scope in replay_scopes or scope in pending
        previous = self._last_family_scan_at.get(scope)
        if (
            not event_driven
            and previous is not None
            and now - previous < self._family_interval(scope)
        ):
            return False
        self._last_family_scan_at[scope] = now
        return True

    def _load_due_work(
        self,
        *,
        resource_session_ids: Sequence[str] = (),
        loop_ids: Sequence[str] = (),
    ) -> QEReconciliationDue:
        reservation_due = (
            """EXISTS (
                SELECT 1 FROM infra.qe_execution_reservation
                WHERE status IN ('reserved', 'submitting', 'running', 'reconciling')
            )"""
            if self._scope_enabled(QEReconciliationScope.RESERVATION)
            else "FALSE"
        )
        experiment_due = (
            """EXISTS (
                SELECT 1 FROM qe_experiments e
                WHERE e.status = 'running'
                   OR (
                       e.status = 'pending'
                       AND e.qe_task_id IS NOT NULL
                       AND e.qe_loop_id IS NOT NULL
                   )
            )"""
            if self._scope_enabled(QEReconciliationScope.EXPERIMENT)
            else "FALSE"
        )
        evolution_due = (
            """EXISTS (
                SELECT 1 FROM qe_evolution_loops l
                JOIN qe_evolution_tasks t ON t.task_id = l.task_id
                WHERE t.status = 'running'
                  AND l.status IN ('pending', 'running', 'processing')
            )"""
            if self._scope_enabled(QEReconciliationScope.EVOLUTION)
            else "FALSE"
        )
        long_trend_due = (
            """EXISTS (
                SELECT 1 FROM qe_archive.run_evaluation
                WHERE status NOT IN ('succeeded', 'partial', 'failed', 'cancelled')
                  AND (owner_id IS NULL OR lease_expires_at < clock_timestamp())
            )"""
            if self._scope_enabled(QEReconciliationScope.LONG_TREND)
            else "FALSE"
        )
        resource_due = (
            """EXISTS (
                SELECT 1
                FROM qe_archive.run_resource_session s
                JOIN qe_evolution_loops l
                  ON l.task_id = s.task_id AND l.loop_index = s.loop_index
                WHERE s.status IN ('reserved', 'running')
                  AND s.source_run_key NOT LIKE 'qelt:%'
                  AND l.status IN (
                      'completed', 'failed', 'cancelled', 'canceled',
                      'interrupted', 'timeout', 'stopped'
                  )
            )"""
            if self._scope_enabled(QEReconciliationScope.RESOURCE_SESSION)
            else "FALSE"
        )
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT
                        {reservation_due} AS reservation,
                        {experiment_due} AS experiment,
                        {evolution_due} AS evolution,
                        {long_trend_due} AS long_trend,
                        {resource_due} AS terminal_resource_session,
                        COALESCE((
                            SELECT jsonb_object_agg(
                                s.session_id,
                                jsonb_build_object(
                                    'session_id', s.session_id,
                                    'current_phase', s.current_phase,
                                    'session_status', s.status,
                                    'gpu_phase_released_at', s.gpu_phase_released_at,
                                    'loop_status', l.status
                                )
                            )
                            FROM qe_archive.run_resource_session s
                            LEFT JOIN qe_evolution_loops l
                              ON l.task_id = s.task_id AND l.loop_index = s.loop_index
                            WHERE s.session_id = ANY(%s)
                        ), '{{}}'::jsonb) AS resource_states,
                        COALESCE((
                            SELECT jsonb_object_agg(l.loop_id, l.status)
                            FROM qe_evolution_loops l
                            WHERE l.loop_id = ANY(%s)
                        ), '{{}}'::jsonb) AS loop_states
                    """,
                    (list(resource_session_ids), list(loop_ids)),
                )
                row = dict(cur.fetchone() or {})
        return QEReconciliationDue(
            reservation=bool(row.get("reservation")),
            experiment=bool(row.get("experiment")),
            evolution=bool(row.get("evolution")),
            long_trend=bool(row.get("long_trend")),
            terminal_resource_session=bool(row.get("terminal_resource_session")),
            resource_states=dict(row.get("resource_states") or {}),
            loop_states={
                str(key): str(value)
                for key, value in dict(row.get("loop_states") or {}).items()
            },
        )


async def run_qe_reconciliation_coordinator(stop_event: asyncio.Event) -> None:
    await QEReconciliationCoordinator().run(stop_event)
