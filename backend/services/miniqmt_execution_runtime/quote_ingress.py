"""Phase 1-B isolated MiniQMT quote-ingress runtime.

This module is intentionally limited to quote ingestion: it owns neither a
broker client nor a database session, never creates orders, and never invokes
strategy code.  The only callback-boundary operation is copying a broker
payload into P1-A's immutable :class:`RawQuoteFrame`.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Mapping

from backend.execution_algos.adaptive_is.reasons import (
    QuoteContractError,
    QuoteContractReasonCode,
    quote_contract_error,
)
from backend.infra.realtime_quote_subscriber import (
    PhaseOneLeaseCallbacks,
    PhaseOneQuoteDelivery,
    PhaseOneQuoteLease,
    RealtimeQuoteSubscriber,
)
from backend.miniqmt_quote_contract_config import QuoteIngressRuntimeConfig
from backend.services.miniqmt_execution_runtime.quote_eligibility import (
    BoundedNormalizedQuoteStore,
    NormalizedQuoteObservation,
    QuoteEvaluationContextStore,
    QuoteOrderingTracker,
    deterministic_market_data_id,
)
from backend.services.miniqmt_execution_runtime.quote_normalizer import (
    RawQuoteFrame,
    capture_raw_quote_frame,
    normalize_raw_quote_frame,
)


logger = logging.getLogger(__name__)


class ReservedSymbolMailbox:
    """Bounded per-symbol latest-frame mailbox with exactly one token per symbol."""

    def __init__(self, *, max_symbols: int):
        if not isinstance(max_symbols, int) or isinstance(max_symbols, bool) or max_symbols <= 0:
            raise quote_contract_error(
                QuoteContractReasonCode.CAPACITY_EXCEEDED,
                "ReservedSymbolMailbox max_symbols must be a positive integer",
                context={"max_symbols": max_symbols},
            )
        self._max_symbols = max_symbols
        self._condition = threading.Condition(threading.RLock())
        self._admitted: set[str] = set()
        self._slots: dict[str, RawQuoteFrame] = {}
        self._queued: set[str] = set()
        self._tokens: deque[str] = deque()
        self._generation_floor = 0
        self._accepted_count = 0
        self._coalesced_count = 0
        self._ordering_rejected_count = 0

    def admit(self, symbols: tuple[str, ...]) -> None:
        normalized = tuple(dict.fromkeys(str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()))
        if not normalized:
            raise quote_contract_error(QuoteContractReasonCode.SYMBOL_INVALID, "mailbox admission requires non-empty symbols")
        with self._condition:
            candidate = self._admitted | set(normalized)
            if len(candidate) > self._max_symbols:
                raise quote_contract_error(
                    QuoteContractReasonCode.CAPACITY_EXCEEDED,
                    "mailbox admission exceeds the configured symbol capacity",
                    context={
                        "requested_symbol_count": len(candidate),
                        "max_symbols": self._max_symbols,
                        "requested_symbols": list(normalized),
                    },
                )
            self._admitted = candidate

    def replace_admitted(self, symbols: tuple[str, ...]) -> None:
        """Atomically replace admission and purge every revoked symbol."""

        normalized = tuple(dict.fromkeys(str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()))
        if len(normalized) > self._max_symbols:
            raise quote_contract_error(
                QuoteContractReasonCode.CAPACITY_EXCEEDED,
                "mailbox admission exceeds the configured symbol capacity",
                context={"requested_symbol_count": len(normalized), "max_symbols": self._max_symbols},
            )
        with self._condition:
            admitted = set(normalized)
            revoked = self._admitted - admitted
            self._admitted = admitted
            for symbol in revoked:
                self._slots.pop(symbol, None)
                self._queued.discard(symbol)
            if revoked:
                self._tokens = deque(symbol for symbol in self._tokens if symbol not in revoked)
            self._condition.notify_all()

    def activate_generation(self, generation: int) -> None:
        if not isinstance(generation, int) or generation < 0:
            raise quote_contract_error(
                QuoteContractReasonCode.ORDERING_REJECTED,
                "mailbox generation must be a non-negative integer",
                context={"generation": generation},
            )
        with self._condition:
            if generation < self._generation_floor:
                raise quote_contract_error(
                    QuoteContractReasonCode.ORDERING_REJECTED,
                    "cannot activate a mailbox generation older than the current fence",
                    context={"generation": generation, "generation_floor": self._generation_floor},
                )
            self._generation_floor = generation
            self._slots.clear()
            self._queued.clear()
            self._tokens.clear()
            self._condition.notify_all()

    def fence_generation(self, generation: int) -> None:
        with self._condition:
            self._generation_floor = max(self._generation_floor, generation + 1)
            self._slots.clear()
            self._queued.clear()
            self._tokens.clear()
            self._condition.notify_all()

    def offer(self, frame: RawQuoteFrame) -> str:
        if not isinstance(frame, RawQuoteFrame):
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "ReservedSymbolMailbox accepts RawQuoteFrame only",
                context={"frame_type": type(frame).__name__},
            )
        with self._condition:
            if frame.symbol not in self._admitted:
                raise quote_contract_error(
                    QuoteContractReasonCode.UNEXPECTED_SYMBOL,
                    "quote callback symbol was not admitted to this mailbox",
                    context={"symbol": frame.symbol, "generation": frame.ingress_generation},
                )
            if frame.ingress_generation < self._generation_floor:
                self._ordering_rejected_count += 1
                return "STALE_GENERATION"
            current = self._slots.get(frame.symbol)
            if current is not None and not self._is_newer(frame, current):
                self._ordering_rejected_count += 1
                return "ORDERING_REJECTED"
            self._slots[frame.symbol] = frame
            if frame.symbol in self._queued:
                self._coalesced_count += 1
                return "COALESCED"
            self._queued.add(frame.symbol)
            self._tokens.append(frame.symbol)
            self._accepted_count += 1
            self._condition.notify()
            return "ACCEPTED"

    def wait_and_drain(self, *, budget: int, timeout_seconds: float) -> list[RawQuoteFrame]:
        if budget <= 0:
            raise ValueError("mailbox drain budget must be positive")
        with self._condition:
            if not self._tokens:
                self._condition.wait(timeout=timeout_seconds)
            frames: list[RawQuoteFrame] = []
            while self._tokens and len(frames) < budget:
                symbol = self._tokens.popleft()
                self._queued.discard(symbol)
                frame = self._slots.pop(symbol, None)
                if frame is not None:
                    frames.append(frame)
            return frames

    def telemetry(self) -> dict[str, Any]:
        with self._condition:
            return {
                "backlog": len(self._tokens),
                "admitted_symbols": sorted(self._admitted),
                "accepted_count": self._accepted_count,
                "coalesced_count": self._coalesced_count,
                "ordering_rejected_count": self._ordering_rejected_count,
                "generation_floor": self._generation_floor,
            }

    @staticmethod
    def _is_newer(candidate: RawQuoteFrame, current: RawQuoteFrame) -> bool:
        if candidate.ingress_generation != current.ingress_generation:
            return candidate.ingress_generation > current.ingress_generation
        return candidate.ingress_sequence > current.ingress_sequence


class PhaseOneRawQuoteSnapshotStore:
    """Bounded in-memory latest raw frame store; P1-C owns normalization later."""

    def __init__(self, *, max_symbols: int) -> None:
        if not isinstance(max_symbols, int) or isinstance(max_symbols, bool) or max_symbols <= 0:
            raise quote_contract_error(
                QuoteContractReasonCode.CAPACITY_EXCEEDED,
                "snapshot store max_symbols must be a positive integer",
                context={"max_symbols": max_symbols},
            )
        self._lock = threading.RLock()
        self._max_symbols = max_symbols
        self._admitted: set[str] = set()
        self._latest_by_symbol: dict[str, RawQuoteFrame] = {}

    def replace_admitted(self, symbols: tuple[str, ...]) -> None:
        normalized = set(dict.fromkeys(str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()))
        if len(normalized) > self._max_symbols:
            raise quote_contract_error(
                QuoteContractReasonCode.CAPACITY_EXCEEDED,
                "snapshot store admission exceeds configured capacity",
                context={"requested_symbol_count": len(normalized), "max_symbols": self._max_symbols},
            )
        with self._lock:
            self._admitted = normalized
            self._latest_by_symbol = {
                symbol: frame for symbol, frame in self._latest_by_symbol.items() if symbol in normalized
            }

    def update(self, frame: RawQuoteFrame) -> None:
        with self._lock:
            if frame.symbol not in self._admitted:
                raise quote_contract_error(
                    QuoteContractReasonCode.UNEXPECTED_SYMBOL,
                    "snapshot store rejected a symbol outside the active lease union",
                    context={"symbol": frame.symbol, "max_symbols": self._max_symbols},
                )
            current = self._latest_by_symbol.get(frame.symbol)
            if current is None or ReservedSymbolMailbox._is_newer(frame, current):
                self._latest_by_symbol[frame.symbol] = frame

    def get(self, symbol: str) -> RawQuoteFrame | None:
        with self._lock:
            return self._latest_by_symbol.get(symbol)

    def snapshot(self) -> dict[str, RawQuoteFrame]:
        with self._lock:
            return dict(self._latest_by_symbol)


class PhaseOneQuoteProjectionSink:
    """P1-C same-writer raw-to-normalized projection; never calls providers or a broker."""

    def __init__(
        self,
        *,
        raw_store: PhaseOneRawQuoteSnapshotStore,
        normalized_store: BoundedNormalizedQuoteStore,
        context_store: QuoteEvaluationContextStore,
        loud_sink: Callable[[QuoteContractError], None] | None = None,
    ) -> None:
        self._raw_store = raw_store
        self._normalized_store = normalized_store
        self._context_store = context_store
        self._ordering = QuoteOrderingTracker()
        self._loud_sink = loud_sink
        self._lock = threading.RLock()
        self._last_error_by_symbol: dict[str, dict[str, Any]] = {}
        self._accepted_count = 0
        self._rejected_count = 0

    @property
    def normalized_store(self) -> BoundedNormalizedQuoteStore:
        return self._normalized_store

    def replace_admitted(self, symbols: tuple[str, ...]) -> None:
        self._raw_store.replace_admitted(symbols)
        self._normalized_store.replace_admitted(symbols)

    def on_generation_published(self, generation: int) -> None:
        self._ordering.activate_generation(generation)

    def project(self, frame: RawQuoteFrame) -> None:
        """Run inside the existing writer. Expected quote failures are loud, not fatal."""

        try:
            self._raw_store.update(frame)
            context = self._context_store.snapshot()
            if context is None:
                raise quote_contract_error(
                    QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                    "quote projection has no scheduler-published evaluation context",
                    context={"symbol": frame.symbol, "ingress_generation": frame.ingress_generation},
                )
            symbol_context = context.symbol_context(frame.symbol)
            if symbol_context is None:
                raise quote_contract_error(
                    QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                    "quote projection has no preloaded symbol authority context",
                    context={"symbol": frame.symbol, "context_id": context.context_id},
                )
            if symbol_context.tradability is None:
                raise quote_contract_error(
                    QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                    "quote projection cannot normalize without tradability authority",
                    context={"symbol": frame.symbol, "context_id": context.context_id},
                )
            quote = normalize_raw_quote_frame(
                frame,
                clock_trade_date=context.clock.clock_trade_date,
                board=symbol_context.board,
                depth_quantity_unit=symbol_context.depth_quantity_unit,
                unit_evidence_version=symbol_context.unit_evidence_version,
                tradability=symbol_context.tradability,
            )
            decision = self._ordering.decide(frame=frame, quote=quote)
            if not decision.accepted:
                self._record_ordering_rejection(frame=frame, disposition=decision.disposition.value)
                return
            observation = NormalizedQuoteObservation(
                frame=frame,
                quote=quote,
                tradability=symbol_context.tradability,
                context_id=context.context_id,
                market_data_id=deterministic_market_data_id(
                    frame=frame,
                    quote=quote,
                    tradability=symbol_context.tradability,
                    calendar_snapshot_set=context.calendar_snapshot_set,
                    policy=context.policy,
                ),
                ordering_disposition=decision.disposition,
            )
            self._normalized_store.accept(observation)
            with self._lock:
                self._accepted_count += 1
                self._last_error_by_symbol.pop(frame.symbol, None)
        except QuoteContractError as error:
            self._record_loud(frame=frame, error=error)

    def health(self) -> dict[str, Any]:
        with self._lock:
            errors = {symbol: dict(payload) for symbol, payload in self._last_error_by_symbol.items()}
            accepted_count = self._accepted_count
            rejected_count = self._rejected_count
        return {
            "projection": {
                "accepted_count": accepted_count,
                "rejected_count": rejected_count,
                "last_error_by_symbol": errors,
                "ordering": self._ordering.health(),
                "normalized_store": self._normalized_store.health(),
                "context": self._context_store.health(),
            }
        }

    def _record_ordering_rejection(self, *, frame: RawQuoteFrame, disposition: str) -> None:
        error = quote_contract_error(
            QuoteContractReasonCode.ORDERING_REJECTED,
            "normalized quote projection rejected ordering input",
            context={
                "symbol": frame.symbol,
                "ingress_generation": frame.ingress_generation,
                "ingress_sequence": frame.ingress_sequence,
                "event": disposition,
            },
        )
        self._record_loud(frame=frame, error=error)

    def _record_loud(self, *, frame: RawQuoteFrame, error: QuoteContractError) -> None:
        payload = error.as_loud_payload()
        with self._lock:
            self._rejected_count += 1
            self._last_error_by_symbol[frame.symbol] = payload
        logger.error(
            "Phase 1 quote projection loud failure: symbol=%s generation=%s sequence=%s payload=%s",
            frame.symbol,
            frame.ingress_generation,
            frame.ingress_sequence,
            payload,
        )
        if self._loud_sink is None:
            return
        try:
            self._loud_sink(error)
        except Exception as exc:  # noqa: BLE001 - reporting cannot alter projection state.
            logger.error(
                "Phase 1 quote projection loud sink failed: symbol=%s exception_type=%s",
                frame.symbol,
                type(exc).__name__,
                exc_info=True,
            )


@dataclass(frozen=True)
class QuoteIngressRestartRequest:
    consumer_id: str
    generation: int
    restart_count: int


class QuoteIngressWorker:
    """One consumer's single writer and its isolated health lifecycle."""

    def __init__(
        self,
        *,
        consumer_id: str,
        config: QuoteIngressRuntimeConfig,
        frame_sink: Callable[[RawQuoteFrame], None],
        loud_sink: Callable[[QuoteContractError], None] | None = None,
    ) -> None:
        if not consumer_id.strip() or not callable(frame_sink) or (loud_sink is not None and not callable(loud_sink)):
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "QuoteIngressWorker requires a consumer id, frame sink, and loud sink",
            )
        self._consumer_id = consumer_id
        self._config = config
        self._frame_sink = frame_sink
        self._loud_sink = loud_sink
        self._mailbox = ReservedSymbolMailbox(max_symbols=config.max_symbols)
        self._lock = threading.RLock()
        self._writer_stop_event: threading.Event | None = None
        self._writer_epoch = 0
        self._writer_thread_epoch: int | None = None
        self._writer_thread: threading.Thread | None = None
        self._active_generation: int | None = None
        self._fenced_generation = -1
        self._pending_by_generation: dict[int, dict[str, RawQuoteFrame]] = {}
        self._status = "IDLE"
        self._last_drain_at: datetime | None = None
        self._last_heartbeat_monotonic_ns: int | None = None
        self._last_failure: dict[str, Any] | None = None
        self._failure_samples: dict[str, dict[str, Any]] = {}
        self._last_loud_emitted_monotonic_ns: dict[str, int] = {}
        self._restart_count = 0
        self._restart_attempts_in_epoch = 0
        self._next_restart_after_monotonic_ns = 0

    def admit_symbols(self, symbols: tuple[str, ...]) -> None:
        self._mailbox.admit(symbols)

    def replace_admitted_symbols(self, symbols: tuple[str, ...]) -> None:
        self._mailbox.replace_admitted(symbols)
        admitted = set(symbols)
        with self._lock:
            for generation, frames in list(self._pending_by_generation.items()):
                retained = {symbol: frame for symbol, frame in frames.items() if symbol in admitted}
                if retained:
                    self._pending_by_generation[generation] = retained
                else:
                    self._pending_by_generation.pop(generation, None)

    def capture_delivery(self, delivery: PhaseOneQuoteDelivery, *, source_session_id: str, clock_domain_id: str) -> bool:
        """Capture exactly at the callback boundary and never throw to xtdata."""

        try:
            frame = capture_raw_quote_frame(
                delivery.payload,
                callback_symbol=delivery.symbol,
                source_session_id=source_session_id,
                ingress_generation=delivery.generation,
                ingress_sequence=delivery.ingress_sequence,
                received_at_utc=delivery.received_at_utc,
                received_monotonic_ns=delivery.received_monotonic_ns,
                clock_domain_id=clock_domain_id,
                source_method=delivery.source_method,
            )
        except QuoteContractError as error:
            self.record_loud_failure(error)
            return False
        return self.ingest_frame(frame)

    def ingest_frame(self, frame: RawQuoteFrame) -> bool:
        with self._lock:
            if frame.ingress_generation <= self._fenced_generation:
                stale = quote_contract_error(
                    QuoteContractReasonCode.ORDERING_REJECTED,
                    "fenced quote generation cannot enter the Phase 1 mailbox",
                    context={
                        "event": "STALE_GENERATION",
                        "consumer_id": self._consumer_id,
                        "generation": frame.ingress_generation,
                        "fenced_generation": self._fenced_generation,
                        "symbol": frame.symbol,
                    },
                )
                self._record_failure_locked(stale, status="DEGRADED")
                error = stale
            elif self._active_generation != frame.ingress_generation:
                pending = self._pending_by_generation.setdefault(frame.ingress_generation, {})
                current = pending.get(frame.symbol)
                if current is None or ReservedSymbolMailbox._is_newer(frame, current):
                    pending[frame.symbol] = frame
                return True
            else:
                error = None
        if error is not None:
            self._emit_loud(error)
            return False
        try:
            outcome = self._mailbox.offer(frame)
        except QuoteContractError as error:
            self.record_loud_failure(error)
            return False
        if outcome in {"STALE_GENERATION", "ORDERING_REJECTED"}:
            self.record_loud_failure(
                quote_contract_error(
                    QuoteContractReasonCode.ORDERING_REJECTED,
                    "mailbox rejected a non-new quote frame",
                    context={
                        "event": outcome,
                        "consumer_id": self._consumer_id,
                        "generation": frame.ingress_generation,
                        "sequence": frame.ingress_sequence,
                        "symbol": frame.symbol,
                    },
                )
            )
            return False
        return True

    def on_generation_published(self, data_session_key: str, generation: int) -> bool:
        with self._lock:
            if generation == self._active_generation and self._status not in {"FAILED", "STOPPED"}:
                return True
            if generation <= self._fenced_generation:
                error = quote_contract_error(
                    QuoteContractReasonCode.ORDERING_REJECTED,
                    "a fenced quote generation cannot be published to a worker",
                    context={
                        "event": "STALE_GENERATION",
                        "data_session_key": data_session_key,
                        "consumer_id": self._consumer_id,
                        "generation": generation,
                        "fenced_generation": self._fenced_generation,
                    },
                )
                self._record_failure_locked(error, status="DEGRADED")
                pending_frames = []
            elif (
                self._writer_thread is not None
                and self._writer_thread.is_alive()
                and self._writer_thread_epoch != self._writer_epoch
            ):
                error = quote_contract_error(
                    QuoteContractReasonCode.CONSUMER_FAILURE,
                    "a fenced quote writer has not exited; generation publication cannot start a parallel writer",
                    context={
                        "data_session_key": data_session_key,
                        "consumer_id": self._consumer_id,
                        "generation": generation,
                    },
                )
                self._record_failure_locked(error, status="FAILED")
                pending_frames = []
            else:
                previous_generation = self._active_generation
                self._active_generation = generation
                if previous_generation is not None and previous_generation < generation:
                    self._fenced_generation = max(self._fenced_generation, previous_generation)
                self._mailbox.activate_generation(generation)
                pending_frames = sorted(
                    self._pending_by_generation.pop(generation, {}).values(),
                    key=lambda frame: (frame.symbol, frame.ingress_sequence),
                )
                for pending_generation in list(self._pending_by_generation):
                    if pending_generation < generation:
                        self._pending_by_generation.pop(pending_generation, None)
                self._status = "STARTING"
                error = None
        if error is not None:
            self._emit_loud(error)
            return False
        for frame in pending_frames:
            if not self.ingest_frame(frame):
                return False
        return self._start_writer_if_needed()

    def prepare_generation(self, data_session_key: str, generation: int) -> bool:
        """Pure readiness check used before a physical generation is committed."""

        with self._lock:
            if self._status == "STOPPED" or generation <= self._fenced_generation:
                return False
            return not (
                self._writer_thread is not None
                and self._writer_thread.is_alive()
                and self._writer_thread_epoch != self._writer_epoch
            )

    def on_generation_fenced(self, data_session_key: str, generation: int) -> None:
        with self._lock:
            self._pending_by_generation.pop(generation, None)
            if self._active_generation is not None and generation == self._active_generation:
                self._mailbox.fence_generation(generation)
                self._fenced_generation = max(self._fenced_generation, generation)
                self._active_generation = None
                self._status = "FENCED"
                self._fence_writer_locked()
            elif self._active_generation is not None and generation < self._active_generation:
                self._fenced_generation = max(self._fenced_generation, generation)

    def record_loud_failure(self, error: QuoteContractError) -> None:
        with self._lock:
            fatal = error.reason_code == QuoteContractReasonCode.CONSUMER_FAILURE
            self._record_failure_locked(error, status="FAILED" if fatal else "DEGRADED")
            if fatal:
                if self._active_generation is not None:
                    self._mailbox.fence_generation(self._active_generation)
                self._fence_writer_locked()
        self._emit_loud(error)

    def watchdog(self, *, now_monotonic_ns: int | None = None) -> QuoteIngressRestartRequest | None:
        now = now_monotonic_ns if now_monotonic_ns is not None else time.monotonic_ns()
        with self._lock:
            generation = self._active_generation
            thread_alive = self._writer_thread is not None and self._writer_thread.is_alive()
            heartbeat_age_ns = None if self._last_heartbeat_monotonic_ns is None else now - self._last_heartbeat_monotonic_ns
            heartbeat_stale = heartbeat_age_ns is not None and heartbeat_age_ns > self._config.heartbeat_timeout_ms * 1_000_000
            if generation is None or self._status in {"IDLE", "FENCED", "STOPPED"}:
                return None
            if self._restart_attempts_in_epoch >= self._config.restart_max_attempts:
                error = quote_contract_error(
                    QuoteContractReasonCode.CONSUMER_FAILURE,
                    "quote writer reached the configured automatic restart limit for this lifecycle epoch",
                    context={
                        "consumer_id": self._consumer_id,
                        "generation": generation,
                        "restart_max_attempts": self._config.restart_max_attempts,
                    },
                )
                self._record_failure_locked(error, status="FAILED")
                emit_error = error
                request = None
            elif now < self._next_restart_after_monotonic_ns:
                return None
            elif self._status == "FAILED":
                self._mailbox.fence_generation(generation)
                self._fence_writer_locked()
                self._status = "RESTART_PENDING"
                emit_error = None
                request = self._new_restart_request_locked(generation=generation, now=now)
            elif thread_alive and heartbeat_stale:
                error = quote_contract_error(
                    QuoteContractReasonCode.CONSUMER_FAILURE,
                    "quote writer heartbeat is stale while its thread is still alive; a parallel writer is forbidden",
                    context={
                        "consumer_id": self._consumer_id,
                        "generation": generation,
                        "heartbeat_timeout_ms": self._config.heartbeat_timeout_ms,
                    },
                )
                self._record_failure_locked(error, status="FAILED")
                self._mailbox.fence_generation(generation)
                self._fence_writer_locked()
                self._status = "RESTART_PENDING"
                emit_error = error
                request = self._new_restart_request_locked(generation=generation, now=now)
            elif thread_alive:
                return None
            else:
                self._mailbox.fence_generation(generation)
                self._status = "RESTART_PENDING"
                emit_error = None
                request = self._new_restart_request_locked(generation=generation, now=now)
        if emit_error is not None:
            self._emit_loud(emit_error)
        return request

    def begin_lifecycle_epoch(self) -> None:
        """Allow a later valid scheduler lifecycle tick to retry automatically."""

        with self._lock:
            if self._status == "FAILED":
                self._restart_attempts_in_epoch = 0
                self._next_restart_after_monotonic_ns = 0
                self._status = "DEGRADED"

    def health(self) -> dict[str, Any]:
        with self._lock:
            thread_alive = self._writer_thread is not None and self._writer_thread.is_alive()
            mailbox = self._mailbox.telemetry()
            heartbeat_age_ms = (
                None
                if self._last_heartbeat_monotonic_ns is None
                else max(0, (time.monotonic_ns() - self._last_heartbeat_monotonic_ns) // 1_000_000)
            )
            return {
                "consumer_id": self._consumer_id,
                "status": self._status,
                "generation": self._active_generation,
                "thread_alive": thread_alive,
                "last_drain_at": self._last_drain_at.isoformat() if self._last_drain_at is not None else None,
                "backlog": mailbox["backlog"],
                "admitted_symbols": mailbox["admitted_symbols"],
                "restart_count": self._restart_count,
                "writer_heartbeat_age_ms": heartbeat_age_ms,
                "last_failure": self._last_failure,
                "accepted_count": mailbox["accepted_count"],
                "coalesced_count": mailbox["coalesced_count"],
                "ordering_rejected_count": mailbox["ordering_rejected_count"],
            }

    def shutdown(self) -> None:
        with self._lock:
            self._status = "STOPPED"
            if self._active_generation is not None:
                self._mailbox.fence_generation(self._active_generation)
                self._fenced_generation = max(self._fenced_generation, self._active_generation)
            self._active_generation = None
            self._pending_by_generation.clear()
            writer = self._writer_thread
            self._fence_writer_locked()
        if writer is not None and writer.is_alive():
            writer.join(timeout=2.0)

    def _start_writer_if_needed(self) -> bool:
        with self._lock:
            if self._status == "STOPPED":
                return False
            if self._writer_thread is not None and self._writer_thread.is_alive():
                return self._status not in {"FAILED", "RESTART_PENDING"}
            self._writer_epoch += 1
            writer_epoch = self._writer_epoch
            stop_event = threading.Event()
            self._writer_stop_event = stop_event
            self._writer_thread_epoch = writer_epoch
            self._writer_thread = threading.Thread(
                target=self._writer_loop,
                args=(writer_epoch, stop_event),
                name=f"miniqmt-quote-ingress-{self._consumer_id}",
                daemon=True,
            )
            self._writer_thread.start()
            return True

    def _writer_loop(self, writer_epoch: int, stop_event: threading.Event) -> None:
        try:
            while not stop_event.is_set():
                frames = self._mailbox.wait_and_drain(
                    budget=self._config.drain_budget,
                    timeout_seconds=min(self._config.heartbeat_timeout_ms / 2000.0, 0.5),
                )
                now_monotonic_ns = time.monotonic_ns()
                with self._lock:
                    if writer_epoch != self._writer_epoch:
                        return
                    self._last_heartbeat_monotonic_ns = now_monotonic_ns
                    self._last_drain_at = datetime.now(UTC)
                    if self._status not in {"STOPPED", "FENCED", "RESTART_PENDING"}:
                        self._status = "ACTIVE"
                for frame in frames:
                    with self._lock:
                        if (
                            writer_epoch != self._writer_epoch
                            or frame.ingress_generation != self._active_generation
                            or self._status in {"STOPPED", "FAILED", "FENCED", "RESTART_PENDING"}
                        ):
                            continue
                    self._frame_sink(frame)
        except Exception as exc:  # noqa: BLE001 - worker failure is fenced and recovered by its supervisor
            error = quote_contract_error(
                QuoteContractReasonCode.CONSUMER_FAILURE,
                "Phase 1 quote ingress writer failed",
                context={
                    "consumer_id": self._consumer_id,
                    "generation": self._active_generation,
                    "exception_type": type(exc).__name__,
                },
            )
            with self._lock:
                self._record_failure_locked(error, status="FAILED")
            self._emit_loud(error)

    def _fence_writer_locked(self) -> None:
        self._writer_epoch += 1
        if self._writer_stop_event is not None:
            self._writer_stop_event.set()

    def _new_restart_request_locked(self, *, generation: int, now: int) -> QuoteIngressRestartRequest:
        self._restart_attempts_in_epoch += 1
        self._restart_count += 1
        backoff_ms = min(
            self._config.restart_backoff_ms * (2 ** (self._restart_attempts_in_epoch - 1)),
            self._config.restart_max_backoff_ms,
        )
        self._next_restart_after_monotonic_ns = now + backoff_ms * 1_000_000
        return QuoteIngressRestartRequest(
            consumer_id=self._consumer_id,
            generation=generation,
            restart_count=self._restart_count,
        )

    def _record_failure_locked(self, error: QuoteContractError, *, status: str) -> None:
        payload = error.as_loud_payload()
        context = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
        sample_key = ":".join(
            str(value)
            for value in (payload["reason_code"], context.get("symbol", ""), context.get("generation", ""))
        )
        now = datetime.now(UTC).isoformat()
        sample = self._failure_samples.get(sample_key)
        if sample is None:
            sample = {"first_observed_at": now, "occurrence_count": 0}
            self._failure_samples[sample_key] = sample
        sample["last_observed_at"] = now
        sample["occurrence_count"] = int(sample["occurrence_count"]) + 1
        self._last_failure = {**payload, **sample, "sample_key": sample_key}
        self._status = status

    def _emit_loud(self, error: QuoteContractError) -> None:
        payload = error.as_loud_payload()
        context = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
        sample_key = ":".join(
            str(value)
            for value in (payload["reason_code"], context.get("symbol", ""), context.get("generation", ""))
        )
        now_monotonic_ns = time.monotonic_ns()
        with self._lock:
            sample = dict(self._failure_samples.get(sample_key, {}))
            previous_emit = self._last_loud_emitted_monotonic_ns.get(sample_key, 0)
            if now_monotonic_ns - previous_emit < self._config.loud_interval_seconds * 1_000_000_000:
                return
            self._last_loud_emitted_monotonic_ns[sample_key] = now_monotonic_ns
        logger.error("Phase 1 quote ingress loud failure: %s", {**payload, **sample})
        if self._loud_sink is None:
            return
        try:
            self._loud_sink(error)
        except Exception as exc:  # noqa: BLE001 - reporting must not rewrite quote business state
            logger.error(
                "Phase 1 quote ingress loud sink failed: reason=%s consumer_id=%s exception_type=%s",
                error.reason_code.value,
                self._consumer_id,
                type(exc).__name__,
                exc_info=True,
            )


@dataclass
class QuoteIngressConsumer:
    """Logical consumer ownership; the supervisor owns the sole writer."""

    data_session_key: str
    consumer_id: str
    symbols: tuple[str, ...]
    lease: PhaseOneQuoteLease | None = None


class QuoteIngressSupervisor:
    """Scheduler-owned lease lifecycle; read-only callers cannot construct it."""

    def __init__(
        self,
        *,
        subscriber: RealtimeQuoteSubscriber,
        config: QuoteIngressRuntimeConfig,
        data_session_key: str,
        owner: str,
        bootstrap_fetcher: Callable[[list[str]], Mapping[str, Mapping[str, Any]]],
        snapshot_store: PhaseOneRawQuoteSnapshotStore | None = None,
        normalized_store: BoundedNormalizedQuoteStore | None = None,
        context_store: QuoteEvaluationContextStore | None = None,
        loud_sink: Callable[[QuoteContractError], None] | None = None,
    ) -> None:
        if not config.enabled:
            raise quote_contract_error(
                QuoteContractReasonCode.SUBSCRIPTION_UNAVAILABLE,
                "Phase 1 quote ingress is disabled by process configuration",
                context={"data_session_key": data_session_key, "ingress_enabled": config.enabled},
            )
        if not data_session_key.strip() or not owner.strip() or not callable(bootstrap_fetcher):
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "QuoteIngressSupervisor requires scheduler ownership and a bootstrap fetcher",
            )
        self._subscriber = subscriber
        self._config = config
        self._data_session_key = data_session_key
        self._owner = owner
        self._bootstrap_fetcher = bootstrap_fetcher
        self._snapshot_store = snapshot_store or PhaseOneRawQuoteSnapshotStore(max_symbols=config.max_symbols)
        self._normalized_store = normalized_store or BoundedNormalizedQuoteStore(max_symbols=config.max_symbols)
        self._context_store = context_store or QuoteEvaluationContextStore()
        self._loud_sink = loud_sink
        self._lock = threading.RLock()
        self._consumers: dict[str, QuoteIngressConsumer] = {}
        self._source_session_id = f"phase1-{hashlib.sha256(data_session_key.encode('utf-8')).hexdigest()[:24]}"
        self._clock_domain_id = "miniqmt_quote_ingress_monotonic_v1"
        self._projection_sink = PhaseOneQuoteProjectionSink(
            raw_store=self._snapshot_store,
            normalized_store=self._normalized_store,
            context_store=self._context_store,
            loud_sink=loud_sink,
        )
        self._worker = QuoteIngressWorker(
            consumer_id=f"{data_session_key}:single-writer",
            config=config,
            frame_sink=self._projection_sink.project,
            loud_sink=loud_sink,
        )

    @property
    def snapshot_store(self) -> PhaseOneRawQuoteSnapshotStore:
        return self._snapshot_store

    @property
    def normalized_store(self) -> BoundedNormalizedQuoteStore:
        return self._normalized_store

    @property
    def context_store(self) -> QuoteEvaluationContextStore:
        return self._context_store

    def acquire_consumer(self, *, consumer_id: str, symbols: list[str]) -> PhaseOneQuoteLease:
        with self._lock:
            if consumer_id in self._consumers:
                raise quote_contract_error(
                    QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                    "a Phase 1 quote consumer id cannot acquire a second lease",
                    context={"consumer_id": consumer_id, "data_session_key": self._data_session_key},
                )
            normalized = tuple(dict.fromkeys(str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()))
            candidate_symbols = self._union_consumer_symbols((*self._consumers.values(),), additional=normalized)
            previous_symbols = self._union_consumer_symbols((*self._consumers.values(),))
            consumer = QuoteIngressConsumer(
                data_session_key=self._data_session_key,
                consumer_id=consumer_id,
                symbols=normalized,
            )
            self._worker.replace_admitted_symbols(candidate_symbols)
            self._projection_sink.replace_admitted(candidate_symbols)
            try:
                lease = self._subscriber.acquire_phase_one_lease(
                    data_session_key=self._data_session_key,
                    owner=self._owner,
                    consumer_id=consumer_id,
                    symbols=symbols,
                    callbacks=self._callbacks(),
                    bootstrap_fetcher=self._bootstrap_fetcher,
                    max_symbols=self._config.max_symbols,
                )
            except QuoteContractError as error:
                self._worker.replace_admitted_symbols(previous_symbols)
                self._projection_sink.replace_admitted(previous_symbols)
                self._worker.record_loud_failure(error)
                raise
            except Exception as exc:  # noqa: BLE001 - only a typed failure may cross the scheduler boundary
                self._worker.replace_admitted_symbols(previous_symbols)
                self._projection_sink.replace_admitted(previous_symbols)
                error = quote_contract_error(
                    QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                    "Phase 1 quote consumer acquisition raised unexpectedly",
                    context={
                        "consumer_id": consumer_id,
                        "data_session_key": self._data_session_key,
                        "exception_type": type(exc).__name__,
                    },
                )
                self._worker.record_loud_failure(error)
                raise error from exc
            consumer.lease = lease
            self._consumers[consumer_id] = consumer
        return lease

    def release_consumer(self, *, consumer_id: str) -> bool:
        with self._lock:
            consumer = self._consumers.get(consumer_id)
            if consumer is None or consumer.lease is None:
                return False
            released = self._subscriber.release_phase_one_lease(
                data_session_key=self._data_session_key,
                lease_id=consumer.lease.lease_id,
                max_symbols=self._config.max_symbols,
            )
            if not released:
                return False
            self._consumers.pop(consumer_id, None)
            admitted = self._union_consumer_symbols((*self._consumers.values(),))
            self._worker.replace_admitted_symbols(admitted)
            self._projection_sink.replace_admitted(admitted)
        return released

    def watchdog_tick(self) -> dict[str, Any]:
        """Run from the scheduler lifecycle; it never restarts the scheduler itself."""

        request = self._worker.watchdog()
        if request is not None:
            try:
                self._subscriber.rebuild_phase_one_leases(
                    data_session_key=self._data_session_key,
                    owner=self._owner,
                    max_symbols=self._config.max_symbols,
                )
            except QuoteContractError as error:
                self._worker.record_loud_failure(error)
        return self.health()

    def begin_lifecycle_epoch(self) -> dict[str, Any]:
        """Called only by a later legal scheduler tick after a capped failure."""

        self._worker.begin_lifecycle_epoch()
        return self.watchdog_tick()

    def health(self) -> dict[str, Any]:
        with self._lock:
            consumers: dict[str, dict[str, Any]] = {}
            for consumer_id, consumer in self._consumers.items():
                current_lease = (
                    self._subscriber.get_phase_one_lease(
                        data_session_key=self._data_session_key,
                        lease_id=consumer.lease.lease_id,
                    )
                    if consumer.lease is not None
                    else None
                )
                consumers[consumer_id] = {
                    "lease_id": current_lease.lease_id if current_lease is not None else None,
                    "symbols": list(consumer.symbols),
                    "lease_generation": current_lease.generation if current_lease is not None else None,
                    "lease_status": current_lease.status if current_lease is not None else "RELEASED",
                }
        return {
            "data_session_key": self._data_session_key,
            "owner": self._owner,
            "subscription": self._subscriber.phase_one_health(data_session_key=self._data_session_key),
            "writer": self._worker.health(),
            **self._projection_sink.health(),
            "consumers": consumers,
        }

    def shutdown(self) -> None:
        with self._lock:
            self._consumers.clear()
        self._subscriber.shutdown_phase_one_leases(data_session_key=self._data_session_key)
        self._worker.replace_admitted_symbols(())
        self._projection_sink.replace_admitted(())
        self._worker.shutdown()

    def _callbacks(self) -> PhaseOneLeaseCallbacks:
        return PhaseOneLeaseCallbacks(
            on_quote=self._capture_delivery,
            on_generation_prepared=self._worker.prepare_generation,
            on_generation_published=self._publish_generation,
            on_generation_fenced=self._on_generation_fenced,
            on_loud_failure=self._worker.record_loud_failure,
        )

    def _capture_delivery(self, delivery: PhaseOneQuoteDelivery) -> bool:
        return self._worker.capture_delivery(
            delivery,
            source_session_id=self._source_session_id,
            clock_domain_id=self._clock_domain_id,
        )

    def _publish_generation(self, data_session_key: str, generation: int) -> None:
        self._projection_sink.on_generation_published(generation)
        if not self._worker.on_generation_published(data_session_key, generation):
            raise RuntimeError("single quote writer rejected a committed generation")

    def _on_generation_fenced(self, data_session_key: str, generation: int) -> None:
        health = self._subscriber.phase_one_health(data_session_key=data_session_key)
        if health.get("generation") == generation and int(health.get("lease_count") or 0) > 0:
            return
        self._worker.on_generation_fenced(data_session_key, generation)

    @staticmethod
    def _union_consumer_symbols(
        consumers: tuple[QuoteIngressConsumer, ...],
        *,
        additional: tuple[str, ...] = (),
    ) -> tuple[str, ...]:
        return tuple(dict.fromkeys([*(symbol for consumer in consumers for symbol in consumer.symbols), *additional]))


__all__ = [
    "PhaseOneRawQuoteSnapshotStore",
    "PhaseOneQuoteProjectionSink",
    "QuoteIngressConsumer",
    "QuoteIngressRestartRequest",
    "QuoteIngressSupervisor",
    "QuoteIngressWorker",
    "ReservedSymbolMailbox",
]
