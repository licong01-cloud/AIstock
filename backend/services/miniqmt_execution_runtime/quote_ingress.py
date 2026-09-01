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
from dataclasses import dataclass, field
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
    MINIQMT_QUOTE_CLOCK_DOMAIN_ID,
    NormalizedQuoteObservation,
    QuoteEvaluationContext,
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
_KERNEL_CONTEXTUAL_FANOUT_WAIT_SECONDS = 0.01


def kernel_product_pending_identity_sha256_v1(
    *,
    runtime_id: str,
    symbol: str,
    market_data_id: str | None,
    ingress_generation: int | None,
    ingress_sequence: int | None,
    context_id: Any,
    values: tuple[Any, ...],
) -> str:
    """Return the single callback-frame identity shared by producer and consumer."""

    def component(value: Any) -> str:
        return "" if value is None else str(value)

    identity_payload = "|".join(
        (
            runtime_id,
            symbol,
            market_data_id or "",
            component(ingress_generation),
            component(ingress_sequence),
            component(context_id),
            ",".join(type(value).__qualname__ for value in values),
        )
    )
    return hashlib.sha256(identity_payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class MiniQMTKernelProductIngressSuppression:
    """Nominal, frame-bound proof that one callback was deliberately not run."""

    runtime_id: str
    consumer_id: str
    operation: str
    disposition: str
    lifecycle_generation: int
    symbol: str | None
    ingress_generation: int | None
    ingress_sequence: int | None
    market_data_id: str | None
    failure_fingerprint_sha256: str | None
    next_retry_at_utc: str | None
    pending_identity_sha256: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "miniqmt_kernel_product_ingress_suppression_v1",
            "runtime_id": self.runtime_id,
            "consumer_id": self.consumer_id,
            "operation": self.operation,
            "disposition": self.disposition,
            "lifecycle_generation": self.lifecycle_generation,
            "symbol": self.symbol,
            "ingress_generation": self.ingress_generation,
            "ingress_sequence": self.ingress_sequence,
            "market_data_id": self.market_data_id,
            "failure_fingerprint_sha256": self.failure_fingerprint_sha256,
            "next_retry_at_utc": self.next_retry_at_utc,
            "pending_identity_sha256": self.pending_identity_sha256,
            "executed": False,
            "business_success": False,
        }


@dataclass(frozen=True)
class MiniQMTKernelProductIngressCompletion:
    """Exact process-local completion for one previously returned pending carrier."""

    runtime_id: str
    consumer_id: str
    operation: str
    lifecycle_generation: int
    attempt_token: int
    symbol: str
    ingress_generation: int
    ingress_sequence: int
    market_data_id: str
    pending_identity_sha256: str
    completed_at_utc: str
    business_success: bool
    failure: Mapping[str, Any] | None = field(default=None, repr=False, compare=False)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "miniqmt_kernel_product_ingress_completion_v1",
            "runtime_id": self.runtime_id,
            "consumer_id": self.consumer_id,
            "operation": self.operation,
            "disposition": "ASYNC_SUCCEEDED" if self.business_success else "ASYNC_FAILED",
            "lifecycle_generation": self.lifecycle_generation,
            "attempt_token": self.attempt_token,
            "symbol": self.symbol,
            "ingress_generation": self.ingress_generation,
            "ingress_sequence": self.ingress_sequence,
            "market_data_id": self.market_data_id,
            "pending_identity_sha256": self.pending_identity_sha256,
            "completed_at_utc": self.completed_at_utc,
            "executed": True,
            "outcome_pending": False,
            "business_success": self.business_success,
            "failure": dict(self.failure) if self.failure is not None else None,
        }


class MiniQMTKernelProductIngressCompletionSignal:
    """Single-assignment process-local completion channel with exact frame identity."""

    def __init__(
        self,
        *,
        runtime_id: str,
        consumer_id: str,
        operation: str,
        lifecycle_generation: int,
        attempt_token: int,
        symbol: str,
        ingress_generation: int,
        ingress_sequence: int,
        market_data_id: str,
        pending_identity_sha256: str,
    ) -> None:
        self._identity = {
            "runtime_id": runtime_id,
            "consumer_id": consumer_id,
            "operation": operation,
            "lifecycle_generation": lifecycle_generation,
            "attempt_token": attempt_token,
            "symbol": symbol,
            "ingress_generation": ingress_generation,
            "ingress_sequence": ingress_sequence,
            "market_data_id": market_data_id,
            "pending_identity_sha256": pending_identity_sha256,
        }
        self._lock = threading.Lock()
        self._completion: MiniQMTKernelProductIngressCompletion | None = None
        self._subscribers: list[Callable[[MiniQMTKernelProductIngressCompletion], None]] = []

    def identity(self) -> dict[str, Any]:
        return dict(self._identity)

    def subscribe(self, subscriber: Callable[[MiniQMTKernelProductIngressCompletion], None]) -> None:
        if not callable(subscriber):
            raise TypeError("kernel product completion subscriber must be callable")
        with self._lock:
            completion = self._completion
            if completion is None:
                self._subscribers.append(subscriber)
                return
        subscriber(completion)

    def resolve(
        self,
        *,
        business_success: bool,
        completed_at_utc: datetime,
        failure: Mapping[str, Any] | None,
    ) -> MiniQMTKernelProductIngressCompletion:
        if type(business_success) is not bool:
            raise TypeError("kernel product completion success must be an exact bool")
        if completed_at_utc.tzinfo is None or completed_at_utc.utcoffset() != UTC.utcoffset(None):
            raise TypeError("kernel product completion timestamp must be UTC aware")
        if business_success and failure is not None:
            raise ValueError("successful kernel product completion cannot carry failure evidence")
        if not business_success and not isinstance(failure, Mapping):
            raise ValueError("failed kernel product completion requires failure evidence")
        completion = MiniQMTKernelProductIngressCompletion(
            **self._identity,
            completed_at_utc=completed_at_utc.astimezone(UTC).isoformat(),
            business_success=business_success,
            failure=dict(failure) if failure is not None else None,
        )
        with self._lock:
            if self._completion is not None:
                raise RuntimeError("kernel product completion signal was resolved more than once")
            self._completion = completion
            subscribers = tuple(self._subscribers)
            self._subscribers.clear()
        for subscriber in subscribers:
            subscriber(completion)
        return completion


@dataclass(frozen=True)
class MiniQMTKernelProductIngressPending:
    """Frame-bound proof that one callback is executing asynchronously."""

    runtime_id: str
    consumer_id: str
    operation: str
    lifecycle_generation: int
    attempt_token: int
    symbol: str | None
    ingress_generation: int | None
    ingress_sequence: int | None
    market_data_id: str | None
    pending_identity_sha256: str
    completion_signal: MiniQMTKernelProductIngressCompletionSignal = field(repr=False, compare=False)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "miniqmt_kernel_product_ingress_pending_v1",
            "runtime_id": self.runtime_id,
            "consumer_id": self.consumer_id,
            "operation": self.operation,
            "disposition": "ASYNC_IN_FLIGHT",
            "lifecycle_generation": self.lifecycle_generation,
            "attempt_token": self.attempt_token,
            "symbol": self.symbol,
            "ingress_generation": self.ingress_generation,
            "ingress_sequence": self.ingress_sequence,
            "market_data_id": self.market_data_id,
            "pending_identity_sha256": self.pending_identity_sha256,
            "executed": True,
            "outcome_pending": True,
            "business_success": None,
        }


MiniQMTKernelProductIngressResult = MiniQMTKernelProductIngressSuppression | MiniQMTKernelProductIngressPending | None


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
        self._drop_count_by_reason: dict[str, int] = {}
        self._last_drop: dict[str, Any] | None = None

    def admit(self, symbols: tuple[str, ...]) -> None:
        normalized = tuple(dict.fromkeys(str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()))
        if not normalized:
            raise quote_contract_error(
                QuoteContractReasonCode.SYMBOL_INVALID, "mailbox admission requires non-empty symbols"
            )
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
                frame = self._slots.pop(symbol, None)
                if frame is not None:
                    self._record_drop_locked(frame, reason="SYMBOL_REVOKED")
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
            for frame in self._slots.values():
                self._record_drop_locked(frame, reason="GENERATION_ACTIVATED_PURGE")
            self._slots.clear()
            self._queued.clear()
            self._tokens.clear()
            self._condition.notify_all()

    def fence_generation(self, generation: int) -> None:
        with self._condition:
            self._generation_floor = max(self._generation_floor, generation + 1)
            for frame in self._slots.values():
                self._record_drop_locked(frame, reason="GENERATION_FENCED")
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
                self._record_drop_locked(frame, reason="MAILBOX_STALE_GENERATION")
                return "STALE_GENERATION"
            current = self._slots.get(frame.symbol)
            if current is not None and not self._is_newer(frame, current):
                self._ordering_rejected_count += 1
                self._record_drop_locked(frame, reason="MAILBOX_ORDERING_REJECTED")
                return "ORDERING_REJECTED"
            if current is not None:
                self._record_drop_locked(current, reason="MAILBOX_COALESCED_SUPERSEDED")
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
                "drop_count_by_reason": dict(sorted(self._drop_count_by_reason.items())),
                "last_drop": dict(self._last_drop) if self._last_drop is not None else None,
            }

    def record_lifecycle_drop(self, frame: RawQuoteFrame, *, reason: str) -> None:
        with self._condition:
            self._record_drop_locked(frame, reason=reason)

    def _record_drop_locked(self, frame: RawQuoteFrame, *, reason: str) -> None:
        self._drop_count_by_reason[reason] = self._drop_count_by_reason.get(reason, 0) + 1
        self._last_drop = {
            "reason": reason,
            "symbol": frame.symbol,
            "generation": frame.ingress_generation,
            "sequence": frame.ingress_sequence,
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


@dataclass(frozen=True)
class _ContextualObservationSinkOwner:
    """Exact process-local ownership for one contextual quote sink."""

    consumer_id: str
    symbols: tuple[str, ...]
    sink: Callable[
        [NormalizedQuoteObservation, QuoteEvaluationContext],
        MiniQMTKernelProductIngressResult,
    ]
    registration_generation: int
    ownership_sha256: str


@dataclass
class _QuoteFailureBucket:
    """Bounded process-local diagnostics for one low-cardinality failure identity."""

    fingerprint_sha256: str
    runtime_id: str
    generation: int | None
    consumer_id: str
    stage: str
    reason_code: str
    exception_type: str
    message_sha256: str
    first_observed_at: str
    last_observed_at: str
    last_observed_monotonic_ns: int
    last_emitted_monotonic_ns: int
    occurrence_count: int = 0
    active_occurrence_count: int = 0
    suppressed_since_emit: int = 0
    emitted_count: int = 0
    recovery_count: int = 0
    active: bool = True
    symbol_samples: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _QuoteFailureDecision:
    emit: bool
    log_payload: Mapping[str, Any]
    snapshot: Mapping[str, Any]


class _ProcessLocalQuoteFailureGovernor:
    """Shared, bounded failure fingerprint governor for the MiniQMT quote plane."""

    _SCHEMA_VERSION = "miniqmt_quote_failure_governor_v1"

    def __init__(
        self,
        *,
        loud_interval_seconds: int,
        max_fingerprints: int = 256,
        max_symbol_samples: int = 3,
    ) -> None:
        if loud_interval_seconds <= 0 or max_fingerprints <= 0 or max_symbol_samples <= 0:
            raise ValueError("quote failure governor bounds must be positive")
        self._loud_interval_ns = loud_interval_seconds * 1_000_000_000
        self._max_fingerprints = max_fingerprints
        self._max_symbol_samples = max_symbol_samples
        self._lock = threading.RLock()
        self._buckets: dict[str, _QuoteFailureBucket] = {}
        self._observed_count = 0
        self._suppressed_count = 0
        self._emitted_count = 0
        self._recovery_count = 0
        self._evicted_count = 0

    @staticmethod
    def _fingerprint(
        *,
        runtime_id: str,
        generation: int | None,
        consumer_id: str,
        stage: str,
        reason_code: str,
        exception_type: str,
        message_sha256: str,
    ) -> str:
        identity = "\x1f".join(
            (
                runtime_id,
                "" if generation is None else str(generation),
                consumer_id,
                stage,
                reason_code,
                exception_type,
                message_sha256,
            )
        )
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    @staticmethod
    def _snapshot(bucket: _QuoteFailureBucket) -> dict[str, Any]:
        return {
            "schema_version": _ProcessLocalQuoteFailureGovernor._SCHEMA_VERSION,
            "fingerprint_sha256": bucket.fingerprint_sha256,
            "runtime_id": bucket.runtime_id,
            "generation": bucket.generation,
            "consumer_id": bucket.consumer_id,
            "stage": bucket.stage,
            "reason_code": bucket.reason_code,
            "exception_type": bucket.exception_type,
            "message_sha256": bucket.message_sha256,
            "first_observed_at": bucket.first_observed_at,
            "last_observed_at": bucket.last_observed_at,
            "occurrence_count": bucket.occurrence_count,
            "active_occurrence_count": bucket.active_occurrence_count,
            "suppressed_since_emit": bucket.suppressed_since_emit,
            "emitted_count": bucket.emitted_count,
            "recovery_count": bucket.recovery_count,
            "active": bucket.active,
            "symbol_samples": list(bucket.symbol_samples),
        }

    def record(
        self,
        *,
        runtime_id: str,
        generation: int | None,
        consumer_id: str,
        stage: str,
        error: QuoteContractError,
        exception_type: str,
        symbol: str | None,
        now_monotonic_ns: int | None = None,
    ) -> _QuoteFailureDecision:
        now_ns = time.monotonic_ns() if now_monotonic_ns is None else now_monotonic_ns
        now_utc = datetime.now(UTC).isoformat()
        message_sha256 = hashlib.sha256(error.message.encode("utf-8")).hexdigest()
        fingerprint = self._fingerprint(
            runtime_id=runtime_id,
            generation=generation,
            consumer_id=consumer_id,
            stage=stage,
            reason_code=error.reason_code.value,
            exception_type=exception_type,
            message_sha256=message_sha256,
        )
        with self._lock:
            bucket = self._buckets.get(fingerprint)
            first_or_recovered = bucket is None or not bucket.active
            if bucket is None:
                if len(self._buckets) >= self._max_fingerprints:
                    evicted_fingerprint = min(
                        self._buckets,
                        key=lambda item: (
                            self._buckets[item].active,
                            self._buckets[item].last_observed_monotonic_ns,
                        ),
                    )
                    self._buckets.pop(evicted_fingerprint)
                    self._evicted_count += 1
                bucket = _QuoteFailureBucket(
                    fingerprint_sha256=fingerprint,
                    runtime_id=runtime_id,
                    generation=generation,
                    consumer_id=consumer_id,
                    stage=stage,
                    reason_code=error.reason_code.value,
                    exception_type=exception_type,
                    message_sha256=message_sha256,
                    first_observed_at=now_utc,
                    last_observed_at=now_utc,
                    last_observed_monotonic_ns=now_ns,
                    last_emitted_monotonic_ns=0,
                )
                self._buckets[fingerprint] = bucket
            elif not bucket.active:
                bucket.active = True
                bucket.active_occurrence_count = 0
                bucket.suppressed_since_emit = 0
                bucket.message_sha256 = message_sha256
            bucket.last_observed_at = now_utc
            bucket.last_observed_monotonic_ns = now_ns
            bucket.occurrence_count += 1
            bucket.active_occurrence_count += 1
            self._observed_count += 1
            if symbol and symbol not in bucket.symbol_samples and len(bucket.symbol_samples) < self._max_symbol_samples:
                bucket.symbol_samples.append(symbol)
            interval_expired = (
                bucket.last_emitted_monotonic_ns > 0
                and now_ns - bucket.last_emitted_monotonic_ns >= self._loud_interval_ns
            )
            emit = first_or_recovered or interval_expired
            if emit:
                event = "FIRST_FAILURE" if first_or_recovered else "AGGREGATE_FAILURE"
                suppressed_for_event = bucket.suppressed_since_emit
                bucket.suppressed_since_emit = 0
                bucket.last_emitted_monotonic_ns = now_ns
                bucket.emitted_count += 1
                self._emitted_count += 1
            else:
                event = "SUPPRESSED"
                bucket.suppressed_since_emit += 1
                suppressed_for_event = bucket.suppressed_since_emit
                self._suppressed_count += 1
            snapshot = self._snapshot(bucket)
            log_payload = {
                **snapshot,
                "event": event,
                "suppressed_occurrence_count": suppressed_for_event,
            }
        return _QuoteFailureDecision(emit=emit, log_payload=log_payload, snapshot=snapshot)

    def resolve(
        self,
        *,
        runtime_id: str | None,
        consumer_id: str,
        generation: int | None = None,
        stage: str | None = None,
    ) -> int:
        resolved = 0
        with self._lock:
            for bucket in self._buckets.values():
                if (
                    not bucket.active
                    or (runtime_id is not None and bucket.runtime_id != runtime_id)
                    or bucket.consumer_id != consumer_id
                    or (generation is not None and bucket.generation != generation)
                    or (stage is not None and bucket.stage != stage)
                ):
                    continue
                bucket.active = False
                bucket.recovery_count += 1
                bucket.suppressed_since_emit = 0
                resolved += 1
            self._recovery_count += resolved
        return resolved

    def health(self) -> dict[str, Any]:
        with self._lock:
            fingerprints = {
                fingerprint: self._snapshot(bucket) for fingerprint, bucket in sorted(self._buckets.items())
            }
            return {
                "schema_version": self._SCHEMA_VERSION,
                "max_fingerprints": self._max_fingerprints,
                "max_symbol_samples": self._max_symbol_samples,
                "tracked_fingerprint_count": len(self._buckets),
                "active_failure_count": sum(bucket.active for bucket in self._buckets.values()),
                "observed_count": self._observed_count,
                "suppressed_count": self._suppressed_count,
                "emitted_count": self._emitted_count,
                "recovery_count": self._recovery_count,
                "evicted_count": self._evicted_count,
                "fingerprints": fingerprints,
            }


class PhaseOneQuoteProjectionSink:
    """P1-C same-writer raw-to-normalized projection; never calls providers or a broker."""

    def __init__(
        self,
        *,
        raw_store: PhaseOneRawQuoteSnapshotStore,
        normalized_store: BoundedNormalizedQuoteStore,
        context_store: QuoteEvaluationContextStore,
        loud_sink: Callable[[QuoteContractError], None] | None = None,
        observation_sink: Callable[[NormalizedQuoteObservation], MiniQMTKernelProductIngressResult] | None = None,
        failure_governor: _ProcessLocalQuoteFailureGovernor | None = None,
        loud_interval_seconds: int = 30,
    ) -> None:
        self._raw_store = raw_store
        self._normalized_store = normalized_store
        self._context_store = context_store
        self._ordering = QuoteOrderingTracker()
        self._loud_sink = loud_sink
        self._failure_governor = failure_governor or _ProcessLocalQuoteFailureGovernor(
            loud_interval_seconds=loud_interval_seconds
        )
        self._observation_sinks: dict[
            str,
            Callable[[NormalizedQuoteObservation], MiniQMTKernelProductIngressResult],
        ] = {}
        self._contextual_observation_sinks: dict[
            str,
            _ContextualObservationSinkOwner,
        ] = {}
        self._contextual_sink_registration_generation = 0
        if observation_sink is not None:
            self._observation_sinks["initial"] = observation_sink
        self._lock = threading.RLock()
        self._last_error_by_symbol: dict[str, dict[str, Any]] = {}
        self._last_suppression_by_symbol: dict[str, dict[str, Any]] = {}
        self._active_pending_by_owner: dict[tuple[str, str], dict[str, Any]] = {}
        self._last_completion_by_owner: dict[tuple[str, str], dict[str, Any]] = {}
        self._pending_drop_count_by_reason: dict[str, int] = {}
        self._last_pending_drop: dict[str, Any] | None = None
        self._accepted_count = 0
        self._rejected_count = 0
        self._suppressed_count = 0
        self._pending_count = 0
        self._pending_completion_count = 0

    @property
    def normalized_store(self) -> BoundedNormalizedQuoteStore:
        return self._normalized_store

    def replace_admitted(self, symbols: tuple[str, ...]) -> None:
        exact_symbols = self._exact_symbol_ownership(symbols, allow_empty=True)
        with self._lock:
            self._validated_contextual_sink_snapshot_locked()
        self._raw_store.replace_admitted(exact_symbols)
        self._normalized_store.replace_admitted(exact_symbols)
        admitted = frozenset(exact_symbols)
        with self._lock:
            self._last_error_by_symbol = {
                symbol: payload for symbol, payload in self._last_error_by_symbol.items() if symbol in admitted
            }
            self._last_suppression_by_symbol = {
                symbol: payload for symbol, payload in self._last_suppression_by_symbol.items() if symbol in admitted
            }
            for owner_key, payload in tuple(self._active_pending_by_owner.items()):
                if owner_key[1] not in admitted:
                    self._record_projection_pending_drop_locked(
                        payload,
                        reason="PROJECTION_PENDING_SYMBOL_REVOKED",
                    )
                    self._active_pending_by_owner.pop(owner_key, None)
            self._last_completion_by_owner = {
                owner_key: payload
                for owner_key, payload in self._last_completion_by_owner.items()
                if owner_key[1] in admitted
            }

    def register_observation_sink(
        self,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
        sink: Callable[
            [NormalizedQuoteObservation, QuoteEvaluationContext],
            MiniQMTKernelProductIngressResult,
        ],
    ) -> None:
        exact_consumer_id = self._exact_consumer_id(consumer_id)
        exact_symbols = self._exact_symbol_ownership(symbols, allow_empty=False)
        if not callable(sink):
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "observation sink registration requires a callable sink",
                context={"consumer_id": exact_consumer_id},
            )
        with self._lock:
            self._validated_contextual_sink_snapshot_locked()
            if exact_consumer_id in self._observation_sinks or exact_consumer_id in self._contextual_observation_sinks:
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                    "observation sink consumer id is already registered",
                    context={"consumer_id": exact_consumer_id},
                )
            self._contextual_sink_registration_generation += 1
            registration_generation = self._contextual_sink_registration_generation
            owner = _ContextualObservationSinkOwner(
                consumer_id=exact_consumer_id,
                symbols=exact_symbols,
                sink=sink,
                registration_generation=registration_generation,
                ownership_sha256=self._contextual_sink_ownership_sha256(
                    consumer_id=exact_consumer_id,
                    symbols=exact_symbols,
                    registration_generation=registration_generation,
                ),
            )
            for owner_key, payload in tuple(self._active_pending_by_owner.items()):
                if owner_key[0] != exact_consumer_id:
                    continue
                self._record_projection_pending_drop_locked(
                    payload,
                    reason="PROJECTION_PENDING_OWNER_REREGISTERED",
                )
                self._active_pending_by_owner.pop(owner_key, None)
            for owner_key in tuple(self._last_completion_by_owner):
                if owner_key[0] == exact_consumer_id:
                    self._last_completion_by_owner.pop(owner_key, None)
            self._contextual_observation_sinks[exact_consumer_id] = owner

    def unregister_observation_sink(
        self,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
        sink: Callable[
            [NormalizedQuoteObservation, QuoteEvaluationContext],
            MiniQMTKernelProductIngressResult,
        ],
    ) -> bool:
        exact_consumer_id = self._exact_consumer_id(consumer_id)
        exact_symbols = self._exact_symbol_ownership(symbols, allow_empty=False)
        if not callable(sink):
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "observation sink unregistration requires a callable sink",
                context={"consumer_id": exact_consumer_id},
            )
        with self._lock:
            self._validated_contextual_sink_snapshot_locked()
            owner = self._contextual_observation_sinks.get(exact_consumer_id)
            if owner is None:
                return False
            self._assert_contextual_sink_identity(
                owner=owner,
                consumer_id=exact_consumer_id,
                symbols=exact_symbols,
                sink=sink,
                operation="UNREGISTER",
            )
            del self._contextual_observation_sinks[exact_consumer_id]
            for owner_key, payload in tuple(self._active_pending_by_owner.items()):
                if owner_key[0] != exact_consumer_id:
                    continue
                self._record_projection_pending_drop_locked(
                    payload,
                    reason="PROJECTION_PENDING_SINK_UNREGISTERED",
                )
                self._active_pending_by_owner.pop(owner_key, None)
            for owner_key in tuple(self._last_completion_by_owner):
                if owner_key[0] == exact_consumer_id:
                    self._last_completion_by_owner.pop(owner_key, None)
            return True

    def get_observation_sink(
        self,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
    ) -> (
        Callable[
            [NormalizedQuoteObservation, QuoteEvaluationContext],
            MiniQMTKernelProductIngressResult,
        ]
        | None
    ):
        exact_consumer_id = self._exact_consumer_id(consumer_id)
        exact_symbols = self._exact_symbol_ownership(symbols, allow_empty=False)
        with self._lock:
            self._validated_contextual_sink_snapshot_locked()
            owner = self._contextual_observation_sinks.get(exact_consumer_id)
            if owner is None:
                return None
            self._assert_contextual_sink_identity(
                owner=owner,
                consumer_id=exact_consumer_id,
                symbols=exact_symbols,
                operation="READBACK",
            )
            return owner.sink

    def shutdown_observation_sinks(self) -> int:
        """Remove every process-local sink only after its owner has fenced deliveries."""

        with self._lock:
            self._validated_contextual_sink_snapshot_locked()
            removed = len(self._observation_sinks) + len(self._contextual_observation_sinks)
            for payload in self._active_pending_by_owner.values():
                self._record_projection_pending_drop_locked(
                    payload,
                    reason="PROJECTION_PENDING_SINK_SHUTDOWN",
                )
            self._active_pending_by_owner.clear()
            self._last_completion_by_owner.clear()
            self._observation_sinks.clear()
            self._contextual_observation_sinks.clear()
        return removed

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
            self._resolve_failure(
                frame=frame,
                consumer_id="quote-projection",
                stage="PROJECTION",
            )
            observation_sink_failed = False
            observation_sink_pending = False
            with self._lock:
                observation_sinks = tuple(self._observation_sinks.items())
                contextual_observation_sink_owners = self._validated_contextual_sink_snapshot_locked()
            for consumer_id, observation_sink in observation_sinks:
                try:
                    result = observation_sink(observation)
                    carrier = self._validated_sink_result(
                        result,
                        consumer_id=consumer_id,
                        frame=frame,
                        observation=observation,
                        values=(observation,),
                    )
                    if isinstance(carrier, MiniQMTKernelProductIngressSuppression):
                        observation_sink_failed = True
                        self._record_suppression(
                            frame=frame,
                            carrier=carrier,
                        )
                    elif isinstance(carrier, MiniQMTKernelProductIngressPending):
                        observation_sink_pending = (
                            self._record_pending(
                                frame=frame,
                                carrier=carrier,
                                expected_owner=observation_sink,
                            )
                            or observation_sink_pending
                        )
                    else:
                        self._resolve_failure(
                            frame=frame,
                            consumer_id=consumer_id,
                            stage="OBSERVATION",
                        )
                except QuoteContractError as error:
                    observation_sink_failed = True
                    self._record_loud(
                        frame=frame,
                        error=error,
                        consumer_id=consumer_id,
                        stage="OBSERVATION",
                    )
                except Exception as exc:  # noqa: BLE001 - observation reporting must not rewrite quote state.
                    observation_sink_failed = True
                    self._record_loud(
                        frame=frame,
                        error=quote_contract_error(
                            QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
                            "quote observation sink raised unexpectedly",
                            context={
                                "symbol": frame.symbol,
                                "consumer_id": consumer_id,
                                "exception_type": type(exc).__name__,
                            },
                        ),
                        consumer_id=consumer_id,
                        stage="OBSERVATION",
                        exception_type=type(exc).__qualname__,
                    )
            deferred_contextual_sinks: list[tuple[_ContextualObservationSinkOwner, Callable[..., Any], Any]] = []
            synchronous_contextual_sinks: list[_ContextualObservationSinkOwner] = []
            for owner in contextual_observation_sink_owners:
                if frame.symbol not in owner.symbols:
                    continue
                enqueue = getattr(owner.sink, "enqueue_kernel_product_callback_v1", None)
                await_result = getattr(owner.sink, "await_kernel_product_callback_v1", None)
                if callable(enqueue) and callable(await_result):
                    try:
                        dispatch = enqueue(observation, context)
                    except QuoteContractError as error:
                        observation_sink_failed = True
                        self._record_loud(
                            frame=frame,
                            error=error,
                            consumer_id=owner.consumer_id,
                            stage="ENQUEUE",
                        )
                    except Exception as exc:  # noqa: BLE001 - all peers must still receive the frame.
                        observation_sink_failed = True
                        self._record_contextual_sink_exception(
                            frame=frame,
                            context=context,
                            consumer_id=owner.consumer_id,
                            exception=exc,
                            stage="ENQUEUE",
                        )
                    else:
                        self._resolve_failure(
                            frame=frame,
                            consumer_id=owner.consumer_id,
                            stage="ENQUEUE",
                        )
                        deferred_contextual_sinks.append((owner, await_result, dispatch))
                elif callable(enqueue) or callable(await_result):
                    observation_sink_failed = True
                    self._record_loud(
                        frame=frame,
                        error=quote_contract_error(
                            QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
                            "contextual quote sink exposes an incomplete asynchronous dispatch contract",
                            context={"consumer_id": owner.consumer_id, "symbol": frame.symbol},
                        ),
                        consumer_id=owner.consumer_id,
                        stage="DISPATCH_CONTRACT",
                    )
                else:
                    synchronous_contextual_sinks.append(owner)

            fanout_deadline = time.monotonic() + _KERNEL_CONTEXTUAL_FANOUT_WAIT_SECONDS
            for owner, await_result, dispatch in deferred_contextual_sinks:
                consumer_id = owner.consumer_id
                try:
                    result = await_result(
                        dispatch=dispatch,
                        timeout_seconds=max(0.0, fanout_deadline - time.monotonic()),
                    )
                    carrier = self._validated_sink_result(
                        result,
                        consumer_id=consumer_id,
                        frame=frame,
                        observation=observation,
                        values=(observation, context),
                    )
                    if isinstance(carrier, MiniQMTKernelProductIngressSuppression):
                        observation_sink_failed = True
                        self._record_suppression(
                            frame=frame,
                            carrier=carrier,
                        )
                    elif isinstance(carrier, MiniQMTKernelProductIngressPending):
                        observation_sink_pending = (
                            self._record_pending(
                                frame=frame,
                                carrier=carrier,
                                expected_owner=owner,
                            )
                            or observation_sink_pending
                        )
                    else:
                        self._resolve_failure(
                            frame=frame,
                            consumer_id=consumer_id,
                            stage="AWAIT",
                        )
                except QuoteContractError as error:
                    observation_sink_failed = True
                    self._record_loud(
                        frame=frame,
                        error=error,
                        consumer_id=consumer_id,
                        stage="AWAIT",
                    )
                except Exception as exc:  # noqa: BLE001 - all peers were already enqueued before this wait.
                    observation_sink_failed = True
                    self._record_contextual_sink_exception(
                        frame=frame,
                        context=context,
                        consumer_id=consumer_id,
                        exception=exc,
                        stage="AWAIT",
                    )
            for owner in synchronous_contextual_sinks:
                consumer_id = owner.consumer_id
                try:
                    result = owner.sink(observation, context)
                    carrier = self._validated_sink_result(
                        result,
                        consumer_id=consumer_id,
                        frame=frame,
                        observation=observation,
                        values=(observation, context),
                    )
                    if isinstance(carrier, MiniQMTKernelProductIngressSuppression):
                        observation_sink_failed = True
                        self._record_suppression(frame=frame, carrier=carrier)
                    elif isinstance(carrier, MiniQMTKernelProductIngressPending):
                        observation_sink_pending = (
                            self._record_pending(
                                frame=frame,
                                carrier=carrier,
                                expected_owner=owner,
                            )
                            or observation_sink_pending
                        )
                    else:
                        self._resolve_failure(
                            frame=frame,
                            consumer_id=consumer_id,
                            stage="SYNCHRONOUS",
                        )
                except QuoteContractError as error:
                    observation_sink_failed = True
                    self._record_loud(
                        frame=frame,
                        error=error,
                        consumer_id=consumer_id,
                        stage="SYNCHRONOUS",
                    )
                except Exception as exc:  # noqa: BLE001 - generic non-kernel observers retain loud isolation.
                    observation_sink_failed = True
                    self._record_contextual_sink_exception(
                        frame=frame,
                        context=context,
                        consumer_id=consumer_id,
                        exception=exc,
                        stage="SYNCHRONOUS",
                    )
            with self._lock:
                self._accepted_count += 1
                if not observation_sink_failed and not observation_sink_pending:
                    self._last_error_by_symbol.pop(frame.symbol, None)
                    self._last_suppression_by_symbol.pop(frame.symbol, None)
        except QuoteContractError as error:
            self._record_loud(
                frame=frame,
                error=error,
                consumer_id="quote-projection",
                stage="PROJECTION",
            )

    def health(self) -> dict[str, Any]:
        with self._lock:
            contextual_observation_sink_owners = self._validated_contextual_sink_snapshot_locked()
            errors = {symbol: dict(payload) for symbol, payload in self._last_error_by_symbol.items()}
            suppressions = {symbol: dict(payload) for symbol, payload in self._last_suppression_by_symbol.items()}
            pending_by_owner = {
                owner_key: dict(payload) for owner_key, payload in self._active_pending_by_owner.items()
            }
            completions_by_owner = {
                owner_key: dict(payload) for owner_key, payload in self._last_completion_by_owner.items()
            }
            accepted_count = self._accepted_count
            rejected_count = self._rejected_count
            suppressed_count = self._suppressed_count
            pending_count = self._pending_count
            pending_completion_count = self._pending_completion_count
            pending_drop_count_by_reason = dict(sorted(self._pending_drop_count_by_reason.items()))
            last_pending_drop = dict(self._last_pending_drop) if self._last_pending_drop is not None else None
        active_pending_by_consumer: dict[str, dict[str, dict[str, Any]]] = {}
        active_pending_by_symbol: dict[str, dict[str, dict[str, Any]]] = {}
        for (consumer_id, symbol), pending_payload in sorted(pending_by_owner.items()):
            active_pending_by_consumer.setdefault(consumer_id, {})[symbol] = pending_payload
            active_pending_by_symbol.setdefault(symbol, {})[consumer_id] = pending_payload
        completion_by_consumer: dict[str, dict[str, dict[str, Any]]] = {}
        for (consumer_id, symbol), completion_payload in sorted(completions_by_owner.items()):
            completion_by_consumer.setdefault(consumer_id, {})[symbol] = completion_payload
        return {
            "projection": {
                "accepted_count": accepted_count,
                "rejected_count": rejected_count,
                "suppressed_count": suppressed_count,
                "pending_count": pending_count,
                "active_pending_count": len(pending_by_owner),
                "pending_completion_count": pending_completion_count,
                "last_error_by_symbol": errors,
                "last_suppression_by_symbol": suppressions,
                "last_pending_by_owner": active_pending_by_consumer,
                "last_pending_by_symbol": active_pending_by_symbol,
                "last_completion_by_owner": completion_by_consumer,
                "pending_drop_count_by_reason": pending_drop_count_by_reason,
                "last_pending_drop": last_pending_drop,
                "ordering": self._ordering.health(),
                "normalized_store": self._normalized_store.health(),
                "context": self._context_store.health(),
                "contextual_observation_sink_owners": {
                    owner.consumer_id: {
                        "schema_version": "miniqmt_contextual_observation_sink_owner_v1",
                        "consumer_id": owner.consumer_id,
                        "symbols": list(owner.symbols),
                        "symbol_count": len(owner.symbols),
                        "registration_generation": owner.registration_generation,
                        "ownership_sha256": owner.ownership_sha256,
                        "sink_registered": True,
                    }
                    for owner in contextual_observation_sink_owners
                },
                "failure_governor": self._failure_governor.health(),
            }
        }

    @staticmethod
    def _exact_consumer_id(consumer_id: object) -> str:
        if type(consumer_id) is not str or not consumer_id or consumer_id != consumer_id.strip():
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "observation sink ownership requires an exact non-empty consumer identity",
                context={"consumer_id_type": type(consumer_id).__qualname__},
            )
        return consumer_id

    @staticmethod
    def _exact_symbol_ownership(symbols: object, *, allow_empty: bool) -> tuple[str, ...]:
        if type(symbols) is not tuple:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "observation sink ownership symbols must be an exact tuple",
                context={"symbols_type": type(symbols).__qualname__},
            )
        if not symbols and not allow_empty:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "observation sink ownership requires at least one symbol",
            )
        exact_symbols: list[str] = []
        for index, symbol in enumerate(symbols):
            if type(symbol) is not str or not symbol or symbol != symbol.strip():
                raise quote_contract_error(
                    QuoteContractReasonCode.PAYLOAD_INVALID,
                    "observation sink ownership contains a non-exact symbol",
                    context={"symbol_index": index, "symbol_type": type(symbol).__qualname__},
                )
            exact_symbols.append(symbol)
        if len(set(exact_symbols)) != len(exact_symbols):
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "observation sink ownership symbols must be unique",
                context={"symbol_count": len(exact_symbols)},
            )
        return tuple(exact_symbols)

    @staticmethod
    def _contextual_sink_ownership_sha256(
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
        registration_generation: int,
    ) -> str:
        hasher = hashlib.sha256()
        for component in (
            "miniqmt_contextual_observation_sink_owner_v1",
            consumer_id,
            str(registration_generation),
            str(len(symbols)),
            *symbols,
        ):
            encoded = component.encode("utf-8")
            hasher.update(len(encoded).to_bytes(8, byteorder="big", signed=False))
            hasher.update(encoded)
        return hasher.hexdigest()

    def _validated_contextual_sink_snapshot_locked(self) -> tuple[_ContextualObservationSinkOwner, ...]:
        owners: list[_ContextualObservationSinkOwner] = []
        for registry_consumer_id, owner in self._contextual_observation_sinks.items():
            if type(owner) is not _ContextualObservationSinkOwner:
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                    "contextual observation sink registry contains an invalid owner carrier",
                    context={"consumer_id": registry_consumer_id, "owner_type": type(owner).__qualname__},
                )
            exact_consumer_id = self._exact_consumer_id(owner.consumer_id)
            exact_symbols = self._exact_symbol_ownership(owner.symbols, allow_empty=False)
            if type(owner.registration_generation) is not int or owner.registration_generation <= 0:
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                    "contextual observation sink registration generation is invalid",
                    context={"consumer_id": registry_consumer_id},
                )
            expected_hash = self._contextual_sink_ownership_sha256(
                consumer_id=exact_consumer_id,
                symbols=exact_symbols,
                registration_generation=owner.registration_generation,
            )
            if (
                registry_consumer_id != exact_consumer_id
                or not callable(owner.sink)
                or owner.ownership_sha256 != expected_hash
            ):
                raise quote_contract_error(
                    QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                    "contextual observation sink ownership identity is inconsistent",
                    context={
                        "registry_consumer_id": registry_consumer_id,
                        "owner_consumer_id": exact_consumer_id,
                        "expected_ownership_sha256": expected_hash,
                        "actual_ownership_sha256": owner.ownership_sha256,
                        "sink_callable": callable(owner.sink),
                    },
                )
            owners.append(owner)
        return tuple(owners)

    @staticmethod
    def _assert_contextual_sink_identity(
        *,
        owner: _ContextualObservationSinkOwner,
        consumer_id: str,
        symbols: tuple[str, ...],
        operation: str,
        sink: Callable[
            [NormalizedQuoteObservation, QuoteEvaluationContext],
            MiniQMTKernelProductIngressResult,
        ]
        | None = None,
    ) -> None:
        expected_hash = PhaseOneQuoteProjectionSink._contextual_sink_ownership_sha256(
            consumer_id=consumer_id,
            symbols=symbols,
            registration_generation=owner.registration_generation,
        )
        if owner.ownership_sha256 != expected_hash or (sink is not None and owner.sink is not sink):
            raise quote_contract_error(
                QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT,
                "contextual observation sink ownership does not match the exact requested identity",
                context={
                    "consumer_id": consumer_id,
                    "operation": operation,
                    "requested_ownership_sha256": expected_hash,
                    "registered_ownership_sha256": owner.ownership_sha256,
                    "sink_identity_match": sink is None or owner.sink is sink,
                },
            )

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

    @classmethod
    def _validated_sink_result(
        cls,
        result: Any,
        *,
        consumer_id: str,
        frame: RawQuoteFrame,
        observation: NormalizedQuoteObservation,
        values: tuple[Any, ...],
    ) -> MiniQMTKernelProductIngressResult:
        if type(result) is MiniQMTKernelProductIngressPending:
            return cls._validated_sink_pending(
                result,
                consumer_id=consumer_id,
                frame=frame,
                observation=observation,
                values=values,
            )
        return cls._validated_sink_suppression(
            result,
            consumer_id=consumer_id,
            frame=frame,
            observation=observation,
            values=values,
        )

    @staticmethod
    def _validated_sink_suppression(
        result: Any,
        *,
        consumer_id: str,
        frame: RawQuoteFrame,
        observation: NormalizedQuoteObservation,
        values: tuple[Any, ...],
    ) -> MiniQMTKernelProductIngressSuppression | None:
        if result is None:
            return None
        if type(result) is not MiniQMTKernelProductIngressSuppression:
            raise quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
                "quote observation sink returned an unregistered non-None result carrier",
                context={"consumer_id": consumer_id, "result_type": type(result).__qualname__},
            )
        payload = result.as_dict()
        required = {
            "schema_version",
            "runtime_id",
            "consumer_id",
            "operation",
            "disposition",
            "lifecycle_generation",
            "symbol",
            "ingress_generation",
            "ingress_sequence",
            "market_data_id",
            "failure_fingerprint_sha256",
            "next_retry_at_utc",
            "pending_identity_sha256",
            "executed",
            "business_success",
        }
        lifecycle_generation = payload.get("lifecycle_generation") if isinstance(payload, dict) else None
        failure_sha256 = payload.get("failure_fingerprint_sha256") if isinstance(payload, dict) else None
        pending_sha256 = payload.get("pending_identity_sha256") if isinstance(payload, dict) else None
        next_retry_at_utc = payload.get("next_retry_at_utc") if isinstance(payload, dict) else None
        disposition = payload.get("disposition") if isinstance(payload, dict) else None
        runtime_id = payload.get("runtime_id") if isinstance(payload, dict) else None
        expected_consumer_id = f"k6d-kernel-v2:{runtime_id}" if type(runtime_id) is str else None
        expected_pending_identity = (
            kernel_product_pending_identity_sha256_v1(
                runtime_id=runtime_id,
                symbol=frame.symbol,
                market_data_id=observation.market_data_id,
                ingress_generation=frame.ingress_generation,
                ingress_sequence=frame.ingress_sequence,
                context_id=observation.context_id,
                values=values,
            )
            if type(runtime_id) is str and runtime_id
            else None
        )

        def valid_sha256(value: Any, *, nullable: bool) -> bool:
            if value is None:
                return nullable
            return (
                type(value) is str
                and len(value) == 64
                and value == value.lower()
                and all(character in "0123456789abcdef" for character in value)
            )

        retry_timestamp_valid = next_retry_at_utc is None
        if type(next_retry_at_utc) is str:
            try:
                parsed_retry_at = datetime.fromisoformat(next_retry_at_utc)
            except ValueError:
                retry_timestamp_valid = False
            else:
                retry_timestamp_valid = (
                    parsed_retry_at.tzinfo is not None
                    and parsed_retry_at.utcoffset() == UTC.utcoffset(None)
                    and next_retry_at_utc == parsed_retry_at.astimezone(UTC).isoformat()
                )
        if (
            not isinstance(payload, dict)
            or set(payload) != required
            or payload.get("schema_version") != "miniqmt_kernel_product_ingress_suppression_v1"
            or type(payload.get("runtime_id")) is not str
            or not payload["runtime_id"]
            or payload["runtime_id"] != payload["runtime_id"].strip()
            or payload.get("consumer_id") != consumer_id
            or payload.get("consumer_id") != expected_consumer_id
            or payload.get("operation") != "CALLBACK"
            or disposition
            not in {
                "LIFECYCLE_FENCED",
                "SINGLE_FLIGHT_SUPPRESSED",
                "RETRY_BACKOFF_SUPPRESSED",
            }
            or type(lifecycle_generation) is not int
            or lifecycle_generation <= 0
            or payload.get("symbol") != frame.symbol
            or type(payload.get("ingress_generation")) is not int
            or payload.get("ingress_generation") != frame.ingress_generation
            or type(payload.get("ingress_sequence")) is not int
            or payload.get("ingress_sequence") != frame.ingress_sequence
            or payload.get("market_data_id") != observation.market_data_id
            or not valid_sha256(failure_sha256, nullable=True)
            or not valid_sha256(pending_sha256, nullable=False)
            or pending_sha256 != expected_pending_identity
            or not retry_timestamp_valid
            or (next_retry_at_utc is not None and failure_sha256 is None)
            or (disposition == "RETRY_BACKOFF_SUPPRESSED" and (failure_sha256 is None or next_retry_at_utc is None))
            or payload.get("executed") is not False
            or payload.get("business_success") is not False
        ):
            raise quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
                "quote observation sink suppression carrier is malformed",
                context={"consumer_id": consumer_id, "result_type": type(result).__qualname__},
            )
        return result

    @staticmethod
    def _validated_sink_pending(
        result: MiniQMTKernelProductIngressPending,
        *,
        consumer_id: str,
        frame: RawQuoteFrame,
        observation: NormalizedQuoteObservation,
        values: tuple[Any, ...],
    ) -> MiniQMTKernelProductIngressPending:
        payload = result.as_dict()
        runtime_id = payload.get("runtime_id")
        expected_consumer_id = f"k6d-kernel-v2:{runtime_id}" if type(runtime_id) is str else None
        expected_pending_identity = (
            kernel_product_pending_identity_sha256_v1(
                runtime_id=runtime_id,
                symbol=frame.symbol,
                market_data_id=observation.market_data_id,
                ingress_generation=frame.ingress_generation,
                ingress_sequence=frame.ingress_sequence,
                context_id=observation.context_id,
                values=values,
            )
            if type(runtime_id) is str and runtime_id
            else None
        )
        required = {
            "schema_version",
            "runtime_id",
            "consumer_id",
            "operation",
            "disposition",
            "lifecycle_generation",
            "attempt_token",
            "symbol",
            "ingress_generation",
            "ingress_sequence",
            "market_data_id",
            "pending_identity_sha256",
            "executed",
            "outcome_pending",
            "business_success",
        }
        pending_sha256 = payload.get("pending_identity_sha256")
        signal_identity = (
            result.completion_signal.identity()
            if type(result.completion_signal) is MiniQMTKernelProductIngressCompletionSignal
            else None
        )
        expected_signal_identity = {
            field_name: payload.get(field_name)
            for field_name in (
                "runtime_id",
                "consumer_id",
                "operation",
                "lifecycle_generation",
                "attempt_token",
                "symbol",
                "ingress_generation",
                "ingress_sequence",
                "market_data_id",
                "pending_identity_sha256",
            )
        }
        if (
            set(payload) != required
            or payload.get("schema_version") != "miniqmt_kernel_product_ingress_pending_v1"
            or type(runtime_id) is not str
            or not runtime_id
            or runtime_id != runtime_id.strip()
            or payload.get("consumer_id") != consumer_id
            or payload.get("consumer_id") != expected_consumer_id
            or payload.get("operation") != "CALLBACK"
            or payload.get("disposition") != "ASYNC_IN_FLIGHT"
            or type(payload.get("lifecycle_generation")) is not int
            or payload["lifecycle_generation"] <= 0
            or type(payload.get("attempt_token")) is not int
            or payload["attempt_token"] <= 0
            or payload.get("symbol") != frame.symbol
            or type(payload.get("ingress_generation")) is not int
            or payload.get("ingress_generation") != frame.ingress_generation
            or type(payload.get("ingress_sequence")) is not int
            or payload.get("ingress_sequence") != frame.ingress_sequence
            or payload.get("market_data_id") != observation.market_data_id
            or type(pending_sha256) is not str
            or len(pending_sha256) != 64
            or pending_sha256 != pending_sha256.lower()
            or any(character not in "0123456789abcdef" for character in pending_sha256)
            or pending_sha256 != expected_pending_identity
            or payload.get("executed") is not True
            or payload.get("outcome_pending") is not True
            or payload.get("business_success") is not None
            or signal_identity != expected_signal_identity
        ):
            raise quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
                "quote observation sink pending carrier is malformed",
                context={"consumer_id": consumer_id, "result_type": type(result).__qualname__},
            )
        return result

    def _record_suppression(
        self,
        *,
        frame: RawQuoteFrame,
        carrier: MiniQMTKernelProductIngressSuppression,
    ) -> None:
        with self._lock:
            self._suppressed_count += 1
            self._last_suppression_by_symbol[frame.symbol] = carrier.as_dict()

    def _record_contextual_sink_exception(
        self,
        *,
        frame: RawQuoteFrame,
        context: QuoteEvaluationContext,
        consumer_id: str,
        exception: Exception,
        stage: str,
    ) -> None:
        self._record_loud(
            frame=frame,
            error=quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
                "contextual quote observation sink raised unexpectedly",
                context={
                    "symbol": frame.symbol,
                    "consumer_id": consumer_id,
                    "context_id": context.context_id,
                    "dispatch_stage": stage,
                    "exception_type": type(exception).__name__,
                },
            ),
            consumer_id=consumer_id,
            stage=stage,
            exception_type=type(exception).__qualname__,
        )

    def _record_pending(
        self,
        *,
        frame: RawQuoteFrame,
        carrier: MiniQMTKernelProductIngressPending,
        expected_owner: object,
    ) -> bool:
        payload = carrier.as_dict()
        owner_key = (carrier.consumer_id, frame.symbol)
        with self._lock:
            self._pending_count += 1
            if self._registered_sink_owner_locked(carrier.consumer_id) is not expected_owner:
                self._record_projection_pending_drop_locked(
                    payload,
                    reason="PROJECTION_PENDING_STALE_SINK_OWNER",
                )
                return False
            existing = self._active_pending_by_owner.get(owner_key)
            if existing is not None and (
                existing.get("pending_identity_sha256") != carrier.pending_identity_sha256
                or existing.get("attempt_token") != carrier.attempt_token
            ):
                self._record_projection_pending_drop_locked(
                    existing,
                    reason="PROJECTION_PENDING_OWNER_REPLACED",
                    replacement=payload,
                )
            self._active_pending_by_owner[owner_key] = payload
        carrier.completion_signal.subscribe(
            lambda completion: self._consume_pending_completion(
                owner_key=owner_key,
                expected=payload,
                expected_owner=expected_owner,
                completion=completion,
            )
        )
        with self._lock:
            current = self._active_pending_by_owner.get(owner_key)
            return bool(
                current is not None
                and current.get("pending_identity_sha256") == carrier.pending_identity_sha256
                and current.get("attempt_token") == carrier.attempt_token
            )

    def _consume_pending_completion(
        self,
        *,
        owner_key: tuple[str, str],
        expected: Mapping[str, Any],
        expected_owner: object,
        completion: MiniQMTKernelProductIngressCompletion,
    ) -> None:
        payload = completion.as_dict()
        identity_fields = (
            "runtime_id",
            "consumer_id",
            "operation",
            "lifecycle_generation",
            "attempt_token",
            "symbol",
            "ingress_generation",
            "ingress_sequence",
            "market_data_id",
            "pending_identity_sha256",
        )
        if any(payload.get(field_name) != expected.get(field_name) for field_name in identity_fields):
            raise quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
                "asynchronous quote completion differs from its exact pending owner",
                context={
                    "consumer_id": owner_key[0],
                    "symbol": owner_key[1],
                    "expected_pending_identity_sha256": expected.get("pending_identity_sha256"),
                    "actual_pending_identity_sha256": payload.get("pending_identity_sha256"),
                    "expected_attempt_token": expected.get("attempt_token"),
                    "actual_attempt_token": payload.get("attempt_token"),
                },
            )
        with self._lock:
            current = self._active_pending_by_owner.get(owner_key)
            if self._registered_sink_owner_locked(owner_key[0]) is not expected_owner:
                if current is not None and (
                    current.get("pending_identity_sha256") == expected.get("pending_identity_sha256")
                    and current.get("attempt_token") == expected.get("attempt_token")
                ):
                    self._active_pending_by_owner.pop(owner_key, None)
                self._record_projection_pending_drop_locked(
                    expected,
                    reason="ASYNC_COMPLETION_STALE_SINK_OWNER",
                    replacement=current,
                )
                return
            if current is None:
                self._record_projection_pending_drop_locked(
                    expected,
                    reason="ASYNC_COMPLETION_PENDING_OWNER_ABSENT",
                    replacement=payload,
                )
                self._last_completion_by_owner[owner_key] = payload
                return
            if current.get("pending_identity_sha256") != expected.get("pending_identity_sha256") or current.get(
                "attempt_token"
            ) != expected.get("attempt_token"):
                self._record_projection_pending_drop_locked(
                    expected,
                    reason="ASYNC_COMPLETION_STALE_PENDING_OWNER",
                    replacement=current,
                )
                return
            self._active_pending_by_owner.pop(owner_key)
            self._pending_completion_count += 1
            self._last_completion_by_owner[owner_key] = payload

    def _registered_sink_owner_locked(self, consumer_id: str) -> object | None:
        contextual_owner = self._contextual_observation_sinks.get(consumer_id)
        if contextual_owner is not None:
            return contextual_owner
        return self._observation_sinks.get(consumer_id)

    def _record_projection_pending_drop_locked(
        self,
        payload: Mapping[str, Any],
        *,
        reason: str,
        replacement: Mapping[str, Any] | None = None,
    ) -> None:
        self._pending_drop_count_by_reason[reason] = self._pending_drop_count_by_reason.get(reason, 0) + 1
        self._last_pending_drop = {
            "reason": reason,
            "runtime_id": payload.get("runtime_id"),
            "consumer_id": payload.get("consumer_id"),
            "symbol": payload.get("symbol"),
            "attempt_token": payload.get("attempt_token"),
            "pending_identity_sha256": payload.get("pending_identity_sha256"),
            "replacement_attempt_token": replacement.get("attempt_token") if replacement is not None else None,
            "replacement_pending_identity_sha256": (
                replacement.get("pending_identity_sha256") if replacement is not None else None
            ),
        }

    @staticmethod
    def _runtime_id_for_consumer(
        *,
        consumer_id: str,
        error: QuoteContractError | None = None,
    ) -> str:
        if error is not None:
            runtime_id = error.context.get("runtime_id")
            if type(runtime_id) is str and runtime_id and runtime_id == runtime_id.strip():
                return runtime_id
        if ":" in consumer_id:
            suffix = consumer_id.rsplit(":", maxsplit=1)[-1]
            if suffix:
                return suffix
        return consumer_id

    def _resolve_failure(
        self,
        *,
        frame: RawQuoteFrame,
        consumer_id: str,
        stage: str,
    ) -> None:
        resolved = self._failure_governor.resolve(
            runtime_id=None,
            generation=frame.ingress_generation,
            consumer_id=consumer_id,
            stage=stage,
        )
        if not resolved:
            return
        with self._lock:
            self._last_error_by_symbol = {
                symbol: payload
                for symbol, payload in self._last_error_by_symbol.items()
                if not (
                    payload.get("consumer_id") == consumer_id
                    and payload.get("generation") == frame.ingress_generation
                    and payload.get("stage") == stage
                )
            }

    def _record_loud(
        self,
        *,
        frame: RawQuoteFrame,
        error: QuoteContractError,
        consumer_id: str = "quote-projection",
        stage: str = "PROJECTION",
        exception_type: str | None = None,
    ) -> None:
        context_exception_type = error.context.get("exception_type")
        exact_exception_type = exception_type or (
            str(context_exception_type)
            if type(context_exception_type) is str and context_exception_type
            else type(error).__qualname__
        )
        decision = self._failure_governor.record(
            runtime_id=self._runtime_id_for_consumer(consumer_id=consumer_id, error=error),
            generation=frame.ingress_generation,
            consumer_id=consumer_id,
            stage=stage,
            error=error,
            exception_type=exact_exception_type,
            symbol=frame.symbol,
        )
        with self._lock:
            self._rejected_count += 1
            self._last_error_by_symbol[frame.symbol] = dict(decision.snapshot)
        if not decision.emit:
            return
        logger.error(
            "Phase 1 quote projection loud failure: %s",
            dict(decision.log_payload),
        )
        if self._loud_sink is None:
            return
        try:
            self._loud_sink(error)
        except Exception as exc:  # noqa: BLE001 - reporting cannot alter projection state.
            reporting_error = quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
                "quote projection loud sink raised unexpectedly",
                context={"consumer_id": consumer_id, "exception_type": type(exc).__qualname__},
            )
            reporting_decision = self._failure_governor.record(
                runtime_id=self._runtime_id_for_consumer(consumer_id=consumer_id),
                generation=frame.ingress_generation,
                consumer_id=consumer_id,
                stage="REPORTING",
                error=reporting_error,
                exception_type=type(exc).__qualname__,
                symbol=frame.symbol,
            )
            if reporting_decision.emit:
                logger.error(
                    "Phase 1 quote projection loud sink failed: %s",
                    dict(reporting_decision.log_payload),
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
        failure_governor: _ProcessLocalQuoteFailureGovernor | None = None,
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
        self._failure_governor = failure_governor or _ProcessLocalQuoteFailureGovernor(
            loud_interval_seconds=config.loud_interval_seconds
        )
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
        self._active_failure: dict[str, Any] | None = None
        self._restart_count = 0
        self._restart_attempts_in_epoch = 0
        self._next_restart_after_monotonic_ns = 0
        self._pending_drop_count_by_reason: dict[str, int] = {}
        self._last_pending_drop: dict[str, Any] | None = None

    def admit_symbols(self, symbols: tuple[str, ...]) -> None:
        self._mailbox.admit(symbols)

    def replace_admitted_symbols(self, symbols: tuple[str, ...]) -> None:
        self._mailbox.replace_admitted(symbols)
        admitted = set(symbols)
        with self._lock:
            for generation, frames in list(self._pending_by_generation.items()):
                for symbol, frame in frames.items():
                    if symbol not in admitted:
                        self._record_pending_drop_locked(frame, reason="PENDING_SYMBOL_REVOKED")
                retained = {symbol: frame for symbol, frame in frames.items() if symbol in admitted}
                if retained:
                    self._pending_by_generation[generation] = retained
                else:
                    self._pending_by_generation.pop(generation, None)

    def capture_delivery(
        self, delivery: PhaseOneQuoteDelivery, *, source_session_id: str, clock_domain_id: str
    ) -> bool:
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
                self._record_pending_drop_locked(frame, reason="PENDING_FENCED_GENERATION")
                stale = quote_contract_error(
                    QuoteContractReasonCode.ORDERING_REJECTED,
                    "fenced quote generation cannot enter the Phase 1 mailbox",
                    context={
                        "event": "STALE_GENERATION",
                        "consumer_id": self._consumer_id,
                        "generation": frame.ingress_generation,
                        "sequence": frame.ingress_sequence,
                        "fenced_generation": self._fenced_generation,
                        "symbol": frame.symbol,
                    },
                )
                self._record_failure_locked(stale, status="DEGRADED")
                error = stale
            elif self._active_generation != frame.ingress_generation:
                pending = self._pending_by_generation.setdefault(frame.ingress_generation, {})
                current = pending.get(frame.symbol)
                if current is None:
                    pending[frame.symbol] = frame
                    return True
                if ReservedSymbolMailbox._is_newer(frame, current):
                    self._record_pending_drop_locked(current, reason="PENDING_COALESCED_SUPERSEDED")
                    pending[frame.symbol] = frame
                    return True
                self._record_pending_drop_locked(frame, reason="PENDING_ORDERING_REJECTED")
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
        resolved = self._failure_governor.resolve(
            runtime_id=self._consumer_id,
            generation=frame.ingress_generation,
            consumer_id=self._consumer_id,
        )
        if resolved:
            with self._lock:
                self._active_failure = None
        return True

    def on_generation_published(self, data_session_key: str, generation: int) -> bool:
        with self._lock:
            if generation == self._active_generation and self._status not in {
                "FAILED",
                "STOPPED",
                "STOPPING",
                "SHUTDOWN_UNKNOWN",
            }:
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
                        stale_frames = self._pending_by_generation.pop(pending_generation, {})
                        for frame in stale_frames.values():
                            self._record_pending_drop_locked(
                                frame,
                                reason="PENDING_GENERATION_SUPERSEDED",
                            )
                self._status = "STARTING"
                error = None
        if error is not None:
            self._emit_loud(error)
            return False
        for index, frame in enumerate(pending_frames):
            if not self.ingest_frame(frame):
                with self._lock:
                    self._record_pending_drop_locked(
                        frame,
                        reason="PENDING_PUBLISH_REPLAY_REJECTED",
                    )
                    for remaining_frame in pending_frames[index + 1 :]:
                        self._record_pending_drop_locked(
                            remaining_frame,
                            reason="PENDING_PUBLISH_REPLAY_ABORTED",
                        )
                return False
        started = self._start_writer_if_needed()
        if started:
            with self._lock:
                self._active_failure = None
            self._failure_governor.resolve(
                runtime_id=self._consumer_id,
                consumer_id=self._consumer_id,
            )
        return started

    def prepare_generation(self, data_session_key: str, generation: int) -> bool:
        """Pure readiness check used before a physical generation is committed."""

        with self._lock:
            if self._status in {"STOPPED", "STOPPING", "SHUTDOWN_UNKNOWN"} or generation <= self._fenced_generation:
                return False
            fenced_writer = (
                self._writer_thread
                if self._writer_thread is not None
                and self._writer_thread.is_alive()
                and self._writer_thread_epoch != self._writer_epoch
                else None
            )
        if fenced_writer is not None:
            # Last-lease release fences the old writer before a successor feed is
            # prepared. Under scheduler/CI load the thread may need one final poll
            # cycle to observe its stop event; wait boundedly instead of turning
            # that harmless handoff into a false consumer failure. Publication
            # still refuses while the old writer remains alive, so no parallel
            # writer or fail-open path is introduced.
            fenced_writer.join(timeout=2.0)
        with self._lock:
            if self._status in {"STOPPED", "STOPPING", "SHUTDOWN_UNKNOWN"} or generation <= self._fenced_generation:
                return False
            return not (
                self._writer_thread is not None
                and self._writer_thread.is_alive()
                and self._writer_thread_epoch != self._writer_epoch
            )

    def on_generation_fenced(self, data_session_key: str, generation: int) -> None:
        with self._lock:
            pending_frames = self._pending_by_generation.pop(generation, {})
            for frame in pending_frames.values():
                self._record_pending_drop_locked(frame, reason="PENDING_GENERATION_FENCED")
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
            heartbeat_age_ns = (
                None if self._last_heartbeat_monotonic_ns is None else now - self._last_heartbeat_monotonic_ns
            )
            heartbeat_stale = (
                heartbeat_age_ns is not None and heartbeat_age_ns > self._config.heartbeat_timeout_ms * 1_000_000
            )
            if generation is None or self._status in {
                "IDLE",
                "FENCED",
                "STOPPED",
                "STOPPING",
                "SHUTDOWN_UNKNOWN",
            }:
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
                "active_generation": self._active_generation,
                "fenced_generation": self._fenced_generation,
                "thread_alive": thread_alive,
                "last_drain_at": self._last_drain_at.isoformat() if self._last_drain_at is not None else None,
                "backlog": mailbox["backlog"],
                "admitted_symbols": mailbox["admitted_symbols"],
                "restart_count": self._restart_count,
                "writer_heartbeat_age_ms": heartbeat_age_ms,
                "last_failure": self._last_failure,
                "active_failure": self._active_failure,
                "accepted_count": mailbox["accepted_count"],
                "coalesced_count": mailbox["coalesced_count"],
                "ordering_rejected_count": mailbox["ordering_rejected_count"],
                "drop_count_by_reason": mailbox["drop_count_by_reason"],
                "last_drop": mailbox["last_drop"],
                "pending_generation_count": len(self._pending_by_generation),
                "pending_frame_count": sum(len(frames) for frames in self._pending_by_generation.values()),
                "pending_drop_count_by_reason": dict(sorted(self._pending_drop_count_by_reason.items())),
                "last_pending_drop": (dict(self._last_pending_drop) if self._last_pending_drop is not None else None),
                "failure_governor": self._failure_governor.health(),
            }

    def shutdown(self) -> None:
        with self._lock:
            self._status = "STOPPING"
            if self._active_generation is not None:
                self._mailbox.fence_generation(self._active_generation)
                self._fenced_generation = max(self._fenced_generation, self._active_generation)
            self._active_generation = None
            for frames in self._pending_by_generation.values():
                for frame in frames.values():
                    self._record_pending_drop_locked(frame, reason="PENDING_SHUTDOWN")
            self._pending_by_generation.clear()
            writer = self._writer_thread
            self._fence_writer_locked()
        if writer is not None and writer.is_alive():
            writer.join(timeout=2.0)
        if writer is not None and writer.is_alive():
            error = quote_contract_error(
                QuoteContractReasonCode.CONSUMER_FAILURE,
                "Phase 1 quote ingress writer did not stop within the bounded shutdown interval",
                context={
                    "consumer_id": self._consumer_id,
                    "writer_epoch": self._writer_thread_epoch,
                    "writer_thread_name": writer.name,
                    "writer_thread_alive": True,
                    "shutdown_outcome": "UNKNOWN",
                },
            )
            with self._lock:
                self._record_failure_locked(error, status="SHUTDOWN_UNKNOWN")
            self._emit_loud(error)
            raise error
        with self._lock:
            self._status = "STOPPED"
            self._writer_thread = None
            self._writer_thread_epoch = None
            self._writer_stop_event = None

    def _start_writer_if_needed(self) -> bool:
        with self._lock:
            if self._status in {"STOPPED", "STOPPING", "SHUTDOWN_UNKNOWN"}:
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
                for index, frame in enumerate(frames):
                    with self._lock:
                        if (
                            writer_epoch != self._writer_epoch
                            or frame.ingress_generation != self._active_generation
                            or self._status in {"STOPPED", "FAILED", "FENCED", "RESTART_PENDING"}
                        ):
                            self._mailbox.record_lifecycle_drop(
                                frame,
                                reason="WRITER_LIFECYCLE_FENCED",
                            )
                            continue
                    try:
                        self._frame_sink(frame)
                    except Exception:
                        self._mailbox.record_lifecycle_drop(
                            frame,
                            reason="WRITER_FRAME_SINK_FAILED",
                        )
                        for remaining_frame in frames[index + 1 :]:
                            self._mailbox.record_lifecycle_drop(
                                remaining_frame,
                                reason="WRITER_BATCH_ABORTED_AFTER_FRAME_SINK_FAILURE",
                            )
                        raise
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

    def _record_pending_drop_locked(self, frame: RawQuoteFrame, *, reason: str) -> None:
        self._pending_drop_count_by_reason[reason] = self._pending_drop_count_by_reason.get(reason, 0) + 1
        self._last_pending_drop = {
            "reason": reason,
            "symbol": frame.symbol,
            "generation": frame.ingress_generation,
            "sequence": frame.ingress_sequence,
        }

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
        self._last_failure = {
            "schema_version": _ProcessLocalQuoteFailureGovernor._SCHEMA_VERSION,
            "runtime_id": self._consumer_id,
            "consumer_id": self._consumer_id,
            "reason_code": error.reason_code.value,
            "stage": f"WORKER_{error.stage.value}",
            "active": True,
        }
        self._active_failure = self._last_failure
        self._status = status

    def _emit_loud(self, error: QuoteContractError) -> None:
        context = error.context
        generation_value = context.get("generation")
        generation = generation_value if type(generation_value) is int else self._active_generation
        exception_type_value = context.get("exception_type")
        exception_type = (
            str(exception_type_value)
            if type(exception_type_value) is str and exception_type_value
            else type(error).__qualname__
        )
        symbol_value = context.get("symbol")
        symbol = str(symbol_value) if type(symbol_value) is str and symbol_value else None
        decision = self._failure_governor.record(
            runtime_id=self._consumer_id,
            generation=generation,
            consumer_id=self._consumer_id,
            stage=f"WORKER_{error.stage.value}",
            error=error,
            exception_type=exception_type,
            symbol=symbol,
        )
        with self._lock:
            self._last_failure = dict(decision.snapshot)
            self._active_failure = self._last_failure
        if not decision.emit:
            return
        logger.error("Phase 1 quote ingress loud failure: %s", dict(decision.log_payload))
        if self._loud_sink is None:
            return
        try:
            self._loud_sink(error)
        except Exception as exc:  # noqa: BLE001 - reporting must not rewrite quote business state
            reporting_error = quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
                "quote ingress loud sink raised unexpectedly",
                context={"consumer_id": self._consumer_id, "exception_type": type(exc).__qualname__},
            )
            reporting_decision = self._failure_governor.record(
                runtime_id=self._consumer_id,
                generation=generation,
                consumer_id=self._consumer_id,
                stage="WORKER_REPORTING",
                error=reporting_error,
                exception_type=type(exc).__qualname__,
                symbol=symbol,
            )
            if reporting_decision.emit:
                logger.error(
                    "Phase 1 quote ingress loud sink failed: %s",
                    dict(reporting_decision.log_payload),
                )


@dataclass
class QuoteIngressConsumer:
    """Logical consumer ownership; the supervisor owns the sole writer."""

    data_session_key: str
    consumer_id: str
    symbols: tuple[str, ...]
    lease: PhaseOneQuoteLease | None = None


@dataclass(frozen=True)
class _QuoteIngressConsumerLeaseOwner:
    """Copy-on-write local claim over one immutable physical lease identity."""

    consumer_id: str
    symbols: tuple[str, ...]
    lease: PhaseOneQuoteLease
    registration_generation: int
    local_state: str
    owner_identity_sha256: str


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
        observation_sink: Callable[[NormalizedQuoteObservation], MiniQMTKernelProductIngressResult] | None = None,
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
        self._consumer_lease_owner_lock = threading.RLock()
        self._consumer_lease_owner_generation = 0
        self._consumer_lease_owners: dict[str, _QuoteIngressConsumerLeaseOwner] = {}
        self._last_release_reconciliation: dict[str, Any] | None = None
        self._source_session_id = f"phase1-{hashlib.sha256(data_session_key.encode('utf-8')).hexdigest()[:24]}"
        self._clock_domain_id = MINIQMT_QUOTE_CLOCK_DOMAIN_ID
        self._failure_governor = _ProcessLocalQuoteFailureGovernor(loud_interval_seconds=config.loud_interval_seconds)
        self._projection_sink = PhaseOneQuoteProjectionSink(
            raw_store=self._snapshot_store,
            normalized_store=self._normalized_store,
            context_store=self._context_store,
            loud_sink=loud_sink,
            observation_sink=observation_sink,
            failure_governor=self._failure_governor,
        )
        self._worker = QuoteIngressWorker(
            consumer_id=f"{data_session_key}:single-writer",
            config=config,
            frame_sink=self._projection_sink.project,
            loud_sink=loud_sink,
            failure_governor=self._failure_governor,
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

    def register_observation_sink(
        self,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
        sink: Callable[
            [NormalizedQuoteObservation, QuoteEvaluationContext],
            MiniQMTKernelProductIngressResult,
        ],
    ) -> None:
        self._projection_sink.register_observation_sink(
            consumer_id=consumer_id,
            symbols=symbols,
            sink=sink,
        )

    def unregister_observation_sink(
        self,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
        sink: Callable[
            [NormalizedQuoteObservation, QuoteEvaluationContext],
            MiniQMTKernelProductIngressResult,
        ],
    ) -> bool:
        return self._projection_sink.unregister_observation_sink(
            consumer_id=consumer_id,
            symbols=symbols,
            sink=sink,
        )

    def get_observation_sink(
        self,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
    ) -> (
        Callable[
            [NormalizedQuoteObservation, QuoteEvaluationContext],
            MiniQMTKernelProductIngressResult,
        ]
        | None
    ):
        return self._projection_sink.get_observation_sink(
            consumer_id=consumer_id,
            symbols=symbols,
        )

    @staticmethod
    def _lease_identity_payload(lease: PhaseOneQuoteLease) -> dict[str, Any]:
        return {
            "lease_id": lease.lease_id,
            "data_session_key": lease.data_session_key,
            "owner": lease.owner,
            "consumer_id": lease.consumer_id,
            "symbols": list(lease.symbols),
            "generation": lease.generation,
            "status": lease.status,
            "physical_subscription_id": lease.physical_subscription_id,
        }

    @classmethod
    def _lease_owner_identity_sha256(
        cls,
        *,
        lease: PhaseOneQuoteLease,
        registration_generation: int,
    ) -> str:
        payload = cls._lease_identity_payload(lease)
        components = (
            "miniqmt_quote_consumer_lease_owner_v1",
            str(registration_generation),
            str(payload["lease_id"]),
            str(payload["data_session_key"]),
            str(payload["owner"]),
            str(payload["consumer_id"]),
            str(len(lease.symbols)),
            *lease.symbols,
            str(payload["generation"]),
            str(payload["status"]),
            str(payload["physical_subscription_id"]),
        )
        hasher = hashlib.sha256()
        for component in components:
            encoded = component.encode("utf-8")
            hasher.update(len(encoded).to_bytes(8, byteorder="big", signed=False))
            hasher.update(encoded)
        return hasher.hexdigest()

    @staticmethod
    def _is_exact_active_lease(
        lease: object,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
        data_session_key: str,
        owner: str,
    ) -> bool:
        return bool(
            type(lease) is PhaseOneQuoteLease
            and lease.lease_id
            and lease.lease_id == lease.lease_id.strip()
            and lease.data_session_key == data_session_key
            and lease.owner == owner
            and lease.consumer_id == consumer_id
            and lease.symbols == symbols
            and type(lease.generation) is int
            and lease.generation > 0
            and lease.status == "ACTIVE"
            and type(lease.physical_subscription_id) is int
            and lease.physical_subscription_id > 0
        )

    def _publish_consumer_lease_owner_locked(
        self,
        *,
        consumer: QuoteIngressConsumer,
        local_state: str,
    ) -> _QuoteIngressConsumerLeaseOwner:
        if consumer.lease is None:
            raise RuntimeError("quote consumer cannot publish ownership without an immutable lease")
        with self._consumer_lease_owner_lock:
            current = self._consumer_lease_owners.get(consumer.consumer_id)
            if current is None:
                self._consumer_lease_owner_generation += 1
                registration_generation = self._consumer_lease_owner_generation
            else:
                registration_generation = current.registration_generation
                if current.lease.lease_id != consumer.lease.lease_id:
                    raise RuntimeError("quote consumer lease identity changed within one registration generation")
                if current.lease.generation > consumer.lease.generation:
                    consumer.lease = current.lease
                elif (
                    current.lease.generation == consumer.lease.generation
                    and current.lease.physical_subscription_id != consumer.lease.physical_subscription_id
                ):
                    raise RuntimeError("quote consumer physical identity changed within one lease generation")
            owner = _QuoteIngressConsumerLeaseOwner(
                consumer_id=consumer.consumer_id,
                symbols=consumer.symbols,
                lease=consumer.lease,
                registration_generation=registration_generation,
                local_state=local_state,
                owner_identity_sha256=self._lease_owner_identity_sha256(
                    lease=consumer.lease,
                    registration_generation=registration_generation,
                ),
            )
            updated = dict(self._consumer_lease_owners)
            updated[consumer.consumer_id] = owner
            self._consumer_lease_owners = updated
            return owner

    def _drop_consumer_lease_owner_locked(self, consumer_id: str) -> None:
        with self._consumer_lease_owner_lock:
            if consumer_id not in self._consumer_lease_owners:
                return
            updated = dict(self._consumer_lease_owners)
            updated.pop(consumer_id, None)
            self._consumer_lease_owners = updated

    def consumer_lease_owner_snapshot(
        self,
        *,
        consumer_id: str,
        symbols: tuple[str, ...],
    ) -> dict[str, Any]:
        """Read exact physical ownership without taking the supervisor lifecycle lock."""

        exact_consumer_id = PhaseOneQuoteProjectionSink._exact_consumer_id(consumer_id)
        exact_symbols = PhaseOneQuoteProjectionSink._exact_symbol_ownership(symbols, allow_empty=False)
        for _attempt in range(3):
            owner = self._consumer_lease_owners.get(exact_consumer_id)
            base: dict[str, Any] = {
                "schema_version": "miniqmt_quote_consumer_lease_owner_snapshot_v1",
                "consumer_id": exact_consumer_id,
                "requested_symbols": list(exact_symbols),
                "readback_current": True,
                "exact_owner": False,
                "state": "ABSENT",
                "reason_code": "MINIQMT_QUOTE_CONSUMER_LEASE_OWNER_ABSENT",
                "registration_generation": None,
                "expected_owner_identity_sha256": None,
                "actual_owner_identity_sha256": None,
                "expected_lease": None,
                "actual_lease": None,
            }
            if owner is None:
                return base
            expected_lease = self._lease_identity_payload(owner.lease)
            base.update(
                {
                    "registration_generation": owner.registration_generation,
                    "expected_owner_identity_sha256": owner.owner_identity_sha256,
                    "expected_lease": expected_lease,
                }
            )
            if owner.consumer_id != exact_consumer_id or owner.symbols != exact_symbols:
                base.update(
                    {
                        "state": "REQUEST_OWNER_DRIFT",
                        "reason_code": "MINIQMT_QUOTE_CONSUMER_LEASE_REQUEST_OWNER_DRIFT",
                    }
                )
                return base
            try:
                actual_lease = self._subscriber.get_phase_one_lease(
                    data_session_key=self._data_session_key,
                    lease_id=owner.lease.lease_id,
                )
            except Exception as exc:  # noqa: BLE001 - exact readback failure is a typed owner state.
                base.update(
                    {
                        "readback_current": False,
                        "state": "READBACK_FAILED",
                        "reason_code": "MINIQMT_QUOTE_CONSUMER_LEASE_READBACK_FAILED",
                        "exception_type": type(exc).__qualname__,
                    }
                )
                return base
            if type(actual_lease) is PhaseOneQuoteLease:
                base["actual_lease"] = self._lease_identity_payload(actual_lease)
                base["actual_owner_identity_sha256"] = self._lease_owner_identity_sha256(
                    lease=actual_lease,
                    registration_generation=owner.registration_generation,
                )
            elif actual_lease is not None:
                base.update(
                    {
                        "state": "LEASE_CARRIER_INVALID",
                        "reason_code": "MINIQMT_QUOTE_CONSUMER_LEASE_CARRIER_INVALID",
                        "actual_lease_type": type(actual_lease).__qualname__,
                    }
                )
                return base
            if owner.local_state != "ACTIVE":
                base.update(
                    {
                        "state": owner.local_state,
                        "reason_code": "MINIQMT_QUOTE_CONSUMER_LEASE_RELEASE_IN_PROGRESS",
                    }
                )
                return base
            if actual_lease is None:
                base.update(
                    {
                        "state": "LEASE_MISSING",
                        "reason_code": "MINIQMT_QUOTE_CONSUMER_LEASE_MISSING",
                    }
                )
                return base
            if actual_lease.status != "ACTIVE":
                base.update(
                    {
                        "state": "LEASE_NOT_ACTIVE",
                        "reason_code": "MINIQMT_QUOTE_CONSUMER_LEASE_NOT_ACTIVE",
                    }
                )
                return base
            legal_successor = bool(
                self._is_exact_active_lease(
                    actual_lease,
                    consumer_id=exact_consumer_id,
                    symbols=exact_symbols,
                    data_session_key=self._data_session_key,
                    owner=self._owner,
                )
                and actual_lease.lease_id == owner.lease.lease_id
                and actual_lease.generation > owner.lease.generation
                and actual_lease.physical_subscription_id != owner.lease.physical_subscription_id
            )
            if base["actual_owner_identity_sha256"] != owner.owner_identity_sha256 and legal_successor:
                refreshed_owner = _QuoteIngressConsumerLeaseOwner(
                    consumer_id=owner.consumer_id,
                    symbols=owner.symbols,
                    lease=actual_lease,
                    registration_generation=owner.registration_generation,
                    local_state=owner.local_state,
                    owner_identity_sha256=str(base["actual_owner_identity_sha256"]),
                )
                with self._consumer_lease_owner_lock:
                    if self._consumer_lease_owners.get(exact_consumer_id) is not owner:
                        continue
                    updated = dict(self._consumer_lease_owners)
                    updated[exact_consumer_id] = refreshed_owner
                    self._consumer_lease_owners = updated
                owner = refreshed_owner
                base["expected_lease"] = dict(base["actual_lease"])
                base["expected_owner_identity_sha256"] = owner.owner_identity_sha256
            if base["actual_owner_identity_sha256"] != owner.owner_identity_sha256:
                base.update(
                    {
                        "state": "LEASE_OWNER_DRIFT",
                        "reason_code": "MINIQMT_QUOTE_CONSUMER_LEASE_OWNER_DRIFT",
                    }
                )
                return base
            base.update(
                {
                    "exact_owner": True,
                    "state": "ACTIVE",
                    "reason_code": None,
                }
            )
            return base
        return {
            "schema_version": "miniqmt_quote_consumer_lease_owner_snapshot_v1",
            "consumer_id": exact_consumer_id,
            "requested_symbols": list(exact_symbols),
            "readback_current": False,
            "exact_owner": False,
            "state": "OWNER_CHANGED_DURING_READBACK",
            "reason_code": "MINIQMT_QUOTE_CONSUMER_LEASE_OWNER_CHANGED_DURING_READBACK",
            "registration_generation": None,
            "expected_owner_identity_sha256": None,
            "actual_owner_identity_sha256": None,
            "expected_lease": None,
            "actual_lease": None,
        }

    def acquire_consumer(self, *, consumer_id: str, symbols: list[str]) -> PhaseOneQuoteLease:
        with self._lock:
            if consumer_id in self._consumers:
                raise quote_contract_error(
                    QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                    "a Phase 1 quote consumer id cannot acquire a second lease",
                    context={"consumer_id": consumer_id, "data_session_key": self._data_session_key},
                )
            normalized = tuple(
                dict.fromkeys(str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip())
            )
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
            if not self._is_exact_active_lease(
                lease,
                consumer_id=consumer_id,
                symbols=normalized,
                data_session_key=self._data_session_key,
                owner=self._owner,
            ):
                self._worker.replace_admitted_symbols(previous_symbols)
                self._projection_sink.replace_admitted(previous_symbols)
                rollback_released: bool | None = None
                rollback_exception_type: str | None = None
                if type(lease) is PhaseOneQuoteLease and lease.lease_id:
                    try:
                        rollback_released = self._subscriber.release_phase_one_lease(
                            data_session_key=self._data_session_key,
                            lease_id=lease.lease_id,
                            max_symbols=self._config.max_symbols,
                        )
                    except Exception as rollback_error:  # noqa: BLE001 - preserve invalid owner and cleanup state.
                        rollback_exception_type = type(rollback_error).__qualname__
                consumer_lease_retained = bool(
                    type(lease) is PhaseOneQuoteLease and bool(lease.lease_id) and rollback_released is not True
                )
                if consumer_lease_retained:
                    consumer.lease = lease
                    self._consumers[consumer_id] = consumer
                    self._publish_consumer_lease_owner_locked(
                        consumer=consumer,
                        local_state="ACQUIRE_ROLLBACK_UNKNOWN",
                    )
                error = quote_contract_error(
                    QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                    "Phase 1 quote consumer acquisition returned a non-exact active physical lease",
                    context={
                        "consumer_id": consumer_id,
                        "data_session_key": self._data_session_key,
                        "expected_symbols": list(normalized),
                        "lease_type": type(lease).__qualname__,
                        "actual_lease": (
                            self._lease_identity_payload(lease) if type(lease) is PhaseOneQuoteLease else None
                        ),
                        "rollback_released": rollback_released,
                        "rollback_exception_type": rollback_exception_type,
                        "release_outcome": (
                            "UNKNOWN"
                            if consumer_lease_retained
                            else ("RELEASED" if rollback_released is True else "NOT_APPLICABLE")
                        ),
                        "consumer_lease_retained": consumer_lease_retained,
                        "retained_owner_state": ("ACQUIRE_ROLLBACK_UNKNOWN" if consumer_lease_retained else None),
                    },
                )
                self._worker.record_loud_failure(error)
                raise error
            consumer.lease = lease
            self._consumers[consumer_id] = consumer
            self._publish_consumer_lease_owner_locked(consumer=consumer, local_state="ACTIVE")
        return lease

    def release_consumer(self, *, consumer_id: str) -> bool:
        with self._lock:
            consumer = self._consumers.get(consumer_id)
            if consumer is None or consumer.lease is None:
                return False
            if (
                self._projection_sink.get_observation_sink(
                    consumer_id=consumer_id,
                    symbols=consumer.symbols,
                )
                is not None
            ):
                raise quote_contract_error(
                    QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                    "Phase 1 consumer lease cannot be released while its observation sink remains active",
                    context={
                        "consumer_id": consumer_id,
                        "lease_id": consumer.lease.lease_id,
                        "release_outcome": "ACTIVE",
                    },
                )
            lease_id = consumer.lease.lease_id
            self._publish_consumer_lease_owner_locked(
                consumer=consumer,
                local_state="RELEASE_IN_FLIGHT",
            )
            try:
                released = self._subscriber.release_phase_one_lease(
                    data_session_key=self._data_session_key,
                    lease_id=lease_id,
                    max_symbols=self._config.max_symbols,
                )
            except Exception as primary:  # noqa: BLE001 - reconcile the exact process-local lease authority.
                current_lease = self._release_consumer_lease_readback(
                    consumer_id=consumer_id,
                    lease_id=lease_id,
                    primary=primary,
                )
                if current_lease is None:
                    self._record_release_reconciliation_locked(
                        consumer_id=consumer_id,
                        lease_id=lease_id,
                        disposition="POST_MUTATION_EXCEPTION_RECONCILED",
                        primary=primary,
                    )
                    self._finalize_released_consumer_locked(consumer_id)
                    return True
                if current_lease.status == "ACTIVE":
                    self._publish_consumer_lease_owner_locked(
                        consumer=consumer,
                        local_state="ACTIVE",
                    )
                    raise quote_contract_error(
                        QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                        "Phase 1 consumer release failed before the exact active lease was removed",
                        context={
                            "consumer_id": consumer_id,
                            "lease_id": lease_id,
                            "lease_status": current_lease.status,
                            "release_outcome": "ACTIVE",
                            "exception_type": type(primary).__qualname__,
                        },
                    ) from primary
                self._publish_consumer_lease_owner_locked(
                    consumer=consumer,
                    local_state="RELEASE_UNKNOWN",
                )
                raise quote_contract_error(
                    QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                    "Phase 1 consumer release outcome remains unknown after lease readback",
                    context={
                        "consumer_id": consumer_id,
                        "lease_id": lease_id,
                        "lease_status": current_lease.status,
                        "release_outcome": "UNKNOWN",
                        "exception_type": type(primary).__qualname__,
                    },
                ) from primary
            if not released:
                current_lease = self._release_consumer_lease_readback(
                    consumer_id=consumer_id,
                    lease_id=lease_id,
                    primary=None,
                )
                if current_lease is None:
                    self._record_release_reconciliation_locked(
                        consumer_id=consumer_id,
                        lease_id=lease_id,
                        disposition="FALSE_RETURN_ABSENT_LEASE_RECONCILED",
                        primary=None,
                    )
                    self._finalize_released_consumer_locked(consumer_id)
                    return True
                if current_lease.status != "ACTIVE":
                    self._publish_consumer_lease_owner_locked(
                        consumer=consumer,
                        local_state="RELEASE_UNKNOWN",
                    )
                    raise quote_contract_error(
                        QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                        "Phase 1 consumer release returned false without an exact active lease",
                        context={
                            "consumer_id": consumer_id,
                            "lease_id": lease_id,
                            "lease_status": current_lease.status,
                            "release_outcome": "UNKNOWN",
                        },
                    )
                self._publish_consumer_lease_owner_locked(
                    consumer=consumer,
                    local_state="ACTIVE",
                )
                return False
            self._finalize_released_consumer_locked(consumer_id)
        return released

    def _release_consumer_lease_readback(
        self,
        *,
        consumer_id: str,
        lease_id: str,
        primary: Exception | None,
    ) -> PhaseOneQuoteLease | None:
        try:
            return self._subscriber.get_phase_one_lease(
                data_session_key=self._data_session_key,
                lease_id=lease_id,
            )
        except Exception as readback:  # noqa: BLE001 - return a typed UNKNOWN outcome, never a guessed bool.
            consumer = self._consumers.get(consumer_id)
            if consumer is not None and consumer.lease is not None and consumer.lease.lease_id == lease_id:
                self._publish_consumer_lease_owner_locked(
                    consumer=consumer,
                    local_state="RELEASE_UNKNOWN",
                )
            raise quote_contract_error(
                QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                "Phase 1 consumer release readback failed",
                context={
                    "consumer_id": consumer_id,
                    "lease_id": lease_id,
                    "release_outcome": "UNKNOWN",
                    "release_exception_type": type(primary).__qualname__ if primary is not None else None,
                    "readback_exception_type": type(readback).__qualname__,
                },
            ) from (primary or readback)

    def _finalize_released_consumer_locked(self, consumer_id: str) -> None:
        consumer = self._consumers.get(consumer_id)
        if consumer is None:
            return
        previous_admitted = self._union_consumer_symbols((*self._consumers.values(),))
        remaining_consumers = tuple(
            owned_consumer
            for owned_consumer_id, owned_consumer in self._consumers.items()
            if owned_consumer_id != consumer_id
        )
        admitted = self._union_consumer_symbols(remaining_consumers)
        try:
            self._worker.replace_admitted_symbols(admitted)
            self._projection_sink.replace_admitted(admitted)
        except Exception as primary:  # noqa: BLE001 - retain the local owner until exact cleanup can retry.
            rollback_failures: list[dict[str, str]] = []
            for component, rollback in (
                ("worker", lambda: self._worker.replace_admitted_symbols(previous_admitted)),
                ("projection", lambda: self._projection_sink.replace_admitted(previous_admitted)),
            ):
                try:
                    rollback()
                except Exception as rollback_error:  # noqa: BLE001 - aggregate rollback evidence.
                    rollback_failures.append(
                        {
                            "component": component,
                            "exception_type": type(rollback_error).__qualname__,
                        }
                    )
            error = quote_contract_error(
                QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                "Phase 1 released consumer local cleanup failed and remains retryable",
                context={
                    "consumer_id": consumer_id,
                    "lease_id": consumer.lease.lease_id if consumer.lease is not None else None,
                    "release_outcome": "UNKNOWN",
                    "exception_type": type(primary).__qualname__,
                    "rollback_failures": rollback_failures,
                },
            )
            self._publish_consumer_lease_owner_locked(
                consumer=consumer,
                local_state="LOCAL_CLEANUP_UNKNOWN",
            )
            self._worker.record_loud_failure(error)
            raise error from primary
        self._consumers.pop(consumer_id, None)
        self._drop_consumer_lease_owner_locked(consumer_id)

    def _record_release_reconciliation_locked(
        self,
        *,
        consumer_id: str,
        lease_id: str,
        disposition: str,
        primary: Exception | None,
    ) -> None:
        evidence = {
            "schema_version": "miniqmt_quote_release_reconciliation_v1",
            "consumer_id": consumer_id,
            "lease_id": lease_id,
            "disposition": disposition,
            "observed_at_utc": datetime.now(UTC).isoformat(),
            "exception_type": type(primary).__qualname__ if primary is not None else None,
        }
        self._last_release_reconciliation = evidence
        logger.warning("Phase 1 quote lease release required exact reconciliation: %s", evidence)

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
        owner_snapshot = self._consumer_lease_owners
        consumers: dict[str, dict[str, Any]] = {}
        for consumer_id, owner in sorted(owner_snapshot.items()):
            snapshot = self.consumer_lease_owner_snapshot(
                consumer_id=consumer_id,
                symbols=owner.symbols,
            )
            actual_lease = snapshot.get("actual_lease")
            consumers[consumer_id] = {
                "lease_id": actual_lease.get("lease_id") if isinstance(actual_lease, Mapping) else None,
                "symbols": (list(actual_lease.get("symbols") or ()) if isinstance(actual_lease, Mapping) else []),
                "expected_symbols": list(owner.symbols),
                "lease_generation": (actual_lease.get("generation") if isinstance(actual_lease, Mapping) else None),
                "lease_status": actual_lease.get("status") if isinstance(actual_lease, Mapping) else None,
                "physical_subscription_id": (
                    actual_lease.get("physical_subscription_id") if isinstance(actual_lease, Mapping) else None
                ),
                "owner_state": snapshot["state"],
                "owner_reason_code": snapshot["reason_code"],
                "owner_registration_generation": snapshot["registration_generation"],
                "owner_identity_sha256": snapshot["expected_owner_identity_sha256"],
                "readback_current": snapshot["readback_current"],
                "exact_owner": snapshot["exact_owner"],
            }
        return {
            "data_session_key": self._data_session_key,
            "owner": self._owner,
            "subscription": self._subscriber.phase_one_health(data_session_key=self._data_session_key),
            "writer": self._worker.health(),
            **self._projection_sink.health(),
            "consumers": consumers,
            "last_release_reconciliation": (
                dict(self._last_release_reconciliation) if self._last_release_reconciliation is not None else None
            ),
        }

    def shutdown(self) -> None:
        self._subscriber.shutdown_phase_one_leases(data_session_key=self._data_session_key)
        subscription_health = self._subscriber.phase_one_health(data_session_key=self._data_session_key)
        if (
            not isinstance(subscription_health, dict)
            or subscription_health.get("status") != "INACTIVE"
            or int(subscription_health.get("lease_count") or 0) != 0
            or int(subscription_health.get("pending_lease_count") or 0) != 0
        ):
            raise quote_contract_error(
                QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                "Phase 1 shutdown did not close the exact logical lease registry",
                context={
                    "data_session_key": self._data_session_key,
                    "status": (
                        subscription_health.get("status")
                        if isinstance(subscription_health, dict)
                        else type(subscription_health).__qualname__
                    ),
                    "lease_count": (
                        subscription_health.get("lease_count") if isinstance(subscription_health, dict) else None
                    ),
                    "pending_lease_count": (
                        subscription_health.get("pending_lease_count")
                        if isinstance(subscription_health, dict)
                        else None
                    ),
                },
            )
        self._worker.shutdown()
        with self._lock:
            self._consumers.clear()
            with self._consumer_lease_owner_lock:
                self._consumer_lease_owners = {}
        self._projection_sink.shutdown_observation_sinks()
        self._worker.replace_admitted_symbols(())
        self._projection_sink.replace_admitted(())

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
