"""Durable, single-writer Phase 1 quote evidence and markout coordination.

The callback and QuoteIngress writer only hand this module immutable accepted
observations.  They never open a database connection or wait for persistence.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from backend.execution_algos.adaptive_is.contracts import (
    ControlRevision,
    EvidenceCaptureType,
    EvidenceMarkStatus,
    MarketDataEvidenceV1,
    QuoteValidationState,
    TradabilityState,
    canonical_sha256,
    require_identity,
    require_sha256,
)
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode, quote_contract_error
from backend.miniqmt_quote_contract_config import QuoteIngressRuntimeConfig

from .models import MiniQMTExecutionEvent, MiniQMTExecutionEventType
from .quote_eligibility import NormalizedQuoteObservation
from .repository import (
    DurableEvidenceReceipt,
    MiniQMTExecutionRuntimeRepository,
    QuoteEvidenceEventCandidate,
    QuoteEvidenceIdempotencyConflict,
)


_MARKOUT_CAPTURE_TYPES = {
    60: EvidenceCaptureType.MARKOUT_60S,
    300: EvidenceCaptureType.MARKOUT_300S,
    900: EvidenceCaptureType.MARKOUT_900S,
}


@dataclass(frozen=True)
class MarkoutAnchor:
    runtime_id: str
    binding_id: str | None
    trade_date: date
    parent_intent_id: str | None
    algo_instance_id: str | None
    action_id: str | None
    child_order_id: str
    trade_id: str
    anchor_trade_event_id: str
    action_evidence_id: str
    anchor_market_data_id: str
    symbol: str
    side: str
    source_session_id: str
    ingress_generation: int
    trade_time_utc: datetime
    continuous_segment_end_utc: datetime
    clock_event_id: str
    benchmark_policy_version: str
    mark_policy_version: str
    markout_max_lag_ms: int
    policy_sha256: str
    config_sha256: str
    adapter_sha256: str
    code_sha256: str
    schema_sha256: str
    calendar_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.trade_date, date):
            raise ValueError("markout anchor trade_date must be a date")
        if self.trade_time_utc.tzinfo is None or self.continuous_segment_end_utc.tzinfo is None:
            raise ValueError("markout anchor timestamps must be timezone-aware")
        if self.continuous_segment_end_utc <= self.trade_time_utc:
            raise ValueError("markout anchor continuous segment must end after trade time")
        if isinstance(self.markout_max_lag_ms, bool) or self.markout_max_lag_ms <= 0:
            raise ValueError("markout anchor requires an explicit positive markout_max_lag_ms")
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("markout anchor side must be BUY or SELL")
        if isinstance(self.ingress_generation, bool) or not isinstance(self.ingress_generation, int) or self.ingress_generation < 0:
            raise ValueError("markout anchor ingress_generation must be a non-negative integer")
        for field_name in (
            "runtime_id",
            "child_order_id",
            "trade_id",
            "anchor_trade_event_id",
            "action_evidence_id",
            "anchor_market_data_id",
            "source_session_id",
            "clock_event_id",
            "benchmark_policy_version",
            "mark_policy_version",
        ):
            require_identity(getattr(self, field_name), field_name=f"markout_anchor.{field_name}")
        for field_name in ("policy_sha256", "config_sha256", "adapter_sha256", "code_sha256", "schema_sha256", "calendar_sha256"):
            require_sha256(getattr(self, field_name), field_name=f"markout_anchor.{field_name}")

    @classmethod
    def from_trade_event(cls, event: MiniQMTExecutionEvent) -> "MarkoutAnchor | None":
        """Recover only an explicitly versioned B0_QUOTE_V2 trade anchor.

        Legacy and unrelated TRADE_EVENT payloads are intentionally ignored;
        a B0_QUOTE_V2 envelope that claims the anchor schema but is malformed
        fails loudly instead of deriving a mark from local time or latest quote.
        """

        if event.event_type != MiniQMTExecutionEventType.TRADE_EVENT or not isinstance(event.payload, dict):
            return None
        raw = event.payload.get("quote_evidence_markout_anchor_v1")
        if raw is None:
            return None
        if not isinstance(raw, Mapping):
            raise ValueError("TRADE_EVENT quote_evidence_markout_anchor_v1 must be a mapping")
        if raw.get("schema_version") != "miniqmt_quote_markout_anchor_v1":
            raise ValueError("TRADE_EVENT quote evidence markout anchor schema is unsupported")
        if raw.get("control_revision") != ControlRevision.B0_QUOTE_V2.value:
            raise ValueError("TRADE_EVENT quote evidence markout anchor must explicitly select B0_QUOTE_V2")
        try:
            trade_date = date.fromisoformat(str(raw["trade_date"]))
            trade_time_utc = _parse_utc_timestamp(raw["trade_time_utc"], field_name="trade_time_utc")
            segment_end_utc = _parse_utc_timestamp(raw["continuous_segment_end_utc"], field_name="continuous_segment_end_utc")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("TRADE_EVENT quote evidence markout anchor has invalid trade-date/time fields") from exc
        return cls(
            runtime_id=event.runtime_id,
            binding_id=_optional_text(raw.get("binding_id")),
            trade_date=trade_date,
            parent_intent_id=_optional_text(raw.get("parent_intent_id")),
            algo_instance_id=_optional_text(raw.get("algo_instance_id")),
            action_id=_optional_text(raw.get("action_id")),
            child_order_id=str(raw["child_order_id"]),
            trade_id=str(raw["trade_id"]),
            anchor_trade_event_id=event.event_id,
            action_evidence_id=str(raw["action_evidence_id"]),
            anchor_market_data_id=str(raw["anchor_market_data_id"]),
            symbol=str(raw["symbol"]),
            side=str(raw["side"]),
            source_session_id=str(raw["source_session_id"]),
            ingress_generation=_strict_non_negative_int(raw["ingress_generation"], field_name="ingress_generation"),
            trade_time_utc=trade_time_utc,
            continuous_segment_end_utc=segment_end_utc,
            clock_event_id=str(raw["clock_event_id"]),
            benchmark_policy_version=str(raw["benchmark_policy_version"]),
            mark_policy_version=str(raw["mark_policy_version"]),
            markout_max_lag_ms=_strict_positive_int(raw["markout_max_lag_ms"], field_name="markout_max_lag_ms"),
            policy_sha256=str(raw["policy_sha256"]),
            config_sha256=str(raw["config_sha256"]),
            adapter_sha256=str(raw["adapter_sha256"]),
            code_sha256=str(raw["code_sha256"]),
            schema_sha256=str(raw["schema_sha256"]),
            calendar_sha256=str(raw["calendar_sha256"]),
        )


@dataclass(frozen=True)
class QuoteEvidenceHealth:
    status: str
    high_priority_backlog: int
    cadence_slots: int
    health_slots: int
    persistence_failures: int
    first_failure: Mapping[str, Any] | None
    last_failure: Mapping[str, Any] | None
    occurrence_count: int
    history_samples: int
    history_gap_symbols: tuple[str, ...]
    outbox_capacity: int


@dataclass(frozen=True)
class QuoteIngressHealthV1:
    """Versioned bounded ingress health payload for QUOTE_INGRESS_HEALTH.

    It contains process/session observations only.  In particular it excludes
    account, runtime business identifiers beyond runtime_id, symbols and raw
    callbacks, so periodic health cannot create a high-cardinality carrier.
    """

    runtime_id: str
    owner_mode: str
    source_session_id: str
    ingress_generation: int
    config_sha256: str
    status: str
    window_start_utc: datetime
    counters: Mapping[str, int]
    health_schema_version: str = "miniqmt_quote_ingress_health_v1"
    health_id: str | None = None
    health_sha256: str = ""

    def __post_init__(self) -> None:
        if self.health_schema_version != "miniqmt_quote_ingress_health_v1":
            raise ValueError("unsupported quote ingress health schema version")
        for field_name in ("runtime_id", "owner_mode", "source_session_id", "status"):
            object.__setattr__(self, field_name, require_identity(getattr(self, field_name), field_name=f"quote_health.{field_name}"))
        if isinstance(self.ingress_generation, bool) or not isinstance(self.ingress_generation, int) or self.ingress_generation < 0:
            raise ValueError("quote ingress health generation must be a non-negative integer")
        object.__setattr__(self, "config_sha256", require_sha256(self.config_sha256, field_name="quote_health.config_sha256"))
        if self.window_start_utc.tzinfo is None:
            raise ValueError("quote ingress health window start must be timezone-aware")
        object.__setattr__(self, "window_start_utc", self.window_start_utc.astimezone(UTC))
        allowed_counters = {
            "accepted",
            "rejected",
            "coalesced",
            "capacity_rejected",
            "writer_restarts",
            "persistence_failures",
        }
        if set(self.counters) != allowed_counters:
            raise ValueError("quote ingress health counters must contain the exact registered keys")
        normalized_counters: dict[str, int] = {}
        for key, value in self.counters.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("quote ingress health counters must be non-negative integers")
            normalized_counters[key] = value
        object.__setattr__(self, "counters", MappingProxyType(normalized_counters))
        identity = {
            "schema_version": self.health_schema_version,
            "runtime_id": self.runtime_id,
            "owner_mode": self.owner_mode,
            "source_session_id": self.source_session_id,
            "ingress_generation": self.ingress_generation,
            "window_start_utc": self.window_start_utc,
            "config_sha256": self.config_sha256,
            "status": self.status,
            "counters": self.counters,
        }
        derived_id = "mqhealth_" + canonical_sha256(identity)
        if self.health_id is not None and self.health_id != derived_id:
            raise ValueError("quote ingress health_id does not match deterministic identity")
        object.__setattr__(self, "health_id", derived_id)
        derived_hash = canonical_sha256(
            {
                **identity,
            }
        )
        if self.health_sha256 and self.health_sha256 != derived_hash:
            raise ValueError("quote ingress health hash does not match canonical payload")
        object.__setattr__(self, "health_sha256", derived_hash)

    @property
    def event_id(self) -> str:
        return "mqrtevt_" + canonical_sha256({"schema": "miniqmt_quote_event_v1", "health_id": self.health_id})

    def runtime_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "miniqmt_quote_ingress_health_payload_v1",
            "health_or_aggregate": {
                "health_schema_version": self.health_schema_version,
                "health_id": self.health_id,
                "health_sha256": self.health_sha256,
                "owner_mode": self.owner_mode,
                "source_session_id_sha256": canonical_sha256({"source_session_id": self.source_session_id}),
                "ingress_generation": self.ingress_generation,
                "config_sha256": self.config_sha256,
                "status": self.status,
                "window_start_utc": self.window_start_utc.isoformat(),
                "counters": dict(self.counters),
            },
        }


@dataclass
class _QueuedEvidence:
    evidence: MarketDataEvidenceV1 | QuoteIngressHealthV1
    event_type: MiniQMTExecutionEventType
    attempts: int = 0
    retry_not_before_utc: datetime | None = None
    terminal_failure: bool = False


@dataclass
class _PendingMarkout:
    anchor: MarkoutAnchor
    horizon_seconds: int

    @property
    def target_time_utc(self) -> datetime:
        return self.anchor.trade_time_utc + timedelta(seconds=self.horizon_seconds)

    @property
    def mark_series_key(self) -> str:
        return canonical_sha256(
            {
                "runtime_id": self.anchor.runtime_id,
                "trade_id": self.anchor.trade_id,
                "child_order_id": self.anchor.child_order_id,
                "horizon_seconds": self.horizon_seconds,
                "mark_policy_version": self.anchor.mark_policy_version,
            }
        )


class QuoteEvidenceCoordinator:
    """The only durable writer for one runtime's quote evidence.

    It is intentionally pull-driven: the ingress writer calls ``observe`` and
    an owner lifecycle tick calls ``flush``/``drain_markouts``.  No callback
    thread can cause a DB write, broker action, or recovery side effect.
    """

    def __init__(self, *, repository: MiniQMTExecutionRuntimeRepository, config: QuoteIngressRuntimeConfig) -> None:
        self._repository = repository
        self._config = config
        self._high: deque[_QueuedEvidence] = deque()
        self._queued_high_hash_by_event_id: dict[str, str] = {}
        self._cadence: dict[tuple[str, str, datetime, str, int], _QueuedEvidence] = {}
        self._health_slots: dict[tuple[str, datetime, int, str], _QueuedEvidence] = {}
        self._history_by_symbol: dict[str, deque[NormalizedQuoteObservation]] = {}
        self._history_fifo: deque[tuple[str, str]] = deque()
        self._history_gap_symbols: set[str] = set()
        self._history_dropped_before_utc: dict[str, datetime] = {}
        self._history_generation_by_symbol: dict[str, tuple[str, int]] = {}
        self._pending: dict[tuple[str, str, int], _PendingMarkout] = {}
        self._terminal_series: set[str] = set()
        self._queued_markout_by_event_id: dict[str, tuple[str, str, int]] = {}
        self._queued_markout_series: set[str] = set()
        self._recovered_unproven_series: set[str] = set()
        self._recovered_future_series: set[str] = set()
        self._failed_symbols: set[str] = set()
        self._first_failure: dict[str, Any] | None = None
        self._last_failure: dict[str, Any] | None = None
        self._failure_count = 0
        self._status = "HEALTHY"

    def observe(self, observation: NormalizedQuoteObservation) -> None:
        """Record an accepted normalized quote in bounded memory only."""

        if observation.quote.source_exchange_time_utc is None:
            raise quote_contract_error(
                QuoteContractReasonCode.MARKOUT_HISTORY_UNAVAILABLE,
                "accepted markout history cannot omit source exchange time",
                context={"symbol": observation.quote.symbol, "market_data_id": observation.market_data_id},
            )
        symbol = observation.quote.symbol
        generation = (observation.quote.source_session_id, observation.quote.ingress_generation)
        previous_generation = self._history_generation_by_symbol.get(symbol)
        if previous_generation is not None and previous_generation != generation:
            self._history_gap_symbols.add(symbol)
            self._history_dropped_before_utc[symbol] = observation.quote.source_exchange_time_utc
        self._history_generation_by_symbol[symbol] = generation
        bucket = self._history_by_symbol.setdefault(symbol, deque())
        bucket.append(observation)
        self._history_fifo.append((symbol, observation.market_data_id))
        history_cutoff = observation.quote.source_exchange_time_utc - timedelta(
            seconds=900,
            milliseconds=self._config.mark_history_max_lag_ms,
        )
        self._trim_history_before(history_cutoff)
        while len(self._history_fifo) > self._config.mark_history_max_samples:
            symbol, market_data_id = self._history_fifo.popleft()
            stale_bucket = self._history_by_symbol.get(symbol)
            if stale_bucket and stale_bucket[0].market_data_id == market_data_id:
                removed = stale_bucket.popleft()
                self._history_gap_symbols.add(symbol)
                if removed.quote.source_exchange_time_utc is not None:
                    self._history_dropped_before_utc[symbol] = removed.quote.source_exchange_time_utc
                if not stale_bucket:
                    self._history_by_symbol.pop(symbol, None)

    def enqueue(self, evidence: MarketDataEvidenceV1, *, event_type: MiniQMTExecutionEventType) -> None:
        if event_type not in {
            MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED,
            MiniQMTExecutionEventType.QUOTE_REJECTED,
            MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED,
            MiniQMTExecutionEventType.QUOTE_OBSERVED,
            MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH,
        }:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "unsupported quote evidence event type")
        if event_type.value != evidence.runtime_event_type:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "capture type and runtime event type are not a registered combination",
                context={"capture_type": evidence.capture_type.value, "event_type": event_type.value},
            )
        if event_type == MiniQMTExecutionEventType.QUOTE_OBSERVED:
            self._enqueue_cadence(evidence, event_type=event_type)
            return
        if evidence.capture_type != EvidenceCaptureType.CHILD_RECEIPT and evidence.symbol in self._failed_symbols:
            raise quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_PERSIST_FAILED,
                "quote evidence symbol gate is closed after a terminal persistence failure",
                context={"runtime_id": evidence.runtime_id, "symbol": evidence.symbol},
            )
        event_id = self._event_id(evidence)
        existing_hash = self._queued_high_hash_by_event_id.get(event_id)
        if existing_hash is not None:
            if existing_hash == evidence.evidence_sha256:
                return
            self._failed_symbols.add(evidence.symbol or "")
            self._status = "FAILED"
            raise quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_IDEMPOTENCY_CONFLICT,
                "quote evidence outbox event identity conflicts with different content",
                context={"runtime_id": evidence.runtime_id, "event_id": event_id, "symbol": evidence.symbol},
            )
        active_high = sum(not item.terminal_failure for item in self._high)
        capacity = self._config.evidence_outbox_max_events
        if evidence.capture_type == EvidenceCaptureType.CHILD_RECEIPT:
            capacity += self._config.evidence_receipt_reserve_events
        reserved_receipts = sum(
            isinstance(item.evidence, MarketDataEvidenceV1)
            and item.evidence.capture_type == EvidenceCaptureType.CHILD_RECEIPT
            for item in self._high
        )
        receipt_reserve_full = (
            evidence.capture_type == EvidenceCaptureType.CHILD_RECEIPT
            and reserved_receipts >= self._config.evidence_receipt_reserve_events
        )
        if active_high >= capacity or receipt_reserve_full:
            self._record_failure(
                quote_contract_error(
                    QuoteContractReasonCode.EVIDENCE_OUTBOX_FULL,
                    "quote evidence high-priority outbox is full; evidence was not accepted",
                    context={"runtime_id": evidence.runtime_id, "symbol": evidence.symbol, "capture_type": evidence.capture_type.value},
                )
            )
            self._failed_symbols.add(evidence.symbol or "")
            raise quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_OUTBOX_FULL,
                "quote evidence high-priority outbox is full",
                context={"runtime_id": evidence.runtime_id, "symbol": evidence.symbol},
            )
        self._high.append(_QueuedEvidence(evidence=evidence, event_type=event_type))
        self._queued_high_hash_by_event_id[event_id] = evidence.evidence_sha256

    def enqueue_health(self, health: QuoteIngressHealthV1) -> None:
        """Queue a bounded periodic health fact without exposing a write API."""

        key = (health.runtime_id, health.window_start_utc, health.ingress_generation, health.status)
        existing = self._health_slots.get(key)
        if existing is not None and self._item_content_sha256(existing.evidence) != health.health_sha256:
            raise quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_IDEMPOTENCY_CONFLICT,
                "quote ingress health slot identity conflicts with different content",
                context={"runtime_id": health.runtime_id, "event_id": health.event_id},
            )
        self._health_slots[key] = _QueuedEvidence(
            evidence=health,
            event_type=MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH,
        )

    def schedule_markouts(self, anchor: MarkoutAnchor) -> None:
        for horizon_seconds in _MARKOUT_CAPTURE_TYPES:
            pending = _PendingMarkout(anchor=anchor, horizon_seconds=horizon_seconds)
            if pending.mark_series_key not in self._terminal_series:
                self._pending[(anchor.runtime_id, pending.mark_series_key, horizon_seconds)] = pending

    def rebuild_pending_markouts(
        self,
        *,
        events: Iterable[MiniQMTExecutionEvent],
        anchors: Iterable[MarkoutAnchor] = (),
        recovered_at_utc: datetime,
    ) -> None:
        """Rebuild pending timers from journal anchors and terminal mark evidence."""

        if recovered_at_utc.tzinfo is None or recovered_at_utc.utcoffset() is None:
            raise ValueError("markout recovery time must be timezone-aware")
        recovered_at = recovered_at_utc.astimezone(UTC)
        event_rows = tuple(events)
        completed = {
            str(event.payload.get("evidence", {}).get("mark_series_key"))
            for event in event_rows
            if event.event_type == MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED
            and isinstance(event.payload, dict)
            and isinstance(event.payload.get("evidence"), dict)
            and str(event.payload["evidence"].get("capture_type") or "").startswith("MARKOUT_")
            and event.payload["evidence"].get("mark_series_key")
        }
        self._terminal_series.update(completed)
        recovered_anchors = [anchor for event in event_rows if (anchor := MarkoutAnchor.from_trade_event(event)) is not None]
        anchors_by_trade = {anchor.anchor_trade_event_id: anchor for anchor in recovered_anchors}
        anchors_by_trade.update({anchor.anchor_trade_event_id: anchor for anchor in anchors})
        for anchor in anchors_by_trade.values():
            for horizon_seconds in _MARKOUT_CAPTURE_TYPES:
                pending = _PendingMarkout(anchor=anchor, horizon_seconds=horizon_seconds)
                if pending.mark_series_key not in completed:
                    self._pending[(anchor.runtime_id, pending.mark_series_key, horizon_seconds)] = pending
                    if pending.target_time_utc <= recovered_at:
                        self._recovered_unproven_series.add(pending.mark_series_key)
                    else:
                        self._recovered_future_series.add(pending.mark_series_key)

    def drain_markouts(self, *, now_utc: datetime) -> None:
        if now_utc.tzinfo is None:
            raise ValueError("markout drain requires timezone-aware now_utc")
        for key, pending in tuple(self._pending.items()):
            if pending.mark_series_key in self._terminal_series:
                self._pending.pop(key, None)
                continue
            if pending.mark_series_key in self._queued_markout_series:
                continue
            evidence = self._markout_evidence_if_due(pending, now_utc=now_utc.astimezone(UTC))
            if evidence is None:
                continue
            self.enqueue(evidence, event_type=MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED)
            event_id = self._event_id(evidence)
            self._queued_markout_by_event_id[event_id] = key
            self._queued_markout_series.add(pending.mark_series_key)

    def flush(self, *, now_utc: datetime | None = None) -> tuple[DurableEvidenceReceipt, ...]:
        """Persist a bounded batch; only registered transient failures retry.

        Retrying is pull-driven by the lifecycle tick.  This avoids sleeping in
        a writer/callback thread and makes the configured exponential backoff
        observable without ever converting an in-memory item into a success.
        """

        current_time = (now_utc or datetime.now(UTC)).astimezone(UTC)
        receipts: list[DurableEvidenceReceipt] = []
        for queue, cadence_key in self._flush_order():
            if len(receipts) >= self._config.evidence_flush_batch_size:
                break
            item = queue[0] if isinstance(queue, deque) else queue
            if item.retry_not_before_utc is not None and current_time < item.retry_not_before_utc:
                break
            try:
                receipt = self._persist(item)
            except QuoteEvidenceIdempotencyConflict as exc:
                item.attempts += 1
                self._record_failure(
                    quote_contract_error(
                        QuoteContractReasonCode.EVIDENCE_IDEMPOTENCY_CONFLICT,
                        "quote evidence deterministic event identity conflicts with persisted content",
                        context={
                            "runtime_id": item.evidence.runtime_id,
                            "event_id": self._item_event_id(item.evidence),
                            "content_sha256": self._item_content_sha256(item.evidence),
                            "exception_type": type(exc).__name__,
                        },
                    )
                )
                self._status = "FAILED"
                item.terminal_failure = True
                self._mark_symbol_failed(item)
                break
            except Exception as exc:  # noqa: BLE001 - converted to typed loud state below.
                item.attempts += 1
                self._record_failure(
                    quote_contract_error(
                        QuoteContractReasonCode.EVIDENCE_PERSIST_FAILED,
                        "quote evidence durable append failed",
                        context={
                            "runtime_id": item.evidence.runtime_id,
                            "event_id": self._item_event_id(item.evidence),
                            "attempt": item.attempts,
                            "exception_type": type(exc).__name__,
                        },
                    )
                )
                if not _is_registered_transient_persistence_error(exc) or item.attempts >= self._config.evidence_retry_max_attempts:
                    self._status = "FAILED"
                    item.terminal_failure = True
                    self._mark_symbol_failed(item)
                else:
                    delay_ms = min(
                        self._config.evidence_retry_max_backoff_ms,
                        self._config.evidence_retry_initial_backoff_ms * (2 ** (item.attempts - 1)),
                    )
                    item.retry_not_before_utc = current_time + timedelta(milliseconds=delay_ms)
                break
            receipts.append(receipt)
            self._complete_durable_markout(receipt)
            if isinstance(queue, deque):
                completed = queue.popleft()
                self._queued_high_hash_by_event_id.pop(self._item_event_id(completed.evidence), None)
            elif cadence_key is not None and cadence_key in self._cadence:
                self._cadence.pop(cadence_key, None)
            elif cadence_key is not None and cadence_key in self._health_slots:
                self._health_slots.pop(cadence_key, None)
            if (
                not any(not queued.terminal_failure for queued in self._high)
                and not any(item.terminal_failure for item in self._cadence.values())
                and not any(item.terminal_failure for item in self._health_slots.values())
                and not self._failed_symbols
            ):
                self._status = "HEALTHY"
        return tuple(receipts)

    def health(self) -> QuoteEvidenceHealth:
        return QuoteEvidenceHealth(
            status=self._status,
            high_priority_backlog=sum(not item.terminal_failure for item in self._high),
            cadence_slots=len(self._cadence),
            health_slots=len(self._health_slots),
            persistence_failures=self._failure_count,
            first_failure=self._first_failure,
            last_failure=self._last_failure,
            occurrence_count=self._failure_count,
            history_samples=len(self._history_fifo),
            history_gap_symbols=tuple(sorted(self._history_gap_symbols)),
            outbox_capacity=self._config.evidence_outbox_max_events,
        )

    def can_accept_action(self, symbol: str) -> bool:
        """Fail closed only for the affected symbol and active high-priority capacity."""

        normalized_symbol = str(symbol).strip().upper()
        return bool(normalized_symbol) and normalized_symbol not in self._failed_symbols and sum(
            not item.terminal_failure for item in self._high
        ) < self._config.evidence_outbox_max_events

    def assert_action_gate_open(self, symbol: str) -> None:
        if not self.can_accept_action(symbol):
            raise quote_contract_error(
                QuoteContractReasonCode.EVIDENCE_PERSIST_FAILED,
                "quote evidence action gate is closed for the symbol",
                context={"symbol": str(symbol).strip().upper(), "failed_symbols": sorted(self._failed_symbols)},
            )

    def _trim_history_before(self, cutoff_utc: datetime) -> None:
        removed_ids: set[tuple[str, str]] = set()
        for symbol, bucket in tuple(self._history_by_symbol.items()):
            while bucket:
                source_time = bucket[0].quote.source_exchange_time_utc
                if source_time is None or source_time >= cutoff_utc:
                    break
                removed = bucket.popleft()
                removed_ids.add((symbol, removed.market_data_id))
                self._history_gap_symbols.add(symbol)
                self._history_dropped_before_utc[symbol] = source_time
            if not bucket:
                self._history_by_symbol.pop(symbol, None)
        if removed_ids:
            self._history_fifo = deque(item for item in self._history_fifo if item not in removed_ids)

    def _mark_symbol_failed(self, item: _QueuedEvidence) -> None:
        if isinstance(item.evidence, MarketDataEvidenceV1) and item.evidence.symbol:
            self._failed_symbols.add(item.evidence.symbol)

    def _complete_durable_markout(self, receipt: DurableEvidenceReceipt) -> None:
        pending_key = self._queued_markout_by_event_id.pop(receipt.event.event_id, None)
        if pending_key is None:
            return
        pending = self._pending.pop(pending_key, None)
        if pending is None:
            raise RuntimeError(f"durable markout receipt has no pending state: {receipt.event.event_id}")
        self._terminal_series.add(pending.mark_series_key)
        self._queued_markout_series.discard(pending.mark_series_key)
        self._recovered_unproven_series.discard(pending.mark_series_key)
        self._recovered_future_series.discard(pending.mark_series_key)

    def _flush_order(self) -> Iterable[tuple[deque[_QueuedEvidence] | _QueuedEvidence, tuple[Any, ...] | None]]:
        scanned_high = 0
        while self._high and scanned_high < len(self._high):
            if self._high[0].terminal_failure:
                # Retain the failed item for diagnostics/recovery but do not
                # repeatedly retry a non-retryable invariant failure or let it
                # block another symbol's independently durable evidence path.
                self._high.rotate(-1)
                scanned_high += 1
                continue
            yield self._high, None
            scanned_high = 0
        for key in tuple(sorted(self._health_slots, key=lambda value: (value[0], value[1], value[2], value[3]))):
            item = self._health_slots[key]
            if not item.terminal_failure:
                yield item, key
        for key in tuple(sorted(self._cadence, key=lambda value: (value[0], value[1], value[2], value[3], value[4]))):
            item = self._cadence[key]
            if not item.terminal_failure:
                yield item, key

    def _persist(self, item: _QueuedEvidence) -> DurableEvidenceReceipt:
        event_id = self._item_event_id(item.evidence)
        payload = item.evidence.runtime_payload()
        candidate = QuoteEvidenceEventCandidate(
            event_id=event_id,
            runtime_id=item.evidence.runtime_id,
            event_type=item.event_type,
            event_time=self._item_event_time(item.evidence),
            payload=payload,
            evidence_sha256=self._item_content_sha256(item.evidence),
            evidence_contract=item.evidence if isinstance(item.evidence, MarketDataEvidenceV1) else None,
        )
        return self._repository.append_evidence_event_idempotent(candidate)

    @staticmethod
    def _event_id(evidence: MarketDataEvidenceV1) -> str:
        return "mqrtevt_" + canonical_sha256({"schema": "miniqmt_quote_event_v1", "evidence_id": evidence.evidence_id})

    @classmethod
    def _item_event_id(cls, item: MarketDataEvidenceV1 | QuoteIngressHealthV1) -> str:
        return item.event_id if isinstance(item, QuoteIngressHealthV1) else cls._event_id(item)

    @staticmethod
    def _item_content_sha256(item: MarketDataEvidenceV1 | QuoteIngressHealthV1) -> str:
        return item.health_sha256 if isinstance(item, QuoteIngressHealthV1) else item.evidence_sha256

    @staticmethod
    def _item_event_time(item: MarketDataEvidenceV1 | QuoteIngressHealthV1) -> datetime:
        return item.window_start_utc if isinstance(item, QuoteIngressHealthV1) else item.event_time_utc

    def _enqueue_cadence(self, evidence: MarketDataEvidenceV1, *, event_type: MiniQMTExecutionEventType) -> None:
        if evidence.cadence_window_start_utc is None:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "cadence evidence requires cadence_window_start_utc")
        if evidence.source_session_id is None or evidence.ingress_generation is None:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "cadence evidence requires session and generation")
        key = (
            evidence.runtime_id,
            evidence.symbol or "",
            evidence.cadence_window_start_utc,
            evidence.source_session_id,
            evidence.ingress_generation,
        )
        existing = self._cadence.get(key)
        if existing is None:
            self._cadence[key] = _QueuedEvidence(evidence=evidence, event_type=event_type)
            return
        if existing.evidence.evidence_sha256 == evidence.evidence_sha256:
            return
        if not isinstance(existing.evidence, MarketDataEvidenceV1):
            raise RuntimeError("cadence slot cannot contain a non-evidence health payload")
        self._cadence[key] = _QueuedEvidence(
            evidence=_merge_cadence_evidence(existing.evidence, evidence),
            event_type=event_type,
        )

    def _markout_evidence_if_due(self, pending: _PendingMarkout, *, now_utc: datetime) -> MarketDataEvidenceV1 | None:
        target = pending.target_time_utc.astimezone(UTC)
        segment_end = pending.anchor.continuous_segment_end_utc.astimezone(UTC)
        if target.date() != pending.anchor.trade_date or target >= segment_end:
            return self._build_unavailable_markout(pending, QuoteContractReasonCode.MARKOUT_MARKET_SESSION_ENDED)
        if pending.mark_series_key in self._recovered_unproven_series and now_utc >= target:
            return self._build_unavailable_markout(
                pending,
                QuoteContractReasonCode.MARKOUT_RECOVERY_FIRST_QUOTE_UNPROVABLE,
            )
        deadline = min(target + timedelta(milliseconds=pending.anchor.markout_max_lag_ms), segment_end)
        if now_utc >= segment_end:
            return self._build_unavailable_markout(pending, QuoteContractReasonCode.MARKOUT_MARKET_SESSION_ENDED)
        dropped_before = self._history_dropped_before_utc.get(pending.anchor.symbol)
        if pending.anchor.symbol in self._history_gap_symbols and dropped_before is not None and target <= dropped_before <= deadline:
            # A bounded-history or generation gap may have discarded the first
            # post-target quote.  Selecting a later survivor would bias the
            # markout, so terminal UNAVAILABLE is the only valid outcome.
            return self._build_unavailable_markout(pending, QuoteContractReasonCode.MARKOUT_HISTORY_UNAVAILABLE)
        candidates = [
            item
            for item in self._history_by_symbol.get(pending.anchor.symbol, ())
            if item.quote.source_exchange_time_utc is not None
            and item.quote.source_trade_date == pending.anchor.trade_date
            and (
                (
                    pending.mark_series_key in self._recovered_future_series
                    and self._history_generation_by_symbol.get(pending.anchor.symbol)
                    == (item.quote.source_session_id, item.quote.ingress_generation)
                )
                or (
                    pending.mark_series_key not in self._recovered_future_series
                    and item.quote.source_session_id == pending.anchor.source_session_id
                    and item.quote.ingress_generation == pending.anchor.ingress_generation
                )
            )
            and target <= item.quote.source_exchange_time_utc <= deadline
            and item.quote.validation_state == QuoteValidationState.VALID
            and item.tradability is not None
            and item.tradability.state == TradabilityState.TRADABLE
        ]
        if candidates:
            selected = min(
                candidates,
                key=lambda item: (item.quote.source_exchange_time_utc, item.quote.ingress_sequence),
            )
            return self._build_captured_markout(pending, selected)
        if now_utc < deadline:
            return None
        if pending.anchor.symbol in self._history_gap_symbols and (
            dropped_before is None or target <= dropped_before
        ):
            reason = QuoteContractReasonCode.MARKOUT_HISTORY_UNAVAILABLE
        elif deadline == segment_end:
            reason = QuoteContractReasonCode.MARKOUT_MARKET_SESSION_ENDED
        else:
            reason = QuoteContractReasonCode.MARKOUT_MARK_WINDOW_EXPIRED
        return self._build_unavailable_markout(pending, reason)

    def _build_captured_markout(self, pending: _PendingMarkout, observation: NormalizedQuoteObservation) -> MarketDataEvidenceV1:
        anchor = pending.anchor
        return MarketDataEvidenceV1(
            market_data_id=observation.market_data_id,
            evidence_schema_version="miniqmt_market_data_evidence_v1",
            capture_type=_MARKOUT_CAPTURE_TYPES[pending.horizon_seconds],
            runtime_id=anchor.runtime_id,
            binding_id=anchor.binding_id,
            trade_date=anchor.trade_date,
            parent_intent_id=anchor.parent_intent_id,
            child_order_id=anchor.child_order_id,
            action_id=anchor.action_id,
            quote=observation.quote,
            tradability=observation.tradability,
            clock_event_id=anchor.clock_event_id,
            quality_reason_code=None,
            stage=None,
            control_revision=ControlRevision.B0_QUOTE_V2,
            policy_sha256=anchor.policy_sha256,
            config_sha256=anchor.config_sha256,
            adapter_sha256=anchor.adapter_sha256,
            code_sha256=anchor.code_sha256,
            schema_sha256=anchor.schema_sha256,
            calendar_sha256=anchor.calendar_sha256,
            captured_at_utc=observation.quote.received_at_utc,
            persisted_at_utc=None,
            quote_age_ms=None,
            source_lag_ms=None,
            transport_lag_ms=None,
            benchmark_policy_version=anchor.benchmark_policy_version,
            mark_policy_version=anchor.mark_policy_version,
            source_input_sha256=None,
            algo_instance_id=anchor.algo_instance_id,
            trade_id=anchor.trade_id,
            symbol=anchor.symbol,
            side=anchor.side,
            anchor_market_data_id=anchor.anchor_market_data_id,
            action_evidence_id=anchor.action_evidence_id,
            mark_series_key=pending.mark_series_key,
            horizon_seconds=pending.horizon_seconds,
            target_time_utc=pending.target_time_utc,
            anchor_trade_event_id=anchor.anchor_trade_event_id,
            mark_status=EvidenceMarkStatus.CAPTURED,
            source_session_id=observation.quote.source_session_id,
            ingress_generation=observation.quote.ingress_generation,
        )

    def _build_unavailable_markout(self, pending: _PendingMarkout, reason: QuoteContractReasonCode) -> MarketDataEvidenceV1:
        anchor = pending.anchor
        return MarketDataEvidenceV1(
            market_data_id=None,
            evidence_schema_version="miniqmt_market_data_evidence_v1",
            capture_type=_MARKOUT_CAPTURE_TYPES[pending.horizon_seconds],
            runtime_id=anchor.runtime_id,
            binding_id=anchor.binding_id,
            trade_date=anchor.trade_date,
            parent_intent_id=anchor.parent_intent_id,
            child_order_id=anchor.child_order_id,
            action_id=anchor.action_id,
            quote=None,
            tradability=None,
            clock_event_id=anchor.clock_event_id,
            quality_reason_code=reason,
            stage="MARKOUT",
            control_revision=ControlRevision.B0_QUOTE_V2,
            policy_sha256=anchor.policy_sha256,
            config_sha256=anchor.config_sha256,
            adapter_sha256=anchor.adapter_sha256,
            code_sha256=anchor.code_sha256,
            schema_sha256=anchor.schema_sha256,
            calendar_sha256=anchor.calendar_sha256,
            captured_at_utc=pending.target_time_utc,
            persisted_at_utc=None,
            quote_age_ms=None,
            source_lag_ms=None,
            transport_lag_ms=None,
            benchmark_policy_version=anchor.benchmark_policy_version,
            mark_policy_version=anchor.mark_policy_version,
            source_input_sha256=None,
            algo_instance_id=anchor.algo_instance_id,
            trade_id=anchor.trade_id,
            symbol=anchor.symbol,
            side=anchor.side,
            anchor_market_data_id=anchor.anchor_market_data_id,
            action_evidence_id=anchor.action_evidence_id,
            mark_series_key=pending.mark_series_key,
            horizon_seconds=pending.horizon_seconds,
            target_time_utc=pending.target_time_utc,
            anchor_trade_event_id=anchor.anchor_trade_event_id,
            mark_status=EvidenceMarkStatus.UNAVAILABLE,
            unavailable_reason=reason,
            source_session_id=anchor.source_session_id,
            ingress_generation=anchor.ingress_generation,
        )

    def _record_failure(self, error: QuoteContractError) -> None:
        payload = error.as_loud_payload()
        self._failure_count += 1
        if self._first_failure is None:
            self._first_failure = payload
        self._last_failure = payload
        self._status = "DEGRADED"


_REGISTERED_TRANSIENT_SQLSTATES = frozenset(
    {
        "08000",  # connection_exception
        "08001",  # sqlclient_unable_to_establish_sqlconnection
        "08003",  # connection_does_not_exist
        "08006",  # connection_failure
        "40001",  # serialization_failure
        "40P01",  # deadlock_detected
        "55P03",  # lock_not_available / lock timeout
        "57P01",  # admin_shutdown / connection interrupted
    }
)


def _is_registered_transient_persistence_error(exc: Exception) -> bool:
    """Keep retry policy explicit; schema/hash/invariant errors never retry."""

    context = getattr(exc, "context", {})
    context_sqlstate = context.get("pgcode") if isinstance(context, Mapping) else None
    sqlstate = str(getattr(exc, "pgcode", "") or context_sqlstate or "")
    return sqlstate in _REGISTERED_TRANSIENT_SQLSTATES


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _strict_non_negative_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"TRADE_EVENT quote evidence {field_name} must be a non-negative integer")
    return value


def _strict_positive_int(value: Any, *, field_name: str) -> int:
    parsed = _strict_non_negative_int(value, field_name=field_name)
    if parsed == 0:
        raise ValueError(f"TRADE_EVENT quote evidence {field_name} must be positive")
    return parsed


def _parse_utc_timestamp(value: Any, *, field_name: str) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"TRADE_EVENT quote evidence {field_name} must include an explicit UTC offset")
    return parsed.astimezone(UTC)


def _merge_cadence_evidence(existing: MarketDataEvidenceV1, incoming: MarketDataEvidenceV1) -> MarketDataEvidenceV1:
    """Deterministically coalesce only a single cadence slot's raw deltas."""

    if (
        existing.capture_type != EvidenceCaptureType.CADENCE_AGGREGATE
        or incoming.capture_type != EvidenceCaptureType.CADENCE_AGGREGATE
        or existing.runtime_id != incoming.runtime_id
        or existing.symbol != incoming.symbol
        or existing.cadence_window_start_utc != incoming.cadence_window_start_utc
        or existing.source_session_id != incoming.source_session_id
        or existing.ingress_generation != incoming.ingress_generation
    ):
        raise ValueError("cadence coalescing requires the exact same runtime/symbol/window/session/generation")
    assert existing.cadence_counts is not None and incoming.cadence_counts is not None
    merged_counts = {key: existing.cadence_counts[key] + incoming.cadence_counts[key] for key in existing.cadence_counts}
    return replace(
        existing,
        evidence_id=None,
        evidence_revision=1,
        supersedes_evidence_id=None,
        captured_at_utc=max(existing.captured_at_utc, incoming.captured_at_utc),
        source_input_sha256=None,
        cadence_counts=merged_counts,
        cadence_last_accepted_sha256=incoming.cadence_last_accepted_sha256,
    )


__all__ = ["MarkoutAnchor", "QuoteEvidenceCoordinator", "QuoteEvidenceHealth", "QuoteIngressHealthV1"]
