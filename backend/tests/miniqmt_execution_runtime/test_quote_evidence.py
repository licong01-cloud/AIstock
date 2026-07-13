from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from dataclasses import replace
from decimal import Decimal
import inspect

import pytest

from backend.execution_algos.adaptive_is.contracts import (
    MARKET_DATA_EVIDENCE_SCHEMA_VERSION,
    QUOTE_CONTRACT_SCHEMA_VERSION,
    ControlRevision,
    DepthQuantityUnit,
    EligibilityState,
    EvidenceCaptureType,
    EvidenceMarkStatus,
    FiveLevelQuote,
    MarketCode,
    MarketDataEvidenceV1,
    PriceBasis,
    QuoteCapability,
    QuoteSource,
    QuoteSourceMethod,
    TradabilitySnapshot,
    TradabilityState,
)
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode
from backend.miniqmt_quote_contract_config import QuoteIngressRuntimeConfig
from backend.services.miniqmt_execution_runtime.models import (
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeRecord,
)
from backend.services.miniqmt_execution_runtime.quote_evidence import (
    MarkoutAnchor,
    QuoteEvidenceCoordinator,
    QuoteIngressHealthV1,
)
from backend.services.miniqmt_execution_runtime.quote_eligibility import NormalizedQuoteObservation, OrderingDisposition
from backend.services.miniqmt_execution_runtime.quote_normalizer import (
    MINIQMT_NORMALIZER_MAP_VERSION,
    MINIQMT_TIMESTAMP_PARSER_VERSION,
    RAW_QUOTE_FRAME_SCHEMA_VERSION,
    RawQuoteFrame,
)
from backend.services.miniqmt_execution_runtime.repository import (
    InMemoryMiniQMTExecutionRuntimeRepository,
    JsonFileMiniQMTExecutionRuntimeRepository,
    PostgresMiniQMTExecutionRuntimeRepository,
    QuoteEvidenceEventCandidate,
    QuoteEvidenceIdempotencyConflict,
)


def _sha(char: str) -> str:
    return char * 64


def _config() -> QuoteIngressRuntimeConfig:
    return QuoteIngressRuntimeConfig(
        enabled=False,
        owner_mode="simulation_scheduler",
        max_symbols=8,
        drain_budget=8,
        heartbeat_timeout_ms=1000,
        restart_backoff_ms=100,
        restart_max_backoff_ms=1000,
        loud_interval_seconds=30,
        evidence_outbox_max_events=4,
        evidence_flush_batch_size=4,
    )


def _quote(*, at: datetime | None = None, sequence: int = 7, generation: int = 2) -> FiveLevelQuote:
    exchange_time = at or datetime(2026, 7, 13, 1, 31, tzinfo=UTC)
    return FiveLevelQuote(
        schema_version=QUOTE_CONTRACT_SCHEMA_VERSION,
        normalizer_map_version="miniqmt_quote_normalizer_map_v2",
        timestamp_parser_version="miniqmt_quote_timestamp_parser_v2",
        source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        source_session_id="quote-session",
        ingress_generation=generation,
        ingress_sequence=sequence,
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
        symbol="000001.SZ",
        market=MarketCode.SZ,
        board="MAIN",
        source_exchange_time_utc=exchange_time,
        source_trade_date=date(2026, 7, 13),
        clock_trade_date=date(2026, 7, 13),
        received_at_utc=exchange_time + timedelta(milliseconds=100),
        received_monotonic_ns=sequence * 1000,
        clock_domain_id="quote-test-clock",
        last_price=Decimal("10.00"),
        pre_close=Decimal("9.80"),
        total_volume=Decimal("1000"),
        total_amount=Decimal("10000"),
        security_status="NORMAL",
        openint_status="OPEN",
        price_basis=PriceBasis.RAW_CNY_PER_SHARE,
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="unit-v1",
        bid_prices=(Decimal("9.99"), Decimal("9.98"), None, None, None),
        bid_quantities=(100, 100, 0, 0, 0),
        bid_quantities_raw=(Decimal("100"), Decimal("100"), Decimal("0"), Decimal("0"), Decimal("0")),
        ask_prices=(Decimal("10.01"), Decimal("10.02"), None, None, None),
        ask_quantities=(100, 100, 0, 0, 0),
        ask_quantities_raw=(Decimal("100"), Decimal("100"), Decimal("0"), Decimal("0"), Decimal("0")),
        quote_capabilities=frozenset(
            {
                QuoteCapability.FIVE_LEVEL_DEPTH,
                QuoteCapability.EXCHANGE_TIMESTAMP,
                QuoteCapability.RAW_PRICE_BASIS,
                QuoteCapability.DEPTH_UNIT_SHARES,
            }
        ),
        source_payload_sha256=_sha("a"),
    )


def _tradability() -> TradabilitySnapshot:
    return TradabilitySnapshot(
        schema_version="tradability-v1",
        tradability_id="trad-1",
        symbol="000001.SZ",
        market=MarketCode.SZ,
        board="MAIN",
        trade_date=date(2026, 7, 13),
        price_basis=PriceBasis.RAW_CNY_PER_SHARE,
        pre_close=Decimal("9.80"),
        limit_up=Decimal("10.78"),
        limit_down=Decimal("8.82"),
        price_tick=Decimal("0.01"),
        lot_size=100,
        is_suspended=False,
        suspension_source=None,
        security_status="NORMAL",
        openint_status="OPEN",
        observed_at_utc=datetime(2026, 7, 13, 1, tzinfo=UTC),
        source="test",
        source_version="v1",
        state=TradabilityState.TRADABLE,
    )


def _quote_for_symbol(symbol: str) -> FiveLevelQuote:
    return replace(_quote(), symbol=symbol)


def _tradability_for_symbol(symbol: str) -> TradabilitySnapshot:
    return replace(_tradability(), symbol=symbol)


def _evidence(capture_type: EvidenceCaptureType = EvidenceCaptureType.ACTION_INPUT, **overrides: object) -> MarketDataEvidenceV1:
    quote = _quote()
    tradability = _tradability()
    fields: dict[str, object] = {
        "market_data_id": "md-action-1",
        "evidence_schema_version": MARKET_DATA_EVIDENCE_SCHEMA_VERSION,
        "capture_type": capture_type,
        "runtime_id": "runtime-1",
        "binding_id": "binding-1",
        "trade_date": date(2026, 7, 13),
        "parent_intent_id": "parent-1",
        "child_order_id": None,
        "action_id": "action-1",
        "quote": quote,
        "tradability": tradability,
        "clock_event_id": "clock-1",
        "quality_reason_code": None,
        "stage": None,
        "control_revision": ControlRevision.B0_QUOTE_V2,
        "policy_sha256": _sha("b"),
        "config_sha256": _sha("c"),
        "adapter_sha256": _sha("d"),
        "code_sha256": _sha("e"),
        "schema_sha256": _sha("f"),
        "calendar_sha256": _sha("1"),
        "captured_at_utc": quote.received_at_utc,
        "persisted_at_utc": None,
        "quote_age_ms": 100,
        "source_lag_ms": 100,
        "transport_lag_ms": 0,
        "benchmark_policy_version": "arrival-v1",
        "mark_policy_version": "mark-v1",
        "source_input_sha256": None,
        "algo_instance_id": "algo-1",
        "side": "BUY",
        "eligibility_state": EligibilityState.READY,
    }
    if capture_type == EvidenceCaptureType.ACTION_REJECT:
        fields.update(
            {
                "action_id": None,
                "quote": None,
                "tradability": None,
                "market_data_id": None,
                "symbol": "000001.SZ",
                "quality_reason_code": QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
                "stage": "ELIGIBILITY",
                "eligibility_state": EligibilityState.STALE,
                "source_session_id": "quote-session",
                "ingress_generation": 2,
                "ingress_sequence": 7,
                "quote_source": QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
                "source_method": QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
                "source_payload_sha256": _sha("a"),
            }
        )
    elif capture_type == EvidenceCaptureType.CHILD_RECEIPT:
        fields.update(
            {
                "child_order_id": "child-1",
                "source_child_event_id": "child-event-1",
                "action_evidence_id": "mde-action-1",
                "anchor_market_data_id": "md-action-1",
                "broker_order_id": "broker-1",
            }
        )
    elif capture_type == EvidenceCaptureType.PROTECTION_BAND_TRIGGER:
        fields.update(
            {
                "child_order_id": "child-1",
                "source_child_event_id": "trigger-1",
                "action_evidence_id": "mde-action-1",
                "anchor_market_data_id": "md-action-1",
            }
        )
    elif capture_type in {EvidenceCaptureType.MARKOUT_60S, EvidenceCaptureType.MARKOUT_300S, EvidenceCaptureType.MARKOUT_900S}:
        horizon = int(capture_type.value.removeprefix("MARKOUT_").removesuffix("S"))
        fields.update(
            {
                "child_order_id": "child-1",
                "trade_id": "trade-1",
                "anchor_trade_event_id": "trade-event-1",
                "anchor_market_data_id": "md-action-1",
                "action_evidence_id": "mde-action-1",
                "mark_series_key": f"series-{horizon}",
                "horizon_seconds": horizon,
                "target_time_utc": datetime(2026, 7, 13, 1, 32, tzinfo=UTC),
                "mark_status": EvidenceMarkStatus.CAPTURED,
            }
        )
    elif capture_type == EvidenceCaptureType.CADENCE_AGGREGATE:
        fields.update(
            {
                "market_data_id": None,
                "binding_id": None,
                "parent_intent_id": None,
                "child_order_id": None,
                "action_id": None,
                "quote": None,
                "tradability": None,
                "clock_event_id": "clock-1",
                "algo_instance_id": None,
                "side": None,
                "eligibility_state": None,
                "symbol": "000001.SZ",
                "source_session_id": "quote-session",
                "ingress_generation": 2,
                "ingress_sequence": None,
                "quote_age_ms": None,
                "source_lag_ms": None,
                "transport_lag_ms": None,
                "cadence_window_start_utc": datetime(2026, 7, 13, 1, 30, tzinfo=UTC),
                "cadence_counts": {"accepted": 2, "rejected": 1, "coalesced": 3, "capacity_rejected": 0, "coverage": 1},
                "cadence_first_accepted_sha256": _sha("3"),
                "cadence_last_accepted_sha256": _sha("4"),
            }
        )
    fields.update(overrides)
    return MarketDataEvidenceV1(**fields)  # type: ignore[arg-type]


def _runtime(repo: InMemoryMiniQMTExecutionRuntimeRepository) -> None:
    repo.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            **MiniQMTExecutionRuntimeConfig(
                runtime_id="runtime-1",
                account_group_id="account-group-1",
                trade_date=date(2026, 7, 13),
                runtime_config_hash=_sha("9"),
            ).model_dump()
        )
    )


def _observation(*, at: datetime, sequence: int, generation: int = 2) -> NormalizedQuoteObservation:
    quote = _quote(at=at, sequence=sequence, generation=generation)
    frame = RawQuoteFrame(
        schema_version=RAW_QUOTE_FRAME_SCHEMA_VERSION,
        normalizer_map_version=MINIQMT_NORMALIZER_MAP_VERSION,
        timestamp_parser_version=MINIQMT_TIMESTAMP_PARSER_VERSION,
        source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
        source_session_id=quote.source_session_id,
        ingress_generation=generation,
        ingress_sequence=sequence,
        symbol_raw="000001.SZ",
        symbol="000001.SZ",
        received_at_utc=quote.received_at_utc,
        received_monotonic_ns=quote.received_monotonic_ns,
        clock_domain_id=quote.clock_domain_id,
        source_timestamp_raw="20260713093100",
        whitelisted_raw_fields={"stock_code": "000001.SZ", "time": "20260713093100"},
    )
    return NormalizedQuoteObservation(
        frame=frame,
        quote=quote,
        tradability=_tradability(),
        context_id="ctx_unit_1",
        market_data_id=f"md_history_{sequence}",
        ordering_disposition=OrderingDisposition.ACCEPTED,
    )


def _anchor() -> MarkoutAnchor:
    return MarkoutAnchor(
        runtime_id="runtime-1",
        binding_id="binding-1",
        trade_date=date(2026, 7, 13),
        parent_intent_id="parent-1",
        algo_instance_id="algo-1",
        action_id="action-1",
        child_order_id="child-1",
        trade_id="trade-1",
        anchor_trade_event_id="trade-event-1",
        action_evidence_id="mde-action-1",
        anchor_market_data_id="md-action-1",
        symbol="000001.SZ",
        side="BUY",
        source_session_id="quote-session",
        ingress_generation=2,
        trade_time_utc=datetime(2026, 7, 13, 1, 30, tzinfo=UTC),
        continuous_segment_end_utc=datetime(2026, 7, 13, 3, 30, tzinfo=UTC),
        clock_event_id="clock-1",
        benchmark_policy_version="arrival-v1",
        mark_policy_version="mark-v1",
        markout_max_lag_ms=1_000,
        policy_sha256=_sha("b"),
        config_sha256=_sha("c"),
        adapter_sha256=_sha("d"),
        code_sha256=_sha("e"),
        schema_sha256=_sha("f"),
        calendar_sha256=_sha("1"),
    )


def test_market_data_evidence_v1_required_fields_by_capture_type() -> None:
    for capture_type in EvidenceCaptureType:
        evidence = _evidence(capture_type)
        assert evidence.capture_type == capture_type
    with pytest.raises(QuoteContractError):
        _evidence(EvidenceCaptureType.ACTION_INPUT, eligibility_state=EligibilityState.STALE)
    with pytest.raises(QuoteContractError):
        _evidence(EvidenceCaptureType.CADENCE_AGGREGATE, market_data_id="md-illegal")
    with pytest.raises(QuoteContractError):
        _evidence(EvidenceCaptureType.ACTION_INPUT, trade_id="forbidden-trade")
    with pytest.raises(QuoteContractError):
        _evidence(EvidenceCaptureType.ACTION_INPUT, source_input_sha256=_sha("9"))


def test_evidence_and_event_identity_are_deterministic_and_exclude_transport_fields() -> None:
    first = _evidence()
    second = _evidence(persisted_at_utc=first.captured_at_utc + timedelta(hours=1))
    assert first.evidence_id == second.evidence_id
    assert first.evidence_sha256 == second.evidence_sha256
    assert first.runtime_event_type == "QUOTE_ELIGIBILITY_EVALUATED"


def test_action_reject_links_market_data_when_present_and_never_reuses_old_quote_when_absent() -> None:
    quote_reject = _evidence(
        EvidenceCaptureType.ACTION_REJECT,
        quote=_quote(),
        tradability=_tradability(),
        market_data_id="md-current-reject",
        source_session_id=None,
        ingress_generation=None,
        ingress_sequence=None,
    )
    quote_less = _evidence(EvidenceCaptureType.ACTION_REJECT)
    assert quote_reject.market_data_id == "md-current-reject"
    assert quote_less.market_data_id is None
    assert quote_less.anchor_market_data_id is None


def test_action_requires_durable_ack_before_any_broker_submit() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    broker_calls: list[object] = []
    coordinator.enqueue(_evidence(), event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED)
    assert broker_calls == []
    assert coordinator.health().high_priority_backlog == 1
    coordinator.flush(now_utc=datetime(2026, 7, 13, 1, 31, tzinfo=UTC))
    assert broker_calls == []  # P1-D has no broker submit path at all.


def test_child_receipt_and_trade_markout_chain_rebuilds_from_market_data_id() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    action = _evidence()
    receipt = _evidence(
        EvidenceCaptureType.CHILD_RECEIPT,
        action_evidence_id=action.evidence_id,
        market_data_id="md-receipt-1",
    )
    coordinator.enqueue(action, event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED)
    coordinator.enqueue(receipt, event_type=MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED)
    coordinator.flush(now_utc=datetime(2026, 7, 13, 1, 31, tzinfo=UTC))
    matches = repo.list_evidence_receipts("runtime-1", market_data_id="md-action-1")
    assert {item.event.payload["evidence"]["capture_type"] for item in matches} == {"ACTION_INPUT", "CHILD_RECEIPT"}
    assert matches[1].event.payload["evidence"]["action_evidence_id"] == action.evidence_id
    assert matches[1].event.payload["evidence"]["child_receipt_evidence_id"] == receipt.evidence_id


def test_high_priority_outbox_never_drops_and_cadence_slot_only_coalesces_same_window() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    coordinator.enqueue(_evidence(), event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED)
    coordinator.enqueue(_evidence(EvidenceCaptureType.ACTION_REJECT), event_type=MiniQMTExecutionEventType.QUOTE_REJECTED)
    first = _evidence(EvidenceCaptureType.CADENCE_AGGREGATE)
    coordinator.enqueue(first, event_type=MiniQMTExecutionEventType.QUOTE_OBSERVED)
    coordinator.enqueue(
        _evidence(
            EvidenceCaptureType.CADENCE_AGGREGATE,
            cadence_counts={"accepted": 3, "rejected": 1, "coalesced": 4, "capacity_rejected": 0, "coverage": 2},
            cadence_last_accepted_sha256=_sha("5"),
        ),
        event_type=MiniQMTExecutionEventType.QUOTE_OBSERVED,
    )
    assert coordinator.health().high_priority_backlog == 2
    assert coordinator.health().cadence_slots == 1
    merged = next(iter(coordinator._cadence.values())).evidence
    assert merged.cadence_counts == {"accepted": 5, "rejected": 2, "coalesced": 7, "capacity_rejected": 0, "coverage": 3}
    assert merged.cadence_first_accepted_sha256 == _sha("3")
    assert merged.cadence_last_accepted_sha256 == _sha("5")
    coordinator.enqueue(
        _evidence(EvidenceCaptureType.CADENCE_AGGREGATE, ingress_generation=3),
        event_type=MiniQMTExecutionEventType.QUOTE_OBSERVED,
    )
    assert coordinator.health().cadence_slots == 2


def test_high_priority_outbox_deduplicates_and_terminal_failure_isolates_symbols() -> None:
    class BrokenRepository(InMemoryMiniQMTExecutionRuntimeRepository):
        def append_evidence_event_idempotent(self, candidate: object) -> object:
            raise RuntimeError("terminal persistence failure")

    repo = BrokenRepository()
    _runtime(repo)
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    first = _evidence()
    coordinator.enqueue(first, event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED)
    coordinator.enqueue(first, event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED)
    assert coordinator.health().high_priority_backlog == 1
    coordinator.flush(now_utc=datetime(2026, 7, 13, 1, 31, tzinfo=UTC))
    assert coordinator.can_accept_action("000001.SZ") is False
    with pytest.raises(QuoteContractError):
        coordinator.enqueue(
            _evidence(action_id="action-blocked"),
            event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED,
        )
    unrelated = _evidence(
        action_id="action-unrelated",
        symbol="000002.SZ",
        quote=_quote_for_symbol("000002.SZ"),
        tradability=_tradability_for_symbol("000002.SZ"),
    )
    coordinator.enqueue(unrelated, event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED)
    assert coordinator.health().high_priority_backlog == 1


def test_health_event_is_versioned_and_excludes_raw_session_identity_from_payload() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    coordinator.enqueue_health(
        QuoteIngressHealthV1(
            runtime_id="runtime-1",
            owner_mode="simulation_scheduler",
            source_session_id="quote-session",
            ingress_generation=2,
            config_sha256=_sha("c"),
            status="HEALTHY",
            window_start_utc=datetime(2026, 7, 13, 1, 30, tzinfo=UTC),
            counters={
                "accepted": 1,
                "rejected": 0,
                "coalesced": 0,
                "capacity_rejected": 0,
                "writer_restarts": 0,
                "persistence_failures": 0,
            },
        )
    )
    assert coordinator.health().high_priority_backlog == 0
    assert coordinator.health().health_slots == 1
    receipt = coordinator.flush(now_utc=datetime(2026, 7, 13, 1, 31, tzinfo=UTC))[0]
    health_payload = receipt.event.payload["health_or_aggregate"]
    assert receipt.event.event_type == MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH
    assert health_payload["source_session_id_sha256"] != "quote-session"


def test_persist_failure_is_loud_and_never_returns_durable_success() -> None:
    class BrokenRepository(InMemoryMiniQMTExecutionRuntimeRepository):
        def append_evidence_event_idempotent(self, candidate: object) -> object:
            raise RuntimeError("non-transient write failure")

    repo = BrokenRepository()
    _runtime(repo)
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    coordinator.enqueue(_evidence(), event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED)
    assert coordinator.flush(now_utc=datetime(2026, 7, 13, 1, 31, tzinfo=UTC)) == ()
    assert coordinator.health().status == "FAILED"
    assert coordinator.health().persistence_failures == 1
    assert coordinator.flush(now_utc=datetime(2026, 7, 13, 1, 32, tzinfo=UTC)) == ()
    assert coordinator.health().persistence_failures == 1


def test_registered_transient_persist_failure_retries_with_configured_backoff_only() -> None:
    class SerializationFailure(Exception):
        pgcode = "40001"

    class RetryRepository(InMemoryMiniQMTExecutionRuntimeRepository):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def append_evidence_event_idempotent(self, candidate: QuoteEvidenceEventCandidate):  # type: ignore[override]
            self.calls += 1
            if self.calls == 1:
                raise SerializationFailure("retryable")
            return super().append_evidence_event_idempotent(candidate)

    repo = RetryRepository()
    _runtime(repo)
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    coordinator.enqueue(_evidence(), event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED)
    now = datetime(2026, 7, 13, 1, 31, tzinfo=UTC)
    assert coordinator.flush(now_utc=now) == ()
    assert coordinator.flush(now_utc=now + timedelta(milliseconds=99)) == ()
    assert coordinator.flush(now_utc=now + timedelta(milliseconds=100))[0].durable_ack is True
    assert repo.calls == 2


def test_idempotent_retry_returns_original_sequence_and_conflicting_hash_fails() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    evidence = _evidence()
    coordinator.enqueue(evidence, event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED)
    receipt = coordinator.flush(now_utc=datetime(2026, 7, 13, 1, 31, tzinfo=UTC))[0]
    second = repo.append_evidence_event_idempotent(
        QuoteEvidenceEventCandidate(
            event_id=receipt.event.event_id,
            runtime_id="runtime-1",
            event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED,
            event_time=evidence.event_time_utc,
            payload=evidence.runtime_payload(),
            evidence_sha256=evidence.evidence_sha256,
            evidence_contract=evidence,
        )
    )
    assert second.event.sequence == receipt.event.sequence
    conflict_payload = evidence.runtime_payload()
    conflicting = QuoteEvidenceEventCandidate(
        event_id=receipt.event.event_id,
        runtime_id="runtime-1",
        event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED,
        event_time=evidence.event_time_utc,
        payload=evidence.runtime_payload(),
        evidence_sha256=evidence.evidence_sha256,
        evidence_contract=evidence,
    )
    conflict_payload["evidence"]["evidence_sha256"] = _sha("7")
    object.__setattr__(conflicting, "payload", conflict_payload)
    with pytest.raises(QuoteEvidenceIdempotencyConflict):
        repo.append_evidence_event_idempotent(conflicting)


def test_json_repository_durable_receipt_survives_restart(tmp_path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "miniqmt-runtime.json"
    repo = JsonFileMiniQMTExecutionRuntimeRepository(path)
    _runtime(repo)
    evidence = _evidence()
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    coordinator.enqueue(evidence, event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED)
    first = coordinator.flush(now_utc=datetime(2026, 7, 13, 1, 31, tzinfo=UTC))[0]
    restarted = JsonFileMiniQMTExecutionRuntimeRepository(path)
    readback = restarted.list_evidence_receipts("runtime-1", evidence_id=evidence.evidence_id)
    assert len(readback) == 1
    assert readback[0].event.event_id == first.event.event_id
    assert readback[0].persisted_at_utc == first.persisted_at_utc


def test_markout_60_300_900_selects_first_eligible_quote_after_target() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    anchor = _anchor()
    coordinator.schedule_markouts(anchor)
    for horizon in (60, 300, 900):
        coordinator.observe(_observation(at=anchor.trade_time_utc + timedelta(seconds=horizon, milliseconds=100), sequence=horizon))
    coordinator.drain_markouts(now_utc=anchor.trade_time_utc + timedelta(seconds=901))
    receipts = coordinator.flush(now_utc=anchor.trade_time_utc + timedelta(seconds=901))
    assert {item.event.payload["evidence"]["horizon_seconds"] for item in receipts} == {60, 300, 900}


def test_restart_rebuilds_pending_marks_without_duplicate_events() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    anchor = _anchor()
    first = QuoteEvidenceCoordinator(repository=repo, config=_config())
    first.schedule_markouts(anchor)
    first.observe(_observation(at=anchor.trade_time_utc + timedelta(seconds=60, milliseconds=1), sequence=60))
    first.drain_markouts(now_utc=anchor.trade_time_utc + timedelta(seconds=61))
    first.flush(now_utc=anchor.trade_time_utc + timedelta(seconds=61))
    trade_event = MiniQMTExecutionEvent(
        event_id=anchor.anchor_trade_event_id,
        runtime_id=anchor.runtime_id,
        sequence=1,
        event_type=MiniQMTExecutionEventType.TRADE_EVENT,
        event_time=anchor.trade_time_utc,
        source="oms",
        payload={
            "quote_evidence_markout_anchor_v1": {
                "schema_version": "miniqmt_quote_markout_anchor_v1",
                "control_revision": "B0_QUOTE_V2",
                "binding_id": anchor.binding_id,
                "trade_date": anchor.trade_date.isoformat(),
                "parent_intent_id": anchor.parent_intent_id,
                "algo_instance_id": anchor.algo_instance_id,
                "action_id": anchor.action_id,
                "child_order_id": anchor.child_order_id,
                "trade_id": anchor.trade_id,
                "action_evidence_id": anchor.action_evidence_id,
                "anchor_market_data_id": anchor.anchor_market_data_id,
                "symbol": anchor.symbol,
                "side": anchor.side,
                "source_session_id": anchor.source_session_id,
                "ingress_generation": anchor.ingress_generation,
                "trade_time_utc": anchor.trade_time_utc.isoformat(),
                "continuous_segment_end_utc": anchor.continuous_segment_end_utc.isoformat(),
                "clock_event_id": anchor.clock_event_id,
                "benchmark_policy_version": anchor.benchmark_policy_version,
                "mark_policy_version": anchor.mark_policy_version,
                "markout_max_lag_ms": anchor.markout_max_lag_ms,
                "policy_sha256": anchor.policy_sha256,
                "config_sha256": anchor.config_sha256,
                "adapter_sha256": anchor.adapter_sha256,
                "code_sha256": anchor.code_sha256,
                "schema_sha256": anchor.schema_sha256,
                "calendar_sha256": anchor.calendar_sha256,
            }
        },
    )
    restarted = QuoteEvidenceCoordinator(repository=repo, config=_config())
    restarted.rebuild_pending_markouts(
        events=(trade_event, *repo.list_events("runtime-1")),
        recovered_at_utc=anchor.trade_time_utc + timedelta(seconds=301),
    )
    assert len(restarted._pending) == 2
    restarted.drain_markouts(now_utc=anchor.trade_time_utc + timedelta(seconds=301))
    assert restarted._high[0].evidence.unavailable_reason == QuoteContractReasonCode.MARKOUT_RECOVERY_FIRST_QUOTE_UNPROVABLE
    invalid_generation = trade_event.model_copy(deep=True)
    invalid_generation.payload["quote_evidence_markout_anchor_v1"]["ingress_generation"] = True
    with pytest.raises(ValueError, match="non-negative integer"):
        MarkoutAnchor.from_trade_event(invalid_generation)


def test_restart_before_future_target_captures_first_proven_post_target_quote() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    anchor = _anchor()
    restarted = QuoteEvidenceCoordinator(repository=repo, config=_config())
    restarted.rebuild_pending_markouts(
        events=(),
        anchors=(anchor,),
        recovered_at_utc=anchor.trade_time_utc + timedelta(seconds=10),
    )
    restarted.observe(_observation(at=anchor.trade_time_utc + timedelta(seconds=59), sequence=59, generation=3))
    restarted.observe(_observation(at=anchor.trade_time_utc + timedelta(seconds=60, milliseconds=1), sequence=60, generation=3))
    restarted.drain_markouts(now_utc=anchor.trade_time_utc + timedelta(seconds=61))
    receipt = restarted.flush(now_utc=anchor.trade_time_utc + timedelta(seconds=61))[0]
    assert receipt.event.payload["evidence"]["mark_status"] == "CAPTURED"
    assert receipt.event.payload["evidence"]["ingress_generation"] == 3


def test_markout_is_not_terminal_until_durable_receipt() -> None:
    class BrokenRepository(InMemoryMiniQMTExecutionRuntimeRepository):
        def append_evidence_event_idempotent(self, candidate: object) -> object:
            raise RuntimeError("terminal persistence failure")

    repo = BrokenRepository()
    _runtime(repo)
    anchor = _anchor()
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    coordinator.schedule_markouts(anchor)
    coordinator.observe(_observation(at=anchor.trade_time_utc + timedelta(seconds=60, milliseconds=1), sequence=60))
    coordinator.drain_markouts(now_utc=anchor.trade_time_utc + timedelta(seconds=61))
    assert coordinator._terminal_series == set()
    assert len(coordinator._pending) == 3
    assert coordinator.flush(now_utc=anchor.trade_time_utc + timedelta(seconds=61)) == ()
    assert coordinator._terminal_series == set()
    assert len(coordinator._pending) == 3


def test_late_fill_uses_proven_history_or_writes_history_unavailable() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    config = _config()
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=config)
    anchor = _anchor()
    coordinator.schedule_markouts(anchor)
    coordinator.drain_markouts(now_utc=anchor.trade_time_utc + timedelta(seconds=61))
    assert coordinator.health().high_priority_backlog == 1
    limited = QuoteEvidenceCoordinator(repository=repo, config=QuoteIngressRuntimeConfig(**{**config.__dict__, "mark_history_max_samples": 1}))
    limited.observe(_observation(at=anchor.trade_time_utc + timedelta(seconds=60, milliseconds=100), sequence=1))
    limited.observe(_observation(at=anchor.trade_time_utc + timedelta(seconds=60, milliseconds=200), sequence=2))
    limited.schedule_markouts(anchor)
    limited.drain_markouts(now_utc=anchor.trade_time_utc + timedelta(seconds=62))
    assert limited._high[0].evidence.unavailable_reason == QuoteContractReasonCode.MARKOUT_HISTORY_UNAVAILABLE


def test_markout_never_crosses_lunch_close_trade_date_or_generation_gap() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    anchor = _anchor()
    close_anchor = MarkoutAnchor(**{**anchor.__dict__, "continuous_segment_end_utc": anchor.trade_time_utc + timedelta(seconds=60)})
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=_config())
    coordinator.schedule_markouts(close_anchor)
    coordinator.observe(_observation(at=anchor.trade_time_utc + timedelta(seconds=61), sequence=61, generation=3))
    coordinator.drain_markouts(now_utc=anchor.trade_time_utc + timedelta(seconds=61))
    assert coordinator._high[0].evidence.unavailable_reason == QuoteContractReasonCode.MARKOUT_MARKET_SESSION_ENDED


def test_markout_history_enforces_time_and_sample_bounds() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _runtime(repo)
    config = _config()
    coordinator = QuoteEvidenceCoordinator(repository=repo, config=config)
    anchor = _anchor()
    coordinator.observe(_observation(at=anchor.trade_time_utc, sequence=1))
    coordinator.observe(_observation(at=anchor.trade_time_utc + timedelta(seconds=912), sequence=2))
    assert coordinator.health().history_samples == 1
    assert coordinator.health().history_gap_symbols == ("000001.SZ",)


def test_type_aware_retention_pins_pending_mark_anchors_and_archives_cadence() -> None:
    source = inspect.getsource(PostgresMiniQMTExecutionRuntimeRepository._archive_events_for_runtime)
    assert "QUOTE_MARK_CAPTURED" in source and "TRADE_EVENT" in source and "INTERVAL '14 days'" in source
