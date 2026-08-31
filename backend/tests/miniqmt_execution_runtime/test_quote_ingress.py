from __future__ import annotations

import ast
from dataclasses import replace
import logging
import threading
import time
from datetime import UTC, date, datetime, time as local_time
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import pytest

import backend.infra.realtime_quote_subscriber as subscriber_module
import backend.services.miniqmt_execution_runtime.quote_ingress as quote_ingress_module
from backend.execution_algos.adaptive_is.contracts import (
    CalendarSnapshot,
    CalendarSnapshotSet,
    DepthQuantityUnit,
    MarketCode,
    PriceBasis,
    QuoteSourceMethod,
    SessionSegment,
    TradabilitySnapshot,
    TradabilityState,
)
from backend.execution_algos.adaptive_is.reasons import (
    QuoteContractError,
    QuoteContractReasonCode,
    QuoteContractStage,
    quote_contract_error,
)
from backend.infra.realtime_quote_subscriber import PhaseOneQuoteDelivery, RealtimeQuoteSubscriber
from backend.miniqmt_quote_contract_config import QuoteContractPolicy, QuoteIngressRuntimeConfig
from backend.services.miniqmt_execution_runtime.quote_eligibility import (
    BoundedNormalizedQuoteStore,
    QuoteEvaluationContext,
    QuoteEvaluationContextStore,
    QuoteSymbolContext,
    build_execution_clock_event,
)
from backend.services.miniqmt_execution_runtime.quote_ingress import (
    MiniQMTKernelProductIngressCompletionSignal,
    MiniQMTKernelProductIngressPending,
    MiniQMTKernelProductIngressSuppression,
    PhaseOneQuoteProjectionSink,
    PhaseOneRawQuoteSnapshotStore,
    QuoteIngressSupervisor,
    QuoteIngressWorker,
    ReservedSymbolMailbox,
    kernel_product_pending_identity_sha256_v1,
)
from backend.services.miniqmt_execution_runtime.quote_normalizer import RawQuoteFrame, capture_raw_quote_frame
from backend.services.simulation_runtime.miniqmt_quote_activation import (
    build_miniqmt_quote_ingress_activation_from_env,
)


class _FakeXtData:
    def __init__(self) -> None:
        self.next_sequence = 200
        self.callbacks: dict[int, Any] = {}
        self.subscribe_calls: list[int] = []
        self.unsubscribe_calls: list[int] = []

    def subscribe_whole_quote(self, code_list, callback):  # noqa: ANN001
        self.next_sequence += 1
        self.callbacks[self.next_sequence] = callback
        self.subscribe_calls.append(self.next_sequence)
        return self.next_sequence

    def unsubscribe_quote(self, sequence: int) -> None:
        self.unsubscribe_calls.append(sequence)

    def run(self) -> None:
        return None

    def emit_last(self, payload: Mapping[str, Any]) -> None:
        self.callbacks[self.subscribe_calls[-1]](payload)


@pytest.fixture
def fake_xtdata(monkeypatch: pytest.MonkeyPatch) -> _FakeXtData:
    fake = _FakeXtData()
    monkeypatch.setattr(subscriber_module, "_load_xtdata", lambda: fake)
    return fake


def _config(**overrides: object) -> QuoteIngressRuntimeConfig:
    values: dict[str, object] = {
        "enabled": True,
        "owner_mode": "simulation_scheduler",
        "max_symbols": 4,
        "drain_budget": 4,
        "heartbeat_timeout_ms": 100,
        "restart_backoff_ms": 1,
        "restart_max_backoff_ms": 8,
        "loud_interval_seconds": 1,
        "evidence_outbox_max_events": 8,
        "evidence_flush_batch_size": 4,
        "restart_max_attempts": 2,
    }
    values.update(overrides)
    return QuoteIngressRuntimeConfig(**values)  # type: ignore[arg-type]


def _payload(last_price: float) -> dict[str, object]:
    return {
        "time": "09300000",
        "lastPrice": last_price,
        "preClose": 9.8,
        "bidPrice": [last_price - 0.01],
        "askPrice": [last_price + 0.01],
        "bidVol": [100],
        "askVol": [100],
        "volume": 1000,
        "amount": 10000,
    }


def _frame(
    *,
    generation: int,
    sequence: int,
    last_price: float,
    symbol: str = "000001.SZ",
) -> object:
    return capture_raw_quote_frame(
        _payload(last_price),
        callback_symbol=symbol,
        source_session_id="test-session",
        ingress_generation=generation,
        ingress_sequence=sequence,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=sequence + 1,
        clock_domain_id="test-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )


def _wait_until(predicate, *, timeout_seconds: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("timed out waiting for quote ingress worker")


def _projection_context() -> QuoteEvaluationContext:
    policy = QuoteContractPolicy.from_execution_policy(
        {
            "quote_contract": {
                "schema_version": "miniqmt_quote_contract_policy_v2",
                "control_revision": "B0_QUOTE_V2",
                "required_capabilities": [
                    "CALENDAR",
                    "DEPTH_UNIT_SHARES",
                    "EXCHANGE_TIMESTAMP",
                    "FIVE_LEVEL_DEPTH",
                    "RAW_PRICE_BASIS",
                    "TRADABILITY",
                ],
                "max_receive_age_ms": 1000,
                "max_source_lag_ms": 1000,
                "max_exchange_age_ms": 1000,
                "max_negative_skew_ms": 10,
                "max_clock_age_divergence_ms": 10,
                "max_dependency_group_skew_ms": 100,
                "auction_mode": "OBSERVE_ONLY",
            }
        }
    )
    trade_date = date(2026, 7, 12)
    segments = (SessionSegment(local_time(9, 30), local_time(11, 30)),)
    calendar_set = CalendarSnapshotSet(
        snapshot_set_id="ingress-calendar-set",
        snapshot_by_market={
            market: CalendarSnapshot(
                calendar_id=f"ingress-{market.value}",
                market=market,
                trade_date=trade_date,
                timezone="Asia/Shanghai",
                session_segments=segments,
                effective_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
                source_version="checksum-v1:schedule-v1",
            )
            for market in MarketCode
        },
    )
    clock = build_execution_clock_event(
        calendar_snapshot_set=calendar_set,
        clock_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        clock_monotonic_ns=2_000_000_000,
        clock_domain_id="test-clock",
        source="test",
    )
    tradability = TradabilitySnapshot(
        schema_version="adaptive_is_tradability_snapshot_v1",
        tradability_id="ingress-tradability",
        symbol="000001.SZ",
        market=MarketCode.SZ,
        board="MAIN",
        trade_date=trade_date,
        price_basis=PriceBasis.RAW_CNY_PER_SHARE,
        pre_close=Decimal("10"),
        limit_up=Decimal("11"),
        limit_down=Decimal("9"),
        price_tick=Decimal("0.01"),
        lot_size=100,
        is_suspended=False,
        suspension_source="market.suspend_d",
        security_status="LISTED",
        openint_status=None,
        observed_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        source="authority",
        source_version="authority-v1",
        state=TradabilityState.TRADABLE,
    )
    return QuoteEvaluationContext(
        calendar_snapshot_set=calendar_set,
        clock=clock,
        continuity_generation=1,
        continuity_valid=True,
        policy=policy,
        symbols={
            "000001.SZ": QuoteSymbolContext(
                symbol="000001.SZ",
                board="MAIN",
                depth_quantity_unit=DepthQuantityUnit.SHARES,
                unit_evidence_version="unit-v1",
                tradability=tradability,
                product_type="EQUITY",
                product_type_proven_equity=True,
                authority_source_version="authority-v1",
            )
        },
    )


def test_projection_sink_is_single_writer_and_bounded_with_raw_normalized_admission_parity() -> None:
    context_store = QuoteEvaluationContextStore()
    context = _projection_context()
    context_store.publish(context)
    raw = PhaseOneRawQuoteSnapshotStore(max_symbols=1)
    normalized = BoundedNormalizedQuoteStore(max_symbols=1)
    sink = PhaseOneQuoteProjectionSink(raw_store=raw, normalized_store=normalized, context_store=context_store)
    sink.replace_admitted(("000001.SZ",))
    sink.on_generation_published(2)
    frame = capture_raw_quote_frame(
        {
            "time": "09300000",
            "lastPrice": "10.00",
            "preClose": "10.00",
            "openint": "OPEN",
            "bidPrice": ["9.99", "9.98", None, None, None],
            "bidVol": [100, 100, 0, 0, 0],
            "askPrice": ["10.01", "10.02", None, None, None],
            "askVol": [100, 100, 0, 0, 0],
        },
        callback_symbol="000001.SZ",
        source_session_id="projection-session",
        ingress_generation=2,
        ingress_sequence=1,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=1_999_500_000,
        clock_domain_id="test-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )
    sink.project(frame)

    assert raw.get("000001.SZ") == frame
    assert normalized.get("000001.SZ", context_id=context.context_id) is not None
    assert raw.snapshot().keys() == normalized.snapshot().keys()
    sink.replace_admitted(())
    assert raw.snapshot() == {} and normalized.snapshot() == {}


def test_projection_observation_failure_is_loud_without_rewriting_normalized_quote_state() -> None:
    context_store = QuoteEvaluationContextStore()
    context = _projection_context()
    context_store.publish(context)
    raw = PhaseOneRawQuoteSnapshotStore(max_symbols=1)
    normalized = BoundedNormalizedQuoteStore(max_symbols=1)

    def broken_observer(_: object) -> None:
        raise RuntimeError("durable coordinator unavailable")

    sink = PhaseOneQuoteProjectionSink(
        raw_store=raw,
        normalized_store=normalized,
        context_store=context_store,
        observation_sink=broken_observer,
    )
    sink.replace_admitted(("000001.SZ",))
    sink.on_generation_published(2)
    frame = capture_raw_quote_frame(
        {
            "time": "09300000",
            "lastPrice": "10.00",
            "preClose": "10.00",
            "openint": "OPEN",
            "bidPrice": ["9.99", "9.98", None, None, None],
            "bidVol": [100, 100, 0, 0, 0],
            "askPrice": ["10.01", "10.02", None, None, None],
            "askVol": [100, 100, 0, 0, 0],
        },
        callback_symbol="000001.SZ",
        source_session_id="projection-session",
        ingress_generation=2,
        ingress_sequence=1,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=1_999_500_000,
        clock_domain_id="test-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )
    sink.project(frame)

    assert normalized.get("000001.SZ", context_id=context.context_id) is not None
    assert sink.health()["projection"]["last_error_by_symbol"]["000001.SZ"]["reason_code"] == (
        QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED.value
    )


def test_contextual_failure_governor_bounds_one_hundred_thousand_identical_errors_and_recovers(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caplog.set_level("ERROR")
    context_store = QuoteEvaluationContextStore()
    context = _projection_context()
    context_store.publish(context)
    raw = PhaseOneRawQuoteSnapshotStore(max_symbols=1)
    normalized = BoundedNormalizedQuoteStore(max_symbols=1)
    loud_failures: list[QuoteContractError] = []
    projection = PhaseOneQuoteProjectionSink(
        raw_store=raw,
        normalized_store=normalized,
        context_store=context_store,
        loud_sink=loud_failures.append,
        loud_interval_seconds=60,
    )
    projection.replace_admitted(("000001.SZ",))
    projection.on_generation_published(2)
    frame = _frame(generation=2, sequence=1, last_price=10.0)
    sample_frames = (
        frame,
        _frame(generation=2, sequence=2, last_price=10.0, symbol="000002.SZ"),
        _frame(generation=2, sequence=3, last_price=10.0, symbol="000003.SZ"),
        _frame(generation=2, sequence=4, last_price=10.0, symbol="000004.SZ"),
    )
    consumer_id = "k6d-kernel-v2:runtime-governor"
    now_monotonic_ns = [1_000_000_000]
    monkeypatch.setattr(quote_ingress_module.time, "monotonic_ns", lambda: now_monotonic_ns[0])

    for occurrence in range(100_000):
        projection._record_contextual_sink_exception(  # noqa: SLF001 - direct contract stress seam
            frame=sample_frames[occurrence % len(sample_frames)],
            context=context,
            consumer_id=consumer_id,
            exception=RuntimeError("do not include a quote payload"),
            stage="SYNCHRONOUS",
        )

    health = projection.health()["projection"]["failure_governor"]
    assert health["observed_count"] == 100_000
    assert health["emitted_count"] == 1
    assert health["suppressed_count"] == 99_999
    assert health["tracked_fingerprint_count"] == 1
    assert health["active_failure_count"] == 1
    bucket = next(iter(health["fingerprints"].values()))
    assert bucket["occurrence_count"] == 100_000
    assert bucket["symbol_samples"] == ["000001.SZ", "000002.SZ", "000003.SZ"]
    assert loud_failures and len(loud_failures) == 1
    assert sum("Phase 1 quote projection loud failure" in record.message for record in caplog.records) == 1
    assert all("bidPrice" not in record.message and "lastPrice" not in record.message for record in caplog.records)

    now_monotonic_ns[0] += 60_000_000_000
    projection._record_contextual_sink_exception(  # noqa: SLF001 - window expiry emits one aggregate
        frame=frame,
        context=context,
        consumer_id=consumer_id,
        exception=RuntimeError("do not include a quote payload"),
        stage="SYNCHRONOUS",
    )
    aggregate = projection.health()["projection"]["failure_governor"]
    assert aggregate["emitted_count"] == 2
    assert aggregate["suppressed_count"] == 99_999
    aggregate_bucket = next(iter(aggregate["fingerprints"].values()))
    assert aggregate_bucket["occurrence_count"] == 100_001
    assert aggregate_bucket["suppressed_since_emit"] == 0

    projection._record_contextual_sink_exception(  # noqa: SLF001 - changed fingerprint must be immediate
        frame=frame,
        context=context,
        consumer_id=consumer_id,
        exception=LookupError("a distinct exception class"),
        stage="SYNCHRONOUS",
    )
    changed = projection.health()["projection"]["failure_governor"]
    assert changed["emitted_count"] == 3
    assert changed["tracked_fingerprint_count"] == 2
    assert len(loud_failures) == 3

    projection._record_loud(  # noqa: SLF001 - explicit runtime labels must not block owner recovery
        frame=frame,
        consumer_id=consumer_id,
        stage="SYNCHRONOUS",
        error=quote_contract_error(
            QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
            "explicit runtime label",
            context={"runtime_id": "runtime-explicit-label", "exception_type": "RuntimeError"},
        ),
    )

    projection.register_observation_sink(
        consumer_id=consumer_id,
        symbols=("000001.SZ",),
        sink=lambda _observation, _context: None,
    )
    projection.project(
        capture_raw_quote_frame(
            {
                **_payload(10.01),
                "openint": "OPEN",
                "bidPrice": [10.00, 9.99, None, None, None],
                "bidVol": [100, 100, 0, 0, 0],
                "askPrice": [10.02, 10.03, None, None, None],
                "askVol": [100, 100, 0, 0, 0],
            },
            callback_symbol="000001.SZ",
            source_session_id="governor-recovery",
            ingress_generation=2,
            ingress_sequence=2,
            received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
            received_monotonic_ns=2_000_000_002,
            clock_domain_id="test-clock",
            source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
        )
    )

    recovered = projection.health()["projection"]
    assert recovered["failure_governor"]["active_failure_count"] == 0
    assert recovered["failure_governor"]["recovery_count"] == 3
    assert recovered["failure_governor"]["observed_count"] == 100_003
    assert recovered["last_error_by_symbol"] == {}


def test_failure_governor_bounds_fingerprint_and_symbol_cardinality() -> None:
    governor = quote_ingress_module._ProcessLocalQuoteFailureGovernor(  # noqa: SLF001 - bounded contract seam
        loud_interval_seconds=60,
        max_fingerprints=16,
        max_symbol_samples=2,
    )
    error = quote_contract_error(
        QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED,
        "bounded diagnostic",
    )

    for index in range(300):
        governor.record(
            runtime_id="runtime-cardinality",
            generation=1,
            consumer_id="consumer-cardinality",
            stage="SYNCHRONOUS",
            error=error,
            exception_type=f"FailureType{index}",
            symbol=f"{index:06d}.SZ",
        )

    health = governor.health()
    assert health["tracked_fingerprint_count"] == 16
    assert health["evicted_count"] == 284
    assert health["max_fingerprints"] == 16
    assert all(len(bucket["symbol_samples"]) <= 2 for bucket in health["fingerprints"].values())


@pytest.mark.parametrize(
    "updates",
    (
        {"operation": "WATCHDOG"},
        {"lifecycle_generation": True},
        {"failure_fingerprint_sha256": "not-a-sha"},
        {"next_retry_at_utc": "2026-08-11T09:31:00"},
        {"next_retry_at_utc": "2026-08-11T09:31:00+08:00"},
        {"pending_identity_sha256": None},
        {"disposition": "RETRY_BACKOFF_SUPPRESSED", "next_retry_at_utc": None},
        {"runtime_id": "runtime_other"},
        {"consumer_id": "k6d-kernel-v2:runtime_other"},
        {"symbol": "600000.SH"},
        {"ingress_generation": 3},
        {"ingress_sequence": 8},
        {"market_data_id": "md_forged"},
        {"pending_identity_sha256": "c" * 64},
    ),
)
def test_projection_rejects_owner_frame_or_field_drift_in_nominal_suppression_carrier(
    updates: dict[str, object],
) -> None:
    context = _projection_context()
    captured: list[object] = []
    raw = PhaseOneRawQuoteSnapshotStore(max_symbols=1)
    normalized = BoundedNormalizedQuoteStore(max_symbols=1)
    context_store = QuoteEvaluationContextStore()
    projection = PhaseOneQuoteProjectionSink(
        raw_store=raw,
        normalized_store=normalized,
        context_store=context_store,
        observation_sink=lambda observation: captured.append(observation),
    )
    context_store.publish(context)
    projection.replace_admitted(("000001.SZ",))
    projection.on_generation_published(2)
    frame = capture_raw_quote_frame(
        {
            **_payload(10.0),
            "openint": "OPEN",
            "bidPrice": [9.99, 9.98, None, None, None],
            "bidVol": [100, 100, 0, 0, 0],
            "askPrice": [10.01, 10.02, None, None, None],
            "askVol": [100, 100, 0, 0, 0],
        },
        callback_symbol="000001.SZ",
        source_session_id="suppression-contract",
        ingress_generation=2,
        ingress_sequence=7,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=1_999_500_000,
        clock_domain_id="test-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )
    projection.project(frame)
    observation = captured[0]
    runtime_id = "runtime_suppression_contract"
    consumer_id = f"k6d-kernel-v2:{runtime_id}"
    carrier_values: dict[str, object] = {
        "runtime_id": runtime_id,
        "consumer_id": consumer_id,
        "operation": "CALLBACK",
        "disposition": "RETRY_BACKOFF_SUPPRESSED",
        "lifecycle_generation": 1,
        "symbol": "000001.SZ",
        "ingress_generation": 2,
        "ingress_sequence": 7,
        "market_data_id": observation.market_data_id,  # type: ignore[attr-defined]
        "failure_fingerprint_sha256": "a" * 64,
        "next_retry_at_utc": "2026-08-11T01:31:00+00:00",
        "pending_identity_sha256": kernel_product_pending_identity_sha256_v1(
            runtime_id=runtime_id,
            symbol="000001.SZ",
            market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
            ingress_generation=2,
            ingress_sequence=7,
            context_id=observation.context_id,  # type: ignore[attr-defined]
            values=(observation, context),
        ),
    }
    carrier_values.update(updates)
    carrier = MiniQMTKernelProductIngressSuppression(**carrier_values)  # type: ignore[arg-type]

    with pytest.raises(QuoteContractError) as caught:
        PhaseOneQuoteProjectionSink._validated_sink_suppression(
            carrier,
            consumer_id=consumer_id,
            frame=frame,  # type: ignore[arg-type]
            observation=observation,  # type: ignore[arg-type]
            values=(observation, context),
        )
    assert caught.value.reason_code == QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED


def test_projection_rejects_shape_correct_duck_typed_suppression_carrier() -> None:
    context = _projection_context()
    frame = _frame(generation=2, sequence=7, last_price=10.0)

    class _ForgedCarrier:
        @staticmethod
        def as_dict() -> dict[str, object]:
            return {
                "schema_version": "miniqmt_kernel_product_ingress_suppression_v1",
                "runtime_id": "runtime_forged",
                "consumer_id": "k6d-kernel-v2:runtime_forged",
                "operation": "CALLBACK",
                "disposition": "RETRY_BACKOFF_SUPPRESSED",
                "lifecycle_generation": 1,
                "symbol": "000001.SZ",
                "ingress_generation": 2,
                "ingress_sequence": 7,
                "market_data_id": "md_forged",
                "failure_fingerprint_sha256": "a" * 64,
                "next_retry_at_utc": "2026-08-11T01:31:00+00:00",
                "pending_identity_sha256": "b" * 64,
                "executed": False,
                "business_success": False,
            }

    with pytest.raises(QuoteContractError) as caught:
        PhaseOneQuoteProjectionSink._validated_sink_suppression(
            _ForgedCarrier(),
            consumer_id="k6d-kernel-v2:runtime_forged",
            frame=frame,  # type: ignore[arg-type]
            observation=object(),  # nominal type is rejected before payload access
            values=(object(), context),
        )
    assert caught.value.reason_code == QuoteContractReasonCode.EVIDENCE_OBSERVATION_FAILED


def test_projection_records_async_callback_as_pending_instead_of_business_success() -> None:
    context = _projection_context()
    raw = PhaseOneRawQuoteSnapshotStore(max_symbols=1)
    normalized = BoundedNormalizedQuoteStore(max_symbols=1)
    context_store = QuoteEvaluationContextStore()
    projection = PhaseOneQuoteProjectionSink(
        raw_store=raw,
        normalized_store=normalized,
        context_store=context_store,
    )
    runtime_id = "runtime_async_pending_contract"
    consumer_id = f"k6d-kernel-v2:{runtime_id}"

    completion_signal: MiniQMTKernelProductIngressCompletionSignal | None = None

    def pending_sink(observation: object, runtime_context: object) -> MiniQMTKernelProductIngressPending:
        nonlocal completion_signal
        pending_identity = kernel_product_pending_identity_sha256_v1(
            runtime_id=runtime_id,
            symbol="000001.SZ",
            market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
            ingress_generation=2,
            ingress_sequence=7,
            context_id=observation.context_id,  # type: ignore[attr-defined]
            values=(observation, runtime_context),
        )
        completion_signal = MiniQMTKernelProductIngressCompletionSignal(
            runtime_id=runtime_id,
            consumer_id=consumer_id,
            operation="CALLBACK",
            lifecycle_generation=3,
            attempt_token=7,
            symbol="000001.SZ",
            ingress_generation=2,
            ingress_sequence=7,
            market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
            pending_identity_sha256=pending_identity,
        )
        return MiniQMTKernelProductIngressPending(
            runtime_id=runtime_id,
            consumer_id=consumer_id,
            operation="CALLBACK",
            lifecycle_generation=3,
            attempt_token=7,
            symbol="000001.SZ",
            ingress_generation=2,
            ingress_sequence=7,
            market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
            pending_identity_sha256=pending_identity,
            completion_signal=completion_signal,
        )

    projection.register_observation_sink(
        consumer_id=consumer_id,
        symbols=("000001.SZ",),
        sink=pending_sink,
    )
    context_store.publish(context)
    projection.replace_admitted(("000001.SZ",))
    projection.on_generation_published(2)
    frame = capture_raw_quote_frame(
        {
            **_payload(10.0),
            "openint": "OPEN",
            "bidPrice": [9.99, 9.98, None, None, None],
            "bidVol": [100, 100, 0, 0, 0],
            "askPrice": [10.01, 10.02, None, None, None],
            "askVol": [100, 100, 0, 0, 0],
        },
        callback_symbol="000001.SZ",
        source_session_id="pending-contract",
        ingress_generation=2,
        ingress_sequence=7,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=1_999_500_000,
        clock_domain_id="test-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )

    projection.project(frame)
    health = projection.health()["projection"]
    assert health["accepted_count"] == 1
    assert health["pending_count"] == 1
    assert health["active_pending_count"] == 1
    assert health["suppressed_count"] == 0
    assert health["last_error_by_symbol"] == {}
    pending_payload = health["last_pending_by_symbol"]["000001.SZ"][consumer_id]
    assert pending_payload["business_success"] is None
    assert pending_payload["disposition"] == "ASYNC_IN_FLIGHT"

    assert completion_signal is not None
    completion_signal.resolve(
        business_success=True,
        completed_at_utc=datetime(2026, 7, 12, 1, 30, 1, tzinfo=UTC),
        failure=None,
    )
    completed = projection.health()["projection"]
    assert completed["active_pending_count"] == 0
    assert completed["last_pending_by_symbol"] == {}
    assert completed["pending_completion_count"] == 1
    completion = completed["last_completion_by_owner"][consumer_id]["000001.SZ"]
    assert completion["pending_identity_sha256"] == pending_payload["pending_identity_sha256"]
    assert completion["disposition"] == "ASYNC_SUCCEEDED"
    assert completion["outcome_pending"] is False
    assert completion["business_success"] is True


def test_projection_pending_is_owned_by_runtime_and_symbol_without_overlap_collision() -> None:
    context_store = QuoteEvaluationContextStore()
    context_store.publish(_projection_context())
    projection = PhaseOneQuoteProjectionSink(
        raw_store=PhaseOneRawQuoteSnapshotStore(max_symbols=1),
        normalized_store=BoundedNormalizedQuoteStore(max_symbols=1),
        context_store=context_store,
    )
    projection.replace_admitted(("000001.SZ",))
    projection.on_generation_published(2)
    signals: dict[str, MiniQMTKernelProductIngressCompletionSignal] = {}

    def pending_sink(runtime_id: str, attempt_token: int):
        consumer_id = f"k6d-kernel-v2:{runtime_id}"

        def observe(observation: object, runtime_context: object) -> MiniQMTKernelProductIngressPending:
            pending_identity = kernel_product_pending_identity_sha256_v1(
                runtime_id=runtime_id,
                symbol="000001.SZ",
                market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
                ingress_generation=2,
                ingress_sequence=9,
                context_id=observation.context_id,  # type: ignore[attr-defined]
                values=(observation, runtime_context),
            )
            signal = MiniQMTKernelProductIngressCompletionSignal(
                runtime_id=runtime_id,
                consumer_id=consumer_id,
                operation="CALLBACK",
                lifecycle_generation=1,
                attempt_token=attempt_token,
                symbol="000001.SZ",
                ingress_generation=2,
                ingress_sequence=9,
                market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
                pending_identity_sha256=pending_identity,
            )
            signals[runtime_id] = signal
            return MiniQMTKernelProductIngressPending(
                runtime_id=runtime_id,
                consumer_id=consumer_id,
                operation="CALLBACK",
                lifecycle_generation=1,
                attempt_token=attempt_token,
                symbol="000001.SZ",
                ingress_generation=2,
                ingress_sequence=9,
                market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
                pending_identity_sha256=pending_identity,
                completion_signal=signal,
            )

        return consumer_id, observe

    for runtime_id, attempt_token in (("runtime_overlap_a", 11), ("runtime_overlap_b", 12)):
        consumer_id, sink = pending_sink(runtime_id, attempt_token)
        projection.register_observation_sink(
            consumer_id=consumer_id,
            symbols=("000001.SZ",),
            sink=sink,
        )
    frame = capture_raw_quote_frame(
        {
            **_payload(10.0),
            "openint": "OPEN",
            "bidPrice": [9.99, 9.98, None, None, None],
            "bidVol": [100, 100, 0, 0, 0],
            "askPrice": [10.01, 10.02, None, None, None],
            "askVol": [100, 100, 0, 0, 0],
        },
        callback_symbol="000001.SZ",
        source_session_id="overlap-pending",
        ingress_generation=2,
        ingress_sequence=9,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=1_999_500_000,
        clock_domain_id="test-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )

    projection.project(frame)
    pending = projection.health()["projection"]
    by_symbol = pending["last_pending_by_symbol"]["000001.SZ"]
    assert pending["active_pending_count"] == 2
    assert set(by_symbol) == {
        "k6d-kernel-v2:runtime_overlap_a",
        "k6d-kernel-v2:runtime_overlap_b",
    }

    signals["runtime_overlap_a"].resolve(
        business_success=True,
        completed_at_utc=datetime(2026, 7, 12, 1, 30, 1, tzinfo=UTC),
        failure=None,
    )
    after_a = projection.health()["projection"]
    assert after_a["active_pending_count"] == 1
    assert set(after_a["last_pending_by_symbol"]["000001.SZ"]) == {"k6d-kernel-v2:runtime_overlap_b"}
    signals["runtime_overlap_b"].resolve(
        business_success=True,
        completed_at_utc=datetime(2026, 7, 12, 1, 30, 1, tzinfo=UTC),
        failure=None,
    )
    assert projection.health()["projection"]["active_pending_count"] == 0


def test_projection_unregister_cancels_pending_and_bounds_late_completion_history() -> None:
    projection = PhaseOneQuoteProjectionSink(
        raw_store=PhaseOneRawQuoteSnapshotStore(max_symbols=1),
        normalized_store=BoundedNormalizedQuoteStore(max_symbols=1),
        context_store=QuoteEvaluationContextStore(),
    )
    runtime_id = "runtime_projection_unregister"
    consumer_id = f"k6d-kernel-v2:{runtime_id}"

    def sink(*_values: object) -> None:
        return None

    projection.register_observation_sink(
        consumer_id=consumer_id,
        symbols=("000001.SZ",),
        sink=sink,
    )
    frame = _frame(generation=2, sequence=11, last_price=10.0)
    pending_identity = "a" * 64
    signal = MiniQMTKernelProductIngressCompletionSignal(
        runtime_id=runtime_id,
        consumer_id=consumer_id,
        operation="CALLBACK",
        lifecycle_generation=3,
        attempt_token=7,
        symbol=frame.symbol,
        ingress_generation=frame.ingress_generation,
        ingress_sequence=frame.ingress_sequence,
        market_data_id="market_data_projection_unregister",
        pending_identity_sha256=pending_identity,
    )
    pending = MiniQMTKernelProductIngressPending(
        runtime_id=runtime_id,
        consumer_id=consumer_id,
        operation="CALLBACK",
        lifecycle_generation=3,
        attempt_token=7,
        symbol=frame.symbol,
        ingress_generation=frame.ingress_generation,
        ingress_sequence=frame.ingress_sequence,
        market_data_id="market_data_projection_unregister",
        pending_identity_sha256=pending_identity,
        completion_signal=signal,
    )
    assert (
        projection._record_pending(
            frame=frame,
            carrier=pending,
            expected_owner=projection._contextual_observation_sinks[consumer_id],
        )
        is True
    )

    assert projection.unregister_observation_sink(
        consumer_id=consumer_id,
        symbols=(frame.symbol,),
        sink=sink,
    )
    unregistered = projection.health()["projection"]
    assert unregistered["active_pending_count"] == 0
    assert unregistered["pending_drop_count_by_reason"] == {"PROJECTION_PENDING_SINK_UNREGISTERED": 1}

    projection.register_observation_sink(
        consumer_id=consumer_id,
        symbols=(frame.symbol,),
        sink=sink,
    )
    successor_identity = "b" * 64
    successor_signal = MiniQMTKernelProductIngressCompletionSignal(
        runtime_id=runtime_id,
        consumer_id=consumer_id,
        operation="CALLBACK",
        lifecycle_generation=4,
        attempt_token=8,
        symbol=frame.symbol,
        ingress_generation=frame.ingress_generation,
        ingress_sequence=frame.ingress_sequence,
        market_data_id="market_data_projection_successor",
        pending_identity_sha256=successor_identity,
    )
    successor_pending = MiniQMTKernelProductIngressPending(
        runtime_id=runtime_id,
        consumer_id=consumer_id,
        operation="CALLBACK",
        lifecycle_generation=4,
        attempt_token=8,
        symbol=frame.symbol,
        ingress_generation=frame.ingress_generation,
        ingress_sequence=frame.ingress_sequence,
        market_data_id="market_data_projection_successor",
        pending_identity_sha256=successor_identity,
        completion_signal=successor_signal,
    )
    assert (
        projection._record_pending(
            frame=frame,
            carrier=successor_pending,
            expected_owner=projection._contextual_observation_sinks[consumer_id],
        )
        is True
    )
    successor_signal.resolve(
        business_success=True,
        completed_at_utc=datetime(2026, 8, 12, 1, 29, tzinfo=UTC),
        failure=None,
    )

    signal.resolve(
        business_success=True,
        completed_at_utc=datetime(2026, 8, 12, 1, 30, tzinfo=UTC),
        failure=None,
    )
    completed = projection.health()["projection"]
    assert completed["active_pending_count"] == 0
    assert completed["last_completion_by_owner"][consumer_id][frame.symbol]["attempt_token"] == 8
    assert (
        completed["last_completion_by_owner"][consumer_id][frame.symbol]["pending_identity_sha256"]
        == successor_identity
    )
    assert completed["pending_drop_count_by_reason"] == {
        "ASYNC_COMPLETION_STALE_SINK_OWNER": 1,
        "PROJECTION_PENDING_SINK_UNREGISTERED": 1,
    }
    assert completed["last_pending_drop"]["runtime_id"] == runtime_id
    assert completed["last_pending_drop"]["consumer_id"] == consumer_id
    assert completed["last_pending_drop"]["attempt_token"] == 7
    assert completed["last_pending_drop"]["pending_identity_sha256"] == pending_identity


def test_projection_unregister_race_rejects_snapshot_late_pending_for_same_symbol_successor() -> None:
    context_store = QuoteEvaluationContextStore()
    context_store.publish(_projection_context())
    projection = PhaseOneQuoteProjectionSink(
        raw_store=PhaseOneRawQuoteSnapshotStore(max_symbols=1),
        normalized_store=BoundedNormalizedQuoteStore(max_symbols=1),
        context_store=context_store,
    )
    projection.replace_admitted(("000001.SZ",))
    projection.on_generation_published(2)
    runtime_id = "runtime_unregister_race"
    consumer_id = f"k6d-kernel-v2:{runtime_id}"
    peer_calls: list[str] = []
    await_entered = threading.Event()
    allow_pending = threading.Event()
    signals: list[MiniQMTKernelProductIngressCompletionSignal] = []

    class _DeferredSink:
        def __call__(self, *_values: object) -> None:
            raise AssertionError("race test must use the asynchronous dispatch contract")

        def enqueue_kernel_product_callback_v1(self, observation: object, runtime_context: object) -> object:
            pending_identity = kernel_product_pending_identity_sha256_v1(
                runtime_id=runtime_id,
                symbol="000001.SZ",
                market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
                ingress_generation=2,
                ingress_sequence=21,
                context_id=observation.context_id,  # type: ignore[attr-defined]
                values=(observation, runtime_context),
            )
            signal = MiniQMTKernelProductIngressCompletionSignal(
                runtime_id=runtime_id,
                consumer_id=consumer_id,
                operation="CALLBACK",
                lifecycle_generation=1,
                attempt_token=1,
                symbol="000001.SZ",
                ingress_generation=2,
                ingress_sequence=21,
                market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
                pending_identity_sha256=pending_identity,
            )
            signals.append(signal)
            return observation, signal, pending_identity

        def await_kernel_product_callback_v1(self, *, dispatch: object, timeout_seconds: float) -> object:
            observation, signal, pending_identity = dispatch  # type: ignore[misc]
            await_entered.set()
            assert allow_pending.wait(timeout=5)
            return MiniQMTKernelProductIngressPending(
                runtime_id=runtime_id,
                consumer_id=consumer_id,
                operation="CALLBACK",
                lifecycle_generation=1,
                attempt_token=1,
                symbol="000001.SZ",
                ingress_generation=2,
                ingress_sequence=21,
                market_data_id=observation.market_data_id,
                pending_identity_sha256=pending_identity,
                completion_signal=signal,
            )

    deferred = _DeferredSink()

    def peer_sink(observation: object, _context: object) -> None:
        peer_calls.append(observation.quote.symbol)  # type: ignore[attr-defined]

    def successor_sink(*_values: object) -> None:
        return None

    projection.register_observation_sink(
        consumer_id=consumer_id,
        symbols=("000001.SZ",),
        sink=deferred,
    )
    predecessor_generation = projection.health()["projection"]["contextual_observation_sink_owners"][consumer_id][
        "registration_generation"
    ]
    projection.register_observation_sink(
        consumer_id="k6d-kernel-v2:runtime_unregister_peer",
        symbols=("000001.SZ",),
        sink=peer_sink,
    )
    frame = capture_raw_quote_frame(
        {
            **_payload(10.0),
            "openint": "OPEN",
            "bidPrice": [9.99, 9.98, None, None, None],
            "bidVol": [100, 100, 0, 0, 0],
            "askPrice": [10.01, 10.02, None, None, None],
            "askVol": [100, 100, 0, 0, 0],
        },
        callback_symbol="000001.SZ",
        source_session_id="projection-unregister-race",
        ingress_generation=2,
        ingress_sequence=21,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=1_999_500_000,
        clock_domain_id="test-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )

    project_thread = threading.Thread(target=lambda: projection.project(frame))
    project_thread.start()
    assert await_entered.wait(timeout=1)
    assert projection.unregister_observation_sink(
        consumer_id=consumer_id,
        symbols=("000001.SZ",),
        sink=deferred,
    )
    projection.register_observation_sink(
        consumer_id=consumer_id,
        symbols=("000001.SZ",),
        sink=successor_sink,
    )
    successor_generation = projection.health()["projection"]["contextual_observation_sink_owners"][consumer_id][
        "registration_generation"
    ]
    allow_pending.set()
    project_thread.join(timeout=5)
    assert not project_thread.is_alive()
    signals[0].resolve(
        business_success=True,
        completed_at_utc=datetime(2026, 7, 12, 1, 30, 1, tzinfo=UTC),
        failure=None,
    )

    health = projection.health()["projection"]
    assert successor_generation > predecessor_generation
    assert peer_calls == ["000001.SZ"]
    assert health["active_pending_count"] == 0
    assert health["last_completion_by_owner"] == {}
    assert health["pending_drop_count_by_reason"] == {"PROJECTION_PENDING_STALE_SINK_OWNER": 1}
    assert set(health["contextual_observation_sink_owners"]) == {
        consumer_id,
        "k6d-kernel-v2:runtime_unregister_peer",
    }


def test_projection_runtime_id_churn_bounds_stale_snapshot_pending_and_completion_state() -> None:
    projection = PhaseOneQuoteProjectionSink(
        raw_store=PhaseOneRawQuoteSnapshotStore(max_symbols=1),
        normalized_store=BoundedNormalizedQuoteStore(max_symbols=1),
        context_store=QuoteEvaluationContextStore(),
    )
    projection.replace_admitted(("000001.SZ",))
    frame = _frame(generation=2, sequence=31, last_price=10.0)

    def peer_sink(*_values: object) -> None:
        return None

    projection.register_observation_sink(
        consumer_id="k6d-kernel-v2:runtime_churn_peer",
        symbols=("000001.SZ",),
        sink=peer_sink,
    )
    churn_count = 32
    for index in range(churn_count):
        runtime_id = f"runtime_churn_{index:02d}"
        consumer_id = f"k6d-kernel-v2:{runtime_id}"

        def sink(*_values: object) -> None:
            return None

        projection.register_observation_sink(
            consumer_id=consumer_id,
            symbols=("000001.SZ",),
            sink=sink,
        )
        expected_owner = projection._contextual_observation_sinks[consumer_id]
        assert projection.unregister_observation_sink(
            consumer_id=consumer_id,
            symbols=("000001.SZ",),
            sink=sink,
        )
        pending_identity = "{index:064x}".format(index=index + 1)
        signal = MiniQMTKernelProductIngressCompletionSignal(
            runtime_id=runtime_id,
            consumer_id=consumer_id,
            operation="CALLBACK",
            lifecycle_generation=index + 1,
            attempt_token=index + 1,
            symbol="000001.SZ",
            ingress_generation=frame.ingress_generation,
            ingress_sequence=frame.ingress_sequence,
            market_data_id=f"market_data_churn_{index:02d}",
            pending_identity_sha256=pending_identity,
        )
        pending = MiniQMTKernelProductIngressPending(
            runtime_id=runtime_id,
            consumer_id=consumer_id,
            operation="CALLBACK",
            lifecycle_generation=index + 1,
            attempt_token=index + 1,
            symbol="000001.SZ",
            ingress_generation=frame.ingress_generation,
            ingress_sequence=frame.ingress_sequence,
            market_data_id=f"market_data_churn_{index:02d}",
            pending_identity_sha256=pending_identity,
            completion_signal=signal,
        )
        assert (
            projection._record_pending(
                frame=frame,
                carrier=pending,
                expected_owner=expected_owner,
            )
            is False
        )
        signal.resolve(
            business_success=True,
            completed_at_utc=datetime(2026, 8, 12, 1, 30, tzinfo=UTC),
            failure=None,
        )

    health = projection.health()["projection"]
    assert health["active_pending_count"] == 0
    assert health["last_completion_by_owner"] == {}
    assert health["pending_drop_count_by_reason"] == {"PROJECTION_PENDING_STALE_SINK_OWNER": churn_count}
    assert set(health["contextual_observation_sink_owners"]) == {"k6d-kernel-v2:runtime_churn_peer"}


def test_contextual_kernel_fanout_enqueues_every_owner_before_one_shared_wait_budget() -> None:
    context_store = QuoteEvaluationContextStore()
    context_store.publish(_projection_context())
    projection = PhaseOneQuoteProjectionSink(
        raw_store=PhaseOneRawQuoteSnapshotStore(max_symbols=1),
        normalized_store=BoundedNormalizedQuoteStore(max_symbols=1),
        context_store=context_store,
    )
    projection.replace_admitted(("000001.SZ",))
    projection.on_generation_published(2)
    owner_count = 21
    events: list[str] = []
    signals: list[MiniQMTKernelProductIngressCompletionSignal] = []

    class _DeferredSink:
        def __init__(self, index: int, *, blocked: bool) -> None:
            self.runtime_id = f"runtime_fanout_{index:02d}"
            self.consumer_id = f"k6d-kernel-v2:{self.runtime_id}"
            self.attempt_token = index + 1
            self.blocked = blocked

        def __call__(self, *_values: object) -> None:
            raise AssertionError("kernel fanout used the sequential callable path")

        def enqueue_kernel_product_callback_v1(self, observation: object, runtime_context: object) -> object:
            events.append(f"enqueue:{self.consumer_id}")
            pending_identity = kernel_product_pending_identity_sha256_v1(
                runtime_id=self.runtime_id,
                symbol="000001.SZ",
                market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
                ingress_generation=2,
                ingress_sequence=13,
                context_id=observation.context_id,  # type: ignore[attr-defined]
                values=(observation, runtime_context),
            )
            signal = MiniQMTKernelProductIngressCompletionSignal(
                runtime_id=self.runtime_id,
                consumer_id=self.consumer_id,
                operation="CALLBACK",
                lifecycle_generation=1,
                attempt_token=self.attempt_token,
                symbol="000001.SZ",
                ingress_generation=2,
                ingress_sequence=13,
                market_data_id=observation.market_data_id,  # type: ignore[attr-defined]
                pending_identity_sha256=pending_identity,
            )
            return observation, signal, pending_identity

        def await_kernel_product_callback_v1(self, *, dispatch: object, timeout_seconds: float) -> object:
            events.append(f"await:{self.consumer_id}")
            observation, signal, pending_identity = dispatch  # type: ignore[misc]
            if not self.blocked:
                return None
            if timeout_seconds > 0:
                time.sleep(timeout_seconds)
            signals.append(signal)
            return MiniQMTKernelProductIngressPending(
                runtime_id=self.runtime_id,
                consumer_id=self.consumer_id,
                operation="CALLBACK",
                lifecycle_generation=1,
                attempt_token=self.attempt_token,
                symbol="000001.SZ",
                ingress_generation=2,
                ingress_sequence=13,
                market_data_id=observation.market_data_id,
                pending_identity_sha256=pending_identity,
                completion_signal=signal,
            )

    for index in range(owner_count):
        sink = _DeferredSink(index, blocked=index < owner_count - 1)
        projection.register_observation_sink(
            consumer_id=sink.consumer_id,
            symbols=("000001.SZ",),
            sink=sink,
        )
    frame = capture_raw_quote_frame(
        {
            **_payload(10.0),
            "openint": "OPEN",
            "bidPrice": [9.99, 9.98, None, None, None],
            "bidVol": [100, 100, 0, 0, 0],
            "askPrice": [10.01, 10.02, None, None, None],
            "askVol": [100, 100, 0, 0, 0],
        },
        callback_symbol="000001.SZ",
        source_session_id="fanout-shared-budget",
        ingress_generation=2,
        ingress_sequence=13,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=1_999_500_000,
        clock_domain_id="test-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )

    started = time.monotonic()
    projection.project(frame)
    elapsed = time.monotonic() - started
    first_await = next(index for index, event in enumerate(events) if event.startswith("await:"))
    assert first_await == owner_count
    assert events[:owner_count] == [f"enqueue:k6d-kernel-v2:runtime_fanout_{index:02d}" for index in range(owner_count)]
    assert elapsed < 0.15
    health = projection.health()["projection"]
    assert health["active_pending_count"] == owner_count - 1
    assert len(health["last_pending_by_symbol"]["000001.SZ"]) == owner_count - 1
    for signal in signals:
        signal.resolve(
            business_success=True,
            completed_at_utc=datetime(2026, 7, 12, 1, 30, 1, tzinfo=UTC),
            failure=None,
        )
    assert projection.health()["projection"]["active_pending_count"] == 0


def test_contextual_observation_sink_routes_only_exact_symbol_owners_and_preserves_overlap() -> None:
    context_store = QuoteEvaluationContextStore()
    context_store.publish(_projection_context())
    projection = PhaseOneQuoteProjectionSink(
        raw_store=PhaseOneRawQuoteSnapshotStore(max_symbols=2),
        normalized_store=BoundedNormalizedQuoteStore(max_symbols=2),
        context_store=context_store,
    )
    projection.replace_admitted(("000001.SZ",))
    projection.on_generation_published(2)
    calls: list[tuple[str, str]] = []

    def sink(owner: str):
        def observe(observation: object, _context: object) -> None:
            calls.append((owner, observation.quote.symbol))  # type: ignore[attr-defined]

        return observe

    owner_a = sink("runtime-a")
    owner_b = sink("runtime-b")
    overlapping_owner = sink("runtime-overlap")
    projection.register_observation_sink(
        consumer_id="runtime-a",
        symbols=("000001.SZ",),
        sink=owner_a,
    )
    projection.register_observation_sink(
        consumer_id="runtime-b",
        symbols=("600000.SH",),
        sink=owner_b,
    )
    projection.register_observation_sink(
        consumer_id="runtime-overlap",
        symbols=("000001.SZ", "600000.SH"),
        sink=overlapping_owner,
    )

    projection.project(
        capture_raw_quote_frame(
            {
                **_payload(10.0),
                "openint": "OPEN",
                "bidPrice": [9.99, 9.98, None, None, None],
                "bidVol": [100, 100, 0, 0, 0],
                "askPrice": [10.01, 10.02, None, None, None],
                "askVol": [100, 100, 0, 0, 0],
            },
            callback_symbol="000001.SZ",
            source_session_id="projection-symbol-owner",
            ingress_generation=2,
            ingress_sequence=7,
            received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
            received_monotonic_ns=1_999_500_000,
            clock_domain_id="test-clock",
            source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
        )
    )

    assert calls == [("runtime-a", "000001.SZ"), ("runtime-overlap", "000001.SZ")]
    health = projection.health()["projection"]
    assert health["rejected_count"] == 0
    assert health["contextual_observation_sink_owners"]["runtime-a"]["symbols"] == ["000001.SZ"]
    assert health["contextual_observation_sink_owners"]["runtime-b"]["symbols"] == ["600000.SH"]
    assert health["contextual_observation_sink_owners"]["runtime-overlap"]["symbols"] == [
        "000001.SZ",
        "600000.SH",
    ]


def test_contextual_observation_sink_identity_is_exact_for_register_readback_and_unregister() -> None:
    projection = PhaseOneQuoteProjectionSink(
        raw_store=PhaseOneRawQuoteSnapshotStore(max_symbols=2),
        normalized_store=BoundedNormalizedQuoteStore(max_symbols=2),
        context_store=QuoteEvaluationContextStore(),
    )

    def sink(*_values: object) -> None:
        return None

    def replacement(*_values: object) -> None:
        return None

    projection.register_observation_sink(
        consumer_id="runtime-exact",
        symbols=("000001.SZ",),
        sink=sink,
    )

    assert (
        projection.get_observation_sink(
            consumer_id="runtime-exact",
            symbols=("000001.SZ",),
        )
        is sink
    )
    with pytest.raises(QuoteContractError) as wrong_symbols:
        projection.get_observation_sink(
            consumer_id="runtime-exact",
            symbols=("600000.SH",),
        )
    assert wrong_symbols.value.reason_code == QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT
    with pytest.raises(QuoteContractError) as wrong_sink:
        projection.unregister_observation_sink(
            consumer_id="runtime-exact",
            symbols=("000001.SZ",),
            sink=replacement,
        )
    assert wrong_sink.value.reason_code == QuoteContractReasonCode.B0_QUOTE_V2_ASSIGNMENT_CONFLICT
    assert projection.unregister_observation_sink(
        consumer_id="runtime-exact",
        symbols=("000001.SZ",),
        sink=sink,
    )
    assert (
        projection.unregister_observation_sink(
            consumer_id="runtime-exact",
            symbols=("000001.SZ",),
            sink=sink,
        )
        is False
    )

    with pytest.raises(QuoteContractError) as nonexact_symbols:
        projection.register_observation_sink(
            consumer_id="runtime-nonexact",
            symbols=("000001.SZ", "000001.SZ"),
            sink=sink,
        )
    assert nonexact_symbols.value.reason_code == QuoteContractReasonCode.PAYLOAD_INVALID


def test_pending_identity_distinguishes_zero_from_missing_sequence_components() -> None:
    common = {
        "runtime_id": "runtime_identity_zero",
        "symbol": "000001.SZ",
        "market_data_id": "md_identity_zero",
        "context_id": "ctx_identity_zero",
        "values": (object(),),
    }
    zero = kernel_product_pending_identity_sha256_v1(
        **common,
        ingress_generation=0,
        ingress_sequence=0,
    )
    missing = kernel_product_pending_identity_sha256_v1(
        **common,
        ingress_generation=None,
        ingress_sequence=None,
    )
    assert zero != missing


def test_projection_to_kernel_runtime_schema_failure_burst_attempts_database_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("ERROR")
    context_store = QuoteEvaluationContextStore()
    context = _projection_context()
    context_store.publish(context)
    raw = PhaseOneRawQuoteSnapshotStore(max_symbols=1)
    normalized = BoundedNormalizedQuoteStore(max_symbols=1)
    projection = PhaseOneQuoteProjectionSink(
        raw_store=raw,
        normalized_store=normalized,
        context_store=context_store,
    )
    projection.replace_admitted(("000001.SZ",))
    projection.on_generation_published(2)

    class _ProjectionSupervisor:
        def register_observation_sink(
            self,
            *,
            consumer_id: str,
            symbols: tuple[str, ...],
            sink: object,
        ) -> None:
            projection.register_observation_sink(  # type: ignore[arg-type]
                consumer_id=consumer_id,
                symbols=symbols,
                sink=sink,
            )

        @staticmethod
        def get_observation_sink(*, consumer_id: str, symbols: tuple[str, ...]) -> object | None:
            return projection.get_observation_sink(
                consumer_id=consumer_id,
                symbols=symbols,
            )

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def consumer_lease_owner_snapshot(
            *,
            consumer_id: str,
            symbols: tuple[str, ...],
        ) -> dict[str, object]:
            lease = {
                "lease_id": f"lease:{consumer_id}",
                "data_session_key": "SIM:B0_QUOTE_V2:simulation_scheduler",
                "owner": "simulation_scheduler",
                "consumer_id": consumer_id,
                "symbols": list(symbols),
                "generation": 1,
                "status": "ACTIVE",
                "physical_subscription_id": 1001,
            }
            return {
                "schema_version": "miniqmt_quote_consumer_lease_owner_snapshot_v1",
                "readback_current": True,
                "exact_owner": True,
                "state": "ACTIVE",
                "registration_generation": 1,
                "expected_owner_identity_sha256": "a" * 64,
                "actual_owner_identity_sha256": "a" * 64,
                "expected_lease": dict(lease),
                "actual_lease": dict(lease),
            }

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

        @staticmethod
        def watchdog_tick() -> None:
            return None

    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _CheckViolation(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Runtime:
        runtime_id = "runtime_projection_schema_burst"
        binding_id = "binding_projection_schema_burst"
        trade_date = date(2026, 7, 12)
        symbols = ("000001.SZ",)

        def __init__(self) -> None:
            self.database_attempt_count = 0
            self.fail_schema = True
            self.last_ingress_sequence = None

        def observe_b0_quote_v1(self, observation: object, *_values: object) -> None:
            self.database_attempt_count += 1
            self.last_ingress_sequence = observation.quote.ingress_sequence  # type: ignore[attr-defined]
            if self.fail_schema:
                raise _CheckViolation("violates check constraint ck_miniqmt_event_source")

        @staticmethod
        def scheduler_tick_v1(**_values: object) -> tuple[str, ...]:
            return ()

    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    runtime = _Runtime()
    now_utc = [datetime(2026, 7, 12, 1, 30, tzinfo=UTC)]
    now_monotonic_ns = [2_000_000_000]
    activation._kernel_retry_clock_utc = lambda: now_utc[0]
    activation._kernel_retry_monotonic_ns = lambda: now_monotonic_ns[0]
    activation.controller_factory = None
    activation.supervisor = _ProjectionSupervisor()  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)

    for sequence in range(1, 102):
        last_price = Decimal("10") + Decimal(sequence) / Decimal("10000")
        projection.project(
            capture_raw_quote_frame(
                {
                    "time": "09300000",
                    "lastPrice": str(last_price),
                    "preClose": "10.00",
                    "openint": "OPEN",
                    "bidPrice": [str(last_price - Decimal("0.01")), "9.98", None, None, None],
                    "bidVol": [100, 100, 0, 0, 0],
                    "askPrice": [str(last_price + Decimal("0.01")), "10.02", None, None, None],
                    "askVol": [100, 100, 0, 0, 0],
                },
                callback_symbol="000001.SZ",
                source_session_id="projection-kernel-burst",
                ingress_generation=2,
                ingress_sequence=sequence,
                received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
                received_monotonic_ns=2_000_000_000 + sequence,
                clock_domain_id="test-clock",
                source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
            )
        )

    assert runtime.database_attempt_count == 1
    latest = normalized.get("000001.SZ", context_id=context.context_id)
    assert latest is not None and latest.frame.ingress_sequence == 101
    retry = activation.health()["kernel_product_runtimes"][0]["ingress_retry"]
    assert retry["state"] == "RETRY_BACKOFF"
    assert retry["suppressed_callback_count"] == 100
    pending = retry["operations"]["CALLBACK"]["pending"]
    assert len(pending) == 1 and pending[0]["ingress_sequence"] == 101
    projection_health = projection.health()["projection"]
    assert projection_health["suppressed_count"] == 100
    assert projection_health["last_error_by_symbol"]["000001.SZ"]["reason_code"] == (
        "ADAPTIVE_IS_QUOTE_CONSUMER_FAILURE"
    )
    assert projection_health["last_suppression_by_symbol"]["000001.SZ"]["disposition"] == ("RETRY_BACKOFF_SUPPRESSED")
    latest_suppression = projection_health["last_suppression_by_symbol"]["000001.SZ"]
    assert latest_suppression["consumer_id"] == "k6d-kernel-v2:runtime_projection_schema_burst"
    assert latest_suppression["runtime_id"] == "runtime_projection_schema_burst"
    assert latest_suppression["symbol"] == "000001.SZ"
    assert latest_suppression["ingress_generation"] == 2
    assert latest_suppression["ingress_sequence"] == 101
    assert latest_suppression["market_data_id"] == latest.market_data_id
    assert sum("Phase 1 quote projection loud failure" in record.message for record in caplog.records) == 1
    assert sum(record.levelno >= logging.ERROR for record in caplog.records) == 1

    runtime.fail_schema = False
    now_utc[0] = datetime(2026, 7, 12, 1, 31, tzinfo=UTC)
    now_monotonic_ns[0] += 60_000_000_000
    activation.watchdog_tick()
    assert runtime.database_attempt_count == 1
    projection.project(
        capture_raw_quote_frame(
            {
                "time": "09310000",
                "lastPrice": "10.02",
                "preClose": "10.00",
                "openint": "OPEN",
                "bidPrice": ["10.01", "10.00", None, None, None],
                "bidVol": [100, 100, 0, 0, 0],
                "askPrice": ["10.03", "10.04", None, None, None],
                "askVol": [100, 100, 0, 0, 0],
            },
            callback_symbol="000001.SZ",
            source_session_id="projection-kernel-burst",
            ingress_generation=2,
            ingress_sequence=102,
            received_at_utc=datetime(2026, 7, 12, 1, 31, tzinfo=UTC),
            received_monotonic_ns=62_000_000_102,
            clock_domain_id="test-clock",
            source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
        )
    )
    recovered = activation.health()
    assert recovered["status"] == "DRAINING"
    assert runtime.database_attempt_count == 2
    assert runtime.last_ingress_sequence == 102
    assert recovered["kernel_product_runtimes"][0]["ingress_retry"]["state"] == "HEALTHY"
    assert projection.health()["projection"]["last_error_by_symbol"] == {}
    assert projection.health()["projection"]["last_suppression_by_symbol"] == {}


def test_reserved_symbol_mailbox_coalesces_per_symbol_without_dropping_admitted_symbols() -> None:
    mailbox = ReservedSymbolMailbox(max_symbols=2)
    mailbox.admit(("000001.SZ", "000002.SZ"))
    mailbox.activate_generation(3)

    assert mailbox.offer(_frame(generation=3, sequence=1, last_price=10.0)) == "ACCEPTED"
    assert mailbox.offer(_frame(generation=3, sequence=2, last_price=10.1)) == "COALESCED"
    frames = mailbox.wait_and_drain(budget=2, timeout_seconds=0)

    assert len(frames) == 1
    assert frames[0].ingress_sequence == 2
    assert mailbox.telemetry()["coalesced_count"] == 1
    assert mailbox.telemetry()["drop_count_by_reason"] == {"MAILBOX_COALESCED_SUPERSEDED": 1}
    with pytest.raises(QuoteContractError) as unexpected:
        mailbox.offer(
            capture_raw_quote_frame(
                _payload(10.0),
                callback_symbol="000003.SZ",
                source_session_id="test-session",
                ingress_generation=3,
                ingress_sequence=3,
                received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
                received_monotonic_ns=4,
                clock_domain_id="test-clock",
                source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
            )
        )
    assert unexpected.value.reason_code == QuoteContractReasonCode.UNEXPECTED_SYMBOL


def test_reserved_symbol_mailbox_rejects_new_capacity_and_fences_old_generations() -> None:
    mailbox = ReservedSymbolMailbox(max_symbols=1)
    mailbox.admit(("000001.SZ",))
    with pytest.raises(QuoteContractError) as capacity:
        mailbox.admit(("000002.SZ",))
    assert capacity.value.reason_code == QuoteContractReasonCode.CAPACITY_EXCEEDED

    mailbox.activate_generation(2)
    assert mailbox.offer(_frame(generation=1, sequence=1, last_price=10.0)) == "STALE_GENERATION"
    assert mailbox.offer(_frame(generation=2, sequence=2, last_price=10.0)) == "ACCEPTED"
    assert mailbox.offer(_frame(generation=2, sequence=2, last_price=10.1)) == "ORDERING_REJECTED"
    mailbox.fence_generation(2)
    assert mailbox.offer(_frame(generation=2, sequence=3, last_price=10.2)) == "STALE_GENERATION"
    telemetry = mailbox.telemetry()
    assert telemetry["drop_count_by_reason"] == {
        "GENERATION_FENCED": 1,
        "MAILBOX_ORDERING_REJECTED": 1,
        "MAILBOX_STALE_GENERATION": 2,
    }
    assert telemetry["last_drop"] == {
        "reason": "MAILBOX_STALE_GENERATION",
        "symbol": "000001.SZ",
        "generation": 2,
        "sequence": 3,
    }


def test_mailbox_lifecycle_purges_retain_exact_drop_reason_and_identity() -> None:
    mailbox = ReservedSymbolMailbox(max_symbols=2)
    mailbox.admit(("000001.SZ", "000002.SZ"))
    mailbox.activate_generation(3)
    first = _frame(generation=3, sequence=1, last_price=10.0)
    second = replace(first, symbol="000002.SZ", ingress_sequence=2)
    assert mailbox.offer(first) == "ACCEPTED"
    assert mailbox.offer(second) == "ACCEPTED"
    mailbox.replace_admitted(("000001.SZ",))
    mailbox.activate_generation(4)

    telemetry = mailbox.telemetry()
    assert telemetry["drop_count_by_reason"] == {
        "GENERATION_ACTIVATED_PURGE": 1,
        "SYMBOL_REVOKED": 1,
    }
    assert telemetry["last_drop"] == {
        "reason": "GENERATION_ACTIVATED_PURGE",
        "symbol": "000001.SZ",
        "generation": 3,
        "sequence": 1,
    }


def test_mailbox_and_snapshot_store_reject_invalid_lifecycle_inputs_without_fallback() -> None:
    with pytest.raises(QuoteContractError):
        ReservedSymbolMailbox(max_symbols=0)
    mailbox = ReservedSymbolMailbox(max_symbols=1)
    with pytest.raises(QuoteContractError) as empty_symbols:
        mailbox.admit(())
    assert empty_symbols.value.reason_code == QuoteContractReasonCode.SYMBOL_INVALID
    mailbox.admit(("000001.SZ",))
    mailbox.activate_generation(2)
    with pytest.raises(QuoteContractError) as old_generation:
        mailbox.activate_generation(1)
    assert old_generation.value.reason_code == QuoteContractReasonCode.ORDERING_REJECTED
    with pytest.raises(ValueError, match="budget"):
        mailbox.wait_and_drain(budget=0, timeout_seconds=0)

    store = PhaseOneRawQuoteSnapshotStore(max_symbols=1)
    store.replace_admitted(("000001.SZ",))
    newer = _frame(generation=2, sequence=2, last_price=10.2)
    store.update(newer)  # type: ignore[arg-type]
    store.update(_frame(generation=2, sequence=1, last_price=10.1))  # type: ignore[arg-type]
    assert store.get("000001.SZ") == newer


def test_bootstrap_callback_race_keeps_the_newer_callback_after_generation_publish(fake_xtdata: _FakeXtData) -> None:
    callback_price = 10.2

    def bootstrap(symbols: list[str]) -> Mapping[str, Mapping[str, Any]]:
        fake_xtdata.emit_last({"000001.SZ": _payload(callback_price)})
        return {symbol: _payload(10.0) for symbol in symbols}

    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(),
        data_session_key="SIM:quote-session",
        owner="scheduler-owner-A",
        bootstrap_fetcher=bootstrap,
    )
    supervisor.acquire_consumer(consumer_id="quote-observer", symbols=["000001.SZ"])

    _wait_until(lambda: supervisor.snapshot_store.get("000001.SZ") is not None)
    frame = supervisor.snapshot_store.get("000001.SZ")
    assert frame is not None
    assert frame.source_method is QuoteSourceMethod.WHOLE_QUOTE_CALLBACK
    assert frame.ingress_sequence == 1
    assert frame.whitelisted_raw_fields["lastPrice"] == callback_price
    health = supervisor.health()
    assert health["subscription"]["bootstrap_coverage_ratio"] == 1.0
    assert health["subscription"]["callback_total"] == 1
    assert health["writer"]["writer_heartbeat_age_ms"] is not None
    supervisor.shutdown()


def test_capacity_rejects_new_consumer_without_disturbing_existing_admission(fake_xtdata: _FakeXtData) -> None:
    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(max_symbols=1),
        data_session_key="SIM:quote-session",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    before = supervisor.health()

    with pytest.raises(QuoteContractError) as exc_info:
        supervisor.acquire_consumer(consumer_id="second", symbols=["000002.SZ"])
    assert exc_info.value.reason_code == QuoteContractReasonCode.CAPACITY_EXCEEDED
    after = supervisor.health()
    assert after["subscription"]["generation"] == before["subscription"]["generation"]
    assert after["subscription"]["symbols"] == ["000001.SZ"]
    supervisor.shutdown()


def test_writer_failure_is_fenced_and_requests_bounded_generation_rebuild() -> None:
    failures: list[QuoteContractError] = []
    worker = QuoteIngressWorker(
        consumer_id="writer-test",
        config=_config(),
        frame_sink=lambda _frame: (_ for _ in ()).throw(RuntimeError("sink failure")),
        loud_sink=failures.append,
    )
    worker.admit_symbols(("000001.SZ",))
    worker.on_generation_published("SIM:quote-session", 1)
    delivery = PhaseOneQuoteDelivery(
        data_session_key="SIM:quote-session",
        lease_id="lease",
        owner="scheduler-owner-A",
        consumer_id="writer-test",
        symbol="000001.SZ",
        payload=_payload(10.0),
        generation=1,
        ingress_sequence=1,
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=1,
    )
    worker.capture_delivery(delivery, source_session_id="session", clock_domain_id="clock")

    _wait_until(lambda: worker.health()["last_failure"] is not None)
    request = worker.watchdog()

    assert request is not None
    assert request.generation == 1
    assert worker.health()["status"] == "RESTART_PENDING"
    assert failures[-1].reason_code == QuoteContractReasonCode.CONSUMER_FAILURE
    worker.shutdown()


def test_writer_batch_failure_records_failed_and_unprocessed_frame_drop_identity() -> None:
    calls: list[str] = []

    def fail_first(frame: object) -> None:
        calls.append(frame.symbol)  # type: ignore[attr-defined]
        raise RuntimeError("first drained frame failed")

    worker = QuoteIngressWorker(
        consumer_id="writer-batch-drop-test",
        config=_config(max_symbols=2, drain_budget=2),
        frame_sink=fail_first,
    )
    worker.admit_symbols(("000001.SZ", "600000.SH"))
    assert worker.ingest_frame(
        _frame(generation=1, sequence=1, last_price=10.0, symbol="000001.SZ")  # type: ignore[arg-type]
    )
    assert worker.ingest_frame(
        _frame(generation=1, sequence=2, last_price=11.0, symbol="600000.SH")  # type: ignore[arg-type]
    )

    assert worker.on_generation_published("SIM:writer-batch-drop", 1) is True
    _wait_until(lambda: worker.health()["status"] == "FAILED")
    _wait_until(lambda: worker.health()["thread_alive"] is False)

    health = worker.health()
    assert calls == ["000001.SZ"]
    assert health["backlog"] == 0
    assert health["drop_count_by_reason"]["WRITER_FRAME_SINK_FAILED"] == 1
    assert health["drop_count_by_reason"]["WRITER_BATCH_ABORTED_AFTER_FRAME_SINK_FAILURE"] == 1
    assert health["last_drop"] == {
        "reason": "WRITER_BATCH_ABORTED_AFTER_FRAME_SINK_FAILURE",
        "symbol": "600000.SH",
        "generation": 1,
        "sequence": 2,
    }
    worker.shutdown()


def test_generation_pending_replay_failure_records_current_and_remaining_frames_without_ack() -> None:
    worker = QuoteIngressWorker(
        consumer_id="pending-replay-drop-test",
        config=_config(max_symbols=2),
        frame_sink=lambda _frame: None,
    )
    worker.admit_symbols(("000001.SZ", "600000.SH"))
    assert worker.ingest_frame(
        _frame(generation=1, sequence=1, last_price=10.0, symbol="000001.SZ")  # type: ignore[arg-type]
    )
    assert worker.ingest_frame(
        _frame(generation=1, sequence=2, last_price=11.0, symbol="600000.SH")  # type: ignore[arg-type]
    )
    replay_calls: list[str] = []

    def reject_first_replay(frame: object) -> bool:
        replay_calls.append(frame.symbol)  # type: ignore[attr-defined]
        return False

    worker.ingest_frame = reject_first_replay  # type: ignore[method-assign]

    assert worker.on_generation_published("SIM:pending-replay-drop", 1) is False
    health = worker.health()
    assert replay_calls == ["000001.SZ"]
    assert health["pending_drop_count_by_reason"]["PENDING_PUBLISH_REPLAY_REJECTED"] == 1
    assert health["pending_drop_count_by_reason"]["PENDING_PUBLISH_REPLAY_ABORTED"] == 1
    assert health["last_pending_drop"] == {
        "reason": "PENDING_PUBLISH_REPLAY_ABORTED",
        "symbol": "600000.SH",
        "generation": 1,
        "sequence": 2,
    }
    worker.shutdown()


def test_shutdown_records_each_unpublished_pending_frame_identity() -> None:
    worker = QuoteIngressWorker(
        consumer_id="pending-shutdown-drop-test",
        config=_config(max_symbols=2),
        frame_sink=lambda _frame: None,
    )
    worker.admit_symbols(("000001.SZ", "600000.SH"))
    assert worker.ingest_frame(
        _frame(generation=7, sequence=1, last_price=10.0, symbol="000001.SZ")  # type: ignore[arg-type]
    )
    assert worker.ingest_frame(
        _frame(generation=7, sequence=2, last_price=11.0, symbol="600000.SH")  # type: ignore[arg-type]
    )

    worker.shutdown()

    health = worker.health()
    assert health["pending_generation_count"] == 0
    assert health["pending_frame_count"] == 0
    assert health["pending_drop_count_by_reason"]["PENDING_SHUTDOWN"] == 2
    assert health["last_pending_drop"] == {
        "reason": "PENDING_SHUTDOWN",
        "symbol": "600000.SH",
        "generation": 7,
        "sequence": 2,
    }


def test_blocked_writer_shutdown_is_loud_until_the_exact_thread_exits() -> None:
    entered = threading.Event()
    release = threading.Event()

    def blocked_sink(_frame: object) -> None:
        entered.set()
        assert release.wait(timeout=5)

    worker = QuoteIngressWorker(
        consumer_id="blocked-shutdown-test",
        config=_config(),
        frame_sink=blocked_sink,
    )
    worker.admit_symbols(("000001.SZ",))
    worker.on_generation_published("SIM:quote-session", 1)
    assert worker.ingest_frame(_frame(generation=1, sequence=1, last_price=10.0)) is True
    assert entered.wait(timeout=1)

    with pytest.raises(QuoteContractError) as caught:
        worker.shutdown()
    assert caught.value.reason_code == QuoteContractReasonCode.CONSUMER_FAILURE
    assert caught.value.context["shutdown_outcome"] == "UNKNOWN"
    health = worker.health()
    assert health["status"] == "SHUTDOWN_UNKNOWN"
    assert health["thread_alive"] is True

    release.set()
    _wait_until(lambda: worker.health()["thread_alive"] is False)
    worker.shutdown()
    assert worker.health()["status"] == "STOPPED"
    assert worker.health()["thread_alive"] is False


def test_worker_preserves_pending_raw_frame_until_publish_and_rejects_fenced_republish() -> None:
    captured: list[object] = []
    worker = QuoteIngressWorker(
        consumer_id="pending-capture-test",
        config=_config(),
        frame_sink=captured.append,
    )
    worker.admit_symbols(("000001.SZ",))
    worker.ingest_frame(_frame(generation=2, sequence=1, last_price=10.0))  # type: ignore[arg-type]
    assert captured == []
    worker.on_generation_published("SIM:quote-session", 2)
    _wait_until(lambda: len(captured) == 1)
    worker.on_generation_fenced("SIM:quote-session", 2)
    worker.on_generation_published("SIM:quote-session", 2)
    assert worker.health()["last_failure"]["reason_code"] == QuoteContractReasonCode.ORDERING_REJECTED.value
    worker.shutdown()


def test_worker_pending_coalescing_and_fencing_retains_drop_reason_and_frame_identity() -> None:
    captured: list[RawQuoteFrame] = []
    worker = QuoteIngressWorker(
        consumer_id="pending-drop-evidence-test",
        config=_config(),
        frame_sink=captured.append,
    )
    worker.admit_symbols(("000001.SZ",))
    assert worker.ingest_frame(_frame(generation=2, sequence=2, last_price=10.0)) is True
    assert worker.ingest_frame(_frame(generation=2, sequence=1, last_price=9.9)) is True
    assert worker.ingest_frame(_frame(generation=2, sequence=3, last_price=10.1)) is True
    pending_health = worker.health()
    assert pending_health["pending_drop_count_by_reason"] == {
        "PENDING_COALESCED_SUPERSEDED": 1,
        "PENDING_ORDERING_REJECTED": 1,
    }
    assert pending_health["last_pending_drop"] == {
        "reason": "PENDING_COALESCED_SUPERSEDED",
        "symbol": "000001.SZ",
        "generation": 2,
        "sequence": 2,
    }

    assert worker.on_generation_published("SIM:quote-session", 2) is True
    _wait_until(lambda: len(captured) == 1)
    assert captured[0].ingress_sequence == 3
    worker.on_generation_fenced("SIM:quote-session", 2)
    assert worker.ingest_frame(_frame(generation=2, sequence=4, last_price=10.2)) is False
    fenced_health = worker.health()
    assert fenced_health["pending_drop_count_by_reason"]["PENDING_FENCED_GENERATION"] == 1
    assert fenced_health["last_pending_drop"] == {
        "reason": "PENDING_FENCED_GENERATION",
        "symbol": "000001.SZ",
        "generation": 2,
        "sequence": 4,
    }
    worker.shutdown()


def test_worker_reaches_restart_cap_then_a_later_lifecycle_epoch_can_retry() -> None:
    worker = QuoteIngressWorker(
        consumer_id="restart-cap-test",
        config=_config(restart_backoff_ms=1, restart_max_backoff_ms=2, restart_max_attempts=2),
        frame_sink=lambda _frame: (_ for _ in ()).throw(RuntimeError("sink failure")),
    )
    worker.admit_symbols(("000001.SZ",))
    worker.on_generation_published("SIM:quote-session", 1)
    worker.capture_delivery(
        PhaseOneQuoteDelivery(
            data_session_key="SIM:quote-session",
            lease_id="lease",
            owner="scheduler-owner-A",
            consumer_id="restart-cap-test",
            symbol="000001.SZ",
            payload=_payload(10.0),
            generation=1,
            ingress_sequence=1,
            source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
            received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
            received_monotonic_ns=1,
        ),
        source_session_id="session",
        clock_domain_id="clock",
    )
    _wait_until(lambda: worker.health()["last_failure"] is not None)
    first = worker.watchdog(now_monotonic_ns=time.monotonic_ns())
    assert first is not None
    second = worker.watchdog(now_monotonic_ns=time.monotonic_ns() + 10_000_000)
    assert second is not None
    assert worker.watchdog(now_monotonic_ns=time.monotonic_ns() + 20_000_000) is None
    assert worker.health()["status"] == "FAILED"
    worker.begin_lifecycle_epoch()
    assert worker.health()["status"] == "DEGRADED"
    worker.on_generation_fenced("SIM:quote-session", 1)
    assert worker.health()["status"] == "FENCED"
    worker.shutdown()


def test_successful_successor_generation_clears_active_failure_but_keeps_diagnostics_history() -> None:
    worker = QuoteIngressWorker(
        consumer_id="active-failure-recovery-test",
        config=_config(),
        frame_sink=lambda _frame: None,
    )
    worker.admit_symbols(("000001.SZ",))
    failure = quote_contract_error(
        QuoteContractReasonCode.CONSUMER_FAILURE,
        "writer failed before a bounded generation rebuild",
        context={"consumer_id": "active-failure-recovery-test", "generation": 1},
    )
    worker.record_loud_failure(failure)
    assert worker.health()["active_failure"]["reason_code"] == QuoteContractReasonCode.CONSUMER_FAILURE.value

    assert worker.on_generation_published("SIM:active-failure-recovery", 2) is True

    health = worker.health()
    assert health["active_failure"] is None
    assert health["last_failure"]["reason_code"] == QuoteContractReasonCode.CONSUMER_FAILURE.value
    assert health["active_generation"] == 2
    assert health["fenced_generation"] == -1
    worker.shutdown()


def test_loud_rate_limit_preserves_failure_first_last_and_occurrence_count() -> None:
    failures: list[QuoteContractError] = []
    worker = QuoteIngressWorker(
        consumer_id="loud-test",
        config=_config(loud_interval_seconds=60),
        frame_sink=lambda _frame: None,
        loud_sink=failures.append,
    )
    error = QuoteContractError(
        reason_code=QuoteContractReasonCode.PAYLOAD_INVALID,
        stage=QuoteContractStage.INGRESS,
        message="bad payload",
        context={"symbol": "000001.SZ", "generation": 1},
    )

    worker.record_loud_failure(error)
    worker.record_loud_failure(error)

    last_failure = worker.health()["last_failure"]
    assert last_failure["occurrence_count"] == 2
    assert last_failure["first_observed_at"] <= last_failure["last_observed_at"]
    assert failures == [error]


def test_supervisor_is_default_off_and_release_isolated_from_the_subscriber(fake_xtdata: _FakeXtData) -> None:
    with pytest.raises(QuoteContractError) as disabled:
        QuoteIngressSupervisor(
            subscriber=RealtimeQuoteSubscriber(),
            config=QuoteIngressRuntimeConfig.from_mapping({}),
            data_session_key="SIM:disabled",
            owner="scheduler-owner-A",
            bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
        )
    assert disabled.value.reason_code == QuoteContractReasonCode.SUBSCRIPTION_UNAVAILABLE

    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:quote-session",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    lease = supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    with pytest.raises(QuoteContractError) as duplicate:
        supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    assert duplicate.value.reason_code == QuoteContractReasonCode.LEASE_REBUILD_FAILED
    assert supervisor.release_consumer(consumer_id="first") is True
    assert supervisor.release_consumer(consumer_id="first") is False
    assert subscriber.phase_one_health(data_session_key="SIM:quote-session")["status"] == "INACTIVE"
    assert lease.status == "ACTIVE"
    assert supervisor.watchdog_tick()["consumers"] == {}

    def shutdown_sink(*_values: object) -> None:
        return None

    supervisor.register_observation_sink(
        consumer_id="shutdown-sink",
        symbols=("000001.SZ",),
        sink=shutdown_sink,
    )
    supervisor.shutdown()
    assert (
        supervisor.unregister_observation_sink(
            consumer_id="shutdown-sink",
            symbols=("000001.SZ",),
            sink=shutdown_sink,
        )
        is False
    )


def test_supervisor_projection_and_worker_share_one_failure_governor(fake_xtdata: _FakeXtData) -> None:
    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(loud_interval_seconds=17),
        data_session_key="SIM:shared-failure-governor",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )

    assert supervisor._projection_sink._failure_governor is supervisor._failure_governor  # noqa: SLF001
    assert supervisor._worker._failure_governor is supervisor._failure_governor  # noqa: SLF001
    assert supervisor.health()["projection"]["failure_governor"] == supervisor.health()["writer"]["failure_governor"]
    supervisor.shutdown()


@pytest.mark.parametrize(
    ("drift", "expected_state"),
    (
        ("missing", "LEASE_MISSING"),
        ("failed", "LEASE_NOT_ACTIVE"),
        ("lease_id", "LEASE_OWNER_DRIFT"),
        ("generation", "LEASE_OWNER_DRIFT"),
        ("symbols", "LEASE_OWNER_DRIFT"),
        ("physical_subscription", "LEASE_OWNER_DRIFT"),
    ),
)
def test_consumer_lease_owner_snapshot_rejects_physical_lease_identity_or_state_drift(
    fake_xtdata: _FakeXtData,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
    expected_state: str,
) -> None:
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:lease-owner-snapshot",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    lease = supervisor.acquire_consumer(consumer_id="consumer", symbols=["000001.SZ"])
    exact = supervisor.consumer_lease_owner_snapshot(
        consumer_id="consumer",
        symbols=("000001.SZ",),
    )
    assert exact["exact_owner"] is True
    assert exact["state"] == "ACTIVE"
    original_get = subscriber.get_phase_one_lease

    def drifted_get(**_kwargs: object) -> object:
        if drift == "missing":
            return None
        if drift == "failed":
            return replace(lease, status="FAILED")
        if drift == "lease_id":
            return replace(lease, lease_id="forged-lease")
        if drift == "generation":
            return replace(lease, generation=lease.generation + 1)
        if drift == "symbols":
            return replace(lease, symbols=("600000.SH",))
        return replace(lease, physical_subscription_id=None)

    monkeypatch.setattr(subscriber, "get_phase_one_lease", drifted_get)
    snapshot = supervisor.consumer_lease_owner_snapshot(
        consumer_id="consumer",
        symbols=("000001.SZ",),
    )
    assert snapshot["exact_owner"] is False
    assert snapshot["state"] == expected_state
    health = supervisor.health()["consumers"]["consumer"]
    assert health["exact_owner"] is False
    assert health["owner_state"] == expected_state

    monkeypatch.setattr(subscriber, "get_phase_one_lease", original_get)
    supervisor.shutdown()


def test_consumer_lease_owner_snapshot_refreshes_every_legal_physical_successor_generation(
    fake_xtdata: _FakeXtData,
) -> None:
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:lease-owner-rollover",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    first_initial = supervisor.consumer_lease_owner_snapshot(
        consumer_id="first",
        symbols=("000001.SZ",),
    )

    supervisor.acquire_consumer(consumer_id="second", symbols=["600000.SH"])
    first_after_union = supervisor.consumer_lease_owner_snapshot(
        consumer_id="first",
        symbols=("000001.SZ",),
    )
    second_after_union = supervisor.consumer_lease_owner_snapshot(
        consumer_id="second",
        symbols=("600000.SH",),
    )
    assert first_after_union["exact_owner"] is True
    assert second_after_union["exact_owner"] is True
    assert first_after_union["registration_generation"] == first_initial["registration_generation"]
    assert first_after_union["expected_lease"] == first_after_union["actual_lease"]
    assert first_after_union["actual_lease"]["generation"] > first_initial["actual_lease"]["generation"]
    assert (
        first_after_union["actual_lease"]["physical_subscription_id"]
        != first_initial["actual_lease"]["physical_subscription_id"]
    )

    rebuilt_generation = subscriber.rebuild_phase_one_leases(
        data_session_key="SIM:lease-owner-rollover",
        owner="scheduler-owner-A",
        max_symbols=_config().max_symbols,
    )
    for consumer_id, symbols, previous in (
        ("first", ("000001.SZ",), first_after_union),
        ("second", ("600000.SH",), second_after_union),
    ):
        after_rebuild = supervisor.consumer_lease_owner_snapshot(
            consumer_id=consumer_id,
            symbols=symbols,
        )
        assert after_rebuild["exact_owner"] is True
        assert after_rebuild["registration_generation"] == previous["registration_generation"]
        assert after_rebuild["actual_lease"]["generation"] == rebuilt_generation
        assert after_rebuild["actual_lease"]["generation"] > previous["actual_lease"]["generation"]

    first_before_release = supervisor.consumer_lease_owner_snapshot(
        consumer_id="first",
        symbols=("000001.SZ",),
    )
    assert supervisor.release_consumer(consumer_id="second") is True
    first_after_release = supervisor.consumer_lease_owner_snapshot(
        consumer_id="first",
        symbols=("000001.SZ",),
    )
    assert first_after_release["exact_owner"] is True
    assert first_after_release["registration_generation"] == first_initial["registration_generation"]
    assert first_after_release["actual_lease"]["generation"] > first_before_release["actual_lease"]["generation"]
    assert first_after_release["expected_lease"] == first_after_release["actual_lease"]
    assert supervisor.release_consumer(consumer_id="first") is True
    supervisor.shutdown()


@pytest.mark.parametrize("rollback_mode", ("false", "error"))
def test_non_exact_acquire_retains_retryable_owner_when_physical_rollback_is_unknown(
    fake_xtdata: _FakeXtData,
    monkeypatch: pytest.MonkeyPatch,
    rollback_mode: str,
) -> None:
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:acquire-rollback-unknown",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    monkeypatch.setattr(supervisor._worker, "on_generation_published", lambda *_values: False)
    original_release = subscriber.release_phase_one_lease

    def unknown_release(**_values: object) -> bool:
        if rollback_mode == "false":
            return False
        raise RuntimeError("acquire rollback transport failed")

    monkeypatch.setattr(subscriber, "release_phase_one_lease", unknown_release)
    with pytest.raises(QuoteContractError) as failure:
        supervisor.acquire_consumer(consumer_id="consumer", symbols=["000001.SZ"])

    assert failure.value.context["release_outcome"] == "UNKNOWN"
    assert failure.value.context["consumer_lease_retained"] is True
    assert failure.value.context["rollback_released"] is (False if rollback_mode == "false" else None)
    owner = supervisor.consumer_lease_owner_snapshot(
        consumer_id="consumer",
        symbols=("000001.SZ",),
    )
    assert owner["state"] == "ACQUIRE_ROLLBACK_UNKNOWN"
    assert owner["exact_owner"] is False
    assert owner["expected_lease"]["status"] == "FAILED"
    assert supervisor.health()["consumers"]["consumer"]["owner_state"] == "ACQUIRE_ROLLBACK_UNKNOWN"

    monkeypatch.setattr(subscriber, "release_phase_one_lease", original_release)
    assert supervisor.release_consumer(consumer_id="consumer") is True
    assert (
        supervisor.consumer_lease_owner_snapshot(
            consumer_id="consumer",
            symbols=("000001.SZ",),
        )["state"]
        == "ABSENT"
    )
    supervisor.shutdown()


def test_supervisor_health_and_exact_owner_snapshot_do_not_wait_for_blocked_physical_release(
    fake_xtdata: _FakeXtData,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:blocked-release-health",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="consumer", symbols=["000001.SZ"])
    original_release = subscriber.release_phase_one_lease
    release_entered = threading.Event()
    allow_release = threading.Event()

    def blocked_release(**kwargs: object) -> bool:
        release_entered.set()
        assert allow_release.wait(timeout=5)
        return original_release(**kwargs)

    monkeypatch.setattr(subscriber, "release_phase_one_lease", blocked_release)
    release_results: list[bool] = []
    release_thread = threading.Thread(
        target=lambda: release_results.append(supervisor.release_consumer(consumer_id="consumer"))
    )
    release_thread.start()
    assert release_entered.wait(timeout=1)

    health_results: list[dict[str, Any]] = []
    health_thread = threading.Thread(target=lambda: health_results.append(supervisor.health()))
    health_thread.start()
    health_thread.join(timeout=0.2)
    owner_snapshot = supervisor.consumer_lease_owner_snapshot(
        consumer_id="consumer",
        symbols=("000001.SZ",),
    )

    allow_release.set()
    release_thread.join(timeout=5)
    health_thread.join(timeout=5)
    assert not release_thread.is_alive() and not health_thread.is_alive()
    assert release_results == [True]
    assert len(health_results) == 1
    assert owner_snapshot["state"] == "RELEASE_IN_FLIGHT"
    assert owner_snapshot["exact_owner"] is False
    assert health_results[0]["consumers"]["consumer"]["owner_state"] == "RELEASE_IN_FLIGHT"
    supervisor.shutdown()


def test_release_consumer_readback_failure_publishes_release_unknown_owner(
    fake_xtdata: _FakeXtData,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:release-readback-unknown",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="consumer", symbols=["000001.SZ"])
    original_get = subscriber.get_phase_one_lease
    monkeypatch.setattr(
        subscriber,
        "release_phase_one_lease",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("release transport failed")),
    )
    monkeypatch.setattr(
        subscriber,
        "get_phase_one_lease",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("readback transport failed")),
    )

    with pytest.raises(QuoteContractError) as failure:
        supervisor.release_consumer(consumer_id="consumer")
    assert failure.value.context["release_outcome"] == "UNKNOWN"
    assert failure.value.context["release_exception_type"] == "RuntimeError"
    assert failure.value.context["readback_exception_type"] == "RuntimeError"

    monkeypatch.setattr(subscriber, "get_phase_one_lease", original_get)
    owner = supervisor.consumer_lease_owner_snapshot(
        consumer_id="consumer",
        symbols=("000001.SZ",),
    )
    assert owner["state"] == "RELEASE_UNKNOWN"
    assert owner["exact_owner"] is False
    supervisor.shutdown()


def test_release_consumer_post_mutation_exception_reconciles_absent_lease_loudly(
    fake_xtdata: _FakeXtData,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:release-post-mutation",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    lease = supervisor.acquire_consumer(consumer_id="consumer", symbols=["000001.SZ"])
    original_release = subscriber.release_phase_one_lease

    def release_then_raise(**kwargs: object) -> bool:
        assert original_release(**kwargs) is True
        raise RuntimeError("unsubscribe acknowledgement lost")

    monkeypatch.setattr(subscriber, "release_phase_one_lease", release_then_raise)
    with caplog.at_level(logging.WARNING):
        assert supervisor.release_consumer(consumer_id="consumer") is True

    health = supervisor.health()
    assert health["consumers"] == {}
    assert health["last_release_reconciliation"] == {
        "schema_version": "miniqmt_quote_release_reconciliation_v1",
        "consumer_id": "consumer",
        "lease_id": lease.lease_id,
        "disposition": "POST_MUTATION_EXCEPTION_RECONCILED",
        "observed_at_utc": health["last_release_reconciliation"]["observed_at_utc"],
        "exception_type": "RuntimeError",
    }
    assert "required exact reconciliation" in caplog.text
    supervisor.shutdown()


def test_release_consumer_pre_mutation_exception_preserves_exact_active_owner(
    fake_xtdata: _FakeXtData,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:release-pre-mutation",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    lease = supervisor.acquire_consumer(consumer_id="consumer", symbols=["000001.SZ"])
    original_release = subscriber.release_phase_one_lease
    monkeypatch.setattr(
        subscriber,
        "release_phase_one_lease",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("unsubscribe rejected")),
    )

    with pytest.raises(QuoteContractError) as caught:
        supervisor.release_consumer(consumer_id="consumer")
    assert caught.value.context["release_outcome"] == "ACTIVE"
    assert supervisor.health()["consumers"]["consumer"]["lease_id"] == lease.lease_id
    monkeypatch.setattr(subscriber, "release_phase_one_lease", original_release)
    supervisor.shutdown()


@pytest.mark.parametrize("post_mutation", [False, True])
def test_release_consumer_false_return_uses_exact_active_or_absent_readback(
    fake_xtdata: _FakeXtData,
    monkeypatch: pytest.MonkeyPatch,
    post_mutation: bool,
) -> None:
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key=f"SIM:release-false:{post_mutation}",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="consumer", symbols=["000001.SZ"])
    original_release = subscriber.release_phase_one_lease

    def false_release(**kwargs: object) -> bool:
        if post_mutation:
            assert original_release(**kwargs) is True
        return False

    monkeypatch.setattr(subscriber, "release_phase_one_lease", false_release)
    assert supervisor.release_consumer(consumer_id="consumer") is post_mutation
    health = supervisor.health()
    if post_mutation:
        assert health["consumers"] == {}
        assert health["last_release_reconciliation"]["disposition"] == ("FALSE_RETURN_ABSENT_LEASE_RECONCILED")
    else:
        assert health["consumers"]["consumer"]["lease_status"] == "ACTIVE"
    monkeypatch.setattr(subscriber, "release_phase_one_lease", original_release)
    supervisor.shutdown()


def test_release_consumer_rejects_live_same_id_observation_sink(fake_xtdata: _FakeXtData) -> None:
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:release-live-sink",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="consumer", symbols=["000001.SZ"])

    def sink(*_values: object) -> None:
        return None

    supervisor.register_observation_sink(
        consumer_id="consumer",
        symbols=("000001.SZ",),
        sink=sink,
    )

    with pytest.raises(QuoteContractError) as caught:
        supervisor.release_consumer(consumer_id="consumer")
    assert caught.value.context["release_outcome"] == "ACTIVE"
    assert (
        supervisor.get_observation_sink(
            consumer_id="consumer",
            symbols=("000001.SZ",),
        )
        is sink
    )
    assert (
        supervisor.unregister_observation_sink(
            consumer_id="consumer",
            symbols=("000001.SZ",),
            sink=sink,
        )
        is True
    )
    assert supervisor.release_consumer(consumer_id="consumer") is True
    supervisor.shutdown()


def test_release_consumer_local_cleanup_failure_remains_retriable(
    fake_xtdata: _FakeXtData,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:release-local-cleanup",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="consumer", symbols=["000001.SZ"])
    original_replace = supervisor._projection_sink.replace_admitted
    calls = 0

    def fail_once(symbols: tuple[str, ...]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("projection cleanup failed")
        original_replace(symbols)

    monkeypatch.setattr(supervisor._projection_sink, "replace_admitted", fail_once)
    with pytest.raises(QuoteContractError) as caught:
        supervisor.release_consumer(consumer_id="consumer")
    assert caught.value.context["release_outcome"] == "UNKNOWN"
    assert "consumer" in supervisor.health()["consumers"]

    monkeypatch.setattr(supervisor._projection_sink, "replace_admitted", original_replace)
    assert supervisor.release_consumer(consumer_id="consumer") is True
    assert supervisor.health()["consumers"] == {}
    supervisor.shutdown()


def test_supervisor_reacquires_after_last_release_without_reusing_fenced_generation(
    fake_xtdata: _FakeXtData,
) -> None:
    def full_depth_payload() -> dict[str, object]:
        return {
            **_payload(10.0),
            "bidPrice": [9.99, 9.98, None, None, None],
            "askPrice": [10.01, 10.02, None, None, None],
            "bidVol": [100, 100, 0, 0, 0],
            "askVol": [100, 100, 0, 0, 0],
        }

    context_store = QuoteEvaluationContextStore()
    context_store.publish(_projection_context())
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:lunch-reacquire",
        owner="simulation-scheduler",
        bootstrap_fetcher=lambda symbols: {symbol: full_depth_payload() for symbol in symbols},
        context_store=context_store,
    )
    morning = supervisor.acquire_consumer(consumer_id="morning", symbols=["000001.SZ"])
    assert supervisor.release_consumer(consumer_id="morning") is True

    afternoon = supervisor.acquire_consumer(consumer_id="afternoon", symbols=["000001.SZ"])

    assert afternoon.generation > morning.generation
    _wait_until(lambda: supervisor.snapshot_store.snapshot().get("000001.SZ") is not None)
    _wait_until(lambda: supervisor.normalized_store.get("000001.SZ") is not None)
    health = supervisor.health()
    assert health["writer"]["generation"] == afternoon.generation
    assert health["writer"]["active_generation"] == afternoon.generation
    assert health["writer"]["fenced_generation"] == morning.generation
    assert health["writer"]["active_failure"] is None
    assert health["writer"]["ordering_rejected_count"] == 0
    supervisor.shutdown()


def test_failed_candidate_fence_serializes_inflight_callback_and_leaves_no_pending_generation(
    fake_xtdata: _FakeXtData,
    caplog: pytest.LogCaptureFixture,
) -> None:
    bootstrap_started = threading.Event()
    allow_bootstrap_failure = threading.Event()
    callback_started = threading.Event()
    allow_callback_capture = threading.Event()
    discard_started = threading.Event()
    discard_completed = threading.Event()
    failed_candidate = False

    def bootstrap(symbols: list[str]) -> Mapping[str, Mapping[str, object]]:
        if not failed_candidate:
            return {symbol: _payload(10.0) for symbol in symbols}
        bootstrap_started.set()
        assert allow_bootstrap_failure.wait(timeout=5)
        return {"000001.SZ": _payload(10.0)}

    context_store = QuoteEvaluationContextStore()
    context_store.publish(_projection_context())
    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:failed-candidate-callback-race",
        owner="simulation-scheduler",
        bootstrap_fetcher=bootstrap,
        context_store=context_store,
    )
    active = supervisor.acquire_consumer(consumer_id="active", symbols=["000001.SZ"])
    worker = supervisor._worker
    original_capture = worker.capture_delivery
    callback_observed_after_fence: list[bool] = []

    def blocking_capture(
        delivery: PhaseOneQuoteDelivery,
        *,
        source_session_id: str,
        clock_domain_id: str,
    ) -> bool:
        if (
            delivery.generation == active.generation + 1
            and delivery.source_method is QuoteSourceMethod.WHOLE_QUOTE_CALLBACK
        ):
            callback_started.set()
            assert allow_callback_capture.wait(timeout=5)
            callback_observed_after_fence.append(discard_completed.is_set())
        return original_capture(
            delivery,
            source_session_id=source_session_id,
            clock_domain_id=clock_domain_id,
        )

    worker.capture_delivery = blocking_capture  # type: ignore[method-assign]
    original_discard = subscriber._discard_preparing_phase_one_feed

    def signaling_discard(feed) -> None:  # type: ignore[no-untyped-def]
        discard_started.set()
        original_discard(feed)
        discard_completed.set()

    subscriber._discard_preparing_phase_one_feed = signaling_discard  # type: ignore[method-assign]
    failed_candidate = True
    errors: list[BaseException] = []

    def acquire_candidate() -> None:
        try:
            supervisor.acquire_consumer(consumer_id="candidate", symbols=["000002.SZ"])
        except BaseException as exc:  # noqa: BLE001 - thread transports the exact typed failure to the assertion
            errors.append(exc)

    acquire_thread = threading.Thread(target=acquire_candidate)
    acquire_thread.start()
    assert bootstrap_started.wait(timeout=5)
    failed_sequence = fake_xtdata.subscribe_calls[-1]
    callback_thread = threading.Thread(
        target=lambda: fake_xtdata.callbacks[failed_sequence]({"000001.SZ": {**_payload(10.1), "time": "13000000"}})
    )
    callback_thread.start()
    assert callback_started.wait(timeout=5)
    allow_bootstrap_failure.set()
    assert discard_started.wait(timeout=5)
    allow_callback_capture.set()
    callback_thread.join(timeout=5)
    acquire_thread.join(timeout=5)

    assert not callback_thread.is_alive()
    assert not acquire_thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], QuoteContractError)
    assert errors[0].reason_code == QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE
    assert discard_completed.is_set()
    assert callback_observed_after_fence == [False]
    assert active.generation + 1 not in worker._pending_by_generation
    assert worker.health()["active_generation"] == active.generation

    capture_count = len(callback_observed_after_fence)
    fake_xtdata.callbacks[failed_sequence]({"000001.SZ": {**_payload(10.2), "time": "13000100"}})
    assert len(callback_observed_after_fence) == capture_count
    assert any("STALE_GENERATION" in record.message for record in caplog.records)
    supervisor.shutdown()


def test_invalid_bootstrap_raw_frame_is_loud_and_never_published(fake_xtdata: _FakeXtData) -> None:
    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(),
        data_session_key="SIM:invalid-bootstrap",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: {"time": object(), "lastPrice": object()} for symbol in symbols},
    )

    with pytest.raises(QuoteContractError) as exc_info:
        supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])

    assert exc_info.value.reason_code == QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE
    health = supervisor.health()
    assert health["subscription"]["status"] == "INACTIVE"
    assert health["subscription"]["bootstrap_coverage_ratio"] == 0.0
    assert supervisor.snapshot_store.snapshot() == {}
    supervisor.shutdown()


def test_multiple_logical_consumers_share_one_writer_and_release_purges_snapshot(fake_xtdata: _FakeXtData) -> None:
    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(max_symbols=2),
        data_session_key="SIM:single-writer",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    supervisor.acquire_consumer(consumer_id="second", symbols=["000002.SZ"])
    _wait_until(lambda: len(supervisor.snapshot_store.snapshot()) == 2)

    health = supervisor.health()
    assert set(health["consumers"]) == {"first", "second"}
    assert health["writer"]["admitted_symbols"] == ["000001.SZ", "000002.SZ"]
    assert health["writer"]["ordering_rejected_count"] == 0

    assert supervisor.release_consumer(consumer_id="second") is True
    assert set(supervisor.snapshot_store.snapshot()) == {"000001.SZ"}
    assert supervisor.health()["writer"]["admitted_symbols"] == ["000001.SZ"]
    supervisor.shutdown()


def test_consumer_callback_failure_fences_writer_and_supervisor_rebuilds_generation(fake_xtdata: _FakeXtData) -> None:
    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(restart_backoff_ms=1, restart_max_backoff_ms=1),
        data_session_key="SIM:consumer-recovery",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    initial_generation = supervisor.health()["subscription"]["generation"]
    fake_xtdata.emit_last({"000001.SZ": {"time": "bad", "lastPrice": object()}})
    _wait_until(lambda: supervisor.health()["writer"]["status"] == "FAILED")
    _wait_until(lambda: not supervisor.health()["writer"]["thread_alive"])

    health = supervisor.watchdog_tick()

    assert health["subscription"]["generation"] == initial_generation + 1
    assert health["writer"]["status"] in {"STARTING", "ACTIVE"}
    supervisor.shutdown()


def test_stale_alive_writer_is_epoch_fenced_and_requests_rebuild() -> None:
    captured: list[object] = []
    worker = QuoteIngressWorker(
        consumer_id="heartbeat-stale",
        config=_config(heartbeat_timeout_ms=10, restart_backoff_ms=1, restart_max_backoff_ms=1),
        frame_sink=captured.append,
    )
    worker.admit_symbols(("000001.SZ",))
    assert worker.on_generation_published("SIM:heartbeat", 1) is True
    _wait_until(lambda: worker.health()["writer_heartbeat_age_ms"] is not None)
    now = time.monotonic_ns() + 100_000_000

    request = worker.watchdog(now_monotonic_ns=now)

    assert request is not None
    assert request.generation == 1
    assert worker.health()["status"] == "RESTART_PENDING"
    _wait_until(lambda: not worker.health()["thread_alive"])
    assert worker.on_generation_published("SIM:heartbeat", 2) is True
    worker.shutdown()


def test_quote_ingress_isolation_has_no_database_broker_or_strategy_imports() -> None:
    tree = ast.parse(Path("backend/services/miniqmt_execution_runtime/quote_ingress.py").read_text(encoding="utf-8"))
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]

    assert not any(module.startswith("backend.db") for module in modules)
    assert not any("qmt_client" in module or "execution_runtime.client" in module for module in modules)
    assert not any("strategy" in module for module in modules)
