from __future__ import annotations

import ast
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from backend.execution_algos.adaptive_is.contracts import (
    QUOTE_CONTRACT_SCHEMA_VERSION,
    ActionQuoteEligibility,
    AuctionCapabilityState,
    CalendarSnapshot,
    CalendarSnapshotSet,
    ClosingAuctionSnapshot,
    ControlRevision,
    DepthQuantityUnit,
    EligibilityState,
    EvidenceCaptureType,
    ExecutionClockEvent,
    FiveLevelQuote,
    MARKET_DATA_EVIDENCE_SCHEMA_VERSION,
    MarketCode,
    MarketDataEvidenceV1,
    MarketPhase,
    PriceBasis,
    QuoteBatchAggregateState,
    QuoteCapability,
    QuoteSnapshotBatch,
    QuoteSource,
    QuoteSourceMethod,
    QuoteValidationState,
    CLOSING_AUCTION_SCHEMA_VERSION,
    SessionSegment,
    TradabilitySnapshot,
    TradabilityState,
    exact_symbol,
)
from backend.execution_algos.adaptive_is.reasons import (
    QUOTE_FAILURE_REGISTRY,
    QuoteContractError,
    QuoteContractReasonCode,
    QuoteContractStage,
    QuoteFailureRetryClass,
    QuoteFailureSeverity,
    quote_contract_error,
)


def _valid_quote(**overrides: object) -> FiveLevelQuote:
    fields: dict[str, object] = {
        "schema_version": QUOTE_CONTRACT_SCHEMA_VERSION,
        "normalizer_map_version": "miniqmt_quote_normalizer_map_v2",
        "timestamp_parser_version": "miniqmt_quote_timestamp_parser_v2",
        "source": QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        "source_session_id": "session-unit",
        "ingress_generation": 4,
        "ingress_sequence": 12,
        "source_method": QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
        "symbol": "000001.SZ",
        "market": MarketCode.SZ,
        "board": "MAIN",
        "source_exchange_time_utc": datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        "source_trade_date": date(2026, 7, 12),
        "clock_trade_date": date(2026, 7, 12),
        "received_at_utc": datetime(2026, 7, 12, 1, 30, 1, tzinfo=UTC),
        "received_monotonic_ns": 1_000_000,
        "clock_domain_id": "unit-clock",
        "last_price": Decimal("10.00"),
        "pre_close": Decimal("9.80"),
        "total_volume": Decimal("10000"),
        "total_amount": Decimal("100000"),
        "security_status": "NORMAL",
        "openint_status": "OPEN",
        "price_basis": PriceBasis.RAW_CNY_PER_SHARE,
        "depth_quantity_unit": DepthQuantityUnit.SHARES,
        "unit_evidence_version": "unit-evidence-v1",
        "bid_prices": (Decimal("9.99"), Decimal("9.98"), None, None, None),
        "bid_quantities": (100, 200, 0, 0, 0),
        "bid_quantities_raw": (Decimal("100"), Decimal("200"), Decimal("0"), Decimal("0"), Decimal("0")),
        "ask_prices": (Decimal("10.01"), Decimal("10.02"), None, None, None),
        "ask_quantities": (100, 200, 0, 0, 0),
        "ask_quantities_raw": (Decimal("100"), Decimal("200"), Decimal("0"), Decimal("0"), Decimal("0")),
        "quote_capabilities": frozenset(
            {
                QuoteCapability.FIVE_LEVEL_DEPTH,
                QuoteCapability.EXCHANGE_TIMESTAMP,
                QuoteCapability.RAW_PRICE_BASIS,
                QuoteCapability.DEPTH_UNIT_SHARES,
            }
        ),
        "source_payload_sha256": "a" * 64,
    }
    fields.update(overrides)
    return FiveLevelQuote(**fields)  # type: ignore[arg-type]


def _sha(character: str) -> str:
    return character * 64


def _valid_tradability(**overrides: object) -> TradabilitySnapshot:
    fields: dict[str, object] = {
        "schema_version": "tradability-v1",
        "tradability_id": "trad-valid",
        "symbol": "000001.SZ",
        "market": MarketCode.SZ,
        "board": "MAIN",
        "trade_date": date(2026, 7, 12),
        "price_basis": PriceBasis.RAW_CNY_PER_SHARE,
        "pre_close": Decimal("9.80"),
        "limit_up": Decimal("10.78"),
        "limit_down": Decimal("8.82"),
        "price_tick": Decimal("0.01"),
        "lot_size": 100,
        "is_suspended": False,
        "suspension_source": None,
        "security_status": "NORMAL",
        "openint_status": "OPEN",
        "observed_at_utc": datetime(2026, 7, 12, 1, tzinfo=UTC),
        "source": "test",
        "source_version": "v1",
        "state": TradabilityState.TRADABLE,
    }
    fields.update(overrides)
    return TradabilitySnapshot(**fields)  # type: ignore[arg-type]


def _eligibility(**overrides: object) -> ActionQuoteEligibility:
    fields: dict[str, object] = {
        "runtime_id": "runtime-1",
        "parent_intent_id": "parent-1",
        "algo_instance_id": "algo-1",
        "symbol": "000001.SZ",
        "side": "BUY",
        "market_data_id": "market-data-1",
        "clock_event_id": "clock-1",
        "tradability_id": "trad-valid",
        "control_revision": ControlRevision.B0_QUOTE_V2,
        "policy_sha256": _sha("b"),
        "config_sha256": _sha("c"),
        "adapter_sha256": _sha("d"),
        "state": EligibilityState.READY,
        "reason_code": None,
        "stage": None,
        "evaluated_at_utc": datetime(2026, 7, 12, 1, tzinfo=UTC),
    }
    fields.update(overrides)
    return ActionQuoteEligibility(**fields)  # type: ignore[arg-type]


def test_quote_hash_excludes_local_receive_time_and_sequence() -> None:
    first = _valid_quote()
    second = _valid_quote(
        ingress_sequence=999,
        received_at_utc=first.received_at_utc + timedelta(minutes=5),
        received_monotonic_ns=999_000_000,
    )

    assert first.validation_state == QuoteValidationState.VALID
    assert first.normalized_quote_sha256 == second.normalized_quote_sha256


def test_missing_five_level_arrays_are_capability_missing_not_fabricated_zero_depth() -> None:
    quote = _valid_quote(
        bid_prices=None,
        bid_quantities=None,
        bid_quantities_raw=None,
        ask_prices=None,
        ask_quantities=None,
        ask_quantities_raw=None,
        quote_capabilities=frozenset({QuoteCapability.RAW_PRICE_BASIS, QuoteCapability.EXCHANGE_TIMESTAMP}),
    )

    assert quote.validation_state == QuoteValidationState.CAPABILITY_MISSING
    assert QuoteContractReasonCode.DEPTH_CAPABILITY_MISSING in quote.validation_reasons
    assert quote.bid_prices is None
    assert quote.ask_prices is None
    assert not quote.has_five_level_depth


def test_bad_depth_prefix_and_crossed_book_are_invalid_but_locked_book_is_explicit() -> None:
    bad_prefix = _valid_quote(
        bid_prices=(Decimal("9.99"), None, Decimal("9.97"), None, None),
        bid_quantities=(100, 0, 100, 0, 0),
    )
    crossed = _valid_quote(ask_prices=(Decimal("9.98"), Decimal("10.02"), None, None, None))
    locked = _valid_quote(ask_prices=(Decimal("9.99"), Decimal("10.02"), None, None, None))

    assert bad_prefix.validation_state == QuoteValidationState.INVALID
    assert QuoteContractReasonCode.DEPTH_SCHEMA_INVALID in bad_prefix.validation_reasons
    assert crossed.validation_state == QuoteValidationState.INVALID
    assert locked.validation_state == QuoteValidationState.VALID
    assert locked.book_state.value == "LOCKED"


def test_exact_symbol_contract_rejects_fuzzy_codes() -> None:
    assert exact_symbol("000001.sz") == ("000001.SZ", MarketCode.SZ)
    with pytest.raises(QuoteContractError) as exc_info:
        exact_symbol("000001")
    assert exc_info.value.reason_code == QuoteContractReasonCode.SYMBOL_INVALID


def test_tradability_keeps_suspension_distinct_from_unexplained_data_invalidity() -> None:
    suspended = TradabilitySnapshot(
        schema_version="tradability-v1",
        tradability_id="trad-suspended",
        symbol="000001.SZ",
        market=MarketCode.SZ,
        board="MAIN",
        trade_date=date(2026, 7, 12),
        price_basis=PriceBasis.RAW_CNY_PER_SHARE,
        pre_close=None,
        limit_up=None,
        limit_down=None,
        price_tick=None,
        lot_size=None,
        is_suspended=True,
        suspension_source="market.suspend_d",
        security_status="SUSPENDED",
        openint_status=None,
        observed_at_utc=datetime(2026, 7, 12, 1, tzinfo=UTC),
        source="market.suspend_d",
        source_version="v1",
        state=TradabilityState.SUSPENDED,
    )
    broken = TradabilitySnapshot(
        schema_version="tradability-v1",
        tradability_id="trad-broken",
        symbol="000001.SZ",
        market=MarketCode.SZ,
        board="MAIN",
        trade_date=date(2026, 7, 12),
        price_basis=PriceBasis.RAW_CNY_PER_SHARE,
        pre_close=None,
        limit_up=None,
        limit_down=None,
        price_tick=None,
        lot_size=None,
        is_suspended=False,
        suspension_source=None,
        security_status="NORMAL",
        openint_status="OPEN",
        observed_at_utc=datetime(2026, 7, 12, 1, tzinfo=UTC),
        source="test",
        source_version="v1",
        state=TradabilityState.TRADABLE,
    )

    assert suspended.state == TradabilityState.SUSPENDED
    assert broken.state == TradabilityState.DATA_INVALID
    assert QuoteContractReasonCode.TRADABILITY_DATA_INVALID in broken.validation_reasons


def test_calendar_requires_all_markets_and_aware_clock_fields() -> None:
    segment = SessionSegment(start_local=time(9, 30), end_local=time(11, 30))
    snapshot_by_market = {
        market: CalendarSnapshot(
            calendar_id=f"calendar-{market.value}",
            market=market,
            trade_date=date(2026, 7, 12),
            timezone="Asia/Shanghai",
            session_segments=(segment,),
            effective_at_utc=datetime(2026, 7, 12, 0, tzinfo=UTC),
            source_version="calendar-v1",
        )
        for market in MarketCode
    }
    calendar_set = CalendarSnapshotSet(snapshot_set_id="set-1", snapshot_by_market=snapshot_by_market)

    assert len(calendar_set.snapshot_by_market) == 3
    with pytest.raises(QuoteContractError) as exc_info:
        CalendarSnapshotSet(snapshot_set_id="missing-bj", snapshot_by_market={MarketCode.SH: snapshot_by_market[MarketCode.SH]})
    assert exc_info.value.reason_code == QuoteContractReasonCode.CLOCK_CALENDAR_INVALID


def test_eligibility_never_reports_ready_with_a_failure_reason() -> None:
    with pytest.raises(QuoteContractError) as exc_info:
        ActionQuoteEligibility(
            runtime_id="runtime-1",
            parent_intent_id="parent-1",
            algo_instance_id="algo-1",
            symbol="000001.SZ",
            side="BUY",
            market_data_id=None,
            clock_event_id="clock-1",
            tradability_id=None,
            control_revision="B0_QUOTE_V2",
            policy_sha256=_sha("b"),
            config_sha256=_sha("c"),
            adapter_sha256=_sha("d"),
            state=EligibilityState.READY,
            reason_code=QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
            stage="ELIGIBILITY",
            evaluated_at_utc=datetime(2026, 7, 12, 1, tzinfo=UTC),
        )
    assert exc_info.value.reason_code == QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE


def test_all_reason_codes_have_one_registered_stage() -> None:
    assert set(QUOTE_FAILURE_REGISTRY) == set(QuoteContractReasonCode)
    assert {definition.stage.value for definition in QUOTE_FAILURE_REGISTRY.values()} >= {"NORMALIZE", "UNIT", "TRADABILITY", "ELIGIBILITY"}


def test_reason_registry_is_immutable_and_carries_complete_loud_metadata() -> None:
    definition = QUOTE_FAILURE_REGISTRY[QuoteContractReasonCode.EVIDENCE_PERSIST_FAILED]
    assert definition.severity == QuoteFailureSeverity.CRITICAL
    assert definition.retry_class == QuoteFailureRetryClass.AUTOMATIC_RETRY
    with pytest.raises(TypeError):
        QUOTE_FAILURE_REGISTRY[QuoteContractReasonCode.EVIDENCE_PERSIST_FAILED] = definition  # type: ignore[index]

    error = quote_contract_error(
        QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
        "calendar mismatch",
        stage=QuoteContractStage.CALENDAR,
    )
    assert error.as_loud_payload()["stage"] == "CALENDAR"
    assert error.as_loud_payload()["severity"] == "ERROR"
    assert error.as_loud_payload()["retry_class"] == "NEXT_EVIDENCE"


def test_calendar_clock_and_batch_mappings_are_deeply_immutable() -> None:
    segment = SessionSegment(start_local=time(9, 30), end_local=time(11, 30))
    snapshots = {
        market: CalendarSnapshot(
            calendar_id=f"calendar-{market.value}",
            market=market,
            trade_date=date(2026, 7, 12),
            timezone="Asia/Shanghai",
            session_segments=(segment,),
            effective_at_utc=datetime(2026, 7, 12, tzinfo=UTC),
            source_version="calendar-v1",
        )
        for market in MarketCode
    }
    calendar_set = CalendarSnapshotSet(snapshot_set_id="set-immutable", snapshot_by_market=snapshots)
    original_hash = calendar_set.set_sha256
    with pytest.raises(TypeError):
        calendar_set.snapshot_by_market[MarketCode.SH] = snapshots[MarketCode.SZ]  # type: ignore[index]
    assert calendar_set.set_sha256 == original_hash

    clock = ExecutionClockEvent(
        clock_event_id="clock-1",
        clock_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        clock_monotonic_ns=1,
        clock_domain_id="clock-domain",
        clock_trade_date=date(2026, 7, 12),
        calendar_snapshot_set_id=calendar_set.snapshot_set_id,
        phase_by_market={market: MarketPhase.CONTINUOUS for market in MarketCode},
        phase_schedule_version="A_SHARE_EQUITY_PHASE_SCHEDULE_V1_20260706",
        source="calendar-clock-v1",
        observed_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
    )
    with pytest.raises(TypeError):
        clock.phase_by_market[MarketCode.SH] = MarketPhase.CLOSED  # type: ignore[index]

    batch = QuoteSnapshotBatch(
        batch_id="batch-1",
        runtime_id="runtime-1",
        clock_event_id="clock-1",
        policy_sha256=_sha("b"),
        active_symbols=("000001.SZ",),
        dependency_groups={},
        eligibility_by_symbol={"000001.SZ": _eligibility()},
        quote_by_symbol={"000001.SZ": _valid_quote()},
        group_watermark_ms={},
        group_max_skew_ms={},
        aggregate_state=QuoteBatchAggregateState.OBSERVED,
    )
    with pytest.raises(TypeError):
        batch.quote_by_symbol["000001.SZ"] = _valid_quote()  # type: ignore[index]


@pytest.mark.parametrize("aggregate_state", ["NOT_REGISTERED", "", None])
def test_quote_snapshot_batch_rejects_unregistered_aggregate_state(aggregate_state: object) -> None:
    with pytest.raises(QuoteContractError):
        QuoteSnapshotBatch(
            batch_id="batch-invalid",
            runtime_id="runtime-1",
            clock_event_id="clock-1",
            policy_sha256=_sha("b"),
            active_symbols=(),
            dependency_groups={},
            eligibility_by_symbol={},
            quote_by_symbol={},
            group_watermark_ms={},
            group_max_skew_ms={},
            aggregate_state=aggregate_state,  # type: ignore[arg-type]
        )


def test_market_data_evidence_is_complete_typed_and_hash_stable() -> None:
    quote = _valid_quote()
    tradability = _valid_tradability()
    evidence = MarketDataEvidenceV1(
        market_data_id="market-data-1",
        evidence_schema_version=MARKET_DATA_EVIDENCE_SCHEMA_VERSION,
        capture_type=EvidenceCaptureType.ACTION_INPUT,
        runtime_id="runtime-1",
        binding_id="binding-1",
        trade_date=date(2026, 7, 12),
        parent_intent_id="parent-1",
        child_order_id=None,
        action_id="action-1",
        quote=quote,
        tradability=tradability,
        clock_event_id="clock-1",
        quality_reason_code=None,
        stage=None,
        control_revision=ControlRevision.B0_QUOTE_V2,
        policy_sha256=_sha("b"),
        config_sha256=_sha("c"),
        adapter_sha256=_sha("d"),
        code_sha256=_sha("e"),
        schema_sha256=_sha("f"),
        calendar_sha256=_sha("a"),
        captured_at_utc=datetime(2026, 7, 12, 1, 30, 2, tzinfo=UTC),
        persisted_at_utc=datetime(2026, 7, 12, 1, 30, 3, tzinfo=UTC),
        quote_age_ms=2000,
        source_lag_ms=1000,
        transport_lag_ms=1000,
        benchmark_policy_version="arrival-v1",
        mark_policy_version="markout-v1",
        source_input_sha256=None,
        algo_instance_id="algo-1",
        side="BUY",
        eligibility_state=EligibilityState.READY,
    )
    payload = evidence.canonical_payload()
    assert payload["mid_price"] == Decimal("10.00")
    assert payload["bid_prices"] == quote.bid_prices
    assert payload["benchmark_policy_version"] == "arrival-v1"
    assert len(payload["source_input_sha256"]) == 64
    assert len(evidence.evidence_sha256) == 64


@pytest.mark.parametrize(
    "override",
    [
        {"evidence_schema_version": "wrong"},
        {"capture_type": "NOT_A_CAPTURE"},
        {"control_revision": "LEGACY_B0"},
        {"policy_sha256": ""},
        {"quality_reason_code": QuoteContractReasonCode.EVIDENCE_PERSIST_FAILED, "stage": "NORMALIZE"},
    ],
)
def test_market_data_evidence_rejects_invalid_contract_fields(override: dict[str, object]) -> None:
    fields: dict[str, object] = {
        "market_data_id": "market-data-reject",
        "evidence_schema_version": MARKET_DATA_EVIDENCE_SCHEMA_VERSION,
        "capture_type": EvidenceCaptureType.ACTION_REJECT,
        "runtime_id": "runtime-1",
        "binding_id": "binding-1",
        "trade_date": date(2026, 7, 12),
        "parent_intent_id": "parent-1",
        "child_order_id": None,
        "action_id": None,
        "quote": None,
        "tradability": None,
        "clock_event_id": "clock-1",
        "quality_reason_code": QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
        "stage": "ELIGIBILITY",
        "control_revision": ControlRevision.B0_QUOTE_V2,
        "policy_sha256": _sha("b"),
        "config_sha256": _sha("c"),
        "adapter_sha256": _sha("d"),
        "code_sha256": _sha("e"),
        "schema_sha256": _sha("f"),
        "calendar_sha256": _sha("a"),
        "captured_at_utc": datetime(2026, 7, 12, 1, 30, 2, tzinfo=UTC),
        "persisted_at_utc": None,
        "quote_age_ms": None,
        "source_lag_ms": None,
        "transport_lag_ms": None,
        "benchmark_policy_version": "arrival-v1",
        "mark_policy_version": "markout-v1",
        "source_input_sha256": None,
        "algo_instance_id": "algo-1",
        "side": "BUY",
        "eligibility_state": EligibilityState.STALE,
        "source_session_id": "session-reject",
        "ingress_generation": 1,
        "ingress_sequence": 1,
        "quote_source": QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        "source_method": QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
        "source_payload_sha256": _sha("a"),
    }
    fields.update(override)
    with pytest.raises(QuoteContractError):
        MarketDataEvidenceV1(**fields)  # type: ignore[arg-type]


def test_unavailable_closing_auction_requires_registered_reason_and_never_synthesizes_fields() -> None:
    snapshot = ClosingAuctionSnapshot(
        schema_version=CLOSING_AUCTION_SCHEMA_VERSION,
        symbol="000001.SZ",
        clock_event_id="clock-auction",
        market_phase=MarketPhase.CLOSING_AUCTION,
        capability_state=AuctionCapabilityState.UNAVAILABLE,
        exchange_time_utc=None,
        received_at_utc=datetime(2026, 7, 12, 6, 57, tzinfo=UTC),
        source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        normalized_quote_sha256=None,
        indicative_match_price=None,
        indicative_match_volume=None,
        unmatched_side=None,
        unmatched_quantity=None,
        reasons=(QuoteContractReasonCode.CLOSING_AUCTION_CAPABILITY_UNAVAILABLE,),
    )
    assert snapshot.capability_state == AuctionCapabilityState.UNAVAILABLE
    with pytest.raises(QuoteContractError):
        ClosingAuctionSnapshot(
            **{
                **snapshot.__dict__,
                "reasons": (),
            }
        )


def test_adaptive_is_contracts_have_no_runtime_or_broker_imports() -> None:
    forbidden_prefixes = (
        "backend.db",
        "backend.infra",
        "backend.routers",
        "backend.services",
        "fastapi",
        "vnpy",
        "xtquant",
    )
    for path in Path("backend/execution_algos/adaptive_is").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        for imported in imports:
            assert not imported.startswith(forbidden_prefixes), f"{path} imports forbidden runtime token {imported}"


def test_p1a_quote_normalizer_has_no_xtdata_db_or_broker_import() -> None:
    path = Path("backend/services/miniqmt_execution_runtime/quote_normalizer.py")
    forbidden_prefixes = ("backend.db", "backend.infra", "xtquant", "fastapi", "sqlalchemy", "requests")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)

    for imported in imports:
        assert not imported.startswith(forbidden_prefixes), f"{path} imports forbidden P1-A token {imported}"
