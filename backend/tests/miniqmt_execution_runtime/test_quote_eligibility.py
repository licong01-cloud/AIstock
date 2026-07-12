from __future__ import annotations

import ast
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from backend.execution_algos.adaptive_is.contracts import (
    CalendarSnapshot,
    CalendarSnapshotSet,
    ControlRevision,
    DepthQuantityUnit,
    EligibilityState,
    MarketCode,
    PriceBasis,
    QuoteSourceMethod,
    SessionSegment,
    TradabilitySnapshot,
    TradabilityState,
    canonical_sha256,
)
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode
from backend.miniqmt_quote_contract_config import QuoteContractPolicy
from backend.services.miniqmt_execution_runtime.quote_eligibility import (
    ActionQuoteEvaluator,
    ActionQuoteRequest,
    ClockContinuityTracker,
    NormalizedQuoteObservation,
    OrderingDisposition,
    QuoteEvaluationContext,
    QuoteEvaluationContextStore,
    QuoteOrderingTracker,
    QuoteSymbolContext,
    build_execution_clock_event,
    build_quote_snapshot_batch,
    deterministic_market_data_id,
    phase_for_shanghai_time,
)
from backend.services.miniqmt_execution_runtime.quote_normalizer import capture_raw_quote_frame, normalize_raw_quote_frame
from backend.services.paper_trading_v2.market_data import (
    DailySuspendStatus,
    EquityInstrumentMetadata,
    PreviousClose,
)
from backend.services.simulation_runtime.miniqmt_quote_context import (
    MiniQMTQuoteContextAuthorityAdapter,
    QuoteContextSymbolSpec,
)
from backend.services.trading_core.limit_price_provider import DailyLimitPrice


UTC_CLOCK = datetime(2026, 7, 12, 1, 30, tzinfo=UTC)
SHA = "a" * 64


def _policy(**overrides: object) -> QuoteContractPolicy:
    payload: dict[str, object] = {
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
            "max_receive_age_ms": 2_000,
            "max_source_lag_ms": 2_000,
            "max_exchange_age_ms": 2_000,
            "max_negative_skew_ms": 20,
            "max_clock_age_divergence_ms": 20,
            "max_dependency_group_skew_ms": 100,
            "auction_mode": "OBSERVE_ONLY",
        }
    }
    payload["quote_contract"].update(overrides)  # type: ignore[index]
    return QuoteContractPolicy.from_execution_policy(payload)


def _calendar_set(trade_date: date = date(2026, 7, 12)) -> CalendarSnapshotSet:
    segments = (
        SessionSegment(time(9, 15), time(9, 25)),
        SessionSegment(time(9, 30), time(11, 30)),
        SessionSegment(time(13, 0), time(14, 57)),
        SessionSegment(time(14, 57), time(15, 0)),
    )
    return CalendarSnapshotSet(
        snapshot_set_id=f"calendar-set-{trade_date.isoformat()}",
        snapshot_by_market={
            market: CalendarSnapshot(
                calendar_id=f"calendar-{market.value}-{trade_date.isoformat()}",
                market=market,
                trade_date=trade_date,
                timezone="Asia/Shanghai",
                session_segments=segments,
                effective_at_utc=datetime.combine(trade_date, time(1, 0), tzinfo=UTC),
                source_version="market.trading_calendar:file_cache:checksum-v1:A_SHARE_EQUITY_PHASE_SCHEDULE_V1_20260706",
            )
            for market in MarketCode
        },
    )


def _tradability(*, symbol: str = "000001.SZ", state: TradabilityState = TradabilityState.TRADABLE) -> TradabilitySnapshot:
    return TradabilitySnapshot(
        schema_version="adaptive_is_tradability_snapshot_v1",
        tradability_id=f"tradability-{symbol}",
        symbol=symbol,
        market=MarketCode.SZ,
        board="MAIN",
        trade_date=date(2026, 7, 12),
        price_basis=PriceBasis.RAW_CNY_PER_SHARE,
        pre_close=Decimal("10.00"),
        limit_up=Decimal("11.00"),
        limit_down=Decimal("9.00"),
        price_tick=Decimal("0.01"),
        lot_size=100,
        is_suspended=state == TradabilityState.SUSPENDED,
        suspension_source="market.suspend_d",
        security_status="LISTED",
        openint_status=None,
        observed_at_utc=UTC_CLOCK,
        source="market.authority.preload",
        source_version="authority-v1",
        state=state,
    )


def _context(
    *,
    clock_at_utc: datetime = UTC_CLOCK,
    continuity_valid: bool = True,
    tradability: TradabilitySnapshot | None = None,
    policy: QuoteContractPolicy | None = None,
) -> QuoteEvaluationContext:
    policy = policy or _policy()
    calendar_set = _calendar_set(clock_at_utc.date())
    clock = build_execution_clock_event(
        calendar_snapshot_set=calendar_set,
        clock_at_utc=clock_at_utc,
        clock_monotonic_ns=2_000_000_000,
        clock_domain_id="test-clock-domain",
        source="test-scheduler",
    )
    tradability = tradability or _tradability()
    return QuoteEvaluationContext(
        calendar_snapshot_set=calendar_set,
        clock=clock,
        continuity_generation=1,
        continuity_valid=continuity_valid,
        policy=policy,
        symbols={
            "000001.SZ": QuoteSymbolContext(
                symbol="000001.SZ",
                board="MAIN",
                depth_quantity_unit=DepthQuantityUnit.SHARES,
                unit_evidence_version="xtdata-depth-unit-v1",
                tradability=tradability,
                product_type="EQUITY",
                product_type_proven_equity=True,
                authority_source_version="authority-v1",
            )
        },
    )


def _frame(
    *,
    sequence: int = 1,
    generation: int = 1,
    source_time: str | None = "09300000",
    last_price: str = "10.00",
    received_at_utc: datetime = UTC_CLOCK,
    received_monotonic_ns: int = 1_999_500_000,
    openint: str | None = "OPEN",
) -> object:
    payload: dict[str, object] = {
        "time": source_time,
        "lastPrice": last_price,
        "preClose": "10.00",
        "bidPrice": ["9.99", "9.98", None, None, None],
        "bidVol": [100, 100, 0, 0, 0],
        "askPrice": ["10.01", "10.02", None, None, None],
        "askVol": [100, 100, 0, 0, 0],
        "stockStatus": "NORMAL",
        "openint": openint,
    }
    return capture_raw_quote_frame(
        payload,
        callback_symbol="000001.SZ",
        source_session_id="quote-test-session",
        ingress_generation=generation,
        ingress_sequence=sequence,
        received_at_utc=received_at_utc,
        received_monotonic_ns=received_monotonic_ns,
        clock_domain_id="test-clock-domain",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )


def _observation(*, context: QuoteEvaluationContext, frame=None) -> NormalizedQuoteObservation:
    frame = frame or _frame()
    quote = normalize_raw_quote_frame(
        frame,
        clock_trade_date=context.clock.clock_trade_date,
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="xtdata-depth-unit-v1",
        tradability=context.symbol_context("000001.SZ").tradability,  # type: ignore[union-attr]
    )
    tradability = context.symbol_context("000001.SZ").tradability  # type: ignore[union-attr]
    assert tradability is not None
    return NormalizedQuoteObservation(
        frame=frame,
        quote=quote,
        tradability=tradability,
        context_id=context.context_id,
        market_data_id=deterministic_market_data_id(
            frame=frame,
            quote=quote,
            tradability=tradability,
            calendar_snapshot_set=context.calendar_snapshot_set,
            policy=context.policy,
        ),
        ordering_disposition=OrderingDisposition.ACCEPTED,
    )


def _request(*, symbol: str = "000001.SZ", side: str = "BUY", group: str | None = None, policy: QuoteContractPolicy | None = None) -> ActionQuoteRequest:
    policy = policy or _policy()
    return ActionQuoteRequest(
        runtime_id="runtime-1",
        parent_intent_id="parent-1",
        algo_instance_id="algo-1",
        symbol=symbol,
        side=side,
        control_revision=ControlRevision.B0_QUOTE_V2,
        policy_sha256=policy.policy_sha256,
        config_sha256=SHA,
        adapter_sha256="b" * 64,
        dependency_group_id=group,
    )


class _Calendar:
    def __init__(self, *, checksum: str = "calendar-checksum", trading: bool = True) -> None:
        self.checksum = checksum
        self.trading = trading
        self.calls = 0

    def status(self, *, as_of_date: date) -> dict[str, object]:
        self.calls += 1
        return {
            "as_of_date": as_of_date.isoformat(),
            "is_trading_day": self.trading,
            "source": "market.trading_calendar:file_cache",
            "cache": {"checksum": self.checksum},
        }


class _Suspend:
    def __init__(self) -> None:
        self.calls = 0

    def get_suspend_status(self, symbol: str, trade_date: date) -> DailySuspendStatus:
        self.calls += 1
        return DailySuspendStatus(symbol=symbol, trade_date=trade_date, is_suspended=False)


class _Limit:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls = 0
        self.fail = fail

    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        self.calls += 1
        if self.fail:
            raise RuntimeError("limit authority unavailable")
        return DailyLimitPrice(symbol=symbol, trade_date=trade_date, pre_close=10.0, up_limit=11.0, down_limit=9.0)


class _PreviousClose:
    def __init__(self) -> None:
        self.calls = 0

    def get_previous_close(self, symbol: str, trade_date: date) -> PreviousClose:
        self.calls += 1
        return PreviousClose(symbol=symbol, trade_date=trade_date, previous_trade_date=trade_date, pre_close=10.0)


class _Metadata:
    def __init__(self) -> None:
        self.calls = 0

    def get_equity_metadata(self, symbol: str, trade_date: date) -> EquityInstrumentMetadata:
        self.calls += 1
        return EquityInstrumentMetadata(
            symbol=symbol,
            market="MAIN",
            exchange="SZSE",
            list_status="L",
            list_date=date(1990, 1, 1),
            delist_date=None,
            product_type="EQUITY",
            source="market.stock_basic",
            source_version="stock-basic-v1",
        )


def _symbol_spec() -> QuoteContextSymbolSpec:
    return QuoteContextSymbolSpec(
        symbol="000001.SZ",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="xtdata-depth-unit-v1",
        price_tick=Decimal("0.01"),
        lot_size=100,
        intraday_halt=False,
        intraday_halt_source="scheduler-authority-v1",
    )


def test_quote_eligibility_core_has_no_db_fastapi_broker_or_scheduler_imports() -> None:
    source = Path("backend/services/miniqmt_execution_runtime/quote_eligibility.py").read_text(encoding="utf-8")
    imported_modules = {
        node.module or ""
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ImportFrom)
    }
    forbidden = ("backend.db", "fastapi", "broker", "paper_trading", "simulation_runtime.scheduler", "xtquant")
    assert not [module for module in imported_modules if any(token in module for token in forbidden)]


def test_calendar_snapshot_uses_authoritative_checksum_and_all_markets() -> None:
    calendar = _Calendar(checksum="checksum-20260712")
    store = QuoteEvaluationContextStore()
    adapter = MiniQMTQuoteContextAuthorityAdapter(
        context_store=store,
        trading_calendar_service=calendar,
        suspend_status_provider=_Suspend(),
        limit_price_provider=_Limit(),
        previous_close_provider=_PreviousClose(),
        equity_metadata_provider=_Metadata(),
    )

    context = adapter.preload(
        symbol_specs=[_symbol_spec()],
        policy=_policy(),
        clock_at_utc=UTC_CLOCK,
        clock_monotonic_ns=10,
        clock_domain_id="scheduler-clock",
    )

    assert calendar.calls == 1
    assert set(context.calendar_snapshot_set.snapshot_by_market) == set(MarketCode)
    assert all("checksum-20260712" in item.source_version for item in context.calendar_snapshot_set.snapshot_by_market.values())
    assert context.clock.phase_schedule_version == "A_SHARE_EQUITY_PHASE_SCHEDULE_V1_20260706"


@pytest.mark.parametrize(
    ("local_time", "expected"),
    [
        (time(9, 14, 59), "CLOSED"),
        (time(9, 15), "PRE_OPEN"),
        (time(9, 24, 59), "PRE_OPEN"),
        (time(9, 25), "CLOSED"),
        (time(9, 30), "CONTINUOUS"),
        (time(11, 30), "CONTINUOUS"),
        (time(11, 30, 1), "CLOSED"),
        (time(13, 0), "CONTINUOUS"),
        (time(14, 57), "CLOSING_AUCTION"),
        (time(15, 0), "CLOSED"),
    ],
)
def test_phase_schedule_boundaries_cover_open_break_continuous_auction_closed(local_time: time, expected: str) -> None:
    local = datetime.combine(date(2026, 7, 12), local_time, tzinfo=phase_for_shanghai_time.__globals__["CHINA_TZ"])
    assert phase_for_shanghai_time(local.astimezone(UTC)).value == expected


def test_clock_continuity_rejects_wall_rollback_domain_change_and_age_divergence() -> None:
    calendar_set = _calendar_set()
    tracker = ClockContinuityTracker()
    first = build_execution_clock_event(
        calendar_snapshot_set=calendar_set,
        clock_at_utc=UTC_CLOCK,
        clock_monotonic_ns=100_000_000,
        clock_domain_id="domain-a",
        source="test",
    )
    assert tracker.observe(clock=first, calendar_snapshot_set=calendar_set, max_negative_skew_ms=5).valid
    rollback = build_execution_clock_event(
        calendar_snapshot_set=calendar_set,
        clock_at_utc=UTC_CLOCK - timedelta(seconds=1),
        clock_monotonic_ns=200_000_000,
        clock_domain_id="domain-a",
        source="test",
    )
    rolled = tracker.observe(clock=rollback, calendar_snapshot_set=calendar_set, max_negative_skew_ms=5)
    assert rolled.valid is False and rolled.reset_reason == "WALL_CLOCK_ROLLBACK"
    changed_domain = build_execution_clock_event(
        calendar_snapshot_set=calendar_set,
        clock_at_utc=UTC_CLOCK,
        clock_monotonic_ns=300_000_000,
        clock_domain_id="domain-b",
        source="test",
    )
    changed = tracker.observe(clock=changed_domain, calendar_snapshot_set=calendar_set, max_negative_skew_ms=5)
    assert changed.valid and changed.reset_reason == "CLOCK_CONTINUITY_IDENTITY_CHANGED"

    context = _context(policy=_policy(max_clock_age_divergence_ms=1))
    observation = _observation(context=context, frame=_frame(received_monotonic_ns=1_000_000_000))
    result = ActionQuoteEvaluator().evaluate(request=_request(policy=context.policy), context=context, observation=observation)
    assert result.eligibility.state == EligibilityState.STALE
    assert "clock_age_divergence_ms:STALE" in result.diagnostics


def test_exact_duplicate_does_not_refresh_receive_time_or_market_data_identity() -> None:
    context = _context()
    frame = _frame()
    quote = normalize_raw_quote_frame(
        frame,
        clock_trade_date=context.clock.clock_trade_date,
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="unit-v1",
        tradability=_tradability(),
    )
    tracker = QuoteOrderingTracker()
    tracker.activate_generation(1)
    assert tracker.decide(frame=frame, quote=quote).accepted
    assert tracker.decide(frame=frame, quote=quote).accepted is False
    first_id = deterministic_market_data_id(
        frame=frame,
        quote=quote,
        tradability=_tradability(),
        calendar_snapshot_set=context.calendar_snapshot_set,
        policy=context.policy,
    )
    assert first_id == deterministic_market_data_id(
        frame=frame,
        quote=quote,
        tradability=_tradability(),
        calendar_snapshot_set=context.calendar_snapshot_set,
        policy=context.policy,
    )


def test_same_exchange_time_changed_payload_is_audited_correction() -> None:
    context = _context()
    tracker = QuoteOrderingTracker()
    tracker.activate_generation(1)
    first = _frame(sequence=1, last_price="10.00")
    correction = _frame(sequence=2, last_price="10.02")
    first_quote = normalize_raw_quote_frame(first, clock_trade_date=context.clock.clock_trade_date, board="MAIN", depth_quantity_unit=DepthQuantityUnit.SHARES, unit_evidence_version="unit-v1", tradability=_tradability())
    correction_quote = normalize_raw_quote_frame(correction, clock_trade_date=context.clock.clock_trade_date, board="MAIN", depth_quantity_unit=DepthQuantityUnit.SHARES, unit_evidence_version="unit-v1", tradability=_tradability())
    assert tracker.decide(frame=first, quote=first_quote).disposition.value == "ACCEPTED"
    assert tracker.decide(frame=correction, quote=correction_quote).disposition.value == "ACCEPTED_CORRECTION"
    same_sequence = _frame(sequence=2, last_price="10.03")
    same_sequence_quote = normalize_raw_quote_frame(same_sequence, clock_trade_date=context.clock.clock_trade_date, board="MAIN", depth_quantity_unit=DepthQuantityUnit.SHARES, unit_evidence_version="unit-v1", tradability=_tradability())
    assert tracker.decide(frame=same_sequence, quote=same_sequence_quote).disposition.value == "OUT_OF_ORDER"
    assert first_quote.normalized_quote_sha256 != correction_quote.normalized_quote_sha256


def test_out_of_order_and_stale_generation_never_overwrite_latest_accepted() -> None:
    context = _context()
    tracker = QuoteOrderingTracker()
    tracker.activate_generation(2)
    stale = _frame(generation=1)
    stale_quote = normalize_raw_quote_frame(stale, clock_trade_date=context.clock.clock_trade_date, board="MAIN", depth_quantity_unit=DepthQuantityUnit.SHARES, unit_evidence_version="unit-v1", tradability=_tradability())
    assert tracker.decide(frame=stale, quote=stale_quote).disposition.value == "STALE_GENERATION"
    latest = _frame(generation=2, sequence=2, source_time="09300100")
    older = _frame(generation=2, sequence=3, source_time="09300000")
    latest_quote = normalize_raw_quote_frame(latest, clock_trade_date=context.clock.clock_trade_date, board="MAIN", depth_quantity_unit=DepthQuantityUnit.SHARES, unit_evidence_version="unit-v1", tradability=_tradability())
    older_quote = normalize_raw_quote_frame(older, clock_trade_date=context.clock.clock_trade_date, board="MAIN", depth_quantity_unit=DepthQuantityUnit.SHARES, unit_evidence_version="unit-v1", tradability=_tradability())
    assert tracker.decide(frame=latest, quote=latest_quote).accepted
    assert tracker.decide(frame=older, quote=older_quote).disposition.value == "OUT_OF_ORDER"


def test_eligibility_precedence_is_total_and_deterministic() -> None:
    context = _context()
    request = _request(policy=context.policy)
    evaluator = ActionQuoteEvaluator()
    waiting = evaluator.evaluate(request=request, context=context, observation=None)
    assert waiting.eligibility.state == EligibilityState.WAITING_FIRST_QUOTE
    invalid_clock = evaluator.evaluate(request=request, context=replace(context, continuity_valid=False), observation=_observation(context=context))
    assert invalid_clock.eligibility.state == EligibilityState.CLOCK_INVALID
    pre_open_context = _context(clock_at_utc=datetime(2026, 7, 12, 1, 16, tzinfo=UTC))
    assert evaluator.evaluate(request=_request(policy=pre_open_context.policy), context=pre_open_context, observation=None).eligibility.state == EligibilityState.WRONG_SESSION
    wrong_session = evaluator.evaluate(
        request=_request(policy=pre_open_context.policy),
        context=pre_open_context,
        observation=_observation(context=pre_open_context, frame=_frame(openint="PRE_OPEN")),
    )
    assert wrong_session.eligibility.state == EligibilityState.WRONG_SESSION
    mismatch = _observation(context=context, frame=_frame(openint="CLOSED"))
    assert evaluator.evaluate(request=request, context=context, observation=mismatch).eligibility.reason_code == QuoteContractReasonCode.MARKET_PHASE_MISMATCH
    domain_conflict = replace(mismatch, quote=replace(mismatch.quote, clock_domain_id="foreign-clock-domain"))
    assert evaluator.evaluate(request=request, context=context, observation=domain_conflict).eligibility.state == EligibilityState.CLOCK_INVALID


@pytest.mark.parametrize(("age_ms", "expected"), [(100, EligibilityState.READY), (101, EligibilityState.STALE)])
def test_freshness_threshold_boundaries_are_fail_closed_at_plus_one_ms(age_ms: int, expected: EligibilityState) -> None:
    policy = _policy(max_receive_age_ms=100, max_source_lag_ms=100, max_exchange_age_ms=100, max_clock_age_divergence_ms=1)
    context = _context(policy=policy)
    observation = _observation(context=context)
    quote = replace(
        observation.quote,
        received_at_utc=context.clock.clock_at_utc - timedelta(milliseconds=age_ms),
        received_monotonic_ns=context.clock.clock_monotonic_ns - age_ms * 1_000_000,
        source_exchange_time_utc=context.clock.clock_at_utc - timedelta(milliseconds=age_ms),
    )
    aged = replace(observation, quote=quote)
    result = ActionQuoteEvaluator().evaluate(request=_request(policy=policy), context=context, observation=aged)
    assert result.eligibility.state == expected


def test_negative_skew_is_preserved_before_any_divergence_absolute_value() -> None:
    context = _context(policy=_policy(max_negative_skew_ms=10))
    observation = _observation(context=context)
    future = replace(
        observation.quote,
        received_at_utc=context.clock.clock_at_utc + timedelta(milliseconds=11),
        received_monotonic_ns=context.clock.clock_monotonic_ns + 11_000_000,
        source_exchange_time_utc=context.clock.clock_at_utc + timedelta(milliseconds=11),
    )
    result = ActionQuoteEvaluator().evaluate(request=_request(policy=context.policy), context=context, observation=replace(observation, quote=future))
    assert result.eligibility.state == EligibilityState.STALE
    assert any("NEGATIVE_SKEW" in item for item in result.diagnostics)


@pytest.mark.parametrize(
    ("state", "side", "ask", "bid", "expected"),
    [
        (TradabilityState.DATA_INVALID, "BUY", "10.01", "9.99", EligibilityState.INVALID),
        (TradabilityState.SUSPENDED, "BUY", "10.01", "9.99", EligibilityState.SUSPENDED),
        (TradabilityState.INTRADAY_HALT, "SELL", "10.01", "9.99", EligibilityState.SUSPENDED),
        (TradabilityState.TRADABLE, "BUY", "11.00", "9.99", EligibilityState.LIMIT_BLOCKED),
        (TradabilityState.TRADABLE, "SELL", "10.01", "9.00", EligibilityState.LIMIT_BLOCKED),
    ],
)
def test_tradability_distinguishes_data_invalid_suspend_halt_limit_and_zero_depth(
    state: TradabilityState, side: str, ask: str, bid: str, expected: EligibilityState
) -> None:
    context = _context(tradability=_tradability(state=state))
    frame = _frame()
    quote = normalize_raw_quote_frame(frame, clock_trade_date=context.clock.clock_trade_date, board="MAIN", depth_quantity_unit=DepthQuantityUnit.SHARES, unit_evidence_version="unit-v1", tradability=_tradability(state=state))
    quote = replace(
        quote,
        ask_prices=(Decimal(ask), Decimal(ask) + Decimal("0.01"), None, None, None),
        bid_prices=(Decimal(bid), Decimal(bid) - Decimal("0.01"), None, None, None),
    )
    tradability = context.symbol_context("000001.SZ").tradability  # type: ignore[union-attr]
    observation = NormalizedQuoteObservation(frame=frame, quote=quote, tradability=tradability, context_id=context.context_id, market_data_id=f"md_{canonical_sha256({'case': state.value, 'side': side})}", ordering_disposition=OrderingDisposition.ACCEPTED)
    result = ActionQuoteEvaluator().evaluate(request=_request(side=side, policy=context.policy), context=context, observation=observation)
    assert result.eligibility.state == expected


def test_zero_opposite_depth_is_not_reclassified_as_a_data_error() -> None:
    context = _context()
    observation = _observation(context=context)
    empty_ask = replace(
        observation.quote,
        ask_prices=(None, None, None, None, None),
        ask_quantities=(0, 0, 0, 0, 0),
        ask_quantities_raw=(Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")),
    )
    result = ActionQuoteEvaluator().evaluate(
        request=_request(policy=context.policy), context=context, observation=replace(observation, quote=empty_ask)
    )
    assert result.eligibility.state == EligibilityState.NO_OPPOSITE_DEPTH


def test_dependency_group_failure_does_not_block_unrelated_symbols() -> None:
    context = _context()
    second_tradability = replace(_tradability(), symbol="000002.SZ", tradability_id="tradability-000002.SZ")
    second_symbol = QuoteSymbolContext(symbol="000002.SZ", board="MAIN", depth_quantity_unit=DepthQuantityUnit.SHARES, unit_evidence_version="unit-v1", tradability=second_tradability, product_type="EQUITY", product_type_proven_equity=True, authority_source_version="authority-v1")
    context = replace(context, symbols={**context.symbols, "000002.SZ": second_symbol})
    first = _observation(context=context)
    requests = {
        "000001.SZ": _request(group="parent-group", policy=context.policy),
        "000002.SZ": _request(symbol="000002.SZ", group="parent-group", policy=context.policy),
    }
    batch = build_quote_snapshot_batch(batch_id="batch-1", runtime_id="runtime-1", context=context, requests=requests, observations={"000001.SZ": first})
    assert batch.eligibility_by_symbol["000001.SZ"].state == EligibilityState.STALE
    assert batch.eligibility_by_symbol["000002.SZ"].state == EligibilityState.WAITING_FIRST_QUOTE

    independent = build_quote_snapshot_batch(
        batch_id="batch-2",
        runtime_id="runtime-1",
        context=context,
        requests={"000001.SZ": _request(policy=context.policy)},
        observations={"000001.SZ": first},
    )
    assert independent.eligibility_by_symbol["000001.SZ"].state == EligibilityState.READY


def test_batch_aggregate_is_observation_only_not_runtime_gate() -> None:
    context = _context()
    batch = build_quote_snapshot_batch(
        batch_id="batch-observe",
        runtime_id="runtime-1",
        context=context,
        requests={"000001.SZ": _request(policy=context.policy)},
        observations={"000001.SZ": _observation(context=context)},
    )
    assert batch.aggregate_state.value == "OBSERVED"
    assert batch.eligibility_by_symbol["000001.SZ"].state == EligibilityState.READY


def test_empty_batch_requires_and_preserves_authoritative_runtime_identity() -> None:
    context = _context()

    batch = build_quote_snapshot_batch(
        batch_id="batch-empty",
        runtime_id="runtime-empty-authority",
        context=context,
        requests={},
        observations={},
    )

    assert batch.runtime_id == "runtime-empty-authority"
    assert batch.aggregate_state.value == "NO_ACTIVE_SYMBOLS"


def test_batch_rejects_request_from_a_different_runtime() -> None:
    context = _context()

    with pytest.raises(QuoteContractError) as exc_info:
        build_quote_snapshot_batch(
            batch_id="batch-runtime-conflict",
            runtime_id="runtime-authority",
            context=context,
            requests={"000001.SZ": _request(policy=context.policy)},
            observations={},
        )

    assert exc_info.value.reason_code == QuoteContractReasonCode.PAYLOAD_INVALID


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "expected_reason"),
    (
        ("product_type_proven_equity", "false", QuoteContractReasonCode.TRADABILITY_DATA_INVALID),
        ("continuity_valid", "false", QuoteContractReasonCode.CLOCK_CALENDAR_INVALID),
        ("continuity_generation", True, QuoteContractReasonCode.CLOCK_CALENDAR_INVALID),
    ),
)
def test_context_security_fields_reject_truthy_non_boolean_or_boolean_integer(
    field_name: str,
    invalid_value: object,
    expected_reason: QuoteContractReasonCode,
) -> None:
    context = _context()

    with pytest.raises(QuoteContractError) as exc_info:
        if field_name == "product_type_proven_equity":
            symbol_context = context.symbol_context("000001.SZ")
            assert symbol_context is not None
            replace(symbol_context, product_type_proven_equity=invalid_value)  # type: ignore[arg-type]
        else:
            replace(context, **{field_name: invalid_value})

    assert exc_info.value.reason_code == expected_reason


def test_normalized_observation_rejects_unknown_ordering_disposition() -> None:
    context = _context()
    observation = _observation(context=context)

    with pytest.raises(QuoteContractError) as exc_info:
        replace(observation, ordering_disposition="SILENTLY_ACCEPT")  # type: ignore[arg-type]

    assert exc_info.value.reason_code == QuoteContractReasonCode.ORDERING_REJECTED
