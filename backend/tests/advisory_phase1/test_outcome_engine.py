"""Batch A frozen-input outcome-engine fixture contracts."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from backend.services.advisory_phase1.label_policy import (
    BarrierPolicy,
    BenchmarkPolicy,
    CashReturnPolicy,
    CashReturnRule,
    CostPolicy,
    EntryBasis,
    EntryExecutionPolicy,
    ExitBasis,
    LabelPolicyBundle,
    MarketDataPolicy,
    OutcomePolicySet,
    Projection,
    StyleFamily,
    TerminalPolicy,
    TradingCalendar,
)
from backend.services.advisory_phase1.outcome_engine import (
    BarrierStatus,
    BenchmarkLeg,
    BenchmarkPortfolio,
    CorporateActionEffect,
    DailyPriceBar,
    EntryStatus,
    FrozenEqualWeight,
    MaturityStatus,
    MissingSourceReceipt,
    OutcomeCalculationRequest,
    OutcomeContractError,
    OutcomeEngine,
    OutcomeEventStatus,
    OutcomeOwner,
    OwnerType,
    PricePath,
    SourceMemberBinding,
    TerminalDisposition,
    TerminalResolution,
)
from backend.services.advisory_phase1.source_ledger import (
    InMemorySourceAvailabilityLedger,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
)
from backend.services.advisory_phase1.source_revision import (
    AvailabilityRequirement,
    SourceRevisionKind,
    SourceRevisionMemberInput,
    build_source_revision_set,
)


UTC = timezone.utc
AS_OF = datetime(2026, 7, 10, 9, 0, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64


def _source_revision_set():
    ledger = InMemorySourceAvailabilityLedger(now_provider=lambda: datetime(2026, 7, 2, 9, 0, tzinfo=UTC))
    event = ledger.append(
        SourceAvailabilityEventRequest(
            dataset_name="market.kline_daily_raw",
            source_role="PRICE_PATH",
            partition_key={"fixture": "outcome"},
            revision_id="fixture-r1",
            event_revision_no=1,
            event_type=SourceAvailabilityEventType.INGESTED,
            schema_fingerprint="fixture-schema-v1",
            row_count=4,
            partition_content_hash=HASH_A,
            quality_status="PASS",
            created_by_service_principal="fixture",
        )
    )
    source = event.input
    member = SourceRevisionMemberInput(
        source_role=source.source_role,
        dataset_name=source.dataset_name,
        query_template_id="fixture-price-v1",
        query_template_version="1",
        query_template_hash=HASH_B,
        bound_parameter_hash=HASH_C,
        enforced_cutoff_predicate_hash=HASH_D,
        partition_key=source.partition_key,
        revision_kind=SourceRevisionKind.IMMUTABLE_INGESTION,
        revision_id=source.revision_id,
        availability_requirement=AvailabilityRequirement.DECISION_CUTOFF,
        business_min_date=date(2026, 7, 3),
        business_max_date=date(2026, 7, 8),
        available_at_min=source.formal_available_at,
        available_at_max=source.formal_available_at,
        schema_fingerprint=source.schema_fingerprint,
        row_count=source.row_count,
        partition_content_hash=source.partition_content_hash,
        quality_status=source.quality_status,
        availability_event=event,
        research_only=True,
    )
    return build_source_revision_set(
        query_registry_hash=HASH_E,
        requested_source_cutoff=AS_OF,
        label_as_of_ts=AS_OF,
        research_only=True,
        members=[member],
    )


def _source_binding() -> SourceMemberBinding:
    member = _source_revision_set().members[0]
    return SourceMemberBinding(
        source_role=member.source_role,
        source_member_key=member.member_key,
        partition_content_hash=member.partition_content_hash,
    )


def _policies(
    *,
    exit_basis: ExitBasis = ExitBasis.HORIZON_CLOSE_V1,
    benchmark_notional: Decimal = Decimal("1000"),
) -> OutcomePolicySet:
    calendar = TradingCalendar(
        calendar_version="fixture-calendar-v1",
        trading_dates=(date(2026, 7, 3), date(2026, 7, 6), date(2026, 7, 7), date(2026, 7, 8)),
    )
    execution = EntryExecutionPolicy(
        policy_id="fixture-entry-v1",
        entry_basis=EntryBasis.NEXT_OPEN_EXECUTABLE_V1,
        exit_basis=exit_basis,
        entry_time=time(9, 30),
        exit_time=time(15, 0) if exit_basis is ExitBasis.HORIZON_CLOSE_V1 else time(9, 30),
    )
    cost = CostPolicy(
        policy_id="fixture-cost-v1",
        commission_buy_rate=Decimal("0.0003"),
        commission_sell_rate=Decimal("0.0003"),
        minimum_commission=Decimal("5"),
        stamp_duty_sell_rate=Decimal("0.0005"),
        transfer_fee_buy_rate=Decimal("0"),
        transfer_fee_sell_rate=Decimal("0"),
        slippage_bps=Decimal("5"),
        lot_size=10,
    )
    benchmark = BenchmarkPolicy(universe_layer="fixture-universe")
    cash_return = CashReturnPolicy(
        policy_id=CashReturnRule.CASH_RETURN_ZERO_V1,
        cash_return_rate=Decimal("0"),
    )
    barrier = BarrierPolicy(
        policy_id="fixture-barrier-v1",
        target_return=Decimal("0.10"),
        stop_return=Decimal("-0.10"),
    )
    terminal_policy = TerminalPolicy(
        policy_id="fixture-terminal-v1",
        terminal_return_rule="EXACT_SETTLEMENT_OR_UNAVAILABLE_V1",
        censor_rule="EXPLICIT_RIGHT_CENSOR_REASON_V1",
    )
    assert calendar.calendar_hash is not None
    assert execution.policy_hash is not None
    assert cost.policy_hash is not None
    assert benchmark.policy_hash is not None
    assert cash_return.policy_hash is not None
    assert barrier.policy_hash is not None
    assert terminal_policy.policy_hash is not None
    bundle = LabelPolicyBundle(
        label_policy_id="fixture-label-v1",
        label_policy_hash=HASH_B,
        label_policy_schema_version="fixture-label-schema-v1",
        phase1_handoff_bundle_hash=HASH_C,
        handoff_readiness_hash=HASH_D,
        admission_scope_id="fixture-scope",
        admission_scope_hash=HASH_E,
        audit_target_id="fixture-target",
        package_id="fixture-package",
        manifest_sha256=HASH_F,
        alpha_mode="single_alpha",
        style_family=StyleFamily.SHORT_REBOUND,
        style_assignment_policy_id="fixture-style-v1",
        style_assignment_policy_hash=HASH_A,
        style_decided_at=date(2026, 7, 3),
        calendar_version=calendar.calendar_version,
        calendar_hash=calendar.calendar_hash,
        price_policy_hash=HASH_B,
        adjustment_policy_hash=HASH_C,
        entry_execution_policy_hash=execution.policy_hash,
        cost_policy_hash=cost.policy_hash,
        benchmark_policy_hash=benchmark.policy_hash,
        cash_return_policy_hash=cash_return.policy_hash,
        terminal_return_policy_hash=terminal_policy.policy_hash,
        barrier_policy_hash=barrier.policy_hash,
        corporate_action_policy_hash=HASH_D,
        symbol_normalization_policy_hash=HASH_E,
        horizons=(1, 2),
        projections_by_horizon={
            1: (
                Projection.RETURN_GROSS,
                Projection.RETURN_NET_ABSOLUTE,
                Projection.RETURN_NET_EXCESS,
                Projection.PATH_MFE,
                Projection.PATH_MAE,
                Projection.EXECUTABLE_MFE,
                Projection.EXECUTABLE_MAE,
                Projection.BARRIER,
                Projection.SURVIVAL,
            ),
            2: (Projection.RETURN_GROSS, Projection.PATH_MFE, Projection.BARRIER, Projection.SURVIVAL),
        },
        gap_1d_enabled=True,
        candidate_reference_notional=Decimal("1000"),
        benchmark_portfolio_notional=benchmark_notional,
    )
    return OutcomePolicySet(
        bundle=bundle,
        calendar=calendar,
        market_data=MarketDataPolicy(
            price_policy_hash=HASH_B,
            adjustment_policy_hash=HASH_C,
            corporate_action_policy_hash=HASH_D,
            symbol_normalization_policy_hash=HASH_E,
        ),
        execution=execution,
        cost=cost,
        benchmark=benchmark,
        cash_return=cash_return,
        barrier=barrier,
        terminal=terminal_policy,
    )


def _bar(
    trade_date: date,
    *,
    open_li: str,
    high_li: str,
    low_li: str,
    close_li: str,
    entry_executable: bool = True,
    sell_executable: bool = True,
) -> DailyPriceBar:
    return DailyPriceBar(
        trade_date=trade_date,
        open_li=Decimal(open_li),
        high_li=Decimal(high_li),
        low_li=Decimal(low_li),
        close_li=Decimal(close_li),
        adj_factor=Decimal("1"),
        entry_executable=entry_executable,
        sell_executable=sell_executable,
        source_available_at=AS_OF - timedelta(hours=1),
        price_source=_source_binding(),
        adjustment_source=_source_binding(),
        tradability_source=_source_binding(),
    )


def _path(*, ambiguous_barrier: bool = False, entry_executable: bool = True) -> PricePath:
    s_high = "11500" if ambiguous_barrier else "11200"
    s_low = "8900" if ambiguous_barrier else "10000"
    return PricePath(
        symbol="000001.SZ",
        bars=(
            _bar(date(2026, 7, 3), open_li="10000", high_li="10100", low_li="9900", close_li="10000"),
            _bar(
                date(2026, 7, 6),
                open_li="10100",
                high_li="11200",
                low_li="10000",
                close_li="10500",
                entry_executable=entry_executable,
            ),
            _bar(date(2026, 7, 7), open_li="10600", high_li=s_high, low_li=s_low, close_li="11000"),
            _bar(date(2026, 7, 8), open_li="11000", high_li="11400", low_li="10800", close_li="11200"),
        )
    )


def _owner(owner_type: OwnerType = OwnerType.CANDIDATE) -> OutcomeOwner:
    if owner_type is OwnerType.CANDIDATE:
        return OutcomeOwner(
            owner_type=owner_type,
            owner_key="candidate-fixture",
            canonical_signal_id="advsig-fixture",
            observation_version_id="advobs-fixture",
            candidate_stage_evidence_id="advstage-fixture",
            symbol="000001.SZ",
            decision_as_of_trade_date=date(2026, 7, 3),
        )
    return OutcomeOwner(
        owner_type=owner_type,
        owner_key="universe-fixture",
        canonical_signal_id="advsig-fixture",
        symbol="000001.SZ",
        decision_as_of_trade_date=date(2026, 7, 3),
        universe_layer="fixture-universe",
    )


def _request(
    projection: Projection,
    *,
    owner_type: OwnerType = OwnerType.CANDIDATE,
    horizon: int = 1,
    path: PricePath | None = None,
    terminal: TerminalResolution | None = None,
    benchmark: BenchmarkPortfolio | None = None,
    actions: tuple[CorporateActionEffect, ...] = (),
    missing: tuple[MissingSourceReceipt, ...] = (),
    policies: OutcomePolicySet | None = None,
) -> OutcomeCalculationRequest:
    return OutcomeCalculationRequest(
        owner=_owner(owner_type),
        policies=policies or _policies(),
        horizon_trading_days=horizon,
        projection=projection,
        label_as_of_ts=AS_OF,
        label_source_revision_set=_source_revision_set(),
        price_path=path or _path(),
        corporate_actions=actions,
        terminal=terminal or TerminalResolution(disposition=TerminalDisposition.NONE),
        benchmark=benchmark,
        missing_source_receipts=missing,
    )


def _benchmark(path: PricePath | None = None) -> BenchmarkPortfolio:
    return BenchmarkPortfolio(
        universe_layer="fixture-universe",
        constituent_source=_source_binding(),
        legs=(
            BenchmarkLeg(
                symbol="000001.SZ",
                frozen_weight=FrozenEqualWeight(denominator=1),
                price_path=path or _path(),
                terminal=TerminalResolution(disposition=TerminalDisposition.NONE),
            ),
        ),
    )


def test_calendar_h1_is_t_plus_two_and_candidate_universe_share_gross_formula() -> None:
    engine = OutcomeEngine()
    candidate = engine.calculate(_request(Projection.RETURN_GROSS))
    universe = engine.calculate(_request(Projection.RETURN_GROSS, owner_type=OwnerType.UNIVERSE))

    assert candidate.intended_entry_trade_date == date(2026, 7, 6)
    assert candidate.earliest_sell_eligible_trade_date == date(2026, 7, 7)
    assert candidate.exit_trade_date == date(2026, 7, 7)
    assert candidate.maturity_status is MaturityStatus.MATURED
    assert candidate.projection_value_decimal == universe.projection_value_decimal
    assert candidate.projection_value_decimal == Decimal("11000") / Decimal("10100") - Decimal("1")


def test_gap_is_horizon_zero_and_uses_frozen_decision_close_to_entry_open() -> None:
    result = OutcomeEngine().calculate(_request(Projection.GAP_1D, horizon=0))

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.exit_trade_date is None
    assert result.projection_value_decimal == Decimal("10100") / Decimal("10000") - Decimal("1")


def test_gap_does_not_require_s_or_x_h_source_rows() -> None:
    path = PricePath(symbol="000001.SZ", bars=_path().bars[:2])

    result = OutcomeEngine().calculate(_request(Projection.GAP_1D, horizon=0, path=path))

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.projection_value_decimal == Decimal("10100") / Decimal("10000") - Decimal("1")


def test_exit_open_path_does_not_use_x_h_high_after_the_exit() -> None:
    result = OutcomeEngine().calculate(
        _request(
            Projection.PATH_MFE,
            horizon=2,
            policies=_policies(exit_basis=ExitBasis.HORIZON_OPEN_V1),
        )
    )

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.projection_value_decimal == Decimal("11200") / Decimal("10100") - Decimal("1")


def test_market_data_policy_hashes_must_match_the_frozen_bundle() -> None:
    policies = _policies()
    bad_market_data = policies.market_data.model_copy(update={"price_policy_hash": HASH_F})

    with pytest.raises(ValueError, match="price hash"):
        OutcomePolicySet.model_validate({**policies.model_dump(mode="python"), "market_data": bad_market_data})


def test_missing_cost_blocks_only_net_projection_and_never_substitutes_zero_cost() -> None:
    receipt = MissingSourceReceipt(
        source_role="COST",
        source_revision_set_hash=_source_revision_set().source_revision_set_hash,
        failure_observed_at=AS_OF - timedelta(minutes=1),
        reason_code="COST_SOURCE_MISSING",
    )
    gross = OutcomeEngine().calculate(_request(Projection.RETURN_GROSS, missing=(receipt,)))
    net = OutcomeEngine().calculate(_request(Projection.RETURN_NET_ABSOLUTE, missing=(receipt,)))

    assert gross.maturity_status is MaturityStatus.MATURED
    assert net.maturity_status is MaturityStatus.UNAVAILABLE
    assert net.projection_value_decimal is None
    assert "ADVISORY_PHASE1C3_COST_UNAVAILABLE" in net.reason_codes


def test_fixed_capital_cashflow_uses_lot_and_minimum_commission() -> None:
    result = OutcomeEngine().calculate(_request(Projection.RETURN_NET_ABSOLUTE))

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.cashflow is not None
    assert result.cashflow.entry_quantity == Decimal("90")
    assert result.cashflow.buy_fee_yuan == Decimal("5.000000")
    assert result.cashflow.residual_cash_yuan > Decimal("0")
    assert result.projection_value_decimal is not None


def test_net_excess_uses_frozen_benchmark_without_reweighting() -> None:
    result = OutcomeEngine().calculate(_request(Projection.RETURN_NET_EXCESS, benchmark=_benchmark()))

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.benchmark_net_total_return is not None
    assert result.projection_value_decimal is not None
    assert result.projection_value_decimal != result.cashflow.terminal_value_yuan / Decimal("1000") - Decimal("1")


def test_e_day_touch_and_same_sellable_bar_double_touch_is_unavailable_not_target_or_stop_first() -> None:
    result = OutcomeEngine().calculate(_request(Projection.BARRIER, path=_path(ambiguous_barrier=True)))

    assert result.maturity_status is MaturityStatus.UNAVAILABLE
    assert result.barrier is not None
    assert result.barrier.entry_day_touch_status is BarrierStatus.PATH_TOUCH_NOT_SELLABLE
    assert result.barrier.executable_status is BarrierStatus.ORDER_AMBIGUOUS
    assert result.projection_event_code is None
    assert "ADVISORY_PHASE1C3_BARRIER_ORDER_AMBIGUOUS" in result.reason_codes


def test_early_barrier_hit_does_not_require_later_horizon_path_source() -> None:
    bars = list(_path().bars)
    late_bar = bars[-1].model_dump(mode="python", exclude={"source_hash"})
    late_bar["source_available_at"] = AS_OF + timedelta(days=1)
    bars[-1] = DailyPriceBar.model_validate(late_bar)
    request = _request(
        Projection.BARRIER,
        horizon=2,
        path=PricePath(symbol="000001.SZ", bars=tuple(bars)),
    )

    result = OutcomeEngine().calculate(request)

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.outcome_event_status is OutcomeEventStatus.BARRIER
    assert result.barrier is not None
    assert result.barrier.executable_event_trade_date == date(2026, 7, 7)
    assert result.source_closed_at == _path().bars[2].source_available_at


def test_terminal_censor_is_not_a_fixed_return_and_survival_carries_observed_days() -> None:
    terminal = TerminalResolution(
        disposition=TerminalDisposition.RIGHT_CENSORED,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 7),
        event_closed_at=AS_OF - timedelta(hours=1),
        source=_source_binding(),
        censor_reason_code="LONG_SUSPENSION",
    )
    survival = OutcomeEngine().calculate(_request(Projection.SURVIVAL, terminal=terminal))
    gross = OutcomeEngine().calculate(_request(Projection.RETURN_GROSS, terminal=terminal))

    assert survival.maturity_status is MaturityStatus.RIGHT_CENSORED
    assert survival.projection_value_decimal == Decimal("1")
    assert gross.maturity_status is MaturityStatus.RIGHT_CENSORED
    assert gross.projection_value_decimal is None


def test_right_censor_does_not_require_a_future_x_h_quote() -> None:
    terminal = TerminalResolution(
        disposition=TerminalDisposition.RIGHT_CENSORED,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 7),
        event_closed_at=AS_OF - timedelta(hours=1),
        source=_source_binding(),
        censor_reason_code="LONG_SUSPENSION",
    )
    path = PricePath(symbol="000001.SZ", bars=_path().bars[:2])

    result = OutcomeEngine().calculate(_request(Projection.SURVIVAL, path=path, terminal=terminal))

    assert result.maturity_status is MaturityStatus.RIGHT_CENSORED
    assert result.projection_value_decimal == Decimal("1")


def test_terminal_benchmark_leg_uses_frozen_terminal_settlement_path() -> None:
    terminal = TerminalResolution(
        disposition=TerminalDisposition.TERMINAL,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 7),
        event_closed_at=AS_OF - timedelta(hours=1),
        source=_source_binding(),
        settlement_raw_li=Decimal("11000"),
        settlement_adj_factor=Decimal("1"),
        settlement_quantity_multiplier=Decimal("1"),
        settlement_cashflow_yuan_per_share=Decimal("0"),
    )
    benchmark = BenchmarkPortfolio(
        universe_layer="fixture-universe",
        constituent_source=_source_binding(),
        legs=(
            BenchmarkLeg(
                symbol="000001.SZ",
                frozen_weight=FrozenEqualWeight(denominator=1),
                price_path=_path(),
                terminal=terminal,
            ),
        ),
    )

    result = OutcomeEngine().calculate(_request(Projection.RETURN_NET_EXCESS, benchmark=benchmark))

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.benchmark_net_total_return is not None


def test_known_non_executable_entry_returns_evidenced_unavailable_not_pending_or_zero() -> None:
    result = OutcomeEngine().calculate(_request(Projection.RETURN_GROSS, path=_path(entry_executable=False)))

    assert result.maturity_status is MaturityStatus.UNAVAILABLE
    assert result.entry_status is EntryStatus.NOT_EXECUTABLE
    assert result.projection_value_decimal is None


def test_rights_needing_external_capital_is_evidenced_unavailable() -> None:
    action = CorporateActionEffect(
        symbol="000001.SZ",
        effective_trade_date=date(2026, 7, 7),
        quantity_multiplier=Decimal("1"),
        cashflow_yuan_per_share=Decimal("0"),
        rights_subscription_cash_required_yuan_per_share=Decimal("1000"),
        source_available_at=AS_OF - timedelta(hours=1),
        source=_source_binding(),
    )
    request = _request(Projection.RETURN_NET_ABSOLUTE, actions=(action,))

    result = OutcomeEngine().calculate(request)

    assert result.maturity_status is MaturityStatus.UNAVAILABLE
    assert result.entry_status is EntryStatus.EXECUTABLE
    assert "ADVISORY_PHASE1C3_COST_UNAVAILABLE" in result.reason_codes


def test_corporate_action_cashflow_and_quantity_are_applied_per_current_share() -> None:
    action = CorporateActionEffect(
        symbol="000001.SZ",
        effective_trade_date=date(2026, 7, 7),
        quantity_multiplier=Decimal("2"),
        cashflow_yuan_per_share=Decimal("1"),
        rights_subscription_cash_required_yuan_per_share=Decimal("0"),
        source_available_at=AS_OF - timedelta(hours=1),
        source=_source_binding(),
    )

    result = OutcomeEngine().calculate(
        _request(Projection.RETURN_NET_ABSOLUTE, actions=(action,))
    )

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.cashflow is not None
    assert result.cashflow.entry_quantity == Decimal("90")
    assert result.cashflow.exit_quantity == Decimal("180")
    assert result.cashflow.exit_cash_yuan > result.cashflow.sell_notional_yuan


def test_terminal_missing_settlement_keeps_terminal_event_and_returns_unavailable() -> None:
    terminal = TerminalResolution(
        disposition=TerminalDisposition.TERMINAL,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 7),
        event_closed_at=AS_OF - timedelta(hours=1),
        source=_source_binding(),
    )
    receipt = MissingSourceReceipt(
        source_role="TERMINAL_SETTLEMENT",
        source_revision_set_hash=_source_revision_set().source_revision_set_hash,
        failure_observed_at=AS_OF - timedelta(minutes=1),
        reason_code="SETTLEMENT_MISSING",
    )

    result = OutcomeEngine().calculate(_request(Projection.RETURN_GROSS, terminal=terminal, missing=(receipt,)))

    assert result.maturity_status is MaturityStatus.UNAVAILABLE
    assert result.outcome_event_status is OutcomeEventStatus.TERMINAL
    assert result.event_closed_at == terminal.event_closed_at


def test_policy_content_cannot_change_under_the_same_hash() -> None:
    policies = _policies()
    payload = policies.model_dump(mode="python")
    payload["cost"]["commission_buy_rate"] = Decimal("0.25")

    with pytest.raises(ValueError, match="policy_hash does not match canonical policy content"):
        OutcomePolicySet.model_validate(payload)


def test_engine_revalidates_unsafe_model_copy_before_calculation() -> None:
    request = _request(Projection.RETURN_NET_ABSOLUTE)
    changed_cost = request.policies.cost.model_copy(update={"commission_buy_rate": Decimal("0.25")})
    changed_policies = request.policies.model_copy(update={"cost": changed_cost})
    changed_request = request.model_copy(update={"policies": changed_policies})

    with pytest.raises(OutcomeContractError, match="canonical revalidation"):
        OutcomeEngine().calculate(changed_request)


def test_calendar_dates_cannot_change_under_the_same_hash() -> None:
    calendar = _policies().calendar
    payload = calendar.model_dump(mode="python")
    payload["trading_dates"] = (
        date(2026, 7, 3),
        date(2026, 7, 6),
        date(2026, 7, 8),
        date(2026, 7, 9),
    )

    with pytest.raises(ValueError, match="calendar_hash does not match canonical policy content"):
        TradingCalendar.model_validate(payload)


def test_price_source_binding_must_belong_to_the_declared_revision_set() -> None:
    request = _request(Projection.RETURN_GROSS)
    bar_payload = request.price_path.bars[0].model_dump(mode="python", exclude={"source_hash"})
    bad_binding = bar_payload["price_source"].copy()
    bad_binding["source_member_key"] = HASH_F
    bar_payload["price_source"] = bad_binding
    bars = (DailyPriceBar.model_validate(bar_payload), *request.price_path.bars[1:])
    request_payload = request.model_dump(mode="python")
    request_payload["price_path"] = PricePath(symbol=request.owner.symbol, bars=bars).model_dump(mode="python")

    with pytest.raises(ValueError, match="absent from the frozen source revision set"):
        OutcomeCalculationRequest.model_validate(request_payload)


def test_engine_rejects_source_revision_content_drift_under_the_same_set_hash() -> None:
    request = _request(Projection.RETURN_GROSS)
    changed_source_set = request.label_source_revision_set.model_copy(
        update={"requested_source_cutoff": AS_OF - timedelta(days=1)}
    )
    changed_request = request.model_copy(update={"label_source_revision_set": changed_source_set})

    with pytest.raises(OutcomeContractError, match="source revision set hash"):
        OutcomeEngine().calculate(changed_request)


def test_owner_symbol_must_match_the_price_path() -> None:
    request = _request(Projection.RETURN_GROSS)
    payload = request.model_dump(mode="python")
    payload["price_path"]["symbol"] = "000002.SZ"

    with pytest.raises(ValueError, match="owner symbol"):
        OutcomeCalculationRequest.model_validate(payload)


def test_partial_terminal_settlement_never_defaults_quantity_or_cashflow() -> None:
    terminal = TerminalResolution(
        disposition=TerminalDisposition.TERMINAL,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 7),
        event_closed_at=AS_OF - timedelta(hours=1),
        source=_source_binding(),
        settlement_raw_li=Decimal("11000"),
        settlement_adj_factor=Decimal("1"),
    )
    result = OutcomeEngine().calculate(_request(Projection.RETURN_NET_ABSOLUTE, terminal=terminal))

    assert result.maturity_status is MaturityStatus.UNAVAILABLE
    assert result.cashflow is None
    assert result.outcome_event_status is OutcomeEventStatus.TERMINAL


def test_terminal_event_after_frozen_horizon_is_rejected() -> None:
    terminal = TerminalResolution(
        disposition=TerminalDisposition.TERMINAL,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 8),
        event_closed_at=AS_OF - timedelta(hours=1),
        source=_source_binding(),
        settlement_raw_li=Decimal("9000"),
        settlement_adj_factor=Decimal("1"),
        settlement_quantity_multiplier=Decimal("1"),
        settlement_cashflow_yuan_per_share=Decimal("0"),
    )

    with pytest.raises(ValueError, match="frozen horizon exit"):
        _request(Projection.RETURN_GROSS, horizon=1, terminal=terminal)


def test_known_terminal_precedes_same_day_barrier_evaluation() -> None:
    terminal = TerminalResolution(
        disposition=TerminalDisposition.TERMINAL,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 7),
        event_closed_at=AS_OF - timedelta(hours=1),
        source=_source_binding(),
        settlement_raw_li=Decimal("10000"),
        settlement_adj_factor=Decimal("1"),
        settlement_quantity_multiplier=Decimal("1"),
        settlement_cashflow_yuan_per_share=Decimal("0"),
    )

    result = OutcomeEngine().calculate(_request(Projection.BARRIER, terminal=terminal))

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.outcome_event_status is OutcomeEventStatus.TERMINAL
    assert result.projection_event_code == "TERMINAL"


def test_unbuyable_benchmark_allocation_remains_cash() -> None:
    result = OutcomeEngine().calculate(
        _request(
            Projection.RETURN_NET_EXCESS,
            benchmark=_benchmark(_path(entry_executable=False)),
        )
    )

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.benchmark_net_total_return == Decimal("0")


def test_zero_lot_benchmark_allocation_remains_cash() -> None:
    result = OutcomeEngine().calculate(
        _request(
            Projection.RETURN_NET_EXCESS,
            benchmark=_benchmark(),
            policies=_policies(benchmark_notional=Decimal("50")),
        )
    )

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.benchmark_net_total_return == Decimal("0")


def test_benchmark_weights_must_be_exact_equal_weight_rationals() -> None:
    with pytest.raises(ValueError, match="exact frozen 1/N equal weights"):
        BenchmarkPortfolio(
            universe_layer="fixture-universe",
            constituent_source=_source_binding(),
            legs=(
                BenchmarkLeg(
                    symbol="000001.SZ",
                    frozen_weight=FrozenEqualWeight(denominator=2),
                    price_path=_path(),
                    terminal=TerminalResolution(disposition=TerminalDisposition.NONE),
                ),
            ),
        )


def test_calendar_contract_negative_matrix() -> None:
    with pytest.raises(ValueError, match="sorted"):
        TradingCalendar(
            calendar_version="bad-order",
            trading_dates=(date(2026, 7, 6), date(2026, 7, 3), date(2026, 7, 7)),
        )
    with pytest.raises(ValueError, match="unique"):
        TradingCalendar(
            calendar_version="duplicate",
            trading_dates=(date(2026, 7, 3), date(2026, 7, 3), date(2026, 7, 6)),
        )

    calendar = _policies().calendar
    with pytest.raises(ValueError, match="absent"):
        calendar.next_trading_day(date(2026, 7, 2))
    with pytest.raises(ValueError, match="next trading day"):
        calendar.next_trading_day(date(2026, 7, 8))
    with pytest.raises(ValueError, match="at least one"):
        calendar.shift_from_entry(date(2026, 7, 6), 0)
    with pytest.raises(ValueError, match="requested horizon"):
        calendar.shift_from_entry(date(2026, 7, 7), 3)
    with pytest.raises(ValueError, match="path boundary"):
        calendar.trading_days_inclusive(date(2026, 7, 2), date(2026, 7, 7))
    with pytest.raises(ValueError, match="precedes"):
        calendar.trading_days_inclusive(date(2026, 7, 7), date(2026, 7, 6))


def test_policy_contract_negative_matrix() -> None:
    with pytest.raises(ValueError, match="09:30"):
        EntryExecutionPolicy(
            policy_id="bad-entry-time",
            entry_basis=EntryBasis.NEXT_OPEN_EXECUTABLE_V1,
            exit_basis=ExitBasis.HORIZON_CLOSE_V1,
            entry_time=time(10, 0),
            exit_time=time(15, 0),
        )
    with pytest.raises(ValueError, match="exit timestamp"):
        EntryExecutionPolicy(
            policy_id="bad-exit-time",
            entry_basis=EntryBasis.NEXT_OPEN_EXECUTABLE_V1,
            exit_basis=ExitBasis.HORIZON_OPEN_V1,
            entry_time=time(9, 30),
            exit_time=time(15, 0),
        )
    with pytest.raises(ValueError, match="fixed rounding"):
        CostPolicy(
            policy_id="bad-rounding",
            commission_buy_rate=Decimal("0"),
            commission_sell_rate=Decimal("0"),
            minimum_commission=Decimal("0"),
            stamp_duty_sell_rate=Decimal("0"),
            transfer_fee_buy_rate=Decimal("0"),
            transfer_fee_sell_rate=Decimal("0"),
            slippage_bps=Decimal("0"),
            lot_size=100,
            quantity_rounding="ROUND_NEAREST",
        )
    with pytest.raises(ValueError, match="PIT eligible equal weight"):
        BenchmarkPolicy(policy_id="OTHER", universe_layer="fixture-universe")
    with pytest.raises(ValueError, match="explicit zero"):
        CashReturnPolicy(
            policy_id=CashReturnRule.CASH_RETURN_ZERO_V1,
            cash_return_rate=Decimal("0.01"),
        )
    with pytest.raises(ValueError, match="target-first"):
        BarrierPolicy(
            policy_id="bad-order",
            target_return=Decimal("0.1"),
            stop_return=Decimal("-0.1"),
            order_policy="TARGET_FIRST",
        )
    with pytest.raises(ValueError, match="terminal return rule"):
        TerminalPolicy(
            policy_id="bad-terminal",
            terminal_return_rule="DEFAULT_ZERO_RETURN",
            censor_rule="EXPLICIT_RIGHT_CENSOR_REASON_V1",
        )


def test_policy_bundle_negative_matrix() -> None:
    bundle = _policies().bundle.model_dump(
        mode="python",
        exclude={"label_policy_bundle_id", "label_policy_bundle_hash"},
    )

    bad_horizons = dict(bundle)
    bad_horizons["horizons"] = (2, 1)
    with pytest.raises(ValueError, match="sorted, unique"):
        LabelPolicyBundle.model_validate(bad_horizons)

    bad_projection_keys = dict(bundle)
    bad_projection_keys["projections_by_horizon"] = {1: (Projection.RETURN_GROSS,)}
    with pytest.raises(ValueError, match="keys must exactly match"):
        LabelPolicyBundle.model_validate(bad_projection_keys)

    execution_enabled = dict(bundle)
    execution_enabled["execution_prohibited"] = False
    with pytest.raises(ValueError, match="execution prohibited"):
        LabelPolicyBundle.model_validate(execution_enabled)


def test_input_model_negative_matrix() -> None:
    with pytest.raises(ValueError, match="OHLC"):
        DailyPriceBar(
            trade_date=date(2026, 7, 6),
            open_li=Decimal("10000"),
            high_li=Decimal("9000"),
            low_li=Decimal("8000"),
            close_li=Decimal("10000"),
            adj_factor=Decimal("1"),
            entry_executable=True,
            sell_executable=True,
            source_available_at=AS_OF,
            price_source=_source_binding(),
            adjustment_source=_source_binding(),
            tradability_source=_source_binding(),
        )
    with pytest.raises(ValueError, match="unique and sorted"):
        PricePath(symbol="000001.SZ", bars=tuple(reversed(_path().bars)))
    with pytest.raises(ValueError, match="non-terminal"):
        TerminalResolution(
            disposition=TerminalDisposition.NONE,
            symbol="000001.SZ",
        )
    with pytest.raises(ValueError, match="censor reason"):
        TerminalResolution(
            disposition=TerminalDisposition.RIGHT_CENSORED,
            symbol="000001.SZ",
            event_trade_date=date(2026, 7, 7),
            event_closed_at=AS_OF,
            source=_source_binding(),
        )
    with pytest.raises(ValueError, match="cannot carry terminal settlement"):
        TerminalResolution(
            disposition=TerminalDisposition.RIGHT_CENSORED,
            symbol="000001.SZ",
            event_trade_date=date(2026, 7, 7),
            event_closed_at=AS_OF,
            source=_source_binding(),
            censor_reason_code="LONG_SUSPENSION",
            settlement_raw_li=Decimal("10000"),
        )


def test_owner_and_evidence_negative_matrix() -> None:
    with pytest.raises(ValueError, match="retrospective research"):
        OutcomeOwner(
            owner_type=OwnerType.CANDIDATE,
            owner_key="bad-scope",
            canonical_signal_id="signal",
            observation_version_id="observation",
            candidate_stage_evidence_id="stage",
            symbol="000001.SZ",
            decision_as_of_trade_date=date(2026, 7, 3),
            evidence_scope="EXECUTION",
        )
    with pytest.raises(ValueError, match="requires observation"):
        OutcomeOwner(
            owner_type=OwnerType.CANDIDATE,
            owner_key="missing-candidate-evidence",
            canonical_signal_id="signal",
            symbol="000001.SZ",
            decision_as_of_trade_date=date(2026, 7, 3),
        )
    with pytest.raises(ValueError, match="cannot carry a universe layer"):
        OutcomeOwner(
            owner_type=OwnerType.CANDIDATE,
            owner_key="candidate-layer",
            canonical_signal_id="signal",
            observation_version_id="observation",
            candidate_stage_evidence_id="stage",
            symbol="000001.SZ",
            decision_as_of_trade_date=date(2026, 7, 3),
            universe_layer="unexpected",
        )
    with pytest.raises(ValueError, match="cannot carry candidate"):
        OutcomeOwner(
            owner_type=OwnerType.UNIVERSE,
            owner_key="universe-candidate-evidence",
            canonical_signal_id="signal",
            observation_version_id="observation",
            symbol="000001.SZ",
            decision_as_of_trade_date=date(2026, 7, 3),
            universe_layer="fixture-universe",
        )
    with pytest.raises(ValueError, match="requires its frozen universe layer"):
        OutcomeOwner(
            owner_type=OwnerType.UNIVERSE,
            owner_key="universe-no-layer",
            canonical_signal_id="signal",
            symbol="000001.SZ",
            decision_as_of_trade_date=date(2026, 7, 3),
        )
    with pytest.raises(ValueError, match="lowercase sha256"):
        SourceMemberBinding(
            source_role="PRICE_PATH",
            source_member_key="G" * 64,
            partition_content_hash=HASH_A,
        )
    with pytest.raises(ValueError, match="explicit timezone"):
        MissingSourceReceipt(
            source_role="ENTRY_QUOTE",
            source_revision_set_hash=_source_revision_set().source_revision_set_hash,
            failure_observed_at=datetime(2026, 7, 10, 9, 0),
            reason_code="MISSING",
        )
    with pytest.raises(ValueError, match="numerator must be one"):
        FrozenEqualWeight(numerator=2, denominator=2)


def test_request_closure_negative_matrix() -> None:
    source_set_hash = _source_revision_set().source_revision_set_hash
    first = MissingSourceReceipt(
        source_role="ENTRY_QUOTE",
        source_revision_set_hash=source_set_hash,
        failure_observed_at=AS_OF,
        reason_code="MISSING_FIRST",
    )
    second = MissingSourceReceipt(
        source_role="ENTRY_QUOTE",
        source_revision_set_hash=source_set_hash,
        failure_observed_at=AS_OF,
        reason_code="MISSING_SECOND",
    )
    with pytest.raises(ValueError, match="roles must be unique"):
        _request(Projection.RETURN_GROSS, missing=(first, second))

    wrong_source_receipt = MissingSourceReceipt(
        source_role="ENTRY_QUOTE",
        source_revision_set_hash=HASH_F,
        failure_observed_at=AS_OF,
        reason_code="WRONG_SET",
    )
    with pytest.raises(ValueError, match="does not belong"):
        _request(Projection.RETURN_GROSS, missing=(wrong_source_receipt,))

    benchmark_payload = _benchmark().model_dump(
        mode="python",
        exclude={"constituent_hash"},
    )
    benchmark_payload["universe_layer"] = "wrong-layer"
    wrong_layer = BenchmarkPortfolio.model_validate(benchmark_payload)
    with pytest.raises(ValueError, match="universe layer"):
        _request(Projection.RETURN_NET_EXCESS, benchmark=wrong_layer)

    future_terminal = TerminalResolution(
        disposition=TerminalDisposition.TERMINAL,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 7),
        event_closed_at=AS_OF + timedelta(hours=1),
        source=_source_binding(),
        settlement_raw_li=Decimal("10000"),
        settlement_adj_factor=Decimal("1"),
        settlement_quantity_multiplier=Decimal("1"),
        settlement_cashflow_yuan_per_share=Decimal("0"),
    )
    with pytest.raises(ValueError, match="after label as-of"):
        _request(Projection.RETURN_GROSS, terminal=future_terminal)


@pytest.mark.parametrize(
    "projection",
    (
        Projection.PATH_MFE,
        Projection.PATH_MAE,
        Projection.EXECUTABLE_MFE,
        Projection.EXECUTABLE_MAE,
        Projection.SURVIVAL,
    ),
)
def test_remaining_numeric_projection_paths_mature(projection: Projection) -> None:
    result = OutcomeEngine().calculate(_request(projection))

    assert result.maturity_status is MaturityStatus.MATURED
    assert result.projection_value_decimal is not None


def test_barrier_no_hit_and_stop_paths_are_distinct() -> None:
    no_hit = PricePath(
        symbol="000001.SZ",
        bars=(
            *_path().bars[:2],
            _bar(date(2026, 7, 7), open_li="10500", high_li="10900", low_li="10000", close_li="10600"),
            _path().bars[3],
        ),
    )
    stop = PricePath(
        symbol="000001.SZ",
        bars=(
            *_path().bars[:2],
            _bar(date(2026, 7, 7), open_li="10000", high_li="10500", low_li="8500", close_li="9000"),
            _path().bars[3],
        ),
    )

    no_hit_result = OutcomeEngine().calculate(_request(Projection.BARRIER, path=no_hit))
    stop_result = OutcomeEngine().calculate(_request(Projection.BARRIER, path=stop))

    assert no_hit_result.projection_event_code == "NO_HIT"
    assert stop_result.projection_event_code == BarrierStatus.HIT_STOP.value
