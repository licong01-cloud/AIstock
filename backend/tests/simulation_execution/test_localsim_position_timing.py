"""LocalSIM economic oracles; synthetic inputs are functional evidence only."""

from datetime import timedelta
from decimal import Decimal

import pytest

from backend.services.position_timing.intraday.contracts import (
    Calendar, DayFacts, MarketView, MinuteBar, Plan, Policy, PriceConstraint,
    PriceInterval, digest, utc,
)
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_execution.localsim.runtime import LocalSimBackend
from backend.services.simulation_execution.localsim.timing import (
    CLOSED_LOOP_MODE, policy_snapshot,
)
from backend.services.trading_core.models import PositionLot


def at(clock="09:35:10"):
    return utc("2026-09-21T" + clock + "+08:00")


def inputs(rule="FIRST_OBSERVABLE_V1", side="BUY", *, hard=False):
    policy = Policy(policy_id=rule, source_commit="1" * 40)
    constraint = PriceConstraint(mode="HARD" if hard else "PREFERRED",
                                 intervals=(PriceInterval(lower="9.8", upper="10.2"),),
                                 source_ref="FUNCTIONAL_FIXTURE", source_sha256="a" * 64,
                                 valid_from=at("09:30:00"), valid_until=at("14:57:00"))
    plan = Plan(plan_id="functional", revision=1, source_kind="FROZEN_REPLAY",
                source_ref="FUNCTIONAL_FIXTURE", source_revision="1", symbol="000001.SZ",
                side=side, target_trade_date=at().date(), valid_from=at("09:30:00"),
                valid_until=at("14:57:00"), target_quantity=1000, quantity_source_ref="test",
                policy_id=rule, policy=policy, policy_sha256=digest(policy), price_constraint=constraint)
    bars = []
    for n in range(30):
        start = at("09:30:00") + timedelta(minutes=n)
        bars.append(MinuteBar(start=start, end=start + timedelta(minutes=1),
                              open="10", high="10.01", low="9.99", close="10", volume_shares="10000",
                              received_at=start + timedelta(seconds=65)))
    facts = DayFacts(trade_date=at().date(), symbol=plan.symbol, is_suspended=False,
                     has_daily_limit=True, reference_price="10", limit_up="11", limit_down="9",
                     available_at=at("09:00:00"), source_ref="test", source_sha256="b" * 64)
    market = MarketView(symbol=plan.symbol, trade_date=at().date(), source_ref="test", source_version="1",
                        calendar=Calendar(trade_date=at().date(), is_trading_day=True, source_ref="test",
                                          source_sha256="c" * 64, available_at=at("09:00:00")),
                        day_facts=facts, bars=tuple(bars), visibility_basis="ASSUMED_VISIBILITY")
    return plan, market


def execution(rule="FIRST_OBSERVABLE_V1", side="BUY", *, cash=100000, sellable=1000, delay=0, hard=False):
    plan, market = inputs(rule, side, hard=hard)
    positions = {} if side == "BUY" else {
        plan.symbol: PositionLot(portfolio_id="fixture", symbol=plan.symbol, quantity=1000,
                                 available_quantity=sellable, avg_cost=10, trade_date=at().date() - timedelta(days=3))}
    backend = LocalSimBackend(portfolio_id="fixture", initial_cash=cash, manifest=None,
                               package_id="FUNCTIONAL_FIXTURE", data_source=MinuteDataSource.DATASET_FILES,
                               execution_policy=policy_snapshot(), market_data_provider=market,
                               initial_positions=positions)
    session = backend.open_timing_execution(plan=plan, delay_seconds=delay, mode=CLOSED_LOOP_MODE,
                                            run_id="functional", binding_id="fixture")
    return backend, session


@pytest.mark.parametrize("rule", ["FIRST_OBSERVABLE_V1", "PRICE_TOUCH_V1", "BOUNDED_REVERSION_V1"])
def test_real_rules_require_confirmed_account_fills(rule):
    backend, session = execution(rule)
    for bar in session.market.bars[1:]:
        now = bar.start - timedelta(seconds=50)
        session.facade.evaluate(session.market, session.feedback(now), now)
        before = session.state.executed_quantity
        assert before == session.order.filled_quantity
        session.step(bar)
        if session.order.remaining_quantity == 0:
            break
    assert session.order.filled_quantity == session.state.executed_quantity == 1000
    assert len(backend._ledger.fills) == 1
    assert backend._ledger.cash == 89995
    assert backend.query_positions()["000001.SZ"].available_quantity == 0
    assert session.feedback(at("10:00:00")).last_confirmed_batch == 1


@pytest.mark.parametrize("delay", [0, 60, 180, 300])
def test_strict_after_four_delays_and_future_suffix_isolation(delay):
    _, session = execution(delay=delay)
    first = at("09:31:10")
    decision = session.facade.evaluate(session.market, session.feedback(first), first)
    changed = session.market.model_copy(update={"bars": tuple(
        b if b.received_at <= first else b.model_copy(update={"close": Decimal("10.9"), "high": Decimal("11")})
        for b in session.market.bars)})
    _, twin = execution(delay=delay)
    assert twin.facade.evaluate(changed, twin.feedback(first), first) == decision
    for bar in session.market.bars[2:]:
        now = bar.start - timedelta(seconds=50)
        session.facade.evaluate(session.market, session.feedback(now), now)
        session.step(bar)
        if session.fills:
            break
    assert session.fills[0].trade_time == at("09:32:00") + timedelta(seconds=delay)
    assert session.waits == [(1000, Decimal(50 + delay))]


def test_partial_fill_parent_minimum_fee_t1_and_retry():
    backend, session = execution()
    now = at("09:31:10")
    session.facade.evaluate(session.market, session.feedback(now), now)
    bar = session.market.bars[2].model_copy(update={"volume_shares": Decimal("300")})
    session.step(bar)
    assert session.order.filled_quantity == 300
    assert session.feedback(at("09:32:10")).confirmed_quantity == 0
    assert session.feedback(at("09:33:10")).confirmed_quantity == 300
    assert session.facade.last_batch == 0
    assert session.step(bar) == []
    assert len(backend._ledger.fills) == 1
    next_bar = session.market.bars[4]
    session.facade.evaluate(session.market, session.feedback(at("09:33:10")), at("09:33:10"))
    session.step(next_bar)
    assert session.order.filled_quantity == 1000
    assert sum(e.fee for e in backend._ledger.cash_entries) == Decimal("5.00")
    assert backend._ledger.positions["000001.SZ"].available_quantity == 0


@pytest.mark.parametrize("kind", ["cash", "sellable", "hard", "limit", "suspended", "cancel", "revision"])
def test_constraints_and_old_intents_never_force_residual_fill(kind):
    backend, session = execution(side="SELL" if kind == "sellable" else "BUY",
                                 cash=1 if kind == "cash" else 100000,
                                 sellable=0 if kind == "sellable" else 1000, hard=kind == "hard")
    now = at("09:31:10")
    session.facade.evaluate(session.market, session.feedback(now), now)
    bar = session.market.bars[2]
    if kind in ("hard", "limit"):
        price = Decimal("10.5") if kind == "hard" else Decimal("11")
        bar = bar.model_copy(update={"open": price, "high": price, "low": price, "close": price})
    if kind == "suspended":
        session.market = session.market.model_copy(update={"day_facts": session.market.day_facts.model_copy(update={"is_suspended": True})})
    if kind == "cancel":
        session.cancel("USER_CANCELLED")
    if kind == "revision":
        session.facade.replace_plan(session.facade.plan.model_copy(update={"revision": 2}))
    session.step(bar)
    assert session.order.filled_quantity == session.state.executed_quantity == 0
    assert not backend._ledger.cash_entries


def test_future_decision_rejected_without_mutating_account():
    backend, session = execution()
    now = at("09:32:10")
    session.facade.evaluate(session.market, session.feedback(now), now)
    before = session.snapshot()
    with pytest.raises(Exception, match="algorithm step failed"):
        session.step(session.market.bars[2])
    assert session.snapshot() == before
    assert not backend._ledger.fills


def test_backend_fill_feedback_and_cancel_use_existing_broker_chain():
    backend, session = execution()
    received = []
    backend.subscribe_fill_callback(received.append)
    now = at("09:31:10")
    session.facade.evaluate(session.market, session.feedback(now), now)
    bar = session.market.bars[2].model_copy(update={"volume_shares": Decimal("300")})
    session.step(bar)
    assert backend.query_status(session.handle).filled_quantity == 300
    assert sum(event.fill_quantity for event in received) == 300
    assert backend.cancel(session.handle).accepted
    assert session.facade.pending is None
    session.step(session.market.bars[3])
    assert session.order.filled_quantity == 300
    assert backend.query_status(session.handle).state == "cancelled"


def test_coordinator_checkpoint_restore_and_exact_retry_preserve_parent_fee(tmp_path):
    from backend.services.simulation_execution.localsim.timing import TimingExecution
    from backend.services.simulation_execution.localsim.timing_store import TimingFileRepository

    backend, session = execution()
    store = TimingFileRepository(tmp_path, session.run_id)
    now = at("09:31:10")
    session.facade.evaluate(session.market, session.feedback(now), now)
    bar = session.market.bars[2].model_copy(update={"volume_shares": Decimal("300")})
    session.step(bar)
    assert store.checkpoint(session, {"cursor": 3}).created
    assert not store.checkpoint(session, {"cursor": 3}).created
    saved = store.latest()["checkpoint"]["account"]
    twin_backend, _ = execution()
    twin_backend._records.clear()
    del twin_backend._timing_execution
    restored = TimingExecution.restore(twin_backend, session.market, saved)
    assert restored.snapshot() == saved
    assert restored.step(bar) == []
    restored.facade.evaluate(restored.market, restored.feedback(at("09:33:10")), at("09:33:10"))
    restored.step(restored.market.bars[4])
    assert restored.order.filled_quantity == 1000
    assert sum(e.fee for e in twin_backend._ledger.cash_entries) == Decimal("5.00")
    assert store.checkpoint(restored, {"cursor": 5}).created
    assert store.latest()["generation"] == 2
    assert len(twin_backend._ledger.fills) == 2


def test_checkpoint_failure_keeps_committed_prefix_and_corruption_is_visible(tmp_path, monkeypatch):
    from backend.services.simulation_execution.localsim.timing_store import TimingFileRepository

    _, session = execution()
    store = TimingFileRepository(tmp_path, session.run_id)
    store.checkpoint(session, {"cursor": 0})
    before = store.latest()
    with monkeypatch.context() as patch:
        patch.setattr(store, "save_fill", lambda *_: (_ for _ in ()).throw(OSError("injected")))
        now = at("09:31:10")
        session.facade.evaluate(session.market, session.feedback(now), now)
        session.step(session.market.bars[2])
        with pytest.raises(OSError, match="injected"):
            store.checkpoint(session, {"cursor": 3})
    assert store.latest() == before
    store.checkpoint(session, {"cursor": 3})
    path = store.root / "generations" / "00000002.json"
    path.write_text(path.read_text().replace('"generation":2', '"generation":9', 1))
    with pytest.raises(ValueError, match="CHAIN_CONFLICT"):
        store.latest()


@pytest.mark.parametrize("opportunity", [True, False])
def test_offline_row_values_unfilled_cash_and_keeps_readable_receipt(tmp_path, opportunity):
    from backend.services.simulation_execution.localsim.timing_research import run_case, ref
    from backend.services.simulation_execution.localsim.timing_store import publish, read_json
    from backend.services.position_timing.intraday.replay import PlanEvent
    from backend.services.position_timing.intraday.research import CostContract

    backend, session = execution()
    plan, market = session.facade.plan, session.market
    now = at("09:31:10")
    decision = session.facade.evaluate(market, session.feedback(now), now)
    if not opportunity:
        decision = decision.model_copy(update={"status": "WAIT", "reason_codes": ("WAITING_FOR_PRICE",)})
    initial_path = tmp_path / "initial.json"
    initial = {"portfolio_id": backend.portfolio_id, "package_id": backend.package_id,
               "origin_initial_cash_cny": "100000", "cash_cny": "100000", "positions": {},
               "initial_nav_cny": "100000", "initial_mark_raw": "10", "target_notional_cny": "10000"}
    publish(initial_path, initial)
    case = {"case_id": "c" * 64, "plan_id": plan.plan_id, "delay_seconds": 0}
    kwargs = dict(case=case, initial_ref=ref(initial_path), market=market,
                  events=[PlanEvent(available_at=plan.valid_from, plan=plan)], decisions=[decision],
                  cost=CostContract(open_rate="0.000095", close_rate="0.000595", minimum_cny="5"),
                  root=tmp_path, request={"identity": "i" * 64, "valuation_contract": "test"})
    row = run_case(**kwargs)
    assert row.status == ("FILLED" if opportunity else "NO_FILL")
    assert row.terminal_nav_cny == (Decimal("99995") if opportunity else Decimal("100000"))
    assert row.fees_cny == (Decimal("5") if opportunity else Decimal("0"))
    assert row.mean_wait_seconds == (Decimal("50") if opportunity else None)
    receipt_path = tmp_path / "cases" / case["case_id"] / "account_receipt.json"
    assert read_json(receipt_path)["account"]["cash_cny"] == ("89995.00" if opportunity else "100000.00")
    assert run_case(**kwargs) == row


def test_split_batches_follow_cumulative_confirmed_fills_and_cooldown():
    plan, market = inputs("BOUNDED_REVERSION_V1")
    policy = plan.policy.model_copy(update={"allow_split": True})
    plan = plan.model_copy(update={"allow_split": True, "policy": policy, "policy_sha256": digest(policy)})
    template = market.bars[0]
    market = market.model_copy(update={"bars": tuple(template.model_copy(update={
        "start": template.start + timedelta(minutes=n), "end": template.end + timedelta(minutes=n),
        "received_at": template.received_at + timedelta(minutes=n)}) for n in range(120))})
    backend = LocalSimBackend(portfolio_id="split", initial_cash=100000, manifest=None,
                              package_id="FUNCTIONAL_FIXTURE", data_source=MinuteDataSource.DATASET_FILES,
                              execution_policy=policy_snapshot(), market_data_provider=market)
    session = backend.open_timing_execution(plan=plan, delay_seconds=0, mode=CLOSED_LOOP_MODE,
                                            run_id="split", binding_id="split")
    for bar in market.bars[1:]:
        clock = bar.start - timedelta(seconds=50)
        if session.facade.last_feedback_at and clock < session.facade.last_feedback_at:
            continue
        session.facade.evaluate(market, session.feedback(clock), clock)
        session.step(bar)
        if session.order.remaining_quantity == 0:
            break
    assert [f.quantity for f in session.fills] == [300, 300, 400]
    assert session.facade.last_batch == 3
    assert sum(e.fee for e in backend._ledger.cash_entries) == Decimal("5.00")


def test_quantity_revision_cancels_old_parent_and_restore_keeps_confirmed_quantity():
    from backend.services.simulation_execution.localsim.timing import TimingExecution
    backend, session = execution()
    now = at("09:31:10")
    session.facade.evaluate(session.market, session.feedback(now), now)
    session.step(session.market.bars[2].model_copy(update={"volume_shares": Decimal("300")}))
    result = session.update_plan(session.facade.plan.model_copy(update={"revision": 2, "target_quantity": 1500}))
    assert result == {"status": "CANCELLED_REQUIRES_NEW_INTENT", "confirmed_quantity": 300, "remaining_quantity": 1200}
    assert session.step(session.market.bars[4]) == []
    saved = session.snapshot()
    twin_backend, _ = execution()
    twin_backend._records.clear()
    del twin_backend._timing_execution
    restored = TimingExecution.restore(twin_backend, session.market, saved)
    assert restored.order.filled_quantity == 300
    assert restored.order.status.value == "CANCELLED"
    assert len(twin_backend._ledger.fills) == 1


@pytest.mark.parametrize("side,price,filled", [("BUY", "9", 1000), ("SELL", "11", 1000), ("SELL", "9", 0)])
def test_directional_limits_allow_opposite_side(side, price, filled):
    _, session = execution(side=side)
    now = at("09:31:10")
    session.facade.evaluate(session.market, session.feedback(now), now)
    bar = session.market.bars[2].model_copy(update={key: Decimal(price) for key in ("open", "high", "low", "close")})
    session.step(bar)
    assert session.order.filled_quantity == filled


def test_campaign_discovers_owned_daily_inputs_without_inventing_price_preferences(tmp_path):
    from backend.services.simulation_execution.localsim.timing_campaign import inspect_selection_history
    from backend.services.simulation_execution.localsim.timing_store import publish

    def artifact(name, day, *, package="selected", cutoff="2026-09-18", weight="0.5"):
        folder = tmp_path / name / "selection"
        publish(folder / "completion.json", {"consumer_scope": "LOCALSIM", "status": "SIGNAL_COMMITTED",
                "clocks": {"target_trade_date": day, "data_cutoff": cutoff}})
        publish(folder / "projection.json", {
            "manifest_sha256_by_package": {package: "a" * 64},
            "runtime_config": {"point_in_time_context": {"effective_trade_date": day, "cutoff_date": cutoff}},
            "aggregate_results": [{"symbol": "000001.SZ", "target_weight": weight, "reference_price": "10"}]})

    artifact("first", "2026-09-21")
    artifact("other-package", "2026-09-22", package="other")
    artifact("outside-candidate", "2026-10-08")
    result = inspect_selection_history(tmp_path, package_id="selected", manifest_sha256="a" * 64,
                                      trade_dates=["2026-09-21", "2026-09-22"])
    assert result["missing_trade_dates"] == ["2026-09-22"]
    assert result["excluded_dates_beyond_candidate"] == ["2026-10-08"]
    assert result["targets_without_entry_band"] == 1
    assert result["days"][0]["targets"][0]["reference_price"] == "10"
    assert result["days"][0]["targets"][0]["suggested_entry_price_band"] is None
    artifact("conflict", "2026-09-21", weight="0.7")
    assert inspect_selection_history(tmp_path, package_id="selected", manifest_sha256="a" * 64,
                                     trade_dates=["2026-09-21"])["conflicting_trade_dates"] == ["2026-09-21"]
    artifact("bad-clock", "2026-09-22", cutoff="2026-09-22")
    with pytest.raises(ValueError, match="SELECTION_CLOCK_CONFLICT"):
        inspect_selection_history(tmp_path, package_id="selected", manifest_sha256="a" * 64,
                                  trade_dates=["2026-09-22"])
