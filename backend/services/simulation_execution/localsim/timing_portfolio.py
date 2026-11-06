"""Offline multi-parent orchestration on the existing LocalSIM account.

No timing rule, target algorithm, matching, fee or accounting implementation
lives here. A completed day is the atomic recovery unit of a continuous study.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, time, timedelta
from decimal import Decimal

from backend.services.position_timing.intraday.contracts import CHINA, MarketView, digest, payload, utc
from backend.services.position_timing.intraday.replay import PlanEvent
from backend.services.selection_center.models import SignalSnapshot
from backend.services.simulation_runtime.decision import RebalanceIntentService
from backend.services.strategy_package.runtime import TargetPositionEngine
from backend.services.trading_core.ledger import InMemoryLedger
from backend.services.trading_core.models import Fill, OrderStatus, PositionLot

from .economic import LocalSimEconomicCommitRequest, local_sim_fact_payload
from .timing_research import backend_for, fee_model


TERMINAL = {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}


def compile_native_intents(*, projection: dict, manifest, account: dict, markets: dict[str, MarketView],
                           prices: dict[str, Decimal], cost):
    """Convert an existing projection through the native target/rebalance code."""
    if projection["manifest_sha256_by_package"] != {account["package_id"]: account["manifest_sha256"]}:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PROJECTION_IDENTITY_CONFLICT")
    day = next(iter(markets.values())).trade_date
    clock = projection["runtime_config"]["point_in_time_context"]
    if clock["effective_trade_date"] != str(day) or clock["cutoff_date"] >= str(day):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_SELECTION_CLOCK_CONFLICT")
    backend = backend_for(account, next(iter(markets.values())), cost)
    backend._ledger.settle_trade_date(day)
    equity = backend._ledger.account_snapshot(prices={s: float(p) for s, p in prices.items()},
                                              snapshot_time=datetime.combine(day, time(9, 30), CHINA)).nav
    snapshot = SignalSnapshot(
        snapshot_id="campaign_" + digest(projection), package_id=account["package_id"],
        manifest_sha256=account["manifest_sha256"], trade_date=day, data_source="dataset_files",
        candidates=projection["aggregate_results"], runtime_config=projection["runtime_config"],
        valid_no_candidate=projection.get("valid_no_candidate", False),
        no_candidate_reason=projection.get("no_candidate_reason"),
        created_at=datetime.combine(day, time(9, 20), CHINA),
    )
    targets = [] if snapshot.valid_no_candidate else TargetPositionEngine().build_targets(
        snapshot=snapshot, total_equity=equity, top_k=len(snapshot.candidates), manifest=manifest,
        current_positions=backend._ledger.positions, current_prices={s: float(p) for s, p in prices.items()},
    )
    tradability = {}
    for symbol, market in markets.items():
        if market.day_facts.is_suspended is None:
            raise ValueError("LOCALSIM_TIMING_TRADABILITY_UNKNOWN")
        tradability[symbol] = {"is_tradable": not market.day_facts.is_suspended,
                               "reason_code": "SUSPENDED_BY_SUSPEND_D" if market.day_facts.is_suspended else "TRADABLE"}
    result = RebalanceIntentService().build_order_intents(
        package_id=account["package_id"], portfolio_id=account["portfolio_id"],
        strategy_id=account["portfolio_id"], trade_date=day, current_positions=backend._ledger.positions,
        target_positions=targets, current_prices={s: float(p) for s, p in prices.items()},
        pre_trade_tradability=tradability,
    )
    return backend, targets, result


def _account(backend) -> dict:
    return {"cash_cny": str(backend._ledger.cash_decimal),
            "positions": {s: p.model_dump(mode="json") for s, p in backend._ledger.positions.items()}}


def _validate_markets(markets, day, start):
    """The frozen candidate reader has uniform end+5s assumed visibility."""
    for symbol, market in markets.items():
        if (symbol != market.symbol or market.trade_date != day
                or market.day_facts.symbol != symbol or market.day_facts.trade_date != day
                or market.calendar.trade_date != day or market.visibility_basis != "ASSUMED_VISIBILITY"
                or market.day_facts.available_at > start or market.calendar.available_at > start):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_MARKET_IDENTITY_CONFLICT")
        stamps = [bar.start for bar in market.bars]
        if stamps != sorted(set(stamps)):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_MARKET_CLOCK_CONFLICT")
        if market.day_facts.is_suspended and market.bars:
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_SUSPENDED_MARKET_HAS_QUOTES")
        if not market.day_facts.is_suspended:
            terminal = utc(datetime.combine(day, time(14, 57), CHINA))
            expected = {start + timedelta(minutes=n) for n in range(327)
                        if (n < 120 or n >= 210) and start + timedelta(minutes=n, seconds=65) <= terminal}
            if not expected.issubset(stamps):
                raise ValueError("LOCALSIM_TIMING_CAMPAIGN_MINUTE_PREFIX_INCOMPLETE")
        for bar in market.bars:
            if (bar.start.astimezone(CHINA).date() != day
                    or max(bar.end, bar.received_at, bar.source_available_at or bar.end)
                    != bar.end + timedelta(seconds=5)):
                raise ValueError("LOCALSIM_TIMING_CAMPAIGN_VISIBILITY_CONTRACT_UNSUPPORTED")


def run_portfolio_day(*, backend, markets: dict[str, MarketView], events: list[PlanEvent],
                      initial_marks: dict[str, Decimal], delay_seconds: int, run_id: str) -> dict:
    """Run one atomic day, with a shared cash pool and confirmed core feedback.

    Signals at each clock are evaluated before execution at that clock. Account
    views and marks become visible only at completed interval availability.
    All competing parents use the same deterministic sell-first/symbol order.
    """
    if not markets:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_MARKETS_REQUIRED")
    day = next(iter(markets.values())).trade_date
    start = utc(datetime.combine(day, time(9, 30), CHINA))
    terminal = utc(datetime.combine(day, time(14, 57), CHINA))
    _validate_markets(markets, day, start)
    if backend._records or backend._ledger.fills or backend._ledger.cash_entries:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_DAY_REQUIRES_FRESH_PARENT_SCOPE")
    backend._ledger.settle_trade_date(day)
    required = set(backend._ledger.positions) | {event.plan.symbol for event in events}
    if not required.issubset(markets) or any(s not in initial_marks or initial_marks[s] <= 0 for s in required):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_MARK_OR_MARKET_MISSING")
    by_plan = {}
    for event in sorted(events, key=lambda e: (e.available_at, e.plan.plan_id, e.plan.revision)):
        plan = event.plan
        if (plan.target_trade_date != day or plan.valid_until > terminal or plan.target_quantity is None
                or plan.allow_split or event.available_at > plan.valid_until):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PLAN_CONTRACT_CONFLICT")
        prior = by_plan.setdefault(plan.plan_id, [])
        if (not prior and (plan.revision != 1 or plan.lifecycle != "ACTIVE" or event.available_at > plan.valid_from)
                or prior and (plan.revision != prior[-1].plan.revision + 1
                              or event.available_at <= prior[-1].available_at
                              or (plan.symbol, plan.side, plan.target_quantity) != (
                                  prior[-1].plan.symbol, prior[-1].plan.side, prior[-1].plan.target_quantity))):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PLAN_REVISION_CONFLICT")
        prior.append(event)
    initial = _account(backend)
    initial_nav = Decimal(str(backend._ledger.account_snapshot(
        prices={s: float(p) for s, p in initial_marks.items()}, snapshot_time=start).nav))
    views = [(start, initial)]
    marks = dict(initial_marks)
    polls = {start + timedelta(minutes=n, seconds=10) for n in range(327)}
    bars, observations, changes = {}, {}, {}
    for market in markets.values():
        for bar in market.bars:
            available = max(bar.end, bar.received_at, bar.source_available_at or bar.end)
            if start <= bar.start < terminal and available <= terminal:
                bars.setdefault(bar.start, {})[market.symbol] = bar
                observations.setdefault(available, {})[market.symbol] = bar
    for series in by_plan.values():
        for event in series:
            changes.setdefault(max(start, event.available_at), []).append(event)
    clocks = sorted({start, terminal} | polls | set(bars) | set(observations) | set(changes)
                    | {event.plan.valid_until for event in events})
    sessions, navs = {}, [{"as_of": start.isoformat(), "nav_cny": str(initial_nav)}]
    for clock in clocks:
        if clock > terminal:
            continue
        for event in changes.get(clock, []):
            plan = event.plan
            if plan.plan_id in sessions:
                sessions[plan.plan_id].update_plan(plan)
            else:
                sessions[plan.plan_id] = backend.open_timing_portfolio_execution(
                    plan=plan, market=markets[plan.symbol], delay_seconds=delay_seconds,
                    run_id=run_id, binding_id=backend.portfolio_id)
        ordered = sorted(sessions.values(), key=lambda s: (s.order.side.value != "SELL", s.order.symbol, s.order.order_id))
        visible_at, visible = max((view for view in views if view[0] <= clock), key=lambda view: view[0])
        for session in ordered:
            if clock >= session.facade.plan.valid_until:
                session.cancel("COMMON_TERMINAL_1457" if clock == terminal else "PLAN_EXPIRED")
            feedback_visible = session.facade.last_feedback_at is None or session.facade.last_feedback_at <= clock
            if clock in polls and session.order.status not in TERMINAL and feedback_visible:
                feedback = session.feedback(clock)
                lot = visible["positions"].get(session.order.symbol)
                feedback = feedback.model_copy(update={"cash_cny": Decimal(visible["cash_cny"]),
                                                       "sellable_quantity": lot["available_quantity"] if lot else 0,
                                                       "available_at": max(feedback.available_at, visible_at),
                                                       "source_ref": run_id,
                                                       "feedback_id": digest([feedback.feedback_id, visible])})
                decision = session.facade.evaluate(session.market, feedback, clock)
                if decision.status == "UNAVAILABLE":
                    raise ValueError("LOCALSIM_TIMING_CAMPAIGN_CORE_UNAVAILABLE:" + ",".join(decision.reason_codes))
        any_fill = False
        for session in ordered:
            bar = bars.get(clock, {}).get(session.order.symbol)
            feedback_visible = session.facade.last_feedback_at is None or session.facade.last_feedback_at <= clock
            if bar is not None and session.order.status not in TERMINAL and feedback_visible:
                any_fill = bool(session.step(bar)) or any_fill
        if any_fill:
            views.append((clock + timedelta(seconds=65), _account(backend)))
        if clock in observations:
            for symbol, bar in observations[clock].items():
                marks[symbol] = bar.close
        if clock in observations or clock == terminal:
            visible = max((view for view in views if view[0] <= clock), key=lambda view: view[0])[1]
            # Use the existing ledger's valuation method on a read-only view;
            # never alter the live account to report a not-yet-visible fill.
            valuation = deepcopy(backend._ledger)
            valuation.cash = Decimal(visible["cash_cny"])
            valuation.positions = {s: PositionLot.model_validate(p) for s, p in visible["positions"].items()}
            nav = valuation.account_snapshot(prices={s: float(p) for s, p in marks.items()}, snapshot_time=clock).nav
            navs.append({"as_of": clock.isoformat(), "nav_cny": str(nav)})
    for session in sessions.values():
        session.cancel("COMMON_TERMINAL_1457")
    return portfolio_day_result(backend=backend, sessions=tuple(sessions.values()), initial=initial,
                                marks=marks, navs=navs, day=day, terminal=terminal, initial_nav=initial_nav)


def portfolio_day_result(*, backend, sessions, initial, marks, navs, day, terminal, initial_nav):
    """One native valuation/result authority, shared by operation orchestrators."""
    values = [Decimal(row["nav_cny"]) for row in navs]
    peak, mdd = values[0], Decimal(0)
    for value in values:
        peak = max(peak, value)
        mdd = max(mdd, (peak - value) / peak)
    parents = []
    for session in sessions:
        qty = session.order.filled_quantity
        parents.append({"plan_id": session.facade.plan.plan_id, "symbol": session.order.symbol,
                        "side": session.order.side.value, "target_quantity": session.order.quantity,
                        "filled_quantity": qty,
                        "status": "FILLED" if qty == session.order.quantity else "PARTIAL" if qty else "NO_FILL",
                        "reason_codes": sorted(set(session.reasons)),
                        "average_price_raw": str(sum(Decimal(str(f.price)) * f.quantity for f in session.fills) / qty) if qty else None,
                        "weighted_wait_seconds": str(sum(q * wait for q, wait in session.waits)),
                        "session": session.snapshot(include_account=False)})
    account = {"schema_version": "localsim_timing_portfolio_day_account_v1",
               "portfolio_id": backend.portfolio_id, "package_id": backend.package_id,
               "trade_date": str(day), "initial_account": initial, **_account(backend),
               "marks_raw_cny": {s: str(p) for s, p in marks.items()},
               "initial_nav_cny": str(initial_nav), "terminal_nav_cny": str(values[-1]),
               "terminal_as_of": terminal.isoformat(), "max_drawdown": str(mdd),
               "nav_observations": navs, "parents": parents,
               "fees_cny": str(sum((entry.fee for entry in backend._ledger.cash_entries), Decimal(0))),
               "mode": "CONFIRMED_FEEDBACK_CLOSED_LOOP", "recovery_unit": "ATOMIC_COMPLETED_DAY"}
    return {"account": payload(account), "sessions": tuple(sessions), "backend": backend}


def attach_native_quantity_evidence(result: dict, *, targets, native, projection_ref: dict,
                                    price_policy_sha256: str, unavailable=()) -> None:
    """Retain rejected requests in the denominator and report actual fees."""
    account, backend = result["account"], result["backend"]
    account["parents"].extend(unavailable)
    by_key = {(d.symbol, d.side.value): d for d in native.trading_rule_decisions}
    for row in account["parents"]:
        decision = by_key[(row["symbol"], row["side"])]
        row.update(requested_quantity=decision.requested_quantity, legal_quantity=decision.legal_quantity,
                   order_created=row.get("order_created", True), native_rule_reason=decision.reason_code)
    actual = [(row["symbol"], row["side"]) for row in account["parents"]]
    expected = {(intent.symbol, intent.side.value) for intent in native.order_intents}
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_INTENT_RESULT_POPULATION_CONFLICT")
    for decision in native.trading_rule_decisions:
        if decision.legal_quantity <= 0:
            account["parents"].append({"plan_id": "native_blocked_" + decision.decision_id,
                                       "symbol": decision.symbol, "side": decision.side.value,
                                       "target_quantity": decision.requested_quantity,
                                       "requested_quantity": decision.requested_quantity, "legal_quantity": 0,
                                       "filled_quantity": 0, "status": "NO_FILL", "order_created": False,
                                       "reason_codes": [decision.reason_code], "average_price_raw": None,
                                       "weighted_wait_seconds": "0", "native_rule_reason": decision.reason_code})
    by_order = {}
    fills = {f.fill_id: f for f in backend._ledger.fills}
    fees_by_side = {"BUY": Decimal(0), "SELL": Decimal(0)}
    for entry in backend._ledger.cash_entries:
        fill = fills[entry.fill_id]
        row = by_order.setdefault(fill.order_id, {"notional": Decimal(0), "fee": Decimal(0), "side": fill.side})
        row["notional"] += entry.notional
        row["fee"] += entry.fee
        fees_by_side[fill.side.value] += entry.fee
    account["minimum_fee_parent_count"] = sum(
        row["fee"] > 0 and row["notional"] * backend._ledger.fee_model.rate_for(row["side"])
        < backend._ledger.fee_model.min_cost_decimal() for row in by_order.values())
    account["fee_positive_parent_count"] = sum(row["fee"] > 0 for row in by_order.values())
    account["fees_by_side_cny"] = {side: str(value) for side, value in fees_by_side.items()}
    account["native_quantity_evidence"] = {
        "projection": projection_ref, "targets": [t.model_dump(mode="json") for t in targets],
        "order_intents": [i.model_dump(mode="json") for i in native.order_intents],
        "trading_rule_decisions": [d.model_dump(mode="json") for d in native.trading_rule_decisions],
        "price_policy_file_sha256": price_policy_sha256}


def validate_day_ledger(record: dict, *, initial: dict, cost) -> None:
    """Independently replay confirmed facts through the same native ledger.

    This is readback validation, not a second accounting model or run. The
    immutable source account supplies the legal origin capital; the completed
    day's opening snapshot supplies its carried cash and positions.
    """
    account = record["checkpoint"]["account"]
    ledger = InMemoryLedger(portfolio_id=initial["account_id"],
                            initial_cash=Decimal(initial["origin_initial_cash_cny"]), fee_model=fee_model(cost))
    ledger.cash = Decimal(account["initial_account"]["cash_cny"])
    ledger.positions = {s: PositionLot.model_validate(p) for s, p in account["initial_account"]["positions"].items()}
    if any(p.portfolio_id != ledger.portfolio_id or p.symbol != s for s, p in ledger.positions.items()):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_OPENING_POSITION_IDENTITY_CONFLICT")
    fills = [Fill.model_validate(row) for row in record["facts"]["fills"].values()]
    for fill in sorted(fills, key=lambda f: (f.trade_time, f.side.value != "SELL", f.symbol, f.order_id, f.fill_id)):
        ledger.apply_fill(fill)
    entries = {entry.fill_id: local_sim_fact_payload(entry, fact_type="cash_entry") for entry in ledger.cash_entries}
    positions = {s: p.model_dump(mode="json") for s, p in ledger.positions.items()}
    if (str(ledger.cash_decimal) != account["cash_cny"] or positions != account["positions"]
            or entries != record["facts"]["cash_entries"]):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_LEDGER_RECONCILIATION_FAILED")


def commit_portfolio_day(*, store, result: dict, input_identity: str, day_index: int):
    """Commit every parent's actual facts in one existing coordinator scope."""
    account = result["account"]
    previous = store.latest()
    expected_index = previous["checkpoint"]["extra"]["day_index"] + 1 if previous else 0
    if day_index != expected_index:
        if (previous and day_index == expected_index - 1 and previous["checkpoint"]["account"] == account
                and previous["checkpoint"]["extra"]["input_identity"] == input_identity):
            return {"status": "ALREADY_MATERIALIZED", "generation": previous["generation"]}
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PREFIX_CONFLICT")
    if previous and previous["checkpoint"]["extra"]["input_identity"] != input_identity:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_SOURCE_CHANGED")
    request = result.get("commit_request")
    if request is None:
        request = prepare_portfolio_commit(result=result, run_id=store.run_id,
                                           input_identity=input_identity, day_index=day_index)
    if (not isinstance(request, LocalSimEconomicCommitRequest)
            or request.run_id != store.run_id or request.binding_id != account["portfolio_id"]
            or request.trade_date != datetime.fromisoformat(account["terminal_as_of"]).astimezone(CHINA).date()
            or request.plan_id != "campaign_day_" + digest([input_identity, day_index])
            or request.economic_facts != {"account_sha256": digest(account), "input_identity": input_identity, "day_index": day_index}
            or request.projection_payload != {"account": account, "extra": {"input_identity": input_identity, "day_index": day_index}}
            or request.status != "OFFLINE_RESEARCH_DAY_COMPLETE"
            or request.expected_versions != {state.state_id: None for state in request.states}
            or request.payload_patch or request.payload_unset or request.on_created is not None):
        raise ValueError("LOCALSIM_RESEARCH_WORKER_COMMIT_IDENTITY_CONFLICT")
    receipt = store.coordinator.commit(request)
    return {"status": "MATERIALIZED", "generation": store.latest()["generation"],
            "receipt_id": receipt.receipt.receipt_id}


def prepare_portfolio_commit(*, result: dict, run_id: str, input_identity: str, day_index: int):
    """Transport native facts from pure computation; only the coordinator writes."""
    account, sessions, backend = result["account"], result["sessions"], result["backend"]
    states = tuple(s.execution_state for s in sessions)
    return LocalSimEconomicCommitRequest(
        run_id=run_id, binding_id=backend.portfolio_id,
        trade_date=datetime.fromisoformat(account["terminal_as_of"]).astimezone(CHINA).date(),
        plan_id="campaign_day_" + digest([input_identity, day_index]), states=states,
        expected_versions={state.state_id: None for state in states},
        economic_facts={"account_sha256": digest(account), "input_identity": input_identity,
                        "day_index": day_index},
        projection_payload={"account": account, "extra": {"input_identity": input_identity, "day_index": day_index}},
        status="OFFLINE_RESEARCH_DAY_COMPLETE", payload_patch={}, payload_unset=(),
        orders=tuple(s.order for s in sessions), fills=tuple(backend._ledger.fills),
        events=tuple(event for s in sessions for event in s.events),
        cash_entries=tuple(backend._ledger.cash_entries),
    )
