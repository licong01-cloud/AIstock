"""Native TWAP research adapter. Matching, fees, state and ledger stay native."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, time, timedelta
from decimal import Decimal
import re

from backend.services.position_timing.intraday.contracts import CHINA, Plan, Policy, digest, payload, utc
from backend.services.simulation_data.contracts import MinuteDataSource, MinuteExecutionMarketInput
from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_snapshot
from backend.services.trading_core.models import MinuteBar, PositionLot

from .runtime import LocalSimBackend
from .research_performance import VisibleAccountIndex
from .timing import CLOSED_LOOP_MODE, TimingExecution
from .timing_portfolio import TERMINAL, _account, _validate_markets, portfolio_day_result
from .timing_research import fee_model


class FrozenResearchMarketProvider:
    """Explicit local-only carrier, never a fallback from production inputs."""

    def __init__(self, *, package_id: str, markets: dict, source_identity: str, cash_only_trade_date=None):
        self.package_id, self.markets, self.source_identity = package_id, markets, source_identity
        self.cash_only_trade_date = cash_only_trade_date
        self.validate()

    def validate(self):
        if (not self.package_id or not re.fullmatch(r"[0-9a-f]{64}", self.source_identity)
                or not self.markets and self.cash_only_trade_date is None
                or self.markets and self.cash_only_trade_date is not None
                or any(m.symbol != symbol for symbol, m in self.markets.items())
                or self.markets and len({m.trade_date for m in self.markets.values()}) != 1):
            raise ValueError("LOCALSIM_NATIVE_FROZEN_MARKET_INVALID")

    def load_symbol_input(self, *, symbol, trade_date, source, min_bars=1):
        market = self.markets[symbol]
        if market.trade_date != trade_date or source != MinuteDataSource.DATASET_FILES:
            raise ValueError("LOCALSIM_NATIVE_FROZEN_MARKET_IDENTITY_CONFLICT")
        bars = [native_bar(symbol, b, market.day_facts) for b in market.bars]
        if len(bars) < min_bars:
            raise ValueError("LOCALSIM_NATIVE_FROZEN_BARS_MISSING")
        return MinuteExecutionMarketInput(symbol=symbol, trade_date=trade_date, source=source,
                                          minute_bars=bars, market_context={"source_identity": self.source_identity})


def native_bar(symbol, bar, facts):
    return MinuteBar(symbol=symbol, bar_time=bar.start.astimezone(CHINA),
                     open=float(bar.open), high=float(bar.high), low=float(bar.low), close=float(bar.close),
                     volume=int(bar.volume_shares), is_suspended=bool(facts.is_suspended),
                     limit_up=float(facts.limit_up) if facts.has_daily_limit else None,
                     limit_down=float(facts.limit_down) if facts.has_daily_limit else None)


def native_backend_for(*, account, markets, cost, manifest, source_identity, cash_only_trade_date=None):
    if cash_only_trade_date is not None and account["positions"]:
        raise ValueError("LOCALSIM_CASH_ONLY_SCOPE_HAS_POSITIONS")
    provider = FrozenResearchMarketProvider(package_id=account["package_id"], markets=markets,
                                            source_identity=source_identity, cash_only_trade_date=cash_only_trade_date)
    return LocalSimBackend(portfolio_id=account["portfolio_id"], package_id=account["package_id"],
                           manifest=manifest, data_source=MinuteDataSource.DATASET_FILES,
                           market_data_provider=provider, frozen_research_inputs=True,
                           initial_cash=Decimal(account["origin_initial_cash_cny"]),
                           initial_available_cash=Decimal(account["cash_cny"]),
                           initial_positions={s: PositionLot.model_validate(p) for s, p in account["positions"].items()},
                           fee_model=fee_model(cost), execution_policy=local_sim_twap_only_policy_snapshot())


def run_cash_only_portfolio_day(*, backend, trade_date, human_policy=None):
    """An authoritative empty-target day still owns a real native cash valuation."""
    if backend._ledger.positions or backend._records or backend._ledger.fills or backend._ledger.cash_entries:
        raise ValueError("LOCALSIM_CASH_ONLY_DAY_NOT_EMPTY")
    start, terminal = (utc(datetime.combine(trade_date, t, CHINA)) for t in (time(9,30),time(14,57)))
    initial = _account(backend)
    nav = Decimal(str(backend._ledger.account_snapshot(prices={}, snapshot_time=start).nav))
    result = portfolio_day_result(backend=backend, sessions=(), initial=initial, marks={},
        navs=[{"as_of":start.isoformat(), "nav_cny":str(nav)}, {"as_of":terminal.isoformat(), "nav_cny":str(nav)}],
        day=trade_date, terminal=terminal, initial_nav=nav)
    if human_policy is not None:
        from .human_operation import HumanOperationQueue
        result["account"].update(human_operation_state=HumanOperationQueue(
            account_id=backend.portfolio_id, policy=human_policy).snapshot(), human_operation_metrics={
            "action_count":0, "maximum_queue":0, "pending_cancellation_at_deadline":0,
            "cancellation_races":0, "expired_actions":0})
    return result


class _NativeExecution(TimingExecution):
    """Reuse the native transaction/feedback frame, not the timing algorithm.

    The facade only carries plan/confirmation identity. Its evaluate and
    compute_step are never called; the backend's existing TWAP adapter owns
    every proposed fill and its own persisted algorithm state.
    """

    def __init__(self, backend, *, plan, market, run_id, intent):
        super().__init__(backend, plan=plan, market=market, delay_seconds=0,
                         mode=CLOSED_LOOP_MODE, run_id=run_id, binding_id=backend.portfolio_id,
                         order_intent=intent)
        snapshot = local_sim_twap_only_policy_snapshot()
        self.native_algo_config = dict(snapshot["policy_json"].get("algo_config") or {})
        self.engine = backend._execution_engine
        self.execution_state = self.execution_state.model_copy(update={
            "algo_code": "TWAP", "schedule_version": snapshot["policy_version_id"], "state_hash": ""})
        self.execution_state = type(self.execution_state).model_validate(self.execution_state.model_dump())
        backend._register_timing_execution(self)

    def execution_bar(self, bar, facts):
        return native_bar(self.order.symbol, bar, facts)

    def confirmed_algo_state(self, engine_state):
        return engine_state.algo_state

    def snapshot(self, *, include_account=True):
        result = super().snapshot(include_account=include_account)
        result.update(execution_policy=local_sim_twap_only_policy_snapshot(),
                      liquidity_proxy="NATIVE_TWAP_MINUTE_ENGINE", operation_mode="NATIVE")
        return result


def run_native_portfolio_day(*, backend, markets, intents, initial_marks, run_id, source_commit, optimized=False):
    """Drive the same native causal TWAP engine on frozen minutes until 14:57."""
    day = next(iter(markets.values())).trade_date
    start, terminal = (utc(datetime.combine(day, t, CHINA)) for t in (time(9, 30), time(14, 57)))
    _validate_markets(markets, day, start)
    if backend._records or backend._ledger.fills or backend._ledger.cash_entries:
        raise ValueError("LOCALSIM_NATIVE_RESEARCH_FRESH_DAY_REQUIRED")
    backend._ledger.settle_trade_date(day)
    initial, marks = _account(backend), dict(initial_marks)
    initial_nav = Decimal(str(backend._ledger.account_snapshot(
        prices={s: float(p) for s, p in marks.items()}, snapshot_time=start).nav))
    policy = Policy(policy_id="FIRST_OBSERVABLE_V1", source_commit=source_commit)
    executions = []
    for intent in sorted(intents, key=lambda i: (i.side.value != "SELL", i.symbol, i.intent_id)):
        plan = Plan(plan_id="native_" + intent.intent_id, revision=1, source_kind="FROZEN_REPLAY",
                    source_ref=run_id, source_revision="1", symbol=intent.symbol, side=intent.side.value,
                    target_trade_date=day, valid_from=start, valid_until=terminal,
                    target_quantity=intent.quantity, quantity_source_ref=intent.intent_id,
                    policy_id=policy.policy_id, policy=policy, policy_sha256=digest(policy))
        executions.append(_NativeExecution(backend, plan=plan, market=markets[intent.symbol], run_id=run_id, intent=intent))
    bars, observations = {}, {}
    for symbol, market in markets.items():
        for bar in market.bars:
            available = max(bar.end, bar.received_at, bar.source_available_at or bar.end)
            if start <= bar.start < terminal and available <= terminal:
                bars.setdefault(bar.start, {})[symbol] = bar
                observations.setdefault(available, {})[symbol] = bar
    views, navs = [(start, initial)], [{"as_of": start.isoformat(), "nav_cny": str(initial_nav)}]
    account_index = VisibleAccountIndex(views, run_id) if optimized else None
    for clock in sorted({terminal} | set(bars) | set(observations)):
        any_fill = False
        for execution in executions:
            bar = bars.get(clock, {}).get(execution.order.symbol)
            if bar is not None and execution.order.status not in TERMINAL:
                any_fill = bool(execution.step(bar)) or any_fill
        if any_fill:
            views.append((clock + timedelta(seconds=65), _account(backend)))
        for symbol, bar in observations.get(clock, {}).items():
            marks[symbol] = bar.close
        if clock in observations or clock == terminal:
            if account_index is not None:
                nav = account_index.nav(backend._ledger, marks=marks, clock=clock)
            else:
                _, visible = max((v for v in views if v[0] <= clock), key=lambda v: v[0])
                valuation = deepcopy(backend._ledger)
                valuation.cash = Decimal(visible["cash_cny"])
                valuation.positions = {s: PositionLot.model_validate(p) for s, p in visible["positions"].items()}
                nav = valuation.account_snapshot(prices={s: float(p) for s, p in marks.items()}, snapshot_time=clock).nav
            navs.append({"as_of": clock.isoformat(), "nav_cny": str(nav)})
    for execution in executions:
        execution.cancel("COMMON_TERMINAL_1457")
        execution.waits = [(f.quantity, Decimal(str((f.trade_time - start).total_seconds()))) for f in execution.fills]
    result = portfolio_day_result(backend=backend, sessions=executions, initial=initial, marks=marks,
                                  navs=navs, day=day, terminal=terminal, initial_nav=initial_nav)
    result["account"].update(operation_mode="NATIVE", native_execution_policy=payload(local_sim_twap_only_policy_snapshot()),
                             execution_contract="NATIVE_CAUSAL_MINUTE_TWAP")
    return result
