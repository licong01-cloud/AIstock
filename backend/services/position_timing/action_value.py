"""Pure daily action-value inputs and execution, shared by research and advice.

Blueprint v2.5 F-029/F-030. No file, database, scheduler or order operations.
Prices entering this module are RAW CNY; factors enter only economic returns.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from backend.execution_algos.board_lot import round_to_board_lot
from backend.services.trading_core.exit_guard import ExitGuardContext, evaluate as evaluate_exit
from backend.services.trading_core.price_guard import PriceGuardContext, evaluate as evaluate_price

from .contracts import TriggerSide, canonical_sha256
from .policy import (
    COST_POLICY_SHA256,
    EXIT_GUARD_SNAPSHOT_ARTIFACT_SHA256,
    PRICE_GUARD_SNAPSHOT_ARTIFACT_SHA256,
    component_cost_for_parent_notionals,
    frozen_exit_guard_policy,
    frozen_price_guard_policy,
    split_legal_parent_order_quantities,
)


TZ = ZoneInfo("Asia/Shanghai")
ZERO = Decimal(0)
BPS = Decimal(10000)
EXPOSURES = (Decimal(0), Decimal(".25"), Decimal(".5"), Decimal(1))
CORE_INFORMATION_BLOCK = "CORE_ONLY"
ATR14_INFORMATION_BLOCK = "ATR14_SMA_GAP_RANGE_V1"
SW_L2_INFORMATION_BLOCK = "SW_L2_RELATIVE_MOMENTUM_20D_V1"
MONEYFLOW_INFORMATION_BLOCK = "MAIN_NET_FLOW_RATIO_5D_LAG1_V1"
MARKET_FEATURES = (
    "return_1d_bps", "return_3d_bps", "return_5d_bps", "return_20d_bps",
    "close_to_ema20_bps", "ema20_slope_10d_bps", "realized_vol_20d_bps",
    "downside_semivol_20d_bps", "intraday_range_bps", "close_location_in_day",
    "volume_ratio_5d_to_20d", "relative_csi300_return_20d_bps",
    "csi300_return_20d_bps", "csi300_vol_20d_bps",
)
ATR14_MARKET_FEATURES = MARKET_FEATURES + ("atr14_sma_bps",)
STATE_FEATURES = (
    "holding_exposure", "cash_fraction", "action_fraction", "log1p_capital_cny",
    "estimated_leg_cost_bps", "holding_age", "holding_age_missing",
    "unrealized_return_bps", "entry_cost_missing",
)
FEATURE_ORDER = MARKET_FEATURES + STATE_FEATURES
FEATURE_SPEC = {
    "schema": "position_timing_core_features_v2",
    "feature_order": FEATURE_ORDER,
    "price_basis": "RAW_CNY_WITH_FACTOR_RATIO_FOR_RETURNS",
    "decision_time": "20:00:00+08:00",
    "ema": "SPAN20_ADJUST_FALSE_MIN_PERIODS20",
    "volatility": "20_SESSION_DDOF1",
    "imputer": "TRAIN_ONLY_MEDIAN_WITH_EXPLICIT_STATE_MISSING_MASKS",
    "optional_blocks": (),
    "benchmark": "000300.SH_PRICE_INDEX_NO_STOCK_FACTOR",
}
FEATURE_SPEC_SHA256 = canonical_sha256(FEATURE_SPEC)
ATR14_FEATURE_ORDER = ATR14_MARKET_FEATURES + STATE_FEATURES
ATR14_FEATURE_SPEC = {
    **FEATURE_SPEC,
    "schema": "position_timing_core_plus_atr14_sma_features_v1",
    "feature_order": ATR14_FEATURE_ORDER,
    "optional_blocks": (ATR14_INFORMATION_BLOCK,),
    "atr14_sma_bps": (
        "SMA14_MAX(ADJ_HIGH-ADJ_LOW,ABS(ADJ_HIGH-PREV_ADJ_CLOSE),"
        "ABS(ADJ_LOW-PREV_ADJ_CLOSE))/ADJ_CLOSE*10000_NO_FORWARD_FILL"
    ),
}
ATR14_FEATURE_SPEC_SHA256 = canonical_sha256(ATR14_FEATURE_SPEC)
SW_L2_MARKET_FEATURES = MARKET_FEATURES + (
    "sw_l2_return_20d_bps",
    "relative_sw_l2_return_20d_bps",
)
SW_L2_FEATURE_ORDER = SW_L2_MARKET_FEATURES + STATE_FEATURES
SW_L2_FEATURE_SPEC = {
    **FEATURE_SPEC,
    "schema": "position_timing_core_plus_sw_l2_relative_momentum_features_v1",
    "feature_order": SW_L2_FEATURE_ORDER,
    "optional_blocks": (SW_L2_INFORMATION_BLOCK,),
    "sw_l2_return_20d_bps": (
        "CURRENT_PIT_L2_INDEPENDENT_CLOSE_T/CLOSE_T_MINUS_20_MINUS_1_TIMES_10000_NO_FORWARD_FILL"
    ),
    "relative_sw_l2_return_20d_bps": "RETURN_20D_BPS_MINUS_SW_L2_RETURN_20D_BPS",
    "l2_code_usage": "PIT_JOIN_KEY_ONLY_NOT_MODEL_FEATURE",
}
SW_L2_FEATURE_SPEC_SHA256 = canonical_sha256(SW_L2_FEATURE_SPEC)
MONEYFLOW_MARKET_FEATURES = MARKET_FEATURES + ("main_net_flow_ratio_5d_lag1_bps",)
MONEYFLOW_FEATURE_ORDER = MONEYFLOW_MARKET_FEATURES + STATE_FEATURES
MONEYFLOW_FEATURE_SPEC = {
    **FEATURE_SPEC,
    "schema": "position_timing_core_plus_main_net_flow_ratio_5d_lag1_features_v1",
    "feature_order": MONEYFLOW_FEATURE_ORDER,
    "optional_blocks": (MONEYFLOW_INFORMATION_BLOCK,),
    "main_net_flow_ratio_5d_lag1_bps": (
        "SHIFT_ONE_GLOBAL_TRADING_SESSION(SUM_5D(LG_BUY_AMT+ELG_BUY_AMT-"
        "LG_SELL_AMT-ELG_SELL_AMT)/SUM_5D(DAILY_AMOUNT)*10000)_NO_FORWARD_FILL"
    ),
    "moneyflow_unit_contract": "tushare_moneyflow_shares_yuan_v1",
    "availability_policy": "T_MINUS_1_GLOBAL_SESSION_END_OF_DAY_CONSERVATIVE",
}
MONEYFLOW_FEATURE_SPEC_SHA256 = canonical_sha256(MONEYFLOW_FEATURE_SPEC)


class ActionValueError(ValueError):
    def __init__(self, code: str, **details: Any) -> None:
        self.code, self.details = code, details
        super().__init__(code)


def money(value: Any) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ActionValueError("NON_FINITE_MONEY")
    return result


def cutoff_on(day: date) -> datetime:
    return datetime.combine(day, time(20), tzinfo=TZ)


def require_causal(value: datetime, cutoff: datetime, *, field: str) -> None:
    if value.tzinfo is None or cutoff.tzinfo is None or value > cutoff:
        raise ActionValueError("FEATURE_NOT_AVAILABLE_AT_CUTOFF", field=field)


def normalize_export_bars(frame: pd.DataFrame, *, benchmark: bool = False) -> pd.DataFrame:
    """Invert authoritative_bin_exporter daily units, not a Qlib guess.

    Stock OHLC=raw*qfq and volume=shares/qfq; limits are already raw.
    The same export's CSI300 component is explicitly a raw price index.
    """
    result = frame.copy()
    fields = ("open", "high", "low", "close", "volume")
    if not set(fields).issubset(result):
        raise ActionValueError("DAILY_EXPORT_SCHEMA_MISSING")
    if benchmark:
        result["factor"] = 1.0
    else:
        if "factor" not in result:
            raise ActionValueError("ADJUSTMENT_FACTOR_MISSING")
        factor = pd.to_numeric(result["factor"], errors="coerce")
        present = result[list(fields)].notna().any(axis=1)
        if ((~np.isfinite(factor) | (factor <= 0)) & present).any():
            raise ActionValueError("ADJUSTMENT_FACTOR_INVALID")
        result[list(fields[:4])] = result[list(fields[:4])].div(factor, axis=0)
        result["volume"] = result["volume"] * factor
    return result


def feature_contract(information_block: str = CORE_INFORMATION_BLOCK) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    if information_block == CORE_INFORMATION_BLOCK:
        return MARKET_FEATURES, FEATURE_ORDER, FEATURE_SPEC_SHA256
    if information_block == ATR14_INFORMATION_BLOCK:
        return ATR14_MARKET_FEATURES, ATR14_FEATURE_ORDER, ATR14_FEATURE_SPEC_SHA256
    if information_block == SW_L2_INFORMATION_BLOCK:
        return SW_L2_MARKET_FEATURES, SW_L2_FEATURE_ORDER, SW_L2_FEATURE_SPEC_SHA256
    if information_block == MONEYFLOW_INFORMATION_BLOCK:
        return MONEYFLOW_MARKET_FEATURES, MONEYFLOW_FEATURE_ORDER, MONEYFLOW_FEATURE_SPEC_SHA256
    raise ActionValueError("INFORMATION_BLOCK_UNSUPPORTED", information_block=information_block)


def market_features(
    bars: pd.DataFrame,
    benchmark: pd.Series,
    *,
    information_block: str = CORE_INFORMATION_BLOCK,
) -> pd.DataFrame:
    """Trailing-only features on an already reindexed global trading calendar.

    Missing sessions are not dropped/forward-filled. Source adapters must keep
    their available-at identity; this function cannot invent publication time.
    """
    required = {"open", "high", "low", "close", "volume", "factor"}
    if not required.issubset(bars):
        raise ActionValueError("CORE_SOURCE_SCHEMA_MISSING", missing=sorted(required - set(bars)))
    if not bars.index.is_unique or not bars.index.is_monotonic_increasing:
        raise ActionValueError("CORE_CALENDAR_NOT_STRICTLY_ORDERED")
    b = bars.astype({name: float for name in required})
    close = b["close"] * b["factor"]
    close = close.where((b["close"] > 0) & (b["factor"] > 0))
    ret = close.pct_change(fill_method=None)
    ema = close.ewm(span=20, adjust=False, min_periods=20).mean()
    # ewm otherwise carries through a missing bar: require the whole window.
    complete20 = close.rolling(20, min_periods=20).count().eq(20)
    out = pd.DataFrame(index=b.index)
    for window in (1, 3, 5, 20):
        out[f"return_{window}d_bps"] = (close / close.shift(window) - 1) * 10000
    out["close_to_ema20_bps"] = ((close / ema - 1) * 10000).where(complete20)
    out["ema20_slope_10d_bps"] = ((ema / ema.shift(10) - 1) * 10000).where(complete20)
    out["realized_vol_20d_bps"] = ret.rolling(20, min_periods=20).std(ddof=1) * 10000
    out["downside_semivol_20d_bps"] = (
        ret.clip(upper=0).pow(2).rolling(20, min_periods=20).mean().pow(.5) * 10000
    )
    day_range = b["high"] - b["low"]
    out["intraday_range_bps"] = day_range / b["close"] * 10000
    # A known flat bar has a neutral location; missing OHLC is never neutral.
    out["close_location_in_day"] = (b["close"] - b["low"]) / day_range
    out.loc[day_range.eq(0) & b["close"].notna(), "close_location_in_day"] = .5
    out["volume_ratio_5d_to_20d"] = (
        b["volume"].rolling(5, min_periods=5).mean()
        / b["volume"].rolling(20, min_periods=20).mean()
    )
    index_close = benchmark.reindex(b.index).where(lambda x: x > 0)
    index_ret = (index_close / index_close.shift(20) - 1) * 10000
    out["relative_csi300_return_20d_bps"] = out["return_20d_bps"] - index_ret
    out["csi300_return_20d_bps"] = index_ret
    out["csi300_vol_20d_bps"] = index_close.pct_change(fill_method=None).rolling(
        20, min_periods=20
    ).std(ddof=1) * 10000
    market_names, _, _ = feature_contract(information_block)
    if information_block == ATR14_INFORMATION_BLOCK:
        adjusted_high = b["high"] * b["factor"]
        adjusted_low = b["low"] * b["factor"]
        previous_adjusted_close = close.shift(1)
        true_range = pd.concat(
            (
                adjusted_high - adjusted_low,
                (adjusted_high - previous_adjusted_close).abs(),
                (adjusted_low - previous_adjusted_close).abs(),
            ),
            axis=1,
        ).max(axis=1, skipna=False)
        out["atr14_sma_bps"] = (
            true_range.rolling(14, min_periods=14).mean() / close * 10000
        )
    elif information_block == SW_L2_INFORMATION_BLOCK:
        if "sw_l2_return_20d_bps" not in bars:
            raise ActionValueError("SW_L2_FEATURE_SOURCE_MISSING")
        sector_return = pd.to_numeric(bars["sw_l2_return_20d_bps"], errors="coerce")
        out["sw_l2_return_20d_bps"] = sector_return
        out["relative_sw_l2_return_20d_bps"] = out["return_20d_bps"] - sector_return
    elif information_block == MONEYFLOW_INFORMATION_BLOCK:
        if "main_net_flow_ratio_5d_lag1_bps" not in bars:
            raise ActionValueError("MONEYFLOW_FEATURE_SOURCE_MISSING")
        out["main_net_flow_ratio_5d_lag1_bps"] = pd.to_numeric(
            bars["main_net_flow_ratio_5d_lag1_bps"], errors="coerce"
        )
    return out.loc[:, market_names].replace([np.inf, -np.inf], np.nan)


@dataclass(frozen=True)
class PositionState:
    quantity: int
    sellable: int
    cash: Decimal
    capital: Decimal
    entry_cost: Decimal | None = None
    holding_age: int | None = None

    def __post_init__(self) -> None:
        if (any(isinstance(value, bool) or not isinstance(value, (int, np.integer)) for value in (self.quantity, self.sellable))
                or not 0 <= self.sellable <= self.quantity or self.quantity < 0):
            raise ActionValueError("POSITION_QUANTITY_INVALID")
        if not self.cash.is_finite() or self.cash < 0 or not self.capital.is_finite() or self.capital <= 0:
            raise ActionValueError("POSITION_CAPITAL_INVALID")
        if self.entry_cost is not None and (not self.entry_cost.is_finite() or self.entry_cost <= 0):
            raise ActionValueError("ENTRY_COST_INVALID")
        if self.holding_age is not None and (isinstance(self.holding_age, bool)
                or not isinstance(self.holding_age, (int, np.integer)) or self.holding_age < 0):
            raise ActionValueError("HOLDING_AGE_INVALID")


@dataclass(frozen=True)
class ActionPlan:
    symbol: str
    delta: int
    reference: Decimal
    risk_exit: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.delta, bool) or not isinstance(self.delta, (int, np.integer)):
            raise ActionValueError("ACTION_QUANTITY_INVALID")
        if not self.reference.is_finite() or self.reference <= 0 or (self.risk_exit and self.delta >= 0):
            raise ActionValueError("ACTION_REFERENCE_OR_SIDE_INVALID")

    @property
    def side(self) -> str:
        return "BUY" if self.delta > 0 else "SELL" if self.delta < 0 else "NONE"


@dataclass(frozen=True)
class Fill:
    status: str
    delta: int = 0
    price: Decimal | None = None
    fee: Decimal = ZERO
    reason: str = ""

    def __post_init__(self) -> None:
        if self.status not in {"FILLED", "NO_FILL", "NO_ACTION", "UNKNOWN"}:
            raise ActionValueError("FILL_STATUS_INVALID")
        if not self.fee.is_finite() or self.fee < 0:
            raise ActionValueError("FILL_FEE_INVALID")
        if isinstance(self.delta, bool) or not isinstance(self.delta, (int, np.integer)):
            raise ActionValueError("FILL_QUANTITY_INVALID")
        if self.status != "FILLED" and (self.delta or self.fee or self.price is not None):
            raise ActionValueError("NON_FILL_WITH_EXECUTION_VALUES")


def leg_fee(symbol: str, delta: int, price: Decimal, *, parent_count: int = 1, full_exit: bool = False) -> Decimal:
    if delta == 0:
        return ZERO
    side = TriggerSide.BUY if delta > 0 else TriggerSide.SELL
    parts = split_legal_parent_order_quantities(
        quantity=abs(delta), symbol=symbol, side=side, requested_count=parent_count, full_exit=full_exit
    )
    if parts is None:
        raise ActionValueError("LEGAL_PARENT_SPLIT_UNAVAILABLE")
    return component_cost_for_parent_notionals(side=side, notionals=[price * q for q in parts])["total"]


def action_candidates(symbol: str, state: PositionState, price: Decimal, *, max_exposure: Decimal = Decimal(1)) -> tuple[ActionPlan, ...]:
    if price <= 0 or not ZERO <= max_exposure <= 1:
        raise ActionValueError("ACTION_BUDGET_INVALID")
    deltas = {0}
    # Reserve at the highest frozen buy trigger, not only the signal close.
    max_price = price * (1 + money(frozen_price_guard_policy().buy["max_chase_bps"]) / BPS)
    for exposure in EXPOSURES:
        if exposure > max_exposure:
            continue
        target = round_to_board_lot(int(state.capital * exposure / price), symbol, side="BUY")
        delta = target - state.quantity
        if delta < 0:
            quantity = min(-delta, state.sellable)
            if quantity != state.quantity:
                quantity = round_to_board_lot(quantity, symbol, side="SELL", allow_sell_residual=False)
            delta = -quantity
        elif delta > 0:
            delta = round_to_board_lot(delta, symbol, side="BUY")
            # Monotone legal-quantity search; STAR increments by one share, so
            # decrementing every share would make large budgets needlessly slow.
            low, high = 0, delta
            while low < high:
                middle = (low + high + 1) // 2
                quantity = round_to_board_lot(middle, symbol, side="BUY")
                if max_price * quantity + leg_fee(symbol, quantity, max_price) <= state.cash:
                    low = middle
                else:
                    high = middle - 1
            delta = round_to_board_lot(low, symbol, side="BUY")
        deltas.add(delta)
    return tuple(ActionPlan(symbol, delta, price) for delta in sorted(deltas))


def state_features(state: PositionState, plan: ActionPlan) -> dict[str, float]:
    if state.capital <= 0:
        raise ActionValueError("POSITION_CAPITAL_INVALID")
    p = plan.reference
    fee = leg_fee(plan.symbol, plan.delta, p, full_exit=-plan.delta == state.quantity) if plan.delta else ZERO
    return {
        "holding_exposure": float(state.quantity * p / state.capital),
        "cash_fraction": float(state.cash / state.capital),
        "action_fraction": float(plan.delta * p / state.capital),
        "log1p_capital_cny": float(np.log1p(float(state.capital))),
        "estimated_leg_cost_bps": float(fee / state.capital * BPS),
        "holding_age": float(state.holding_age) if state.holding_age is not None else np.nan,
        "holding_age_missing": float(state.holding_age is None),
        "unrealized_return_bps": float((p / state.entry_cost - 1) * BPS) if state.entry_cost else np.nan,
        "entry_cost_missing": float(state.entry_cost is None),
    }


def risk_exit_plan(symbol: str, state: PositionState, price: Decimal, *, delisted: bool = False) -> ActionPlan | None:
    if not state.quantity:
        return None
    sellable = state.sellable if state.sellable == state.quantity else round_to_board_lot(
        state.sellable, symbol, side="SELL", allow_sell_residual=False
    )
    if delisted:
        return ActionPlan(symbol, -sellable, price, True) if sellable else None
    if state.entry_cost is None:
        return None  # Unknown cost is a typed feature mask, never a fabricated cost.
    result = evaluate_exit(
        ExitGuardContext(actual_entry_cost=float(state.entry_cost), current_price=float(price),
                         days_since_entry=state.holding_age, t1_eligible=state.sellable > 0,
                         suspend_status="ACTIVE", price_basis="raw"),
        frozen_exit_guard_policy(),
    )
    return ActionPlan(symbol, -sellable, price, True) if result.should_exit and sellable else None


def choose_action(plans: Sequence[ActionPlan], predicted_values: Sequence[float]) -> ActionPlan:
    if len(plans) != len(predicted_values) or not plans:
        raise ActionValueError("ACTION_PREDICTION_SHAPE_INVALID")
    if any(not np.isfinite(value) for value in predicted_values):
        raise ActionValueError("ACTION_PREDICTION_NON_FINITE")
    if not any(plan.delta == 0 for plan in plans):
        raise ActionValueError("NO_TRADE_BASELINE_MISSING")
    eligible = [(float(value) if plan.delta else 0.0, plan) for plan, value in zip(plans, predicted_values)]
    # A zero or negative value cannot beat no-trade. Ties minimize turnover.
    return min(eligible, key=lambda pair: (-pair[0], abs(pair[1].delta), pair[1].delta))[1]


def quote_fill(plan: ActionPlan, *, open_price: Decimal, price: Decimal, up_limit: Decimal, down_limit: Decimal,
               sellable: int, parent_count: int = 1, full_exit: bool = False) -> Fill:
    if plan.delta == 0:
        return Fill("NO_ACTION", reason="NO_ACTION")
    if min(open_price, price, up_limit, down_limit) <= 0 or down_limit >= up_limit:
        return Fill("UNKNOWN", reason="TRADING_PRICE_UNAVAILABLE")
    if (plan.delta > 0 and price >= up_limit) or (plan.delta < 0 and price <= down_limit):
        return Fill("NO_FILL", reason="DIRECTIONAL_LIMIT_BLOCKED")
    if plan.delta < 0 and -plan.delta > sellable:
        return Fill("NO_FILL", reason="T1_SELLABLE_INSUFFICIENT")
    if plan.risk_exit:
        delta = plan.delta
    else:
        result = evaluate_price(
            PriceGuardContext(signal_ref_price=float(plan.reference), prev_close=float(plan.reference),
                              current_price=float(price), open_gap_bps=float((open_price / plan.reference - 1) * BPS),
                              current_gap_bps=float((price / plan.reference - 1) * BPS),
                              limit_up=float(up_limit), limit_down=float(down_limit), side=plan.side.lower(),
                              sell_reason="rebalance" if plan.delta < 0 else None, price_basis="raw"),
            frozen_price_guard_policy(),
        )
        if result.action not in {"ACCEPT", "REDUCE", "SELL"}:
            return Fill("NO_FILL", reason=result.reason_code)
        delta = plan.delta
        if result.action == "REDUCE":
            delta = round_to_board_lot(int(delta * money(result.size_multiplier)), plan.symbol, side="BUY")
        if not delta:
            return Fill("NO_FILL", reason="DELTA_BELOW_BOARD_LOT")
    return Fill("FILLED", delta, price, leg_fee(plan.symbol, delta, price, parent_count=parent_count, full_exit=full_exit), "FROZEN_GUARD_ACCEPTED")


def daily_fill(plan: ActionPlan, bar: Mapping[str, Any] | None, *, sellable: int,
               parent_count: int = 1, full_exit: bool = False, slippage_bps: Decimal = ZERO) -> Fill:
    """Conservative daily proxy with the same shared guard as a live quote.

    No-fill remains cash/holdings; absent market inputs are UNKNOWN, not no-fill.
    """
    if not slippage_bps.is_finite() or slippage_bps < 0:
        raise ActionValueError("SLIPPAGE_SCENARIO_INVALID")
    if plan.delta == 0:
        return Fill("NO_ACTION", reason="NO_ACTION")
    if bar is None:
        return Fill("UNKNOWN", reason="TARGET_BAR_MISSING")
    if bar.get("is_suspended"):
        return Fill("NO_FILL", reason="TARGET_DAY_SUSPENDED")
    keys = ("open", "high", "low", "close", "up_limit", "down_limit")
    try:
        values = {key: money(bar[key]) for key in keys}
    except (KeyError, ValueError, ArithmeticError):
        return Fill("UNKNOWN", reason="TARGET_BAR_INVALID")
    if min(values.values()) <= 0 or not values["low"] <= min(values["open"], values["close"]) <= max(values["open"], values["close"]) <= values["high"]:
        return Fill("UNKNOWN", reason="TARGET_OHLC_INVALID")
    args = {"up_limit": values["up_limit"], "down_limit": values["down_limit"], "sellable": sellable,
            "parent_count": parent_count, "full_exit": full_exit, "open_price": values["open"]}
    opened = quote_fill(plan, price=values["open"], **args)
    if opened.status == "FILLED" or plan.risk_exit:
        selected = opened
    else:
        policy = frozen_price_guard_policy()
        if plan.delta > 0:
            thresholds = [plan.reference * (1 + money(policy.buy[key]) / BPS) for key in ("yellow_chase_bps", "max_chase_bps")]
            rounding = ROUND_FLOOR
        else:
            thresholds = [plan.reference * (1 - money(policy.sell["rebalance_max_slippage_bps"]) / BPS)]
            rounding = ROUND_CEILING
        candidates = []
        for threshold in thresholds:
            threshold = threshold.quantize(Decimal(".01"), rounding=rounding)
            if values["low"] <= threshold <= values["high"]:
                fill = quote_fill(plan, price=threshold, **args)
                if fill.status == "FILLED":
                    candidates.append(fill)
        selected = min(candidates, key=lambda f: (abs(f.delta), -f.price if f.delta > 0 else f.price)) if candidates else opened
    if selected.status != "FILLED" or not slippage_bps:
        return selected
    price = selected.price * (1 + slippage_bps / BPS * (1 if selected.delta > 0 else -1))
    price = price.quantize(Decimal(".01"), rounding=ROUND_CEILING if selected.delta > 0 else ROUND_FLOOR)
    if not values["down_limit"] < price < values["up_limit"]:
        return Fill("NO_FILL", reason="SLIPPAGE_EXCEEDS_LEGAL_PRICE")
    if not values["low"] <= price <= values["high"]:
        return Fill("NO_FILL", reason="SLIPPAGE_EXCEEDS_OBSERVED_RANGE")
    # Re-evaluate the original quantity once: crossing green/yellow must not
    # bypass the guard or halve a previously reduced quantity a second time.
    repriced = quote_fill(plan, price=price, **args)
    return replace(repriced, reason="SLIPPAGE_SCENARIO") if repriced.status == "FILLED" else repriced


def apply_fill(state: PositionState, fill: Fill) -> PositionState:
    if fill.status == "UNKNOWN":
        raise ActionValueError("PATH_VALUATION_UNKNOWN", reason=fill.reason)
    if fill.status != "FILLED":
        return state
    if fill.price is None or fill.price <= 0 or not fill.delta:
        raise ActionValueError("FILL_INVALID")
    cash = state.cash - fill.delta * fill.price - fill.fee
    quantity = state.quantity + fill.delta
    if cash < 0 or quantity < 0 or (fill.delta < 0 and -fill.delta > state.sellable):
        raise ActionValueError("CASH_OR_POSITION_INVARIANT")
    cost = state.entry_cost
    if fill.delta > 0:
        cost = ((cost * state.quantity if cost is not None else ZERO) + fill.delta * fill.price + fill.fee) / quantity if cost is not None or state.quantity == 0 else None
    if not quantity:
        cost = None
    return replace(state, quantity=quantity, sellable=state.sellable + min(0, fill.delta), cash=cash,
                   entry_cost=cost, holding_age=None if not quantity else 0 if state.quantity == 0 else state.holding_age)


POLICY_IDENTITY = {
    "policy": "DAILY_ACTION_VALUE_POLICY_V2", "feature_spec_sha256": FEATURE_SPEC_SHA256,
    "exposures": EXPOSURES, "economic_threshold_bps": 0,
    "cost_policy_sha256": COST_POLICY_SHA256,
    "exit_guard_snapshot_sha256": EXIT_GUARD_SNAPSHOT_ARTIFACT_SHA256,
    "price_guard_snapshot_sha256": PRICE_GUARD_SNAPSHOT_ARTIFACT_SHA256,
    "fill_policy": "DAILY_SHARED_GUARD_CONSERVATIVE_V2",
}
POLICY_SHA256 = canonical_sha256(POLICY_IDENTITY)


def policy_sha256_for(information_block: str = CORE_INFORMATION_BLOCK) -> str:
    _, _, feature_spec_sha256 = feature_contract(information_block)
    if information_block == CORE_INFORMATION_BLOCK:
        return POLICY_SHA256
    return canonical_sha256({**POLICY_IDENTITY, "feature_spec_sha256": feature_spec_sha256})
