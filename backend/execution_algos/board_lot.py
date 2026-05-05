"""Chinese A-share board-lot rules and cost-aware sizing helpers.

Board minimum quantity and increment per A-share board prefix.

Source rules (verified 2026-05-04):
- Main board (60xxxx 沪市主板, 00xxxx 深市主板): >=100 shares per order, multiples of 100.
- ChiNext (300xxx, 301xxx 创业板): >=100 shares per order, multiples of 100.
- STAR market (688xxx, 689xxx 科创板): >=200 shares per order, may increment by 1
  share above 200; sell-side residual below 200 must be flushed in one fill.

These rules are used by V25.1 onwards. Legacy V20/V24/TWAP keep
``BaseExecutionAlgo._round_lot`` (lot_size=100) for backward compatibility.
"""
from __future__ import annotations

import math
from typing import Tuple

import numpy as np


__all__ = [
    "BoardLotRule",
    "board_lot_rule",
    "round_to_board_lot",
    "min_slice_qty_for_cost",
    "build_cost_aware_bucket_schedule",
]


class BoardLotRule(Tuple[int, int]):
    """(min_qty, increment) tuple alias kept for type clarity."""


def board_lot_rule(stock_id: str) -> Tuple[int, int]:
    """Return (min_qty, increment) for a stock based on its prefix.

    The contract is intentionally strict: any unrecognized prefix raises so
    the caller cannot silently fall through to a wrong rule. Adapters that
    receive non-A-share symbols must filter them out before reaching here.
    """

    if not stock_id:
        raise ValueError("board_lot_rule requires a non-empty stock_id")
    code = str(stock_id).split(".")[0].strip()
    if not code or not code.isdigit() or len(code) != 6:
        raise ValueError(f"board_lot_rule expects a 6-digit A-share code, got {stock_id!r}")
    if code.startswith(("688", "689")):
        return 200, 1
    if code.startswith(("300", "301", "302")):
        return 100, 100
    if code.startswith(("60", "00")):
        return 100, 100
    raise ValueError(f"board_lot_rule does not recognise prefix for {stock_id!r}")


def _floor_to_increment(qty: int, increment: int) -> int:
    if increment <= 0:
        raise ValueError("increment must be positive")
    return (int(qty) // increment) * increment


def _ceil_to_increment(qty: int, increment: int) -> int:
    if increment <= 0:
        raise ValueError("increment must be positive")
    q = int(math.ceil(qty / increment))
    return q * increment


def round_to_board_lot(qty: int, stock_id: str, *, side: str = "BUY") -> int:
    """Floor ``qty`` to a legal board-lot quantity.

    For BUY side the result is always a legal increment multiple, so it may
    return 0 when the requested quantity is below the board minimum.

    For SELL side residuals below the board minimum are preserved verbatim
    (the exchange residual rule lets a holder dump < min_qty in one shot).
    Callers that want to enforce min_qty must check ``qty >= min_qty`` first.
    """

    min_qty, increment = board_lot_rule(stock_id)
    qty = int(qty)
    if qty <= 0:
        return 0
    side_norm = str(side or "").strip().upper()
    if side_norm == "SELL" and qty < min_qty:
        return qty  # residual flush is legal for SELL
    floored = _floor_to_increment(qty, increment)
    if floored < min_qty:
        return 0
    return floored


def min_slice_qty_for_cost(
    *,
    price: float,
    stock_id: str,
    min_cost: float,
    commission_rate: float,
    tolerance_bps: float,
) -> int:
    """Compute the minimum legal child-order qty so commission overshoot is bounded.

    Solve ``amount * commission_rate + amount * tolerance >= min_cost`` for
    amount, then convert to a board-legal qty (rounding UP). Always returns
    at least the board minimum.
    """

    if price <= 0 or not math.isfinite(price):
        raise ValueError("min_slice_qty_for_cost requires positive finite price")
    if commission_rate <= 0:
        raise ValueError("commission_rate must be positive")
    if tolerance_bps < 0:
        raise ValueError("tolerance_bps must be non-negative")
    if min_cost < 0:
        raise ValueError("min_cost must be non-negative")
    min_qty, increment = board_lot_rule(stock_id)
    tolerance = float(tolerance_bps) / 10000.0
    rate = float(commission_rate) + tolerance
    if rate <= 0:
        raise ValueError("commission_rate + tolerance must be positive")
    amount_floor = float(min_cost) / rate
    raw_qty = int(math.ceil(amount_floor / float(price)))
    qty = max(min_qty, _ceil_to_increment(raw_qty, increment))
    return qty


def build_cost_aware_bucket_schedule(
    *,
    plan: np.ndarray,
    total_qty: int,
    cur_price: float,
    stock_id: str,
    min_cost: float,
    commission_rate: float,
    tolerance_bps: float,
    max_buckets: int,
    horizon: int,
) -> dict[int, int]:
    """Project a per-minute weight ``plan`` onto a board-legal bucket schedule.

    Returns ``{bar_index: shares_to_emit}``. Bars not in the dict get 0 fills
    (the caller must roll any residual onto the final bar).

    Behaviour:
    - ``total_qty < min_qty``: a single residual fill at the final bar (caller
      decides whether to emit it; this respects the SELL-side residual rule).
    - ``total_qty < 2 * min_slice_qty``: a single fill at the plan-argmax bar.
    - Otherwise the plan's cumulative distribution is split into ``n_buckets``
      equal-weight segments (capped by ``max_buckets``) and each bucket places
      one fill at the segment's local argmax bar. Each fill is at least
      ``min_slice_qty`` and a multiple of the board increment; the residual
      (if any) is folded into the last bucket.
    """

    if not isinstance(plan, np.ndarray):
        raise TypeError("plan must be a numpy array")
    if plan.ndim != 1 or plan.size == 0:
        raise ValueError("plan must be a non-empty 1D array")
    if horizon <= 0 or horizon > plan.size:
        raise ValueError("horizon must satisfy 0 < horizon <= len(plan)")
    if total_qty <= 0:
        return {}
    if cur_price <= 0 or not math.isfinite(cur_price):
        raise ValueError("cur_price must be positive finite")
    if max_buckets <= 0:
        raise ValueError("max_buckets must be positive")

    min_qty, increment = board_lot_rule(stock_id)
    last_bar = horizon - 1

    # Sub-min-qty residual: emit as one trailing fill (legal only for SELL,
    # caller enforces side-specific legality).
    if total_qty < min_qty:
        return {last_bar: int(total_qty)}

    # total_qty must be increment-legal. Floor down so we never overshoot.
    legal_total = _floor_to_increment(int(total_qty), increment)
    if legal_total < min_qty:
        return {last_bar: int(total_qty)}

    min_slice_qty = min_slice_qty_for_cost(
        price=cur_price,
        stock_id=stock_id,
        min_cost=min_cost,
        commission_rate=commission_rate,
        tolerance_bps=tolerance_bps,
    )

    if legal_total < 2 * min_slice_qty:
        argmax_bar = int(np.argmax(plan[:horizon]))
        return {argmax_bar: legal_total}

    n_buckets = legal_total // min_slice_qty
    n_buckets = int(min(n_buckets, max_buckets, horizon))
    n_buckets = max(n_buckets, 1)
    if n_buckets <= 1:
        argmax_bar = int(np.argmax(plan[:horizon]))
        return {argmax_bar: legal_total}

    # Base allocation per bucket, floored to increment, ensuring >= min_qty.
    base_per = legal_total // n_buckets
    base_per = _floor_to_increment(base_per, increment)
    if base_per < max(min_qty, increment):
        argmax_bar = int(np.argmax(plan[:horizon]))
        return {argmax_bar: legal_total}

    # Bucket boundaries via cumulative plan distribution.
    plan_h = np.asarray(plan[:horizon], dtype=np.float64)
    plan_sum = float(plan_h.sum())
    if plan_sum <= 0:
        raise ValueError("plan weights sum to zero in horizon window")
    cum = np.cumsum(plan_h) / plan_sum

    bars: list[int] = []
    used: set[int] = set()
    for i in range(n_buckets):
        target = (i + 0.5) / n_buckets
        idx = int(np.searchsorted(cum, target, side="right"))
        idx = max(0, min(idx, horizon - 1))
        # If a previous bucket already claimed this bar, walk forward to the
        # next free bar; this happens when the plan is highly peaked.
        while idx in used and idx < horizon - 1:
            idx += 1
        if idx in used:
            # All later bars used; walk backwards.
            j = idx - 1
            while j in used and j > 0:
                j -= 1
            if j in used:
                continue
            idx = j
        used.add(idx)
        bars.append(idx)

    if not bars:
        return {last_bar: legal_total}

    bars.sort()
    n_actual = len(bars)
    schedule: dict[int, int] = {}
    allocated = 0
    for bar in bars[:-1]:
        schedule[bar] = base_per
        allocated += base_per
    residual = legal_total - allocated
    # Residual must be increment-legal (it is, since legal_total and base_per
    # are both increment multiples).
    if residual < max(min_qty, increment):
        # Numerical edge: collapse to single fill at argmax.
        return {int(np.argmax(plan_h)): legal_total}
    schedule[bars[-1]] = residual
    return schedule
