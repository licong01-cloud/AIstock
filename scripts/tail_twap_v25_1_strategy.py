"""V25.1 small-cap two-stage execution strategy for QE Qlib workspaces.

Subclass of ``TailTWAPWithV25TwoStageStrategy``: keeps the V25 plan generator
(EarlyPlanNetEnhanced + LatePlanNet, 88.79/11.21 weighting) intact, but
**transforms the per-minute plan into a board-aware, cost-aware bucket
schedule** before the parent's per-step slicing formula consumes it.

Why this works without forking ``generate_trade_decision``: the parent computes
``base_delta = amount_remain * plan[step] / sum(plan[step:])``. This formula is
scale-invariant, so if we set ``plan[step] = bucket_qty[step]`` (zeros at
non-bucket bars), the parent naturally emits ``bucket_qty[step]`` shares at
each bucket bar and zero elsewhere. The 88.79/11.21 invariant is checked
inside the parent's own plan generator (which we still call) before the
transformation, so no parent invariant is broken.

Board-lot rules honoured:
  - main board / ChiNext (``00`` / ``60`` / ``300`` / ``301``): 100-share min,
    100-share increment.
  - STAR market (``688`` / ``689``): 200-share min, 1-share increment.

Cost floor: ``min_slice_amount = min_cost / (commission_rate + tolerance_bps/1e4)``.
Commission overshoot above ``tolerance_bps`` is the trigger; default is 10 bps.

This file is intentionally self-contained for Qlib experiment workspaces
(no ``backend`` imports). The board-lot / bucket helpers below mirror
``backend.execution_algos.board_lot`` and are kept in sync by the contract
tests in ``backend/tests/trading_core/test_v25_1_small_cap_contract.py``.
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

import numpy as np
from qlib.backtest.decision import Order

from tail_twap_v25_strategy import (
    EARLY_LEN,
    LATE_LEN,
    TOTAL_LEN,
    TailTWAPWithV25TwoStageStrategy,
    _V25MarketNoFill,
    _is_valid_factor,
    _is_valid_price,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Board-lot helpers (inlined; mirror backend/execution_algos/board_lot.py)
# ---------------------------------------------------------------------------


def _board_lot_rule(stock_id: str):
    """Return ``(min_qty, increment)`` for an A-share stock id."""

    code = str(stock_id).split(".")[0].strip()
    if not code:
        raise ValueError(f"V25.1 board_lot_rule got empty stock_id={stock_id!r}")
    if code.startswith(("688", "689")):
        return 200, 1
    if code.startswith(("300", "301", "302")):
        return 100, 100
    if code.startswith(("60", "00")):
        return 100, 100
    raise ValueError(
        f"V25.1 board_lot_rule does not recognise prefix for {stock_id!r}"
    )


def _min_slice_qty_for_cost(
    price: float,
    stock_id: str,
    min_cost: float,
    commission_rate: float,
    tolerance_bps: float,
) -> int:
    min_qty, increment = _board_lot_rule(stock_id)
    if price <= 0:
        return min_qty
    if min_cost <= 0:
        return min_qty
    denom = commission_rate + tolerance_bps / 1e4
    if denom <= 0:
        return min_qty
    cost_amount = min_cost / denom
    raw_shares = cost_amount / float(price)
    raw_ceil = int(np.ceil(raw_shares))
    if raw_ceil < min_qty:
        return min_qty
    rem = raw_ceil % increment
    if rem == 0:
        return raw_ceil
    return raw_ceil + (increment - rem)


def _build_cost_aware_bucket_schedule(
    plan: np.ndarray,
    total_qty: int,
    cur_price: float,
    stock_id: str,
    min_cost: float,
    commission_rate: float,
    tolerance_bps: float,
    max_buckets: int,
    horizon: int,
    side: str = "BUY",
) -> Dict[int, int]:
    """Convert a V25 normalized plan into a (bar -> shares) schedule.

    Properties:
      * ``sum(schedule.values()) == legal_total`` where ``legal_total`` is the
        increment-floor of ``total_qty``.
      * Every emitted qty is a multiple of the board increment (1 for STAR,
        100 for main/ChiNext) and ≥ ``min_qty`` unless this is a SELL residual
        flush below the board minimum.
      * Bucket bars are placed at the local-argmax inside each equal-weight
        segment of ``plan``'s cumulative distribution.
    """

    plan = np.asarray(plan, dtype=np.float64)
    horizon = int(min(horizon, len(plan)))
    if horizon <= 0:
        return {}
    plan = plan[:horizon]
    plan_sum = float(plan.sum())
    if plan_sum <= 1e-12:
        plan = np.ones(horizon, dtype=np.float64) / horizon
    else:
        plan = plan / plan_sum

    min_qty, increment = _board_lot_rule(stock_id)
    side_norm = str(side or "").strip().upper()
    total_qty = int(total_qty)
    if total_qty <= 0:
        return {}
    if total_qty < min_qty:
        if side_norm == "SELL":
            # Single residual flush at the close.
            return {horizon - 1: total_qty}
        return {}
    legal_total = (total_qty // increment) * increment
    if legal_total <= 0:
        return {}

    min_slice = _min_slice_qty_for_cost(
        cur_price, stock_id, min_cost, commission_rate, tolerance_bps
    )
    n_by_cost = max(1, legal_total // max(min_slice, 1))
    n_buckets = int(min(max_buckets, n_by_cost))
    if n_buckets <= 1:
        peak = int(np.argmax(plan))
        return {peak: legal_total}

    base_per_bucket = (legal_total // n_buckets // increment) * increment
    if base_per_bucket <= 0:
        # Cap n_buckets so each bucket carries at least one increment.
        n_buckets = max(1, legal_total // increment)
        base_per_bucket = increment if n_buckets > 0 else legal_total

    cdf = np.cumsum(plan)
    schedule: Dict[int, int] = {}
    used = 0
    for k in range(n_buckets):
        lo_w = k / n_buckets
        hi_w = (k + 1) / n_buckets
        lo_bar = int(np.searchsorted(cdf, lo_w, side="left"))
        hi_bar = int(np.searchsorted(cdf, hi_w, side="left"))
        lo_bar = min(max(lo_bar, 0), horizon - 1)
        hi_bar = min(max(hi_bar, lo_bar), horizon - 1)
        seg = plan[lo_bar : hi_bar + 1]
        if seg.size == 0:
            peak_bar = lo_bar
        else:
            peak_bar = lo_bar + int(np.argmax(seg))

        if k == n_buckets - 1:
            qty = legal_total - used
        else:
            qty = base_per_bucket
        qty = max(int(qty), 0)
        # Last-bucket residual may absorb a non-increment remainder; otherwise
        # snap to increment.
        if k != n_buckets - 1:
            qty = (qty // increment) * increment
        if qty <= 0:
            continue
        schedule[peak_bar] = schedule.get(peak_bar, 0) + qty
        used += qty
    if used < legal_total:
        last_bar = max(schedule.keys()) if schedule else (horizon - 1)
        schedule[last_bar] = schedule.get(last_bar, 0) + (legal_total - used)
    return schedule


# ---------------------------------------------------------------------------
# QE strategy class
# ---------------------------------------------------------------------------


class TailTWAPWithV25_1SmallCapStrategy(TailTWAPWithV25TwoStageStrategy):
    """V25.1 — V25 plan, board+cost-aware bucket schedule.

    Extra config kwargs (all optional, with safe defaults):
      * ``v25_1_min_cost`` (RMB, default 5.0) — A-share minimum commission
      * ``v25_1_commission_rate`` (default 0.0003 = 3 bps)
      * ``v25_1_tolerance_bps`` (default 10.0) — max acceptable commission
        overshoot above ``commission_rate`` for any single child fill
      * ``v25_1_max_buckets`` (default 30) — upper bound on intraday fills
    """

    DEFAULT_MIN_COST = 5.0
    DEFAULT_COMMISSION_RATE = 0.0003
    DEFAULT_TOLERANCE_BPS = 10.0
    DEFAULT_MAX_BUCKETS = 30

    def __init__(
        self,
        *args,
        v25_1_min_cost: Optional[float] = None,
        v25_1_commission_rate: Optional[float] = None,
        v25_1_tolerance_bps: Optional[float] = None,
        v25_1_max_buckets: Optional[int] = None,
        **kwargs,
    ):
        v25_1_min_cost = self._consume_v25_1_alias(
            kwargs, "min_cost", "v25_1_min_cost", v25_1_min_cost
        )
        v25_1_commission_rate = self._consume_v25_1_alias(
            kwargs,
            "commission_rate",
            "v25_1_commission_rate",
            v25_1_commission_rate,
        )
        v25_1_tolerance_bps = self._consume_v25_1_alias(
            kwargs,
            "tolerance_bps",
            "v25_1_tolerance_bps",
            v25_1_tolerance_bps,
        )
        v25_1_max_buckets = self._consume_v25_1_alias(
            kwargs, "max_buckets", "v25_1_max_buckets", v25_1_max_buckets
        )
        super().__init__(*args, **kwargs)
        self._v25_1_min_cost = float(
            self.DEFAULT_MIN_COST if v25_1_min_cost is None else v25_1_min_cost
        )
        self._v25_1_commission_rate = float(
            self.DEFAULT_COMMISSION_RATE
            if v25_1_commission_rate is None
            else v25_1_commission_rate
        )
        self._v25_1_tolerance_bps = float(
            self.DEFAULT_TOLERANCE_BPS
            if v25_1_tolerance_bps is None
            else v25_1_tolerance_bps
        )
        self._v25_1_max_buckets = int(
            self.DEFAULT_MAX_BUCKETS
            if v25_1_max_buckets is None
            else v25_1_max_buckets
        )
        if (
            self._v25_1_min_cost < 0
            or self._v25_1_commission_rate <= 0
            or self._v25_1_tolerance_bps < 0
            or self._v25_1_max_buckets <= 0
        ):
            raise ValueError(
                "V25.1 invalid config: "
                f"min_cost={self._v25_1_min_cost} "
                f"commission_rate={self._v25_1_commission_rate} "
                f"tolerance_bps={self._v25_1_tolerance_bps} "
                f"max_buckets={self._v25_1_max_buckets}"
            )
        logger.info(
            "[TailTWAPv25.1] config min_cost=%.2f commission_rate=%.6f "
            "tolerance_bps=%.2f max_buckets=%d",
            self._v25_1_min_cost,
            self._v25_1_commission_rate,
            self._v25_1_tolerance_bps,
            self._v25_1_max_buckets,
        )

    @staticmethod
    def _consume_v25_1_alias(kwargs, alias_name, canonical_name, canonical_value):
        """Promote generic QE config aliases before the parent swallows them."""

        if alias_name not in kwargs:
            return canonical_value
        alias_value = kwargs.pop(alias_name)
        if alias_value is None:
            return canonical_value
        if canonical_value is None:
            return alias_value
        try:
            same_value = float(alias_value) == float(canonical_value)
        except (TypeError, ValueError):
            same_value = alias_value == canonical_value
        if not same_value:
            raise ValueError(
                "V25.1 conflicting config aliases: "
                f"{canonical_name}={canonical_value!r} "
                f"{alias_name}={alias_value!r}"
            )
        return canonical_value

    def _minimum_child_order_amount(
        self,
        stock_id,
        direction,
        unit,
        *,
        trade_start_time=None,
        trade_end_time=None,
    ):
        min_qty, _ = _board_lot_rule(stock_id)
        factor = self._child_order_factor(
            stock_id,
            unit,
            trade_start_time=trade_start_time,
            trade_end_time=trade_end_time,
        )
        return float(min_qty) / float(factor)

    def _buy_below_min_child_order_reason(
        self,
        stock_id,
        direction,
        unit,
        *,
        trade_start_time=None,
        trade_end_time=None,
    ):
        return "buy_below_board_lot"

    def _legalize_child_order_amount(
        self,
        stock_id,
        direction,
        amount_delta,
        max_amount,
        unit,
        *,
        is_last_step=False,
        allow_sell_residual=False,
        round_by_unit=True,
        trade_start_time=None,
        trade_end_time=None,
    ):
        """Apply stock-aware board-lot rules after the parent computes a slice.

        Qlib orders use adjusted-share amounts when ``$factor`` is present. The
        legal board-lot rule, however, is stated in raw shares, so convert the
        candidate child amount to raw shares, legalize there, then convert back
        to the adjusted amount that Qlib expects. This also lets V25.1 run with
        ``exchange_kwargs.trade_unit: null`` so the final Qlib Exchange layer
        does not re-round STAR-market 200+1 orders back to 100-share lots.
        """

        min_qty, increment = _board_lot_rule(stock_id)
        side = "BUY" if direction == Order.BUY else "SELL"
        factor = self._child_order_factor(
            stock_id,
            unit,
            trade_start_time=trade_start_time,
            trade_end_time=trade_end_time,
        )
        max_amount = max(float(max_amount), 0.0)
        amount_delta = min(max(float(amount_delta), 0.0), max_amount)
        if amount_delta <= 1e-5:
            return 0.0

        max_raw = max_amount * factor
        delta_raw = min(amount_delta * factor, max_raw)

        if side == "SELL" and max_raw < min_qty:
            if delta_raw >= max_raw - 1e-5 and max_raw > 0:
                return max_amount

        if delta_raw < min_qty:
            return 0.0

        nearest_raw = float(np.round(delta_raw / increment) * increment)
        if nearest_raw > max_raw + 1e-5:
            nearest_raw = float((int(max_raw) // increment) * increment)
        if nearest_raw < min_qty:
            return 0.0
        return min(nearest_raw / factor, max_amount)

    def _child_order_factor(
        self,
        stock_id,
        unit,
        *,
        trade_start_time=None,
        trade_end_time=None,
    ) -> float:
        """Return the Qlib adjustment factor for child-order amount conversion."""

        if trade_start_time is not None and trade_end_time is not None:
            factor = self._read_quote_data(
                stock_id, trade_start_time, trade_end_time, "$factor"
            )
            if _is_valid_factor(factor):
                return float(factor)
        if unit is not None and unit > 0:
            # Parent V25 receives Qlib's adjusted trade unit: raw 100 / factor.
            inferred = 100.0 / float(unit)
            if _is_valid_factor(inferred):
                return float(inferred)
        return 1.0

    def _generate_plan_for_order(
        self, stock_id, direction, trade_start_time, trade_end_time
    ):
        """Generate V25 plan, then convert to bucket schedule.

        The parent's per-step formula treats ``plan`` as a weight vector. By
        substituting in a bucket-share vector (zeros at non-bucket bars), the
        formula naturally emits the bucket qty at each scheduled bar and zero
        elsewhere — without modifying the parent.
        """

        v25_plan = super()._generate_plan_for_order(
            stock_id, direction, trade_start_time, trade_end_time
        )

        amount_remain = float(self.trade_amount_remain.get(stock_id, 0))
        if amount_remain <= 0:
            return v25_plan

        open_adj = self._read_quote_data(
            stock_id, trade_start_time, trade_end_time, "$open"
        )
        try:
            open_price, factor = self._require_raw_price(
                stock_id,
                trade_start_time,
                trade_end_time,
                open_adj,
                "$open",
            )
        except _V25MarketNoFill:
            # Market unavailable — let parent's downstream market-state checks
            # handle no-fill recording. Returning the V25 plan is safe; parent
            # will skip the order anyway.
            return v25_plan
        if not _is_valid_price(open_price):
            return v25_plan

        side = "BUY" if direction == Order.BUY else "SELL"
        raw_amount_remain = max(int(np.floor(amount_remain * float(factor) + 1e-6)), 0)
        schedule = _build_cost_aware_bucket_schedule(
            plan=v25_plan,
            total_qty=raw_amount_remain,
            cur_price=float(open_price),
            stock_id=stock_id,
            min_cost=self._v25_1_min_cost,
            commission_rate=self._v25_1_commission_rate,
            tolerance_bps=self._v25_1_tolerance_bps,
            max_buckets=self._v25_1_max_buckets,
            horizon=TOTAL_LEN,
            side=side,
        )
        if not schedule:
            reason = (
                "buy_below_board_lot"
                if side == "BUY"
                else "v25_1_empty_board_lot_schedule"
            )
            logger.warning(
                "[TailTWAPv25.1] empty schedule stock=%s side=%s amount_remain=%.2f "
                "raw_amount_remain=%d open=%.6f reason=%s order not executable under board rules",
                stock_id,
                side,
                amount_remain,
                raw_amount_remain,
                float(open_price),
                reason,
            )
            raise _V25MarketNoFill(
                stock_id,
                reason,
                f"amount_remain={amount_remain:.6f} min_board_lot={_board_lot_rule(stock_id)[0]}",
            )

        bucket_plan = np.zeros(TOTAL_LEN, dtype=np.float64)
        for step, qty in schedule.items():
            if 0 <= step < TOTAL_LEN:
                bucket_plan[step] = float(qty)
        s = float(bucket_plan.sum())
        if s <= 1e-12:
            return v25_plan
        bucket_plan /= s
        # Pad with tiny uniform weight so the plan tail is never zero. The
        # parent's per-step formula raises if ``plan[step:].sum() <= 1e-8``,
        # which happens when a sparse bucket plan is exhausted before the
        # day's last bar (e.g. a SELL is blocked all afternoon by limit-down
        # and ``amount_remain > 0`` at step 239). Padding makes the parent
        # check pass; non-bucket bars then yield a delta below the 1e-5
        # filter, and the parent's ``is_last_step`` override still flushes
        # the residual at the close.
        # Use 1e-6 so plan[step:].sum() stays above the parent's 1e-8 threshold
        # even at the last bar (1 * 1e-6 = 1e-6 >> 1e-8). Per-bar contribution
        # of ~1e-6 is filtered by the parent's 1e-5 share-delta filter, so no
        # spurious fills occur at non-bucket bars.
        epsilon = 1e-6
        bucket_plan = bucket_plan + epsilon
        bucket_plan /= bucket_plan.sum()

        scheduled_total = int(sum(schedule.values()))
        early_share = float(bucket_plan[:EARLY_LEN].sum())
        late_share = float(bucket_plan[EARLY_LEN:].sum())
        logger.info(
            "[TailTWAPv25.1] schedule stock=%s side=%s n_buckets=%d "
            "scheduled_total=%d amount_remain=%.6f raw_amount_remain=%d factor=%.8f open=%.6f "
            "early_share=%.4f late_share=%.4f",
            stock_id,
            side,
            len(schedule),
            scheduled_total,
            amount_remain,
            raw_amount_remain,
            float(factor),
            float(open_price),
            early_share,
            late_share,
        )
        return bucket_plan
