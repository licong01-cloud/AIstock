"""V25.1 small-capital execution algorithm.

V25.1 reuses the V25 plan generator (same Early/Late networks, same 88.79/11.21
weighting) but replaces the per-minute slicing with a board-aware,
cost-aware bucket scheduler. This is required for small-capital deployments
(e.g. 10M RMB) where the original V25 emits sub-100-share child orders that
fall under the A-share board minimum and quietly drop via ``round_lot_zero``.

Differences from V25_TWO_STAGE:

1. Board-aware sizing: STAR-board (688/689) orders honour the 200-share
   minimum and 1-share increment; main and ChiNext stay at 100 multiples.
2. Cost-aware bucketization: child-order amount is bounded below by
   ``min_cost / (commission_rate + tolerance_bps / 1e4)`` so realised
   commission overshoot does not exceed the configured tolerance.
3. Schedule-based execution: instead of per-minute ``int(remaining * frac)``,
   a fixed schedule of (bar -> qty) is built once per order from the V25 plan
   and replayed deterministically.
4. Same fail-fast behaviour as V25 (no TWAP fallback; explicit market states).

Original V25 behaviour is unchanged.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import numpy as np

from .base_algo import OrderState, StepResult
from .board_lot import (
    board_lot_rule,
    build_cost_aware_bucket_schedule,
    round_to_board_lot,
)
from .registry import register
from .v25_core import (
    REASON_LIMIT_DATA_MISSING_DUE_TO_SUSPEND,
    REASON_PREV_CLOSE_MISSING_WITH_SUSPEND,
    REASON_PRICE_MISSING_WITH_SUSPEND,
    REASON_SUSPENDED_BY_EXCHANGE,
    REASON_SUSPENDED_BY_SUSPEND_D,
    TOTAL_LEN,
    V25MarketAction,
    classify_v25_minute_market_state,
    infer_limit_pct,
)
from .v25_two_stage_algo import V25TwoStageAlgo, V25TwoStageUnavailableError


__all__ = ["V25_1SmallCapAlgo", "V25_1SmallCapUnavailableError"]


class V25_1SmallCapUnavailableError(V25TwoStageUnavailableError):
    """Raised when V25_1_SMALL_CAP cannot run authoritatively."""


@register
class V25_1SmallCapAlgo(V25TwoStageAlgo):
    ALGO_CODE = "V25_1_SMALL_CAP"
    HANDLES_MARKET_STATE = True

    DEFAULT_MIN_COST = 5.0
    DEFAULT_COMMISSION_RATE = 0.0003
    DEFAULT_TOLERANCE_BPS = 10.0
    DEFAULT_MAX_BUCKETS = 30

    def __init__(self, config: Dict[str, Any] | None = None) -> None:
        super().__init__(config)
        cfg = self.config
        try:
            self._min_cost = float(cfg.get("min_cost", self.DEFAULT_MIN_COST))
            self._commission_rate = float(cfg.get("commission_rate", self.DEFAULT_COMMISSION_RATE))
            self._tolerance_bps = float(
                cfg.get("commission_overshoot_tolerance_bps", self.DEFAULT_TOLERANCE_BPS)
            )
            self._max_buckets = int(cfg.get("max_buckets", self.DEFAULT_MAX_BUCKETS))
        except (TypeError, ValueError) as exc:
            raise V25_1SmallCapUnavailableError(
                f"V25_1_SMALL_CAP config types are invalid: {exc}"
            ) from exc
        if self._min_cost < 0 or self._commission_rate <= 0:
            raise V25_1SmallCapUnavailableError(
                "V25_1_SMALL_CAP requires min_cost>=0 and commission_rate>0"
            )
        if self._tolerance_bps < 0 or self._max_buckets <= 0:
            raise V25_1SmallCapUnavailableError(
                "V25_1_SMALL_CAP requires tolerance_bps>=0 and max_buckets>0"
            )
        # Per-order schedule cache keyed by (stock_id, side).
        self._schedule: Optional[Dict[int, int]] = None
        self._schedule_key: Optional[Tuple[str, str]] = None
        self._schedule_metadata: Dict[str, Any] = {}

    def init_order(self, symbol: str, side: str, total_quantity: int) -> OrderState:
        # Use board-aware rounding instead of the legacy 100-lot floor.
        adjusted = round_to_board_lot(int(total_quantity), symbol, side=side)
        if adjusted <= 0:
            # Below board minimum: keep the raw qty so the SELL-residual rule
            # can still flush it. BUY orders below min are illegal and the
            # adapter is expected to filter them upstream; we surface the
            # original qty so the caller decides.
            adjusted = max(int(total_quantity), 0)
        return OrderState(symbol=symbol, side=side, total_quantity=adjusted)

    def compute_step(
        self,
        state: OrderState,
        bar_data: Dict[str, Any],
        market_context: Dict[str, Any],
    ) -> Optional[StepResult]:
        self._last_no_fill_reason = None
        self._last_no_fill_context = {}
        if self.is_complete(state):
            return None

        remaining = state.total_quantity - state.executed_quantity
        if remaining <= 0:
            state.is_complete = True
            return None

        cur_price = float(bar_data.get("close") or 0)
        prev_close = market_context.get("prev_close")
        limit_up = bar_data.get("limit_up") or market_context.get("limit_up")
        limit_down = bar_data.get("limit_down") or market_context.get("limit_down")
        price_basis = str(bar_data.get("price_basis") or market_context.get("price_basis") or "raw")
        limit_price_basis = str(
            bar_data.get("limit_price_basis")
            or market_context.get("limit_price_basis")
            or market_context.get("prev_close_basis")
            or price_basis
        )

        market_state = classify_v25_minute_market_state(
            side=state.side,
            price=cur_price,
            volume=bar_data.get("volume"),
            prev_close=prev_close,
            limit_up=limit_up,
            limit_down=limit_down,
            is_suspended=bool(bar_data.get("is_suspended")),
            suspend_status=market_context.get("suspend_status"),
            require_limit_price=True,
            price_basis=price_basis,
            limit_price_basis=limit_price_basis,
        )
        if market_state.is_error:
            raise V25_1SmallCapUnavailableError(
                f"V25_1_SMALL_CAP market data error: {market_state.reason}"
            )
        if market_state.action == V25MarketAction.SKIP and market_state.reason in {
            REASON_SUSPENDED_BY_SUSPEND_D,
            REASON_SUSPENDED_BY_EXCHANGE,
            REASON_PREV_CLOSE_MISSING_WITH_SUSPEND,
            REASON_PRICE_MISSING_WITH_SUSPEND,
            REASON_LIMIT_DATA_MISSING_DUE_TO_SUSPEND,
        }:
            self._last_no_fill_reason = market_state.reason
            self._last_no_fill_context = dict(market_state.context)
            state.step += 1
            return None

        close_arr = self._require_array(market_context, "full_day_close")
        vol_arr = self._require_array(market_context, "full_day_volume")
        high_arr = self._require_array(market_context, "full_day_high")
        low_arr = self._require_array(market_context, "full_day_low")
        if not (len(close_arr) == len(vol_arr) == len(high_arr) == len(low_arr)):
            raise V25_1SmallCapUnavailableError(
                "V25_1_SMALL_CAP full-day arrays must have equal length"
            )
        realtime_streaming = bool(
            market_context.get("v25_realtime_streaming") or market_context.get("observed_only")
        )
        if len(close_arr) < TOTAL_LEN and not realtime_streaming:
            raise V25_1SmallCapUnavailableError(
                "V25_1_SMALL_CAP requires at least 240 minute bars"
            )

        open_arr = market_context.get("full_day_open")
        if open_arr is not None:
            open_price = float(self._require_array(market_context, "full_day_open")[0])
        else:
            open_price = float(bar_data.get("open") or close_arr[0])
        if cur_price <= 0 or open_price <= 0:
            raise V25_1SmallCapUnavailableError(
                "V25_1_SMALL_CAP requires positive open/close prices"
            )

        stock_id = str(market_context.get("stock_id") or state.symbol)
        side = state.side.upper()
        if side not in {"BUY", "SELL"}:
            raise V25_1SmallCapUnavailableError(f"V25_1_SMALL_CAP unsupported side: {state.side}")
        is_buy = side == "BUY"
        limit_pct = float(market_context.get("limit_pct") or infer_limit_pct(stock_id))
        if limit_pct <= 0:
            raise V25_1SmallCapUnavailableError("V25_1_SMALL_CAP requires positive limit_pct")
        day_features = self._day_features(market_context.get("day_features"))

        plan_key = (stock_id, side)
        if self._plan is None or self._plan_key != plan_key:
            self._plan = self._generate_plan(
                open_price=open_price,
                prev_close=float(prev_close),
                stock_id=stock_id,
                is_buy=is_buy,
                limit_pct=limit_pct,
                day_features=day_features,
            )
            self._plan_key = plan_key
            self._schedule = None  # invalidate
            self._schedule_key = None

        cur_step = int(state.step)
        if cur_step < 0:
            raise V25_1SmallCapUnavailableError("V25_1_SMALL_CAP state.step cannot be negative")
        horizon = TOTAL_LEN if realtime_streaming else min(len(close_arr), TOTAL_LEN)
        if horizon <= 0:
            raise V25_1SmallCapUnavailableError("V25_1_SMALL_CAP execution horizon is invalid")

        # Build the bucket schedule lazily on first tradable step so we have a
        # real cur_price for the cost-tolerance calculation.
        if self._schedule is None or self._schedule_key != plan_key:
            self._schedule = build_cost_aware_bucket_schedule(
                plan=self._plan,
                total_qty=int(state.total_quantity),
                cur_price=open_price,
                stock_id=stock_id,
                min_cost=self._min_cost,
                commission_rate=self._commission_rate,
                tolerance_bps=self._tolerance_bps,
                max_buckets=self._max_buckets,
                horizon=horizon,
            )
            self._schedule_key = plan_key
            self._schedule_metadata = {
                "n_buckets": len(self._schedule),
                "scheduled_qty": int(sum(self._schedule.values())),
                "horizon": int(horizon),
                "open_price": float(open_price),
            }

        if market_state.action == V25MarketAction.SKIP:
            self._last_no_fill_reason = market_state.reason
            self._last_no_fill_context = dict(market_state.context)
            state.step += 1
            return None

        if market_state.action == V25MarketAction.P0_FORCE:
            step_qty = remaining
            reason = market_state.reason
        elif cur_step >= horizon - 1:
            # Final bar: flush whatever the schedule did not place + any
            # residual from earlier skipped buckets.
            step_qty = remaining
            reason = f"V25_1_SMALL_CAP residual_flush {state.step + 1}/{horizon}"
        else:
            scheduled = int(self._schedule.get(cur_step, 0))
            step_qty = min(scheduled, remaining)
            if step_qty <= 0:
                self._last_no_fill_reason = "v25_1_bucket_off_step"
                self._last_no_fill_context = {
                    "cur_step": cur_step,
                    "scheduled": scheduled,
                    "remaining": remaining,
                }
                state.step += 1
                return None
            # Make sure step_qty is board-legal even after min(remaining).
            step_qty = self._legalize_step_qty(step_qty, remaining, stock_id, side)
            if step_qty <= 0:
                self._last_no_fill_reason = "v25_1_residual_below_board_lot"
                self._last_no_fill_context = {
                    "cur_step": cur_step,
                    "scheduled": scheduled,
                    "remaining": remaining,
                }
                state.step += 1
                return None
            reason = f"V25_1_SMALL_CAP bucket {state.step + 1}/{horizon}"

        step_qty = self._legalize_step_qty(step_qty, remaining, stock_id, side)
        state.step += 1
        if step_qty <= 0:
            self._last_no_fill_reason = self._last_no_fill_reason or "v25_1_zero_qty"
            self._last_no_fill_context = self._last_no_fill_context or {
                "cur_step": cur_step,
                "remaining": remaining,
            }
            return None

        state.executed_quantity += step_qty
        if state.executed_quantity >= state.total_quantity:
            state.is_complete = True

        return StepResult(
            symbol=state.symbol,
            side=state.side,
            quantity=step_qty,
            price=cur_price,
            reason=reason,
        )

    @staticmethod
    def _legalize_step_qty(step_qty: int, remaining: int, stock_id: str, side: str) -> int:
        """Floor ``step_qty`` to a board-legal multiple, but allow a SELL-side
        residual flush when ``step_qty == remaining`` and remaining is below the
        board minimum (the exchange residual rule)."""

        min_qty, increment = board_lot_rule(stock_id)
        side_norm = str(side or "").strip().upper()
        if step_qty >= min_qty:
            legal = (int(step_qty) // increment) * increment
            return max(legal, 0)
        if side_norm == "SELL" and step_qty == remaining and step_qty > 0:
            # Single residual flush on the SELL side.
            return int(step_qty)
        return 0
