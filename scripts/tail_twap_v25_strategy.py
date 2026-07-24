"""QE V25 two-stage minute execution strategy.

This module is intentionally self-contained for Qlib experiment workspaces.
It fails fast for missing models/devices and logs the early/late allocation
trace so the effective execution plan is auditable.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from qlib.backtest.decision import Order, TradeDecisionWO
from qlib.backtest.utils import get_start_end_idx

from tail_twap_strategy import (
    TailTWAPWithLimitStrategy,
    TAIL_START_OFFSET,
    REALLOC_OFFSET,
)
from minute_execution_contract import (
    DayFeatureArtifact,
    MinuteExecutionContractError,
    normalize_trade_step,
)
try:
    from qe_suspend_filter import QESuspendFilter
except Exception:  # pragma: no cover - Qlib workspace packaging guard
    QESuspendFilter = None

logger = logging.getLogger(__name__)

EARLY_WEIGHT = 0.8879
LATE_WEIGHT = 0.1121
EARLY_LEN = 30
LATE_LEN = 210
TOTAL_LEN = 240
GAP_RATIO_EDGES = [-0.70, -0.50, -0.30, -0.10, 0.10, 0.30, 0.50, 0.70]
PRICE_EPSILON = 1e-6


class EarlyPlanNetEnhanced(nn.Module):
    def __init__(self):
        super().__init__()
        self.mlp = nn.Sequential(
            nn.Linear(15, 128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 256),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(256, EARLY_LEN),
            nn.Softmax(dim=-1),
        )

    def forward(self, gap_bucket, gap_ratio, gap_ratio_signed, limit_pct, is_buy, day_features):
        gap_bucket_norm = gap_bucket.float() / 8.0
        x = torch.cat([
            gap_ratio.unsqueeze(1),
            gap_ratio_signed.unsqueeze(1),
            limit_pct.unsqueeze(1),
            is_buy.unsqueeze(1),
            gap_bucket_norm.unsqueeze(1),
            day_features,
        ], dim=1)
        return self.mlp(x)


class LatePlanNet(nn.Module):
    def __init__(self, gap_buckets: int = 9, gap_emb_dim: int = 16, late_len: int = LATE_LEN):
        super().__init__()
        self.gap_embedding = nn.Embedding(gap_buckets, gap_emb_dim)
        self.mlp = nn.Sequential(
            nn.Linear(gap_emb_dim + 5, 128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 256),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(256, late_len),
        )

    def forward(self, gap_bucket_idx, gap_ratio, is_buy, early_weight, early_peak_pos, early_concentration):
        gap_emb = self.gap_embedding(gap_bucket_idx)
        extra = torch.stack([gap_ratio, is_buy, early_weight, early_peak_pos, early_concentration], dim=1)
        logits = self.mlp(torch.cat([gap_emb, extra], dim=1))
        return F.softmax(logits, dim=-1)


def _get_limit_pct(stock_id: str) -> float:
    code = str(stock_id).split(".")[0]
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    return 0.10


def _gap_ratio_to_bucket(gap_ratio: float) -> int:
    for i, edge in enumerate(GAP_RATIO_EDGES):
        if gap_ratio < edge:
            return i
    return len(GAP_RATIO_EDGES)


def _load_state(path: str, device: torch.device):
    ckpt = torch.load(path, map_location=device, weights_only=False)
    if isinstance(ckpt, dict) and "model" in ckpt:
        return ckpt["model"]
    return ckpt


def _is_valid_price(value) -> bool:
    try:
        return value is not None and not np.isnan(value) and float(value) > 0
    except (TypeError, ValueError):
        return False


def _is_valid_factor(value) -> bool:
    try:
        return value is not None and not np.isnan(value) and float(value) > 0
    except (TypeError, ValueError):
        return False


def _to_raw_price(adjusted_price, factor) -> float:
    return float(adjusted_price) / float(factor)


def _price_at_or_above(price, limit_price) -> bool:
    return float(price) >= float(limit_price) * (1.0 - PRICE_EPSILON)


def _price_at_or_below(price, limit_price) -> bool:
    return float(price) <= float(limit_price) * (1.0 + PRICE_EPSILON)


class _V25MarketNoFill(Exception):
    """Market-state no-fill; this must not be treated as a V25 config failure."""

    def __init__(self, stock_id: str, reason: str, detail: str = ""):
        super().__init__(f"{reason}: stock={stock_id} {detail}".strip())
        self.stock_id = stock_id
        self.reason = reason
        self.detail = detail


class TailTWAPWithV25TwoStageStrategy(TailTWAPWithLimitStrategy):
    """V25 two-stage execution mapped to Qlib NestedExecutor inner_strategy."""

    def __init__(
        self,
        early_model_path: str,
        late_model_path: str,
        device: str = "cpu",
        day_features_file=None,
        day_features_schema_version=None,
        start_time=None,
        end_time=None,
        split_count=None,
        lookback_days=None,
        participation_rate=None,
        unfilled_handler=None,
        unfilled_trigger_minute=None,
        unfilled_backup_depth=None,
        filter_suspended_on_signal=False,
        suspend_filter_file=None,
        suspend_filter_strict=True,
        **kwargs,
    ):
        super().__init__(
            start_time=start_time,
            end_time=end_time,
            split_count=split_count,
            lookback_days=lookback_days,
            participation_rate=participation_rate,
            unfilled_handler=unfilled_handler,
            unfilled_trigger_minute=unfilled_trigger_minute,
            unfilled_backup_depth=unfilled_backup_depth,
            **kwargs,
        )
        if not early_model_path or not Path(str(early_model_path)).exists():
            raise FileNotFoundError(f"V25 early_model_path missing: {early_model_path}")
        if not late_model_path or not Path(str(late_model_path)).exists():
            raise FileNotFoundError(f"V25 late_model_path missing: {late_model_path}")
        if str(device).startswith("cuda") and not torch.cuda.is_available():
            raise RuntimeError("V25 requested CUDA device but torch.cuda.is_available() is false")
        self._device = torch.device(device)
        self._early_model_path = str(early_model_path)
        self._late_model_path = str(late_model_path)
        self._early_model = EarlyPlanNetEnhanced().to(self._device)
        self._late_model = LatePlanNet().to(self._device)
        self._early_model.load_state_dict(_load_state(self._early_model_path, self._device))
        self._late_model.load_state_dict(_load_state(self._late_model_path, self._device))
        self._early_model.eval()
        self._late_model.eval()
        if not day_features_file or not day_features_schema_version:
            raise RuntimeError(
                "V25 requires day_features_file and day_features_schema_version; "
                "zero/default day features are forbidden"
            )
        try:
            self._day_features_artifact = DayFeatureArtifact(
                day_features_file,
                expected_schema_version=day_features_schema_version,
            )
        except MinuteExecutionContractError as exc:
            raise RuntimeError(str(exc)) from exc
        if filter_suspended_on_signal:
            if QESuspendFilter is None:
                raise RuntimeError("V25 suspend filter requested but qe_suspend_filter is not importable")
            self._qe_suspend_filter = QESuspendFilter(
                enabled=True,
                suspend_filter_file=suspend_filter_file,
                strict=suspend_filter_strict,
                logger_obj=logger,
            )
        else:
            self._qe_suspend_filter = None
        logger.info("[TailTWAPv25] loaded early=%s late=%s device=%s", self._early_model_path, self._late_model_path, self._device)

    def reset(self, outer_trade_decision=None, **kwargs):
        super().reset(outer_trade_decision=outer_trade_decision, **kwargs)
        if outer_trade_decision is not None:
            self._v25_plans: dict[str, np.ndarray] = {}
            self._v25_plan_generated: dict[str, bool] = {}
            self._v25_plan_failed: set[str] = set()
            self._v25_no_fill_reasons: dict[str, str] = {}

    def _is_artifact_suspended(self, stock_id, trade_time) -> bool:
        if self._qe_suspend_filter is None:
            return False
        suspended = self._qe_suspend_filter.suspended_symbols(trade_time)
        aliases = self._qe_suspend_filter._symbol_aliases(stock_id)
        return bool(aliases & suspended)

    def _read_quote_data(self, stock_id, trade_start_time, trade_end_time, field):
        try:
            return self.trade_exchange.quote.get_data(
                stock_id,
                trade_start_time,
                trade_end_time,
                field=field,
                method="ts_data_last",
            )
        except (KeyError, IndexError, ValueError, AttributeError):
            return None

    def _require_raw_price(self, stock_id, trade_start_time, trade_end_time, adjusted_price, field, prev_close=None):
        if not _is_valid_price(adjusted_price):
            reason = self._market_block_reason(
                stock_id,
                trade_start_time,
                trade_end_time,
                close_price=adjusted_price,
                prev_close=prev_close,
            )
            if reason:
                raise _V25MarketNoFill(stock_id, reason, f"{field}={adjusted_price}")
            raise RuntimeError(
                f"{field}_missing_data_error for V25 raw-price conversion: "
                f"stock={stock_id} adjusted_price={adjusted_price}"
            )
        factor = self._read_quote_data(stock_id, trade_start_time, trade_end_time, "$factor")
        if not _is_valid_factor(factor):
            reason = self._market_block_reason(
                stock_id,
                trade_start_time,
                trade_end_time,
                close_price=adjusted_price,
                prev_close=prev_close,
            )
            if reason:
                raise _V25MarketNoFill(stock_id, reason, f"factor={factor} field={field}")
            raise RuntimeError(
                f"factor_missing_data_error for V25 raw-price conversion: "
                f"stock={stock_id} field={field} factor={factor}"
            )
        raw_price = _to_raw_price(adjusted_price, factor)
        if not _is_valid_price(raw_price):
            raise RuntimeError(
                f"raw_price_invalid_data_error for V25 raw-price conversion: "
                f"stock={stock_id} field={field} adjusted_price={adjusted_price} factor={factor} raw_price={raw_price}"
            )
        return raw_price, float(factor)

    def _safe_check_exchange_suspended(self, stock_id, trade_start_time, trade_end_time) -> bool:
        try:
            return bool(self.trade_exchange.check_stock_suspended(
                stock_id=stock_id,
                start_time=trade_start_time,
                end_time=trade_end_time,
            ))
        except (KeyError, IndexError, ValueError, AttributeError):
            return False

    def _market_block_reason(self, stock_id, trade_start_time, trade_end_time, close_price=None, prev_close=None):
        if self._safe_check_exchange_suspended(stock_id, trade_start_time, trade_end_time):
            return "suspended_by_exchange"
        if self._is_artifact_suspended(stock_id, trade_start_time):
            return "suspended_by_suspend_d"

        volume = self._read_quote_data(stock_id, trade_start_time, trade_end_time, "$volume")
        try:
            volume_is_zero = volume is not None and not np.isnan(volume) and float(volume) <= 0
        except (TypeError, ValueError):
            volume_is_zero = False
        if volume_is_zero:
            return "intraday_halt_or_no_bar"

        if close_price is not None and not _is_valid_price(close_price):
            if volume is None:
                return None
            return "intraday_halt_or_no_bar"
        if prev_close is not None and not _is_valid_price(prev_close) and volume_is_zero:
            return "intraday_halt_or_no_bar"
        return None

    def _record_no_fill_reason(self, stock_id, reason, trade_start_time, detail=""):
        self._v25_no_fill_reasons[stock_id] = reason
        logger.info(
            "[TailTWAPv25] market-state stock=%s trade_time=%s reason=%s detail=%s",
            stock_id,
            trade_start_time,
            reason,
            detail,
        )

    def _minimum_child_order_amount(
        self,
        stock_id,
        direction,
        unit,
        *,
        trade_start_time=None,
        trade_end_time=None,
    ):
        """Minimum child-order amount for the legacy V25/Qlib trade unit rule."""

        return float(unit) if unit is not None and unit > 0 else 100.0

    def _buy_below_min_child_order_reason(
        self,
        stock_id,
        direction,
        unit,
        *,
        trade_start_time=None,
        trade_end_time=None,
    ):
        return "buy_below_trade_unit"

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
        """Apply the final child-order sizing rule before constructing Order.

        Subclasses such as V25.1 override this hook with stock-aware exchange
        board-lot rules. The base implementation intentionally preserves the
        historical V25 behaviour, including final sell residual cleanup.
        """

        max_amount = max(float(max_amount), 0.0)
        amount_delta = min(max(float(amount_delta), 0.0), max_amount)
        if allow_sell_residual and direction == Order.SELL:
            return amount_delta
        if is_last_step and direction == Order.SELL:
            return amount_delta
        if not round_by_unit:
            return amount_delta
        if unit is not None and unit > 0:
            return min(np.round(amount_delta / unit) * unit, max_amount)
        return amount_delta

    def _append_legal_child_order(
        self,
        order_list,
        stock_id,
        direction,
        amount_delta,
        max_amount,
        unit,
        trade_start_time,
        trade_end_time,
        *,
        is_last_step=False,
        allow_sell_residual=False,
        round_by_unit=True,
        zero_reason=None,
    ):
        amount_delta_target = self._legalize_child_order_amount(
            stock_id,
            direction,
            amount_delta,
            max_amount,
            unit,
            is_last_step=is_last_step,
            allow_sell_residual=allow_sell_residual,
            round_by_unit=round_by_unit,
            trade_start_time=trade_start_time,
            trade_end_time=trade_end_time,
        )
        if amount_delta_target > 1e-5:
            order_list.append(Order(
                stock_id=stock_id,
                amount=amount_delta_target,
                start_time=trade_start_time,
                end_time=trade_end_time,
                direction=direction,
            ))
            return amount_delta_target
        if zero_reason:
            self._record_no_fill_reason(stock_id, zero_reason, trade_start_time)
        return 0.0

    def _generate_plan_for_order(self, stock_id, direction, trade_start_time, trade_end_time):
        open_price_adjusted = self._read_quote_data(stock_id, trade_start_time, trade_end_time, "$open")
        prev_close = self._read_quote_data(stock_id, trade_start_time, trade_end_time, "$prev_close")
        if not _is_valid_price(prev_close):
            reason = self._market_block_reason(
                stock_id, trade_start_time, trade_end_time, close_price=open_price_adjusted, prev_close=prev_close
            )
            if reason:
                raise _V25MarketNoFill(stock_id, reason, f"prev_close={prev_close}")
            raise RuntimeError(f"prev_close_missing_data_error for V25 plan: stock={stock_id} prev_close={prev_close}")
        open_price, factor = self._require_raw_price(
            stock_id,
            trade_start_time,
            trade_end_time,
            open_price_adjusted,
            "$open",
            prev_close=prev_close,
        )
        limit_pct = _get_limit_pct(stock_id)
        gap_pct = np.clip((float(open_price) - float(prev_close)) / float(prev_close), -0.20, 0.20)
        gap_ratio = float(gap_pct / limit_pct) if limit_pct > 1e-8 else 0.0
        gap_bucket = _gap_ratio_to_bucket(gap_ratio)
        is_buy = 1.0 if direction == Order.BUY else 0.0
        try:
            day_features = np.asarray(
                self._day_features_artifact.vector(
                    trade_date=trade_start_time,
                    symbol=stock_id,
                ),
                dtype=np.float32,
            )
        except MinuteExecutionContractError as exc:
            raise RuntimeError(str(exc)) from exc

        with torch.no_grad():
            gb = torch.LongTensor([gap_bucket]).to(self._device)
            gr_abs = torch.FloatTensor([abs(gap_ratio)]).to(self._device)
            gr_signed = torch.FloatTensor([gap_ratio]).to(self._device)
            lp = torch.FloatTensor([limit_pct]).to(self._device)
            ib = torch.FloatTensor([is_buy]).to(self._device)
            df = torch.FloatTensor([day_features]).to(self._device)
            pred_early = self._early_model(gb, gr_abs, gr_signed, lp, ib, df).cpu().numpy()[0]
            early_weight_raw = float(pred_early.sum())
            early_peak_pos = float(pred_early.argmax() / max(EARLY_LEN - 1, 1))
            early_mean = float(pred_early.mean())
            early_concentration = float(pred_early.max() / (early_mean + 1e-8))
            ew = torch.FloatTensor([early_weight_raw]).to(self._device)
            epp = torch.FloatTensor([early_peak_pos]).to(self._device)
            ec = torch.FloatTensor([early_concentration]).to(self._device)
            pred_late = self._late_model(gb, gr_abs, ib, ew, epp, ec).cpu().numpy()[0]

        plan = np.concatenate([pred_early * EARLY_WEIGHT, pred_late * LATE_WEIGHT]).astype(np.float64)
        if len(plan) != TOTAL_LEN or np.isnan(plan).any() or plan.sum() <= 1e-8:
            raise RuntimeError(f"V25 plan invalid: stock={stock_id} len={len(plan)} sum={plan.sum()} nan={np.isnan(plan).any()}")
        plan = plan / plan.sum()
        early_sum = float(plan[:EARLY_LEN].sum())
        late_sum = float(plan[EARLY_LEN:].sum())
        if abs(early_sum - EARLY_WEIGHT) > 1e-4 or abs(late_sum - LATE_WEIGHT) > 1e-4:
            raise RuntimeError(f"V25 plan weight mismatch: early={early_sum:.6f} late={late_sum:.6f}")
        logger.info(
            "[TailTWAPv25] generated plan stock=%s is_buy=%s early_sum=%.4f late_sum=%.4f "
            "gap_ratio=%.4f price_basis=raw open_raw=%.6f prev_close_raw=%.6f factor=%.8f",
            stock_id, bool(is_buy), early_sum, late_sum, gap_ratio, open_price, float(prev_close), factor,
        )
        return plan

    def generate_trade_decision(self, execute_result=None):
        if len(self.outer_trade_decision.get_decision()) == 0:
            return TradeDecisionWO(order_list=[], strategy=self)

        trade_step = self.trade_calendar.get_trade_step()
        start_idx, end_idx = get_start_end_idx(self.trade_calendar, self.outer_trade_decision)
        if trade_step < start_idx or trade_step > end_idx:
            return TradeDecisionWO(order_list=[], strategy=self)

        try:
            rel_trade_step = normalize_trade_step(
                trade_step=trade_step,
                start_idx=start_idx,
                end_idx=end_idx,
            )
        except MinuteExecutionContractError as exc:
            raise RuntimeError(str(exc)) from exc
        if rel_trade_step is None:
            self._v25_no_fill_reasons["__calendar__"] = "auction_wait"
            return TradeDecisionWO(order_list=[], strategy=self)

        if execute_result is not None:
            for order, _, _, _ in execute_result:
                if order.stock_id in self.trade_amount_remain:
                    self.trade_amount_remain[order.stock_id] -= order.deal_amount
                if order.stock_id in self._realloc_extra:
                    self._realloc_extra[order.stock_id] = max(0, self._realloc_extra[order.stock_id] - order.deal_amount)

        trade_start_time, trade_end_time = self.trade_calendar.get_step_time(trade_step)
        is_last_step = rel_trade_step >= TOTAL_LEN - 1 or trade_step >= end_idx
        if rel_trade_step >= REALLOC_OFFSET and not self._realloc_done:
            self._realloc_done = True
            if self._unfilled_handler == "TAIL_SUBSTITUTE":
                self._do_realloc_substitute(trade_start_time, trade_end_time)
            else:
                self._do_realloc(trade_start_time, trade_end_time)

        order_list = []
        for order in self.outer_trade_decision.get_decision():
            market_reason = self._market_block_reason(order.stock_id, trade_start_time, trade_end_time)
            if market_reason:
                self._record_no_fill_reason(order.stock_id, market_reason, trade_start_time)
                continue
            amount_remain = self.trade_amount_remain[order.stock_id]
            if amount_remain <= 1e-5:
                continue

            unit = self.trade_exchange.get_amount_of_trade_unit(
                stock_id=order.stock_id,
                start_time=order.start_time,
                end_time=order.end_time,
            )
            minimum_child_amount = self._minimum_child_order_amount(
                order.stock_id,
                order.direction,
                unit,
                trade_start_time=trade_start_time,
                trade_end_time=trade_end_time,
            )
            if order.direction == Order.BUY and amount_remain < minimum_child_amount:
                self._v25_no_fill_reasons[order.stock_id] = self._buy_below_min_child_order_reason(
                    order.stock_id,
                    order.direction,
                    unit,
                    trade_start_time=trade_start_time,
                    trade_end_time=trade_end_time,
                )
                continue
            if order.direction == Order.SELL and amount_remain < minimum_child_amount:
                # Odd-lot/fractional sell orders are cleanup orders; sending
                # them immediately avoids wasting V25 slices on sub-lot dust.
                self._append_legal_child_order(
                    order_list,
                    order.stock_id,
                    order.direction,
                    amount_remain,
                    amount_remain,
                    unit,
                    trade_start_time,
                    trade_end_time,
                    allow_sell_residual=True,
                )
                continue

            if order.stock_id not in self._p0_done:
                try:
                    close_price_adjusted = self.trade_exchange.get_close(order.stock_id, trade_start_time, trade_end_time, method="ts_data_last")
                    if not _is_valid_price(close_price_adjusted):
                        reason = self._market_block_reason(
                            order.stock_id, trade_start_time, trade_end_time, close_price=close_price_adjusted
                        )
                        if reason:
                            self._record_no_fill_reason(order.stock_id, reason, trade_start_time, f"close_price={close_price_adjusted}")
                            continue
                        raise RuntimeError(f"V25 P0 close_price_missing_data_error for {order.stock_id}: {close_price_adjusted}")
                    close_price, factor = self._require_raw_price(
                        order.stock_id,
                        trade_start_time,
                        trade_end_time,
                        close_price_adjusted,
                        "$close",
                    )
                    limit_up = self._read_quote_data(order.stock_id, trade_start_time, trade_end_time, "$up_limit_price")
                    limit_down = self._read_quote_data(order.stock_id, trade_start_time, trade_end_time, "$down_limit_price")
                    if not _is_valid_price(limit_up) or not _is_valid_price(limit_down):
                        reason = self._market_block_reason(
                            order.stock_id, trade_start_time, trade_end_time, close_price=close_price_adjusted
                        )
                        if reason:
                            self._record_no_fill_reason(
                                order.stock_id,
                                "limit_data_missing_due_to_suspend",
                                trade_start_time,
                                f"market_reason={reason} limit_up={limit_up} limit_down={limit_down}",
                            )
                            continue
                        raise RuntimeError(
                            f"V25 P0 limit_price_missing_data_error for {order.stock_id}: "
                            f"up_limit={limit_up} down_limit={limit_down}"
                        )
                    if order.direction == Order.BUY and _price_at_or_below(close_price, limit_down):
                        self._record_no_fill_reason(
                            order.stock_id,
                            "p0_limit_buy_at_down_limit",
                            trade_start_time,
                            f"price_basis=raw close_raw={close_price:.6f} down_limit_raw={float(limit_down):.6f} factor={factor:.8f}",
                        )
                        self._append_legal_child_order(
                            order_list,
                            order.stock_id,
                            order.direction,
                            amount_remain,
                            amount_remain,
                            unit,
                            trade_start_time,
                            trade_end_time,
                            round_by_unit=False,
                            zero_reason="p0_buy_below_min_child_order",
                        )
                        self._p0_done.add(order.stock_id)
                        continue
                    if order.direction == Order.SELL and _price_at_or_above(close_price, limit_up):
                        self._record_no_fill_reason(
                            order.stock_id,
                            "p0_limit_sell_at_up_limit",
                            trade_start_time,
                            f"price_basis=raw close_raw={close_price:.6f} up_limit_raw={float(limit_up):.6f} factor={factor:.8f}",
                        )
                        self._append_legal_child_order(
                            order_list,
                            order.stock_id,
                            order.direction,
                            amount_remain,
                            amount_remain,
                            unit,
                            trade_start_time,
                            trade_end_time,
                            allow_sell_residual=True,
                            round_by_unit=False,
                            zero_reason="p0_sell_below_min_child_order",
                        )
                        self._p0_done.add(order.stock_id)
                        continue
                    if order.direction == Order.BUY and _price_at_or_above(close_price, limit_up):
                        self._record_no_fill_reason(
                            order.stock_id,
                            "limit_up_buy_blocked",
                            trade_start_time,
                            f"price_basis=raw close_raw={close_price:.6f} up_limit_raw={float(limit_up):.6f} factor={factor:.8f}",
                        )
                        continue
                    if order.direction == Order.SELL and _price_at_or_below(close_price, limit_down):
                        self._record_no_fill_reason(
                            order.stock_id,
                            "limit_down_sell_blocked",
                            trade_start_time,
                            f"price_basis=raw close_raw={close_price:.6f} down_limit_raw={float(limit_down):.6f} factor={factor:.8f}",
                        )
                        continue
                except _V25MarketNoFill as exc:
                    self._record_no_fill_reason(order.stock_id, exc.reason, trade_start_time, exc.detail)
                    continue
                except (KeyError, IndexError, ValueError) as exc:
                    reason = self._market_block_reason(order.stock_id, trade_start_time, trade_end_time)
                    if reason:
                        self._record_no_fill_reason(
                            order.stock_id,
                            "limit_data_missing_due_to_suspend",
                            trade_start_time,
                            f"market_reason={reason} error={exc}",
                        )
                        continue
                    raise RuntimeError(f"V25 P0 limit_price_missing_data_error for {order.stock_id}: {exc}") from exc

            if not self._v25_plan_generated.get(order.stock_id, False):
                try:
                    self._v25_plans[order.stock_id] = self._generate_plan_for_order(
                        order.stock_id, order.direction, trade_start_time, trade_end_time
                    )
                    self._v25_plan_generated[order.stock_id] = True
                except _V25MarketNoFill as exc:
                    self._record_no_fill_reason(order.stock_id, exc.reason, trade_start_time, exc.detail)
                    self._v25_plan_generated[order.stock_id] = False
                    continue
                except Exception:
                    self._v25_plan_failed.add(order.stock_id)
                    raise
            plan = self._v25_plans.get(order.stock_id)
            remaining_day_steps = max(end_idx - trade_step + 1, 1)
            if plan is None:
                raise RuntimeError(
                    f"V25 missing execution plan for {order.stock_id}; refusing to fall back to TWAP"
                )
            else:
                remaining_weight = float(plan[rel_trade_step:].sum())
                if remaining_weight <= 1e-8:
                    raise RuntimeError(f"V25 remaining plan weight is zero: stock={order.stock_id} step={rel_trade_step}")
                base_delta = amount_remain * float(plan[rel_trade_step]) / remaining_weight

            extra = self._realloc_extra.get(order.stock_id, 0)
            extra_delta = extra / remaining_day_steps if extra > 1e-5 and order.direction == Order.BUY else 0.0
            amount_delta = base_delta + extra_delta
            if is_last_step:
                amount_delta = amount_remain + self._realloc_extra.get(order.stock_id, 0)

            max_amount = amount_remain + self._realloc_extra.get(order.stock_id, 0)
            self._append_legal_child_order(
                order_list,
                order.stock_id,
                order.direction,
                amount_delta,
                max_amount,
                unit,
                trade_start_time,
                trade_end_time,
                is_last_step=is_last_step,
                allow_sell_residual=is_last_step and order.direction == Order.SELL,
            )

        if rel_trade_step >= TAIL_START_OFFSET and self._unfilled_handler == "TAIL_SUBSTITUTE":
            existing_sids = {o.stock_id for o in order_list}
            for sid, extra in self._realloc_extra.items():
                if extra <= 1e-5 or sid in existing_sids:
                    continue
                if self.trade_exchange.check_stock_suspended(stock_id=sid, start_time=trade_start_time, end_time=trade_end_time):
                    continue
                remaining_steps = max(end_idx - trade_step + 1, 1)
                amount_delta_target = extra if is_last_step else extra / remaining_steps
                unit = self.trade_exchange.get_amount_of_trade_unit(stock_id=sid, start_time=trade_start_time, end_time=trade_end_time)
                self._append_legal_child_order(
                    order_list,
                    sid,
                    Order.BUY,
                    amount_delta_target,
                    extra,
                    unit,
                    trade_start_time,
                    trade_end_time,
                    is_last_step=is_last_step,
                    zero_reason="tail_substitute_buy_below_min_child_order",
                )

        return TradeDecisionWO(order_list=order_list, strategy=self)
