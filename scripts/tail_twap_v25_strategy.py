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


class TailTWAPWithV25TwoStageStrategy(TailTWAPWithLimitStrategy):
    """V25 two-stage execution mapped to Qlib NestedExecutor inner_strategy."""

    def __init__(
        self,
        early_model_path: str,
        late_model_path: str,
        device: str = "cpu",
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

    def _generate_plan_for_order(self, stock_id, direction, trade_start_time, trade_end_time):
        open_price = self.trade_exchange.get_close(stock_id, trade_start_time, trade_end_time, method="ts_data_last")
        prev_close = self.trade_exchange.quote.get_data(
            stock_id, trade_start_time, trade_end_time, field="$prev_close", method="ts_data_last"
        )
        if open_price is None or np.isnan(open_price) or open_price <= 0:
            raise RuntimeError(f"invalid open/current price for V25 plan: stock={stock_id} price={open_price}")
        if prev_close is None or np.isnan(prev_close) or prev_close <= 0:
            raise RuntimeError(f"missing prev_close for V25 plan: stock={stock_id} prev_close={prev_close}")
        limit_pct = _get_limit_pct(stock_id)
        gap_pct = np.clip((float(open_price) - float(prev_close)) / float(prev_close), -0.20, 0.20)
        gap_ratio = float(gap_pct / limit_pct) if limit_pct > 1e-8 else 0.0
        gap_bucket = _gap_ratio_to_bucket(gap_ratio)
        is_buy = 1.0 if direction == Order.BUY else 0.0
        day_features = np.zeros(10, dtype=np.float32)

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
            "[TailTWAPv25] generated plan stock=%s is_buy=%s early_sum=%.4f late_sum=%.4f gap_ratio=%.4f",
            stock_id, bool(is_buy), early_sum, late_sum, gap_ratio,
        )
        return plan

    def generate_trade_decision(self, execute_result=None):
        if len(self.outer_trade_decision.get_decision()) == 0:
            return TradeDecisionWO(order_list=[], strategy=self)

        trade_step = self.trade_calendar.get_trade_step()
        start_idx, end_idx = get_start_end_idx(self.trade_calendar, self.outer_trade_decision)
        trade_len = end_idx - start_idx + 1
        if trade_step < start_idx or trade_step > end_idx:
            return TradeDecisionWO(order_list=[], strategy=self)

        rel_trade_step = trade_step - start_idx
        has_auction = trade_len == 241
        if has_auction:
            if rel_trade_step == 0:
                return TradeDecisionWO(order_list=[], strategy=self)
            rel_trade_step -= 1
        if rel_trade_step < 0 or rel_trade_step >= TOTAL_LEN:
            return TradeDecisionWO(order_list=[], strategy=self)

        if execute_result is not None:
            for order, _, _, _ in execute_result:
                if order.stock_id in self.trade_amount_remain:
                    self.trade_amount_remain[order.stock_id] -= order.deal_amount
                if order.stock_id in self._realloc_extra:
                    self._realloc_extra[order.stock_id] = max(0, self._realloc_extra[order.stock_id] - order.deal_amount)

        trade_start_time, trade_end_time = self.trade_calendar.get_step_time(trade_step)
        is_last_step = rel_trade_step >= TOTAL_LEN - 1 or trade_step >= end_idx
        trigger_step = REALLOC_OFFSET - 1
        if rel_trade_step >= trigger_step and not self._realloc_done:
            self._realloc_done = True
            if self._unfilled_handler == "TAIL_SUBSTITUTE":
                self._do_realloc_substitute(trade_start_time, trade_end_time)
            else:
                self._do_realloc(trade_start_time, trade_end_time)

        order_list = []
        for order in self.outer_trade_decision.get_decision():
            if self.trade_exchange.check_stock_suspended(
                stock_id=order.stock_id,
                start_time=trade_start_time,
                end_time=trade_end_time,
            ):
                self._v25_no_fill_reasons[order.stock_id] = "suspended_by_exchange"
                continue
            if self._is_artifact_suspended(order.stock_id, trade_start_time):
                logger.info(
                    "[TailTWAPv25] skip suspended stock from artifact stock=%s trade_time=%s reason=suspended_by_suspend_d",
                    order.stock_id,
                    trade_start_time,
                )
                self._v25_no_fill_reasons[order.stock_id] = "suspended_by_suspend_d"
                continue
            amount_remain = self.trade_amount_remain[order.stock_id]
            if amount_remain <= 1e-5:
                continue

            unit = self.trade_exchange.get_amount_of_trade_unit(
                stock_id=order.stock_id,
                start_time=order.start_time,
                end_time=order.end_time,
            )
            effective_unit = unit if unit is not None and unit > 0 else 100
            if order.direction == Order.BUY and amount_remain < effective_unit:
                self._v25_no_fill_reasons[order.stock_id] = "buy_below_trade_unit"
                continue
            if order.direction == Order.SELL and amount_remain < effective_unit:
                # Odd-lot/fractional sell orders are cleanup orders; sending
                # them immediately avoids wasting V25 slices on sub-lot dust.
                order_list.append(Order(
                    stock_id=order.stock_id,
                    amount=amount_remain,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                    direction=order.direction,
                ))
                continue

            if order.stock_id not in self._p0_done:
                try:
                    close_price = self.trade_exchange.get_close(order.stock_id, trade_start_time, trade_end_time, method="ts_data_last")
                    if close_price is None or np.isnan(close_price) or close_price <= 0:
                        raise RuntimeError(f"invalid close price: {close_price}")
                    limit_up = self.trade_exchange.quote.get_data(order.stock_id, trade_start_time, trade_end_time, field="$up_limit_price", method="ts_data_last")
                    limit_down = self.trade_exchange.quote.get_data(order.stock_id, trade_start_time, trade_end_time, field="$down_limit_price", method="ts_data_last")
                    if limit_up is None or np.isnan(limit_up) or limit_up <= 0:
                        raise RuntimeError(f"invalid up_limit_price: {limit_up}")
                    if limit_down is None or np.isnan(limit_down) or limit_down <= 0:
                        raise RuntimeError(f"invalid down_limit_price: {limit_down}")
                    if order.direction == Order.BUY and close_price <= limit_down:
                        order_list.append(Order(
                            stock_id=order.stock_id,
                            amount=amount_remain,
                            start_time=trade_start_time,
                            end_time=trade_end_time,
                            direction=order.direction,
                        ))
                        self._p0_done.add(order.stock_id)
                        continue
                    if order.direction == Order.SELL and close_price >= limit_up:
                        order_list.append(Order(
                            stock_id=order.stock_id,
                            amount=amount_remain,
                            start_time=trade_start_time,
                            end_time=trade_end_time,
                            direction=order.direction,
                        ))
                        self._p0_done.add(order.stock_id)
                        continue
                except (KeyError, IndexError, ValueError) as exc:
                    raise RuntimeError(f"V25 P0 limit data missing for {order.stock_id}: {exc}") from exc

            if not self._v25_plan_generated.get(order.stock_id, False):
                self._v25_plan_generated[order.stock_id] = True
                self._v25_plans[order.stock_id] = self._generate_plan_for_order(
                    order.stock_id, order.direction, trade_start_time, trade_end_time
                )
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
            if is_last_step and order.direction == Order.SELL:
                # A-share sells can clear odd-lot/fractional residuals; do not
                # re-round final liquidation back to the 100-share trade unit.
                amount_delta_target = min(amount_delta, max_amount)
            elif unit is not None and unit > 0:
                amount_delta_target = min(np.round(amount_delta / unit) * unit, max_amount)
            else:
                amount_delta_target = min(amount_delta, max_amount)
            if amount_delta_target > 1e-5:
                order_list.append(Order(
                    stock_id=order.stock_id,
                    amount=amount_delta_target,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                    direction=order.direction,
                ))

        trigger_step = TAIL_START_OFFSET - 1
        if rel_trade_step >= trigger_step and self._unfilled_handler == "TAIL_SUBSTITUTE":
            existing_sids = {o.stock_id for o in order_list}
            for sid, extra in self._realloc_extra.items():
                if extra <= 1e-5 or sid in existing_sids:
                    continue
                if self.trade_exchange.check_stock_suspended(stock_id=sid, start_time=trade_start_time, end_time=trade_end_time):
                    continue
                remaining_steps = max(end_idx - trade_step + 1, 1)
                amount_delta_target = extra if is_last_step else extra / remaining_steps
                unit = self.trade_exchange.get_amount_of_trade_unit(stock_id=sid, start_time=trade_start_time, end_time=trade_end_time)
                if unit is not None and unit > 0:
                    amount_delta_target = min(np.round(amount_delta_target / unit) * unit, extra)
                if amount_delta_target > 1e-5:
                    order_list.append(Order(
                        stock_id=sid,
                        amount=amount_delta_target,
                        start_time=trade_start_time,
                        end_time=trade_end_time,
                        direction=Order.BUY,
                    ))

        return TradeDecisionWO(order_list=order_list, strategy=self)
