"""QE ScoreWeighted V2 wrappers with causal sector-risk exposure control."""
from __future__ import annotations

import json
import math
from pathlib import Path

from qlib.backtest.decision import Order, OrderDir

from qe_sector_risk_overlay import QESectorRiskOverlayPolicy
from qe_suspend_filter_score_weighted_strategy import _SuspendFilterScoreWeightedMixin
from score_weighted_strategy_v2 import ScoreWeightedTopkStrategyV2
from score_weighted_strategy_v2_capacity_v1 import ScoreWeightedTopkStrategyV2CapacityV1


class _QESectorRiskOverlayMixin:
    def __init__(
        self,
        *args,
        sector_risk_overlay_enabled=False,
        sector_risk_overlay_mode="none",
        sector_risk_overlay_manifest_file=None,
        sector_risk_overlay_data_file=None,
        sector_risk_overlay_strict=True,
        sector_risk_overlay_override_hold_thresh=True,
        sector_risk_overlay_reentry_confirm_days=3,
        sector_risk_overlay_state_multipliers=None,
        sector_risk_overlay_action_log=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._qe_sector_risk_policy = QESectorRiskOverlayPolicy(
            enabled=sector_risk_overlay_enabled,
            mode=sector_risk_overlay_mode,
            manifest_file=sector_risk_overlay_manifest_file,
            data_file=sector_risk_overlay_data_file,
            strict=sector_risk_overlay_strict,
            reentry_confirm_days=sector_risk_overlay_reentry_confirm_days,
            state_multipliers=sector_risk_overlay_state_multipliers,
        )
        self._qe_sector_risk_override_hold_thresh = bool(
            sector_risk_overlay_override_hold_thresh
        )
        self._qe_sector_risk_action_log = (
            Path(str(sector_risk_overlay_action_log)).expanduser().resolve()
            if sector_risk_overlay_action_log
            else None
        )
        if self._qe_sector_risk_policy.enabled and self._qe_sector_risk_action_log is None:
            raise RuntimeError("enabled QE sector-risk overlay requires an action log path")
        if self._qe_sector_risk_action_log is not None:
            self._qe_sector_risk_action_log.parent.mkdir(parents=True, exist_ok=True)
            self._qe_sector_risk_action_log.touch(exist_ok=True)
        self._qe_sector_risk_action_keys = set()
        self._qe_sector_risk_missing_action_keys = set()
        self._qe_sector_risk_last_multiplier = {}
        self._qe_sector_risk_base_weights = {}
        if self._qe_sector_risk_policy.enabled and self._qe_sector_risk_policy.mode in {
            "bounded_de_risk",
            "exit_reentry",
        }:
            parent = super(_QESectorRiskOverlayMixin, self)
            missing_hooks = [
                name
                for name in (
                    "_adjust_target_weight_map",
                    "_build_additional_rebalance_orders",
                )
                if not callable(getattr(parent, name, None))
            ]
            if missing_hooks:
                raise RuntimeError(
                    "QE sector-risk overlay parent strategy is missing required rebalance hooks: "
                    + ", ".join(missing_hooks)
                )

    def _record_sector_risk_action(self, payload):
        if not self._qe_sector_risk_policy.enabled:
            return
        event = dict(payload)
        event["policy_mode"] = self._qe_sector_risk_policy.mode
        event["policy_hash"] = str(
            (self._qe_sector_risk_policy.manifest or {}).get("manifest_payload_sha256") or ""
        )
        key = (
            str(event.get("trade_date")),
            str(event.get("instrument")),
            str(event.get("action_type")),
            event["policy_hash"],
        )
        if key in self._qe_sector_risk_action_keys:
            raise RuntimeError(f"duplicate QE sector-risk action identity: {key}")
        self._qe_sector_risk_action_keys.add(key)
        with self._qe_sector_risk_action_log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()

    def _record_missing_sector_risk_rows(self):
        for event in self._qe_sector_risk_policy.missing_row_events():
            key = (str(event["trade_date"]), str(event["instrument"]))
            if key in self._qe_sector_risk_missing_action_keys:
                continue
            self._qe_sector_risk_missing_action_keys.add(key)
            self._record_sector_risk_action(
                {
                    **event,
                    "action_type": "MISSING_ARTIFACT_ROW",
                    "target_multiplier": 1.0,
                    "order_generated": False,
                    "reason": "sector_risk_runtime_missing_artifact_row_neutral",
                }
            )

    def _normalize_signal_scores(self, all_pred_scores, pred_end_time):
        scores = super()._normalize_signal_scores(all_pred_scores, pred_end_time)
        if not self._qe_sector_risk_policy.enabled or scores is None or scores.empty:
            return scores
        trade_time = self._qe_suspend_filter_trade_time or pred_end_time
        current_holdings = set(self.trade_position.get_stock_list())
        blocked = []
        for instrument in scores.index:
            if instrument in current_holdings:
                continue
            if not self._qe_sector_risk_policy.entry_allowed(instrument, trade_time):
                blocked.append(instrument)
                self._record_sector_risk_action(
                    {
                        "trade_date": str(trade_time)[:10],
                        "instrument": str(instrument),
                        "action_type": "ENTRY_BLOCK",
                        "risk_state": self._qe_sector_risk_policy.state(instrument, trade_time),
                        "target_multiplier": self._qe_sector_risk_policy.multiplier(
                            instrument, trade_time
                        ),
                        "order_generated": False,
                        "reason": "sector_risk_entry_not_allowed",
                    }
                )
        self._record_missing_sector_risk_rows()
        return scores.drop(blocked, errors="ignore") if blocked else scores

    def _adjust_target_weight_map(self, weight_map, trade_start_time):
        base = super()._adjust_target_weight_map(weight_map, trade_start_time)
        self._qe_sector_risk_base_weights = dict(base)
        if not self._qe_sector_risk_policy.enabled:
            return base
        adjusted = {
            instrument: float(weight)
            * self._qe_sector_risk_policy.multiplier(instrument, trade_start_time)
            for instrument, weight in base.items()
        }
        self._record_missing_sector_risk_rows()
        return adjusted

    def _round_amount(self, amount, *, factor, instrument, trade_start_time, trade_end_time):
        unit = self.trade_exchange.get_amount_of_trade_unit(
            factor=factor,
            stock_id=instrument,
            start_time=trade_start_time,
            end_time=trade_end_time,
        )
        if unit is None or not math.isfinite(float(unit)) or float(unit) <= 0:
            unit = self._shares_to_adjusted_amount(self.lot_size, factor)
        if unit is None or not math.isfinite(float(unit)) or float(unit) <= 0:
            raise RuntimeError(f"invalid trade unit for sector-risk order: {instrument}")
        return float(math.floor(float(amount) / float(unit)) * float(unit))

    def _build_additional_rebalance_orders(
        self,
        *,
        weight_map,
        current_holdings,
        existing_sell_ids,
        planned_buy_orders,
        total_account_value,
        trade_step,
        trade_start_time,
        trade_end_time,
    ):
        orders = list(
            super()._build_additional_rebalance_orders(
                weight_map=weight_map,
                current_holdings=current_holdings,
                existing_sell_ids=existing_sell_ids,
                planned_buy_orders=planned_buy_orders,
                total_account_value=total_account_value,
                trade_step=trade_step,
                trade_start_time=trade_start_time,
                trade_end_time=trade_end_time,
            )
        )
        policy = self._qe_sector_risk_policy
        if not policy.enabled or policy.mode in {"none", "entry_gate"}:
            return orders
        planned_buy_value = 0.0
        for order in planned_buy_orders:
            price = self._get_current_price(order.stock_id, trade_step, OrderDir.BUY)
            if price is not None and float(price) > 0:
                planned_buy_value += float(order.amount) * float(price)
        available_cash = max(0.0, float(self.trade_position.get_cash()) - planned_buy_value)
        trade_date = str(trade_start_time)[:10]

        for instrument in current_holdings:
            instrument = str(instrument)
            if instrument in existing_sell_ids or instrument not in weight_map:
                continue
            price = self._get_current_price(instrument, trade_step, OrderDir.SELL)
            amount = self.trade_position.get_stock_amount(instrument)
            if price is None or float(price) <= 0 or amount is None or float(amount) <= 0:
                continue
            state = policy.state(instrument, trade_start_time)
            multiplier = policy.multiplier(instrument, trade_start_time)
            base_weight = float(self._qe_sector_risk_base_weights.get(instrument, 0.0))
            target_value = float(total_account_value) * base_weight * multiplier
            current_value = float(amount) * float(price)
            factor = self._get_current_factor(instrument, trade_step)
            last_multiplier = float(self._qe_sector_risk_last_multiplier.get(instrument, 1.0))

            if current_value > target_value:
                if not self._qe_sector_risk_override_hold_thresh and not self._can_sell_under_hold_thresh(
                    instrument, trade_start_time
                ):
                    self._record_sector_risk_action(
                        {
                            "trade_date": trade_date,
                            "instrument": instrument,
                            "action_type": "DE_RISK_BLOCKED_BY_HOLD",
                            "risk_state": state,
                            "base_weight": base_weight,
                            "target_weight": base_weight * multiplier,
                            "target_multiplier": multiplier,
                            "current_amount": float(amount),
                            "order_generated": False,
                            "hold_thresh_overridden": False,
                            "reason": "hold_thresh_not_overridden",
                        }
                    )
                    self._qe_sector_risk_last_multiplier[instrument] = multiplier
                    continue
                target_shares = target_value / float(price)
                target_amount = self._shares_to_adjusted_amount(target_shares, factor)
                sell_amount = self._round_amount(
                    max(0.0, float(amount) - float(target_amount)),
                    factor=factor,
                    instrument=instrument,
                    trade_start_time=trade_start_time,
                    trade_end_time=trade_end_time,
                )
                sell_amount = min(float(amount), sell_amount)
                orderable = sell_amount > 0 and self._is_orderable_without_warning(
                    instrument, trade_start_time, trade_end_time, OrderDir.SELL
                )
                if orderable:
                    orders.append(
                        Order(
                            instrument,
                            sell_amount,
                            OrderDir.SELL,
                            trade_start_time,
                            trade_end_time,
                        )
                    )
                self._record_sector_risk_action(
                    {
                        "trade_date": trade_date,
                        "instrument": instrument,
                        "action_type": "EXIT" if multiplier == 0.0 else "DE_RISK_SELL",
                        "risk_state": state,
                        "base_weight": base_weight,
                        "target_weight": base_weight * multiplier,
                        "target_multiplier": multiplier,
                        "current_amount": float(amount),
                        "order_amount": sell_amount,
                        "order_generated": bool(orderable),
                        "hold_thresh_overridden": self._qe_sector_risk_override_hold_thresh,
                        "reason": "sector_risk_target_exposure",
                    }
                )
            elif (
                multiplier > last_multiplier
                and policy.entry_allowed(instrument, trade_start_time)
                and target_value > current_value
                and available_cash > 0
            ):
                desired_value = min(target_value - current_value, available_cash)
                desired_amount = self._shares_to_adjusted_amount(desired_value / float(price), factor)
                buy_amount = self._round_amount(
                    desired_amount,
                    factor=factor,
                    instrument=instrument,
                    trade_start_time=trade_start_time,
                    trade_end_time=trade_end_time,
                )
                orderable = buy_amount > 0 and self._is_orderable_without_warning(
                    instrument, trade_start_time, trade_end_time, OrderDir.BUY
                )
                if orderable:
                    orders.append(
                        Order(
                            instrument,
                            buy_amount,
                            OrderDir.BUY,
                            trade_start_time,
                            trade_end_time,
                        )
                    )
                    available_cash -= buy_amount * float(price)
                self._record_sector_risk_action(
                    {
                        "trade_date": trade_date,
                        "instrument": instrument,
                        "action_type": "REENTRY_BUY",
                        "risk_state": state,
                        "base_weight": base_weight,
                        "target_weight": base_weight * multiplier,
                        "target_multiplier": multiplier,
                        "current_amount": float(amount),
                        "order_amount": buy_amount,
                        "order_generated": bool(orderable),
                        "hold_thresh_overridden": False,
                        "reason": "sector_risk_reentry_confirmed",
                    }
                )
            self._qe_sector_risk_last_multiplier[instrument] = multiplier
        self._record_missing_sector_risk_rows()
        return orders


class QESectorRiskOverlayScoreWeightedTopkStrategyV2(
    _QESectorRiskOverlayMixin,
    _SuspendFilterScoreWeightedMixin,
    ScoreWeightedTopkStrategyV2,
):
    pass


class QESectorRiskOverlayScoreWeightedTopkStrategyV2CapacityV1(
    _QESectorRiskOverlayMixin,
    _SuspendFilterScoreWeightedMixin,
    ScoreWeightedTopkStrategyV2CapacityV1,
):
    pass
