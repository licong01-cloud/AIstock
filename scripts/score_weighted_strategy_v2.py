"""
Score-Weighted TopK Strategy V2 — TopK 强制执行修复版

基于 ScoreWeightedTopkStrategy (v1) 修复三个导致持仓失控的 Bug：

Bug #1（主因）：补仓模式不卖出 + 幽灵持仓导致持仓失控
  v1 中补仓模式 actual_sells=[] 且幽灵持仓静默积累，导致：
  - 持仓中不在 TopK 的股票永远不被卖出
  - 每天买入 topk - len(current_holdings) 只新股票
  - 持仓单调递增至 1781 只，topk=50 完全失效
  修复：补仓模式也卖出跌出 TopK 的持仓，建仓阶段直接填满到 topk 不限速

Bug #2（次因）：幽灵持仓静默积累
  v1 中 final_holdings 过滤掉 scores.index 之外的旧持仓，但不生成卖出订单，
  导致这些股票永远持有但永远不参与权重分配，也永远不被卖出。
  修复：无评分的旧持仓直接加入 actual_sells 强制卖出。

Bug #3（V2 引入）：补仓模式禁止所有卖出导致现金枯竭死锁
  V2 修复 Bug#1 时，将补仓模式的 actual_sells 设为 []（空列表），
  导致持仓不满 topk 时完全不执行日频卖出。结果：
  - 早期买入消耗全部现金，持仓卡在 topk/2 附近
  - 已持仓股票跌出 TopK 也不会被卖出（实测 26 只持仓中 0 只在 Top50 内）
  - 无卖出 → 无现金释放 → 无法买入新候选 → 策略永久冻结
  修复：补仓模式也卖出跌出 TopK 的持仓（受 max_n_drop 约束），
  卖出释放现金后再买入新候选，保证策略持续运转。

Created: 2026-04-13
Updated: 2026-04-14  Fix #3 + Fix #4 (建仓不限速)
"""

import logging
import numpy as np
import pandas as pd
from qlib.backtest.decision import Order, OrderDir, TradeDecisionWO

from score_weighted_strategy import ScoreWeightedTopkStrategy

logger = logging.getLogger(__name__)


class ScoreWeightedTopkStrategyV2(ScoreWeightedTopkStrategy):
    """
    ScoreWeightedTopkStrategy 的修复版，topk 持仓数量严格受控。

    与 v1 参数完全兼容，可直接替换。修复内容：
    1. 建仓阶段直接填满到 topk（不限制速度），换仓受 max_n_drop 约束
    2. 无评分的旧持仓（停牌/退市等）强制卖出，消除幽灵持仓
    3. 补仓模式也卖出跌出 TopK 的持仓，避免现金枯竭死锁
    """

    def _apply_hmm_risk_gate(self, pred_score: pd.Series, trade_date_str: str) -> pd.Series:
        """Remove blocked stocks from candidate pool (transition-based risk gate)."""
        if not getattr(self, "enable_hmm_risk_gate", False):
            return pred_score
        if pred_score is None or (hasattr(pred_score, "empty") and pred_score.empty):
            return pred_score

        if not hasattr(self, "_risk_gate_config") or self._risk_gate_config is None:
            gate_file = getattr(self, "hmm_risk_gate_file", None)
            if not gate_file:
                raise RuntimeError("enable_hmm_risk_gate=True but hmm_risk_gate_file not set")
            import json
            with open(gate_file, "r", encoding="utf-8") as f:
                self._risk_gate_config = json.load(f)
            if self._risk_gate_config.get("artifact_type") != "hmm_risk_gate_v1":
                raise RuntimeError(f"Invalid risk gate artifact type")
            logger.info("Loaded HMM risk gate: %d sectors", self._risk_gate_config.get("sector_count", 0))

        daily_gates = self._risk_gate_config.get("daily_gates", {})
        stock_sector_map = self._risk_gate_config.get("stock_sector_map", {})
        protect_top = self._risk_gate_config.get("gate_config", {}).get("protect_top", 30)

        gates_today = daily_gates.get(trade_date_str)
        if not gates_today:
            return pred_score

        blocked_sectors = {s for s, g in gates_today.items() if g.get("blocked", False)}
        if not blocked_sectors:
            return pred_score

        sorted_scores = pred_score.sort_values(ascending=False)
        protected = set(sorted_scores.head(protect_top).index)
        current_holdings = set(self.trade_position.get_stock_list())

        blocked = []
        for sym in sorted_scores.index:
            if sym in protected or sym in current_holdings:
                continue
            sector = stock_sector_map.get(sym)
            if sector and sector in blocked_sectors:
                blocked.append(sym)

        if blocked:
            logger.info("[RiskGate] date=%s blocked %d stocks from %d fading sectors",
                       trade_date_str, len(blocked), len(blocked_sectors))
            return pred_score.drop(blocked, errors="ignore")
        return pred_score

    def _can_sell_under_hold_thresh(self, sid, trade_start_time):
        hold_thresh = float(getattr(self, "hold_thresh", 0) or 0)
        if hold_thresh <= 0:
            return True
        time_per_step = self.trade_calendar.get_freq()
        try:
            hold_count = self.trade_position.get_stock_count(sid, bar=time_per_step)
        except TypeError:
            hold_count = self.trade_position.get_stock_count(sid)
        if hold_count is None:
            raise RuntimeError(
                f"[ScoreWeightedV2] hold_thresh check returned None for {sid}; "
                "refusing to sell silently"
            )
        if float(hold_count) < hold_thresh:
            logger.info(
                "[ScoreWeightedV2] hold_thresh blocked sell: stock=%s hold_count=%s threshold=%s date=%s",
                sid, hold_count, hold_thresh, trade_start_time,
            )
            return False
        return True

    def generate_trade_decision(self, execute_result=None):
        trade_step = self.trade_calendar.get_trade_step()
        trade_start_time, trade_end_time = self.trade_calendar.get_step_time(trade_step)

        cur_dt = pd.Timestamp(trade_start_time).date() if trade_start_time is not None else None
        if cur_dt != self._last_diag_date:
            self._diag_stats = {"sells": 0, "buys": 0, "ndrop_filtered": 0, "threshold": 0.0}
            self._last_diag_date = cur_dt

        # 1. 获取预测评分
        pred_start_time, pred_end_time = self.trade_calendar.get_step_time(trade_step, shift=1)
        all_pred_scores = self.signal.get_signal(start_time=pred_start_time, end_time=pred_end_time)

        scores = self._normalize_signal_scores(all_pred_scores, pred_end_time)

        if scores is None or scores.empty:
            return TradeDecisionWO([], self)

        # 1.5 HMM 行业热度调整
        trade_date_str = str(trade_start_time)[:10]
        scores = self._apply_hmm_adjustment(scores, trade_date_str)

        # 1.6 HMM Risk Gate: 阻止新买入 fading 行业的边界股票
        scores = self._apply_hmm_risk_gate(scores, trade_date_str)

        # 2. 当前持仓
        current_holdings = list(self.trade_position.get_stock_list())

        # 3. TopK 候选
        # kind='mergesort': stable sort ensures deterministic tie-breaking
        # across different CPU SIMD (AVX-512 vs AVX2) — fixes cross-node divergence
        ranked = scores.sort_values(ascending=False, kind='mergesort')
        topk_stocks = set(ranked.head(self.topk).index.tolist())

        # ── Fix #2: 幽灵持仓强制卖出 ──
        # 无评分的旧持仓（停牌/退市等）直接加入卖出列表，不再静默积累
        ghost_sells = [s for s in current_holdings if s not in scores.index]
        if ghost_sells:
            logger.info(
                "[ScoreWeightedV2] 幽灵持仓强制卖出: %d 只. date=%s",
                len(ghost_sells), cur_dt,
            )

        # 有效持仓（有评分的）
        valid_holdings = [s for s in current_holdings if s in scores.index]

        # 4. 确定 sell/buy 候选（基于有效持仓）
        sell_candidates = []
        for sid in valid_holdings:
            if sid not in topk_stocks:
                sc = float(scores.get(sid, -999.0))
                sell_candidates.append((sid, sc))
        sell_candidates.sort(key=lambda x: x[1])

        buy_candidates = []
        for sid in topk_stocks:
            if sid not in current_holdings:
                sc = float(scores.get(sid, 0.0))
                buy_candidates.append((sid, sc))
        buy_candidates.sort(key=lambda x: -x[1])

        # 5. 建仓/补仓 vs 动态 n_drop
        # 建仓阶段：直接买入填满到 topk（不限制速度）
        # 换仓阶段：sell-buy 配对受 max_n_drop 约束
        if len(valid_holdings) < self.topk:
            # 补仓模式：卖出跌出TopK的持仓（受 max_n_drop 约束）+ 买入填满空位
            actual_sells = [s[0] for s in sell_candidates[:self.max_n_drop]]

            # 卖出后的剩余持仓数，计算可买入空位（建仓不限速）
            remaining_after_sell = len(valid_holdings) - len(actual_sells)
            buy_slots = self.topk - remaining_after_sell
            actual_buys = [b[0] for b in buy_candidates[:buy_slots]]
            logger.info(
                "[ScoreWeightedV2] 补仓模式: valid_holdings=%d < topk=%d, "
                "卖出 %d 只, 买入 %d 只 (建仓不限速). date=%s",
                len(valid_holdings), self.topk,
                len(actual_sells), len(actual_buys), cur_dt,
            )
        else:
            # 正常换仓模式：sell-buy 配对 + 动态 n_drop
            current_scores_arr = np.array(
                [float(scores.get(s, 0.0)) for s in valid_holdings]
            )
            actual_sells, actual_buys = self._filter_dynamic_ndrop(
                sell_candidates, buy_candidates, current_scores_arr,
            )

        # 合并幽灵持仓卖出
        requested_sells = list(actual_sells) + ghost_sells
        all_sells = []
        hold_blocked_sells = []
        for sid in requested_sells:
            if self._can_sell_under_hold_thresh(sid, trade_start_time):
                all_sells.append(sid)
            else:
                hold_blocked_sells.append(sid)
        all_sells_set = set(all_sells)
        retained_count = len([s for s in current_holdings if s not in all_sells_set])
        max_buy_slots = max(0, self.topk - retained_count)
        if len(actual_buys) > max_buy_slots:
            actual_buys = actual_buys[:max_buy_slots]
        self._diag_stats["hold_blocked_sells"] = len(hold_blocked_sells)
        self._diag_stats["ndrop_filtered"] = len(sell_candidates) - len(actual_sells) + len(hold_blocked_sells)


        # 6. 计算最终持仓列表和权重
        # 此时 final_holdings 中所有股票都在 scores.index 中，无需再过滤
        final_holdings = [s for s in current_holdings if s not in all_sells_set] + actual_buys
        scored_final_holdings = [s for s in final_holdings if s in scores.index]
        final_scores = np.array([float(scores[s]) for s in scored_final_holdings])
        weights = self._compute_weights(final_scores) if len(scored_final_holdings) else np.array([])

        if len(weights) == 0 and not all_sells:
            return TradeDecisionWO([], self)

        # 构建 symbol -> weight 映射
        weight_map = {}
        for i, sid in enumerate(scored_final_holdings):
            if i < len(weights):
                weight_map[sid] = float(weights[i])

        # 7. 生成卖出订单（含幽灵持仓）
        sell_orders = []
        for sid in all_sells:
            amount = self.trade_position.get_stock_amount(sid)
            if amount is not None and float(amount) > 0:
                sell_orders.append(
                    Order(sid, amount, OrderDir.SELL, trade_start_time, trade_end_time)
                )
            else:
                logger.warning(
                    "[ScoreWeightedV2] 卖出目标 %s 的持仓量为 %s, 跳过. date=%s",
                    sid, amount, cur_dt,
                )
        self._diag_stats["sells"] = len(sell_orders)

        # 8. 生成买入订单（按权重分配资金给新增持仓）
        total_cash = self.trade_position.get_cash()
        if total_cash is None:
            raise RuntimeError(
                f"trade_position.get_cash() 返回 None, 持仓状态异常. date={cur_dt}"
            )
        total_account_value = float(total_cash)
        for sid in current_holdings:
            stock_price = self._get_current_price(sid, trade_step, OrderDir.SELL)
            if stock_price is not None and not (isinstance(stock_price, float) and np.isnan(stock_price)) and stock_price > 0:
                amt = self.trade_position.get_stock_amount(sid)
                if amt is not None and float(amt) > 0:
                    total_account_value += float(amt) * float(stock_price)

        buy_orders = []
        for sid in actual_buys:
            w = weight_map.get(sid, 0.0)
            if w <= 0:
                continue

            target_value = total_account_value * w
            target_value = min(target_value, self.max_single_order_value)

            buy_price = self._get_current_price(sid, trade_step, OrderDir.BUY)
            if buy_price is None or (isinstance(buy_price, float) and np.isnan(buy_price)) or buy_price <= 0:
                logger.warning("[ScoreWeightedV2] %s 买入价格无效: %s, 跳过. date=%s", sid, buy_price, cur_dt)
                continue
            if buy_price < self.min_trade_price or buy_price > self.max_trade_price:
                logger.warning(
                    "[ScoreWeightedV2] %s 价格 %.2f 超出范围 [%.2f, %.2f], 跳过. date=%s",
                    sid, buy_price, self.min_trade_price, self.max_trade_price, cur_dt,
                )
                continue

            factor = self._get_current_factor(sid, trade_step)
            target_shares = target_value / float(buy_price)
            buy_amount = self._shares_to_adjusted_amount(target_shares, factor)

            avail_cash = float(self.trade_position.get_cash())
            affordable_shares = avail_cash / float(buy_price)
            affordable_amount = self._shares_to_adjusted_amount(affordable_shares, factor)
            if np.isfinite(affordable_amount):
                buy_amount = min(float(buy_amount), float(affordable_amount))

            amount_unit = self.trade_exchange.get_amount_of_trade_unit(
                factor=factor, stock_id=sid,
                start_time=trade_start_time, end_time=trade_end_time,
            )
            if amount_unit is None or not np.isfinite(float(amount_unit)) or float(amount_unit) <= 0:
                amount_unit = self._shares_to_adjusted_amount(self.lot_size, factor)
                logger.debug(
                    "[ScoreWeightedV2] %s 无 trade_unit 信息, 使用 lot_size=%s", sid, self.lot_size,
                )
            if np.isfinite(buy_amount) and amount_unit is not None and float(amount_unit) > 0:
                buy_amount = float(np.floor(buy_amount / float(amount_unit)) * float(amount_unit))

            if buy_amount > 0:
                buy_orders.append(
                    Order(sid, buy_amount, OrderDir.BUY, trade_start_time, trade_end_time)
                )

        self._diag_stats["buys"] = len(buy_orders)

        # 备选股（供 inner_strategy TAIL_SUBSTITUTE 使用）
        # 修复：提供完整的高排名候选列表，不限制在 topk 之外
        # TAIL_SUBSTITUTE 会按排名从高到低选择可交易的股票
        backup_depth = 100
        backup_sids = ranked.iloc[:self.topk + backup_depth].index.tolist()
        already_ordered = set(actual_buys)
        self._backup_candidates = [
            (sid, float(ranked[sid])) for sid in backup_sids
            if sid not in current_holdings and sid not in already_ordered
        ]

        if cur_dt is not None:
            logger.info(
                "[ScoreWeightedV2] date=%s holdings=%d→%d sells=%d(ghost=%d) buys=%d "
                "ndrop_filtered=%d weight=%s threshold=%.4f",
                cur_dt, len(current_holdings), len(final_holdings),
                len(sell_orders), len(ghost_sells), len(buy_orders),
                self._diag_stats["ndrop_filtered"], self.weight_method,
                self._diag_stats.get("threshold", 0.0),
            )

        return TradeDecisionWO(sell_orders + buy_orders, self)
