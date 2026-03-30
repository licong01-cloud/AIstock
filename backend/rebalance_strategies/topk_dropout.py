"""TopkDropout 持仓再平衡策略.

逻辑来源：Qlib TopkDropoutStrategy，适配 AIStock 模拟盘平台。

策略逻辑：
  1. 冷启动（无持仓）：直接买入评分最高的 topk 只股票。
  2. 正常运行：
     a. 持仓中已跌出 topk 的股票 → 强制卖出（force_sell）
     b. 持仓中仍在 topk 但评分最差的若干只 → dropout 卖出
     c. 买入 topk 中未持有的最优股票，补齐持仓到 topk

可配置参数（全部从 portfolio_config 读取）：
  max_positions    : topk 持仓数量上限（默认 20）
  max_turnover_pct : 每日最大换手比例，决定 n_drop（默认 0.15，即每天最多换 15%）
  max_position_pct : 单只股票最大权重（默认 0.10）
  risk_degree      : 风险度，1.0=全仓，<1.0=留现金缓冲（默认 0.95）
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable, Dict, List, Optional

from .base_strategy import BaseRebalanceStrategy
from .registry import register

logger = logging.getLogger("aistock.rebalance.topk_dropout")


@register
class TopkDropoutStrategy(BaseRebalanceStrategy):
    """TopkDropout 持仓再平衡策略.

    与平台的契约：
    - 输入 score_items 为**全量**评分列表（不截断），已按 score 降序排列
    - score_map 覆盖全量，确保持仓股票都能查到评分
    - 输出为 signal 列表，由平台写入 trade_signals 表
    """

    STRATEGY_CODE = "TOPK_DROPOUT"

    def generate_orders(
        self,
        score_items: List[Dict[str, Any]],
        current_positions: Dict[str, Dict[str, Any]],
        portfolio_value: float,
        config: Dict[str, Any],
        signal_date: date,
        next_trade_date: date,
        portfolio_id: int,
        close_price_fn: Callable[[str, date], Optional[float]],
    ) -> List[Dict[str, Any]]:
        """生成买卖信号列表."""

        # ── 读取策略参数 ─────────────────────────────────────────────
        topk = int(config.get("max_positions") or 20)
        turnover_pct = float(config.get("max_turnover_pct") or 0.15)
        n_drop = max(1, int(topk * turnover_pct))
        target_weight = self._calc_target_weight(config)

        # ── 全量 score_map（覆盖所有持仓，避免 score=None 的问题）───
        score_map: Dict[str, float] = {
            it["symbol"]: float(it.get("score") or 0)
            for it in score_items
        }

        # ── TopK 候选集（评分最高的 topk 只）────────────────────────
        topk_list = [it["symbol"] for it in score_items[:topk]]
        topk_symbols = set(topk_list)

        current_symbols = set(current_positions.keys())
        n_current = len(current_symbols)

        logger.info(
            "TopkDropout 开始: portfolio=%s date=%s topk=%d n_drop=%d "
            "hold=%d score_items=%d",
            portfolio_id, signal_date, topk, n_drop,
            n_current, len(score_items),
        )

        # ── 决策逻辑 ─────────────────────────────────────────────────
        if n_current == 0:
            # 冷启动：直接买满 topk
            sell_list: List[str] = []
            buy_list: List[str] = topk_list[:]
            logger.info("冷启动买入: portfolio=%s count=%d", portfolio_id, len(buy_list))
        else:
            # 持仓中已跌出 topk → 强制卖出（按评分升序，最差先卖）
            out_of_topk = current_symbols - topk_symbols
            force_sell = sorted(out_of_topk, key=lambda s: score_map.get(s, 0.0))

            # 持仓中仍在 topk → 评分最差的 n_dropout 只 dropout 卖出
            in_topk = current_symbols & topk_symbols
            in_topk_ranked = sorted(in_topk, key=lambda s: score_map.get(s, 0.0))
            n_dropout = max(0, n_drop - len(force_sell))
            dropout_sell = in_topk_ranked[:n_dropout]

            sell_list = force_sell + dropout_sell

            # 买入：topk 中未持有的，按评分降序
            not_held = topk_symbols - current_symbols
            buy_ranked = sorted(
                not_held,
                key=lambda s: score_map.get(s, 0.0),
                reverse=True,
            )

            # 卖多少买多少；如果当前持仓不足 topk 则额外补齐
            n_sell = len(sell_list)
            n_fill = max(0, topk - n_current)   # 需要补齐的空缺
            n_buy = n_sell + n_fill              # 总买入数 = 换出 + 补齐
            buy_list = buy_ranked[:n_buy]

        logger.info(
            "TopkDropout 决策: portfolio=%s sell=%d buy=%d",
            portfolio_id, len(sell_list), len(buy_list),
        )

        # ── 生成 signal 列表 ─────────────────────────────────────────
        signals: List[Dict[str, Any]] = []

        for symbol in sell_list:
            pos = current_positions.get(symbol, {})
            qty = pos.get("quantity", 0)
            if qty <= 0:
                continue
            signals.append({
                "portfolio_id": portfolio_id,
                "signal_date": signal_date,
                "trade_date": next_trade_date,
                "symbol": symbol,
                "side": "SELL",
                "target_quantity": qty,
                "target_weight": 0.0,
                "score": score_map.get(symbol),
            })

        for symbol in buy_list:
            price = close_price_fn(symbol, signal_date)
            qty = self._calc_quantity(portfolio_value, target_weight, price)
            signals.append({
                "portfolio_id": portfolio_id,
                "signal_date": signal_date,
                "trade_date": next_trade_date,
                "symbol": symbol,
                "side": "BUY",
                "target_quantity": qty,
                "target_weight": round(target_weight, 6),
                "score": score_map.get(symbol),
            })

        return signals
