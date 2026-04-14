"""TopkDropout + 规则层风控 持仓再平衡策略.

在原有 TopkDropoutStrategy 基础上新增：
1. 止损规则：亏损 ≥ stop_loss_pct 时强制卖出
2. 换手率上限：每日卖出市值不超过 max_daily_turnover_pct（Task 2.1 实现）

策略代码: TOPK_DROPOUT_RC
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable, Dict, List, Optional

from .registry import register
from .topk_dropout import TopkDropoutStrategy

logger = logging.getLogger("aistock.rebalance.topk_dropout_rc")

# 默认风控参数
_DEFAULT_STOP_LOSS_PCT = 0.10
_DEFAULT_MAX_DAILY_TURNOVER_PCT = 0.30


@register
class TopkDropoutWithRiskControlStrategy(TopkDropoutStrategy):
    """带风控规则的 TopkDropout 策略.

    在原有 TopK Dropout 逻辑基础上新增：
    1. 止损规则：亏损 ≥ stop_loss_pct 时强制卖出
    2. 换手率上限：每日卖出市值不超过 max_daily_turnover_pct
    """

    STRATEGY_CODE = "TOPK_DROPOUT_RC"

    # ── 参数验证 ─────────────────────────────────────────────────

    @staticmethod
    def _validate_risk_params(config: Dict[str, Any]) -> Dict[str, Any]:
        """验证并返回风控参数，非法值回退到默认值."""
        # stop_loss_pct: 合法范围 (0, 1)
        raw_sl = config.get("stop_loss_pct")
        if raw_sl is None:
            stop_loss_pct = _DEFAULT_STOP_LOSS_PCT
        else:
            try:
                stop_loss_pct = float(raw_sl)
            except (TypeError, ValueError):
                logger.warning(
                    "stop_loss_pct 值无法转换为浮点数: %r，使用默认值 %s",
                    raw_sl, _DEFAULT_STOP_LOSS_PCT,
                )
                stop_loss_pct = _DEFAULT_STOP_LOSS_PCT
            else:
                if not (0 < stop_loss_pct < 1):
                    logger.warning(
                        "stop_loss_pct=%s 超出合法范围 (0, 1)，使用默认值 %s",
                        stop_loss_pct, _DEFAULT_STOP_LOSS_PCT,
                    )
                    stop_loss_pct = _DEFAULT_STOP_LOSS_PCT

        # max_daily_turnover_pct: 合法范围 (0, 1]
        raw_to = config.get("max_daily_turnover_pct")
        if raw_to is None:
            max_daily_turnover_pct = _DEFAULT_MAX_DAILY_TURNOVER_PCT
        else:
            try:
                max_daily_turnover_pct = float(raw_to)
            except (TypeError, ValueError):
                logger.warning(
                    "max_daily_turnover_pct 值无法转换为浮点数: %r，使用默认值 %s",
                    raw_to, _DEFAULT_MAX_DAILY_TURNOVER_PCT,
                )
                max_daily_turnover_pct = _DEFAULT_MAX_DAILY_TURNOVER_PCT
            else:
                if not (0 < max_daily_turnover_pct <= 1):
                    logger.warning(
                        "max_daily_turnover_pct=%s 超出合法范围 (0, 1]，使用默认值 %s",
                        max_daily_turnover_pct, _DEFAULT_MAX_DAILY_TURNOVER_PCT,
                    )
                    max_daily_turnover_pct = _DEFAULT_MAX_DAILY_TURNOVER_PCT

        return {
            "stop_loss_pct": stop_loss_pct,
            "max_daily_turnover_pct": max_daily_turnover_pct,
        }

    # ── 止损检查 ─────────────────────────────────────────────────

    def _check_stop_loss(
        self,
        current_positions: Dict[str, Dict[str, Any]],
        close_price_fn: Callable[[str, date], Optional[float]],
        signal_date: date,
        next_trade_date: date,
        portfolio_id: int,
        stop_loss_pct: float,
    ) -> List[Dict[str, Any]]:
        """检查止损规则，返回止损 SELL 信号列表.

        遍历 current_positions，用 close_price_fn 获取当前价格，
        当 price ≤ avg_cost × (1 - stop_loss_pct) 时生成 SELL 信号。
        """
        stop_loss_signals: List[Dict[str, Any]] = []

        for symbol, pos in current_positions.items():
            avg_cost = pos.get("avg_cost")
            quantity = pos.get("quantity", 0)

            if avg_cost is None or avg_cost <= 0 or quantity <= 0:
                continue

            price = close_price_fn(symbol, signal_date)
            if price is None:
                # 价格缺失时跳过止损检查
                logger.debug(
                    "止损检查跳过 %s: close_price_fn 返回 None", symbol,
                )
                continue

            threshold = avg_cost * (1 - stop_loss_pct)
            if price <= threshold:
                logger.info(
                    "止损触发: symbol=%s price=%.4f avg_cost=%.4f "
                    "threshold=%.4f stop_loss_pct=%.2f",
                    symbol, price, avg_cost, threshold, stop_loss_pct,
                )
                stop_loss_signals.append({
                    "portfolio_id": portfolio_id,
                    "signal_date": signal_date,
                    "trade_date": next_trade_date,
                    "symbol": symbol,
                    "side": "SELL",
                    "target_quantity": quantity,
                    "target_weight": 0.0,
                    "score": None,
                    "reason": "stop_loss",
                })

        return stop_loss_signals

    # ── 换手率上限截断 ───────────────────────────────────────────

    @staticmethod
    def _apply_turnover_cap(
        all_signals: List[Dict[str, Any]],
        portfolio_value: float,
        max_daily_turnover_pct: float,
        close_price_fn: Callable[[str, date], Optional[float]],
        signal_date: date,
        topk_symbols: set,
    ) -> List[Dict[str, Any]]:
        """对合并后的信号列表应用换手率上限截断.

        Parameters
        ----------
        all_signals:
            合并后的全部信号（止损 + 父类信号）
        portfolio_value:
            组合总资产
        max_daily_turnover_pct:
            每日最大换手率上限
        close_price_fn:
            callable(symbol, date) -> Optional[float]
        signal_date:
            信号日期，用于获取收盘价
        topk_symbols:
            当前 TopK 候选集（用于区分 force_sell 和 dropout_sell）

        Returns
        -------
        截断后的信号列表
        """
        if portfolio_value <= 0:
            logger.warning("portfolio_value <= 0，跳过换手率截断")
            return all_signals

        # 分离 SELL 和 BUY 信号
        sell_signals = [s for s in all_signals if s["side"] == "SELL"]
        buy_signals = [s for s in all_signals if s["side"] == "BUY"]

        if not sell_signals:
            return all_signals

        # 为每个 SELL 信号计算市值并分配优先级
        # 优先级: stop_loss (0) > force_sell (1) > dropout_sell (2)
        sell_with_info: List[Dict[str, Any]] = []
        for sig in sell_signals:
            symbol = sig["symbol"]
            price = close_price_fn(symbol, signal_date)
            if price is None or price <= 0:
                market_value = 0.0
            else:
                market_value = price * sig.get("target_quantity", 0)

            reason = sig.get("reason")
            if reason == "stop_loss":
                priority = 0
            elif symbol not in topk_symbols:
                # 不在 TopK 中 → force_sell
                priority = 1
            else:
                # 仍在 TopK 中但被 dropout → dropout_sell
                priority = 2

            sell_with_info.append({
                "signal": sig,
                "market_value": market_value,
                "priority": priority,
            })

        # 计算总卖出市值
        total_sell_mv = sum(item["market_value"] for item in sell_with_info)
        turnover_rate = total_sell_mv / portfolio_value

        if turnover_rate <= max_daily_turnover_pct:
            logger.info(
                "换手率 %.4f 未超过上限 %.4f，无需截断",
                turnover_rate, max_daily_turnover_pct,
            )
            return all_signals

        # 需要截断：从最低优先级组开始移除信号，直到换手率不超过上限
        # 同优先级内按市值降序排列（先移除市值大的，更快降低换手率）
        # 按优先级降序（低优先级先移除），同优先级内按市值降序（大市值先移除）
        sell_with_info.sort(key=lambda x: (-x["priority"], -x["market_value"]))

        max_sell_mv = portfolio_value * max_daily_turnover_pct
        truncated_count = 0

        # 从低优先级开始逐个移除，直到总市值 ≤ 上限
        while total_sell_mv > max_sell_mv and sell_with_info:
            # 移除排在最前面的（最低优先级、最大市值）
            removed = sell_with_info.pop(0)
            total_sell_mv -= removed["market_value"]
            truncated_count += 1

        # 剩余的就是保留的卖出信号
        kept_sell = [item["signal"] for item in sell_with_info]

        original_sell_count = len(sell_signals)
        kept_sell_count = len(kept_sell)
        kept_sell_mv = total_sell_mv  # total_sell_mv was decremented during removal
        actual_turnover = kept_sell_mv / portfolio_value

        logger.info(
            "换手率截断: 原始换手率=%.4f 截断后=%.4f 上限=%.4f "
            "原始卖出=%d 保留卖出=%d 截断=%d",
            turnover_rate, actual_turnover, max_daily_turnover_pct,
            original_sell_count, kept_sell_count, truncated_count,
        )

        # 相应减少买入信号数量，保持买卖平衡
        # 减少的买入数量 = 被截断的卖出数量
        sell_reduction = original_sell_count - kept_sell_count
        kept_buy_count = max(0, len(buy_signals) - sell_reduction)
        kept_buy = buy_signals[:kept_buy_count]

        if sell_reduction > 0:
            logger.info(
                "买入信号相应减少: 原始买入=%d 保留买入=%d 减少=%d",
                len(buy_signals), kept_buy_count, sell_reduction,
            )

        return kept_sell + kept_buy

    # ── 行业 HMM 热度调整 ─────────────────────────────────────────

    def _get_stock_sector_map(
        self, symbols: List[str], trade_date: date,
    ) -> Dict[str, str]:
        """查询股票→申万一级行业代码映射.

        Parameters
        ----------
        symbols:
            股票代码列表
        trade_date:
            交易日期，用于 PIT 查询

        Returns
        -------
        {symbol: sector_l1_code}，未找到映射的股票不包含在结果中
        """
        if not symbols:
            return {}

        from ..db.pg_pool import get_conn

        placeholders = ",".join(["%s"] * len(symbols))
        sql = f"""
            SELECT ts_code, l1_code
            FROM market.sw_index_member
            WHERE ts_code IN ({placeholders})
              AND in_date <= %s
              AND (out_date IS NULL OR out_date >= %s)
        """

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (*symbols, trade_date, trade_date))
                rows = cur.fetchall()

        # 一只股票可能有多条记录（行业调整），取最新的一条
        result: Dict[str, str] = {}
        for ts_code, l1_code in rows:
            if ts_code and l1_code:
                result[ts_code] = l1_code

        return result

    def _apply_sector_hmm_adjustment(
        self,
        score_items: List[Dict[str, Any]],
        signal_date: date,
        config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """根据行业 HMM 热度系数调整 score_items 中的评分.

        Parameters
        ----------
        score_items:
            原始评分列表（已按 score 降序排列）
        signal_date:
            信号日期
        config:
            策略配置字典

        Returns
        -------
        调整后的 score_items 列表（按调整后评分降序重排），
        若 HMM 未启用则返回原始列表；启用但配置/推断失败则抛出异常
        """
        # 检查开关
        if not config.get("enable_sector_hmm"):
            return score_items

        # 检查模型路径
        model_path = config.get("sector_hmm_model_path")
        if not model_path:
            raise RuntimeError(
                "enable_sector_hmm=True 但未提供 sector_hmm_model_path，"
                "请在策略配置中指定 HMM 模型路径"
            )

        if not score_items:
            return score_items

        from ..quant_models.hmm.sector_hmm import SectorHMMInference

        # 获取行业热度系数
        inference = SectorHMMInference(model_path=model_path)
        sector_coefficients = inference.get_sector_coefficients(signal_date)

        # 读取信号系数预设（按天数分级）
        signal_preset_key = config.get("hmm_signal_preset")  # "preset_A" or "preset_B"
        rebalance_days = str(config.get("rebalance_days", 1))
        preset_coeffs = None
        if signal_preset_key:
            preset_map = config.get("hmm_signal_presets", {})
            if signal_preset_key in preset_map:
                day_coeffs = preset_map[signal_preset_key].get("coefficients", {})
                preset_coeffs = day_coeffs.get(rebalance_days) or day_coeffs.get("1")
            else:
                raise RuntimeError(
                    f"hmm_signal_preset='{signal_preset_key}' 在 hmm_signal_presets 中不存在，"
                    f"可用预设: {list(preset_map.keys())}"
                )

        # 获取股票→行业映射
        symbols = [item["symbol"] for item in score_items]
        stock_sector_map = self._get_stock_sector_map(symbols, signal_date)

        # 记录热态和冷态行业
        trending_sectors = [
            code for code, coeff in sector_coefficients.items()
            if coeff > 1.0
        ]
        fading_sectors = [
            code for code, coeff in sector_coefficients.items()
            if coeff < 1.0
        ]
        if trending_sectors:
            logger.info("热态行业: %s", trending_sectors)
        if fading_sectors:
            logger.info("冷态行业: %s", fading_sectors)

        # 调整评分：score × 行业热度系数
        adjusted_items: List[Dict[str, Any]] = []
        for item in score_items:
            symbol = item["symbol"]
            sector_code = stock_sector_map.get(symbol)
            if sector_code and sector_code in sector_coefficients:
                # sector_coefficients 返回的是状态标签 → 需要映射到系数
                # get_sector_coefficients 已经返回了数值系数
                raw_coeff = sector_coefficients[sector_code]
                # 如果有预设系数，用预设覆盖默认的 1.5/0.5
                if preset_coeffs:
                    if raw_coeff > 1.0:
                        coeff = preset_coeffs.get("trending", raw_coeff)
                    elif raw_coeff < 1.0:
                        coeff = preset_coeffs.get("fading", raw_coeff)
                    else:
                        coeff = preset_coeffs.get("neutral", 1.0)
                else:
                    coeff = raw_coeff
            else:
                # 无映射或无模型的股票使用中性系数 1.0
                coeff = 1.0

            original_score = float(item.get("score") or 0)
            adjusted_score = original_score * coeff

            adjusted_item = dict(item)
            adjusted_item["score"] = adjusted_score
            adjusted_items.append(adjusted_item)

        # 按调整后评分降序重排
        adjusted_items.sort(key=lambda x: float(x.get("score") or 0), reverse=True)

        logger.info(
            "行业热度调整完成: 调整 %d 只股票评分，热态行业 %d 个，冷态行业 %d 个",
            len(adjusted_items), len(trending_sectors), len(fading_sectors),
        )

        return adjusted_items

    # ── 重写 generate_orders ─────────────────────────────────────

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
        """重写 generate_orders：先执行风控规则，再调用父类逻辑.

        流程：
        1. 验证风控参数
        2. 执行止损检查，生成止损 SELL 信号
        3. 从 score_items 和 current_positions 中移除止损股票
        4. 调用 super().generate_orders() 执行原有 TopK Dropout 逻辑
        5. 合并止损信号 + 父类信号
        6. 应用换手率上限截断
        """
        # 1. 验证风控参数
        risk_params = self._validate_risk_params(config)
        stop_loss_pct = risk_params["stop_loss_pct"]

        logger.info(
            "TopkDropoutRC 开始: portfolio=%s date=%s "
            "stop_loss_pct=%.2f max_daily_turnover_pct=%.2f",
            portfolio_id, signal_date,
            stop_loss_pct, risk_params["max_daily_turnover_pct"],
        )

        # 1.5 行业 HMM 热度调整（在止损检查之前）
        score_items = self._apply_sector_hmm_adjustment(
            score_items, signal_date, config,
        )

        # 2. 执行止损检查
        stop_loss_signals = self._check_stop_loss(
            current_positions=current_positions,
            close_price_fn=close_price_fn,
            signal_date=signal_date,
            next_trade_date=next_trade_date,
            portfolio_id=portfolio_id,
            stop_loss_pct=stop_loss_pct,
        )

        stop_loss_symbols = {s["symbol"] for s in stop_loss_signals}

        if stop_loss_symbols:
            logger.info(
                "止损股票: portfolio=%s symbols=%s",
                portfolio_id, stop_loss_symbols,
            )

        # 3. 从 score_items 和 current_positions 中移除止损股票
        filtered_score_items = [
            item for item in score_items
            if item["symbol"] not in stop_loss_symbols
        ]
        filtered_positions = {
            sym: pos for sym, pos in current_positions.items()
            if sym not in stop_loss_symbols
        }

        # 4. 调用父类 TopK Dropout 逻辑
        parent_signals = super().generate_orders(
            score_items=filtered_score_items,
            current_positions=filtered_positions,
            portfolio_value=portfolio_value,
            config=config,
            signal_date=signal_date,
            next_trade_date=next_trade_date,
            portfolio_id=portfolio_id,
            close_price_fn=close_price_fn,
        )

        # 5. 合并止损信号 + 父类信号
        all_signals = stop_loss_signals + parent_signals

        # 6. 应用换手率上限截断
        # 计算 topk 候选集（基于过滤后的 score_items，用于区分 force_sell / dropout_sell）
        topk = int(config.get("max_positions") or 20)
        topk_symbols = {
            item["symbol"] for item in filtered_score_items[:topk]
        }

        all_signals = self._apply_turnover_cap(
            all_signals=all_signals,
            portfolio_value=portfolio_value,
            max_daily_turnover_pct=risk_params["max_daily_turnover_pct"],
            close_price_fn=close_price_fn,
            signal_date=signal_date,
            topk_symbols=topk_symbols,
        )

        logger.info(
            "TopkDropoutRC 完成: portfolio=%s "
            "stop_loss=%d parent=%d final=%d",
            portfolio_id,
            len(stop_loss_signals),
            len(parent_signals),
            len(all_signals),
        )

        return all_signals
