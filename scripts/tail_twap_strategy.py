"""
尾盘TWAP + 涨跌停全量执行策略 (TailTWAPWithLimitStrategy)

替代 Qlib 内置 TWAPStrategy 作为 NestedExecutor 的 inner_strategy。

规则:
  P0 (任意时刻, 最高优先级):
    - 买入股票 + 当前价格 <= 跌停价 → 全量买入 (跌停抄底)
    - 卖出股票 + 当前价格 >= 涨停价 → 全量卖出 (涨停高卖)
  P3 (14:30~15:00 尾盘):
    - 剩余订单均匀 TWAP 执行
  P4 (14:55 触发一次):
    - 检测被涨停卡住的买入订单 (成交率 < 20%)
    - 将闲置资金按比例加仓到正常成交的买入股票

A股交易时间: 09:30-11:30 (120min) + 13:00-15:00 (120min) = 240min/天
尾盘 14:30-15:00 = 第210-239分钟 (0-indexed)
再分配触发 14:55 = 第235分钟
"""

import numpy as np
from qlib.backtest.decision import Order, OrderDir, TradeDecisionWO
from qlib.contrib.strategy.rule_strategy import TWAPStrategy
from qlib.backtest.utils import get_start_end_idx


# 14:30 = 第210个交易分钟 (0-indexed)
TAIL_START_OFFSET = 210
# 14:55 = 第235个交易分钟, 触发闲置资金再分配
REALLOC_OFFSET = 235
# 买入订单成交率低于此值视为被卡住
BLOCKED_FILL_THRESHOLD = 0.2


class TailTWAPWithLimitStrategy(TWAPStrategy):
    """尾盘TWAP + P0涨跌停全量执行 + P4闲置资金再分配。

    继承 TWAPStrategy 以复用 reset() 中的 trade_amount_remain 初始化。
    完全重写 generate_trade_decision() 实现自定义执行逻辑。
    """

    def reset(self, outer_trade_decision=None, **kwargs):
        super().reset(outer_trade_decision=outer_trade_decision, **kwargs)
        if outer_trade_decision is not None:
            self._p0_done = set()  # 已触发P0的股票，避免重复触发
            # 记录每只股票的原始下单量，用于计算成交率
            self._original_amount = {}
            for order in outer_trade_decision.get_decision():
                self._original_amount[order.stock_id] = order.amount
            # 再分配额外买入量: {stock_id: extra_shares}
            self._realloc_extra = {}
            self._realloc_done = False

    def _do_realloc(self, trade_start_time, trade_end_time):
        """14:55 触发: 检测被卡住的买入订单, 将闲置资金加仓到正常股票。"""
        blocked_cash = 0.0
        healthy_buys = []  # (stock_id, current_price)

        for order in self.outer_trade_decision.get_decision():
            if order.direction != Order.BUY:
                continue
            orig = self._original_amount.get(order.stock_id, 0)
            if orig <= 1e-5:
                continue
            remain = self.trade_amount_remain.get(order.stock_id, 0)
            fill_rate = 1.0 - remain / orig

            # 获取当前价格
            price = self.trade_exchange.get_deal_price(
                stock_id=order.stock_id,
                start_time=trade_start_time,
                end_time=trade_end_time,
                direction=OrderDir.BUY,
            )
            if price is None or np.isnan(price) or price <= 1e-8:
                continue

            if fill_rate < BLOCKED_FILL_THRESHOLD:
                # 被卡住: 剩余未成交部分的资金视为闲置
                blocked_cash += remain * price
            else:
                # 正常成交的股票, 可以加仓
                if self.trade_exchange.is_stock_tradable(
                    stock_id=order.stock_id,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                    direction=Order.BUY,
                ):
                    healthy_buys.append((order.stock_id, price))

        if blocked_cash <= 1e-5 or len(healthy_buys) == 0:
            return

        # 按比例分配闲置资金到健康股票
        cash_per_stock = blocked_cash / len(healthy_buys)
        for stock_id, price in healthy_buys:
            extra_shares = cash_per_stock / price
            # 交易单位取整
            _unit = self.trade_exchange.get_amount_of_trade_unit(
                stock_id=stock_id,
                start_time=trade_start_time,
                end_time=trade_end_time,
            )
            if _unit is not None and _unit > 0:
                extra_shares = np.floor(extra_shares / _unit) * _unit
            if extra_shares > 1e-5:
                self._realloc_extra[stock_id] = extra_shares

    def generate_trade_decision(self, execute_result=None):
        # 空决策
        if len(self.outer_trade_decision.get_decision()) == 0:
            return TradeDecisionWO(order_list=[], strategy=self)

        trade_step = self.trade_calendar.get_trade_step()
        start_idx, end_idx = get_start_end_idx(
            self.trade_calendar, self.outer_trade_decision
        )
        trade_len = end_idx - start_idx + 1

        if trade_step < start_idx or trade_step > end_idx:
            return TradeDecisionWO(order_list=[], strategy=self)

        rel_trade_step = trade_step - start_idx

        # 更新已执行数量
        if execute_result is not None:
            for order, _, _, _ in execute_result:
                self.trade_amount_remain[order.stock_id] -= order.deal_amount
                # 扣减再分配额外量的已执行部分
                if order.stock_id in self._realloc_extra:
                    self._realloc_extra[order.stock_id] = max(
                        0, self._realloc_extra[order.stock_id] - order.deal_amount
                    )

        trade_start_time, trade_end_time = self.trade_calendar.get_step_time(
            trade_step
        )

        # 判断是否进入尾盘 (14:30-15:00)
        is_tail = rel_trade_step >= TAIL_START_OFFSET

        # ================================================
        # P4: 14:55 触发闲置资金再分配 (仅一次)
        # ================================================
        if rel_trade_step >= REALLOC_OFFSET and not self._realloc_done:
            self._realloc_done = True
            self._do_realloc(trade_start_time, trade_end_time)

        order_list = []
        for order in self.outer_trade_decision.get_decision():
            # 停牌检测
            if self.trade_exchange.check_stock_suspended(
                stock_id=order.stock_id,
                start_time=trade_start_time,
                end_time=trade_end_time,
            ):
                continue

            amount_remain = self.trade_amount_remain[order.stock_id]
            if amount_remain <= 1e-5:
                continue

            # ================================================
            # P0: 涨跌停全量执行 (全天有效, 不限于尾盘)
            # ================================================
            if order.stock_id not in self._p0_done:
                try:
                    close_price = self.trade_exchange.get_close(
                        order.stock_id,
                        trade_start_time,
                        trade_end_time,
                        method="ts_data_last",
                    )
                    if (
                        close_price is not None
                        and not np.isnan(close_price)
                        and close_price > 0
                    ):
                        limit_up = self.trade_exchange.quote.get_data(
                            order.stock_id,
                            trade_start_time,
                            trade_end_time,
                            field="$limit_up",
                            method="ts_data_last",
                        )
                        limit_down = self.trade_exchange.quote.get_data(
                            order.stock_id,
                            trade_start_time,
                            trade_end_time,
                            field="$limit_down",
                            method="ts_data_last",
                        )

                        # 买入 + 跌停价 → 全量买入 (最低价买入)
                        if (
                            order.direction == Order.BUY
                            and limit_down is not None
                            and not np.isnan(limit_down)
                            and close_price <= limit_down
                        ):
                            order_list.append(
                                Order(
                                    stock_id=order.stock_id,
                                    amount=amount_remain,
                                    start_time=trade_start_time,
                                    end_time=trade_end_time,
                                    direction=order.direction,
                                )
                            )
                            self._p0_done.add(order.stock_id)
                            continue

                        # 卖出 + 涨停价 → 全量卖出 (最高价卖出)
                        if (
                            order.direction == Order.SELL
                            and limit_up is not None
                            and not np.isnan(limit_up)
                            and close_price >= limit_up
                        ):
                            order_list.append(
                                Order(
                                    stock_id=order.stock_id,
                                    amount=amount_remain,
                                    start_time=trade_start_time,
                                    end_time=trade_end_time,
                                    direction=order.direction,
                                )
                            )
                            self._p0_done.add(order.stock_id)
                            continue
                except Exception:
                    pass  # quote 数据访问异常，跳过P0检测

            # ================================================
            # P3: 尾盘TWAP (14:30-15:00)
            # ================================================
            if not is_tail:
                continue  # 非尾盘且无P0触发 → 本分钟不执行

            # 计算本分钟应执行量 (原始订单部分)
            remaining_steps = end_idx - trade_step + 1
            if remaining_steps <= 0:
                continue

            amount_delta = amount_remain / remaining_steps

            # 加上再分配的额外量 (14:55后生效, 均摊到剩余分钟)
            extra = self._realloc_extra.get(order.stock_id, 0)
            if extra > 1e-5 and order.direction == Order.BUY:
                amount_delta += extra / remaining_steps

            # 交易单位取整
            _amount_trade_unit = self.trade_exchange.get_amount_of_trade_unit(
                stock_id=order.stock_id,
                start_time=order.start_time,
                end_time=order.end_time,
            )
            if _amount_trade_unit is not None:
                amount_delta_target = min(
                    np.round(amount_delta / _amount_trade_unit) * _amount_trade_unit,
                    amount_remain + self._realloc_extra.get(order.stock_id, 0),
                )
            else:
                amount_delta_target = amount_delta

            # 最后一步: 全部剩余 (包含再分配额外量)
            if rel_trade_step == trade_len - 1:
                amount_delta_target = amount_remain + self._realloc_extra.get(
                    order.stock_id, 0
                )

            if amount_delta_target > 1e-5:
                order_list.append(
                    Order(
                        stock_id=order.stock_id,
                        amount=amount_delta_target,
                        start_time=trade_start_time,
                        end_time=trade_end_time,
                        direction=order.direction,
                    )
                )

        return TradeDecisionWO(order_list=order_list, strategy=self)
