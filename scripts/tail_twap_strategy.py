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

import logging

import numpy as np
from qlib.backtest.decision import Order, OrderDir, TradeDecisionWO
from qlib.contrib.strategy.rule_strategy import TWAPStrategy
from qlib.backtest.utils import get_start_end_idx
from minute_execution_contract import (
    MarketAction,
    MinuteExecutionContractError,
    classify_market_state,
    normalize_trade_step,
    raw_price,
    select_tail_substitute_candidates,
)

try:
    from qe_suspend_filter import QESuspendFilter
except Exception:  # pragma: no cover - Qlib workspace packaging guard
    QESuspendFilter = None


# 14:30 = 第210个交易分钟 (0-indexed)
TAIL_START_OFFSET = 210
# 14:55 = 第235个交易分钟, 触发闲置资金再分配
REALLOC_OFFSET = 235
# 买入订单成交率低于此值视为被卡住
BLOCKED_FILL_THRESHOLD = 0.2

logger = logging.getLogger(__name__)


class TailTWAPWithLimitStrategy(TWAPStrategy):
    """尾盘TWAP + P0涨跌停全量执行 + P4闲置资金再分配。

    继承 TWAPStrategy 以复用 reset() 中的 trade_amount_remain 初始化。
    完全重写 generate_trade_decision() 实现自定义执行逻辑。
    """

    def __init__(self, start_time=None, end_time=None, split_count=None,
                 lookback_days=None, participation_rate=None,
                 unfilled_handler=None, unfilled_trigger_minute=None,
                 unfilled_backup_depth=None, filter_suspended_on_signal=False,
                 suspend_filter_file=None, suspend_filter_strict=True, **kwargs):
        """接受 conf.yaml 中 execution_algo_params 传入的参数。

        Qlib init_instance_by_config 会把 inner_strategy.kwargs 全部传到 __init__。
        TWAPStrategy.__init__ 不接受这些参数，必须在此拦截，否则 TypeError。

        unfilled_handler: "TAIL_BOOST" (默认, 加仓持仓股) / "TAIL_SUBSTITUTE" (替补买入)
        unfilled_trigger_minute: P4 触发分钟 (默认 235 = 14:55)
        unfilled_backup_depth: TAIL_SUBSTITUTE 备选股深度 (默认 15)
        """
        super().__init__()
        self._cfg_start_time = start_time
        self._cfg_end_time = end_time
        self._cfg_split_count = split_count
        self._cfg_lookback_days = lookback_days
        self._cfg_participation_rate = participation_rate
        # 尾盘未成交处理配置
        self._unfilled_handler = (unfilled_handler or "TAIL_BOOST").upper()
        _valid_handlers = {"TAIL_BOOST", "TAIL_SUBSTITUTE"}
        if self._unfilled_handler not in _valid_handlers:
            raise ValueError(
                f"unfilled_handler='{self._unfilled_handler}' 无效，"
                f"允许值: {', '.join(sorted(_valid_handlers))}"
            )
        self._realloc_offset = int(unfilled_trigger_minute or REALLOC_OFFSET)
        self._backup_depth = int(unfilled_backup_depth or 15)
        if filter_suspended_on_signal:
            if QESuspendFilter is None:
                raise RuntimeError("TailTWAP suspend filter requested but qe_suspend_filter is not importable")
            self._qe_suspend_filter = QESuspendFilter(
                enabled=True,
                suspend_filter_file=suspend_filter_file,
                strict=suspend_filter_strict,
                logger_obj=logger,
            )
        else:
            self._qe_suspend_filter = None

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
            self._execution_no_fill_reasons = {}

    def _quote_value(self, stock_id, trade_start_time, trade_end_time, field):
        try:
            return self.trade_exchange.quote.get_data(
                stock_id,
                trade_start_time,
                trade_end_time,
                field=field,
                method="ts_data_last",
            )
        except (KeyError, IndexError, ValueError, AttributeError) as exc:
            raise RuntimeError(
                f"minute quote field missing: stock={stock_id} field={field}"
            ) from exc

    def _is_artifact_suspended(self, stock_id, trade_time) -> bool:
        if self._qe_suspend_filter is None:
            return False
        suspended = self._qe_suspend_filter.suspended_symbols(trade_time)
        aliases = self._qe_suspend_filter._symbol_aliases(stock_id)
        return bool(aliases & suspended)

    def _raw_deal_price(self, stock_id, trade_start_time, trade_end_time, direction):
        adjusted_price = self.trade_exchange.get_deal_price(
            stock_id=stock_id,
            start_time=trade_start_time,
            end_time=trade_end_time,
            direction=direction,
        )
        factor = self._quote_value(stock_id, trade_start_time, trade_end_time, "$factor")
        try:
            return raw_price(adjusted_price, factor)
        except MinuteExecutionContractError as exc:
            raise RuntimeError(f"stock={stock_id}: {exc}") from exc

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

            if self._is_artifact_suspended(order.stock_id, trade_start_time):
                self._execution_no_fill_reasons[order.stock_id] = "suspended_by_suspend_d"
                continue
            if self.trade_exchange.check_stock_suspended(
                stock_id=order.stock_id,
                start_time=trade_start_time,
                end_time=trade_end_time,
            ):
                self._execution_no_fill_reasons[order.stock_id] = "suspended_by_exchange"
                continue
            price = self._raw_deal_price(
                order.stock_id, trade_start_time, trade_end_time, OrderDir.BUY
            )

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

    def _do_realloc_substitute(self, trade_start_time, trade_end_time):
        """TAIL_SUBSTITUTE: 把被涨停卡住的闲置资金买入备选股（topk 之外排名靠前的候选）。

        规则：
        1. 统计未成交买单数量 n_blocked（完全未成交或严重未成交）
        2. 从备选股中按 score 排名依次选取，最多选 n_blocked 只
        3. 如果备选股也涨停不可交易，跳过继续往后选，总数不超过 n_blocked
        4. 当前持仓总数 + 新增数量不超过外层策略的 topk
        5. 闲置资金平均分配给实际选中的备选股（不超过 n_blocked 只）
        """
        # 1. 统计未成交买单：n_blocked = 未成交笔数，blocked_cash = 对应资金
        blocked_cash = 0.0
        n_blocked = 0
        for order in self.outer_trade_decision.get_decision():
            if order.direction != Order.BUY:
                continue
            orig = self._original_amount.get(order.stock_id, 0)
            if orig <= 1e-5:
                continue
            remain = self.trade_amount_remain.get(order.stock_id, 0)
            fill_rate = 1.0 - remain / orig
            if fill_rate < BLOCKED_FILL_THRESHOLD:
                if self._is_artifact_suspended(order.stock_id, trade_start_time):
                    self._execution_no_fill_reasons[order.stock_id] = "suspended_by_suspend_d"
                    continue
                if self.trade_exchange.check_stock_suspended(
                    stock_id=order.stock_id,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                ):
                    self._execution_no_fill_reasons[order.stock_id] = "suspended_by_exchange"
                    continue
                price = self._raw_deal_price(
                    order.stock_id, trade_start_time, trade_end_time, OrderDir.BUY
                )
                blocked_cash += remain * price
                n_blocked += 1

        if blocked_cash <= 1e-5 or n_blocked == 0:
            return

        # 2. 从外层策略获取备选股列表和 topk 限制
        outer_strategy = self.outer_trade_decision.strategy
        backup_candidates = getattr(outer_strategy, "_backup_candidates", [])
        if not backup_candidates:
            self._execution_no_fill_reasons["__tail_substitute__"] = "tail_substitute_no_candidates"
            logger.info("[TailTWAP] TAIL_SUBSTITUTE no-fill: no backup candidates")
            return

        # 3. 计算当前持仓数量，确定还能新增多少只
        topk = getattr(outer_strategy, "topk", None)
        if topk is None or int(topk) <= 0:
            raise RuntimeError("TAIL_SUBSTITUTE requires a positive outer strategy topk")
        try:
            current_holdings = set(outer_strategy.trade_position.get_stock_list())
        except Exception as exc:
            raise RuntimeError("TAIL_SUBSTITUTE requires authoritative trade_position") from exc
        # 已在 _realloc_extra 中的备选股也算入
        already_added = set(self._realloc_extra.keys())
        # 统计当前正在执行的卖出订单数量——这些订单会释放持仓位
        n_selling = sum(
            1 for order in self.outer_trade_decision.get_decision()
            if order.direction == Order.SELL
        )
        prices = {}

        def is_tradable(sid):
            if self._is_artifact_suspended(sid, trade_start_time):
                self._execution_no_fill_reasons[sid] = "suspended_by_suspend_d"
                return False
            if not self.trade_exchange.is_stock_tradable(
                stock_id=sid,
                start_time=trade_start_time,
                end_time=trade_end_time,
                direction=Order.BUY,
            ):
                return False
            price = self._raw_deal_price(sid, trade_start_time, trade_end_time, OrderDir.BUY)
            prices[sid] = float(price)
            return True

        ranked = select_tail_substitute_candidates(
            backup_candidates,
            blocked_count=n_blocked,
            current_holdings=current_holdings,
            already_added=already_added,
            selling_count=n_selling,
            topk=int(topk),
            is_tradable=is_tradable,
        )
        selected = [(sid, prices[sid], score) for sid, score in ranked]

        if not selected:
            effective_count = len(current_holdings) + len(already_added) - n_selling
            reason = (
                "tail_substitute_topk_capacity_exhausted"
                if effective_count >= int(topk)
                else "tail_substitute_candidates_not_tradable"
            )
            self._execution_no_fill_reasons["__tail_substitute__"] = reason
            logger.info("[TailTWAP] TAIL_SUBSTITUTE no-fill: backup candidates are not tradable")
            return

        # 5. 闲置资金平均分配给选中的备选股
        cash_per_stock = blocked_cash / len(selected)
        for sid, price, _score in selected:
            extra_shares = cash_per_stock / price
            _unit = self.trade_exchange.get_amount_of_trade_unit(
                stock_id=sid,
                start_time=trade_start_time,
                end_time=trade_end_time,
            )
            if _unit is not None and _unit > 0:
                extra_shares = np.floor(extra_shares / _unit) * _unit
            if extra_shares > 1e-5:
                self._realloc_extra[sid] = extra_shares
                self._execution_no_fill_reasons[sid] = "tail_substitute_selected"

    def generate_trade_decision(self, execute_result=None):
        # 空决策
        if len(self.outer_trade_decision.get_decision()) == 0:
            return TradeDecisionWO(order_list=[], strategy=self)

        trade_step = self.trade_calendar.get_trade_step()
        start_idx, end_idx = get_start_end_idx(
            self.trade_calendar, self.outer_trade_decision
        )
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
            self._execution_no_fill_reasons["__calendar__"] = "auction_wait"
            return TradeDecisionWO(order_list=[], strategy=self)

        # 更新已执行数量
        if execute_result is not None:
            for order, _, _, _ in execute_result:
                if order.stock_id in self.trade_amount_remain:
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
        # P4: 尾盘触发闲置资金再分配 (仅一次)
        # ================================================
        if rel_trade_step >= self._realloc_offset and not self._realloc_done:
            self._realloc_done = True
            if self._unfilled_handler == "TAIL_SUBSTITUTE":
                self._do_realloc_substitute(trade_start_time, trade_end_time)
            else:
                self._do_realloc(trade_start_time, trade_end_time)

        order_list = []
        for order in self.outer_trade_decision.get_decision():
            # 停牌检测
            if self._is_artifact_suspended(order.stock_id, trade_start_time):
                self._execution_no_fill_reasons[order.stock_id] = "suspended_by_suspend_d"
                continue
            if self.trade_exchange.check_stock_suspended(
                stock_id=order.stock_id,
                start_time=trade_start_time,
                end_time=trade_end_time,
            ):
                self._execution_no_fill_reasons[order.stock_id] = "suspended_by_exchange"
                continue

            amount_remain = self.trade_amount_remain[order.stock_id]
            if amount_remain <= 1e-5:
                continue

            # ================================================
            # P0: 涨跌停全量执行 (全天有效, 不限于尾盘)
            # ================================================
            if order.stock_id not in self._p0_done:
                try:
                    close_adjusted = self.trade_exchange.get_close(
                        order.stock_id,
                        trade_start_time,
                        trade_end_time,
                        method="ts_data_last",
                    )
                    factor = self._quote_value(
                        order.stock_id, trade_start_time, trade_end_time, "$factor"
                    )
                    close_price = raw_price(close_adjusted, factor)
                    limit_up = self._quote_value(
                        order.stock_id, trade_start_time, trade_end_time, "$up_limit_price"
                    )
                    limit_down = self._quote_value(
                        order.stock_id, trade_start_time, trade_end_time, "$down_limit_price"
                    )
                    prev_close = self._quote_value(
                        order.stock_id, trade_start_time, trade_end_time, "$prev_close"
                    )
                    volume = self._quote_value(
                        order.stock_id, trade_start_time, trade_end_time, "$volume"
                    )
                    state = classify_market_state(
                        side="BUY" if order.direction == Order.BUY else "SELL",
                        price=close_price,
                        prev_close=prev_close,
                        limit_up=limit_up,
                        limit_down=limit_down,
                        volume=volume,
                    )
                    if state.action == MarketAction.NO_FILL:
                        self._execution_no_fill_reasons[order.stock_id] = state.reason
                        continue
                    if state.action == MarketAction.P0_FORCE:
                        if order.direction == Order.BUY:
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
                        if order.direction == Order.SELL:
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
                except (KeyError, IndexError, ValueError, MinuteExecutionContractError) as exc:
                    raise RuntimeError(
                        f"TailTWAP market data error stock={order.stock_id}: {exc}"
                    ) from exc

            # ================================================
            # P3: 尾盘TWAP (14:30-15:00)
            # ================================================
            if not is_tail:
                continue  # 非尾盘且无P0触发 → 本分钟不执行

            # 计算本分钟应执行量 (原始订单部分)
            remaining_steps = 240 - rel_trade_step
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
            if rel_trade_step == 239:
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

        # TAIL_SUBSTITUTE: 为备选股生成买入订单（这些股票不在 outer_trade_decision 中）
        if is_tail and self._unfilled_handler == "TAIL_SUBSTITUTE":
            # 收集已在 order_list 中的股票
            existing_sids = {o.stock_id for o in order_list}
            for sid, extra in self._realloc_extra.items():
                if extra <= 1e-5 or sid in existing_sids:
                    continue
                # 备选股不在原始订单中，需要检查可交易性
                if self._is_artifact_suspended(sid, trade_start_time):
                    self._execution_no_fill_reasons[sid] = "suspended_by_suspend_d"
                    continue
                if self.trade_exchange.check_stock_suspended(
                    stock_id=sid,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                ):
                    continue
                remaining_steps = 240 - rel_trade_step
                if remaining_steps <= 0:
                    continue
                if rel_trade_step == 239:
                    amount_delta_target = extra
                else:
                    amount_delta_target = extra / remaining_steps
                _unit = self.trade_exchange.get_amount_of_trade_unit(
                    stock_id=sid,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                )
                if _unit is not None and _unit > 0:
                    amount_delta_target = min(
                        np.round(amount_delta_target / _unit) * _unit, extra
                    )
                if amount_delta_target > 1e-5:
                    order_list.append(
                        Order(
                            stock_id=sid,
                            amount=amount_delta_target,
                            start_time=trade_start_time,
                            end_time=trade_end_time,
                            direction=Order.BUY,
                        )
                    )

        return TradeDecisionWO(order_list=order_list, strategy=self)
