"""日内多步执行引擎 — 管理一个 portfolio 在一个交易日内的执行状态."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, time as dt_time
from typing import Any, Dict, List, Optional

from ...db.pg_pool import get_conn
from ...execution_algos import get_algo
from ...execution_algos.base_algo import BaseExecutionAlgo, OrderState, StepResult
from .trade_executor import TradeExecutor, calculate_trade_cost
from .minute_data_provider import MinuteDataProvider

logger = logging.getLogger("aistock.paper_trading.execution_engine")


class ExecutionEngine:
    """日内多步执行引擎.

    管理一个 portfolio 在一个交易日内所有订单的执行状态。
    每个 bar 到来时调用 step()，由算法决定本步执行数量和价格。
    """

    def __init__(self, portfolio_id: int, trade_date: date) -> None:
        self.portfolio_id = portfolio_id
        self.trade_date = trade_date
        self._config: Dict[str, Any] = {}
        self._fee_config: Dict[str, Any] = {}
        self._algo: Optional[BaseExecutionAlgo] = None
        self._orders: Dict[str, OrderState] = {}  # symbol -> OrderState
        self._signals: List[Dict[str, Any]] = []
        self._positions: Dict[str, Dict] = {}
        self._cash: float = 0
        self._initialized: bool = False
        self._step_count: int = 0
        # 累计费用统计
        self._buy_amount_total: float = 0
        self._sell_amount_total: float = 0
        self._total_commission: float = 0
        self._total_stamp_tax: float = 0
        self._total_transfer_fee: float = 0
        self._total_slippage: float = 0
        self._executed_buys: int = 0
        self._executed_sells: int = 0
        self._limit_prices: Dict[str, Dict[str, float]] = {}

    def initialize(self) -> None:
        """读取配置、加载算法、初始化订单状态."""
        self._config = TradeExecutor._get_config(self.portfolio_id)

        # 费用配置
        fc = self._config.get("fee_config", {})
        self._fee_config = json.loads(fc) if isinstance(fc, str) else fc

        # 加载执行算法
        algo_code = self._config.get("intraday_strategy", "CLOSE_PRICE")
        algo_config = self._config.get("intraday_config", {})
        if isinstance(algo_config, str):
            algo_config = json.loads(algo_config)
        self._algo = get_algo(algo_code, algo_config)

        # 读取 pending 信号
        self._signals = TradeExecutor._get_pending_signals(self.portfolio_id, self.trade_date)

        # 读取前日持仓和现金
        self._positions = TradeExecutor._get_prev_positions(self.portfolio_id, self.trade_date)
        self._cash = TradeExecutor._get_current_cash(self.portfolio_id, self.trade_date, self._config)

        # 为每个信号初始化 OrderState
        for sig in self._signals:
            symbol = sig["symbol"]
            side = sig["side"]
            qty = sig["target_quantity"]

            # 卖出验证：不超过持仓，T+1 检查
            if side == "SELL":
                held_qty = self._positions.get(symbol, {}).get("quantity", 0)
                entry_date = self._positions.get(symbol, {}).get("entry_date")
                if held_qty <= 0:
                    self._skip_signal(sig, "无持仓")
                    continue
                # T+1 限制
                if entry_date and entry_date >= self.trade_date:
                    self._skip_signal(sig, "T+1限制")
                    continue
                qty = min(qty, held_qty)

            state = self._algo.init_order(symbol, side, qty)
            # 保存 signal_id 用于后续更新
            state.child_fills.append({"signal_id": sig["id"]})
            self._orders[f"{symbol}_{side}"] = state

        self._initialized = True
        # 加载当日涨跌停价格
        all_symbols = [s["symbol"] for s in self._signals]
        if all_symbols:
            self._limit_prices = MinuteDataProvider.fetch_limit_prices(all_symbols, self.trade_date)
        logger.info(
            "ExecutionEngine 初始化完成: portfolio=%s date=%s algo=%s orders=%d",
            self.portfolio_id, self.trade_date,
            self._config.get("intraday_strategy"), len(self._orders),
        )

    def step(
        self,
        bar_data_map: Dict[str, Dict[str, Any]],
        market_context: Dict[str, Any],
        bar_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """执行一步 — 对每个未完成订单调用 algo.compute_step().

        Args:
            bar_data_map: {symbol: {open, high, low, close, volume, ...}}
            market_context: 全局上下文 {volume_profile, sigma, close_series, ...}
            bar_time: 当前 bar 的时间戳（日内回放时由调用方传入）

        Returns:
            本步成交列表
        """
        fills: List[Dict[str, Any]] = []
        self._step_count += 1

        for key, state in list(self._orders.items()):
            if self._algo.is_complete(state):
                continue

            bar = bar_data_map.get(state.symbol, {})
            if not bar:
                continue

            result = self._algo.compute_step(state, bar, market_context)
            if result is None:
                continue

            # 涨跌停检查：无法在该价格成交时跳过
            if result.price <= 0:
                continue

            # 涨跌停封板检查
            if self._limit_prices:
                limits = self._limit_prices.get(state.symbol)
                if limits:
                    if state.side == "BUY" and result.price >= limits["up_limit"]:
                        continue  # 涨停封板，无法买入
                    if state.side == "SELL" and result.price <= limits["down_limit"]:
                        continue  # 跌停封板，无法卖出

            # 买入资金验证
            if state.side == "BUY":
                needed = result.price * result.quantity * 1.005
                if needed > self._cash:
                    max_qty = int(self._cash / (result.price * 1.005))
                    max_qty = BaseExecutionAlgo._round_lot(max_qty)
                    if max_qty <= 0:
                        continue
                    result.quantity = max_qty

            # 计算费用
            costs = calculate_trade_cost(
                result.symbol, result.side, result.price, result.quantity, self._fee_config,
            )
            amount = result.price * result.quantity

            if result.side == "BUY":
                net_amount = amount + costs["total_cost"]
                self._cash -= net_amount
                self._buy_amount_total += amount
                self._executed_buys += 1
            else:
                net_amount = amount - costs["total_cost"]
                self._cash += net_amount
                self._sell_amount_total += amount
                self._executed_sells += 1

            self._total_commission += costs["commission"]
            self._total_stamp_tax += costs["stamp_tax"]
            self._total_transfer_fee += costs["transfer_fee"]
            self._total_slippage += costs["slippage_cost"]

            # 计算卖出盈亏
            realized_pnl = None
            holding_days = None
            avg_cost_at_trade = None
            if result.side == "SELL":
                pos = self._positions.get(result.symbol, {})
                avg_cost_at_trade = pos.get("avg_cost")
                entry_date = pos.get("entry_date")
                if avg_cost_at_trade and float(avg_cost_at_trade) > 0:
                    realized_pnl = round(
                        (result.price - float(avg_cost_at_trade)) * result.quantity - costs["total_cost"], 2,
                    )
                if entry_date:
                    holding_days = (self.trade_date - entry_date).days

            # 获取名称
            name = TradeExecutor._get_stock_name(result.symbol)

            # 取 signal_id
            signal_id = state.child_fills[0].get("signal_id") if state.child_fills else None

            # 确定 exec_time
            exec_time = bar_time or datetime.now()

            # 写入 trades 表
            fill_record = self._write_trade(
                result, costs, amount, net_amount, name,
                realized_pnl, holding_days, avg_cost_at_trade, signal_id,
                exec_time=exec_time,
            )
            fills.append(fill_record)

            # 记录子成交
            state.child_fills.append({
                "step": self._step_count,
                "quantity": result.quantity,
                "price": result.price,
            })

            # 更新内存持仓
            self._update_positions_in_memory(result)

        return fills

    def finalize(self) -> Dict[str, Any]:
        """交易日结束 — 更新信号状态、写入持仓、更新净值快照."""
        # 更新信号状态
        with get_conn() as conn:
            with conn.cursor() as cur:
                for key, state in self._orders.items():
                    signal_id = state.child_fills[0].get("signal_id") if state.child_fills else None
                    if signal_id is None:
                        continue
                    if state.executed_quantity > 0:
                        status = "executed"
                    else:
                        status = "skipped"
                    cur.execute(
                        "UPDATE paper_trading.trade_signals SET status = %s WHERE id = %s",
                        (status, signal_id),
                    )
                conn.commit()

        # 获取收盘价用于持仓估值
        symbols = list(self._positions.keys())
        close_prices = TradeExecutor._get_close_prices(symbols, self.trade_date) if symbols else {}

        # 写入持仓
        TradeExecutor._write_positions(
            self.portfolio_id, self.trade_date, self._positions, close_prices,
        )

        # 更新净值快照
        TradeExecutor._update_daily_snapshot(
            self.portfolio_id, self.trade_date, self._config,
            buy_count=self._executed_buys,
            sell_count=self._executed_sells,
            buy_amount=self._buy_amount_total,
            sell_amount=self._sell_amount_total,
            total_commission=self._total_commission,
            total_stamp_tax=self._total_stamp_tax,
            total_transfer_fee=self._total_transfer_fee,
            total_slippage=self._total_slippage,
        )

        logger.info(
            "ExecutionEngine finalize: portfolio=%s date=%s buys=%d sells=%d",
            self.portfolio_id, self.trade_date,
            self._executed_buys, self._executed_sells,
        )

        return {
            "portfolio_id": self.portfolio_id,
            "date": str(self.trade_date),
            "executed_buys": self._executed_buys,
            "executed_sells": self._executed_sells,
            "steps": self._step_count,
        }

    @property
    def all_complete(self) -> bool:
        """所有订单是否执行完毕."""
        if not self._orders:
            return True
        return all(self._algo.is_complete(s) for s in self._orders.values())

    def _skip_signal(self, sig: Dict[str, Any], reason: str) -> None:
        """将信号标记为 skipped."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE paper_trading.trade_signals SET status = 'skipped', skip_reason = %s WHERE id = %s",
                    (reason, sig["id"]),
                )
                conn.commit()

    def _write_trade(
        self,
        result: StepResult,
        costs: Dict[str, float],
        amount: float,
        net_amount: float,
        name: Optional[str],
        realized_pnl: Optional[float],
        holding_days: Optional[int],
        avg_cost_at_trade: Optional[Any],
        signal_id: Optional[int],
        exec_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """写入一条交易记录."""
        algo_code = self._config.get("intraday_strategy", "CLOSE_PRICE")
        if exec_time is None:
            exec_time = datetime.now()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_trading.trades (
                        portfolio_id, trade_date, symbol, symbol_name, side,
                        quantity, price, amount, commission, stamp_tax,
                        transfer_fee, slippage_cost, total_cost, net_amount,
                        realized_pnl, holding_days, avg_cost_at_trade,
                        reason, signal_id, exec_algo, exec_bars, exec_time
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        self.portfolio_id, self.trade_date, result.symbol, name, result.side,
                        result.quantity, round(result.price, 4), round(amount, 2),
                        costs["commission"], costs["stamp_tax"],
                        costs["transfer_fee"], costs["slippage_cost"],
                        costs["total_cost"], round(net_amount, 2),
                        realized_pnl, holding_days, avg_cost_at_trade,
                        result.reason, signal_id, algo_code, self._step_count,
                        exec_time,
                    ),
                )
                conn.commit()

        return {
            "symbol": result.symbol,
            "side": result.side,
            "quantity": result.quantity,
            "price": result.price,
            "exec_algo": algo_code,
            "exec_bars": self._step_count,
        }

    def _update_positions_in_memory(self, result: StepResult) -> None:
        """内存中更新持仓字典（用于后续步骤的资金和持仓计算）."""
        symbol = result.symbol
        if result.side == "BUY":
            if symbol in self._positions:
                old = self._positions[symbol]
                old_qty = old.get("quantity", 0)
                old_cost = float(old.get("avg_cost", 0) or 0)
                new_qty = old_qty + result.quantity
                new_cost = (old_cost * old_qty + result.price * result.quantity) / new_qty if new_qty > 0 else result.price
                self._positions[symbol] = {**old, "quantity": new_qty, "avg_cost": new_cost}
            else:
                self._positions[symbol] = {
                    "quantity": result.quantity, "avg_cost": result.price,
                    "entry_date": self.trade_date, "symbol": symbol,
                }
        elif result.side == "SELL":
            if symbol in self._positions:
                old = self._positions[symbol]
                remaining = old.get("quantity", 0) - result.quantity
                if remaining <= 0:
                    del self._positions[symbol]
                else:
                    self._positions[symbol] = {**old, "quantity": remaining}
