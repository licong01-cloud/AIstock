"""T 日成交执行器 — 读取 pending 信号，以收盘价模拟成交，更新持仓和净值."""
from __future__ import annotations

import json
import logging
import math
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.paper_trading.executor")


def calculate_trade_cost(
    symbol: str, side: str, price: float, quantity: int, fee_config: Dict[str, Any],
) -> Dict[str, float]:
    """v4 §4.3 费用计算：个股覆盖 > 全局默认."""
    fees = {**fee_config.get("default_fees", {})}
    custom = fee_config.get("custom_fees", {})
    if symbol in custom:
        fees.update(custom[symbol])

    amount = price * quantity

    # 佣金（买卖双向，不低于最低佣金）
    commission = max(amount * fees.get("commission_rate", 0.0003), fees.get("min_commission", 5))

    # 印花税（仅卖出）
    stamp_tax = amount * fees.get("stamp_tax_rate", 0.0005) if side == "SELL" else 0

    # 过户费（沪市买卖双向）
    transfer_fee = amount * fees.get("transfer_fee_rate", 0.00002) if symbol.endswith(".SH") else 0

    # 滑点
    slippage_cost = amount * fees.get("slippage", 0.001)

    return {
        "commission": round(commission, 4),
        "stamp_tax": round(stamp_tax, 4),
        "transfer_fee": round(transfer_fee, 4),
        "slippage_cost": round(slippage_cost, 4),
        "total_cost": round(commission + stamp_tax + transfer_fee + slippage_cost, 4),
    }


class TradeExecutor:

    @staticmethod
    def execute_trades(portfolio_id: int, trade_date: date) -> Dict[str, Any]:
        """
        T 日成交处理：
        1. 读取 pending/confirmed 信号
        2. 获取收盘价
        3. 过滤不可交易
        4. 计算费用并写入 trades
        5. 更新 positions
        6. 更新 daily_snapshot
        7. 更新信号状态
        """
        # 读取组合配置
        config = TradeExecutor._get_config(portfolio_id)
        fee_config = config.get("fee_config", {})
        if isinstance(fee_config, str):
            fee_config = json.loads(fee_config)

        # 1. 读取待执行信号
        signals = TradeExecutor._get_pending_signals(portfolio_id, trade_date)
        if not signals:
            logger.info("无待执行信号: portfolio=%s date=%s", portfolio_id, trade_date)
            # 仍需更新持仓市值和净值
            TradeExecutor._update_positions_value(portfolio_id, trade_date)
            TradeExecutor._update_daily_snapshot(portfolio_id, trade_date, config)
            return {"executed": 0, "skipped": 0}

        # 2. 获取收盘价
        symbols = list({s["symbol"] for s in signals})
        close_prices = TradeExecutor._get_close_prices(symbols, trade_date)

        # 3+4. 逐信号处理
        executed_count = 0
        skipped_count = 0
        buy_amount_total = 0.0
        sell_amount_total = 0.0
        total_commission = 0.0
        total_stamp_tax = 0.0
        total_transfer_fee = 0.0
        total_slippage = 0.0

        # 获取前一日持仓
        prev_positions = TradeExecutor._get_prev_positions(portfolio_id, trade_date)
        # 获取当前现金
        cash = TradeExecutor._get_current_cash(portfolio_id, trade_date, config)

        # 先处理卖出（释放现金），再处理买入
        sell_signals = [s for s in signals if s["side"] == "SELL"]
        buy_signals = [s for s in signals if s["side"] == "BUY"]

        with get_conn() as conn:
            with conn.cursor() as cur:
                for sig in sell_signals + buy_signals:
                    symbol = sig["symbol"]
                    price = close_prices.get(symbol)

                    if price is None or price <= 0:
                        cur.execute(
                            "UPDATE paper_trading.trade_signals SET status = 'skipped', skip_reason = '无收盘价/停牌' WHERE id = %s",
                            (sig["id"],),
                        )
                        skipped_count += 1
                        continue

                    qty = sig["target_quantity"]
                    side = sig["side"]

                    # 验证卖出数量不超过持仓
                    if side == "SELL":
                        held_qty = prev_positions.get(symbol, {}).get("quantity", 0)
                        if held_qty <= 0:
                            cur.execute(
                                "UPDATE paper_trading.trade_signals SET status = 'skipped', skip_reason = '无持仓' WHERE id = %s",
                                (sig["id"],),
                            )
                            skipped_count += 1
                            continue
                        qty = min(qty, held_qty)

                    # 验证买入资金充足
                    if side == "BUY":
                        needed = price * qty * 1.005  # 预留费用
                        if needed > cash:
                            # 缩减买入量
                            max_qty = int(cash / (price * 1.005))
                            max_qty = (max_qty // 100) * 100
                            if max_qty <= 0:
                                cur.execute(
                                    "UPDATE paper_trading.trade_signals SET status = 'skipped', skip_reason = '资金不足' WHERE id = %s",
                                    (sig["id"],),
                                )
                                skipped_count += 1
                                continue
                            qty = max_qty

                    amount = price * qty
                    costs = calculate_trade_cost(symbol, side, price, qty, fee_config)

                    if side == "BUY":
                        net_amount = amount + costs["total_cost"]
                        cash -= net_amount
                        buy_amount_total += amount
                    else:
                        net_amount = amount - costs["total_cost"]
                        cash += net_amount
                        sell_amount_total += amount

                    total_commission += costs["commission"]
                    total_stamp_tax += costs["stamp_tax"]
                    total_transfer_fee += costs["transfer_fee"]
                    total_slippage += costs["slippage_cost"]

                    # 计算卖出盈亏
                    realized_pnl = None
                    holding_days = None
                    avg_cost_at_trade = None
                    if side == "SELL":
                        pos = prev_positions.get(symbol, {})
                        avg_cost_at_trade = pos.get("avg_cost")
                        entry_date = pos.get("entry_date")
                        if avg_cost_at_trade and avg_cost_at_trade > 0:
                            realized_pnl = round((price - float(avg_cost_at_trade)) * qty - costs["total_cost"], 2)
                        if entry_date:
                            holding_days = (trade_date - entry_date).days

                    # 获取名称
                    name = TradeExecutor._get_stock_name(symbol)

                    # 写入 trades (收盘价模式: exec_time = 当日 15:00)
                    exec_time = datetime.combine(trade_date, dt_time(15, 0))
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
                            portfolio_id, trade_date, symbol, name, side,
                            qty, round(price, 4), round(amount, 2),
                            costs["commission"], costs["stamp_tax"],
                            costs["transfer_fee"], costs["slippage_cost"],
                            costs["total_cost"], round(net_amount, 2),
                            realized_pnl, holding_days, avg_cost_at_trade,
                            f"{'进入' if side == 'BUY' else '退出'}TopK",
                            sig["id"], "CLOSE_PRICE", 1,
                            exec_time,
                        ),
                    )

                    # 更新信号状态
                    cur.execute(
                        "UPDATE paper_trading.trade_signals SET status = 'executed' WHERE id = %s",
                        (sig["id"],),
                    )
                    executed_count += 1

                    # 更新持仓字典（用于后续买入资金计算）
                    if side == "BUY":
                        if symbol in prev_positions:
                            old = prev_positions[symbol]
                            old_qty = old.get("quantity", 0)
                            old_cost = float(old.get("avg_cost", 0) or 0)
                            new_qty = old_qty + qty
                            new_cost = (old_cost * old_qty + price * qty) / new_qty if new_qty > 0 else price
                            prev_positions[symbol] = {**old, "quantity": new_qty, "avg_cost": new_cost}
                        else:
                            prev_positions[symbol] = {
                                "quantity": qty, "avg_cost": price,
                                "entry_date": trade_date, "symbol": symbol,
                            }
                    elif side == "SELL":
                        if symbol in prev_positions:
                            old = prev_positions[symbol]
                            remaining = old.get("quantity", 0) - qty
                            if remaining <= 0:
                                del prev_positions[symbol]
                            else:
                                prev_positions[symbol] = {**old, "quantity": remaining}

                conn.commit()

        # 5+6. 更新持仓和净值
        TradeExecutor._write_positions(portfolio_id, trade_date, prev_positions, close_prices)
        # 已执行的买卖计数（排除 skipped 的）
        executed_buy_count = sum(1 for s in buy_signals if close_prices.get(s["symbol"]))
        executed_sell_count = sum(1 for s in sell_signals if close_prices.get(s["symbol"]))
        TradeExecutor._update_daily_snapshot(
            portfolio_id, trade_date, config,
            buy_count=executed_buy_count,
            sell_count=executed_sell_count,
            buy_amount=buy_amount_total,
            sell_amount=sell_amount_total,
            total_commission=total_commission,
            total_stamp_tax=total_stamp_tax,
            total_transfer_fee=total_transfer_fee,
            total_slippage=total_slippage,
        )

        logger.info(
            "成交执行完成: portfolio=%s date=%s executed=%d skipped=%d",
            portfolio_id, trade_date, executed_count, skipped_count,
        )
        return {"executed": executed_count, "skipped": skipped_count}

    @staticmethod
    def _get_config(portfolio_id: int) -> Dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM paper_trading.portfolio_config WHERE id = %s", (portfolio_id,))
                cols = [d[0] for d in cur.description]
                row = cur.fetchone()
        if row is None:
            raise ValueError(f"模拟盘 {portfolio_id} 不存在")
        return dict(zip(cols, row))

    @staticmethod
    def _get_pending_signals(portfolio_id: int, trade_date: date) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, portfolio_id, signal_date, trade_date, symbol,
                           side, target_quantity, target_weight, score, status
                    FROM paper_trading.trade_signals
                    WHERE portfolio_id = %s AND trade_date = %s AND status IN ('pending', 'confirmed')
                    ORDER BY side DESC, score DESC
                    """,
                    (portfolio_id, trade_date),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]

    @staticmethod
    def _get_close_prices(symbols: List[str], trade_date: date) -> Dict[str, float]:
        if not symbols:
            return {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ts_code, close_li FROM market.kline_daily_raw
                    WHERE ts_code = ANY(%s) AND trade_date = %s
                    """,
                    (symbols, trade_date),
                )
                return {r[0]: float(r[1]) / 1000.0 for r in cur.fetchall() if r[1]}

    @staticmethod
    def _get_prev_positions(portfolio_id: int, trade_date: date) -> Dict[str, Dict]:
        """获取交易日前的最新持仓."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, quantity, avg_cost, entry_date, realized_pnl
                    FROM paper_trading.positions
                    WHERE portfolio_id = %s AND trade_date = (
                        SELECT MAX(trade_date) FROM paper_trading.positions
                        WHERE portfolio_id = %s AND trade_date < %s
                    )
                    """,
                    (portfolio_id, portfolio_id, trade_date),
                )
                result = {}
                for r in cur.fetchall():
                    result[r[0]] = {
                        "symbol": r[0], "quantity": r[1], "avg_cost": r[2],
                        "entry_date": r[3], "realized_pnl": r[4],
                    }
                return result

    @staticmethod
    def _get_current_cash(portfolio_id: int, trade_date: date, config: Dict) -> float:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cash FROM paper_trading.daily_snapshot
                    WHERE portfolio_id = %s AND trade_date < %s
                    ORDER BY trade_date DESC LIMIT 1
                    """,
                    (portfolio_id, trade_date),
                )
                row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
        return float(config.get("initial_capital", 1000000))

    @staticmethod
    def _get_stock_name(symbol: str) -> Optional[str]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT name FROM market.stock_basic WHERE ts_code = %s LIMIT 1",
                    (symbol,),
                )
                row = cur.fetchone()
        return row[0] if row else None

    @staticmethod
    def _write_positions(
        portfolio_id: int, trade_date: date,
        positions: Dict[str, Dict], close_prices: Dict[str, float],
    ) -> None:
        """写入当日持仓快照."""
        if not positions:
            return
        with get_conn() as conn:
            with conn.cursor() as cur:
                for symbol, pos in positions.items():
                    qty = pos.get("quantity", 0)
                    if qty <= 0:
                        continue
                    avg_cost = float(pos.get("avg_cost", 0) or 0)
                    close = close_prices.get(symbol, avg_cost)
                    mv = close * qty
                    entry = pos.get("entry_date")
                    hdays = (trade_date - entry).days if entry else None
                    unr_pnl = (close - avg_cost) * qty if avg_cost > 0 else 0
                    unr_pct = (close / avg_cost - 1) if avg_cost > 0 else 0
                    name = TradeExecutor._get_stock_name(symbol)
                    realized = float(pos.get("realized_pnl", 0) or 0)

                    cur.execute(
                        """
                        INSERT INTO paper_trading.positions (
                            portfolio_id, trade_date, symbol, symbol_name,
                            quantity, avg_cost, close_price, market_value,
                            unrealized_pnl, unrealized_pnl_pct, realized_pnl,
                            entry_date, holding_days
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (portfolio_id, trade_date, symbol)
                        DO UPDATE SET
                            quantity = EXCLUDED.quantity,
                            close_price = EXCLUDED.close_price,
                            market_value = EXCLUDED.market_value,
                            unrealized_pnl = EXCLUDED.unrealized_pnl,
                            unrealized_pnl_pct = EXCLUDED.unrealized_pnl_pct,
                            holding_days = EXCLUDED.holding_days
                        """,
                        (
                            portfolio_id, trade_date, symbol, name,
                            qty, round(avg_cost, 4), round(close, 4), round(mv, 2),
                            round(unr_pnl, 2), round(unr_pct, 6), round(realized, 2),
                            entry, hdays,
                        ),
                    )

                # 更新权重
                cur.execute(
                    """
                    UPDATE paper_trading.positions p
                    SET weight = p.market_value / NULLIF(t.total_mv, 0)
                    FROM (
                        SELECT portfolio_id, trade_date, SUM(market_value) AS total_mv
                        FROM paper_trading.positions
                        WHERE portfolio_id = %s AND trade_date = %s
                        GROUP BY portfolio_id, trade_date
                    ) t
                    WHERE p.portfolio_id = t.portfolio_id
                      AND p.trade_date = t.trade_date
                      AND p.portfolio_id = %s AND p.trade_date = %s
                    """,
                    (portfolio_id, trade_date, portfolio_id, trade_date),
                )
                conn.commit()

    @staticmethod
    def _update_positions_value(portfolio_id: int, trade_date: date) -> None:
        """无交易时，仅更新持仓市值."""
        prev = TradeExecutor._get_prev_positions(portfolio_id, trade_date)
        if not prev:
            return
        symbols = list(prev.keys())
        close_prices = TradeExecutor._get_close_prices(symbols, trade_date)
        if close_prices:
            TradeExecutor._write_positions(portfolio_id, trade_date, prev, close_prices)

    @staticmethod
    def _update_daily_snapshot(
        portfolio_id: int, trade_date: date, config: Dict[str, Any],
        buy_count: int = 0, sell_count: int = 0,
        buy_amount: float = 0, sell_amount: float = 0,
        total_commission: float = 0, total_stamp_tax: float = 0,
        total_transfer_fee: float = 0, total_slippage: float = 0,
    ) -> None:
        """计算并写入每日快照."""
        # 当日股票市值
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(SUM(market_value), 0), COUNT(*)
                    FROM paper_trading.positions
                    WHERE portfolio_id = %s AND trade_date = %s
                    """,
                    (portfolio_id, trade_date),
                )
                stock_value, position_count = cur.fetchone()
                stock_value = float(stock_value)

        # 现金
        cash = TradeExecutor._get_current_cash(portfolio_id, trade_date, config)
        # 调整现金：如果当日有交易，现金已在 execute_trades 中计算
        # 但这里需要从 trades 表重算
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(SUM(CASE WHEN side='SELL' THEN net_amount ELSE -net_amount END), 0)
                    FROM paper_trading.trades
                    WHERE portfolio_id = %s AND trade_date = %s
                    """,
                    (portfolio_id, trade_date),
                )
                cash_delta = float(cur.fetchone()[0])
        cash = cash + cash_delta

        total_value = cash + stock_value

        # 前一日快照
        prev_snapshot = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT total_value, cumulative_return, max_drawdown
                    FROM paper_trading.daily_snapshot
                    WHERE portfolio_id = %s AND trade_date < %s
                    ORDER BY trade_date DESC LIMIT 1
                    """,
                    (portfolio_id, trade_date),
                )
                row = cur.fetchone()
                if row:
                    prev_snapshot = {"total_value": float(row[0]), "cumulative_return": float(row[1] or 0), "max_drawdown": float(row[2] or 0)}

        initial_capital = float(config.get("initial_capital", 1000000))
        if prev_snapshot:
            prev_value = prev_snapshot["total_value"]
            daily_pnl = total_value - prev_value
            daily_return = daily_pnl / prev_value if prev_value > 0 else 0
            cumulative_return = total_value / initial_capital - 1
        else:
            daily_pnl = total_value - initial_capital
            daily_return = daily_pnl / initial_capital if initial_capital > 0 else 0
            cumulative_return = daily_return

        # 最大回撤
        peak = initial_capital * (1 + cumulative_return)
        # 获取历史最高
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(total_value) FROM paper_trading.daily_snapshot WHERE portfolio_id = %s",
                    (portfolio_id,),
                )
                row = cur.fetchone()
                hist_peak = float(row[0]) if row and row[0] else initial_capital
        peak = max(hist_peak, total_value)
        current_drawdown = (total_value / peak - 1) if peak > 0 else 0
        max_drawdown = min(current_drawdown, prev_snapshot["max_drawdown"] if prev_snapshot else 0)

        # 基准收益率
        benchmark = config.get("benchmark", "000300.SH")
        bench_return, bench_cum = TradeExecutor._get_benchmark_return(benchmark, trade_date, portfolio_id)

        # 换手率
        turnover = (buy_amount + sell_amount) / total_value if total_value > 0 else 0

        # ── 6 个新字段 ──

        # excess_return = daily_return - benchmark_return
        excess_return = round(daily_return - (bench_return or 0), 6)

        # win_rate_trade: 累计交易胜率
        win_rate_trade = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE realized_pnl > 0) AS win_count,
                        COUNT(*) FILTER (WHERE side = 'SELL') AS sell_count
                    FROM paper_trading.trades
                    WHERE portfolio_id = %s AND trade_date <= %s
                    """,
                    (portfolio_id, trade_date),
                )
                row = cur.fetchone()
                if row and row[1] and row[1] > 0:
                    win_rate_trade = round(row[0] / row[1], 6)

        # long_exposure = stock_value / total_value
        long_exposure = round(stock_value / total_value, 6) if total_value > 0 else 0

        # cum_cost: 累计总费用
        cum_cost = 0.0
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(SUM(total_cost), 0) FROM paper_trading.trades WHERE portfolio_id = %s AND trade_date <= %s",
                    (portfolio_id, trade_date),
                )
                row = cur.fetchone()
                cum_cost = round(float(row[0]), 4) if row else 0

        # sharpe_rolling_20 和 volatility_rolling_20
        sharpe_rolling_20 = None
        volatility_rolling_20 = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT daily_return FROM paper_trading.daily_snapshot
                    WHERE portfolio_id = %s AND trade_date < %s
                    ORDER BY trade_date DESC LIMIT 19
                    """,
                    (portfolio_id, trade_date),
                )
                hist_rets = [float(r[0]) for r in cur.fetchall() if r[0] is not None]

        # 加入当日收益
        all_rets = hist_rets + [daily_return]
        if len(all_rets) >= 5:
            mean_ret = sum(all_rets) / len(all_rets)
            variance = sum((r - mean_ret) ** 2 for r in all_rets) / (len(all_rets) - 1)
            std_ret = math.sqrt(variance) if variance > 0 else 0
            volatility_rolling_20 = round(std_ret * math.sqrt(252), 6)
            rf_daily = 0.03 / 252
            if std_ret > 0:
                sharpe_rolling_20 = round((mean_ret - rf_daily) / std_ret * math.sqrt(252), 4)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_trading.daily_snapshot (
                        portfolio_id, trade_date, total_value, cash, stock_value,
                        daily_pnl, daily_return, cumulative_return, max_drawdown,
                        current_drawdown, benchmark_return, benchmark_cumulative,
                        position_count, turnover,
                        buy_count, sell_count, buy_amount, sell_amount,
                        total_commission, total_stamp_tax, total_transfer_fee, total_slippage,
                        excess_return, win_rate_trade, sharpe_rolling_20,
                        volatility_rolling_20, long_exposure, cum_cost
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (portfolio_id, trade_date)
                    DO UPDATE SET
                        total_value = EXCLUDED.total_value,
                        cash = EXCLUDED.cash,
                        stock_value = EXCLUDED.stock_value,
                        daily_pnl = EXCLUDED.daily_pnl,
                        daily_return = EXCLUDED.daily_return,
                        cumulative_return = EXCLUDED.cumulative_return,
                        max_drawdown = EXCLUDED.max_drawdown,
                        current_drawdown = EXCLUDED.current_drawdown,
                        benchmark_return = EXCLUDED.benchmark_return,
                        benchmark_cumulative = EXCLUDED.benchmark_cumulative,
                        position_count = EXCLUDED.position_count,
                        turnover = EXCLUDED.turnover,
                        buy_count = EXCLUDED.buy_count,
                        sell_count = EXCLUDED.sell_count,
                        buy_amount = EXCLUDED.buy_amount,
                        sell_amount = EXCLUDED.sell_amount,
                        total_commission = EXCLUDED.total_commission,
                        total_stamp_tax = EXCLUDED.total_stamp_tax,
                        total_transfer_fee = EXCLUDED.total_transfer_fee,
                        total_slippage = EXCLUDED.total_slippage,
                        excess_return = EXCLUDED.excess_return,
                        win_rate_trade = EXCLUDED.win_rate_trade,
                        sharpe_rolling_20 = EXCLUDED.sharpe_rolling_20,
                        volatility_rolling_20 = EXCLUDED.volatility_rolling_20,
                        long_exposure = EXCLUDED.long_exposure,
                        cum_cost = EXCLUDED.cum_cost
                    """,
                    (
                        portfolio_id, trade_date, round(total_value, 2),
                        round(cash, 2), round(stock_value, 2),
                        round(daily_pnl, 2), round(daily_return, 6),
                        round(cumulative_return, 6), round(max_drawdown, 6),
                        round(current_drawdown, 6), bench_return, bench_cum,
                        position_count, round(turnover, 6),
                        buy_count, sell_count, round(buy_amount, 2),
                        round(sell_amount, 2), round(total_commission, 4),
                        round(total_stamp_tax, 4), round(total_transfer_fee, 4),
                        round(total_slippage, 4),
                        excess_return, win_rate_trade, sharpe_rolling_20,
                        volatility_rolling_20, long_exposure, cum_cost,
                    ),
                )
                conn.commit()

    @staticmethod
    def _get_benchmark_return(benchmark: str, trade_date: date, portfolio_id: int) -> Tuple[Optional[float], Optional[float]]:
        """获取基准当日收益率和累计收益率."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT close_li FROM market.kline_daily_raw
                    WHERE ts_code = %s AND trade_date <= %s
                    ORDER BY trade_date DESC LIMIT 2
                    """,
                    (benchmark, trade_date),
                )
                rows = cur.fetchall()
        if len(rows) < 1:
            return None, None
        today_close = float(rows[0][0]) / 1000.0
        daily_ret = None
        if len(rows) >= 2:
            prev_close = float(rows[1][0]) / 1000.0
            daily_ret = round((today_close / prev_close - 1), 6) if prev_close > 0 else None

        # 累计：从组合 start_date 起算
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT start_date FROM paper_trading.portfolio_config WHERE id = %s",
                    (portfolio_id,),
                )
                row = cur.fetchone()
                start_date = row[0] if row else None

        if start_date:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT close_li FROM market.kline_daily_raw
                        WHERE ts_code = %s AND trade_date <= %s
                        ORDER BY trade_date DESC LIMIT 1
                        """,
                        (benchmark, start_date),
                    )
                    row = cur.fetchone()
            if row:
                base = float(row[0]) / 1000.0
                bench_cum = round((today_close / base - 1), 6) if base > 0 else None
            else:
                bench_cum = None
        else:
            bench_cum = None

        return daily_ret, bench_cum
