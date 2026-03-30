"""个股盈亏追踪 — 维护 stock_pnl_summary 表."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.paper_trading.stock_pnl")


class StockPnLTracker:

    @staticmethod
    def update_stock_pnl(portfolio_id: int, trade_date: date) -> None:
        """遍历当日 trades，更新 stock_pnl_summary."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 获取当日全部交易
                cur.execute(
                    """
                    SELECT symbol, symbol_name, side, quantity, realized_pnl, holding_days
                    FROM paper_trading.trades
                    WHERE portfolio_id = %s AND trade_date = %s
                    """,
                    (portfolio_id, trade_date),
                )
                trades = cur.fetchall()

                if not trades:
                    return

                for symbol, name, side, qty, realized_pnl, holding_days in trades:
                    # UPSERT stock_pnl_summary
                    cur.execute(
                        """
                        INSERT INTO paper_trading.stock_pnl_summary (
                            portfolio_id, symbol, symbol_name,
                            buy_count, sell_count,
                            total_realized_pnl,
                            win_count, loss_count,
                            first_trade_date, last_trade_date,
                            is_holding, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW())
                        ON CONFLICT (portfolio_id, symbol) DO UPDATE SET
                            symbol_name = COALESCE(EXCLUDED.symbol_name, paper_trading.stock_pnl_summary.symbol_name),
                            buy_count = paper_trading.stock_pnl_summary.buy_count + EXCLUDED.buy_count,
                            sell_count = paper_trading.stock_pnl_summary.sell_count + EXCLUDED.sell_count,
                            total_realized_pnl = paper_trading.stock_pnl_summary.total_realized_pnl + EXCLUDED.total_realized_pnl,
                            win_count = paper_trading.stock_pnl_summary.win_count + EXCLUDED.win_count,
                            loss_count = paper_trading.stock_pnl_summary.loss_count + EXCLUDED.loss_count,
                            last_trade_date = EXCLUDED.last_trade_date,
                            updated_at = NOW()
                        """,
                        (
                            portfolio_id, symbol, name,
                            1 if side == "BUY" else 0,
                            1 if side == "SELL" else 0,
                            float(realized_pnl) if realized_pnl and side == "SELL" else 0,
                            1 if side == "SELL" and realized_pnl and float(realized_pnl) > 0 else 0,
                            1 if side == "SELL" and realized_pnl and float(realized_pnl) <= 0 else 0,
                            trade_date, trade_date,
                        ),
                    )

                # 更新 is_holding 和浮动盈亏
                cur.execute(
                    """
                    UPDATE paper_trading.stock_pnl_summary s
                    SET is_holding = EXISTS(
                        SELECT 1 FROM paper_trading.positions p
                        WHERE p.portfolio_id = s.portfolio_id
                          AND p.symbol = s.symbol
                          AND p.trade_date = %s
                          AND p.quantity > 0
                    ),
                    total_unrealized_pnl = COALESCE((
                        SELECT p.unrealized_pnl FROM paper_trading.positions p
                        WHERE p.portfolio_id = s.portfolio_id
                          AND p.symbol = s.symbol
                          AND p.trade_date = %s
                    ), 0),
                    total_pnl = s.total_realized_pnl + COALESCE((
                        SELECT p.unrealized_pnl FROM paper_trading.positions p
                        WHERE p.portfolio_id = s.portfolio_id
                          AND p.symbol = s.symbol
                          AND p.trade_date = %s
                    ), 0),
                    updated_at = NOW()
                    WHERE s.portfolio_id = %s
                    """,
                    (trade_date, trade_date, trade_date, portfolio_id),
                )

                # 更新平均持仓天数
                cur.execute(
                    """
                    UPDATE paper_trading.stock_pnl_summary s
                    SET avg_holding_days = sub.avg_hd
                    FROM (
                        SELECT portfolio_id, symbol,
                               AVG(holding_days) AS avg_hd
                        FROM paper_trading.trades
                        WHERE portfolio_id = %s AND side = 'SELL' AND holding_days IS NOT NULL
                        GROUP BY portfolio_id, symbol
                    ) sub
                    WHERE s.portfolio_id = sub.portfolio_id AND s.symbol = sub.symbol
                    """,
                    (portfolio_id,),
                )
                conn.commit()

        logger.info("个股盈亏更新完成: portfolio=%s date=%s trades=%d", portfolio_id, trade_date, len(trades))

    @staticmethod
    def get_stock_pnl_list(portfolio_id: int) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM paper_trading.stock_pnl_summary
                    WHERE portfolio_id = %s
                    ORDER BY total_pnl DESC
                    """,
                    (portfolio_id,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]

    @staticmethod
    def get_stock_trade_history(portfolio_id: int, symbol: str) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM paper_trading.trades
                    WHERE portfolio_id = %s AND symbol = %s
                    ORDER BY trade_date, created_at
                    """,
                    (portfolio_id, symbol),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
