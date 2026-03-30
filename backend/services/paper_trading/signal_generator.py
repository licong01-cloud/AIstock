"""T-1 日信号生成器 — 纯平台调度层，策略逻辑由 rebalance_strategies 提供."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.paper_trading.signal")

# 默认再平衡策略代码（当 portfolio_config 未指定时使用）
DEFAULT_REBALANCE_STRATEGY = "TOPK_DROPOUT"


def _get_next_trade_date(current_date: date) -> Optional[date]:
    """查询 market.trading_calendar 获取下一交易日."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cal_date FROM market.trading_calendar
                WHERE cal_date > %s AND is_trading = TRUE
                ORDER BY cal_date LIMIT 1
                """,
                (current_date,),
            )
            row = cur.fetchone()
    return row[0] if row else None


def _is_trading_day(d: date) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM market.trading_calendar WHERE cal_date = %s AND is_trading = TRUE",
                (d,),
            )
            return cur.fetchone() is not None


class SignalGenerator:

    @staticmethod
    def generate_signals(portfolio_id: int, current_date: date) -> List[Dict[str, Any]]:
        """
        在 T-1 日收盘后生成 T 日交易信号。

        平台职责（本函数）：
          1. 读取 portfolio_config
          2. 调用推理管线获取全量评分（不截断）
          3. 获取当前持仓、总资产
          4. 分发给再平衡策略生成 sell/buy 列表
          5. 写入 trade_signals 表

        策略职责（rebalance_strategies/）：
          - 根据评分和持仓决定买卖哪些股票及数量
        """
        # 1. 读取组合配置
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM paper_trading.portfolio_config WHERE id = %s",
                    (portfolio_id,),
                )
                cols = [d[0] for d in cur.description]
                row = cur.fetchone()
        if row is None:
            raise ValueError(f"模拟盘 {portfolio_id} 不存在")
        config = dict(zip(cols, row))

        next_trade_date = _get_next_trade_date(current_date)
        if next_trade_date is None:
            logger.warning("无法获取下一交易日，跳过信号生成: portfolio=%s date=%s", portfolio_id, current_date)
            return []

        # 2. 调用推理管线 — 全量评分（top_k 足够大，不截断）
        selection_result = SignalGenerator._run_selection(config, str(current_date))
        items = selection_result.get("items", [])
        if not items:
            logger.warning("选股结果为空: portfolio=%s date=%s", portfolio_id, current_date)
            return []

        # 3. 获取当前持仓和总资产
        current_positions = SignalGenerator._get_current_positions(portfolio_id, current_date)
        portfolio_value = SignalGenerator._get_portfolio_value(portfolio_id, current_date, config)

        # 4. 选择并调用再平衡策略
        strategy_code = (
            config.get("rebalance_strategy")
            or DEFAULT_REBALANCE_STRATEGY
        )
        from ...rebalance_strategies.registry import get_strategy
        strategy = get_strategy(strategy_code)

        signals = strategy.generate_orders(
            score_items=items,
            current_positions=current_positions,
            portfolio_value=portfolio_value,
            config=config,
            signal_date=current_date,
            next_trade_date=next_trade_date,
            portfolio_id=portfolio_id,
            close_price_fn=SignalGenerator._get_latest_close,
        )

        # 5. 写入 DB
        if signals:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    for sig in signals:
                        cur.execute(
                            """
                            INSERT INTO paper_trading.trade_signals
                                (portfolio_id, signal_date, trade_date, symbol, side,
                                 target_quantity, target_weight, score, status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                            ON CONFLICT (portfolio_id, trade_date, symbol)
                            DO UPDATE SET
                                side = EXCLUDED.side,
                                target_quantity = EXCLUDED.target_quantity,
                                target_weight = EXCLUDED.target_weight,
                                score = EXCLUDED.score,
                                status = 'pending',
                                signal_date = EXCLUDED.signal_date
                            """,
                            (
                                sig["portfolio_id"], sig["signal_date"],
                                sig["trade_date"], sig["symbol"], sig["side"],
                                sig["target_quantity"], sig["target_weight"],
                                sig["score"],
                            ),
                        )
                conn.commit()

        buy_count = sum(1 for s in signals if s["side"] == "BUY")
        sell_count = sum(1 for s in signals if s["side"] == "SELL")
        logger.info(
            "信号生成完成: portfolio=%s strategy=%s signal_date=%s trade_date=%s buy=%d sell=%d",
            portfolio_id, strategy_code, current_date, next_trade_date, buy_count, sell_count,
        )
        return signals

    @staticmethod
    def _run_selection(config: Dict[str, Any], trade_date: str) -> Dict[str, Any]:
        """调用 DB 数据服务层选股管线，返回全量评分（不截断）."""
        source = config.get("signal_source")
        source_id = config.get("signal_source_id")
        if not source or not source_id:
            raise ValueError(
                f"portfolio_config 缺少 signal_source 或 signal_source_id: "
                f"source={source!r} source_id={source_id!r}"
            )
        loop_id = config.get("signal_loop_id")
        top_k = int(config.get("top_k") or config.get("max_positions") or 50)

        from .db_selection_service import build_db_selection
        return build_db_selection(
            signal_source=source,
            signal_source_id=source_id,
            signal_loop_id=loop_id,
            trade_date=trade_date,
            top_k=top_k,
        )

    @staticmethod
    def _get_current_positions(portfolio_id: int, trade_date: date) -> Dict[str, Dict]:
        """获取最新持仓."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, quantity, avg_cost, market_value, entry_date
                    FROM paper_trading.positions
                    WHERE portfolio_id = %s AND trade_date = (
                        SELECT MAX(trade_date) FROM paper_trading.positions
                        WHERE portfolio_id = %s AND trade_date <= %s
                    )
                    """,
                    (portfolio_id, portfolio_id, trade_date),
                )
                result = {}
                for row in cur.fetchall():
                    result[row[0]] = {
                        "symbol": row[0],
                        "quantity": row[1],
                        "avg_cost": row[2],
                        "market_value": row[3],
                        "entry_date": row[4],
                    }
                return result

    @staticmethod
    def _get_portfolio_value(portfolio_id: int, trade_date: date, config: Dict) -> float:
        """获取最新组合总价值."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT total_value FROM paper_trading.daily_snapshot
                    WHERE portfolio_id = %s AND trade_date <= %s
                    ORDER BY trade_date DESC LIMIT 1
                    """,
                    (portfolio_id, trade_date),
                )
                row = cur.fetchone()
        if row and row[0]:
            return float(row[0])
        return float(config.get("initial_capital", 1000000))

    @staticmethod
    def _get_latest_close(symbol: str, trade_date: date) -> Optional[float]:
        """获取最近收盘价."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT close_li FROM market.kline_daily_raw
                    WHERE ts_code = %s AND trade_date <= %s
                    ORDER BY trade_date DESC LIMIT 1
                    """,
                    (symbol, trade_date),
                )
                row = cur.fetchone()
        return float(row[0]) / 1000.0 if row and row[0] else None
