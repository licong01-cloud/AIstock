"""因子实盘 IC 计算 — 写入已有的 factor_live_track 表."""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
from scipy import stats

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.paper_trading.live_ic")


class LiveICTracker:

    @staticmethod
    def compute_and_save(portfolio_id: int, trade_date: date) -> None:
        """
        1. 获取组合使用的因子列表
        2. 获取 trade_date 的因子截面值
        3. 获取 T+1 收益率
        4. 计算 spearman rank IC
        5. 写入 factor_live_track
        """
        config = LiveICTracker._get_config(portfolio_id)
        if not config.get("enable_live_ic"):
            return

        factor_list = config.get("factor_list")
        if not factor_list:
            logger.debug("组合 %s 无因子列表，跳过 IC 计算", portfolio_id)
            return

        # 获取持仓股票
        symbols = LiveICTracker._get_position_symbols(portfolio_id, trade_date)
        if len(symbols) < 5:
            logger.debug("持仓不足 5 只，跳过 IC 计算: portfolio=%s", portfolio_id)
            return

        # 获取 T+1 收益率
        next_returns = LiveICTracker._get_next_day_returns(symbols, trade_date)
        if len(next_returns) < 5:
            return

        valid_symbols = list(next_returns.keys())

        # 对每个因子计算 IC
        for factor_info in factor_list:
            factor_name = factor_info if isinstance(factor_info, str) else factor_info.get("name", "")
            if not factor_name:
                continue

            factor_catalog_id = LiveICTracker._get_factor_catalog_id(factor_name)
            if factor_catalog_id is None:
                continue

            # 获取因子值（从最近的推理结果或持仓 score）
            factor_values = LiveICTracker._get_factor_values(
                portfolio_id, trade_date, factor_name, valid_symbols,
            )
            if len(factor_values) < 5:
                continue

            # 对齐
            common = set(factor_values.keys()) & set(next_returns.keys())
            if len(common) < 5:
                continue

            fv = np.array([factor_values[s] for s in common])
            rv = np.array([next_returns[s] for s in common])

            # Spearman rank IC
            ic, _ = stats.spearmanr(fv, rv)
            if np.isnan(ic):
                continue

            # 滚动 IC
            rolling_20d_ic, rolling_20d_icir = LiveICTracker._get_rolling_stats(
                factor_catalog_id, str(portfolio_id), trade_date, 20,
            )
            rolling_60d_ic, rolling_60d_icir = LiveICTracker._get_rolling_stats(
                factor_catalog_id, str(portfolio_id), trade_date, 60,
            )

            # 写入
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO factor_live_track (
                            factor_name, strategy_id, trade_date,
                            daily_ic, daily_rank_ic,
                            rolling_20d_ic, rolling_20d_icir,
                            rolling_60d_ic, rolling_60d_icir,
                            factor_catalog_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (factor_name, strategy_id, trade_date)
                        DO UPDATE SET
                            daily_ic = EXCLUDED.daily_ic,
                            daily_rank_ic = EXCLUDED.daily_rank_ic,
                            rolling_20d_ic = EXCLUDED.rolling_20d_ic,
                            rolling_20d_icir = EXCLUDED.rolling_20d_icir,
                            rolling_60d_ic = EXCLUDED.rolling_60d_ic,
                            rolling_60d_icir = EXCLUDED.rolling_60d_icir
                        """,
                        (
                            factor_name, str(portfolio_id), trade_date,
                            round(float(ic), 6), round(float(ic), 6),
                            rolling_20d_ic, rolling_20d_icir,
                            rolling_60d_ic, rolling_60d_icir,
                            factor_catalog_id,
                        ),
                    )
                    conn.commit()

        logger.info("因子实盘 IC 计算完成: portfolio=%s date=%s", portfolio_id, trade_date)

    # ── 内部方法 ──

    @staticmethod
    def _get_config(portfolio_id: int) -> Dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT enable_live_ic, factor_list FROM paper_trading.portfolio_config WHERE id = %s",
                    (portfolio_id,),
                )
                row = cur.fetchone()
        if row is None:
            return {}
        return {"enable_live_ic": row[0], "factor_list": row[1]}

    @staticmethod
    def _get_position_symbols(portfolio_id: int, trade_date: date) -> List[str]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol FROM paper_trading.positions
                    WHERE portfolio_id = %s AND trade_date = %s AND quantity > 0
                    """,
                    (portfolio_id, trade_date),
                )
                return [r[0] for r in cur.fetchall()]

    @staticmethod
    def _get_next_day_returns(symbols: List[str], trade_date: date) -> Dict[str, float]:
        """获取 T+1 日收益率."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH today AS (
                        SELECT ts_code, close_li FROM market.kline_daily_raw
                        WHERE ts_code = ANY(%s) AND trade_date = %s
                    ),
                    tmr AS (
                        SELECT DISTINCT ON (ts_code) ts_code, close_li
                        FROM market.kline_daily_raw
                        WHERE ts_code = ANY(%s) AND trade_date > %s
                        ORDER BY ts_code, trade_date
                    )
                    SELECT t.ts_code, (n.close_li::float / t.close_li - 1) AS ret
                    FROM today t JOIN tmr n ON t.ts_code = n.ts_code
                    WHERE t.close > 0
                    """,
                    (symbols, trade_date, symbols, trade_date),
                )
                return {r[0]: float(r[1]) for r in cur.fetchall() if r[1] is not None}

    @staticmethod
    def _get_factor_catalog_id(factor_name: str) -> Optional[int]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM aistock_factor_catalog WHERE factor_name = %s LIMIT 1",
                    (factor_name,),
                )
                row = cur.fetchone()
        return row[0] if row else None

    @staticmethod
    def _get_factor_values(
        portfolio_id: int, trade_date: date, factor_name: str, symbols: List[str],
    ) -> Dict[str, float]:
        """从持仓 score 获取因子值（简化：用模型评分作为因子代理）."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, score FROM paper_trading.positions
                    WHERE portfolio_id = %s AND trade_date = %s
                      AND symbol = ANY(%s) AND score IS NOT NULL
                    """,
                    (portfolio_id, trade_date, symbols),
                )
                return {r[0]: float(r[1]) for r in cur.fetchall()}

    @staticmethod
    def _get_rolling_stats(
        factor_catalog_id: int, strategy_id: str, trade_date: date, window: int,
    ) -> tuple:
        """获取滚动 IC 均值和 ICIR."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT daily_ic FROM factor_live_track
                    WHERE factor_catalog_id = %s AND strategy_id = %s
                      AND trade_date >= %s AND trade_date < %s
                    ORDER BY trade_date
                    """,
                    (factor_catalog_id, strategy_id, trade_date - timedelta(days=window * 2), trade_date),
                )
                ics = [float(r[0]) for r in cur.fetchall() if r[0] is not None]

        ics = ics[-window:]
        if len(ics) < 3:
            return None, None
        mean_ic = round(float(np.mean(ics)), 6)
        std_ic = float(np.std(ics, ddof=1))
        icir = round(mean_ic / std_ic, 6) if std_ic > 0 else None
        return mean_ic, icir
