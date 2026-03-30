"""因子贡献归因 — 按因子前缀分组计算对当日收益的贡献."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.paper_trading.attribution")

# 因子前缀分组
FACTOR_PREFIXES = [
    "sw2_", "db_", "mf_", "bb_", "cp_",
    "alpha158_", "precomp_",
]


class FactorAttribution:

    @staticmethod
    def compute_and_save(portfolio_id: int, trade_date: date) -> None:
        """
        按因子前缀分组，计算各组对当日收益的贡献占比。
        简化方法：基于该组因子的 IC 和持仓权重估算贡献。
        """
        config = FactorAttribution._get_config(portfolio_id)
        if not config.get("enable_factor_attribution"):
            return

        factor_list = config.get("factor_list")
        if not factor_list:
            return

        # 获取当日收益
        daily_return = FactorAttribution._get_daily_return(portfolio_id, trade_date)
        if daily_return is None:
            return

        # 按前缀分组因子
        groups: Dict[str, List[str]] = {}
        for f in factor_list:
            name = f if isinstance(f, str) else f.get("name", "")
            if not name:
                continue
            prefix = "other"
            for p in FACTOR_PREFIXES:
                if name.startswith(p):
                    prefix = p.rstrip("_")
                    break
            groups.setdefault(prefix, []).append(name)

        if not groups:
            return

        # 获取因子 IC（从 factor_live_track）
        ic_map = FactorAttribution._get_factor_ics(portfolio_id, trade_date)

        # 计算每组贡献
        total_abs_contribution = 0
        group_contributions: Dict[str, Dict[str, Any]] = {}
        for prefix, factors in groups.items():
            ics = [ic_map.get(f, 0) for f in factors]
            avg_ic = sum(ics) / len(ics) if ics else 0
            # 贡献 ≈ avg_ic × 因子数 (作为权重信号)
            contribution = avg_ic * len(factors)
            total_abs_contribution += abs(contribution)
            group_contributions[prefix] = {
                "factor_count": len(factors),
                "avg_ic": round(avg_ic, 6),
                "raw_contribution": contribution,
            }

        # 归一化为百分比
        with get_conn() as conn:
            with conn.cursor() as cur:
                for prefix, info in group_contributions.items():
                    pct = info["raw_contribution"] / total_abs_contribution if total_abs_contribution > 0 else 0
                    contribution_amount = round(daily_return * pct * FactorAttribution._get_total_value(portfolio_id, trade_date), 2)

                    cur.execute(
                        """
                        INSERT INTO paper_trading.factor_attribution (
                            portfolio_id, trade_date, factor_prefix,
                            factor_count, contribution_pct, contribution_amount,
                            avg_factor_ic
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (portfolio_id, trade_date, factor_prefix)
                        DO UPDATE SET
                            factor_count = EXCLUDED.factor_count,
                            contribution_pct = EXCLUDED.contribution_pct,
                            contribution_amount = EXCLUDED.contribution_amount,
                            avg_factor_ic = EXCLUDED.avg_factor_ic
                        """,
                        (
                            portfolio_id, trade_date, prefix,
                            info["factor_count"], round(pct, 6),
                            contribution_amount, info["avg_ic"],
                        ),
                    )
                conn.commit()

        logger.info("因子归因完成: portfolio=%s date=%s groups=%d", portfolio_id, trade_date, len(group_contributions))

    @staticmethod
    def _get_config(portfolio_id: int) -> Dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT enable_factor_attribution, factor_list FROM paper_trading.portfolio_config WHERE id = %s",
                    (portfolio_id,),
                )
                row = cur.fetchone()
        if row is None:
            return {}
        return {"enable_factor_attribution": row[0], "factor_list": row[1]}

    @staticmethod
    def _get_daily_return(portfolio_id: int, trade_date: date) -> float | None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT daily_return FROM paper_trading.daily_snapshot WHERE portfolio_id = %s AND trade_date = %s",
                    (portfolio_id, trade_date),
                )
                row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None

    @staticmethod
    def _get_total_value(portfolio_id: int, trade_date: date) -> float:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT total_value FROM paper_trading.daily_snapshot WHERE portfolio_id = %s AND trade_date = %s",
                    (portfolio_id, trade_date),
                )
                row = cur.fetchone()
        return float(row[0]) if row and row[0] else 0

    @staticmethod
    def _get_factor_ics(portfolio_id: int, trade_date: date) -> Dict[str, float]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT factor_name, daily_ic FROM factor_live_track
                    WHERE strategy_id = %s AND trade_date = %s
                    """,
                    (str(portfolio_id), trade_date),
                )
                return {r[0]: float(r[1]) for r in cur.fetchall() if r[1] is not None}
