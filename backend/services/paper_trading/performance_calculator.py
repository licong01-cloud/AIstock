"""绩效计算器 — 全部技术指标（v4 §4.5）."""
from __future__ import annotations

import logging
import math
from datetime import date
from typing import Any, Dict, List, Optional

import numpy as np

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.paper_trading.performance")


class PerformanceCalculator:

    @staticmethod
    def compute_portfolio_metrics(
        portfolio_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """读取 daily_snapshot 序列，计算全部指标."""
        snapshots = PerformanceCalculator._load_snapshots(portfolio_id, start_date, end_date)
        if not snapshots:
            return {"error": "无快照数据"}

        returns = np.array([s["daily_return"] or 0 for s in snapshots], dtype=float)
        values = np.array([float(s["total_value"] or 0) for s in snapshots], dtype=float)
        bench_rets = np.array([float(s.get("benchmark_return") or 0) for s in snapshots], dtype=float)
        n = len(returns)

        # 收益类
        total_pnl = float(values[-1] - values[0]) if n > 1 else 0
        total_return = float(snapshots[-1].get("cumulative_return") or 0)
        ann_factor = 252 / n if n > 0 else 1
        annualized_return = (1 + total_return) ** ann_factor - 1 if n > 0 else 0
        avg_daily = float(np.mean(returns)) if n > 0 else 0

        # 月度收益
        monthly_returns = PerformanceCalculator._compute_monthly_returns(snapshots)
        weekly_returns = PerformanceCalculator._compute_weekly_returns(snapshots)

        # 风险类
        max_dd = float(min(s.get("max_drawdown") or 0 for s in snapshots))
        current_dd = float(snapshots[-1].get("current_drawdown") or 0)
        volatility = float(np.std(returns, ddof=1) * math.sqrt(252)) if n > 1 else 0
        downside = returns[returns < 0]
        downside_vol = float(np.std(downside, ddof=1) * math.sqrt(252)) if len(downside) > 1 else 0

        # VaR / CVaR (95%)
        if n > 5:
            sorted_rets = np.sort(returns)
            idx_5 = max(int(n * 0.05), 1)
            var_95 = float(-sorted_rets[idx_5 - 1])
            cvar_95 = float(-np.mean(sorted_rets[:idx_5]))
        else:
            var_95 = None
            cvar_95 = None

        # 最大回撤区间和持续天数
        dd_info = PerformanceCalculator._compute_drawdown_details(values)

        # 风险调整
        rf = 0.03 / 252  # 无风险利率（年化 3%）
        excess = returns - rf
        sharpe = float(np.mean(excess) / np.std(returns, ddof=1) * math.sqrt(252)) if n > 1 and np.std(returns, ddof=1) > 0 else None
        sortino = float(np.mean(excess) / (np.std(downside, ddof=1)) * math.sqrt(252)) if len(downside) > 1 and np.std(downside, ddof=1) > 0 else None
        calmar = annualized_return / abs(max_dd) if max_dd != 0 else None

        # Omega ratio
        threshold = rf
        gains = returns[returns > threshold] - threshold
        losses = threshold - returns[returns <= threshold]
        omega = float(np.sum(gains) / np.sum(losses)) if np.sum(losses) > 0 else None

        # 盈亏比
        win_rets = returns[returns > 0]
        loss_rets = returns[returns < 0]
        profit_loss_ratio = float(np.mean(win_rets) / abs(np.mean(loss_rets))) if len(loss_rets) > 0 and np.mean(loss_rets) != 0 else None

        # 交易统计
        trade_stats = PerformanceCalculator._compute_trade_stats(portfolio_id, start_date, end_date)

        # 超额收益
        excess_returns = returns - bench_rets
        alpha = float(np.mean(excess_returns) * 252) if n > 0 else None
        te = float(np.std(excess_returns, ddof=1) * math.sqrt(252)) if n > 1 else None
        ir = alpha / te if te and te > 0 else None

        # Beta
        if n > 5:
            cov = np.cov(returns, bench_rets)
            beta = float(cov[0, 1] / cov[1, 1]) if cov[1, 1] > 0 else None
        else:
            beta = None

        # 超额回撤
        excess_cum = np.cumprod(1 + excess_returns)
        excess_peak = np.maximum.accumulate(excess_cum)
        excess_dd = excess_cum / excess_peak - 1
        excess_max_dd = float(np.min(excess_dd)) if len(excess_dd) > 0 else None

        # 持仓分析
        pos_counts = [s.get("position_count") or 0 for s in snapshots]
        avg_positions = float(np.mean(pos_counts)) if pos_counts else 0

        # 连续盈亏
        win_streak, loss_streak = PerformanceCalculator._compute_streaks(returns)

        # 胜率
        win_days = int(np.sum(returns > 0))
        loss_days = int(np.sum(returns < 0))
        win_rate = win_days / n if n > 0 else 0

        return {
            "period": {
                "start": str(snapshots[0]["trade_date"]),
                "end": str(snapshots[-1]["trade_date"]),
                "trading_days": n,
            },
            "returns": {
                "total_pnl": round(total_pnl, 2),
                "total_return": round(total_return, 6),
                "annualized_return": round(annualized_return, 6),
                "avg_daily_return": round(avg_daily, 6),
                "monthly_returns": monthly_returns,
                "weekly_returns": weekly_returns,
            },
            "risk": {
                "max_drawdown": round(max_dd, 6),
                "max_drawdown_start": dd_info.get("start"),
                "max_drawdown_end": dd_info.get("end"),
                "max_drawdown_duration_days": dd_info.get("duration"),
                "current_drawdown": round(current_dd, 6),
                "volatility": round(volatility, 6),
                "downside_volatility": round(downside_vol, 6),
                "var_95": round(var_95, 6) if var_95 is not None else None,
                "cvar_95": round(cvar_95, 6) if cvar_95 is not None else None,
            },
            "risk_adjusted": {
                "sharpe": round(sharpe, 4) if sharpe is not None else None,
                "sortino": round(sortino, 4) if sortino is not None else None,
                "calmar": round(calmar, 4) if calmar is not None else None,
                "omega": round(omega, 4) if omega is not None else None,
                "profit_loss_ratio": round(profit_loss_ratio, 4) if profit_loss_ratio is not None else None,
            },
            "trading": {
                "total_trades": trade_stats.get("total_trades", 0),
                "total_buys": trade_stats.get("total_buys", 0),
                "total_sells": trade_stats.get("total_sells", 0),
                "win_rate": round(win_rate, 4),
                "trade_win_rate": trade_stats.get("trade_win_rate"),
                "win_days": win_days,
                "loss_days": loss_days,
                "max_win_streak": win_streak,
                "max_loss_streak": loss_streak,
                "avg_holding_days": trade_stats.get("avg_holding_days"),
                "total_commission": trade_stats.get("total_commission"),
                "win_count": trade_stats.get("win_count", 0),
                "loss_count": trade_stats.get("loss_count", 0),
                "avg_win": trade_stats.get("avg_win"),
                "avg_loss": trade_stats.get("avg_loss"),
                "trade_profit_loss_ratio": trade_stats.get("profit_loss_ratio"),
                "expectancy": trade_stats.get("expectancy"),
                "max_single_win": trade_stats.get("max_single_win"),
                "max_single_loss": trade_stats.get("max_single_loss"),
                "total_realized_pnl": trade_stats.get("total_realized_pnl", 0),
            },
            "benchmark": {
                "alpha": round(alpha, 6) if alpha is not None else None,
                "beta": round(beta, 4) if beta is not None else None,
                "information_ratio": round(ir, 4) if ir is not None else None,
                "tracking_error": round(te, 6) if te is not None else None,
                "excess_max_drawdown": round(excess_max_dd, 6) if excess_max_dd is not None else None,
            },
            "portfolio": {
                "avg_positions": round(avg_positions, 1),
                "final_value": round(float(values[-1]), 2) if n > 0 else None,
                "final_cash": round(float(snapshots[-1].get("cash") or 0), 2) if n > 0 else None,
            },
        }

    @staticmethod
    def compute_compare_report(
        portfolio_ids: List[int],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """多组合对比报告."""
        results = {}
        for pid in portfolio_ids:
            results[str(pid)] = PerformanceCalculator.compute_portfolio_metrics(pid, start_date, end_date)
        return results

    # ── 内部方法 ──

    @staticmethod
    def _load_snapshots(
        portfolio_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        conditions = ["portfolio_id = %s"]
        params: list = [portfolio_id]
        if start_date:
            conditions.append("trade_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= %s")
            params.append(end_date)

        sql = f"""
            SELECT * FROM paper_trading.daily_snapshot
            WHERE {' AND '.join(conditions)}
            ORDER BY trade_date
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]

    @staticmethod
    def _compute_monthly_returns(snapshots: List[Dict]) -> Dict[str, float]:
        monthly = {}
        for s in snapshots:
            d = s["trade_date"]
            key = f"{d.year}-{d.month:02d}"
            if key not in monthly:
                monthly[key] = 1.0
            monthly[key] *= (1 + float(s.get("daily_return") or 0))
        return {k: round(v - 1, 6) for k, v in monthly.items()}

    @staticmethod
    def _compute_weekly_returns(snapshots: List[Dict]) -> Dict[str, float]:
        weekly = {}
        for s in snapshots:
            d = s["trade_date"]
            iso = d.isocalendar()
            key = f"{iso[0]}-W{iso[1]:02d}"
            if key not in weekly:
                weekly[key] = 1.0
            weekly[key] *= (1 + float(s.get("daily_return") or 0))
        return {k: round(v - 1, 6) for k, v in weekly.items()}

    @staticmethod
    def _compute_drawdown_details(values: np.ndarray) -> Dict[str, Any]:
        if len(values) < 2:
            return {}
        peaks = np.maximum.accumulate(values)
        dd = values / peaks - 1
        min_idx = int(np.argmin(dd))
        peak_idx = int(np.argmax(values[:min_idx + 1])) if min_idx > 0 else 0
        return {
            "start": peak_idx,
            "end": min_idx,
            "duration": min_idx - peak_idx,
        }

    @staticmethod
    def _compute_streaks(returns: np.ndarray) -> tuple:
        win_streak = 0
        loss_streak = 0
        cur_win = 0
        cur_loss = 0
        for r in returns:
            if r > 0:
                cur_win += 1
                cur_loss = 0
            elif r < 0:
                cur_loss += 1
                cur_win = 0
            else:
                cur_win = 0
                cur_loss = 0
            win_streak = max(win_streak, cur_win)
            loss_streak = max(loss_streak, cur_loss)
        return win_streak, loss_streak

    @staticmethod
    def _compute_trade_stats(
        portfolio_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        conditions = ["portfolio_id = %s"]
        params: list = [portfolio_id]
        if start_date:
            conditions.append("trade_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= %s")
            params.append(end_date)
        where = " AND ".join(conditions)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total_trades,
                        COUNT(*) FILTER (WHERE side = 'BUY') AS total_buys,
                        COUNT(*) FILTER (WHERE side = 'SELL') AS total_sells,
                        AVG(holding_days) FILTER (WHERE side = 'SELL') AS avg_holding_days,
                        SUM(total_cost) AS total_cost,
                        COUNT(*) FILTER (WHERE side = 'SELL' AND realized_pnl > 0) AS win_count,
                        COUNT(*) FILTER (WHERE side = 'SELL' AND realized_pnl <= 0) AS loss_count,
                        AVG(realized_pnl) FILTER (WHERE side = 'SELL' AND realized_pnl > 0) AS avg_win,
                        AVG(ABS(realized_pnl)) FILTER (WHERE side = 'SELL' AND realized_pnl <= 0) AS avg_loss,
                        MAX(realized_pnl) FILTER (WHERE side = 'SELL') AS max_single_win,
                        MIN(realized_pnl) FILTER (WHERE side = 'SELL') AS max_single_loss,
                        SUM(realized_pnl) FILTER (WHERE side = 'SELL') AS total_realized_pnl
                    FROM paper_trading.trades
                    WHERE {where}
                    """,
                    params,
                )
                row = cur.fetchone()
                if not row:
                    return {"total_trades": 0}

                total_trades = row[0] or 0
                total_buys = row[1] or 0
                total_sells = row[2] or 0
                avg_holding_days = round(float(row[3]), 1) if row[3] else None
                total_cost = round(float(row[4]), 2) if row[4] else 0
                win_count = row[5] or 0
                loss_count = row[6] or 0
                avg_win = float(row[7]) if row[7] else 0
                avg_loss = float(row[8]) if row[8] else 0
                max_single_win = round(float(row[9]), 2) if row[9] else None
                max_single_loss = round(float(row[10]), 2) if row[10] else None
                total_realized_pnl = round(float(row[11]), 2) if row[11] else 0

                # 派生指标
                total_closed = win_count + loss_count
                trade_win_rate = round(win_count / total_closed, 4) if total_closed > 0 else None
                profit_loss_ratio = round(avg_win / avg_loss, 4) if avg_loss > 0 else None
                expectancy = None
                if trade_win_rate is not None and avg_win and avg_loss:
                    expectancy = round(
                        trade_win_rate * avg_win - (1 - trade_win_rate) * avg_loss, 2,
                    )

                return {
                    "total_trades": total_trades,
                    "total_buys": total_buys,
                    "total_sells": total_sells,
                    "avg_holding_days": avg_holding_days,
                    "total_commission": total_cost,
                    "win_count": win_count,
                    "loss_count": loss_count,
                    "trade_win_rate": trade_win_rate,
                    "avg_win": round(avg_win, 2) if avg_win else None,
                    "avg_loss": round(avg_loss, 2) if avg_loss else None,
                    "profit_loss_ratio": profit_loss_ratio,
                    "expectancy": expectancy,
                    "max_single_win": max_single_win,
                    "max_single_loss": max_single_loss,
                    "total_realized_pnl": total_realized_pnl,
                }
