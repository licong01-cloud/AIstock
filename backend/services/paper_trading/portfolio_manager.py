"""模拟盘 CRUD + 状态管理."""
from __future__ import annotations

import json
import logging
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.paper_trading.portfolio")

DEFAULT_FEE_CONFIG: Dict[str, Any] = {
    "default_fees": {
        "commission_rate": 0.0003,
        "stamp_tax_rate": 0.0005,
        "transfer_fee_rate": 0.00002,
        "slippage": 0.001,
        "min_commission": 5,
    },
    "custom_fees": {},
}


class PortfolioManager:

    @staticmethod
    def create_portfolio(config: Dict[str, Any]) -> Dict[str, Any]:
        fee_config = {**DEFAULT_FEE_CONFIG}
        if "fee_config" in config:
            user_fees = config["fee_config"]
            if isinstance(user_fees, str):
                user_fees = json.loads(user_fees)
            if "default_fees" in user_fees:
                fee_config["default_fees"] = {**fee_config["default_fees"], **user_fees["default_fees"]}
            if "custom_fees" in user_fees:
                fee_config["custom_fees"] = user_fees["custom_fees"]

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO paper_trading.portfolio_config (
                        portfolio_name, signal_source, signal_source_id,
                        signal_loop_id, model_source, training_job_id,
                        initial_capital, max_positions, max_position_pct,
                        trade_freq, max_turnover_pct, fee_config,
                        benchmark, auto_run, execute_time,
                        enable_factor_attribution,
                        enable_live_ic, factor_list, model_catalog_id,
                        asset_bundle_id,
                        intraday_strategy, intraday_config, intraday_freq,
                        enable_intraday, intraday_exec_mode,
                        start_date
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb, %s, %s,
                        %s, %s::jsonb, %s, %s, %s,
                        %s
                    )
                    RETURNING id, created_at
                    """,
                    (
                        config["portfolio_name"],
                        config["signal_source"],
                        config["signal_source_id"],
                        config.get("signal_loop_id"),
                        config.get("model_source", "original"),
                        config.get("training_job_id"),
                        config.get("initial_capital", 1000000),
                        config.get("max_positions", 20),
                        config.get("max_position_pct", 0.10),
                        config.get("trade_freq", "daily"),
                        config.get("max_turnover_pct", 0.30),
                        json.dumps(fee_config),
                        config.get("benchmark", "000300.SH"),
                        config.get("auto_run", True),
                        config.get("execute_time", "17:30"),
                        config.get("enable_factor_attribution", True),
                        config.get("enable_live_ic", True),
                        json.dumps(config["factor_list"]) if config.get("factor_list") else None,
                        config.get("model_catalog_id"),
                        config.get("asset_bundle_id"),
                        config.get("intraday_strategy", "CLOSE_PRICE"),
                        json.dumps(config.get("intraday_config", {})),
                        config.get("intraday_freq", "5m"),
                        config.get("enable_intraday", False),
                        config.get("intraday_exec_mode", "replay"),
                        config.get("start_date"),
                    ),
                )
                row = cur.fetchone()
                conn.commit()

        return {"id": row[0], "created_at": row[1].isoformat(), **config, "fee_config": fee_config, "status": "created"}

    @staticmethod
    def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
        """将 datetime/date/time 等类型转为字符串，确保 JSON 可序列化。"""
        for k, v in row.items():
            if isinstance(v, time):
                row[k] = v.strftime("%H:%M")
            elif isinstance(v, date) and not isinstance(v, datetime):
                row[k] = v.isoformat()
            elif isinstance(v, datetime):
                row[k] = v.isoformat()
        return row

    @staticmethod
    def list_portfolios() -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, portfolio_name, signal_source, signal_source_id,
                           signal_loop_id, model_source, initial_capital,
                           max_positions, status, start_date, execute_time,
                           created_at, updated_at
                    FROM paper_trading.portfolio_config
                    ORDER BY created_at DESC
                    """
                )
                cols = [d[0] for d in cur.description]
                return [PortfolioManager._serialize_row(dict(zip(cols, r))) for r in cur.fetchall()]

    @staticmethod
    def get_portfolio(portfolio_id: int) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM paper_trading.portfolio_config WHERE id = %s",
                    (portfolio_id,),
                )
                if cur.description is None:
                    return None
                cols = [d[0] for d in cur.description]
                row = cur.fetchone()
                if row is None:
                    return None
                return PortfolioManager._serialize_row(dict(zip(cols, row)))

    @staticmethod
    def update_portfolio(portfolio_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        allowed = {
            "portfolio_name", "max_positions", "max_position_pct",
            "max_turnover_pct", "fee_config", "benchmark", "auto_run",
            "execute_time", "enable_factor_attribution", "enable_live_ic",
            "model_source", "training_job_id", "model_catalog_id",
            "asset_bundle_id", "factor_list",
            "intraday_strategy", "intraday_config", "intraday_freq",
            "enable_intraday", "intraday_exec_mode",
            "start_date",
        }
        sets = []
        vals = []
        for k, v in updates.items():
            if k not in allowed:
                continue
            if k in ("fee_config", "factor_list", "intraday_config"):
                sets.append(f"{k} = %s::jsonb")
                vals.append(json.dumps(v) if not isinstance(v, str) else v)
            else:
                sets.append(f"{k} = %s")
                vals.append(v)

        if not sets:
            return PortfolioManager.get_portfolio(portfolio_id)

        sets.append("updated_at = NOW()")
        vals.append(portfolio_id)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE paper_trading.portfolio_config SET {', '.join(sets)} WHERE id = %s",
                    vals,
                )
                conn.commit()

        return PortfolioManager.get_portfolio(portfolio_id)

    @staticmethod
    def delete_portfolio(portfolio_id: int) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM paper_trading.portfolio_config WHERE id = %s",
                    (portfolio_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError(f"模拟盘 {portfolio_id} 不存在")
                if row[0] not in ("created", "stopped", "caught_up"):
                    raise ValueError(f"仅 created/stopped/caught_up 状态可删除，当前: {row[0]}")
                cur.execute(
                    "DELETE FROM paper_trading.portfolio_config WHERE id = %s",
                    (portfolio_id,),
                )
                conn.commit()
        return True

    @staticmethod
    def _transition(portfolio_id: int, new_status: str, valid_from: tuple) -> Dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM paper_trading.portfolio_config WHERE id = %s",
                    (portfolio_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError(f"模拟盘 {portfolio_id} 不存在")
                if row[0] not in valid_from:
                    raise ValueError(f"当前状态 {row[0]} 不允许转为 {new_status}")

                extra = ""
                if new_status in ("running", "catching_up") and row[0] == "created":
                    extra = ", start_date = COALESCE(start_date, CURRENT_DATE)"

                cur.execute(
                    f"UPDATE paper_trading.portfolio_config SET status = %s, updated_at = NOW(){extra} WHERE id = %s",
                    (new_status, portfolio_id),
                )
                conn.commit()
        return PortfolioManager.get_portfolio(portfolio_id)

    @staticmethod
    def start_portfolio(portfolio_id: int) -> Dict[str, Any]:
        return PortfolioManager._transition(portfolio_id, "running", ("created", "paused", "stopped"))

    @staticmethod
    def pause_portfolio(portfolio_id: int) -> Dict[str, Any]:
        return PortfolioManager._transition(portfolio_id, "paused", ("running",))

    @staticmethod
    def stop_portfolio(portfolio_id: int) -> Dict[str, Any]:
        return PortfolioManager._transition(portfolio_id, "stopped", ("running", "paused"))

    @staticmethod
    def get_running_portfolios() -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM paper_trading.portfolio_config WHERE status = 'running' ORDER BY id"
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]

    @staticmethod
    def start_catchup(portfolio_id: int) -> Dict[str, Any]:
        """created/catching_up → catching_up: 开始或重试追赶模式."""
        return PortfolioManager._transition(portfolio_id, "catching_up", ("created", "catching_up"))

    @staticmethod
    def go_live(portfolio_id: int) -> Dict[str, Any]:
        """caught_up → running: 手工切换到实盘模式.

        验证两个前提条件:
        1. 已追赶到最新交易日
        2. 当前不在交易时段内
        """
        # 验证状态
        config = PortfolioManager.get_portfolio(portfolio_id)
        if config is None:
            raise ValueError(f"模拟盘 {portfolio_id} 不存在")
        if config.get("status") != "caught_up":
            raise ValueError(f"仅 caught_up 状态可切换到 running，当前: {config.get('status')}")

        # 验证当前不在交易时段内
        now = datetime.now().time()
        morning_open = time(9, 30)
        morning_close = time(11, 30)
        afternoon_open = time(13, 0)
        afternoon_close = time(15, 0)
        if (morning_open <= now <= morning_close) or (afternoon_open <= now <= afternoon_close):
            raise ValueError("交易时段内不允许切换到实盘模式，请在非交易时段操作")

        return PortfolioManager._transition(portfolio_id, "running", ("caught_up",))

    @staticmethod
    def finish_catchup(portfolio_id: int) -> Dict[str, Any]:
        """catching_up → caught_up: 追赶完成."""
        return PortfolioManager._transition(portfolio_id, "caught_up", ("catching_up",))

    @staticmethod
    def get_catchup_portfolios() -> List[Dict[str, Any]]:
        """获取所有 catching_up 状态的组合."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM paper_trading.portfolio_config WHERE status = 'catching_up' ORDER BY id"
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
