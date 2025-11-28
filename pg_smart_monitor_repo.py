import os
import json
import math
from datetime import datetime, date, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import psycopg2.extras as pg_extras

from app_pg import get_conn

load_dotenv(override=True)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


def _sanitize_numbers(obj: Any) -> Any:
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, list):
        return [_sanitize_numbers(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _sanitize_numbers(v) for k, v in obj.items()}
    return obj


def _to_json(val: Any) -> Any:
    if val is None or val == "":
        return None
    if isinstance(val, (dict, list)):
        return _sanitize_numbers(val)
    try:
        data = json.loads(val, parse_constant=lambda _c: None)
        return _sanitize_numbers(data)
    except Exception:
        return None


class SmartMonitorDB:
    """Postgres-backed SmartMonitor repository with the same interface."""

    # ========== 监控任务 ==========

    def add_monitor_task(self, task_data: Dict) -> int:
        sql = (
            "INSERT INTO app.monitor_tasks (task_name, stock_code, stock_name, enabled, check_interval, "
            "auto_trade, position_size_pct, stop_loss_pct, take_profit_pct, qmt_account_id, notify_email, "
            "notify_webhook, has_position, position_cost, position_quantity, position_date) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        task_data.get("task_name"),
                        task_data.get("stock_code"),
                        task_data.get("stock_name"),
                        bool(task_data.get("enabled", 1)),
                        int(task_data.get("check_interval", 300)),
                        bool(task_data.get("auto_trade", 0)),
                        task_data.get("position_size_pct", 20),
                        task_data.get("stop_loss_pct", 5),
                        task_data.get("take_profit_pct", 10),
                        task_data.get("qmt_account_id"),
                        task_data.get("notify_email"),
                        task_data.get("notify_webhook"),
                        bool(task_data.get("has_position", 0)),
                        task_data.get("position_cost", 0),
                        task_data.get("position_quantity", 0),
                        _parse_date(task_data.get("position_date")),
                    ),
                )
                return int(cur.fetchone()[0])

    def get_monitor_tasks(self, enabled_only: bool = True) -> List[Dict]:
        base = "SELECT id, task_name, stock_code, stock_name, enabled, check_interval, auto_trade, position_size_pct, stop_loss_pct, take_profit_pct, qmt_account_id, notify_email, notify_webhook, has_position, position_cost, position_quantity, position_date, created_at, updated_at FROM app.monitor_tasks"
        sql = base + (" WHERE enabled = TRUE ORDER BY id DESC" if enabled_only else " ORDER BY id DESC")
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                for r in cur.fetchall():
                    out.append(
                        {
                            "id": r[0],
                            "task_name": r[1],
                            "stock_code": r[2],
                            "stock_name": r[3],
                            "enabled": bool(r[4]),
                            "check_interval": r[5],
                            "auto_trade": bool(r[6]),
                            "position_size_pct": r[7],
                            "stop_loss_pct": r[8],
                            "take_profit_pct": r[9],
                            "qmt_account_id": r[10],
                            "notify_email": r[11],
                            "notify_webhook": r[12],
                            "has_position": bool(r[13]),
                            "position_cost": r[14],
                            "position_quantity": r[15],
                            "position_date": r[16].isoformat() if r[16] else None,
                            "created_at": r[17].isoformat() if r[17] else None,
                            "updated_at": r[18].isoformat() if r[18] else None,
                        }
                    )
        return out

    def update_monitor_task(self, stock_code: str, task_data: Dict):
        allowed = {
            "task_name",
            "check_interval",
            "auto_trade",
            "position_size_pct",
            "has_position",
            "position_cost",
            "position_quantity",
            "position_date",
            "notify_email",
            "stop_loss_pct",
            "take_profit_pct",
            "qmt_account_id",
            "notify_webhook",
            "enabled",
        }
        sets = []
        vals: List[Any] = []
        for k, v in task_data.items():
            if k not in allowed:
                continue
            if k in {"enabled", "auto_trade", "has_position"}:
                v = bool(v)
            if k == "position_date":
                v = _parse_date(v)
            sets.append(f"{k} = %s")
            vals.append(v)
        if not sets:
            return
        sets.append("updated_at = now()")
        vals.append(stock_code)
        sql = f"UPDATE app.monitor_tasks SET {', '.join(sets)} WHERE stock_code = %s"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(vals))

    def delete_monitor_task(self, task_id: int):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM app.monitor_tasks WHERE id = %s", (task_id,))

    # ========== AI决策 ==========

    def save_ai_decision(self, decision_data: Dict) -> int:
        sql = (
            "INSERT INTO app.ai_decisions (stock_code, stock_name, decision_time, trading_session, action, confidence, reasoning, position_size_pct, stop_loss_pct, take_profit_pct, risk_level, key_price_levels, market_data, account_info, executed, execution_result) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        decision_data.get("stock_code"),
                        decision_data.get("stock_name"),
                        _parse_dt(decision_data.get("decision_time")) or _utcnow(),
                        decision_data.get("trading_session"),
                        decision_data.get("action"),
                        decision_data.get("confidence"),
                        decision_data.get("reasoning"),
                        decision_data.get("position_size_pct"),
                        decision_data.get("stop_loss_pct"),
                        decision_data.get("take_profit_pct"),
                        decision_data.get("risk_level"),
                        pg_extras.Json(_to_json(decision_data.get("key_price_levels")) or {}),
                        pg_extras.Json(_to_json(decision_data.get("market_data")) or {}),
                        pg_extras.Json(_to_json(decision_data.get("account_info")) or {}),
                        bool(decision_data.get("executed", False)),
                        decision_data.get("execution_result"),
                    ),
                )
                return int(cur.fetchone()[0])

    def get_ai_decisions(self, stock_code: str = None, limit: int = 100) -> List[Dict]:
        if stock_code:
            sql = (
                "SELECT id, stock_code, stock_name, decision_time, trading_session, action, confidence, reasoning, position_size_pct, stop_loss_pct, take_profit_pct, risk_level, key_price_levels, market_data, account_info, executed, execution_result, created_at "
                "FROM app.ai_decisions WHERE stock_code = %s ORDER BY decision_time DESC LIMIT %s"
            )
            params = (stock_code, limit)
        else:
            sql = (
                "SELECT id, stock_code, stock_name, decision_time, trading_session, action, confidence, reasoning, position_size_pct, stop_loss_pct, take_profit_pct, risk_level, key_price_levels, market_data, account_info, executed, execution_result, created_at "
                "FROM app.ai_decisions ORDER BY decision_time DESC LIMIT %s"
            )
            params = (limit,)
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for r in cur.fetchall():
                    def _jsonv(x):
                        if isinstance(x, dict):
                            return x
                        if isinstance(x, str):
                            try:
                                return json.loads(x)
                            except Exception:
                                return {}
                        return {}
                    out.append(
                        {
                            "id": r[0],
                            "stock_code": r[1],
                            "stock_name": r[2],
                            "decision_time": r[3].isoformat() if r[3] else None,
                            "trading_session": r[4],
                            "action": r[5],
                            "confidence": r[6],
                            "reasoning": r[7],
                            "position_size_pct": r[8],
                            "stop_loss_pct": r[9],
                            "take_profit_pct": r[10],
                            "risk_level": r[11],
                            "key_price_levels": _jsonv(r[12]),
                            "market_data": _jsonv(r[13]),
                            "account_info": _jsonv(r[14]),
                            "executed": bool(r[15]),
                            "execution_result": r[16],
                            "created_at": r[17].isoformat() if r[17] else None,
                        }
                    )
        return out

    def update_decision_execution(self, decision_id: int, executed: bool, result: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE app.ai_decisions SET executed=%s, execution_result=%s WHERE id=%s",
                    (bool(executed), result, decision_id),
                )

    # ========== 交易记录 ==========

    def save_trade_record(self, trade_data: Dict) -> int:
        sql = (
            "INSERT INTO app.trade_records (stock_code, stock_name, trade_type, quantity, price, amount, order_id, order_status, ai_decision_id, trade_time, commission, tax, profit_loss) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        trade_data.get("stock_code"),
                        trade_data.get("stock_name"),
                        trade_data.get("trade_type"),
                        trade_data.get("quantity"),
                        trade_data.get("price"),
                        trade_data.get("amount"),
                        trade_data.get("order_id"),
                        trade_data.get("order_status"),
                        trade_data.get("ai_decision_id"),
                        _parse_dt(trade_data.get("trade_time")) or _utcnow(),
                        trade_data.get("commission", 0),
                        trade_data.get("tax", 0),
                        trade_data.get("profit_loss", 0),
                    ),
                )
                return int(cur.fetchone()[0])

    def get_trade_records(self, stock_code: str = None, limit: int = 100) -> List[Dict]:
        if stock_code:
            sql = (
                "SELECT id, stock_code, stock_name, trade_type, quantity, price, amount, order_id, order_status, ai_decision_id, trade_time, commission, tax, profit_loss, created_at "
                "FROM app.trade_records WHERE stock_code = %s ORDER BY trade_time DESC LIMIT %s"
            )
            params = (stock_code, limit)
        else:
            sql = (
                "SELECT id, stock_code, stock_name, trade_type, quantity, price, amount, order_id, order_status, ai_decision_id, trade_time, commission, tax, profit_loss, created_at "
                "FROM app.trade_records ORDER BY trade_time DESC LIMIT %s"
            )
            params = (limit,)
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for r in cur.fetchall():
                    out.append(
                        {
                            "id": r[0],
                            "stock_code": r[1],
                            "stock_name": r[2],
                            "trade_type": r[3],
                            "quantity": r[4],
                            "price": r[5],
                            "amount": r[6],
                            "order_id": r[7],
                            "order_status": r[8],
                            "ai_decision_id": r[9],
                            "trade_time": r[10].isoformat() if r[10] else None,
                            "commission": r[11],
                            "tax": r[12],
                            "profit_loss": r[13],
                            "created_at": r[14].isoformat() if r[14] else None,
                        }
                    )
        return out

    # ========== 持仓监控 ==========

    def save_position(self, position_data: Dict):
        sql = (
            "INSERT INTO app.position_monitor (stock_code, stock_name, quantity, cost_price, current_price, profit_loss, profit_loss_pct, holding_days, buy_date, stop_loss_price, take_profit_price, last_check_time, status) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (stock_code) DO UPDATE SET "
            "stock_name = EXCLUDED.stock_name, quantity = EXCLUDED.quantity, cost_price = EXCLUDED.cost_price, current_price = EXCLUDED.current_price, "
            "profit_loss = EXCLUDED.profit_loss, profit_loss_pct = EXCLUDED.profit_loss_pct, holding_days = EXCLUDED.holding_days, buy_date = EXCLUDED.buy_date, "
            "stop_loss_price = EXCLUDED.stop_loss_price, take_profit_price = EXCLUDED.take_profit_price, last_check_time = EXCLUDED.last_check_time, updated_at = now()"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        position_data.get("stock_code"),
                        position_data.get("stock_name"),
                        position_data.get("quantity"),
                        position_data.get("cost_price"),
                        position_data.get("current_price"),
                        position_data.get("profit_loss"),
                        position_data.get("profit_loss_pct"),
                        position_data.get("holding_days"),
                        _parse_date(position_data.get("buy_date")),
                        position_data.get("stop_loss_price"),
                        position_data.get("take_profit_price"),
                        _utcnow(),
                        position_data.get("status", "holding"),
                    ),
                )

    def get_positions(self) -> List[Dict]:
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, stock_code, stock_name, quantity, cost_price, current_price, profit_loss, profit_loss_pct, holding_days, buy_date, stop_loss_price, take_profit_price, last_check_time, status, created_at, updated_at FROM app.position_monitor WHERE status = 'holding' ORDER BY id DESC"
                )
                for r in cur.fetchall():
                    out.append(
                        {
                            "id": r[0],
                            "stock_code": r[1],
                            "stock_name": r[2],
                            "quantity": r[3],
                            "cost_price": r[4],
                            "current_price": r[5],
                            "profit_loss": r[6],
                            "profit_loss_pct": r[7],
                            "holding_days": r[8],
                            "buy_date": r[9].isoformat() if r[9] else None,
                            "stop_loss_price": r[10],
                            "take_profit_price": r[11],
                            "last_check_time": r[12].isoformat() if r[12] else None,
                            "status": r[13],
                            "created_at": r[14].isoformat() if r[14] else None,
                            "updated_at": r[15].isoformat() if r[15] else None,
                        }
                    )
        return out

    def close_position(self, stock_code: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE app.position_monitor SET status = 'closed', updated_at = now() WHERE stock_code = %s",
                    (stock_code,),
                )

    # ========== 通知/日志 ==========

    def save_notification(self, notify_data: Dict) -> int:
        sql = (
            "INSERT INTO app.notifications (stock_code, notify_type, notify_target, subject, content, status) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        notify_data.get("stock_code"),
                        notify_data.get("notify_type"),
                        notify_data.get("notify_target"),
                        notify_data.get("subject"),
                        notify_data.get("content"),
                        notify_data.get("status", "pending"),
                    ),
                )
                return int(cur.fetchone()[0])

    def update_notification_status(self, notify_id: int, status: str, error_msg: str = None):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE app.notifications SET status = %s, error_msg = %s, sent_at = now() WHERE id = %s",
                    (status, error_msg, notify_id),
                )

    def log_system_event(self, level: str, module: str, message: str, details: str = None):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO app.system_logs (log_level, module, message, details) VALUES (%s,%s,%s,%s)",
                    (level, module, message, details),
                )

    # ========== 通知读取（兼容 notification_service） ==========

    def get_pending_notifications(self) -> List[Dict]:
        sql = (
            "SELECT n.id, n.stock_code, COALESCE(mt.stock_name, ''), n.notify_type, n.content, n.created_at "
            "FROM app.notifications n LEFT JOIN app.monitor_tasks mt ON n.stock_code = mt.stock_code "
            "WHERE n.status = 'pending' ORDER BY n.created_at"
        )
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                for r in cur.fetchall():
                    out.append(
                        {
                            "id": r[0],
                            "symbol": r[1],
                            "name": r[2],
                            "type": r[3],
                            "message": r[4],
                            "triggered_at": r[5].isoformat() if r[5] else None,
                        }
                    )
        return out

    def mark_notification_sent(self, notification_id: int):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE app.notifications SET status = 'sent', sent_at = now() WHERE id = %s",
                    (notification_id,),
                )


# Provide a global instance for modules expecting `from ... import monitor_db`
monitor_db = SmartMonitorDB()
