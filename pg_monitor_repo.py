import json
import math
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import psycopg2.extras as pg_extras

from app_pg import get_conn

load_dotenv(override=True)


def _utcnow():
    return datetime.now(timezone.utc)


def _to_json(val: Any) -> Any:
    if val is None or val == "":
        return None
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val, parse_constant=lambda _c: None)
    except Exception:
        return None


class StockMonitorDatabase:
    """Postgres-backed monitor_db API compatible with existing code."""

    # ---- Monitored stocks ----
    def add_monitored_stock(
        self,
        symbol: str,
        name: str,
        rating: str,
        entry_range: Dict,
        take_profit: Optional[float],
        stop_loss: Optional[float],
        check_interval: int = 30,
        notification_enabled: bool = True,
        quant_enabled: bool = False,
        quant_config: Optional[Dict] = None,
    ) -> int:
        sql = (
            "INSERT INTO app.monitored_stocks (symbol, name, rating, entry_range, take_profit, stop_loss, "
            "current_price, last_checked, check_interval, notification_enabled, quant_enabled, quant_config) "
            "VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL,%s,%s,%s,%s) "
            "ON CONFLICT (symbol) DO UPDATE SET name=EXCLUDED.name, rating=EXCLUDED.rating, entry_range=EXCLUDED.entry_range, "
            "take_profit=EXCLUDED.take_profit, stop_loss=EXCLUDED.stop_loss, check_interval=EXCLUDED.check_interval, "
            "notification_enabled=EXCLUDED.notification_enabled, quant_enabled=EXCLUDED.quant_enabled, quant_config=EXCLUDED.quant_config, updated_at=now() "
            "RETURNING id"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        symbol,
                        name,
                        rating,
                        pg_extras.Json(entry_range or {}),
                        take_profit,
                        stop_loss,
                        int(check_interval),
                        bool(notification_enabled),
                        bool(quant_enabled),
                        pg_extras.Json(quant_config or {}),
                    ),
                )
                return int(cur.fetchone()[0])

    def get_monitored_stocks(self) -> List[Dict]:
        sql = (
            "SELECT id, symbol, name, rating, entry_range, take_profit, stop_loss, current_price, last_checked, "
            "check_interval, notification_enabled, quant_enabled, quant_config, created_at, updated_at "
            "FROM app.monitored_stocks ORDER BY created_at DESC"
        )
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                for r in cur.fetchall():
                    def _jsonv(x):
                        if isinstance(x, dict):
                            return x
                        if isinstance(x, str):
                            try:
                                return json.loads(x)
                            except Exception:
                                return None
                        return None
                    out.append(
                        {
                            "id": r[0],
                            "symbol": r[1],
                            "name": r[2],
                            "rating": r[3],
                            "entry_range": _jsonv(r[4]) or {},
                            "take_profit": r[5],
                            "stop_loss": r[6],
                            "current_price": r[7],
                            "last_checked": r[8].isoformat() if r[8] else None,
                            "check_interval": r[9],
                            "notification_enabled": bool(r[10]),
                            "quant_enabled": bool(r[11]),
                            "quant_config": _jsonv(r[12]) or {},
                            "created_at": r[13].isoformat() if r[13] else None,
                            "updated_at": r[14].isoformat() if r[14] else None,
                        }
                    )
        return out

    def update_stock_price(self, stock_id: int, price: float):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE app.monitored_stocks SET current_price=%s, last_checked=now(), updated_at=now() WHERE id=%s",
                    (price, stock_id),
                )
                cur.execute(
                    "INSERT INTO app.price_history (stock_id, price, timestamp) VALUES (%s,%s,now())",
                    (stock_id, price),
                )

    def update_last_checked(self, stock_id: int):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE app.monitored_stocks SET last_checked=now(), updated_at=now() WHERE id=%s", (stock_id,))

    def has_recent_notification(self, stock_id: int, notification_type: str, minutes: int = 60) -> bool:
        symbol = self.get_stock_symbol_by_id(stock_id)
        if not symbol:
            return False
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM app.notifications WHERE stock_code=%s AND notify_type=%s AND created_at > now() - (%s || ' minutes')::interval",
                    (symbol, notification_type, int(minutes)),
                )
                return int(cur.fetchone()[0]) > 0

    def add_notification(self, stock_id: int, notification_type: str, message: str) -> int:
        symbol = self.get_stock_symbol_by_id(stock_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO app.notifications (stock_code, notify_type, content, status) VALUES (%s,%s,%s,'pending') RETURNING id",
                    (symbol, notification_type, message),
                )
                return int(cur.fetchone()[0])

    def get_pending_notifications(self) -> List[Dict]:
        sql = (
            "SELECT n.id, ms.id, ms.symbol, ms.name, n.notify_type, n.content, n.created_at "
            "FROM app.notifications n LEFT JOIN app.monitored_stocks ms ON n.stock_code = ms.symbol "
            "WHERE n.status='pending' ORDER BY n.created_at"
        )
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                for r in cur.fetchall():
                    out.append(
                        {
                            "id": r[0],
                            "stock_id": r[1],
                            "symbol": r[2],
                            "name": r[3],
                            "type": r[4],
                            "message": r[5],
                            "triggered_at": r[6].isoformat() if r[6] else None,
                        }
                    )
        return out

    def get_all_recent_notifications(self, limit: int = 10) -> List[Dict]:
        sql = (
            "SELECT n.id, ms.id, ms.symbol, ms.name, n.notify_type, n.content, n.created_at, n.status "
            "FROM app.notifications n LEFT JOIN app.monitored_stocks ms ON n.stock_code = ms.symbol "
            "ORDER BY n.created_at DESC LIMIT %s"
        )
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(limit),))
                for r in cur.fetchall():
                    out.append(
                        {
                            "id": r[0],
                            "stock_id": r[1],
                            "symbol": r[2],
                            "name": r[3],
                            "type": r[4],
                            "message": r[5],
                            "triggered_at": r[6].isoformat() if r[6] else None,
                            "sent": (r[7] != 'pending'),
                        }
                    )
        return out

    def mark_notification_sent(self, notification_id: int):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE app.notifications SET status='sent', sent_at=now() WHERE id=%s",
                    (notification_id,),
                )

    def mark_all_notifications_sent(self) -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE app.notifications SET status='sent', sent_at=now() WHERE status!='sent'")
                return cur.rowcount

    def clear_all_notifications(self) -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM app.notifications")
                return cur.rowcount

    def remove_monitored_stock(self, stock_id: int) -> bool:
        symbol = self.get_stock_symbol_by_id(stock_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM app.price_history WHERE stock_id=%s", (stock_id,))
                if symbol:
                    cur.execute("DELETE FROM app.notifications WHERE stock_code=%s", (symbol,))
                cur.execute("DELETE FROM app.monitored_stocks WHERE id=%s", (stock_id,))
                return cur.rowcount > 0

    def update_monitored_stock(
        self,
        stock_id: int,
        rating: str,
        entry_range: Dict,
        take_profit: Optional[float],
        stop_loss: Optional[float],
        check_interval: int,
        notification_enabled: bool,
        quant_enabled: Optional[bool] = None,
        quant_config: Optional[Dict] = None,
    ):
        sets = [
            ("rating", rating),
            ("entry_range", pg_extras.Json(entry_range or {})),
            ("take_profit", take_profit),
            ("stop_loss", stop_loss),
            ("check_interval", int(check_interval)),
            ("notification_enabled", bool(notification_enabled)),
        ]
        if quant_enabled is not None:
            sets.append(("quant_enabled", bool(quant_enabled)))
        if quant_config is not None:
            sets.append(("quant_config", pg_extras.Json(quant_config)))
        cols = ", ".join([f"{k}=%s" for k, _ in sets] + ["updated_at=now()"])
        vals = [v for _, v in sets] + [stock_id]
        sql = f"UPDATE app.monitored_stocks SET {cols} WHERE id=%s"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(vals))

    def toggle_notification(self, stock_id: int, enabled: bool):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE app.monitored_stocks SET notification_enabled=%s, updated_at=now() WHERE id=%s",
                    (bool(enabled), stock_id),
                )

    def get_stock_by_id(self, stock_id: int) -> Optional[Dict]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, symbol, name, rating, entry_range, take_profit, stop_loss, current_price, last_checked, check_interval, notification_enabled, quant_enabled, quant_config FROM app.monitored_stocks WHERE id=%s",
                    (stock_id,),
                )
                r = cur.fetchone()
                if not r:
                    return None
                entry_range = r[4] if isinstance(r[4], dict) else _to_json(r[4]) or {}
                quant_config = r[12] if isinstance(r[12], dict) else _to_json(r[12]) or {}
                return {
                    "id": r[0],
                    "symbol": r[1],
                    "name": r[2],
                    "rating": r[3],
                    "entry_range": entry_range,
                    "take_profit": r[5],
                    "stop_loss": r[6],
                    "current_price": r[7],
                    "last_checked": r[8].isoformat() if r[8] else None,
                    "check_interval": r[9],
                    "notification_enabled": bool(r[10]),
                    "quant_enabled": bool(r[11]),
                    "quant_config": quant_config,
                }

    def get_monitor_by_code(self, symbol: str) -> Optional[Dict]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, symbol, name, rating, entry_range, take_profit, stop_loss, current_price, last_checked, check_interval, notification_enabled, quant_enabled, quant_config FROM app.monitored_stocks WHERE symbol=%s",
                    (symbol,),
                )
                r = cur.fetchone()
                if not r:
                    return None
                entry_range = r[4] if isinstance(r[4], dict) else _to_json(r[4]) or {}
                quant_config = r[12] if isinstance(r[12], dict) else _to_json(r[12]) or {}
                return {
                    "id": r[0],
                    "symbol": r[1],
                    "name": r[2],
                    "rating": r[3],
                    "entry_range": entry_range,
                    "take_profit": r[5],
                    "stop_loss": r[6],
                    "current_price": r[7],
                    "last_checked": r[8].isoformat() if r[8] else None,
                    "check_interval": r[9],
                    "notification_enabled": bool(r[10]),
                    "quant_enabled": bool(r[11]),
                    "quant_config": quant_config,
                }

    def batch_add_or_update_monitors(self, monitors_data: List[Dict]) -> Dict[str, int]:
        added = 0
        updated = 0
        failed = 0
        for data in monitors_data:
            try:
                symbol = data.get('code') or data.get('symbol')
                name = data.get('name', symbol)
                rating = data.get('rating', '持有')
                entry_min = data.get('entry_min')
                entry_max = data.get('entry_max')
                take_profit = data.get('take_profit')
                stop_loss = data.get('stop_loss')
                check_interval = int(data.get('check_interval', 60))
                notification_enabled = bool(data.get('notification_enabled', True))
                if not symbol or not all([entry_min, entry_max, take_profit, stop_loss]):
                    failed += 1
                    continue
                entry_range = {"min": float(entry_min), "max": float(entry_max)}
                before = self.get_monitor_by_code(symbol)
                self.add_monitored_stock(
                    symbol=symbol,
                    name=name,
                    rating=rating,
                    entry_range=entry_range,
                    take_profit=float(take_profit),
                    stop_loss=float(stop_loss),
                    check_interval=check_interval,
                    notification_enabled=notification_enabled,
                    quant_enabled=False,
                    quant_config=None,
                )
                after = self.get_monitor_by_code(symbol)
                if before:
                    updated += 1
                else:
                    added += 1
            except Exception:
                failed += 1
        return {"added": added, "updated": updated, "failed": failed, "total": added + updated + failed}

    # helpers
    def get_stock_symbol_by_id(self, stock_id: int) -> Optional[str]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol FROM app.monitored_stocks WHERE id=%s", (stock_id,))
                r = cur.fetchone()
                return r[0] if r else None


# global instance compatible with existing imports
monitor_db = StockMonitorDatabase()
