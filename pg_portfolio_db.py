import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple, Any

from dotenv import load_dotenv
import psycopg2.extras as pg_extras

from app_pg import get_conn

load_dotenv(override=True)


def _utcnow():
    return datetime.now(timezone.utc)


def _parse_dt(s: Any) -> Optional[datetime]:
    if s is None:
        return None
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=timezone.utc)
    # best effort parse
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.strptime(str(s), fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(str(s))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


class PortfolioDBPG:
    def add_stock(self, code: str, name: str, cost_price: Optional[float] = None,
                  quantity: Optional[int] = None, note: str = "",
                  auto_monitor: bool = True) -> int:
        sql = (
            "INSERT INTO app.portfolio_stocks (code, name, cost_price, quantity, note, auto_monitor) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (code, name, cost_price, quantity, note, bool(auto_monitor)))
                return int(cur.fetchone()[0])

    def update_stock(self, stock_id: int, **kwargs) -> bool:
        allowed = {"code", "name", "cost_price", "quantity", "note", "auto_monitor"}
        sets = []
        vals: List[Any] = []
        for k, v in kwargs.items():
            if k in allowed:
                if k == "auto_monitor":
                    v = bool(v)
                sets.append(f"{k}=%s")
                vals.append(v)
        if not sets:
            return False
        sets.append("updated_at=now()")
        vals.append(stock_id)
        sql = f"UPDATE app.portfolio_stocks SET {', '.join(sets)} WHERE id=%s"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(vals))
                return cur.rowcount > 0

    def delete_stock(self, stock_id: int) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM app.portfolio_stocks WHERE id=%s", (stock_id,))
                return cur.rowcount > 0

    def get_stock(self, stock_id: int) -> Optional[Dict]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at FROM app.portfolio_stocks WHERE id=%s", (stock_id,))
                r = cur.fetchone()
                if not r:
                    return None
                return {
                    "id": r[0],
                    "code": r[1],
                    "name": r[2],
                    "cost_price": r[3],
                    "quantity": r[4],
                    "note": r[5],
                    "auto_monitor": bool(r[6]),
                    "created_at": r[7].isoformat() if r[7] else None,
                    "updated_at": r[8].isoformat() if r[8] else None,
                }

    def get_stock_by_code(self, code: str) -> Optional[Dict]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at FROM app.portfolio_stocks WHERE code=%s", (code,))
                r = cur.fetchone()
                if not r:
                    return None
                return {
                    "id": r[0],
                    "code": r[1],
                    "name": r[2],
                    "cost_price": r[3],
                    "quantity": r[4],
                    "note": r[5],
                    "auto_monitor": bool(r[6]),
                    "created_at": r[7].isoformat() if r[7] else None,
                    "updated_at": r[8].isoformat() if r[8] else None,
                }

    def get_all_stocks(self, auto_monitor_only: bool = False) -> List[Dict]:
        sql = "SELECT id, code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at FROM app.portfolio_stocks"
        if auto_monitor_only:
            sql += " WHERE auto_monitor = TRUE"
        sql += " ORDER BY created_at DESC"
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                for r in cur.fetchall():
                    out.append({
                        "id": r[0],
                        "code": r[1],
                        "name": r[2],
                        "cost_price": r[3],
                        "quantity": r[4],
                        "note": r[5],
                        "auto_monitor": bool(r[6]),
                        "created_at": r[7].isoformat() if r[7] else None,
                        "updated_at": r[8].isoformat() if r[8] else None,
                    })
        return out

    def search_stocks(self, keyword: str) -> List[Dict]:
        kw = f"%{keyword}%"
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at FROM app.portfolio_stocks WHERE code ILIKE %s OR name ILIKE %s ORDER BY created_at DESC",
                    (kw, kw),
                )
                for r in cur.fetchall():
                    out.append({
                        "id": r[0],
                        "code": r[1],
                        "name": r[2],
                        "cost_price": r[3],
                        "quantity": r[4],
                        "note": r[5],
                        "auto_monitor": bool(r[6]),
                        "created_at": r[7].isoformat() if r[7] else None,
                        "updated_at": r[8].isoformat() if r[8] else None,
                    })
        return out

    def get_stock_count(self) -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM app.portfolio_stocks")
                return int(cur.fetchone()[0])

    # ----- analysis history -----
    def save_analysis(self, stock_id: int, rating: str, confidence: float,
                      current_price: float, target_price: Optional[float] = None,
                      entry_min: Optional[float] = None, entry_max: Optional[float] = None,
                      take_profit: Optional[float] = None, stop_loss: Optional[float] = None,
                      summary: str = "") -> int:
        sql = (
            "INSERT INTO app.portfolio_analysis_history (portfolio_stock_id, analysis_time, rating, confidence, current_price, target_price, entry_min, entry_max, take_profit, stop_loss, summary) "
            "VALUES (%s, now(), %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    stock_id, rating, confidence, current_price, target_price, entry_min, entry_max, take_profit, stop_loss, summary
                ))
                return int(cur.fetchone()[0])

    def get_analysis_history(self, stock_id: int, limit: int = 10) -> List[Dict]:
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, portfolio_stock_id, analysis_time, rating, confidence, current_price, target_price, entry_min, entry_max, take_profit, stop_loss, summary FROM app.portfolio_analysis_history WHERE portfolio_stock_id = %s ORDER BY analysis_time DESC LIMIT %s",
                    (stock_id, int(limit)),
                )
                for r in cur.fetchall():
                    out.append({
                        "id": r[0],
                        "portfolio_stock_id": r[1],
                        "analysis_time": r[2].isoformat() if r[2] else None,
                        "rating": r[3],
                        "confidence": r[4],
                        "current_price": r[5],
                        "target_price": r[6],
                        "entry_min": r[7],
                        "entry_max": r[8],
                        "take_profit": r[9],
                        "stop_loss": r[10],
                        "summary": r[11],
                    })
        return out

    def get_latest_analysis_history(self, stock_id: int, limit: int = 10) -> List[Dict]:
        return self.get_analysis_history(stock_id, limit)

    def get_latest_analysis(self, stock_id: int) -> Optional[Dict]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, portfolio_stock_id, analysis_time, rating, confidence, current_price, target_price, entry_min, entry_max, take_profit, stop_loss, summary FROM app.portfolio_analysis_history WHERE portfolio_stock_id = %s ORDER BY analysis_time DESC LIMIT 1",
                    (stock_id,),
                )
                r = cur.fetchone()
                if not r:
                    return None
                return {
                    "id": r[0],
                    "portfolio_stock_id": r[1],
                    "analysis_time": r[2].isoformat() if r[2] else None,
                    "rating": r[3],
                    "confidence": r[4],
                    "current_price": r[5],
                    "target_price": r[6],
                    "entry_min": r[7],
                    "entry_max": r[8],
                    "take_profit": r[9],
                    "stop_loss": r[10],
                    "summary": r[11],
                }

    def get_all_latest_analysis(self) -> List[Dict]:
        sql = (
            "SELECT s.id, s.code, s.name, s.cost_price, s.quantity, s.note, s.auto_monitor, h.rating, h.confidence, h.current_price, h.target_price, h.entry_min, h.entry_max, h.take_profit, h.stop_loss, h.analysis_time "
            "FROM app.portfolio_stocks s "
            "LEFT JOIN LATERAL (SELECT rating, confidence, current_price, target_price, entry_min, entry_max, take_profit, stop_loss, analysis_time FROM app.portfolio_analysis_history h WHERE h.portfolio_stock_id = s.id ORDER BY analysis_time DESC LIMIT 1) h ON TRUE "
            "ORDER BY s.created_at DESC"
        )
        out: List[Dict] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                for r in cur.fetchall():
                    out.append({
                        "id": r[0],
                        "code": r[1],
                        "name": r[2],
                        "cost_price": r[3],
                        "quantity": r[4],
                        "note": r[5],
                        "auto_monitor": bool(r[6]),
                        "rating": r[7],
                        "confidence": r[8],
                        "current_price": r[9],
                        "target_price": r[10],
                        "entry_min": r[11],
                        "entry_max": r[12],
                        "take_profit": r[13],
                        "stop_loss": r[14],
                        "analysis_time": r[15].isoformat() if r[15] else None,
                    })
        return out

    def get_rating_changes(self, stock_id: int, days: int = 30) -> List[Tuple[str, str, str]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT analysis_time, rating FROM app.portfolio_analysis_history WHERE portfolio_stock_id = %s AND analysis_time >= now() - (%s || ' days')::interval ORDER BY analysis_time ASC",
                    (stock_id, int(days)),
                )
                rows = cur.fetchall()
                changes: List[Tuple[str, str, str]] = []
                for i in range(1, len(rows)):
                    prev_rating = rows[i - 1][1]
                    curr_rating = rows[i][1]
                    if prev_rating != curr_rating:
                        changes.append((rows[i][0].isoformat(), prev_rating, curr_rating))
                return changes

    def delete_old_analysis(self, days: int = 90) -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM app.portfolio_analysis_history WHERE analysis_time < now() - (%s || ' days')::interval",
                    (int(days),),
                )
                return cur.rowcount


# global instance to match old usage
portfolio_db = PortfolioDBPG()
