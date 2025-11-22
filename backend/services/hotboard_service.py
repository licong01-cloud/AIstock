from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import Any, Dict, List, Optional

import psycopg2.extras as pgx
import requests
from zoneinfo import ZoneInfo

from ..core.data_source_manager_impl import data_source_manager
from ..db.pg_pool import get_conn


def _fetchall(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(row) for row in rows]


def _now_sh() -> dt.datetime:
    return dt.datetime.now(ZoneInfo("Asia/Shanghai"))


def _isoformat(value: Optional[dt.datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat()


# --------------------- 实时与历史热点 ---------------------


def _latest_intraday_ts() -> Optional[dt.datetime]:
    rows = _fetchall("SELECT MAX(ts) AS ts FROM market.sina_board_intraday")
    if rows and rows[0].get("ts"):
        return rows[0]["ts"]
    return None


def _norm(vals: List[float]) -> List[float]:
    nz = [v for v in vals if v is not None]
    if not nz:
        return [0.0 for _ in vals]
    mn = min(nz)
    mx = max(nz)
    rng = (mx - mn) or 1.0
    return [(((v - mn) / rng) * 2 - 1) if v is not None else 0.0 for v in vals]


def get_hotboard_realtime(
    metric: str = "combo",
    alpha: float = 0.5,
    cate_type: Optional[int] = None,
    at: Optional[str] = None,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    the_ts: Optional[dt.datetime] = None
    if at:
        try:
            the_ts = dt.datetime.fromisoformat(at.replace("Z", "+00:00"))
        except Exception:
            the_ts = None
    if the_ts is None:
        the_ts = _latest_intraday_ts()
    if the_ts is None:
        return {"ts": None, "items": []}

    where = "WHERE ts=%s"
    params: List[Any] = [the_ts]
    if cate_type is not None:
        where += " AND cate_type=%s"
        params.append(int(cate_type))

    rows = _fetchall(
        f"""
        SELECT cate_type, board_code, board_name, pct_chg, amount, net_inflow, turnover, ratioamount
          FROM market.sina_board_intraday
          {where}
        ORDER BY cate_type ASC, board_code ASC
        """,
        tuple(params),
    )

    chg = [float(r.get("pct_chg") or 0.0) for r in rows]
    flow = [float(r.get("net_inflow") or 0.0) for r in rows]
    nz_chg = _norm(chg)
    nz_flow = _norm(flow)
    score: List[float] = []
    m = (metric or "combo").lower()
    for i in range(len(rows)):
        if m == "chg":
            score.append(nz_chg[i])
        elif m == "flow":
            score.append(nz_flow[i])
        else:
            a = max(0.0, min(1.0, float(alpha or 0.5)))
            score.append(a * nz_chg[i] + (1 - a) * nz_flow[i])
    for i, r in enumerate(rows):
        r["score"] = score[i]

    _ = t0  # 保留以便后续需要性能日志
    return {"ts": _isoformat(the_ts), "items": rows}


def get_hotboard_realtime_timestamps(
    date: Optional[str] = None,
    cate_type: Optional[int] = None,
) -> Dict[str, Any]:
    if not date:
        date = _now_sh().strftime("%Y-%m-%d")
    if "T" in date:
        d0 = dt.datetime.fromisoformat(date.split("T", 1)[0])
    else:
        d0 = dt.datetime.fromisoformat(date)
    start = d0.replace(tzinfo=dt.timezone.utc)
    end = (d0 + dt.timedelta(days=1)).replace(tzinfo=dt.timezone.utc)

    where = "WHERE ts >= %s AND ts < %s"
    params: List[Any] = [start, end]
    if cate_type is not None:
        where += " AND cate_type=%s"
        params.append(int(cate_type))

    rows = _fetchall(
        f"SELECT DISTINCT ts FROM market.sina_board_intraday {where} ORDER BY ts ASC",
        tuple(params),
    )
    return {"date": date, "timestamps": [_isoformat(r.get("ts")) for r in rows]}


def get_hotboard_daily(date: str, cate_type: Optional[int] = None) -> Dict[str, Any]:
    where = "WHERE trade_date=%s"
    params: List[Any] = [date]
    if cate_type is not None:
        where += " AND cate_type=%s"
        params.append(int(cate_type))

    rows = _fetchall(
        f"""
        SELECT trade_date, cate_type, board_code, board_name, pct_chg, amount, net_inflow, turnover, ratioamount
          FROM market.sina_board_daily
          {where}
        ORDER BY cate_type ASC, board_code ASC
        """,
        tuple(params),
    )
    return {"date": date, "items": rows}


# --------------------- TDX 板块历史 ---------------------


def get_tdx_board_types() -> Dict[str, Any]:
    rows = _fetchall(
        """
        SELECT DISTINCT idx_type FROM market.tdx_board_index
         WHERE idx_type IS NOT NULL
         ORDER BY idx_type
        """,
    )
    types = [r.get("idx_type") for r in rows if r.get("idx_type")]
    return {"items": types}


def get_tdx_board_daily(
    date: str,
    idx_type: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    params: List[Any] = [date, date]
    where_extra = ""
    if idx_type:
        where_extra = " AND i2.idx_type=%s"
        params.append(idx_type)

    sql = (
        """
        WITH i2 AS (
            SELECT DISTINCT ON (ts_code) ts_code, name, idx_type
              FROM market.tdx_board_index
             WHERE trade_date IS NULL OR trade_date <= %s
             ORDER BY ts_code, trade_date DESC NULLS LAST
        )
        SELECT d.trade_date, d.ts_code AS board_code, i2.name AS board_name, i2.idx_type,
               d.pct_chg, d.amount
          FROM market.tdx_board_daily d
          JOIN i2 ON i2.ts_code = d.ts_code
         WHERE d.trade_date = %s
        """
        + where_extra
        + """
         ORDER BY i2.idx_type, d.amount DESC NULLS LAST
         LIMIT %s
        """
    )
    params.append(max(1, int(limit)))
    rows = _fetchall(sql, tuple(params))
    return {"date": date, "items": rows}


# --------------------- Top stocks ---------------------


def _sina_headers() -> Dict[str, str]:
    return {
        "Host": "vip.stock.finance.sina.com.cn",
        "Referer": "https://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }


def _sina_concept_stocks(concept_code: str, page: int = 1, num: int = 200) -> List[Dict[str, Any]]:
    url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
    try:
        r = requests.get(
            url,
            params={
                "node": concept_code,
                "page": page,
                "num": num,
                "sort": "symbol",
                "asc": 1,
                "symbol": "",
                "_s_r_a": "page",
            },
            headers=_sina_headers(),
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def get_top_stocks_realtime(
    board_code: str,
    metric: str = "chg",
    limit: int = 20,
) -> Dict[str, Any]:
    stocks: List[Dict[str, Any]] = []
    page = 1
    while page <= 5 and len(stocks) < max(200, limit):
        part = _sina_concept_stocks(board_code, page=page, num=200)
        if not part:
            break
        stocks.extend(part)
        if len(part) < 200:
            break
        page += 1

    enriched: List[Dict[str, Any]] = []
    for s in stocks:
        code6 = str(s.get("code") or s.get("symbol") or "").split(".")[-1]
        if not code6 or len(code6) != 6:
            continue
        try:
            q = data_source_manager.get_realtime_quotes(code6)
        except Exception:
            q = {}
        price = q.get("price")
        pre_close = q.get("pre_close")
        pct = None
        if isinstance(price, (int, float)) and isinstance(pre_close, (int, float)) and pre_close not in (0, None):
            try:
                pct = (price - pre_close) / pre_close * 100.0
            except Exception:
                pct = None
        amount = q.get("amount")

        name = s.get("name") or s.get("ts_name")
        if not name:
            try:
                sec = data_source_manager.get_security_name_and_type(code6)
            except Exception:
                sec = None
            if isinstance(sec, dict) and sec.get("name"):
                name = sec.get("name")
        if not name:
            name = code6

        enriched.append(
            {
                "code": code6,
                "name": name,
                "pct_change": pct,
                "amount": amount,
                "open": q.get("open"),
                "prev_close": pre_close,
                "high": q.get("high"),
                "low": q.get("low"),
                "volume": q.get("volume"),
            }
        )

    m = (metric or "chg").lower()

    def _key(it: Dict[str, Any]) -> float:
        try:
            return float(it.get("pct_change") if m == "chg" else (it.get("amount") or 0.0))
        except Exception:
            return -1e18

    ranked = sorted(enriched, key=_key, reverse=True)[: max(1, int(limit))]
    return {"items": ranked}


def get_top_stocks_tdx(
    board_code: str,
    date: str,
    metric: str = "chg",
    limit: int = 20,
) -> Dict[str, Any]:
    # 从 membership 表中找出成分股代码
    mem = _fetchall(
        """
        SELECT con_code FROM market.tdx_board_member
         WHERE trade_date=%s AND ts_code=%s
        """,
        (date, board_code),
    )
    codes = [r.get("con_code") for r in mem if r.get("con_code")]
    if not codes:
        return {"items": []}

    enriched: List[Dict[str, Any]] = []
    for ts_code in codes:
        base = ts_code
        if "." in str(ts_code):
            try:
                base = data_source_manager._convert_from_ts_code(ts_code)  # type: ignore[attr-defined]
            except Exception:
                base = ts_code
        try:
            q = data_source_manager.get_realtime_quotes(base)
        except Exception:
            q = {}
        price = q.get("price")
        pre_close = q.get("pre_close")
        pct = None
        if isinstance(price, (int, float)) and isinstance(pre_close, (int, float)) and pre_close not in (0, None):
            try:
                pct = (price - pre_close) / pre_close * 100.0
            except Exception:
                pct = None
        amount = q.get("amount")

        name = None
        try:
            info = data_source_manager.get_stock_basic_info(base)
        except Exception:
            info = {}
        if isinstance(info, dict):
            name = info.get("name") or info.get("stock_name")
        if not name:
            name = base

        enriched.append(
            {
                "ts_code": ts_code,
                "code": base,
                "name": name,
                "pct_chg": pct,
                "amount": amount,
                "open_li": q.get("open"),
                "high_li": q.get("high"),
                "low_li": q.get("low"),
                "volume_hand": (q.get("volume") or 0) / 100.0 if q.get("volume") else None,
            }
        )

    m = (metric or "chg").lower()

    def _key2(it: Dict[str, Any]) -> float:
        try:
            return float(it.get("pct_chg") if m == "chg" else (it.get("amount") or 0.0))
        except Exception:
            return -1e18

    ranked = sorted(enriched, key=_key2, reverse=True)[: max(1, int(limit))]
    return {"items": ranked}
