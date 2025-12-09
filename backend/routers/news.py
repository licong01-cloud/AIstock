from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query

from ..db.pg_pool import get_conn


router = APIRouter(prefix="/news", tags=["news"])


def _fetchone(sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return None
            cols = [c[0] for c in cur.description]
            return dict(zip(cols, row))


def _fetchall(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def _to_iso(ts: Optional[dt.datetime]) -> Optional[str]:
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc).isoformat()


@router.get("/stats", summary="新闻数据统计（本地数据管理用）")
async def news_stats() -> Dict[str, Any]:
    """Return basic statistics about localized news data.

    - total_count: 总新闻条数
    - earliest_time / latest_time: 最早/最新发布时间
    - sources: 每个 source 的数量统计
    """

    # 使用实时表 app.news_articles_ts 统计最新数据，避免依赖日志或缓慢的聚合缓存。
    summary = _fetchone(
        """
        SELECT COUNT(*) AS total_count,
               MIN(publish_time) AS earliest_time,
               MAX(publish_time) AS latest_time
          FROM app.news_articles_ts
        """
    ) or {"total_count": 0, "earliest_time": None, "latest_time": None}

    by_source = _fetchall(
        """
        SELECT source, COUNT(*) AS count
          FROM app.news_articles_ts
         GROUP BY source
         ORDER BY count DESC
        """
    )

    return {
        "total_count": int(summary.get("total_count") or 0),
        "earliest_time": _to_iso(summary.get("earliest_time")),
        "latest_time": _to_iso(summary.get("latest_time")),
        "sources": [
            {"source": row["source"], "count": int(row["count"] or 0)} for row in by_source
        ],
    }


@router.get("/fast", summary="市场快讯列表（本地数据库，供前端实时展示）")
async def fast_news(
    limit: int = Query(100, ge=1, le=500, description="返回的最大新闻条数"),
    offset: int = Query(0, ge=0, description="起始偏移，用于分页"),
    source: Optional[str] = Query(None, description="按 source 精确过滤，可为空"),
) -> Dict[str, Any]:
    """Return latest fast news from localized DB for UI display.

    结果按 publish_time DESC 排序，尽量贴近 Go 程序的“市场快讯”风格。
    """

    params: List[Any] = []
    where = ""
    if source:
        where = "WHERE source = %s"
        params.append(source)

    sql = f"""
        SELECT id,
               source,
               title,
               content,
               url,
               publish_time,
               is_important
          FROM app.news_articles_ts
          {where}
         ORDER BY publish_time DESC
         LIMIT %s OFFSET %s
    """
    params.append(limit)
    params.append(offset)

    rows = _fetchall(sql, tuple(params))

    items = [
        {
            "id": r["id"],
            "source": r["source"],
            "title": r.get("title"),
            "content": r.get("content"),
            "url": r.get("url"),
            "publish_time": _to_iso(r.get("publish_time")),
            "is_important": bool(r.get("is_important")),
        }
        for r in rows
    ]

    return {"items": items}
