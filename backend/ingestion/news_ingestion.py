from __future__ import annotations

"""News ingestion module for AIstock.

This module is independent from existing TDX/Go ingestion code. It focuses on
fetching real-time news from external sources and writing them into
``app.news_articles`` using the shared PostgreSQL connection utilities.

At this stage we implement CLS telegraph ingestion as the first source and
provide a unified ``run_once_for_all_sources`` entrypoint which can be used by
external schedulers or scripts. Additional sources (Sina live, TradingView,
Eastmoney notices, etc.) can be added later following the same pattern.
"""

import datetime as dt
from dataclasses import dataclass
from typing import Iterable, List, Optional

import json
import re
import time

import requests
from dotenv import load_dotenv

from backend.db.pg_pool import get_conn


load_dotenv(override=True)


@dataclass
class NewsItem:
    source: str
    title: Optional[str]
    content: Optional[str]
    url: Optional[str]
    publish_time: dt.datetime
    is_important: bool = False
    external_id: Optional[str] = None


# ---------------------------------------------------------------------------
# CLS (财联社电报) ingestion
# ---------------------------------------------------------------------------


def _fetch_cls_telegraph_raw(timeout: int = 30) -> List[dict]:
    """Fetch latest CLS telegraph list using the public node API.

    Mirrors the Go implementation "TelegraphList":
    - GET https://www.cls.cn/nodeapi/telegraphList
    - Expects JSON with ``data.roll_data`` list.
    """

    url = "https://www.cls.cn/nodeapi/telegraphList"
    headers = {
        "Referer": "https://www.cls.cn/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Safari/537.36"
        ),
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        return []
    if int(data.get("error", 0)) != 0:
        return []
    payload = data.get("data") or {}
    roll = payload.get("roll_data") or []
    if not isinstance(roll, list):
        return []
    return [x for x in roll if isinstance(x, dict)]


def _normalize_cls_items(rows: Iterable[dict]) -> List[NewsItem]:
    items: List[NewsItem] = []
    for row in rows:
        try:
            content = str(row.get("content") or "").strip()
            if not content:
                continue
            share_url = row.get("shareurl") or ""
            level = str(row.get("level") or "")
            ctime = int(row.get("ctime") or 0)
            if ctime <= 0:
                # fallback: use current time
                publish_time = dt.datetime.now(dt.timezone.utc)
            else:
                publish_time = dt.datetime.fromtimestamp(ctime, tz=dt.timezone.utc)

            is_red = level != "C"
            item = NewsItem(
                source="cls_telegraph",
                title=None,
                content=content,
                url=share_url or None,
                publish_time=publish_time,
                is_important=is_red,
                external_id=str(row.get("id") or ""),
            )
            items.append(item)
        except Exception:
            # ignore broken rows
            continue
    return items


def fetch_cls_telegraph(timeout: int = 30) -> List[NewsItem]:
    """Fetch latest CLS telegraph messages as normalized ``NewsItem`` list."""

    raw = _fetch_cls_telegraph_raw(timeout=timeout)
    return _normalize_cls_items(raw)


# ---------------------------------------------------------------------------
# Sina Finance live (直播) ingestion
# ---------------------------------------------------------------------------


_SINA_FEED_URL = (
    "https://zhibo.sina.com.cn/api/zhibo/feed?callback=callback"
    "&page=1&page_size=20&zhibo_id=152&tag_id=0&dire=f&dpc=1&pagesize=20&id=4161089&type=0&_={ts}"
)


def _fetch_sina_live_raw(timeout: int = 30) -> List[dict]:
    """Fetch latest Sina Finance live feed.

    The API returns JavaScript like ``try{callback(...);}catch(e){};``.
    We strip the wrapper and parse JSON.
    """

    url = _SINA_FEED_URL.format(ts=int(time.time()))
    headers = {
        "Referer": "https://finance.sina.com.cn",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Safari/537.36"
        ),
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    text = resp.text or ""

    # 直接参考 Go 版逻辑：把 try{callback( 和 );}catch(e){}; 替换掉
    cleaned = text
    cleaned = cleaned.replace("try{callback(", "").replace(");}catch(e){};", "")
    cleaned = cleaned.strip()
    if not cleaned:
        return []

    try:
        data = json.loads(cleaned)
    except Exception:
        return []

    if not isinstance(data, dict):
        return []
    result = data.get("result") or {}
    if not isinstance(result, dict):
        return []
    feed = result.get("data") or {}
    if not isinstance(feed, dict):
        return []
    lst = feed.get("feed") or {}
    if not isinstance(lst, dict):
        return []
    items = lst.get("list") or []
    if not isinstance(items, list):
        return []
    return [x for x in items if isinstance(x, dict)]


def _normalize_sina_items(rows: Iterable[dict]) -> List[NewsItem]:
    items: List[NewsItem] = []
    for row in rows:
        try:
            content = str(row.get("rich_text") or "").strip()
            if not content:
                continue
            create_time = str(row.get("create_time") or "").strip()
            if create_time:
                # "2025-12-03 15:18:53" interpreted as local Asia/Shanghai then to UTC
                try:
                    local_dt = dt.datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S")
                    local_dt = local_dt.replace(tzinfo=dt.timezone(dt.timedelta(hours=8)))
                    publish_time = local_dt.astimezone(dt.timezone.utc)
                except Exception:
                    publish_time = dt.datetime.now(dt.timezone.utc)
            else:
                publish_time = dt.datetime.now(dt.timezone.utc)

            # tags: mark important if contains "焦点"
            tags = row.get("tag") or []
            important = False
            if isinstance(tags, list):
                for t in tags:
                    if not isinstance(t, dict):
                        continue
                    name = str(t.get("name") or "")
                    if "焦点" in name:
                        important = True
                        break

            item = NewsItem(
                source="sina_finance",
                title=None,
                content=content,
                url=None,
                publish_time=publish_time,
                is_important=important,
                external_id=str(row.get("id") or ""),
            )
            items.append(item)
        except Exception:
            continue
    return items


def fetch_sina_live(timeout: int = 30) -> List[NewsItem]:
    raw = _fetch_sina_live_raw(timeout=timeout)
    return _normalize_sina_items(raw)


# ---------------------------------------------------------------------------
# TradingView news ingestion
# ---------------------------------------------------------------------------


_TV_LIST_URL = (
    "https://news-mediator.tradingview.com/news-flow/v2/news?"
    "filter=area%3AWLD&filter=lang%3Azh-Hans&client=screener&streaming=false"
)


def _fetch_tv_list_raw(timeout: int = 30) -> List[dict]:
    headers = {
        "Host": "news-mediator.tradingview.com",
        "Origin": "https://cn.tradingview.com",
        "Referer": "https://cn.tradingview.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Safari/537.36"
        ),
    }
    resp = requests.get(_TV_LIST_URL, headers=headers, timeout=timeout)
    resp.raise_for_status()
    try:
        data = resp.json()
    except Exception:
        return []
    if not isinstance(data, dict):
        return []
    items = data.get("items") or []
    if not isinstance(items, list):
        return []
    return [x for x in items if isinstance(x, dict)]


def _fetch_tv_detail(news_id: str, timeout: int = 5) -> Optional[dict]:
    if not news_id:
        return None
    url = f"https://news-headlines.tradingview.com/v3/story?id={requests.utils.quote(news_id)}&lang=zh-Hans"
    headers = {
        "Host": "news-headlines.tradingview.com",
        "Origin": "https://cn.tradingview.com",
        "Referer": "https://cn.tradingview.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Safari/537.36"
        ),
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            return data
    except Exception:
        return None
    return None


def _normalize_tv_items(rows: Iterable[dict]) -> List[NewsItem]:
    items: List[NewsItem] = []
    for idx, row in enumerate(rows):
        try:
            news_id = str(row.get("id") or "")
            if not news_id:
                continue
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            published = row.get("published")
            try:
                ts = int(published)
                publish_time = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
            except Exception:
                publish_time = dt.datetime.now(dt.timezone.utc)

            # 为了控制调用量，仅对前若干条请求详情
            detail = None
            if idx < 10:
                detail = _fetch_tv_detail(news_id)
            content = ""
            if isinstance(detail, dict):
                content = str(detail.get("shortDescription") or "").strip()

            item = NewsItem(
                source="tradingview",
                title=title,
                content=content or None,
                url=f"https://cn.tradingview.com/news/{news_id}",
                publish_time=publish_time,
                is_important=False,
                external_id=news_id,
            )
            items.append(item)
        except Exception:
            continue
    return items


def fetch_tradingview(timeout: int = 30) -> List[NewsItem]:
    raw = _fetch_tv_list_raw(timeout=timeout)
    return _normalize_tv_items(raw)


# ---------------------------------------------------------------------------
# DB insertion helpers
# ---------------------------------------------------------------------------


def insert_news_items(items: Iterable[NewsItem]) -> int:
    """Insert a batch of news items into ``app.news_articles_ts``.

    We perform two layers of de-duplication:

    1. Application-level check against existing rows using
       ``(source, external_id, publish_time)`` when ``external_id`` is
       available. This mirrors the behaviour of the legacy Go implementation
       which de-duped by (source, content) / external identifiers.
    2. Rely on the database UNIQUE constraint
       ``uq_news_articles_source_time_title`` (source, publish_time, title)
       together with ``ON CONFLICT DO NOTHING`` as a final safety net.

    Returns the count of *newly inserted* rows.
    """

    rows = list(items)
    if not rows:
        return 0

    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO app.news_articles_ts (
                    source, external_id, title, content, url,
                    publish_time, is_important, ingest_time, raw_source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (source, external_id, publish_time) DO NOTHING
            """

            for it in rows:
                # 1) 如果有 external_id，优先用 (source, external_id, publish_time)
                #    在数据库中检查是否已存在，存在则直接跳过。
                if it.external_id:
                    cur.execute(
                        """
                        SELECT 1
                          FROM app.news_articles_ts
                         WHERE source = %s
                           AND external_id = %s
                           AND publish_time = %s
                        LIMIT 1
                        """,
                        (it.source, it.external_id, it.publish_time),
                    )
                    if cur.fetchone() is not None:
                        continue

                params = (
                    it.source,
                    it.external_id,
                    it.title,
                    it.content,
                    it.url,
                    it.publish_time,
                    it.is_important,
                    None,
                )
                cur.execute(sql, params)
                # ``rowcount`` is 1 only when a new row was inserted
                if cur.rowcount == 1:
                    inserted += 1
    return inserted


# ---------------------------------------------------------------------------
# Public entrypoints for schedulers / scripts
# ---------------------------------------------------------------------------


def run_once_for_all_sources(timeout_cls: int = 30) -> int:
    """Fetch and insert news from all configured sources once.

    For now we implement CLS telegraph, Sina live finance and TradingView news.
    Returns the total number of newly inserted articles across all sources.
    """

    total_inserted = 0

    # CLS telegraph
    cls_items = fetch_cls_telegraph(timeout=timeout_cls)
    total_inserted += insert_news_items(cls_items)

    # Sina 财经直播
    try:
        sina_items = fetch_sina_live(timeout=timeout_cls)
        total_inserted += insert_news_items(sina_items)
    except Exception:
        # 对单个源的失败保持容错，避免影响其他源
        pass

    # TradingView 外媒新闻
    try:
        tv_items = fetch_tradingview(timeout=timeout_cls)
        total_inserted += insert_news_items(tv_items)
    except Exception:
        pass

    return total_inserted


if __name__ == "__main__":
    # Simple manual test entrypoint; real scheduling should use an external
    # scheduler calling ``run_once_for_all_sources`` periodically.
    inserted = run_once_for_all_sources()
    print(f"Inserted {inserted} news articles (CLS telegraph)")
