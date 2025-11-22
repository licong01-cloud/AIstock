from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import time


class EastmoneyCloudSelector:
    """东方财富智能选股/热门策略接口封装（next_app 内部实现）。

    逻辑基于根目录 cloud_screening.EastmoneyCloudSelector 拷贝，
    仅用于在新后端中提供云选股能力。
    """

    SEARCH_URL = "https://np-tjxg-g.eastmoney.com/api/smart-tag/stock/v3/pw/search-code"
    HOT_STRATEGY_URL = "https://np-ipick.eastmoney.com/recommend/stock/heat/ranking"

    COMMON_HEADERS = {
        "Origin": "https://xuangu.eastmoney.com",
        "Referer": "https://xuangu.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:145.0) Gecko/20100101 Firefox/145.0",
        "Content-Type": "application/json",
    }

    def search(self, keyword: str, page_size: int = 50) -> Dict[str, Any]:
        """调用东财智能选股搜索接口，返回原始 JSON。"""

        payload = {
            "keyWord": keyword,
            "pageSize": int(page_size),
            "pageNo": 1,
            "fingerprint": "02efa8944b1f90fbfe050e1e695a480d",
            "gids": [],
            "matchWord": "",
            "timestamp": str(int(time.time())),
            "shareToGuba": False,
            "requestId": f"gs_cloud_{int(time.time() * 1000)}",
            "needCorrect": True,
            "removedConditionIdList": [],
            "xcId": "xc0d61279aad33008260",
            "ownSelectAll": False,
            "dxInfo": [],
            "extraCondition": "",
        }

        headers = dict(self.COMMON_HEADERS)
        headers["Host"] = "np-tjxg-g.eastmoney.com"

        resp = requests.post(self.SEARCH_URL, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_hot_strategies(self, limit: int = 20) -> Dict[str, Any]:
        """获取东财云选股热门策略列表。"""

        ts = int(time.time())
        params = {
            "count": int(limit),
            "trace": ts,
            "client": "web",
            "biz": "web_smart_tag",
        }
        headers = dict(self.COMMON_HEADERS)
        headers["Host"] = "np-ipick.eastmoney.com"

        resp = requests.get(self.HOT_STRATEGY_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()


_selector_singleton: Optional[EastmoneyCloudSelector] = None


def get_cloud_selector() -> EastmoneyCloudSelector:
    global _selector_singleton
    if _selector_singleton is None:
        _selector_singleton = EastmoneyCloudSelector()
    return _selector_singleton


def _extract_stock_df(resp: Dict[str, Any]) -> pd.DataFrame:
    """按照旧 UI 中的逻辑，从东财响应中提取股票列表 DataFrame。"""

    if not isinstance(resp, dict):
        return pd.DataFrame()

    code_val = resp.get("code")
    # 东财有时返回字符串 "100"，有时返回整型 100，这里统一视为成功
    if str(code_val) != "100":
        return pd.DataFrame()

    data = resp.get("data") or {}
    if not isinstance(data, dict):
        return pd.DataFrame()

    result = data.get("result") or {}
    if not isinstance(result, dict):
        return pd.DataFrame()

    columns = result.get("columns") or []
    data_list = result.get("dataList") or []
    if not isinstance(columns, list) or not isinstance(data_list, list) or not data_list:
        return pd.DataFrame()

    # 构造列 key -> 显示名
    headers: Dict[str, str] = {}
    for col in columns:
        if not isinstance(col, dict):
            continue
        if col.get("hiddenNeed"):
            continue
        title = str(col.get("title") or "")
        unit = col.get("unit") or ""
        if unit:
            title = f"{title}[{unit}]"

        children = col.get("children")
        if not children:
            key = col.get("key")
            if key:
                headers[str(key)] = title
        else:
            for child in children:
                if not isinstance(child, dict) or child.get("hiddenNeed"):
                    continue
                child_key = child.get("key")
                if not child_key:
                    continue
                child_title = child.get("dateMsg") or title
                headers[str(child_key)] = str(child_title)

    rows: List[Dict[str, Any]] = []
    for item in data_list:
        if not isinstance(item, dict):
            continue

        code = (
            item.get("SECURITY_CODE")
            or item.get("code")
            or item.get("stockCode")
            or item.get("f12")
            or ""
        )
        name = (
            item.get("SECURITY_SHORT_NAME")
            or item.get("name")
            or item.get("stockName")
            or item.get("f14")
            or ""
        )

        row: Dict[str, Any] = {
            "code": str(code),
            "name": str(name),
        }

        for key, col_name in headers.items():
            row[col_name] = item.get(key)

        rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).drop_duplicates(subset=["code"]).reset_index(drop=True)
    return df


def _parse_hot_strategies(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """从热门策略接口返回数据中解析出策略列表（id/name/desc/keyword）。"""

    if not isinstance(raw, dict):
        return []

    data = raw.get("data")
    items: List[Dict[str, Any]] = []
    if isinstance(data, list):
        items = [it for it in data if isinstance(it, dict)]
    elif isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                items = [it for it in v if isinstance(it, dict)]
                break

    strategies: List[Dict[str, Any]] = []
    for idx, it in enumerate(items):
        question = it.get("question")

        name = (
            question
            or it.get("name")
            or it.get("strategyName")
            or it.get("title")
            or it.get("label")
            or it.get("tagName")
            or it.get("desc")
            or it.get("description")
            or f"策略{idx + 1}"
        )

        desc = (
            question
            or it.get("desc")
            or it.get("description")
            or it.get("subTitle")
            or it.get("subtitle")
            or it.get("reason")
            or it.get("reasonDesc")
            or it.get("content")
            or it.get("remark")
            or it.get("tip")
            or ""
        )

        keyword = (
            question
            or it.get("keyWord")
            or it.get("keyword")
            or it.get("words")
            or it.get("query")
            or it.get("name")
            or it.get("strategyName")
            or it.get("title")
            or it.get("label")
            or ""
        )

        sid = it.get("id") or it.get("strategyId") or it.get("code") or name
        strategies.append(
            {
                "id": sid,
                "name": str(name),
                "desc": str(desc),
                "keyword": str(keyword),
            }
        )

    return strategies


def search_stocks(keyword: str, page_size: int = 100) -> List[Dict[str, Any]]:
    """云选股：按关键词调用东财接口并返回股票列表记录。"""

    selector = get_cloud_selector()
    resp = selector.search(keyword, page_size)
    df = _extract_stock_df(resp)
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


def get_hot_strategy_list(limit: int = 20) -> List[Dict[str, Any]]:
    """获取热门云选股策略的精简列表。"""

    selector = get_cloud_selector()
    raw = selector.get_hot_strategies(limit=limit)
    return _parse_hot_strategies(raw)
