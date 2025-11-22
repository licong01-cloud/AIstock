from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://tushare.pro/document/2"


def _cache_path() -> Path:
    root = Path(__file__).resolve().parents[2]
    docs_dir = root / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    return docs_dir / "tushare_fields_cache.json"


def load_cache() -> Dict[str, Any]:
    path = _cache_path()
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(cache: Dict[str, Any]) -> None:
    path = _cache_path()
    with path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def fetch_doc_html(doc_id: int) -> str:
    resp = requests.get(BASE_URL, params={"doc_id": doc_id}, timeout=15)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def parse_fields_from_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    fields: List[Dict[str, Any]] = []
    for table in tables:
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if not headers:
            continue
        # 带有“必选/必填”列的表一般是【输入参数】，这里跳过，专注解析输出字段
        if any("必选" in h or "必填" in h for h in headers):
            continue
        lower_headers = [h.lower() for h in headers]

        # 识别字段定义表：
        # 1）原逻辑：表头包含“字段/field”
        # 2）Tushare 文档常见模式：表头为 ["名称","类型","描述"]
        has_field_word = any("字段" in h or "field" in h.lower() for h in headers)
        has_name_col = any("名称" in h or "name" == h.lower() for h in headers)
        has_type_col = any("类型" in h or "type" == h.lower() for h in headers)
        has_desc_col = any("说明" in h or "描述" in h or "描述" in h or "desc" in h.lower() for h in headers)
        if not (has_field_word or (has_name_col and has_type_col and has_desc_col)):
            continue
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < len(headers):
                continue
            row = {}
            for idx, head in enumerate(headers):
                key = head.strip()
                value = tds[idx].get_text(strip=True)
                row[key] = value
            name_keys = [
                i
                for i, h in enumerate(lower_headers)
                if ("字段" in h) or ("name" in h) or ("名称" in headers[i])
            ]
            if not name_keys:
                continue
            name = tds[name_keys[0]].get_text(strip=True)
            if not name:
                continue
            field: Dict[str, Any] = {"name": name}
            cn_keys = [i for i, h in enumerate(lower_headers) if "说明" in h or "描述" in h or "name" == h]
            if cn_keys:
                field["cn"] = tds[cn_keys[0]].get_text(strip=True)
            type_keys = [i for i, h in enumerate(lower_headers) if "类型" in h or "type" in h]
            if type_keys:
                field["type"] = tds[type_keys[0]].get_text(strip=True)
            # 使用表格最后一列作为原始描述，供后续生成字段说明使用
            try:
                field["desc"] = tds[len(headers) - 1].get_text(strip=True)
            except Exception:  # noqa: BLE001
                field["desc"] = field.get("cn", "")
            field["raw"] = row
            fields.append(field)
        if fields:
            break
    return fields


def get_interface_fields(name: str, doc_id: int | None = None, force_refresh: bool = False) -> List[Dict[str, Any]]:
    cache = load_cache()
    if not force_refresh and name in cache:
        return cache[name].get("fields", [])
    if doc_id is None:
        raise ValueError("doc_id is required when fields are not cached")
    html = fetch_doc_html(doc_id)
    fields = parse_fields_from_html(html)
    cache[name] = {"doc_id": doc_id, "fields": fields}
    save_cache(cache)
    return fields


def list_cached_interfaces() -> List[str]:
    cache = load_cache()
    return list(cache.keys())
