from __future__ import annotations

"""通过在线访问 Tushare 文档页，按接口名自动发现 doc_id 并缓存映射。

注意：
- 此脚本依赖公网访问 https://tushare.pro/document/2
- 如果接口文档需要登录才能看到完整内容，本方法可能不稳定。
- 建议只在你本机运行，用于减少每次手工查 doc_id 的工作量。
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://tushare.pro/document/2"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOC_ID_CACHE_PATH = PROJECT_ROOT / "docs" / "tushare_doc_ids.json"


@dataclass
class DocMatch:
    interface: str
    doc_id: int
    url: str
    title: str


def load_doc_id_cache() -> Dict[str, int]:
    if not DOC_ID_CACHE_PATH.exists():
        return {}
    try:
        with DOC_ID_CACHE_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        # 只保留 str->int 的映射
        return {k: int(v) for k, v in data.items()}
    except Exception:
        return {}


def save_doc_id_cache(cache: Dict[str, int]) -> None:
    DOC_ID_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with DOC_ID_CACHE_PATH.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def fetch_doc_index_html() -> str:
    resp = requests.get(BASE_URL, timeout=15)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def discover_doc_ids_from_html(html: str) -> List[DocMatch]:
    """从文档首页 HTML 中解析出候选接口名及 doc_id。

    逻辑：
    - 在左侧树或正文链接中查找 href 中包含 `doc_id=` 的 a 标签；
    - 将 a 标签文本中的英文单词视为可能的接口名（例如 daily、pro_bar 等）；
    - 返回 (接口名, doc_id, url, 标题文本) 的候选列表。
    """
    soup = BeautifulSoup(html, "html.parser")
    matches: List[DocMatch] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "doc_id=" not in href:
            continue
        m = re.search(r"doc_id=(\d+)", href)
        if not m:
            continue
        doc_id = int(m.group(1))
        text = a.get_text(strip=True) or ""
        # 尝试从文本中提取接口名：通常接口名会在文案里出现，例如 "日线行情(daily)" 或直接是 "daily"
        interface = None
        # 1）直接是英文单词
        m_en = re.search(r"([A-Za-z_]+)", text)
        if m_en:
            interface = m_en.group(1)
        # 2）在 title 或 data-* 属性里
        if not interface:
            title = a.get("title")
            if title:
                m2 = re.search(r"([A-Za-z_]+)", title)
                if m2:
                    interface = m2.group(1)
        if not interface:
            continue
        full_url = href if href.startswith("http") else ("https://tushare.pro" + href)
        matches.append(DocMatch(interface=interface, doc_id=doc_id, url=full_url, title=text))

    return matches


def build_interface_doc_map(html: str) -> Dict[str, DocMatch]:
    result: Dict[str, DocMatch] = {}
    for m in discover_doc_ids_from_html(html):
        # 如果同一接口名出现多次，优先保留第一次发现的
        if m.interface not in result:
            result[m.interface] = m
    return result


def ensure_doc_id(interface: str) -> Optional[int]:
    """确保某个接口名有 doc_id 映射。如果已有缓存则直接返回，否则尝试在线发现。"""
    cache = load_doc_id_cache()
    if interface in cache:
        return cache[interface]

    html = fetch_doc_index_html()
    mapping = build_interface_doc_map(html)
    if interface not in mapping:
        print(f"未在在线文档中找到接口 {interface} 对应的 doc_id")
        return None

    doc_match = mapping[interface]
    cache[interface] = doc_match.doc_id
    save_doc_id_cache(cache)
    print(f"发现接口 {interface} 的 doc_id={doc_match.doc_id}, url={doc_match.url}")
    return doc_match.doc_id


def main() -> None:
    # 示例：默认尝试 daily 接口
    interface = "daily"
    doc_id = ensure_doc_id(interface)
    if doc_id is None:
        print(f"未能发现 {interface} 的 doc_id")
    else:
        print(f"接口 {interface} doc_id = {doc_id}")


if __name__ == "__main__":
    main()
