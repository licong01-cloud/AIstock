from __future__ import annotations

"""在线递归抓取 Tushare 文档目录树。

流程：
1. 使用本地 `Tushare数据.htm` 解析出第一层 doc_id（例如 股票数据/基础数据/行情数据 等）。
2. 对每个 doc_id 在线请求 `https://tushare.pro/document/2?doc_id=XXX`，
   在该页面中解析左侧 jstree，把当前分支下的所有子节点抽取出来。
3. 将所有节点汇总写入 `docs/tushare_doc_tree_full.csv`。

说明：
- 该脚本只依赖公开网页，不做登录（如需登录可自行扩展 Cookie）。
- 目前只递归一层：从根页面发现的所有 doc_id 各抓一次页面，
  已足以覆盖“基础数据/行情数据”等二级目录下的接口列表。
"""

from pathlib import Path
import csv
from collections import defaultdict
from typing import Dict, List

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_HTML = PROJECT_ROOT / "Tushare数据.htm"
OUT_CSV = PROJECT_ROOT / "docs" / "tushare_doc_tree_full.csv"

BASE_URL = "https://tushare.pro/document/2"


def parse_jstree_from_html(html: str) -> List[Dict[str, str]]:
    """解析单个 HTML 页面中的 jstree 节点。

    返回：[{category_path, title, doc_id, href}...]
    """
    soup = BeautifulSoup(html, "html.parser")
    tree = soup.find("div", id="jstree")
    if tree is None:
        return []

    results: List[Dict[str, str]] = []

    def walk_li(li, parent_titles):
        a = li.find("a", class_="jstree-anchor")
        if a is None:
            return
        title = (a.get_text(strip=True) or "").replace("\xa0", " ")
        href = a.get("href") or ""
        doc_id = ""
        if "doc_id=" in href:
            import urllib.parse as up

            parsed = up.urlparse(href)
            qs = up.parse_qs(parsed.query)
            doc_id = (qs.get("doc_id") or [""])[0]

        path_list = parent_titles + [title]
        category_path = "/".join(path_list)
        results.append(
            {
                "category_path": category_path,
                "title": title,
                "doc_id": doc_id,
                "href": href,
            }
        )

        ul = li.find("ul", recursive=False)
        if not ul:
            return
        for child_li in ul.find_all("li", recursive=False):
            walk_li(child_li, path_list)

    root_ul = tree.find("ul", class_="jstree-container-ul")
    if root_ul is None:
        return []

    for li in root_ul.find_all("li", recursive=False):
        walk_li(li, [])

    return results


def load_root_doc_ids_from_local() -> List[str]:
    """从本地 HTML 中提取顶层/一级节点的 doc_id 列表。"""
    if not LOCAL_HTML.exists():
        raise SystemExit(f"本地 HTML 不存在: {LOCAL_HTML}")
    html = LOCAL_HTML.read_text(encoding="utf-8", errors="ignore")
    nodes = parse_jstree_from_html(html)
    doc_ids = []
    for n in nodes:
        if n.get("doc_id"):
            doc_ids.append(n["doc_id"])
    # 去重
    return sorted(set(doc_ids))


def fetch_doc_page(doc_id: str) -> str:
    params = {"doc_id": doc_id}
    resp = requests.get(BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.text


def build_full_tree() -> List[Dict[str, str]]:
    all_records: List[Dict[str, str]] = []

    # 1. 先用本地页面解析出根层节点
    root_html = LOCAL_HTML.read_text(encoding="utf-8", errors="ignore")
    root_nodes = parse_jstree_from_html(root_html)
    all_records.extend(root_nodes)

    root_doc_ids = load_root_doc_ids_from_local()
    print(f"found {len(root_doc_ids)} root doc_ids from local html: {root_doc_ids}")

    # 2. 针对每个根 doc_id 在线抓取其页面，解析子树
    seen_keys = {(r["category_path"], r.get("doc_id", "")) for r in all_records}

    for doc_id in root_doc_ids:
        try:
            html = fetch_doc_page(doc_id)
        except Exception as e:  # noqa: BLE001
            print(f"fetch doc_id={doc_id} failed: {e}")
            continue
        sub_nodes = parse_jstree_from_html(html)
        print(f"doc_id={doc_id} -> {len(sub_nodes)} nodes")
        for n in sub_nodes:
            key = (n["category_path"], n.get("doc_id", ""))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            all_records.append(n)

    return all_records


def main() -> None:
    records = build_full_tree()
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["category_path", "title", "doc_id", "href"],
        )
        writer.writeheader()
        writer.writerows(records)
    print(f"wrote {len(records)} records to {OUT_CSV}")


if __name__ == "__main__":
    main()
