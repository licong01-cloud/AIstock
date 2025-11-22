from __future__ import annotations

"""从本地保存的 Tushare 文档目录页面中抽取接口树结构。

读取项目根目录下的 `Tushare数据.htm`，解析左侧 jstree，
输出 CSV：`docs/tushare_doc_tree.csv`，列包括：

- category_path: 例如 "股票数据/行情数据/A股日线行情"
- title: 节点标题（通常就是文档标题）
- doc_id: URL 中的 doc_id（字符串）
- href: 节点原始 href
"""

from pathlib import Path
import csv
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HTML_PATH = PROJECT_ROOT / "Tushare数据.htm"
OUT_CSV = PROJECT_ROOT / "docs" / "tushare_doc_tree.csv"


def parse_doc_tree(html: str):
    soup = BeautifulSoup(html, "html.parser")
    tree = soup.find("div", id="jstree")
    if tree is None:
        raise RuntimeError("未在 HTML 中找到 id=jstree 的节点")

    results = []

    def walk_li(li, parent_titles):
        a = li.find("a", class_="jstree-anchor")
        if a is None:
            return
        title = (a.get_text(strip=True) or "").replace("\xa0", " ")
        href = a.get("href") or ""
        doc_id = ""
        if "doc_id=" in href:
            # 简单解析 ?doc_id=XXX 或 &doc_id=XXX
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
        raise RuntimeError("未在 jstree 中找到根 ul")

    for li in root_ul.find_all("li", recursive=False):
        walk_li(li, [])

    return results


def main() -> None:
    if not HTML_PATH.exists():
        raise SystemExit(f"本地 HTML 不存在: {HTML_PATH}")
    html = HTML_PATH.read_text(encoding="utf-8", errors="ignore")
    records = parse_doc_tree(html)

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
