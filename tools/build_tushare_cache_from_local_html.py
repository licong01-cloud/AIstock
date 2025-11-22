from __future__ import annotations

from pathlib import Path

from bs4 import BeautifulSoup

from tushare_docs_cache import load_cache, save_cache

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _parse_output_fields(html: str) -> list[dict]:
    """从 Tushare 文档 HTML 中解析【输出参数】表格。

    只解析紧跟在包含“输出参数”文字的 <p><strong> 后面的那一张表。
    """
    soup = BeautifulSoup(html, "html.parser")
    # 找到包含“输出参数”的 strong 或 p 元素
    marker = None
    for strong in soup.find_all("strong"):
        if "输出参数" in strong.get_text(strip=True):
            marker = strong
            break
    if marker is None:
        return []
    # strong 所在的 p 之后的第一个 table
    p = marker.find_parent("p") or marker
    table = None
    node = p
    # 向后寻找第一个 table
    while node is not None:
        node = node.find_next_sibling()
        if node is None:
            break
        if node.name == "table":
            table = node
            break
    if table is None:
        return []

    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    lower_headers = [h.lower() for h in headers]

    # 名称列索引
    name_idx = next(
        (i for i, h in enumerate(headers) if "名称" in h or "字段" in h or "name" in h.lower()),
        None,
    )
    type_idx = next(
        (i for i, h in enumerate(headers) if "类型" in h or "type" in h.lower()),
        None,
    )
    desc_idx = next(
        (i for i, h in enumerate(headers) if "描述" in h or "说明" in h or "desc" in h.lower()),
        None,
    )
    if name_idx is None:
        return []

    fields: list[dict] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) <= name_idx:
            continue
        name = tds[name_idx].get_text(strip=True)
        if not name:
            continue
        field: dict = {"name": name}
        if desc_idx is not None and len(tds) > desc_idx:
            field["cn"] = tds[desc_idx].get_text(strip=True)
        if type_idx is not None and len(tds) > type_idx:
            field["type"] = tds[type_idx].get_text(strip=True)
        fields.append(field)
    return fields


def main() -> None:
    # 本地保存的 Tushare 文档 HTML 文件名（位于项目根目录）
    # daily: Tushare数据.htm
    # pro_bar: Tushare数据pro_bar.htm
    daily_html_path = PROJECT_ROOT / "Tushare数据.htm"
    pro_bar_html_path = PROJECT_ROOT / "Tushare数据pro_bar.htm"

    if not daily_html_path.exists():
        raise FileNotFoundError(f"daily 文档 HTML 未找到: {daily_html_path}")
    if not pro_bar_html_path.exists():
        raise FileNotFoundError(f"pro_bar 文档 HTML 未找到: {pro_bar_html_path}")

    daily_html = daily_html_path.read_text(encoding="utf-8", errors="ignore")
    pro_bar_html = pro_bar_html_path.read_text(encoding="utf-8", errors="ignore")

    daily_fields = _parse_output_fields(daily_html)
    pro_bar_fields = _parse_output_fields(pro_bar_html)

    cache = load_cache()
    cache["daily"] = {"doc_id": 27, "fields": daily_fields}
    cache["pro_bar"] = {"doc_id": 109, "fields": pro_bar_fields}
    save_cache(cache)

    print(f"解析 daily 字段数: {len(daily_fields)}")
    print(f"解析 pro_bar 字段数: {len(pro_bar_fields)}")


if __name__ == "__main__":
    main()
