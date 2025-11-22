from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List
import sys

# 允许脚本以 "python next_app/tools/..." 方式直接运行
THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR.parent) not in sys.path:
    sys.path.insert(0, str(THIS_DIR.parent))

import tushare_docs_cache as tcache

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIELDS_CSV = PROJECT_ROOT / "docs" / "data_schema_fields.csv"
MAPPING_CSV = PROJECT_ROOT / "docs" / "data_schema_source_mapping.csv"

# major_news 文档 doc_id = 195
MAJOR_NEWS_DOC_ID = 195


def build_news_major_rows() -> List[Dict[str, str]]:
    fields = tcache.get_interface_fields("major_news", doc_id=MAJOR_NEWS_DOC_ID, force_refresh=False)
    rows: List[Dict[str, str]] = []
    for f in fields:
        name = f.get("name") or ""
        if not name:
            continue
        cn = f.get("cn") or f.get("desc") or name
        ftype = (f.get("type") or "").lower()
        unit = "无"
        precision = "0"
        fmt = ""
        # 日期/时间字段
        if name.endswith("_date") or name in {"pub_date"}:
            fmt = "YYYY-MM-DD"
        if any(k in name for k in ["time", "_tm"]):
            fmt = fmt or "YYYY-MM-DD HH:MM:SS"
        row: Dict[str, str] = {
            "数据集": "news_major",
            "规范字段名": name,
            "中文字段名": cn,
            "数据类型": ftype,
            "单位": unit,
            "小数位数": precision,
            "默认格式": fmt,
            "字段说明": f.get("desc") or cn,
        }
        rows.append(row)
    return rows


def append_to_fields_csv(rows: List[Dict[str, str]]) -> None:
    FIELDS_CSV.parent.mkdir(parents=True, exist_ok=True)
    existing: List[Dict[str, str]] = []
    if FIELDS_CSV.exists():
        with FIELDS_CSV.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            existing = list(reader)

    fieldnames = [
        "数据集",
        "规范字段名",
        "中文字段名",
        "数据类型",
        "单位",
        "小数位数",
        "默认格式",
        "字段说明",
    ]

    # 删除旧的 news_major 行
    existing = [r for r in existing if r.get("数据集") != "news_major"]
    existing_keys = {(r.get("数据集"), r.get("规范字段名")) for r in existing}
    new_rows = [r for r in rows if (r["数据集"], r["规范字段名"]) not in existing_keys]

    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in existing:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            writer.writerow(r)

    print(f"追加 {len(new_rows)} 条 news_major 规范字段到 {FIELDS_CSV}")


def append_to_mapping_csv(rows: List[Dict[str, str]]) -> None:
    MAPPING_CSV.parent.mkdir(parents=True, exist_ok=True)
    existing: List[Dict[str, str]] = []
    if MAPPING_CSV.exists():
        with MAPPING_CSV.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            existing = list(reader)

    fieldnames = [
        "数据集",
        "规范字段名",
        "来源系统",
        "来源接口",
        "来源字段名",
        "来源数据类型",
        "来源单位",
        "转换系数",
        "来源字段说明",
        "备注",
    ]

    # 删除旧的 news_major + tushare_major_news 行
    existing = [
        r
        for r in existing
        if not (r.get("数据集") == "news_major" and r.get("来源系统") == "tushare_major_news")
    ]

    existing_keys = {
        (r.get("数据集"), r.get("规范字段名"), r.get("来源系统")) for r in existing
    }

    cache = tcache.load_cache().get("major_news", {})
    src_fields = cache.get("fields", [])

    new_rows: List[Dict[str, str]] = []
    for r in rows:
        key = (r["数据集"], r["规范字段名"], "tushare_major_news")
        if key in existing_keys:
            continue
        src_desc = ""
        for sf in src_fields:
            if sf.get("name") == r["规范字段名"]:
                src_desc = sf.get("desc") or sf.get("cn") or ""
                break
        new_rows.append(
            {
                "数据集": r["数据集"],
                "规范字段名": r["规范字段名"],
                "来源系统": "tushare_major_news",
                "来源接口": "major_news",
                "来源字段名": r["规范字段名"],
                "来源数据类型": r["数据类型"],
                "来源单位": r["单位"],
                "转换系数": "1.0",
                "来源字段说明": src_desc,
                "备注": "重大新闻",
            }
        )

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in existing:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            writer.writerow(r)

    print(f"追加 {len(new_rows)} 条 news_major 映射到 {MAPPING_CSV}")


def main() -> None:
    rows = build_news_major_rows()
    append_to_fields_csv(rows)
    append_to_mapping_csv(rows)


if __name__ == "__main__":
    main()
