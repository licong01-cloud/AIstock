from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List
import sys

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR.parent) not in sys.path:
    sys.path.insert(0, str(THIS_DIR.parent))

import tushare_docs_cache as tcache

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIELDS_CSV = PROJECT_ROOT / "docs" / "data_schema_fields.csv"
MAPPING_CSV = PROJECT_ROOT / "docs" / "data_schema_source_mapping.csv"


def build_market_daily_info_rows() -> List[Dict[str, str]]:
    # daily_info 文档 doc_id = 258（交易所/交易对日度信息）
    fields = tcache.get_interface_fields("daily_info", doc_id=258, force_refresh=False)
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
        if "date" in name:
            fmt = "YYYY-MM-DD"
        if ftype in {"float", "double"}:
            precision = "2"
            fmt = fmt or "%.2f"
        row: Dict[str, str] = {
            "数据集": "market_daily_info",
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

    # 删除旧的 market_daily_info 行
    existing = [r for r in existing if r.get("数据集") != "market_daily_info"]
    existing_keys = {(r.get("数据集"), r.get("规范字段名")) for r in existing}
    new_rows = [r for r in rows if (r["数据集"], r["规范字段名"]) not in existing_keys]

    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in existing:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            writer.writerow(r)

    print(f"追加 {len(new_rows)} 条 market_daily_info 规范字段到 {FIELDS_CSV}")


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

    # 删除旧的 market_daily_info + tushare_daily_info 行
    existing = [
        r
        for r in existing
        if not (
            r.get("数据集") == "market_daily_info" and r.get("来源系统") == "tushare_daily_info"
        )
    ]

    existing_keys = {
        (r.get("数据集"), r.get("规范字段名"), r.get("来源系统")) for r in existing
    }

    cache = tcache.load_cache().get("daily_info", {})
    src_fields = cache.get("fields", [])

    new_rows: List[Dict[str, str]] = []
    for r in rows:
        key = (r["数据集"], r["规范字段名"], "tushare_daily_info")
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
                "来源系统": "tushare_daily_info",
                "来源接口": "daily_info",
                "来源字段名": r["规范字段名"],
                "来源数据类型": r["数据类型"],
                "来源单位": "无",
                "转换系数": "1.0",
                "来源字段说明": src_desc,
                "备注": "市场日度信息",
            }
        )

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in existing:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            writer.writerow(r)

    print(f"追加 {len(new_rows)} 条 market_daily_info 映射到 {MAPPING_CSV}")


def main() -> None:
    rows = build_market_daily_info_rows()
    append_to_fields_csv(rows)
    append_to_mapping_csv(rows)


if __name__ == "__main__":
    main()
