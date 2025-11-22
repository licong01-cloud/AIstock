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

# income 文档 doc_id = 33
INCOME_DOC_ID = 33


def build_income_rows() -> List[Dict[str, str]]:
    fields = tcache.get_interface_fields("income", doc_id=INCOME_DOC_ID, force_refresh=False)
    rows: List[Dict[str, str]] = []
    for f in fields:
        name = f.get("name") or ""
        if not name:
            continue
        cn = f.get("cn") or f.get("desc") or name
        desc = f.get("desc") or cn
        ftype = (f.get("type") or "").lower()
        unit = "无"
        precision = "0"
        fmt = ""
        # 日期字段
        if name in {"ann_date", "end_date", "f_ann_date", "ann_dt"} or name.endswith("_date"):
            fmt = "YYYY-MM-DD"
        # 百分比类
        if desc and ("%" in desc or "百分比" in desc or "率" in cn):
            unit = "%"
            if ftype in {"float", "double"}:
                precision = "2"
                fmt = fmt or "%.2f"
        # 通用浮点
        if ftype in {"float", "double"} and precision == "0":
            precision = "2"
            fmt = fmt or "%.2f"

        row: Dict[str, str] = {
            "数据集": "income_quarterly",
            "规范字段名": name,
            "中文字段名": cn,
            "数据类型": ftype,
            "单位": unit,
            "小数位数": precision,
            "默认格式": fmt,
            "字段说明": desc,
        }
        rows.append(row)
    return rows


def _append_to_fields_csv(dataset: str, rows: List[Dict[str, str]]) -> None:
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

    existing = [r for r in existing if r.get("数据集") != dataset]
    existing_keys = {(r.get("数据集"), r.get("规范字段名")) for r in existing}
    new_rows = [r for r in rows if (r["数据集"], r["规范字段名"]) not in existing_keys]

    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in existing:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            writer.writerow(r)

    print(f"追加 {len(new_rows)} 条 {dataset} 规范字段到 {FIELDS_CSV}")


def _append_to_mapping_csv(dataset: str, source_system: str, api_name: str, rows: List[Dict[str, str]], remark: str) -> None:
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

    existing = [
        r
        for r in existing
        if not (r.get("数据集") == dataset and r.get("来源系统") == source_system)
    ]

    existing_keys = {
        (r.get("数据集"), r.get("规范字段名"), r.get("来源系统")) for r in existing
    }

    cache = tcache.load_cache().get(api_name, {})
    src_fields = cache.get("fields", [])

    new_rows: List[Dict[str, str]] = []
    for r in rows:
        key = (r["数据集"], r["规范字段名"], source_system)
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
                "来源系统": source_system,
                "来源接口": api_name,
                "来源字段名": r["规范字段名"],
                "来源数据类型": r["数据类型"],
                "来源单位": r["单位"],
                "转换系数": "1.0",
                "来源字段说明": src_desc,
                "备注": remark,
            }
        )

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in existing:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"追加 {len(new_rows)} 条 {dataset} 映射到 {MAPPING_CSV}")


def main() -> None:
    rows = build_income_rows()
    _append_to_fields_csv("income_quarterly", rows)
    _append_to_mapping_csv("income_quarterly", "tushare_income", "income", rows, "利润表季度")


if __name__ == "__main__":
    main()
