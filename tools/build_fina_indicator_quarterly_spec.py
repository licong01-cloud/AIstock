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

# fina_indicator 文档 doc_id = 79
FINA_INDICATOR_DOC_ID = 79


def build_fina_indicator_rows() -> List[Dict[str, str]]:
    fields = tcache.get_interface_fields(
        "fina_indicator", doc_id=FINA_INDICATOR_DOC_ID, force_refresh=False
    )
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
        # 时间与日期
        if name in {"ann_date", "end_date", "report_date"} or name.endswith("_date"):
            fmt = "YYYY-MM-DD"
        # 百分比类：根据中文描述中的 % 或 百分比
        desc = f.get("desc") or cn
        if desc and ("%" in desc or "百分比" in desc or "率" in cn):
            unit = "%"
            if ftype in {"float", "double"}:
                precision = "2"
                fmt = fmt or "%.2f"
        # 其它浮点型默认 2~3 位小数
        if ftype in {"float", "double"} and precision == "0":
            precision = "2"
            fmt = fmt or "%.2f"

        row: Dict[str, str] = {
            "数据集": "fina_indicator_quarterly",
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

    # 删除旧的 fina_indicator_quarterly 行
    existing = [r for r in existing if r.get("数据集") != "fina_indicator_quarterly"]
    existing_keys = {(r.get("数据集"), r.get("规范字段名")) for r in existing}
    new_rows = [r for r in rows if (r["数据集"], r["规范字段名"]) not in existing_keys]

    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in existing:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            writer.writerow(r)

    print(f"追加 {len(new_rows)} 条 fina_indicator_quarterly 规范字段到 {FIELDS_CSV}")


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

    # 删除旧的 fina_indicator_quarterly + tushare_fina_indicator 行
    existing = [
        r
        for r in existing
        if not (
            r.get("数据集") == "fina_indicator_quarterly"
            and r.get("来源系统") == "tushare_fina_indicator"
        )
    ]

    existing_keys = {
        (r.get("数据集"), r.get("规范字段名"), r.get("来源系统")) for r in existing
    }

    cache = tcache.load_cache().get("fina_indicator", {})
    src_fields = cache.get("fields", [])

    new_rows: List[Dict[str, str]] = []
    for r in rows:
        key = (r["数据集"], r["规范字段名"], "tushare_fina_indicator")
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
                "来源系统": "tushare_fina_indicator",
                "来源接口": "fina_indicator",
                "来源字段名": r["规范字段名"],
                "来源数据类型": r["数据类型"],
                "来源单位": r["单位"],
                "转换系数": "1.0",
                "来源字段说明": src_desc,
                "备注": "财务指标季度",
            }
        )

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in existing:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            writer.writerow(r)

    print(f"追加 {len(new_rows)} 条 fina_indicator_quarterly 映射到 {MAPPING_CSV}")


def main() -> None:
    rows = build_fina_indicator_rows()
    append_to_fields_csv(rows)
    append_to_mapping_csv(rows)


if __name__ == "__main__":
    main()
