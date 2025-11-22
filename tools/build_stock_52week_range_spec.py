from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIELDS_CSV = PROJECT_ROOT / "docs" / "data_schema_fields.csv"
MAPPING_CSV = PROJECT_ROOT / "docs" / "data_schema_source_mapping.csv"


FIELD_DEFS = [
    # 规范字段名, 中文名, 数据类型, 单位, 小数位数, 默认格式, 字段说明
    ("symbol", "股票代码", "str", "无", "0", "", "股票代码（6位或带后缀）"),
    ("high_52w", "52周最高价", "float", "元", "2", "%.2f", "过去52周内的最高价"),
    ("low_52w", "52周最低价", "float", "元", "2", "%.2f", "过去52周内的最低价"),
    ("high_date", "52周最高价日期", "str", "无", "0", "YYYYMM-DD", "52周最高价对应的交易日期"),
    ("low_date", "52周最低价日期", "str", "无", "0", "YYYYMM-DD", "52周最低价对应的交易日期"),
    ("current_price", "当前价格", "float", "元", "2", "%.2f", "最近一个交易日的收盘价"),
    ("position_percent", "52周区间位置", "float", "%", "2", "%.2f", "当前价格在52周高低位区间中的百分位位置"),
    ("success", "是否成功", "bool", "无", "0", "", "是否成功获取到52周高低位数据"),
]


def build_rows() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for name, cn, dtype, unit, prec, fmt, desc in FIELD_DEFS:
        rows.append(
            {
                "数据集": "stock_52week_range",
                "规范字段名": name,
                "中文字段名": cn,
                "数据类型": dtype,
                "单位": unit,
                "小数位数": prec,
                "默认格式": fmt,
                "字段说明": desc,
            }
        )
    return rows


def write_fields(rows: List[Dict[str, str]]) -> None:
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

    existing = [r for r in existing if r.get("数据集") != "stock_52week_range"]
    existing_keys = {(r.get("数据集"), r.get("规范字段名")) for r in existing}
    new_rows = [r for r in rows if (r["数据集"], r["规范字段名"]) not in existing_keys]

    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in existing:
            w.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            w.writerow(r)

    print(f"追加 {len(new_rows)} 条 stock_52week_range 规范字段到 {FIELDS_CSV}")


def write_mapping(rows: List[Dict[str, str]]) -> None:
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
        if not (
            r.get("数据集") == "stock_52week_range"
            and r.get("来源系统") == "internal_unified_data"
        )
    ]
    existing_keys = {
        (r.get("数据集"), r.get("规范字段名"), r.get("来源系统")) for r in existing
    }

    new_rows: List[Dict[str, str]] = []
    for r in rows:
        key = (r["数据集"], r["规范字段名"], "internal_unified_data")
        if key in existing_keys:
            continue
        new_rows.append(
            {
                "数据集": r["数据集"],
                "规范字段名": r["规范字段名"],
                "来源系统": "internal_unified_data",
                "来源接口": "get_52week_high_low",
                "来源字段名": r["规范字段名"],
                "来源数据类型": r["数据类型"],
                "来源单位": r["单位"],
                "转换系数": "1.0",
                "来源字段说明": r["字段说明"],
                "备注": "UnifiedDataAccess 计算的52周高低位结果",
            }
        )

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in existing:
            w.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            w.writerow(r)

    print(f"追加 {len(new_rows)} 条 stock_52week_range 映射到 {MAPPING_CSV}")


def main() -> None:
    rows = build_rows()
    write_fields(rows)
    write_mapping(rows)


if __name__ == "__main__":
    main()
