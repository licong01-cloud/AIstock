from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIELDS_CSV = PROJECT_ROOT / "docs" / "data_schema_fields.csv"
MAPPING_CSV = PROJECT_ROOT / "docs" / "data_schema_source_mapping.csv"


DB_DATASETS = [
    ("kline_daily_raw", "market.kline_daily_raw", "原始日线（厘/手）"),
    ("kline_daily_qfq", "market.kline_daily_qfq", "前复权日线（厘/手）"),
    ("kline_daily_hfq", "market.kline_daily_hfq", "后复权日线（厘/手）"),
]


def build_db_rows(dataset: str, comment: str) -> List[Dict[str, str]]:
    # 统一按照 rebuild_adjusted_daily.py 中读写的列定义
    cols = [
        ("trade_date", "交易日期", "str", "无", "0", "YYYY-MM-DD"),
        ("ts_code", "TS代码", "str", "无", "0", ""),
        ("open_li", "开盘价（厘）", "float", "厘", "0", ""),
        ("high_li", "最高价（厘）", "float", "厘", "0", ""),
        ("low_li", "最低价（厘）", "float", "厘", "0", ""),
        ("close_li", "收盘价（厘）", "float", "厘", "0", ""),
        ("volume_hand", "成交量（手）", "float", "手", "0", ""),
        ("amount_li", "成交额（厘）", "float", "厘", "0", ""),
        ("source", "来源标识", "str", "无", "0", ""),
    ]
    rows: List[Dict[str, str]] = []
    for name, cn, dtype, unit, prec, fmt in cols:
        rows.append(
            {
                "数据集": dataset,
                "规范字段名": name,
                "中文字段名": cn,
                "数据类型": dtype,
                "单位": unit,
                "小数位数": prec,
                "默认格式": fmt,
                "字段说明": cn,
            }
        )
    return rows


def write_fields(all_rows: List[Dict[str, str]]) -> None:
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

    target_datasets = {ds for ds, _, _ in DB_DATASETS}
    existing = [r for r in existing if r.get("数据集") not in target_datasets]
    existing_keys = {(r.get("数据集"), r.get("规范字段名")) for r in existing}
    new_rows = [r for r in all_rows if (r["数据集"], r["规范字段名"]) not in existing_keys]

    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in existing:
            w.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            w.writerow(r)

    print(f"追加 {len(new_rows)} 条 DB kline_daily* 规范字段到 {FIELDS_CSV}")


def write_mapping() -> None:
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

    target_datasets = {ds for ds, _, _ in DB_DATASETS}
    # 删除旧的这些 DB 数据集映射
    existing = [r for r in existing if r.get("数据集") not in target_datasets]
    existing_keys = {
        (r.get("数据集"), r.get("规范字段名"), r.get("来源系统")) for r in existing
    }

    new_rows: List[Dict[str, str]] = []
    for ds, table, comment in DB_DATASETS:
        for name, dtype in [
            ("trade_date", "str"),
            ("ts_code", "str"),
            ("open_li", "float"),
            ("high_li", "float"),
            ("low_li", "float"),
            ("close_li", "float"),
            ("volume_hand", "float"),
            ("amount_li", "float"),
            ("source", "str"),
        ]:
            key = (ds, name, "db_aistock")
            if key in existing_keys:
                continue
            new_rows.append(
                {
                    "数据集": ds,
                    "规范字段名": name,
                    "来源系统": "db_aistock",
                    "来源接口": table,
                    "来源字段名": name,
                    "来源数据类型": dtype,
                    "来源单位": "无",  # 这里记录的就是表中原始存储单位，已在 fields 中体现
                    "转换系数": "1.0",
                    "来源字段说明": f"{table} 字段 {name}",
                    "备注": comment,
                }
            )

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in existing:
            w.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            w.writerow(r)

    print(f"追加 {len(new_rows)} 条 DB kline_daily* 映射到 {MAPPING_CSV}")


def main() -> None:
    all_rows: List[Dict[str, str]] = []
    for ds, _, comment in DB_DATASETS:
        all_rows.extend(build_db_rows(ds, comment))
    write_fields(all_rows)
    write_mapping()


if __name__ == "__main__":
    main()
