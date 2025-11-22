from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIELDS_CSV = PROJECT_ROOT / "docs" / "data_schema_fields.csv"
MAPPING_CSV = PROJECT_ROOT / "docs" / "data_schema_source_mapping.csv"


# 根据 fund_flow_akshare.py 中 format_fund_flow_for_ai 使用到的列，
# 以及 Tushare 分支中构造的列，定义 Akshare 个股资金流向数据集字段。
FIELD_DEFS = [
    # (规范字段名, 中文名, 数据类型, 单位, 小数位数, 默认格式, 字段说明)
    ("日期", "交易日期", "str", "无", "0", "YYYY-MM-DD", "交易日期"),
    ("收盘价", "收盘价", "float", "元", "2", "%.2f", "当日收盘价"),
    ("涨跌幅", "涨跌幅", "float", "%", "2", "%.2f", "当日相对前一交易日涨跌幅"),
    ("主力净流入-净额", "主力净流入-净额", "float", "元", "2", "%.2f", "大单+超大单合计净流入金额"),
    ("主力净流入-净占比", "主力净流入-净占比", "float", "%", "2", "%.2f", "主力净流入金额占成交额比例"),
    ("超大单净流入-净额", "超大单净流入-净额", "float", "元", "2", "%.2f", "超大单净流入金额"),
    ("超大单净流入-净占比", "超大单净流入-净占比", "float", "%", "2", "%.2f", "超大单净流入金额占成交额比例"),
    ("大单净流入-净额", "大单净流入-净额", "float", "元", "2", "%.2f", "大单净流入金额"),
    ("大单净流入-净占比", "大单净流入-净占比", "float", "%", "2", "%.2f", "大单净流入金额占成交额比例"),
    ("中单净流入-净额", "中单净流入-净额", "float", "元", "2", "%.2f", "中单净流入金额"),
    ("中单净流入-净占比", "中单净流入-净占比", "float", "%", "2", "%.2f", "中单净流入金额占成交额比例"),
    ("小单净流入-净额", "小单净流入-净额", "float", "元", "2", "%.2f", "小单净流入金额"),
    ("小单净流入-净占比", "小单净流入-净占比", "float", "%", "2", "%.2f", "小单净流入金额占成交额比例"),
]


def build_moneyflow_stock_ak_rows() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for name, cn, dtype, unit, prec, fmt, desc in FIELD_DEFS:
        rows.append(
            {
                "数据集": "moneyflow_stock_akshare",
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

    # 删除旧的 moneyflow_stock_akshare 行
    existing = [r for r in existing if r.get("数据集") != "moneyflow_stock_akshare"]
    existing_keys = {(r.get("数据集"), r.get("规范字段名")) for r in existing}
    new_rows = [r for r in rows if (r["数据集"], r["规范字段名"]) not in existing_keys]

    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in existing:
            w.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            w.writerow(r)

    print(f"追加 {len(new_rows)} 条 moneyflow_stock_akshare 规范字段到 {FIELDS_CSV}")


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

    # 删除旧的 moneyflow_stock_akshare + akshare_stock_individual_fund_flow 行
    existing = [
        r
        for r in existing
        if not (
            r.get("数据集") == "moneyflow_stock_akshare"
            and r.get("来源系统") == "akshare_stock_individual_fund_flow"
        )
    ]

    existing_keys = {
        (r.get("数据集"), r.get("规范字段名"), r.get("来源系统")) for r in existing
    }

    new_rows: List[Dict[str, str]] = []
    for r in rows:
        key = (r["数据集"], r["规范字段名"], "akshare_stock_individual_fund_flow")
        if key in existing_keys:
            continue
        new_rows.append(
            {
                "数据集": r["数据集"],
                "规范字段名": r["规范字段名"],
                "来源系统": "akshare_stock_individual_fund_flow",
                "来源接口": "stock_individual_fund_flow",
                "来源字段名": r["规范字段名"],
                "来源数据类型": r["数据类型"],
                "来源单位": r["单位"],
                "转换系数": "1.0",
                "来源字段说明": r["字段说明"],
                "备注": "Akshare 个股资金流向（与 moneyflow_stock 语义对应）",
            }
        )

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in existing:
            w.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            w.writerow(r)

    print(f"追加 {len(new_rows)} 条 moneyflow_stock_akshare 映射到 {MAPPING_CSV}")


def main() -> None:
    rows = build_moneyflow_stock_ak_rows()
    write_fields(rows)
    write_mapping(rows)


if __name__ == "__main__":
    main()
