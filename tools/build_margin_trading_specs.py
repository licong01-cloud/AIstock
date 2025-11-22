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


def _build_rows_from_interface(dataset: str, interface: str, doc_id: int) -> List[Dict[str, str]]:
    fields = tcache.get_interface_fields(interface, doc_id=doc_id, force_refresh=False)
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
        # 简单规则：日期字段
        if "date" in name:
            fmt = "YYYY-MM-DD"
        # 浮点字段默认两位小数
        if ftype in {"float", "double"}:
            precision = "2"
            fmt = fmt or "%.2f"
        row: Dict[str, str] = {
            "数据集": dataset,
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


def build_margin_datasets() -> Dict[str, List[Dict[str, str]]]:
    # 汇总：使用 Tushare margin 接口（doc_id=58）
    margin_summary_rows = _build_rows_from_interface(
        dataset="margin_trading_summary",
        interface="margin",
        doc_id=58,
    )
    # 明细：使用 Tushare margin_detail 接口（doc_id=59）
    margin_detail_rows = _build_rows_from_interface(
        dataset="margin_trading_detail",
        interface="margin_detail",
        doc_id=59,
    )
    return {
        "margin_trading_summary": margin_summary_rows,
        "margin_trading_detail": margin_detail_rows,
    }


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

    target_datasets = {"margin_trading_summary", "margin_trading_detail"}
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

    print(f"追加 {len(new_rows)} 条 margin_* 规范字段到 {FIELDS_CSV}")


def write_mapping(datasets: Dict[str, List[Dict[str, str]]]) -> None:
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

    target_datasets = {"margin_trading_summary", "margin_trading_detail"}
    existing = [r for r in existing if r.get("数据集") not in target_datasets]
    existing_keys = {
        (r.get("数据集"), r.get("规范字段名"), r.get("来源系统")) for r in existing
    }

    cache = tcache.load_cache()
    new_rows: List[Dict[str, str]] = []

    for dataset, rows in datasets.items():
        if dataset == "margin_trading_summary":
            interface = "margin"
            source_system = "tushare_margin"
            remark = "融资融券汇总（Tushare margin）"
        else:
            interface = "margin_detail"
            source_system = "tushare_margin_detail"
            remark = "融资融券明细（Tushare margin_detail）"
        src_fields = cache.get(interface, {}).get("fields", [])

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
                    "来源接口": interface,
                    "来源字段名": r["规范字段名"],
                    "来源数据类型": r["数据类型"],
                    "来源单位": r["单位"],
                    "转换系数": "1.0",
                    "来源字段说明": src_desc,
                    "备注": remark,
                }
            )

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in existing:
            w.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            w.writerow(r)

    print(f"追加 {len(new_rows)} 条 margin_* 映射到 {MAPPING_CSV}")


def main() -> None:
    datasets = build_margin_datasets()
    all_rows: List[Dict[str, str]] = []
    for rows in datasets.values():
        all_rows.extend(rows)
    write_fields(all_rows)
    write_mapping(datasets)


if __name__ == "__main__":
    main()
