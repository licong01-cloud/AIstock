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

# balancesheet 文档 doc_id = 36
BALANCESHEET_DOC_ID = 36


def build_balancesheet_rows() -> List[Dict[str, str]]:
    fields = tcache.get_interface_fields("balancesheet", doc_id=BALANCESHEET_DOC_ID, force_refresh=False)
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
        if name in {"ann_date", "end_date", "f_ann_date", "ann_dt"} or name.endswith("_date"):
            fmt = "YYYY-MM-DD"
        if desc and ("%" in desc or "百分比" in desc or "率" in cn):
            unit = "%"
            if ftype in {"float", "double"}:
                precision = "2"
                fmt = fmt or "%.2f"
        if ftype in {"float", "double"} and precision == "0":
            precision = "2"
            fmt = fmt or "%.2f"

        row: Dict[str, str] = {
            "数据集": "balancesheet_quarterly",
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


from build_income_quarterly_spec import _append_to_fields_csv, _append_to_mapping_csv  # type: ignore


def main() -> None:
    rows = build_balancesheet_rows()
    _append_to_fields_csv("balancesheet_quarterly", rows)
    _append_to_mapping_csv("balancesheet_quarterly", "tushare_balancesheet", "balancesheet", rows, "资产负债表季度")


if __name__ == "__main__":
    main()
