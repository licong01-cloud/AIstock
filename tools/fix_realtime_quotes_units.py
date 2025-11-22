from __future__ import annotations

import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = PROJECT_ROOT / "docs"
FIELDS_CSV = DOCS_DIR / "data_schema_fields.csv"
MAPPING_CSV = DOCS_DIR / "data_schema_source_mapping.csv"


def update_units() -> None:
    # 更新 data_schema_fields.csv
    with FIELDS_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    for r in rows:
        if r.get("数据集") != "realtime_quotes":
            continue
        name = r.get("规范字段名", "")
        if name.endswith("_p"):
            r["单位"] = "元"
        elif name.endswith("_v"):
            r["单位"] = "手"

    fieldnames = list(rows[0].keys()) if rows else [
        "数据集","规范字段名","中文字段名","数据类型","单位","小数位数","默认格式","字段说明",
    ]
    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    # 更新 data_schema_source_mapping.csv
    with MAPPING_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        mrows = list(csv.DictReader(f))

    for r in mrows:
        if r.get("数据集") != "realtime_quotes":
            continue
        name = r.get("规范字段名", "")
        if name.endswith("_p"):
            r["来源单位"] = "元"
        elif name.endswith("_v"):
            r["来源单位"] = "手"

    mfieldnames = list(mrows[0].keys()) if mrows else [
        "数据集","规范字段名","来源系统","来源接口","来源字段名","来源数据类型","来源单位","转换系数","来源字段说明","备注",
    ]
    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=mfieldnames)
        w.writeheader()
        for r in mrows:
            w.writerow({k: r.get(k, "") for k in mfieldnames})

    print("realtime_quotes *_p 单位已统一为 元，*_v 单位已统一为 手")


if __name__ == "__main__":
    update_units()
