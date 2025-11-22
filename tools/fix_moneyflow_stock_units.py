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

    updated_fields = 0
    for r in rows:
        if r.get("数据集") != "moneyflow_stock":
            continue
        name = r.get("规范字段名", "")
        if name.endswith("_amount") or name == "net_mf_amount":
            if r.get("单位") != "元":
                r["单位"] = "元"
                updated_fields += 1
        elif name.endswith("_vol"):
            if r.get("单位") != "手":
                r["单位"] = "手"
                updated_fields += 1

    fieldnames = list(rows[0].keys()) if rows else []
    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    # 更新 data_schema_source_mapping.csv
    with MAPPING_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        mrows = list(csv.DictReader(f))

    updated_src = 0
    for r in mrows:
        if r.get("数据集") != "moneyflow_stock":
            continue
        name = r.get("规范字段名", "")
        if name.endswith("_amount") or name == "net_mf_amount":
            if r.get("来源单位") != "元":
                r["来源单位"] = "元"
                updated_src += 1
        elif name.endswith("_vol"):
            if r.get("来源单位") != "手":
                r["来源单位"] = "手"
                updated_src += 1

    mfieldnames = list(mrows[0].keys()) if mrows else []
    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=mfieldnames)
        w.writeheader()
        for r in mrows:
            w.writerow({k: r.get(k, "") for k in mfieldnames})

    print(f"moneyflow_stock 规范单位更新 {updated_fields} 条，来源单位更新 {updated_src} 条")


if __name__ == "__main__":
    update_units()
