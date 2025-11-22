from __future__ import annotations

import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = PROJECT_ROOT / "docs"
MAPPING_CSV = DOCS_DIR / "data_schema_source_mapping.csv"


def main() -> None:
    with MAPPING_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    missing = []
    for r in rows:
        unit = (r.get("来源单位") or "").strip()
        if unit in {"", "无", "待确认"}:
            missing.append(
                (
                    r.get("数据集", ""),
                    r.get("规范字段名", ""),
                    r.get("来源接口", ""),
                    r.get("来源字段名", ""),
                    r.get("来源字段说明", ""),
                )
            )

    print(f"missing unit rows: {len(missing)}")
    for ds, name, api, src_name, desc in missing:
        print(f"{ds},{name},api={api},src={src_name},desc={desc}")


if __name__ == "__main__":
    main()
