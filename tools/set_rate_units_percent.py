from __future__ import annotations

import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = PROJECT_ROOT / "docs"
MAPPING_CSV = DOCS_DIR / "data_schema_source_mapping.csv"


def main() -> None:
    with MAPPING_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    updated = 0
    for r in rows:
        desc = (r.get("来源字段说明") or "").strip()
        # 去掉末尾标点和空格后，看是否以“率”结尾，例如 “换手率（%）” / “回报率” 等
        normalized = desc.rstrip(" 　。、，,.;；！!（）()[]【】％%")
        if not normalized:
            continue
        if normalized.endswith("率"):
            if (r.get("来源单位") or "").strip() != "%":
                r["来源单位"] = "%"
                updated += 1

    fieldnames = list(rows[0].keys()) if rows else []
    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"set 来源单位='%' for {updated} rate-like fields")


if __name__ == "__main__":
    main()
