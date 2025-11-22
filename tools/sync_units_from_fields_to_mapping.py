from __future__ import annotations

import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = PROJECT_ROOT / "docs"
FIELDS_CSV = DOCS_DIR / "data_schema_fields.csv"
MAPPING_CSV = DOCS_DIR / "data_schema_source_mapping.csv"


def sync_units(strict: bool = True) -> None:
    """根据规范表中的单位，同步更新映射表中的来源单位。

    strict=True:  只要规范表有单位（非空且非"无"），就直接覆盖映射表的来源单位。
    strict=False: 只在来源单位为空/无/待确认时才填充，不覆盖已有值。
    """

    # 1. 读取规范表：构建 (数据集, 规范字段名) -> 单位
    with FIELDS_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        fields_rows = list(csv.DictReader(f))

    unit_index = {}
    for r in fields_rows:
        dataset = r.get("数据集", "").strip()
        name = r.get("规范字段名", "").strip()
        unit = (r.get("单位") or "").strip()
        if not dataset or not name:
            continue
        if unit and unit != "无":  # 只对有意义的单位做索引
            unit_index[(dataset, name)] = unit

    # 2. 读取映射表并同步来源单位
    with MAPPING_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        map_rows = list(csv.DictReader(f))

    updated = 0
    for r in map_rows:
        dataset = r.get("数据集", "").strip()
        name = r.get("规范字段名", "").strip()
        key = (dataset, name)
        if key not in unit_index:
            continue
        canon_unit = unit_index[key]
        if not canon_unit:
            continue

        src_unit = (r.get("来源单位") or "").strip()
        if strict:
            # 规范表为主，直接覆盖
            if src_unit != canon_unit:
                r["来源单位"] = canon_unit
                updated += 1
        else:
            # 保守模式：仅在来源单位为空/无/待确认时覆盖
            if src_unit in {"", "无", "待确认"}:
                r["来源单位"] = canon_unit
                updated += 1

    # 3. 写回映射表
    if map_rows:
        fieldnames = list(map_rows[0].keys())
    else:
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

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in map_rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"synced 来源单位 from fields to mapping: {updated} rows (strict={strict})")


if __name__ == "__main__":
    # 默认采用严格模式：规范表中的单位是唯一标准
    sync_units(strict=True)
