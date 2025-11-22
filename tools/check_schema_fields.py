from __future__ import annotations

"""检查 data_schema_fields.csv 的命名和单位规范。

用法：

    python next_app/tools/check_schema_fields.py

检查内容：
- 同名规范字段在不同数据集中的单位/类型是否一致。
- 周期语义与命名后缀是否匹配（简单基于关键词）。
- 主力/北向等语义是否使用了过于模糊的字段名（如 volume/amount）。
- 常用全局字段（ts_code/trade_date/pct_chg）的单位和类型是否一致。
"""

from pathlib import Path
import csv
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_FILE = PROJECT_ROOT / "docs" / "data_schema_fields.csv"

# 预设的全局字段（字段名: (期望数据类型, 期望单位)）
GLOBAL_FIELDS = {
    "ts_code": ("str", "无"),
    "trade_date": ("str", "无"),
    "pct_chg": ("float", "%"),
}

# 简单的周期关键词和后缀映射（仅用于提示，不是强校验）
PERIOD_KEYWORDS = {
    "分钟": ["_m1", "_m5", "_m15", "_m30", "_m60"],
    "1分钟": ["_m1"],
    "5分钟": ["_m5"],
    "15分钟": ["_m15"],
    "30分钟": ["_m30"],
    "60分钟": ["_m60", "_h1"],
    "小时": ["_h1", "_h4"],
    "日": ["_d", "_daily"],
    "周": ["_w", "_weekly"],
    "月": ["_M", "_mon", "_monthly"],
    "季度": ["_q"],
    "年": ["_y"],
    "实时": ["_rt", "_intraday"],
}

MAIN_FORCE_KEYWORDS = ["主力", "北向", "融资", "融券"]
BASIC_NAMES = {"volume", "vol", "amount"}


def load_schema(path: Path):
    rows = []
    if not path.exists():
        print(f"schema file not found: {path}")
        return rows
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def check_unit_consistency(rows):
    """同一规范字段名在不同数据集中的单位是否一致。"""
    by_field = defaultdict(list)
    for r in rows:
        key = r.get("规范字段名", "").strip()
        if not key:
            continue
        by_field[key].append(r)

    issues = []
    for field, items in by_field.items():
        units = {i.get("单位", "").strip() for i in items}
        dtypes = {i.get("数据类型", "").strip() for i in items}
        if len(units) > 1:
            issues.append(f"[UNIT] 字段 {field} 在不同数据集中单位不一致: {units}")
        if len(dtypes) > 1:
            issues.append(f"[DTYPE] 字段 {field} 在不同数据集中数据类型不一致: {dtypes}")
    return issues


def check_period_suffix(rows):
    """周期语义和字段名后缀的简单检查。"""
    issues = []
    for r in rows:
        name = r.get("规范字段名", "").strip()
        # 维度字段（例如 trade_date、ts_code 等）不做周期后缀检查
        if name in {"trade_date", "ts_code"}:
            continue
        dtype = (r.get("数据类型", "") or "").strip().lower()
        # 只对数值度量字段做周期检查
        if dtype not in {"float", "int"}:
            continue
        desc = r.get("字段说明", "") or ""
        for kw, suffixes in PERIOD_KEYWORDS.items():
            if kw in desc and not any(name.endswith(suf) for suf in suffixes):
                issues.append(
                    f"[PERIOD] 字段 {name} 的说明包含 '{kw}'，但字段名未带周期后缀 {suffixes}"
                )
    return issues


def check_main_force_naming(rows):
    """主力/北向等关键字是否使用了模糊字段名。"""
    issues = []
    for r in rows:
        name = r.get("规范字段名", "").strip()
        desc = r.get("字段说明", "") or ""
        if any(kw in desc for kw in MAIN_FORCE_KEYWORDS) and name in BASIC_NAMES:
            issues.append(
                f"[MAIN] 字段 {name} 的说明包含主力/北向/融资等语义，建议改为更具体的名字（如 main_volume/main_inflow_d 等）"
            )
    return issues



def check_global_fields(rows):
    """检查预设全局字段的单位和类型。"""
    issues = []
    by_field = defaultdict(list)
    for r in rows:
        key = r.get("规范字段名", "").strip()
        by_field[key].append(r)

    for field, (dtype, unit) in GLOBAL_FIELDS.items():
        if field not in by_field:
            continue
        for r in by_field[field]:
            ds = r.get("数据集", "")
            cur_dtype = r.get("数据类型", "").strip()
            cur_unit = r.get("单位", "").strip()
            if cur_dtype and cur_dtype != dtype:
                issues.append(
                    f"[GLOBAL] 数据集 {ds} 字段 {field} 的数据类型为 {cur_dtype}，与预期 {dtype} 不一致"
                )
            if cur_unit and cur_unit != unit:
                issues.append(
                    f"[GLOBAL] 数据集 {ds} 字段 {field} 的单位为 {cur_unit}，与预期 {unit} 不一致"
                )
    return issues


def main() -> None:
    rows = load_schema(SCHEMA_FILE)
    if not rows:
        return

    issues = []
    issues += check_unit_consistency(rows)
    issues += check_period_suffix(rows)
    issues += check_main_force_naming(rows)
    issues += check_global_fields(rows)

    if not issues:
        print("schema check passed: no issues found")
        return

    print("schema check found issues:")
    for msg in issues:
        print("-", msg)


if __name__ == "__main__":
    main()
