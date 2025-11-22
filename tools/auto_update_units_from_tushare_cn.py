from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = PROJECT_ROOT / "docs"
FIELDS_CSV = DOCS_DIR / "data_schema_fields.csv"
MAPPING_CSV = DOCS_DIR / "data_schema_source_mapping.csv"
FIELDS_CACHE = DOCS_DIR / "tushare_fields_cache.json"


def load_fields_cache() -> Dict[str, Dict]:
    with FIELDS_CACHE.open("r", encoding="utf-8") as f:
        return json.load(f)


def infer_unit_from_text(text: str) -> str:
    """基于中文描述/字段名简单推断单位。

    只返回:
    - "%" 百分比
    - "千元" / "万元" / "元"
    - "万股" / "手"
    - 其他返回 "无"
    """

    if not text:
        return "无"
    t = text.replace(" ", "")  # 去掉空格方便匹配

    # 百分比类
    if "%" in t or "百分比" in t or "涨跌幅" in t or "股息率" in t:
        return "%"

    # 金额类
    if "千元" in t:
        return "千元"
    if "万元" in t or "万  元" in t:
        return "万元"
    if "元CNY" in t or "元Cny" in t or "元cny" in t:
        return "元"
    if "(元" in t or "（元" in t or "元)" in t or "元）" in t:
        return "元"

    # 股数/量
    if "万股" in t:
        return "万股"
    if "单位：手" in t or "(手" in t or "（手" in t or "手)" in t or "手）" in t:
        return "手"

    return "无"


def build_ts_field_index(cache: Dict[str, Dict]) -> Dict[Tuple[str, str], Dict]:
    """(接口名, 字段名) -> 字段元数据"""

    idx: Dict[Tuple[str, str], Dict] = {}
    for api_name, meta in cache.items():
        fields = meta.get("fields") or []
        for f in fields:
            name = f.get("name")
            if not name:
                continue
            idx[(api_name, name)] = f
    return idx


def update_units() -> None:
    cache = load_fields_cache()
    ts_index = build_ts_field_index(cache)

    # 读两张表
    with FIELDS_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        fields_reader = csv.DictReader(f)
        fields_rows: List[Dict[str, str]] = list(fields_reader)

    with MAPPING_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        map_reader = csv.DictReader(f)
        map_rows: List[Dict[str, str]] = list(map_reader)

    # 建立 (数据集, 规范字段名) -> 规范行 索引
    fields_idx: Dict[Tuple[str, str], Dict[str, str]] = {}
    for r in fields_rows:
        key = (r.get("数据集", ""), r.get("规范字段名", ""))
        fields_idx[key] = r

    updated_fields = 0
    updated_mapping = 0

    for mr in map_rows:
        dataset = mr.get("数据集", "")
        canon = mr.get("规范字段名", "")
        src_system = mr.get("来源系统", "") or ""
        src_api = mr.get("来源接口", "") or ""
        src_field = mr.get("来源字段名", "") or ""

        # 从来源系统推断接口名：约定为去掉 tushare_ 前缀，否则回退到 来源接口
        api_name = ""
        if src_system.startswith("tushare_"):
            api_name = src_system[len("tushare_") :]
        if not api_name:
            api_name = src_api or ""

        if not (api_name and src_field):
            continue

        ts_meta = ts_index.get((api_name, src_field))
        if not ts_meta:
            continue

        desc = ts_meta.get("desc") or ts_meta.get("cn") or ""
        unit = infer_unit_from_text(desc)
        if unit == "无":
            continue

        # 更新来源单位：仅在为空/无/待确认 时覆盖
        src_unit = (mr.get("来源单位") or "").strip()
        if src_unit in {"", "无", "待确认"}:
            mr["来源单位"] = unit
            updated_mapping += 1

        # 更新规范表里的单位：仅在当前为 无/空/待确认 时覆盖
        fr = fields_idx.get((dataset, canon))
        if fr is not None:
            canon_unit = (fr.get("单位") or "").strip()
            if canon_unit in {"", "无", "待确认"}:
                fr["单位"] = unit
                updated_fields += 1

    # 写回 CSV（保持字段顺序）
    if fields_rows:
        field_fieldnames = list(fields_rows[0].keys())
    else:
        field_fieldnames = [
            "数据集",
            "规范字段名",
            "中文字段名",
            "数据类型",
            "单位",
            "小数位数",
            "默认格式",
            "字段说明",
        ]

    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=field_fieldnames)
        writer.writeheader()
        for r in fields_rows:
            writer.writerow({k: r.get(k, "") for k in field_fieldnames})

    if map_rows:
        map_fieldnames = list(map_rows[0].keys())
    else:
        map_fieldnames = [
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
        writer = csv.DictWriter(f, fieldnames=map_fieldnames)
        writer.writeheader()
        for r in map_rows:
            writer.writerow({k: r.get(k, "") for k in map_fieldnames})

    print(f"updated 单位 in data_schema_fields.csv: {updated_fields}")
    print(f"updated 来源单位 in data_schema_source_mapping.csv: {updated_mapping}")


if __name__ == "__main__":
    update_units()
