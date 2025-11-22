from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = PROJECT_ROOT / "docs"
MAPPING_CSV = DOCS_DIR / "data_schema_source_mapping.csv"
FIELDS_CACHE = DOCS_DIR / "tushare_fields_cache.json"


def load_fields_cache() -> Dict[str, Dict]:
    with FIELDS_CACHE.open("r", encoding="utf-8") as f:
        return json.load(f)


def infer_unit_from_text(text: str, field_name: str = "") -> str:
    """与 auto_update_units_from_tushare_cn.py 中逻辑保持一致的简化版本，
    并对 daily_basic 的常见描述做一些额外兼容。
    """
    if not text:
        return "无"
    t = text.replace(" ", "")

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

    # 针对 daily_basic 自由流通股本 （万） 这种写法的特例：按万股处理
    if "股本" in t and "（万）" in t:
        return "万股"

    return "无"


def build_ts_field_index(cache: Dict[str, Dict]) -> Dict[Tuple[str, str], Dict]:
    idx: Dict[Tuple[str, str], Dict] = {}
    for api_name, meta in cache.items():
        fields = meta.get("fields") or []
        for f in fields:
            name = f.get("name")
            if not name:
                continue
            idx[(api_name, name)] = f
    return idx


def main() -> None:
    cache = load_fields_cache()
    ts_index = build_ts_field_index(cache)

    with MAPPING_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows: List[Dict[str, str]] = list(csv.DictReader(f))

    updated_remark = 0
    updated_src_desc = 0
    updated_unit = 0

    for r in rows:
        dataset = (r.get("数据集") or "").strip()
        if dataset != "daily_basic":
            continue

        src_system = (r.get("来源系统") or "").strip()
        src_api = (r.get("来源接口") or "").strip()
        src_field = (r.get("来源字段名") or "").strip()

        # 推断 Tushare 接口名：优先从 来源系统 去掉前缀 tushare_
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

        desc = (ts_meta.get("desc") or ts_meta.get("cn") or "").strip()
        if desc:
            # 无条件用 Tushare 输出参数描述覆盖来源字段说明
            r["来源字段说明"] = desc
            updated_src_desc += 1

            # 备注：这里也直接用 desc 覆盖，daily_basic 原先多为通用的“每日指标”
            r["备注"] = desc
            updated_remark += 1

            # 基于描述推断来源单位：只要推断出的单位不是“无”，就直接覆盖来源单位
            unit = infer_unit_from_text(desc, src_field)
            if unit != "无":
                r["来源单位"] = unit
                updated_unit += 1

    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = []

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})

    print(
        "daily_basic: updated 来源字段说明 = {}, 备注 = {}, 来源单位 = {}".format(
            updated_src_desc, updated_remark, updated_unit
        )
    )


if __name__ == "__main__":
    main()
