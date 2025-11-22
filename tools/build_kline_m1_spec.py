from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_PATH = PROJECT_ROOT / "docs" / "tushare_fields_cache.json"
FIELDS_CSV = PROJECT_ROOT / "docs" / "data_schema_fields.csv"
MAPPING_CSV = PROJECT_ROOT / "docs" / "data_schema_source_mapping.csv"


def load_tushare_cache() -> Dict[str, Any]:
    if not CACHE_PATH.exists():
        raise FileNotFoundError(f"Tushare cache not found: {CACHE_PATH}")
    with CACHE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def guess_unit_and_precision(name: str, cn: str, ftype: str) -> tuple[str, int, str]:
    """与 kline_daily 规则保持一致的简单推断。"""
    lname = name.lower()
    lcn = cn.lower()

    unit = "无"
    precision = 0
    fmt = ""

    if ftype in {"float", "double", "number"}:
        if any(k in lname for k in ["amount", "money", "net_amount", "main_amount"]):
            unit = "万"
            precision = 2
            fmt = "%.2f"
        elif any(k in lname for k in ["pct", "ratio"]) or "%" in lcn:
            unit = "%"
            precision = 2
            fmt = "%.2f"
        elif "vol" in lname or "volume" in lname:
            unit = "手"
            precision = 0
            fmt = "%.0f"
        elif any(k in lname for k in ["open", "high", "low", "close", "price", "pre_close", "change"]):
            unit = "元"
            precision = 3
            fmt = "%.3f"
        else:
            unit = "无"
            precision = 3
            fmt = "%.3f"
    elif ftype in {"int", "int64", "integer"}:
        unit = "无"
        precision = 0
        fmt = "%.0f"
    elif "date" in lname or "time" in lname or "日期" in lcn or "时间" in lcn:
        unit = "无"
        precision = 0
        fmt = "YYYY-MM-DD HH:MM:SS"
    return unit, precision, fmt


def build_kline_m1_rows(cache: Dict[str, Any]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    stk_mins = cache.get("stk_mins", {})
    rt_min = cache.get("rt_min", {})
    rt_min_daily = cache.get("rt_min_daily", {})

    fields_stk = {f["name"]: f for f in stk_mins.get("fields", [])}
    fields_rt = {f["name"]: f for f in rt_min.get("fields", [])}
    fields_rt_daily = {f["name"]: f for f in rt_min_daily.get("fields", [])}

    all_names = sorted(set(fields_stk) | set(fields_rt) | set(fields_rt_daily))

    for name in all_names:
        src = fields_stk.get(name) or fields_rt.get(name) or fields_rt_daily.get(name) or {}
        cn = src.get("cn") or src.get("name") or name
        ftype = src.get("type", "")
        unit, precision, fmt = guess_unit_and_precision(name, cn, ftype)

        canon_name = name
        # 日线数据集中使用 amount 表示“日成交额”；
        # 分钟线中的成交额语义不同（分钟成交额），规范名改为 amount_m，
        # 具体是 1/5/15/30/60 分钟由 freq 字段决定。
        if name == "amount":
            canon_name = "amount_m"
            cn = "分钟成交额"

        # 字段说明优先使用 Tushare 原始描述，其次使用中文名
        desc = src.get("desc") or cn
        if canon_name == "amount_m":
            desc = "分钟成交额，配合 freq 字段决定 1/5/15/30/60 分钟，规范单位：万"

        row: Dict[str, str] = {
            "数据集": "kline_m1",
            "规范字段名": canon_name,
            "中文字段名": cn,
            "数据类型": ftype or "",
            "单位": unit,
            "小数位数": str(precision),
            "默认格式": fmt,
            "字段说明": desc,
        }
        rows.append(row)

    return rows


def append_to_fields_csv(rows: List[Dict[str, str]]) -> None:
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

    # 先剔除旧的 kline_m1 行，避免历史残留（如 amount_m1）
    existing = [r for r in existing if r.get("数据集") != "kline_m1"]
    # 再避免重复 (数据集, 规范字段名)
    existing_keys = {(r.get("数据集"), r.get("规范字段名")) for r in existing}
    new_rows = [r for r in rows if (r["数据集"], r["规范字段名"]) not in existing_keys]

    with FIELDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in existing:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            writer.writerow(r)

    print(f"追加 {len(new_rows)} 条 kline_m1 规范字段到 {FIELDS_CSV}")


def append_to_mapping_csv(rows: List[Dict[str, str]]) -> None:
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

    # 剔除旧的 kline_m1+tushare_stk_mins 行
    existing = [
        r
        for r in existing
        if not (r.get("数据集") == "kline_m1" and r.get("来源系统") == "tushare_stk_mins")
    ]

    existing_keys = {
        (r.get("数据集"), r.get("规范字段名"), r.get("来源系统")) for r in existing
    }

    new_rows: List[Dict[str, str]] = []
    for r in rows:
        key = (r["数据集"], r["规范字段名"], "tushare_stk_mins")
        if key in existing_keys:
            continue
        # 查找对应字段的原始 desc 作为来源字段说明
        src_desc = ""
        for src in [
            *load_tushare_cache().get("stk_mins", {}).get("fields", []),
        ]:
            if src.get("name") == ("amount" if r["规范字段名"] == "amount_m" else r["规范字段名"]):
                src_desc = src.get("desc") or src.get("cn") or ""
                break

        new_rows.append(
            {
                "数据集": r["数据集"],
                "规范字段名": r["规范字段名"],
                "来源系统": "tushare_stk_mins",
                "来源接口": "stk_mins",
                # 对于 amount_m，来源字段名仍然是 Tushare 的 "amount"
                "来源字段名": "amount" if r["规范字段名"] == "amount_m" else r["规范字段名"],
                "来源数据类型": r["数据类型"],
                "来源单位": "待确认",
                "转换系数": "1.0",
                "来源字段说明": src_desc,
                "备注": "分钟线单位待人工确认",
            }
        )

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in existing:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        for r in new_rows:
            writer.writerow(r)

    print(f"追加 {len(new_rows)} 条 kline_m1 映射到 {MAPPING_CSV}")


def main() -> None:
    cache = load_tushare_cache()
    rows = build_kline_m1_rows(cache)
    append_to_fields_csv(rows)
    append_to_mapping_csv(rows)


if __name__ == "__main__":
    main()
