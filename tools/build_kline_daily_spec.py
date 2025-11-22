from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_PATH = PROJECT_ROOT / "docs" / "tushare_fields_cache.json"
OUTPUT_PATH = PROJECT_ROOT / "docs" / "data_schema_kline_daily_auto.csv"


def load_tushare_cache() -> Dict[str, Any]:
    if not CACHE_PATH.exists():
        raise FileNotFoundError(f"Tushare cache not found: {CACHE_PATH}")
    with CACHE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def guess_unit_and_precision(name: str, cn: str, ftype: str) -> tuple[str, int, str]:
    """Simple heuristic to guess unit, precision and default format.

    This is only a first draft; you can adjust later in Excel.
    """
    lname = name.lower()
    lcn = cn.lower()

    # 默认
    unit = "无"
    precision = 0
    fmt = ""

    if ftype in {"float", "double", "number"}:  # 数值型
        # 成交额类：统一用“万”，2 位小数
        if any(k in lname for k in ["amount", "money", "net_amount", "main_amount"]):
            unit = "万"
            precision = 2
            fmt = "%.2f"
        # 百分比：统一“%”，2 位小数
        elif any(k in lname for k in ["pct", "ratio"]) or "%" in lcn:
            unit = "%"
            precision = 2
            fmt = "%.2f"
        # 成交量：统一“手”，整数
        elif "vol" in lname or "volume" in lname:
            unit = "手"
            precision = 0
            fmt = "%.0f"
        # 价格相关：统一“元”，3 位小数
        elif any(k in lname for k in ["open", "high", "low", "close", "price", "pre_close", "change"]):
            unit = "元"
            precision = 3
            fmt = "%.3f"
        else:
            # 其他浮点，先给一个通用默认
            unit = "无"
            precision = 3
            fmt = "%.3f"
    elif ftype in {"int", "int64", "integer"}:
        unit = "无"
        precision = 0
        fmt = "%.0f"
    elif "date" in lname or "date" in lcn:
        unit = "无"
        precision = 0
        fmt = "YYYY-MM-DD"
    return unit, precision, fmt


def build_kline_daily_rows(cache: Dict[str, Any]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    # 我们只从 daily / pro_bar 两个接口抽字段，作为 kline_daily 的初稿
    daily = cache.get("daily", {})
    pro_bar = cache.get("pro_bar", {})
    daily_fields = {f["name"]: f for f in daily.get("fields", [])}
    pro_fields = {f["name"]: f for f in pro_bar.get("fields", [])}

    # 取字段名并集
    all_names = sorted(set(daily_fields.keys()) | set(pro_fields.keys()))

    for name in all_names:
        d = daily_fields.get(name)
        p = pro_fields.get(name)
        # 优先用 daily 的中文与类型，其次 pro_bar
        src = d or p or {}
        cn = src.get("cn") or src.get("name") or name
        ftype = src.get("type", "")
        unit, precision, fmt = guess_unit_and_precision(name, cn, ftype)

        row: Dict[str, str] = {
            "数据集": "kline_daily",
            "规范字段名": name,
            "中文字段名": cn,
            "数据类型": ftype or "",
            "单位": unit,
            "小数位数": str(precision),
            "默认格式": fmt,
            "字段说明": cn,  # 初稿直接用中文名，后续你可细化
            "TDX字段名": "",  # 暂留空，后续根据需要补
            "Tushare_DAILY字段名": name if name in daily_fields else "",
            "Tushare_PRO_BAR字段名": name if name in pro_fields else "",
            "本地DB字段名": "",
            "其他数据源字段名": "",
            "备注": "",
        }
        rows.append(row)

    return rows


def write_csv(rows: List[Dict[str, str]]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "数据集",
        "规范字段名",
        "中文字段名",
        "数据类型",
        "单位",
        "小数位数",
        "默认格式",
        "字段说明",
        "TDX字段名",
        "Tushare_DAILY字段名",
        "Tushare_PRO_BAR字段名",
        "本地DB字段名",
        "其他数据源字段名",
        "备注",
    ]
    # 使用 utf-8-sig 写入，避免 Excel 打开时中文乱码
    with OUTPUT_PATH.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    cache = load_tushare_cache()
    rows = build_kline_daily_rows(cache)
    write_csv(rows)
    print(f"写入 {len(rows)} 条 kline_daily 字段到 {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
