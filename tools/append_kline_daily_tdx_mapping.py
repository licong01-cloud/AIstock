from __future__ import annotations

import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAPPING_CSV = PROJECT_ROOT / "docs" / "data_schema_source_mapping.csv"


def main() -> None:
    with MAPPING_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

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

    # 保留原有行，避免重复写 kline_daily + tdx_kline_api
    existing_keys = {
        (r.get("数据集"), r.get("规范字段名"), r.get("来源系统")) for r in rows
    }

    # 我们只添加与 kline_daily 规范字段一一对应的基础行情字段
    # 这里暂时不给 TDX API 写具体单位/转换系数，统一留给后续脚本/人工确认
    kline_fields = [
        ("trade_date", "str"),
        ("ts_code", "str"),
        ("open", "float"),
        ("high", "float"),
        ("low", "float"),
        ("close", "float"),
        ("vol", "float"),
        ("amount", "float"),
    ]

    new_rows = []
    for name, dtype in kline_fields:
        key = ("kline_daily", name, "tdx_kline_api")
        if key in existing_keys:
            continue
        new_rows.append(
            {
                "数据集": "kline_daily",
                "规范字段名": name,
                "来源系统": "tdx_kline_api",
                "来源接口": "kline-history",
                "来源字段名": name,
                "来源数据类型": dtype,
                "来源单位": "无",  # TDX API 已在 DataSourceManager 中做过一次单位换算，这里先标记为无，后续再精细化
                "转换系数": "1.0",
                "来源字段说明": f"TDX 本地 API kline-history 字段 {name}",
                "备注": "TDX 历史K线（经 DataSourceManager 标准化后）",
            }
        )

    if new_rows:
        with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in fieldnames})
            for r in new_rows:
                w.writerow(r)

    print(f"追加 {len(new_rows)} 条 kline_daily TDX 来源映射到 {MAPPING_CSV}")


if __name__ == "__main__":
    main()
