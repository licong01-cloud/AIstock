from __future__ import annotations

import csv
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = PROJECT_ROOT / "docs"
MAPPING_CSV = DOCS_DIR / "data_schema_source_mapping.csv"


def extract_original_unit(text: str) -> str:
    """从备注或来源字段说明中抽取 '原始为X' 这样的原始单位描述。

    返回值例子: '元', '千元', '万元', '手', '股' 等；未匹配则返回空字符串。
    """

    if not text:
        return ""
    t = text.replace(" ", "")

    # 优先匹配 "原始为XXX" 模式
    m = re.search(r"原始为([\u4e00-\u9fa5A-Za-z0-9%]+)", t)
    if m:
        return m.group(1)

    # 兼容形如 "成交额（千元）" / "成交金额（元CNY）" 一类
    m2 = re.search(r"[（(]([^()（）]*?元CNY)[)）]", t)
    if m2:
        return "元"  # 元 CNY 统一记为 元

    m3 = re.search(r"[（(]([^()（）]*?千元)[)）]", t)
    if m3:
        return "千元"

    m4 = re.search(r"[（(]([^()（）]*?万元)[)）]", t)
    if m4:
        return "万元"

    m5 = re.search(r"[（(]([^()（）]*?手)[)）]", t)
    if m5:
        return "手"

    m6 = re.search(r"[（(]([^()（）]*?万股)[)）]", t)
    if m6:
        return "万股"

    return ""


def main() -> None:
    with MAPPING_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    updated = 0
    for r in rows:
        src_unit = (r.get("来源单位") or "").strip()
        remark = (r.get("备注") or "").strip()
        desc = (r.get("来源字段说明") or "").strip()

        # 备注优先，其次来源字段说明
        orig = extract_original_unit(remark) or extract_original_unit(desc)
        if not orig:
            continue

        # 只在解析出来的原始单位与当前不同的时候覆盖
        if orig != src_unit:
            r["来源单位"] = orig
            updated += 1

    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = []

    with MAPPING_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"updated 来源单位 from 备注/来源字段说明 for {updated} rows")


if __name__ == "__main__":
    main()
