from __future__ import annotations

"""基于 tushare_atomic_api.py 抽取的元数据，在线抓取股票行情相关接口的字段信息。

输入：docs/tushare_apis_from_atomic.csv
输出：更新 docs/tushare_fields_cache.json（使用 tushare_docs_cache 工具）

筛选逻辑（行情相关）：
- description 或 api_name 中包含以下任一关键词：
  - "行情", "K线", "k线", "分钟", "实时", "实时日线", "分时",
  - "auction"（集合竞价）、"factor"（技术因子）、"limit_list_d"（涨跌停统计）、
  - "daily_info"（市场每日统计）、"stk_mins", "rt_k", "rt_min" 等。

你可以根据需要后续调整关键词或直接用白名单。
"""

from pathlib import Path
import csv
from typing import List, Dict

import sys

# 允许脚本以 "python next_app/tools/..." 方式直接运行
THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR.parent) not in sys.path:
    sys.path.insert(0, str(THIS_DIR.parent))

import tushare_docs_cache as tcache

PROJECT_ROOT = Path(__file__).resolve().parents[2]
META_CSV = PROJECT_ROOT / "docs" / "tushare_apis_from_atomic.csv"


KEYWORDS = [
    "行情",
    "K线",
    "k线",
    "分钟",
    "实时",
    "分时",
    "auction",
    "factor",
    "limit_list_d",
    "daily_info",
    "stk_mins",
    "rt_k",
    "rt_min",
]


def load_meta() -> List[Dict[str, str]]:
    if not META_CSV.exists():
        raise SystemExit(f"meta csv not found: {META_CSV}")
    with META_CSV.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def is_market_api(row: Dict[str, str]) -> bool:
    name = (row.get("api_name") or "").lower()
    desc = (row.get("description") or "")
    text = name + " " + desc
    return any(kw.lower() in text for kw in KEYWORDS)


def main() -> None:
    rows = load_meta()
    market_rows = [r for r in rows if is_market_api(r)]
    print(f"total apis: {len(rows)}, market-related: {len(market_rows)}")

    for r in market_rows:
        name = r.get("api_name") or ""
        doc_id_str = (r.get("doc_id") or "").strip()
        if not name or not doc_id_str:
            print(f"skip {name or '[no name]'}: missing doc_id")
            continue
        try:
            doc_id = int(doc_id_str)
        except ValueError:
            print(f"skip {name}: invalid doc_id={doc_id_str}")
            continue
        try:
            # 强制刷新，以便在调整解析规则后更新缓存
            fields = tcache.get_interface_fields(name, doc_id=doc_id, force_refresh=True)
        except Exception as e:  # noqa: BLE001
            print(f"get_interface_fields failed for {name} (doc_id={doc_id}): {e}")
            continue
        print(f"cached {len(fields)} fields for {name} (doc_id={doc_id})")


if __name__ == "__main__":
    main()
