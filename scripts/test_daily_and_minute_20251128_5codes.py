from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, List, Tuple

import requests

TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
TARGET_DATE = dt.date(2025, 11, 28)
TARGET_DATE_STR_8 = TARGET_DATE.strftime("%Y%m%d")
TARGET_DATE_STR_10 = TARGET_DATE.isoformat()

# 选择 5 个股票代码做抽样验证
TS_CODES: List[str] = [
    "000001.SZ",
    "000002.SZ",
    "000004.SZ",
    "000006.SZ",
    "000007.SZ",
]


def to_date(text: Any) -> str | None:
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None
    # YYYYMMDD
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    # YYYY-MM-DD
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    # ISO 带时间
    if "T" in s:
        try:
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            return None
    return None


def check_minute_for_code(ts_code: str) -> Tuple[int, List[str]]:
    code = ts_code.split(".")[0]
    url = TDX_API_BASE + "/api/minute"
    params = {"code": code, "date": TARGET_DATE_STR_8}
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    bars: List[Dict[str, Any]] = []
    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        # 按当前实现，分钟接口通常 data 是 dict，内部可能有 List
        lst = payload.get("List") or payload.get("list")
        if isinstance(lst, list):
            bars = lst
        else:
            # 兜底：如果直接就是一条或其他结构
            bars = [payload]
    elif isinstance(data, list):
        bars = data

    dates: List[str] = []
    for bar in bars:
        if not isinstance(bar, dict):
            continue
        d = (
            bar.get("date")
            or bar.get("trade_date")
            or bar.get("Time")
            or bar.get("time")
        )
        d_norm = to_date(d)
        if d_norm:
            dates.append(d_norm)
    return len(bars), sorted(set(dates))


def check_daily_for_code(ts_code: str) -> Tuple[int, List[str]]:
    code = ts_code.split(".")[0]
    url = TDX_API_BASE + "/api/kline-history"
    params = {
        "code": code,
        "type": "day",
        # 这里按照“仅传入起始日期”的场景，只给 start_date，end_date 留空，
        # 然后通过返回数据里的日期做过滤检查。
        "start_date": TARGET_DATE_STR_8,
        # 不传 end_date
    }
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    payload = data.get("data") if isinstance(data, dict) else None
    if not isinstance(payload, dict):
        return 0, []
    lst = payload.get("List") or payload.get("list") or []
    if not isinstance(lst, list):
        return 0, []

    dates: List[str] = []
    for row in lst:
        if not isinstance(row, dict):
            continue
        d = row.get("Date") or row.get("date") or row.get("Time") or row.get("time")
        d_norm = to_date(d)
        if d_norm:
            dates.append(d_norm)
    return len(lst), sorted(set(dates))


def main() -> None:
    print(f"[INFO] using TDX_API_BASE={TDX_API_BASE}")
    print(f"[INFO] target date = {TARGET_DATE_STR_10} ({TARGET_DATE_STR_8})")
    print(f"[INFO] test codes = {TS_CODES}\n")

    print("=== Minute /api/minute 检查 ===")
    for ts in TS_CODES:
        try:
            total, dates = check_minute_for_code(ts)
        except Exception as exc:  # noqa: BLE001
            print(f"[MINUTE] {ts}: ERROR {exc}")
            continue
        print(f"[MINUTE] {ts}: bars={total}, unique_dates={dates}")
        if dates and any(d != TARGET_DATE_STR_10 for d in dates):
            print(f"  [WARN] 非目标日期数据存在: {dates}")

    print("\n=== Daily /api/kline-history 检查 ===")
    for ts in TS_CODES:
        try:
            total, dates = check_daily_for_code(ts)
        except Exception as exc:  # noqa: BLE001
            print(f"[DAILY] {ts}: ERROR {exc}")
            continue
        print(f"[DAILY] {ts}: bars={total}, unique_dates={dates}")
        if dates and any(d < TARGET_DATE_STR_10 for d in dates):
            print(f"  [WARN] 存在早于 {TARGET_DATE_STR_10} 的历史数据")
        # 我们最关心的是：是否包含 11-28 当天
        if TARGET_DATE_STR_10 not in dates:
            print(f"  [WARN] 未包含目标日期 {TARGET_DATE_STR_10}")


if __name__ == "__main__":
    main()
