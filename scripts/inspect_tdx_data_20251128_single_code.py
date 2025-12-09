from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, List, Tuple

import requests

TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
TARGET_DATE = dt.date(2025, 11, 28)
TARGET_DATE_STR_8 = TARGET_DATE.strftime("%Y%m%d")
TARGET_DATE_STR_10 = TARGET_DATE.isoformat()

# 单只股票代码，可通过环境变量覆盖
TS_CODE = os.getenv("TEST_TS_CODE", "000001.SZ")


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


def fetch_minute_for_code(ts_code: str) -> Tuple[int, List[str]]:
    code = ts_code.split(".")[0]
    url = TDX_API_BASE + "/api/minute"
    params = {"code": code, "date": TARGET_DATE_STR_8}
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    bars: List[Dict[str, Any]] = []
    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        lst = payload.get("List") or payload.get("list")
        if isinstance(lst, list):
            bars = lst
        else:
            bars = [payload]
    elif isinstance(data, list):
        bars = data

    dates: List[str] = []
    for bar in bars:
        if not isinstance(bar, dict):
            continue
        d = bar.get("date") or bar.get("trade_date") or bar.get("Time") or bar.get("time")
        d_norm = to_date(d)
        if d_norm:
            dates.append(d_norm)
    return len(bars), sorted(set(dates))


def fetch_daily_for_code(ts_code: str) -> Tuple[int, List[str]]:
    code = ts_code.split(".")[0]
    url = TDX_API_BASE + "/api/kline-history"
    params = {
        "code": code,
        "type": "day",
        "start_date": TARGET_DATE_STR_8,
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
    print(f"[INFO] test code = {TS_CODE}\n")

    print("=== Minute /api/minute 检查 ===")
    try:
        total_minute, minute_dates = fetch_minute_for_code(TS_CODE)
    except Exception as exc:  # noqa: BLE001
        print(f"[MINUTE] {TS_CODE}: ERROR {exc}")
    else:
        print(f"[MINUTE] {TS_CODE}: bars={total_minute}, unique_dates={minute_dates}")
        if minute_dates and any(d != TARGET_DATE_STR_10 for d in minute_dates):
            print(f"  [WARN] 分钟线返回包含非目标日期数据: {minute_dates}")

    print("\n=== Daily /api/kline-history 未复权日线 检查 ===")
    try:
        total_daily, daily_dates = fetch_daily_for_code(TS_CODE)
    except Exception as exc:  # noqa: BLE001
        print(f"[DAILY] {TS_CODE}: ERROR {exc}")
    else:
        print(f"[DAILY] {TS_CODE}: bars={total_daily}, unique_dates={daily_dates}")
        if daily_dates and any(d < TARGET_DATE_STR_10 for d in daily_dates):
            print(f"  [WARN] 日线返回包含早于 {TARGET_DATE_STR_10} 的数据: {daily_dates}")
        if TARGET_DATE_STR_10 not in daily_dates:
            print(f"  [WARN] 日线数据未包含目标日期 {TARGET_DATE_STR_10}")


if __name__ == "__main__":
    main()
