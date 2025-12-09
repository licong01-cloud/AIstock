from __future__ import annotations

import datetime as dt
import os
from typing import List

import requests

TARGET_DATE_STR = "20251128"  # 2025-11-28
TS_CODES: List[str] = ["000001.SZ", "000002.SZ"]

TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
MINUTE_API_PATH = "/api/minute"  # indicator_screening_service 中使用的路径


def main() -> None:
    print(f"[INFO] using TDX_API_BASE={TDX_API_BASE}")
    print(f"[INFO] checking /api/minute for trade_date={TARGET_DATE_STR} codes={TS_CODES}")

    for ts_code in TS_CODES:
        code = ts_code.split(".")[0]
        params = {"code": code, "date": TARGET_DATE_STR}
        url = TDX_API_BASE + MINUTE_API_PATH
        print(f"\n[REQUEST] {url} params={params}")
        try:
            resp = requests.get(url, params=params, timeout=10)
        except Exception as exc:  # noqa: BLE001
            print(f"  [ERROR] request failed: {exc}")
            continue

        print("  status_code:", resp.status_code)
        if resp.status_code != 200:
            print("  body:", resp.text[:500])
            continue

        try:
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            print(f"  [ERROR] json decode failed: {exc}")
            print("  raw:", resp.text[:500])
            continue

        # 对返回结构不做强约束：
        # - 若 data 是 dict 且包含 "data" 字段：
        #     - 若 data["data"] 是 list，则直接使用；
        #     - 若 data["data"] 是 dict，则包装成单元素 list；
        # - 若顶层就是 list，则直接使用；
        # 这样可以避免当 data["data"] 为 dict 时，对其做切片导致 KeyError。
        bars = []
        if isinstance(data, dict) and "data" in data:
            payload = data.get("data")
            if isinstance(payload, list):
                bars = payload
            elif isinstance(payload, dict):
                bars = [payload]
        elif isinstance(data, list):
            bars = data

        print(f"  got {len(bars)} bars from API")
        for idx, bar in enumerate(bars[:5]):
            print("    ", idx, bar)


if __name__ == "__main__":
    main()
