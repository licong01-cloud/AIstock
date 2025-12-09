from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, List, Tuple

import psycopg2
import psycopg2.extras as pgx
import requests

pgx.register_uuid()

TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

TARGET_DATE = dt.date(2025, 11, 28)
TARGET_DATE_STR_8 = TARGET_DATE.strftime("%Y%m%d")
TARGET_DATE_STR_10 = TARGET_DATE.isoformat()

# 抽样股票代码，可按需调整
TS_CODES: List[str] = [
    "000001.SZ",
    "000002.SZ",
    "000004.SZ",
    "000006.SZ",
    "000007.SZ",
]


def fetch_minute_from_go(ts_code: str) -> List[Dict[str, Any]]:
    """从 Go /api/minute 接口按指定日期拉取分钟线数据。"""
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
    return bars


def to_iso_datetime(text: Any) -> str | None:
    """将 Go 返回的时间字段标准化为本地无时区的 'YYYY-MM-DD HH:MM:SS'。

    支持几种常见格式：
    - 'YYYYMMDD'       -> 'YYYY-MM-DD 00:00:00'
    - 'YYYY-MM-DD'     -> 'YYYY-MM-DD 00:00:00'
    - 'HH:MM'/'HH:MM:SS' -> 使用 TARGET_DATE 拼接成完整日期时间
    - ISO datetime 字符串
    """

    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None

    # 纯日期：YYYYMMDD
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]} 00:00:00"

    # 纯日期：YYYY-MM-DD
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return f"{s} 00:00:00"

    # 仅时间：HH:MM 或 HH:MM:SS，拼接 TARGET_DATE
    if len(s) in {5, 8} and s[2] == ":":
        if len(s) == 5:  # HH:MM -> HH:MM:00
            s_full = f"{s}:00"
        else:
            s_full = s
        return f"{TARGET_DATE_STR_10} {s_full}"

    # 其它情况尝试按 ISO datetime 解析
    try:
        dt_obj = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt_obj.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def check_db_for_bar(conn, ts_code: str, trade_time_str: str) -> Tuple[int, List[Tuple[Any, ...]]]:
    """在 kline_minute_raw 中查找是否已存在同一 ts_code+trade_time 的记录。"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_time, ts_code, freq, open_li, high_li, low_li, close_li, volume_hand, amount_li, adjust_type, source
              FROM market.kline_minute_raw
             WHERE ts_code = %s
               AND trade_time = %s::timestamp
             ORDER BY trade_time
            """,
            (ts_code, trade_time_str),
        )
        rows = cur.fetchall() or []
    return len(rows), rows


def main() -> None:
    print(f"[INFO] using TDX_API_BASE={TDX_API_BASE}")
    print(f"[INFO] DB_CFG={DB_CFG}")
    print(f"[INFO] target date = {TARGET_DATE_STR_10} ({TARGET_DATE_STR_8})")
    print(f"[INFO] test codes = {TS_CODES}\n")

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True

        for ts_code in TS_CODES:
            print("=" * 80)
            print(f"[CODE] {ts_code} - fetching minute data from Go for {TARGET_DATE_STR_10} ...")
            try:
                bars = fetch_minute_from_go(ts_code)
            except Exception as exc:  # noqa: BLE001
                print(f"[ERROR] fetch from Go failed: {exc}")
                continue

            print(f"[INFO] Go returned {len(bars)} bars for {ts_code} on {TARGET_DATE_STR_10}")
            if not bars:
                continue

            # 打印前 10 条记录的关键字段，并检查是否已存在于 DB
            for idx, bar in enumerate(bars[:10]):
                if not isinstance(bar, dict):
                    continue
                raw_time = bar.get("Time") or bar.get("time") or bar.get("trade_time")
                trade_time_str = to_iso_datetime(raw_time)
                freq = bar.get("freq") or bar.get("Freq") or 1
                o = bar.get("Open") or bar.get("open") or bar.get("open_li")
                h = bar.get("High") or bar.get("high") or bar.get("high_li")
                l = bar.get("Low") or bar.get("low") or bar.get("low_li")
                c = bar.get("Close") or bar.get("close") or bar.get("close_li")

                print(f"  [GO #{idx+1}] time={raw_time!r} -> norm={trade_time_str!r}, freq={freq}, O={o}, H={h}, L={l}, C={c}")

                if not trade_time_str:
                    print("    [WARN] cannot normalize trade_time, skip DB check")
                    continue

                cnt, rows = check_db_for_bar(conn, ts_code, trade_time_str)
                if cnt == 0:
                    print("    [DB] no existing row with same ts_code+trade_time")
                else:
                    print(f"    [DB] FOUND {cnt} existing row(s) with same ts_code+trade_time:")
                    for r in rows:
                        print(f"      trade_time={r[0]}, ts_code={r[1]}, freq={r[2]}, open={r[3]}, high={r[4]}, low={r[5]}, close={r[6]}, volume={r[7]}, amount={r[8]}, adjust_type={r[9]}, source={r[10]}")


if __name__ == "__main__":
    main()
