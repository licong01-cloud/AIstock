import datetime as dt
from typing import Any, Dict, List

from repair_minute_via_minute_api import (
    TDX_API_BASE,
    http_get,
    get_db_conn,
    to_tdx_code,
    upsert_minute,
)

# 只修复这一只股票
TARGET_CODE = "000851.SZ"
# 只修复这两天
TARGET_DATES = [
    "2025-04-30",
    "2025-12-01",
]


def fetch_minute_single_day_via_kline_history(code: str, date: dt.date) -> List[Dict[str, Any]]:
    """通过 /api/kline-history?type=minute1 拉取某一自然日的 1m 分钟线。

    这里使用 start_date=end_date=当天日期，保证仅返回当天的数据。
    返回结构与 upsert_minute 期望的字段兼容（包含 TradeTime/Open/High/Low/Close/Volume/Amount 等）。
    """

    ymd = date.strftime("%Y%m%d")
    params = {
        "code": code,
        "type": "minute1",
        "start_date": ymd,
        "end_date": ymd,
    }
    data = http_get("/api/kline-history", params=params)
    payload = data.get("data") if isinstance(data, dict) else None

    items: Any = []
    if isinstance(payload, dict):
        items = payload.get("List") or payload.get("list") or payload
        if isinstance(items, dict):
            items = items.get("List") or items.get("list") or []
    else:
        items = payload or []

    return list(items) if isinstance(items, list) else []


def main() -> int:
    try:
        dates = [dt.date.fromisoformat(d) for d in TARGET_DATES]
    except ValueError as exc:
        print(f"[ERROR] invalid TARGET_DATES: {exc}")
        return 1

    base_code = to_tdx_code(TARGET_CODE)
    if not base_code:
        print(f"[ERROR] invalid TARGET_CODE format: {TARGET_CODE}")
        return 1

    print(f"Using TDX_API_BASE={TDX_API_BASE}")
    print(f"Repairing minute data via /api/kline-history for {TARGET_CODE} ({base_code})")

    conn = get_db_conn()
    try:
        for d in dates:
            print("\n==========")
            print(f"Repairing {TARGET_CODE} via /api/kline-history for date {d.isoformat()} ...")
            try:
                bars = fetch_minute_single_day_via_kline_history(base_code, d)
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[ERROR] fetch /api/kline-history failed for {TARGET_CODE} ({base_code}) {d}: {exc}"
                )
                continue

            if not bars:
                print(
                    f"[WARN] /api/kline-history returned empty data for {TARGET_CODE} ({base_code}) {d}"
                )
                continue

            inserted, last_ts = upsert_minute(conn, TARGET_CODE, d, bars)
            if inserted > 0:
                print(
                    f"[INFO] upsert {TARGET_CODE} {d}: inserted_rows={inserted}, last_ts={last_ts}"
                )
            else:
                print(
                    f"[INFO] upsert {TARGET_CODE} {d}: no rows inserted (all bars filtered by validation)"
                )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
