import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

from repair_minute_via_minute_api import (
    TDX_API_BASE,
    get_db_conn,
    to_tdx_code,
    fetch_minute_single_day,
    upsert_minute,
)

# 需要精确重试修复的日期（可以按需修改/扩展）
TARGET_DATES: List[str] = [
    "2025-04-30",
    "2025-12-01",
]

# 需要精确重试修复的 ts_code 列表（基于前一轮脚本日志中超时或失败的股票）
# 可以按需增删，例如:
#   000851.SZ, 603388.SH
TARGET_CODES: List[str] = [
    "000851.SZ",
    "603388.SH",
]


def main() -> int:
    try:
        dates = [dt.date.fromisoformat(d) for d in TARGET_DATES]
    except ValueError as exc:
        print(f"[ERROR] invalid TARGET_DATES: {exc}")
        return 1

    if not TARGET_CODES:
        print("[ERROR] TARGET_CODES is empty, nothing to repair.")
        return 1

    print(f"Using TDX_API_BASE={TDX_API_BASE}")
    print(f"Target ts_code count={len(TARGET_CODES)}: {', '.join(TARGET_CODES)}")

    conn = get_db_conn()
    try:
        for d in dates:
            print("\n==========")
            print(f"Retry repairing minute data via /api/minute for date {d.isoformat()} ...")
            total_inserted = 0
            codes_with_data = 0

            for ts_code in TARGET_CODES:
                base_code = to_tdx_code(ts_code)
                if not base_code:
                    print(f"[WARN] invalid ts_code format, skip: {ts_code}")
                    continue
                try:
                    bars = fetch_minute_single_day(base_code, d)
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"[WARN] fetch /api/minute failed for {ts_code} ({base_code}) {d}: {exc}"
                    )
                    continue
                if not bars:
                    print(
                        f"[WARN] /api/minute returned empty data for {ts_code} ({base_code}) {d}"
                    )
                    continue

                inserted, last_ts = upsert_minute(conn, ts_code, d, bars)
                if inserted > 0:
                    codes_with_data += 1
                    total_inserted += inserted
                    print(
                        f"[INFO] upsert {ts_code} {d}: inserted_rows={inserted}, last_ts={last_ts}"
                    )
                else:
                    print(f"[INFO] upsert {ts_code} {d}: no rows inserted (bars filtered)")

            print(
                f"Date {d.isoformat()}: codes_with_data={codes_with_data}, total_inserted_rows={total_inserted}"
            )
            if codes_with_data == 0:
                print("=> /api/minute 在该日期未返回任何可写入的数据（针对重试股票）。")
            else:
                avg = total_inserted / codes_with_data
                print(
                    f"=> 平均每支有数据的重试股票写入约 {avg:.1f} 条分钟线（通过 upsert）。"
                )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
