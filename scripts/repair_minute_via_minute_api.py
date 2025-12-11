import os
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx
import requests
from requests import exceptions as req_exc


TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:19080")
DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-repair-minute-via-minute-api",
)

# 需要修补的日期
TARGET_DATES = ["2025-04-30", "2025-12-01"]


def http_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = TDX_API_BASE.rstrip("/") + path
    max_retries = 3
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and data.get("code") not in (0, None):
                raise RuntimeError(f"TDX API error {path}: {data}")
            return data
        except (req_exc.ConnectionError, req_exc.Timeout) as exc:
            last_exc = exc
            if attempt >= max_retries:
                break
        except Exception:
            raise
    raise last_exc or RuntimeError(f"TDX API request failed after retries: {url}")


def get_db_conn():
    pgx.register_uuid()
    conn = psycopg2.connect(**DB_CFG)
    return conn


def get_all_codes() -> List[str]:
    """从 market.stock_basic 中取全部 ts_code 作为修补目标，仅覆盖个股。"""

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code
                  FROM market.stock_basic
                 WHERE ts_code IS NOT NULL
                 ORDER BY ts_code
                """
            )
            rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def to_tdx_code(ts_code: str) -> Optional[str]:
    ts_code = (ts_code or "").strip().upper()
    if not ts_code or "." not in ts_code:
        return None
    code, _suffix = ts_code.split(".", 1)
    if len(code) != 6 or not code.isdigit():
        return None
    return code


def fetch_minute_single_day(code: str, date: dt.date) -> List[Dict[str, Any]]:
    """通过 /api/minute 获取单支股票在某一自然日的 1m 分钟线。"""

    ymd = date.strftime("%Y%m%d")
    params = {"code": code, "type": "minute1", "date": ymd}
    data = http_get("/api/minute", params=params)
    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        items = payload.get("List") or payload.get("list") or payload
        if isinstance(items, dict):
            items = items.get("List") or items.get("list") or []
    else:
        items = payload or []
    return list(items) if isinstance(items, list) else []


def _combine_trade_time(date_hint: dt.date, value: Any) -> Optional[str]:
    """与 ingest_full_minute.py 中的实现保持一致，组合日期和时间为 ISO8601。"""

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    cleaned = text.replace("Z", "+00:00")
    try:
        dt_obj = dt.datetime.fromisoformat(cleaned)
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
        return dt_obj.isoformat()
    except ValueError:
        pass

    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            time_obj = dt.datetime.strptime(text, fmt).time()
            tzinfo = dt.timezone(dt.timedelta(hours=8))
            return dt.datetime.combine(date_hint, time_obj).replace(tzinfo=tzinfo).isoformat()
        except ValueError:
            continue
    return None


def upsert_minute(conn, ts_code: str, trade_date: dt.date, bars: List[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    """与 ingest_full_minute.py 相同的 upsert 逻辑。"""

    sql = (
        "INSERT INTO market.kline_minute_raw (trade_time, ts_code, freq, open_li, high_li, low_li, "
        "close_li, volume_hand, amount_li, adjust_type, source) "
        "VALUES %s ON CONFLICT (ts_code, trade_time, freq) DO UPDATE SET "
        "open_li=EXCLUDED.open_li, high_li=EXCLUDED.high_li, low_li=EXCLUDED.low_li, "
        "close_li=EXCLUDED.close_li, volume_hand=EXCLUDED.volume_hand, amount_li=EXCLUDED.amount_li"
    )
    values: List[Tuple[Any, ...]] = []
    last_ts: Optional[str] = None
    for row in bars:
        if not isinstance(row, dict):
            continue
        trade_time = row.get("TradeTime") or row.get("trade_time") or row.get("Time") or row.get("time")
        trade_time_iso = _combine_trade_time(trade_date, trade_time)
        open_li = row.get("Open") or row.get("open")
        high_li = row.get("High") or row.get("high")
        low_li = row.get("Low") or row.get("low")
        close_li = row.get("Close") or row.get("close") or row.get("Price") or row.get("price")
        volume_hand = row.get("Volume") or row.get("volume") or 0
        amount_li = row.get("Amount") or row.get("amount") or 0
        if trade_time_iso is None or close_li is None:
            continue
        last_ts = trade_time_iso if last_ts is None or trade_time_iso > last_ts else last_ts
        values.append((
            trade_time_iso,
            ts_code,
            "1m",
            open_li,
            high_li,
            low_li,
            close_li,
            volume_hand,
            amount_li,
            "none",
            "tdx_api",
        ))
    if not values:
        return 0, None
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values)
    if not getattr(conn, "autocommit", False):
        conn.commit()
    return len(values), last_ts


def main() -> int:
    try:
        dates = [dt.date.fromisoformat(d) for d in TARGET_DATES]
    except ValueError as exc:
        print(f"[ERROR] invalid TARGET_DATES: {exc}")
        return 1

    codes = get_all_codes()
    if not codes:
        print("[ERROR] no codes found in market.symbol_dim")
        return 1

    print(f"Using TDX_API_BASE={TDX_API_BASE}")
    print(f"Total ts_code count={len(codes)}")

    conn = get_db_conn()
    try:
        for d in dates:
            print("\n==========")
            print(f"Repairing minute data via /api/minute for date {d.isoformat()} ...")
            total_inserted = 0
            codes_with_data = 0
            for ts_code in codes:
                base_code = to_tdx_code(ts_code)
                if not base_code:
                    continue
                try:
                    bars = fetch_minute_single_day(base_code, d)
                except Exception as exc:  # noqa: BLE001
                    print(f"[WARN] fetch /api/minute failed for {ts_code} ({base_code}) {d}: {exc}")
                    continue
                if not bars:
                    continue
                inserted, last_ts = upsert_minute(conn, ts_code, d, bars)
                if inserted > 0:
                    codes_with_data += 1
                    total_inserted += inserted
            print(
                f"Date {d.isoformat()}: codes_with_data={codes_with_data}, total_inserted_rows={total_inserted}"
            )
            if codes_with_data == 0:
                print("=> /api/minute 在该日期未返回任何可写入的数据。")
            else:
                avg = total_inserted / codes_with_data
                print(f"=> 平均每支有数据的股票写入约 {avg:.1f} 条分钟线（通过 upsert）。")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
