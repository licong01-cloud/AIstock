import os
import datetime as dt
from typing import Any, Dict, List

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
    application_name="AIstock-test-tdx-kline-history-minute",
)

TARGET_DATES = ["2025-04-30", "2025-12-01"]
MAX_CODES = 50  # 抽样股票数量，避免对 TDX 造成太大压力


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


def get_sample_codes(limit: int) -> List[str]:
    """从 symbol_dim 中取一批 ts_code 作为样本。"""

    pgx.register_uuid()
    conn = psycopg2.connect(**DB_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code
                  FROM market.symbol_dim
                 WHERE ts_code IS NOT NULL
                 ORDER BY ts_code
                 LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def to_tdx_code(ts_code: str) -> str | None:
    ts_code = (ts_code or "").strip().upper()
    if not ts_code or "." not in ts_code:
        return None
    code, _suffix = ts_code.split(".", 1)
    if len(code) != 6 or not code.isdigit():
        return None
    return code


def fetch_history_minute_for_range(code: str, start: dt.date, end: dt.date) -> List[Dict[str, Any]]:
    """调用 /api/kline-history 获取单支股票在 [start, end] 的 minute1 数据。"""

    if start > end:
        return []
    params = {
        "code": code,
        "type": "minute1",
        "start_date": start.strftime("%Y%m%d"),
        "end_date": end.strftime("%Y%m%d"),
        "limit": 800,
    }
    data = http_get("/api/kline-history", params=params)
    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        items = payload.get("List") or payload.get("list") or []
    else:
        items = payload or []
    return list(items) if isinstance(items, list) else []


def main() -> int:
    try:
        dates = [dt.date.fromisoformat(d) for d in TARGET_DATES]
    except ValueError as exc:
        print(f"[ERROR] invalid TARGET_DATES: {exc}")
        return 1

    codes = get_sample_codes(MAX_CODES)
    if not codes:
        print("[ERROR] no sample codes from market.symbol_dim")
        return 1

    print(f"Using TDX_API_BASE={TDX_API_BASE}")
    print(f"Sample codes (ts_code) count={len(codes)} (limit={MAX_CODES})")
    print("First 5 sample codes:", ", ".join(codes[:5]))

    for d in dates:
        total_rows = 0
        codes_with_data = 0
        print("\n==========")
        print(f"Testing TDX /api/kline-history type=minute1 for date {d.isoformat()} ...")
        for ts in codes:
            code = to_tdx_code(ts)
            if not code:
                continue
            try:
                bars = fetch_history_minute_for_range(code, d, d)
            except Exception as exc:  # noqa: BLE001
                print(f"[WARN] code {ts} -> {code} date {d} fetch error: {exc}")
                continue
            n = len(bars)
            if n > 0:
                codes_with_data += 1
                total_rows += n
        print(f"Date {d.isoformat()}: codes_with_data={codes_with_data}, total_rows={total_rows}")
        if codes_with_data == 0:
            print("=> 在抽样的代码中，这一天 /api/kline-history(type=minute1) 未返回任何分钟数据。")
        else:
            avg = total_rows / codes_with_data
            print(f"=> 抽样平均每支股票约 {avg:.1f} 条分钟线（来自 /api/kline-history）。")

    print("\n说明：")
    print("- 本脚本直接调用 TDX /api/kline-history?type=minute1，检查指定日期在一批样本股票上的分钟线可用性。")
    print("- 与 test_tdx_minute_for_dates.py 使用的 /api/minute 单日接口相对应，可对比两者返回的一致性。")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
