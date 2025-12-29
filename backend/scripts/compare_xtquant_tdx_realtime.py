"""Compare realtime quotes between xtquant and TDX HTTP backend.

This script is for *verification only* and does NOT affect runtime logic.
It will:
- Sample realtime quotes for a set of instruments from both xtquant and TDX;
- Align fields (price, volume, amount, etc.);
- Print a summary of differences for manual inspection.

Usage (from project root):

    python -m backend.scripts.compare_xtquant_tdx_realtime

Requirements:
- MiniQMT + xtquant correctly installed and logged in;
- TDX HTTP backend running and accessible via TDX_HTTP_PORT or default 19080.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

import pandas as pd
import requests

from backend.data_service.xtquant_adapter import fetch_realtime_snapshot_xt


TDX_DEFAULT_PORT = os.environ.get("TDX_HTTP_PORT", "19080").strip() or "19080"
TDX_BASE_URL = f"http://localhost:{TDX_DEFAULT_PORT}"


@dataclass
class QuoteRow:
    instrument: str
    datetime: datetime | None
    close: float | None
    open: float | None
    high: float | None
    low: float | None
    volume: float | None
    amount: float | None


def _to_tdx_codes(universe: List[str]) -> List[str]:
    """Convert instruments like 000001.SZ / 600000.SH to TDX codes.

    TDX expects codes like SZ000001 / SH600000 (see error message
    "例如:SZ000001"). We derive them from the logical instrument
    strings used by the data service.
    """

    tdx_codes: List[str] = []
    for inst in universe:
        code, _, market = inst.partition(".")
        market = market.upper()
        if len(code) == 6 and market in {"SZ", "SH"}:
            tdx_codes.append(f"{market}{code}")
        else:
            # Fallback: pass through as-is; server may still handle it or
            # return a clear error.
            tdx_codes.append(inst)
    return tdx_codes


def fetch_tdx_batch_quote(universe: List[str]) -> Dict[str, QuoteRow]:
    url = f"{TDX_BASE_URL}/api/batch-quote"
    tdx_codes = _to_tdx_codes(universe)
    resp = requests.post(url, json={"codes": tdx_codes}, timeout=5)
    resp.raise_for_status()
    data = resp.json()

    # 期望 data 为列表或字典形式，具体结构取决于 server_api_extended.go 的 successResponse
    # 这里假设返回形如 {"data": [{"Code": "000001", "Exchange": "SZ", "K": {...}, "TotalHand": ...}, ...]}
    items = data.get("data") if isinstance(data, dict) else data
    result: Dict[str, QuoteRow] = {}

    if not items:
        return result

    for item in items:
        try:
            code = str(item.get("Code") or "").strip()
            exch_raw = item.get("Exchange")
            exch: str
            # Exchange 在协议里是 uint8 枚举: 0=SZ,1=SH,2=BJ
            if isinstance(exch_raw, (int, float)):
                m = {0: "SZ", 1: "SH", 2: "BJ"}
                exch = m.get(int(exch_raw), "")
            else:
                exch = str(exch_raw or "").strip().upper()
            if not code or not exch:
                continue
            instrument = f"{code}.{exch}"

            k = item.get("K") or {}
            last_price = k.get("Last")
            open_price = k.get("Open")
            high_price = k.get("High")
            low_price = k.get("Low")

            # TotalHand / Amount 字段
            volume = item.get("TotalHand")
            amount = item.get("Amount")

            # ServerTime 例如 "2024-12-24 14:55:01"，按实际格式解析
            server_time = item.get("ServerTime")
            dt: datetime | None
            if isinstance(server_time, str) and server_time:
                try:
                    dt = datetime.fromisoformat(server_time.replace("/", "-"))
                except Exception:
                    dt = None
            else:
                dt = None

            result[instrument] = QuoteRow(
                instrument=instrument,
                datetime=dt,
                close=last_price,
                open=open_price,
                high=high_price,
                low=low_price,
                volume=volume,
                amount=amount,
            )
        except Exception:
            # 单条解析错误直接跳过，由人工在原始 JSON 中排查
            continue

    return result


def compare_once(universe: List[str]) -> pd.DataFrame:
    xt_df = fetch_realtime_snapshot_xt(universe, fields=[
        "close",
        "open",
        "high",
        "low",
        "volume",
        "amount",
    ])
    tdx_map = fetch_tdx_batch_quote(universe)

    rows: List[dict] = []
    now = datetime.now()

    for inst in universe:
        xt_row = xt_df.loc[inst] if inst in xt_df.index else None
        tdx_row = tdx_map.get(inst)

        row: dict = {
            "instrument": inst,
            "ts_sample": now.isoformat(timespec="seconds"),
        }

        if xt_row is None:
            row.update({"xt_status": "missing"})
        else:
            row.update(
                {
                    "xt_status": "ok",
                    "xt_close": float(xt_row.get("close")) if "close" in xt_row else None,
                    "xt_open": float(xt_row.get("open")) if "open" in xt_row else None,
                    "xt_high": float(xt_row.get("high")) if "high" in xt_row else None,
                    "xt_low": float(xt_row.get("low")) if "low" in xt_row else None,
                    "xt_volume": float(xt_row.get("volume")) if "volume" in xt_row else None,
                    "xt_amount": float(xt_row.get("amount")) if "amount" in xt_row else None,
                }
            )

        if tdx_row is None:
            row.update({"tdx_status": "missing"})
        else:
            row.update(
                {
                    "tdx_status": "ok",
                    "tdx_close": tdx_row.close,
                    "tdx_open": tdx_row.open,
                    "tdx_high": tdx_row.high,
                    "tdx_low": tdx_row.low,
                    "tdx_volume": tdx_row.volume,
                    "tdx_amount": tdx_row.amount,
                    "tdx_time": tdx_row.datetime.isoformat(timespec="seconds") if tdx_row.datetime else None,
                }
            )

        # 简单差值（仅供人工参考，不做业务逻辑判断）
        for f in ["close", "open", "high", "low", "volume", "amount"]:
            xt_val = row.get(f"xt_{f}")
            tdx_val = row.get(f"tdx_{f}")
            if xt_val is not None and tdx_val is not None:
                try:
                    row[f"diff_{f}"] = float(xt_val) - float(tdx_val)
                except Exception:
                    row[f"diff_{f}"] = None
            else:
                row[f"diff_{f}"] = None

        rows.append(row)

    return pd.DataFrame(rows)


def main() -> None:
    # 选一小批代表性股票做对比，你可以按需调整
    universe = [
        "000001.SZ",
        "000002.SZ",
        "600000.SH",
        "600519.SH",
    ]

    # 一次性打印 /api/batch-quote 的原始返回，确认真实字段和结构
    try:
        debug_codes = _to_tdx_codes(universe)
        debug_resp = requests.post(
            f"{TDX_BASE_URL}/api/batch-quote",
            json={"codes": debug_codes},
            timeout=5,
        )
        print("\n=== TDX /api/batch-quote RAW RESPONSE ===")
        print(debug_resp.text)
    except Exception as exc:
        print("[WARN] failed to fetch raw TDX batch-quote response:", exc)

    samples: List[pd.DataFrame] = []
    for i in range(5):
        print(f"[sample {i+1}] collecting...", flush=True)
        df = compare_once(universe)
        samples.append(df)
        time.sleep(3)

    all_df = pd.concat(samples, ignore_index=True)
    print("\n=== RAW COMPARISON SAMPLES ===")
    print(all_df.to_string(index=False))

    # 汇总误差统计（仅供人工参考）
    print("\n=== SUMMARY (non-null diff counts) ===")
    for f in ["close", "open", "high", "low", "volume", "amount"]:
        col = f"diff_{f}"
        non_null = all_df[col].notna().sum()
        zero_diff = (all_df[col] == 0).sum()
        print(f"{col}: non-null={non_null}, zero-diff={zero_diff}")


if __name__ == "__main__":
    main()
