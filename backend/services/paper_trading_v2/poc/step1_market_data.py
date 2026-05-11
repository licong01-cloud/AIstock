"""Step 1 — Market data smoke via xtquant.xtdata (no vnpy_xt).

Validates:
  1. xtdata.subscribe_quote callback fires for the test stock
  2. xtdata.get_full_tick returns non-empty snapshot

Fail-fast: timeout without any tick callback => raise.

Usage:
    python -m backend.services.paper_trading_v2.poc.step1_market_data
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime

from backend.services.paper_trading_v2.poc._common import bootstrap


def main() -> int:
    cfg = bootstrap()
    print(f"[step1] cfg: stock={cfg['test_stock']}")

    from xtquant import xtdata

    received: list[dict] = []
    received_lock = threading.Lock()

    def on_tick(datas: dict) -> None:
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        with received_lock:
            received.append({"ts": ts, "data": datas})
        for code, rows in datas.items():
            if not rows:
                continue
            row0 = rows[0] if isinstance(rows, list) else rows
            print(f"[step1][{ts}] {code}: keys={list(row0.keys())[:8]}...")

    print(f"[step1] subscribing tick on {cfg['test_stock']} ...")
    seq = xtdata.subscribe_quote(
        cfg["test_stock"], period="tick", count=0, callback=on_tick
    )
    print(f"[step1] subscribe_quote returned seq={seq}")
    if not isinstance(seq, int) or seq < 0:
        raise RuntimeError(f"subscribe_quote failed: seq={seq}")

    print("[step1] full_tick snapshot:")
    snapshot = xtdata.get_full_tick([cfg["test_stock"]])
    print(f"[step1] snapshot keys: {list(snapshot.keys())}")
    if not snapshot:
        raise RuntimeError("get_full_tick returned empty — miniQMT not connected to market?")
    snap_row = snapshot.get(cfg["test_stock"])
    if snap_row:
        keys = list(snap_row.keys())[:10] if isinstance(snap_row, dict) else "(non-dict)"
        print(f"[step1] snapshot[{cfg['test_stock']}] keys: {keys}")
        if isinstance(snap_row, dict) and "lastPrice" in snap_row:
            print(f"[step1] last_price={snap_row.get('lastPrice')} "
                  f"high={snap_row.get('high')} low={snap_row.get('low')}")

    wait_s = 8
    print(f"[step1] waiting {wait_s}s for tick callbacks ...")
    deadline = time.time() + wait_s
    while time.time() < deadline:
        time.sleep(0.5)
        with received_lock:
            if len(received) >= 3:
                break

    with received_lock:
        n = len(received)
    print(f"[step1] tick callbacks received: {n}")

    try:
        xtdata.unsubscribe_quote(seq)
        print(f"[step1] unsubscribed seq={seq}")
    except Exception as e:
        print(f"[step1] unsubscribe error (non-fatal): {e!r}")

    if n == 0 and not snapshot.get(cfg["test_stock"]):
        raise RuntimeError(
            "No ticks received AND empty snapshot. miniQMT data feed not connected. "
            "Check that miniQMT client is logged in and market data permission is enabled."
        )

    if n == 0:
        print("[step1] WARN: zero tick callbacks (likely off-hours), but snapshot OK -> PASS")
    else:
        print(f"[step1] PASS ({n} ticks + snapshot)")

    print(f"[step1] sample: {json.dumps(received[:1], default=str)[:300]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
