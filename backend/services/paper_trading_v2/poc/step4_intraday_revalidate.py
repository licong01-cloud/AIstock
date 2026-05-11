"""Step 3 — Intraday revalidation (Task #10).

Run during a real trading session (Mon-Fri 09:30-11:30 / 13:00-15:00 CST) to
verify the three behaviors that step2 could not confirm off-hours:

  V1. Cancel takes effect: order_status transitions 50 (REPORTED) -> 54 (CANCELED)
      after cancel_order_stock. Step2 saw rc=0 + callback but state stayed at 50.
  V2. Tick callback streams continuously: subscribe_quote('tick') delivers
      multiple callbacks within a 30-second window. Step1 saw zero (off-hours).
  V3. Partial-fill / full-fill path: place a SELL of an existing held position
      at near market_price, observe on_stock_trade callbacks and final
      order_status in {55 PART_SUCC, 56 SUCCEEDED}.

Fail-fast: any failed transition raises with the exact event log.

Usage (run during trading hours):
    python -m backend.services.paper_trading_v2.poc.step3_intraday_revalidate
"""

from __future__ import annotations

import threading
import time
from datetime import datetime

from backend.services.paper_trading_v2.poc._common import bootstrap


def _now_in_trading_hours() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    hm = now.hour * 100 + now.minute
    return 930 <= hm <= 1130 or 1300 <= hm <= 1500


class _CB:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict]] = []
        self.lock = threading.Lock()
        self.order_status_history: dict[int, list[tuple[str, int]]] = {}
        self.trades: list[dict] = []

    def _stamp(self) -> str:
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]

    def _log(self, kind: str, payload: dict) -> None:
        ts = self._stamp()
        with self.lock:
            self.events.append((ts, kind, payload))
        print(f"[step3][{ts}] {kind}: {payload}")

    def on_connected(self): self._log("connected", {})
    def on_disconnected(self): self._log("disconnected", {})

    def on_stock_order(self, order):
        oid = getattr(order, "order_id", None)
        st = getattr(order, "order_status", getattr(order, "status", None))
        with self.lock:
            self.order_status_history.setdefault(oid, []).append(
                (self._stamp(), int(st) if st is not None else -1)
            )
        self._log("stock_order", {"order_id": oid, "status": st,
                                  "code": getattr(order, "stock_code", None),
                                  "filled_qty": getattr(order, "traded_volume", None)})

    def on_stock_trade(self, trade):
        rec = {
            "order_id": getattr(trade, "order_id", None),
            "code": getattr(trade, "stock_code", None),
            "qty": getattr(trade, "traded_volume", None),
            "price": getattr(trade, "traded_price", None),
        }
        with self.lock:
            self.trades.append(rec)
        self._log("stock_trade", rec)

    def on_order_error(self, e): self._log("order_error", {"err": repr(e)})
    def on_cancel_error(self, e): self._log("cancel_error", {"err": repr(e)})
    def on_stock_asset(self, a): pass
    def on_stock_position(self, p): pass
    def on_account_status(self, s): self._log("account_status", {"status": repr(s)})
    def on_order_stock_async_response(self, r): pass
    def on_cancel_order_stock_async_response(self, r): pass


def verify_v1_cancel_to_54(trader, account, xtconstant, xtdata, cfg, cb) -> dict:
    """V1: place far-below-market BUY, cancel, expect status -> 54."""
    print("\n[step3][V1] === cancel takes effect ===")
    snap = xtdata.get_full_tick([cfg["test_stock"]])
    last_px = snap[cfg["test_stock"]]["lastPrice"]
    limit_px = round(last_px - 1.50, 2)
    if limit_px <= 0:
        limit_px = round(last_px * 0.85, 2)

    print(f"[step3][V1] BUY {cfg['test_stock']} @ {limit_px} x {cfg['order_volume']}")
    order_id = trader.order_stock(
        account, cfg["test_stock"], xtconstant.STOCK_BUY,
        cfg["order_volume"], xtconstant.FIX_PRICE, limit_px,
        "poc_v1", "intraday")
    if not isinstance(order_id, int) or order_id <= 0:
        raise RuntimeError(f"V1 order_stock failed: {order_id}")
    print(f"[step3][V1] order_id={order_id}")

    time.sleep(1.5)
    rc = trader.cancel_order_stock(account, order_id)
    print(f"[step3][V1] cancel rc={rc}")

    deadline = time.time() + 8
    final_status = None
    while time.time() < deadline:
        time.sleep(0.5)
        with cb.lock:
            hist = cb.order_status_history.get(order_id, [])
        if hist and hist[-1][1] in (54, 53):
            final_status = hist[-1][1]
            break

    with cb.lock:
        hist = list(cb.order_status_history.get(order_id, []))
    print(f"[step3][V1] status history: {hist}")

    if final_status not in (54, 53):
        raise RuntimeError(
            f"V1 FAIL: order {order_id} did not transition to 54/53. "
            f"history={hist}")
    print(f"[step3][V1] PASS final_status={final_status}")
    return {"order_id": order_id, "final_status": final_status, "history": hist}


def verify_v2_tick_streaming(xtdata, cfg) -> dict:
    """V2: tick callback fires >= 5 times in 30s."""
    print("\n[step3][V2] === tick streaming ===")
    received: list[float] = []
    lock = threading.Lock()

    def on_tick(datas):
        with lock:
            received.append(time.time())

    seq = xtdata.subscribe_quote(cfg["test_stock"], period="tick", count=0, callback=on_tick)
    print(f"[step3][V2] subscribed seq={seq}")
    if seq < 0:
        raise RuntimeError(f"V2 subscribe failed: {seq}")

    deadline = time.time() + 30
    while time.time() < deadline:
        time.sleep(1)
        with lock:
            n = len(received)
        if n >= 5:
            break

    with lock:
        n = len(received)
        first, last = (received[0], received[-1]) if received else (0, 0)
    xtdata.unsubscribe_quote(seq)
    span = last - first if n >= 2 else 0
    print(f"[step3][V2] received {n} ticks, span={span:.1f}s")
    if n < 5:
        raise RuntimeError(f"V2 FAIL: only {n} ticks in 30s (expected >=5)")
    print(f"[step3][V2] PASS")
    return {"count": n, "span_s": span}


def verify_v3_fill_path(trader, account, xtconstant, xtdata, cb) -> dict:
    """V3: SELL existing held position at <= market price; expect status 55/56."""
    print("\n[step3][V3] === fill path ===")
    positions = trader.query_stock_positions(account) or []
    sellable = [p for p in positions if (getattr(p, "can_use_volume", 0) or 0) >= 100]
    if not sellable:
        print("[step3][V3] SKIP: no sellable position with >=100 shares")
        return {"skipped": True, "reason": "no sellable position"}

    pos = sellable[0]
    code = getattr(pos, "stock_code", None)
    avail = int(getattr(pos, "can_use_volume", 0))
    sell_qty = min(100, avail)
    snap = xtdata.get_full_tick([code])
    last_px = snap[code]["lastPrice"]
    sell_px = round(last_px - 0.01, 2)
    print(f"[step3][V3] SELL {code} @ {sell_px} x {sell_qty} (avail={avail})")

    order_id = trader.order_stock(
        account, code, xtconstant.STOCK_SELL,
        sell_qty, xtconstant.FIX_PRICE, sell_px,
        "poc_v3", "intraday")
    if not isinstance(order_id, int) or order_id <= 0:
        raise RuntimeError(f"V3 order_stock failed: {order_id}")
    print(f"[step3][V3] order_id={order_id}")

    deadline = time.time() + 30
    final_status = None
    while time.time() < deadline:
        time.sleep(1)
        with cb.lock:
            hist = cb.order_status_history.get(order_id, [])
        if hist and hist[-1][1] in (55, 56):
            final_status = hist[-1][1]
            break

    with cb.lock:
        hist = list(cb.order_status_history.get(order_id, []))
        my_trades = [t for t in cb.trades if t["order_id"] == order_id]
    print(f"[step3][V3] status history: {hist}")
    print(f"[step3][V3] trades: {my_trades}")

    if final_status not in (55, 56):
        try:
            trader.cancel_order_stock(account, order_id)
        except Exception:
            pass
        raise RuntimeError(
            f"V3 FAIL: order {order_id} did not fill (no 55/56 in 30s). "
            f"history={hist}")
    print(f"[step3][V3] PASS final_status={final_status}, trades={len(my_trades)}")
    return {"order_id": order_id, "final_status": final_status,
            "trades": my_trades, "history": hist}


def main() -> int:
    if not _now_in_trading_hours():
        print(f"[step3] WARN: not in trading hours ({datetime.now()}); "
              f"V2 will likely return zero ticks and V3 will skip. "
              f"Re-run during 09:30-11:30 or 13:00-15:00 on a weekday.")

    cfg = bootstrap()
    print(f"[step3] cfg: account={cfg['account_id']} session={cfg['session_id']} "
          f"stock={cfg['test_stock']}")

    from xtquant import xttrader, xttype, xtconstant, xtdata

    cb = _CB()
    trader = xttrader.XtQuantTrader(cfg["userdata_path"], cfg["session_id"], cb)
    trader.start()
    rc = trader.connect()
    if rc != 0:
        raise RuntimeError(f"connect rc={rc}")
    if trader.subscribe(account := xttype.StockAccount(cfg["account_id"], "STOCK")) != 0:
        raise RuntimeError("subscribe failed")
    print(f"[step3] connected and subscribed")

    results = {}
    try:
        results["V1"] = verify_v1_cancel_to_54(trader, account, xtconstant, xtdata, cfg, cb)
        results["V2"] = verify_v2_tick_streaming(xtdata, cfg)
        results["V3"] = verify_v3_fill_path(trader, account, xtconstant, xtdata, cb)
    finally:
        try:
            trader.stop()
        except Exception:
            pass

    print(f"\n[step3] === SUMMARY ===")
    for k, v in results.items():
        print(f"[step3] {k}: {v}")
    print("[step3] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
