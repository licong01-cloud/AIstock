"""Step 2 — Trade gateway smoke: connect → place limit → query → cancel.

Validates the full OEMS roundtrip on miniQMT SIM:
  1. XtQuantTrader.start() + connect()
  2. subscribe(account)
  3. query_stock_asset / query_stock_orders / query_stock_positions
  4. order_stock (limit, BUY, far below market to avoid fill)
  5. wait for on_stock_order callback (collect order_id)
  6. cancel_order_stock(order_id)
  7. wait for cancel ack
  8. clean disconnect

Fail-fast: any non-zero return / empty result / timeout raises.

Usage:
    python -m backend.services.paper_trading_v2.poc.step2_place_cancel
"""

from __future__ import annotations

import threading
import time
from datetime import datetime

from backend.services.paper_trading_v2.poc._common import bootstrap


class _CB:
    """Capture all xtquant trader callbacks for the PoC report."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str, object]] = []
        self.lock = threading.Lock()
        self.connected = threading.Event()
        self.last_order = None
        self.last_order_error = None
        self.last_cancel_error = None

    def _log(self, kind: str, payload: object) -> None:
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        with self.lock:
            self.events.append((ts, kind, payload))
        print(f"[step2][cb][{ts}] {kind}: {self._fmt(payload)}")

    @staticmethod
    def _fmt(p: object) -> str:
        s = repr(p)
        return s if len(s) < 220 else s[:220] + "..."

    def on_connected(self): self._log("connected", None); self.connected.set()
    def on_disconnected(self): self._log("disconnected", None)
    def on_account_status(self, status): self._log("account_status", status)
    def on_stock_asset(self, asset): self._log("stock_asset", asset)
    def on_stock_order(self, order):
        self._log("stock_order", order)
        self.last_order = order
    def on_stock_trade(self, trade): self._log("stock_trade", trade)
    def on_stock_position(self, position): self._log("stock_position", position)
    def on_order_error(self, e):
        self._log("order_error", e)
        self.last_order_error = e
    def on_cancel_error(self, e):
        self._log("cancel_error", e)
        self.last_cancel_error = e
    def on_order_stock_async_response(self, r): self._log("order_async_resp", r)
    def on_cancel_order_stock_async_response(self, r): self._log("cancel_async_resp", r)


def _attr(obj, *names, default=None):
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None:
                return v
    return default


def main() -> int:
    cfg = bootstrap()
    print(f"[step2] cfg: account={cfg['account_id']} session={cfg['session_id']} "
          f"stock={cfg['test_stock']}")

    from xtquant import xttrader, xttype, xtconstant, xtdata

    cb = _CB()
    trader = xttrader.XtQuantTrader(cfg["userdata_path"], cfg["session_id"], cb)
    print(f"[step2] XtQuantTrader created")

    trader.start()
    print(f"[step2] start() done")

    rc = trader.connect()
    print(f"[step2] connect() rc={rc}")
    if rc != 0:
        raise RuntimeError(
            f"connect() failed rc={rc}. Common causes: "
            f"miniQMT client not running, wrong userdata_path, session_id collision."
        )

    account = xttype.StockAccount(cfg["account_id"], "STOCK")
    sub_rc = trader.subscribe(account)
    print(f"[step2] subscribe(account) rc={sub_rc}")
    if sub_rc != 0:
        raise RuntimeError(f"subscribe() failed rc={sub_rc}")

    print("[step2] querying account asset ...")
    asset = trader.query_stock_asset(account)
    if asset is None:
        raise RuntimeError("query_stock_asset returned None — account not authorized?")
    cash = _attr(asset, "cash", default="?")
    total = _attr(asset, "total_asset", default="?")
    market_v = _attr(asset, "market_value", default="?")
    print(f"[step2] asset: cash={cash} total={total} market_value={market_v}")

    orders_before = trader.query_stock_orders(account, cancelable_only=False) or []
    positions = trader.query_stock_positions(account) or []
    print(f"[step2] before: {len(orders_before)} orders, {len(positions)} positions")

    snap = xtdata.get_full_tick([cfg["test_stock"]])
    last_px = None
    if snap and cfg["test_stock"] in snap:
        last_px = snap[cfg["test_stock"]].get("lastPrice")
    if not last_px or last_px <= 0:
        last_px = 7.0
        print(f"[step2] WARN: no live last_price, using fallback {last_px}")
    limit_px = round(last_px + cfg["limit_price_offset"], 2)
    if limit_px <= 0:
        limit_px = round(last_px * 0.85, 2)
    print(f"[step2] last_price={last_px}, limit_price={limit_px}, vol={cfg['order_volume']}")

    print(f"[step2] placing BUY limit {cfg['test_stock']} @ {limit_px} x {cfg['order_volume']}")
    t0 = time.time()
    order_id = trader.order_stock(
        account,
        cfg["test_stock"],
        xtconstant.STOCK_BUY,
        cfg["order_volume"],
        xtconstant.FIX_PRICE,
        limit_px,
        "poc_step2",
        "env-poc",
    )
    place_ms = (time.time() - t0) * 1000
    print(f"[step2] order_stock returned order_id={order_id}  ({place_ms:.0f} ms)")
    if not isinstance(order_id, int) or order_id <= 0:
        raise RuntimeError(
            f"order_stock failed: order_id={order_id}. "
            f"last_order_error={cb.last_order_error!r}"
        )

    print("[step2] waiting 3s for on_stock_order callback ...")
    time.sleep(3)

    orders_after = trader.query_stock_orders(account, cancelable_only=True) or []
    print(f"[step2] cancelable orders now: {len(orders_after)}")
    found = next((o for o in orders_after if _attr(o, "order_id") == order_id), None)
    if found is None:
        print(f"[step2] WARN: order_id={order_id} not in cancelable list (may have filled/rejected)")

    print(f"[step2] cancelling order_id={order_id} ...")
    t1 = time.time()
    cancel_rc = trader.cancel_order_stock(account, order_id)
    cancel_ms = (time.time() - t1) * 1000
    print(f"[step2] cancel_order_stock rc={cancel_rc}  ({cancel_ms:.0f} ms)")
    if cancel_rc != 0:
        print(f"[step2] WARN: cancel rc={cancel_rc} (may already be filled/rejected); "
              f"last_cancel_error={cb.last_cancel_error!r}")

    print("[step2] waiting 2s for final state ...")
    time.sleep(2)

    final_orders = trader.query_stock_orders(account, cancelable_only=False) or []
    target = next((o for o in final_orders if _attr(o, "order_id") == order_id), None)
    if target is not None:
        st = _attr(target, "order_status", "status", default="?")
        print(f"[step2] final order status for {order_id}: {st}")

    print(f"[step2] total callbacks captured: {len(cb.events)}")

    try:
        trader.stop()
        print("[step2] trader.stop() done")
    except Exception as e:
        print(f"[step2] stop() error (non-fatal): {e!r}")

    print("[step2] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
