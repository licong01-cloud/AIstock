"""Realtime quote subscription helpers for xtquant."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import xtquant.xtdata as xtdata  # type: ignore[import-not-found]

    XTDATA_AVAILABLE = True
except ImportError:
    xtdata = None  # type: ignore[assignment]
    XTDATA_AVAILABLE = False
    logger.warning("xtquant.xtdata is not available; realtime quote subscription is disabled")


def _load_xtdata():
    """Load xtdata lazily after callers have configured xtquant paths."""

    global xtdata, XTDATA_AVAILABLE
    if XTDATA_AVAILABLE and xtdata is not None:
        return xtdata
    try:
        import xtquant.xtdata as xtdata_mod  # type: ignore[import-not-found]
    except ImportError:
        XTDATA_AVAILABLE = False
        return None
    xtdata = xtdata_mod
    XTDATA_AVAILABLE = True
    return xtdata_mod


class RealtimeQuoteSubscriber:
    """Process-local whole-quote subscription manager."""

    def __init__(self):
        self.subscriptions: Dict[int, List[str]] = {}  # seq -> stocks
        self.callbacks: Dict[str, List[Callable]] = {}  # stock -> callbacks
        self.managed_subscriptions: Dict[str, Dict] = {}
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

    def subscribe(self, stocks: List[str], callback: Callable) -> Optional[int]:
        """Subscribe to realtime quotes and return the xtdata sequence id."""

        xtdata_mod = _load_xtdata()
        if xtdata_mod is None:
            logger.error("xtquant.xtdata is not available; cannot subscribe realtime quotes")
            return None

        try:
            seq = xtdata_mod.subscribe_whole_quote(
                code_list=stocks,
                callback=self._on_quote,
            )

            if seq > 0:
                with self._lock:
                    self.subscriptions[seq] = stocks
                    for stock in stocks:
                        if stock not in self.callbacks:
                            self.callbacks[stock] = []
                        self.callbacks[stock].append(callback)
                logger.info("subscribed realtime quotes: stocks=%s seq=%s", stocks, seq)
                return seq
            logger.error("realtime quote subscription failed: stocks=%s seq=%s", stocks, seq)
            return None

        except Exception as e:  # noqa: BLE001
            logger.error("realtime quote subscription raised: %s", e, exc_info=True)
            return None

    def ensure_subscription(
        self,
        *,
        key: str,
        stocks: List[str],
        callback: Callable,
        force: bool = False,
    ) -> Dict:
        """Ensure one managed whole-quote subscription is active.

        This path is used by MiniQMT pre-trade quote reads and is intentionally
        loud: subscribe failures are surfaced instead of silently using stale
        xtdata cache rows.
        """

        normalized = list(dict.fromkeys(str(stock or "").strip() for stock in stocks if str(stock or "").strip()))
        if not normalized:
            raise RuntimeError("MINIQMT_QUOTE_SUBSCRIPTION_SYMBOLS_EMPTY: no stocks requested")
        xtdata_mod = _load_xtdata()
        if xtdata_mod is None:
            raise RuntimeError("MINIQMT_QUOTE_SUBSCRIPTION_UNAVAILABLE: xtquant.xtdata is not available")
        self.start()
        requested = set(normalized)
        target_stocks = list(normalized)
        with self._lock:
            existing = self.managed_subscriptions.get(key)
            existing_stocks = [str(stock) for stock in (existing or {}).get("stocks") or []]
            if existing_stocks:
                target_stocks = list(dict.fromkeys([*existing_stocks, *normalized]))
            if (
                not force
                and existing
                and int(existing.get("seq") or 0) in self.subscriptions
                and requested.issubset(set(existing_stocks))
            ):
                return {
                    **existing,
                    "status": "active",
                    "forced": False,
                    "requested_stocks": normalized,
                    "subscription_reused": True,
                }
            old_seq = int(existing.get("seq") or 0) if existing else None
        if old_seq:
            self.unsubscribe(old_seq)
        seq = self.subscribe(target_stocks, callback)
        if not isinstance(seq, int) or seq <= 0:
            raise RuntimeError(
                "MINIQMT_QUOTE_SUBSCRIPTION_FAILED: xtdata.subscribe_whole_quote did not return a positive seq"
            )
        payload = {
            "key": key,
            "seq": seq,
            "stocks": target_stocks,
            "status": "active",
            "forced": bool(force),
            "requested_stocks": normalized,
            "subscription_reused": False,
            "subscribed_at": datetime.now(UTC).isoformat(),
        }
        with self._lock:
            self.managed_subscriptions[key] = dict(payload)
        return payload

    def _on_quote(self, datas: Dict):
        """Dispatch quote callbacks."""

        try:
            for stock_code, quote in datas.items():
                with self._lock:
                    callbacks = self.callbacks.get(stock_code, [])

                for callback in callbacks:
                    try:
                        callback(stock_code, quote)
                    except Exception as e:  # noqa: BLE001
                        logger.error("quote callback failed for %s: %s", stock_code, e, exc_info=True)
        except Exception as e:  # noqa: BLE001
            logger.error("quote callback dispatch raised: %s", e, exc_info=True)

    def unsubscribe(self, seq: int) -> bool:
        """Unsubscribe one xtdata quote sequence."""

        xtdata_mod = _load_xtdata()
        if xtdata_mod is None:
            return False

        try:
            with self._lock:
                if seq in self.subscriptions:
                    stocks = self.subscriptions[seq]
                    xtdata_mod.unsubscribe_quote(seq)
                    del self.subscriptions[seq]
                    for key, payload in list(self.managed_subscriptions.items()):
                        if int(payload.get("seq") or 0) == seq:
                            del self.managed_subscriptions[key]

                    for stock in stocks:
                        if stock in self.callbacks:
                            del self.callbacks[stock]

                    logger.info("unsubscribed realtime quotes: stocks=%s seq=%s", stocks, seq)
                    return True
                logger.warning("quote subscription seq not found: seq=%s", seq)
                return False

        except Exception as e:  # noqa: BLE001
            logger.error("unsubscribe realtime quote raised: %s", e, exc_info=True)
            return False

    def start(self):
        """Start xtdata event loop in a daemon thread."""

        if self.running:
            return

        if _load_xtdata() is None:
            logger.error("xtquant.xtdata is not available; cannot start realtime quote subscriber")
            return

        self.running = True
        self.thread = threading.Thread(target=self._run, name="realtime-quote-subscriber", daemon=True)
        self.thread.start()
        logger.info("realtime quote subscriber started")

    def stop(self):
        """Stop subscription service and unsubscribe all known sequences."""

        if not self.running:
            return

        self.running = False

        with self._lock:
            for seq in list(self.subscriptions.keys()):
                self.unsubscribe(seq)

        logger.info("realtime quote subscriber stopped")

    def _run(self):
        """Run xtdata callback event loop."""

        try:
            xtdata_mod = _load_xtdata()
            if xtdata_mod is None:
                raise RuntimeError("xtquant.xtdata is not available")
            xtdata_mod.run()
        except Exception as e:  # noqa: BLE001
            logger.error("realtime quote subscriber loop raised: %s", e, exc_info=True)
        finally:
            self.running = False

    def get_latest_quote(self, stock_code: str) -> Optional[Dict]:
        """Fetch latest cached quote via xtdata.get_market_data."""

        xtdata_mod = _load_xtdata()
        if xtdata_mod is None:
            return None

        try:
            data = xtdata_mod.get_market_data(
                field_list=["time", "lastPrice", "open", "high", "low", "volume", "amount"],
                stock_list=[stock_code],
                period="tick",
                count=1,
            )

            if data and "lastPrice" in data:
                df_price = data["lastPrice"]
                df_time = data.get("time")
                df_volume = data.get("volume")

                if not df_price.empty:
                    quote = {
                        "time": int(df_time.iloc[0, 0]) if df_time is not None and not df_time.empty else None,
                        "lastPrice": float(df_price.iloc[0, 0]),
                        "close": float(df_price.iloc[0, 0]),
                        "volume": float(df_volume.iloc[0, 0]) if df_volume is not None and not df_volume.empty else None,
                        "open": float(data.get("open").iloc[0, 0]) if "open" in data and not data["open"].empty else None,
                        "high": float(data.get("high").iloc[0, 0]) if "high" in data and not data["high"].empty else None,
                        "low": float(data.get("low").iloc[0, 0]) if "low" in data and not data["low"].empty else None,
                        "amount": float(data.get("amount").iloc[0, 0]) if "amount" in data and not data["amount"].empty else None,
                    }
                    return quote

        except Exception as e:  # noqa: BLE001
            logger.error("get latest quote failed for %s: %s", stock_code, e, exc_info=True)

        return None


_subscriber_instance: Optional[RealtimeQuoteSubscriber] = None


def get_realtime_quote_subscriber() -> RealtimeQuoteSubscriber:
    """Return the process-wide realtime quote subscriber."""

    global _subscriber_instance
    if _subscriber_instance is None:
        _subscriber_instance = RealtimeQuoteSubscriber()
    return _subscriber_instance
