"""MiniQMT-backed Paper v2 broker adapter.

This backend is intentionally thin: it translates AIstock ``OrderIntent``
objects into documented ``xtquant`` order parameters and treats MiniQMT as the
only authority for acceptance, fills, cash, positions, and order status. It
never imports LocalSim or the in-process minute execution engine.
"""

from __future__ import annotations

import json
import threading
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable
from uuid import uuid4

from backend.infra.qmt_client import BaseQMTClient, QMTNotAvailableError, get_qmt_client_singleton
from backend.services.paper_trading_v2.market_data import (
    MinuteDataSource,
    assert_broker_market_source_match,
)
from backend.services.trading_core.errors import (
    BrokerConnectivityError,
    BrokerRejectedError,
    BrokerSubmitError,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType, PositionLot

from .base import (
    BackendId,
    BrokerAccountSnapshot,
    BrokerBackend,
    BrokerBindCapacity,
    CancelAck,
    FillEvent,
    MarketDataChannel,
    OrderHandle,
    OrderHandleStatus,
    OrderHandleStatusState,
    SubscriptionHandle,
)


_BACKEND_ID: BackendId = "minqmt_sim"
_BACKEND_VERSION = "0.1.0"
_STOCK_BUY = 23
_STOCK_SELL = 24
_LATEST_PRICE = 5
_FIX_PRICE = 11
_ORDER_PENDING = {48, 49, 50, 51}
_ORDER_PARTIAL = {52, 53, 55}
_ORDER_CANCELLED = {54}
_ORDER_FILLED = {56}
_ORDER_REJECTED = {57}
_ORDER_UNKNOWN = {255}
_EXCLUSIVE_ACCOUNT = "exclusive_account"


class _OrderRecord:
    __slots__ = (
        "handle",
        "intent",
        "miniqmt_order_id",
        "strategy_name",
        "order_remark",
        "status",
    )

    def __init__(
        self,
        *,
        handle: OrderHandle,
        intent: OrderIntent,
        miniqmt_order_id: str,
        strategy_name: str,
        order_remark: str,
        status: OrderHandleStatus,
    ) -> None:
        self.handle = handle
        self.intent = intent
        self.miniqmt_order_id = miniqmt_order_id
        self.strategy_name = strategy_name
        self.order_remark = order_remark
        self.status = status


class MiniQMTSimBackend(BrokerBackend):
    """BrokerBackend implementation for one MiniQMT simulation account.

    The default ``exclusive_account`` mode accepts exactly one strategy package
    binding per backend instance. Any future shared-account attribution or soft
    capital-control mode must be enabled explicitly outside this MVP.
    """

    backend_id: BackendId = _BACKEND_ID
    backend_version: str = _BACKEND_VERSION

    def __init__(
        self,
        *,
        portfolio_id: str,
        package_id: str,
        data_source: MinuteDataSource,
        qmt_client: BaseQMTClient | Any | None = None,
        strategy_slot_id: str | None = None,
        account_mode: str = _EXCLUSIVE_ACCOUNT,
        auto_connect: bool = True,
    ) -> None:
        if not portfolio_id:
            raise ValueError("portfolio_id is required")
        if not package_id:
            raise ValueError("package_id is required")
        assert_broker_market_source_match(self.backend_id, data_source)
        normalized_mode = str(account_mode or _EXCLUSIVE_ACCOUNT).strip()
        if normalized_mode != _EXCLUSIVE_ACCOUNT:
            raise BrokerSubmitError(
                "MiniQMTSimBackend MVP supports exclusive_account only",
                context={"account_mode": normalized_mode, "supported": [_EXCLUSIVE_ACCOUNT]},
            )

        self._portfolio_id = portfolio_id
        self._package_id = package_id
        self._data_source = data_source
        self._qmt_client = qmt_client or get_qmt_client_singleton()
        self._strategy_slot_id = strategy_slot_id or portfolio_id
        self._account_mode = normalized_mode
        self._records: dict[str, _OrderRecord] = {}
        self._intent_index: dict[str, str] = {}
        self._subscribers: dict[str, Callable[[FillEvent], None]] = {}
        self._lock = threading.RLock()
        self._closed = False
        self._connected = False
        if auto_connect:
            self.ensure_connected()

    @property
    def portfolio_id(self) -> str:
        return self._portfolio_id

    @property
    def package_id(self) -> str:
        return self._package_id

    @property
    def data_source(self) -> MinuteDataSource:
        return self._data_source

    @property
    def account_mode(self) -> str:
        return self._account_mode

    @property
    def strategy_slot_id(self) -> str:
        return self._strategy_slot_id

    def ensure_connected(self) -> None:
        """Connect to MiniQMT and fail fast if the client is unavailable."""

        self._ensure_alive()
        try:
            status = self._qmt_client.status()
            if not bool(getattr(status, "connected", False)):
                ok, message = self._qmt_client.connect()
                if not ok:
                    raise BrokerConnectivityError(
                        "MiniQMT connection failed",
                        context={
                            "backend_id": self.backend_id,
                            "portfolio_id": self._portfolio_id,
                            "package_id": self._package_id,
                            "message": message,
                            "status": _status_to_dict(self._safe_status()),
                        },
                    )
                status = self._qmt_client.status()
            if str(getattr(status, "mode", "") or "").upper() != "SIM":
                raise BrokerConnectivityError(
                    "MiniQMTSimBackend requires MiniQMT SIM account mode",
                    context={
                        "backend_id": self.backend_id,
                        "portfolio_id": self._portfolio_id,
                        "package_id": self._package_id,
                        "mode": getattr(status, "mode", None),
                        "account_id": getattr(status, "account_id", None),
                    },
                )
            self._connected = True
        except BrokerConnectivityError:
            self._connected = False
            raise
        except Exception as exc:
            self._connected = False
            raise BrokerConnectivityError(
                "MiniQMT connection probe failed",
                context={
                    "backend_id": self.backend_id,
                    "portfolio_id": self._portfolio_id,
                    "package_id": self._package_id,
                    "reason": f"{type(exc).__name__}: {exc}",
                },
            ) from exc

    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle:
        self._ensure_ready_for_order(intent)
        with self._lock:
            if intent.intent_id in self._intent_index:
                raise BrokerSubmitError(
                    "OrderIntent already submitted to this MiniQMTSim instance",
                    context={"intent_id": intent.intent_id, "backend_id": self.backend_id},
                )
            native_order_type = _native_order_type(intent.side)
            native_price_type, native_price = _native_price(intent)
            strategy_name = _safe_strategy_name(self._strategy_slot_id)
            order_remark = _order_remark(
                portfolio_id=self._portfolio_id,
                package_id=self._package_id,
                intent_id=intent.intent_id,
            )
            submitted_at = datetime.now(UTC)
            _enforce_submit_deadline(intent, submitted_at)
            try:
                order_id, message = self._qmt_client.place_order(
                    stock_code=intent.symbol,
                    order_type=native_order_type,
                    order_volume=int(intent.quantity),
                    price_type=native_price_type,
                    price=native_price,
                    strategy_name=strategy_name,
                    order_remark=order_remark,
                )
                submit_diagnostic = _safe_last_order_diagnostic(self._qmt_client)
            except QMTNotAvailableError as exc:
                raise BrokerConnectivityError(
                    "MiniQMT order submit failed because client is unavailable",
                    context={
                        "intent_id": intent.intent_id,
                        "portfolio_id": self._portfolio_id,
                        "package_id": self._package_id,
                        "symbol": intent.symbol,
                        "reason": str(exc),
                    },
                ) from exc
            except Exception as exc:
                raise BrokerSubmitError(
                    "MiniQMT order submit call failed",
                    context={
                        "intent_id": intent.intent_id,
                        "portfolio_id": self._portfolio_id,
                        "package_id": self._package_id,
                        "symbol": intent.symbol,
                        "reason": f"{type(exc).__name__}: {exc}",
                    },
                ) from exc
            if int(order_id or -1) <= 0:
                status = self._rejected_status(
                    handle_id=f"mqh_{uuid4().hex}",
                    reason=str(message or "MiniQMT rejected the order"),
                    event_at=submitted_at,
                    diagnostic=submit_diagnostic,
                )
                handle = OrderHandle(
                    handle_id=status.handle_id,
                    backend_id=self.backend_id,
                    submitted_at=submitted_at,
                    intent_id=intent.intent_id,
                )
                self._records[handle.handle_id] = _OrderRecord(
                    handle=handle,
                    intent=intent,
                    miniqmt_order_id=str(order_id),
                    strategy_name=strategy_name,
                    order_remark=order_remark,
                    status=status,
                )
                self._intent_index[intent.intent_id] = handle.handle_id
                raise BrokerRejectedError(
                    "MiniQMT rejected the order",
                    context={
                        "intent_id": intent.intent_id,
                        "handle_id": handle.handle_id,
                        "miniqmt_order_id": str(order_id),
                        "message": str(message or ""),
                        "symbol": intent.symbol,
                        "side": intent.side.value,
                        "quantity": intent.quantity,
                    },
                )

            handle = OrderHandle(
                handle_id=f"mqh_{uuid4().hex}",
                backend_id=self.backend_id,
                submitted_at=submitted_at,
                intent_id=intent.intent_id,
            )
            status = OrderHandleStatus(
                handle_id=handle.handle_id,
                state="pending",
                filled_quantity=0,
                avg_fill_price=None,
                last_event_at=submitted_at,
                rejection_reason=None,
                raw_status="submitted",
                status_msg=None,
                raw={
                    "miniqmt_order_id": str(order_id),
                    "message": str(message or ""),
                    **({"submit_diagnostic": submit_diagnostic} if submit_diagnostic else {}),
                },
            )
            self._records[handle.handle_id] = _OrderRecord(
                handle=handle,
                intent=intent,
                miniqmt_order_id=str(order_id),
                strategy_name=strategy_name,
                order_remark=order_remark,
                status=status,
            )
            self._intent_index[intent.intent_id] = handle.handle_id
            return handle

    def cancel(self, handle: OrderHandle) -> CancelAck:
        self._ensure_alive()
        record = self._record_for(handle)
        try:
            accepted, reason = self._qmt_client.cancel_order(record.miniqmt_order_id)
        except QMTNotAvailableError as exc:
            raise BrokerConnectivityError(
                "MiniQMT cancel failed because client is unavailable",
                context={"handle_id": handle.handle_id, "miniqmt_order_id": record.miniqmt_order_id},
            ) from exc
        except Exception as exc:
            raise BrokerSubmitError(
                "MiniQMT cancel call failed",
                context={
                    "handle_id": handle.handle_id,
                    "miniqmt_order_id": record.miniqmt_order_id,
                    "reason": f"{type(exc).__name__}: {exc}",
                },
            ) from exc
        return CancelAck(handle_id=handle.handle_id, accepted=bool(accepted), reason=str(reason or ""))

    def query_status(self, handle: OrderHandle) -> OrderHandleStatus:
        self._ensure_alive()
        record = self._record_for(handle)
        order = self._find_qmt_order(record)
        if order is None:
            return record.status
        status = self._status_from_order(record, order)
        with self._lock:
            record.status = status
        return status

    def query_trades(self, handle: OrderHandle) -> list[dict[str, Any]]:
        self._ensure_alive()
        record = self._record_for(handle)
        return self._find_qmt_trades(record)

    def order_context(self, handle: OrderHandle) -> dict[str, str]:
        """Return broker-native identifiers that callers must persist.

        MiniQMT remains the authority, but AIstock needs these ids to reconcile
        a later process tick after the in-memory backend instance is gone.
        """

        record = self._record_for(handle)
        return {
            "handle_id": record.handle.handle_id,
            "intent_id": record.intent.intent_id,
            "miniqmt_order_id": record.miniqmt_order_id,
            "strategy_name": record.strategy_name,
            "order_remark": record.order_remark,
        }

    def query_status_from_native(
        self,
        *,
        handle_id: str,
        intent: OrderIntent,
        miniqmt_order_id: str,
        strategy_name: str,
        order_remark: str,
    ) -> OrderHandleStatus:
        """Reconcile a previously persisted MiniQMT order.

        This is the process-restart path for Paper v2 sessions: persisted
        order metadata supplies the MiniQMT ids, then this backend queries the
        current MiniQMT order snapshot and maps it back into the common
        ``OrderHandleStatus`` contract.
        """

        handle = OrderHandle(
            handle_id=handle_id,
            backend_id=self.backend_id,
            submitted_at=datetime.now(UTC),
            intent_id=intent.intent_id,
        )
        pending = OrderHandleStatus(
            handle_id=handle_id,
            state="pending",
            filled_quantity=0,
            avg_fill_price=None,
            last_event_at=datetime.now(UTC),
            rejection_reason=None,
            raw_status="reconcile_pending",
            status_msg=None,
            raw={"miniqmt_order_id": str(miniqmt_order_id), "order_remark": str(order_remark)},
        )
        record = _OrderRecord(
            handle=handle,
            intent=intent,
            miniqmt_order_id=str(miniqmt_order_id),
            strategy_name=strategy_name,
            order_remark=order_remark,
            status=pending,
        )
        order = self._find_qmt_order(record)
        if order is None:
            return pending.model_copy(
                update={
                    "raw": {
                        **pending.raw,
                        "diagnostic_gap": True,
                        "diagnostic_gap_reason": "native_order_snapshot_not_found",
                    }
                }
            )
        return self._status_from_order(record, order)

    def query_trades_from_native(
        self,
        *,
        handle_id: str,
        intent: OrderIntent,
        miniqmt_order_id: str,
        strategy_name: str,
        order_remark: str,
    ) -> list[dict[str, Any]]:
        handle = OrderHandle(
            handle_id=handle_id,
            backend_id=self.backend_id,
            submitted_at=datetime.now(UTC),
            intent_id=intent.intent_id,
        )
        record = _OrderRecord(
            handle=handle,
            intent=intent,
            miniqmt_order_id=str(miniqmt_order_id),
            strategy_name=strategy_name,
            order_remark=order_remark,
            status=OrderHandleStatus(
                handle_id=handle_id,
                state="pending",
                filled_quantity=0,
                avg_fill_price=None,
                last_event_at=datetime.now(UTC),
                rejection_reason=None,
            ),
        )
        return self._find_qmt_trades(record)

    def subscribe_fill_callback(self, cb: Callable[[FillEvent], None]) -> SubscriptionHandle:
        self._ensure_alive()
        sub_id = f"mqsub_{uuid4().hex}"
        with self._lock:
            self._subscribers[sub_id] = cb
        return SubscriptionHandle(subscription_id=sub_id, backend_id=self.backend_id)

    def unsubscribe_fill_callback(self, handle: SubscriptionHandle) -> None:
        with self._lock:
            self._subscribers.pop(handle.subscription_id, None)

    def query_account(self) -> BrokerAccountSnapshot:
        self._ensure_alive()
        try:
            raw = self._qmt_client.get_account_info()
        except QMTNotAvailableError as exc:
            raise BrokerConnectivityError("MiniQMT account query failed", context={"reason": str(exc)}) from exc
        except Exception as exc:
            raise BrokerConnectivityError(
                "MiniQMT account query failed",
                context={"reason": f"{type(exc).__name__}: {exc}"},
            ) from exc
        return BrokerAccountSnapshot(
            backend_id=self.backend_id,
            cash=_decimal_from_any(raw.get("available_cash", raw.get("cash", 0))),
            nav=_decimal_from_any(raw.get("total_asset", raw.get("nav", 0))),
            margin_used=_decimal_from_optional(raw.get("frozen_cash")),
            as_of=datetime.now(UTC),
        )

    def query_positions(self) -> dict[str, PositionLot]:
        positions, _prices = self.query_position_marks()
        return positions

    def query_quote(self, symbol: str) -> dict[str, object] | None:
        """Return L1 quote from the underlying MiniQMT client when available."""

        self._ensure_alive()
        getter = getattr(self._qmt_client, "get_full_tick", None)
        if not callable(getter):
            return None
        try:
            data = getter([symbol])
        except QMTNotAvailableError as exc:
            raise BrokerConnectivityError("MiniQMT quote query failed", context={"symbol": symbol, "reason": str(exc)}) from exc
        except Exception as exc:
            raise BrokerConnectivityError(
                "MiniQMT quote query failed",
                context={"symbol": symbol, "reason": f"{type(exc).__name__}: {exc}"},
            ) from exc
        if not data:
            return None
        row = data.get(symbol) if isinstance(data, dict) else None
        if row is None and isinstance(data, dict):
            raw_code = str(symbol).split(".")[0]
            for key, value in data.items():
                if str(key).split(".")[0] == raw_code:
                    row = value
                    break
        if not isinstance(row, dict):
            return None
        return _normalize_quote_row(symbol, row)

    def query_position_marks(self) -> tuple[dict[str, PositionLot], dict[str, float]]:
        """Return MiniQMT-authoritative positions plus mark prices.

        Paper v2 persistence still needs a price column for position rows, but
        the MiniQMT path must not fetch DB/TDX prices to synthesize marks. Use
        only fields returned by MiniQMT position snapshots.
        """

        self._ensure_alive()
        try:
            rows = self._qmt_client.get_positions()
        except QMTNotAvailableError as exc:
            raise BrokerConnectivityError("MiniQMT positions query failed", context={"reason": str(exc)}) from exc
        except Exception as exc:
            raise BrokerConnectivityError(
                "MiniQMT positions query failed",
                context={"reason": f"{type(exc).__name__}: {exc}"},
            ) from exc
        result: dict[str, PositionLot] = {}
        prices: dict[str, float] = {}
        today = date.today()
        for row in rows or []:
            symbol = str(row.get("stock_code") or row.get("symbol") or "").strip()
            if not symbol:
                continue
            quantity = int(row.get("quantity", row.get("volume", 0)) or 0)
            if quantity <= 0:
                continue
            available = int(row.get("can_sell", row.get("available_quantity", quantity)) or 0)
            result[symbol] = PositionLot(
                portfolio_id=self._portfolio_id,
                symbol=symbol,
                quantity=quantity,
                available_quantity=max(0, available),
                avg_cost=float(row.get("cost_price", row.get("avg_cost", row.get("open_price", 0.0))) or 0.0),
                trade_date=today,
            )
            price = _position_mark_price(row, quantity=quantity)
            if price is not None:
                prices[symbol] = price
        return result, prices

    def market_data_channel(self) -> MarketDataChannel:
        return MarketDataChannel(
            backend_id=self.backend_id,
            source=self._data_source,
            channel_kind="minqmt_xtdata",
        )

    def bind_capacity(self) -> BrokerBindCapacity:
        return BrokerBindCapacity(
            backend_id=self.backend_id,
            max_concurrent_packages=1,
            rejection_reason_if_exceeded=(
                "MiniQMTSim exclusive_account binds one strategy package to one MiniQMT account"
            ),
        )

    def shutdown(self) -> None:
        with self._lock:
            self._closed = True
            self._subscribers.clear()

    def _ensure_alive(self) -> None:
        if self._closed:
            raise BrokerConnectivityError(
                "MiniQMTSimBackend has been shut down",
                context={"backend_id": self.backend_id, "portfolio_id": self._portfolio_id},
            )

    def _ensure_ready_for_order(self, intent: OrderIntent) -> None:
        self._ensure_alive()
        if not self._connected:
            self.ensure_connected()
        if intent.portfolio_id != self._portfolio_id:
            raise BrokerSubmitError(
                "OrderIntent.portfolio_id does not match MiniQMTSim binding",
                context={
                    "intent_id": intent.intent_id,
                    "intent_portfolio_id": intent.portfolio_id,
                    "backend_portfolio_id": self._portfolio_id,
                },
            )
        if intent.package_id != self._package_id:
            raise BrokerSubmitError(
                "OrderIntent.package_id does not match MiniQMTSim binding",
                context={
                    "intent_id": intent.intent_id,
                    "intent_package_id": intent.package_id,
                    "backend_package_id": self._package_id,
                },
            )

    def _record_for(self, handle: OrderHandle) -> _OrderRecord:
        if handle.backend_id != self.backend_id:
            raise BrokerSubmitError(
                "OrderHandle.backend_id does not match MiniQMTSim",
                context={"handle_id": handle.handle_id, "backend_id": handle.backend_id},
            )
        with self._lock:
            record = self._records.get(handle.handle_id)
        if record is None:
            raise BrokerSubmitError("unknown OrderHandle", context={"handle_id": handle.handle_id})
        return record

    def _find_qmt_order(self, record: _OrderRecord) -> dict[str, Any] | None:
        try:
            orders = self._qmt_client.get_orders(cancelable_only=False)
        except QMTNotAvailableError as exc:
            raise BrokerConnectivityError(
                "MiniQMT order status query failed",
                context={"handle_id": record.handle.handle_id, "reason": str(exc)},
            ) from exc
        except Exception as exc:
            raise BrokerConnectivityError(
                "MiniQMT order status query failed",
                context={"handle_id": record.handle.handle_id, "reason": f"{type(exc).__name__}: {exc}"},
            ) from exc
        for order in orders or []:
            if str(order.get("order_id") or "") == record.miniqmt_order_id:
                return order
        for order in orders or []:
            if str(order.get("order_remark") or "") == record.order_remark:
                return order
        return None

    def _find_qmt_trades(self, record: _OrderRecord) -> list[dict[str, Any]]:
        try:
            trades = self._qmt_client.get_trades()
        except QMTNotAvailableError as exc:
            raise BrokerConnectivityError(
                "MiniQMT trade query failed",
                context={"handle_id": record.handle.handle_id, "reason": str(exc)},
            ) from exc
        except Exception as exc:
            raise BrokerConnectivityError(
                "MiniQMT trade query failed",
                context={"handle_id": record.handle.handle_id, "reason": f"{type(exc).__name__}: {exc}"},
            ) from exc
        matched: list[dict[str, Any]] = []
        for trade in trades or []:
            if str(trade.get("order_id") or "") == record.miniqmt_order_id:
                matched.append(dict(trade))
                continue
            if str(trade.get("order_remark") or "") == record.order_remark:
                matched.append(dict(trade))
        matched.sort(key=lambda item: (str(item.get("traded_time") or ""), str(item.get("traded_id") or "")))
        return matched

    def _status_from_order(self, record: _OrderRecord, raw: dict[str, Any]) -> OrderHandleStatus:
        raw_status = _int_or_none(raw.get("order_status"))
        filled = int(raw.get("traded_volume") or 0)
        avg_price = _decimal_from_optional(raw.get("traded_price"))
        status_msg = str(raw.get("status_msg") or "") or None
        if raw_status in _ORDER_FILLED:
            state: OrderHandleStatusState = "filled"
        elif raw_status in _ORDER_PARTIAL:
            state = "partial_filled"
        elif raw_status in _ORDER_CANCELLED:
            state = "cancelled"
        elif raw_status in _ORDER_REJECTED:
            state = "rejected"
        elif raw_status in _ORDER_PENDING or raw_status in _ORDER_UNKNOWN or raw_status is None:
            state = "pending"
        else:
            state = "pending"
        if state == "filled" and filled <= 0:
            filled = record.intent.quantity
        if state == "partial_filled" and filled <= 0:
            filled = max(0, record.status.filled_quantity)
        if state == "rejected" and not status_msg:
            status_msg = "MiniQMT order rejected"
        return OrderHandleStatus(
            handle_id=record.handle.handle_id,
            state=state,
            filled_quantity=filled,
            avg_fill_price=avg_price,
            last_event_at=datetime.now(UTC),
            rejection_reason=status_msg if state == "rejected" else None,
            raw_status=raw_status,
            status_msg=status_msg,
            raw=dict(raw),
        )

    @staticmethod
    def _rejected_status(
        *, handle_id: str, reason: str, event_at: datetime, diagnostic: dict[str, Any] | None = None
    ) -> OrderHandleStatus:
        raw: dict[str, object] = {"message": reason}
        if diagnostic:
            raw["submit_diagnostic"] = diagnostic
            raw["raw_return_code"] = diagnostic.get("raw_return_code")
            raw["diagnostic_gap"] = True
            raw["diagnostic_gap_reason"] = diagnostic.get("classification") or "miniqmt_submit_rejected"
        return OrderHandleStatus(
            handle_id=handle_id,
            state="rejected",
            filled_quantity=0,
            avg_fill_price=None,
            last_event_at=event_at,
            rejection_reason=reason,
            raw_status="submit_rejected",
            status_msg=reason,
            raw=raw,
        )

    def _safe_status(self) -> Any | None:
        try:
            return self._qmt_client.status()
        except Exception:
            return None


def _native_order_type(side: OrderSide) -> int:
    if side == OrderSide.BUY:
        return _STOCK_BUY
    if side == OrderSide.SELL:
        return _STOCK_SELL
    raise BrokerSubmitError("unsupported order side for MiniQMT", context={"side": str(side)})


def _native_price(intent: OrderIntent) -> tuple[int, float]:
    if intent.order_type == OrderType.MARKET:
        return _LATEST_PRICE, 0.0
    if intent.order_type == OrderType.LIMIT:
        if intent.limit_price is None:
            raise BrokerSubmitError("limit order requires limit_price", context={"intent_id": intent.intent_id})
        return _FIX_PRICE, float(intent.limit_price)
    raise BrokerSubmitError(
        "unsupported order type for MiniQMT",
        context={"intent_id": intent.intent_id, "order_type": str(intent.order_type)},
    )


def _safe_last_order_diagnostic(qmt_client: Any) -> dict[str, Any] | None:
    getter = getattr(qmt_client, "get_last_order_diagnostic", None)
    if not callable(getter):
        return None
    try:
        diagnostic = getter()
    except Exception:  # noqa: BLE001
        return None
    return dict(diagnostic) if isinstance(diagnostic, dict) else None


def _safe_strategy_name(value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(value or ""))
    return (safe or "aistock_minqmt")[:48]


def _order_remark(*, portfolio_id: str, package_id: str, intent_id: str) -> str:
    payload = {"portfolio_id": portfolio_id, "package_id": package_id, "intent_id": intent_id}
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(raw) <= 240:
        return raw
    return f"aistock:{portfolio_id[:32]}:{package_id[:32]}:{intent_id[:48]}"


def _enforce_submit_deadline(intent: OrderIntent, submit_started_at: datetime) -> None:
    raw_scheduled = intent.metadata.get("scheduled_submit_at")
    if raw_scheduled in (None, ""):
        return
    try:
        scheduled = datetime.fromisoformat(str(raw_scheduled).replace("Z", "+00:00"))
    except ValueError as exc:
        raise BrokerSubmitError(
            "scheduled_submit_at must be an ISO datetime",
            context={"intent_id": intent.intent_id, "scheduled_submit_at": raw_scheduled},
        ) from exc
    if scheduled.tzinfo is None:
        scheduled = scheduled.replace(tzinfo=UTC)
    try:
        max_lag = float(intent.metadata.get("max_submit_lag_seconds", 0) or 0)
    except (TypeError, ValueError) as exc:
        raise BrokerSubmitError(
            "max_submit_lag_seconds must be numeric",
            context={"intent_id": intent.intent_id, "max_submit_lag_seconds": intent.metadata.get("max_submit_lag_seconds")},
        ) from exc
    deadline = scheduled + timedelta(seconds=max(0.0, max_lag))
    if submit_started_at > deadline:
        raise BrokerSubmitError(
            "MiniQMT order missed scheduled submit deadline",
            context={
                "intent_id": intent.intent_id,
                "scheduled_submit_at": scheduled.isoformat(),
                "submit_started_at": submit_started_at.isoformat(),
                "max_submit_lag_seconds": max_lag,
                "broker_backend": _BACKEND_ID,
            },
        )


def _status_to_dict(value: Any | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if hasattr(value, "__dict__"):
        return dict(value.__dict__)
    if isinstance(value, dict):
        return dict(value)
    return {"repr": repr(value)}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal_from_any(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _decimal_from_optional(value: Any) -> Decimal | None:
    if value is None:
        return None
    parsed = _decimal_from_any(value)
    return parsed if parsed > 0 else None


def _position_mark_price(row: dict[str, Any], *, quantity: int) -> float | None:
    for key in ("current_price", "last_price", "market_price"):
        value = row.get(key)
        try:
            price = float(value)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    try:
        market_value = float(row.get("market_value") or 0.0)
    except (TypeError, ValueError):
        market_value = 0.0
    if market_value > 0 and quantity > 0:
        return market_value / quantity
    for key in ("cost_price", "avg_cost", "open_price"):
        value = row.get(key)
        try:
            price = float(value)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    return None


__all__ = ["MiniQMTSimBackend"]


def _normalize_quote_row(symbol: str, row: dict[str, Any]) -> dict[str, object]:
    def first(keys: tuple[str, ...], default: object = None) -> object:
        for key in keys:
            value = row.get(key)
            if value is not None:
                return value
        return default

    bid_prices = first(("bidPrice", "bid_price", "bidPrice1", "bid"), [])
    ask_prices = first(("askPrice", "ask_price", "askPrice1", "ask"), [])
    bid_volumes = first(("bidVol", "bid_volume", "bidVol1", "bidVolume"), [])
    ask_volumes = first(("askVol", "ask_volume", "askVol1", "askVolume"), [])

    def level(value: object) -> object:
        if isinstance(value, (list, tuple)) and value:
            return value[0]
        return value

    normalized = {
        "symbol": symbol,
        "bid_price_1": level(bid_prices),
        "ask_price_1": level(ask_prices),
        "bid_volume_1": level(bid_volumes),
        "ask_volume_1": level(ask_volumes),
        "time": first(("time", "timetag", "datetime"), None),
        "raw": dict(row),
    }
    return normalized
