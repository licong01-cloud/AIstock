"""miniQMT adapter for AIstock data service.

This module wraps miniQMT account/position/order/trade APIs into simple
Python dataclasses used by the public data service API.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class Position:
    instrument: str
    volume: float
    available: float
    avg_price: float
    market_value: float


@dataclass
class PortfolioState:
    cash: float
    equity: float
    positions: List[Position]
    timestamp: datetime


@dataclass
class Order:
    order_id: str
    instrument: str
    side: str
    volume: float
    price: Optional[float]
    status: str
    created_at: datetime


@dataclass
class Trade:
    trade_id: str
    order_id: str
    instrument: str
    side: str
    volume: float
    price: float
    traded_at: datetime


def load_portfolio_state_qmt() -> PortfolioState:
    """Load current portfolio state from miniQMT.
    
    Implementation notes:
    - Uses ``xtquant.xttrader`` to fetch asset and position data.
    - Equity is calculated as cash + market_value of positions.
    """

    try:
        from xtquant import xttrader, xtconstant  # type: ignore[import-not-found]
        from .api import get_xt_trader_and_account  # Need to ensure this exists or similar helper
    except Exception as exc:
        raise RuntimeError("xtquant is not available in current environment") from exc

    trader, account = get_xt_trader_and_account()
    if not trader or not account:
        raise RuntimeError("xtquant trader not connected or account not specified")

    asset = trader.query_stock_asset(account)
    if not asset:
        raise RuntimeError(f"failed to query asset for account {account.account_id}")

    xt_positions = trader.query_stock_positions(account)
    positions: List[Position] = []
    total_market_value = 0.0

    if xt_positions:
        for p in xt_positions:
            mv = float(p.market_value)
            total_market_value += mv
            positions.append(
                Position(
                    instrument=p.stock_code,
                    volume=float(p.volume),
                    available=float(p.can_use_volume),
                    avg_price=float(p.open_price),
                    market_value=mv
                )
            )

    return PortfolioState(
        cash=float(asset.cash),
        equity=float(asset.cash) + total_market_value,
        positions=positions,
        timestamp=datetime.now()
    )


def load_open_orders_qmt() -> List[Order]:
    """Load open orders from miniQMT."""

    try:
        from xtquant import xttrader, xtconstant
        from .api import get_xt_trader_and_account
    except Exception as exc:
        raise RuntimeError("xtquant is not available in current environment") from exc

    trader, account = get_xt_trader_and_account()
    xt_orders = trader.query_stock_orders(account)
    orders: List[Order] = []

    if xt_orders:
        for o in xt_orders:
            # Filter for open orders
            if o.order_status in [
                xtconstant.ORDER_UNREPORTED,
                xtconstant.ORDER_WAIT_REPORTING,
                xtconstant.ORDER_REPORTED,
                xtconstant.ORDER_PART_REPORTED,
                xtconstant.ORDER_PART_SUCCEED,
            ]:
                orders.append(
                    Order(
                        order_id=str(o.order_id),
                        instrument=o.stock_code,
                        side="buy" if o.order_type == xtconstant.STOCK_BUY else "sell",
                        volume=float(o.order_volume),
                        price=float(o.price),
                        status=str(o.order_status),
                        created_at=datetime.fromtimestamp(o.order_time)
                    )
                )
    return orders


def load_trades_qmt(
    *, start: Optional[datetime] = None, end: Optional[datetime] = None
) -> List[Trade]:
    """Load trades from miniQMT within an optional time range."""

    try:
        from xtquant import xttrader, xtconstant
        from .api import get_xt_trader_and_account
    except Exception as exc:
        raise RuntimeError("xtquant is not available in current environment") from exc

    trader, account = get_xt_trader_and_account()
    xt_trades = trader.query_stock_trades(account)
    trades: List[Trade] = []

    if xt_trades:
        for t in xt_trades:
            trade_time = datetime.fromtimestamp(t.traded_time)
            if start and trade_time < start:
                continue
            if end and trade_time > end:
                continue

            trades.append(
                Trade(
                    trade_id=str(t.traded_id),
                    order_id=str(t.order_id),
                    instrument=t.stock_code,
                    side="buy" if t.order_type == xtconstant.STOCK_BUY else "sell",
                    volume=float(t.traded_volume),
                    price=float(t.traded_price),
                    traded_at=trade_time
                )
            )
    return trades
