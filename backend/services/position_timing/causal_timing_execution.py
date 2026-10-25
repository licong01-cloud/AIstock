"""Execution adapters for the PT-NEXT-024 offline continuous-account replay."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from backend.execution_algos.board_lot import round_to_board_lot

from .action_value import ActionValueError
from .action_value_data import file_reference
from .causal_timing_contracts import E0, E1, E2, EXECUTION_MINUTE, EXECUTION_VIEW_IDS
from .contracts import canonical_sha256
from .minute_execution_pipeline import _load_calendar
from .pattern_close_cash_replay import Account, ZERO, dec, fee, positive


MINUTE_FIELDS = (
    "open", "close", "volume", "factor", "up_limit_price", "down_limit_price",
    "limit_up", "limit_down",
)
LIMIT_TOLERANCE = Decimal("0.005")


@dataclass(frozen=True)
class ExecutionQuote:
    view_id: str
    trade_date: date
    raw_price: Decimal | None
    factor: Decimal | None
    raw_volume: Decimal | None
    up_limit: Decimal | None
    down_limit: Decimal | None
    limit_up: bool | None
    limit_down: bool | None
    is_suspended: bool
    source_status: str
    minute_label: str | None = None


class MinuteExecutionSource:
    """Read only the two pre-registered bars used by this research."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        meta_path = self.root / "meta_export.json"
        calendar_path = self.root / "calendars" / "1min.txt"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        required = tuple(meta.get("required_minute_fields") or ())
        if any(field not in required for field in MINUTE_FIELDS):
            raise ActionValueError("CAUSAL_MINUTE_REQUIRED_FIELD_MISSING")
        self.references = {
            "minute_meta": file_reference(meta_path),
            "minute_calendar": file_reference(calendar_path),
        }
        self.calendar = _load_calendar(calendar_path)
        self.index = {value: ordinal for ordinal, value in enumerate(self.calendar)}
        if len(self.index) != len(self.calendar):
            raise ActionValueError("CAUSAL_MINUTE_CALENDAR_DUPLICATE")
        minute_suffixes = tuple(f" {value}:00" for value in EXECUTION_MINUTE.values())
        self._selected_ordinals = np.asarray(
            [ordinal for ordinal, label in enumerate(self.calendar) if label.endswith(minute_suffixes)],
            dtype=np.int64,
        )
        self._selected_index = {
            int(ordinal): compact for compact, ordinal in enumerate(self._selected_ordinals)
        }
        if not len(self._selected_ordinals):
            raise ActionValueError("CAUSAL_MINUTE_SELECTED_CALENDAR_EMPTY")
        self.coverage_start = date.fromisoformat(str(meta["start"]))
        self.coverage_end = date.fromisoformat(str(meta["end"]))
        self._cache: dict[tuple[str, str], np.ndarray] = {}

    def identity(self) -> dict[str, Any]:
        value = {
            "root": self.root.as_posix(),
            "references": self.references,
            "coverage": [self.coverage_start.isoformat(), self.coverage_end.isoformat()],
            "fields": list(MINUTE_FIELDS),
            "minutes": dict(EXECUTION_MINUTE),
        }
        return {**value, "identity_sha256": canonical_sha256(value)}

    def _values(self, symbol: str, field: str, indexes: Sequence[int]) -> np.ndarray:
        key = (symbol, field)
        values = self._cache.get(key)
        if values is None:
            path = self.root / "features" / symbol.lower() / f"{field}.1min.bin"
            if not path.is_file():
                raise ActionValueError("CAUSAL_MINUTE_SYMBOL_FIELD_MISSING", symbol=symbol, field=field)
            # Loading once per worker/symbol avoids repeatedly reopening the full bin.
            before = path.stat()
            source = np.fromfile(path, dtype="<f4")
            if (
                len(source) < 2 or not np.isfinite(source[0])
                or source[0] < 0 or not float(source[0]).is_integer()
            ):
                raise ActionValueError("CAUSAL_MINUTE_BIN_HEADER_INVALID", symbol=symbol, field=field)
            positions = self._selected_ordinals - int(source[0]) + 1
            valid = (positions >= 1) & (positions < len(source))
            values = np.full(len(self._selected_ordinals), np.nan, dtype=np.float64)
            values[valid] = source[positions[valid]]
            after = path.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise ActionValueError("CAUSAL_MINUTE_SOURCE_CHANGED", symbol=symbol, field=field)
            self._cache[key] = values
        try:
            compact = np.asarray([self._selected_index[int(index)] for index in indexes], dtype=int)
        except KeyError as exc:
            raise ActionValueError("CAUSAL_MINUTE_ORDINAL_OUTSIDE_FROZEN_TIMES") from exc
        return values[compact]

    def quote(self, *, symbol: str, trade_date: date, view_id: str) -> ExecutionQuote:
        if view_id not in (E1, E2):
            raise ActionValueError("CAUSAL_MINUTE_VIEW_INVALID", view_id=view_id)
        minute = EXECUTION_MINUTE[view_id]
        label = f"{trade_date.isoformat()} {minute}:00"
        ordinal = self.index.get(label)
        if ordinal is None:
            return ExecutionQuote(view_id, trade_date, None, None, None, None, None, None, None,
                                  False, "MINUTE_CALENDAR_MISSING", label)
        try:
            arrays = {field: self._values(symbol, field, (ordinal,))[0] for field in MINUTE_FIELDS}
        except (OSError, ValueError):
            return ExecutionQuote(view_id, trade_date, None, None, None, None, None, None, None,
                                  False, "MINUTE_FIELD_MISSING_OR_INVALID", label)
        factor = arrays["factor"]
        adjusted_price = arrays["close"] if view_id == E1 else arrays["open"]
        finite = all(np.isfinite(arrays[field]) for field in MINUTE_FIELDS)
        if not finite or factor <= 0 or adjusted_price <= 0:
            return ExecutionQuote(view_id, trade_date, None, None, None, None, None, None, None,
                                  False, "MINUTE_PRICE_OR_FACTOR_INVALID", label)
        if arrays["limit_up"] not in (0.0, 1.0) or arrays["limit_down"] not in (0.0, 1.0):
            return ExecutionQuote(view_id, trade_date, None, None, None, None, None, None, None,
                                  False, "MINUTE_LIMIT_FLAG_INVALID", label)
        raw_price = Decimal(str(round(float(adjusted_price / factor), 2)))
        raw_volume = dec(arrays["volume"]) * dec(factor)
        up = Decimal(str(round(float(arrays["up_limit_price"]), 2)))
        down = Decimal(str(round(float(arrays["down_limit_price"]), 2)))
        if raw_price <= ZERO or raw_volume < ZERO or up <= down or down <= ZERO:
            return ExecutionQuote(view_id, trade_date, None, None, None, None, None, None, None,
                                  False, "MINUTE_RAW_BASIS_INVALID", label)
        return ExecutionQuote(
            view_id, trade_date, raw_price, dec(factor), raw_volume, up, down,
            bool(arrays["limit_up"]), bool(arrays["limit_down"]), False,
            "PRICE_PROXY_NOT_QUEUE_PROVEN", label,
        )

    def clear_symbol_cache(self, symbol: str) -> None:
        for key in tuple(self._cache):
            if key[0] == symbol:
                self._cache.pop(key)


def daily_quote(bar: Mapping[str, Any], *, trade_date: date) -> ExecutionQuote:
    required = ("close", "factor", "up_limit", "down_limit")
    if bool(bar.get("is_suspended")):
        return ExecutionQuote(E0, trade_date, None, None, None, None, None, None, None, True, "SUSPENDED")
    if not all(positive(bar.get(name)) for name in required):
        return ExecutionQuote(E0, trade_date, None, None, None, None, None, None, None, False,
                              "DAILY_FIELD_INVALID")
    raw = dec(bar["close"])
    up, down = dec(bar["up_limit"]), dec(bar["down_limit"])
    if up <= down:
        return ExecutionQuote(E0, trade_date, None, None, None, None, None, None, None, False,
                              "DAILY_LIMIT_INVALID")
    return ExecutionQuote(
        E0, trade_date, raw, dec(bar["factor"]), None, up, down,
        raw >= up - LIMIT_TOLERANCE, raw <= down + LIMIT_TOLERANCE, False, "DAILY_CLOSE_PROXY",
    )


def quote_status(quote: ExecutionQuote, side: str) -> str:
    if side not in {"BUY", "SELL"}:
        raise ActionValueError("CAUSAL_EXECUTION_SIDE_INVALID", side=side)
    if quote.source_status not in {"DAILY_CLOSE_PROXY", "PRICE_PROXY_NOT_QUEUE_PROVEN"}:
        return quote.source_status
    if quote.raw_price is None or quote.factor is None:
        return "PRICE_UNKNOWN"
    if quote.raw_volume is not None and quote.raw_volume <= ZERO:
        return "NO_OBSERVED_VOLUME"
    if side == "BUY" and (quote.limit_up or quote.raw_price >= quote.up_limit - LIMIT_TOLERANCE):
        return "DIRECTIONAL_LIMIT_BLOCKED"
    if side == "SELL" and (quote.limit_down or quote.raw_price <= quote.down_limit + LIMIT_TOLERANCE):
        return "DIRECTIONAL_LIMIT_BLOCKED"
    return "EXECUTABLE"


def affordable_for_budget(symbol: str, cash: Decimal, price: Decimal, budget: Decimal) -> int:
    ceiling = min(cash, max(ZERO, budget))
    low, high = 0, int(ceiling / price) if price > ZERO else 0
    while low < high:
        middle = (low + high + 1) // 2
        quantity = round_to_board_lot(middle, symbol, side="BUY")
        notional = price * quantity
        if quantity == 0 or notional + fee(notional, "BUY") <= ceiling:
            low = middle
        else:
            high = middle - 1
    return round_to_board_lot(low, symbol, side="BUY")


def execute_quote(
    account: Account,
    *,
    symbol: str,
    quote: ExecutionQuote,
    ordinal: int,
    side: str,
    budget: Decimal | None = None,
    sell_fraction: Decimal = Decimal(1),
) -> dict[str, Any]:
    """Mutate one virtual-unit cash account using one parent-order proxy."""
    status = quote_status(quote, side)
    result: dict[str, Any] = {
        "side": side, "status": status, "view_id": quote.view_id,
        "source_status": quote.source_status, "minute_label": quote.minute_label,
    }
    if status != "EXECUTABLE":
        return result
    if side == "SELL" and (account.units <= ZERO or ordinal <= account.bought_on):
        return {**result, "status": "T1_NOT_SELLABLE"}
    assert quote.raw_price is not None and quote.factor is not None
    raw, factor = quote.raw_price, quote.factor
    if side == "BUY":
        quantity = affordable_for_budget(symbol, account.cash, raw, budget or account.cash)
        if quantity <= 0:
            return {**result, "status": "CASH_BELOW_MINIMUM_LOT"}
        notional = raw * quantity
        charged = fee(notional, "BUY")
        if notional + charged > account.cash:
            raise ActionValueError("CAUSAL_EXECUTION_OVERSPEND")
        units = Decimal(quantity) / factor
        account.entry = (account.units * (account.entry or ZERO) + notional + charged) / (account.units + units)
        account.units += units
        account.cash -= notional + charged
        account.bought_on = ordinal
        account.bought_once = True
        result["raw_quantity"] = quantity
    else:
        if sell_fraction <= ZERO or sell_fraction > Decimal(1):
            raise ActionValueError("CAUSAL_SELL_FRACTION_INVALID")
        if sell_fraction == Decimal(1):
            units = account.units
            quantity = int(units * factor)
        else:
            raw_available = int(account.units * factor)
            requested = int(Decimal(raw_available) * sell_fraction)
            quantity = round_to_board_lot(requested, symbol, side="SELL", allow_sell_residual=False)
            if quantity <= 0:
                return {**result, "status": "PARTIAL_QUANTITY_UNAVAILABLE"}
            units = min(account.units, Decimal(quantity) / factor)
        notional = units * raw * factor
        charged = fee(notional, "SELL")
        if account.cash + notional < charged:
            return {**result, "status": "CASH_BELOW_SELL_FEE"}
        account.cash += notional - charged
        account.units -= units
        if account.units <= ZERO:
            account.units, account.entry = ZERO, None
    account.fees += charged
    return {
        **result, "status": "FILLED", "raw_price": float(raw), "raw_quantity": quantity,
        "notional": float(notional), "fee": float(charged), "virtual_units": float(units),
        "remaining_virtual_units": float(account.units), "cash_after": float(account.cash),
    }


def quote_for_view(
    *, view_id: str, symbol: str, trade_date: date, daily_bar: Mapping[str, Any],
    minute_source: MinuteExecutionSource | None,
) -> ExecutionQuote:
    if view_id not in EXECUTION_VIEW_IDS:
        raise ActionValueError("CAUSAL_EXECUTION_VIEW_INVALID", view_id=view_id)
    if view_id == E0:
        return daily_quote(daily_bar, trade_date=trade_date)
    if minute_source is None:
        raise ActionValueError("CAUSAL_MINUTE_SOURCE_REQUIRED", view_id=view_id)
    return minute_source.quote(symbol=symbol, trade_date=trade_date, view_id=view_id)
