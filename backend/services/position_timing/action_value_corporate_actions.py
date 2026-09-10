"""Immutable corporate-action source and pure position transformations.

This module is deliberately narrow.  It freezes implemented dividend rows
from the existing local database into a timing-owned artifact, then applies
cash and stock distributions to historical research states.  It never writes
the database and it does not provide runtime advice or order execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .action_value import ActionValueError, PositionState, cutoff_on
from .artifact_store import PositionTimingArtifactStore
from .contracts import canonical_json_bytes, canonical_sha256


SNAPSHOT_SCHEMA = "position_timing_corporate_action_snapshot_v1"
IMPLEMENTED_DIVIDEND = "\u5b9e\u65bd"
SOURCE_QUERY_IDENTITY = {
    "table": "market.dividend",
    "filter": "trim(div_proc)=IMPLEMENTED AND ex_date BETWEEN start AND end AND ts_code IN symbols",
    "canonical_unit": "PER_PRE_ACTION_SHARE",
    "account_cash_component": "cash_div (after-tax per local DDL contract)",
    "reference_price_cash_component": "cash_div_tax (pre-tax per local DDL contract)",
    "stock_component": "stk_div=stk_bo_rate+stk_co_rate",
}


@dataclass(frozen=True)
class CorporateAction:
    symbol: str
    effective_trade_date: date
    quantity_multiplier: Decimal
    cashflow_yuan_per_share: Decimal
    reference_price_cash_yuan_per_share: Decimal
    cash_pay_date: date | None
    share_listing_date: date | None
    source_available_at: datetime
    source_row_count: int
    source_rows_sha256: str

    def __post_init__(self) -> None:
        if (
            not self.symbol
            or self.quantity_multiplier < 1
            or not self.quantity_multiplier.is_finite()
            or self.cashflow_yuan_per_share < 0
            or not self.cashflow_yuan_per_share.is_finite()
            or self.reference_price_cash_yuan_per_share < 0
            or not self.reference_price_cash_yuan_per_share.is_finite()
            or self.source_available_at.tzinfo is None
            or self.source_row_count <= 0
            or len(self.source_rows_sha256) != 64
        ):
            raise ActionValueError("CORPORATE_ACTION_CONTRACT_INVALID", symbol=self.symbol)


@dataclass(frozen=True)
class CorporateActionBook:
    actions: tuple[CorporateAction, ...]
    snapshot_sha256: str
    _by_key: Mapping[tuple[str, date], CorporateAction] = field(init=False, repr=False, compare=False)
    _by_symbol: Mapping[str, tuple[CorporateAction, ...]] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        keys = [(item.symbol, item.effective_trade_date) for item in self.actions]
        if keys != sorted(keys) or len(keys) != len(set(keys)) or len(self.snapshot_sha256) != 64:
            raise ActionValueError("CORPORATE_ACTION_BOOK_INVALID")
        object.__setattr__(self, "_by_key", dict(zip(keys, self.actions)))
        by_symbol: dict[str, list[CorporateAction]] = {}
        for action in self.actions:
            by_symbol.setdefault(action.symbol, []).append(action)
        object.__setattr__(self, "_by_symbol", {key: tuple(value) for key, value in by_symbol.items()})

    @classmethod
    def empty(cls) -> "CorporateActionBook":
        return cls((), canonical_sha256({"schema_version": SNAPSHOT_SCHEMA, "actions": []}))

    @classmethod
    def open(cls, path: Path) -> "CorporateActionBook":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ActionValueError("CORPORATE_ACTION_SNAPSHOT_UNAVAILABLE") from exc
        identity = {key: value for key, value in payload.items() if key != "snapshot_sha256"}
        if (
            payload.get("schema_version") != SNAPSHOT_SCHEMA
            or payload.get("snapshot_sha256") != canonical_sha256(identity)
        ):
            raise ActionValueError("CORPORATE_ACTION_SNAPSHOT_IDENTITY_MISMATCH")
        try:
            actions = tuple(
                CorporateAction(
                    symbol=str(item["symbol"]),
                    effective_trade_date=date.fromisoformat(item["effective_trade_date"]),
                    quantity_multiplier=Decimal(item["quantity_multiplier"]),
                    cashflow_yuan_per_share=Decimal(item["cashflow_yuan_per_share"]),
                    reference_price_cash_yuan_per_share=Decimal(
                        item["reference_price_cash_yuan_per_share"]
                    ),
                    cash_pay_date=(date.fromisoformat(item["cash_pay_date"]) if item.get("cash_pay_date") else None),
                    share_listing_date=(date.fromisoformat(item["share_listing_date"]) if item.get("share_listing_date") else None),
                    source_available_at=datetime.fromisoformat(item["source_available_at"]),
                    source_row_count=int(item["source_row_count"]),
                    source_rows_sha256=str(item["source_rows_sha256"]),
                )
                for item in payload["actions"]
            )
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            raise ActionValueError("CORPORATE_ACTION_SNAPSHOT_SCHEMA_INVALID") from exc
        return cls(actions, payload["snapshot_sha256"])

    def on(self, symbol: str, trade_date: date) -> CorporateAction | None:
        return self._by_key.get((symbol.upper(), trade_date))

    def between(self, symbol: str, start_exclusive: date, end_inclusive: date) -> tuple[CorporateAction, ...]:
        normalized = symbol.upper()
        return tuple(
            item
            for item in self._by_symbol.get(normalized, ())
            if start_exclusive < item.effective_trade_date <= end_inclusive
        )


@dataclass(frozen=True)
class CorporateActionApplication:
    state: PositionState
    fractional_share_discarded: Decimal
    fractional_sellable_share_discarded: Decimal


def freeze_corporate_action_snapshot(
    connection: Any,
    *,
    symbols: Sequence[str],
    start: date,
    end: date,
    timing_root: Path,
) -> Path:
    """Read one repeatable, read-only DB view and publish it immutably."""

    normalized = tuple(sorted({str(symbol).upper() for symbol in symbols}))
    if not normalized or start > end:
        raise ActionValueError("CORPORATE_ACTION_SNAPSHOT_SCOPE_INVALID")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT ts_code, end_date, ann_date, imp_ann_date, ex_date,
                       stk_div, stk_bo_rate, stk_co_rate, cash_div,
                       cash_div_tax, record_date, pay_date, div_listdate,
                       base_date, base_share
                  FROM market.dividend
                 WHERE ts_code = ANY(%s)
                   AND ex_date BETWEEN %s AND %s
                   AND trim(div_proc) = %s
                 ORDER BY ts_code, ex_date, imp_ann_date DESC NULLS LAST,
                          end_date DESC, ann_date DESC NULLS LAST
                """,
                (list(normalized), start, end, IMPLEMENTED_DIVIDEND),
            )
            raw_rows = cursor.fetchall()
    except Exception as exc:
        raise ActionValueError("CORPORATE_ACTION_SOURCE_READ_FAILED") from exc
    payload = _snapshot_payload(raw_rows, symbols=normalized, start=start, end=end)
    digest = payload["snapshot_sha256"]
    path = (
        timing_root.resolve()
        / "research"
        / "action_value_v2"
        / "corporate_actions"
        / f"{digest}.json"
    ).resolve()
    if not path.is_relative_to(timing_root.resolve()):
        raise ActionValueError("CORPORATE_ACTION_SNAPSHOT_PATH_OUTSIDE_OWNER")
    PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(payload))
    return path


def apply_corporate_action(
    state: PositionState,
    action: CorporateAction,
    *,
    next_trade_date: date | None,
) -> PositionState:
    return apply_corporate_action_with_audit(
        state,
        action,
        next_trade_date=next_trade_date,
    ).state


def apply_corporate_action_with_audit(
    state: PositionState,
    action: CorporateAction,
    *,
    next_trade_date: date | None,
) -> CorporateActionApplication:
    """Apply one ex-date action with conservative legal-share rounding.

    The public source does not contain investor-level odd-lot allocation.  The
    frozen research policy therefore floors only the fractional entitlement
    and credits no invented compensation; the discarded fraction is returned
    for receipt-level sensitivity disclosure.
    """

    if (
        action.cashflow_yuan_per_share or action.reference_price_cash_yuan_per_share
    ) and (
        action.cash_pay_date is None or action.cash_pay_date > action.effective_trade_date
    ):
        raise ActionValueError(
            "CORPORATE_ACTION_DEFERRED_CASH_UNAVAILABLE",
            symbol=action.symbol,
            effective_trade_date=action.effective_trade_date.isoformat(),
        )
    if action.quantity_multiplier > 1:
        if action.share_listing_date is None:
            raise ActionValueError("CORPORATE_ACTION_SHARE_LISTING_DATE_UNAVAILABLE", symbol=action.symbol)
        if action.share_listing_date > action.effective_trade_date and action.share_listing_date != next_trade_date:
            raise ActionValueError(
                "CORPORATE_ACTION_SHARE_LISTING_DELAY_UNSUPPORTED",
                symbol=action.symbol,
                share_listing_date=action.share_listing_date.isoformat(),
            )
    if state.quantity == 0:
        return CorporateActionApplication(state, Decimal(0), Decimal(0))
    new_quantity_decimal = Decimal(state.quantity) * action.quantity_multiplier
    new_sellable_decimal = Decimal(state.sellable) * action.quantity_multiplier
    new_quantity = int(new_quantity_decimal.to_integral_value(rounding=ROUND_FLOOR))
    fractional_share_discarded = new_quantity_decimal - Decimal(new_quantity)
    if action.share_listing_date is None or action.share_listing_date <= action.effective_trade_date:
        new_sellable = int(new_sellable_decimal.to_integral_value(rounding=ROUND_FLOOR))
        fractional_sellable_share_discarded = new_sellable_decimal - Decimal(new_sellable)
    else:
        new_sellable = state.sellable
        fractional_sellable_share_discarded = Decimal(0)
    cashflow = Decimal(state.quantity) * action.cashflow_yuan_per_share
    entry_cost = state.entry_cost
    if entry_cost is not None:
        remaining_cost = entry_cost * state.quantity - cashflow
        entry_cost = remaining_cost / new_quantity if remaining_cost > 0 and new_quantity else None
    return CorporateActionApplication(
        replace(
            state,
            quantity=new_quantity,
            sellable=new_sellable,
            cash=state.cash + cashflow,
            entry_cost=entry_cost,
        ),
        fractional_share_discarded,
        fractional_sellable_share_discarded,
    )


def _snapshot_payload(
    rows: Iterable[Sequence[Any]],
    *,
    symbols: Sequence[str],
    start: date,
    end: date,
) -> dict[str, Any]:
    grouped: dict[tuple[str, date], list[dict[str, Any]]] = {}
    for values in rows:
        if len(values) != 15:
            raise ActionValueError("CORPORATE_ACTION_SOURCE_SCHEMA_INVALID")
        raw = {
            "symbol": str(values[0]).upper(),
            "end_date": _date_text(values[1]),
            "ann_date": _date_text(values[2]),
            "imp_ann_date": _date_text(values[3]),
            "effective_trade_date": _date_text(values[4]),
            "stk_div": _decimal_text(values[5]),
            "stk_bo_rate": _decimal_text(values[6]),
            "stk_co_rate": _decimal_text(values[7]),
            "cash_div": (_decimal_text(values[8]) if values[8] is not None else None),
            "cash_div_tax": (_decimal_text(values[9]) if values[9] is not None else None),
            "record_date": _date_text(values[10]),
            "cash_pay_date": _date_text(values[11]),
            "share_listing_date": _date_text(values[12]),
            "base_date": _date_text(values[13]),
            "base_share": _decimal_text(values[14]),
        }
        if raw["symbol"] not in symbols or not raw["effective_trade_date"]:
            raise ActionValueError("CORPORATE_ACTION_SOURCE_SCOPE_INVALID", symbol=raw["symbol"])
        effective = date.fromisoformat(raw["effective_trade_date"])
        if not start <= effective <= end:
            raise ActionValueError("CORPORATE_ACTION_SOURCE_SCOPE_INVALID", symbol=raw["symbol"])
        grouped.setdefault((raw["symbol"], effective), []).append(raw)

    actions: list[dict[str, Any]] = []
    equivalent_revisions = 0
    for (symbol, effective), versions in sorted(grouped.items()):
        economics = {_economic_identity(row) for row in versions}
        if len(economics) != 1:
            raise ActionValueError(
                "CORPORATE_ACTION_ECONOMIC_CONFLICT",
                symbol=symbol,
                effective_trade_date=effective.isoformat(),
                economic_action_count=len(economics),
            )
        (
            stock_dividend,
            account_cash_dividend,
            reference_price_cash_dividend,
            pay_date,
            listing_date,
        ) = next(iter(economics))
        implementation_dates = {row["imp_ann_date"] for row in versions}
        if None in implementation_dates or len(implementation_dates) != 1:
            raise ActionValueError("CORPORATE_ACTION_AVAILABILITY_CONFLICT", symbol=symbol)
        implementation_date = date.fromisoformat(next(iter(implementation_dates)))
        if implementation_date > effective:
            raise ActionValueError("CORPORATE_ACTION_AVAILABLE_AFTER_EFFECTIVE_DATE", symbol=symbol)
        stable_rows = sorted(versions, key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")))
        actions.append(
            {
                "symbol": symbol,
                "effective_trade_date": effective.isoformat(),
                "quantity_multiplier": str(Decimal(1) + Decimal(stock_dividend)),
                "cashflow_yuan_per_share": account_cash_dividend,
                "reference_price_cash_yuan_per_share": reference_price_cash_dividend,
                "cash_pay_date": pay_date,
                "share_listing_date": listing_date,
                "source_available_at": cutoff_on(implementation_date).isoformat(),
                "source_row_count": len(stable_rows),
                "source_rows_sha256": canonical_sha256(stable_rows),
            }
        )
        equivalent_revisions += len(stable_rows) - 1
    identity = {
        "schema_version": SNAPSHOT_SCHEMA,
        "source_query": SOURCE_QUERY_IDENTITY,
        "scope": {"symbols": list(symbols), "start": start.isoformat(), "end": end.isoformat()},
        "raw_source_row_count": sum(len(items) for items in grouped.values()),
        "canonical_action_count": len(actions),
        "canonicalized_equivalent_revision_count": equivalent_revisions,
        "actions": actions,
    }
    return {**identity, "snapshot_sha256": canonical_sha256(identity)}


def _economic_identity(
    row: Mapping[str, Any],
) -> tuple[str, str, str, str | None, str | None]:
    stock = Decimal(row["stk_div"])
    bonus = Decimal(row["stk_bo_rate"])
    capitalization = Decimal(row["stk_co_rate"])
    cash_after_tax = Decimal(row["cash_div"] or "0")
    cash_pre_tax = Decimal(row["cash_div_tax"] or "0")
    if (cash_after_tax > 0 and row["cash_div_tax"] is None) or (
        cash_pre_tax > 0 and row["cash_div"] is None
    ):
        raise ActionValueError("CORPORATE_ACTION_CASH_COMPONENT_UNAVAILABLE", symbol=row["symbol"])
    if min(stock, bonus, capitalization, cash_pre_tax, cash_after_tax) < 0 or stock != bonus + capitalization:
        raise ActionValueError("CORPORATE_ACTION_ECONOMICS_INVALID", symbol=row["symbol"])
    return (
        str(stock),
        str(cash_after_tax),
        str(cash_pre_tax),
        row["cash_pay_date"],
        row["share_listing_date"],
    )


def _date_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)[:10]).isoformat()


def _decimal_text(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return "0"
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise ActionValueError("CORPORATE_ACTION_NUMERIC_INVALID") from exc
    if not parsed.is_finite():
        raise ActionValueError("CORPORATE_ACTION_NUMERIC_INVALID")
    return "0" if parsed == 0 else str(parsed.normalize())


__all__ = [
    "CorporateAction",
    "CorporateActionApplication",
    "CorporateActionBook",
    "apply_corporate_action",
    "apply_corporate_action_with_audit",
    "freeze_corporate_action_snapshot",
]
