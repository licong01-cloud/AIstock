from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Iterator

import psycopg2.extras
import pytest

from backend.services.paper_trading_v2.repository import PaperTradingV2Repository
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import (
    Fill,
    Order,
    OrderEvent,
    OrderEventType,
    OrderSide,
    OrderStatus,
    OrderType,
)


class _PsycopgJsonCursor:
    rowcount = 1

    def __init__(self, connection: "_PsycopgJsonConnection") -> None:
        self._connection = connection

    def __enter__(self) -> "_PsycopgJsonCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        serialized_payloads: list[Any] = []
        for param in params or ():
            if isinstance(param, psycopg2.extras.Json):
                # Exercise the same json.dumps adapter used by psycopg2 before
                # PostgreSQL receives a JSON/JSONB parameter.
                param.getquoted()
                serialized_payloads.append(json.loads(json.dumps(param.adapted, allow_nan=False)))
        self._connection.executed.append((" ".join(sql.split()), params, serialized_payloads))

    def fetchone(self) -> tuple[bool]:
        return (True,)


class _PsycopgJsonConnection:
    def __init__(self) -> None:
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0
        self.executed: list[tuple[str, tuple[Any, ...] | None, list[Any]]] = []

    def cursor(self, *args, **kwargs) -> _PsycopgJsonCursor:
        return _PsycopgJsonCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _connection_factory(connection: _PsycopgJsonConnection):
    @contextmanager
    def factory(*, autocommit: bool = True, manage_transaction: bool = False) -> Iterator[_PsycopgJsonConnection]:
        original_autocommit = connection.autocommit
        connection.autocommit = autocommit
        try:
            yield connection
            if not autocommit and manage_transaction:
                connection.commit()
        except Exception:
            if not autocommit and manage_transaction:
                connection.rollback()
            raise
        finally:
            connection.autocommit = original_autocommit

    return factory


def _repository(connection: _PsycopgJsonConnection) -> PaperTradingV2Repository:
    resolver = type("NoopSymbolNameResolver", (), {"resolve": lambda self, symbols: {}})()
    return PaperTradingV2Repository(
        conn_factory=_connection_factory(connection),
        symbol_name_resolver=resolver,
    )


def _immutable_payload() -> MappingProxyType:
    return MappingProxyType(
        {
            "nested": MappingProxyType(
                {
                    "trade_date": date(2026, 7, 22),
                    "intended_price": Decimal("10.25"),
                }
            ),
            "flags": ("immutable", True),
        }
    )


def _order(*, metadata: dict[str, Any]) -> Order:
    return Order(
        order_id="ord_bug824",
        intent_id="intent_bug824",
        package_id="pkg_bug824",
        portfolio_id="portfolio_bug824",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        order_type=OrderType.MARKET,
        status=OrderStatus.SUBMITTED,
        metadata=metadata,
    )


def _fill(*, metadata: dict[str, Any]) -> Fill:
    return Fill(
        fill_id="fill_bug824",
        order_id="ord_bug824",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        price=10.25,
        trade_time=datetime(2026, 7, 22, 9, 31, tzinfo=UTC),
        reason="unit_fill",
        metadata=metadata,
    )


def test_local_sim_economic_transaction_serializes_immutable_order_fill_and_event_payloads() -> None:
    connection = _PsycopgJsonConnection()
    repository = _repository(connection)
    order = _order(metadata={"market_snapshot": _immutable_payload()})
    fill = _fill(metadata={"market_snapshot": _immutable_payload()})
    event = OrderEvent(
        event_id="event_bug824",
        order_id=order.order_id,
        event_type=OrderEventType.FILLED,
        event_time=fill.trade_time,
        fill=fill,
        reason="unit_event",
        metadata={"market_snapshot": _immutable_payload()},
    )

    with repository.local_sim_economic_transaction("run_bug824"):
        repository.save_order("run_bug824", order)
        repository.save_fill(
            "run_bug824",
            fill,
            fill_market_context={"market_snapshot": _immutable_payload()},
        )
        repository.save_order_event("run_bug824", event)
        repository.save_run_event(
            run_id="run_bug824",
            event_type="RUN_ECONOMIC_COMMITTED",
            message="unit durable commit",
            context={"market_snapshot": _immutable_payload()},
        )

    json_payloads = [
        payload
        for sql, _, serialized_payloads in connection.executed
        if sql.startswith("INSERT INTO paper_v2.")
        for payload in serialized_payloads
    ]
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert len(json_payloads) == 6
    for payload in json_payloads:
        snapshot = payload.get("market_snapshot") if isinstance(payload, dict) else None
        if snapshot is None and isinstance(payload, dict):
            snapshot = payload.get("metadata", {}).get("market_snapshot")
        assert snapshot == {
            "flags": ["immutable", True],
            "nested": {"intended_price": "10.25", "trade_date": "2026-07-22"},
        }


def test_local_sim_economic_transaction_rejects_unknown_json_value_and_rolls_back() -> None:
    connection = _PsycopgJsonConnection()
    repository = _repository(connection)
    order = _order(metadata={"unsupported": object()})

    with pytest.raises(DataUnavailableError) as exc_info:
        with repository.local_sim_economic_transaction("run_bug824_invalid"):
            repository.save_order("run_bug824_invalid", order)

    assert exc_info.value.context == {
        "reason_code": "PAPER_V2_DURABLE_FACT_JSON_TYPE_INVALID",
        "fact_type": "order",
        "fact_id": "ord_bug824",
        "field": "metadata",
        "path": "$.unsupported",
        "value_type": "object",
    }
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any(sql.startswith("INSERT INTO paper_v2.orders") for sql, _, _ in connection.executed)


def test_local_sim_economic_transaction_rolls_back_prior_fact_when_later_event_is_invalid() -> None:
    connection = _PsycopgJsonConnection()
    repository = _repository(connection)
    order = _order(metadata={"valid": True})
    event = OrderEvent(
        event_id="event_bug824_invalid",
        order_id=order.order_id,
        event_type=OrderEventType.REJECTED,
        reason="invalid durable metadata",
        metadata={"unsupported": object()},
    )

    with pytest.raises(DataUnavailableError) as exc_info:
        with repository.local_sim_economic_transaction("run_bug824_mid_transaction_invalid"):
            repository.save_order("run_bug824_mid_transaction_invalid", order)
            repository.save_order_event("run_bug824_mid_transaction_invalid", event)

    assert exc_info.value.context["reason_code"] == "PAPER_V2_DURABLE_FACT_JSON_TYPE_INVALID"
    assert exc_info.value.context["fact_type"] == "order_event"
    assert exc_info.value.context["fact_id"] == "event_bug824_invalid"
    assert exc_info.value.context["field"] == "metadata"
    assert exc_info.value.context["path"] == "$.unsupported"
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert any(sql.startswith("INSERT INTO paper_v2.orders") for sql, _, _ in connection.executed)
    assert not any(sql.startswith("INSERT INTO paper_v2.order_events") for sql, _, _ in connection.executed)


@pytest.mark.parametrize(
    ("invalid_payload", "reason_code"),
    [
        ({"invalid": float("nan")}, "PAPER_V2_DURABLE_FACT_JSON_NUMBER_INVALID"),
        ({"invalid": float("inf")}, "PAPER_V2_DURABLE_FACT_JSON_NUMBER_INVALID"),
        ({"invalid": Decimal("NaN")}, "PAPER_V2_DURABLE_FACT_JSON_NUMBER_INVALID"),
        ({"invalid": MappingProxyType({None: "null-key"})}, "PAPER_V2_DURABLE_FACT_JSON_KEY_INVALID"),
        ({"invalid": MappingProxyType({1: "integer-key"})}, "PAPER_V2_DURABLE_FACT_JSON_KEY_INVALID"),
    ],
)
def test_durable_fact_json_contract_rejects_nonfinite_values_and_nonstring_keys(
    invalid_payload: dict[str, Any],
    reason_code: str,
) -> None:
    connection = _PsycopgJsonConnection()
    repository = _repository(connection)

    with pytest.raises(DataUnavailableError) as exc_info:
        with repository.local_sim_economic_transaction("run_bug824_invalid_matrix"):
            repository.save_order(
                "run_bug824_invalid_matrix",
                _order(metadata=invalid_payload),
            )

    assert exc_info.value.context["reason_code"] == reason_code
    assert exc_info.value.context["fact_type"] == "order"
    assert exc_info.value.context["fact_id"] == "ord_bug824"
    assert exc_info.value.context["field"] == "metadata"
    assert connection.commits == 0
    assert connection.rollbacks == 1
