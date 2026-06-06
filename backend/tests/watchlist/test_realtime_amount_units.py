from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backend.core import data_source_manager_impl
from backend.core.data_source_manager_impl import DataSourceManager
from backend.services import watchlist_service
from backend.services.watchlist_service import _compute_realtime_fields


class _Response:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "code": 0,
            "data": [
                {
                    "Amount": 259_653_008,
                    "TotalHand": 219_761,
                    "ServerTime": "2026-06-02 10:30:00",
                    "K": {
                        "Close": 12_150,
                        "Last": 12_100,
                        "Open": 12_000,
                        "High": 12_300,
                        "Low": 11_900,
                    },
                }
            ],
        }


def test_tdx_quote_amount_stays_in_yuan(monkeypatch):
    manager = DataSourceManager.__new__(DataSourceManager)
    manager.tdx_available = True
    manager.tdx_api_base = "http://tdx.example"
    manager._convert_to_ts_code = lambda _code: "000411.SZ"

    monkeypatch.setattr(data_source_manager_impl.requests, "get", lambda *args, **kwargs: _Response())

    quote = manager._get_tdx_quote("000411.SZ")

    assert quote is not None
    assert quote["source"] == "tdx"
    assert quote["volume"] == 21_976_100
    assert quote["amount"] == 259_653_008
    assert quote["amount"] / 100_000_000 == 2.59653008


def test_watchlist_realtime_fields_do_not_rescale_amount():
    fields = _compute_realtime_fields(
        {
            "price": 12.15,
            "pre_close": 12.1,
            "open": 12.0,
            "high": 12.3,
            "low": 11.9,
            "volume": 21_976_100,
            "amount": 259_653_008,
        }
    )

    assert fields["volume_hand"] == 219_761
    assert fields["amount"] == 259_653_008


def test_watchlist_pct_since_entry_uses_qfq_adjusted_entry_price():
    fields = _compute_realtime_fields(
        {"price": 10.0, "pre_close": 9.8},
        entry_price=20.0,
        entry_price_for_return=5.0,
    )

    assert fields["pct_since_entry"] == 100.0


class _AdjCursor:
    def __init__(self) -> None:
        self.params = None
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None) -> None:
        self.sql = sql
        self.params = params

    def fetchall(self):
        return [
            (
                "000001.SZ",
                date(2024, 1, 2),
                date(2024, 1, 2),
                "entry_as_of",
                Decimal("0.5"),
                date(2024, 1, 2),
                Decimal("1.0"),
                date(2024, 6, 3),
            )
        ]


class _AdjConn:
    def __init__(self, cursor: _AdjCursor) -> None:
        self.cursor_obj = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self.cursor_obj


def test_fetch_qfq_entry_adjustments_uses_market_adj_factor(monkeypatch):
    cursor = _AdjCursor()
    monkeypatch.setattr("backend.db.pg_pool.get_conn", lambda: _AdjConn(cursor))

    result = watchlist_service._fetch_qfq_entry_adjustments(
        [
            {
                "code": "000001.SZ",
                "entry_price": 20.0,
                "entry_as_of": "2024-01-02",
                "created_at": "2024-01-03T09:30:00",
            }
        ]
    )

    assert "market.adj_factor" in cursor.sql
    assert "selection.aggregate_result" in cursor.sql
    assert cursor.params[0] == ["000001.SZ"]
    assert cursor.params[2] == [""]
    assert result["000001.SZ"]["entry_price_basis"] == "qfq_adjusted"
    assert result["000001.SZ"]["entry_price_adjusted"] == 10.0
    assert result["000001.SZ"]["entry_adjustment_factor"] == 0.5
    assert result["000001.SZ"]["entry_price_basis_date"] == "2024-01-02"
    assert result["000001.SZ"]["entry_price_basis_source"] == "entry_as_of"


def test_fetch_qfq_entry_adjustments_prefers_selection_reference_date(monkeypatch):
    class SelectionBasisCursor(_AdjCursor):
        def fetchall(self):
            return [
                (
                    "301312.SZ",
                    date(2026, 6, 3),
                    date(2026, 6, 2),
                    "selection_reference_date",
                    Decimal("3.0176"),
                    date(2026, 6, 2),
                    Decimal("4.2343"),
                    date(2026, 6, 4),
                )
            ]

    cursor = SelectionBasisCursor()
    monkeypatch.setattr("backend.db.pg_pool.get_conn", lambda: _AdjConn(cursor))

    result = watchlist_service._fetch_qfq_entry_adjustments(
        [
            {
                "code": "301312.SZ",
                "entry_price": 133.08,
                "entry_as_of": "2026-06-03",
                "entry_task_id": "sel_ac2346a1225d4e59b6ed421980698e31",
            }
        ]
    )

    assert cursor.params[0] == ["301312.SZ"]
    assert cursor.params[1] == [date(2026, 6, 3)]
    assert cursor.params[2] == ["sel_ac2346a1225d4e59b6ed421980698e31"]
    assert result["301312.SZ"]["entry_price_adjusted"] == pytest.approx(94.840282, rel=1e-6)
    assert result["301312.SZ"]["entry_adjustment_factor"] == pytest.approx(3.0176 / 4.2343)
    assert result["301312.SZ"]["entry_price_basis_date"] == "2026-06-02"
    assert result["301312.SZ"]["entry_price_basis_source"] == "selection_reference_date"


def test_fetch_qfq_entry_adjustments_marks_query_failure(monkeypatch):
    def _raise_conn():
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("backend.db.pg_pool.get_conn", _raise_conn)

    result = watchlist_service._fetch_qfq_entry_adjustments(
        [{"code": "000001.SZ", "entry_price": 20.0, "entry_as_of": "2024-01-02"}]
    )

    assert result["000001.SZ"]["entry_price_basis"] == "raw_fallback_adjustment_query_failed"


def test_list_items_with_quotes_returns_adjusted_entry_metadata(monkeypatch):
    monkeypatch.setattr(
        watchlist_service.watchlist_repo,
        "list_items",
        lambda **_kwargs: {
            "total": 1,
            "items": [
                {
                    "id": 1,
                    "code": "000001.SZ",
                    "entry_price": Decimal("20.0"),
                    "entry_as_of": "2024-01-02",
                }
            ],
        },
    )
    monkeypatch.setattr(
        watchlist_service,
        "_fetch_quotes",
        lambda _codes: {"000001.SZ": {"price": 10.0, "pre_close": 9.8}},
    )
    monkeypatch.setattr(
        watchlist_service,
        "_fetch_qfq_entry_adjustments",
        lambda _items: {
            "000001.SZ": {
                "entry_price_basis": "qfq_adjusted",
                "entry_price_adjusted": 5.0,
                "entry_adjustment_factor": 0.25,
                "entry_price_basis_date": "2024-01-02",
                "entry_price_basis_source": "entry_as_of",
                "entry_adj_factor_date": "2024-01-02",
                "latest_adj_factor_date": "2024-06-03",
            }
        },
    )

    result = watchlist_service.list_items_with_quotes()
    row = result["items"][0]

    assert row["pct_since_entry"] == 100.0
    assert row["entry_price_basis"] == "qfq_adjusted"
    assert row["entry_price_adjusted"] == 5.0
    assert row["entry_price_basis_date"] == "2024-01-02"
    assert row["entry_price_basis_source"] == "entry_as_of"
