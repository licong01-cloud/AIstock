from __future__ import annotations

from backend.core import data_source_manager_impl
from backend.core.data_source_manager_impl import DataSourceManager
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
