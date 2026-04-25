from __future__ import annotations

from datetime import date

import pytest

from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.limit_price_provider import (
    StkLimitPriceProvider,
    limit_rows_conn,
)


def test_stk_limit_price_provider_loads_required_rows() -> None:
    provider = StkLimitPriceProvider(
        conn_factory=lambda: limit_rows_conn(
            [("000001.SZ", 10.0, 11.0, 9.0)]
        )
    )

    price = provider.get_limit_price("000001.SZ", date(2024, 1, 2))

    assert price.symbol == "000001.SZ"
    assert price.pre_close == 10.0
    assert price.up_limit == 11.0
    assert price.down_limit == 9.0


def test_stk_limit_price_provider_fails_when_row_is_missing() -> None:
    provider = StkLimitPriceProvider(conn_factory=lambda: limit_rows_conn([]))

    with pytest.raises(DataUnavailableError, match="missing limit price rows"):
        provider.get_limit_price("000001.SZ", date(2024, 1, 2))


def test_stk_limit_price_provider_fails_on_invalid_range() -> None:
    provider = StkLimitPriceProvider(
        conn_factory=lambda: limit_rows_conn(
            [("000001.SZ", 10.0, 9.0, 11.0)]
        )
    )

    with pytest.raises(DataUnavailableError, match="invalid limit price range"):
        provider.get_limit_price("000001.SZ", date(2024, 1, 2))
