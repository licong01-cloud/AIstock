from __future__ import annotations

from datetime import date

import pytest

from backend.services.hmm_evolution.errors import MarketDataUnavailableError
from backend.services.hmm_evolution.market_repository import HMMMarketReturnRepository


@pytest.mark.integration
def test_market_repository_real_dev_postgres_readonly_trading_day_smoke(
    hmm_evolution_dev_conn_factory,
) -> None:
    repository = HMMMarketReturnRepository(hmm_evolution_dev_conn_factory)
    with pytest.raises(MarketDataUnavailableError, match="no completed dates"):
        repository.resolve_watermark(
            policy="latest_common_completed",
            requested_date=None,
        )

    with hmm_evolution_dev_conn_factory() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute(
                """
                SELECT cal_date
                FROM market.trading_calendar
                WHERE is_trading = TRUE
                ORDER BY cal_date DESC
                LIMIT 1 OFFSET 15
                """
            )
            trade_date_row = cursor.fetchone()
            assert trade_date_row is not None
            trade_date: date = trade_date_row[0]
            cursor.execute(
                """
                SELECT MAX(cal_date)
                FROM market.trading_calendar
                WHERE is_trading = TRUE
                """
            )
            as_of_date: date = cursor.fetchone()[0]

    read = repository.read_forward_returns(
        symbols=["000001.SZ"],
        trade_dates=[trade_date],
        horizon_trading_days=10,
        as_of_date=as_of_date,
    )

    assert read.returns.empty
    assert read.read_only_transaction["transaction_read_only"] is True
    assert read.price_row_count == 0
