from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.suspension_aware_bar_policy import (
    BAR_STATE_SUSPENDED_VERIFIED,
    BAR_STATE_TRADED,
    build_suspension_aware_bar_panel,
)


def _daily(dates: pd.DatetimeIndex, closes: list[float | None]) -> pd.DataFrame:
    index = pd.MultiIndex.from_product([dates, ["000001.SZ"]], names=["datetime", "instrument"])
    close = pd.Series(closes, index=index, dtype=float)
    return pd.DataFrame(
        {
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": [100.0 if value is not None else None for value in closes],
            "amount": [1000.0 if value is not None else None for value in closes],
            "factor": 1.0,
        },
        index=index,
    )


def _suspend(date: pd.Timestamp) -> pd.DataFrame:
    return pd.DataFrame({"trade_date": [date], "instrument": ["000001.SZ"], "suspend_type": ["S"]})


def test_verified_suspension_creates_zero_liquidity_fixed_session_row() -> None:
    dates = pd.bdate_range("2024-01-02", periods=4)
    daily = _daily(dates, [10.0, None, None, 12.0]).drop(index=(dates[1], "000001.SZ"))
    suspend = pd.concat([_suspend(dates[1]), _suspend(dates[2])], ignore_index=True)

    result = build_suspension_aware_bar_panel(daily=daily, suspend_rows=suspend, trading_calendar=dates)

    suspended = result.panel.loc[(dates[1], "000001.SZ")]
    stale = result.panel.loc[(dates[2], "000001.SZ")]
    resumed = result.panel.loc[(dates[3], "000001.SZ")]
    assert suspended[["open", "high", "low", "close"]].tolist() == [10.0] * 4
    assert stale[["open", "high", "low", "close"]].tolist() == [10.0] * 4
    assert suspended[["volume", "amount"]].tolist() == [0.0, 0.0]
    assert suspended["bar_state"] == BAR_STATE_SUSPENDED_VERIFIED
    assert resumed["bar_state"] == BAR_STATE_TRADED
    assert resumed["close"] == 12.0
    assert result.receipt["suspended_verified_row_count"] == 2


def test_sparse_full_day_suspension_continues_causally_until_traded_bar() -> None:
    dates = pd.bdate_range("2024-01-02", periods=5)
    daily = _daily(dates, [10.0, None, None, None, 12.0])

    result = build_suspension_aware_bar_panel(
        daily=daily,
        suspend_rows=_suspend(dates[1]),
        trading_calendar=dates,
    )

    for date in dates[1:4]:
        row = result.panel.loc[(date, "000001.SZ")]
        assert row[["open", "high", "low", "close"]].tolist() == [10.0] * 4
        assert row[["volume", "amount"]].tolist() == [0.0, 0.0]
        assert row["bar_state"] == BAR_STATE_SUSPENDED_VERIFIED
    assert result.panel.loc[(dates[4], "000001.SZ"), "bar_state"] == BAR_STATE_TRADED
    assert result.panel.loc[(dates[4], "000001.SZ"), "close"] == 12.0
    assert result.receipt["explicit_suspended_row_count"] == 1
    assert result.receipt["continued_suspended_row_count"] == 2
    assert result.receipt["suspended_verified_row_count"] == 3


def test_sparse_suspension_continuation_does_not_read_future_resume_price() -> None:
    dates = pd.bdate_range("2024-01-02", periods=4)
    suspend = _suspend(dates[1])
    baseline = build_suspension_aware_bar_panel(
        daily=_daily(dates, [10.0, None, None, 11.0]),
        suspend_rows=suspend,
        trading_calendar=dates,
    ).panel.loc[(dates[2], "000001.SZ"), "close"]
    poisoned = build_suspension_aware_bar_panel(
        daily=_daily(dates, [10.0, None, None, 9999.0]),
        suspend_rows=suspend,
        trading_calendar=dates,
    ).panel.loc[(dates[2], "000001.SZ"), "close"]
    assert baseline == poisoned == 10.0


def test_future_resume_price_cannot_leak_into_suspension_value() -> None:
    dates = pd.bdate_range("2024-01-02", periods=3)
    suspend = _suspend(dates[1])
    baseline = build_suspension_aware_bar_panel(
        daily=_daily(dates, [10.0, None, 11.0]),
        suspend_rows=suspend,
        trading_calendar=dates,
    ).panel.loc[(dates[1], "000001.SZ"), "close"]
    poisoned = build_suspension_aware_bar_panel(
        daily=_daily(dates, [10.0, None, 9999.0]),
        suspend_rows=suspend,
        trading_calendar=dates,
    ).panel.loc[(dates[1], "000001.SZ"), "close"]
    assert baseline == poisoned == 10.0


def test_stale_same_day_ohlc_is_ignored_when_sidecar_verifies_suspension() -> None:
    dates = pd.bdate_range("2024-01-02", periods=3)
    daily = _daily(dates, [10.0, 777.0, 12.0])
    daily.loc[(dates[1], "000001.SZ"), ["volume", "amount"]] = 0.0
    result = build_suspension_aware_bar_panel(daily=daily, suspend_rows=_suspend(dates[1]), trading_calendar=dates)
    assert result.panel.loc[(dates[1], "000001.SZ"), "close"] == 10.0


def test_positive_liquidity_conflict_fails_closed() -> None:
    dates = pd.bdate_range("2024-01-02", periods=2)
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        build_suspension_aware_bar_panel(
            daily=_daily(dates, [10.0, 11.0]),
            suspend_rows=_suspend(dates[1]),
            trading_calendar=dates,
        )
    assert exc_info.value.reason_code == "ADVISORY_SUSPENSION_SOURCE_CONFLICT"
    assert exc_info.value.context["bar_state"] == "SOURCE_CONFLICT"


def test_unexplained_missing_session_fails_instead_of_dropping_instrument() -> None:
    dates = pd.bdate_range("2024-01-02", periods=2)
    daily = _daily(dates, [10.0, None])
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        build_suspension_aware_bar_panel(
            daily=daily,
            suspend_rows=pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"]),
            trading_calendar=dates,
        )
    assert exc_info.value.reason_code == "ADVISORY_SUSPENSION_UNEXPLAINED_MISSING"
    assert exc_info.value.context["bar_state"] == "MISSING_UNEXPLAINED"


def test_negative_liquidity_is_not_hidden_by_prior_suspension_state() -> None:
    dates = pd.bdate_range("2024-01-02", periods=3)
    daily = _daily(dates, [10.0, None, 10.0])
    daily.loc[(dates[2], "000001.SZ"), ["volume", "amount"]] = [-1.0, -1.0]
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        build_suspension_aware_bar_panel(
            daily=daily,
            suspend_rows=_suspend(dates[1]),
            trading_calendar=dates,
        )
    assert exc_info.value.reason_code == "ADVISORY_SUSPENSION_SOURCE_CONFLICT"


def test_leading_suspension_without_in_window_anchor_starts_at_first_executable_bar() -> None:
    dates = pd.bdate_range("2024-01-02", periods=2)
    daily = _daily(dates, [None, 11.0])
    result = build_suspension_aware_bar_panel(
        daily=daily,
        suspend_rows=_suspend(dates[0]),
        trading_calendar=dates,
    )
    assert result.panel.index.tolist() == [(dates[1], "000001.SZ")]
    assert result.panel.iloc[0]["bar_state"] == BAR_STATE_TRADED


def test_instrument_without_any_executable_bar_fails_closed() -> None:
    dates = pd.bdate_range("2024-01-02", periods=2)
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        build_suspension_aware_bar_panel(
            daily=_daily(dates, [None, None]),
            suspend_rows=_suspend(dates[0]),
            trading_calendar=dates,
        )
    assert exc_info.value.reason_code == "ADVISORY_SUSPENSION_LAST_CLOSE_UNAVAILABLE"


def test_non_suspended_zero_price_is_not_guessed_as_suspension() -> None:
    dates = pd.bdate_range("2024-01-02", periods=2)
    daily = _daily(dates, [10.0, 0.0])
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        build_suspension_aware_bar_panel(
            daily=daily,
            suspend_rows=pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"]),
            trading_calendar=dates,
        )
    assert exc_info.value.reason_code == "ADVISORY_SUSPENSION_SOURCE_CONFLICT"
