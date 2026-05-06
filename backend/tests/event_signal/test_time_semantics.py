import datetime as dt

import pytest

from backend.services.event_signal.time_semantics import (
    CHINA_TZ,
    DATE_ONLY,
    EXACT,
    LOCAL_FIRST_SEEN,
    MIDNIGHT_DEFAULT,
    OBSERVED,
    compute_event_time,
)


TRADING_DAYS = [
    dt.date(2026, 5, 6),
    dt.date(2026, 5, 7),
    dt.date(2026, 5, 8),
    dt.date(2026, 5, 11),
]


def test_backtest_date_only_ignores_local_first_seen_and_uses_next_trading_day():
    result = compute_event_time(
        dt.date(2026, 5, 7),
        TRADING_DAYS,
        time_mode="backtest",
        first_seen_at=dt.datetime(2026, 5, 6, 20, 20, tzinfo=CHINA_TZ),
    )

    assert result.source_time_quality == DATE_ONLY
    assert result.available_at is None
    assert result.effective_trade_date == dt.date(2026, 5, 8)
    assert result.effective_rule == "tushare_date_only_next_trading_day"


def test_backtest_exact_preopen_publish_time_is_same_trading_day():
    result = compute_event_time(
        dt.date(2026, 5, 7),
        TRADING_DAYS,
        time_mode="backtest",
        source_publish_time=dt.datetime(2026, 5, 7, 8, 0, tzinfo=CHINA_TZ),
    )

    assert result.source_time_quality == EXACT
    assert result.available_at == dt.datetime(2026, 5, 7, 8, 0, tzinfo=CHINA_TZ)
    assert result.source_available_at == result.available_at
    assert result.effective_trade_date == dt.date(2026, 5, 7)
    assert result.effective_rule == "exact_publish_time_before_preopen"


def test_backtest_exact_after_preopen_publish_time_is_next_trading_day():
    result = compute_event_time(
        dt.date(2026, 5, 7),
        TRADING_DAYS,
        time_mode="backtest",
        source_publish_time=dt.datetime(2026, 5, 7, 20, 0, tzinfo=CHINA_TZ),
    )

    assert result.source_time_quality == EXACT
    assert result.effective_trade_date == dt.date(2026, 5, 8)
    assert result.effective_rule == "exact_publish_time_after_preopen_next_trading_day"


def test_backtest_midnight_publish_time_defaults_to_next_trading_day():
    result = compute_event_time(
        dt.date(2026, 5, 7),
        TRADING_DAYS,
        time_mode="backtest",
        source_publish_time=dt.datetime(2026, 5, 7, 0, 0, tzinfo=CHINA_TZ),
    )

    assert result.source_time_quality == MIDNIGHT_DEFAULT
    assert result.available_at == dt.datetime(2026, 5, 7, 0, 0, tzinfo=CHINA_TZ)
    assert result.effective_trade_date == dt.date(2026, 5, 8)
    assert result.effective_rule == "midnight_default_next_trading_day"


def test_paper_future_ann_date_can_use_actual_prior_observation_time():
    result = compute_event_time(
        dt.date(2026, 5, 7),
        TRADING_DAYS,
        time_mode="paper",
        first_seen_at=dt.datetime(2026, 5, 6, 12, 20, tzinfo=dt.timezone.utc),
    )

    assert result.source_time_quality == LOCAL_FIRST_SEEN
    assert result.available_at == dt.datetime(2026, 5, 6, 20, 20, tzinfo=CHINA_TZ)
    assert result.effective_trade_date == dt.date(2026, 5, 7)
    assert result.effective_rule == "local_first_seen_after_preopen_next_trading_day"


def test_paper_preopen_local_first_seen_is_same_trading_day():
    result = compute_event_time(
        dt.date(2026, 5, 7),
        TRADING_DAYS,
        time_mode="live",
        first_seen_at=dt.datetime(2026, 5, 7, 7, 0),
    )

    assert result.source_time_quality == LOCAL_FIRST_SEEN
    assert result.available_at == dt.datetime(2026, 5, 7, 7, 0, tzinfo=CHINA_TZ)
    assert result.effective_trade_date == dt.date(2026, 5, 7)
    assert result.effective_rule == "local_first_seen_before_preopen"


def test_paper_without_observation_time_falls_back_to_date_only_conservative_rule():
    result = compute_event_time(
        dt.date(2026, 5, 7),
        TRADING_DAYS,
        time_mode="paper",
    )

    assert result.source_time_quality == DATE_ONLY
    assert result.available_at is None
    assert result.effective_trade_date == dt.date(2026, 5, 8)


def test_observed_mode_prefers_latest_observed_at_for_audit():
    result = compute_event_time(
        dt.date(2026, 5, 7),
        TRADING_DAYS,
        time_mode="observed",
        first_seen_at=dt.datetime(2026, 5, 7, 7, 0, tzinfo=CHINA_TZ),
        observed_at=dt.datetime(2026, 5, 7, 16, 30, tzinfo=CHINA_TZ),
    )

    assert result.source_time_quality == OBSERVED
    assert result.available_at == dt.datetime(2026, 5, 7, 16, 30, tzinfo=CHINA_TZ)
    assert result.effective_trade_date == dt.date(2026, 5, 8)


def test_missing_trading_day_raises_clear_error():
    with pytest.raises(ValueError, match="trading calendar has no effective date"):
        compute_event_time(
            dt.date(2026, 5, 11),
            [dt.date(2026, 5, 6)],
            time_mode="backtest",
        )
