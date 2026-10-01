from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from backend.services.position_timing.action_value_corporate_actions import (
    CorporateAction,
    CorporateActionBook,
)
from backend.services.position_timing.contracts import canonical_sha256
from backend.services.position_timing.pattern_strategy import (
    BreakoutEvent,
    acceleration_volume_exit,
    advance_entry_event,
    breakout_observed,
    pattern_feature_frame,
)


def _bars(periods: int = 60) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    close = np.linspace(9.0, 10.0, periods)
    return pd.DataFrame(
        {
            "open": close,
            "high": close + 0.2,
            "low": close - 0.2,
            "close": close,
            "volume": 1_000.0,
            "factor": 1.0,
            "is_suspended": False,
            "pit_active": True,
        },
        index=dates,
    )


def test_pattern_features_are_causal_and_suspension_does_not_compress_clock():
    bars = _bars()
    prefix = pattern_feature_frame(bars.iloc[:45], symbol="000001.SZ")
    changed = bars.copy()
    changed.iloc[50:, changed.columns.get_loc("close")] = 99.0
    full = pattern_feature_frame(changed, symbol="000001.SZ")
    pd.testing.assert_frame_equal(prefix, full.iloc[:45])

    suspended = bars.copy()
    suspended.iloc[25, suspended.columns.get_loc("is_suspended")] = True
    features = pattern_feature_frame(suspended, symbol="000001.SZ")
    assert np.isnan(features.iloc[25].adjusted_close)
    assert np.isnan(features.iloc[44].low20_prior)
    assert np.isfinite(features.iloc[46].low20_prior)


def test_volume_is_comparable_across_share_distribution():
    bars = _bars(45)
    split_date = bars.index[30].date()
    bars.loc[bars.index[30] :, "volume"] = 2_000.0
    action = CorporateAction(
        symbol="000001.SZ",
        effective_trade_date=split_date,
        quantity_multiplier=Decimal("2"),
        cashflow_yuan_per_share=Decimal("0"),
        reference_price_cash_yuan_per_share=Decimal("0"),
        cash_pay_date=None,
        share_listing_date=split_date,
        source_available_at=datetime(2024, 1, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
        source_row_count=1,
        source_rows_sha256="a" * 64,
    )
    book = CorporateActionBook((action,), canonical_sha256({"test": "split"}))
    features = pattern_feature_frame(bars, symbol="000001.SZ", corporate_actions=book)
    assert features.iloc[29].comparable_volume == features.iloc[30].comparable_volume
    assert features.iloc[35].volume_ratio == 1.0


def test_breakout_and_pullback_boundary_are_explicit():
    dates = pd.bdate_range("2024-01-02", periods=12)
    frame = pd.DataFrame(index=dates, columns=["adjusted_low", "adjusted_close", "sma5", "sma10", "atr14"], dtype=float)
    frame.loc[:, :] = [9.9, 10.0, 10.0, 10.0, 1.0]
    frame["low20_prior"] = 8.0
    frame["low20_age"] = 5.0
    frame.iloc[0, frame.columns.get_loc("adjusted_close")] = 9.9
    frame.iloc[1, frame.columns.get_loc("adjusted_close")] = 10.2
    assert breakout_observed(frame, 1)

    event = BreakoutEvent(1, "R0", 100)
    for ordinal in range(2, 6):
        transition = advance_entry_event(frame, event, ordinal)
        assert transition.state == "PULLBACK_WAITING"
    frame.iloc[6, frame.columns.get_loc("adjusted_low")] = 10.0
    frame.iloc[6, frame.columns.get_loc("adjusted_close")] = 10.4
    frame.iloc[6, frame.columns.get_loc("sma5")] = 10.2
    frame.iloc[6, frame.columns.get_loc("sma10")] = 10.1
    transition = advance_entry_event(frame, event, 6)
    assert transition.state == "PULLBACK_CONFIRMED"


def test_invalidation_precedes_confirmation_and_r2_has_eight_day_clock():
    dates = pd.bdate_range("2024-01-02", periods=12)
    frame = pd.DataFrame(
        {
            "adjusted_low": [10.0] * 12,
            "adjusted_close": [10.2] * 12,
            "sma5": np.linspace(10.0, 10.5, 12),
            "sma10": np.linspace(9.9, 10.4, 12),
            "atr14": [1.0] * 12,
        },
        index=dates,
    )
    event = BreakoutEvent(1, "R0")
    frame.iloc[2, frame.columns.get_loc("adjusted_close")] = 9.0
    assert advance_entry_event(frame, event, 2).state == "PULLBACK_INVALIDATED"
    event_r2 = BreakoutEvent(1, "R2")
    assert advance_entry_event(frame.assign(adjusted_low=20.0), event_r2, 7).state == "PULLBACK_WAITING"
    assert advance_entry_event(frame.assign(adjusted_low=20.0), event_r2, 9).state == "PULLBACK_WINDOW_EXPIRED"


def test_acceleration_exit_requires_volume_and_optional_exhaustion():
    dates = pd.bdate_range("2024-01-02", periods=10)
    frame = pd.DataFrame(
        {
            "adjusted_close": np.arange(10.0, 20.0),
            "sma5": np.arange(10.0, 20.0),
            "sma10": np.arange(9.0, 19.0),
            "atr14": [1.0] * 10,
            "acceleration_atr": [0.3] * 10,
            "distance_ma10_atr": [2.0] * 10,
            "volume_ratio": [2.0] * 10,
            "close_position": [0.8] * 10,
            "upper_wick_fraction": [0.1] * 10,
        },
        index=dates,
    )
    assert acceleration_volume_exit(frame, 9, "R0")[0]
    assert not acceleration_volume_exit(frame, 9, "R6")[0]
    frame.iloc[9, frame.columns.get_loc("upper_wick_fraction")] = 0.6
    assert acceleration_volume_exit(frame, 9, "R6")[0]
