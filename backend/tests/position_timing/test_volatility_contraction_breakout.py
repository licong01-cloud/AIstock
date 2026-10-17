from decimal import Decimal

import numpy as np
import pandas as pd

from backend.services.position_timing.action_value_corporate_actions import (
    CorporateAction,
    CorporateActionBook,
)
from backend.services.position_timing.action_value import cutoff_on
from backend.services.position_timing.contracts import canonical_sha256
from backend.services.position_timing.volatility_contraction_breakout import (
    FEATURE_COLUMNS,
    RULE_SPEC_SHA256,
    observe_volatility_contraction_breakout,
    volatility_contraction_breakout_features,
)


def _bars(periods: int = 320) -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=periods)
    close = np.linspace(9.5, 10.0, periods)
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


def _observation_frame(**overrides: float) -> pd.DataFrame:
    row = {
        "adjusted_close": 10.5,
        "atr_fraction": 0.02,
        "prior_atr_fraction": 0.01,
        "prior_volatility_q30": 0.01,
        "prior_volume_mean5": 800.0,
        "prior_volume_mean20": 1_000.0,
        "prior_range_high20": 10.0,
    }
    row.update(overrides)
    return pd.DataFrame([row], columns=FEATURE_COLUMNS)


def test_frozen_signal_boundaries_and_rule_identity_are_explicit():
    observation = observe_volatility_contraction_breakout(_observation_frame(), 0)
    assert observation.status == "AVAILABLE_MATCH"
    assert observation.matched
    assert len(RULE_SPEC_SHA256) == 64

    assert not observe_volatility_contraction_breakout(
        _observation_frame(prior_volume_mean5=1_000.0), 0
    ).matched
    assert not observe_volatility_contraction_breakout(
        _observation_frame(adjusted_close=10.0), 0
    ).matched
    assert not observe_volatility_contraction_breakout(
        _observation_frame(prior_atr_fraction=0.010001), 0
    ).matched


def test_missing_or_invalid_signal_inputs_are_not_no_signal():
    missing = observe_volatility_contraction_breakout(
        _observation_frame(prior_range_high20=np.nan), 0
    )
    assert missing.status == "UNAVAILABLE"
    assert missing.reason == "VCB_SIGNAL_INPUT_UNAVAILABLE"

    invalid = observe_volatility_contraction_breakout(
        _observation_frame(prior_volume_mean20=0.0), 0
    )
    assert invalid.status == "UNAVAILABLE"
    assert invalid.reason == "VCB_SIGNAL_INPUT_INVALID"


def test_feature_frame_is_causal_and_does_not_compress_suspension_clock():
    bars = _bars()
    prefix = volatility_contraction_breakout_features(
        bars.iloc[:300], symbol="000001.SZ"
    )
    changed = bars.copy()
    changed.iloc[305:, changed.columns.get_loc("close")] = 99.0
    full = volatility_contraction_breakout_features(changed, symbol="000001.SZ")
    pd.testing.assert_frame_equal(prefix, full.iloc[:300])

    suspended = bars.copy()
    suspended.iloc[270, suspended.columns.get_loc("is_suspended")] = True
    features = volatility_contraction_breakout_features(
        suspended, symbol="000001.SZ"
    )
    assert np.isnan(features.iloc[270].adjusted_close)
    assert np.isnan(features.iloc[271].prior_atr_fraction)
    assert np.isnan(features.iloc[319].prior_volatility_q30)


def test_split_adjustment_does_not_create_a_false_price_breakout():
    bars = _bars()
    split_ordinal = len(bars) - 1
    split_date = bars.index[split_ordinal].date()
    bars.iloc[split_ordinal, bars.columns.get_loc("open")] = 5.0
    bars.iloc[split_ordinal, bars.columns.get_loc("high")] = 5.1
    bars.iloc[split_ordinal, bars.columns.get_loc("low")] = 4.9
    bars.iloc[split_ordinal, bars.columns.get_loc("close")] = 5.0
    bars.iloc[split_ordinal, bars.columns.get_loc("volume")] = 2_000.0
    bars.iloc[split_ordinal, bars.columns.get_loc("factor")] = 2.0
    action = CorporateAction(
        symbol="000001.SZ",
        effective_trade_date=split_date,
        quantity_multiplier=Decimal("2"),
        cashflow_yuan_per_share=Decimal("0"),
        reference_price_cash_yuan_per_share=Decimal("0"),
        cash_pay_date=None,
        share_listing_date=split_date,
        source_available_at=cutoff_on(bars.index[split_ordinal - 1].date()),
        source_row_count=1,
        source_rows_sha256="a" * 64,
    )
    book = CorporateActionBook((action,), canonical_sha256({"test": "split"}))

    features = volatility_contraction_breakout_features(
        bars,
        symbol="000001.SZ",
        corporate_actions=book,
    )
    observation = observe_volatility_contraction_breakout(features, split_ordinal)
    assert observation.status != "AVAILABLE_MATCH"
    assert observation.values["adjusted_close"] == 10.0
