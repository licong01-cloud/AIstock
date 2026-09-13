"""Pure daily features for the frozen PT-NEXT-019 entry-timing rule."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .action_value_corporate_actions import CorporateActionBook
from .contracts import canonical_sha256
from .pattern_strategy import pattern_feature_frame


STRATEGY_ID = "VOLATILITY_CONTRACTION_BREAKOUT_V1"
ATR_WINDOW = 14
RANGE_WINDOW = 20
VOLUME_SHORT_WINDOW = 5
VOLUME_LONG_WINDOW = 20
VOLATILITY_HISTORY_WINDOW = 252
VOLATILITY_QUANTILE = 0.30

RULE_SPEC: Mapping[str, Any] = {
    "schema_version": "position_timing_volatility_contraction_breakout_rule_v1",
    "strategy_id": STRATEGY_ID,
    "decision_clock": "T_20_00_ASIA_SHANGHAI",
    "target_session": "T_PLUS_1",
    "adjusted_price_formula": "RAW_OHLC_MULTIPLIED_BY_FACTOR",
    "atr_window_sessions": ATR_WINDOW,
    "range_window_sessions": RANGE_WINDOW,
    "volume_short_window_sessions": VOLUME_SHORT_WINDOW,
    "volume_long_window_sessions": VOLUME_LONG_WINDOW,
    "volatility_history_window_sessions": VOLATILITY_HISTORY_WINDOW,
    "volatility_quantile": VOLATILITY_QUANTILE,
    "volatility_comparison": "ATR14_OVER_CLOSE_T_MINUS_1_LE_PRIOR_252_Q30_ENDING_T_MINUS_2",
    "volume_comparison": "MEAN_T_MINUS_5_TO_T_MINUS_1_LT_MEAN_T_MINUS_20_TO_T_MINUS_1",
    "breakout_comparison": "CLOSE_T_GT_MAX_HIGH_T_MINUS_20_TO_T_MINUS_1",
    "entry_action": "MAX_BUDGETED_OPEN_NEXT_SESSION",
    "holding_exit_authority": "FROZEN_RISK_EXIT_ONLY",
}
RULE_SPEC_SHA256 = canonical_sha256(RULE_SPEC)

FEATURE_COLUMNS: tuple[str, ...] = (
    "adjusted_close",
    "atr_fraction",
    "prior_atr_fraction",
    "prior_volatility_q30",
    "prior_volume_mean5",
    "prior_volume_mean20",
    "prior_range_high20",
)


@dataclass(frozen=True)
class SignalObservation:
    status: str
    reason: str
    matched: bool
    values: Mapping[str, float | None]

    def __post_init__(self) -> None:
        if self.status not in {"AVAILABLE_MATCH", "AVAILABLE_NO_MATCH", "UNAVAILABLE"}:
            raise ActionValueError("VCB_SIGNAL_STATUS_INVALID", status=self.status)
        if self.matched != (self.status == "AVAILABLE_MATCH"):
            raise ActionValueError("VCB_SIGNAL_MATCH_STATUS_INCONSISTENT")

    @property
    def identity(self) -> Mapping[str, Any]:
        return asdict(self)


def volatility_contraction_breakout_features(
    bars: pd.DataFrame,
    *,
    symbol: str,
    corporate_actions: CorporateActionBook | None = None,
) -> pd.DataFrame:
    """Build the frozen rule inputs without compressing missing sessions."""

    base = pattern_feature_frame(
        bars,
        symbol=symbol,
        corporate_actions=corporate_actions,
    )
    close = pd.to_numeric(base["adjusted_close"], errors="coerce")
    high = pd.to_numeric(base["adjusted_high"], errors="coerce")
    atr = pd.to_numeric(base["atr14"], errors="coerce")
    volume = pd.to_numeric(base["comparable_volume"], errors="coerce")

    atr_fraction = (atr / close).where(close.gt(0))
    result = pd.DataFrame(index=base.index)
    result["adjusted_close"] = close
    result["atr_fraction"] = atr_fraction
    result["prior_atr_fraction"] = atr_fraction.shift(1)
    result["prior_volatility_q30"] = (
        atr_fraction.shift(2)
        .rolling(VOLATILITY_HISTORY_WINDOW, min_periods=VOLATILITY_HISTORY_WINDOW)
        .quantile(VOLATILITY_QUANTILE)
    )
    result["prior_volume_mean5"] = (
        volume.shift(1)
        .rolling(VOLUME_SHORT_WINDOW, min_periods=VOLUME_SHORT_WINDOW)
        .mean()
    )
    result["prior_volume_mean20"] = (
        volume.shift(1)
        .rolling(VOLUME_LONG_WINDOW, min_periods=VOLUME_LONG_WINDOW)
        .mean()
    )
    result["prior_range_high20"] = (
        high.shift(1).rolling(RANGE_WINDOW, min_periods=RANGE_WINDOW).max()
    )
    return result.loc[:, FEATURE_COLUMNS].replace([np.inf, -np.inf], np.nan)


def observe_volatility_contraction_breakout(
    features: pd.DataFrame,
    ordinal: int,
) -> SignalObservation:
    if tuple(features.columns) != FEATURE_COLUMNS:
        raise ActionValueError("VCB_FEATURE_SCHEMA_INVALID")
    if ordinal < 0 or ordinal >= len(features):
        raise ActionValueError("VCB_SIGNAL_ORDINAL_INVALID", ordinal=ordinal)

    current = features.iloc[ordinal]
    values = {
        name: (float(current[name]) if pd.notna(current[name]) else None)
        for name in FEATURE_COLUMNS
    }
    if any(value is None or not np.isfinite(value) for value in values.values()):
        return SignalObservation(
            status="UNAVAILABLE",
            reason="VCB_SIGNAL_INPUT_UNAVAILABLE",
            matched=False,
            values=values,
        )
    if (
        values["adjusted_close"] <= 0
        or values["prior_atr_fraction"] < 0
        or values["prior_volatility_q30"] < 0
        or values["prior_volume_mean5"] < 0
        or values["prior_volume_mean20"] <= 0
        or values["prior_range_high20"] <= 0
    ):
        return SignalObservation(
            status="UNAVAILABLE",
            reason="VCB_SIGNAL_INPUT_INVALID",
            matched=False,
            values=values,
        )

    low_volatility = values["prior_atr_fraction"] <= values["prior_volatility_q30"]
    volume_contraction = values["prior_volume_mean5"] < values["prior_volume_mean20"]
    range_breakout = values["adjusted_close"] > values["prior_range_high20"]
    matched = bool(low_volatility and volume_contraction and range_breakout)
    return SignalObservation(
        status="AVAILABLE_MATCH" if matched else "AVAILABLE_NO_MATCH",
        reason="VCB_ALL_CONDITIONS_MET" if matched else "VCB_CONDITIONS_NOT_MET",
        matched=matched,
        values={
            **values,
            "low_volatility": float(low_volatility),
            "volume_contraction": float(volume_contraction),
            "range_breakout": float(range_breakout),
        },
    )
