"""Causal daily pattern primitives for PT-NEXT-018.

The module contains no I/O and no serving integration.  Raw CNY prices remain
available to the shared execution engine; pattern prices are adjusted with the
same ``raw * factor`` convention as the existing action-value research.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import ActionValueError, cutoff_on
from .action_value_corporate_actions import CorporateActionBook
from .contracts import canonical_sha256


@dataclass(frozen=True)
class PatternTemplate:
    template_id: str
    pullback_wait_sessions: int = 5
    pullback_tolerance_atr: float = 0.25
    exit_fraction: float = 1.0
    require_exhaustion: bool = False

    def __post_init__(self) -> None:
        if (
            self.template_id not in {f"R{index}" for index in range(8)}
            or self.pullback_wait_sessions not in {3, 5, 8}
            or self.pullback_tolerance_atr not in {0.10, 0.25, 0.50}
            or self.exit_fraction not in {0.5, 1.0}
            or not isinstance(self.require_exhaustion, bool)
        ):
            raise ActionValueError("PATTERN_TEMPLATE_INVALID")

    @property
    def identity(self) -> Mapping[str, Any]:
        return asdict(self)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.identity)


TEMPLATES: tuple[PatternTemplate, ...] = (
    PatternTemplate("R0"),
    PatternTemplate("R1", pullback_wait_sessions=3),
    PatternTemplate("R2", pullback_wait_sessions=8),
    PatternTemplate("R3", pullback_tolerance_atr=0.10),
    PatternTemplate("R4", pullback_tolerance_atr=0.50),
    PatternTemplate("R5", exit_fraction=0.5),
    PatternTemplate("R6", require_exhaustion=True),
    PatternTemplate("R7", exit_fraction=0.5, require_exhaustion=True),
)
TEMPLATE_BY_ID = {item.template_id: item for item in TEMPLATES}
PATTERN_TEMPLATE_SET_SHA256 = canonical_sha256(tuple(item.identity for item in TEMPLATES))


@dataclass(frozen=True)
class BreakoutEvent:
    breakout_ordinal: int
    template_id: str
    planned_quantity: int = 0

    def __post_init__(self) -> None:
        if self.breakout_ordinal < 0 or self.template_id not in TEMPLATE_BY_ID or self.planned_quantity < 0:
            raise ActionValueError("BREAKOUT_EVENT_INVALID")


@dataclass(frozen=True)
class EntryTransition:
    state: str
    event: BreakoutEvent | None
    reason_values: Mapping[str, float | int | None]


PATTERN_FEATURE_COLUMNS: tuple[str, ...] = (
    "adjusted_open",
    "adjusted_high",
    "adjusted_low",
    "adjusted_close",
    "sma5",
    "sma10",
    "atr14",
    "low20_prior",
    "low20_age",
    "comparable_volume",
    "volume_mean20_prior",
    "volume_ratio",
    "acceleration_atr",
    "distance_ma10_atr",
    "close_position",
    "upper_wick_fraction",
)


def pattern_feature_frame(
    bars: pd.DataFrame,
    *,
    symbol: str,
    corporate_actions: CorporateActionBook | None = None,
) -> pd.DataFrame:
    """Build features with no forward fill and no compressed suspension clock."""

    required = {"open", "high", "low", "close", "volume", "factor", "is_suspended", "pit_active"}
    if not required.issubset(bars.columns) or not isinstance(bars.index, pd.DatetimeIndex):
        raise ActionValueError("PATTERN_SOURCE_SCHEMA_INVALID", symbol=symbol)
    if not bars.index.is_monotonic_increasing or not bars.index.is_unique:
        raise ActionValueError("PATTERN_SOURCE_CALENDAR_INVALID", symbol=symbol)

    numeric = bars.loc[:, ["open", "high", "low", "close", "volume", "factor"]].apply(pd.to_numeric, errors="coerce")
    valid = (
        bars["pit_active"].astype(bool)
        & ~bars["is_suspended"].astype(bool)
        & numeric[["open", "high", "low", "close"]].gt(0).all(axis=1)
        & numeric["factor"].gt(0)
        & numeric["volume"].ge(0)
    )
    adjusted = numeric[["open", "high", "low", "close"]].mul(numeric["factor"], axis=0).where(valid)
    adjusted.columns = [f"adjusted_{name}" for name in adjusted.columns]

    # Normalize share units across bonus/split actions only.  The cumulative
    # multiplier is causal and cash dividends do not distort volume.
    scale = np.ones(len(bars), dtype=float)
    action_book = corporate_actions or CorporateActionBook.empty()
    cumulative = 1.0
    action_by_day = {
        item.effective_trade_date: float(item.quantity_multiplier)
        for item in action_book.actions
        if item.symbol == symbol.upper()
    }
    for item in action_book.actions:
        if item.symbol == symbol.upper() and item.source_available_at > cutoff_on(item.effective_trade_date):
            raise ActionValueError(
                "PATTERN_CORPORATE_ACTION_NOT_VISIBLE",
                symbol=symbol,
                effective_trade_date=item.effective_trade_date.isoformat(),
            )
    for ordinal, timestamp in enumerate(bars.index):
        multiplier = action_by_day.get(timestamp.date())
        if multiplier is not None:
            if not np.isfinite(multiplier) or multiplier <= 0:
                raise ActionValueError("PATTERN_VOLUME_ACTION_INVALID", symbol=symbol)
            cumulative *= multiplier
        scale[ordinal] = cumulative
    comparable_volume = (numeric["volume"] / pd.Series(scale, index=bars.index)).where(valid)

    close = adjusted["adjusted_close"]
    high = adjusted["adjusted_high"]
    low = adjusted["adjusted_low"]
    prior_close = close.shift(1)
    true_range = pd.concat([(high - low).abs(), (high - prior_close).abs(), (low - prior_close).abs()], axis=1).max(
        axis=1, skipna=False
    )
    atr14 = true_range.rolling(14, min_periods=14).mean()
    sma5 = close.rolling(5, min_periods=5).mean()
    sma10 = close.rolling(10, min_periods=10).mean()

    prior_lows = low.shift(1)
    low20_prior = prior_lows.rolling(20, min_periods=20).min()
    low20_age = pd.Series(np.nan, index=bars.index, dtype=float)
    values = prior_lows.to_numpy(float)
    for ordinal in range(20, len(bars)):
        window = values[ordinal - 20 : ordinal]
        if np.isfinite(window).all():
            minimum = window.min()
            # Equal lows use the most recent date.
            latest = int(np.flatnonzero(window == minimum)[-1])
            low20_age.iloc[ordinal] = ordinal - (ordinal - 20 + latest)

    volume_mean20 = comparable_volume.shift(1).rolling(20, min_periods=20).mean()
    volume_ratio = comparable_volume / volume_mean20
    acceleration = (((close - close.shift(3)) / 3.0) - ((close.shift(3) - close.shift(8)) / 5.0)) / atr14.shift(1)
    distance = (close - sma10) / atr14
    day_range = high - low
    close_position = ((close - low) / day_range).where(day_range > 0)
    upper_wick = ((high - pd.concat([adjusted["adjusted_open"], close], axis=1).max(axis=1)) / day_range).where(
        day_range > 0
    )

    output = adjusted.assign(
        sma5=sma5,
        sma10=sma10,
        atr14=atr14,
        low20_prior=low20_prior,
        low20_age=low20_age,
        comparable_volume=comparable_volume,
        volume_mean20_prior=volume_mean20,
        volume_ratio=volume_ratio,
        acceleration_atr=acceleration,
        distance_ma10_atr=distance,
        close_position=close_position,
        upper_wick_fraction=upper_wick,
    )
    return output.loc[:, PATTERN_FEATURE_COLUMNS].replace([np.inf, -np.inf], np.nan)


def breakout_observed(features: pd.DataFrame, ordinal: int) -> bool:
    if ordinal <= 0 or ordinal >= len(features):
        return False
    current = features.iloc[ordinal]
    previous = features.iloc[ordinal - 1]
    names = ("adjusted_close", "sma5", "sma10", "atr14", "low20_prior", "low20_age")
    if current.loc[list(names)].isna().any() or previous.loc[["adjusted_close", "sma5", "sma10"]].isna().any():
        return False
    ceiling_previous = max(float(previous.sma5), float(previous.sma10))
    ceiling_current = max(float(current.sma5), float(current.sma10))
    distance = float(current.adjusted_close - current.low20_prior)
    return bool(
        previous.adjusted_close <= ceiling_previous
        and current.adjusted_close > ceiling_current
        and 0 < distance <= 3.0 * current.atr14
        and 0 < current.low20_age <= 10
    )


def advance_entry_event(
    features: pd.DataFrame,
    event: BreakoutEvent,
    ordinal: int,
) -> EntryTransition:
    """Evaluate one waiting event; invalidation precedes confirmation."""

    template = TEMPLATE_BY_ID[event.template_id]
    age = ordinal - event.breakout_ordinal
    if age <= 0:
        raise ActionValueError("PULLBACK_EVENT_CLOCK_INVALID")
    if age > template.pullback_wait_sessions:
        return EntryTransition("PULLBACK_WINDOW_EXPIRED", None, {"waited_sessions": age - 1})
    current = features.iloc[ordinal]
    previous = features.iloc[ordinal - 1]
    needed_current = ["adjusted_low", "adjusted_close", "sma5", "sma10"]
    needed_previous = ["sma5", "sma10", "atr14"]
    if current.loc[needed_current].isna().any() or previous.loc[needed_previous].isna().any():
        return EntryTransition("PATTERN_SOURCE_UNAVAILABLE", None, {"waited_sessions": age})
    if previous.atr14 <= 0:
        return EntryTransition("PATTERN_SCALE_UNAVAILABLE", None, {"waited_sessions": age})
    tolerance = template.pullback_tolerance_atr * float(previous.atr14)
    floor = float(previous.sma10) - tolerance
    if float(current.adjusted_close) < floor:
        return EntryTransition(
            "PULLBACK_INVALIDATED",
            None,
            {"waited_sessions": age, "close": float(current.adjusted_close), "invalidation_floor": floor},
        )
    confirmed = bool(
        abs(float(current.adjusted_low - previous.sma10)) <= tolerance
        and current.adjusted_close >= previous.sma10
        and current.adjusted_close > current.sma5 > current.sma10
        and current.sma5 > previous.sma5
        and current.sma10 > previous.sma10
    )
    reason = {
        "waited_sessions": age,
        "low_to_prior_ma10_atr": float((current.adjusted_low - previous.sma10) / previous.atr14),
        "close_to_prior_ma10_atr": float((current.adjusted_close - previous.sma10) / previous.atr14),
    }
    if confirmed:
        return EntryTransition("PULLBACK_CONFIRMED", None, reason)
    if age == template.pullback_wait_sessions:
        return EntryTransition("PULLBACK_WINDOW_EXPIRED", None, reason)
    return EntryTransition("PULLBACK_WAITING", event, reason)


def acceleration_volume_exit(
    features: pd.DataFrame, ordinal: int, template_id: str
) -> tuple[bool, dict[str, float | bool]]:
    template = TEMPLATE_BY_ID[template_id]
    if ordinal <= 0 or ordinal >= len(features):
        return False, {"available": False}
    row = features.iloc[ordinal]
    previous = features.iloc[ordinal - 1]
    needed = [
        "adjusted_close",
        "sma5",
        "sma10",
        "atr14",
        "acceleration_atr",
        "distance_ma10_atr",
        "volume_ratio",
        "close_position",
        "upper_wick_fraction",
    ]
    if (
        row.loc[needed].isna().any()
        or pd.isna(previous.sma10)
        or ordinal < 3
        or pd.isna(features.iloc[ordinal - 3].adjusted_close)
    ):
        return False, {"available": False}
    exhaustion = bool(row.close_position <= 0.5 or row.upper_wick_fraction >= 0.5)
    matched = bool(
        row.adjusted_close > features.iloc[ordinal - 3].adjusted_close
        and row.sma5 > row.sma10
        and row.sma10 > previous.sma10
        and row.acceleration_atr >= 0.25
        and row.distance_ma10_atr >= 2.0
        and row.volume_ratio >= 2.0
        and (exhaustion or not template.require_exhaustion)
    )
    return matched, {
        "available": True,
        "acceleration_atr": float(row.acceleration_atr),
        "distance_ma10_atr": float(row.distance_ma10_atr),
        "volume_ratio": float(row.volume_ratio),
        "close_position": float(row.close_position),
        "upper_wick_fraction": float(row.upper_wick_fraction),
        "exhaustion_confirmed": exhaustion,
    }


def pattern_model_features(features: pd.DataFrame, ordinal: int) -> dict[str, float]:
    row = features.iloc[ordinal]
    previous = features.iloc[ordinal - 1] if ordinal > 0 else row * np.nan
    if pd.isna(row.atr14) or row.atr14 <= 0:
        raise ActionValueError("PATTERN_SCALE_UNAVAILABLE")
    values = {
        "pattern_low_age": float(row.low20_age),
        "pattern_distance_low_atr": float((row.adjusted_close - row.low20_prior) / row.atr14),
        "pattern_distance_ma5_atr": float((row.adjusted_close - row.sma5) / row.atr14),
        "pattern_distance_ma10_atr": float((row.adjusted_close - row.sma10) / row.atr14),
        "pattern_ma5_slope_atr": float((row.sma5 - previous.sma5) / row.atr14),
        "pattern_ma10_slope_atr": float((row.sma10 - previous.sma10) / row.atr14),
        "pattern_low_to_prior_ma10_atr": float((row.adjusted_low - previous.sma10) / row.atr14),
        "pattern_acceleration_atr": float(row.acceleration_atr),
        "pattern_volume_ratio": float(row.volume_ratio),
    }
    if not np.isfinite(tuple(values.values())).all():
        raise ActionValueError("PATTERN_FEATURE_UNAVAILABLE")
    return values


__all__: Sequence[str] = (
    "BreakoutEvent",
    "EntryTransition",
    "PATTERN_FEATURE_COLUMNS",
    "PATTERN_TEMPLATE_SET_SHA256",
    "PatternTemplate",
    "TEMPLATES",
    "TEMPLATE_BY_ID",
    "acceleration_volume_exit",
    "advance_entry_event",
    "breakout_observed",
    "pattern_feature_frame",
    "pattern_model_features",
)
