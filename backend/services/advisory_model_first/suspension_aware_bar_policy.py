from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256

BAR_STATE_TRADED: Final = "TRADED"
BAR_STATE_SUSPENDED_VERIFIED: Final = "SUSPENDED_VERIFIED"
BAR_STATE_MISSING_UNEXPLAINED: Final = "MISSING_UNEXPLAINED"
BAR_STATE_SOURCE_CONFLICT: Final = "SOURCE_CONFLICT"

BAR_POLICY_PAYLOAD: Final = {
    "schema_version": "suspension_aware_bar_policy_v1",
    "candidate_coverage_policy": "PRESERVE_EXACT_SELECTION_TOP20_V1",
    "verified_suspension_price_mode": "LAST_EXECUTABLE_CLOSE_MARKET_SESSION_V1",
    "verified_suspension_liquidity_mode": "ZERO_MARKET_SESSION_V1",
    "unexplained_missing_mode": "FAIL_DATASET_NO_CANDIDATE_DROP_V1",
    "price_domain": "REQUEST_BOUND_CANONICAL_ADJUSTED_PRICE_V1",
}
BAR_POLICY_HASH: Final = canonical_json_sha256(BAR_POLICY_PAYLOAD)

_PRICE_COLUMNS: Final = ("open", "high", "low", "close")
_LIQUIDITY_COLUMNS: Final = ("volume", "amount")
_EXECUTION_QUOTE_COLUMNS: Final = (
    "up_limit_price",
    "down_limit_price",
    "limit_up",
    "limit_down",
)
_REQUIRED_RAW_COLUMNS: Final = (*_PRICE_COLUMNS, *_LIQUIDITY_COLUMNS, "factor")


@dataclass(frozen=True)
class SuspensionAwareBarResult:
    panel: pd.DataFrame
    receipt: dict[str, object]


def build_suspension_aware_bar_panel(
    *,
    daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
) -> SuspensionAwareBarResult:
    """Return a fixed-session, PIT-only bar panel without mutating raw input.

    The suspend sidecar is the only suspension authority. A verified suspended
    session is valued from the latest *earlier* traded close and never from a
    stale same-day provider bar or a later resumed price.
    """

    raw = _normalize_daily(daily)
    calendar = _normalize_calendar(trading_calendar)
    suspended = _normalize_suspend_rows(suspend_rows, calendar)
    instruments = tuple(sorted(raw.index.get_level_values("instrument").unique()))
    if not instruments:
        raise _error(
            "daily panel contains no instruments",
            reason_code="ADVISORY_SUSPENSION_BAR_POLICY_INVALID",
        )

    unexpected_suspend_instruments = sorted(set(suspended.index.get_level_values("instrument")) - set(instruments))
    if unexpected_suspend_instruments:
        raise _error(
            "suspend sidecar contains instruments outside the requested panel",
            reason_code="ADVISORY_SUSPENSION_SOURCE_CONFLICT",
            instruments=unexpected_suspend_instruments[:10],
        )

    raw_dates = raw.index.get_level_values("datetime")
    raw_instruments = raw.index.get_level_values("instrument")
    first_visible = pd.Series(raw_dates, index=raw_instruments).groupby(level=0).min()
    candidate_index = pd.MultiIndex.from_product([calendar, instruments], names=["datetime", "instrument"])
    candidate_dates = candidate_index.get_level_values("datetime")
    candidate_first_visible = candidate_index.get_level_values("instrument").map(first_visible)
    full_index = candidate_index[candidate_dates >= candidate_first_visible]
    panel = raw.reindex(full_index).copy()
    suspended_mask = panel.index.isin(suspended.index)
    positive_liquidity = pd.to_numeric(panel["volume"], errors="coerce").fillna(0).gt(0) | pd.to_numeric(
        panel["amount"], errors="coerce"
    ).fillna(0).gt(0)
    conflict = suspended_mask & positive_liquidity.to_numpy()
    if conflict.any():
        raise _row_error(
            "verified suspension conflicts with positive raw liquidity",
            reason_code="ADVISORY_SUSPENSION_SOURCE_CONFLICT",
            index=panel.index[conflict],
            bar_state=BAR_STATE_SOURCE_CONFLICT,
        )

    finite_required = pd.DataFrame(
        {column: np.isfinite(pd.to_numeric(panel[column], errors="coerce")) for column in _REQUIRED_RAW_COLUMNS},
        index=panel.index,
    )
    missing_unexplained = (~suspended_mask) & (~finite_required.all(axis=1).to_numpy())
    if missing_unexplained.any():
        raise _row_error(
            "non-suspended market session is missing required raw bar values",
            reason_code="ADVISORY_SUSPENSION_UNEXPLAINED_MISSING",
            index=panel.index[missing_unexplained],
            bar_state=BAR_STATE_MISSING_UNEXPLAINED,
        )

    nonpositive_price_or_factor = (~suspended_mask) & (
        panel[[*_PRICE_COLUMNS, "factor"]].apply(pd.to_numeric, errors="coerce").le(0).any(axis=1).to_numpy()
    )
    if nonpositive_price_or_factor.any():
        raise _row_error(
            "non-suspended market session has nonpositive price or factor",
            reason_code="ADVISORY_SUSPENSION_SOURCE_CONFLICT",
            index=panel.index[nonpositive_price_or_factor],
            bar_state=BAR_STATE_SOURCE_CONFLICT,
        )

    negative_liquidity = (~suspended_mask) & (
        pd.to_numeric(panel["volume"], errors="coerce").lt(0).to_numpy()
        | pd.to_numeric(panel["amount"], errors="coerce").lt(0).to_numpy()
    )
    if negative_liquidity.any():
        raise _row_error(
            "non-suspended market session has negative liquidity",
            reason_code="ADVISORY_SUSPENSION_SOURCE_CONFLICT",
            index=panel.index[negative_liquidity],
            bar_state=BAR_STATE_SOURCE_CONFLICT,
        )

    traded_close = pd.to_numeric(panel["close"], errors="coerce").where(~suspended_mask)
    # shift before ffill is the causal boundary: the current or any later row
    # can never supply a suspended session's valuation anchor.
    prior_traded_close = traded_close.groupby(level="instrument").transform(lambda values: values.shift(1).ffill())
    no_anchor = suspended_mask & prior_traded_close.isna().to_numpy()
    if no_anchor.any():
        raise _row_error(
            "verified suspension has no earlier executable close",
            reason_code="ADVISORY_SUSPENSION_LAST_CLOSE_UNAVAILABLE",
            index=panel.index[no_anchor],
            bar_state=BAR_STATE_SUSPENDED_VERIFIED,
        )

    for column in _PRICE_COLUMNS:
        panel.loc[suspended_mask, column] = prior_traded_close.loc[suspended_mask]
    for column in _LIQUIDITY_COLUMNS:
        panel.loc[suspended_mask, column] = 0.0
    for column in _EXECUTION_QUOTE_COLUMNS:
        if column in panel:
            panel.loc[suspended_mask, column] = np.nan
    panel["bar_state"] = np.where(suspended_mask, BAR_STATE_SUSPENDED_VERIFIED, BAR_STATE_TRADED)
    panel["bar_is_suspended_verified"] = suspended_mask.astype("int8")

    return SuspensionAwareBarResult(
        panel=panel,
        receipt={
            **BAR_POLICY_PAYLOAD,
            "bar_policy_hash": BAR_POLICY_HASH,
            "calendar_start": calendar[0].date().isoformat(),
            "calendar_end": calendar[-1].date().isoformat(),
            "calendar_session_count": int(len(calendar)),
            "instrument_count": int(len(instruments)),
            "panel_row_count": int(len(panel)),
            "traded_row_count": int((~suspended_mask).sum()),
            "suspended_verified_row_count": int(suspended_mask.sum()),
        },
    )


def _normalize_daily(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame.index, pd.MultiIndex) or set(frame.index.names) != {
        "datetime",
        "instrument",
    }:
        raise _error(
            "daily panel must use datetime/instrument MultiIndex",
            reason_code="ADVISORY_SUSPENSION_BAR_POLICY_INVALID",
        )
    missing = sorted(set(_REQUIRED_RAW_COLUMNS) - set(frame.columns))
    if missing:
        raise _error(
            "daily panel is missing raw bar columns",
            reason_code="ADVISORY_SUSPENSION_BAR_POLICY_INVALID",
            missing_columns=missing,
        )
    reset = frame.reset_index().copy()
    reset["datetime"] = pd.to_datetime(reset["datetime"]).dt.normalize()
    reset["instrument"] = reset["instrument"].astype(str).str.upper()
    if reset.duplicated(["datetime", "instrument"]).any():
        raise _error(
            "daily panel contains duplicate identities",
            reason_code="ADVISORY_SUSPENSION_SOURCE_CONFLICT",
        )
    return reset.set_index(["datetime", "instrument"]).sort_index()


def _normalize_calendar(values: Sequence[pd.Timestamp]) -> pd.DatetimeIndex:
    calendar = pd.DatetimeIndex(pd.to_datetime(list(values))).normalize()
    if calendar.empty or calendar.has_duplicates or not calendar.is_monotonic_increasing:
        raise _error(
            "trading calendar must be non-empty, unique, and increasing",
            reason_code="ADVISORY_SUSPENSION_BAR_POLICY_INVALID",
        )
    return calendar


def _normalize_suspend_rows(frame: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.DataFrame:
    required = {"trade_date", "instrument", "suspend_type"}
    if not required.issubset(frame.columns):
        raise _error(
            "suspend sidecar schema is invalid",
            reason_code="ADVISORY_SUSPENSION_BAR_POLICY_INVALID",
            missing_columns=sorted(required - set(frame.columns)),
        )
    result = frame.loc[frame["suspend_type"].astype(str).str.upper().eq("S")].copy()
    result["trade_date"] = pd.to_datetime(result["trade_date"]).dt.normalize()
    result["instrument"] = result["instrument"].astype(str).str.upper()
    if result.duplicated(["trade_date", "instrument"]).any():
        raise _error(
            "suspend sidecar contains duplicate identities",
            reason_code="ADVISORY_SUSPENSION_SOURCE_CONFLICT",
        )
    outside = ~result["trade_date"].isin(calendar)
    if outside.any():
        raise _error(
            "suspend sidecar contains dates outside the bound calendar",
            reason_code="ADVISORY_SUSPENSION_SOURCE_CONFLICT",
            dates=sorted(result.loc[outside, "trade_date"].dt.date.astype(str).unique())[:10],
        )
    return result.set_index(["trade_date", "instrument"]).sort_index()


def _row_error(
    message: str,
    *,
    reason_code: str,
    index: pd.MultiIndex,
    bar_state: str,
) -> AdvisoryModelFirstError:
    samples = [f"{date.date().isoformat()}:{instrument}" for date, instrument in index[:10]]
    return _error(
        message,
        reason_code=reason_code,
        bar_state=bar_state,
        row_count=len(index),
        samples=samples,
    )


def _error(message: str, *, reason_code: str, **context: object) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(message, reason_code=reason_code, context=context or None)
