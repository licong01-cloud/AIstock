"""Shared, model-neutral contracts for active HMM risk consumers."""

from __future__ import annotations

import hashlib
import json
from typing import Any


BASE_FEATURES = (
    "daily_return",
    "excess_return_Nd",
    "volume_ratio",
    "limit_up_ratio",
    "volatility_Nd",
    "net_mf_ratio",
    "elg_net_mf_ratio",
)
ALL_CORE_FEATURES = BASE_FEATURES + (
    "sf_turnover_pctile_250d_neg",
    "sf_turnover_pctile_120d_neg",
    "sf_turnover_ma5_ma20_neg",
    "sf_mf_net_ratio_std_5d_neg",
    "sf_small_net_ratio_5d",
    "sf_intraday_range_5d_neg",
    "sf_atr14_pctile_250d_neg",
    "sf_range_vs_market_10d",
    "sf_vol_vs_market_20d",
    "sf_breadth_1d",
    "sf_breadth_5d",
    "sf_excess_breadth_5d",
    "sf_dispersion_5d_neg",
)


class StateModelSetError(RuntimeError):
    """Raised when an HMM risk contract cannot be proven."""


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize one value using the module's canonical finite-JSON contract."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    """Hash one value using the canonical finite-JSON representation."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()
