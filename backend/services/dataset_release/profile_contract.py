"""Shared active-dataset profile identities used by producers and consumers."""

from __future__ import annotations

from types import MappingProxyType


ACTIVE_PROFILE_SCHEMA_V4 = "aistock_active_dataset_profile_v4"

_REQUIREMENTS = {
    "qe": frozenset(
        {
            "benchmark",
            "coverage",
            "day",
            "factor",
            "index",
            "manifest",
            "minute",
            "sector_context",
            "stock_pools",
            "suspend",
        }
    ),
    "hmm": frozenset({"factor", "index", "manifest", "sector_context"}),
    "selection": frozenset(
        {"day", "factor", "index", "manifest", "minute", "stock_pools", "suspend"}
    ),
    "advisory": frozenset(
        {"day", "factor", "index", "manifest", "minute", "stock_pools", "suspend"}
    ),
    "factor_research": frozenset({"day", "factor", "manifest", "stock_pools"}),
    "position_timing": frozenset({"day", "factor", "manifest", "stock_pools"}),
    "unified_backtest": frozenset(
        {"day", "factor", "index", "manifest", "minute", "stock_pools", "suspend"}
    ),
}

ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS = MappingProxyType(
    {
        **_REQUIREMENTS,
        "qe_single": _REQUIREMENTS["qe"],
        "qe_custom": _REQUIREMENTS["qe"],
        "qe_multi_alpha": _REQUIREMENTS["qe"],
        "qe_p10": _REQUIREMENTS["qe"] | {"derived_assets"},
        "qe_p11": _REQUIREMENTS["qe"] | {"derived_assets"},
        "hmm_file_only": _REQUIREMENTS["hmm"],
    }
)


__all__ = (
    "ACTIVE_PROFILE_SCHEMA_V4",
    "ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS",
)
