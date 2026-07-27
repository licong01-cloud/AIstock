from __future__ import annotations

from backend.services.advisory_historical_range.outcome_policy_catalog import (
    R4_DEFAULT_HORIZONS,
    R4_LONG_TREND_HORIZONS,
    load_historical_range_outcome_policy_catalog,
)


def test_options_use_exact_r4_catalog_without_new_policy() -> None:
    catalog = load_historical_range_outcome_policy_catalog()
    assert catalog.default_horizons == R4_DEFAULT_HORIZONS
    assert catalog.long_trend_horizons == R4_LONG_TREND_HORIZONS
    assert len(catalog.catalog_content_hash) == 64
