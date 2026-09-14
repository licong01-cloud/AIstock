from __future__ import annotations

from datetime import date

import pytest

from scripts.precompute_hmm_coefficients import (
    build_input_data_max_dates_by_date,
    build_stock_sector_maps_by_date,
)


def test_build_stock_sector_maps_by_date_uses_membership_visible_on_each_day() -> None:
    rows = [
        {
            "ts_code": "000001.SZ",
            "l2_code": "OLD.SI",
            "in_date": date(2026, 1, 1),
            "out_date": date(2026, 6, 1),
        },
        {
            "ts_code": "000001.SZ",
            "l2_code": "NEW.SI",
            "in_date": date(2026, 6, 2),
            "out_date": None,
        },
    ]

    result = build_stock_sector_maps_by_date(
        rows,
        [date(2026, 6, 1), date(2026, 6, 2)],
    )

    assert result["2026-06-01"]["000001.SZ"] == "OLD.SI"
    assert result["2026-06-02"]["000001.SZ"] == "NEW.SI"


def test_build_stock_sector_maps_by_date_rejects_conflicting_visible_membership() -> None:
    rows = [
        {"ts_code": "000001.SZ", "l2_code": "A.SI", "in_date": date(2026, 1, 1), "out_date": None},
        {"ts_code": "000001.SZ", "l2_code": "B.SI", "in_date": date(2026, 1, 1), "out_date": None},
    ]

    with pytest.raises(ValueError, match="conflicting stock-sector memberships"):
        build_stock_sector_maps_by_date(rows, [date(2026, 6, 1)])


def test_build_input_data_max_dates_by_date_closes_each_causal_watermark() -> None:
    result = build_input_data_max_dates_by_date(
        trade_dates=[date(2026, 6, 1), date(2026, 6, 2)],
        sector_dates=[date(2026, 5, 29), date(2026, 6, 2)],
        index_dates=[date(2026, 6, 1), date(2026, 6, 2)],
        market_volume_dates=[date(2026, 5, 29), date(2026, 6, 1)],
    )

    assert result["2026-06-01"] == {
        "sector_data": "2026-05-29",
        "index_daily": "2026-06-01",
        "sw_daily": "2026-06-01",
        "sw_index_member_effective_as_of": "2026-06-01",
    }
    assert result["2026-06-02"]["sector_data"] == "2026-06-02"
