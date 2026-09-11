from datetime import date

import pandas as pd

from backend.services.position_timing.pattern_optimizer import monthly_template_schedule


def _development_rows(calendar: pd.DatetimeIndex) -> pd.DataFrame:
    rows = []
    for template in range(8):
        for symbol in ("000001.SZ", "000002.SZ"):
            for day in calendar:
                rows.append(
                    {
                        "symbol": symbol,
                        "valuation_date": day.date(),
                        "template_id": f"R{template}",
                        "incremental_net_value_bps": float(template),
                    }
                )
    return pd.DataFrame(rows)


def test_monthly_selector_uses_prior_complete_lagged_months_only():
    calendar = pd.bdate_range("2022-01-03", "2024-06-28")
    rows = _development_rows(calendar)
    schedule = monthly_template_schedule(
        rows,
        calendar_dates=tuple(calendar.date),
        evaluation_start=date(2024, 1, 2),
        evaluation_end=date(2024, 3, 29),
    )
    assert set(schedule["selected_template_id"]) == {"R7"}
    assert all(
        pd.Timestamp(value) < pd.Timestamp(month + "-01")
        for value, month in zip(schedule.training_window_end, schedule.month)
    )

    changed = rows.copy()
    changed.loc[changed.valuation_date >= date(2024, 1, 2), "incremental_net_value_bps"] = -9999.0
    rerun = monthly_template_schedule(
        changed,
        calendar_dates=tuple(calendar.date),
        evaluation_start=date(2024, 1, 2),
        evaluation_end=date(2024, 3, 29),
    )
    pd.testing.assert_series_equal(schedule.selected_template_id, rerun.selected_template_id)


def test_selector_tie_breaks_by_frozen_template_order():
    calendar = pd.bdate_range("2022-01-03", "2024-03-29")
    rows = _development_rows(calendar)
    rows["incremental_net_value_bps"] = 1.0
    schedule = monthly_template_schedule(
        rows,
        calendar_dates=tuple(calendar.date),
        evaluation_start=date(2024, 1, 2),
        evaluation_end=date(2024, 1, 31),
    )
    assert schedule.iloc[0].selected_template_id == "R0"


def test_asymmetric_template_support_uses_explicit_fallback():
    calendar = pd.bdate_range("2022-01-03", "2024-03-29")
    rows = _development_rows(calendar)
    rows = rows.loc[~((rows.template_id == "R7") & (rows.symbol == "000002.SZ"))]
    schedule = monthly_template_schedule(
        rows,
        calendar_dates=tuple(calendar.date),
        evaluation_start=date(2024, 1, 2),
        evaluation_end=date(2024, 1, 31),
    )
    assert schedule.iloc[0].selected_template_id == "R0"
    assert schedule.iloc[0].status == "OPTIMIZER_UNAVAILABLE_USING_R0"


def test_incomplete_development_coverage_forces_frozen_fallback():
    calendar = pd.bdate_range("2022-01-03", "2024-03-29")
    rows = _development_rows(calendar)
    schedule = monthly_template_schedule(
        rows,
        calendar_dates=tuple(calendar.date),
        evaluation_start=date(2024, 1, 2),
        evaluation_end=date(2024, 2, 29),
        development_coverage_complete=False,
    )
    assert set(schedule.selected_template_id) == {"R0"}
    assert all("DEVELOPMENT_COVERAGE_INCOMPLETE" in status for status in schedule.status)


def test_empty_development_rows_return_typed_fallback_schedule():
    calendar = pd.bdate_range("2022-01-03", "2024-03-29")
    schedule = monthly_template_schedule(
        pd.DataFrame(),
        calendar_dates=tuple(calendar.date),
        evaluation_start=date(2024, 1, 2),
        evaluation_end=date(2024, 1, 31),
        development_coverage_complete=False,
    )
    assert schedule.iloc[0].selected_template_id == "R0"
    assert schedule.iloc[0].status == "OPTIMIZER_UNAVAILABLE_DEVELOPMENT_COVERAGE_INCOMPLETE_USING_R0"
