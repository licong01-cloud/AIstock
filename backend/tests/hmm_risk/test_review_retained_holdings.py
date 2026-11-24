import pandas as pd
import pytest

from scripts.hmm_risk.review_retained_holdings import DAYS, PRED, STOCKS, review


def sample():
    days = pd.DataFrame([["a", "2026-05-08", "2026-05-11", 1000, True, 3]], columns=DAYS)
    positions = pd.DataFrame([
        ["a", "2026-05-08", "s1", "A", 500, 0],
        ["a", "2026-05-08", "s2", "A", 100, 100],
        ["a", "2026-05-08", "s3", "B", 200, 150],
    ], columns=STOCKS)
    return days, positions


def test_selects_remaining_exposure_without_warnings():
    days, stocks = sample()
    row = review(days, stocks).iloc[0]
    assert (row.status, row.code, row.stocks, row.nav_weight_pct) == ("READY", "B", "s3", 15)
    assert row.warning_status == "UNAVAILABLE" and pd.isna(row.M1)
    stocks.loc[2, "retained_value"] = 100
    assert review(days, stocks.iloc[::-1]).iloc[0].code == "A"


def test_missing_coverage_and_zero_retained_remain_distinct():
    days, stocks = sample()
    assert review(days, stocks.iloc[:2]).iloc[0].status == "INCOMPLETE_HOLDINGS"
    assert review(days, stocks.assign(code=None)).iloc[0].status == "UNKNOWN_INDUSTRY"
    assert review(days, stocks.assign(retained_value=0)).iloc[0].status == "NO_RETAINED_OLD_HOLDING"
    assert review(days.assign(has_next_target=False), stocks).iloc[0].status == "NO_NEXT_TARGET"
    with pytest.raises(ValueError, match="duplicate"):
        review(days, pd.concat([stocks, stocks.iloc[:1]]))


def test_warning_must_match_both_dates_and_never_changes_the_choice():
    days, stocks = sample()
    warnings = pd.DataFrame([["2026-05-08", "2026-05-11", "B", True, False, .3, .1]], columns=PRED)
    row = review(days, stocks, warnings).iloc[0]
    assert row.code == "B" and row.warning_status == "AVAILABLE" and row.M0 and not row.M1
    with pytest.raises(ValueError, match="effective date mismatch"):
        review(days, stocks, warnings.assign(effective="2026-05-12"))
    with pytest.raises(ValueError, match="values must be finite"):
        review(days, stocks.assign(retained_value=float("nan")))


def test_explicitly_unavailable_model_keeps_the_other_warning():
    days, stocks = sample()
    warnings = pd.DataFrame([["2026-05-08", "2026-05-11", "B", None, False, None, .1]], columns=PRED)
    row = review(days, stocks, warnings).iloc[0]
    assert row.code == "B" and pd.isna(row.M0) and not row.M1 and row.warning_status == "PARTIAL"
    with pytest.raises(ValueError, match="incomplete"):
        review(days, stocks, warnings.assign(p0=.3))
