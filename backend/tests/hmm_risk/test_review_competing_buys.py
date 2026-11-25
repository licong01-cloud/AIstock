import pandas as pd
import pytest

from scripts.hmm_risk.review_competing_buys import SCORES, TICKETS, buy_fee, review


DATE = "2026-05-06"
TICK = "2026-05-07 09:31:00"


def tickets():
    return pd.DataFrame([
        ["a", TICK, DATE, "000001.SZ", "A", 1, 300, 300, True, False, 500, 1000, 0, 20, 3],
        ["a", TICK, DATE, "600001.SH", "B", 2, 100, 300, True, True, 500, 900, 0, 20, 1],
    ], columns=TICKETS)


def test_protects_cash_and_ranks_after_first_eligible_choice():
    frame = tickets()
    later = frame.assign(tick="2026-05-07 09:33:00")
    scores = pd.DataFrame([[DATE, "2026-05-07", "A", .2], [DATE, "2026-05-07", "B", .6]], columns=SCORES)
    result = review(pd.concat([later, frame.iloc[::-1]]), scores)
    assert result.status.tolist() == ["EXCLUSIVE", "EXCLUSIVE"]
    assert result.first_daily.tolist() == result.whole_first_daily.tolist() == [True, False]
    assert result.hmm_choice.eq("SWITCH").all() and result.rank_choice.eq("SWITCH").all()
    row = result.iloc[0]
    assert (row.budget_cents, row.extra_cents, row.saved_cents) == (100020, 90000, 100000)
    assert (row.whole_budget_cents, row.whole_funded_shares, row.whole_limited_shares) == (391020, 100, 300)


def test_weak_or_incomplete_evidence_and_unknown_predictions():
    frame = tickets()
    assert review(frame).iloc[0].hmm_choice == "UNKNOWN"
    frame.loc[1, "exact_attempt"] = False
    row = review(frame).iloc[0]
    assert row.status == "EXCLUSIVE" and row.whole_status == "UNKNOWN_ATTEMPT_SIZE"
    assert pd.isna(row.whole_children_exclusive) and not row.whole_first_daily
    frame.loc[1, "code"] = None
    assert review(frame).iloc[0].status == "UNKNOWN_INDUSTRY"
    assert review(tickets().iloc[1:]).iloc[0].status == "NO_EARLIER_FUNDED_BUY"
    assert review(tickets().assign(cash_end_cents=999999)).iloc[0].status == "BOTH_AFFORDABLE"


def test_star_residual_and_rank_tie():
    frame = tickets()
    frame.loc[0, ["stock", "filled", "attempted"]] = ["688001.SH", 310, 310]
    assert review(frame).iloc[0].status == "NO_INDEPENDENT_LOT"
    tied = pd.DataFrame([[DATE, "2026-05-07", c, .5] for c in ["A", "B"]], columns=SCORES)
    assert review(tickets(), tied).iloc[0].hmm_choice == "KEEP"
    unrelated = tied.assign(effective="2026-05-08")
    assert review(tickets(), unrelated).iloc[0].hmm_choice == "UNKNOWN"
    assert buy_fee(0) == 0 and buy_fee(100000) == 500 and buy_fee(10_000_000) == 950


@pytest.mark.parametrize(("col", "value", "error"), [
    ("sequence", 1, "duplicate"), ("cash_end_cents", 40, "cash or decision date"),
    ("attempted", 50, "inconsistent"), ("cash_limited", False, "inconsistent"),
    ("rank", -1, "rank"), ("tick", DATE, "decision date"),
])
def test_rejects_invalid_receipts(col, value, error):
    frame = tickets()
    frame.loc[1, col] = value
    with pytest.raises(ValueError, match=error):
        review(frame)
