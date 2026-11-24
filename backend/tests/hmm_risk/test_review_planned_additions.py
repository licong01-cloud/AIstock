import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from scripts.hmm_risk.review_planned_additions import DAYS, TARGETS, planned_reviews
from scripts.hmm_risk.review_retained_holdings import PRED, STOCKS


D, E = "2026-05-08", "2026-05-11"


def inputs():
    days = pd.DataFrame([["book", D, E, 1000., True, 2, 4]], columns=DAYS)
    stocks = pd.DataFrame([["book", D, "a", "A", 100, 50],
                           ["book", D, "b", "B", 200, 150]], columns=STOCKS)
    targets = pd.DataFrame([["book", D, E, "a", "A", .2, 150],
                            ["book", D, E, "c", "A", .05, 0],
                            ["book", D, E, "b", "B", .1, 200],
                            ["book", D, E, "d", "C", .3, 0]], columns=TARGETS)
    return days, stocks, targets


def test_separate_new_money_from_retained_exposure_and_attach_warnings_after_choice():
    days, stocks, targets = inputs()
    warning = pd.DataFrame([[D, E, "A", None, True, None, .4]], columns=PRED)
    sheet = planned_reviews(days, stocks, targets.iloc[::-1], warning).set_index("focus")
    r = sheet.loc["retained"]
    assert (r.code, r.planned_addition, r.retained_value, r.warning_status) == ("B", 0, 150, "UNAVAILABLE")
    assert pd.isna(r.first_stock)
    r = sheet.loc["incoming"]
    assert (r.code, r.first_stock, r.first_gap, r.planned_addition) == ("A", "a", 50, 100)
    assert (r.new_position_gap, r.existing_position_gap, r.addition_nav_pct) == (50, 50, 10)
    assert r.warning_status == "PARTIAL" and r.M1 and pd.isna(r.M0)


def test_sector_ties_prefer_retained_amount_then_code_and_zero_gaps_do_not_select():
    days, stocks, targets = inputs()
    targets.loc[targets.stock == "b", "target_weight"] = .30
    assert planned_reviews(days, stocks, targets).iloc[1].code == "B"
    stocks.loc[1, "retained_value"] = 50
    assert planned_reviews(days, stocks, targets).iloc[1].code == "A"
    result = planned_reviews(days, stocks, targets.assign(target_weight=0))
    assert result.status.tolist() == ["READY", "NO_PLANNED_ADDITION"]
    assert result.planned_addition.tolist() == [0, 0]


def test_missing_coverage_keeps_the_reason_and_unknown_amount():
    days, stocks, targets = inputs()
    assert planned_reviews(days, stocks, targets.iloc[:3]).status.eq("INCOMPLETE_TARGETS").all()
    targets.loc[2, "code"] = None
    assert planned_reviews(days, stocks, targets).status.eq("READY").all()
    targets.loc[1, "code"] = None
    result = planned_reviews(days, stocks, targets)
    assert result.status.eq("UNKNOWN_TARGET_INDUSTRY").all() and result.planned_addition.isna().all()
    targets.loc[0, "held_value"] = float("nan")
    assert planned_reviews(days, stocks, targets).status.eq("UNKNOWN_TARGET_HOLDINGS").all()


def test_rejects_inconsistent_target_sources():
    days, stocks, targets = inputs()
    for col, value, reason in [("effective", D, "effective date"), ("held_value", 50, "conflicts"),
                               ("code", "X", "conflicts"), ("target_weight", -1, "nonnegative")]:
        broken = targets.copy()
        broken.loc[0, col] = value
        with pytest.raises(ValueError, match=reason):
            planned_reviews(days, stocks, broken)
    with pytest.raises(ValueError, match="duplicate"):
        planned_reviews(days, stocks, pd.concat([targets, targets.iloc[:1]]))


def test_cli_file_and_stdout_return_the_same_decisions(tmp_path):
    for name, frame in zip(("days", "stocks", "targets"), inputs()):
        frame.to_parquet(tmp_path / f"{name}.parquet", index=False)
    script = Path(__file__).resolve().parents[3] / "scripts/hmm_risk/review_planned_additions.py"
    cmd = [sys.executable, str(script), "--days", "days.parquet", "--stocks", "stocks.parquet",
           "--targets", "targets.parquet"]
    stream = subprocess.check_output(cmd, cwd=tmp_path)
    subprocess.run([*cmd, "--output", "sheet.csv"], cwd=tmp_path, check=True)
    assert stream == (tmp_path / "sheet.csv").read_bytes()
    assert len(stream.decode("utf-8-sig").splitlines()) == 3
