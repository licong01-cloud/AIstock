from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from backend.services.hmm_risk import risk_l2
from backend.services.hmm_risk.risk_l2_value_replay import ReplayError, load_inputs, replay, zero_compute


def panel():
    catalog = [f"{801000 + i}.SI" for i in range(131)]
    days = ["2026-03-26", "2026-03-27", "2026-03-30", "2026-03-31"]
    signals = {
        d: {
            c: {
                "as_of_date": days[i - 1] if i else "2026-03-25",
                "probability": 0.1,
                "warning": False,
                "availability": "available",
                "reason_code": None,
            }
            for c in catalog
        }
        for i, d in enumerate(days)
    }
    returns = {d: {c: 0.01 for c in catalog} for d in days[1:]}
    return catalog, days, signals, returns


def warn(signals, code, day):
    signals[day][code].update(probability=0.3, warning=True)


def test_no_warning_arms_identical_initial_cost_and_no_final_liquidation():
    c, d, s, r = panel()
    result = replay(c, d, s, r)
    assert result["status"] == "REFERENCE_RISK_REDUCTION_NOT_OBSERVED"
    assert result["gross_full_path"]["R"]["cumulative_return"] == pytest.approx(1.01**3 - 1)
    assert all(row["arms"]["R"] == row["arms"]["B"] == row["arms"]["X"] for row in result["daily"])
    assert result["daily"][0]["arms"]["R"]["one_sided_risk_turnover"] == pytest.approx(1)
    assert result["daily"][0]["arms"]["R"]["cost_sensitivity_return"]["20"] == pytest.approx(0.008)
    assert result["daily"][0]["arms"]["R"]["closing_cash_budget"] == pytest.approx(0)
    assert result["exposure"]["R"]["final_risk_budget"] == pytest.approx(1)
    assert result["cost_sensitivity"]["break_even"]["status"] == "NOT_COMPUTABLE"


def test_one_day_delay_and_same_exposure_reference_do_not_redistribute_warning_budget():
    c, d, s, r = panel()
    warn(s, c[0], d[0])
    r[d[1]][c[0]] = -0.5
    for code in c[1:]:
        r[d[1]][code] = 0
    result = replay(c, d, s, r)
    first = result["daily"][0]
    assert first["signal_date"] == d[0] and first["signal_as_of"] == "2026-03-25"
    assert first["arms"]["R"]["risk_budget"] == first["arms"]["X"]["risk_budget"] == pytest.approx(130 / 131)
    assert first["arms"]["R"]["gross_return"] == 0
    changed = deepcopy(s)
    for day in d[1:]:
        for code in c:
            warn(changed, code, day)
    assert replay(c, d, changed, r)["daily"][0] == first
    assert result["comparisons"]["R_minus_X"]["maximum_drawdown"] > 0
    assert result["status"] == "REFERENCE_RISK_REDUCTION_OBSERVED"


def test_all_warning_is_legitimate_cash_and_all_unknown_is_not_no_warning():
    c, d, s, r = panel()
    for rows in s.values():
        for row in rows.values():
            row.update(probability=0.3, warning=True)
    result = replay(c, d, s, r)
    assert all(x["arms"]["R"]["risk_budget"] == x["arms"]["X"]["risk_budget"] == 0 for x in result["daily"])
    for rows in s.values():
        for row in rows.values():
            row.update(probability=None, warning=None, availability="unavailable", reason_code="input_na")
    result = replay(c, d, s, r)
    assert result["status"] == "INSUFFICIENT_REFERENCE_PATH"
    assert result["prediction_availability"]["available_sector_dates"] == 0
    assert result["daily"][0]["warning_count"] == 0
    assert result["daily"][0]["input_unavailable_cash_count"] == 131


def test_held_na_keeps_full_denominator_separate_blocks_and_unknown_cost():
    c, d, s, r = panel()
    r[d[2]][c[0]] = None
    result = replay(c, d, s, r)
    assert result["status"] == "INSUFFICIENT_REFERENCE_PATH"
    assert result["planned_return_dates"] == 3 and result["paired_return_dates"] == 2
    assert result["legal_na_dates"] == [d[2]]
    assert result["gross_full_path"]["R"] is None
    assert [b["date_count"] for b in result["gross_blocks"]["R"]] == [1, 1]
    assert len(result["paired_block_comparisons"]) == 2
    assert all(b["diagnostic_only_not_full_path"] for b in result["paired_block_comparisons"])
    assert result["daily"][1]["arms"]["R"]["gross_return"] is None
    assert result["daily"][2]["arms"]["R"]["one_sided_risk_turnover"] is None
    assert result["daily"][2]["arms"]["R"]["cost_sensitivity_return"]["5"] is None
    assert result["daily"][2]["arms"]["R"]["cost_sensitivity_return"]["0"] == pytest.approx(0.01)


def test_unheld_na_is_not_filled_and_warning_upside_is_reported():
    c, d, s, r = panel()
    for day in d:
        s[day][c[0]].update(probability=None, warning=None, availability="unavailable", reason_code="warmup_na")
        warn(s, c[1], day)
    for day in d[1:]:
        r[day][c[0]] = None
        r[day][c[1]] = 0.1
    result = replay(c, d, s, r)
    assert result["complete_reference_path"] is True
    assert result["opportunity_cost"]["warning_return_distribution"]["positive_share"] == 1
    assert result["opportunity_cost"]["R_minus_B_cumulative_return"] < 0
    assert r[d[1]][c[0]] is None


def test_turnover_uses_drift_not_only_two_equal_targets():
    c, d, s, r = panel()
    r[d[1]][c[0]] = 1
    for code in c[1:]:
        r[d[1]][code] = 0
    result = replay(c, d, s, r)
    actual = result["daily"][1]["arms"]["B"]["one_sided_risk_turnover"]
    assert actual == pytest.approx(abs(1 / 131 - 2 / 132) + 130 * abs(1 / 131 - 1 / 132))
    assert actual > 0


@pytest.mark.parametrize(
    "kind", ["future_asof", "missing_date", "unknown_code", "warning_drift", "nan", "below_minus_one"]
)
def test_contract_errors_fail_closed(kind):
    c, d, s, r = panel()
    if kind == "future_asof":
        s[d[0]][c[0]]["as_of_date"] = d[0]
    elif kind == "missing_date":
        r.pop(d[2])
    elif kind == "unknown_code":
        r[d[1]]["999999.SI"] = 0
    elif kind == "warning_drift":
        s[d[0]][c[0]]["warning"] = True
    else:
        r[d[1]][c[0]] = float("nan") if kind == "nan" else -1.01
    with pytest.raises(ReplayError):
        replay(c, d, s, r)


def test_capital_depletion_is_insufficient_not_model_failure_or_division_by_zero():
    c, d, s, r = panel()
    r[d[1]] = {code: -1 for code in c}
    result = replay(c, d, s, r)
    assert result["status"] == "INSUFFICIENT_REFERENCE_PATH"
    assert result["reference_capital_depleted"] is True
    assert result["paired_return_dates"] == 1
    assert result["legal_na_dates"] == []
    assert result["daily"][1]["arms"]["B"]["one_sided_risk_turnover"] is None


def test_request_cannot_self_rehash_to_approve_a_different_model(tmp_path):
    from backend.services.hmm_risk.formal_state_model import receipt
    from backend.services.hmm_risk.contracts import canonical_json_bytes
    from backend.services.hmm_risk.risk_l2_value_replay import APPROVED_PINS, VERSION

    request = receipt({"schema_version": VERSION + "_request", **APPROVED_PINS, "model_hash": "a" * 64})
    path = tmp_path / "request.json"
    path.write_bytes(canonical_json_bytes(request))
    with pytest.raises(ReplayError) as error:
        load_inputs(path, request["receipt_sha256"])
    assert error.value.reason_code == "hmm_risk_l2_value_identity_mismatch"


def test_file_only_poison_rejects_producer_and_database_and_bad_artifact(tmp_path):
    with zero_compute():
        with pytest.raises(ReplayError, match="forbidden"):
            risk_l2.prepare_file_inputs(Path("unused"), work_parent=tmp_path, source_commit="x")
        import psycopg2

        with pytest.raises(ReplayError, match="forbidden"):
            psycopg2.connect("unused")
        with pytest.raises(ReplayError):
            load_inputs(tmp_path / "missing.json", "a" * 64)
