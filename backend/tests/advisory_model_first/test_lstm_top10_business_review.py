"""Saved cohort arithmetic and additive read-only status projection."""
from copy import deepcopy

import pytest

from backend.services.advisory_model_first import lstm_top10_business_review_v1 as business

app = business.shadow


def example():
    predictions, finance = [], []
    values = {1: 300., 2: -400., 3: 0., 4: 100., 11: 800., 12: -200., 13: 0., 14: -100.}
    for i in range(1, 51):
        key = dict(**app.TARGET, decision_as_of_trade_date="2026-10-30", target_trade_date="2026-11-02", instrument=f"{i:06}.SZ")
        rank = i + 10 if i <= 4 else i - 10 if 11 <= i <= 14 else i
        predictions.append(dict(**key, selection_effective_rank=i, advisory_rank=rank))
        finance.append(dict(**key, valuation_policy_sha256=app.RELEASE["valuation_policy_sha256"],
            valuation_status="CASH_ENTRY_NOT_EXECUTABLE" if i in {3, 13} else "AVAILABLE",
            hypothetical_liquidation_net_bps=values.get(i, 10.)))
    prediction = dict(sha256="prediction", predictions=predictions)
    result = app.paired_result(app.frame(predictions), app.frame(finance))
    return prediction, dict(prediction_sha256="prediction", result=result, finance=finance)


def test_saved_ten_slot_components_and_set_changes_reconstruct_delta():
    prediction, evaluation = example()
    before = deepcopy((prediction, evaluation))
    result = business.review_day(prediction, evaluation)
    assert result["native_net_bps"] == 6 and result["specialist_net_bps"] == 56 and result["delta_bps"] == 50
    assert result["changes"] == dict(ENTERED=4, EXITED=4, RETAINED=6, OUTSIDE=36)
    assert result["components"] == dict(added_profit_bps=80., missed_profit_bps=40., avoided_loss_bps=40., added_loss_bps=30.)
    assert sum(r["delta_contribution_bps"] for r in result["rows"]) == pytest.approx(50.)
    assert result["native_cash_slots"] == result["specialist_cash_slots"] == 1
    assert result["target_status"] == "POINT_ESTIMATE_BELOW_TARGET"
    assert result["target_increment_bps"] == 100
    assert before == (prediction, evaluation)


def test_unmatured_all_returns_stay_unknown_and_ranks_remain_available():
    prediction, _ = example()
    result = business.review_day(prediction)
    assert result["status"] == "IMMATURE_OR_PENDING" and result["delta_bps"] is None
    assert all(r["net_bps"] is None and r["delta_contribution_bps"] is None for r in result["rows"])
    assert all(v is None for v in result["components"].values())
    assert result["native_cash_slots"] is result["native_unknown_slots"] is None
    assert result["changes"]["ENTERED"] == 4 and result["target_status"] == "NOT_EVALUATED"


def test_unknown_selected_slot_keeps_partial_arm_and_withholds_components():
    prediction, evaluation = example()
    evaluation["finance"][10].update(valuation_status="UNKNOWN", hypothetical_liquidation_net_bps=None)
    evaluation["result"] = app.paired_result(app.frame(prediction["predictions"]), app.frame(evaluation["finance"]))
    result = business.review_day(prediction, evaluation)
    assert result["status"] == "UNKNOWN" and result["native_net_bps"] == 6
    assert result["specialist_net_bps"] is result["delta_bps"] is None
    assert result["rows"][10]["net_bps"] is None
    assert result["specialist_unknown_slots"] == 1 and all(v is None for v in result["components"].values())


def test_unknown_outside_both_tens_does_not_change_their_results():
    prediction, evaluation = example()
    evaluation["finance"][-1].update(valuation_status="UNKNOWN", hypothetical_liquidation_net_bps=None)
    result = business.review_day(prediction, evaluation)
    assert result["status"] == "COMPLETE" and result["delta_bps"] == 50
    assert result["rows"][-1]["net_bps"] is result["rows"][-1]["delta_contribution_bps"] is None


@pytest.mark.parametrize("change", ["population", "rank", "package", "parent", "result", "cash"])
def test_identity_and_arithmetic_errors_are_visible(change):
    prediction, evaluation = example()
    if change == "population":
        prediction["predictions"].pop()
    elif change == "rank":
        prediction["predictions"][0]["advisory_rank"] = 2
    elif change == "package":
        prediction["predictions"][0]["package_id"] = "other"
    elif change == "parent":
        evaluation["prediction_sha256"] = "another"
    elif change == "result":
        evaluation["result"]["delta_bps"] += 1
    else:
        evaluation["finance"][2]["hypothetical_liquidation_net_bps"] = 1
    with pytest.raises(ValueError):
        business.review_day(prediction, evaluation)


def test_summary_uses_the_same_complete_pairs_and_discloses_unknown_denominators():
    _, evaluation = example()
    rows = [dict(evaluation=evaluation["result"]), dict(evaluation=dict(status="UNKNOWN")),
            dict(evaluation=dict(status="IMMATURE_OR_PENDING"))]
    result = business.summarize(rows)
    assert result["complete_pairs"] == 1 and result["captured_days"] == 3
    assert result["unknown_days"] == result["unevaluated_days"] == 1
    assert result["native_mean_net_bps"] == 6 and result["mean_delta_bps"] == 50
    assert result["evidence_scope"] == "DESCRIPTIVE_COMPLETE_PAIRS_ONLY"
    assert business.target_status(100.) == "POINT_ESTIMATE_AT_OR_ABOVE_TARGET"
    assert business.summarize([])["mean_delta_bps"] is None


def test_status_projection_never_executes_inference_source_or_writes(tmp_path, monkeypatch):
    prediction, evaluation = example()
    service = app.LstmSpoShadowService(root=tmp_path)
    monkeypatch.setattr(service, "_release", lambda: dict(sha256="release"))
    monkeypatch.setenv("AISTOCK_ADVISORY_LSTM_SPO_SHADOW_ENABLED", "true")
    monkeypatch.setattr(service, "_source", lambda: pytest.fail("status cannot read a market source"))
    monkeypatch.setattr(app.spo, "fit", lambda *_: pytest.fail("status cannot fit"))
    day = service.root / "days/2026-10-30"
    prediction.pop("sha256")
    prediction.update(release_sha256="release", publication=dict(target_date="2026-11-02"),
                      observed_at="2026-10-31T06:00:00+00:00", source_receipt={})
    packet = app.publish(day / "prediction.json", prediction)
    evaluation.update(prediction_sha256=packet["sha256"], observed_at="2026-11-06T08:00:00+00:00")
    app.publish(day / "evaluations/one.json", evaluation)
    before = {str(p): p.read_bytes() for p in service.root.rglob("*.json")}
    result = service.status(program_id=app.PROGRAM)
    assert result["business_summary"]["mean_delta_bps"] == 50
    assert result["selected"]["business_review"]["changes"]["ENTERED"] == 4
    assert result["business_contract"]["target_increment_bps"] == 100
    assert not result["production_ranking_replaced"]
    assert before == {str(p): p.read_bytes() for p in service.root.rglob("*.json")}
    assert service.status(program_id="other")["status"] == "NOT_APPLICABLE"
