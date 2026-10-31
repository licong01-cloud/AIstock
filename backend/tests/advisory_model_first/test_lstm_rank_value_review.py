"""Numerical explanation, role isolation, historical denominators and immutability."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from scripts import advisory_lstm_rank_value_review as review


def example(coef=None):
    core = review.core
    records = []
    for rank in range(1, 51):
        fields = {name: float(np.sin(rank * (j + 1))) for j, name in enumerate(core.spo.FEATURES)}
        records.append(dict(**core.LSTM, decision_as_of_trade_date=pd.Timestamp("2025-04-01"),
            target_trade_date=pd.Timestamp("2025-04-02"), instrument=f"{rank:06}.SZ",
            selection_effective_rank=rank, candidate_group_size=50, parent_score=float(-rank), **fields))
    frame = pd.DataFrame(records)
    model = dict(arm="candidate", cutoff="2025-03-31", contract_sha256=core.spo.sha(core.spo.CONTRACT),
                 fields=list(core.spo.FEATURES), median=[.25] * 8, scale=[2.] * 8,
                 coef=coef if coef is not None else [.1 * (i + 1) for i in range(16)])
    return frame, packet(model)


def packet(model):
    model = deepcopy(model)
    core = review.core
    model["model_payload_sha256"] = core.spo.sha({k: v for k, v in model.items() if k != "model_payload_sha256"})
    result = dict(schema=core.CONTRACT["schema"], contract_sha256=core.spo.sha(core.CONTRACT),
                  target=core.LSTM, fold=1, model=model)
    result["payload_sha256"] = core.spo.sha(result)
    return result


def test_contributions_match_independent_matrix_without_fit_or_live_source(monkeypatch):
    frame, p = example()
    frame.loc[2, "ret_5"] = np.nan
    saved = review.core.predict_specialist(frame, p)
    def forbidden(*args, **kwargs):
        pytest.fail("fitting or live-source execution is forbidden")
    monkeypatch.setattr(review.core, "fit_specialist", forbidden)
    monkeypatch.setattr(review.core.spo, "fit", forbidden)
    monkeypatch.setattr(review.core.spo, "solve_spo", forbidden)
    monkeypatch.setattr(review.shadow.LstmSpoShadowService, "_source", forbidden)
    exp = review.explain(frame, p, saved)
    raw = frame[p["model"]["fields"]].to_numpy(float)
    encoded = np.c_[(np.where(np.isnan(raw), .25, raw) - .25) / 2., np.isnan(raw).astype(float)]
    manual = (encoded - encoded.mean(axis=0)) * np.asarray(p["model"]["coef"])
    np.testing.assert_allclose(manual.sum(axis=1), exp.decision_score, atol=1e-12)
    np.testing.assert_allclose(manual[:, 9], exp.missing__ret_5, atol=1e-12)
    np.testing.assert_allclose(exp.parent_contribution + exp.market_contribution, exp.decision_score)
    assert len(exp) == 50 and exp.advisory_rank.nunique() == 50
    assert exp.selection_change.eq("ENTERED").sum() == exp.selection_change.eq("EXITED").sum()


def test_no_advice_keeps_slot_and_unknown_contributions():
    frame, p = example()
    frame.loc[0, review.core.spo.FEATURES] = np.nan
    saved = review.core.predict_specialist(frame, p)
    exp = review.explain(frame, p, saved)
    assert exp.loc[0, "no_rank_advice"] and exp.loc[0, "advisory_rank"] == 1
    assert pd.isna(exp.loc[0, "decision_score"]) and pd.isna(exp.loc[0, "market_contribution"])
    pairs = review.boundaries(exp, p["model"])
    assert len(pairs) == 400
    assert pairs.loc[pairs.inside.eq("000001.SZ"), "status"].eq("NO_RANK_ADVICE").all()


def test_boundary_distance_really_crosses_pair_under_original_predictor():
    frame, p = example(coef=[2.] + [0.] * 15)
    frame["ret_1"] = np.arange(50, dtype=float)
    exp = review.explain(frame, p, review.core.predict_specialist(frame, p))
    pairs = review.boundaries(exp, p["model"])
    a = exp.loc[exp.advisory_rank.eq(10)].iloc[0]
    b = exp.loc[exp.advisory_rank.eq(11)].iloc[0]
    row = pairs.loc[pairs.inside.eq(a.instrument) & pairs.outside.eq(b.instrument)].iloc[0]
    assert row.distance_scales__ret_1 == pytest.approx(.5)
    changed = frame.copy()
    changed.loc[changed.instrument.eq(b.instrument), "ret_1"] += (row.distance_scales__ret_1 + 1e-6) * 2.
    pred = review.core.predict_specialist(changed, p).set_index("instrument")
    assert pred.loc[b.instrument, "advisory_rank"] < pred.loc[a.instrument, "advisory_rank"]
    assert not any(name.startswith("distance_scales__parent") for name in pairs)
    assert pairs.interpretation.eq("ALGEBRAIC_SENSITIVITY_ONLY").all()


def test_ties_zero_slopes_and_missing_coordinates_are_distinct():
    frame, p = example(coef=[0.] * 16)
    exp = review.explain(frame, p, review.core.predict_specialist(frame, p))
    assert exp.advisory_rank.tolist() == list(range(1, 51))
    pairs = review.boundaries(exp, p["model"])
    assert pairs.status.eq("TIE_LIMIT_ZERO").all()
    assert pairs.coordinate_status__ret_1.eq("ZERO_SLOPE").all()
    assert pairs.min_market_change_scales.isna().all()
    frame.loc[0, "ret_1"] = np.nan
    exp = review.explain(frame, p, review.core.predict_specialist(frame, p))
    pairs = review.boundaries(exp, p["model"])
    assert pairs.loc[pairs.inside.eq("000001.SZ"), "coordinate_status__ret_1"].eq("NONDIFFERENTIABLE_MISSING").all()


@pytest.mark.parametrize("kind", ["duplicate", "package", "rank", "future", "score"])
def test_wrong_population_clock_and_saved_score_rejected(kind):
    frame, p = example()
    saved = review.core.predict_specialist(frame, p)
    if kind == "duplicate":
        saved = pd.concat([saved.iloc[:-1], saved.iloc[[0]]], ignore_index=True)
    elif kind == "package":
        frame["package_id"] = "other"
    elif kind == "rank":
        frame.loc[0, "selection_effective_rank"] = 2
    elif kind == "future":
        frame[review.DAY] = pd.Timestamp("2026-10-30")
    else:
        saved.loc[0, "decision_score"] += 1
    with pytest.raises((ValueError, AssertionError)):
        review.explain(frame, p, saved)


def test_role_and_hash_rejected_before_reading_financial_payload(tmp_path, monkeypatch):
    path = tmp_path / "old.parquet"
    path.write_bytes(b"old")
    monkeypatch.setattr(review, "source_spec", lambda: {"train1": (path, "OLD_TRAINING_ELIGIBILITY")})
    protocol = {"sources": {"train1": dict(path=str(path), role="CURRENT_INPUT_NO_OUTCOMES",
                                          sha256=review.file_sha(path), size_bytes=3)}}
    calls = []
    monkeypatch.setattr(pd, "read_parquet", lambda *a, **k: calls.append(1))
    with pytest.raises(ValueError, match="role"):
        review.read_frame(protocol, "current")
    with pytest.raises(ValueError, match="role"):
        review.read_frame(protocol, "train1")
    protocol["sources"]["train1"]["role"] = "OLD_TRAINING_ELIGIBILITY"
    path.write_bytes(b"new")
    with pytest.raises(ValueError, match="bytes"):
        review.read_frame(protocol, "train1")
    assert calls == []


def test_output_rejects_live_assets_and_code():
    for path in (review.ROOT / "tmp/new", review.MODELS / "new", review.ARCHIVE / "new"):
        with pytest.raises(ValueError, match="output"):
            review.output_root(path)


def test_source_identity_normalizes_only_crlf_and_binds_dependencies(tmp_path, monkeypatch):
    assert review.code_sha(b"x=1\r\n") == review.code_sha(b"x=1\n")
    assert review.code_sha(b"x=1\n") != review.code_sha(b"x=2\n")
    monkeypatch.setattr(review, "ROOT", tmp_path)
    monkeypatch.setattr(review, "SOURCE_FILES", ("operator.py", "predictor.py"))
    (tmp_path / "operator.py").write_bytes(b"a=1\n")
    (tmp_path / "predictor.py").write_bytes(b"b=2\r\n")
    initial = review.source_closure()
    (tmp_path / "predictor.py").write_bytes(b"b=3\r\n")
    assert initial != review.source_closure()
    monkeypatch.setattr(review.subprocess, "check_output", lambda *a, **k: b"different\n")
    with pytest.raises(ValueError, match="uncommitted"):
        review.source_closure(require_committed=True)


def historical_fixture():
    frame, _ = example()
    predictions, finance, panels, days = [], [], [], []
    for fold, (_, first, _) in enumerate(review.core.spo.base.FOLDS, 1):
        for day in pd.bdate_range(first, periods=42):
            part = frame.copy()
            part[review.DAY], part["target_trade_date"] = day, day + pd.offsets.BDay(1)
            part["advisory_rank"] = 51 - part.selection_effective_rank
            part["selection_change"] = np.select([part.selection_effective_rank.le(10), part.advisory_rank.le(10)],
                                                  ["EXITED", "ENTERED"], default="OUTSIDE_BOTH")
            part["fold"] = fold
            predictions.append(part)
            value = part[[*review.KEY, "fold"]].copy()
            value["valuation_status"] = "AVAILABLE"
            value["hypothetical_liquidation_net_bps"] = np.arange(1., 51.)
            value["valuation_policy_sha256"] = review.core.spo.CONTRACT["valuation_policy_sha256"]
            finance.append(value)
            i = len(panels)
            panels.append(dict(package_id=review.core.LSTM["package_id"], decision_date=day, fold=fold,
                mature=i < 111, original_candidates=50, candidate=45.5 if 9 <= i < 111 else np.nan,
                baseline=5.5, candidate_full_model=20., delta=40.,
                avoided_loss=0., added_loss=0., added_profit=45.5, missed_profit=5.5,
                candidate_unknown=0 if 9 <= i < 111 else 1, baseline_unknown=0, shared_unknown=0))
            days.append(dict(decision_date=day, fold=fold, delta=40.))
    return pd.concat(predictions, ignore_index=True), pd.concat(finance, ignore_index=True), pd.DataFrame(panels), pd.DataFrame(days)


def test_historical_stock_attribution_keeps_all_dates_and_primary_denominator():
    pred, finance, panel, daily = historical_fixture()
    finance.loc[0, "valuation_status"] = "UNKNOWN"
    finance.loc[0, "hypothetical_liquidation_net_bps"] = np.nan
    result, stock = review.historical_dependence(pred, finance, panel, daily)
    assert result["original_days"] == 126 and result["common_days"] == 102
    assert result["mature_days"] == 111 and result["original_mean_delta_bps"] == pytest.approx(40.)
    assert len(stock) == 6300 and stock[review.DAY].nunique() == 126
    assert stock.loc[0, "valuation_status"] == "UNKNOWN" and pd.isna(stock.loc[0, "stock_delta_bps"])
    assert len(result["influence"]) == 126 + 26
    assert result["primary_population_changed"] is False and result["independent_confirmation"] is False
    changed = finance.copy()
    changed.loc[500, "hypothetical_liquidation_net_bps"] += 100
    with pytest.raises(AssertionError):
        review.historical_dependence(pred, changed, panel, daily)


def test_rejects_changed_economic_denominator_and_cash_default():
    pred, finance, panel, daily = historical_fixture()
    panel.loc[10, "candidate_full_model"] = np.nan
    with pytest.raises(ValueError, match="denominator"):
        review.historical_dependence(pred, finance, panel, daily)
    panel.loc[10, "candidate_full_model"] = 20.
    finance.loc[0, "valuation_status"] = "CASH_ENTRY_NOT_EXECUTABLE"
    with pytest.raises(ValueError, match="cash"):
        review.historical_dependence(pred, finance, panel, daily)


def test_immutable_publisher_reuse_and_conflicting_results(tmp_path):
    root = tmp_path / "study"
    plan = "a" * 64
    path = review.publish_stage(study_root=root, stage="preregistered", plan_sha256=plan,
                                parent_sha256=None, artifacts={"protocol.json": review.json_bytes({"max_new_fits": 0})})
    initial = review.read_stage(path, stage="preregistered", plan_sha256=plan, parent_sha256=None)
    reused = review.publish_stage(study_root=root, stage="preregistered", plan_sha256=plan,
                                  parent_sha256=None, artifacts={"protocol.json": review.json_bytes({"max_new_fits": 0})})
    assert reused == path and review.load_json(path / "manifest.json") == initial
    with pytest.raises(AdvisoryModelFirstError, match="conflict"):
        review.publish_stage(study_root=root, stage="preregistered", plan_sha256=plan,
                             parent_sha256=None, artifacts={"protocol.json": review.json_bytes({"max_new_fits": 1})})
    assert not (root / "trained").exists()
    assert json.loads(review.json_bytes({"unknown": np.nan})) == {"unknown": None}
