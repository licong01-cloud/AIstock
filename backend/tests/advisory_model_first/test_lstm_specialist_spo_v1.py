"""Package identity, causal projection, fit budget and the two research decisions."""

from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import lstm_specialist_spo_v1 as core


def inputs(days=("2025-04-01", "2025-05-30")):
    records = []
    for d in pd.to_datetime(days):
        for rank in range(1, 51):
            records.append(dict(**core.LSTM, decision_as_of_trade_date=d, target_trade_date=d + pd.offsets.BDay(1),
                instrument=f"{rank:06}.SZ", selection_effective_rank=rank, candidate_group_size=50, parent_score=-rank,
                **{name: np.sin(rank * (i + 1)) for i, name in enumerate(core.spo.FEATURES)}))
    return pd.DataFrame(records)


def packet():
    m = dict(arm="candidate", cutoff="2025-03-31", contract_sha256=core.spo.sha(core.spo.CONTRACT),
             fields=list(core.spo.FEATURES), median=[0.] * 8, scale=[1.] * 8, coef=list(range(16)))
    m["model_payload_sha256"] = core.spo.sha(m)
    p = dict(schema=core.CONTRACT["schema"], contract_sha256=core.spo.sha(core.CONTRACT),
             target=deepcopy(core.LSTM), fold=1, model=m)
    p["payload_sha256"] = core.spo.sha(p)
    return p


def test_persisted_model_unknown_keeps_original_slot():
    frame = inputs()
    frame.loc[0, core.spo.FEATURES] = np.nan
    p = json.loads(json.dumps(packet()))
    prediction = core.predict_specialist(frame, p)
    pd.testing.assert_frame_equal(prediction, core.spo.predict(frame, p["model"]))
    assert prediction.loc[0, "no_rank_advice"] and prediction.loc[0, "advisory_rank"] == 1
    assert prediction.groupby(core.spo.GROUP).selected.sum().eq(10).all()


@pytest.mark.parametrize("kind", ["package", "manifest", "source", "fold", "training_date", "outer", "tamper"])
def test_wrong_population_clock_or_model_is_rejected(kind):
    frame, p = inputs(), packet()
    if kind in ("package", "manifest", "source"):
        field = {"package": "package_id", "manifest": "manifest_sha256", "source": "source_id"}[kind]
        frame[field] = "wrong"
    elif kind == "fold":
        p["fold"] = 4
    elif kind == "training_date":
        frame = inputs(("2025-03-31",))
    elif kind == "outer":
        frame = inputs(("2026-03-11",))
    else:
        p["model"]["coef"][0] += 1
    with pytest.raises(ValueError):
        core.predict_specialist(frame, p)


def test_training_rejects_future_population_before_fit():
    calls = []
    with pytest.raises(ValueError, match="cutoff"):
        core.fit_specialist(inputs(), 1, before_fit=lambda: calls.append(1), budget=lambda: None)
    assert calls == []


def test_finance_filters_package_and_future_h_before_payload(tmp_path):
    frame = inputs()
    f = frame[core.KEY].copy()
    f["label_information_end"] = f[core.DAY] + pd.offsets.BDay(5)
    f["valuation_status"] = "AVAILABLE"
    f["hypothetical_liquidation_net_bps"] = 10.
    f["valuation_gross_terminal_ratio"] = 1.01
    f["observed_gap_bps"] = 0.
    f["exit_execution_status"] = "NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF"
    f["valuation_policy_sha256"] = core.spo.base.VALUATION_POLICY_SHA256
    other = f.copy()
    other["package_id"] = "other"
    other["hypothetical_liquidation_net_bps"] = -99999.
    path = tmp_path / "finance.parquet"
    pd.concat([f, other]).to_parquet(path, index=False)
    result = core.finance_projection(path, "2025-04-01", "2025-05-30", "2025-05-30")
    assert len(result) == 100 and result.package_id.eq(core.LSTM["package_id"]).all()
    future = result[core.DAY].eq(pd.Timestamp("2025-05-30"))
    assert result.loc[future, "valuation_status"].eq("IMMATURE").all()
    assert result.loc[future, "hypothetical_liquidation_net_bps"].isna().all()
    with pytest.raises(ValueError, match="clock"):
        core.finance_projection(path, "2025-04-01", "2026-08-28", "2026-08-28")


def test_claims_forbid_retry_fourth_fit_and_cross_plan(tmp_path):
    claims = core.FitClaims(tmp_path / "claims", "a" * 64)
    claims.write(1)
    with pytest.raises(FileExistsError):
        claims.write(1)
    with pytest.raises(ValueError, match="different plan"):
        core.FitClaims(claims.root, "b" * 64).write(2)
    claims.write(1, "c" * 64)
    claims.write(2)
    claims.write(3)
    with pytest.raises(ValueError, match="budget"):
        claims.write(2)
    with pytest.raises(ValueError, match="fourth fit"):
        claims.write(4)
    assert len(list(claims.root.glob("*_started.json"))) == 3
    assert len(list(claims.root.glob("*_finished.json"))) == 1


def cohorts(candidate=20., shared=15.):
    records = []
    for fold, (_, first, _) in enumerate(core.spo.base.FOLDS, 1):
        for day in pd.bdate_range(first, periods=7):
            records.append(dict(package_id=core.LSTM["package_id"], decision_date=day, fold=fold,
                mature=True, original_candidates=50, candidate=candidate, baseline=10., candidate_full_model=shared,
                delta=candidate-10., candidate_unknown=0, baseline_unknown=0, shared_unknown=0,
                intervention_count=2, matched_intervention_count=1, avoided_loss=0., added_loss=0.,
                added_profit=max(candidate-10., 0.), missed_profit=max(10.-candidate, 0.)))
    return pd.DataFrame(records)


def test_native_and_specialization_have_separate_verdicts():
    report, _ = core.summarize(cohorts(shared=25.))
    assert report["classification"] == "INNER_SUPPORTED"
    assert report["specialization"] == "SPECIALIZATION_NOT_SUPPORTED"
    assert report["next_step"] == "STOP_EXACT_SPECIALIST_NO_REFIT"
    report, _ = core.summarize(cohorts(candidate=5., shared=0.))
    assert report["classification"] == "INNER_NOT_SUPPORTED"
    assert report["specialization"] == "SPECIALIZATION_SUPPORTED_DESCRIPTIVE"
    assert report["next_step"] == "STOP_EXACT_SPECIALIST_NO_REFIT"


def test_three_arm_common_axis_preserves_missing_day_and_pairwise_denominator():
    data = cohorts()
    data.loc[0, "candidate_full_model"] = np.nan
    data.loc[0, "shared_unknown"] = 1
    report, daily = core.summarize(data)
    assert len(daily) == 21 and report["common_days"] == 20
    assert pd.isna(daily.loc[0, "delta"])
    assert report["pairwise_native"]["paired_days"] == 21
    assert report["statistics"]["paired_days"] == 20
    assert report["next_step"] == "DESIGN_FINAL_AND_NEW_PROSPECTIVE_ONLY"
    assert not report["production_ready"] and not report["independent_oos_evidence"] and not report["final_model_fitted"]


def test_risk_failure_and_fold_axis_are_not_ignored():
    data = cohorts()
    data["candidate_unknown"] = 1
    report, _ = core.summarize(data)
    assert report["classification"] == "INNER_NOT_SUPPORTED"
    with pytest.raises(ValueError, match="three-fold"):
        core.summarize(data.loc[data.fold.ne(3)])


def test_real_publication_all_stages_and_fresh_readback(tmp_path):
    root, parent, plan = tmp_path / "study", None, "a" * 64
    for logical in core.STAGE_PATHS:
        data = {"receipt.json": json.dumps(dict(logical_stage=logical)).encode()}
        digest = core.publish_artifacts(root, logical, plan, parent, data)
        path = core.stage_location(root, logical)
        actual = core.read_stage(path, stage=path.name, plan_sha256=plan, parent_sha256=parent)
        assert actual["stage_sha256"] == digest
        assert core.publish_artifacts(root, logical, plan, parent, data) == digest
        with pytest.raises(Exception, match="conflicts"):
            core.publish_artifacts(root, logical, plan, parent, {"receipt.json": b'{"changed":true}'})
        parent = digest
    assert (root / "fold1/trained/manifest.json").is_file()
    assert (root / "terminal/evaluated/manifest.json").is_file()
    with pytest.raises(ValueError, match="unregistered"):
        core.stage_location(root, "fold4_trained")
