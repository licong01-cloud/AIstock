"""Exit-only temporal, intervention and no-test-decode contracts; tiny fixtures."""
from datetime import date, timedelta
from datetime import datetime, timezone
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import generic_remaining_value_exit_5td_audit_v1 as m


@pytest.fixture
def population():
    days = [date(2025, 1, 1)+timedelta(days=i) for i in range(110)]
    records = []
    for i in range(100):
        for offset in range(4):
            records.append(dict(episode_id=f"episode_{i}", entry_date=days[i+1], endpoint_date=days[i+5],
                s_date=days[i+1+offset], u_date=days[i+2+offset], selection_effective_rank=1,
                remaining_sessions=4-offset, remaining_session_fraction=(4-offset)/4,
                geometry_status="MATURE", held=True, y_hold_bps=30.+i, baseline_return_bps=100.,
                sell_scenario_bps=100.+offset, sell_return_bps=110.+offset,
                sell_executable=True, **dict.fromkeys(m.FEATURES, float(i))))
    return pd.DataFrame(records), days


def test_date_maturity_embargo_and_whole_episode_cohort_blocks(population):
    rows, calendar = population
    folds = m.exit_fold_schedule_v1(labels=rows, calendar=calendar)
    assert len(folds) == 4
    seen = set()
    for fold in folds:
        train = rows.loc[rows.episode_id.isin(fold["train_episode_ids"])]
        future = rows.loc[rows.episode_id.isin(fold["evaluation_episode_ids"])]
        assert set(train.episode_id).isdisjoint(future.episode_id) and set(future.episode_id).isdisjoint(seen)
        assert calendar.index(min(future.s_date))-calendar.index(max(train.endpoint_date)) >= 2
        assert future.groupby("episode_id").size().eq(4).all()
        seen.update(future.episode_id)
    assert len(seen) == 80 and folds[0]["evaluation_cohorts"][0] == rows.entry_date.iloc[80].isoformat()


def test_frozen_ridge_sees_only_past_labels_and_training_preprocessing(population, monkeypatch):
    rows, calendar = population
    fold = m.exit_fold_schedule_v1(labels=rows, calendar=calendar)[0]
    evaluation = rows.episode_id.isin(fold["evaluation_episode_ids"])
    rows.loc[evaluation, [*m.FEATURES, "y_hold_bps"]] = 1e20  # Must not enter train median/scaling/target.
    rows.loc[~evaluation, m.FEATURES[-1]] = np.nan
    observed, checks = {}, []
    class FixedRidge:
        def __init__(self, alpha):
            assert alpha == 1
        def fit(self, x, y):
            observed.update(x=x, y=y)
            self.coef_, self.intercept_ = np.zeros(x.shape[1]), 0.
            return self
        def predict(self, x):
            return np.zeros(len(x))
    monkeypatch.setattr(m, "Ridge", FixedRidge)
    artifact, predictions = m.fit_exit_fold_v1(rows=rows, fold=fold, before_fit=checks.append)
    assert checks == [1] and artifact["physical_fits"] == 1 and len(predictions) == 80
    assert observed["x"].shape[1] == 19 and max(observed["y"]) < 100
    assert artifact["recipe"]["medians"][-2] == 0 and artifact["recipe"]["medians"][0] < 20
    assert np.all(observed["x"][:, -1] == 1.)  # Last daily-field missing flag is one.


def test_features_reject_future_scenario_and_foreign_keys(population):
    rows, _ = population
    features = rows.loc[:, m.FEATURE_COLUMNS].copy()
    labels = rows.drop(columns=[*m.FEATURES, "remaining_session_fraction"])
    assert len(m._join_features(labels, features)) == len(rows)
    for mutation in ("future", "foreign", "remaining"):
        invalid = features.copy()
        if mutation == "future":
            invalid["u_actual_open"] = 1
        elif mutation == "foreign":
            invalid.loc[0, "episode_id"] = "foreign"
        else:
            invalid.loc[0, "remaining_session_fraction"] = .01
        with pytest.raises(ValueError):
            m._join_features(labels, invalid)


def test_first_executable_intervention_one_chain_and_fixed_five_slots(population):
    rows, _ = population
    rows = rows.loc[rows.episode_id.eq("episode_0")].copy()
    rows.loc[rows.index[0], "sell_executable"] = False
    predictions = rows.loc[:, m.FEATURE_KEY].copy()
    predictions["predicted_y_hold_bps"] = 0.
    episodes, cohorts, decisions = m.evaluate_exit_chains_v1(rows=rows, predictions=predictions, evaluation_episode_ids=["episode_0"])
    assert len(decisions) == 4 and len(episodes) == 1 and episodes.intervention.sum() == 1
    assert episodes.candidate_bps.iloc[0] == 111. and episodes.oracle_bps.iloc[0] == 113.
    assert episodes.remaining_sessions.iloc[0] == 3 and cohorts.empty_slots.iloc[0] == 4
    assert cohorts.candidate_bps.iloc[0] == pytest.approx(111./5)


def test_unknown_cash_not_zero_and_sparse_interventions_not_activation(population):
    rows, _ = population
    rows.loc[rows.episode_id.eq("episode_5"), "baseline_return_bps"] = np.nan
    predictions = rows.loc[:, m.FEATURE_KEY].copy()
    predictions["predicted_y_hold_bps"] = 1e6
    predictions.loc[predictions.episode_id.eq("episode_0"), "predicted_y_hold_bps"] = 0
    episodes, cohorts, _ = m.evaluate_exit_chains_v1(rows=rows, predictions=predictions, evaluation_episode_ids=rows.episode_id.unique())
    result = m.summarize_exit_v1(episodes=episodes, cohorts=cohorts)
    assert len(cohorts) == 100 and result["complete_cohorts"] == 99 and result["unknown_cohorts"] == 1
    assert cohorts.loc[cohorts.entry_date.eq(rows.entry_date.iloc[20]), "candidate_bps"].isna().all()
    assert result["intervention_support"]["episodes"] == 1 and not result["intervention_minima_met"]
    assert result["candidate_increment_ci95_bps"] is not None and result["mde80_bps"] is not None
    assert result["decision_use"] == "NAVIGATION_ONLY" and not result["deployable"]


def test_financial_projection_filters_test_before_decode(tmp_path):
    path = tmp_path/"quotes.parquet"
    pd.DataFrame(dict(trade_date=pd.to_datetime(["2025-09-30", "2025-10-09"]), close=[100., np.inf], forbidden_future_label=[2., 1e20])).to_parquet(path)
    frame = m._read_development(path, ("trade_date", "close"), date(2025, 9, 30))
    assert len(frame) == 1 and frame.close.iloc[0] == 100. and list(frame.columns) == ["trade_date", "close"]


def test_s_features_do_not_change_when_later_price_and_factor_are_poisoned():
    days = [date(2025, 1, 1)+timedelta(days=i) for i in range(30)]
    geometry = pd.DataFrame([dict(episode_id="one", s_date=days[19], instrument="000001.SZ", remaining_sessions=4)])
    raw = pd.DataFrame([dict(trade_date=day, instrument="000001.SZ", adj_factor=1.,
        raw_open_cny=100.+i, raw_close_cny=100.+i, raw_high_cny=101.+i, raw_low_cny=99.+i) for i, day in enumerate(days)])
    volumes = pd.DataFrame(dict(trade_date=days, instrument="000001.SZ", volume_hand=100))
    benchmark = pd.DataFrame(dict(trade_date=days, instrument="000300.SH", close=np.arange(30)+200.))
    arguments = dict(geometry=geometry, calendar=days, raw=raw, volumes=volumes, benchmark=benchmark,
        cutoff=days[24], source_evidence="UNIT_FIXTURE")
    before = m._feature_rows(**arguments)
    raw.loc[raw.trade_date.gt(days[19]), ["adj_factor", "raw_open_cny", "raw_close_cny", "raw_high_cny", "raw_low_cny"]] *= 100
    after = m._feature_rows(**arguments)
    pd.testing.assert_frame_equal(before, after)
    assert before.ret_1.iloc[0] == pytest.approx(119./118.-1) and pd.isna(before.market_up_ratio.iloc[0])


@pytest.mark.parametrize("failure", [False, True])
def test_fresh_idle_fit_budget_resume_and_no_implicit_crash_retry(population, tmp_path, monkeypatch, failure):
    rows, calendar = population
    source = tmp_path/"source.json"
    source.write_text(json.dumps(dict(configuration=dict(train_start=calendar[0].isoformat(),
        validation_end=calendar[-1].isoformat(), test_start=(calendar[-1]+timedelta(days=1)).isoformat()),
        dataset_identity="tiny-unit-fixture-not-research", decision_dates=[calendar[0].isoformat()])), encoding="utf8")
    refs = {name: m.evidence_reference_for_file(source, role=name).model_dump(mode="json") for name in
        ("calendar", "roster", "raw_daily", "volume_daily", "index_daily")}
    plan = dict(schema_version="exit5_remaining_value_plan_v1", parent_plan=m.evidence_reference_for_file(source, role="original_development_scope").model_dump(mode="json"),
        inputs=refs, dataset_identity="tiny-unit-fixture-not-research",
        parent_lineage=["unit-fixture"], decision_dates=[calendar[0].isoformat()], development_start=calendar[0].isoformat(),
        development_cutoff=calendar[-1].isoformat(), implementation_sha256=m.implementation_sha256(), source_evidence="UNIT_FIXTURE")
    prereg = m.preregister_exit5_v1(plan=plan, output_root=tmp_path)
    invalid = dict(plan, development_cutoff=(calendar[-1]+timedelta(days=1)).isoformat())
    with pytest.raises(ValueError, match="original test/sealed"):
        m._load(invalid, tmp_path)
    root, identity, _ = m._load(plan, tmp_path)
    manifest = m._stage(root, identity, "preregistered")
    m.publish_stage(study_root=root, stage="prepared", plan_sha256=identity, parent_sha256=manifest["stage_sha256"],
        artifacts={"rows.parquet": m._parquet(rows), "folds.json": m._json_bytes(m.exit_fold_schedule_v1(labels=rows, calendar=calendar))})
    assert prereg.exists()
    checks, simulated_fits = [], []
    def idle():
        checks.append(1)
        return dict(checked_at_utc=datetime.now(timezone.utc).isoformat(), running_counts=dict(single=0, custom_evo=0, multi_alpha=0))
    def one_fold(*, rows, fold, before_fit):
        before_fit(fold["ordinal"])
        simulated_fits.append(fold["ordinal"])
        if failure:
            raise RuntimeError("simulated interruption not a real fit")
        output = rows.loc[rows.episode_id.isin(fold["evaluation_episode_ids"]), m.FEATURE_KEY].copy()
        output["predicted_y_hold_bps"] = 0.
        return dict(physical_fits=1, ordinal=fold["ordinal"]), output
    monkeypatch.setattr(m, "fit_exit_fold_v1", one_fold)
    busy = idle()
    busy["running_counts"]["multi_alpha"] = 1
    with pytest.raises(ValueError, match="fresh three-path"):
        m.train_exit5_v1(plan=plan, output_root=tmp_path, qe_idle_check=lambda: busy)
    assert not simulated_fits and not (root/"folds"/"1"/"fit_attempt.json").exists()
    if failure:
        with pytest.raises(RuntimeError):
            m.train_exit5_v1(plan=plan, output_root=tmp_path, qe_idle_check=idle)
        with pytest.raises(ValueError, match="unresolved physical fit"):
            m.train_exit5_v1(plan=plan, output_root=tmp_path, qe_idle_check=idle)
        assert simulated_fits == [1]
    else:
        trained = m.train_exit5_v1(plan=plan, output_root=tmp_path, qe_idle_check=idle)
        assert json.loads((trained/"models.json").read_text(encoding="utf8"))["physical_fits"] == 4
        before = len(checks)
        m.train_exit5_v1(plan=plan, output_root=tmp_path, qe_idle_check=idle)
        assert simulated_fits == [1, 2, 3, 4] and len(checks) == before
