"""Public 19/20D price contracts and immutable four-stage orchestration, no actual research."""
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
import time

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.cross_package_consensus_price_5td_contracts_v1 import (
    ARMS, FEATURES, KEY, MARK_POLICY_SHA256, PACKAGES, SCHEMA_SHA256, SOURCE_COLUMNS, CrossPackageConsensusStudyPlanV1,
)
from backend.services.advisory_model_first.cross_package_consensus_price_5td_inputs_v1 import build_consensus_training_encoding_v1
from backend.services.advisory_model_first.cross_package_consensus_price_5td_model_v1 import (
    _weights_for_leaves,
    consensus_model_from_payload_v1, consensus_model_payload_v1, cross_package_consensus_price_set_v1,
    freeze_consensus_distribution_v1, query_frozen_consensus_nodes_v1, train_cross_package_consensus_model_v1,
)
from backend.services.advisory_model_first import cross_package_consensus_price_5td_study_v1 as study
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.tests.advisory_model_first.test_cross_package_consensus_price_5td_inputs_v1 import synthetic_inputs


@pytest.fixture(scope="module")
def models():
    rows, _, _, _, _, parent, plan = synthetic_inputs()
    encoding = build_consensus_training_encoding_v1(rows=rows, parent_encoding=parent, plan=plan)
    fitted = {a: train_cross_package_consensus_model_v1(rows=rows, encoding=encoding, arm=a, plan_sha256="a"*64,
        before_fit=lambda a: None, after_fit=lambda a: None) for a in ARMS}
    return fitted, rows, encoding


def test_19_20_serialization_equal_supervision_and_payment_invariance(models):
    models, rows, _ = models
    for arm, fitted in models.items():
        model = consensus_model_from_payload_v1(consensus_model_payload_v1(fitted))
        assert model.recipe["dimensions"] == (20 if arm == ARMS[0] else 19)
        frozen = freeze_consensus_distribution_v1(fitted=model, d_features=pd.concat([rows.iloc[:1]]*3, ignore_index=True))
        result = query_frozen_consensus_nodes_v1(fitted=model, frozen=frozen, price_cny=[9., 10., 11.], reference_cny=[10.]*3)
        assert np.diff(result.expected_net_bps).max() < 0 and np.diff(result.downside_q90_bps).min() >= 0
        assert np.allclose((result.expected_net_bps/10000+1)*[9., 10., 11.], (result.expected_net_bps.iloc[1]/10000+1)*10.)
        assert result.actual_fill_proven.eq(False).all()
        # Independent brute-force payoff check protects the cached price-law optimization.
        from backend.services.advisory_model_first.generic_population_price_5td_model_v1 import weighted_quantile_v1
        w, _ = _weights_for_leaves(model, frozen.leaf_ids.iloc[0])
        for i, price in enumerate((9., 10., 11.)):
            a = price/10
            net = 10000*(np.asarray(model.calibration["terminal"])*(1-5.95/10000)/(a*(1+.95/10000))-1)
            risk = 10000*np.maximum(0, 1-np.asarray(model.calibration["path"])/a)
            assert result.expected_net_bps.iloc[i] == pytest.approx(w @ net)
            assert result.profit_probability.iloc[i] == pytest.approx(w @ (net > 0))
            assert result.downside_q90_bps.iloc[i] == pytest.approx(weighted_quantile_v1(risk, w, .9))
    assert models[ARMS[0]].calibration["clusters"] == models[ARMS[1]].calibration["clusters"]


def test_full_decimal_grid_unknown_empty_and_reference_tampering(models):
    models, rows, _ = models
    args = dict(fitted=models[ARMS[0]], d_features=rows.iloc[0].to_dict(), reference_cny=10.,
        legal_low_cny=8.005, legal_high_cny=13.005, tick_cny=.01,
        grid_context=dict(source_sha256="c"*64, observed_at="2024-07-05T09:30:00+08:00", coordinate="D_ANCHORED_CNY", currency="CNY"))
    result = cross_package_consensus_price_set_v1(**args)
    assert result["complete_nodes"] == 500 and result["nodes"].price_cny.iloc[0] == "8.01"
    assert result["bands_cny"][0]["low"] == "8.01" and not result["actual_fill_proven"]
    unknown = deepcopy(args)
    unknown["d_features"].update({n: None for n in FEATURES})
    assert cross_package_consensus_price_set_v1(**unknown)["unknown_nodes"] == 500
    args.update(legal_low_cny=100., legal_high_cny=101.)
    assert cross_package_consensus_price_set_v1(**args)["status"] == "EMPTY_ACCEPTABLE_SET"
    args["reference_cny"] = 11.
    with pytest.raises(ValueError, match="reference"):
        cross_package_consensus_price_set_v1(**args)


def test_held_poison_and_no_global_leaf_fallback(models, tmp_path):
    models, rows, encoding = models
    held = rows.iloc[:1].copy()
    held["consensus_training_role"] = "NOT_TRAIN_SUPERVISION"
    held[[*FEATURES, "terminal_mark_ratio"]] = "opaque held finance"
    replay = train_cross_package_consensus_model_v1(rows=pd.concat([rows, held], ignore_index=True), encoding=encoding,
        arm=ARMS[0], plan_sha256="a"*64, before_fit=lambda a: None, after_fit=lambda a: None)
    assert replay.model_sha256 == models[ARMS[0]].model_sha256
    body = asdict(replay)
    tree = dict(feature=[19, -2, -2], threshold=[.5, -2., -2.], left=[1, -1, -1], right=[2, -1, -1],
        members={"1": list(range(len(replay.calibration["terminal"])))})
    body["forest"] = [deepcopy(tree) for _ in range(128)]
    body["model_sha256"] = sha({k: v for k, v in body.items() if k != "model_sha256"})
    model = consensus_model_from_payload_v1(body)
    query = rows.iloc[:1].copy()
    query["cross_package_consensus_strength"], query["consensus_rank_fraction"] = .9, "9/10"
    query["strongest_rank_strength"], query["strongest_rank_fraction"] = 1., "1"
    frozen = freeze_consensus_distribution_v1(fitted=model, d_features=query)
    p = tmp_path/"frozen.parquet"
    p.write_bytes(parquet_bytes(frozen))
    result = query_frozen_consensus_nodes_v1(fitted=model, frozen=pd.read_parquet(p), price_cny=[10.], reference_cny=[10.])
    assert result.status.iloc[0] == "UNKNOWN_MODEL_DISTRIBUTION" and pd.isna(result.expected_net_bps.iloc[0])
    frozen.loc[0, "context_sha256"] = "e"*64
    with pytest.raises(ValueError, match="identity"):
        query_frozen_consensus_nodes_v1(fitted=model, frozen=frozen, price_cny=[10.], reference_cny=[10.])


def parent_plan(tmp_path):
    rows, days, calendar, _, _, encoding, specification = synthetic_inputs()
    sources = [dict(package_id=p, manifest_sha256=v[0], run_id=v[1], source_id=v[2]) for p, v in PACKAGES.items()]
    parent_root, parent_sha = tmp_path/"parent", "d"*64
    initial = publish_stage(study_root=parent_root, stage="preregistered", plan_sha256=parent_sha, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(dict(sources=sources))})
    first = json.loads((initial/"manifest.json").read_bytes())["stage_sha256"]
    daily = pd.DataFrame([dict(trade_date=d, instrument=f"{600000+n}.SH", raw_open_cny=10., raw_high_cny=10.5,
        raw_low_cny=9.7, raw_close_cny=10., adj_factor=1., up_limit=11., down_limit=9.,
        suspended=False, tradability_unknown=False) for d in calendar if d.date() <= specification.evaluation_end for n in range(12)])
    prepared = publish_stage(study_root=parent_root, stage="prepared", plan_sha256=parent_sha, parent_sha256=first,
        artifacts={"rows.parquet": parquet_bytes(rows), "days.parquet": parquet_bytes(days), "encoding.json": _json_bytes(encoding),
        "daily.parquet": parquet_bytes(daily), "calendar.json": _json_bytes([d.date().isoformat() for d in calendar]),
        "receipt.json": _json_bytes(dict(database_written=False))})
    refs = {k: dict(artifact_uri=str(prepared/name), sha256=hashlib.sha256((prepared/name).read_bytes()).hexdigest(),
        size_bytes=(prepared/name).stat().st_size, role=k) for k, name in dict(parent_manifest_ref="manifest.json",
        parent_rows_ref="rows.parquet", parent_days_ref="days.parquet", calendar_ref="calendar.json", encoding_ref="encoding.json", daily_ref="daily.parquet").items()}
    return CrossPackageConsensusStudyPlanV1(**vars(specification), sources=sources, **refs, parent_plan_sha256=parent_sha,
        parent_stage_sha256=json.loads((prepared/"manifest.json").read_bytes())["stage_sha256"],
        parent_preregistered_stage_sha256=first, implementation_sha256=study.consensus_implementation_sha256_v1(),
        source_commit="a"*40, node_python_uri="/existing/python", qe_api_base="http://127.0.0.1:8001/api/v1",
        library_versions={n: "fixture" for n in ("numpy", "pandas", "scikit-learn", "pyarrow", "pydantic")})


def observation(*, busy=False):
    return dict(captured_at=datetime.now(timezone.utc).isoformat(), active_counts=dict(single=int(busy), custom_evo=0, multi_alpha=0))


def test_four_stages_readonly_prepare_and_original_slot_accounting(tmp_path, monkeypatch):
    plan = parent_plan(tmp_path)
    root = tmp_path/"new"
    study.preregister_cross_package_consensus_study_v1(plan=plan, output_root=root)
    prepared = study.prepare_cross_package_consensus_study_v1(plan=plan, output_root=root)
    assert json.loads((prepared/"receipt.json").read_bytes())["database_queries"] == 0
    assert not (prepared/"daily.parquet").exists()
    monkeypatch.setattr(study, "_node", lambda p: dict(fixture=True))
    calls = []
    def probe():
        calls.append(1)
        return observation()
    trained = study.train_cross_package_consensus_study_v1(plan=plan, output_root=root, qe_idle_probe=probe)
    assert len(calls) == 4 and json.loads((trained/"receipt.json").read_bytes())["physical_fit_count"] == 2
    assert study.train_cross_package_consensus_study_v1(plan=plan, output_root=root, qe_idle_probe=lambda: pytest.fail("implicit refit")) == trained
    evaluated = study.evaluate_cross_package_consensus_study_v1(plan=plan, output_root=root)
    result = json.loads((evaluated/"evaluation.json").read_bytes())
    assert result["physical_fit_count"] == 2 and result["actual_fill_proven"] is False
    assert result["activation_evidence"] is False and result["original_package_days"] > result["primary"]["original_mature_days"]*2
    assert all(g["original_slots"] == 5 for g in result["original_groups"])
    assert set(result["primary"]["pairs"]) == {ARMS[1], "baseline"}


@pytest.mark.parametrize("post_condition", ["busy", "runtime_error"])
def test_post_fit_busy_retains_component_and_partial_attempt_cannot_refit(tmp_path, monkeypatch, post_condition):
    plan = parent_plan(tmp_path)
    root = tmp_path/"new"
    study.preregister_cross_package_consensus_study_v1(plan=plan, output_root=root)
    prepared = study.prepare_cross_package_consensus_study_v1(plan=plan, output_root=root)
    monkeypatch.setattr(study, "_node", lambda p: dict(fixture=True))
    probes = iter([observation(), observation(busy=True) if post_condition == "busy" else RuntimeError("public GET failed")])
    def probe():
        item = next(probes)
        if isinstance(item, Exception):
            raise item
        return item
    with pytest.raises(ValueError, match="retain completed|UNKNOWN_POST_FIT"):
        study.train_cross_package_consensus_study_v1(plan=plan, output_root=root, qe_idle_probe=probe)
    assert (prepared.parent/"fits"/ARMS[0]/"trained"/"model.json").is_file()
    assert not (prepared.parent/"trained").exists()
    with pytest.raises(ValueError, match="never implicitly refit"):
        study.train_cross_package_consensus_study_v1(plan=plan, output_root=root, qe_idle_probe=lambda: pytest.fail("QE control"))


def capacity_observation():
    return dict(captured_at=datetime.now(timezone.utc).isoformat(),
        observed_monotonic_ns=time.monotonic_ns(), observer_pid=os.getpid(), sampling_elapsed_seconds=1,
        host=dict(logical_processors=32, cpu_busy_pct=30, available_memory_gib=32),
        node=dict(logical_processors=28, cpu_busy_pct=10, available_memory_gib=32),
        output_free_gib=100, scratch_free_gib=100, scratch_root="/mnt/x/AIstock_temp")


@pytest.mark.parametrize("qe_state", ["busy", "unknown"])
def test_capacity_mode_allows_qe_activity_without_faking_counts_or_refitting(tmp_path, monkeypatch, qe_state):
    plan = CrossPackageConsensusStudyPlanV1.model_validate(dict(parent_plan(tmp_path).model_dump(), qe_fit_policy="CAPACITY_CHECKED_CONCURRENT"))
    root = tmp_path/"new"
    study.preregister_cross_package_consensus_study_v1(plan=plan, output_root=root)
    prepared = study.prepare_cross_package_consensus_study_v1(plan=plan, output_root=root)
    monkeypatch.setattr(study, "_node", lambda p: dict(fixture=True))
    calls = []
    def capacity():
        calls.append(1)
        return capacity_observation()
    def qe():
        if qe_state == "unknown":
            raise RuntimeError("public QE telemetry unavailable")
        return observation(busy=True)
    trained = study.train_cross_package_consensus_study_v1(plan=plan, output_root=root, qe_idle_probe=qe, resource_probe=capacity)
    assert len(calls) == 4 and json.loads((trained/"receipt.json").read_bytes())["physical_fit_count"] == 2
    events = [json.loads(line) for line in (prepared.parent/"fit_journal.jsonl").read_text().splitlines()]
    observations = [e["qe_observation"] for e in events if "qe_observation" in e]
    assert len(observations) == 4
    if qe_state == "busy":
        assert all(any(e["active_counts"].values()) for e in observations)
    else:
        assert all(e["status"] == "UNKNOWN_QE_TELEMETRY" and "active_counts" not in e for e in observations)
    assert study.train_cross_package_consensus_study_v1(plan=plan, output_root=root,
        qe_idle_probe=lambda: pytest.fail("completed run cannot refit"), resource_probe=lambda: pytest.fail("no new fit")) == trained


@pytest.mark.parametrize("when", ["before", "after"])
def test_capacity_shortage_stops_only_new_fit_and_preserves_completed_component(tmp_path, monkeypatch, when):
    plan = CrossPackageConsensusStudyPlanV1.model_validate(dict(parent_plan(tmp_path).model_dump(), qe_fit_policy="CAPACITY_CHECKED_CONCURRENT"))
    root = tmp_path/"new"
    study.preregister_cross_package_consensus_study_v1(plan=plan, output_root=root)
    prepared = study.prepare_cross_package_consensus_study_v1(plan=plan, output_root=root)
    monkeypatch.setattr(study, "_node", lambda p: dict(fixture=True))
    call = 0
    def capacity():
        nonlocal call
        call += 1
        value = capacity_observation()
        if when == "before" or call == 2:
            value["host"]["available_memory_gib"] = 1
        return value
    with pytest.raises(ValueError, match="insufficient|FIT_RESOURCE_UNAVAILABLE_POST_FIT"):
        study.train_cross_package_consensus_study_v1(plan=plan, output_root=root,
            qe_idle_probe=lambda: observation(busy=True), resource_probe=capacity)
    assert (prepared.parent/"fit_attempt.json").exists() == (when == "after")
    assert (prepared.parent/"fits"/ARMS[0]/"trained"/"model.json").exists() == (when == "after")
    assert not (prepared.parent/"trained").exists()


@pytest.mark.parametrize("invalid", ["stale", "naive", "cpu", "bool", "missing", "scratch"])
def test_capacity_unknown_or_insufficient_accounting_never_defaults_to_available(invalid):
    value = capacity_observation()
    if invalid == "stale":
        value["observed_monotonic_ns"] -= 61_000_000_000
    elif invalid == "naive":
        value["captured_at"] = datetime.now().isoformat()
    elif invalid == "cpu":
        value["node"]["cpu_busy_pct"] = 99
    elif invalid == "bool":
        value["host"]["available_memory_gib"] = True
    elif invalid == "missing":
        del value["host"]
    else:
        value["scratch_root"] = "/mnt/c/temp"
    with pytest.raises(ValueError):
        study.validate_consensus_fit_resources_v1(value)


def test_capacity_freshness_uses_same_process_monotonic_time_not_wall_clock_steps():
    from datetime import timedelta
    value = capacity_observation()
    value["captured_at"] = (datetime.now(timezone.utc)+timedelta(seconds=2)).isoformat()
    assert study.validate_consensus_fit_resources_v1(value) == value
    value["observer_pid"] += 1
    with pytest.raises(ValueError, match="stale"):
        study.validate_consensus_fit_resources_v1(value)


def test_hole_in_original_panel_cannot_be_compressed_into_confidence():
    result = study.paired_consensus_statistics_v1({ARMS[1]: [1.]*12+[None]+[2.]*12, "baseline": [1.]*25})
    assert result[ARMS[1]]["original_mature_days"] == 25 and result[ARMS[1]]["paired_days"] == 24
    assert all(v["simultaneous_95_bonferroni_bps"] is None and v["mde_80_bps"] is None for v in result.values())


def test_nonidentifiable_stage_has_zero_fit_and_does_not_read_evaluation(tmp_path, monkeypatch):
    plan = parent_plan(tmp_path)
    first = study.preregister_cross_package_consensus_study_v1(plan=plan, output_root=tmp_path/"new")
    initial = json.loads((first/"manifest.json").read_bytes())["stage_sha256"]
    study._publish(plan, first.parent, "prepared", initial,
        {"encoding.json": _json_bytes(dict(contrast_status="NOT_IDENTIFIABLE_CONSENSUS_CONTRAST"))})
    monkeypatch.setattr(study, "_node", lambda p: pytest.fail("zero fit may not require or control a worker"))
    target = study.train_cross_package_consensus_study_v1(plan=plan, output_root=tmp_path/"new",
        qe_idle_probe=lambda: pytest.fail("no new fit is permitted"))
    assert json.loads((target/"receipt.json").read_bytes())["physical_fit_count"] == 0
    evaluated = study.evaluate_cross_package_consensus_study_v1(plan=plan, output_root=tmp_path/"new")
    assert json.loads((evaluated/"evaluation.json").read_bytes())["financial_evaluation_performed"] is False


def test_opportunity_cost_unknown_cash_and_no_refill_are_separate():
    from types import SimpleNamespace
    rows, days, calendar, _, _, _, plan = synthetic_inputs()
    plan = SimpleNamespace(**vars(plan), sources=[SimpleNamespace(package_id=p, manifest_sha256=v[0], run_id=v[1], source_id=v[2])
        for p, v in PACKAGES.items()], objective_contract="RISK_MANAGED_ADVISORY", study_type="EXPLORATORY_SCREEN")
    predictions = {}
    for arm in ARMS:
        frame = rows.loc[:, [*SOURCE_COLUMNS, "context_sha256", "context_pair_sha256"]].copy()
        frame["schema_sha256"], frame["mark_policy_sha256"], frame["model_sha256"] = SCHEMA_SHA256, MARK_POLICY_SHA256, sha(arm)
        frame["status"] = "AVOID" if arm == ARMS[0] else "ACCEPTABLE"
        frame["expected_net_bps"], frame["profit_probability"], frame["downside_q90_bps"] = (-1. if arm == ARMS[0] else 1.), .5, 100.
        predictions[arm] = frame
    result = study.evaluate_consensus_cohorts_v1(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    pair = result["primary"]["pairs"]["baseline"]
    assert pair["attribution"]["missed_profit_bps"] > 0 and pair["attribution"]["avoided_loss_bps"] == 0
    assert pair["mean_increment_bps"] == pytest.approx(-pair["attribution"]["missed_profit_bps"])
    assert result["status"] == "NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE"
    assert all(e["rank"] <= 5 for e in result["original_episodes"])
    predictions[ARMS[0]]["status"] = "UNKNOWN_MODEL_DISTRIBUTION"
    predictions[ARMS[0]][["expected_net_bps", "profit_probability", "downside_q90_bps"]] = np.nan
    unknown = study.evaluate_consensus_cohorts_v1(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    attribution = unknown["primary"]["pairs"]["baseline"]["attribution"]
    assert attribution["known_action_contribution_bps"] == 0 and attribution["unknown_cash_contribution_bps"] < 0
    # A missing executable-entry valuation remains a hole, not SKIP/zero or a compressed day.
    d = rows.loc[rows[KEY[0]].ge(pd.Timestamp(plan.evaluation_start)), KEY[0]].min()
    rows.loc[rows[KEY[0]].eq(d) & rows.instrument.eq("600000.SH"), "entry_execution_status"] = "UNKNOWN"
    hole = study.evaluate_consensus_cohorts_v1(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    assert hole["primary"]["original_daily_panel"][0]["values_bps"]["baseline"] is None
    assert hole["primary"]["pairs"]["baseline"]["simultaneous_95_bonferroni_bps"] is None
