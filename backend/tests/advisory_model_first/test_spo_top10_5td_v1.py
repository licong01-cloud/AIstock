"""Decision mathematics, causal training and immutable bounded-study contracts."""

from copy import deepcopy
import itertools
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first import spo_top10_5td_v1 as core
from backend.services.advisory_model_first import spo_top10_5td_pipeline_v1 as pipe
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY
from backend.services.advisory_model_first.prequential_top10_5td_study_v1 import FINANCE


def population():
    records = []
    for d in pd.to_datetime(["2024-11-01", "2024-11-04", "2025-04-01"]):
        for package in ("a", "b"):
            for rank in range(1, 51):
                y = float(100 - rank)
                records.append(dict(package_id=package, manifest_sha256="a" * 64, run_id="run_" + package,
                                    source_id="source", decision_as_of_trade_date=d,
                                    target_trade_date=d + pd.offsets.BDay(1), instrument=f"{rank-1:06}.SZ",
                                    selection_effective_rank=rank, candidate_group_size=50, parent_score=-rank,
                                    **{n: np.sin(rank * (i + 1) / 50) for i, n in enumerate(core.FEATURES)},
                                    label_information_end=d + pd.offsets.BDay(5), valuation_status="AVAILABLE",
                                    hypothetical_liquidation_net_bps=y, observed_gap_bps=0.0,
                                    valuation_gross_terminal_ratio=(1 + y / 10000) * (1 + POLICY["buy_bps"] / 10000) / (1 - POLICY["sell_bps"] / 10000),
                                    exit_execution_status="NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF",
                                    valuation_policy_sha256=core.base.VALUATION_POLICY_SHA256))
    return pd.DataFrame(records)


@pytest.mark.parametrize("k", [1, 2, 3])
def test_oracle_spo_bound_and_subgradient_against_enumeration(k):
    x = np.array([[1.0, 0.0], [-1.0, 2.0], [0.5, -1.0], [0.0, 0.0]])
    y, w = np.array([2.0, -3.0, 1.0, 0.0]), np.array([0.7, -0.2])
    groups = [np.arange(4)]
    s = x @ w
    loss, grad = core.spo_oracle(x, y, groups, [1.0], w, k)
    sets = list(itertools.combinations(range(4), k))
    best = max(np.mean(y[list(c)]) for c in sets)
    expected = max(np.mean((2 * s - y)[list(c)]) for c in sets) - 2 * s @ core.topk_weights(y, k) + best
    regret = best - y @ core.topk_weights(s, k)
    assert loss == pytest.approx(expected) and loss >= regret - 1e-12
    for step in [np.array([0.1, -0.3]), np.array([-2.0, 1.0])]:
        changed = core.spo_oracle(x, y, groups, [1.0], w + step, k)[0]
        assert changed >= loss + grad @ step - 1e-10


def test_spo_is_not_pairwise_square_and_ties_stable():
    x, y = np.eye(3), np.array([2.0, 0.0, -1.0])
    half, exact = y / 2, y.copy()
    assert core.spo_oracle(x, y, [np.arange(3)], [1.0], half, 1)[0] == pytest.approx(0)
    assert core.spo_oracle(x, y, [np.arange(3)], [1.0], exact, 1)[0] == pytest.approx(0)
    assert np.square(half - y).sum() > np.square(exact - y).sum()
    np.testing.assert_array_equal(core.topk_weights(np.zeros(4), 2), [0.5, 0.5, 0, 0])


def test_convex_solver_certificate_and_analytic_one_dimensional_optimum():
    # n=2,k=1,r=(1,-1),s=(w,-w): L=max(0,2-4w).
    # lambda=.01 => optimum w=.5; regularized optimum .00125.
    x, y = np.array([[1.0], [-1.0]]), np.array([1.0, -1.0])
    coef, cert = core.solve_spo(x, y, [np.arange(2)], [1.0], k=1)
    assert coef[0] == pytest.approx(0.5, abs=1e-4)
    assert cert["lower"] <= 0.00125 + 1e-10 <= cert["upper"] + 1e-10
    assert cert["gap"] <= 1e-5 * max(1, cert["upper"])


def test_solver_failures_never_become_models(monkeypatch):
    x, y, groups = np.eye(3), np.array([2.0, -1.0, 0.0]), [np.arange(3)]
    monkeypatch.setattr(core, "minimize", lambda *a, **k: SimpleNamespace(success=False, x=np.ones(2)/2, nit=1, message="failed"))
    with pytest.raises(ValueError, match="did not converge"):
        core.solve_spo(x, y, groups, [1.0], k=1)


def test_solver_invalid_and_exhausted_budget(monkeypatch):
    with pytest.raises(ValueError, match="partition"):
        core.solve_spo(np.eye(3), np.ones(3), [np.array([0, 1])], [1.0], k=1)
    monkeypatch.setitem(core.CONTRACT["solver"], "max_cuts", 1)
    with pytest.raises(ValueError, match="cut budget"):
        core.solve_spo(np.eye(3), np.arange(3.0), [np.arange(3)], [1.0], k=1)


def test_future_poison_and_common_complete_group_training():
    rows = population()
    expected, _ = core.training(rows, "2025-03-31")
    future = rows.decision_as_of_trade_date > pd.Timestamp("2025-03-31")
    rows.loc[future, [*core.FEATURES, "hypothetical_liquidation_net_bps"]] = np.inf
    actual, _ = core.training(rows, "2025-03-31")
    pd.testing.assert_frame_equal(expected, actual)
    # One not-yet-mature member removes its whole group only from training.
    rows.loc[0, "label_information_end"] = pd.Timestamp("2025-04-02")
    rows.loc[0, "hypothetical_liquidation_net_bps"] = np.inf
    reduced, stats = core.training(rows, "2025-03-31")
    assert len(reduced) == len(expected) - 50 and stats["excluded_train_groups"] == 1
    assert len(rows) == 300


def test_cross_package_contradiction_and_future_cutoff_rejected():
    rows = population()
    rows.loc[50, "hypothetical_liquidation_net_bps"] = 12.0
    with pytest.raises(ValueError):
        core.training(rows, "2025-03-31")
    with pytest.raises(ValueError, match="outside"):
        core.training(population(), "2026-01-01")


@pytest.fixture(scope="module")
def pair():
    rows = population()
    return {arm: core.fit(rows, "2025-03-31", arm) for arm in ("matched", "candidate")}


def test_same_training_encoding_unknown_slots_and_model_identity(pair):
    a, b = pair["matched"], pair["candidate"]
    for key in ("fields", "median", "scale", "training_keys_sha256", "training_support"):
        assert a[key] == b[key]
    assert b["certificate"]["converged"]
    rows = population()
    rows.loc[rows.selection_effective_rank.eq(1), list(core.FEATURES)] = np.nan
    p = core.predict(rows, b)
    assert len(p) == len(rows) and p.groupby(core.GROUP).selected.sum().eq(10).all()
    assert p.loc[p.no_rank_advice, "advisory_rank"].eq(1).all()
    corrupted = deepcopy(b)
    corrupted["coef"][0] += 1
    with pytest.raises(ValueError, match="identity"):
        core.predict(rows, corrupted)
    broken = rows.copy()
    broken.loc[0, "selection_effective_rank"] = 2
    with pytest.raises(ValueError, match="Top50"):
        core.predict(broken, b)


def test_fresh_process_json_model_prediction_parity(pair, tmp_path):
    rows = population()
    rows.to_parquet(tmp_path / "inputs.parquet")
    (tmp_path / "model.json").write_text(json.dumps(pair["candidate"]), encoding="utf-8")
    code = "from pathlib import Path; import json,pandas as pd; from backend.services.advisory_model_first import spo_top10_5td_v1 as m; p=Path(__import__('sys').argv[1]); m.predict(pd.read_parquet(p/'inputs.parquet'),json.loads((p/'model.json').read_text())).to_parquet(p/'predictions.parquet')"
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[3])
    subprocess.run([sys.executable, "-c", code, str(tmp_path)], check=True, env=env, timeout=40, capture_output=True)
    pd.testing.assert_frame_equal(core.predict(rows, pair["candidate"]), pd.read_parquet(tmp_path / "predictions.parquet"))


def test_native_endpoint_not_replaced_by_matched():
    rows = []
    for i, day in enumerate(pd.bdate_range("2025-04-01", periods=60)):
        for package in ("a", "b"):
            rows.append(dict(package_id=package, decision_date=day, fold=i//20+1, mature=True, original_candidates=50,
                             candidate=30.0, baseline=10.0, candidate_full_model=50.0, delta=20.0,
                             intervention_count=1, matched_intervention_count=1, candidate_unknown=0, baseline_unknown=0))
    cohort = pd.DataFrame(rows)
    report, _ = core.summarize(cohort)
    assert report["classification"] == "INNER_SUPPORTED" and report["versus_matched"]["mean_bps"] < 0
    assert not report["proceed_outer"]
    cohort.candidate = 0.0
    assert core.summarize(cohort)[0]["classification"] == "INNER_NOT_SUPPORTED"


def test_claim_interrupt_budget_and_clock(tmp_path):
    study = object.__new__(pipe.Study)
    study.root, study.digest, study.budget = tmp_path, "a" * 64, lambda: None
    study.claim("fold1_matched")
    with pytest.raises(ValueError, match="unpublished fit"):
        pipe.fit_pair(study, None, None, None, prefix="fold1", cutoff="2025-03-31", first="2025-04-01", last="2025-05-30", progress=lambda e: None)
    with pytest.raises(ValueError, match="unregistered"):
        study.claim("fold4_candidate")
    with pytest.raises(ValueError, match="outside"):
        study.finance("2026-01-01", "2026-01-02", cutoff="2026-01-02")


def test_final_is_conditional_and_unsupported_never_claimed(tmp_path):
    study = SimpleNamespace(root=tmp_path)
    assert pipe.final_candidate(study, "parent", None, {"classification":"INNER_INCONCLUSIVE"}) == ("parent", False)
    (tmp_path / "fit_claims").mkdir()
    (tmp_path / "fit_claims/final_candidate_started.json").write_text("{}")
    with pytest.raises(ValueError, match="without supported"):
        pipe.final_candidate(study, None, None, {"classification":"INNER_NOT_SUPPORTED"})
    with pytest.raises(ValueError, match="unpublished final"):
        pipe.final_candidate(study, None, None, {"classification":"INNER_SUPPORTED"})


def test_source_hash_drift_detected(tmp_path):
    (tmp_path / "prepared").mkdir()
    for name in ("inputs.parquet", "train_finance.parquet"):
        (tmp_path / "prepared" / name).write_bytes(b"fixture")
    source = SimpleNamespace(root=tmp_path)
    before = pipe.source_hashes(source)
    (tmp_path / "prepared/inputs.parquet").write_bytes(b"altered")
    with pytest.raises(ValueError, match="drift"):
        pipe.verify_hashes(source, before)


def test_finance_projects_maturity_before_decoding(tmp_path, monkeypatch):
    rows = population()
    rows.loc[0, "label_information_end"] = pd.Timestamp("2025-04-02")
    rows.loc[0, "hypothetical_liquidation_net_bps"] = np.inf
    folder = tmp_path / "prepared"
    folder.mkdir()
    rows.to_parquet(folder / "train_finance.parquet", index=False)
    study = object.__new__(pipe.Study)
    study.source = SimpleNamespace(root=tmp_path)
    real_read, reads = pd.read_parquet, []

    def read(*args, **kwargs):
        value = real_read(*args, **kwargs)
        reads.append(kwargs)
        if "columns" not in kwargs:
            assert value.label_information_end.le(pd.Timestamp("2025-03-31")).all()
            assert np.isfinite(value.hypothetical_liquidation_net_bps).all()
        return value

    monkeypatch.setattr(pipe.pd, "read_parquet", read)
    projected = study.finance("2024-07-04", "2025-03-31", cutoff="2025-03-31")
    assert len(reads) == 2 and len(projected) == 200
    assert "hypothetical_liquidation_net_bps" not in reads[0]["columns"]
    assert projected.iloc[0].valuation_status == "IMMATURE"
    assert pd.isna(projected.iloc[0].hypothetical_liquidation_net_bps)


def test_supported_final_publishes_loadable_model_once(tmp_path):
    rows = population()
    inputs = rows[[*core.IDENTITY, "source_id", "parent_score", *core.FEATURES]]
    finance = rows[[*core.KEY, *FINANCE]]
    study = object.__new__(pipe.Study)
    study.root, study.digest, study.budget = tmp_path, "a" * 64, lambda: None
    study.finance = lambda *args, **kwargs: finance
    parent, frozen = pipe.final_candidate(study, "b" * 64, inputs, {"classification": "INNER_SUPPORTED"})
    assert frozen
    stage = tmp_path / "final/trained"
    model = json.loads((stage / "model.json").read_bytes())
    actual = core.predict(pd.read_parquet(stage / "probe_inputs.parquet"), model)
    pd.testing.assert_frame_equal(actual, pd.read_parquet(stage / "probe_predictions.parquet"))
    assert len(actual) == 100 and len(list((tmp_path / "fit_claims").glob("*_started.json"))) == 1
    assert pipe.final_candidate(study, "b" * 64, inputs, {"classification": "INNER_SUPPORTED"}) == (parent, True)
    assert not json.loads((stage / "readiness.json").read_bytes())["independent_confirmation"]


def test_all_seven_claims_exhaust_budget(tmp_path):
    study = object.__new__(pipe.Study)
    study.root, study.digest, study.budget = tmp_path, "a" * 64, lambda: None
    for key in [f"fold{i}_{arm}" for i in (1, 2, 3) for arm in ("matched", "candidate")] + ["final_candidate"]:
        study.claim(key)
    with pytest.raises(ValueError, match="budget exhausted"):
        study.claim("final_candidate")


def test_many_cuts_noisy_synthetic_primal_dual_certificate():
    rng = np.random.default_rng(42)
    x = rng.normal(size=(18000, 16))
    y = rng.normal(0, 5, len(x)) + x[:, 0] * 0.2
    x = x.reshape(-1, 50, 16)
    x -= x.mean(axis=1, keepdims=True)
    x = x.reshape(-1, 16)
    groups = [np.arange(i, i + 50) for i in range(0, len(x), 50)]
    weights = np.ones(len(groups)) / len(groups)
    with threadpool_limits(limits=2):
        coef, cert = core.solve_spo(x, y, groups, weights)
    actual = core.spo_oracle(x, y, groups, weights, coef, 10)[0] + 0.005 * (coef @ coef)
    assert actual == pytest.approx(cert["upper"])
    assert 0 <= cert["gap"] <= 1e-5 * max(1, abs(actual))
    assert cert["cuts"] > 20
