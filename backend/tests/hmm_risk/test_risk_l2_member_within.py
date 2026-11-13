"""W contracts only; synthetic kernel fits do not consume formal research fits."""

from copy import deepcopy
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_member_return as h
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt
from scripts.hmm_risk import run_risk_l2_member_within as cli

HEAD = "a" * 40
NUMERIC = dict(
    versions=h.loss.EXPECTED_VERSIONS,
    thread_variables=dict.fromkeys(h.THREAD_VARIABLES, "1"),
    thread_pools=[dict(num_threads=1)],
)


def seal(obj):
    return receipt({k: v for k, v in obj.items() if k != "receipt_sha256"})


@pytest.fixture
def inputs(monkeypatch):
    codes = [f"801{i:03d}.SI" for i in range(131)]
    days = ["2026-03-13", "2026-03-16"]
    rows = [
        [d, codes[i], [float(i), float(j)] + [0.0] * 18, i / 10 + j / 2, i / 100]
        for j, d in enumerate(days)
        for i in range(3 + 2 * j)
    ]
    plan = dict(train=days + ["2026-03-31"], fit=days, predict=["2026-04-01", "2026-04-02"])
    monkeypatch.setattr(h, "schedule", lambda _, v: {**plan, "predict": plan["predict"] if v == h.VINTAGES[0] else []})
    packet = receipt(
        dict(
            schema_version=h.VERSION + "_train",
            contract=h.CONTRACT,
            rows=rows,
            catalog=codes,
            vintage=h.VINTAGES[0],
            calendar=pd.bdate_range("2026-03-12", "2026-04-20").strftime("%Y-%m-%d").tolist(),
            excluded={"ORIGINAL_COMMON_MASK_EXCLUDED": 131 * 3 - len(rows)},
            mask_sha256=h.canonical_sha256([r[:2] for r in rows]),
            release_binding=dict(manifest_sha256=h.PINS["manifest"]),
            feature_definition_sha256=h.PINS["feature_definition"],
            feature_sha256="f" * 64,
            target_facts_sha256="e" * 64,
            source_commit=HEAD,
        )
    )
    _, _, _, scaler = h.loss._ridge_inputs([r[:4] for r in rows])
    params = dict(**scaler, coef=[0.1] * sum(scaler["active"]), intercept=0.0)
    reference = receipt(
        dict(
            schema_version=h.VERSION + "_model",
            contract=h.CONTRACT,
            arm="S",
            vintage=packet["vintage"],
            parameters=params,
            model_sha256=h.canonical_sha256(params),
            packet_sha256=packet["receipt_sha256"],
            mask_sha256=packet["mask_sha256"],
            release_binding=packet["release_binding"],
            source_commit=HEAD,
            catalog=codes,
            numeric_environment=NUMERIC,
            fit_rows=len(rows),
            fit_dates=2,
        )
    )
    return packet, reference


def fit(inputs, tmp_path):
    return h.within_fit(*inputs, tmp_path / "model", tmp_path / "attempts", HEAD, NUMERIC)


def test_train_residuals_reuse_scaler_and_equal_date_weights(inputs):
    packet, reference = inputs
    x, y, weights, scaler = h.within_training(packet, reference)
    for day in {r[0] for r in packet["rows"]}:
        ids = [i for i, r in enumerate(packet["rows"]) if r[0] == day]
        np.testing.assert_allclose(x[ids].mean(axis=0), 0, atol=1e-15)
        assert y[ids].mean() == pytest.approx(0, abs=1e-15)
        assert weights[ids].sum() == pytest.approx(len(y) / 2)
    assert scaler == {k: reference["parameters"][k] for k in ("mean", "scale", "active")}
    assert np.count_nonzero(x[:, 1]) == 0  # Date-only feature remains an active but zero residual column.


def test_fitted_objective_matches_independent_normal_equations(inputs, tmp_path):
    frozen = deepcopy(inputs)
    x, y, weights, _ = h.within_training(*inputs)
    expected = np.linalg.solve(x.T @ (weights[:, None] * x) + np.eye(x.shape[1]), x.T @ (weights * y))
    model = fit(inputs, tmp_path)
    np.testing.assert_allclose(model["parameters"]["coef"], expected, atol=1e-13)
    assert abs(model["parameters"]["intercept"]) < 1e-15
    assert model["parameters"]["coef"][0] > 0
    assert abs(model["parameters"]["coef"][1]) < 1e-15
    assert inputs == frozen
    with pytest.raises(FormalStateError):
        h.validate_model(model)  # W must not masquerade as the approved S schema.
    with pytest.raises(FormalStateError):
        h.within_validate_model(inputs[1])


@pytest.mark.parametrize("field", ["scale", "packet_sha256", "vintage"])
def test_reference_mismatch_stops_before_attempt(inputs, tmp_path, field):
    packet, model = inputs
    model = deepcopy(model)
    if field == "scale":
        model["parameters"]["scale"][0] *= 2
        model["model_sha256"] = h.canonical_sha256(model["parameters"])
    elif field == "vintage":
        model[field] = h.VINTAGES[1]
    else:
        model[field] = "0" * 64
    with pytest.raises(FormalStateError):
        h.within_fit(packet, seal(model), tmp_path / "model", tmp_path / "attempts", HEAD, NUMERIC)
    assert not (tmp_path / "attempts").exists()


def test_failed_fit_consumes_attempt_and_cannot_retry_new_output(inputs, tmp_path, monkeypatch):
    from sklearn.linear_model import Ridge

    calls = []

    def broken(*args, **kwargs):
        calls.append(1)
        raise RuntimeError("numerical failure")

    monkeypatch.setattr(Ridge, "fit", broken)
    with pytest.raises(RuntimeError):
        fit(inputs, tmp_path)
    with pytest.raises((FormalStateError, FileExistsError)):
        h.within_fit(*inputs, tmp_path / "another-model", tmp_path / "attempts", HEAD, NUMERIC)
    assert calls == [1]


def forecast(inputs, model):
    packet, _ = inputs
    days, codes = ["2026-04-01", "2026-04-02"], packet["catalog"]
    features = receipt(
        dict(
            calendar=packet["calendar"],
            catalog=codes,
            prediction_dates=days,
            as_of={d: packet["calendar"][packet["calendar"].index(d) - 1] for d in days},
            rows={
                d: {
                    c: dict(
                        features=[float(i), 12.0] + [0.0] * 18 if i < (3 if j == 0 else 1) else None,
                        reason_code=None if i < (3 if j == 0 else 1) else "legal_na",
                    )
                    for i, c in enumerate(codes)
                }
                for j, d in enumerate(days)
            },
            release_binding=packet["release_binding"],
        )
    )
    models = {}
    for v in h.VINTAGES:
        models[v["id"]] = seal({**model, "vintage": v})
    acceptance = receipt(
        dict(
            schema_version=h.WITHIN_VERSION + "_fit_acceptance",
            contract=h.WITHIN_CONTRACT,
            status="COMPLETED",
            source_commit=HEAD,
            completed_fits=6,
            fresh_processes=2,
            models=models,
            features_sha256=features["receipt_sha256"],
        )
    )
    return features, acceptance


def test_ex_ante_centering_full_catalog_na_and_common_shift_invariance(inputs, tmp_path, monkeypatch):
    features, acceptance = forecast(inputs, fit(inputs, tmp_path))
    monkeypatch.setattr(
        h, "validate_prediction_features", lambda _: None
    )  # Tiny synthetic calendar; real validator tested by S suite.
    prediction = h.within_predict(acceptance, features)
    rows = prediction["predictions"]
    assert len(rows) == 2 * 131 and len(prediction["centering_sets"]["2026-04-01"]["codes"]) == 3
    assert rows[0]["relative_member_return_score"] < 0 < rows[2]["relative_member_return_score"]
    assert rows[3]["relative_member_return_score"] is None and rows[3]["reason_code"] == "legal_na"
    assert rows[131]["reason_code"] == "insufficient_cross_section"
    assert all(r["relative_member_return_score"] is None for r in rows[131:])
    assert "expected_member_return" not in rows[0] and "probability" not in rows[0]
    shifted = deepcopy(features)
    for item in shifted["rows"]["2026-04-01"].values():
        if item["features"] is not None:
            item["features"] = [v + 7.0 for v in item["features"]]
    shifted = seal(shifted)
    rebound = seal({**acceptance, "features_sha256": shifted["receipt_sha256"]})
    other = h.within_predict(rebound, shifted)["predictions"]
    np.testing.assert_allclose(
        [r["relative_member_return_score"] for r in rows[:3]],
        [r["relative_member_return_score"] for r in other[:3]],
        atol=1e-14,
    )
    changed = seal({**acceptance, "features_sha256": "0" * 64})
    with pytest.raises(FormalStateError):
        h.within_predict(changed, features)
    features["rows"]["2026-04-01"][features["catalog"][0]]["features"][0] = float("inf")
    with pytest.raises(FormalStateError):
        h.within_predict(acceptance, features)


def test_fit_ledger_prevents_reentry_even_after_child_failure(tmp_path, monkeypatch):
    args = SimpleNamespace(preflight=tmp_path / "preflight.json")
    p = dict(
        receipt_sha256="f" * 64,
        features_sha256="b" * 64,
        wrappers={v["id"]: dict(path="explicit", sha256="a" * 64) for v in h.VINTAGES},
    )
    monkeypatch.setattr(cli, "asset", lambda *_: {})
    calls = []

    def fail(*_):
        calls.append(1)
        raise RuntimeError("child failed")

    monkeypatch.setattr(cli, "child", fail)
    with pytest.raises(RuntimeError):
        cli.fit_all(args, tmp_path / "first", HEAD, p)
    with pytest.raises((FormalStateError, FileExistsError)):
        cli.fit_all(args, tmp_path / "second", HEAD, p)
    assert calls == [1]


def test_cli_explicit_wrong_frozen_pin_is_not_ignored(tmp_path, monkeypatch):
    args = SimpleNamespace(prepared_sha256="0" * 64, controls_sha256=None, baseline_sha256=None)
    monkeypatch.setattr(h, "within_inputs", lambda *_: pytest.fail("wrong pin must fail before asset read"))
    with pytest.raises(FormalStateError, match="pin differs"):
        cli.preflight(args, tmp_path / "output", HEAD, NUMERIC)


def test_evaluation_seal_blocks_all_label_reads(monkeypatch):
    monkeypatch.setattr(h, "load_request", lambda *_: pytest.fail("unsealed source read"))
    sealed = receipt(dict(schema_version=h.WITHIN_VERSION + "_prediction_acceptance", status="PASS", fresh_processes=1))
    with pytest.raises(FormalStateError):
        h.within_evaluate({}, {}, {}, {}, {}, {}, sealed)


def test_closure_requires_two_equal_processes_and_exact_reference(inputs, tmp_path):
    packet, reference = inputs
    model = fit(inputs, tmp_path)
    group, wrappers = {}, {}
    for v in h.VINTAGES:
        q = v["id"]
        group[q] = seal({**model, "vintage": v})
        wrappers[q] = receipt(
            dict(
                schema_version=h.WITHIN_VERSION + "_train_reference",
                contract=h.WITHIN_CONTRACT,
                source_commit=HEAD,
                packet_sha256=packet["receipt_sha256"],
                reference_S=seal({**reference, "vintage": v}),
            )
        )
    closed = h.within_close([group, deepcopy(group)], wrappers, HEAD, "b" * 64)
    assert closed["completed_fits"] == 6 and closed["fresh_processes"] == 2
    with pytest.raises(FormalStateError):
        h.within_close([group], wrappers, HEAD, "b" * 64)
    wrappers["2026Q3"]["reference_S"] = reference
    wrappers["2026Q3"] = seal(wrappers["2026Q3"])
    with pytest.raises(FormalStateError, match="identity"):
        h.within_close([group, group], wrappers, HEAD, "b" * 64)


def test_synthetic_w_two_fresh_processes(inputs, tmp_path):
    import json
    import os
    import subprocess
    import sys

    path = tmp_path / "synthetic.json"
    path.write_text(json.dumps(dict(packet=inputs[0], reference=inputs[1], numeric=NUMERIC)), encoding="utf-8")
    code = """import json,sys
from pathlib import Path
from backend.services.hmm_risk import risk_l2_member_return as h
from scripts.hmm_risk.run_risk_l2_loss_severity import guard
p=json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
h.schedule=lambda _,v: dict(train=['2026-03-13','2026-03-16','2026-03-31'],fit=['2026-03-13','2026-03-16'])
with guard(allow_ridge=True):
 m=h.within_fit(p['packet'],p['reference'],Path(sys.argv[2])/'model',Path(sys.argv[2])/'attempt','a'*40,p['numeric'])
print(m['receipt_sha256'])
"""
    env = {**os.environ, **dict.fromkeys(h.THREAD_VARIABLES, "1"), "PYTHONPATH": str(cli.ROOT), "PYTHONUTF8": "1"}
    values = [
        subprocess.check_output([sys.executable, "-c", code, str(path), str(tmp_path / str(i))], env=env, cwd=tmp_path)
        for i in (1, 2)
    ]
    assert values[0] == values[1] and len(values[0].strip()) == 64


def test_three_arm_common_metrics_centered_errors_and_cohort_dedup():
    days, codes = ["2026-04-01", "2026-04-02"], ["801001.SI", "801002.SI", "801003.SI"]
    f = dict(
        prediction_dates=days,
        catalog=codes,
        calendar=days,
        rows={d: {c: dict(features=[0.0, float(i)] + [0.0] * 18) for i, c in enumerate(codes)} for d in days},
    )

    def rows(field, fn):
        return [dict(trade_date=d, sector_code=c, **{field: fn(d, i)}) for d in days for i, c in enumerate(codes)]

    w = receipt(dict(predictions=rows("relative_member_return_score", lambda d, i: 10 + (i - 1) / 100)))
    baseline = receipt(
        dict(
            predictions=dict(
                S=rows("expected_member_return", lambda d, i: (1 - i) / 100 if d == days[0] else 0.0),
                C=rows("expected_member_loss", lambda d, i: i / 100),
            )
        )
    )
    labels = {
        d: {
            c: dict(status="AVAILABLE", reason=None, B=max(0.0, (1 - i) / 100), C=0.02, upside=0.02 + (i - 1) / 100)
            for i, c in enumerate(codes)
        }
        for d in days
    }
    cohort = dict(
        rows=[
            dict(date=d, stock=str(i), code=c, stock_return=(i - 1) / 100, reason=None, seed=s)
            for s in (42, 2024, 2026)
            for d in days
            for i, c in enumerate(codes)
        ],
        secondary_rows=[dict(do_not_use=True)],
    )
    r = h.within_metrics(w, baseline, labels, f, cohort)
    assert r["l2"]["full"]["delta"]["mean"] == pytest.approx(2.0)
    assert r["daily_l2"][days[0]]["versus_S"] == pytest.approx(2.0)
    assert r["l2"]["full"]["versus_S"]["mean"] is None  # Existing HAC rule: fewer than two dates is undefined.
    assert r["l2"]["full"]["coverage"] == dict(requested_dates=2, paired_WC=2, paired_WS=1, undefined_dates=[])
    assert r["auxiliary"]["full"]["centered_errors"]["W_mse"] < 1e-26
    assert r["cohort_source_rows"] == 18 and r["cohort_unique_keys"] == 6
    assert r["status"] == "RESEARCH_LEAD_REQUIRES_VALUE_REVIEW" and not r["economic_value_confirmed"]
    cohort["rows"][-1]["stock_return"] = 0.9
    with pytest.raises(FormalStateError, match="cross-seed"):
        h.within_metrics(w, baseline, labels, f, cohort)
