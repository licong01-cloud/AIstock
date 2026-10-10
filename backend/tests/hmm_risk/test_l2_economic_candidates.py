"""Direct causal, score, reference and executor invariants; no dataset or DB."""

from copy import deepcopy
from datetime import date, timedelta

import numpy as np
import pytest
from sklearn.ensemble import GradientBoostingRegressor

from backend.services.hmm_risk import l2_economic_candidates as m
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as hashes
from backend.services.hmm_risk.contracts import canonical_sha256
from scripts.hmm_risk import run_l2_economic_candidates as cli


def target(day, *, event=0, drawdown=-0.01, result=0.0):
    return {
        "trade_date": day,
        "sector_code": "801783.SI",
        "status": "AVAILABLE",
        "event": event,
        "drawdown": drawdown,
        "return": result,
    }


def parameters():
    return {
        "mean": [0.0] * 20,
        "scale": [1.0] + [0.0] * 19,
        "active": [True] + [False] * 19,
        "coef": [[1.0]],
        "intercept": [0.0],
        "iterations": [1],
        "classes": [0, 1],
    }


def test_cost_weights_are_mature_train_only_and_mean_one():
    entries = [target("2024-01-02", event=1, drawdown=-0.24), target("2024-01-03"), target("2024-01-03", result=0.16)]
    assert np.allclose(m.cost_weights(entries), [1.8, 0.3, 0.9])
    assert m.cost_weights(entries).mean() == 1.0
    assert np.allclose(m.date_weights(entries), [1.5, 0.75, 0.75])


@pytest.mark.parametrize(
    "change",
    [{"status": "OUTCOME_NOT_MATURE"}, {"event": 0, "drawdown": -0.09}, {"return": float("nan")}, {"drawdown": -1}],
)
def test_invalid_or_immature_cost_target_fails(change):
    entry = target("2024-01-02")
    entry.update(change)
    with pytest.raises(ValueError):
        m.cost_weights([entry])


def test_scaler_constant_mask_is_exact_and_train_only():
    x = np.zeros((3, 20))
    x[:, 0] = [1, 2, 3]
    x[:, 1] = 1e-12
    params = m.scaler(x)
    assert params["active"] == [True] + [False] * 19
    assert params["mean"][0] == 2
    assert params["scale"][0] == np.std(x[:, 0], ddof=0)
    unseen = np.ones((1, 20)) * 10
    assert m.transform(unseen, params)[0, 0] == (10 - 2) / params["scale"][0]
    assert params["mean"][0] == 2


@pytest.mark.parametrize("shape", [(2, 9), (0, 20), (2, 20)])
def test_bad_matrix_or_all_constant_is_not_clamped(shape):
    with pytest.raises(m.rotation.RotationL2Error):
        m.scaler(np.zeros(shape))


def test_sigmoid_is_action_score_not_probability_with_typed_na():
    rows = {
        "2026-04-01": {
            "801783.SI": {"as_of_date": "2026-03-31", "features": [0.0] * 20, "reason_code": None},
            "801983.SI": {"as_of_date": "2026-03-31", "features": None, "reason_code": "quote_unavailable"},
        }
    }
    bundle = {"rows": rows, "risk_windows": [["2026-04-01"]], "catalog": list(rows["2026-04-01"])}
    result = m.risk_predict(bundle, parameters())
    assert result[0]["risk_action_score"] == 0.5 and result[0]["warning"] is True
    assert "probability" not in result[0] and "PROBABILITY" in result[0]["score_semantics"]
    assert result[1]["risk_action_score"] is result[1]["warning"] is None
    assert result[1]["reason_code"] == "quote_unavailable"


def test_monthly_train_labels_mature_before_origin_without_search():
    days = []
    d = date(2025, 1, 1)
    while d <= date(2026, 8, 31):
        if d.weekday() < 5:
            days.append(d.isoformat())
        d += timedelta(days=1)
    schedules = m.monthly_schedule(days)
    assert len(schedules) == 5
    for s in schedules:
        assert len(s["train_days"]) == 126
        i = days.index(s["origin"])
        assert days[days.index(s["train_days"][-1]) + 10] == s["as_of"]
        assert s["prediction_days"][0] == s["origin"] == days[i]


def test_official_quote_gate_does_not_erase_membership_or_finite_features():
    raw = {"structural_eligible": False, "reason_code": "quote_unavailable", "trade_date": "2026-04-29"}
    result = m.rotation_shell(raw, {"features": [1.0] * 20, "reason_code": None})
    assert result["availability"] == "unavailable" and result["reason_code"] == "quote_unavailable"
    assert result["structural_eligible"] is False


def test_json_tree_restoration_exact_and_leaf_date_readback():
    rng = np.random.default_rng(42)
    x = rng.normal(size=(1000, 20))
    entries = [
        {"trade_date": f"d{i // 20:03d}", "sector_code": str(i), "target": float(x[i, 0] * x[i, 1])}
        for i in range(1000)
    ]
    rows = defaultdict_rows(entries, x)
    bundle = {
        "rows": rows,
        "months": [{"training": entries, "schedule": {"origin": "2026-04-01", "train_days": sorted(rows)}}],
    }
    progress = {"started_fits": 0, "completed_fits": 0}
    fitted = m.fit_rotation(bundle, progress)[0]
    p = fitted["parameters"]
    model = GradientBoostingRegressor(**m.TREE_PARAMS).fit(
        m.transform(x, p), [e["target"] for e in entries], sample_weight=m.date_weights(entries)
    )
    assert np.array_equal(model.predict(m.transform(x, p)), m.tree_predict(x, p))
    assert progress == {"started_fits": 1, "completed_fits": 1}
    assert len(fitted["leaf_distinct_dates"]) == 64
    assert all(v > 0 for t in fitted["leaf_distinct_dates"] for v in t.values())
    broken = deepcopy(p)
    broken["trees"][0]["left"][0] = 0
    with pytest.raises(m.rotation.RotationL2Error, match="cyclic"):
        m.tree_predict(x, broken)


def defaultdict_rows(entries, x):
    result = {}
    for e, values in zip(entries, x, strict=True):
        result.setdefault(e["trade_date"], {})[e["sector_code"]] = {"features": values.tolist()}
    return result


def test_parent_rejects_self_hashed_prediction_drift_without_fit(monkeypatch):
    rows = {"2026-04-01": {"801783.SI": {"as_of_date": "2026-03-31", "features": [0.0] * 20, "reason_code": None}}}
    bundle = {"rows": rows, "risk_windows": [["2026-04-01"]], "catalog": ["801783.SI"], "input_sha256": "input"}
    bundle["risk_training"] = [target("2024-01-02")]
    models = {
        "parameters": parameters(),
        "fit_rows": 1,
        "fit_dates": 1,
        "weights_sha256": canonical_sha256([1.0]),
        "weight_sum": 1.0,
    }
    predictions = m.risk_predict(bundle, parameters())
    predictions[0]["risk_action_score"] = 0.6
    sealed = hashes.seal(
        {
            "candidate": "risk",
            "input_sha256": "input",
            "contract_hash": m.CONTRACT_HASH,
            "models": models,
            "model_sha256": canonical_sha256(models),
            "predictions": predictions,
            "prediction_sha256": canonical_sha256(predictions),
            "fit_counts": {"started_fits": 1, "completed_fits": 1},
        },
        "sealed_sha256",
    )
    monkeypatch.setattr(m.LogisticRegression, "fit", lambda *a, **k: pytest.fail("parent must not fit"))
    with pytest.raises(m.rotation.RotationL2Error, match="readback"):
        m.readback(bundle, sealed)


def risk_fixture():
    codes = [f"{i:06d}.SI" for i in range(131)]
    days = ["2026-04-01", "2026-04-02", "2026-04-03"]
    candidate, original = [], []
    for day, prior in zip(days, ["2026-03-31", *days[:-1]], strict=True):
        for i, c in enumerate(codes):
            row = {
                "trade_date": day,
                "sector_code": c,
                "as_of_date": prior,
                "availability": "available",
                "reason_code": None,
            }
            candidate.append({**row, "risk_action_score": 0.9 if i == 0 else 0.1, "warning": i == 0})
            original.append({**row, "probability": 0.1, "warning": False})
    returns = {d: dict.fromkeys(codes, 0.01) for d in days[1:]}
    return codes, days, candidate, original, returns


def test_economic_reference_has_full_denominator_matched_exposure_and_cost():
    result = m.risk_reference(*risk_fixture())
    first = result["daily"][0]["arms"]
    assert first["C"]["risk_budget"] == first["XC"]["risk_budget"]
    assert first["R"]["risk_budget"] == first["XR"]["risk_budget"] == 1.0
    assert first["C"]["risk_budget"] == pytest.approx(130 / 131)
    assert first["C"]["cost_sensitivity_return"]["20"] == pytest.approx(first["C"]["gross_return"] - 0.002 * 130 / 131)
    assert result["net_return_status"] == "UNASSESSED"


def test_legal_held_na_is_not_zero_or_stitched_and_next_block_stays_visible():
    args = list(risk_fixture())
    args[4]["2026-04-02"][args[0][2]] = None
    result = m.risk_reference(*args)
    assert result["full_path_available"] is False
    assert result["daily"][0]["arms"]["C"]["gross_return"] is None
    assert result["cost_blocks"]["C"]["0"][0]["start"] == "2026-04-03"


@pytest.mark.parametrize("kind", ["duplicate", "missing", "score", "fallback_probability"])
def test_reference_rejects_invalid_scores_or_grid(kind):
    args = list(risk_fixture())
    if kind == "duplicate":
        args[2].append(dict(args[2][0]))
    elif kind == "missing":
        args[2].pop()
    elif kind == "score":
        args[2][0]["risk_action_score"] = float("inf")
    else:
        args[2][0].pop("risk_action_score")
        args[2][0]["probability"] = 0.9
    with pytest.raises((m.rotation.RotationL2Error, KeyError)):
        m.risk_reference(*args)


@pytest.mark.parametrize(
    "change", [{"dataset_manifest_sha256": "v15"}, {"cutoff": "2026-06-30"}, {"frozen_release_generation": "old"}]
)
def test_cross_release_and_old_cutoff_are_explicit_failures(change):
    identity = {
        "dataset_manifest_sha256": m.MANIFEST,
        "frozen_release_binding_sha256": m.BINDING,
        "frozen_release_generation": "20261002-v17-unified-basic-history1",
        "cutoff": "2026-08-31",
    }
    identity.update(change)
    with pytest.raises(m.rotation.RotationL2Error):
        m._release(identity)


def test_cli_fit_failure_has_durable_truthful_receipt(tmp_path, monkeypatch):
    bundle = {"source_commit": "head", "input_sha256": "input"}
    request = tmp_path / "input.json"
    cli.write_once(request, bundle)
    output = tmp_path / "child.json"
    monkeypatch.setattr(cli, "clean_head", lambda: "head")
    monkeypatch.setattr(cli, "environment", lambda: {})

    def failed(_bundle, _candidate, progress):
        progress["started_fits"] = 1
        raise RuntimeError("optimizer failure")

    monkeypatch.setattr(m, "execute", failed)
    assert (
        cli.main(
            [
                "child",
                "--request",
                str(request),
                "--output",
                str(output),
                "--candidate",
                "risk",
                "--process-index",
                "1",
                "--input-sha256",
                "input",
                "--source-commit",
                "head",
            ]
        )
        == 1
    )
    failure = cli.read_json(output.with_name("child.json.failure.json"))
    hashes.verify(failure, "failure_sha256")
    assert failure["fit_counts"] == {"started_fits": 1, "completed_fits": 0}
    assert failure["database_write"] is failure["dataset_write"] is failure["runtime_action"] is False


def test_parent_source_authority_checked_before_any_child(tmp_path, monkeypatch):
    path = tmp_path / "prepared.json"
    cli.write_once(path, {"source_commit": "head", "input_sha256": "tampered", "request": {}})
    monkeypatch.setattr(cli, "clean_head", lambda: "head")
    monkeypatch.setattr(m, "validate_input", lambda value: None)
    monkeypatch.setattr(m, "prepare", lambda request, commit: {"input_sha256": "authority"})
    monkeypatch.setattr(cli.subprocess, "run", lambda *a, **k: pytest.fail("no child before source closure"))
    assert cli.main(["run", "--request", str(path), "--output", str(tmp_path / "run"), "--candidate", "risk"]) == 1
    result = cli.read_json(tmp_path / "run.failure.json")
    assert result["fit_counts"] == {"started_fits": 0, "completed_fits": 0}


def test_two_children_must_match_before_outcome_access(monkeypatch):
    monkeypatch.setattr(m, "validate_input", lambda value: None)
    monkeypatch.setattr(m, "readback", lambda *a: None)
    monkeypatch.setattr(m, "evaluate", lambda *a: pytest.fail("outcomes must remain unopened"))
    children = [
        hashes.seal(
            {
                "schema_version": m.VERSION + "_process",
                "process_index": i,
                "source_commit": "head",
                "numeric_environment": {},
                "sealed": {"candidate": "risk", "changed": i},
            },
            "process_sha256",
        )
        for i in (1, 2)
    ]
    with pytest.raises(m.rotation.RotationL2Error, match="bitwise"):
        cli.close({}, children, "head", "risk")
