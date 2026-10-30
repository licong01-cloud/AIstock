"""Direct loss contract tests; synthetic unit fits are not formal research fits."""

from copy import deepcopy
import hashlib

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_loss_severity as h
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_executor import write_once
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt
from scripts.hmm_risk import run_risk_l2_loss_severity as cli


def reseal(body):
    return receipt({k: v for k, v in body.items() if k != "receipt_sha256"})


def target(value, drawdown=-0.1):
    return dict(status="AVAILABLE", event=int(drawdown <= -0.08), drawdown=drawdown, **{"return": value})


@pytest.fixture
def sample(monkeypatch):
    codes = [f"801{i:03d}.SI" for i in range(131)]
    plans = {
        "2026Q2": dict(train=["2026-03-16", "2026-03-31"], fit=["2026-03-16"], predict=["2026-04-01"]),
        "2026Q3": dict(train=["2026-06-15", "2026-06-30"], fit=["2026-06-15"], predict=["2026-07-01"]),
    }
    monkeypatch.setattr(h.rolling, "schedule", lambda _c, v: plans[v["id"]])
    calendar = pd.bdate_range("2026-03-13", "2026-07-20").strftime("%Y-%m-%d").tolist()
    days = sorted({d for p in plans.values() for d in p["train"] + p["predict"]})
    as_of = {d: calendar[calendar.index(d) - 1] for d in days}
    binding = dict(manifest_sha256=h.MANIFEST_PIN)
    features = receipt(
        dict(
            calendar=calendar,
            catalog=codes,
            as_of=as_of,
            model_identity=dict(feature_definition_sha256="b" * 64),
            source=dict(release_binding=binding),
            inference_identity=dict(
                release_identity=dict(
                    dataset_manifest_sha256=h.MANIFEST_PIN, frozen_release_binding_sha256=canonical_sha256(binding)
                )
            ),
            rows={
                d: {
                    c: dict(as_of_date=as_of[d], features=[i / 131.0] + [0.0] * 19, reason_code=None)
                    for i, c in enumerate(codes)
                }
                for d in days
            },
        )
    )
    monkeypatch.setattr(h, "FEATURE_PIN", features["receipt_sha256"])
    labels = {}
    for v in h.rolling.VINTAGES:
        p = plans[v["id"]]
        labels[v["id"]] = receipt(
            dict(
                schema_version=h.rolling.VERSION + "_train_labels",
                vintage=v,
                feature_sha256=features["receipt_sha256"],
                target=h.rolling.TARGET,
                labels={
                    p["train"][0]: {c: target(-i / 1310) for i, c in enumerate(codes)},
                    p["train"][1]: {
                        c: dict(status="OUTCOME_NOT_MATURE", event=None, drawdown=None, **{"return": None})
                        for c in codes
                    },
                },
            )
        )
        monkeypatch.setitem(h.LABEL_PINS, v["id"], labels[v["id"]]["receipt_sha256"])
    return features, labels


@pytest.mark.parametrize("value,drawdown,expected", [(0.1, -0.2, 0.0), (0, -0.3, 0.0), (-0.2, -0.2, 0.2)])
def test_terminal_loss_is_not_peak_drawdown(value, drawdown, expected):
    assert h.loss_from_label(target(value, drawdown), mature=True) == expected


def test_missing_immature_and_nonfinite_are_not_zero():
    label = dict(status="OUTCOME_NOT_MATURE", event=None, drawdown=None, **{"return": None})
    assert h.loss_from_label(label, mature=False) is None
    for invalid in (target(float("nan")), target(-1), label):
        with pytest.raises(FormalStateError):
            h.loss_from_label(invalid, mature=True)


def test_weighting_scaler_and_fixed_ridge_solution():
    rows = [
        ("a", "x", [0.0] + [2.0] * 19, 0.0),
        ("b", "x", [1.0] + [2.0] * 19, 0.1),
        ("b", "y", [2.0] + [2.0] * 19, 0.3),
    ]
    params, stats = h.fit_parameters(rows)
    assert stats["training_loss_mean"] == pytest.approx(0.1)
    assert stats["sample_weight_sum"] == pytest.approx(3)
    assert params["active"] == [True] + [False] * 19
    assert params["mean"][0] == 1 and params["scale"][0] == pytest.approx(np.sqrt(2 / 3))
    z = np.array([[-np.sqrt(1.5), 1], [0, 1], [np.sqrt(1.5), 1]])
    weights = np.diag([1.5, 0.75, 0.75])
    expected = np.linalg.solve(z.T @ weights @ z + np.diag([1, 0]), z.T @ weights @ np.array([0, 0.1, 0.3]))
    assert params["coef"] + [params["intercept"]] == pytest.approx(expected.tolist())


def fit(sample):
    features, labels = sample
    return h.fit_vintage(features, labels["2026Q2"], h.rolling.VINTAGES[0], "a" * 40, lambda: None)


def test_future_data_cannot_change_training_and_maturity_is_checked(sample, monkeypatch):
    features, labels = sample
    first = fit(sample)
    changed = deepcopy(features)
    changed["rows"]["2026-07-01"][features["catalog"][0]]["features"][0] = 1e8
    changed = reseal(changed)
    updated = reseal({**labels["2026Q2"], "feature_sha256": changed["receipt_sha256"]})
    monkeypatch.setattr(h, "FEATURE_PIN", changed["receipt_sha256"])
    monkeypatch.setitem(h.LABEL_PINS, "2026Q2", updated["receipt_sha256"])
    second = fit((changed, {"2026Q2": updated}))
    assert first["model"]["parameters"] == second["model"]["parameters"]
    assert first["predictions"] == second["predictions"]
    bad = deepcopy(updated)
    bad["labels"]["2026-03-31"][features["catalog"][0]] = target(-0.2)
    bad = reseal(bad)
    monkeypatch.setitem(h.LABEL_PINS, "2026Q2", bad["receipt_sha256"])
    attempts = []
    with pytest.raises(FormalStateError):
        h.fit_vintage(changed, bad, h.rolling.VINTAGES[0], "a" * 40, lambda: attempts.append(1))
    assert attempts == []


def test_identity_clipping_na_and_new_schema(sample):
    features, _ = sample
    model = fit(sample)["model"]
    params = {**model["parameters"], "coef": [0.0], "intercept": -0.2}
    model = reseal({**model, "parameters": params, "model_sha256": canonical_sha256(params)})
    rows = deepcopy({"2026-04-01": features["rows"]["2026-04-01"]})
    rows["2026-04-01"][features["catalog"][0]].update(features=None, reason_code="hmm_risk_quote_unavailable")
    result, counts = h.predict(model, rows, ["2026-04-01"], features["as_of"], features["calendar"])
    assert len(result) == 131 and counts["clipped_rows"] == 130
    assert result[0]["expected_loss"] is None and result[0]["availability"] == "unavailable"
    assert result[1]["raw_expected_loss"] == -0.2 and result[1]["expected_loss"] == 0
    assert "probability" not in result[1] and "warning" not in result[1]
    for key, value in [
        ("schema_version", "old_probability"),
        ("training_label_sha256", "f" * 64),
        ("feature_sha256", "f" * 64),
    ]:
        with pytest.raises(FormalStateError):
            h.validate_model(reseal({**model, key: value}))
    params = {**params, "coef": ["0"]}
    with pytest.raises(FormalStateError):
        h.parameter_arrays(params)
    with pytest.raises(FormalStateError):
        h.predict(model, rows, ["2026-07-01"], features["as_of"], features["calendar"])
    del rows["2026-04-01"][features["catalog"][1]]
    with pytest.raises(FormalStateError):
        h.predict(model, rows, ["2026-04-01"], features["as_of"], features["calendar"])


def test_children_reproducibility_and_budget():
    body = receipt(
        dict(
            contract=h.CONTRACT,
            fits=2,
            source_commit="a" * 40,
            original_prepared_sha256=h.PREPARED_PIN,
            sealed=[],
            evaluation={},
            numeric_environment={},
        )
    )
    assert cli.close_processes(body, body, "a" * 40)["completed_fits"] == 4
    for changed in (reseal({**body, "fits": 3}), reseal({**body, "evaluation": {"changed": True}})):
        with pytest.raises(FormalStateError):
            cli.close_processes(body, changed, "a" * 40)


def test_seal_order_and_no_retry_after_kernel_failure(sample, tmp_path, monkeypatch):
    features, labels = sample
    locations = {}
    for v, body in labels.items():
        path = tmp_path / f"{v}.json"
        write_once(path, body)
        locations[v] = dict(path=str(path), sha256=body["receipt_sha256"])
    facts = receipt({"unit": "evaluation"})
    path = tmp_path / "facts.json"
    write_once(path, facts)
    original = dict(training_labels=locations, outcome_path=str(path), outcome_sha256=facts["receipt_sha256"])
    monkeypatch.setattr(h, "load_study", lambda _: (original, features))
    monkeypatch.setattr(h, "load_baselines", lambda *_: {})
    monkeypatch.setattr(h, "evaluate", lambda *_: {})
    read = h.core.inputs._asset
    output = tmp_path / "out"

    def audited(path, sha):
        if path == locations["2026Q3"]["path"]:
            assert (output / "2026Q2.sealed.json").is_file()
        if path == original["outcome_path"]:
            assert (output / "2026Q3.sealed.json").is_file()
        return read(path, sha)

    monkeypatch.setattr(h.core.inputs, "_asset", audited)
    assert h.execute_child("unit", {}, output, "a" * 40)["fits"] == 2
    assert len(list(output.glob("*.attempt.json"))) == 2

    def broken(_):
        raise RuntimeError("synthetic kernel failure")

    monkeypatch.setattr(h, "fit_parameters", broken)
    failed = tmp_path / "failed"
    with pytest.raises(RuntimeError):
        h.execute_child("unit", {}, failed, "a" * 40)
    assert len(list(failed.glob("*.attempt.json"))) == 1
    assert not list(failed.glob("*.sealed.json"))


def test_baseline_pin_checked_before_fit(sample, tmp_path, monkeypatch):
    features, _ = sample
    predictions = [dict(trade_date=d, sector_code=c) for d in ("2026-04-01", "2026-07-01") for c in features["catalog"]]
    path = tmp_path / "baseline.json"
    write_once(path, receipt(dict(predictions=predictions)))
    pin = hashlib.sha256(path.read_bytes()).hexdigest()
    for arm in ("M0", "M1"):
        monkeypatch.setitem(h.PREDICTION_PINS, arm, pin)
    assert len(h.load_baselines(dict(M0=path, M1=path), features)["M0"]) == 262
    monkeypatch.setitem(h.PREDICTION_PINS, "M1", "f" * 64)
    with pytest.raises(FormalStateError):
        h.load_baselines(dict(M0=path, M1=path), features)


def test_evaluation_daily_weighting_undefined_and_unmature(sample, monkeypatch):
    features, labels = sample
    sealed = [h.fit_vintage(features, labels[v["id"]], v, "a" * 40, lambda: None) for v in h.rolling.VINTAGES]
    predictions = [r for s in sealed for r in s["predictions"]]
    baselines = {
        a: {
            (r["trade_date"], r["sector_code"]): dict(availability="available", probability=0.3, reason_code=None)
            for r in predictions
        }
        for a in ("M0", "M1")
    }
    facts = receipt(
        dict(
            feature_sha256=features["receipt_sha256"],
            schema_version=h.rolling.VERSION + "_outcomes",
            returns={d: {c: 0.0 for c in features["catalog"]} for d in features["calendar"]},
        )
    )
    result = h.evaluate(features, sealed, facts, baselines)
    assert result["prediction_rows"] == 262 and result["NA_reasons"]["OUTCOME_NOT_MATURE"] == 131
    assert result["segments"]["full"]["rank_defined_days"] == 0
    for s in sealed:
        s["model"]["training_loss_mean"] = 0.0
    result = h.evaluate(features, sealed, facts, baselines)
    assert result["segments"]["full"]["mse_skill"] is None
    assert result["research_disposition"] == "STOP_NO_PREDICTIVE_LOSS_INCREMENT"


def test_guard_forbids_db_network_and_nonapproved_fit():
    import socket
    from sklearn.linear_model import LogisticRegression, Ridge

    with cli.guard(allow_ridge=False):
        for call in (
            lambda: socket.create_connection(("localhost", 1)),
            lambda: Ridge().fit([[1]], [1]),
            lambda: LogisticRegression().fit([[1]], [1]),
        ):
            with pytest.raises(FormalStateError):
                call()


@pytest.mark.parametrize("mode", ["frozen_file", "database_daily"])
def test_dual_entry_validates_carrier_and_never_uses_probability_kernel(sample, tmp_path, monkeypatch, mode):
    from backend.services.hmm_risk import risk_l2_database_inference as db

    features, _ = sample
    model = fit(sample)["model"]
    bundle = receipt(
        dict(
            catalog=features["catalog"],
            model_identity=features["model_identity"],
            rows={"2026-04-01": features["rows"]["2026-04-01"]},
            as_of=features["as_of"],
            source_commit="a" * 40,
            inference_identity=dict(calendar=features["calendar"]),
        )
    )
    request = dict(decision_dates=["2026-04-01"], source=dict(candidate_root=str(tmp_path), calendar_sha256="b" * 64))
    calendar_file = tmp_path / "calendar.json"
    # Use a real text calendar through pytest's fixture API, never business I/O.
    calendar_file.write_text("\n".join(features["calendar"]), encoding="utf-8")
    checked = []

    def validate(request, value, pin):
        h.core.verify_receipt(value, pin)
        checked.append(mode)

    consumer = h.core if mode == "frozen_file" else db
    monkeypatch.setattr(consumer, "validate_bundle", validate)
    monkeypatch.setattr(h.core, "date_plan", lambda _: None)
    monkeypatch.setattr(h.core.inputs, "_stamp", lambda _: "fixture")
    monkeypatch.setattr(h.core.inputs.reader, "_require_file", lambda *_: calendar_file)

    def no_probability(*args, **kwargs):
        pytest.fail("loss entry must never call probability kernel")

    monkeypatch.setattr(h.rolling.risk, "predictions_from_parameters", no_probability)
    with cli.guard(allow_ridge=False):
        result = h.infer_prepared(
            model,
            request,
            bundle,
            source_mode=mode,
            expected_model_sha256=model["receipt_sha256"],
            expected_bundle_sha256=bundle["receipt_sha256"],
        )
    assert checked == [mode] and len(result["predictions"]) == 131 and result["fits"] == 0
    with pytest.raises(FormalStateError):
        h.infer_prepared(
            model,
            request,
            bundle,
            source_mode=mode,
            expected_model_sha256="e" * 64,
            expected_bundle_sha256=bundle["receipt_sha256"],
        )


def test_cli_does_not_write_failure_into_unvalidated_dataset(tmp_path, monkeypatch):
    import sys

    monkeypatch.setattr(cli, "clean_head", lambda: "a" * 40)
    monkeypatch.setattr(h, "load_study", lambda *_: (_ for _ in ()).throw(FormalStateError("blocked", "input differs")))
    output = tmp_path / "must_not_exist"
    monkeypatch.setattr(cli, "validate_output_location", lambda p, **_: p)
    monkeypatch.setattr(
        sys, "argv", ["loss", "run", "--study", "unit", "--m0", "unit", "--m1", "unit", "--output", str(output)]
    )
    assert cli.main() == 1
    assert not output.exists() and not output.with_name(output.name + ".failure.json").exists()
