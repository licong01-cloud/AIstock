"""Quarterly contracts: mature labels, immutable versions and bounded execution."""

from copy import deepcopy

import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_rolling as h
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json, write_once
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt
from scripts.hmm_risk import run_risk_l2_rolling as cli


def reseal(body):
    return receipt({k: v for k, v in body.items() if k != "receipt_sha256"})


def test_real_calendar_structure_purges_ten_days_and_keeps_quarters_disjoint():
    calendar = pd.bdate_range("2021-01-01", "2026-09-30").strftime("%Y-%m-%d").tolist()
    for vintage in h.VINTAGES:
        plan = h.schedule(calendar, vintage)
        assert len(plan["train"]) - len(plan["fit"]) == 10
        assert plan["train"][-1] < plan["predict"][0]
        assert calendar[calendar.index(plan["fit"][-1]) + 10] == vintage["train_end"]
        assert all(plan["as_of"][d] < d for d in plan["train"] + plan["predict"])
    with pytest.raises(FormalStateError):
        h.schedule(calendar, {**h.VINTAGES[0], "train_end": "2026-04-01"})
    with pytest.raises(FormalStateError):
        h.schedule(calendar[700:], h.VINTAGES[0])


@pytest.fixture
def sample(monkeypatch):
    codes = [f"801{i:03d}.SI" for i in range(131)]
    plans = {
        "2026Q2": dict(train=["2026-03-16", "2026-03-31"], fit=["2026-03-16"], predict=["2026-04-01"]),
        "2026Q3": dict(train=["2026-06-15", "2026-06-30"], fit=["2026-06-15"], predict=["2026-07-01"]),
    }
    monkeypatch.setattr(h, "schedule", lambda _calendar, vintage: plans[vintage["id"]])
    calendar = pd.bdate_range("2026-03-13", "2026-07-20").strftime("%Y-%m-%d").tolist()
    days = sorted({d for p in plans.values() for d in p["train"] + p["predict"]})
    as_of = {d: calendar[calendar.index(d) - 1] for d in days}
    parameters, _ = h.risk.fit_parameters(
        [("unit", c, [float(i % 7)] + [0.0] * 19, int(i % 7 >= 3)) for i, c in enumerate(codes)]
    )
    monkeypatch.setitem(h.core.value.APPROVED_PINS, "model_hash", canonical_sha256(parameters))
    binding = dict(manifest_sha256="b" * 64, cutoff="2026-09-30")
    authority = {"identity": {"bundle_hash": "c" * 64}}
    source = dict(
        release_binding=binding,
        industry_authority=authority,
        calendar_sha256="d" * 64,
        security_identity_sha256="e" * 64,
        provider_absence_sha256="f" * 64,
    )
    inference = dict(
        release_identity=dict(
            frozen_release_binding_sha256=canonical_sha256(binding), dataset_manifest_sha256="b" * 64
        ),
        industry_authority_sha256=canonical_sha256(authority),
        **{k: source[k] for k in ("calendar_sha256", "security_identity_sha256", "provider_absence_sha256")},
    )
    features = receipt(
        dict(
            schema_version=h.VERSION + "_features",
            contract=h.CONTRACT,
            calendar=calendar,
            catalog=codes,
            feature_names=list(h.ALL_CORE_FEATURES),
            parameters=parameters,
            source=source,
            source_commit="a" * 40,
            inference_identity=inference,
            model_identity=dict(
                model_sha256=canonical_sha256(parameters), feature_definition_sha256=canonical_sha256({"unit": True})
            ),
            as_of=as_of,
            rows={
                d: {
                    c: dict(as_of_date=as_of[d], features=[float(i % 7)] + [0.0] * 19, reason_code=None)
                    for i, c in enumerate(codes)
                }
                for d in days
            },
        )
    )
    training = {}
    for vintage in h.VINTAGES:
        plan = plans[vintage["id"]]
        labels = {
            d: {
                c: dict(
                    status="AVAILABLE", event=int(i % 7 >= 3), drawdown=-0.1 if i % 7 >= 3 else 0.0, return_value=0.0
                )
                for i, c in enumerate(codes)
            }
            for d in plan["fit"]
        }
        for rows in labels.values():
            for row in rows.values():
                row["return"] = row.pop("return_value")
        labels[plan["train"][-1]] = {
            c: dict(status="OUTCOME_NOT_MATURE", event=None, drawdown=None, **{"return": None}) for c in codes
        }
        training[vintage["id"]] = receipt(
            dict(
                schema_version=h.VERSION + "_train_labels",
                vintage=vintage,
                feature_sha256=features["receipt_sha256"],
                target=h.TARGET,
                labels=labels,
            )
        )
    return features, training


def test_future_features_cannot_fit_scaler_and_immature_labels_cannot_enter_fit(sample):
    features, training = sample
    h.validate_features(features)
    first = h.fit_vintage(features, training["2026Q2"], h.VINTAGES[0])
    changed = deepcopy(features)
    changed["rows"]["2026-07-01"][features["catalog"][0]]["features"][0] = 100000.0
    changed = reseal(changed)
    target = reseal({**training["2026Q2"], "feature_sha256": changed["receipt_sha256"]})
    second = h.fit_vintage(changed, target, h.VINTAGES[0])
    assert first["model"]["parameters"] == second["model"]["parameters"]
    assert first["predictions"] == second["predictions"]
    assert first["model"]["fit_rows"] == 131 and first["model"]["fit_dates"] == 1
    broken = deepcopy(training["2026Q2"])
    broken["labels"]["2026-03-31"][features["catalog"][0]] = broken["labels"]["2026-03-16"][features["catalog"][0]]
    with pytest.raises(FormalStateError):
        h.fit_vintage(features, reseal(broken), h.VINTAGES[0])


def test_model_pin_schema_training_identity_and_effective_window(sample, tmp_path):
    features, training = sample
    bundle = h.fit_vintage(features, training["2026Q2"], h.VINTAGES[0])["model"]
    path = tmp_path / "model.json"
    write_once(path, bundle)
    assets = dict.fromkeys(h.BASE_MODEL_FIELDS, str(tmp_path / "bootstrap.json"))
    assets.update(model_bundle_path=str(path), model_bundle_sha256=bundle["receipt_sha256"])
    frozen = dict(catalog=features["catalog"], feature_definition={"unit": True})
    bootstrap = {**features["model_identity"], "training_pit_bundle_sha256": "0" * 64, "sealed_sha256": "1" * 64}
    parameters, identity = h.load_model(assets, frozen, bootstrap)
    assert parameters == bundle["parameters"]
    assert identity["training_pit_bundle_sha256"] == "c" * 64
    assert identity["bootstrap_identity"] == bootstrap and "sealed_sha256" not in identity
    h.validate_model_dates({"decision_dates": ["2026-04-01", "2026-06-30"]}, identity)
    for day in ("2026-03-31", "2026-07-01"):
        with pytest.raises(FormalStateError):
            h.validate_model_dates({"decision_dates": [day]}, identity)
    with pytest.raises(FormalStateError):
        h.load_model({**assets, "model_bundle_sha256": "f" * 64}, frozen, bootstrap)
    altered = reseal({**bundle, "schema_version": "unknown"})
    path2 = tmp_path / "unknown.json"
    write_once(path2, altered)
    with pytest.raises(FormalStateError):
        h.load_model(
            {**assets, "model_bundle_path": str(path2), "model_bundle_sha256": altered["receipt_sha256"]},
            frozen,
            bootstrap,
        )


def test_child_seals_each_vintage_before_later_training_and_evaluation(sample, tmp_path, monkeypatch):
    features, training = sample
    write_once(tmp_path / "features.json", features)
    labels = {}
    for key, body in training.items():
        write_once(tmp_path / f"{key}.json", body)
        labels[key] = dict(path=str(tmp_path / f"{key}.json"), sha256=body["receipt_sha256"])
    facts = receipt({"unit_outcomes": True})
    write_once(tmp_path / "facts.json", facts)
    prepared = receipt(
        dict(
            schema_version=h.VERSION + "_prepared",
            contract=h.CONTRACT,
            source_commit="a" * 40,
            feature_path=str(tmp_path / "features.json"),
            feature_sha256=features["receipt_sha256"],
            training_labels=labels,
            outcome_path=str(tmp_path / "facts.json"),
            outcome_sha256=facts["receipt_sha256"],
        )
    )
    output = tmp_path / "result"
    original = h.core.inputs._asset

    def load(path, pin):
        if path == labels["2026Q3"]["path"]:
            assert read_json(output / "2026Q2.sealed.json")["fits"] == 1
        if path == prepared["outcome_path"]:
            assert read_json(output / "2026Q3.sealed.json")["fits"] == 1
        return original(path, pin)

    monkeypatch.setattr(h.core.inputs, "_asset", load)
    monkeypatch.setattr(h, "evaluate", lambda *_: {"unit_evaluation": True})
    result = h.execute_child(prepared, output, "a" * 40)
    assert result["fits"] == 2 and len(result["model_bundles"]) == 2
    full = reseal({**result, "numeric_environment": {"unit": True}})
    accepted = cli.close_processes(full, deepcopy(full), prepared, "a" * 40)
    assert accepted["completed_fits"] == 4 and accepted["automatic_promotion"] is False
    with pytest.raises(FormalStateError):
        cli.close_processes(full, reseal({**full, "fits": 1}), prepared, "a" * 40)
    with pytest.raises(FormalStateError):
        cli.close_processes(full, reseal({**full, "numeric_environment": {"drift": True}}), prepared, "a" * 40)


def test_file_only_poison_and_empty_or_one_class_fail_closed():
    import psycopg2

    with cli.file_only(allow_fit=True):
        with pytest.raises(FormalStateError, match="forbidden"):
            psycopg2.connect("unused")
    with cli.file_only(allow_fit=False):
        with pytest.raises(FormalStateError, match="forbidden"):
            h.risk.LogisticRegression().fit([[0], [1]], [0, 1])
    for rows in ([], [("d", "c", [0.0] * 20, 0)]):
        with pytest.raises(FormalStateError):
            h.risk.fit_parameters(rows)


def test_paired_evaluation_preserves_unmatured_dates_and_explicit_model_anchor(sample):
    features, training = sample
    models = [h.fit_vintage(features, training[v["id"]], v) for v in h.VINTAGES]
    days = ["2026-04-01", "2026-07-01"]
    predictions, _ = h.risk.predictions_from_parameters(
        features["rows"], days, features["catalog"], features["parameters"]
    )
    baseline = receipt(
        dict(
            feature_sha256=features["receipt_sha256"],
            model_sha256=h.core.value.APPROVED_PINS["model_hash"],
            predictions=predictions,
            fits=0,
        )
    )
    returns = {
        d: {c: -0.01 if i % 2 else 0.01 for i, c in enumerate(features["catalog"])} for d in features["calendar"]
    }
    facts = receipt(
        dict(schema_version=h.VERSION + "_outcomes", feature_sha256=features["receipt_sha256"], returns=returns)
    )
    before = deepcopy(models)
    result = h.evaluate(features, models, facts, baseline)
    assert models == before
    assert len(result["M1_predictions"]) == 262
    assert result["segments"]["full"]["M1"]["labelled_rows"] == 131
    assert result["segments"]["2026Q3"]["M1"]["NA_reasons"] == {"OUTCOME_NOT_MATURE": 131}
    assert result["M1_predictions"][0]["target_first_return_date"] == "2026-04-02"
    assert result["M1_predictions"][-1]["vintage_id"] == "2026Q3"
    assert not result["automatic_promotion"] and not result["historical_as_known"]
