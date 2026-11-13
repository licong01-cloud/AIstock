"""Release separation, causal quarter boundaries and fixed-budget closure."""

from copy import deepcopy
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_member_release as h
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt


def reseal(value):
    return receipt({k: v for k, v in value.items() if k != "receipt_sha256"})


def test_quarter_end_does_not_require_future_calendar_or_truncate_request():
    calendar = pd.bdate_range("2018-08-01", "2026-10-30").strftime("%Y-%m-%d").tolist()
    plan = h.schedule(calendar, "2026-10-30")
    assert plan["predict"][-1] == "2026-10-30" and h.Q4["effective_end"] == "2026-12-31"
    positions = {d: i for i, d in enumerate(calendar)}
    assert all(positions[d] + 10 <= positions["2026-09-30"] for d in plan["fit"])
    assert plan["as_of"][plan["predict"][0]] == "2026-09-30"
    with pytest.raises(FormalStateError):
        h.schedule(calendar, "2026-11-02")
    with pytest.raises(FormalStateError):
        h.schedule(calendar, "2026-09-30")


def test_authority_boundary_is_not_extended_or_called_full_preflight(monkeypatch, tmp_path):
    from backend.services.hmm_risk.formal_state_executor import write_once
    import hashlib

    root = tmp_path / "release"
    manifest = root / "qe_dataset_manifest.json"
    write_once(manifest, {"dataset_manifest_sha256": "a" * 64})
    calendar = root / "components/daily_bin_candidate/calendars/day.txt"
    calendar.parent.mkdir(parents=True)
    calendar.write_text("2026-09-30\n2026-10-09\n", encoding="utf-8")
    request = {
        "decision_dates": ["2026-04-01"],
        "model_assets": dict.fromkeys(h.core.MODEL_FIELDS, "locator"),
        "source": {
            "candidate_root": str(root),
            "calendar_sha256": hashlib.sha256(calendar.read_bytes()).hexdigest(),
            "industry_authority": {},
            "release_binding": {
                "manifest_sha256": "a" * 64,
                "manifest_file_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
            },
        },
    }
    monkeypatch.setattr(h.core, "validate_request", lambda r: None)
    monkeypatch.setattr(h.core, "model_source", lambda r: None)
    monkeypatch.setattr(h.core, "date_plan", lambda r: {"as_of": {"a": "2026-09-30", "b": "2026-10-09"}})
    monkeypatch.setattr(
        h.core.reader,
        "_industry_adapter",
        lambda *a, **k: SimpleNamespace(
            authority_bundle=SimpleNamespace(
                preflight_report={"window_start": "2018-08-01", "window_end": "2026-09-30"}
            )
        ),
    )
    result = h.source_preflight(request)
    assert result["status"] == "BLOCKED" and result["first_unsupported_as_of"] == "2026-10-09"
    assert result["full_file_preflight"] is False and result["fits"] == 0
    request["source"]["release_binding"]["manifest_file_sha256"] = "0" * 64
    with pytest.raises(FormalStateError):
        h.source_preflight(request)


@pytest.fixture
def carrier(monkeypatch):
    calendar = pd.bdate_range("2026-03-30", "2026-10-30").strftime("%Y-%m-%d").tolist()
    positions = {d: i for i, d in enumerate(calendar)}
    prediction = [d for d in calendar if d >= "2026-04-01"]
    train = ["2026-09-14", "2026-09-15", "2026-09-30"]
    days = sorted(set(train + prediction))
    catalog = [f"801{i:03d}.SI" for i in range(131)]
    as_of = {d: calendar[positions[d] - 1] for d in days}
    binding = dict(manifest_sha256="a" * 64, cutoff="2026-10-30")
    source = dict(
        release_binding=binding,
        calendar_sha256="b" * 64,
        industry_authority={},
        security_identity_sha256="c" * 64,
        provider_absence_sha256="d" * 64,
    )
    parameters = {"original_bootstrap": True}
    monkeypatch.setitem(h.core.value.APPROVED_PINS, "model_hash", canonical_sha256(parameters))
    features = receipt(
        dict(
            schema_version=h.VERSION + "_features",
            request_sha256="e" * 64,
            source_commit="f" * 40,
            contract=h.CONTRACT,
            calendar=calendar,
            source=source,
            prediction_dates=prediction,
            model_identity={"feature_definition_sha256": "1" * 64},
            catalog=catalog,
            feature_names=list(h.ALL_CORE_FEATURES),
            parameters=parameters,
            as_of=as_of,
            rows={
                d: {
                    c: dict(as_of_date=as_of[d], features=[float(j), float(i)] + [1.0] * 18, reason_code=None)
                    for i, c in enumerate(catalog)
                }
                for j, d in enumerate(days)
            },
            inference_identity=dict(
                release_identity={
                    "frozen_release_binding_sha256": canonical_sha256(binding),
                    "dataset_manifest_sha256": "a" * 64,
                },
                calendar_sha256="b" * 64,
                industry_authority_sha256=canonical_sha256({}),
                security_identity_sha256="c" * 64,
                provider_absence_sha256="d" * 64,
            ),
        )
    )
    plan = dict(train=train, fit=train[:2], predict=[d for d in prediction if d >= "2026-10-01"], as_of=as_of)
    monkeypatch.setattr(h, "schedule", lambda *a: plan)
    labels = {d: {c: dict(status="AVAILABLE", B=0.01, C=0.03, reason=None) for c in catalog} for d in train}
    labels[train[-1]] = {c: dict(status="OUTCOME_NOT_MATURE", B=None, C=None) for c in catalog}
    labels[train[0]][catalog[0]] = dict(status="UNAVAILABLE", B=None, C=None, reason="FIXED_COHORT_ENDPOINT_NA")
    original = {d: dict.fromkeys(catalog, "AVAILABLE" if d != train[-1] else "OUTCOME_NOT_MATURE") for d in train}
    original[train[0]][catalog[1]] = "OUTCOME_LEGAL_NA"
    training = receipt(
        dict(
            schema_version=h.VERSION + "_train",
            contract=h.CONTRACT,
            feature_sha256=features["receipt_sha256"],
            labels=labels,
            original_status=original,
        )
    )
    return features, training, plan


def test_common_mask_preserves_legal_na_and_rejects_future_or_unknown(carrier):
    features, training, plan = carrier
    rows = h.training_rows(features, training)
    assert len(rows) == 260 and {r[0] for r in rows} == set(plan["fit"])
    for mutate in ("future", "unknown", "missing", "nonfinite"):
        bad = deepcopy(training)
        day, code = plan["train"][0], features["catalog"][2]
        if mutate == "future":
            bad["labels"][plan["train"][-1]][code]["B"] = 0.0
        elif mutate == "unknown":
            bad["original_status"][day][code] = "SILENT_MISSING"
        elif mutate == "missing":
            del bad["labels"][day][code]
        else:
            bad["labels"][day][code]["C"] = True
        with pytest.raises(FormalStateError):
            h.training_rows(features, reseal(bad))


def test_features_cannot_contain_outcomes_or_rebound_manifest(carrier):
    features, _, _ = carrier
    for modify in ("outcomes", "manifest", "grid", "as_of"):
        bad = deepcopy(features)
        day, code = next(iter(bad["rows"])), bad["catalog"][0]
        if modify == "outcomes":
            bad["outcomes"] = {}
        elif modify == "manifest":
            bad["source"]["release_binding"]["manifest_sha256"] = "0" * 64
        elif modify == "grid":
            del bad["rows"][day][code]
        else:
            bad["rows"][day][code]["as_of_date"] = day
        with pytest.raises(FormalStateError):
            h.validate_features(reseal(bad))


def run_child(carrier, monkeypatch, tmp_path):
    features, training, _ = carrier
    rows = h.training_rows(features, training)
    prepared = receipt(
        dict(
            common_mask_sha256=canonical_sha256([[r[0], r[1]] for r in rows]),
            training_sha256=training["receipt_sha256"],
            release_binding=features["source"]["release_binding"],
            member_source_identity=dict(
                release_identity={"dataset_manifest_sha256": "a" * 64}, target_contract=h.member.TARGET
            ),
        )
    )
    monkeypatch.setattr(h, "load_prepared", lambda *a: (prepared, features, training, rows))
    # Real numeric fit in the unit fixture, not a formal research attempt.
    result = h.execute_child(tmp_path / "prepared.json", prepared["receipt_sha256"], tmp_path / "child", "f" * 40)
    result = reseal({**result, "numeric_environment": {"versions": h.member.loss.EXPECTED_VERSIONS}})
    return result, prepared, features


def test_pair_budget_closure_recomputes_values_and_full_request(carrier, monkeypatch, tmp_path):
    first, prepared, features = run_child(carrier, monkeypatch, tmp_path)
    result = h.close_processes(first, deepcopy(first), "f" * 40, prepared, features)
    assert result["completed_fits"] == 4
    assert {p.name for p in (tmp_path / "child").glob("*.attempt.json")} == {"B.attempt.json", "C.attempt.json"}
    for mutate in ("window", "value", "mask", "budget", "scaler"):
        bad = deepcopy(first)
        if mutate == "window":
            bad["prediction_dates"].pop()
        elif mutate == "value":
            bad["sealed"]["C"]["predictions"][0]["expected_member_loss"] = 0.97
        elif mutate == "mask":
            bad["sealed"]["C"]["model"]["training_mask_sha256"] = "0" * 64
        elif mutate == "budget":
            bad["fits"] = 3
        else:
            model = bad["sealed"]["C"]["model"]
            model["parameters"]["mean"][0] += 1
            model["model_sha256"] = canonical_sha256(model["parameters"])
        for arm in h.member.ARMS:
            bad["sealed"][arm]["model"] = reseal(bad["sealed"][arm]["model"])
            bad["sealed"][arm] = reseal(bad["sealed"][arm])
        bad = reseal(bad)
        with pytest.raises(FormalStateError):
            h.close_processes(bad, deepcopy(bad), "f" * 40, prepared, features)


def test_failed_fit_attempt_is_persisted_and_not_retried(carrier, monkeypatch, tmp_path):
    features, training, _ = carrier
    rows = h.training_rows(features, training)
    monkeypatch.setattr(h, "load_prepared", lambda *a: ({}, features, training, rows))
    calls = []

    def failed(*a):
        calls.append("fit")
        raise RuntimeError("numeric failure")

    monkeypatch.setattr(h.member.loss, "fit_parameters", failed)
    with pytest.raises(RuntimeError, match="numeric failure"):
        h.execute_child(tmp_path / "prepared.json", "a" * 64, tmp_path / "child", "f" * 40)
    assert calls == ["fit"]
    assert read_json(tmp_path / "child/B.attempt.json")["fit_kernel_attempts"] == 1
    assert not (tmp_path / "child/C.attempt.json").exists()
    with pytest.raises(FileExistsError):
        h.execute_child(tmp_path / "prepared.json", "a" * 64, tmp_path / "child", "f" * 40)
    assert calls == ["fit"]


def test_original_models_keep_v1_validator_without_mutation(monkeypatch):
    model = {"schema_version": h.member.ARMS["C"][0] + "_model", "release_binding": {"manifest_sha256": "old"}}
    before = deepcopy(model)
    seen = []
    monkeypatch.setattr(h.member, "infer_prepared", lambda m, *a, **k: seen.append(deepcopy(m)) or "old-path")
    assert h.reinfer(model, {}, {}, model_sha256="a" * 64, bundle_sha256="b" * 64) == "old-path"
    assert seen == [before] and model == before


def test_shared_kernel_preserves_na_and_asof_without_requiring_mature_labels(carrier, monkeypatch, tmp_path):
    result, _, features = run_child(carrier, monkeypatch, tmp_path)
    model = result["sealed"]["C"]["model"]
    days = result["prediction_dates"]
    rows = {d: deepcopy(features["rows"][d]) for d in days}
    rows[days[-1]][features["catalog"][0]].update(features=None, reason_code="hmm_risk_legal_empty")
    out, _ = h.member.predict_parameters(model, rows, days, features["as_of"], features["calendar"])
    assert len(out) == len(days) * 131
    assert out[-131]["expected_member_loss"] is None and out[-131]["reason_code"] == "hmm_risk_legal_empty"
    assert out[-1]["target_last_return_date"] is None
    with pytest.raises(FormalStateError):
        h.member.predict_parameters(model, rows, days, dict.fromkeys(days, days[0]), features["calendar"])
