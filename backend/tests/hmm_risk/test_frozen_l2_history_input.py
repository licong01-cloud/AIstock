"""Pinned file input contracts; no numerical new-window source read."""

import copy
import hashlib
import json
from pathlib import Path

import pytest

from backend.services.hmm_risk import frozen_l2_history as h
from backend.services.hmm_risk import frozen_l2_history_input as source
from backend.services.hmm_risk.formal_state_model import receipt


def test_request_hash_drift_stops_before_source_access(tmp_path, monkeypatch):
    p = tmp_path / "request.json"
    p.write_text(
        json.dumps(receipt({"schema_version": h.VERSION + "_request", "contract": h.CONTRACT, "kind": "P1"})),
        encoding="utf8",
    )
    with pytest.raises(Exception, match="identity differs"):
        source._asset(p, "0" * 64)
    monkeypatch.setattr(source, "_rotation_source", lambda _: pytest.fail("must not enter source on wrong contract"))
    with pytest.raises(Exception, match="request contract"):
        source.prepare(receipt({"schema_version": "unknown", "contract": h.CONTRACT, "kind": "P1"}), "a" * 40)


def test_source_mutation_is_not_a_legal_data_na(tmp_path):
    path = tmp_path / "source.json"
    path.write_text("{}", encoding="utf8")
    stamps = {"source": source._stamp(path)}
    path.write_text('{"changed":true}', encoding="utf8")
    with pytest.raises(Exception, match="source changed"):
        source._stable_files({"source": path}, stamps)


@pytest.fixture
def risk_source_case(tmp_path, monkeypatch):
    risk = h.risk
    parameters = {"frozen": [1.0, 2.0]}
    identity = {"pit_bundle_sha256": risk.PIT_BUNDLE_SHA}
    features = receipt({"contract": risk.CONTRACT, "input_identity": identity})
    model_hash = h.canonical_sha256(parameters)
    sealed = receipt(
        {
            "contract": risk.CONTRACT,
            "feature_sha256": features["receipt_sha256"],
            "input_identity": identity,
            "parameters": parameters,
            "model_sha256": model_hash,
            "predictions": [],
        }
    )
    acceptance = receipt(
        {
            "model": {k: v for k, v in sealed.items() if k not in {"predictions", "receipt_sha256"}},
            "sealed_prediction_sha256": sealed["receipt_sha256"],
        }
    )
    source_path = tmp_path / "original_request.json"
    source_path.write_text("{}", encoding="utf8")
    monkeypatch.setattr(risk, "SOURCE_REQUEST_FILE_SHA", hashlib.sha256(source_path.read_bytes()).hexdigest())
    original = {
        "source": {"candidate_root": str(tmp_path)},
        "frozen": {"industry_authority": {"identity": {"bundle_hash": risk.PIT_BUNDLE_SHA}}},
    }
    monkeypatch.setattr(source, "load_effect_request", lambda _: copy.deepcopy(original))
    assets = {"sealed": sealed, "features": features, "acceptance": acceptance}
    pins = {name + "_hash": value["receipt_sha256"] for name, value in assets.items()}
    pins["model_hash"] = model_hash
    monkeypatch.setattr(h.risk_value, "APPROVED_PINS", pins)
    request = {"source_request_path": str(source_path), "work_parent": str(tmp_path / "work")}
    for name, value in assets.items():
        path = tmp_path / (name + ".json")
        path.write_text(json.dumps(value), encoding="utf8")
        request["risk_" + name + "_path"] = str(path)
    monkeypatch.setattr(source.reader, "_require_file", lambda *args: tmp_path / "calendar")
    monkeypatch.setattr(source.reader, "_parse_calendar", lambda _: [])
    monkeypatch.setattr(h, "schedule", lambda _: None)
    return request, assets, pins


def test_risk_source_reads_original_nested_acceptance_without_top_level_alias(risk_source_case):
    request, assets, _ = risk_source_case
    assert "model_sha256" not in assets["acceptance"]
    _, features, parameters, _ = source._risk_source(request)
    assert features == assets["features"]
    assert parameters == assets["sealed"]["parameters"]


@pytest.mark.parametrize("drift", ["absent_model", "wrong_model_type", "parameter_drift", "sealed_identity_drift"])
def test_risk_source_rejects_self_hashed_acceptance_drift_before_calendar(risk_source_case, monkeypatch, drift):
    request, assets, pins = risk_source_case
    acceptance = copy.deepcopy(assets["acceptance"])
    if drift == "absent_model":
        del acceptance["model"]
    elif drift == "wrong_model_type":
        acceptance["model"] = []
    elif drift == "parameter_drift":
        acceptance["model"]["parameters"] = {"frozen": [9.0, 2.0]}
    else:
        acceptance["sealed_prediction_sha256"] = "0" * 64
    # A top-level alias must never rescue a missing/drifted nested authority.
    acceptance["model_sha256"] = pins["model_hash"]
    acceptance = receipt({k: v for k, v in acceptance.items() if k != "receipt_sha256"})
    pins["acceptance_hash"] = acceptance["receipt_sha256"]
    Path(request["risk_acceptance_path"]).write_text(json.dumps(acceptance), encoding="utf8")
    monkeypatch.setattr(source.reader, "_require_file", lambda *args: pytest.fail("must fail before calendar/source"))
    with pytest.raises(h.risk.FormalStateError, match="risk frozen model linkage differs"):
        source._risk_source(request)
