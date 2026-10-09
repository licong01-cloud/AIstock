"""Pinned file input contracts; no numerical new-window source read."""

import copy
import hashlib
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.hmm_risk import frozen_l2_history as h
from backend.services.hmm_risk import frozen_l2_history_input as source
from backend.services.hmm_risk.formal_state_model import receipt
from backend.tests.hmm_risk.test_rotation_l2_moneyflow_supervised import _calendar


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


@pytest.fixture
def risk_outcome_case(monkeypatch):
    closed = {"2026-04-06", "2026-05-01", "2026-05-04", "2026-05-05", "2026-06-19"}
    calendar = [d.isoformat() for d in _calendar()] + [
        d.date().isoformat() for d in pd.bdate_range("2026-04-01", "2026-08-31") if d.date().isoformat() not in closed
    ]
    catalog = [f"801{i:03d}.SI" for i in range(131)]
    identity = {"release_identity": {"manifest": "fixed-original-release"}}
    original = {"frozen": {"catalog": catalog}, "source": {"candidate_root": "explicit-frozen-root"}}
    monkeypatch.setattr(source, "_risk_source", lambda _: (original, {"input_identity": identity}, {}, calendar))
    request = {"kind": "P2", "receipt_sha256": "a" * 64}
    bundle = {
        "request_sha256": request["receipt_sha256"],
        "receipt_sha256": "b" * 64,
        "catalog": catalog,
        "calendar": calendar,
        "source_identity": identity,
    }
    calls = []

    def reader(frozen, binding, *, start, end):
        assert frozen == original["frozen"] and binding == original["source"]
        calls.append((start, end))
        window = tuple(date.fromisoformat(d) for d in calendar if start.isoformat() <= d <= end.isoformat())
        # The production stock-fact kernel needs ten strictly prior closes.
        # A cold slice has no complete aggregate during its first ten days.
        aggregates = [SimpleNamespace(trade_date=d, l1_code=c, l1_return=0.01) for d in window[10:] for c in catalog]
        return {}, window, aggregates, identity

    monkeypatch.setattr(source, "_bounded_l2_stock_facts", reader)
    return request, bundle, calls, reader


def test_risk_outcomes_use_real_ten_session_context_without_shortening_window(risk_outcome_case):
    request, bundle, calls, _ = risk_outcome_case
    result = source.outcomes(request, bundle)
    first = bundle["calendar"].index(h.START.isoformat())
    assert calls == [(date.fromisoformat(bundle["calendar"][first - 10]), h.END)]
    assert list(result["event_returns"]) == h.schedule(bundle["calendar"])["days"]
    assert len(result["returns"]) == 103
    assert all(
        set(rows) == set(bundle["catalog"]) and set(rows.values()) == {0.01}
        for rows in result["event_returns"].values()
    )


def test_risk_outcome_warmup_does_not_fill_genuine_missing_held_return(risk_outcome_case, monkeypatch):
    request, bundle, _, reader = risk_outcome_case
    missing = (date(2026, 4, 2), bundle["catalog"][0])

    def with_legal_na(*args, **kwargs):
        assets, window, aggregates, identity = reader(*args, **kwargs)
        aggregates = [a for a in aggregates if (a.trade_date, a.l1_code) != missing]
        return assets, window, aggregates, identity

    monkeypatch.setattr(source, "_bounded_l2_stock_facts", with_legal_na)
    result = source.outcomes(request, bundle)
    assert result["returns"][missing[0].isoformat()][missing[1]] is None
    assert sum(v is None for rows in result["returns"].values() for v in rows.values()) == 1


def test_risk_outcome_calendar_drift_fails_before_read(risk_outcome_case, monkeypatch):
    request, bundle, _, _ = risk_outcome_case
    bundle["calendar"] = bundle["calendar"][1:]
    monkeypatch.setattr(source, "_bounded_l2_stock_facts", lambda *args, **kwargs: pytest.fail("must stop before read"))
    with pytest.raises(h.risk.FormalStateError, match="risk outcome calendar changed"):
        source.outcomes(request, bundle)


def test_risk_outcome_warmup_does_not_accept_release_identity_drift(risk_outcome_case, monkeypatch):
    request, bundle, _, reader = risk_outcome_case

    def changed_release(*args, **kwargs):
        assets, window, aggregates, _ = reader(*args, **kwargs)
        return assets, window, aggregates, {"release_identity": {"manifest": "different-release"}}

    monkeypatch.setattr(source, "_bounded_l2_stock_facts", changed_release)
    with pytest.raises(h.risk.FormalStateError, match="risk outcome release changed"):
        source.outcomes(request, bundle)
