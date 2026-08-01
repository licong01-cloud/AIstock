from __future__ import annotations

import hashlib
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import health as subject


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(subject.router, prefix="/api/v1")
    return TestClient(app)


def _identity() -> dict:
    return {
        "status": "ready",
        "merge_commit": "a" * 40,
        "source_sha256": {
            "backend/services/hmm_risk/observation_eligibility.py": "b" * 64,
            "backend/services/hmm_risk/stock_fact_observation.py": "c" * 64,
        },
    }


def test_capture_runtime_identity_binds_git_head_and_exact_source_bytes(monkeypatch, tmp_path) -> None:
    first = tmp_path / "first.py"
    second = tmp_path / "second.py"
    first.write_bytes(b"first-source\n")
    second.write_bytes(b"second-source\n")
    monkeypatch.setattr(subject, "_RUNTIME_SOURCE_FILES", ("first.py", "second.py"))

    def fake_run(command, **kwargs):
        del kwargs
        return SimpleNamespace(stdout=("d" * 40 + "\n") if command[1:3] == ["rev-parse", "HEAD"] else "")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    identity = subject._capture_runtime_identity(tmp_path)

    assert identity == {
        "status": "ready",
        "merge_commit": "d" * 40,
        "source_sha256": {
            "first.py": hashlib.sha256(b"first-source\n").hexdigest(),
            "second.py": hashlib.sha256(b"second-source\n").hexdigest(),
        },
    }


def test_capture_runtime_identity_rejects_dirty_tracked_checkout(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(subject, "_RUNTIME_SOURCE_FILES", ())

    def fake_run(command, **kwargs):
        del kwargs
        return SimpleNamespace(
            stdout=("d" * 40 + "\n") if command[1:3] == ["rev-parse", "HEAD"] else " M backend/main.py\n"
        )

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    identity = subject._capture_runtime_identity(tmp_path)

    assert identity["status"] == "unavailable"
    assert identity["reason_code"] == "AISTOCK_RUNTIME_IDENTITY_UNAVAILABLE"
    assert "dirty" in identity["message"]


def test_runtime_identity_returns_process_frozen_merge_and_source_hashes(monkeypatch) -> None:
    monkeypatch.setattr(subject, "_PROCESS_RUNTIME_IDENTITY", _identity())

    response = _client().get("/api/v1/runtime-identity")

    assert response.status_code == 200
    assert response.json() == _identity()


def test_runtime_identity_fails_closed_when_startup_capture_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(
        subject,
        "_PROCESS_RUNTIME_IDENTITY",
        {
            "status": "unavailable",
            "reason_code": "AISTOCK_RUNTIME_IDENTITY_UNAVAILABLE",
            "message": "git identity unavailable",
        },
    )

    response = _client().get("/api/v1/runtime-identity")

    assert response.status_code == 503
    assert response.json()["detail"]["reason_code"] == "AISTOCK_RUNTIME_IDENTITY_UNAVAILABLE"


def test_hmm_risk_c010_a5_runtime_contract_loads_exact_v2_authority(monkeypatch) -> None:
    monkeypatch.setattr(subject, "_PROCESS_RUNTIME_IDENTITY", _identity())

    response = _client().get("/api/v1/runtime-contracts/hmm-risk-c010-a5")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "contract": "hmm_risk_c010_a5_runtime_contract_v1",
        "merge_commit": "a" * 40,
        "policy_version": "hmm_risk_c010_feature_domain_policy_v2",
        "eligibility_receipt_version": "hmm_risk_c010_train_observation_eligibility_v2",
        "expected_opportunity_contract": "hmm_risk_c010_expected_opportunity_dates_v2",
        "provider_absence_partition_version": "hmm_risk_c010_provider_absence_domain_partition_v1",
    }
