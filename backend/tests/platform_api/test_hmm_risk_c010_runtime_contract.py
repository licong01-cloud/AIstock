from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import health as subject
from backend.services.hmm_risk import stock_fact_observation


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(subject.router, prefix="/api/v1")
    return TestClient(app)


def test_hmm_risk_c010_a5_runtime_contract_loads_exact_v2_authority(monkeypatch) -> None:
    monkeypatch.setattr(subject, "_PROCESS_RUNTIME_IDENTITY", {"status": "ready", "merge_commit": "a" * 40})

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


def test_hmm_risk_c010_a5_runtime_contract_fails_closed_on_loaded_constant_drift(monkeypatch) -> None:
    monkeypatch.setattr(subject, "_PROCESS_RUNTIME_IDENTITY", {"status": "ready", "merge_commit": "a" * 40})
    monkeypatch.setattr(stock_fact_observation, "C010_POLICY_VERSION", "drifted-policy")

    response = _client().get("/api/v1/runtime-contracts/hmm-risk-c010-a5")

    assert response.status_code == 503
    assert response.json()["detail"]["reason_code"] == "HMM_RISK_C010_A5_RUNTIME_CONTRACT_DRIFT"
