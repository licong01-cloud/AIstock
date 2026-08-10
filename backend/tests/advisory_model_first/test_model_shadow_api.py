from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.routers import advisory as advisory_router


class _ShadowService:
    def model_shadow(self, *, program_id: str, target_trade_date):
        return {
            "status": "MODEL_UNAVAILABLE",
            "calibration_state": "UNCALIBRATED",
            "program_id": program_id,
            "binding_version_id": None,
            "package_id": None,
            "manifest_sha256": None,
            "decision_as_of_trade_date": None,
            "target_trade_date": target_trade_date.isoformat(),
            "selection_runtime_semantics_hash": None,
            "model_version": None,
            "bundle_id": None,
            "feature_schema_version": None,
            "candidate_count": 0,
            "shortlist_count": 0,
            "candidates": [],
            "baselines": {},
            "hmm_unavailable": [],
            "outcome": {
                "status": "OUTCOME_UNAVAILABLE",
                "calibration_state": "UNCALIBRATED",
                "outcome_bundle_id": None,
                "parent_bundle_id": None,
                "model_version": None,
                "horizons": [1, 3, 5, 10, 20],
                "candidates": [],
                "reason_code": "ADVISORY_OUTCOME_BUNDLE_NOT_AVAILABLE",
                "message": "parent model shadow is unavailable",
            },
            "price_range": {
                "status": "PRICE_RANGE_UNAVAILABLE",
                "calibration_state": "UNCALIBRATED",
                "price_range_bundle_id": None,
                "parent_bundle_id": None,
                "outcome_bundle_id": None,
                "model_version": None,
                "price_basis": "UNADJUSTED_CNY_DECISION_CLOSE",
                "candidates": [],
                "reason_code": "ADVISORY_PRICE_RANGE_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
                "message": "parent model shadow is unavailable",
            },
            "reason_code": "ADVISORY_MODEL_ROOT_NOT_CONFIGURED",
            "message": "model root is not configured",
        }


def test_model_shadow_endpoint_returns_typed_readonly_envelope() -> None:
    app.dependency_overrides[advisory_router.get_advisory_model_shadow_service] = lambda: _ShadowService()
    try:
        response = TestClient(app).get(
            "/api/v1/advisory/programs/program-1/model-shadow",
            params={"target_trade_date": "2026-07-21"},
        )
    finally:
        app.dependency_overrides.pop(advisory_router.get_advisory_model_shadow_service, None)
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["status"] == "MODEL_UNAVAILABLE"
    assert payload["reason_code"] == "ADVISORY_MODEL_ROOT_NOT_CONFIGURED"
    assert payload["target_trade_date"] == "2026-07-21"
    assert payload["outcome"]["status"] == "OUTCOME_UNAVAILABLE"
    assert payload["price_range"]["status"] == "PRICE_RANGE_UNAVAILABLE"
