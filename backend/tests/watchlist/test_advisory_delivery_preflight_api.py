from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import advisory as advisory_router
from backend.services.advisory_universe import AdvisoryUniverseContractError


class _PreflightService:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def preflight(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {
            "schema_version": "advisory_delivery_preflight_v1",
            "overall_status": "READY_BASELINE_ONLY",
            "package": {
                "package_id": kwargs["package_id"],
                "manifest_sha256": "a" * 64,
                "package_status": "SELECTION_ENABLED",
                "source_type": "qe_experiment",
                "source_id": "qe_test",
                "asset_eligible": True,
                "asset_blockers": [],
            },
            "universe_compatibility": {
                "status": "LEGACY_UNIVERSE_UNSPECIFIED",
                "requested": kwargs["universe_selection"],
                "source_declared": None,
                "evidence_paths": [],
                "evidence_errors": [],
            },
            "policy_compatibility": {
                "status": "NEW_POLICY_BINDING_REQUIRED",
                "requested_target_count": kwargs["target_count"],
                "active_target_count": None,
                "package_backtest_topk": None,
                "package_policy_authority": "DIAGNOSTIC_ONLY_NOT_ADVISORY_RUNTIME_AUTHORITY",
            },
            "model_compatibility": {
                "status": "REQUIRED_AFTER_BINDING",
                "validation_stage": "PUBLICATION_FULL_RESOLUTION",
                "binding_version_id": None,
            },
            "blockers": [],
            "warnings": ["PACKAGE_UNIVERSE_IDENTITY_UNSPECIFIED"],
        }


def _client() -> tuple[TestClient, _PreflightService]:
    app = FastAPI()
    app.include_router(advisory_router.router, prefix="/api/v1")
    service = _PreflightService()
    app.dependency_overrides[advisory_router.get_advisory_delivery_preflight_service] = lambda: service
    return TestClient(app), service


def test_delivery_preflight_api_returns_read_only_business_classification() -> None:
    client, service = _client()

    response = client.post(
        "/api/v1/advisory/delivery-preflight",
        json={
            "package_id": "pkg_test",
            "universe_selection": {"mode": "single_index", "pool_ids": ["csi300"]},
            "target_count": 20,
            "program_id": "advp_test",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["overall_status"] == "READY_BASELINE_ONLY"
    assert payload["universe_compatibility"]["status"] == "LEGACY_UNIVERSE_UNSPECIFIED"
    assert service.calls == [
        {
            "package_id": "pkg_test",
            "universe_selection": {"mode": "single_index", "pool_ids": ["csi300"]},
            "target_count": 20,
            "program_id": "advp_test",
        }
    ]


def test_delivery_preflight_api_rejects_unknown_fields_before_service_call() -> None:
    client, service = _client()

    response = client.post(
        "/api/v1/advisory/delivery-preflight",
        json={
            "package_id": "pkg_test",
            "universe_selection": {"mode": "stock_universe", "pool_ids": []},
            "target_count": 20,
            "unexpected": True,
        },
    )

    assert response.status_code == 422
    assert service.calls == []


def test_delivery_preflight_api_maps_advisory_contract_error_to_typed_400() -> None:
    app = FastAPI()
    app.include_router(advisory_router.router, prefix="/api/v1")

    class _InvalidService:
        @staticmethod
        def preflight(**_kwargs: Any) -> dict[str, Any]:
            raise AdvisoryUniverseContractError(
                "ADVISORY_UNIVERSE_SELECTION_INVALID",
                "invalid test universe",
                context={"field": "universe_selection"},
            )

    app.dependency_overrides[advisory_router.get_advisory_delivery_preflight_service] = _InvalidService
    client = TestClient(app)

    response = client.post(
        "/api/v1/advisory/delivery-preflight",
        json={
            "package_id": "pkg_test",
            "universe_selection": {"mode": "single_index", "pool_ids": ["csi300"]},
            "target_count": 20,
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == {
        "reason_code": "ADVISORY_UNIVERSE_SELECTION_INVALID",
        "message": "invalid test universe",
        "context": {"field": "universe_selection"},
    }
