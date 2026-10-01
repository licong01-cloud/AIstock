from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import advisory as advisory_router
from backend.services.advisory_historical_range.query_repository import HistoricalRangeQueryError


class _ComparisonService:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def compare_runs(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {
            "schema_version": "advisory_historical_range_comparison_v1",
            "batch_id": kwargs["batch_id"],
            "comparability": {
                "status": "COMPARABLE",
                "blockers": [],
                "warnings": [],
                "summary_policy_hash": "a" * 64,
                "producer_code_hash": "b" * 64,
                "decision_use": "BUSINESS_VALIDATION_ONLY",
            },
            "baseline": {"range_run_id": kwargs["baseline_range_run_id"]},
            "candidate": {"range_run_id": kwargs["candidate_range_run_id"]},
            "day_support": {},
            "metrics": [],
            "interpretation": {
                "delta_semantics": "CANDIDATE_MINUS_BASELINE",
                "winner_declared": False,
                "significance_claimed": False,
            },
        }


def _client(service: object) -> TestClient:
    app = FastAPI()
    app.include_router(advisory_router.router, prefix="/api/v1")
    app.dependency_overrides[advisory_router.get_historical_range_application_service] = lambda: service
    return TestClient(app)


def test_comparison_api_preserves_explicit_baseline_and_candidate_roles() -> None:
    service = _ComparisonService()
    response = _client(service).get(
        "/api/v1/advisory/historical-range-batches/ahrb_1/comparison",
        params={
            "baseline_range_run_id": "run_baseline",
            "candidate_range_run_id": "run_candidate",
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["comparison"]["comparability"]["status"] == "COMPARABLE"
    assert service.calls == [
        {
            "batch_id": "ahrb_1",
            "baseline_range_run_id": "run_baseline",
            "candidate_range_run_id": "run_candidate",
        }
    ]


def test_comparison_api_maps_domain_validation_to_typed_422() -> None:
    class _InvalidService:
        @staticmethod
        def compare_runs(**_kwargs: Any) -> dict[str, Any]:
            raise HistoricalRangeQueryError(
                "ADVISORY_HR_COMPARISON_BATCH_MISMATCH",
                "run is outside batch",
                context={"batch_id": "ahrb_1"},
            )

    response = _client(_InvalidService()).get(
        "/api/v1/advisory/historical-range-batches/ahrb_1/comparison",
        params={
            "baseline_range_run_id": "run_a",
            "candidate_range_run_id": "run_b",
        },
    )

    assert response.status_code == 422
    assert response.json()["detail"]["reason_code"] == "ADVISORY_HR_COMPARISON_BATCH_MISMATCH"
