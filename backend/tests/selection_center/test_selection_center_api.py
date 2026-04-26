from __future__ import annotations

from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import selection_center as selection_center_router
from backend.services.selection_center.models import (
    SelectionCandidate,
    SelectionExclusion,
    SelectionMode,
    SelectionRun,
    SelectionRunStatus,
)
from backend.services.trading_core.errors import UnsupportedFeatureError


class FakeSelectionCenterService:
    def __init__(self, run: SelectionRun) -> None:
        self.run = run
        self.calls: list[dict] = []

    def run_packages(self, **kwargs):
        self.calls.append(kwargs)
        return self.run

    def aggregate_existing_runs(self, **kwargs):
        self.calls.append(kwargs)
        return self.run

    def list_selectable_packages(self, *, limit: int = 200):
        assert limit > 0
        return [
            {
                "package_id": "pkg_a",
                "package_name": "Package A",
                "package_status": "SELECTION_ENABLED",
                "manifest_sha256": "sha_a",
                "metrics_summary": {"ic": 0.05, "rank_ic": 0.04, "sharpe": 1.2},
                "model_state": {"package_id": "pkg_a", "staleness_status": "CURRENT"},
                "latest_selection_run": {"run_id": "sel_latest", "candidate_count": 10},
            }
        ]

    def get_run(self, run_id: str) -> SelectionRun:
        assert run_id == self.run.run_id
        return self.run

    def list_runs(self, *, limit: int = 100) -> list[SelectionRun]:
        assert limit > 0
        return [self.run]

    def create_paper_portfolio_from_run(self, **kwargs):
        raise UnsupportedFeatureError(
            "creating a paper portfolio from multi-package selection requires a combined StrategyPackage",
            context={"run_id": kwargs["run_id"], "mode": self.run.mode.value, "package_ids": self.run.package_ids},
        )

    def list_paper_portfolio_links(self, run_id: str) -> list:
        assert run_id == self.run.run_id
        return []


def _client(service: FakeSelectionCenterService) -> TestClient:
    app = FastAPI()
    app.include_router(selection_center_router.router, prefix="/api/v1")
    app.dependency_overrides[selection_center_router.get_selection_center_service] = lambda: service
    return TestClient(app)


def _weighted_run() -> SelectionRun:
    return SelectionRun(
        run_id="sel_weighted_api",
        mode=SelectionMode.WEIGHTED_FUSION,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        package_ids=["pkg_a", "pkg_b"],
        runtime_config={"package_weights": {"pkg_a": 0.6, "pkg_b": 0.4}},
        status=SelectionRunStatus.SUCCEEDED,
        aggregate_results=[
            SelectionCandidate(
                symbol="000001.SZ",
                score=0.8,
                rank=1,
                target_weight=0.03,
                reference_price=10.0,
                component_scores={
                    "fusion_method": "weighted_rank_fusion",
                    "source_package_ids": ["pkg_a", "pkg_b"],
                    "package_ranks": {"pkg_a": 1, "pkg_b": 2},
                    "package_weights": {"pkg_a": 0.6, "pkg_b": 0.4},
                },
                reason="weighted_fusion_aggregate",
            )
        ],
        excluded_results={
            "pkg_a": [
                SelectionExclusion(
                    symbol="000002.SZ",
                    score=0.7,
                    rank=2,
                    reason="industry_blacklisted",
                    source="runtime_profile.industry_blacklist",
                    context={"industry": "Bank"},
                )
            ]
        },
        manifest_sha256_by_package={"pkg_a": "sha_a", "pkg_b": "sha_b"},
    )


def test_selection_center_api_accepts_weighted_fusion_and_returns_trace() -> None:
    service = FakeSelectionCenterService(_weighted_run())
    client = _client(service)

    response = client.post(
        "/api/v1/selection-center/runs",
        json={
            "package_ids": ["pkg_a", "pkg_b"],
            "trade_date": "2024-01-02",
            "data_source": "DB_HISTORICAL",
            "mode": "weighted_fusion",
            "runtime_config": {"package_weights": {"pkg_a": 0.6, "pkg_b": 0.4}},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["run"]["mode"] == "weighted_fusion"
    assert payload["run"]["package_ids"] == ["pkg_a", "pkg_b"]
    assert payload["run"]["aggregate_results"][0]["component_scores"]["fusion_method"] == "weighted_rank_fusion"
    assert service.calls[0]["mode"] == SelectionMode.WEIGHTED_FUSION
    assert service.calls[0]["runtime_config"]["package_weights"] == {"pkg_a": 0.6, "pkg_b": 0.4}


def test_selection_center_api_exposes_aggregate_results() -> None:
    service = FakeSelectionCenterService(_weighted_run())
    client = _client(service)

    response = client.get("/api/v1/selection-center/runs/sel_weighted_api/aggregate-results")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["run_id"] == "sel_weighted_api"
    assert payload["aggregate_results"][0]["component_scores"]["source_package_ids"] == ["pkg_a", "pkg_b"]


def test_selection_center_api_exposes_excluded_results() -> None:
    service = FakeSelectionCenterService(_weighted_run())
    client = _client(service)

    response = client.get("/api/v1/selection-center/runs/sel_weighted_api/excluded-results")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["excluded_results"]["pkg_a"][0]["reason"] == "industry_blacklisted"
    assert payload["excluded_results"]["pkg_a"][0]["context"]["industry"] == "Bank"


def test_selection_center_api_lists_selectable_packages() -> None:
    service = FakeSelectionCenterService(_weighted_run())
    client = _client(service)

    response = client.get("/api/v1/selection-center/selectable-packages")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["packages"][0]["package_id"] == "pkg_a"
    assert payload["packages"][0]["metrics_summary"]["sharpe"] == 1.2


def test_selection_center_api_aggregates_existing_runs() -> None:
    service = FakeSelectionCenterService(_weighted_run())
    client = _client(service)

    response = client.post(
        "/api/v1/selection-center/aggregate-runs",
        json={
            "source_run_ids": ["sel_a", "sel_b"],
            "mode": "weighted_fusion",
            "runtime_config": {"package_weights": {"pkg_a": 0.6, "pkg_b": 0.4}},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["run"]["run_id"] == "sel_weighted_api"
    assert service.calls[0]["source_run_ids"] == ["sel_a", "sel_b"]
    assert service.calls[0]["mode"] == SelectionMode.WEIGHTED_FUSION


def test_selection_center_api_rejects_multi_package_paper_creation_fail_fast() -> None:
    service = FakeSelectionCenterService(_weighted_run())
    client = _client(service)

    response = client.post(
        "/api/v1/selection-center/runs/sel_weighted_api/create-paper-portfolio",
        json={
            "portfolio_name": "should fail",
            "initial_cash": 100000,
            "start_date": "2024-01-03",
            "data_source": "DB_HISTORICAL",
        },
    )

    assert response.status_code == 422
    payload = response.json()
    assert payload["detail"]["error_code"] == "UNSUPPORTED_FEATURE"
    assert payload["detail"]["context"]["package_ids"] == ["pkg_a", "pkg_b"]
