from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import strategy_packages as router_module
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


@pytest.fixture
def app_and_repo(monkeypatch: pytest.MonkeyPatch) -> tuple[FastAPI, InMemoryStrategyPackageRepository]:
    repo = InMemoryStrategyPackageRepository()

    def _factory(*args, **kwargs):  # noqa: ANN001, ARG001 -- mirror router construction.
        return StrategyPackageService(repository=repo)

    monkeypatch.setattr(router_module, "StrategyPackageService", _factory)

    app = FastAPI()
    app.include_router(router_module.router)
    return app, repo


def _seed_drifted_package(repo: InMemoryStrategyPackageRepository) -> str:
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_id": "pkg", "package_name": "test"}))
    repo.save_manifest(manifest)
    repo.records["pkg"] = repo.records["pkg"].model_copy(update={"manifest_sha256": "00badhash"})
    return manifest.manifest_sha256 or ""


def test_manifest_integrity_endpoint_returns_repair_plan(
    app_and_repo: tuple[FastAPI, InMemoryStrategyPackageRepository],
):
    app, repo = app_and_repo
    correct_hash = _seed_drifted_package(repo)
    client = TestClient(app)

    response = client.get("/strategy-packages/manifest-integrity")

    assert response.status_code == 200
    payload = response.json()
    drift = payload["report"]["drifted"][0]
    assert drift["package_id"] == "pkg"
    assert drift["computed_sha256"] == correct_hash
    assert drift["repair_plan"]["requires_operator_confirmation"] is True
    assert drift["repair_plan"]["confirm_stored_sha256"] == "00badhash"
    assert drift["repair_plan"]["confirm_computed_sha256"] == correct_hash
    assert drift["repair_plan"]["rollback_restore"]["restore_value"] == "00badhash"


def test_repair_manifest_hash_endpoint_requires_confirmation(
    app_and_repo: tuple[FastAPI, InMemoryStrategyPackageRepository],
):
    app, repo = app_and_repo
    correct_hash = _seed_drifted_package(repo)
    client = TestClient(app)

    response = client.post("/strategy-packages/pkg/repair-manifest-hash", json={"operator": "unit_test"})

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["error_code"] == "INVALID_STATE_TRANSITION"
    assert detail["context"]["stored_sha256"] == "00badhash"
    assert detail["context"]["computed_sha256"] == correct_hash
    assert repo.records["pkg"].manifest_sha256 == "00badhash"


def test_repair_manifest_hash_endpoint_passes_confirmation_fields(
    app_and_repo: tuple[FastAPI, InMemoryStrategyPackageRepository],
):
    app, repo = app_and_repo
    correct_hash = _seed_drifted_package(repo)
    client = TestClient(app)

    response = client.post(
        "/strategy-packages/pkg/repair-manifest-hash",
        json={
            "operator": "unit_test",
            "confirm_stored_sha256": "00badhash",
            "confirm_computed_sha256": correct_hash,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["package"]["package_id"] == "pkg"
    assert payload["package"]["manifest_sha256"] == correct_hash
    assert repo.records["pkg"].manifest_sha256 == correct_hash
    repair_events = [event for event in repo.events if event.reason == "manifest_hash_repaired"]
    assert repair_events[0].context["operator"] == "unit_test"
