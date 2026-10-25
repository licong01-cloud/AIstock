from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from backend.routers import advisory, position_timing
from backend.services.dataset_release.managed_consumer_task import ManagedDatasetTaskError


@dataclass
class _Artifact:
    consumer_id: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": f"C:/state/{self.consumer_id}.json",
            "sha256": "a" * 64,
            "size": 123,
            "request": {"consumer_id": self.consumer_id},
        }


class _Store:
    def __init__(self, *, error: ManagedDatasetTaskError | None = None) -> None:
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> _Artifact:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return _Artifact(consumer_id=str(kwargs["consumer_id"]))


@pytest.mark.parametrize(
    ("module", "route", "consumer_id"),
    [
        (advisory, "/api/v1/advisory/dataset-preparations", "advisory"),
        (
            position_timing,
            "/api/v1/position-timing/dataset-preparations",
            "position_timing",
        ),
    ],
)
def test_managed_preparation_routes_freeze_the_expected_consumer(
    module: Any,
    route: str,
    consumer_id: str,
) -> None:
    store = _Store()
    app = FastAPI()
    app.include_router(module.router, prefix="/api/v1")
    app.dependency_overrides[module.get_managed_dataset_task_store] = lambda: store
    client = TestClient(app)

    response = client.post(
        route,
        headers={"Idempotency-Key": "monthly-research-202609"},
        json={"start_date": "2018-08-01", "end_date": "2026-09-30"},
    )

    assert response.status_code == 200
    assert response.json()["data"]["request"]["consumer_id"] == consumer_id
    assert store.calls == [
        {
            "consumer_id": consumer_id,
            "business_task_key": "monthly-research-202609",
            "start_date": date(2018, 8, 1),
            "end_date": date(2026, 9, 30),
        }
    ]


@pytest.mark.parametrize(
    ("module", "route"),
    [
        (advisory, "/api/v1/advisory/dataset-preparations"),
        (position_timing, "/api/v1/position-timing/dataset-preparations"),
    ],
)
def test_managed_preparation_routes_fail_closed_on_idempotency_drift(
    module: Any,
    route: str,
) -> None:
    store = _Store(
        error=ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_IDEMPOTENCY_CONFLICT",
            "business key drift",
        )
    )
    app = FastAPI()
    app.include_router(module.router, prefix="/api/v1")
    app.dependency_overrides[module.get_managed_dataset_task_store] = lambda: store

    response = TestClient(app).post(
        route,
        headers={"Idempotency-Key": "monthly-research-202609"},
        json={"start_date": "2018-08-01", "end_date": "2026-09-30"},
    )

    assert response.status_code == 409
    assert response.json() == {
        "detail": {
            "error_code": "MANAGED_DATASET_TASK_IDEMPOTENCY_CONFLICT",
            "message": "business key drift",
        }
    }


@pytest.mark.parametrize(
    ("module", "route"),
    [
        (advisory, "/api/v1/advisory/dataset-preparations"),
        (position_timing, "/api/v1/position-timing/dataset-preparations"),
    ],
)
def test_managed_preparation_routes_report_artifact_corruption_as_server_error(
    module: Any,
    route: str,
) -> None:
    store = _Store(
        error=ManagedDatasetTaskError(
            "MANAGED_DATASET_TASK_IDENTITY_INVALID",
            "stored identity drift",
        )
    )
    app = FastAPI()
    app.include_router(module.router, prefix="/api/v1")
    app.dependency_overrides[module.get_managed_dataset_task_store] = lambda: store

    response = TestClient(app).post(
        route,
        headers={"Idempotency-Key": "monthly-research-202609"},
        json={"start_date": "2018-08-01", "end_date": "2026-09-30"},
    )

    assert response.status_code == 500
    assert response.json()["detail"]["error_code"] == (
        "MANAGED_DATASET_TASK_IDENTITY_INVALID"
    )


@pytest.mark.parametrize("module", [advisory, position_timing])
def test_managed_preparation_dependency_reports_missing_state_root(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
) -> None:
    monkeypatch.delenv("AISTOCK_MONTHLY_RELEASE_STATE_ROOT", raising=False)
    module.get_managed_dataset_task_store.cache_clear()

    with pytest.raises(module.HTTPException) as raised:
        module.get_managed_dataset_task_store()

    assert raised.value.status_code == 503
    assert raised.value.detail["error_code"] == "MANAGED_DATASET_TASK_ROOT_UNAVAILABLE"
    module.get_managed_dataset_task_store.cache_clear()
