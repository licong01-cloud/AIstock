from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import prometheus_admin as prometheus_admin_router
from backend.services.prometheus_admin import (
    PROMETHEUS_HISTORY_CONFIRM_TEXT,
    PrometheusAdminService,
    get_prometheus_admin_service,
)


class FakePrometheusService:
    def __init__(self) -> None:
        self.cleanup_calls: list[dict] = []

    def get_status(self) -> dict:
        return {
            "base_url": "http://prometheus:9090",
            "ready": {"status_code": 200, "body": "Prometheus Server is Ready."},
            "retention": {"time": "14d", "size": "30GB"},
            "admin_api_enabled": True,
            "lifecycle_enabled": True,
            "runtime_info": {},
            "tsdb_status": {},
        }

    def build_cleanup_plan(self, **kwargs):
        service = PrometheusAdminService(
            now_provider=lambda: datetime(2026, 5, 3, 12, 0, 0, tzinfo=timezone.utc)
        )
        return service.build_cleanup_plan(**kwargs)

    def cleanup_history(self, **kwargs) -> dict:
        self.cleanup_calls.append(kwargs)
        return {
            "plan": self.build_cleanup_plan(**kwargs).to_dict(),
            "delete_series": {"status": "success", "data": None},
            "clean_tombstones": {"status": "success", "data": None},
        }


class EmptySuccessResponse:
    status_code = 204
    text = ""

    def json(self):
        raise ValueError("empty response")


class RecordingPrometheusSession:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def request(self, method: str, url: str, params=None, timeout=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "params": params,
                "timeout": timeout,
            }
        )
        return EmptySuccessResponse()


def _client(service: FakePrometheusService) -> TestClient:
    app = FastAPI()
    app.include_router(prometheus_admin_router.router, prefix="/api/v1")
    app.dependency_overrides[get_prometheus_admin_service] = lambda: service
    return TestClient(app)


def test_prometheus_status_exposes_retention_and_admin_flags() -> None:
    client = _client(FakePrometheusService())

    response = client.get("/api/v1/prometheus-admin/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["status"]["retention"] == {"time": "14d", "size": "30GB"}
    assert payload["status"]["admin_api_enabled"] is True


def test_prometheus_cleanup_preview_is_read_only_and_uses_default_14_days() -> None:
    service = FakePrometheusService()
    client = _client(service)

    response = client.post("/api/v1/prometheus-admin/cleanup/preview", json={})

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["dry_run"] is True
    assert payload["plan"]["older_than_days"] == 14
    assert payload["plan"]["end"] == "2026-04-19T12:00:00Z"
    assert payload["plan"]["confirm_text_required"] == PROMETHEUS_HISTORY_CONFIRM_TEXT
    assert service.cleanup_calls == []


def test_prometheus_cleanup_rejects_wrong_confirmation_without_side_effect() -> None:
    service = FakePrometheusService()
    client = _client(service)

    response = client.post(
        "/api/v1/prometheus-admin/cleanup",
        json={"confirm_text": "wrong", "older_than_days": 14},
    )

    assert response.status_code == 400
    assert service.cleanup_calls == []


def test_prometheus_cleanup_executes_delete_and_tombstone_clean_with_confirmation() -> None:
    service = FakePrometheusService()
    client = _client(service)

    response = client.post(
        "/api/v1/prometheus-admin/cleanup",
        json={"confirm_text": PROMETHEUS_HISTORY_CONFIRM_TEXT, "older_than_days": 14},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["cleanup"]["delete_series"]["status"] == "success"
    assert service.cleanup_calls == [
        {
            "older_than_days": 14,
            "matchers": ['{__name__=~".+"}'],
            "clean_tombstones": True,
        }
    ]


def test_prometheus_service_accepts_real_admin_api_empty_204_responses() -> None:
    session = RecordingPrometheusSession()
    service = PrometheusAdminService(
        base_url="http://localhost:9090",
        session=session,
        now_provider=lambda: datetime(2026, 5, 3, 12, 0, 0, tzinfo=timezone.utc),
    )

    result = service.cleanup_history(older_than_days=14)

    assert result["delete_series"] == {"status": "success", "data": None}
    assert result["clean_tombstones"] == {"status": "success", "data": None}
    assert session.calls[0]["method"] == "POST"
    assert session.calls[0]["url"] == "http://localhost:9090/api/v1/admin/tsdb/delete_series"
    assert session.calls[0]["params"] == [
        ("match[]", '{__name__=~".+"}'),
        ("start", "1970-01-01T00:00:00Z"),
        ("end", "2026-04-19T12:00:00Z"),
    ]
    assert session.calls[1]["url"] == "http://localhost:9090/api/v1/admin/tsdb/clean_tombstones"
