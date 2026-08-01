from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import health as subject


def test_health_contract_reports_configured_application_identity(monkeypatch) -> None:
    monkeypatch.setattr(subject, "get_app_settings", lambda: SimpleNamespace(app_name="AIstock-test"))
    app = FastAPI()
    app.include_router(subject.router, prefix="/api/v1")

    response = TestClient(app).get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "app": "AIstock-test"}


def test_runtime_identity_returns_process_frozen_merge_commit(monkeypatch) -> None:
    monkeypatch.setattr(subject, "_PROCESS_RUNTIME_IDENTITY", {"status": "ready", "merge_commit": "a" * 40})
    app = FastAPI()
    app.include_router(subject.router, prefix="/api/v1")

    response = TestClient(app).get("/api/v1/runtime-identity")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "merge_commit": "a" * 40}


def test_capture_runtime_identity_binds_exact_git_head(monkeypatch, tmp_path) -> None:
    def fake_run(command, **kwargs):
        del kwargs
        return SimpleNamespace(stdout=("d" * 40 + "\n") if command[1:3] == ["rev-parse", "HEAD"] else "")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    assert subject._capture_runtime_identity(tmp_path) == {"status": "ready", "merge_commit": "d" * 40}


def test_capture_runtime_identity_rejects_dirty_tracked_checkout(monkeypatch, tmp_path) -> None:
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
    app = FastAPI()
    app.include_router(subject.router, prefix="/api/v1")

    response = TestClient(app).get("/api/v1/runtime-identity")

    assert response.status_code == 503
    assert response.json()["detail"]["reason_code"] == "AISTOCK_RUNTIME_IDENTITY_UNAVAILABLE"
