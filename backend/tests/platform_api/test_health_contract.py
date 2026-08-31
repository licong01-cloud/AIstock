from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import health as subject


_REPO_ROOT = Path(__file__).resolve().parents[3]


def test_router_package_import_does_not_eager_load_route_modules() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json, sys; import backend.routers; "
                "print(json.dumps(sorted(name for name in sys.modules "
                "if name.startswith('backend.routers.'))))"
            ),
        ],
        cwd=_REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert json.loads(completed.stdout) == []


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
        output = kwargs["stdout"]
        output.write(("d" * 40 + "\n") if command[1:3] == ["rev-parse", "HEAD"] else "")
        output.flush()
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    assert subject._capture_runtime_identity(tmp_path) == {"status": "ready", "merge_commit": "d" * 40}


def test_capture_runtime_identity_rejects_dirty_tracked_checkout(monkeypatch, tmp_path) -> None:
    def fake_run(command, **kwargs):
        output = kwargs["stdout"]
        output.write(
            ("d" * 40 + "\n") if command[1:3] == ["rev-parse", "HEAD"] else " M backend/main.py\n"
        )
        output.flush()
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    identity = subject._capture_runtime_identity(tmp_path)

    assert identity["status"] == "unavailable"
    assert identity["reason_code"] == "AISTOCK_RUNTIME_IDENTITY_UNAVAILABLE"
    assert "dirty" in identity["message"]


def test_capture_runtime_identity_does_not_create_pipe_reader_threads(monkeypatch, tmp_path) -> None:
    observed_kwargs = []

    def fake_run(command, **kwargs):
        observed_kwargs.append(kwargs)
        output = kwargs["stdout"]
        output.write(("e" * 40 + "\n") if command[1:3] == ["rev-parse", "HEAD"] else "")
        output.flush()
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    assert subject._capture_runtime_identity(tmp_path) == {"status": "ready", "merge_commit": "e" * 40}
    assert len(observed_kwargs) == 2
    assert all("capture_output" not in kwargs for kwargs in observed_kwargs)
    assert all(kwargs["stdout"] is not subprocess.PIPE for kwargs in observed_kwargs)
    assert all(kwargs["stderr"] is subprocess.STDOUT for kwargs in observed_kwargs)


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
