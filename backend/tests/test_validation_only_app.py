from __future__ import annotations

import importlib
import sys

from fastapi.testclient import TestClient


def test_validation_only_app_import_does_not_import_main_runtime() -> None:
    sys.modules.pop("backend.validation_app", None)
    sys.modules.pop("backend.main", None)

    module = importlib.import_module("backend.validation_app")

    assert "backend.main" not in sys.modules
    assert module.app.title == "AIstock Validation Center Only"


def test_validation_only_app_exposes_only_validation_api_paths() -> None:
    from backend.validation_app import create_app

    client = TestClient(create_app())
    response = client.get("/openapi.json")

    assert response.status_code == 200
    paths = set(response.json()["paths"])
    assert "/api/v1/validation/health" in paths
    assert all(path.startswith("/api/v1/validation/") for path in paths)
    assert "/api/v1/qmt/status" not in paths
    assert not any(path.startswith("/api/v1/paper-v2") for path in paths)
    assert not any(path.startswith("/api/v1/qmt") for path in paths)
