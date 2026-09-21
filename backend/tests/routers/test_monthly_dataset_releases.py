from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path

from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from backend.deps import DatasetReleasePrincipal, require_dataset_release_operator
from backend.routers import monthly_dataset_releases as api
from backend.services.dataset_release.monthly_runtime import MonthlyRuntimeSettings


def _write_active(path: Path) -> None:
    baseline = path.parent / "baseline"
    baseline.mkdir()
    manifest = baseline / "qe_dataset_manifest.json"
    manifest.write_text(
        json.dumps(
            {"dataset_manifest_sha256": "a" * 64},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    manifest_file_sha256 = hashlib.sha256(manifest.read_bytes()).hexdigest()
    path.write_text(
        json.dumps(
            {
                "schema_version": "aistock_active_dataset_profile_v3",
                "generation": "20260920-v14-unified",
                "release_id": "qe_hmm_full_v2_20260831",
                "cutoff": "2026-08-31",
                "controller_paths": {"candidate_root": str(baseline)},
                "components": {
                    "dataset_manifest_sha256": "a" * 64,
                    "dataset_manifest_file_sha256": manifest_file_sha256,
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )


def _client(tmp_path: Path) -> tuple[TestClient, MonthlyRuntimeSettings, FastAPI]:
    active = tmp_path / "active.json"
    _write_active(active)
    roots = {name: tmp_path / name for name in ("state", "releases", "profiles", "artifacts", "authorizations")}
    for root in roots.values():
        root.mkdir()
    settings = MonthlyRuntimeSettings(
        state_root=roots["state"].absolute(),
        active_profile=active.absolute(),
        controller_release_root=roots["releases"].absolute(),
        profile_candidate_root=roots["profiles"].absolute(),
        artifact_root=roots["artifacts"].absolute(),
        wsl_release_root="/mnt/data/releases",
        node1_release_root="/home/data/releases",
        authorization_root=roots["authorizations"].absolute(),
    )
    app = FastAPI()
    app.include_router(api.router, prefix="/api/v1")
    app.dependency_overrides[require_dataset_release_operator] = lambda: DatasetReleasePrincipal(
        principal_id="dataset-operator:test",
        token_file_id="test",
        cursor_signing_key=b"x" * 32,
    )
    app.dependency_overrides[api.get_monthly_release_settings] = lambda: settings
    app.dependency_overrides[api.get_monthly_release_service] = lambda: settings.service(
        cutoff_resolver=lambda: date(2026, 9, 30)
    )
    return TestClient(app), settings, app


def _body() -> dict[str, object]:
    return {
        "schema_version": "aistock_monthly_release_request_v1",
        "target_cutoff": "2026-09-30",
        "product_profile": "qe_hmm_full_v2",
        "activation_mode": "prepare_only",
        "activation_authorization_ref": None,
        "repair_authorization_refs": [],
    }


def test_all_routes_require_dataset_release_operator(tmp_path: Path) -> None:
    _, _, app = _client(tmp_path)
    routes = [
        route
        for route in app.routes
        if isinstance(route, APIRoute) and route.path.startswith("/api/v1/qlib/monthly-releases")
    ]
    assert len(routes) == 8
    for route in routes:
        dependencies = {dependency.call for dependency in route.dependant.dependencies}
        assert require_dataset_release_operator in dependencies, route.path


def test_plan_is_read_only_and_submit_is_durable_and_idempotent(tmp_path: Path) -> None:
    client, settings, _ = _client(tmp_path)
    headers = {"Idempotency-Key": "september-2026"}
    plan = client.post("/api/v1/qlib/monthly-releases/plan", json=_body(), headers=headers)
    assert plan.status_code == 200
    assert plan.json()["data"]["active_profile_write"] is False
    assert not any((settings.state_root / "monthly").glob("*"))

    first = client.post("/api/v1/qlib/monthly-releases", json=_body(), headers=headers)
    second = client.post("/api/v1/qlib/monthly-releases", json=_body(), headers=headers)
    assert first.status_code == 202
    assert second.status_code == 202
    operation_id = first.json()["data"]["operation_id"]
    assert second.json()["data"]["operation_id"] == operation_id
    assert first.json()["data"]["status_url"] == (f"/api/v1/qlib/monthly-releases/{operation_id}")
    status = client.get(f"/api/v1/qlib/monthly-releases/{operation_id}")
    assert status.status_code == 200
    assert status.json()["data"]["status"] == "PLANNED"


def test_request_schema_and_idempotency_fail_closed(tmp_path: Path) -> None:
    client, _, _ = _client(tmp_path)
    assert client.post("/api/v1/qlib/monthly-releases", json=_body()).status_code == 422
    body = _body()
    body["unknown"] = True
    invalid = client.post(
        "/api/v1/qlib/monthly-releases",
        json=body,
        headers={"Idempotency-Key": "september-2026"},
    )
    assert invalid.status_code == 422
    assert invalid.json() == {
        "detail": {
            "error_code": "MONTHLY_RELEASE_REQUEST_INVALID",
            "message": "Monthly release request validation failed.",
            "retryable": False,
            "context": {},
        }
    }
    activation = _body()
    activation["activation_mode"] = "activate_when_ready"
    assert (
        client.post(
            "/api/v1/qlib/monthly-releases",
            json=activation,
            headers={"Idempotency-Key": "september-activation"},
        ).status_code
        == 422
    )


def test_cancel_resume_and_receipt_index_use_same_operation(tmp_path: Path) -> None:
    client, settings, _ = _client(tmp_path)
    created = client.post(
        "/api/v1/qlib/monthly-releases",
        json=_body(),
        headers={"Idempotency-Key": "september-2026"},
    ).json()["data"]
    operation_id = created["operation_id"]
    cancelled = client.post(
        f"/api/v1/qlib/monthly-releases/{operation_id}/cancel",
        json={"schema_version": "dataset_release_command_request_v1"},
    )
    assert cancelled.status_code == 202
    assert cancelled.json()["data"]["operation_id"] == operation_id
    assert cancelled.json()["data"]["cancel_requested"] is True

    state_path = settings.state_root / "monthly" / operation_id / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["status"] = "CANCELLED"
    state_path.write_text(
        json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    resumed = client.post(
        f"/api/v1/qlib/monthly-releases/{operation_id}/resume",
        json={"schema_version": "dataset_release_command_request_v1"},
    )
    assert resumed.status_code == 202
    assert resumed.json()["data"]["status"] == "PLANNED"
    receipts = client.get(f"/api/v1/qlib/monthly-releases/{operation_id}/receipts")
    assert receipts.status_code == 200
    assert receipts.json()["items"] == []


def test_operation_and_action_inputs_fail_closed(tmp_path: Path) -> None:
    client, _, _ = _client(tmp_path)
    assert client.get("/api/v1/qlib/monthly-releases/not-an-operation").status_code == 409
    response = client.post(
        "/api/v1/qlib/monthly-releases/not-an-operation/activate",
        json={
            "schema_version": "aistock_monthly_release_action_v1",
            "authorization_ref": "invalid",
        },
    )
    assert response.status_code == 422


def test_receipt_index_requires_an_existing_operation_and_is_bounded(tmp_path: Path) -> None:
    client, settings, _ = _client(tmp_path)
    missing = "dmr_" + "f" * 32
    assert client.get(f"/api/v1/qlib/monthly-releases/{missing}/receipts").status_code == 400

    operation_id = client.post(
        "/api/v1/qlib/monthly-releases",
        json=_body(),
        headers={"Idempotency-Key": "september-2026"},
    ).json()["data"]["operation_id"]
    receipts = settings.state_root / "monthly" / operation_id / "receipts"
    for index in range(api.MAX_RECEIPT_FILES + 1):
        (receipts / f"{index:03d}.json").write_text("{}\n", encoding="utf-8")
    response = client.get(f"/api/v1/qlib/monthly-releases/{operation_id}/receipts")
    assert response.status_code == 400
    assert response.json()["detail"]["error_code"] == "MONTHLY_RELEASE_ERROR"
