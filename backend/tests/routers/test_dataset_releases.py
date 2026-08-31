from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.deps import (
    DATASET_RELEASE_TOKEN_FILE_ENV,
    DatasetReleasePrincipal,
    require_dataset_release_operator,
)
from backend.routers import dataset_releases
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_service import (
    DatasetReleaseControlService,
    DatasetReleaseProfileBinding,
)
from backend.services.dataset_release.control_store import ControlStore, append_event, utc_now
from backend.services.dataset_release.errors import ProfileValidationError
from backend.services.dataset_release.subprocess_runner import run_streamed


@pytest.fixture
def service(tmp_path: Path) -> DatasetReleaseControlService:
    store = ControlStore.initialize(tmp_path / "control")
    return DatasetReleaseControlService(
        [
            DatasetReleaseProfileBinding(
                profile_id="qe_hmm_full_v1",
                semantic_profile_digest="a" * 64,
                cutoff_policy="previous_month_last_completed_trading_day",
                store=store,
                cas=CASStore(store.root),
                cutoff_resolver=lambda _: date(2026, 7, 31),
            )
        ]
    )


def _client(service: DatasetReleaseControlService, *, override_auth: bool = True) -> TestClient:
    app = FastAPI()
    app.include_router(dataset_releases.router, prefix="/api/v1")
    app.dependency_overrides[dataset_releases.get_dataset_release_control_service] = lambda: service
    if override_auth:
        app.dependency_overrides[require_dataset_release_operator] = lambda: DatasetReleasePrincipal(
            principal_id="dataset-operator:test",
            token_file_id="test-file",
            cursor_signing_key=b"x" * 32,
        )
    return TestClient(app)


def _monthly() -> dict[str, object]:
    return {
        "schema_version": "dataset_release_monthly_request_v1",
        "profile": "qe_hmm_full_v1",
        "cutoff_policy": "auto-previous-month",
        "scope": "full",
        "candidate_only": True,
    }


def _dependency_calls(dependant) -> set[object]:
    calls = {dependant.call}
    for child in dependant.dependencies:
        calls.update(_dependency_calls(child))
    return calls


def test_every_dataset_release_route_requires_operator_dependency() -> None:
    routes = [route for route in dataset_releases.router.routes if hasattr(route, "dependant")]
    assert routes
    for route in routes:
        assert require_dataset_release_operator in _dependency_calls(route.dependant), route.path


def _insert_run(service: DatasetReleaseControlService) -> tuple[ControlStore, str, str]:
    store = service._bindings["qe_hmm_full_v1"].store
    run_id = "dsr_" + "1" * 32
    attempt_id = "dsa_" + "2" * 32
    now = utc_now()
    with store.transaction() as connection:
        connection.execute(
            """
            INSERT INTO intents(
                intent_id,logical_request_key,resolved_intent_key,source_content_root,
                source_provenance_root,pit_snapshot_digest,created_at
            ) VALUES (?,?,?,?,?,?,?)
            """,
            ("dsi_test", "2" * 64, "3" * 64, "4" * 64, "5" * 64, "6" * 64, now),
        )
        connection.execute(
            """
            INSERT INTO runs(
                run_id,intent_id,run_generation_digest,operation_kind,lineage_root_run_id,
                state,active_attempt_id,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                run_id,
                "dsi_test",
                "7" * 64,
                "BUILD",
                run_id,
                "EXECUTING",
                attempt_id,
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO attempts(
                attempt_id,run_id,ordinal,attempt_kind,state,owner,attempt_fence,
                created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (attempt_id, run_id, 1, "BUILD", "RUNNING", "fixture", 1, now, now),
        )
    return store, run_id, attempt_id


def test_all_routes_are_operator_protected_and_token_rotates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    service: DatasetReleaseControlService,
) -> None:
    token_file = tmp_path / "operator.token"
    first_value = "first-value-" * 3
    second_value = "second-value-" * 3
    token_file.write_text(first_value, encoding="utf-8")
    monkeypatch.setenv(DATASET_RELEASE_TOKEN_FILE_ENV, str(token_file))
    client = _client(service, override_auth=False)

    assert client.post("/api/v1/dataset-releases/preview", json=_monthly()).status_code == 401
    assert client.post("/api/v1/dataset-releases/runs", json=_monthly()).status_code == 401
    assert (
        client.post(
            "/api/v1/dataset-releases/preview",
            json=_monthly(),
            headers={"X-Dataset-Release-Operator-Token": "wrong"},
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/api/v1/dataset-releases/preview",
            json=_monthly(),
            headers={"X-Dataset-Release-Operator-Token": first_value},
        ).status_code
        == 200
    )
    token_file.write_text(second_value, encoding="utf-8")
    assert (
        client.post(
            "/api/v1/dataset-releases/preview",
            json=_monthly(),
            headers={"X-Dataset-Release-Operator-Token": first_value},
        ).status_code
        == 401
    )


def test_preview_submit_status_cancel_and_validation_contract(
    service: DatasetReleaseControlService,
) -> None:
    client = _client(service)
    preview = client.post("/api/v1/dataset-releases/preview", json=_monthly())
    assert preview.status_code == 200
    assert preview.json()["data"]["activation"] == "not_requested"
    preview_token = preview.json()["data"]["preview_token"]
    assert preview_token.startswith("dsp1_")

    no_key = client.post("/api/v1/dataset-releases/runs", json=_monthly())
    assert no_key.status_code == 422
    assert no_key.json()["detail"]["error_code"] == "DATASET_RELEASE_REQUEST_INVALID"

    submitted = client.post(
        "/api/v1/dataset-releases/runs",
        json={**_monthly(), "preview_token": preview_token},
        headers={"Idempotency-Key": "monthly-one"},
    )
    assert submitted.status_code == 202
    body = submitted.json()
    assert body["run_id"] is None
    submission_id = body["submission_id"]
    submission_response = client.get(f"/api/v1/dataset-releases/submissions/{submission_id}")
    assert submission_response.status_code == 200
    assert "request_ref" not in submission_response.json()["data"]

    cancelled = client.post(
        f"/api/v1/dataset-releases/submissions/{submission_id}/cancel-request",
        json={},
        headers={"Idempotency-Key": "cancel-one"},
    )
    assert cancelled.status_code == 202
    assert cancelled.json()["data"]["state"] == "QUEUED"

    for field, value in (
        ("candidate_root", "E:/production"),
        ("production_path", "X:/production"),
        ("shell", "powershell -Command whoami"),
        ("env_file", "F:/.env"),
        ("activate", True),
        ("db_repair", True),
        ("restart", True),
        ("cleanup", True),
    ):
        rejected = client.post(
            "/api/v1/dataset-releases/preview",
            json={**_monthly(), field: value},
        )
        assert rejected.status_code == 422
        assert rejected.json()["detail"]["error_code"] == "DATASET_RELEASE_REQUEST_INVALID"


def test_preview_token_mismatch_is_visible_and_idempotency_conflict_has_exact_code(
    service: DatasetReleaseControlService,
) -> None:
    client = _client(service)
    preview = client.post("/api/v1/dataset-releases/preview", json=_monthly()).json()["data"]
    token = preview["preview_token"]
    replacement = "0" if token[-1] != "0" else "1"
    drifted = client.post(
        "/api/v1/dataset-releases/runs",
        json={**_monthly(), "preview_token": token[:-1] + replacement},
        headers={"Idempotency-Key": "monthly-preview-drift"},
    )
    assert drifted.status_code == 202
    assert drifted.json()["preview_token_status"] == "stale_or_mismatch"
    submission_id = drifted.json()["submission_id"]
    events = client.get(f"/api/v1/dataset-releases/submissions/{submission_id}/events").json()["items"]
    assert [item["type"] for item in events] == [
        "SUBMISSION_QUEUED",
        "PREVIEW_TOKEN_STALE_OR_MISMATCH",
    ]

    conflict = client.post(
        "/api/v1/dataset-releases/runs",
        json={**_monthly(), "scope": "sample"},
        headers={"Idempotency-Key": "monthly-preview-drift"},
    )
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["error_code"] == "DATASET_RELEASE_IDEMPOTENCY_CONFLICT"


def test_event_cursor_is_bounded_and_bound_to_principal_and_target(
    service: DatasetReleaseControlService,
) -> None:
    client = _client(service)
    submitted = client.post(
        "/api/v1/dataset-releases/runs",
        json=_monthly(),
        headers={"Idempotency-Key": "monthly-events"},
    ).json()
    submission_id = submitted["submission_id"]
    store = service._bindings["qe_hmm_full_v1"].store
    with store.transaction() as connection:
        append_event(
            connection,
            event_type="ONE",
            submission_id=submission_id,
            created_at=utc_now(),
        )
        append_event(
            connection,
            event_type="TWO",
            submission_id=submission_id,
            created_at=utc_now(),
        )

    first = client.get(f"/api/v1/dataset-releases/submissions/{submission_id}/events?limit=1")
    assert first.status_code == 200
    page = first.json()
    assert len(page["items"]) == 1
    assert page["has_more"] is True
    assert page["next_cursor"]
    second = client.get(
        f"/api/v1/dataset-releases/submissions/{submission_id}/events",
        params={"limit": 1, "cursor": page["next_cursor"]},
    )
    assert second.status_code == 200
    assert second.json()["items"][0]["event_id"] > page["items"][0]["event_id"]

    other = client.post(
        "/api/v1/dataset-releases/runs",
        json=_monthly(),
        headers={"Idempotency-Key": "monthly-other"},
    ).json()["submission_id"]
    cross_target = client.get(
        f"/api/v1/dataset-releases/submissions/{other}/events",
        params={"cursor": page["next_cursor"]},
    )
    assert cross_target.status_code == 400
    assert cross_target.json()["detail"]["error_code"] == "DATASET_RELEASE_CURSOR_INVALID"


def test_unknown_run_and_profile_fail_closed(service: DatasetReleaseControlService) -> None:
    client = _client(service)
    missing = client.get("/api/v1/dataset-releases/runs/missing")
    assert missing.status_code == 404
    forbidden = client.get("/api/v1/dataset-releases/runs/missing", params={"profile": "unknown"})
    assert forbidden.status_code == 403


def test_submission_terminal_receipt_is_bounded_and_queryable(
    service: DatasetReleaseControlService,
) -> None:
    client = _client(service)
    submitted = client.post(
        "/api/v1/dataset-releases/runs",
        json=_monthly(),
        headers={"Idempotency-Key": "submission-receipt"},
    ).json()
    submission_id = submitted["submission_id"]
    pending = client.get(f"/api/v1/dataset-releases/submissions/{submission_id}/receipt")
    assert pending.status_code == 409

    binding = service._bindings["qe_hmm_full_v1"]
    reference = binding.cas.put_json(
        {
            "schema_version": "dataset_release_worker_error_v1",
            "kind": "resolution",
            "target_id": submission_id,
            "error_code": "TRANSIENT_SOURCE",
        }
    )
    with binding.store.transaction() as connection:
        connection.execute(
            "UPDATE submissions SET state='BLOCKED_RETRY_EXHAUSTED',terminal_receipt_ref=? WHERE submission_id=?",
            (reference.sha256, submission_id),
        )
    receipt = client.get(f"/api/v1/dataset-releases/submissions/{submission_id}/receipt")
    assert receipt.status_code == 200
    assert receipt.json()["data"]["target_id"] == submission_id


def test_unavailable_default_control_dependency_returns_versioned_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(dataset_releases.router, prefix="/api/v1")
    app.dependency_overrides[require_dataset_release_operator] = lambda: DatasetReleasePrincipal(
        principal_id="dataset-operator:test",
        token_file_id="test-file",
        cursor_signing_key=b"x" * 32,
    )
    dataset_releases.get_dataset_release_control_service.cache_clear()

    def unavailable(_path: Path):
        raise ProfileValidationError("fixture profile unavailable")

    monkeypatch.setattr(dataset_releases, "load_dataset_profile", unavailable)
    try:
        response = TestClient(app).post("/api/v1/dataset-releases/preview", json=_monthly())
    finally:
        dataset_releases.get_dataset_release_control_service.cache_clear()

    assert response.status_code == 503
    assert response.json() == {
        "detail": {
            "error_code": "DATASET_RELEASE_CONTROL_UNAVAILABLE",
            "message": "Dataset release control plane is unavailable.",
            "retryable": True,
            "context_ref": None,
        }
    }


def test_run_log_is_byte_line_bounded_and_cursor_bound_to_stream(
    service: DatasetReleaseControlService,
) -> None:
    store, run_id, attempt_id = _insert_run(service)
    catalog = store.register_run_log_execution(
        run_id=run_id,
        attempt_id=attempt_id,
        attempt_fence=1,
        execution_id="fixture-runner",
    )
    log_root = store.root / str(catalog["relative_log_root"])
    result = run_streamed(
        [
            sys.executable,
            "-c",
            "import sys; sys.stdout.buffer.write(b''.join(f'line-{index}\\n'.encode() for index in range(20)))",
        ],
        cwd=Path.cwd(),
        log_root=log_root,
        segment_limit_bytes=1024,
    )
    assert result.returncode == 0
    client = _client(service)

    first = client.get(
        f"/api/v1/dataset-releases/runs/{run_id}/log",
        params={"max_bytes": 1024, "max_lines": 2},
    )
    assert first.status_code == 200
    page = first.json()
    assert page["data"]["text"] == "line-0\nline-1\n"
    assert page["data"]["execution_id"] == "fixture-runner"
    assert page["data"]["log_id"] == catalog["log_id"]
    assert page["has_more"] is True
    second = client.get(
        f"/api/v1/dataset-releases/runs/{run_id}/log",
        params={
            "max_bytes": 1024,
            "max_lines": 2,
            "cursor": page["next_cursor"],
        },
    )
    assert second.status_code == 200
    assert second.json()["data"]["text"].startswith("line-2")
    cross_stream = client.get(
        f"/api/v1/dataset-releases/runs/{run_id}/log",
        params={"stream": "stderr", "cursor": page["next_cursor"]},
    )
    assert cross_stream.status_code == 400

    receipt = client.get(f"/api/v1/dataset-releases/runs/{run_id}/receipt")
    assert receipt.status_code == 409
