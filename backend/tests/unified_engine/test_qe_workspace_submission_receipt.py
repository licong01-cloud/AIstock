from __future__ import annotations

import asyncio

import httpx
import pytest

from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceExecutionEnvironmentError,
    QEWorkspaceSubmissionContractError,
    QEWorkspaceSubmissionRejected,
    QEWorkspaceSubmissionTransportError,
    QEWorkspaceTypedKillContractError,
    QEWorkspaceTypedKillTransportError,
)


INTENT_HASH = "a" * 64
REQUEST_DIGEST = "b" * 64
KILL_INTENT_HASH = "c" * 64
COMMAND_ID = "macmd_2b84ea4e40d2d69ca8cc3c71d938ad30"
PROCESS_IDENTITY = {"pid": 43210, "pgid": 43210, "start_time_ticks": 987654321}
ENVIRONMENT_SNAPSHOT_ID = "qeenv_test_snapshot"
ENVIRONMENT_MANIFEST = {"schema_version": "qe_execution_environment_manifest_v1", "value": "stable"}
ENVIRONMENT_MANIFEST_HASH = __import__("hashlib").sha256(
    __import__("json").dumps(
        ENVIRONMENT_MANIFEST,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


def _client(handler: httpx.MockTransport) -> QEWorkspaceClient:
    client = QEWorkspaceClient(base_url="https://qe.example/api/v1/qe_workspace")
    old_client = client.client
    client.client = httpx.AsyncClient(transport=handler, timeout=1.0)
    asyncio.run(old_client.aclose())
    return client


def test_submit_loop_requires_and_validates_server_receipt() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["payload"] = __import__("json").loads(request.content)
        return httpx.Response(
            200,
            json={
                "loop_id": "Loop3",
                "status": "accepted",
                "message": "reserved",
                "submission_intent_hash": INTENT_HASH,
                "request_digest": REQUEST_DIGEST,
                "receipt_status": "reserved",
                "duplicate_replay": False,
            },
        )

    client = _client(httpx.MockTransport(handler))
    try:
        receipt = asyncio.run(
            client.submit_loop(
                "qe_task",
                3,
                {"model_id": "lgbm"},
                {"conf.yaml": "body"},
                "python qrun.py conf.yaml",
                callback_url="https://callback.example/qe",
                submission_intent_hash=INTENT_HASH,
            )
        )
    finally:
        asyncio.run(client.close())

    assert receipt.task_id == "qe_task"
    assert receipt.loop_id == "Loop3"
    assert receipt.receipt_status == "reserved"
    assert receipt.duplicate_replay is False
    assert captured["path"] == "/api/v1/qe_workspace/tasks/qe_task/loops"
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["submission_intent_hash"] == INTENT_HASH
    assert payload["callback_url"] == "https://callback.example/qe"


def test_create_and_run_loop_is_only_a_receipt_validating_loop_id_wrapper() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "loop_id": "Loop1",
                "status": "accepted",
                "message": "existing",
                "submission_intent_hash": INTENT_HASH,
                "request_digest": REQUEST_DIGEST,
                "receipt_status": "running",
                "duplicate_replay": True,
            },
        )

    client = _client(httpx.MockTransport(handler))
    try:
        loop_id = asyncio.run(
            client.create_and_run_loop(
                "qe_task",
                1,
                {},
                submission_intent_hash=INTENT_HASH,
            )
        )
    finally:
        asyncio.run(client.close())
    assert loop_id == "Loop1"


def test_execution_environment_manifest_and_submission_binding_are_exact() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/execution-environment"):
            return httpx.Response(
                200,
                json={
                    "schema_version": "qe_execution_environment_manifest_v1",
                    "execution_environment_snapshot_id": ENVIRONMENT_SNAPSHOT_ID,
                    "execution_environment_manifest_sha256": ENVIRONMENT_MANIFEST_HASH,
                    "manifest": ENVIRONMENT_MANIFEST,
                },
            )
        captured["payload"] = __import__("json").loads(request.content)
        return httpx.Response(
            200,
            json={
                "loop_id": "Loop1",
                "status": "accepted",
                "submission_intent_hash": INTENT_HASH,
                "request_digest": REQUEST_DIGEST,
                "receipt_status": "reserved",
                "duplicate_replay": False,
                "execution_identity_hash": "d" * 64,
                "execution_environment_snapshot_id": ENVIRONMENT_SNAPSHOT_ID,
                "execution_environment_manifest_sha256": ENVIRONMENT_MANIFEST_HASH,
            },
        )

    client = _client(httpx.MockTransport(handler))
    try:
        environment = asyncio.run(client.get_execution_environment())
        receipt = asyncio.run(
            client.submit_loop(
                "qe_task",
                1,
                {},
                submission_intent_hash=INTENT_HASH,
                execution_identity_hash="d" * 64,
                execution_environment_snapshot_id=environment.execution_environment_snapshot_id,
                execution_environment_manifest_sha256=environment.execution_environment_manifest_sha256,
            )
        )
    finally:
        asyncio.run(client.close())

    assert environment.manifest == ENVIRONMENT_MANIFEST
    assert receipt.execution_identity_hash == "d" * 64
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["execution_environment_snapshot_id"] == ENVIRONMENT_SNAPSHOT_ID


def test_execution_environment_manifest_hash_mismatch_is_explicit() -> None:
    client = _client(
        httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={
                    "schema_version": "qe_execution_environment_manifest_v1",
                    "execution_environment_snapshot_id": ENVIRONMENT_SNAPSHOT_ID,
                    "execution_environment_manifest_sha256": "0" * 64,
                    "manifest": ENVIRONMENT_MANIFEST,
                },
            )
        )
    )
    try:
        with pytest.raises(QEWorkspaceExecutionEnvironmentError) as caught:
            asyncio.run(client.get_execution_environment())
    finally:
        asyncio.run(client.close())
    assert caught.value.reason_code == "qe_workspace_execution_environment_hash_mismatch"


@pytest.mark.parametrize(
    "payload",
    [
        {"loop_id": "Loop1"},
        {
            "loop_id": "Loop2",
            "status": "accepted",
            "submission_intent_hash": INTENT_HASH,
            "request_digest": REQUEST_DIGEST,
            "receipt_status": "reserved",
            "duplicate_replay": False,
        },
        {
            "loop_id": "Loop1",
            "status": "accepted",
            "submission_intent_hash": "c" * 64,
            "request_digest": REQUEST_DIGEST,
            "receipt_status": "reserved",
            "duplicate_replay": False,
        },
    ],
)
def test_submit_loop_rejects_missing_or_mismatched_receipt(payload: dict[str, object]) -> None:
    client = _client(httpx.MockTransport(lambda _request: httpx.Response(200, json=payload)))
    try:
        with pytest.raises(QEWorkspaceSubmissionContractError):
            asyncio.run(
                client.submit_loop(
                    "qe_task",
                    1,
                    {},
                    submission_intent_hash=INTENT_HASH,
                )
            )
    finally:
        asyncio.run(client.close())


def test_inspect_loop_submission_distinguishes_not_reserved_and_reserved() -> None:
    calls = 0
    queries: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        queries.append(request.url.query.decode())
        if calls == 1:
            return httpx.Response(
                200,
                json={
                    "schema_version": "qe_submission_receipt_v1",
                    "task_id": "qe_task",
                    "loop_id": "Loop1",
                    "status": "not_reserved",
                },
            )
        return httpx.Response(
            200,
            json={
                "schema_version": "qe_submission_receipt_v1",
                "task_id": "qe_task",
                "loop_id": "Loop1",
                "status": "running",
                "submission_intent_hash": INTENT_HASH,
                "request_digest": REQUEST_DIGEST,
                "created_at": "2026-07-19T00:00:00+00:00",
                "updated_at": "2026-07-19T00:00:01+00:00",
                "pid": 123,
            },
        )

    client = _client(httpx.MockTransport(handler))
    try:
        absent = asyncio.run(client.inspect_loop_submission("qe_task", "qe_task_Loop1"))
        present = asyncio.run(
            client.inspect_loop_submission(
                "qe_task",
                "Loop1",
                submission_intent_hash=INTENT_HASH,
            )
        )
    finally:
        asyncio.run(client.close())

    assert absent.status == "not_reserved"
    assert absent.is_reserved is False
    assert present.status == "running"
    assert present.is_reserved is True
    assert present.submission_intent_hash == INTENT_HASH
    assert queries == ["", f"submission_intent_hash={INTENT_HASH}"]


def test_submission_identity_conflict_is_structured_rejection() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "detail": {
                    "reason_code": "qe_workspace_submission_identity_conflict",
                    "message": "different hash",
                }
            },
        )

    client = _client(httpx.MockTransport(handler))
    try:
        with pytest.raises(QEWorkspaceSubmissionRejected) as caught:
            asyncio.run(
                client.submit_loop(
                    "qe_task",
                    1,
                    {},
                    submission_intent_hash=INTENT_HASH,
                )
            )
    finally:
        asyncio.run(client.close())

    assert caught.value.status_code == 409
    assert caught.value.reason_code == "qe_workspace_submission_identity_conflict"


def test_transport_failure_preserves_unknown_remote_acceptance() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("response lost", request=request)

    client = _client(httpx.MockTransport(handler))
    try:
        with pytest.raises(QEWorkspaceSubmissionTransportError) as caught:
            asyncio.run(
                client.submit_loop(
                    "qe_task",
                    1,
                    {},
                    submission_intent_hash=INTENT_HASH,
                )
            )
    finally:
        asyncio.run(client.close())

    assert caught.value.reason_code == "qe_workspace_submission_transport_unknown"


def test_typed_kill_requires_exact_identity_and_validates_typed_receipt() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["payload"] = __import__("json").loads(request.content)
        return httpx.Response(
            200,
            json={
                "schema_version": "qe_kill_receipt_v1",
                "task_id": "qe_task",
                "loop_id": "Loop1",
                "command_id": COMMAND_ID,
                "kill_intent_generation": 1,
                "kill_intent_hash": KILL_INTENT_HASH,
                "expected_submission_intent_hash": INTENT_HASH,
                "expected_process_identity": PROCESS_IDENTITY,
                "expected_phase": None,
                "process_identity": PROCESS_IDENTITY,
                "status": "reconciling",
                "signal_attempt_count": 1,
                "signal_sent_at": "2026-07-21T00:00:01Z",
                "signal_sent": True,
                "process_observation": {"identity": PROCESS_IDENTITY},
                "result_observation": {"path": "qlib_results_enhanced.json", "present": False, "valid": False},
                "submission_receipt_status": "running",
                "terminal_reason": None,
                "error": None,
                "created_at": "2026-07-21T00:00:00Z",
                "updated_at": "2026-07-21T00:00:01Z",
                "completed_at": None,
            },
        )

    client = _client(httpx.MockTransport(handler))
    try:
        receipt = asyncio.run(
            client.kill_loop_typed(
                "qe_task",
                "qe_task_Loop1",
                command_id=COMMAND_ID,
                kill_intent_generation=1,
                kill_intent_hash=KILL_INTENT_HASH,
                expected_submission_intent_hash=INTENT_HASH,
                expected_process_identity=PROCESS_IDENTITY,
                expected_phase=None,
            )
        )
    finally:
        asyncio.run(client.close())

    assert receipt.status == "reconciling"
    assert receipt.process_identity == PROCESS_IDENTITY
    assert captured["path"] == "/api/v1/qe_workspace/tasks/qe_task/loops/Loop1/kill-intents"
    assert captured["payload"] == {
        "command_id": COMMAND_ID,
        "kill_intent_generation": 1,
        "kill_intent_hash": KILL_INTENT_HASH,
        "expected_submission_intent_hash": INTENT_HASH,
        "expected_process_identity": PROCESS_IDENTITY,
        "expected_phase": None,
    }


def test_typed_kill_never_downgrades_missing_identity_or_transport_error() -> None:
    client = _client(httpx.MockTransport(lambda request: (_ for _ in ()).throw(
        httpx.ReadTimeout("response lost", request=request),
    )))
    try:
        with pytest.raises(QEWorkspaceTypedKillContractError):
            asyncio.run(
                client.kill_loop_typed(
                    "qe_task",
                    "Loop1",
                    command_id=COMMAND_ID,
                    kill_intent_generation=1,
                    kill_intent_hash=KILL_INTENT_HASH,
                    expected_submission_intent_hash=INTENT_HASH,
                    expected_process_identity=None,
                    expected_phase=None,
                )
            )
        with pytest.raises(QEWorkspaceTypedKillTransportError) as caught:
            asyncio.run(
                client.kill_loop_typed(
                    "qe_task",
                    "Loop1",
                    command_id=COMMAND_ID,
                    kill_intent_generation=1,
                    kill_intent_hash=KILL_INTENT_HASH,
                    expected_submission_intent_hash=INTENT_HASH,
                    expected_process_identity=PROCESS_IDENTITY,
                    expected_phase=None,
                )
            )
    finally:
        asyncio.run(client.close())

    assert caught.value.reason_code == "qe_workspace_typed_kill_transport_unknown"
