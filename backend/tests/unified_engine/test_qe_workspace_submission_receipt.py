from __future__ import annotations

import asyncio

import httpx
import pytest

from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceSubmissionContractError,
    QEWorkspaceSubmissionRejected,
    QEWorkspaceSubmissionTransportError,
)


INTENT_HASH = "a" * 64
REQUEST_DIGEST = "b" * 64


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
