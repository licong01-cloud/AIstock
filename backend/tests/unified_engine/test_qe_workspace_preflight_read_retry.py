"""Read-only metadata retry must never duplicate submission or mask bad config."""

import asyncio
from unittest.mock import AsyncMock, call

import httpx
import pytest

from backend.services.quantevolver import node_execution
from backend.services.quantevolver import qe_workspace_client as workspace


CONFIG = {
    "workspace_base": "/home/test/rdagent_state/qe_workspace",
    "factor_data_dir": "/home/test/data/factors",
    "qlib_data_path": "/home/test/data/day",
    "qlib_minute_path": "/home/test/data/minute",
    "qlib_rdagent_root": "/home/test/projects/RD-Agent-main",
}
TRANSIENTS = [httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError]


async def read_with_transport(handler):
    async with workspace.QEWorkspaceClient("http://node/api/v1/qe_workspace", node_id="node") as client:
        await client.client.aclose()
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client.client = http
            return await client.get_workspace_config()


@pytest.mark.parametrize("failure", [*TRANSIENTS, 429, 502, 503, 504])
def test_transient_config_get_recovers_without_any_submission(monkeypatch, failure):
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) < 3:
            if isinstance(failure, int):
                return httpx.Response(failure, json={"detail": "temporarily unavailable"})
            raise failure("transient", request=request)
        return httpx.Response(200, json=CONFIG)

    sleep = AsyncMock()
    monkeypatch.setattr(workspace.asyncio, "sleep", sleep)
    assert asyncio.run(read_with_transport(handler)) == CONFIG
    assert len(requests) == 3
    assert all(r.method == "GET" and r.url.path == "/api/v1/qe_workspace/config" for r in requests)
    assert sleep.await_args_list == [call(1), call(2)]


@pytest.mark.parametrize("failure", [httpx.ConnectTimeout, httpx.ReadTimeout, 503])
def test_persistent_failure_is_bounded_and_preserves_original_error(monkeypatch, failure):
    requests = []
    original = failure("persistent") if not isinstance(failure, int) else None

    def handler(request):
        requests.append(request)
        if original is not None:
            raise original
        return httpx.Response(failure, json={"detail": "unavailable"})

    monkeypatch.setattr(workspace.asyncio, "sleep", AsyncMock())
    with pytest.raises(httpx.HTTPStatusError if isinstance(failure, int) else failure) as error:
        asyncio.run(read_with_transport(handler))
    if original is not None:
        assert error.value is original
    assert len(requests) == 3


@pytest.mark.parametrize("status", [400, 401, 403, 404, 500])
def test_permanent_http_errors_are_not_retried(monkeypatch, status):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, json={"detail": "permanent"})

    sleep = AsyncMock()
    monkeypatch.setattr(workspace.asyncio, "sleep", sleep)
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(read_with_transport(handler))
    assert len(requests) == 1
    sleep.assert_not_awaited()


@pytest.mark.parametrize("payload", [None, {}, [], "invalid-json"])
def test_invalid_metadata_is_not_retried_or_filled(monkeypatch, payload):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, text="not-json") if payload == "invalid-json" else httpx.Response(200, json=payload)

    sleep = AsyncMock()
    monkeypatch.setattr(workspace.asyncio, "sleep", sleep)
    with pytest.raises(ValueError):
        asyncio.run(read_with_transport(handler))
    assert len(requests) == 1
    sleep.assert_not_awaited()


@pytest.mark.parametrize("failure", [asyncio.CancelledError, httpx.UnsupportedProtocol])
def test_cancellation_and_invalid_protocol_are_not_retried(monkeypatch, failure):
    requests = []

    def handler(request):
        requests.append(request)
        raise failure("stop")

    sleep = AsyncMock()
    monkeypatch.setattr(workspace.asyncio, "sleep", sleep)
    with pytest.raises(failure):
        asyncio.run(read_with_transport(handler))
    assert len(requests) == 1
    sleep.assert_not_awaited()


def test_real_preflight_still_validates_config_after_transient_recovery(monkeypatch):
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ConnectTimeout("transient", request=request)
        return httpx.Response(200, json=CONFIG)

    monkeypatch.setattr(workspace.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(node_execution, "get_compute_node", lambda _: {
        "node_id": "node", "status": "online", "api_base_url": "http://node",
        "workspace_base": CONFIG["workspace_base"],
    })

    async def run():
        client = workspace.QEWorkspaceClient("http://node/api/v1/qe_workspace", node_id="node")
        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        monkeypatch.setattr(node_execution.QEWorkspaceClient, "for_node", staticmethod(lambda _: client))
        return await node_execution.preflight_qe_node("node")

    value = asyncio.run(run())
    assert value["workspace_config"] == CONFIG
    assert len(requests) == 2


@pytest.mark.parametrize("unavailable", [False, True])
def test_readonly_probe_uses_real_preflight_and_returns_policy_or_typed_failure(monkeypatch, unavailable):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.routers import quantevolver_evolution as router

    error = node_execution.QENodePreflightError("QE_NODE_API_UNREACHABLE", "unavailable", {"node_id": "node"})
    preflight = AsyncMock(side_effect=error) if unavailable else AsyncMock(return_value={"node": CONFIG})
    monkeypatch.setattr(router, "preflight_qe_nodes", preflight)
    app = FastAPI()
    app.include_router(router.router, prefix="/api/v1")
    with TestClient(app) as client:
        response = client.get("/api/v1/quantevolver/evolution/workspace-preflight/node")
    preflight.assert_awaited_once_with(["node"])
    if unavailable:
        assert response.status_code == 503
        assert response.json()["detail"]["error_code"] == "QE_NODE_API_UNREACHABLE"
    else:
        assert response.status_code == 200
        assert response.json() == {
            "status": "ready", "node_id": "node",
            "workspace_config_read_policy": workspace.QE_WORKSPACE_CONFIG_READ_POLICY,
            "max_read_attempts": 3, "submission_performed": False,
        }


def test_blank_node_probe_cannot_report_false_readiness(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.routers import quantevolver_evolution as router

    preflight = AsyncMock()
    monkeypatch.setattr(router, "preflight_qe_nodes", preflight)
    app = FastAPI()
    app.include_router(router.router, prefix="/api/v1")
    with TestClient(app) as client:
        response = client.get("/api/v1/quantevolver/evolution/workspace-preflight/%20")
    assert response.status_code == 400
    preflight.assert_not_awaited()


@pytest.mark.parametrize("defect", [None, "fields", "status", "node", "policy", "attempts", "boolean", "submission", "extra"])
def test_post_restart_semantic_contract_rejects_http_only_or_wrong_metadata(defect):
    from scripts.aistock_issue_workflow import _business_smoke_semantic_contract

    contract, validate = _business_smoke_semantic_contract("/api/v1/quantevolver/evolution/workspace-preflight/node")
    assert contract == "qe_workspace_preflight"
    payload = {"status": "ready", "node_id": "node", "workspace_config_read_policy": "qe_workspace_config_get_retry_v1",
               "max_read_attempts": 3, "submission_performed": False}
    if defect == "fields":
        payload = {"status": "ok"}
    elif defect == "status":
        payload["status"] = "failed"
    elif defect == "node":
        payload["node_id"] = " "
    elif defect == "policy":
        payload["workspace_config_read_policy"] = "legacy"
    elif defect == "attempts":
        payload["max_read_attempts"] = 4
    elif defect == "boolean":
        payload["max_read_attempts"] = True
    elif defect == "submission":
        payload["submission_performed"] = True
    elif defect == "extra":
        payload["error"] = "failed"
    assert validate(payload)[0] == ("passed" if defect is None else "failed")
