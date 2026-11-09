"""Node control and observation must use direct transport despite desktop proxies."""

import asyncio

import httpx
import pytest

from backend.infra.compute_node_client import ComputeNodeClient


CALLS = [
    ("get_system_metrics", (), "GET", "/system/metrics"),
    ("probe_health", (), "GET", "/health"),
    ("create_task", ({"task_name": "unit-test"},), "POST", "/scheduler/tasks"),
    ("get_task_progress", ("task-1",), "GET", "/scheduler/tasks/task-1/progress"),
    ("get_scheduler_task", ("task-1",), "GET", "/scheduler/tasks/task-1"),
    ("get_scheduler_results", ("task-1",), "GET", "/scheduler/tasks/task-1/results"),
    ("get_rdagent_loops", ("log-1",), "GET", "/tasks/log-1/loops"),
    ("pause_task", ("task-1",), "POST", "/scheduler/tasks/task-1/pause"),
    ("resume_task", ("task-1",), "POST", "/scheduler/tasks/task-1/resume"),
    ("cancel_task", ("task-1",), "POST", "/scheduler/tasks/task-1/cancel"),
    ("get_task_logs", ("task-1",), "GET", "/scheduler/tasks/task-1/logs"),
    ("stream_logs", ("task-1",), "GET", "/scheduler/tasks/task-1/logs/stream"),
    ("proxy_request", ("GET", "/readonly"), "GET", "/readonly"),
    ("list_scheduler_tasks", (), "GET", "/scheduler/tasks"),
    ("trigger_discover", (), "POST", "/scheduler/tasks/discover"),
    ("delete_workspace", ("task-1",), "DELETE", "/api/v1/qe_workspace/tasks/task-1"),
]


@pytest.mark.parametrize("method,args,verb,path", CALLS, ids=[row[0] for row in CALLS])
def test_all_node_requests_ignore_environment_proxies(monkeypatch, method, args, verb, path):
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.setenv(key, "http://proxy.invalid:7896")
    monkeypatch.setenv("NO_PROXY", "")
    requests = []

    def handler(request):
        requests.append(request)
        if method == "stream_logs":
            return httpx.Response(200, text="first\nsecond\n")
        return httpx.Response(200, json={"status": "ok"})

    def reject_proxy(*_args, **_kwargs):
        raise AssertionError("node traffic must not construct an environment proxy transport")

    # Keep the real AsyncClient and its environment/mount selection, replacing
    # only sockets. A client that inherits proxies fails before any mock request.
    monkeypatch.setattr(httpx.AsyncClient, "_init_proxy_transport", reject_proxy)
    monkeypatch.setattr(httpx.AsyncClient, "_init_transport", lambda *_a, **_kw: httpx.MockTransport(handler))

    async def run():
        client = ComputeNodeClient("http://192.168.50.215:9000")
        value = getattr(client, method)(*args)
        return [line async for line in value] if method == "stream_logs" else await value

    result = asyncio.run(run())
    if method == "probe_health":
        assert result["online"] is True
    elif method == "delete_workspace":
        assert result is True
    elif method == "stream_logs":
        assert result == ["first", "second"]
    assert [(r.method, r.url.path) for r in requests] == [(verb, path)]


@pytest.mark.parametrize("failure", [httpx.ReadTimeout(""), httpx.ConnectError("refused"), 502])
def test_real_health_failure_stays_offline_with_typed_evidence(monkeypatch, failure):
    requests = []

    def handler(request):
        requests.append(request)
        if isinstance(failure, int):
            return httpx.Response(failure)
        raise failure

    monkeypatch.setattr(httpx.AsyncClient, "_init_transport", lambda *_a, **_kw: httpx.MockTransport(handler))
    result = asyncio.run(ComputeNodeClient("http://node.invalid").probe_health())
    assert result["online"] is False
    assert result["error_type"] == ("HTTPStatusError" if isinstance(failure, int) else type(failure).__name__)
    assert result["error"]
    assert result["latency_ms"] >= 0
    assert result["http_status"] == (failure if isinstance(failure, int) else None)
    assert len(requests) == 1


def test_cancellation_is_not_converted_to_an_offline_observation(monkeypatch):
    def handler(_request):
        raise asyncio.CancelledError()

    monkeypatch.setattr(httpx.AsyncClient, "_init_transport", lambda *_a, **_kw: httpx.MockTransport(handler))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(ComputeNodeClient("http://node.invalid").probe_health())
