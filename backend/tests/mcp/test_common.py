"""Unit tests for the shared MCP platform helpers."""

from __future__ import annotations

import sys
import types

import httpx
import pytest


class StubFastMCP:
    def __init__(self, _name: str) -> None:
        self.tools: dict[str, object] = {}

    def tool(self, name: str | None = None, **_kwargs):
        def decorator(func):
            self.tools[name or func.__name__] = func
            return func

        return decorator

    def run(self, **_kwargs) -> None:
        return None


def _install_stub_fastmcp() -> None:
    try:
        from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: F401
        return
    except ImportError:
        pass

    mcp_module = types.ModuleType("mcp")
    server_module = types.ModuleType("mcp.server")
    fastmcp_module = types.ModuleType("mcp.server.fastmcp")
    fastmcp_module.FastMCP = StubFastMCP
    mcp_module.server = server_module
    server_module.fastmcp = fastmcp_module
    sys.modules.setdefault("mcp", mcp_module)
    sys.modules.setdefault("mcp.server", server_module)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_module)


_install_stub_fastmcp()

from backend.mcp.common import (  # noqa: E402
    AIstockApiClient,
    assert_loopback_url,
    confirm,
    sanitize_identifier,
)


def _mock_transport(handler):
    def adapter(request: httpx.Request) -> httpx.Response:
        result = handler(request)
        if isinstance(result, httpx.Response):
            return result
        if isinstance(result, tuple):
            status, payload = result
            return httpx.Response(status, json=payload)
        return httpx.Response(200, json=result)

    return httpx.MockTransport(adapter)


@pytest.mark.parametrize(
    "base_url",
    [
        "http://127.0.0.1:8001/api/v1",
        "http://localhost:8001/api/v1/",
        "http://[::1]:8001/api/v1",
    ],
)
def test_common_accepts_loopback_base_urls(base_url: str) -> None:
    assert assert_loopback_url(base_url).startswith("http")
    AIstockApiClient(base_url, env_name="test")


@pytest.mark.parametrize(
    "base_url",
    [
        "http://example.com/api/v1",
        "https://aistock.internal/api/v1",
        "http://192.168.1.10:8001/api/v1",
    ],
)
def test_common_rejects_non_loopback_base_urls(base_url: str) -> None:
    with pytest.raises(ValueError, match="loopback"):
        AIstockApiClient(base_url, env_name="test")


@pytest.mark.parametrize("value", ["exp_001", "qe-run.1", "MODEL_20260518-01"])
def test_sanitize_identifier_accepts_path_safe_values(value: str) -> None:
    assert sanitize_identifier(value, "experiment_id") == value


@pytest.mark.parametrize("value", ["", "../x", "a/b", "x%2Fy", "id?force=1", "white space"])
def test_sanitize_identifier_rejects_path_or_query_injection(value: str) -> None:
    with pytest.raises(ValueError):
        sanitize_identifier(value, "experiment_id")


def test_confirm_rejects_wrong_token_before_http() -> None:
    with pytest.raises(ValueError, match="confirm_run"):
        confirm("WRONG", "RESEARCH_RUN", "confirm_run")


def test_confirm_accepts_exact_token() -> None:
    assert confirm("RESEARCH_RUN", "RESEARCH_RUN", "confirm_run") is None


def test_api_client_uses_injected_transport_for_get_post_delete() -> None:
    captured: list[tuple[str, str, dict[str, str]]] = []

    def handler(request: httpx.Request):
        captured.append((request.method, request.url.path, dict(request.url.params)))
        return {"ok": True, "method": request.method}

    client = AIstockApiClient(
        "http://127.0.0.1:8001/api/v1",
        env_name="test",
        transport=_mock_transport(handler),
    )

    assert client.get("/research-pipeline", params={"status": "running", "empty": None})["method"] == "GET"
    assert client.post("/research-pipeline/exp_1/run", json_body={"stage": "qe"})["method"] == "POST"
    assert client.delete("/research-pipeline/exp_1", json_body={"confirm": "DELETE"})["method"] == "DELETE"
    assert captured == [
        ("GET", "/api/v1/research-pipeline", {"status": "running"}),
        ("POST", "/api/v1/research-pipeline/exp_1/run", {}),
        ("DELETE", "/api/v1/research-pipeline/exp_1", {}),
    ]


def test_api_client_http_error_includes_method_path_status_and_body() -> None:
    client = AIstockApiClient(
        "http://127.0.0.1:8001/api/v1",
        env_name="test",
        transport=httpx.MockTransport(lambda _: httpx.Response(503, text="backend unavailable")),
    )

    with pytest.raises(RuntimeError) as exc_info:
        client.get("/research-pipeline/exp_404")

    message = str(exc_info.value)
    assert "GET" in message
    assert "/research-pipeline/exp_404" in message
    assert "503" in message
    assert "backend unavailable" in message


def test_api_client_rejects_non_json_success_response() -> None:
    client = AIstockApiClient(
        "http://127.0.0.1:8001/api/v1",
        env_name="test",
        transport=httpx.MockTransport(lambda _: httpx.Response(200, text="not-json")),
    )

    with pytest.raises(RuntimeError, match="non-JSON"):
        client.get("/research-pipeline")


def test_api_client_unwrap_data_is_opt_in() -> None:
    transport = _mock_transport(lambda _: {"status": "success", "data": {"experiment_id": "exp_1"}})
    raw_client = AIstockApiClient("http://127.0.0.1:8001/api/v1", env_name="test", transport=transport)
    unwrap_client = AIstockApiClient(
        "http://127.0.0.1:8001/api/v1",
        env_name="test",
        unwrap_data=True,
        transport=transport,
    )

    assert raw_client.get("/research-pipeline/exp_1") == {
        "status": "success",
        "data": {"experiment_id": "exp_1"},
    }
    assert unwrap_client.get("/research-pipeline/exp_1") == {"experiment_id": "exp_1"}
