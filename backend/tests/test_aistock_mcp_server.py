"""Unit tests for scripts/aistock_mcp_server.py.

Coverage:
- HTTP client envelope unwrap + error propagation (3 tests)
- Each MCP tool happy path via mocked transport (8 tests)
- ``report_bug`` write path including next-id allocation, slug, dedup (4 tests)
- ``health`` smoke (1 test)
- start_validation_execution body assembly + posargs (1 test)
- module-quality client filter (1 test)

Tests do NOT spawn the MCP stdio server. They exercise the decorated tool
functions and the underlying ``ValidationCenterClient`` directly.
"""

from __future__ import annotations

import json
import os
import sys
import types
from pathlib import Path
from typing import Any

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
    """Force the MCP import path to use the lightweight test stub."""
    mcp_module = types.ModuleType("mcp")
    server_module = types.ModuleType("mcp.server")
    fastmcp_module = types.ModuleType("mcp.server.fastmcp")
    fastmcp_module.FastMCP = StubFastMCP
    mcp_module.server = server_module
    server_module.fastmcp = fastmcp_module
    sys.modules["mcp"] = mcp_module
    sys.modules["mcp.server"] = server_module
    sys.modules["mcp.server.fastmcp"] = fastmcp_module


@pytest.fixture
def mcp_module(tmp_path, monkeypatch):
    """Import the MCP server with REPO_ROOT redirected to a temp dir.

    BUG_ROOT computes as REPO_ROOT/tests/aistock_validation/bugs and is created
    on demand by report_bug, so we just point AISTOCK_REPO_ROOT at tmp_path.
    """
    monkeypatch.setenv("AISTOCK_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("AISTOCK_CANONICAL_ROOT", str(tmp_path))
    monkeypatch.setenv("AISTOCK_WORKTREE_ROOT", str(tmp_path / "worktrees"))
    monkeypatch.setenv("AISTOCK_BUG_ID_RESERVATION_ROOT", str(tmp_path / "bug-id-reservations"))
    monkeypatch.setenv("AISTOCK_VALIDATION_BASE_URL", "http://127.0.0.1/api/v1/validation")
    import importlib
    _install_stub_fastmcp()
    sys.modules.pop("scripts.aistock_mcp_server", None)
    module = importlib.import_module("scripts.aistock_mcp_server")
    expected_bug_root = (tmp_path / "tests" / "aistock_validation" / "bugs").resolve()
    assert Path(module.BUG_ROOT).resolve() == expected_bug_root, (
        f"BUG_ROOT did not redirect: {module.BUG_ROOT} vs {expected_bug_root}"
    )
    module._github_client_factory = lambda **_kwargs: types.SimpleNamespace(list_issues=lambda **_params: [])
    yield module
    sys.modules.pop("scripts.aistock_mcp_server", None)


def _envelope(data: Any) -> dict[str, Any]:
    return {"data": data}


def _mock_transport(handler):
    """Wrap a callable into an httpx MockTransport returning JSON envelopes."""
    def adapter(request: httpx.Request) -> httpx.Response:
        result = handler(request)
        if isinstance(result, httpx.Response):
            return result
        if isinstance(result, tuple):
            status, payload = result
            return httpx.Response(status, json=payload)
        return httpx.Response(200, json=_envelope(result))
    return httpx.MockTransport(adapter)


def _swap_client(mcp_module, client) -> None:
    """Replace the module-level _default_client so tools see the mock."""
    mcp_module._default_client = client  # type: ignore[attr-defined]


# --- HTTP client behavior ------------------------------------------------


def test_client_unwraps_data_envelope(mcp_module):
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json=_envelope({"ok": True}))

    transport = _mock_transport(handler)
    client = mcp_module.ValidationCenterClient(
        base_url="http://127.0.0.1/api/v1/validation",
        transport=transport,
    )
    payload = client.get("/health")
    assert payload == {"ok": True}
    assert captured["url"].endswith("/api/v1/validation/health")


def test_client_requires_refinement_for_large_success_response(mcp_module):
    payload = {"data": {"items": ["x" * 200]}}

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    client = mcp_module.ValidationCenterClient(
        base_url="http://127.0.0.1/api/v1/validation",
        transport=_mock_transport(handler),
        max_response_bytes=64,
    )
    result = client.get("/bugs")

    assert result["status"] == "requires_refinement"
    assert result["mcp_response_too_large"] is True
    assert result["mcp_response_refinement_required"] is True
    assert result["partial_payload_returned"] is False
    assert result["method"] == "GET"
    assert result["path"] == "/bugs"
    assert result["original_bytes"] > result["max_bytes"]
    assert "preview" not in result
    assert result["omitted_sections"] == ["response_payload"]
    assert result["retry_with"]["params"]["compact"] is True
    assert result["retry_with"]["params"]["page_size"] == 20


def test_client_raises_on_http_error(mcp_module):
    transport = _mock_transport(lambda req: (500, {"detail": "boom"}))
    client = mcp_module.ValidationCenterClient(
        base_url="http://127.0.0.1/api/v1/validation", transport=transport
    )
    with pytest.raises(RuntimeError, match="HTTP 500"):
        client.get("/health")


def test_client_raises_on_missing_envelope(mcp_module):
    transport = _mock_transport(lambda req: httpx.Response(200, json={"unexpected": True}))
    client = mcp_module.ValidationCenterClient(
        base_url="http://127.0.0.1/api/v1/validation", transport=transport
    )
    with pytest.raises(RuntimeError, match="unexpected envelope"):
        client.get("/health")


# --- Read tools ---------------------------------------------------------


def test_health_tool_calls_health_endpoint(mcp_module):
    captured = []
    transport = _mock_transport(
        lambda req: (captured.append(req.url.path), {"status": "ok"})[1]
    )
    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation", transport=transport
        ),
    )
    result = mcp_module.health()
    assert result == {"status": "ok"}
    assert captured[-1].endswith("/health")


def test_list_plans_returns_data(mcp_module):
    plans_payload = {"plans": [{"plan_key": "l0"}, {"plan_key": "qe_archive_backend"}]}
    transport = _mock_transport(lambda req: plans_payload)
    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation", transport=transport
        ),
    )
    result = mcp_module.list_plans()
    assert result == plans_payload


def test_get_plan_includes_plan_key_in_path(mcp_module):
    captured = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured.append(req.url.path)
        return httpx.Response(200, json=_envelope({"plan_key": "qe_archive_backend"}))

    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation",
            transport=_mock_transport(handler),
        ),
    )
    result = mcp_module.get_plan("qe_archive_backend")
    assert result["plan_key"] == "qe_archive_backend"
    assert captured[-1].endswith("/plans/qe_archive_backend")


def test_list_validation_runs_passes_filters(mcp_module):
    captured = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["query"] = dict(req.url.params)
        return httpx.Response(200, json=_envelope({"items": []}))

    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation",
            transport=_mock_transport(handler),
        ),
    )
    mcp_module.list_validation_runs(module="qe", level="L2")
    assert captured["query"]["module"] == "qe"
    assert captured["query"]["level"] == "L2"
    # None values must not be sent on the wire.
    assert "status" not in captured["query"]


def test_list_findings_maps_source_to_source_type(mcp_module):
    captured = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["query"] = dict(req.url.params)
        return httpx.Response(200, json=_envelope({"items": []}))

    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation",
            transport=_mock_transport(handler),
        ),
    )
    mcp_module.list_findings(severity="P1", source="guardrail")
    assert captured["query"]["severity"] == "P1"
    assert captured["query"]["source_type"] == "guardrail"


def test_list_bugs_returns_envelope_inner(mcp_module):
    bugs_payload = {
        "items": [{"bug_id": "BUG-001"}, {"bug_id": "BUG-002"}],
        "total": 2,
    }
    transport = _mock_transport(lambda req: bugs_payload)
    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation", transport=transport
        ),
    )
    result = mcp_module.list_bugs(status="open", severity="P1", compact=False)
    assert result == bugs_payload


def test_list_bugs_defaults_to_compact_page(mcp_module):
    captured = {}
    bugs_payload = {
        "items": [
            {
                "bug_id": "BUG-001",
                "title": "Large bug",
                "description": "x" * 1000,
                "reproduce_command": "pytest",
                "module": "qe",
                "severity": "P1",
                "status": "open",
            }
        ],
        "total": 1,
    }

    def handler(req: httpx.Request) -> httpx.Response:
        captured["query"] = dict(req.url.params)
        return httpx.Response(200, json=_envelope(bugs_payload))

    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation",
            transport=_mock_transport(handler),
        ),
    )
    result = mcp_module.list_bugs()

    assert captured["query"]["page_size"] == "20"
    assert result["compact"] is True
    assert result["items"][0]["bug_id"] == "BUG-001"
    assert "description" not in result["items"][0]
    assert "reproduce_command" not in result["items"][0]


def test_get_bug_agent_context_endpoint_path(mcp_module):
    captured = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured.append(req.url.path)
        return httpx.Response(
            200,
            json=_envelope(
                {
                    "schema_version": "aistock_validation_agent_context_v1",
                    "bug_id": "BUG-023",
                    "reproduce_command": "pytest …",
                }
            ),
        )

    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation",
            transport=_mock_transport(handler),
        ),
    )
    result = mcp_module.get_bug_agent_context("BUG-023")
    assert result["bug_id"] == "BUG-023"
    assert captured[-1].endswith("/bugs/BUG-023/agent-context")


def test_module_quality_summary_filters_modules(mcp_module):
    full_summary = {
        "modules": [
            {"module_id": "qe.archive", "score": 0.9},
            {"module_id": "paper_v2", "score": 0.7},
        ],
        "generated_at": "2026-05-10T20:30:00Z",
    }
    transport = _mock_transport(lambda req: full_summary)
    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation", transport=transport
        ),
    )
    filtered = mcp_module.get_module_quality_summary(module="paper_v2")
    assert len(filtered["modules"]) == 1
    assert filtered["modules"][0]["module_id"] == "paper_v2"
    assert filtered["filter"] == {"module": "paper_v2"}
    # No filter returns the original
    full = mcp_module.get_module_quality_summary()
    assert len(full["modules"]) == 2


# --- Action tools -------------------------------------------------------


def test_start_validation_execution_posts_minimal_body(mcp_module):
    captured = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["method"] = req.method
        captured["path"] = req.url.path
        captured["body"] = json.loads(req.content.decode("utf-8"))
        return httpx.Response(200, json=_envelope({"job_id": "exec-1"}))

    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation",
            transport=_mock_transport(handler),
        ),
    )
    result = mcp_module.start_validation_execution(plan_key="l0")
    assert result == {"job_id": "exec-1"}
    assert captured["method"] == "POST"
    assert captured["path"].endswith("/executions")
    assert captured["body"] == {"plan_key": "l0", "requested_by": "mcp_agent"}


def test_get_validation_execution_log_passes_tail(mcp_module):
    captured = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["query"] = dict(req.url.params)
        captured["path"] = req.url.path
        return httpx.Response(200, json=_envelope({"tail": "log line"}))

    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation",
            transport=_mock_transport(handler),
        ),
    )
    mcp_module.get_validation_execution_log("exec-1", tail=42)
    assert captured["query"]["tail_lines"] == "42"
    assert captured["path"].endswith("/executions/exec-1/log")


def test_get_validation_execution_log_rejects_out_of_range(mcp_module):
    with pytest.raises(ValueError, match="tail must be between"):
        mcp_module.get_validation_execution_log("exec-1", tail=0)


# --- report_bug write path ----------------------------------------------


def test_report_bug_creates_file(mcp_module):
    result = mcp_module.report_bug(
        title="MCP smoke bug",
        severity="P2",
        module="rl_execution",
        files=["backend/services/rl_execution/__init__.py"],
        reproduce_command="python -c 'import backend.services.rl_execution'",
        expected="import succeeds",
        actual="ModuleNotFoundError",
        fix_owner="claude_code",
    )
    assert result["deduplicated"] is False
    assert result["bug_id"] == "BUG-001"
    written = Path(mcp_module.REPO_ROOT) / result["path"]
    assert written.exists()
    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["bug_id"] == "BUG-001"
    assert payload["module"] == "rl_execution"
    assert payload["severity"] == "P2"
    assert payload["status"] == "open"
    assert payload["fingerprint"] == result["fingerprint"]
    allocator = json.loads(Path(mcp_module.BUG_ID_ALLOCATOR_PATH).read_text(encoding="utf-8"))
    assert allocator["schema_version"] == mcp_module.BUG_ID_ALLOCATOR_SCHEMA
    assert allocator["last_allocated"] == 1


def test_report_bug_dedupes_on_fingerprint(mcp_module):
    first = mcp_module.report_bug(
        title="Same exact issue",
        severity="P1",
        module="qe_archive",
        files=["backend/services/qe_archive/foo.py"],
        reproduce_command="pytest backend/tests/qe_archive/test_foo.py",
        expected="pass",
        actual="fail",
    )
    assert first["deduplicated"] is False
    second = mcp_module.report_bug(
        title="Same exact issue",
        severity="P1",
        module="qe_archive",
        files=["backend/services/qe_archive/foo.py"],
        reproduce_command="pytest backend/tests/qe_archive/test_foo.py",
        expected="pass",
        actual="fail",
    )
    assert second["deduplicated"] is True
    assert second["existing"]["bug_id"] == first["bug_id"]


def test_report_bug_increments_id(mcp_module):
    a = mcp_module.report_bug(
        title="alpha",
        severity="P3",
        module="alpha_mod",
        files=["foo.py"],
        reproduce_command="cmd alpha",
        expected="x",
        actual="y",
    )
    b = mcp_module.report_bug(
        title="beta",
        severity="P3",
        module="beta_mod",
        files=["bar.py"],
        reproduce_command="cmd beta",
        expected="x",
        actual="y",
    )
    assert a["bug_id"] == "BUG-001"
    assert b["bug_id"] == "BUG-002"


def test_report_bug_uses_allocator_when_it_is_ahead_of_registry(mcp_module):
    mcp_module.BUG_ROOT.mkdir(parents=True, exist_ok=True)
    mcp_module._write_bug_id_allocator(41)

    result = mcp_module.report_bug(
        title="allocator ahead",
        severity="P3",
        module="allocator_mod",
        files=["foo.py"],
        reproduce_command="cmd allocator",
        expected="x",
        actual="y",
    )

    assert result["bug_id"] == "BUG-042"


def test_report_bug_uses_registry_max_when_allocator_is_stale(mcp_module):
    mcp_module.BUG_ROOT.mkdir(parents=True, exist_ok=True)
    existing = {
        "schema_version": mcp_module.SCHEMA_VERSION,
        "bug_id": "BUG-105",
        "title": "existing high id",
        "module": "validation",
        "severity": "P2",
        "status": "open",
    }
    (Path(mcp_module.BUG_ROOT) / "20260523_BUG-105-existing-high-id.json").write_text(
        json.dumps(existing),
        encoding="utf-8",
    )
    mcp_module._write_bug_id_allocator(12)

    result = mcp_module.report_bug(
        title="registry ahead",
        severity="P3",
        module="allocator_mod",
        files=["bar.py"],
        reproduce_command="cmd registry",
        expected="x",
        actual="y",
    )

    assert result["bug_id"] == "BUG-106"
    allocator = json.loads(Path(mcp_module.BUG_ID_ALLOCATOR_PATH).read_text(encoding="utf-8"))
    assert allocator["last_allocated"] == 106


def test_report_bug_uses_worktree_registry_max_when_allocator_is_stale(mcp_module):
    mcp_module.BUG_ROOT.mkdir(parents=True, exist_ok=True)
    mcp_module._write_bug_id_allocator(12)
    worktree_bug_root = (
        Path(os.environ["AISTOCK_WORKTREE_ROOT"])
        / "other-window"
        / "tests"
        / "aistock_validation"
        / "bugs"
    )
    worktree_bug_root.mkdir(parents=True, exist_ok=True)
    (worktree_bug_root / "20260528_BUG-136-other-window.json").write_text(
        json.dumps({"schema_version": mcp_module.SCHEMA_VERSION, "bug_id": "BUG-136"}),
        encoding="utf-8",
    )

    result = mcp_module.report_bug(
        title="worktree registry ahead",
        severity="P3",
        module="allocator_mod",
        files=["bar.py"],
        reproduce_command="cmd worktree registry",
        expected="x",
        actual="y",
    )

    assert result["bug_id"] == "BUG-137"
    allocator = json.loads(Path(mcp_module.BUG_ID_ALLOCATOR_PATH).read_text(encoding="utf-8"))
    assert allocator["last_allocated"] == 137


def test_report_bug_rejects_invalid_severity(mcp_module):
    with pytest.raises(ValueError, match="severity must be one of"):
        mcp_module.report_bug(
            title="x",
            severity="HIGH",
            module="m",
            files=["f.py"],
            reproduce_command="cmd",
            expected="e",
            actual="a",
        )


# --- Security: path traversal + loopback enforcement -------------------


@pytest.mark.parametrize(
    "tool_name, kwargs",
    [
        ("get_plan", {"plan_key": "../../health"}),
        ("get_plan", {"plan_key": "qe_archive/../../health"}),
        ("get_validation_run", {"run_id": "a/b"}),
        ("get_validation_run", {"run_id": "id?force=1"}),
        ("get_bug_agent_context", {"bug_id": "BUG-001/../../bugs/summary"}),
        ("get_bug_agent_context", {"bug_id": "BUG 001"}),  # whitespace
        ("get_validation_execution_status", {"execution_id": "exec/../runs/x"}),
        ("get_validation_execution_log", {"execution_id": "x%2Fy", "tail": 10}),
    ],
)
def test_path_interpolating_tools_reject_dangerous_identifiers(mcp_module, tool_name, kwargs):
    # Even with a working client, the sanitize check must fire BEFORE the HTTP request.
    transport = _mock_transport(lambda req: {"unreachable": True})
    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation", transport=transport,
        ),
    )
    tool = getattr(mcp_module, tool_name)
    with pytest.raises(ValueError, match="contains illegal characters|must be a non-empty string"):
        tool(**kwargs)


@pytest.mark.parametrize(
    "tool_name, kwargs",
    [
        ("get_plan", {"plan_key": "qe_archive_backend"}),
        ("get_validation_run", {"run_id": "qe_20260415_173338_d1c5"}),
        ("get_bug_agent_context", {"bug_id": "BUG-023"}),
        ("get_validation_execution_status", {"execution_id": "exec-1"}),
    ],
)
def test_path_interpolating_tools_accept_canonical_ids(mcp_module, tool_name, kwargs):
    captured = []

    def handler(req):
        captured.append(req.url.path)
        return httpx.Response(200, json=_envelope({"ok": True}))

    _swap_client(
        mcp_module,
        mcp_module.ValidationCenterClient(
            base_url="http://127.0.0.1/api/v1/validation",
            transport=_mock_transport(handler),
        ),
    )
    tool = getattr(mcp_module, tool_name)
    result = tool(**kwargs)
    assert result == {"ok": True}
    # The identifier must appear verbatim (no encoding distortion) in the path
    primary = next(iter(kwargs.values()))
    assert primary in captured[-1]


def test_client_construction_rejects_non_loopback_url(mcp_module):
    with pytest.raises(ValueError, match="must be loopback"):
        mcp_module.ValidationCenterClient(
            base_url="https://example.com/api/v1/validation",
        )


def test_client_construction_accepts_loopback_variants(mcp_module):
    for url in [
        "http://127.0.0.1:8011/api/v1/validation",
        "http://localhost:8011/api/v1/validation",
        "http://[::1]:8011/api/v1/validation",
    ]:
        client = mcp_module.ValidationCenterClient(base_url=url)
        assert client.base_url.startswith(("http://127.0.0.1", "http://localhost", "http://[::1]"))


def test_report_bug_normalizes_drawer_evidence(mcp_module):
    result = mcp_module.report_bug(
        title="drawer normalization",
        severity="P3",
        module="cross_tool",
        files=[],
        reproduce_command="cmd",
        expected="e",
        actual="a",
        related_drawer="abc1234567",
    )
    written = Path(mcp_module.REPO_ROOT) / result["path"]
    payload = json.loads(written.read_text(encoding="utf-8"))
    drawer_uri = next((u for u in payload["evidence_uris"] if u.startswith("drawer:")), None)
    assert drawer_uri == "drawer:cross-tool/codex-claude-coord/abc1234567"
