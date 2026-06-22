from __future__ import annotations

import copy

import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import research_assistant
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.runtime_config import load_runtime_config
from backend.services.research_assistant.service import (
    DialogueIntent,
    DialogueMode,
    ModeDecision,
    ResearchAssistantService,
)


OPERATOR_TOKEN = "unit-test-reload-token"


def _write_runtime_config(tmp_path, mutator) -> object:
    payload = copy.deepcopy(load_runtime_config().config)
    mutator(payload)
    path = tmp_path / "runtime_context.yaml"
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path


def _rewrite_runtime_config(path, mutator) -> None:
    payload = copy.deepcopy(load_runtime_config().config)
    mutator(payload)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _client(service: ResearchAssistantService) -> TestClient:
    app = FastAPI()
    app.include_router(research_assistant.router, prefix="/api/v1")
    app.dependency_overrides[research_assistant.get_research_assistant_service] = lambda: service
    return TestClient(app)


def _reload_headers() -> dict[str, str]:
    return {"X-Research-Assistant-Operator-Token": OPERATOR_TOKEN}


def _analysis_mode_decision() -> ModeDecision:
    return ModeDecision(
        mode=DialogueMode.ANALYSIS,
        intent_type=DialogueIntent.STATUS_QUERY,
        confidence=0.9,
        mode_reason="unit-test",
        requires_tool=True,
        allowed_tool_side_effect="read_only",
        requires_user_confirmation=False,
        requires_approval=False,
        visible_audit_default=True,
    )


def test_config_reload_endpoint_requires_operator_token(monkeypatch, tmp_path) -> None:
    runtime_path = _write_runtime_config(tmp_path, lambda payload: payload.update({"config_version": "reload-auth"}))
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), runtime_config_path=runtime_path)
    client = _client(svc)

    monkeypatch.delenv("AISTOCK_RA_CONFIG_RELOAD_TOKEN", raising=False)
    missing_config = client.post("/api/v1/research-assistant/config/reload", json={"actor": "pytest"})
    assert missing_config.status_code == 503
    assert missing_config.json()["detail"]["reason_code"] == "operator_token_not_configured"

    monkeypatch.setenv("AISTOCK_RA_CONFIG_RELOAD_TOKEN", OPERATOR_TOKEN)
    missing_header = client.post("/api/v1/research-assistant/config/reload", json={"actor": "pytest"})
    assert missing_header.status_code == 403
    assert missing_header.json()["detail"]["reason_code"] == "operator_token_required"


def test_config_reload_endpoint_updates_memory_snapshot_and_writes_trace_audit(monkeypatch, tmp_path) -> None:
    runtime_path = _write_runtime_config(tmp_path, lambda payload: payload.update({"config_version": "reload-before"}))
    repo = InMemoryResearchAssistantRepository()
    svc = ResearchAssistantService(repository=repo, runtime_config_path=runtime_path)
    client = _client(svc)
    old_status = svc.declarative_config_status()

    _rewrite_runtime_config(runtime_path, lambda payload: payload.update({"config_version": "reload-after"}))
    monkeypatch.setenv("AISTOCK_RA_CONFIG_RELOAD_TOKEN", OPERATOR_TOKEN)

    response = client.post(
        "/api/v1/research-assistant/config/reload",
        json={"actor": "pytest-operator"},
        headers=_reload_headers(),
    )

    assert response.status_code == 200
    data = response.json()["data"]
    new_status = data["declarative_config_status"]
    assert data["status"] == "succeeded"
    assert data["actor"] == "pytest-operator"
    assert "current worker process" in data["multi_worker_notice"]
    assert new_status["runtime_config"]["config_version"] == "reload-after"
    assert new_status["source_sha256"] != old_status["source_sha256"]
    assert new_status["counts"]["workflow_capabilities"] == new_status["workflow_capability_count"]
    assert svc.declarative_config_status()["source_sha256"] == new_status["source_sha256"]

    traces = repo.list_records("trace_events", filters={"event_type": "declarative_config_reloaded"}, limit=10)["items"]
    assert len(traces) == 1
    trace = traces[0]
    assert trace["component"] == "declarative_config"
    assert trace["status"] == "succeeded"
    payload = trace["payload_json"]
    assert payload["actor"] == "pytest-operator"
    assert payload["success"] is True
    assert payload["old_source_sha256"] == old_status["source_sha256"]
    assert payload["new_source_sha256"] == new_status["source_sha256"]
    assert payload["old_runtime_config_version"] == "reload-before"
    assert payload["new_runtime_config_version"] == "reload-after"


def test_config_reload_bad_yaml_keeps_last_good_and_audits_failure(monkeypatch, tmp_path) -> None:
    runtime_path = _write_runtime_config(tmp_path, lambda payload: payload.update({"config_version": "reload-good"}))
    repo = InMemoryResearchAssistantRepository()
    svc = ResearchAssistantService(repository=repo, runtime_config_path=runtime_path)
    client = _client(svc)
    old_status = svc.declarative_config_status()

    def corrupt(payload: dict[str, object]) -> None:
        payload["config_version"] = "reload-bad"
        planner = payload["planner"]
        assert isinstance(planner, dict)
        capabilities = planner["workflow_capabilities"]
        assert isinstance(capabilities, list)
        for capability in capabilities:
            if isinstance(capability, dict) and capability.get("capability_key") == "skill_library.reuse":
                capability["mcp_tool_refs"] = "[]"
                return
        raise AssertionError("skill_library.reuse missing from fixture")

    _rewrite_runtime_config(runtime_path, corrupt)
    monkeypatch.setenv("AISTOCK_RA_CONFIG_RELOAD_TOKEN", OPERATOR_TOKEN)

    response = client.post(
        "/api/v1/research-assistant/config/reload",
        json={"actor": "pytest-operator"},
        headers=_reload_headers(),
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["reason_code"] == "declarative_config_invalid_capability_mcp_tool_refs"
    assert detail["capability_key"] == "skill_library.reuse"
    assert detail["actual_type"] == "str"
    assert detail["last_good_source_sha256"] == old_status["source_sha256"]
    assert detail["audit_trace_id"]
    assert svc.declarative_config_status()["source_sha256"] == old_status["source_sha256"]

    traces = repo.list_records("trace_events", filters={"event_type": "declarative_config_reloaded"}, limit=10)["items"]
    assert len(traces) == 1
    trace = traces[0]
    assert trace["status"] == "failed"
    payload = trace["payload_json"]
    assert payload["actor"] == "pytest-operator"
    assert payload["success"] is False
    assert payload["old_source_sha256"] == old_status["source_sha256"]
    assert payload["new_source_sha256"] == old_status["source_sha256"]
    assert payload["error"]["reason_code"] == "declarative_config_invalid_capability_mcp_tool_refs"
    assert payload["error"]["source_path"].endswith("runtime_context.yaml")


def test_config_reload_is_not_exposed_to_agent_tool_loop() -> None:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository())

    tools, registry = svc._agentic_function_tools(_analysis_mode_decision())

    rendered = " ".join([str(item) for item in tools] + list(registry))
    assert "config_reload" not in rendered
    assert "declarative_config" not in rendered
    assert "reload" not in rendered
    assert all(mapping.get("tool_name") != "assistant_reload_declarative_config" for mapping in registry.values())
