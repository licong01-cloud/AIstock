from __future__ import annotations

from types import MethodType
from typing import Any

from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


def test_code_impact_graph_is_consumed_by_agent_team_decomposition(monkeypatch) -> None:
    service = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=object())
    consumed_context_refs: list[list[dict[str, Any]]] = []

    def fake_build_query_code_context(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["user_query"] == "改 backend/services/research_assistant/service.py 的 build_context_pack 后要分解影响测试"
        return {
            "schema_version": "aistock_research_assistant_code_context_refs_v1",
            "status": "ok",
            "reason_codes": ["code_context_refs_built"],
            "code_context_refs": [
                {
                    "code_ref_id": "code_ref_phase8",
                    "query_scope": "path:backend/services/research_assistant/service.py",
                    "query_scope_type": "path",
                    "source": "codegraph",
                    "summary": "build_context_pack affects RA context-pack tests.",
                    "provenance": {
                        "commit": "commit-phase8",
                        "file": "backend/services/research_assistant/service.py",
                        "symbol": "build_context_pack",
                        "generated_at": "2026-06-16T09:00:00Z",
                    },
                    "as_of": "2026-06-16T09:00:00Z",
                    "manifest_json": {
                        "affected_tests": ["backend/tests/research_assistant/test_code_intel_context_injection.py"],
                        "summary_first": True,
                    },
                    "affected_tests": ["backend/tests/research_assistant/test_code_intel_context_injection.py"],
                }
            ],
        }

    def fake_build_context_pack(self, request: Any) -> dict[str, Any]:
        return {"context_pack_id": f"ctx-{request.agent_id}", "pack_json": {"route_reason": "worker isolated"}}

    class RecordingWorkerExecutor:
        def __init__(self, service: Any, *, user_message: str) -> None:
            self.service = service
            self.user_message = user_message

        def run_worker(self, task: Any, agent: Any, context_pack: dict[str, Any], catalog_entries: list[Any]) -> Any:
            from backend.services.research_assistant.agent_teams.models import WorkerRunResult

            refs = list(task.input_json.get("code_context_refs") or [])
            consumed_context_refs.append(refs)
            return WorkerRunResult(
                agent_run_id="service_runtime_pending",
                parent_task_id=task.parent_task_id,
                agent_key=task.agent_key,
                role=task.role,
                status="succeeded",
                task_order=task.task_order,
                summary=f"{task.agent_key} consumed {len(refs)} code refs",
                evidence_refs=tuple(ref["code_ref_id"] for ref in refs),
                result_json={
                    "code_context_refs": refs,
                    "code_affected_tests": task.input_json.get("code_affected_tests"),
                },
                context_pack_id=context_pack["context_pack_id"],
            )

    monkeypatch.setattr("backend.services.research_assistant.service.build_query_code_context", fake_build_query_code_context)
    monkeypatch.setattr("backend.services.research_assistant.service._ServiceAgentWorkerExecutor", RecordingWorkerExecutor)
    monkeypatch.setattr(service, "build_context_pack", MethodType(fake_build_context_pack, service))
    monkeypatch.setattr(service, "_react_tool_catalog_entries", lambda: [])

    result = service.run_agent_team(
        parent_task_id="task-code-team",
        objective="改 backend/services/research_assistant/service.py 的 build_context_pack 后要分解影响测试",
        requested_agent_keys=["qe_experiment_designer", "factor_developer"],
    )

    assert consumed_context_refs
    assert all(refs and refs[0]["code_ref_id"] == "code_ref_phase8" for refs in consumed_context_refs)
    worker_items = result["reduce_json"]["worker_results"]
    assert all(item["result_json"]["code_context_refs"][0]["provenance"]["file"].endswith("service.py") for item in worker_items)
    assert "code_ref_phase8" in result["reduce_json"]["evidence_refs"]
    persisted = service.repository.get_record("code_context_refs", "code_ref_phase8")
    assert persisted is not None
    assert persisted["provenance_json"]["symbol"] == "build_context_pack"
