from __future__ import annotations

from types import MethodType

from backend.services.research_assistant import service as service_module
from backend.services.research_assistant.qe_autonomy.models import AutonomyBudgetUsage, AutonomyReport
from backend.services.research_assistant.react_grounding import ToolCatalogEntry
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


def test_qe_autonomy_report_is_consumed_by_qe_worker_and_reduced_by_orchestrator(monkeypatch) -> None:
    service = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=object())
    consumed_payloads: list[dict[str, object]] = []
    report = AutonomyReport(
        auto_run_id="qaer-agent-team",
        qe_task_id="qe-task-agent-team",
        status="stopped_no_improve",
        loops_completed=2,
        stop_reason="2 consecutive rounds without SOTA improvement",
        triggered_guard="no_improve",
        last_verdict_json={"is_sota": False, "source_refs": ["verdict:2"]},
        budget_usage=AutonomyBudgetUsage(2, 2.0, 20.0),
        evidence_refs=("qe_autonomy_report:qaer-agent-team", "verdict:2"),
        proposals=(),
        submit_decisions=(),
    )

    def fake_run_qe_autonomy(self, request_payload):
        consumed_payloads.append(dict(request_payload))
        return report

    class ExplodingAdapter:
        def __init__(self, *args, **kwargs) -> None:  # pragma: no cover - negative assertion
            raise AssertionError("orchestrator must not construct QeAutonomyAdapter directly")

    monkeypatch.setattr(service_module, "QeAutonomyAdapter", ExplodingAdapter)
    monkeypatch.setattr(service, "run_qe_autonomous_evolution", MethodType(fake_run_qe_autonomy, service))
    monkeypatch.setattr(
        service,
        "build_context_pack",
        lambda request: {"context_pack_id": "ctx-qe-autonomy", "pack_json": {"route_reason": "worker isolated"}},
    )
    monkeypatch.setattr(
        service,
        "_react_tool_catalog_entries",
        lambda: [ToolCatalogEntry("aistock-qe-experiment", "qe_template_validate", "approved", "low", "read_only")],
    )

    result = service.run_agent_team(
        parent_task_id="task-qe-autonomy",
        objective="run qe autonomous evolution as worker",
        requested_agent_keys=["qe_experiment_designer"],
        qe_autonomy_request={"enabled": True, "qe_task_id": "qe-task-agent-team", "stop_conditions": {"max_no_improve_rounds": 2}, "budget": {"max_loops": 2}},
    )

    assert consumed_payloads and consumed_payloads[0]["qe_task_id"] == "qe-task-agent-team"
    worker_report = result["worker_results"][0]["result_json"]["autonomy_report"]
    reduced_report = result["reduce_json"]["worker_results"][0]["result_json"]["autonomy_report"]
    assert worker_report["auto_run_id"] == "qaer-agent-team"
    assert reduced_report["triggered_guard"] == "no_improve"
    assert "qe_autonomy_report:qaer-agent-team" in result["reduce_json"]["evidence_refs"]
    assert result["trace"][0]["orchestrator_does_domain_work"] is False
    assert "thought:" not in result["assistant_text"].lower()
    assert "observation:" not in result["assistant_text"].lower()
