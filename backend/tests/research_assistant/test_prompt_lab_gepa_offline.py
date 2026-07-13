from __future__ import annotations

from typing import Any, Mapping

from backend.services.research_assistant.models import TraceEventCreate
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


class SpyRepository(InMemoryResearchAssistantRepository):
    def __init__(self) -> None:
        super().__init__()
        self.write_calls: list[tuple[str, str]] = []

    def create_record(self, kind: str, row: Mapping[str, Any]) -> dict[str, Any]:
        self.write_calls.append(("create_record", kind))
        return super().create_record(kind, row)

    def update_record(self, kind: str, record_id: str, updates: Mapping[str, Any]) -> dict[str, Any]:
        self.write_calls.append(("update_record", kind))
        return super().update_record(kind, record_id, updates)


class OfflineJudgeSpy:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.production_calls = 0

    def evaluate(
        self,
        *,
        target_prompt_key: str,
        baseline_text: str,
        candidate_text: str,
        eval_items: list[dict[str, Any]],
        source_refs: list[str],
    ) -> Mapping[str, Any]:
        self.calls.append(
            {
                "target_prompt_key": target_prompt_key,
                "baseline_text": baseline_text,
                "candidate_text": candidate_text,
                "eval_items": eval_items,
                "source_refs": source_refs,
            }
        )
        return {
            "judge": "offline_llm_judge_spy",
            "score": 0.91,
            "dimensions": {"semantic_intent": 0.92, "evidence_grounding": 0.9, "approval_gate": 0.93},
            "reason_codes": ["offline_llm_judge_passed"],
            "warnings": [],
        }


def _service(repo: SpyRepository | None = None) -> ResearchAssistantService:
    service = ResearchAssistantService(repository=repo or SpyRepository())
    service.seed_catalogs()
    if isinstance(service.repository, SpyRepository):
        service.repository.write_calls.clear()
    return service


def test_prompt_lab_gepa_generates_candidate_and_offline_judge_score() -> None:
    repo = SpyRepository()
    service = _service(repo)
    service.create_trace_event(
        TraceEventCreate(
            event_type="llm_done",
            component="prompt_bundle.root.assistant",
            status="failed",
            payload_json={
                "target_prompt_key": "root.assistant",
                "user_message": "请根据语义选择工具，不要固定短句路由",
                "failure_mode": "semantic intent was mapped by keywords without source evidence",
                "judge_feedback": "Require evidence grounding and semantic tool selection.",
            },
            cost_json={"offline_eval_fixture": True},
        )
    )
    repo.write_calls.clear()
    judge = OfflineJudgeSpy()

    run = service.run_prompt_lab_offline(target_prompt_key="root.assistant", optimizer="gepa", offline_judge=judge)

    assert run["status"] == "candidate"
    assert run["optimizer"] == "gepa"
    assert run["target_prompt_key"] == "root.assistant"
    assert run["approval_request_id"]
    assert run["eval_set_ref"].startswith("prompt_lab_eval_set_root.assistant_")
    assert "Prompt Lab Candidate Addendum" in run["candidate_text"]
    assert "source_refs" in run["candidate_text"]
    assert "approval gate" in run["candidate_text"].lower()
    assert judge.production_calls == 0
    assert len(judge.calls) == 1
    assert judge.calls[0]["eval_items"]
    assert judge.calls[0]["source_refs"][0].startswith("assistant_trace_events:")

    score = run["judge_score_json"]
    assert score["judge"] == "offline_llm_judge_spy"
    assert score["score"] == 0.91
    assert score["activation_changed"] is False
    assert score["offline_only"] is True
    assert score["candidate_reason_codes"] == []
    assert score["candidate_warnings"] == []

    approval = repo.get_record("approvals", run["approval_request_id"])
    assert approval is not None
    assert approval["status"] == "pending"
    assert approval["approval_type"] == "prompt_lab.activate"
    assert approval["summary"].startswith("Prompt Lab candidate for root.assistant")

    write_kinds = [kind for _, kind in repo.write_calls]
    assert "prompt_lab_runs" in write_kinds
    assert "approvals" in write_kinds
    assert "prompt_activations" not in write_kinds
    assert "prompt_activation_events" not in write_kinds
    assert "action_proposals" not in write_kinds
    assert "mcp_tool_events" not in write_kinds
