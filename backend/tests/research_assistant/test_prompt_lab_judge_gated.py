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


class FailingOfflineJudge:
    def evaluate(
        self,
        *,
        target_prompt_key: str,
        baseline_text: str,
        candidate_text: str,
        eval_items: list[dict[str, Any]],
        source_refs: list[str],
    ) -> Mapping[str, Any]:
        raise RuntimeError("offline judge fixture failure")


def _service(repo: SpyRepository | None = None) -> ResearchAssistantService:
    service = ResearchAssistantService(repository=repo or SpyRepository())
    service.seed_catalogs()
    if isinstance(service.repository, SpyRepository):
        service.repository.write_calls.clear()
    return service


def test_prompt_lab_candidate_cannot_change_activation_without_approval() -> None:
    repo = SpyRepository()
    service = _service(repo)
    before = service.active_prompt_activation()
    service.create_trace_event(
        TraceEventCreate(
            event_type="llm_done",
            component="prompt_bundle.root.assistant",
            status="completed",
            payload_json={
                "target_prompt_key": "root.assistant",
                "judge_feedback": "Keep activation behind approval and preserve source_refs.",
            },
        )
    )
    run = service.run_prompt_lab_offline(target_prompt_key="root.assistant", optimizer="gepa")
    repo.write_calls.clear()

    try:
        service.activate_prompt_lab_candidate(run["lab_run_id"])
        raised = False
    except ValueError as exc:
        raised = True
        assert "approval_request_id" in str(exc) or "requires" in str(exc)
    assert raised

    after = service.active_prompt_activation()
    assert after["activation_id"] == before["activation_id"]
    assert after["bundle_signature"] == before["bundle_signature"]
    assert repo.get_record("prompt_lab_runs", run["lab_run_id"])["status"] == "candidate"
    assert repo.get_record("approvals", run["approval_request_id"])["status"] == "pending"
    assert all(kind != "prompt_activations" for _, kind in repo.write_calls)
    assert all(kind != "prompt_activation_events" for _, kind in repo.write_calls)


def test_prompt_lab_records_judge_degradation_with_reason_code_and_warning() -> None:
    repo = SpyRepository()
    service = _service(repo)
    service.create_trace_event(
        TraceEventCreate(
            event_type="llm_failed",
            component="prompt_bundle.root.assistant",
            status="failed",
            payload_json={
                "target_prompt_key": "root.assistant",
                "failure_mode": "source_refs missing",
            },
        )
    )

    run = service.run_prompt_lab_offline(
        target_prompt_key="root.assistant",
        optimizer="gepa",
        offline_judge=FailingOfflineJudge(),
    )

    score = run["judge_score_json"]
    assert score["status"] == "degraded"
    assert "prompt_lab_offline_judge_failed" in score["reason_codes"]
    assert score["warnings"]
    assert score["activation_changed"] is False
    assert score["offline_only"] is True
    assert run["status"] == "candidate"
