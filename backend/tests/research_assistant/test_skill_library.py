from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from backend.services.research_assistant.models import (
    ActionProposalApprovalRequest,
    ActionProposalDecisionRequest,
    ActionProposalPreflightRequest,
    TaskCreate,
    TaskEventCreate,
)
from backend.services.research_assistant.qe_autonomy.models import (
    AutonomousEvolutionRequest,
    EvolutionVerdict,
    LoopObservation,
)
from backend.services.research_assistant.qe_autonomy.runtime import (
    AutonomousEvolutionProviders,
    AutonomousEvolutionRuntime,
)
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService
from backend.services.research_assistant.skill_library import (
    RepositorySkillLibraryExperienceReplayProvider,
    SKILL_LIBRARY_REUSE_CONFIRMATION,
)


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


def _service(repo: SpyRepository | None = None) -> ResearchAssistantService:
    service = ResearchAssistantService(repository=repo or SpyRepository())
    service.seed_catalogs()
    if isinstance(service.repository, SpyRepository):
        service.repository.write_calls.clear()
    return service


def _successful_task(service: ResearchAssistantService) -> dict[str, Any]:
    task = service.create_task(
        TaskCreate(
            title="QE task improvement workflow",
            task_type="qe.task.improvement",
            input_json={"objective": "improve archived QE loop with approved recipe"},
        )
    )
    service.add_task_event(
        task["task_id"],
        TaskEventCreate(
            event_type="action_proposed",
            message="Proposed low-cost QE analysis workflow",
            payload_json={
                "capability_key": "qe.analyze_result",
                "selected_tool": {"server_key": "aistock-qe-experiment", "tool_name": "qe_archive_query_run_leaderboard"},
                "prompt_key": "root.assistant",
            },
            evidence_refs=["qe_archive:leaderboard"],
        ),
    )
    service.add_task_event(
        task["task_id"],
        TaskEventCreate(
            event_type="skill_done",
            message="QE diagnostics skill completed with evidence",
            payload_json={"capability_key": "qe.analyze_result", "status": "completed"},
            evidence_refs=["skill_event:qe-diagnostics"],
        ),
    )
    return service.repository.get_record("tasks", task["task_id"]) or task


def test_successful_workflow_deposits_draft_and_requires_approval_before_reuse() -> None:
    repo = SpyRepository()
    service = _service(repo)
    task = _successful_task(service)
    repo.write_calls.clear()

    skill = service.deposit_successful_workflow_skill(
        task_id=task["task_id"],
        skill_key="qe.task.improvement",
        description="Reuse a proven QE analysis workflow",
    )

    assert skill["status"] == "draft"
    assert skill["success_count"] == 1
    assert skill["recipe_json"]["schema_version"] == "aistock_research_assistant_skill_recipe_v1"
    assert skill["recipe_json"]["risk_gate"]["action_proposal_required"] is True
    assert skill["recipe_json"]["risk_gate"]["direct_execution_allowed"] is False
    assert "research_agent_tasks:" + task["task_id"] in skill["recipe_json"]["source_refs"]
    assert skill["provenance_json"]["approval_request_id"] == skill["approval_request_id"]
    assert repo.get_record("approvals", skill["approval_request_id"])["status"] == "pending"

    blocked = service.propose_skill_reuse(task_id=task["task_id"], skill_id=skill["skill_id"])
    assert blocked["status"] == "blocked"
    assert blocked["action_proposal"] is None
    assert "skill_library_reuse_requires_approved_skill" in blocked["reason_codes"]
    assert not repo.list_records("action_proposals", limit=10)["items"]
    assert ("create_record", "skill_library") in repo.write_calls
    assert ("create_record", "approvals") in repo.write_calls


def test_approved_skill_reuse_goes_through_action_proposal_preflight_and_approval_gate() -> None:
    repo = SpyRepository()
    service = _service(repo)
    task = _successful_task(service)
    skill = service.deposit_successful_workflow_skill(task_id=task["task_id"], skill_key="qe.task.improvement")
    approved_skill = service.approve_skill_library_entry(
        skill["skill_id"],
        approval_id=skill["approval_request_id"],
        confirmation_text="APPROVE SKILL qe.task.improvement",
    )
    repo.write_calls.clear()

    reuse = service.propose_skill_reuse(
        task_id=task["task_id"],
        skill_id=approved_skill["skill_id"],
        input_json={"target": "similar qe task"},
    )
    proposal = reuse["action_proposal"]
    assert reuse["status"] == "proposal_created"
    assert proposal["proposal_type"] == "skill"
    assert proposal["status"] == "proposed"
    assert proposal["risk_level"] == "high"
    assert proposal["side_effect_level"] == "write_nonprod"

    service.confirm_action_proposal(
        proposal["action_proposal_id"],
        ActionProposalDecisionRequest(confirmation_text=SKILL_LIBRARY_REUSE_CONFIRMATION),
    )
    preflight = service.preflight_action_proposal(
        proposal["action_proposal_id"],
        ActionProposalPreflightRequest(),
    )
    assert preflight["proposal"]["status"] == "approval_required"
    assert preflight["preflight"]["approval_required"] is True
    assert preflight["preflight"]["preflight_checks"] == ["capability_status", "skill_registry"]
    assert preflight["preflight"]["failed_checks"] == []

    approved = service.approve_action_proposal(
        proposal["action_proposal_id"],
        ActionProposalApprovalRequest(confirmation_text=SKILL_LIBRARY_REUSE_CONFIRMATION),
    )
    assert approved["proposal"]["status"] == "approved"
    assert approved["approval"]["approval_type"] == "action_proposal.execute"
    assert approved["approval"]["approval_context_json"]["action_proposal_id"] == proposal["action_proposal_id"]
    assert ("create_record", "approvals") in repo.write_calls
    assert ("create_record", "mcp_tool_events") not in repo.write_calls


class MemoryRunStore:
    def __init__(self) -> None:
        self.archived: dict[str, dict[str, Any]] = {}

    def create_run(self, state: Any) -> None:
        return None

    def update_run(self, state: Any) -> None:
        return None

    def get_run(self, auto_run_id: str) -> dict[str, object] | None:
        return None

    def archive_report(self, state: Any, report: dict[str, object]) -> None:
        self.archived[state.auto_run_id] = report


class OneShotLoopExecutor:
    def run_or_wait_loop_n(self, state: Any) -> LoopObservation:
        return LoopObservation(
            loop_index=1,
            metrics={"rank_ic": 0.04},
            source_refs=("qe_archive:loop-1",),
            as_of="2026-06-17T00:00:00+00:00",
        )


class StopAfterFirstEvaluator:
    def evaluate_loop(self, observation: LoopObservation, state: Any) -> EvolutionVerdict:
        return EvolutionVerdict(
            is_sota=False,
            reason="no improvement",
            method="deterministic_test",
            metrics=observation.metrics,
            source_refs=("verdict:loop-1",),
            as_of="2026-06-17T00:00:00+00:00",
        )


class UnusedProvider:
    def decide_direction(self, verdict: EvolutionVerdict, state: Any) -> Any:
        raise AssertionError("stop condition should prevent next-loop direction")

    def generate_next_config(self, direction: Any, state: Any) -> Any:
        raise AssertionError("stop condition should prevent config generation")

    def submit_or_preflight_next_loop(self, proposal: Any, state: Any) -> Any:
        raise AssertionError("stop condition should prevent submission")


def test_approved_skill_is_available_to_l4_curriculum_experience_replay() -> None:
    repo = SpyRepository()
    service = _service(repo)
    task = _successful_task(service)
    skill = service.deposit_successful_workflow_skill(task_id=task["task_id"], skill_key="qe.task.improvement")
    service.approve_skill_library_entry(
        skill["skill_id"],
        approval_id=skill["approval_request_id"],
        confirmation_text="APPROVE SKILL qe.task.improvement",
    )
    repo.write_calls.clear()

    replay = service.search_skill_library_for_curriculum(query="qe.task.improvement", limit=5)
    assert replay["status"] == "ready"
    assert replay["items"][0]["skill_key"] == "qe.task.improvement"
    assert replay["items"][0]["recipe_ref"]["risk_gate"]["action_proposal_required"] is True

    store = MemoryRunStore()
    unused = UnusedProvider()
    runtime = AutonomousEvolutionRuntime(
        providers=AutonomousEvolutionProviders(
            run_store=store,
            loop_executor=OneShotLoopExecutor(),
            evaluator=StopAfterFirstEvaluator(),
            direction_decider=unused,
            config_generator=unused,
            submitter=unused,
        ),
        experience_replay_provider=RepositorySkillLibraryExperienceReplayProvider(repo),
        clock=lambda: datetime(2026, 6, 17, tzinfo=timezone.utc),
        id_factory=lambda prefix, stable_key: f"{prefix}_{stable_key}",
    )
    report = runtime.autonomous_evolve(
        AutonomousEvolutionRequest(
            enabled=True,
            qe_task_id="qe.task.improvement",
            methodology_ref="methodology:test",
            stop_conditions={"max_no_improve_rounds": 1},
            budget={"max_loops": 5, "max_total_seconds": 999},
        )
    )

    assert report.curriculum_replay
    assert report.curriculum_replay[0]["skill_key"] == "qe.task.improvement"
    assert report.curriculum_replay[0]["status"] == "approved"
    assert report.curriculum_replay[0]["source_refs"]
    assert store.archived[report.auto_run_id]["curriculum_replay"][0]["skill_key"] == "qe.task.improvement"
    assert all(kind not in {"action_proposals", "mcp_tool_events"} for _, kind in repo.write_calls)
