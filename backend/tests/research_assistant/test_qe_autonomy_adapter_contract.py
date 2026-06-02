from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from backend.services.research_assistant.qe_autonomy import adapter as adapter_module
from backend.services.research_assistant.qe_autonomy.adapter import QeAutonomyAdapter
from backend.services.research_assistant.qe_autonomy.models import AutonomousEvolutionState


def test_qe_adapter_reuses_existing_scheduler_agents_evaluator_analyst_submit_and_preflight(monkeypatch) -> None:
    calls: list[object] = []

    class FakeScheduler:
        def __init__(self) -> None:
            calls.append("AutoEvolutionScheduler")

        def get_latest_loop_metrics(self, task_id: str) -> dict[str, object]:
            calls.append(("get_latest_loop_metrics", task_id))
            return {"loop_index": 1, "metrics": {"IC": 0.012}, "source_refs": ["qe_loop:1"], "as_of": "2026-06-02"}

        def _build_full_evolution_history(self, task_id: str) -> dict[str, object]:
            calls.append(("_build_full_evolution_history", task_id))
            return {"task_id": task_id}

        def get_current_loop_config(self, task_id: str) -> dict[str, object]:
            calls.append(("get_current_loop_config", task_id))
            return {"task_id": task_id, "model": "baseline"}

        async def submit_next_loop(self, task_id: str) -> str:
            calls.append(("submit_next_loop", task_id))
            return "qe-loop-submitted"

    class FakeAgents:
        def __init__(self) -> None:
            calls.append("EvolutionAgents")

        async def run_evaluator(self, metrics, historical_sota, *, evolution_history):
            calls.append(("run_evaluator", metrics, historical_sota, evolution_history))
            return {"is_sota": False, "reason": "needs improvement", "method": "three-layer-evaluator"}

        async def run_analyst(self, loop_index, config, metrics, *, analysis_context, evolution_history):
            calls.append(("run_analyst", loop_index, config, metrics, analysis_context, evolution_history))
            return SimpleNamespace(
                direction={"action_type": "low_cost_validation", "rationale": "analyst two-step", "config_delta": {"alpha": "try"}},
                report_text="analyst two-step report",
            )

    async def fake_preflight_qe_node(node_id: str) -> dict[str, object]:
        calls.append(("preflight_qe_node", node_id))
        return {"passed": True, "approval_required": False, "node_id": node_id}

    monkeypatch.setattr(adapter_module, "AutoEvolutionScheduler", FakeScheduler)
    monkeypatch.setattr(adapter_module, "EvolutionAgents", FakeAgents)
    monkeypatch.setattr("backend.services.quantevolver.node_execution.preflight_qe_node", fake_preflight_qe_node)

    adapter = QeAutonomyAdapter()
    state = AutonomousEvolutionState(
        auto_run_id="qaer-contract",
        qe_task_id="qe-task-contract",
        methodology_ref="methodology:test",
        stop_conditions={"max_no_improve_rounds": 5},
        budget={"max_loops": 5},
        started_at=datetime(2026, 6, 2, tzinfo=timezone.utc),
    )
    observation = adapter.run_or_wait_loop_n(state)
    state.record_observation(observation)
    verdict = adapter.evaluate_loop(observation, state)
    state.record_verdict(verdict)
    direction = adapter.decide_direction(verdict, state)
    proposal = adapter.generate_next_config(direction, state)
    decision = adapter.submit_or_preflight_next_loop(proposal, state)

    assert "AutoEvolutionScheduler" in calls
    assert "EvolutionAgents" in calls
    assert any(call[0] == "run_evaluator" for call in calls if isinstance(call, tuple))
    assert any(call[0] == "run_analyst" for call in calls if isinstance(call, tuple))
    assert ("submit_next_loop", "qe-task-contract") in calls
    assert ("preflight_qe_node", "qe-task-contract") in calls
    assert decision.executed is True
    assert decision.submitted_loop_id == "qe-loop-submitted"
