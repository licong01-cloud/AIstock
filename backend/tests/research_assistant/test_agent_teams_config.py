from __future__ import annotations

from pathlib import Path


from backend.services.research_assistant.agent_teams.config import load_agent_teams_config


REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = REPO_ROOT / "configs/research_assistant/agent_teams.yaml"


def test_agent_teams_config_declares_first_wave_workers_and_tool_subsets() -> None:
    config = load_agent_teams_config(CONFIG_PATH)
    assert config.orchestrator["model_role"] == "primary_reasoner"
    assert config.max_parallel_workers == 4
    assert [worker.agent_key for worker in config.workers] == [
        "qe_experiment_designer",
        "hmm_evolution",
        "factor_developer",
        "local_data_doctor",
    ]
    for worker in config.workers:
        assert worker.model_role == "cheap_worker"
        assert worker.allowed_servers
        assert worker.allowed_tools
        assert worker.max_tool_iterations > 0
        assert "required" in worker.output_schema


def test_agent_teams_runtime_does_not_hardcode_worker_selection() -> None:
    config = load_agent_teams_config(CONFIG_PATH)
    selected = [worker.agent_key for worker in config.workers if "因子" in worker.triggers or "factor" in worker.triggers]
    assert selected == ["factor_developer"]
