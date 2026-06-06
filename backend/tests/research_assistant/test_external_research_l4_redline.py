from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
PHASE4_FILES = [
    REPO_ROOT / "backend/services/research_assistant/external_research.py",
    REPO_ROOT / "backend/routers/external_research.py",
    REPO_ROOT / "backend/mcp/modules/external_research.py",
]


def test_external_research_phase4_does_not_wire_qe_or_high_cost_experiment_apis() -> None:
    forbidden = (
        "qe_template_run_confirmed",
        "qe_template_materialize_confirmed",
        "qe_experiment_run_confirmed",
        "high_cost_compute",
        "quantevolver",
        "custom_evo_run",
    )
    for path in PHASE4_FILES:
        text = path.read_text(encoding="utf-8")
        hits = [token for token in forbidden if token in text]
        assert not hits, f"{path} wires Phase 6/L4 or high-cost QE APIs too early: {hits!r}"


def test_external_research_candidates_mark_hypothesis_and_low_cost_intent_only() -> None:
    text = (REPO_ROOT / "backend/services/research_assistant/external_research.py").read_text(encoding="utf-8")
    assert "research_hypothesis" in text
    assert "low_cost_intent" in text
    assert '"l4_submission_allowed": False' in text
