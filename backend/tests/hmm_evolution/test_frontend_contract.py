from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
HMM_CLIENT_LIB_ROOT = REPO_ROOT / "frontend/src/lib/hmm-evolution"
FORMAL_ROOTS = (
    REPO_ROOT / "frontend/src/app/hmm-evolution",
    REPO_ROOT / "frontend/src/components/hmm-evolution",
    REPO_ROOT / "frontend/src/components/hmm-research",
    REPO_ROOT / "frontend/src/lib/hmm-evolution",
    REPO_ROOT / "frontend/src/lib/hmm-research",
)


def _formal_sources() -> dict[Path, str]:
    output: dict[Path, str] = {}
    for root in FORMAL_ROOTS:
        for path in root.rglob("*"):
            if path.suffix in {".ts", ".tsx", ".css"}:
                output[path] = path.read_text(encoding="utf-8")
    return output


def test_hmm_ui_has_no_paper_v2_drawer_or_raw_payload_renderer() -> None:
    sources = _formal_sources()
    combined = "\n".join(sources.values())
    assert "components/paper-v2" not in combined
    assert "paper-v2.css" not in combined
    assert "pv2-" not in combined
    assert "<Drawer" not in combined
    assert "<pre" not in combined
    component_sources = "\n".join(text for path, text in sources.items() if HMM_CLIENT_LIB_ROOT not in path.parents)
    assert "JSON.stringify(" not in component_sources


def test_phase_one_registers_only_real_evolution_routes() -> None:
    sources = _formal_sources()
    combined = "\n".join(sources.values())
    assert "/hmm-evolution" in combined
    assert "/hmm-risk" not in combined
    assert "/hmm-research-training" not in combined
    assert not (REPO_ROOT / "frontend/src/app/hmm-risk").exists()
    assert not (REPO_ROOT / "frontend/src/app/hmm-research-training").exists()


def test_hmm_workspace_uses_full_width_shell_instead_of_legacy_sidebar() -> None:
    sidebar = (REPO_ROOT / "frontend/src/app/Sidebar.tsx").read_text(encoding="utf-8")
    assert 'pathname.startsWith("/hmm-evolution")' in sidebar


def test_ui_contains_required_real_states_and_fixed_evidence() -> None:
    sources = _formal_sources()
    combined = "\n".join(sources.values())
    for required in (
        "VisibleErrorState",
        "EvidencePanel",
        "候选排行榜",
        "Top-3 研究推荐",
        "QE 资产浏览",
        "固定证据区",
        "partial_failed",
        "timed_out",
        "evidence_quality",
    ):
        assert required in combined


def test_mutating_requests_reuse_session_scoped_idempotency_intents() -> None:
    api_source = (HMM_CLIENT_LIB_ROOT / "api.ts").read_text(encoding="utf-8")
    assert "idempotencyKeyForIntent" in api_source
    assert "window.sessionStorage.getItem" in api_source
    assert 'headers: { "Idempotency-Key": idempotencyKey }' in api_source
    assert 'headers: { "Idempotency-Key": crypto.randomUUID() }' not in api_source
