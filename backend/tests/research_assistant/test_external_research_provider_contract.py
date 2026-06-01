from __future__ import annotations

import pytest

from backend.services.research_assistant.external_research import (
    ALLOWED_EVIDENCE_BRANCH_PREFIXES,
    ExternalEvidenceItem,
    assert_token_safe,
    build_evidence_candidate,
    evidence_summary_response,
    stable_evidence_ref,
)


def _evidence() -> dict[str, str]:
    return {
        "title": "factor timing paper",
        "summary": "summary-first evidence with provenance",
        "url": "https://example.org/paper",
        "source": "paper_search",
        "as_of": "2026-06-01",
        "evidence_ref": stable_evidence_ref("paper_search", "https://example.org/paper", "2026-06-01"),
        "provider": "fake_provider",
    }


def test_external_research_candidate_branch_whitelist_is_external_or_personal_topic_only() -> None:
    assert ALLOWED_EVIDENCE_BRANCH_PREFIXES == ("external.", "personal.topic.")

    external_candidate = build_evidence_candidate(evidence=_evidence(), target_branch="external.factor.hmm")
    personal_candidate = build_evidence_candidate(evidence=_evidence(), target_branch="personal.topic.factor_hmm")

    assert external_candidate.memory_type == "external"
    assert external_candidate.scope == "project"
    assert personal_candidate.memory_type == "analysis_note"
    assert personal_candidate.scope == "personal"
    assert external_candidate.approval_status == "draft"
    assert personal_candidate.approval_status == "draft"


def test_external_research_candidate_rejects_project_topic_and_unknown_branches() -> None:
    for branch in ["project.topic.factor", "project.module.factor", "personal.preference.response", "external."]:
        with pytest.raises(ValueError):
            build_evidence_candidate(evidence=_evidence(), target_branch=branch)


def test_external_research_candidate_carries_hypothesis_and_low_cost_metadata() -> None:
    candidate = build_evidence_candidate(
        evidence=_evidence(),
        target_branch="external.factor.hmm",
        hypothesis="HMM regime evidence may explain factor decay.",
        low_cost_intent="Run a small historical sanity check before QE loops.",
    )

    assert candidate.content_json["research_hypothesis"] == "HMM regime evidence may explain factor decay."
    assert candidate.content_json["low_cost_intent"] == "Run a small historical sanity check before QE loops."
    assert candidate.content_json["l4_submission_allowed"] is False
    assert candidate.provenance_json["as_of"] == "2026-06-01"
    memory_payload = candidate.memory_create_payload()
    assert memory_payload["approval_status"] == "draft"
    assert memory_payload["tree_path"] == "external.factor.hmm"
    assert "target_branch" not in memory_payload
    assert "hypothesis" not in memory_payload


def test_external_research_summary_is_provenance_first_and_token_safe() -> None:
    item = ExternalEvidenceItem(
        title="paper",
        summary="x" * 3000,
        url="https://example.org/paper",
        source="paper_search",
        as_of="2026-06-01",
        evidence_ref="external-evidence:test",
        provider="fake",
        result_type="paper",
    )
    payload = evidence_summary_response(domain="external_research.papers", items=[item], limit=5)

    assert payload["summary_first"] is True
    assert payload["items"][0]["source"] == "paper_search"
    assert payload["items"][0]["url"] == "https://example.org/paper"
    assert payload["items"][0]["as_of"] == "2026-06-01"
    assert payload["items"][0]["evidence_ref"] == "external-evidence:test"
    assert len(payload["items"][0]["summary"]) <= 600
    assert "full_text" in payload["omitted_sections"]
    assert payload["evidence_policy"]["not_final_conclusion"] is True
    assert_token_safe(payload)
