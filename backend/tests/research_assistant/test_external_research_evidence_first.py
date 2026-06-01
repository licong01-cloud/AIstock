from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import external_research as external_research_router


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(external_research_router.router, prefix="/api/v1")
    return TestClient(app)


def test_external_search_results_are_evidence_candidates_not_conclusions() -> None:
    client = _client()

    response = client.post("/api/v1/external-research/search-web", json={"query": "HMM factor decay", "limit": 5})

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary_first"] is True
    assert payload["evidence_policy"]["external_evidence_only"] is True
    assert payload["evidence_policy"]["not_final_conclusion"] is True
    assert payload["evidence_policy"]["candidate_branches"] == ["external.", "personal.topic."]
    first = payload["items"][0]
    assert first["url"].startswith("https://")
    assert first["source"]
    assert first["as_of"]
    assert first["evidence_ref"].startswith("external-evidence:")
    assert "full_text" not in first
    assert "raw_html" not in first


def test_external_fetch_extract_returns_capped_preview_and_detail_ref() -> None:
    client = _client()

    response = client.post("/api/v1/external-research/fetch-extract", json={"url": "https://example.org/research", "max_chars": 300})

    assert response.status_code == 200
    payload = response.json()
    item = payload["item"]
    assert payload["summary_first"] is True
    assert len(item["content_preview"]) <= 300
    assert item["detail_ref"]["inline"] is False
    assert "full_content" in payload["omitted_sections"]


def test_save_evidence_candidate_is_draft_only_and_branch_limited() -> None:
    client = _client()
    evidence = client.post("/api/v1/external-research/search-papers", json={"query": "factor timing", "limit": 1}).json()["items"][0]

    ok = client.post(
        "/api/v1/external-research/save-evidence-candidate",
        json={
            "evidence": evidence,
            "target_branch": "external.factor.timing",
            "hypothesis": "External paper suggests a factor timing hypothesis.",
            "low_cost_intent": "Run a cheap offline metric sanity check.",
        },
    )

    assert ok.status_code == 200
    payload = ok.json()
    assert payload["draft_only"] is True
    assert payload["candidate"]["approval_status"] == "draft"
    assert payload["candidate"]["target_branch"] == "external.factor.timing"
    assert payload["candidate"]["hypothesis"] == "External paper suggests a factor timing hypothesis."
    assert payload["candidate"]["low_cost_intent"] == "Run a cheap offline metric sanity check."
    assert payload["candidate"]["l4_submission_allowed"] is False

    rejected = client.post(
        "/api/v1/external-research/save-evidence-candidate",
        json={"evidence": evidence, "target_branch": "project.topic.factor"},
    )
    assert rejected.status_code == 422
    assert "external." in rejected.text
    assert "personal.topic." in rejected.text


def test_facade_gate_is_self_contained_and_does_not_touch_production_8001() -> None:
    client = _client()

    response = client.post("/api/v1/external-research/search-web", json={"query": "self-contained gate"})

    assert response.status_code == 200
    assert "127.0.0.1:8001" not in response.text
    assert response.json()["source"] == "external_research_provider"
