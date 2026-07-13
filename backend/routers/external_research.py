"""Summary-first external research facade for MCP access."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.services.research_assistant.external_research import (
    ExtractedEvidence,
    ExternalEvidenceItem,
    ExternalResearchProvider,
    build_evidence_candidate,
    candidate_response,
    clamp_chars,
    clamp_limit,
    evidence_summary_response,
    extract_response,
    stable_evidence_ref,
    utc_today,
)

router = APIRouter(prefix="/external-research", tags=["external-research"])


class SearchWebRequest(BaseModel):
    query: str = Field(min_length=1)
    locale: str = "zh-CN"
    limit: int | None = Field(default=10, ge=1)


class SearchPapersRequest(BaseModel):
    query: str = Field(min_length=1)
    provider: str | None = None
    limit: int | None = Field(default=10, ge=1)


class FetchExtractRequest(BaseModel):
    url: str = Field(min_length=1)
    max_chars: int | None = Field(default=2000, ge=200)


class SaveEvidenceRequest(BaseModel):
    evidence: dict[str, Any]
    target_branch: str = Field(min_length=1)
    topic_key: str | None = None
    hypothesis: str | None = None
    low_cost_intent: str | None = None


class DeterministicExternalResearchProvider:
    """Offline provider used by default and by gates; it never calls the network."""

    provider_key = "deterministic_offline_external_research"

    def search_web(self, query: str, *, locale: str = "zh-CN", limit: int = 10) -> list[ExternalEvidenceItem]:
        safe_limit = clamp_limit(limit)
        today = utc_today()
        items = [
            ExternalEvidenceItem(
                title=f"{query} external evidence overview",
                summary=f"Summary-first web evidence for {query}; treat as evidence candidate, not a final conclusion.",
                url=f"https://example.org/research/{stable_evidence_ref(query, locale, 'web')[-8:]}",
                source="example_web_index",
                as_of=today,
                evidence_ref=stable_evidence_ref(query, locale, "web", today),
                provider=self.provider_key,
                result_type="web",
                detail_ref={
                    "server": "aistock-external-research",
                    "tool": "external_research_fetch_extract",
                    "args_hint": {"url": "<url>", "max_chars": 2000},
                },
            )
        ]
        return items[:safe_limit]

    def search_papers(self, query: str, *, provider: str | None = None, limit: int = 10) -> list[ExternalEvidenceItem]:
        safe_limit = clamp_limit(limit)
        selected = provider or "paper_search"
        today = utc_today()
        items = [
            ExternalEvidenceItem(
                title=f"{query} academic paper candidate",
                summary=f"Paper-search evidence about {query}; use it to form a hypothesis before low-cost validation.",
                url=f"https://papers.example.org/{stable_evidence_ref(query, selected, 'paper')[-8:]}",
                source=selected,
                as_of=today,
                evidence_ref=stable_evidence_ref(query, selected, "paper", today),
                provider=self.provider_key,
                result_type="paper",
                published_at=today,
                authors=("AIstock Test Author",),
                detail_ref={
                    "server": "aistock-external-research",
                    "tool": "external_research_fetch_extract",
                    "args_hint": {"url": "<paper_url>", "max_chars": 2000},
                },
            )
        ]
        return items[:safe_limit]

    def fetch_extract(self, url: str, *, max_chars: int = 2000) -> ExtractedEvidence:
        safe_chars = clamp_chars(max_chars)
        today = utc_today()
        preview = (
            "Extracted evidence preview. This is capped and summary-first; the full document remains behind detail_ref. "
            f"URL={url}"
        )[:safe_chars]
        return ExtractedEvidence(
            title=f"Extracted evidence for {url}",
            url=url,
            source="offline_extract_provider",
            as_of=today,
            evidence_ref=stable_evidence_ref(url, "extract", today),
            provider=self.provider_key,
            extract_summary="Capped extract for external evidence candidate creation.",
            content_preview=preview,
            detail_ref={"kind": "external_extract_ref", "uri": url, "max_chars": safe_chars, "inline": False},
        )


_provider: ExternalResearchProvider = DeterministicExternalResearchProvider()


def set_external_research_provider(provider: ExternalResearchProvider) -> None:
    global _provider
    _provider = provider


def get_external_research_provider() -> ExternalResearchProvider:
    return _provider


def _empty_result_extra(
    provider: ExternalResearchProvider,
    *,
    query: str,
    operation: str,
    provider_name: str | None = None,
) -> dict[str, Any]:
    last_failure = getattr(provider, "last_failure", None)
    if not callable(last_failure):
        return {}
    try:
        failure = last_failure(operation)
    except TypeError:
        failure = last_failure()
    if not isinstance(failure, dict) or not failure.get("reason_code"):
        return {}
    extra: dict[str, Any] = {
        "reason_codes": [str(failure["reason_code"])],
        "status": "no_results",
        "warnings": [
            {
                "reason_code": str(failure["reason_code"]),
                "message": "External research provider returned no evidence; no fabricated fallback was used.",
                "query": query,
            }
        ],
    }
    if provider_name:
        extra["provider"] = provider_name
    return extra


@router.post("/search-web")
def search_web(request: SearchWebRequest) -> dict[str, Any]:
    provider = get_external_research_provider()
    items = provider.search_web(request.query, locale=request.locale, limit=clamp_limit(request.limit))
    extra = {"query": request.query, "locale": request.locale}
    if not items:
        extra.update(_empty_result_extra(provider, query=request.query, operation="search_web"))
    return evidence_summary_response(
        domain="external_research.web",
        items=items,
        limit=request.limit,
        detail_tool="aistock-external-research/external_research_fetch_extract",
        extra=extra,
    )


@router.post("/search-papers")
def search_papers(request: SearchPapersRequest) -> dict[str, Any]:
    provider = get_external_research_provider()
    items = provider.search_papers(request.query, provider=request.provider, limit=clamp_limit(request.limit))
    provider_name = request.provider or "paper_search"
    extra = {"query": request.query, "provider": provider_name}
    if not items:
        extra.update(
            _empty_result_extra(provider, query=request.query, operation="search_papers", provider_name=provider_name)
        )
    return evidence_summary_response(
        domain="external_research.papers",
        items=items,
        limit=request.limit,
        detail_tool="aistock-external-research/external_research_fetch_extract",
        extra=extra,
    )


@router.post("/fetch-extract")
def fetch_extract(request: FetchExtractRequest) -> dict[str, Any]:
    provider = get_external_research_provider()
    extract = provider.fetch_extract(request.url, max_chars=clamp_chars(request.max_chars))
    return extract_response(extract, max_preview_chars=request.max_chars)


@router.post("/save-evidence-candidate")
def save_evidence_candidate(request: SaveEvidenceRequest) -> dict[str, Any]:
    try:
        candidate = build_evidence_candidate(
            evidence=request.evidence,
            target_branch=request.target_branch,
            topic_key=request.topic_key,
            hypothesis=request.hypothesis,
            low_cost_intent=request.low_cost_intent,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail={"error": "invalid_external_evidence_candidate", "message": str(exc)}) from exc
    return candidate_response(candidate)
