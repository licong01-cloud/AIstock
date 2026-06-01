"""Provider-only external research core for Research Assistant.

The core owns evidence contracts and draft-candidate shaping. Search vendors,
HTTP clients, database writes, and AIstock-specific routing stay in adapters.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Protocol


SERVER_KEY = "aistock-external-research"
TOOL_NAMES = (
    "external_research_search_web",
    "external_research_search_papers",
    "external_research_fetch_extract",
    "external_research_save_evidence",
)
ALLOWED_EVIDENCE_BRANCH_PREFIXES = ("external.", "personal.topic.")
SUMMARY_OMITTED_SECTIONS = (
    "raw_html",
    "raw_pdf",
    "full_text",
    "full_content",
    "large_payload",
    "provider_raw_response",
)
_IDENTIFIER_RE = re.compile(r"[^A-Za-z0-9_.:-]+")


class ExternalResearchProvider(Protocol):
    def search_web(self, query: str, *, locale: str = "zh-CN", limit: int = 10) -> list["ExternalEvidenceItem"]:
        ...

    def search_papers(self, query: str, *, provider: str | None = None, limit: int = 10) -> list["ExternalEvidenceItem"]:
        ...

    def fetch_extract(self, url: str, *, max_chars: int = 2000) -> "ExtractedEvidence":
        ...


@dataclass(frozen=True)
class ExternalEvidenceItem:
    title: str
    summary: str
    url: str
    source: str
    as_of: str
    evidence_ref: str
    provider: str
    result_type: str = "web"
    published_at: str | None = None
    authors: tuple[str, ...] = ()
    artifact_refs: tuple[dict[str, Any], ...] = ()
    detail_ref: dict[str, Any] | None = None

    def compact(self, *, max_summary_chars: int = 600) -> dict[str, Any]:
        summary = self.summary[:max_summary_chars].strip()
        payload = {
            "title": self.title,
            "summary": summary,
            "url": self.url,
            "source": self.source,
            "as_of": self.as_of,
            "evidence_ref": self.evidence_ref,
            "provider": self.provider,
            "result_type": self.result_type,
        }
        if self.published_at:
            payload["published_at"] = self.published_at
        if self.authors:
            payload["authors"] = list(self.authors[:8])
        if self.artifact_refs:
            payload["artifact_refs"] = [dict(item) for item in self.artifact_refs[:4]]
        if self.detail_ref:
            payload["detail_ref"] = dict(self.detail_ref)
        return payload


@dataclass(frozen=True)
class ExtractedEvidence:
    title: str
    url: str
    source: str
    as_of: str
    evidence_ref: str
    provider: str
    extract_summary: str
    content_preview: str
    detail_ref: dict[str, Any]
    artifact_refs: tuple[dict[str, Any], ...] = ()

    def compact(self, *, max_preview_chars: int = 1200) -> dict[str, Any]:
        return {
            "title": self.title,
            "url": self.url,
            "source": self.source,
            "as_of": self.as_of,
            "evidence_ref": self.evidence_ref,
            "provider": self.provider,
            "extract_summary": self.extract_summary[:600].strip(),
            "content_preview": self.content_preview[:max_preview_chars].strip(),
            "detail_ref": dict(self.detail_ref),
            "artifact_refs": [dict(item) for item in self.artifact_refs[:4]],
        }


@dataclass(frozen=True)
class DraftEvidenceCandidate:
    target_branch: str
    memory_type: str
    subject_key: str
    title: str
    content_text: str
    content_json: dict[str, Any]
    provenance_json: dict[str, Any]
    evidence_refs: tuple[str, ...]
    scope: str
    approval_status: str = "draft"
    risk_level: str = "medium"
    node_type: str = "fact"
    trust_level: str = "external_unverified"
    auto_created: bool = True
    hypothesis: str = ""
    low_cost_intent: str = ""

    def memory_create_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence_refs"] = list(self.evidence_refs)
        payload["tree_path"] = self.target_branch
        payload["parent_key"] = parent_branch(self.target_branch)
        payload["resident"] = False
        payload["confidence"] = 0.7
        payload["source_type"] = "external_research"
        payload["source_ref"] = self.evidence_refs[0] if self.evidence_refs else None
        payload["created_by"] = "external_research_save_evidence"
        for key in ("target_branch", "hypothesis", "low_cost_intent"):
            payload.pop(key, None)
        return payload


def utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def stable_evidence_ref(*parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"external-evidence:{digest}"


def clamp_limit(limit: int | None, *, default: int = 10, max_limit: int = 20) -> int:
    try:
        parsed = int(limit if limit is not None else default)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, 1), max_limit)


def clamp_chars(value: int | None, *, default: int = 2000, max_chars: int = 4000) -> int:
    try:
        parsed = int(value if value is not None else default)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, 200), max_chars)


def validate_evidence_branch(branch: str) -> str:
    normalized = str(branch or "").strip()
    if not normalized:
        raise ValueError("target_branch is required")
    if not normalized.startswith(ALLOWED_EVIDENCE_BRANCH_PREFIXES):
        allowed = ", ".join(ALLOWED_EVIDENCE_BRANCH_PREFIXES)
        raise ValueError(f"external evidence candidates must target {allowed}; got {normalized!r}")
    if normalized in {"external.", "personal.topic."}:
        raise ValueError("target_branch must include a leaf segment")
    return normalized


def parent_branch(branch: str) -> str | None:
    if "." not in branch:
        return None
    return branch.rsplit(".", 1)[0] or None


def sanitize_topic_key(value: str | None) -> str:
    raw = str(value or "research").strip().lower()
    safe = _IDENTIFIER_RE.sub("_", raw).strip("._-:")
    return safe or "research"


def ensure_evidence_item(payload: dict[str, Any]) -> dict[str, Any]:
    required = ("source", "url", "as_of", "evidence_ref")
    missing = [key for key in required if not payload.get(key)]
    if missing:
        raise ValueError(f"external evidence is missing required provenance fields: {missing}")
    return dict(payload)


def normalize_items(items: list[ExternalEvidenceItem], *, limit: int | None = None) -> list[ExternalEvidenceItem]:
    safe_limit = clamp_limit(limit, default=len(items) or 10)
    dedup: dict[tuple[str, str], ExternalEvidenceItem] = {}
    for item in items:
        ensure_evidence_item(item.compact())
        dedup[(item.source, item.url)] = item
    return sorted(dedup.values(), key=lambda item: (item.source, item.url, item.title, item.evidence_ref))[:safe_limit]


def evidence_summary_response(
    *,
    domain: str,
    items: list[ExternalEvidenceItem],
    limit: int | None = None,
    detail_tool: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    safe_limit = clamp_limit(limit)
    normalized = normalize_items(items, limit=safe_limit)
    payload: dict[str, Any] = {
        "ok": True,
        "domain": domain,
        "summary_first": True,
        "items": [item.compact() for item in normalized],
        "total": len(normalized),
        "pagination": {"limit": safe_limit, "offset": 0, "next_offset": len(normalized), "has_more": False},
        "omitted_sections": list(SUMMARY_OMITTED_SECTIONS),
        "evidence_policy": {
            "external_evidence_only": True,
            "not_final_conclusion": True,
            "candidate_branches": list(ALLOWED_EVIDENCE_BRANCH_PREFIXES),
            "l4_handoff": "hypothesis_then_low_cost_validation_only",
        },
        "source": "external_research_provider",
    }
    if detail_tool:
        payload["detail_tool"] = detail_tool
    if extra:
        payload.update(extra)
    assert_token_safe(payload)
    return payload


def extract_response(extract: ExtractedEvidence, *, max_preview_chars: int | None = None) -> dict[str, Any]:
    safe_chars = clamp_chars(max_preview_chars, default=1200, max_chars=4000)
    payload = {
        "ok": True,
        "domain": "external_research.extract",
        "summary_first": True,
        "item": extract.compact(max_preview_chars=safe_chars),
        "omitted_sections": list(SUMMARY_OMITTED_SECTIONS),
        "detail_tool": f"{SERVER_KEY}/external_research_fetch_extract",
        "source": "external_research_provider",
    }
    assert_token_safe(payload)
    return payload


def build_evidence_candidate(
    *,
    evidence: dict[str, Any],
    target_branch: str,
    topic_key: str | None = None,
    hypothesis: str | None = None,
    low_cost_intent: str | None = None,
) -> DraftEvidenceCandidate:
    branch = validate_evidence_branch(target_branch)
    item = ensure_evidence_item(evidence)
    topic = sanitize_topic_key(topic_key or item.get("title") or item["source"])
    memory_type = "external" if branch.startswith("external.") else "analysis_note"
    scope = "personal" if branch.startswith("personal.") else "project"
    derived_hypothesis = str(hypothesis or item.get("hypothesis") or f"Investigate evidence from {item['source']}.").strip()
    derived_low_cost = str(low_cost_intent or item.get("low_cost_intent") or "Run a low-cost literature/data sanity check before any expensive experiment.").strip()
    content_json = {
        "external_evidence": item,
        "research_hypothesis": derived_hypothesis,
        "low_cost_intent": derived_low_cost,
        "evidence_first": True,
        "direct_conclusion_allowed": False,
        "l4_submission_allowed": False,
    }
    return DraftEvidenceCandidate(
        target_branch=branch,
        memory_type=memory_type,
        subject_key=f"{branch}.{topic}" if branch.endswith(".") else branch,
        title=str(item.get("title") or f"External evidence: {item['source']}")[:160],
        content_text=str(item.get("summary") or item.get("extract_summary") or item.get("title") or "")[:1200],
        content_json=content_json,
        provenance_json={
            "source": item["source"],
            "url": item["url"],
            "as_of": item["as_of"],
            "evidence_ref": item["evidence_ref"],
            "provider": item.get("provider"),
        },
        evidence_refs=(str(item["evidence_ref"]),),
        scope=scope,
        hypothesis=derived_hypothesis,
        low_cost_intent=derived_low_cost,
    )


def candidate_response(candidate: DraftEvidenceCandidate) -> dict[str, Any]:
    payload = {
        "ok": True,
        "domain": "external_research.evidence_candidate",
        "summary_first": True,
        "draft_only": True,
        "candidate": {
            "target_branch": candidate.target_branch,
            "memory_type": candidate.memory_type,
            "subject_key": candidate.subject_key,
            "title": candidate.title,
            "approval_status": candidate.approval_status,
            "risk_level": candidate.risk_level,
            "evidence_refs": list(candidate.evidence_refs),
            "hypothesis": candidate.hypothesis,
            "low_cost_intent": candidate.low_cost_intent,
            "l4_submission_allowed": False,
        },
        "omitted_sections": list(SUMMARY_OMITTED_SECTIONS),
        "source": "external_research_save_evidence",
    }
    assert_token_safe(payload)
    return payload


def assert_token_safe(payload: dict[str, Any]) -> None:
    blocked = {item.lower() for item in SUMMARY_OMITTED_SECTIONS}

    def walk(value: Any, path: str = "") -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                key_text = str(key)
                if key_text.lower() in blocked:
                    raise ValueError(f"external research payload contains forbidden heavy field {path}{key_text}")
                walk(item, f"{path}{key_text}.")
        elif isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{path}{index}.")
        elif isinstance(value, (str, bytes)) and len(value) > 5000:
            raise ValueError(f"external research payload field {path} exceeds summary budget")

    walk(payload)


def item_from_provider_payload(raw: dict[str, Any], *, provider: str, result_type: str, today: str | None = None) -> ExternalEvidenceItem:
    as_of = str(raw.get("as_of") or today or utc_today())
    url = str(raw.get("url") or raw.get("link") or "").strip()
    source = str(raw.get("source") or provider).strip()
    title = str(raw.get("title") or url or source).strip()
    summary = str(raw.get("summary") or raw.get("snippet") or raw.get("abstract") or title).strip()
    return ExternalEvidenceItem(
        title=title,
        summary=summary,
        url=url,
        source=source,
        as_of=as_of,
        evidence_ref=str(raw.get("evidence_ref") or stable_evidence_ref(source, url, as_of, title)),
        provider=provider,
        result_type=result_type,
        published_at=str(raw.get("published_at")) if raw.get("published_at") else None,
        authors=tuple(str(item) for item in raw.get("authors", []) if item) if isinstance(raw.get("authors"), list) else (),
        artifact_refs=tuple(dict(item) for item in raw.get("artifact_refs", []) if isinstance(item, dict)) if isinstance(raw.get("artifact_refs"), list) else (),
        detail_ref=dict(raw["detail_ref"]) if isinstance(raw.get("detail_ref"), dict) else None,
    )


def parse_as_of(value: str | None) -> str:
    if not value:
        return utc_today()
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError:
        return utc_today()
