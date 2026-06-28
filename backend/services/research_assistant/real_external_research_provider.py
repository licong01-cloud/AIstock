"""Opt-in real external research provider for Research Assistant.

The provider keeps the existing summary-first contract while routing real
queries to free/self-hosted sources.  All network failures are recorded as
explicit reason codes instead of being converted into fabricated evidence.
"""

from __future__ import annotations

import importlib
import ipaddress
import logging
import re
from dataclasses import dataclass, field
from html import unescape
from typing import Any
from urllib.parse import quote_plus, urlparse, urlunparse
from xml.etree import ElementTree

import httpx

from backend.services.research_assistant.external_research import (
    ExtractedEvidence,
    ExternalEvidenceItem,
    assert_token_safe,
    clamp_chars,
    clamp_limit,
    item_from_provider_payload,
    stable_evidence_ref,
    utc_today,
)


logger = logging.getLogger("aistock.research_assistant.real_external_research")

AGENTSEARCH_WEB_PROVIDER = "agentsearch_web"
AGENTSEARCH_EXTRACT_PROVIDER = "agentsearch_extract"
SEMANTIC_SCHOLAR_PROVIDER = "semantic_scholar"
ARXIV_PROVIDER = "arxiv"
LOCAL_EXTRACT_PROVIDER = "local_trafilatura_extract"
DEFAULT_S2_BASE_URL = "https://api.semanticscholar.org/graph/v1"
DEFAULT_ARXIV_BASE_URL = "https://export.arxiv.org/api/query"
_S2_FIELDS = "title,abstract,tldr,url,year,authors,externalIds,openAccessPdf,publicationDate"
_DETAIL_TOOL_HINT = {
    "server": "aistock-external-research",
    "tool": "external_research_fetch_extract",
    "args_hint": {"url": "<url>", "max_chars": 2000},
}


@dataclass(frozen=True)
class RealExternalResearchConfig:
    agentsearch_base_url: str
    paper_provider: str = SEMANTIC_SCHOLAR_PROVIDER
    s2_api_key: str | None = None
    s2_base_url: str = DEFAULT_S2_BASE_URL
    arxiv_base_url: str = DEFAULT_ARXIV_BASE_URL
    timeout_seconds: float = 8.0


@dataclass
class RealExternalResearchProvider:
    """Free/self-hosted real provider for RA external_research.

    ``http_client`` is injectable so tests use ``httpx.MockTransport`` and do
    not touch the network.
    """

    agentsearch_base_url: str
    paper_provider: str = SEMANTIC_SCHOLAR_PROVIDER
    s2_api_key: str | None = None
    s2_base_url: str = DEFAULT_S2_BASE_URL
    arxiv_base_url: str = DEFAULT_ARXIV_BASE_URL
    timeout_seconds: float = 8.0
    local_extract_allowed_hosts: tuple[str, ...] = ()
    http_client: httpx.Client | None = None
    _last_failures: dict[str, dict[str, Any]] = field(default_factory=dict, init=False)
    _owned_client: httpx.Client | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.agentsearch_base_url = _required_base_url(self.agentsearch_base_url, "RA_AGENTSEARCH_BASE_URL")
        self.paper_provider = _normalize_paper_provider(self.paper_provider)
        self.s2_base_url = self.s2_base_url.rstrip("/")
        self.arxiv_base_url = self.arxiv_base_url.rstrip("/")
        self.local_extract_allowed_hosts = _normalize_allowed_hosts(self.local_extract_allowed_hosts)

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> "RealExternalResearchProvider":
        import os

        env = os.environ if environ is None else environ
        return cls(
            agentsearch_base_url=str(env.get("RA_AGENTSEARCH_BASE_URL") or "").strip(),
            paper_provider=str(env.get("RA_PAPER_PROVIDER") or SEMANTIC_SCHOLAR_PROVIDER).strip(),
            s2_api_key=str(env.get("S2_API_KEY") or "").strip() or None,
            local_extract_allowed_hosts=_split_allowed_hosts(env.get("RA_LOCAL_EXTRACT_ALLOWED_HOSTS")),
        )

    def last_failure(self, operation: str | None = None) -> dict[str, Any] | None:
        if operation:
            return self._last_failures.get(operation)
        return next(reversed(self._last_failures.values()), None) if self._last_failures else None

    def search_web(self, query: str, *, locale: str = "zh-CN", limit: int = 10) -> list[ExternalEvidenceItem]:
        operation = "search_web"
        self._clear_failure(operation)
        safe_limit = clamp_limit(limit)
        try:
            payload = self._get_agentsearch(
                "/search",
                params={"q": query, "language": locale, "count": safe_limit},
                operation=operation,
            )
            rows = _agentsearch_results(payload)
            items = [
                _web_item_from_agentsearch(row, provider=AGENTSEARCH_WEB_PROVIDER)
                for row in rows
                if isinstance(row, dict)
            ]
            safe_items = _dedup_items(items, limit=safe_limit)
            for item in safe_items:
                assert_token_safe(item.compact())
            if not safe_items:
                self._record_reason(operation, "AGENTSEARCH_WEB_NO_RESULTS", {"query": query, "locale": locale})
            return safe_items
        except Exception as exc:  # noqa: BLE001 - loud diagnostic is recorded for the router/no-data guard.
            self._record_failure(operation, exc, "AGENTSEARCH_WEB_REQUEST_FAILED", {"query": query, "locale": locale})
            return []

    def search_papers(self, query: str, *, provider: str | None = None, limit: int = 10) -> list[ExternalEvidenceItem]:
        operation = "search_papers"
        self._clear_failure(operation)
        try:
            selected = _normalize_paper_provider(provider or self.paper_provider)
        except Exception as exc:  # noqa: BLE001 - provider selection failure must be explicit for no-data guard.
            self._record_failure(
                operation,
                exc,
                "RA_PAPER_PROVIDER_UNSUPPORTED",
                {"query": query, "provider": provider or self.paper_provider},
            )
            return []
        safe_limit = clamp_limit(limit)
        if selected == ARXIV_PROVIDER:
            return self._search_arxiv(query, limit=safe_limit, operation=operation)
        try:
            return self._search_semantic_scholar(query, limit=safe_limit, operation=operation)
        except _SemanticScholarRateLimited:
            logger.warning(
                "Semantic Scholar paper search rate-limited; falling back to arXiv, reason_code=S2_RATE_LIMIT_FALLBACK"
            )
            return self._search_arxiv(
                query,
                limit=safe_limit,
                operation=operation,
                fallback_reason="S2_RATE_LIMIT_FALLBACK",
            )
        except Exception as exc:  # noqa: BLE001 - provider failure must be visible to callers.
            self._record_failure(operation, exc, "SEMANTIC_SCHOLAR_REQUEST_FAILED", {"query": query})
            return []

    def fetch_extract(self, url: str, *, max_chars: int = 2000) -> ExtractedEvidence:
        operation = "fetch_extract"
        self._clear_failure(operation)
        safe_chars = clamp_chars(max_chars)
        try:
            payload = self._get_agentsearch(
                "/read",
                params={"url": url, "max_chars": safe_chars},
                operation=operation,
            )
            extract = _extract_from_agentsearch_payload(payload, url=url, max_chars=safe_chars)
            assert_token_safe(extract.compact(max_preview_chars=safe_chars))
            return extract
        except Exception as agentsearch_exc:  # noqa: BLE001 - fallback remains explicit and auditable.
            self._record_failure(
                operation,
                agentsearch_exc,
                "AGENTSEARCH_EXTRACT_REQUEST_FAILED",
                {"url": url, "fallback": "trafilatura"},
            )
            fallback = self._fetch_extract_with_trafilatura(url=url, max_chars=safe_chars, upstream_error=agentsearch_exc)
            assert_token_safe(fallback.compact(max_preview_chars=safe_chars))
            return fallback

    def _search_semantic_scholar(self, query: str, *, limit: int, operation: str) -> list[ExternalEvidenceItem]:
        headers = {"x-api-key": self.s2_api_key} if self.s2_api_key else None
        response = self._client().get(
            f"{self.s2_base_url}/paper/search",
            params={"query": query, "limit": limit, "fields": _S2_FIELDS},
            headers=headers,
        )
        if response.status_code == 429:
            raise _SemanticScholarRateLimited("reason_code=SEMANTIC_SCHOLAR_RATE_LIMITED,status_code=429")
        if response.status_code >= 400:
            raise _HttpStatusFailure("SEMANTIC_SCHOLAR_HTTP_ERROR", response)
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("reason_code=SEMANTIC_SCHOLAR_JSON_INVALID") from exc
        rows = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise RuntimeError("reason_code=SEMANTIC_SCHOLAR_SCHEMA_INVALID")
        items = [_paper_item_from_s2(row) for row in rows if isinstance(row, dict)]
        safe_items = _dedup_items(items, limit=limit)
        for item in safe_items:
            assert_token_safe(item.compact())
        if not safe_items:
            self._record_reason(operation, "SEMANTIC_SCHOLAR_NO_RESULTS", {"query": query})
        return safe_items

    def _search_arxiv(
        self,
        query: str,
        *,
        limit: int,
        operation: str,
        fallback_reason: str | None = None,
    ) -> list[ExternalEvidenceItem]:
        try:
            response = self._client().get(
                self.arxiv_base_url,
                params={"search_query": f"all:{query}", "start": 0, "max_results": limit},
            )
            if response.status_code >= 400:
                raise _HttpStatusFailure("ARXIV_HTTP_ERROR", response)
            items = _paper_items_from_arxiv_atom(response.text)
            safe_items = _dedup_items(items, limit=limit)
            for item in safe_items:
                assert_token_safe(item.compact())
            if fallback_reason:
                self._record_reason(operation, fallback_reason, {"query": query, "fallback_provider": ARXIV_PROVIDER})
            elif not safe_items:
                self._record_reason(operation, "ARXIV_NO_RESULTS", {"query": query})
            return safe_items
        except Exception as exc:  # noqa: BLE001 - no fabricated paper evidence.
            self._record_failure(operation, exc, "ARXIV_REQUEST_FAILED", {"query": query})
            return []

    def _fetch_extract_with_trafilatura(self, *, url: str, max_chars: int, upstream_error: Exception) -> ExtractedEvidence:
        try:
            safe_url = _require_local_extract_allowed_url(url, self.local_extract_allowed_hosts)
            # This fallback is host allow-listed by RA_LOCAL_EXTRACT_ALLOWED_HOSTS
            # and rejects localhost/private/reserved IP targets before the request.
            # CodeQL[py/full-ssrf] safe_url is rebuilt after explicit host allowlist and internal-target rejection.
            response = self._client().get(safe_url)
            if response.status_code >= 400:
                raise _HttpStatusFailure("LOCAL_EXTRACT_HTTP_ERROR", response)
            trafilatura = importlib.import_module("trafilatura")
            extracted = trafilatura.extract(
                response.text,
                include_comments=False,
                include_tables=False,
                favor_precision=True,
            )
            content = _clean_text(extracted)
            if not content:
                raise RuntimeError("reason_code=LOCAL_TRAFILATURA_EMPTY")
            title = _title_from_html(response.text) or url
            return _build_extract(
                title=title,
                url=safe_url,
                source=_host(safe_url) or LOCAL_EXTRACT_PROVIDER,
                provider=LOCAL_EXTRACT_PROVIDER,
                content=content,
                max_chars=max_chars,
                detail_extra={
                    "fallback_from": AGENTSEARCH_EXTRACT_PROVIDER,
                    "fallback_reason_code": _reason_code_from_exception(upstream_error)
                    or "AGENTSEARCH_EXTRACT_REQUEST_FAILED",
                },
            )
        except Exception as exc:  # noqa: BLE001 - extraction failure is represented, not hidden.
            reason = _reason_code_from_exception(exc) or "LOCAL_TRAFILATURA_EXTRACT_FAILED"
            self._record_failure("fetch_extract", exc, reason, {"url": url, "fallback": "trafilatura"})
            return _empty_extract(
                url=url,
                max_chars=max_chars,
                provider=LOCAL_EXTRACT_PROVIDER,
                reason_code=reason,
                upstream_reason_code=_reason_code_from_exception(upstream_error) or "AGENTSEARCH_EXTRACT_REQUEST_FAILED",
            )

    def _get_agentsearch(self, path: str, *, params: dict[str, Any], operation: str) -> dict[str, Any]:
        response = self._client().get(f"{self.agentsearch_base_url}{path}", params=params)
        if response.status_code >= 400:
            raise _HttpStatusFailure(f"{operation.upper()}_HTTP_ERROR", response)
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"reason_code={operation.upper()}_JSON_INVALID") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"reason_code={operation.upper()}_SCHEMA_INVALID")
        return payload

    def _client(self) -> httpx.Client:
        if self.http_client is not None:
            return self.http_client
        if self._owned_client is None:
            self._owned_client = httpx.Client(timeout=self.timeout_seconds)
        return self._owned_client

    def _clear_failure(self, operation: str) -> None:
        self._last_failures.pop(operation, None)

    def _record_reason(self, operation: str, reason_code: str, context: dict[str, Any] | None = None) -> None:
        self._last_failures[operation] = {
            "reason_code": reason_code,
            "context": dict(context or {}),
            "provider": "real_external_research_provider",
        }

    def _record_failure(
        self,
        operation: str,
        exc: Exception,
        reason_code: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        derived = _reason_code_from_exception(exc) or reason_code
        self._last_failures[operation] = {
            "reason_code": derived,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "context": dict(context or {}),
            "provider": "real_external_research_provider",
        }
        logger.warning(
            "Real external research provider operation failed; reason_code=%s operation=%s error_type=%s error=%s",
            derived,
            operation,
            type(exc).__name__,
            exc,
        )


class _SemanticScholarRateLimited(RuntimeError):
    pass


class _HttpStatusFailure(RuntimeError):
    def __init__(self, reason_code: str, response: httpx.Response) -> None:
        try:
            request_url = str(response.request.url)
        except RuntimeError:
            request_url = ""
        super().__init__(
            f"reason_code={reason_code}, status_code={response.status_code}, url={request_url}"
        )
        self.reason_code = reason_code
        self.status_code = response.status_code


def _required_base_url(value: str, env_name: str) -> str:
    normalized = str(value or "").strip().rstrip("/")
    if not normalized:
        raise RuntimeError(f"reason_code=RA_AGENTSEARCH_BASE_URL_MISSING,{env_name}=empty")
    return normalized


def _normalize_paper_provider(value: str | None) -> str:
    normalized = str(value or SEMANTIC_SCHOLAR_PROVIDER).strip().lower()
    if normalized in {"s2", "semantic-scholar", "semantic_scholar"}:
        return SEMANTIC_SCHOLAR_PROVIDER
    if normalized == ARXIV_PROVIDER:
        return ARXIV_PROVIDER
    raise RuntimeError(
        "reason_code=RA_PAPER_PROVIDER_UNSUPPORTED, expected semantic_scholar or arxiv, "
        f"got {value!r}"
    )


def _split_allowed_hosts(value: str | None) -> tuple[str, ...]:
    return tuple(part.strip() for part in str(value or "").split(",") if part.strip())


def _normalize_allowed_hosts(hosts: tuple[str, ...]) -> tuple[str, ...]:
    normalized: list[str] = []
    for host in hosts:
        item = str(host or "").strip().lower()
        if not item:
            continue
        if item == "*":
            raise RuntimeError("reason_code=RA_LOCAL_EXTRACT_ALLOWED_HOSTS_WILDCARD_FORBIDDEN")
        if item.startswith("*."):
            item = item[1:]
        pattern = item[1:] if item.startswith(".") else item
        if not pattern or not re.fullmatch(r"[a-z0-9.-]+", pattern):
            raise RuntimeError("reason_code=RA_LOCAL_EXTRACT_ALLOWED_HOSTS_INVALID")
        normalized.append(item)
    return tuple(dict.fromkeys(normalized))


def _require_local_extract_allowed_url(url: str, allowed_hosts: tuple[str, ...]) -> str:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"}:
        raise RuntimeError("reason_code=LOCAL_TRAFILATURA_URL_SCHEME_NOT_ALLOWED")
    if parsed.username or parsed.password:
        raise RuntimeError("reason_code=LOCAL_TRAFILATURA_URL_USERINFO_FORBIDDEN")
    hostname = str(parsed.hostname or "").strip().lower().rstrip(".")
    if not hostname:
        raise RuntimeError("reason_code=LOCAL_TRAFILATURA_URL_HOST_MISSING")
    address = _ip_literal(hostname)
    if address is not None:
        if _ip_address_is_internal(address):
            raise RuntimeError("reason_code=LOCAL_TRAFILATURA_INTERNAL_HOST_FORBIDDEN")
        raise RuntimeError("reason_code=LOCAL_TRAFILATURA_IP_LITERAL_FORBIDDEN")
    if _hostname_is_internal_name(hostname):
        raise RuntimeError("reason_code=LOCAL_TRAFILATURA_INTERNAL_HOST_FORBIDDEN")
    if not _host_matches_allowed(hostname, allowed_hosts):
        raise RuntimeError("reason_code=LOCAL_TRAFILATURA_HOST_NOT_ALLOWED")
    try:
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError("reason_code=LOCAL_TRAFILATURA_PORT_NOT_ALLOWED") from exc
    if port not in (None, 80, 443):
        raise RuntimeError("reason_code=LOCAL_TRAFILATURA_PORT_NOT_ALLOWED")
    netloc = hostname if port is None else f"{hostname}:{port}"
    path = parsed.path or "/"
    return urlunparse((parsed.scheme, netloc, path, parsed.params, parsed.query, ""))


def _hostname_is_internal_name(hostname: str) -> bool:
    lowered = hostname.lower()
    return lowered in {"localhost", "localhost.localdomain"} or lowered.endswith(".localhost")


def _ip_literal(hostname: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    try:
        return ipaddress.ip_address(hostname.strip("[]"))
    except ValueError:
        return None


def _ip_address_is_internal(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return any(
        (
            address.is_private,
            address.is_loopback,
            address.is_link_local,
            address.is_multicast,
            address.is_reserved,
            address.is_unspecified,
        )
    )


def _host_matches_allowed(hostname: str, allowed_hosts: tuple[str, ...]) -> bool:
    for allowed in allowed_hosts:
        if allowed.startswith("."):
            suffix = allowed[1:]
            if hostname == suffix or hostname.endswith(allowed):
                return True
            continue
        if hostname == allowed:
            return True
    return False


def _agentsearch_results(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = payload.get("results")
    if candidates is None:
        candidates = payload.get("items")
    if isinstance(candidates, list):
        return [item for item in candidates if isinstance(item, dict)]
    raise RuntimeError("reason_code=AGENTSEARCH_WEB_SCHEMA_INVALID")


def _web_item_from_agentsearch(raw: dict[str, Any], *, provider: str) -> ExternalEvidenceItem:
    url = str(raw.get("url") or raw.get("link") or "").strip()
    source = str(raw.get("source") or raw.get("engine") or _host(url) or "agentsearch").strip()
    payload = {
        "title": _clean_text(raw.get("title")) or url or source,
        "summary": _clean_text(raw.get("summary") or raw.get("snippet") or raw.get("content"))
        or "Search result summary unavailable.",
        "url": url,
        "source": source,
        "as_of": str(raw.get("as_of") or utc_today()),
        "detail_ref": {
            **_DETAIL_TOOL_HINT,
            "provider": provider,
            "score": raw.get("score"),
            "inline": False,
        },
    }
    return item_from_provider_payload(payload, provider=provider, result_type="web", today=utc_today())


def _paper_item_from_s2(raw: dict[str, Any]) -> ExternalEvidenceItem:
    external_ids = raw.get("externalIds") if isinstance(raw.get("externalIds"), dict) else {}
    paper_id = str(raw.get("paperId") or raw.get("corpusId") or "")
    url = (
        _open_access_pdf_url(raw)
        or str(raw.get("url") or "").strip()
        or _doi_url(external_ids)
        or _arxiv_url(external_ids)
        or (f"https://www.semanticscholar.org/paper/{paper_id}" if paper_id else "")
    )
    summary = _clean_text((raw.get("tldr") or {}).get("text") if isinstance(raw.get("tldr"), dict) else None)
    if not summary:
        summary = _clean_text(raw.get("abstract")) or "Semantic Scholar paper metadata returned no abstract."
    authors_raw = raw.get("authors") if isinstance(raw.get("authors"), list) else []
    authors = tuple(_clean_text(item.get("name")) for item in authors_raw if isinstance(item, dict) and item.get("name"))
    artifact_refs = _artifact_refs_from_external_ids(external_ids)
    payload = {
        "title": _clean_text(raw.get("title")) or "Untitled Semantic Scholar result",
        "summary": summary,
        "url": url,
        "source": "semanticscholar.org",
        "as_of": utc_today(),
        "published_at": str(raw.get("publicationDate") or raw.get("year") or "") or None,
        "authors": list(authors),
        "artifact_refs": artifact_refs,
        "detail_ref": {
            **_DETAIL_TOOL_HINT,
            "provider": SEMANTIC_SCHOLAR_PROVIDER,
            "paper_id": paper_id,
            "external_ids": external_ids,
            "inline": False,
        },
    }
    return item_from_provider_payload(payload, provider=SEMANTIC_SCHOLAR_PROVIDER, result_type="paper", today=utc_today())


def _paper_items_from_arxiv_atom(xml_text: str) -> list[ExternalEvidenceItem]:
    try:
        root = ElementTree.fromstring(xml_text.encode("utf-8"))
    except ElementTree.ParseError as exc:
        raise RuntimeError("reason_code=ARXIV_ATOM_INVALID") from exc
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items: list[ExternalEvidenceItem] = []
    for entry in root.findall("atom:entry", ns):
        url = _node_text(entry, "atom:id", ns)
        title = _clean_text(_node_text(entry, "atom:title", ns)) or url or "Untitled arXiv result"
        summary = _clean_text(_node_text(entry, "atom:summary", ns)) or "arXiv result metadata returned no summary."
        published = _node_text(entry, "atom:published", ns)[:10] or None
        authors = tuple(
            _clean_text(_node_text(author, "atom:name", ns))
            for author in entry.findall("atom:author", ns)
            if _clean_text(_node_text(author, "atom:name", ns))
        )
        arxiv_id = url.rstrip("/").rsplit("/", 1)[-1] if url else ""
        payload = {
            "title": title,
            "summary": summary,
            "url": url,
            "source": "arxiv.org",
            "as_of": utc_today(),
            "published_at": published,
            "authors": list(authors),
            "artifact_refs": [{"kind": "arxiv_id", "value": arxiv_id}] if arxiv_id else [],
            "detail_ref": {
                **_DETAIL_TOOL_HINT,
                "provider": ARXIV_PROVIDER,
                "arxiv_id": arxiv_id,
                "inline": False,
            },
        }
        items.append(item_from_provider_payload(payload, provider=ARXIV_PROVIDER, result_type="paper", today=utc_today()))
    return items


def _extract_from_agentsearch_payload(payload: dict[str, Any], *, url: str, max_chars: int) -> ExtractedEvidence:
    content = _clean_text(
        payload.get("content_preview")
        or payload.get("extract")
        or payload.get("text")
        or payload.get("content")
        or payload.get("summary")
    )
    if not content:
        reason_detail = _clean_text(payload.get("error"))
        reason = f", detail={reason_detail}" if reason_detail else ""
        raise RuntimeError(f"reason_code=AGENTSEARCH_EXTRACT_EMPTY{reason}")
    return _build_extract(
        title=_clean_text(payload.get("title")) or url,
        url=str(payload.get("url") or url),
        source=str(payload.get("source") or _host(str(payload.get("url") or url)) or "agentsearch").strip(),
        provider=AGENTSEARCH_EXTRACT_PROVIDER,
        content=content,
        max_chars=max_chars,
        detail_extra={"provider": AGENTSEARCH_EXTRACT_PROVIDER},
    )


def _build_extract(
    *,
    title: str,
    url: str,
    source: str,
    provider: str,
    content: str,
    max_chars: int,
    detail_extra: dict[str, Any] | None = None,
) -> ExtractedEvidence:
    clean_content = _clean_text(content)
    preview = clean_content[:max_chars].strip()
    summary = preview[:600].strip() or "Extracted content unavailable."
    return ExtractedEvidence(
        title=title[:180],
        url=url,
        source=source,
        as_of=utc_today(),
        evidence_ref=stable_evidence_ref(provider, url, utc_today(), title),
        provider=provider,
        extract_summary=summary,
        content_preview=preview,
        detail_ref={
            "kind": "external_extract_ref",
            "uri": url,
            "inline": False,
            "content_chars": len(clean_content),
            "max_chars": max_chars,
            **dict(detail_extra or {}),
        },
    )


def _empty_extract(
    *,
    url: str,
    max_chars: int,
    provider: str,
    reason_code: str,
    upstream_reason_code: str | None = None,
) -> ExtractedEvidence:
    summary = f"Extraction unavailable; reason_code={reason_code}."
    return ExtractedEvidence(
        title=f"Extraction unavailable for {_host(url) or url}",
        url=url,
        source=_host(url) or provider,
        as_of=utc_today(),
        evidence_ref=stable_evidence_ref(provider, url, utc_today(), reason_code),
        provider=provider,
        extract_summary=summary,
        content_preview="",
        detail_ref={
            "kind": "external_extract_ref",
            "uri": url,
            "inline": False,
            "reason_code": reason_code,
            "upstream_reason_code": upstream_reason_code,
            "max_chars": max_chars,
        },
    )


def _dedup_items(items: list[ExternalEvidenceItem], *, limit: int) -> list[ExternalEvidenceItem]:
    seen: set[tuple[str, str]] = set()
    deduped: list[ExternalEvidenceItem] = []
    for item in items:
        key = (item.provider, item.url)
        if not item.url or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped[:limit]


def _host(url: str) -> str:
    try:
        return str(urlparse(url).hostname or "")
    except ValueError:
        return ""


def _clean_text(value: Any, *, max_chars: int = 1800) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:max_chars].strip()


def _reason_code_from_exception(exc: Exception) -> str | None:
    text = str(exc)
    match = re.search(r"reason_code=([A-Z0-9_]+)", text)
    if match:
        return match.group(1)
    if isinstance(exc, httpx.TimeoutException):
        return "EXTERNAL_RESEARCH_TIMEOUT"
    if isinstance(exc, httpx.ConnectError):
        return "EXTERNAL_RESEARCH_CONNECTION_FAILED"
    return getattr(exc, "reason_code", None)


def _node_text(node: ElementTree.Element, path: str, ns: dict[str, str]) -> str:
    child = node.find(path, ns)
    return str(child.text or "").strip() if child is not None else ""


def _title_from_html(html: str) -> str:
    for tag in ("title", "h1"):
        match = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", html or "", flags=re.IGNORECASE | re.DOTALL)
        if match:
            text = re.sub(r"<[^>]+>", " ", match.group(1))
            return _clean_text(unescape(text))
    return ""


def _open_access_pdf_url(raw: dict[str, Any]) -> str:
    pdf = raw.get("openAccessPdf")
    if isinstance(pdf, dict):
        return str(pdf.get("url") or "").strip()
    return ""


def _doi_url(external_ids: dict[str, Any]) -> str:
    doi = str(external_ids.get("DOI") or external_ids.get("doi") or "").strip()
    return f"https://doi.org/{doi}" if doi else ""


def _arxiv_url(external_ids: dict[str, Any]) -> str:
    arxiv_id = str(external_ids.get("ArXiv") or external_ids.get("arXiv") or "").strip()
    return f"https://arxiv.org/abs/{quote_plus(arxiv_id)}" if arxiv_id else ""


def _artifact_refs_from_external_ids(external_ids: dict[str, Any]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for key, kind in (("ArXiv", "arxiv_id"), ("arXiv", "arxiv_id"), ("DOI", "doi"), ("doi", "doi")):
        value = str(external_ids.get(key) or "").strip()
        if value and {"kind": kind, "value": value} not in refs:
            refs.append({"kind": kind, "value": value})
    return refs
