"""Provider-neutral code context injection contracts."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol

from pydantic import ValidationError

from .models import CodeContextManifest, CodeContextRef, canonical_json_dumps

CODE_CONTEXT_SCHEMA_VERSION = "research_assistant_code_context_manifest_v1"
NON_CODE_REASON = "non_code_query"
INSUFFICIENT_REASON = "evidence_insufficient"
TOKEN_SAFE_KEYS = (
    "as_of",
    "affected_tests",
    "call_chain",
    "detail_ref",
    "edge_refs",
    "file_path",
    "impact_radius",
    "manifest_ref",
    "provenance",
    "reason_code",
    "status",
    "summary",
    "summary_ref",
    "symbol",
)
BLOCKED_PAYLOAD_KEYS = {
    "ast",
    "code",
    "content",
    "raw",
    "selected_edges",
    "selected_nodes",
    "source_text",
    "text",
}
TEST_SUCCESS_WORDS = {"ci_passed", "nox_passed", "passed", "pytest_passed", "verified"}
CODE_QUERY_MARKERS = (
    ".py",
    ".sql",
    "backend/",
    "call chain",
    "class ",
    "def ",
    "frontend/",
    "function",
    "impact",
    "import ",
    "module",
    "script",
    "service",
    "symbol",
    "affected test",
    "代码",
    "调用",
    "函数",
    "模块",
    "影响",
    "类",
    "受影响测试",
)
PATH_RE = re.compile(r"(?P<path>(?:backend|frontend|scripts|tests|configs|docs)[/\\][A-Za-z0-9_.\\/-]+)")


@dataclass(frozen=True)
class CodeContextQuery:
    query: str
    task_id: str | None = None
    changed_files: tuple[str, ...] = ()
    max_refs: int = 6
    explicit_as_of: str | None = None
    repo_root: Path | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class CodeIntelligenceProvider(Protocol):
    def query_code_context(self, request: CodeContextQuery) -> CodeContextManifest | Mapping[str, Any]:
        ...


def normalize_repo_path(path: str | Path) -> str:
    text = str(path).replace("\\", "/").strip()
    while text.startswith("./"):
        text = text[2:]
    if text.startswith("repo://"):
        return "repo://" + re.sub(r"/+", "/", text[len("repo://"):])
    text = re.sub(r"/+", "/", text)
    return text


def extract_repo_paths(text: str | None) -> list[str]:
    paths: list[str] = []
    for match in PATH_RE.finditer(text or ""):
        path = normalize_repo_path(match.group("path").rstrip(".,;:，。；：)）]】"))
        if path and path not in paths:
            paths.append(path)
    return paths


def is_code_context_query(query: str | None, changed_files: list[str] | tuple[str, ...] | None = None) -> bool:
    files = [normalize_repo_path(path) for path in changed_files or [] if normalize_repo_path(path)]
    if files:
        return True
    lowered = (query or "").lower()
    return any(marker in lowered for marker in CODE_QUERY_MARKERS)


def stable_code_context_item_id(*, task_id: str | None, query: str, changed_files: list[str] | tuple[str, ...]) -> str:
    if task_id:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", task_id).strip("-")
        if safe:
            return safe
    digest = hashlib.sha256(
        canonical_json_dumps({"changed_files": list(changed_files), "query": query}).encode("utf-8")
    ).hexdigest()[:16]
    return f"ra-code-intel-{digest}"


def build_code_context_manifest(
    *,
    provider: CodeIntelligenceProvider,
    query: str,
    task_id: str | None = None,
    changed_files: list[str] | tuple[str, ...] | None = None,
    max_refs: int = 6,
    explicit_as_of: str | None = None,
    repo_root: Path | None = None,
) -> CodeContextManifest:
    normalized_files = tuple(sorted({normalize_repo_path(path) for path in changed_files or [] if normalize_repo_path(path)}))
    if not is_code_context_query(query, normalized_files):
        return CodeContextManifest(
            provider="code_intelligence_provider",
            query=query or "",
            status="not_applicable",
            refs=[],
            reason_code=NON_CODE_REASON,
        )
    request = CodeContextQuery(
        query=query or "",
        task_id=task_id,
        changed_files=normalized_files,
        max_refs=max_refs,
        explicit_as_of=explicit_as_of,
        repo_root=repo_root,
    )
    manifest = provider.query_code_context(request)
    return finalize_code_context_manifest(manifest, query=query or "", explicit_as_of=explicit_as_of, max_refs=max_refs)


def finalize_code_context_manifest(
    manifest: CodeContextManifest | Mapping[str, Any],
    *,
    query: str | None = None,
    explicit_as_of: str | None = None,
    max_refs: int = 6,
) -> CodeContextManifest:
    payload = manifest.model_dump() if isinstance(manifest, CodeContextManifest) else dict(manifest)
    payload.setdefault("schema_version", CODE_CONTEXT_SCHEMA_VERSION)
    payload.setdefault("provider", "code_intelligence_provider")
    payload.setdefault("query", query or payload.get("query") or "")
    payload.setdefault("refs", [])
    payload.setdefault("insufficient_refs", [])
    valid_refs: list[CodeContextRef] = []
    insufficient_refs: list[dict[str, Any]] = list(payload.get("insufficient_refs") or [])
    manifest_as_of = str(payload.get("as_of") or explicit_as_of or "").strip()
    for raw_ref in list(payload.get("refs") or []):
        ref_payload = raw_ref.model_dump() if isinstance(raw_ref, CodeContextRef) else dict(raw_ref)
        if explicit_as_of and not ref_payload.get("as_of"):
            ref_payload["as_of"] = explicit_as_of
        if not ref_payload.get("as_of") and manifest_as_of:
            ref_payload["as_of"] = manifest_as_of
        try:
            _assert_token_safe_payload(ref_payload)
            _assert_affected_tests_are_not_run_results(ref_payload.get("affected_tests") or [])
            ref = CodeContextRef(**ref_payload)
        except (TypeError, ValueError, ValidationError) as exc:
            insufficient_refs.append(
                {
                    "reason_code": INSUFFICIENT_REASON,
                    "error": f"{type(exc).__name__}: {exc}",
                    "source_ref": _compact_source_ref(ref_payload),
                }
            )
            continue
        valid_refs.append(ref)
    valid_refs = _dedupe_refs(sorted(valid_refs, key=_ref_sort_key))[: max(0, int(max_refs))]
    status = "ok" if valid_refs else ("not_applicable" if payload.get("status") == "not_applicable" else "evidence_insufficient")
    reason_code = payload.get("reason_code")
    if status == "evidence_insufficient" and not reason_code:
        reason_code = INSUFFICIENT_REASON
    return CodeContextManifest(
        schema_version=str(payload["schema_version"]),
        provider=str(payload["provider"]),
        query=str(payload.get("query") or ""),
        status=status,
        refs=valid_refs,
        insufficient_refs=insufficient_refs,
        reason_code=str(reason_code) if reason_code else None,
        as_of=manifest_as_of or None,
        manifest_ref=payload.get("manifest_ref"),
        summary_ref=payload.get("summary_ref"),
        detail_ref=payload.get("detail_ref"),
        source_refs=[normalize_repo_path(item) for item in payload.get("source_refs") or [] if str(item).strip()],
    )


def code_context_refs_for_pack(manifest: CodeContextManifest) -> list[dict[str, Any]]:
    return [_compact_ref(ref) for ref in manifest.refs]


def code_context_refs_for_worker(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    worker_refs: list[dict[str, Any]] = []
    for ref in refs:
        _assert_token_safe_payload(ref)
        compact = {key: ref[key] for key in TOKEN_SAFE_KEYS if key in ref}
        compact["affected_tests"] = [
            {
                "classification": test.get("classification"),
                "reason": test.get("reason"),
                "source_ref": test.get("source_ref"),
                "test_path": test.get("test_path"),
            }
            for test in ref.get("affected_tests") or []
        ]
        _assert_token_safe_payload(compact)
        _assert_affected_tests_are_not_run_results(compact.get("affected_tests") or [])
        worker_refs.append(compact)
    return sorted(worker_refs, key=lambda item: (str(item.get("file_path")), str(item.get("symbol"))))


def code_context_manifest_bytes(manifest: CodeContextManifest) -> bytes:
    payload = {
        "as_of": manifest.as_of,
        "insufficient_refs": manifest.insufficient_refs,
        "provider": manifest.provider,
        "query": manifest.query,
        "reason_code": manifest.reason_code,
        "refs": code_context_refs_for_pack(manifest),
        "schema_version": manifest.schema_version,
        "source_refs": manifest.source_refs,
        "status": manifest.status,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _compact_ref(ref: CodeContextRef) -> dict[str, Any]:
    payload = ref.model_dump(exclude_none=True)
    payload["affected_tests"] = [
        test.model_dump(exclude_none=True)
        for test in sorted(ref.affected_tests, key=lambda item: (item.test_path, item.classification))
    ]
    compact = {key: payload[key] for key in TOKEN_SAFE_KEYS if key in payload}
    _assert_token_safe_payload(compact)
    _assert_affected_tests_are_not_run_results(compact.get("affected_tests") or [])
    return compact


def _ref_sort_key(ref: CodeContextRef) -> tuple[str, str, str, str]:
    first_edge = ref.edge_refs[0] if ref.edge_refs else {}
    edge_id = str(first_edge.get("edge_id") or first_edge.get("id") or "")
    first_test = ref.affected_tests[0].test_path if ref.affected_tests else ""
    return (ref.file_path, ref.symbol, edge_id, first_test)


def _dedupe_refs(refs: list[CodeContextRef]) -> list[CodeContextRef]:
    seen: set[tuple[str, str, str]] = set()
    result: list[CodeContextRef] = []
    for ref in refs:
        first_edge = ref.edge_refs[0] if ref.edge_refs else {}
        key = (ref.file_path, ref.symbol, str(first_edge.get("edge_id") or first_edge.get("id") or ""))
        if key in seen:
            continue
        seen.add(key)
        result.append(ref)
    return result


def _compact_source_ref(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "file_path": payload.get("file_path"),
        "symbol": payload.get("symbol"),
        "summary_ref": payload.get("summary_ref"),
        "detail_ref": payload.get("detail_ref"),
    }


def _assert_token_safe_payload(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            lowered = str(key).lower()
            if lowered in BLOCKED_PAYLOAD_KEYS:
                raise ValueError(f"token-unsafe key is forbidden in code context refs: {key}")
            if isinstance(child, str) and len(child) > 1200:
                raise ValueError(f"token-unsafe string is too long at key: {key}")
            _assert_token_safe_payload(child)
    elif isinstance(value, list):
        if len(value) > 40:
            raise ValueError("token-unsafe list is too long")
        for child in value:
            _assert_token_safe_payload(child)


def _assert_affected_tests_are_not_run_results(items: list[Any]) -> None:
    for item in items:
        payload = item if isinstance(item, Mapping) else {}
        for key in ("classification", "status", "state", "result"):
            word = str(payload.get(key) or "").strip().lower()
            if word in TEST_SUCCESS_WORDS:
                raise ValueError("affected tests must not be marked as completed test runs")
