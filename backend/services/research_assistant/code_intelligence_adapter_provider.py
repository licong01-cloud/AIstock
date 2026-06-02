"""AIstock bridge for the existing code intelligence adapter."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import scripts.code_intelligence_adapter as adapter

from .code_intelligence_core import (
    CodeContextQuery,
    extract_repo_paths,
    normalize_repo_path,
    stable_code_context_item_id,
)
from .models import CodeContextManifest, CodeContextRef
from .runtime_config import REPO_ROOT


class CodeIntelligenceProviderError(RuntimeError):
    """Raised when the existing adapter cannot produce traceable refs."""


class AistockCodeIntelligenceAdapterProvider:
    """Bridge `scripts.code_intelligence_adapter` into Research Assistant refs."""

    provider_name = "scripts.code_intelligence_adapter"

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        skip_external: bool = True,
        default_modules: tuple[str, ...] = ("research_assistant",),
    ) -> None:
        self.repo_root = repo_root or REPO_ROOT
        self.skip_external = skip_external
        self.default_modules = default_modules

    def query_code_context(self, request: CodeContextQuery) -> CodeContextManifest:
        item_id = stable_code_context_item_id(
            task_id=request.task_id,
            query=request.query,
            changed_files=request.changed_files,
        )
        changed_files = _changed_files_for_request(request)
        status = adapter.codegraph_status(self.repo_root, skip_external=self.skip_external)
        context = adapter.build_context_artifacts(
            item_id=item_id,
            query=request.query,
            changed_files=changed_files,
            root=self.repo_root,
            skip_external=self.skip_external,
        )
        affected = adapter.build_affected_tests_artifact(
            item_id=item_id,
            changed_files=changed_files,
            root=self.repo_root,
            skip_external=self.skip_external,
        )
        module = _infer_module(request.query, changed_files, default=self.default_modules[0])
        ua_summary = adapter.build_understand_anything_summary(module=module, root=self.repo_root)
        ua_manifest = adapter.build_understand_anything_summary_manifest(
            modules=sorted({module, *self.default_modules}),
            root=self.repo_root,
        )
        as_of = _adapter_as_of(context, affected, ua_summary, ua_manifest, request.explicit_as_of)
        if not as_of:
            raise CodeIntelligenceProviderError("adapter manifest did not provide generated_at/as_of")
        source_refs = _source_refs(context, affected, ua_summary, ua_manifest)
        refs = [
            _build_ref(
                file_path=file_path,
                symbol=_infer_symbol(file_path, request.query),
                query=request.query,
                as_of=as_of,
                status=status,
                context=context,
                affected=affected,
                ua_summary=ua_summary,
                ua_manifest=ua_manifest,
            )
            for file_path in _ref_file_paths(changed_files, request.query, module)[: max(1, request.max_refs)]
        ]
        return CodeContextManifest(
            provider=self.provider_name,
            query=request.query,
            status="ok" if refs else "evidence_insufficient",
            refs=refs,
            reason_code=None if refs else "evidence_insufficient",
            as_of=as_of,
            manifest_ref=context.get("manifest_path"),
            summary_ref=ua_summary.get("summary_ref"),
            detail_ref=context.get("context_markdown"),
            source_refs=source_refs,
        )


def _changed_files_for_request(request: CodeContextQuery) -> list[str]:
    paths = [normalize_repo_path(path) for path in request.changed_files if normalize_repo_path(path)]
    if paths:
        return sorted(set(paths))
    return extract_repo_paths(request.query)


def _ref_file_paths(changed_files: list[str], query: str, module: str) -> list[str]:
    paths = list(dict.fromkeys([*changed_files, *extract_repo_paths(query)]))
    if paths:
        return sorted(paths)
    return [f"repo://{module}"]


def _adapter_as_of(*payloads: Any) -> str | None:
    for payload in payloads:
        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        if isinstance(payload, dict):
            for key in ("as_of", "generated_at"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return None


def _source_refs(*payloads: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for payload in payloads:
        for key in ("artifact_path", "manifest_path", "context_markdown", "summary_ref"):
            value = payload.get(key)
            if isinstance(value, str) and value and value not in refs:
                refs.append(normalize_repo_path(value))
    return refs


def _build_ref(
    *,
    file_path: str,
    symbol: str,
    query: str,
    as_of: str,
    status: dict[str, Any],
    context: dict[str, Any],
    affected: dict[str, Any],
    ua_summary: dict[str, Any],
    ua_manifest: dict[str, Any],
) -> CodeContextRef:
    normalized_path = normalize_repo_path(file_path)
    context_ref = normalize_repo_path(str(context.get("context_markdown") or ""))
    affected_ref = normalize_repo_path(str(affected.get("artifact_path") or ""))
    edge_id = f"codegraph:{normalized_path}:{symbol}:context"
    provenance = {
        "adapter": "scripts.code_intelligence_adapter",
        "affected_tests_ref": affected_ref or None,
        "codegraph_status": {
            "git_commit": status.get("git_commit"),
            "graph_root_source": status.get("graph_root_source"),
            "index_exists": status.get("index_exists"),
            "status": status.get("status"),
        },
        "context_ref": context_ref or None,
        "manifest_ref": normalize_repo_path(str(context.get("manifest_path") or "")) or None,
        "ua_manifest_ref": "tmp/validation/code-intelligence/ua-summary-manifest.json",
        "ua_summary_ref": normalize_repo_path(str(ua_summary.get("summary_ref") or "")) or None,
    }
    suggested_tests = [normalize_repo_path(str(item)) for item in affected.get("suggested_tests") or [] if str(item).strip()]
    codegraph_tests = {normalize_repo_path(str(item)) for item in affected.get("codegraph_suggested_tests") or []}
    return CodeContextRef(
        file_path=normalized_path,
        symbol=symbol,
        edge_refs=[
            {
                "edge_id": edge_id,
                "edge_type": "code_context",
                "source_file": normalized_path,
                "target_ref": context_ref or affected_ref or str(ua_summary.get("summary_ref") or ""),
            }
        ],
        provenance=provenance,
        as_of=as_of,
        summary=_compact_summary(query=query, file_path=normalized_path, symbol=symbol, affected_count=len(suggested_tests)),
        summary_ref=normalize_repo_path(str(ua_summary.get("summary_ref") or "")) or None,
        detail_ref=context_ref or None,
        manifest_ref=normalize_repo_path(str(context.get("manifest_path") or "")) or None,
        call_chain=[
            {
                "from": normalized_path,
                "to": context_ref or "codegraph-context",
                "edge_id": edge_id,
            }
        ],
        impact_radius={
            "affected_test_count": len(suggested_tests),
            "context_status": context.get("status"),
            "graph_root_source": context.get("graph_root_source") or affected.get("graph_root_source"),
            "ua_status": ua_summary.get("status"),
            "ua_manifest_modules": list(ua_manifest.get("modules") or []),
        },
        affected_tests=[
            {
                "test_path": test_path,
                "classification": "impacted" if test_path in codegraph_tests else "recommended",
                "source_ref": affected_ref,
                "reason": "codegraph affected summary",
            }
            for test_path in suggested_tests
        ],
    )


def _compact_summary(*, query: str, file_path: str, symbol: str, affected_count: int) -> str:
    query_text = re.sub(r"\s+", " ", query).strip()
    if len(query_text) > 180:
        query_text = f"{query_text[:177]}..."
    return f"{file_path}::{symbol} is relevant to `{query_text or 'code query'}`; affected_tests={affected_count}."


def _infer_module(query: str, changed_files: list[str], *, default: str) -> str:
    lowered = query.lower()
    for module in ("research_assistant", "validation_center", "paper_v2", "qe", "issue_workflow"):
        if module in lowered or any(module in path for path in changed_files):
            return module
    if changed_files:
        first = changed_files[0]
        parts = first.split("/")
        if len(parts) >= 3 and parts[0] == "backend" and parts[1] == "services":
            return parts[2]
    return default


def _infer_symbol(file_path: str, query: str) -> str:
    normalized = normalize_repo_path(file_path)
    if normalized.startswith("repo://"):
        return normalized.rsplit("/", 1)[-1] or "repository"
    match = re.search(r"(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)", query)
    if match:
        return match.group(1)
    stem = Path(normalized).stem
    return stem or "module"
