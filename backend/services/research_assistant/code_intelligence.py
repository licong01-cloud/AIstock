"""Read-only code-intelligence context for Research Assistant packs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.services.validation.history_store import ValidationHistoryStore

CODE_INTELLIGENCE_CONTEXT_SCHEMA = "aistock_research_assistant_code_intelligence_context_v1"
DEFAULT_MAX_ARTIFACT_REFS = 8
DEFAULT_MAX_WARNINGS = 6


def build_code_intelligence_context(
    *,
    repo_root: Path,
    max_artifact_refs: int = DEFAULT_MAX_ARTIFACT_REFS,
    max_warnings: int = DEFAULT_MAX_WARNINGS,
) -> dict[str, Any]:
    """Return a bounded, warning-only summary of existing graph artifacts."""

    try:
        summary = ValidationHistoryStore(repo_root=repo_root).code_intelligence_summary()
    except Exception as exc:  # pragma: no cover - defensive guard for runtime-only IO errors.
        return {
            "schema_version": CODE_INTELLIGENCE_CONTEXT_SCHEMA,
            "data_state": "unavailable",
            "blocking_for_issue_workflow": False,
            "codegraph": {"status": "unavailable", "freshness": None, "artifact_ref": None},
            "understand_anything": {"summary_count": 0, "manifest_ref": None, "latest_summary_refs": []},
            "artifact_refs": [],
            "warnings": [f"Code-intelligence artifact summary unavailable: {exc}"][:max_warnings],
            "reason_codes": ["code_intelligence_summary_unavailable"],
            "provenance": "validation_history_store",
        }

    artifacts = [
        _compact_artifact_ref(item)
        for item in _as_list(summary.get("artifacts"))
        if isinstance(item, dict)
    ][: max(max_artifact_refs, 0)]
    warnings = _dedupe_strings(
        [
            *_as_list(summary.get("warnings")),
            *_as_list((summary.get("codegraph") or {}).get("warnings") if isinstance(summary.get("codegraph"), dict) else []),
        ]
    )[: max(max_warnings, 0)]
    return {
        "schema_version": CODE_INTELLIGENCE_CONTEXT_SCHEMA,
        "data_state": str(summary.get("data_state") or "missing"),
        "blocking_for_issue_workflow": False,
        "codegraph": _compact_codegraph(summary.get("codegraph")),
        "understand_anything": _compact_understand_anything(summary.get("understand_anything")),
        "artifact_refs": artifacts,
        "artifact_roots": _as_list(summary.get("artifact_roots")),
        "warnings": warnings,
        "reason_codes": _as_list(summary.get("reason_codes"))[: max(max_warnings, 0)],
        "provenance": "validation_history_store",
    }


def artifact_ref_paths(context: dict[str, Any]) -> list[str]:
    """Extract compact artifact paths for context_pack.external_source_refs."""

    refs: list[str] = []
    for item in _as_list(context.get("artifact_refs")):
        if not isinstance(item, dict):
            continue
        for key in ("artifact_path", "summary_ref"):
            value = item.get(key)
            if isinstance(value, str) and value and value not in refs:
                refs.append(value)
    return refs


def _compact_codegraph(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {"status": "missing", "freshness": None, "artifact_ref": None}
    index_summary = raw.get("index_summary") if isinstance(raw.get("index_summary"), dict) else {}
    return {
        "status": raw.get("status"),
        "freshness": raw.get("freshness"),
        "generated_at": raw.get("generated_at"),
        "git_commit": raw.get("git_commit"),
        "artifact_ref": raw.get("artifact_path"),
        "summary_ref": raw.get("summary_ref"),
        "summary_exists": raw.get("summary_exists"),
        "index_summary": {
            "files": index_summary.get("files"),
            "nodes": index_summary.get("nodes"),
            "edges": index_summary.get("edges"),
            "up_to_date": index_summary.get("up_to_date"),
        },
    }


def _compact_understand_anything(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {"summary_count": 0, "manifest_ref": None, "latest_summary_refs": []}
    manifest = raw.get("manifest") if isinstance(raw.get("manifest"), dict) else {}
    latest_refs = [
        _compact_artifact_ref(item)
        for item in _as_list(raw.get("latest_summaries"))
        if isinstance(item, dict)
    ][:3]
    return {
        "summary_count": int(raw.get("summary_count") or 0),
        "manifest_ref": manifest.get("artifact_path"),
        "manifest_summary_ref": manifest.get("summary_ref"),
        "latest_summary_refs": latest_refs,
    }


def _compact_artifact_ref(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_type": item.get("artifact_type"),
        "provider": item.get("provider"),
        "status": item.get("status"),
        "freshness": item.get("freshness"),
        "module": item.get("module"),
        "generated_at": item.get("generated_at"),
        "git_commit": item.get("git_commit"),
        "artifact_path": item.get("artifact_path"),
        "summary_ref": item.get("summary_ref"),
    }


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _dedupe_strings(values: list[Any]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value)
        if text and text not in result:
            result.append(text)
    return result
