"""Read-only code-intelligence context for Research Assistant packs."""

from __future__ import annotations

import hashlib
import importlib.util
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from backend.services.validation.history_store import ValidationHistoryStore

CODE_INTELLIGENCE_CONTEXT_SCHEMA = "aistock_research_assistant_code_intelligence_context_v1"
CODE_CONTEXT_REFS_SCHEMA = "aistock_research_assistant_code_context_refs_v1"
DEFAULT_MAX_ARTIFACT_REFS = 8
DEFAULT_MAX_WARNINGS = 6
DEFAULT_CODEGRAPH_MAX_CONTEXT_SYMBOLS_T1 = 8
DEFAULT_CODEGRAPH_MAX_CONTEXT_SYMBOLS_T2 = 16
DEFAULT_CODEGRAPH_MAX_IMPACT_DEPTH = 3
DEFAULT_UA_MAX_CONTEXT_NODES_T2 = 20
DEFAULT_UA_MAX_CONTEXT_NODES_T3 = 60

PATH_RE = re.compile(
    r"(?P<path>(?:backend|frontend|scripts|tests|configs|docs)[\\/][A-Za-z0-9_.@()\\/ -]+?"
    r"\.(?:py|tsx|ts|js|jsx|sql|ya?ml|md|json))"
)
MODULE_RE = re.compile(r"\b(?P<module>(?:backend|frontend|scripts|tests|configs)(?:\.[A-Za-z_][A-Za-z0-9_]*){1,})\b")
BACKTICK_RE = re.compile(r"`([^`]+)`")
SYMBOL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\b")
CODE_INTENT_RE = re.compile(
    r"(code|class|function|method|module|path|file|import|call chain|caller|callee|affected test|"
    r"代码|函数|方法|类|模块|文件|路径|调用链|调用方|影响测试|受影响测试|改动|实现|入口)"
)
SYMBOL_STOPWORDS = {
    "code",
    "class",
    "function",
    "method",
    "module",
    "path",
    "file",
    "import",
    "backend",
    "frontend",
    "scripts",
    "tests",
    "configs",
    "docs",
}


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


def build_query_code_context(
    *,
    user_query: str | None,
    task_id: str | None,
    repo_root: Path,
    token_budget: int | None = None,
    adapter_module: Any | None = None,
    skip_external: bool = False,
    cache_lookup: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build query-scoped code refs through the existing code-intelligence adapter."""

    query = (user_query or "").strip()
    generated_at = _utc_now()
    limits = _load_code_intelligence_limits(repo_root, token_budget=token_budget)
    scope = parse_code_query_scope(query, repo_root=repo_root)
    limit_reason_codes = _as_list(limits.get("reason_codes"))
    limit_warnings = _as_list(limits.get("warnings"))
    base: dict[str, Any] = {
        "schema_version": CODE_CONTEXT_REFS_SCHEMA,
        "status": "skipped",
        "query": query,
        "scope": scope,
        "code_context_refs": [],
        "reason_codes": list(limit_reason_codes),
        "warnings": list(limit_warnings),
        "limits": limits,
        "adapter_contract": {
            "provider": "codegraph",
            "deterministic_ast_only": True,
            "embedding_used": False,
            "llm_repo_scan_used": False,
        },
        "generated_at": generated_at,
        "as_of": generated_at,
    }
    if not query:
        base["reason_codes"].append("empty_query")
        base["warnings"].append("code context skipped: empty query")
        return base
    if not scope["is_code_query"]:
        base["reason_codes"].append("non_code_query")
        base["warnings"].append("code context skipped: non-code query")
        return base
    if not scope["concrete"]:
        base["reason_codes"].append("no_code_scope_detected")
        base["warnings"].append("code context skipped: no concrete code scope detected")
        return base

    cache_payload = _query_scope_cache_payload(query=query, task_id=task_id, scope=scope, limits=limits)
    if cache_lookup is not None:
        try:
            cached_refs = cache_lookup(cache_payload)
        except Exception as exc:  # noqa: BLE001 - cache failure is surfaced and adapter remains authoritative.
            base["reason_codes"].append("code_context_cache_unavailable")
            base["warnings"].append(f"code context cache lookup failed: {type(exc).__name__}: {exc}")
        else:
            cache_reason_codes = _as_list(cached_refs.get("reason_codes"))
            cache_warnings = _as_list(cached_refs.get("warnings"))
            base["reason_codes"].extend(cache_reason_codes)
            base["warnings"].extend(cache_warnings)
            refs = _as_list(cached_refs.get("code_context_refs"))
            if refs and cached_refs.get("status") == "hit":
                base.update(
                    {
                        "status": "ok",
                        "code_context_refs": refs,
                        "reason_codes": _dedupe_strings([*base["reason_codes"], "code_context_cache_hit"]),
                        "warnings": _dedupe_strings(base["warnings"]),
                        "as_of": cached_refs.get("as_of") or refs[0].get("as_of") or generated_at,
                        "generated_at": cached_refs.get("generated_at") or generated_at,
                        "cache": {
                            "status": "hit",
                            "expected_ref_ids": cache_payload["expected_ref_ids"],
                        },
                    }
                )
                return base
            if cached_refs.get("status") != "unavailable" and "code_context_cache_miss" not in base["reason_codes"]:
                base["reason_codes"].append("code_context_cache_miss")
            if cached_refs.get("status") != "unavailable" and not cache_warnings:
                base["warnings"].append("code context cache miss; invoking adapter")

    item_id = _context_item_id(task_id=task_id, query=query)
    changed_files = list(scope["paths"])
    try:
        adapter = adapter_module or _load_adapter(repo_root)
        context = adapter.build_context_artifacts(
            item_id=item_id,
            query=query,
            changed_files=changed_files,
            root=repo_root,
            max_symbols=int(limits["max_context_symbols"]),
            skip_external=skip_external,
        )
        affected = adapter.build_affected_tests_artifact(
            item_id=item_id,
            changed_files=changed_files,
            root=repo_root,
            skip_external=skip_external,
        )
    except FileNotFoundError as exc:
        base["reason_codes"].append("code_intelligence_adapter_missing")
        base["warnings"].append(str(exc))
        return base
    except Exception as exc:  # noqa: BLE001 - explicit degraded route, not silent.
        base["reason_codes"].append("code_intelligence_adapter_failed")
        base["warnings"].append(f"{type(exc).__name__}: {exc}")
        return base

    refs = _build_code_context_refs(
        query=query,
        task_id=task_id,
        scope=scope,
        context=context,
        affected=affected,
        limits=limits,
    )
    reason_codes = _artifact_reason_codes(context, affected)
    base.update(
        {
            "status": "ok" if refs else "skipped",
            "code_context_refs": refs,
            "context_artifact_ref": context.get("context_markdown"),
            "manifest_artifact_ref": context.get("manifest_path"),
            "affected_tests_ref": affected.get("artifact_path"),
            "reason_codes": _dedupe_strings([*base["reason_codes"], *reason_codes, "code_context_refs_built" if refs else "no_code_context_refs_built"]),
            "warnings": _dedupe_strings(base["warnings"]),
            "as_of": context.get("generated_at") or affected.get("generated_at") or generated_at,
            "generated_at": context.get("generated_at") or generated_at,
        }
    )
    return base


def parse_code_query_scope(query: str, *, repo_root: Path) -> dict[str, Any]:
    """Extract explicit path/module/symbol scope without walking the repository."""

    normalized_query = query.replace("\\", "/")
    paths = _dedupe_strings([_normalize_repo_path(match.group("path")) for match in PATH_RE.finditer(normalized_query)])
    modules = _dedupe_strings([match.group("module") for match in MODULE_RE.finditer(query)])
    converted_paths = [_module_to_path(module, repo_root=repo_root) for module in modules]
    paths = _dedupe_strings([*paths, *[path for path in converted_paths if path]])

    quoted_symbols: list[str] = []
    for quoted in BACKTICK_RE.findall(query):
        if "/" in quoted or "\\" in quoted or "." in quoted and quoted.rsplit(".", 1)[-1] in {"py", "ts", "tsx", "sql", "yaml", "yml"}:
            continue
        quoted_symbols.append(quoted.strip())
    symbols = [
        symbol
        for symbol in _dedupe_strings([*quoted_symbols, *_candidate_symbols(query)])
        if symbol.lower() not in SYMBOL_STOPWORDS and not symbol.startswith(("backend.", "frontend.", "scripts.", "tests.", "configs."))
    ]
    is_code_query = bool(paths or modules or symbols or CODE_INTENT_RE.search(query))
    concrete = bool(paths or modules or symbols)
    if paths:
        primary_type = "path"
    elif modules:
        primary_type = "module"
    elif symbols:
        primary_type = "symbol"
    else:
        primary_type = "unknown"
    return {
        "schema_version": "aistock_research_assistant_code_query_scope_v1",
        "is_code_query": is_code_query,
        "concrete": concrete,
        "primary_type": primary_type,
        "paths": paths,
        "modules": modules,
        "symbols": symbols[:16],
    }


def expected_code_context_ref_ids(*, user_query: str | None, task_id: str | None, repo_root: Path, token_budget: int | None = None) -> dict[str, Any]:
    """Return deterministic cache keys for a query without invoking the adapter."""

    query = (user_query or "").strip()
    limits = _load_code_intelligence_limits(repo_root, token_budget=token_budget)
    scope = parse_code_query_scope(query, repo_root=repo_root)
    payload = _query_scope_cache_payload(query=query, task_id=task_id, scope=scope, limits=limits)
    payload["is_cacheable"] = bool(query and scope.get("is_code_query") and scope.get("concrete"))
    payload["reason_codes"] = _as_list(limits.get("reason_codes"))
    payload["warnings"] = _as_list(limits.get("warnings"))
    return payload


def code_context_artifact_paths(context: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for key in ("context_artifact_ref", "manifest_artifact_ref", "affected_tests_ref"):
        value = context.get(key)
        if isinstance(value, str) and value and value not in refs:
            refs.append(value)
    for item in _as_list(context.get("code_context_refs")):
        if not isinstance(item, dict):
            continue
        for key in ("context_artifact_ref", "manifest_artifact_ref", "affected_tests_ref"):
            value = item.get(key)
            if isinstance(value, str) and value and value not in refs:
                refs.append(value)
    return refs


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


def _load_code_intelligence_limits(repo_root: Path, *, token_budget: int | None) -> dict[str, Any]:
    catalog = _load_code_intelligence_catalog(repo_root)
    codegraph = catalog.get("codegraph") if isinstance(catalog.get("codegraph"), dict) else {}
    ua = catalog.get("understand_anything") if isinstance(catalog.get("understand_anything"), dict) else {}
    use_t2 = bool(token_budget and token_budget >= 3000)
    max_symbols_key = "max_context_symbols_t2" if use_t2 else "max_context_symbols_t1"
    max_nodes_key = "max_context_nodes_t3" if use_t2 else "max_context_nodes_t2"
    return {
        "max_context_symbols": int(codegraph.get(max_symbols_key) or (DEFAULT_CODEGRAPH_MAX_CONTEXT_SYMBOLS_T2 if use_t2 else DEFAULT_CODEGRAPH_MAX_CONTEXT_SYMBOLS_T1)),
        "max_impact_depth": int(codegraph.get("max_impact_depth") or DEFAULT_CODEGRAPH_MAX_IMPACT_DEPTH),
        "max_context_nodes": int(ua.get(max_nodes_key) or (DEFAULT_UA_MAX_CONTEXT_NODES_T3 if use_t2 else DEFAULT_UA_MAX_CONTEXT_NODES_T2)),
        "reason_codes": _as_list(catalog.get("reason_codes")),
        "warnings": _as_list(catalog.get("warnings")),
    }


def _load_code_intelligence_catalog(repo_root: Path) -> dict[str, Any]:
    path = repo_root / "tests" / "aistock_validation" / "catalog" / "code_intelligence.yaml"
    if not path.exists():
        return {}
    try:
        import yaml  # type: ignore

        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if isinstance(payload, dict):
            return payload
        return {
            "reason_codes": ["code_intelligence_catalog_parse_failed"],
            "warnings": [f"code intelligence catalog is not a mapping: {path}"],
        }
    except Exception as exc:  # noqa: BLE001 - explicit fallback to default limits.
        return {
            "reason_codes": ["code_intelligence_catalog_parse_failed"],
            "warnings": [f"code intelligence catalog parse failed: {type(exc).__name__}: {exc}"],
        }


def _query_scope_cache_payload(*, query: str, task_id: str | None, scope: dict[str, Any], limits: dict[str, Any]) -> dict[str, Any]:
    candidates = _scope_candidates(scope)[: int(limits["max_context_symbols"])]
    expected_ref_ids = [
        _stable_ref_id(task_id=task_id, query_scope=f"{candidate['type']}:{candidate['value']}", index=index)
        for index, candidate in enumerate(candidates)
    ]
    return {
        "schema_version": "aistock_research_assistant_code_context_cache_lookup_v1",
        "task_id": task_id,
        "query": query,
        "scope": scope,
        "limits": limits,
        "expected_ref_ids": expected_ref_ids,
    }


def _load_adapter(repo_root: Path) -> Any:
    adapter_path = repo_root / "scripts" / "code_intelligence_adapter.py"
    if not adapter_path.exists():
        raise FileNotFoundError(f"code intelligence adapter missing: {adapter_path}")
    spec = importlib.util.spec_from_file_location("aistock_code_intelligence_adapter_for_ra", adapter_path)
    if spec is None or spec.loader is None:
        raise FileNotFoundError(f"code intelligence adapter cannot be loaded: {adapter_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _context_item_id(*, task_id: str | None, query: str) -> str:
    digest = hashlib.sha1(f"{task_id or 'no-task'}|{query}".encode("utf-8")).hexdigest()[:12]
    return f"ra-code-context-{digest}"


def _build_code_context_refs(
    *,
    query: str,
    task_id: str | None,
    scope: dict[str, Any],
    context: dict[str, Any],
    affected: dict[str, Any],
    limits: dict[str, int],
) -> list[dict[str, Any]]:
    as_of = str(context.get("generated_at") or affected.get("generated_at") or _utc_now())
    commit = context.get("git_commit") or (context.get("codegraph_status") or {}).get("git_commit")
    suggested_tests = [str(item) for item in _as_list(affected.get("suggested_tests"))][: int(limits["max_context_symbols"])]
    candidates = _scope_candidates(scope)
    refs: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates[: int(limits["max_context_symbols"])]):
        provenance = {
            "commit": commit or "unknown",
            "file": candidate.get("file") or "unknown",
            "symbol": candidate.get("symbol") or candidate.get("value") or "unknown",
            "generated_at": as_of,
        }
        query_scope = f"{candidate['type']}:{candidate['value']}"
        manifest = {
            "schema_version": "aistock_research_assistant_code_ref_manifest_v1",
            "query": query,
            "query_scope": query_scope,
            "context_artifact_ref": context.get("context_markdown"),
            "manifest_artifact_ref": context.get("manifest_path"),
            "affected_tests_ref": affected.get("artifact_path"),
            "affected_tests": suggested_tests,
            "impact_depth": min(int(limits["max_impact_depth"]), DEFAULT_CODEGRAPH_MAX_IMPACT_DEPTH),
            "context_status": context.get("status"),
            "context_quality": context.get("context_quality"),
            "affected_quality": affected.get("quality"),
            "summary_first": True,
            "raw_context_embedded": False,
        }
        refs.append(
            {
                "code_ref_id": _stable_ref_id(task_id=task_id, query_scope=query_scope, index=index),
                "query_scope": query_scope,
                "query_scope_type": candidate["type"],
                "source": "codegraph",
                "summary": _ref_summary(candidate, suggested_tests=suggested_tests),
                "provenance": provenance,
                "as_of": as_of,
                "manifest_json": manifest,
                "context_artifact_ref": context.get("context_markdown"),
                "manifest_artifact_ref": context.get("manifest_path"),
                "affected_tests_ref": affected.get("artifact_path"),
                "affected_tests": suggested_tests,
            }
        )
    return refs


def _scope_candidates(scope: dict[str, Any]) -> list[dict[str, str | None]]:
    candidates: list[dict[str, str | None]] = []
    scoped_paths = [str(path) for path in scope.get("paths") or []]
    default_file = scoped_paths[0] if scoped_paths else None
    for path in scoped_paths:
        candidates.append({"type": "path", "value": str(path), "file": str(path), "symbol": None})
    for module in scope.get("modules") or []:
        file_path = _module_guess_path(str(module))
        candidates.append({"type": "module", "value": str(module), "file": file_path, "symbol": str(module).split(".")[-1]})
    for symbol in scope.get("symbols") or []:
        candidates.append({"type": "symbol", "value": str(symbol), "file": _file_from_dotted_symbol(str(symbol)) or default_file, "symbol": str(symbol).split(".")[-1]})
    deduped: list[dict[str, str | None]] = []
    seen: set[tuple[str | None, str | None]] = set()
    for item in candidates:
        key = (item.get("file"), item.get("symbol") or item.get("value"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _stable_ref_id(*, task_id: str | None, query_scope: str, index: int) -> str:
    digest = hashlib.sha1(f"{task_id or 'no-task'}|{query_scope}|{index}".encode("utf-8")).hexdigest()[:16]
    return f"code_ref_{digest}"


def _ref_summary(candidate: dict[str, str | None], *, suggested_tests: list[str]) -> str:
    scope = f"{candidate['type']}={candidate['value']}"
    tests = ", ".join(suggested_tests[:3]) if suggested_tests else "no affected-test suggestion"
    return f"Code context scoped to {scope}; affected tests: {tests}."


def _artifact_reason_codes(context: dict[str, Any], affected: dict[str, Any]) -> list[str]:
    reason_codes: list[str] = []
    fallback = context.get("fallback") if isinstance(context.get("fallback"), dict) else {}
    if fallback.get("used"):
        reason_codes.append(str(fallback.get("reason") or "codegraph_context_fallback"))
    if context.get("status") not in {"ok", "repo_index_ready"}:
        reason_codes.append(f"code_context_status_{context.get('status') or 'unknown'}")
    affected_fallback = affected.get("fallback") if isinstance(affected.get("fallback"), dict) else {}
    if affected_fallback.get("used"):
        reason_codes.append(str(affected_fallback.get("reason") or "codegraph_affected_fallback"))
    return _dedupe_strings(reason_codes)


def _candidate_symbols(query: str) -> list[str]:
    symbols: list[str] = []
    for match in SYMBOL_RE.finditer(query):
        token = match.group(1).strip()
        if len(token) < 3:
            continue
        if not ("_" in token or "." in token or any(char.isupper() for char in token)):
            continue
        symbols.append(token)
    return symbols


def _normalize_repo_path(path: str) -> str:
    normalized = re.sub(r"\s+", "", path.strip().strip("`'\"")).replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _module_to_path(module: str, *, repo_root: Path) -> str | None:
    candidate = _module_guess_path(module)
    if candidate and (repo_root / candidate).exists():
        return candidate
    return None


def _module_guess_path(module: str) -> str | None:
    if module.startswith(("backend.", "frontend.", "scripts.", "tests.", "configs.")):
        return module.replace(".", "/") + ".py"
    if module.startswith("research_assistant."):
        return "backend/services/" + module.replace(".", "/") + ".py"
    return None


def _file_from_dotted_symbol(symbol: str) -> str | None:
    parts = symbol.split(".")
    if len(parts) >= 3 and parts[0] in {"backend", "scripts", "tests"}:
        return "/".join(parts[:-1]) + ".py"
    return None


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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
