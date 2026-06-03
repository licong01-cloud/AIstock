from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol


class MemoryStorageProvider(Protocol):
    def list_records(self, kind: str, *, filters: Mapping[str, Any] | None = None, search: str | None = None, limit: int, offset: int = 0) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class MemoryRetrievalResult:
    memory_items: list[dict[str, Any]]
    refs_by_type: dict[str, list[str]]
    matched_branches: list[str]
    route_reason: dict[str, Any]
    omitted_refs: list[str] = field(default_factory=list)


_PERSONAL_RESIDENT_TYPES = {"directive", "user_preference", "habit"}
_TREE_KEYWORDS: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
    (("qe", "experiment", "factor", "ic", "rankic", "seed"), ("project.qe", "project.qe.factor")),
    (("model", "training", "hyperparameter"), ("project.qe.model", "project.model")),
    (("strategy", "portfolio", "backtest", "paper"), ("project.strategy", "project.paper")),
    (("bug", "issue", "validation", "workflow"), ("project.validation", "project.issue")),
    (("dataset", "local data", "sync", "repair"), ("project.local_data", "project.data")),
    (("preference", "prefer", "habit", "remember"), ("personal.preference", "personal.habit")),
    (("directive", "rule", "must", "always"), ("personal.directive", "project.directive")),
)


def select_memory_branches(
    user_message: str | None,
    intent: Any,
    *,
    repo: MemoryStorageProvider,
    runtime_config: Mapping[str, Any] | None,
) -> MemoryRetrievalResult:
    cfg = dict((runtime_config or {}).get("memory_tree") or {})
    query_limits = dict((runtime_config or {}).get("query_limits") or {})
    namespace = str(cfg.get("namespace") or "aistock")
    candidate_limit = int(cfg.get("candidate_limit") or query_limits.get("memory_items_context_pack") or 100)
    max_items = int(cfg.get("max_items") or query_limits.get("memory_items_context_pack") or 12)
    token_budget = int(cfg.get("token_budget") or query_limits.get("default_context_pack_token_budget") or 4000)

    page = repo.list_records(
        "memory_items",
        filters={"namespace": namespace, "approval_status": "approved"},
        limit=max(candidate_limit, max_items),
    )
    candidates = [_with_tree_defaults(item) for item in page.get("items", [])]
    facts = [item for item in candidates if item.get("node_type", "fact") == "fact"]
    resident = [
        item
        for item in facts
        if item.get("scope") == "personal"
        and bool(item.get("resident"))
        and str(item.get("memory_type") or "") in _PERSONAL_RESIDENT_TYPES
    ]

    query = (user_message or "").strip()
    seed_branches = _seed_branches(query, intent)
    matched = [item for item in facts if _matches_branch_or_query(item, seed_branches, query)]
    if not query and not seed_branches:
        matched = list(facts)
    selected_pool = _dedupe_items([*matched, *resident])
    matched_branches = _matched_branches(selected_pool, seed_branches)
    scored = sorted(selected_pool, key=_score_item, reverse=True)
    selected, omitted = _apply_limits(scored, max_items=max_items, token_budget=token_budget)

    refs_by_type: dict[str, list[str]] = {}
    for item in selected:
        refs_by_type.setdefault(str(item.get("memory_type") or "unknown"), []).append(str(item["memory_id"]))

    reason = {
        "algorithm": "memory_tree_v1",
        "query_present": bool(query),
        "intent": getattr(intent, "value", intent),
        "seed_branches": seed_branches,
        "matched_branch_count": len(matched_branches),
        "resident": [item["memory_id"] for item in resident],
        "candidate_count": len(candidates),
        "selected_count": len(selected),
    }
    return MemoryRetrievalResult(
        memory_items=selected,
        refs_by_type=refs_by_type,
        matched_branches=matched_branches,
        route_reason=reason,
        omitted_refs=[str(item["memory_id"]) for item in omitted],
    )


def _seed_branches(user_message: str, intent: Any) -> list[str]:
    text = f"{user_message} {getattr(intent, 'value', intent) or ''}".lower()
    branches: list[str] = []
    for needles, prefixes in _TREE_KEYWORDS:
        if any(needle in text for needle in needles):
            branches.extend(prefixes)
    return _unique(branches)


def _with_tree_defaults(item: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(item)
    subject = str(row.get("subject_key") or row.get("memory_id") or "memory")
    memory_type = str(row.get("memory_type") or "core")
    tree_path = str(row.get("tree_path") or "")
    if not tree_path:
        if subject.startswith(("project.", "personal.")):
            tree_path = subject
        elif memory_type in _PERSONAL_RESIDENT_TYPES:
            tree_path = f"personal.{memory_type}.{_slug(subject)}"
        else:
            tree_path = f"project.{memory_type}.{_slug(subject)}"
    row["tree_path"] = tree_path
    row.setdefault("scope", "personal" if tree_path.startswith("personal.") else "project")
    row.setdefault("node_type", "fact")
    row.setdefault("importance", 0.5)
    row.setdefault("use_count", 0)
    row.setdefault("resident", False)
    return row


def _matches_branch_or_query(item: Mapping[str, Any], seed_branches: list[str], query: str) -> bool:
    path = str(item.get("tree_path") or "")
    if any(path == branch or path.startswith(f"{branch}.") or branch.startswith(f"{path}.") for branch in seed_branches):
        return True
    if not query:
        return False
    haystack = " ".join(
        str(item.get(field) or "")
        for field in ("tree_path", "subject_key", "title", "content_text", "memory_type")
    ).lower()
    terms = [term for term in _terms(query) if len(term) >= 3]
    return bool(terms and any(term in haystack for term in terms))


def _matched_branches(items: list[Mapping[str, Any]], seed_branches: list[str]) -> list[str]:
    branches = list(seed_branches)
    for item in items:
        path = str(item.get("tree_path") or "")
        parts = path.split(".")
        if len(parts) >= 2:
            branches.append(".".join(parts[:2]))
        if len(parts) >= 3:
            branches.append(".".join(parts[:3]))
    return _unique(branches)


def _score_item(item: Mapping[str, Any]) -> tuple[float, str]:
    importance = _float(item.get("importance"), default=0.5)
    recency = _recency_score(item.get("last_used_at") or item.get("updated_at") or item.get("created_at"))
    resident_bonus = 0.05 if item.get("resident") else 0.0
    score = min(1.0, importance * 0.85 + recency * 0.10 + resident_bonus)
    return (score, str(item.get("memory_id") or ""))


def _apply_limits(items: list[dict[str, Any]], *, max_items: int, token_budget: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    selected: list[dict[str, Any]] = []
    omitted: list[dict[str, Any]] = []
    remaining = max(0, token_budget)
    for item in items:
        cost = _estimate_tokens(item)
        if len(selected) >= max_items or cost > remaining:
            omitted.append(item)
            continue
        selected.append(item)
        remaining -= cost
    return selected, omitted


def _estimate_tokens(item: Mapping[str, Any]) -> int:
    text = " ".join(str(item.get(field) or "") for field in ("title", "content_text", "tree_path"))
    return max(1, len(text) // 4)


def _recency_score(value: Any) -> float:
    if not value:
        return 0.0
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age_days = max((datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 86400, 0)
    return 1.0 / (1.0 + age_days / 30.0)


def _float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _terms(text: str) -> list[str]:
    normalized = "".join(ch.lower() if ch.isalnum() else " " for ch in text)
    return normalized.split()


def _slug(value: str) -> str:
    cleaned = [ch.lower() if ch.isalnum() or ch in "._-" else "_" for ch in value.strip()]
    return "".join(cleaned).strip("._-") or "memory"


def _dedupe_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for item in items:
        memory_id = str(item.get("memory_id") or "")
        if not memory_id or memory_id in seen:
            continue
        seen.add(memory_id)
        unique.append(item)
    return unique


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result
