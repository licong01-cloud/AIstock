from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol
from uuid import uuid4


class MemoryWriteProvider(Protocol):
    def list_records(self, kind: str, *, filters: Mapping[str, Any] | None = None, search: str | None = None, limit: int, offset: int = 0) -> dict[str, Any]:
        ...

    def create_record(self, kind: str, row: Mapping[str, Any]) -> dict[str, Any]:
        ...

    def update_record(self, kind: str, record_id: str, updates: Mapping[str, Any]) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class CuratorResult:
    created_branch_ids: list[str] = field(default_factory=list)
    created_memory_ids: list[str] = field(default_factory=list)
    updated_memory_ids: list[str] = field(default_factory=list)
    approval_required_ids: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _Candidate:
    memory_type: str
    scope: str
    tree_path: str
    title: str
    content_text: str
    trust_level: str
    resident: bool
    requires_approval: bool
    importance: float


class MemoryCurator:
    def __init__(self, repo: MemoryWriteProvider, *, namespace: str = "aistock") -> None:
        self.repo = repo
        self.namespace = namespace

    def curate_turn(
        self,
        *,
        user_message: str,
        assistant_message: str,
        conversation_id: str | None,
        user_message_id: str | None,
        assistant_message_id: str | None,
        task_id: str | None,
    ) -> CuratorResult:
        provenance = self._provenance(
            conversation_id=conversation_id,
            user_message_id=user_message_id,
            assistant_message_id=assistant_message_id,
            task_id=task_id,
        )
        if not provenance:
            return CuratorResult(skipped=["missing_provenance"])
        candidates = self._extract_candidates(user_message=user_message, assistant_message=assistant_message)
        if not candidates:
            return CuratorResult(skipped=["no_candidates"])

        created_branch_ids: list[str] = []
        created_memory_ids: list[str] = []
        updated_memory_ids: list[str] = []
        approval_required_ids: list[str] = []
        for candidate in candidates:
            branch = self._ensure_branch(candidate, provenance)
            if branch:
                created_branch_ids.append(str(branch["memory_id"]))
            existing = self._find_existing_fact(candidate)
            if existing:
                updated = self.repo.update_record(
                    "memory_items",
                    str(existing["memory_id"]),
                    {
                        "content_text": candidate.content_text,
                        "content_json": {"text": candidate.content_text, "self_edit": True},
                        "importance": max(float(existing.get("importance") or 0), candidate.importance),
                        "use_count": int(existing.get("use_count") or 0) + 1,
                        "last_used_at": _now_iso(),
                        "provenance_json": provenance,
                    },
                )
                updated_memory_ids.append(str(updated["memory_id"]))
                if updated.get("approval_status") == "draft":
                    approval_required_ids.append(str(updated["memory_id"]))
                continue
            created = self.repo.create_record("memory_items", self._fact_row(candidate, provenance))
            created_memory_ids.append(str(created["memory_id"]))
            if created.get("approval_status") == "draft":
                approval_required_ids.append(str(created["memory_id"]))
        return CuratorResult(
            created_branch_ids=created_branch_ids,
            created_memory_ids=created_memory_ids,
            updated_memory_ids=updated_memory_ids,
            approval_required_ids=approval_required_ids,
        )

    def _extract_candidates(self, *, user_message: str, assistant_message: str) -> list[_Candidate]:
        del assistant_message
        text = user_message.strip()
        lower = text.lower()
        candidates: list[_Candidate] = []
        if "project directive:" in lower or "project rule:" in lower:
            content = _after_marker(text, ":", default=text)
            candidates.append(
                _Candidate(
                    memory_type="directive",
                    scope="project",
                    tree_path="project.directive.workflow",
                    title="Project directive",
                    content_text=content,
                    trust_level="user_stated",
                    resident=False,
                    requires_approval=True,
                    importance=0.9,
                )
            )
        if "remember preference:" in lower or "prefer" in lower or "preference:" in lower:
            content = _after_marker(text, ":", default=text)
            candidates.append(
                _Candidate(
                    memory_type="user_preference",
                    scope="personal",
                    tree_path="personal.preference.response",
                    title="Response preference",
                    content_text=content,
                    trust_level="user_stated",
                    resident=True,
                    requires_approval=False,
                    importance=0.85,
                )
            )
        if "remember habit:" in lower or "habit:" in lower:
            content = _after_marker(text, ":", default=text)
            candidates.append(
                _Candidate(
                    memory_type="habit",
                    scope="personal",
                    tree_path="personal.habit.workflow",
                    title="Workflow habit",
                    content_text=content,
                    trust_level="user_stated",
                    resident=False,
                    requires_approval=False,
                    importance=0.75,
                )
            )
        if ("always" in lower or "must" in lower) and "project directive:" not in lower and "project rule:" not in lower:
            candidates.append(
                _Candidate(
                    memory_type="directive",
                    scope="personal",
                    tree_path="personal.directive.response",
                    title="Personal directive",
                    content_text=text,
                    trust_level="user_stated",
                    resident=True,
                    requires_approval=False,
                    importance=0.9,
                )
            )
        return _dedupe_candidates(candidates)

    def _ensure_branch(self, candidate: _Candidate, provenance: Mapping[str, Any]) -> dict[str, Any] | None:
        page = self.repo.list_records(
            "memory_items",
            filters={
                "namespace": self.namespace,
                "tree_path": candidate.tree_path,
                "node_type": "branch",
                "scope": candidate.scope,
            },
            limit=1,
        )
        if page.get("items"):
            return None
        return self.repo.create_record(
            "memory_items",
            {
                "memory_id": _new_memory_id(),
                "memory_type": "core",
                "namespace": self.namespace,
                "subject_key": candidate.tree_path,
                "title": f"{candidate.title} branch",
                "content_json": {"branch": candidate.tree_path},
                "content_text": f"Auto-created memory branch for {candidate.tree_path}",
                "source_type": "curator",
                "source_ref": _source_ref(provenance),
                "confidence": 1.0,
                "approval_status": "approved",
                "risk_level": "low",
                "evidence_refs": [_source_ref(provenance)],
                "checksum": _checksum_like(candidate.tree_path, "branch"),
                "created_by": "assistant_curator",
                "tree_path": candidate.tree_path,
                "parent_key": _parent_path(candidate.tree_path),
                "node_type": "branch",
                "scope": candidate.scope,
                "importance": candidate.importance,
                "last_used_at": None,
                "use_count": 0,
                "auto_created": True,
                "trust_level": candidate.trust_level,
                "provenance_json": dict(provenance),
                "resident": False,
            },
        )

    def _find_existing_fact(self, candidate: _Candidate) -> dict[str, Any] | None:
        page = self.repo.list_records(
            "memory_items",
            filters={
                "namespace": self.namespace,
                "memory_type": candidate.memory_type,
                "scope": candidate.scope,
                "tree_path": candidate.tree_path,
                "node_type": "fact",
            },
            limit=50,
        )
        normalized = _normalize(candidate.content_text)
        for item in page.get("items", []):
            if _normalize(str(item.get("content_text") or "")) == normalized:
                return item
        return None

    def _fact_row(self, candidate: _Candidate, provenance: Mapping[str, Any]) -> dict[str, Any]:
        status = "draft" if candidate.requires_approval else "approved"
        source_ref = _source_ref(provenance)
        return {
            "memory_id": _new_memory_id(),
            "memory_type": candidate.memory_type,
            "namespace": self.namespace,
            "subject_key": f"{candidate.tree_path}.{_slug(candidate.content_text)[:48]}",
            "title": candidate.title,
            "content_json": {"text": candidate.content_text},
            "content_text": candidate.content_text,
            "source_type": "curator",
            "source_ref": source_ref,
            "confidence": 1.0 if candidate.trust_level == "user_stated" else 0.6,
            "approval_status": status,
            "risk_level": "medium" if candidate.requires_approval else "low",
            "evidence_refs": [source_ref],
            "checksum": _checksum_like(candidate.tree_path, candidate.content_text),
            "created_by": "assistant_curator",
            "tree_path": candidate.tree_path,
            "parent_key": candidate.tree_path,
            "node_type": "fact",
            "scope": candidate.scope,
            "importance": candidate.importance,
            "last_used_at": None,
            "use_count": 0,
            "auto_created": True,
            "trust_level": candidate.trust_level,
            "provenance_json": dict(provenance),
            "resident": candidate.resident,
        }

    @staticmethod
    def _provenance(
        *,
        conversation_id: str | None,
        user_message_id: str | None,
        assistant_message_id: str | None,
        task_id: str | None,
    ) -> dict[str, Any]:
        if not conversation_id or not user_message_id:
            return {}
        return {
            "source": "chat_turn",
            "conversation_id": conversation_id,
            "user_message_id": user_message_id,
            "assistant_message_id": assistant_message_id,
            "task_id": task_id,
            "captured_at": _now_iso(),
        }


def _after_marker(text: str, marker: str, *, default: str) -> str:
    if marker not in text:
        return default.strip()
    return text.split(marker, 1)[1].strip() or default.strip()


def _parent_path(path: str) -> str | None:
    parts = path.split(".")
    if len(parts) <= 1:
        return None
    return ".".join(parts[:-1])


def _source_ref(provenance: Mapping[str, Any]) -> str:
    conversation_id = str(provenance.get("conversation_id") or "conversation")
    user_message_id = str(provenance.get("user_message_id") or "message")
    return f"conversation://{conversation_id}/{user_message_id}"


def _new_memory_id() -> str:
    return f"mem_{uuid4().hex}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize(text: str) -> str:
    return " ".join(text.lower().split())


def _slug(value: str) -> str:
    cleaned = [ch.lower() if ch.isalnum() or ch in "._-" else "_" for ch in value.strip()]
    return "".join(cleaned).strip("._-") or "memory"


def _checksum_like(*parts: str) -> str:
    value = "|".join(parts)
    return f"curator-{abs(hash(value))}"


def _dedupe_candidates(candidates: list[_Candidate]) -> list[_Candidate]:
    seen: set[tuple[str, str, str, str]] = set()
    result: list[_Candidate] = []
    for candidate in candidates:
        key = (candidate.memory_type, candidate.scope, candidate.tree_path, _normalize(candidate.content_text))
        if key in seen:
            continue
        seen.add(key)
        result.append(candidate)
    return result
