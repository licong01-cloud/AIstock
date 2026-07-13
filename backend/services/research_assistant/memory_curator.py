from __future__ import annotations

from collections.abc import Callable
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


SemanticMemoryCandidateExtractor = Callable[..., list[Mapping[str, Any]]]


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
    def __init__(
        self,
        repo: MemoryWriteProvider,
        *,
        namespace: str = "aistock",
        semantic_extractor: SemanticMemoryCandidateExtractor | None = None,
    ) -> None:
        self.repo = repo
        self.namespace = namespace
        self.semantic_extractor = semantic_extractor

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

    def create_reflection_memory(self, memory_row: Mapping[str, Any]) -> dict[str, Any]:
        row = dict(memory_row)
        if row.get("source_type") != "reflection_card":
            raise ValueError("reflection memory source_type must be reflection_card")
        if row.get("memory_type") != "episodic":
            raise ValueError("reflection memory_type must be episodic")
        if row.get("scope") != "personal" or not str(row.get("tree_path") or "").startswith("personal.episodic."):
            raise ValueError("reflection memory must target personal.episodic.*")
        if row.get("approval_status") != "approved" or row.get("risk_level") != "low":
            raise ValueError("reflection memory must be approved low-risk")
        return self.repo.create_record("memory_items", row)

    def _extract_candidates(self, *, user_message: str, assistant_message: str) -> list[_Candidate]:
        if self.semantic_extractor is not None:
            return _dedupe_candidates(self._semantic_candidates(user_message=user_message, assistant_message=assistant_message))
        return _dedupe_candidates(self._seed_candidates(user_message=user_message))

    def _semantic_candidates(self, *, user_message: str, assistant_message: str) -> list[_Candidate]:
        if self.semantic_extractor is None:
            return []
        raw_candidates = self.semantic_extractor(user_message=user_message, assistant_message=assistant_message)
        if not isinstance(raw_candidates, list):
            raise ValueError(
                "reason_code=memory_curator_invalid_semantic_candidates; semantic_memory_candidates must be a list: "
                f"actual_type={type(raw_candidates).__name__}"
            )
        candidates: list[_Candidate] = []
        for index, item in enumerate(raw_candidates):
            if not isinstance(item, Mapping):
                raise ValueError(
                    "reason_code=memory_curator_invalid_semantic_candidate; semantic_memory_candidates entries must be objects: "
                    f"index={index}; actual_type={type(item).__name__}"
                )
            candidates.append(_candidate_from_semantic_mapping(item, index=index))
        return candidates

    @staticmethod
    def _seed_candidates(*, user_message: str) -> list[_Candidate]:
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
        return candidates

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


def _candidate_from_semantic_mapping(item: Mapping[str, Any], *, index: int) -> _Candidate:
    memory_type = _required_str(item, "memory_type", index=index)
    scope = _required_str(item, "scope", index=index)
    tree_path = _required_str(item, "tree_path", index=index)
    title = _required_str(item, "title", index=index)
    content_text = _required_str(item, "content_text", index=index)
    trust_level = _required_str(item, "trust_level", index=index)
    resident = _required_bool(item, "resident", index=index)
    requires_approval = _required_bool(item, "requires_approval", index=index)
    importance = _required_float(item, "importance", index=index)
    allowed_memory_types = {"user_preference", "habit", "directive", "task_state", "analysis_note"}
    if memory_type not in allowed_memory_types:
        raise ValueError(
            "reason_code=memory_curator_invalid_semantic_candidate; semantic memory candidate has invalid memory_type: "
            f"index={index}; memory_type={memory_type}; allowed={sorted(allowed_memory_types)}"
        )
    if scope not in {"personal", "project"}:
        raise ValueError(f"reason_code=memory_curator_invalid_semantic_candidate; semantic memory candidate has invalid scope: index={index}; scope={scope}")
    if not tree_path.startswith(f"{scope}."):
        raise ValueError(
            "reason_code=memory_curator_invalid_semantic_candidate; semantic memory candidate tree_path must stay inside scope: "
            f"index={index}; scope={scope}; tree_path={tree_path}"
        )
    if trust_level not in {"user_stated", "assistant_inferred"}:
        raise ValueError(
            "reason_code=memory_curator_invalid_semantic_candidate; semantic memory candidate has invalid trust_level: "
            f"index={index}; trust_level={trust_level}"
        )
    if scope == "project" and not requires_approval:
        raise ValueError(
            "reason_code=memory_curator_invalid_semantic_candidate; project semantic memory candidates require draft approval: "
            f"index={index}; tree_path={tree_path}; requires_approval={requires_approval}"
        )
    if not 0 <= importance <= 1:
        raise ValueError(
            "reason_code=memory_curator_invalid_semantic_candidate; semantic memory candidate importance must be between 0 and 1: "
            f"index={index}; importance={importance}"
        )
    return _Candidate(
        memory_type=memory_type,
        scope=scope,
        tree_path=tree_path,
        title=title,
        content_text=content_text,
        trust_level=trust_level,
        resident=resident,
        requires_approval=requires_approval,
        importance=importance,
    )


def _required_str(item: Mapping[str, Any], field: str, *, index: int) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            "reason_code=memory_curator_invalid_semantic_candidate; semantic memory candidate field must be a non-empty string: "
            f"index={index}; field={field}; actual_type={type(value).__name__}"
        )
    return value.strip()


def _required_bool(item: Mapping[str, Any], field: str, *, index: int) -> bool:
    value = item.get(field)
    if not isinstance(value, bool):
        raise ValueError(
            "reason_code=memory_curator_invalid_semantic_candidate; semantic memory candidate field must be a bool: "
            f"index={index}; field={field}; actual_type={type(value).__name__}"
        )
    return value


def _required_float(item: Mapping[str, Any], field: str, *, index: int) -> float:
    value = item.get(field)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(
            "reason_code=memory_curator_invalid_semantic_candidate; semantic memory candidate field must be a number: "
            f"index={index}; field={field}; actual_type={type(value).__name__}"
        )
    return float(value)


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
