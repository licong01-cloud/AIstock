"""Small JSON contracts for P1 research records (design sections 5 and 6)."""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

TASK_TYPES = {"new_factor", "diagnosis", "improvement", "redundancy"}
STATUSES = {"active", "paused", "completed"}
RECORD_TYPES = {"progress", "attempt", "result", "decision", "request", "correction"}
TASK_FIELDS = {"title", "objective", "task_type", "status", "phase", "completed_summary",
               "next_action", "blocking_reason", "factor_names", "context_json"}


class ResearchError(ValueError):
    def __init__(self, code: str, detail: str, **context):
        super().__init__(detail)
        self.code, self.detail, self.context = code, detail, context

    def as_dict(self):
        return {"code": self.code, "detail": self.detail, **self.context}


def identifier(value, name: str) -> str:
    try:
        return str(UUID(str(value)))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ResearchError("invalid_request", f"{name} must be a UUID") from exc


def json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (UUID, Path)):
        return str(value)
    raise TypeError(f"Unsupported JSON type: {type(value).__name__}")


def encode(value) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, default=json_default)


def read_json(path: Path) -> dict:
    def invalid_constant(value):
        raise ResearchError("invalid_request", f"Non-finite JSON constant: {value}")
    value = json.loads(path.read_text(encoding="utf-8-sig"), parse_constant=invalid_constant)
    return json_object(value)


def json_object(value) -> dict:
    if not isinstance(value, dict) or any(not isinstance(k, str) for k in value):
        raise ResearchError("invalid_request", "Expected a JSON object with string keys")
    try:
        return json.loads(encode(value))
    except (ValueError, TypeError) as exc:
        raise ResearchError("invalid_request", "JSON must contain finite serializable values") from exc


def task_patch(value: dict, *, create=False) -> dict:
    value = json_object(value)
    unknown = set(value) - TASK_FIELDS
    if unknown:
        raise ResearchError("invalid_request", f"Unknown task fields: {sorted(unknown)}")
    if create:
        for field in ("title", "objective", "task_type", "context_json"):
            if field not in value:
                raise ResearchError("invalid_request", f"Missing task field: {field}")
    for key, item in value.items():
        if key == "factor_names":
            if not isinstance(item, list) or any(not isinstance(n, str) or not n.strip() for n in item):
                raise ResearchError("invalid_request", "factor_names must be a list of names")
        elif key == "context_json":
            json_object(item)
        elif not isinstance(item, str):
            raise ResearchError("invalid_request", f"{key} must be text")
    if "task_type" in value and value["task_type"] not in TASK_TYPES:
        raise ResearchError("invalid_request", "Unknown task_type")
    if "status" in value and value["status"] not in STATUSES:
        raise ResearchError("invalid_request", "Unknown task status")
    for key in ("title", "objective"):
        if key in value and not value[key].strip():
            raise ResearchError("invalid_request", f"{key} must not be blank")
    return value


def request(value: dict, *, create=False) -> dict:
    value = json_object(value)
    allowed = {"task_id", "record_id", "task", "summary", "payload"} if create else {
        "task_id", "record_id", "expected_revision", "record_type", "attempt_id",
        "related_record_id", "summary", "payload", "task_update"}
    if set(value) - allowed:
        raise ResearchError("invalid_request", f"Unknown request fields: {sorted(set(value) - allowed)}")
    for key in ("task_id", "record_id"):
        value[key] = identifier(value.get(key), key)
    if not isinstance(value.get("summary"), str) or not value["summary"].strip():
        raise ResearchError("invalid_request", "A non-empty summary is required")
    value["payload"] = json_object(value.get("payload", {}))
    if create:
        value["task"] = task_patch(value.get("task", {}), create=True)
    else:
        if type(value.get("expected_revision")) is not int or value["expected_revision"] < 1:
            raise ResearchError("invalid_request", "expected_revision must be a positive integer")
        if value.get("record_type") not in RECORD_TYPES:
            raise ResearchError("invalid_request", "Unknown record_type")
        for key in ("attempt_id", "related_record_id"):
            if value.get(key) is not None:
                value[key] = identifier(value[key], key)
        if value["record_type"] == "attempt" and not value.get("attempt_id"):
            raise ResearchError("invalid_request", "attempt_id is required")
        if value["record_type"] == "correction" and not value.get("related_record_id"):
            raise ResearchError("invalid_request", "correction requires related_record_id")
        value["task_update"] = task_patch(value.get("task_update", {}))
    return value


def response(*, result=None, task_id=None, record_id=None, revision=None,
             applied=None, replayed=None, error=None):
    return {"ok": error is None, "task_id": task_id, "record_id": record_id,
            "revision": revision, "applied": applied, "replayed": replayed,
            "result": result, "error": error}
