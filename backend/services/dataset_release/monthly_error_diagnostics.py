"""Small, non-secret exception locations for the existing status endpoint."""

from __future__ import annotations

import re
from typing import Any


_SAFE = re.compile(r"^[A-Za-z0-9_.:/ -]{1,160}$")
_SECRET = re.compile(
    r"(?i)(password|passwd|pwd|token|secret|api[_-]?key|authorization)[\"']?\s*[:=]\s*(?:\"[^\"]*\"|'[^']*'|[^\s;,]+)"
)
_URL_LOGIN = re.compile(r"(\w+://)[^/\s]+@")
_BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]+=*")
_CONTEXT_KEYS = {"query_id", "partition", "partition_key", "dataset", "start", "end", "field", "max_rows"}


def monthly_error_diagnostics(error: BaseException) -> list[dict[str, Any]]:
    """No traceback source text, SQL, bind values, repr or connection objects."""
    chain = []
    seen = set()
    current = error
    while current is not None and id(current) not in seen and len(chain) < 8:
        seen.add(id(current))
        item: dict[str, Any] = {"type": type(current).__name__}
        # Only code-owned release exceptions supply messages. Arbitrary driver
        # messages can contain SQL/bind values; retain SQLSTATE and location.
        if type(current).__module__.startswith("backend.services.dataset_release."):
            message = _URL_LOGIN.sub(r"\1[REDACTED]@", str(current))
            message = _BEARER.sub("Bearer [REDACTED]", message)
            item["message"] = _SECRET.sub(r"\1=[REDACTED]", message)[:512]
        for key in ("sqlstate", "pgcode", "errno"):
            value = getattr(current, key, None)
            if type(value) is int or (isinstance(value, str) and _SAFE.fullmatch(value)):
                item[key] = value
        diag = getattr(current, "diag", None)
        for key in ("schema_name", "table_name", "column_name", "constraint_name"):
            value = getattr(diag, key, None)
            if isinstance(value, str) and _SAFE.fullmatch(value):
                item[key] = value
        context = getattr(current, "context", None)
        if isinstance(context, dict):
            item["context"] = {
                key: value for key, value in context.items() if key in _CONTEXT_KEYS
                and (type(value) is int or isinstance(value, str) and _SAFE.fullmatch(value))
            }
        frames = []
        traceback = current.__traceback__
        while traceback is not None:
            frame = traceback.tb_frame
            module = str(frame.f_globals.get("__name__") or "")
            if module.startswith("backend.services.dataset_release."):
                frames.append({"module": module, "function": frame.f_code.co_name, "line": traceback.tb_lineno})
            traceback = traceback.tb_next
        item["locations"] = frames[-8:]
        chain.append(item)
        current = current.__cause__ if current.__cause__ is not None else (
            None if current.__suppress_context__ else current.__context__
        )
    return chain


def monthly_error_message(error: BaseException) -> str:
    """Top-level last_error must not leak the driver text omitted below it."""
    item = monthly_error_diagnostics(error)[0]
    return item.get("message") or f"{item['type']}: see exception_chain for error code and location"
