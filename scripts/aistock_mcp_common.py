"""Shared helpers for thin AIstock MCP HTTP wrappers.

The helpers intentionally avoid importing backend services. MCP servers must
call loopback FastAPI endpoints so UI and agent tools share the same execution
path and audit surface.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.parse import urlparse

import httpx

LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RESPONSE_BYTES = 1_048_576
TRUNCATED_PREVIEW_BYTES = 4096


def assert_loopback_url(url: str, *, env_name: str = "base_url") -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host not in LOOPBACK_HOSTS:
        raise ValueError(
            f"{env_name} must point to loopback host {sorted(LOOPBACK_HOSTS)}; "
            f"got host={host!r} url={url!r}"
        )
    return url.rstrip("/")


def sanitize_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string; got {value!r}")
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"{name} contains illegal characters: {value!r}; "
            "only [A-Za-z0-9_.-] allowed"
        )
    return value


def sanitize_tail(value: int | None, *, default: int = 500, maximum: int = 5000) -> int:
    tail = int(default if value is None else value)
    if tail < 1 or tail > maximum:
        raise ValueError(f"tail must be between 1 and {maximum}; got {tail}")
    return tail


def require_confirm(actual: str | None, expected: str, field_name: str) -> None:
    if actual != expected:
        raise ValueError(f"{field_name} must equal {expected!r}")


def _int_from_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(value, 0)


class LoopbackApiClient:
    """Small JSON HTTP client for MCP wrappers.

    It returns the complete backend envelope by default because AIstock QE
    routers use mixed response shapes (``data`` envelope, ``ok`` booleans, and
    plain status payloads). This avoids MCP-only transformations.
    """

    def __init__(
        self,
        *,
        base_url: str,
        env_name: str,
        timeout: float | None = None,
        transport: httpx.BaseTransport | None = None,
        max_response_bytes: int | None = None,
    ) -> None:
        self.base_url = assert_loopback_url(base_url, env_name=env_name)
        self.timeout = float(timeout if timeout is not None else os.environ.get("AISTOCK_HTTP_TIMEOUT", DEFAULT_TIMEOUT))
        self.max_response_bytes = (
            max(int(max_response_bytes), 0)
            if max_response_bytes is not None
            else _int_from_env("AISTOCK_MCP_MAX_RESPONSE_BYTES", DEFAULT_MAX_RESPONSE_BYTES)
        )
        self._transport = transport

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            transport=self._transport,
            trust_env=False,
        )

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        clean_params = None if params is None else {k: v for k, v in params.items() if v is not None}
        with self._client() as client:
            response = client.get(path, params=clean_params)
        return self._decode(response, "GET", path)

    def post(self, path: str, json_body: dict[str, Any] | None = None, params: dict[str, Any] | None = None) -> Any:
        clean_params = None if params is None else {k: v for k, v in params.items() if v is not None}
        with self._client() as client:
            response = client.post(path, params=clean_params, json=json_body or {})
        return self._decode(response, "POST", path)

    def delete(self, path: str, json_body: dict[str, Any] | None = None) -> Any:
        with self._client() as client:
            response = client.request("DELETE", path, json=json_body or {})
        return self._decode(response, "DELETE", path)

    def _decode(self, response: httpx.Response, method: str, path: str) -> Any:
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path} failed with HTTP {response.status_code}: {response.text[:500]}")
        content = response.content
        original_bytes = len(content)
        if self.max_response_bytes and original_bytes > self.max_response_bytes:
            preview = content[:TRUNCATED_PREVIEW_BYTES].decode(response.encoding or "utf-8", errors="replace")
            return {
                "status": "truncated",
                "mcp_response_truncated": True,
                "method": method,
                "path": path,
                "status_code": response.status_code,
                "original_bytes": original_bytes,
                "max_bytes": self.max_response_bytes,
                "preview": preview,
            }
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{method} {path} returned non-JSON body (HTTP {response.status_code})") from exc
