"""Common helpers for AIstock MCP modules.

MCP tools should call loopback FastAPI endpoints through this client instead of
importing backend services directly. That keeps UI, API, and agent entry points
on the same audited execution path.
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
DEFAULT_BODY_EXCERPT_LIMIT = 500


def assert_loopback_url(url: str, *, env_name: str = "base_url") -> str:
    """Return a normalized loopback URL or raise a diagnostic ValueError."""

    if not isinstance(url, str) or not url.strip():
        raise ValueError(f"{env_name} must be a non-empty URL; got {url!r}")
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or host not in LOOPBACK_HOSTS:
        raise ValueError(
            f"{env_name} must point to loopback host {sorted(LOOPBACK_HOSTS)} "
            f"using http(s); got scheme={parsed.scheme!r} host={host!r} url={url!r}"
        )
    return url.rstrip("/")


def join_url_path(base_url: str, path_prefix: str = "") -> str:
    """Join a URL and a relative path prefix without accepting a new host."""

    base = assert_loopback_url(base_url)
    prefix = path_prefix.strip("/")
    return base if not prefix else f"{base}/{prefix}"


def sanitize_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string; got {value!r}")
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"{name} contains illegal characters: {value!r}; "
            "only [A-Za-z0-9_.-] allowed"
        )
    return value


# Short alias expected by module registry consumers.
sanitize = sanitize_identifier


def confirm(actual: str | None, expected: str, field_name: str) -> None:
    if actual != expected:
        raise ValueError(f"{field_name} must equal {expected!r}")


def _clean_params(params: dict[str, Any] | None) -> dict[str, Any] | None:
    if params is None:
        return None
    return {key: value for key, value in params.items() if value is not None}


def _body_excerpt(response: httpx.Response, *, limit: int = DEFAULT_BODY_EXCERPT_LIMIT) -> str:
    text = response.text.replace("\r", " ").replace("\n", " ").strip()
    return text[:limit]


class AIstockApiClient:
    """Small JSON HTTP client for loopback-only AIstock MCP modules."""

    def __init__(
        self,
        base_url: str,
        *,
        env_name: str = "dev",
        timeout: float | None = None,
        unwrap_data: bool = False,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = assert_loopback_url(base_url, env_name=env_name)
        self.env_name = env_name
        self.timeout = float(timeout if timeout is not None else os.environ.get("AISTOCK_HTTP_TIMEOUT", DEFAULT_TIMEOUT))
        self.unwrap_data = unwrap_data
        self._transport = transport

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            transport=self._transport,
            trust_env=False,
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        with self._client() as client:
            response = client.request(
                method.upper(),
                path,
                params=_clean_params(params),
                json=json_body if json_body is not None else None,
            )
        return self._decode(response, method.upper(), path)

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self.request("GET", path, params=params)

    def post(
        self,
        path: str,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return self.request("POST", path, json_body=json_body or {}, params=params)

    def delete(self, path: str, json_body: dict[str, Any] | None = None) -> Any:
        return self.request("DELETE", path, json_body=json_body or {})

    def _decode(self, response: httpx.Response, method: str, path: str) -> Any:
        if response.status_code >= 400:
            body = _body_excerpt(response)
            raise RuntimeError(
                f"{method} {path} failed with HTTP {response.status_code}: "
                f"response body excerpt={body!r}"
            )
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{method} {path} returned non-JSON body (HTTP {response.status_code})") from exc
        if self.unwrap_data and isinstance(payload, dict) and "data" in payload:
            return payload["data"]
        return payload
