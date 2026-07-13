"""Callback URL helpers for RD-Agent loop completion notifications."""

from __future__ import annotations

import os
import re
from urllib.parse import urlparse


def _is_local_callback_base(url: str | None) -> bool:
    if not url:
        return False
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return host in {"127.0.0.1", "localhost", "::1"}


def _callback_base_env_names(node_id: str | None = None) -> list[str]:
    names: list[str] = []
    if node_id:
        suffix = re.sub(r"[^A-Za-z0-9]+", "_", node_id).strip("_").upper()
        if suffix:
            names.extend(
                [
                    f"AISTOCK_QE_CALLBACK_BASE_URL_{suffix}",
                    f"AISTOCK_BACKEND_CALLBACK_BASE_URL_{suffix}",
                ]
            )
    names.extend(
        [
            "AISTOCK_QE_CALLBACK_BASE_URL",
            "AISTOCK_BACKEND_CALLBACK_BASE_URL",
            "AISTOCK_BACKEND_BASE_URL",
        ]
    )
    return names


def _first_env_value(names: list[str]) -> tuple[str, str]:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return name, value
    return "", ""


def _validate_callback_url(url: str, *, source: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{source} must be an absolute http(s) URL, got: {url!r}")
    return url.rstrip("/")


def _reject_remote_localhost(url: str, *, node_id: str | None, source: str) -> None:
    if node_id and node_id != "wsl2-5080" and _is_local_callback_base(url):
        raise ValueError(
            f"{source} must be reachable from remote node_id={node_id!r}; "
            "localhost is not allowed for remote QE callbacks."
        )


def _strip_known_callback_endpoint(url: str) -> str:
    base = url.rstrip("/")
    for suffix in (
        "/api/v1/quantevolver/evolution/webhook/loop-completed",
        "/api/v1/quantevolver/webhook/loop-completed",
        "/api/v1/webhook/loop-completed",
    ):
        if base.endswith(suffix):
            return base[: -len(suffix)].rstrip("/")
    return base


def build_aistock_callback_base_url(
    *,
    full_callback_url: str | None = None,
    full_url_env: str | None = None,
    node_id: str | None = None,
    node_callback_url: str | None = None,
    require_env_base: bool = False,
) -> str:
    """Resolve the AIstock backend base URL using the same callback rules.

    QE prediction-store uploads are runner-side HTTP posts, so remote nodes
    must receive a LAN-reachable backend base instead of localhost.
    """
    full_override = (os.getenv(full_url_env or "") or "").strip() if full_url_env else ""
    if full_override:
        full_override = _validate_callback_url(full_override, source=full_url_env or "callback URL override")
        _reject_remote_localhost(full_override, node_id=node_id, source=full_url_env or "callback URL override")
        return _strip_known_callback_endpoint(full_override)

    if full_callback_url:
        full_callback_url = _validate_callback_url(full_callback_url, source="callback URL")
        _reject_remote_localhost(full_callback_url, node_id=node_id, source="callback URL")
        return _strip_known_callback_endpoint(full_callback_url)

    env_name, configured_base = _first_env_value(_callback_base_env_names(node_id))
    if configured_base:
        base = _validate_callback_url(configured_base, source=env_name)
        _reject_remote_localhost(base, node_id=node_id, source=env_name)
        return base

    if require_env_base:
        expected = ", ".join(_callback_base_env_names(node_id))
        raise ValueError(
            "QE callback base URL must be configured via environment before submitting QE work; "
            f"set one of: {expected}."
        )

    raw = (node_callback_url or "").strip()
    if not raw:
        raise ValueError(
            "QE callback base URL could not be resolved from environment, full callback URL, "
            f"or compute-node callback_url for node_id={node_id!r}."
        )
    base = _validate_callback_url(raw, source="compute_nodes.callback_url")
    _reject_remote_localhost(base, node_id=node_id, source="compute_nodes.callback_url")
    return _strip_known_callback_endpoint(base)


def build_aistock_callback_url(
    *,
    endpoint_path: str,
    full_url_env: str | None = None,
    node_id: str | None = None,
    node_callback_url: str | None = None,
    require_env_base: bool = False,
) -> str:
    """Build a full AIstock callback endpoint URL.

    ``infra.compute_nodes.callback_url`` historically stores only a base URL
    such as ``http://host:8001``. RD-Agent posts directly to the value it
    receives, so submissions must expand base URLs to a concrete webhook path.
    """
    endpoint_path = "/" + endpoint_path.strip("/")

    full_override = (os.getenv(full_url_env or "") or "").strip() if full_url_env else ""
    if full_override:
        full_override = _validate_callback_url(full_override, source=full_url_env or "callback URL override")
        _reject_remote_localhost(full_override, node_id=node_id, source=full_url_env or "callback URL override")
        return full_override

    env_name, configured_base = _first_env_value(_callback_base_env_names(node_id))

    raw = (node_callback_url or "").strip()
    is_default_wsl = not node_id or node_id == "wsl2-5080"
    if configured_base:
        base = _validate_callback_url(configured_base, source=env_name)
        if not is_default_wsl:
            _reject_remote_localhost(base, node_id=node_id, source=env_name)
    elif require_env_base:
        expected = ", ".join(_callback_base_env_names(node_id))
        raise ValueError(
            "QE callback base URL must be configured via environment before submitting QE work; "
            f"set one of: {expected}."
        )
    else:
        if not raw or _is_local_callback_base(raw):
            raise ValueError(
                "Remote QE callback URL must be configured with a non-localhost "
                f"AIstock base URL for node_id={node_id!r}; refusing localhost fallback."
            )
        base = raw

    base = base.rstrip("/")
    if base.endswith(endpoint_path):
        return base
    if "/webhook/loop-completed" in base:
        return base
    if base.endswith("/api/v1"):
        return f"{base}{endpoint_path.removeprefix('/api/v1')}"
    return f"{base}{endpoint_path}"
