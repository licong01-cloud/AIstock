"""Callback URL helpers for RD-Agent loop completion notifications."""

from __future__ import annotations

import os
from urllib.parse import urlparse


def _is_local_callback_base(url: str | None) -> bool:
    if not url:
        return False
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return host in {"127.0.0.1", "localhost", "::1"}


def build_aistock_callback_url(
    *,
    endpoint_path: str,
    full_url_env: str | None = None,
    node_id: str | None = None,
    node_callback_url: str | None = None,
) -> str:
    """Build a full AIstock callback endpoint URL.

    ``infra.compute_nodes.callback_url`` historically stores only a base URL
    such as ``http://host:8001``. RD-Agent posts directly to the value it
    receives, so submissions must expand base URLs to a concrete webhook path.
    """
    endpoint_path = "/" + endpoint_path.strip("/")

    full_override = (os.getenv(full_url_env or "") or "").strip() if full_url_env else ""
    if full_override:
        return full_override.rstrip("/")

    configured_base = (
        os.getenv("AISTOCK_QE_CALLBACK_BASE_URL")
        or os.getenv("AISTOCK_BACKEND_CALLBACK_BASE_URL")
        or os.getenv("AISTOCK_BACKEND_BASE_URL")
        or ""
    ).strip()

    raw = (node_callback_url or "").strip()
    is_default_wsl = not node_id or node_id == "wsl2-5080"
    if configured_base:
        base = configured_base
    elif is_default_wsl or _is_local_callback_base(raw):
        # The FastAPI backend for this project normally runs on 8001.
        base = "http://127.0.0.1:8001"
    else:
        base = raw or "http://127.0.0.1:8001"

    base = base.rstrip("/")
    if base.endswith(endpoint_path):
        return base
    if "/webhook/loop-completed" in base:
        return base
    if base.endswith("/api/v1"):
        return f"{base}{endpoint_path.removeprefix('/api/v1')}"
    return f"{base}{endpoint_path}"
