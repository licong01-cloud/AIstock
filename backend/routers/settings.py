from __future__ import annotations

"""Settings / environment summary endpoints.

This router exposes a **read-only** view of selected environment
configuration values for frontend diagnostics. It does **not** allow
modifying `.env` or any runtime configuration.
"""

from typing import Any, Dict

import os

from fastapi import APIRouter


router = APIRouter(prefix="/settings", tags=["settings"])


_SENSITIVE_KEYS = {
    "DEEPSEEK_API_KEY",
    "EMAIL_PASSWORD",
    "PROXYPOOL_TOKEN",
    "PROXYPOOL_PASSWORD",
}


def _mask(value: str | None) -> str | None:
    if not value:
        return None
    v = str(value)
    if len(v) <= 4:
        return "****"
    # keep first 4 and last 2 chars
    return f"{v[:4]}***{v[-2:]}"


@router.get("/env-summary", summary="环境配置概览")
async def get_env_summary() -> Dict[str, Any]:
    """Return a read-only summary of selected environment settings.

    Sensitive values are masked. The purpose of this endpoint is to
    help the UI show current backend configuration status without
    exposing secrets.
    """

    def getenv(key: str, default: str | None = None) -> str | None:
        return os.getenv(key, default) or None

    data: Dict[str, Any] = {
        "deepseek": {
            "api_key_configured": bool(getenv("DEEPSEEK_API_KEY")),
            "base_url": getenv("DEEPSEEK_BASE_URL"),
        },
        "tushare": {
            "token_configured": bool(getenv("TUSHARE_TOKEN")),
        },
        "tdx_backend": {
            "api_base": getenv("TDX_API_BASE"),
            "backend_base": getenv("TDX_BACKEND_BASE"),
        },
        "timescaledb": {
            "host": getenv("TDX_DB_HOST"),
            "port": getenv("TDX_DB_PORT"),
            "name": getenv("TDX_DB_NAME"),
            "user": getenv("TDX_DB_USER"),
            "password_configured": bool(getenv("TDX_DB_PASSWORD")),
        },
        "miniqmt": {
            "enabled": (getenv("MINIQMT_ENABLED", "false").lower() == "true"),
            "account_id": getenv("MINIQMT_ACCOUNT_ID"),
            "host": getenv("MINIQMT_HOST"),
            "port": getenv("MINIQMT_PORT"),
        },
        "email": {
            "enabled": (getenv("EMAIL_ENABLED", "false").lower() == "true"),
            "smtp_server": getenv("SMTP_SERVER"),
            "smtp_port": getenv("SMTP_PORT"),
            "email_from": getenv("EMAIL_FROM"),
            "email_to": getenv("EMAIL_TO"),
        },
        "webhook": {
            "enabled": (getenv("WEBHOOK_ENABLED", "false").lower() == "true"),
            "webhook_type": getenv("WEBHOOK_TYPE"),
            "webhook_url_configured": bool(getenv("WEBHOOK_URL")),
        },
    }

    # Add masked view of a few sensitive fields (optional, for debugging)
    masked: Dict[str, Any] = {
        key: _mask(getenv(key)) for key in _SENSITIVE_KEYS if getenv(key)
    }
    if masked:
        data["masked"] = masked

    return data
