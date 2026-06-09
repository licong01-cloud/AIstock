"""DeepSeek model and credential resolution helpers.

The resolver keeps DeepSeek V4 Pro as the only default model for new backend
callers and records credential sources without exposing secret values.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable


DEFAULT_DEEPSEEK_MODEL = "deepseek-v4-pro"
DEFAULT_DEEPSEEK_LITELLM_MODEL = "deepseek/deepseek-v4-pro"
DEFAULT_DEEPSEEK_BASE_URL = "https://api.deepseek.com"


class DeepSeekConfigError(RuntimeError):
    """Raised when a DeepSeek credential/configuration gate fails closed."""


@dataclass(frozen=True)
class DeepSeekConfig:
    model: str
    api_key: str = field(repr=False)
    base_url: str
    credential_source: str
    api_base_source: str

    @property
    def has_api_key(self) -> bool:
        return bool(self.api_key)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "base_url": self.base_url,
            "credential_source": self.credential_source,
            "api_base_source": self.api_base_source,
            "has_api_key": self.has_api_key,
        }


_SECRET_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"(?i)(authorization\s*[:=]\s*bearer\s+)[A-Za-z0-9._\-:/+=]{6,}"
        ),
        r"\1<redacted>",
    ),
    (
        re.compile(
            r"(?i)\b(DEEPSEEK_API_KEY|GITHUB_TOKEN|OPENAI_API_KEY|ANTHROPIC_API_KEY)"
            r"(\s*[:=]\s*)['\"]?[^'\"\s,;]+"
        ),
        r"\1\2<redacted>",
    ),
    (
        re.compile(r"\b(?:sk-[A-Za-z0-9_\-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,})\b"),
        "<redacted-token>",
    ),
    (
        re.compile(
            r"\b(?:postgresql|postgres|mysql|redis)://[^@\s]+@[^/\s]+[^\s]*",
            re.IGNORECASE,
        ),
        "<redacted-db-url>",
    ),
)


def _clean(value: Any) -> str:
    return str(value or "").strip().strip("\"'")


def redact_secret_text(text: Any) -> str:
    """Return text with common API keys, auth headers, and DB URLs removed."""

    redacted = str(text)
    for pattern, replacement in _SECRET_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


def _env_first(names: Iterable[str]) -> tuple[str, str] | tuple[None, None]:
    for name in names:
        value = _clean(os.getenv(name))
        if value:
            return value, f"env:{name}"
    return None, None


def _read_aistock_env() -> dict[str, str]:
    try:
        from backend.config_manager_compat import ConfigManager
    except Exception:
        return {}
    try:
        return ConfigManager().read_env()
    except Exception:
        return {}


def _resolve_from_aistock_env() -> tuple[str, str, str, str] | None:
    config = _read_aistock_env()
    key = _clean(config.get("DEEPSEEK_API_KEY"))
    if not key:
        return None
    base_url = _clean(config.get("DEEPSEEK_BASE_URL")) or DEFAULT_DEEPSEEK_BASE_URL
    return key, base_url, "aistock_config:backend.config_manager_compat", "aistock_config:DEEPSEEK_BASE_URL"


def _resolve_from_db(
    get_conn_fn: Callable[[], Any] | None,
) -> tuple[str, str, str, str] | None:
    if get_conn_fn is None:
        try:
            from backend.db.pg_pool import get_conn as get_conn_fn  # type: ignore[assignment]
        except Exception:
            return None

    try:
        conn_cm = get_conn_fn()
        with conn_cm as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT ac.api_base,
                           ac.api_key,
                           ac.env_api_base_name,
                           ac.env_api_key_name,
                           p.api_base_url
                    FROM aistock_llm_api_configs ac
                    JOIN aistock_llm_providers p ON p.id = ac.provider_id
                    WHERE lower(p.provider_name) = 'deepseek'
                      AND ac.is_active = true
                      AND ac.config_purpose IN ('chat', 'default')
                    ORDER BY
                      CASE ac.config_purpose WHEN 'chat' THEN 0 ELSE 1 END,
                      ac.priority DESC,
                      ac.updated_at DESC,
                      ac.id DESC
                    LIMIT 1
                    """
                )
                row = cursor.fetchone()
    except Exception:
        return None

    if not row:
        return None

    api_base, api_key, env_api_base_name, env_api_key_name, provider_base_url = row
    env_key = _clean(os.getenv(_clean(env_api_key_name))) if env_api_key_name else ""
    key = _clean(api_key) or env_key
    if not key:
        return None

    env_base = _clean(os.getenv(_clean(env_api_base_name))) if env_api_base_name else ""
    base_url = _clean(api_base) or env_base or _clean(provider_base_url) or DEFAULT_DEEPSEEK_BASE_URL
    key_source = "aistock_config:aistock_llm_api_configs"
    base_source = "aistock_config:aistock_llm_api_configs"
    if env_key and not _clean(api_key):
        key_source = f"env:{env_api_key_name}"
    if env_base and not _clean(api_base):
        base_source = f"env:{env_api_base_name}"
    return key, base_url, key_source, base_source


def resolve_deepseek_config(
    *,
    model: str | None = None,
    require_api_key: bool = True,
    get_conn_fn: Callable[[], Any] | None = None,
) -> DeepSeekConfig:
    """Resolve DeepSeek config from env, AIstock .env, then DB config.

    The function only returns secret values to direct API callers; callers that
    log evidence should use ``as_safe_dict()``.
    """

    target_model = _clean(model) or DEFAULT_DEEPSEEK_MODEL
    env_key, env_key_source = _env_first(("DEEPSEEK_API_KEY",))
    env_base, env_base_source = _env_first(("DEEPSEEK_BASE_URL",))

    if env_key:
        return DeepSeekConfig(
            model=target_model,
            api_key=env_key,
            base_url=env_base or DEFAULT_DEEPSEEK_BASE_URL,
            credential_source=env_key_source or "env",
            api_base_source=env_base_source or "default",
        )

    aistock_env = _resolve_from_aistock_env()
    if aistock_env:
        key, base_url, key_source, base_source = aistock_env
        return DeepSeekConfig(
            model=target_model,
            api_key=key,
            base_url=env_base or base_url,
            credential_source=key_source,
            api_base_source=env_base_source or base_source,
        )

    if not require_api_key:
        return DeepSeekConfig(
            model=target_model,
            api_key="",
            base_url=env_base or DEFAULT_DEEPSEEK_BASE_URL,
            credential_source="missing",
            api_base_source=env_base_source or "default",
        )

    db_config = _resolve_from_db(get_conn_fn)
    if db_config:
        key, base_url, key_source, base_source = db_config
        return DeepSeekConfig(
            model=target_model,
            api_key=key,
            base_url=env_base or base_url,
            credential_source=key_source,
            api_base_source=env_base_source or base_source,
        )

    raise DeepSeekConfigError(
        "DeepSeek API key is not configured; checked env, AIstock config, "
        "and aistock_llm_api_configs"
    )
