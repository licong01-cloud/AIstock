from __future__ import annotations

import contextlib

import pytest

from backend.infra import deepseek_config
from backend.infra.deepseek_config import (
    DEFAULT_DEEPSEEK_MODEL,
    DeepSeekConfigError,
    redact_secret_text,
    resolve_deepseek_config,
)


def test_env_credentials_take_priority(monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-env-secret-123456")
    monkeypatch.setenv("DEEPSEEK_BASE_URL", "https://env.deepseek.test")
    monkeypatch.setattr(
        deepseek_config,
        "_read_aistock_env",
        lambda: {
            "DEEPSEEK_API_KEY": "sk-config-secret-123456",
            "DEEPSEEK_BASE_URL": "https://config.deepseek.test",
        },
    )

    resolved = resolve_deepseek_config()

    assert resolved.model == DEFAULT_DEEPSEEK_MODEL
    assert resolved.api_key == "sk-env-secret-123456"
    assert resolved.base_url == "https://env.deepseek.test"
    assert resolved.as_safe_dict() == {
        "model": DEFAULT_DEEPSEEK_MODEL,
        "base_url": "https://env.deepseek.test",
        "credential_source": "env:DEEPSEEK_API_KEY",
        "api_base_source": "env:DEEPSEEK_BASE_URL",
        "has_api_key": True,
    }


def test_aistock_env_fallback(monkeypatch):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("DEEPSEEK_BASE_URL", raising=False)
    monkeypatch.setattr(
        deepseek_config,
        "_read_aistock_env",
        lambda: {
            "DEEPSEEK_API_KEY": "sk-config-secret-123456",
            "DEEPSEEK_BASE_URL": "https://config.deepseek.test",
        },
    )

    resolved = resolve_deepseek_config()

    assert resolved.api_key == "sk-config-secret-123456"
    assert resolved.base_url == "https://config.deepseek.test"
    assert resolved.credential_source == "aistock_config:backend.config_manager_compat"


def test_db_config_fallback(monkeypatch):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("DEEPSEEK_BASE_URL", raising=False)
    monkeypatch.setattr(deepseek_config, "_read_aistock_env", lambda: {})

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return (
                "https://db.deepseek.test",
                "sk-db-secret-123456",
                "DEEPSEEK_API_BASE",
                "DEEPSEEK_API_KEY",
                "https://provider.deepseek.test",
            )

    class FakeConn:
        def cursor(self):
            return FakeCursor()

    @contextlib.contextmanager
    def fake_get_conn():
        yield FakeConn()

    resolved = resolve_deepseek_config(get_conn_fn=fake_get_conn)

    assert resolved.api_key == "sk-db-secret-123456"
    assert resolved.base_url == "https://db.deepseek.test"
    assert resolved.credential_source == "aistock_config:aistock_llm_api_configs"


def test_missing_key_fails_closed(monkeypatch):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("DEEPSEEK_BASE_URL", raising=False)
    monkeypatch.setattr(deepseek_config, "_read_aistock_env", lambda: {})

    @contextlib.contextmanager
    def fake_get_conn():
        class FakeConn:
            def cursor(self):
                raise AssertionError("DB should not be reached in this fixture")

        yield FakeConn()

    with pytest.raises(DeepSeekConfigError):
        resolve_deepseek_config(get_conn_fn=fake_get_conn)


def test_safe_summary_and_redaction_do_not_leak_secrets():
    text = (
        "Authorization: Bearer sk-secret-1234567890 "
        "DEEPSEEK_API_KEY=sk-another-secret-123456 "
        "postgresql://user:pass@localhost:5432/aistock "
        "GITHUB_TOKEN=ghp_abcdefghijklmnopqrstuvwxyz"
    )

    redacted = redact_secret_text(text)

    assert "sk-secret" not in redacted
    assert "sk-another" not in redacted
    assert "user:pass" not in redacted
    assert "ghp_" not in redacted
    assert "<redacted" in redacted
