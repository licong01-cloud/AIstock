from __future__ import annotations

import contextlib
import asyncio
import importlib
import sys
from types import SimpleNamespace

import pytest

from backend.infra import deepseek_config
from backend.infra.deepseek_config import (
    DEFAULT_DEEPSEEK_MODEL,
    DeepSeekConfigError,
    redact_secret_text,
    resolve_deepseek_config,
    enforce_deepseek_model_policy,
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


@pytest.mark.parametrize("selected,expected", [
    ("deepseek-v4-pro", "deepseek-v4-flash"),
    (" 'DeepSeek-V4-Pro' ", "deepseek-v4-flash"),
    ("deepseek-pro", "deepseek-v4-flash"),
    ("deepseek-v5-pro-20270101", "deepseek-v4-flash"),
    ("deepseek/deepseek-v4-pro", "deepseek/deepseek-v4-flash"),
    ("openrouter/deepseek/deepseek-v4-pro:online", "openrouter/deepseek/deepseek-v4-flash"),
    ("deepseek-v4-flash", "deepseek-v4-flash"),
    ("deepseek/deepseek-chat", "deepseek/deepseek-chat"),
    ("deepseek-reasoner", "deepseek-reasoner"),
    ("openai/o3-pro", "openai/o3-pro"),
    ("siliconflow/Qwen/Qwen3-Embedding-8B", "siliconflow/Qwen/Qwen3-Embedding-8B"),
])
def test_model_policy_is_precise_and_idempotent(selected, expected):
    assert enforce_deepseek_model_policy(selected) == expected
    assert enforce_deepseek_model_policy(expected) == expected


def test_default_and_explicit_override_resolve_to_flash(monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-test-key")
    monkeypatch.setattr(deepseek_config, "_read_aistock_env", lambda: pytest.fail("credential file read"))
    assert DEFAULT_DEEPSEEK_MODEL == "deepseek-v4-flash"
    assert deepseek_config.DEFAULT_DEEPSEEK_LITELLM_MODEL == "deepseek/deepseek-v4-flash"
    assert resolve_deepseek_config(model="deepseek-v4-pro").model == DEFAULT_DEEPSEEK_MODEL


@pytest.mark.parametrize("module", ["backend.infra.deepseek_client", "deepseek_client"])
@pytest.mark.parametrize("override", [None, "deepseek-v4-pro"])
def test_sdk_wire_never_uses_pro(monkeypatch, module, override):
    monkeypatch.setitem(sys.modules, "config", SimpleNamespace())
    client_module = importlib.import_module(module)
    calls = []
    def create(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])
    client = client_module.DeepSeekClient.__new__(client_module.DeepSeekClient)
    client.model = "deepseek-v4-pro"  # Emulate an instance holding old cached configuration.
    client.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    assert client.call_api([{"role": "user", "content": "test"}], model=override) == "ok"
    assert calls[0]["model"] == "deepseek-v4-flash"


@pytest.mark.parametrize("rows,expected", [
    ([None], "deepseek/deepseek-v4-flash"),
    ([("deepseek/deepseek-v4-pro",)], "deepseek/deepseek-v4-flash"),
    ([("12",), ("deepseek/deepseek-v4-pro", 1, "chat", 1, "https://example.invalid", "dummy")],
     "deepseek/deepseek-v4-flash"),
])
def test_qe_numeric_legacy_and_default_selections(monkeypatch, rows, expected):
    from backend.services.quantevolver import llm_client
    values = iter(rows)
    class Cursor:
        def execute(self, *args):
            pass
        def fetchone(self):
            return next(values)
    with contextlib.ExitStack() as stack:
        cursor = stack.enter_context(contextlib.nullcontext(Cursor()))
        conn = SimpleNamespace(cursor=lambda: contextlib.nullcontext(cursor))
        monkeypatch.setattr(llm_client, "get_conn", lambda: contextlib.nullcontext(conn))
        result = llm_client.get_llm_kwargs("test", default_model="deepseek/deepseek-v4-pro")
    assert result["model"] == expected


def test_rdagent_outgoing_config_and_verify_payloads(monkeypatch):
    from backend.services.rdagent_llm_config_client import RDAgentLLMConfigClient
    payloads = []
    async def post(url, *, json):
        payloads.append(json)
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"ok": True})
    client = RDAgentLLMConfigClient.__new__(RDAgentLLMConfigClient)
    client.base_url = "http://example.invalid"
    client.client = SimpleNamespace(post=post)
    async def run():
        await client.update_config([{"stage_name": "direct_exp_gen", "full_model_id": "deepseek/deepseek-v4-pro"}])
        await client.verify_model("deepseek/deepseek-v4-pro")
    asyncio.run(run())
    assert payloads[0]["stage_mappings"][0]["model_id"] == "deepseek/deepseek-v4-flash"
    assert payloads[1]["model_id"] == "deepseek/deepseek-v4-flash"


@pytest.mark.parametrize("selected", ["deepseek/deepseek-v4-pro", "deepseek/deepseek-v4-flash"])
def test_verification_endpoint_never_verifies_pro_using_flash_response(monkeypatch, selected):
    from backend.routers.rdagent_llm_config_endpoints import comprehensive_model_verification
    calls = []
    async def complete(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(model=kwargs["model"])
    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(acompletion=complete))
    result = asyncio.run(comprehensive_model_verification(
        "deepseek", selected.split("/")[-1], selected, "dummy",
        run_health_check=False,
    ))
    if selected.endswith("-pro"):
        assert calls == []
        assert result["overall_success"] is False
        assert result["effective_model_id"] == "deepseek/deepseek-v4-flash"
    else:
        assert calls[0]["model"] == "deepseek/deepseek-v4-flash"
        assert result["litellm_test"]["success"] is True


@pytest.mark.parametrize("provider", ["deepseek", "openai"])
def test_assistant_final_wire_and_usage_record_effective_model(monkeypatch, provider):
    from backend.services.research_assistant import service
    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    monkeypatch.setattr(service, "resolve_deepseek_config", lambda **kw: SimpleNamespace(
        model=enforce_deepseek_model_policy(kw["model"]), api_key="dummy", base_url="https://example.invalid"))
    calls = []
    def complete(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(choices=[SimpleNamespace(
            message=SimpleNamespace(content="ok", tool_calls=None), finish_reason="stop")],
            usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1, total_tokens=2))
    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=complete))
    result = service.ResearchAssistantLlmClient().complete(
        messages=[{"role": "user", "content": "test"}],
        model_profile={"provider": provider, "model_name": "deepseek/deepseek-v4-pro"},
        temperature=0.7, max_tokens=10,
    )
    assert calls[0]["model"] == "deepseek/deepseek-v4-flash"
    assert result.model == result.usage_event["model"] == "deepseek/deepseek-v4-flash"


@pytest.mark.parametrize("models,expected", [([], "ready"), ([("deepseek-v4-pro",)], "blocked")])
def test_policy_endpoint_has_config_semantics_and_no_model_call(monkeypatch, models, expected):
    from backend.routers import llm_config
    from scripts.aistock_runtime_semantics import _validate_deepseek_flash_policy
    cursor = SimpleNamespace(execute=lambda *args: None, fetchall=lambda: models)
    conn = SimpleNamespace(cursor=lambda: contextlib.nullcontext(cursor))
    monkeypatch.setattr(llm_config, "get_conn", lambda: contextlib.nullcontext(conn))
    payload = llm_config.deepseek_policy_status()
    assert payload["status"] == expected
    verdict, _, _ = _validate_deepseek_flash_policy(payload)
    assert verdict == ("passed" if expected == "ready" else "failed")
    for field in tuple(payload):
        broken = dict(payload)
        del broken[field]
        assert _validate_deepseek_flash_policy(broken)[0] == "failed"
    assert _validate_deepseek_flash_policy({"status": "ok"})[0] == "failed"
