from __future__ import annotations

import pytest

from scripts import llm_provider_adapter as adapter


def test_llm_triage_config_defaults_are_safe():
    config = adapter.load_config()

    adapter.validate_config(config)

    assert config["default_provider"] == "github_models"
    assert config["providers"]["deepseek_api"]["enabled"] is False
    assert config["providers"]["deepseek_api"]["model"] == "deepseek-v4-pro"
    assert config["providers"]["github_models"]["model_selector"]["allow_lower_tier_fallback"] is False


def test_github_model_catalog_selects_deepseek_v4_pro_variant():
    selector = adapter.load_config()["providers"]["github_models"]["model_selector"]
    catalog = {
        "models": [
            {
                "id": "openai/gpt-test",
                "publisher": "OpenAI",
                "capabilities": ["tool-calling", "json-output"],
            },
            {
                "id": "deepseek/deepseek-v4-pro-202606",
                "publisher": {"name": "DeepSeek"},
                "capabilities": {"tool-calling": True, "json-output": True},
            },
        ]
    }

    selected = adapter.select_github_model(catalog, selector)

    assert selected["model_id"] == "deepseek/deepseek-v4-pro-202606"
    assert selected["publisher"] == "DeepSeek"
    assert selected["capabilities"] == ["json-output", "tool-calling"]


def test_github_model_catalog_fails_closed_without_v4_pro():
    selector = adapter.load_config()["providers"]["github_models"]["model_selector"]
    catalog = {
        "models": [
            {
                "id": "deepseek/deepseek-v4-flash",
                "publisher": "DeepSeek",
                "capabilities": ["tool-calling", "json-output"],
            }
        ]
    }

    with pytest.raises(adapter.ProviderAdapterError):
        adapter.select_github_model(catalog, selector)


def test_invalid_provider_json_fails_closed():
    with pytest.raises(adapter.ProviderAdapterError):
        adapter.parse_json_response("{not-json")

    with pytest.raises(adapter.ProviderAdapterError):
        adapter.parse_json_response("[1, 2, 3]")


def test_validate_config_cli_uses_compact_success_output(capsys):
    exit_code = adapter.main(["--json", "validate-config", "--provider", "deterministic"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"gate": "passed"' in captured.out
    assert "DEEPSEEK_API_KEY" not in captured.out
