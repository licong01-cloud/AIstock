from __future__ import annotations

import json

import pytest

from scripts import llm_provider_adapter as adapter


def test_llm_triage_config_defaults_are_safe():
    config = adapter.load_config()

    adapter.validate_config(config)

    assert config["default_provider"] == "github_models"
    assert config["providers"]["deepseek_api"]["enabled"] is False
    assert config["providers"]["deepseek_api"]["model"] == "deepseek-v4-pro"
    selector = config["providers"]["github_models"]["model_selector"]
    assert selector["required_model_family"] == "deepseek-r1"
    assert selector["preferred_models"] == ["deepseek/deepseek-r1"]
    assert selector["allow_lower_tier_fallback"] is False


def test_github_model_catalog_selects_deepseek_r1():
    selector = adapter.load_config()["providers"]["github_models"]["model_selector"]
    catalog = {
        "models": [
            {
                "id": "openai/gpt-test",
                "publisher": "OpenAI",
                "capabilities": ["tool-calling", "reasoning"],
            },
            {
                "id": "deepseek/deepseek-r1",
                "publisher": {"name": "DeepSeek"},
                "capabilities": {"tool-calling": True, "reasoning": True, "streaming": True},
            },
        ]
    }

    selected = adapter.select_github_model(catalog, selector)

    assert selected["model_id"] == "deepseek/deepseek-r1"
    assert selected["publisher"] == "DeepSeek"
    assert selected["capabilities"] == ["reasoning", "streaming", "tool-calling"]


def test_github_model_catalog_fails_closed_without_r1():
    selector = adapter.load_config()["providers"]["github_models"]["model_selector"]
    catalog = {
        "models": [
            {
                "id": "deepseek/deepseek-v3-0324",
                "publisher": "DeepSeek",
                "capabilities": ["tool-calling"],
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


def test_triage_quality_smoke_has_schema_issue_draft_and_allowlisted_plans():
    payload = adapter.build_triage_quality_smoke("github_models", adapter.load_config())

    assert payload["schema_version"] == adapter.TRIAGE_ADVICE_SCHEMA_VERSION
    assert payload["provider"] == "github_models"
    assert payload["model"] == "deepseek/deepseek-r1"
    assert payload["actionability"]["is_actionable"] is True
    assert payload["issue_draft"]["contains_reproduce_command"] is True
    assert payload["issue_draft"]["full_logs_included"] is False
    assert payload["prompt_quality"]["full_repo_scan_allowed"] is False
    assert [item["plan_key"] for item in payload["test_plan_advice"]] == ["validation_module_registry_l0", "l0"]
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert payload["deterministic_gate"]["plan_keys_allowlisted"] is True


def test_triage_quality_smoke_cli_uses_compact_success_output(capsys, tmp_path):
    output = tmp_path / "triage-advice.json"

    exit_code = adapter.main(
        [
            "--json",
            "triage-quality-smoke",
            "--provider",
            "github_models",
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"check": "triage-quality-smoke"' in captured.out
    assert '"suggested_plan_count": 2' in captured.out
    assert "DeepSeek API key" not in captured.out
    assert output.exists()


def test_test_plan_advice_allows_runner_enabled_catalog_plan():
    payload = adapter.build_test_plan_advice(
        "github_models",
        adapter.load_config(),
        plan_keys=["l0", "validation_catalog_integrity", "validation_center_backend"],
        changed_files=["scripts/llm_provider_adapter.py"],
        module="validation.runner",
    )

    assert payload["schema_version"] == adapter.TEST_PLAN_ADVICE_SCHEMA_VERSION
    assert payload["provider"] == "github_models"
    assert payload["deterministic_gate"]["workflow_gate"] == "ready"
    assert payload["deterministic_gate"]["runner_enabled_only"] is True
    assert payload["deterministic_gate"]["command_keys_allowlisted"] is True
    assert payload["deterministic_gate"]["validation_select_compatible"] is True
    assert payload["deterministic_gate"]["shell_commands_allowed"] is False
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert [item["plan_key"] for item in payload["test_plan_advice"]] == [
        "l0",
        "validation_catalog_integrity",
        "validation_center_backend",
    ]


def test_test_plan_advice_blocks_unknown_non_runner_and_production_state_plans():
    config = adapter.load_config()

    unknown = adapter.build_test_plan_advice("deterministic", config, plan_keys=["missing_plan"])
    assert unknown["deterministic_gate"]["workflow_gate"] == "blocked"
    assert unknown["test_plan_advice"][0]["rejection_reasons"] == ["unknown_plan_key"]

    non_runner = adapter.build_test_plan_advice("deterministic", config, plan_keys=["validation_center_ui"])
    assert non_runner["deterministic_gate"]["workflow_gate"] == "blocked"
    assert "runner_not_enabled" in non_runner["test_plan_advice"][0]["rejection_reasons"]

    business_state = adapter.build_test_plan_advice("deterministic", config, plan_keys=["miniqmt_sim_trading_hours_l5"])
    assert business_state["deterministic_gate"]["workflow_gate"] == "blocked"
    assert "runner_not_enabled" in business_state["test_plan_advice"][0]["rejection_reasons"]
    assert "writes_business_state" in business_state["test_plan_advice"][0]["rejection_reasons"]


def test_test_plan_advice_rejects_unregistered_workspace_path(tmp_path):
    payload = adapter.build_test_plan_advice(
        "deterministic",
        adapter.load_config(),
        plan_keys=["l0"],
        workspace_path=str(tmp_path),
    )

    assert payload["deterministic_gate"]["workflow_gate"] == "blocked"
    assert payload["workspace_gate"]["allowed"] is False
    assert payload["workspace_gate"]["reason"] == "workspace_path_not_registered_git_worktree"


def test_test_plan_advice_rejects_shell_command_fields():
    with pytest.raises(adapter.ProviderAdapterError):
        adapter.validate_test_plan_advice(
            {
                "schema_version": adapter.TEST_PLAN_ADVICE_SCHEMA_VERSION,
                "test_plan_advice": [{"plan_key": "l0", "command": "python -m nox -s l0"}],
                "deterministic_gate": {"shell_commands_allowed": False},
            }
        )


def test_test_plan_advice_cli_uses_compact_success_output(capsys, tmp_path):
    output = tmp_path / "test-plan-advice.json"

    exit_code = adapter.main(
        [
            "--json",
            "test-plan-advice",
            "--provider",
            "github_models",
            "--changed-file",
            "scripts/llm_provider_adapter.py",
            "--module",
            "validation.runner",
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"check": "test-plan-advice"' in captured.out
    assert '"workflow_gate": "passed"' not in captured.out
    assert '"advised_plan_count": 3' in captured.out
    assert '"llm_invoked": false' in captured.out
    assert output.exists()


def test_nightly_scheduler_advice_uses_fixed_baseline_without_changes_or_failures():
    payload = adapter.build_nightly_scheduler_advice("deterministic", adapter.load_config(), codegraph_freshness="fresh")

    assert payload["schema_version"] == adapter.NIGHTLY_SCHEDULER_ADVICE_SCHEMA_VERSION
    assert payload["deterministic_gate"]["workflow_gate"] == "ready"
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert [item["plan_key"] for item in payload["queue"]] == ["l0"]
    assert payload["queue"][0]["priority"] == "baseline"
    assert payload["queue"][0]["allowed"] is True


def test_nightly_scheduler_advice_qe_ui_failure_recommends_l3_and_safe_runner_fallback():
    payload = adapter.build_nightly_scheduler_advice(
        "github_models",
        adapter.load_config(),
        recent_failure_modules=["qe_ui"],
        codegraph_freshness="fresh",
        resource_budget_seconds=1200,
    )
    queue = {item["plan_key"]: item for item in payload["queue"]}

    assert "qe_archive_l3" in queue
    assert queue["qe_archive_l3"]["allowed"] is False
    assert "runner_not_enabled" in queue["qe_archive_l3"]["deferred_reason"]
    assert queue["qe_archive_backend"]["allowed"] is True
    assert payload["deterministic_gate"]["production_actions_allowed"] is False


def test_nightly_scheduler_advice_defers_over_resource_budget():
    payload = adapter.build_nightly_scheduler_advice(
        "deterministic",
        adapter.load_config(),
        recent_failure_modules=["research_assistant"],
        codegraph_freshness="fresh",
        resource_budget_seconds=200,
    )

    item = payload["queue"][0]
    assert item["plan_key"] == "research_assistant_backend"
    assert item["allowed"] is False
    assert item["deferred_reason"] == "resource_budget_exceeded"
    assert payload["deterministic_gate"]["workflow_gate"] == "ready"


def test_nightly_scheduler_advice_codegraph_missing_is_warning_only():
    payload = adapter.build_nightly_scheduler_advice(
        "deterministic",
        adapter.load_config(),
        changed_files=["scripts/llm_provider_adapter.py"],
        codegraph_freshness="missing",
    )

    assert payload["deterministic_gate"]["workflow_gate"] == "warning"
    assert payload["codegraph"]["warning_only"] is True
    assert payload["deterministic_gate"]["allowed_plan_count"] >= 1


def test_nightly_scheduler_advice_rejects_live_trading_or_business_state_plans():
    payload = adapter.build_nightly_scheduler_advice(
        "deterministic",
        adapter.load_config(),
        recent_failure_plan_keys=["miniqmt_sim_trading_hours_l5"],
        codegraph_freshness="fresh",
        resource_budget_seconds=9000,
    )

    item = payload["queue"][0]
    assert item["allowed"] is False
    assert "writes_business_state" in item["deferred_reason"]
    assert "requires_confirmation" in item["deferred_reason"]
    assert payload["deterministic_gate"]["workflow_gate"] == "blocked"


def test_nightly_scheduler_advice_rejects_shell_command_fields():
    with pytest.raises(adapter.ProviderAdapterError):
        adapter.validate_nightly_scheduler_advice(
            {
                "schema_version": adapter.NIGHTLY_SCHEDULER_ADVICE_SCHEMA_VERSION,
                "queue": [{"plan_key": "l0", "command": "python -m nox -s l0"}],
                "deterministic_gate": {
                    "shell_commands_allowed": False,
                    "production_actions_allowed": False,
                },
            }
        )


def test_nightly_scheduler_advice_cli_uses_compact_success_output(capsys, tmp_path):
    output = tmp_path / "nightly-scheduler-advice.json"

    exit_code = adapter.main(
        [
            "--json",
            "nightly-scheduler-advice",
            "--provider",
            "github_models",
            "--changed-file",
            "scripts/llm_provider_adapter.py",
            "--recent-failure-module",
            "validation.runner",
            "--codegraph-freshness",
            "fresh",
            "--resource-budget-seconds",
            "900",
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"check": "nightly-scheduler-advice"' in captured.out
    assert '"queue_count":' in captured.out
    assert '"llm_invoked": false' in captured.out
    assert output.exists()


def test_prompt_evaluation_has_20_fixtures_and_compact_metrics():
    payload = adapter.build_prompt_evaluation("deterministic", adapter.load_config())

    assert payload["schema_version"] == adapter.PROMPT_EVALUATION_SCHEMA_VERSION
    assert payload["metrics"]["case_count"] >= 20
    assert payload["metrics"]["actionability_precision"] == 1.0
    assert payload["metrics"]["dedupe_hit_rate"] == 1.0
    assert payload["metrics"]["plan_recommendation_accuracy"] == 1.0
    assert payload["metrics"]["issue_body_completeness"] == 1.0
    assert payload["metrics"]["average_prompt_tokens"] > 0
    assert payload["metrics"]["average_completion_tokens"] is None
    assert payload["policy_gate"]["workflow_gate"] == "passed"
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert "triage_failure" in payload["prompt_pack_versions"]


def _write_false_positive_prompt_eval_cases(tmp_path):
    payload = json.loads(adapter.DEFAULT_EVALUATION_CASES.read_text(encoding="utf-8-sig"))
    case = payload["cases"][0]
    case["failure_event"]["error_signature"] = "AssertionError: synthetic false positive fixture"
    case["expected"].update(
        {
            "actionable": False,
            "expected_action": "skip",
            "expected_plan_keys": [],
            "issue_body_required_sections": [],
            "false_positive": True,
        }
    )
    cases_path = tmp_path / "false-positive-fixtures.json"
    cases_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return cases_path


def test_prompt_evaluation_blocks_validation_llm_changes_when_false_positive_rate_is_high(tmp_path):
    payload = adapter.build_prompt_evaluation(
        "deterministic",
        adapter.load_config(),
        cases_path=_write_false_positive_prompt_eval_cases(tmp_path),
        changed_files=["prompt_packs/validation_llm/triage_failure.prompt.yml"],
        false_positive_threshold=0.0,
    )

    assert payload["policy_gate"]["workflow_gate"] == "blocked"
    assert payload["policy_gate"]["auto_file_enabled"] is False
    assert payload["policy_gate"]["blocking"] == ["false_positive_auto_file_rate"]


def test_prompt_evaluation_warns_but_does_not_block_unrelated_business_changes(tmp_path):
    payload = adapter.build_prompt_evaluation(
        "deterministic",
        adapter.load_config(),
        cases_path=_write_false_positive_prompt_eval_cases(tmp_path),
        changed_files=["backend/services/example.py"],
        false_positive_threshold=0.0,
    )

    assert payload["policy_gate"]["workflow_gate"] == "warning"
    assert payload["policy_gate"]["blocking_relevant_change"] is False


def test_prompt_evaluation_cli_uses_compact_success_output(capsys, tmp_path):
    output = tmp_path / "prompt-evaluation.json"

    exit_code = adapter.main(
        [
            "--json",
            "prompt-evaluation",
            "--provider",
            "github_models",
            "--changed-file",
            "prompt_packs/validation_llm/triage_failure.prompt.yml",
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"check": "prompt-evaluation"' in captured.out
    assert '"case_count": 20' in captured.out
    assert '"llm_invoked": false' in captured.out
    assert output.exists()
