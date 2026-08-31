from __future__ import annotations

import json
from pathlib import Path

from scripts import nightly_adaptive_scheduler as scheduler


def test_nightly_adaptive_scheduler_baseline_without_changes_or_failures(tmp_path: Path) -> None:
    output = tmp_path / "scheduler.json"
    markdown = tmp_path / "scheduler.md"

    exit_code = scheduler.main(
        [
            "--provider",
            "deterministic",
            "--codegraph-freshness",
            "fresh",
            "--output",
            str(output),
            "--markdown-output",
            str(markdown),
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "aistock_public_scheduler_status_v1"
    assert payload["workflow_gate"] == "ready"
    assert payload["execution_mode"] == "warning_only_advice"
    assert payload["public_artifact"] is True
    assert payload["llm_gate"] == "degraded"
    assert payload["queue_summary"]["allowed_plan_keys"] == ["l0"]
    assert payload["llm_invocation_evidence"]["invoked"] is False
    assert payload["advice_consumption"]["warning_only"] is True
    assert "does not create GitHub Issues" in markdown.read_text(encoding="utf-8")


def test_nightly_adaptive_scheduler_maps_qe_ui_failure_to_safe_queue(tmp_path: Path) -> None:
    status_path = tmp_path / "status.json"
    status_path.write_text(json.dumps({"statuses": {"nightlyL3": "failure"}}), encoding="utf-8")

    report = scheduler.build_report(
        provider="deterministic",
        config_path=scheduler.llm_provider_adapter.DEFAULT_CONFIG_PATH,
        changed_files=[],
        statuses=scheduler.collect_statuses(status_json=status_path, inline_statuses=None),
        codegraph={"freshness": "fresh", "source": "test"},
        resource_budget_seconds=1200,
    )
    queue = {item["plan_key"]: item for item in report["queue"]}

    assert report["workflow_gate"] == "warning"
    assert "qe_archive_l3" in queue
    assert queue["qe_archive_l3"]["allowed"] is False
    assert "runner_not_enabled" in queue["qe_archive_l3"]["deferred_reason"]
    assert queue["qe_archive_backend"]["allowed"] is True
    assert report["production_actions_allowed"] is False


def test_nightly_adaptive_scheduler_resource_budget_defers_without_blocking(tmp_path: Path) -> None:
    report = scheduler.build_report(
        provider="deterministic",
        config_path=scheduler.llm_provider_adapter.DEFAULT_CONFIG_PATH,
        changed_files=[],
        statuses={"nightly_l3": "failure"},
        codegraph={"freshness": "fresh", "source": "test"},
        resource_budget_seconds=30,
    )

    assert report["workflow_gate"] in {"ready", "warning"}
    assert report["queue_summary"]["deferred_plan_keys"]
    assert any(
        reason == "resource_budget_exceeded" or str(reason).endswith(",resource_budget_exceeded")
        for reason in report["queue_summary"]["deferred_reasons"].values()
    )


def test_nightly_adaptive_scheduler_codegraph_missing_is_warning_only(tmp_path: Path) -> None:
    codegraph = scheduler.codegraph_freshness_from_artifact(tmp_path / "missing.json", None)
    report = scheduler.build_report(
        provider="deterministic",
        config_path=scheduler.llm_provider_adapter.DEFAULT_CONFIG_PATH,
        changed_files=["scripts/llm_provider_adapter.py"],
        statuses={},
        codegraph=codegraph,
        resource_budget_seconds=900,
    )

    assert codegraph["freshness"] == "missing"
    assert report["workflow_gate"] == "warning"
    assert report["queue_summary"]["allowed_plan_keys"]


def test_nightly_adaptive_scheduler_incomplete_codegraph_index_is_warning_only(tmp_path: Path) -> None:
    artifact = tmp_path / "codegraph-freshness.json"
    artifact.write_text(
        json.dumps(
            {
                "schema_version": "aistock_codegraph_freshness_v1",
                "freshness": "incomplete_index",
                "index_file_coverage": {"missing_files": ["scripts/llm_provider_adapter.py"]},
            }
        ),
        encoding="utf-8",
    )

    codegraph = scheduler.codegraph_freshness_from_artifact(artifact, None)
    report = scheduler.build_report(
        provider="deterministic",
        config_path=scheduler.llm_provider_adapter.DEFAULT_CONFIG_PATH,
        changed_files=["scripts/llm_provider_adapter.py"],
        statuses={},
        codegraph=codegraph,
        resource_budget_seconds=900,
    )

    assert codegraph["freshness"] == "stale"
    assert codegraph["raw_freshness"] == "incomplete_index"
    assert report["workflow_gate"] == "warning"
    assert report["queue_summary"]["allowed_plan_keys"]


def test_nightly_adaptive_scheduler_compact_stdout(capsys, tmp_path: Path) -> None:
    output = tmp_path / "scheduler.json"

    exit_code = scheduler.main(
        [
            "--provider",
            "deterministic",
            "--codegraph-freshness",
            "fresh",
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "nightly-adaptive-scheduler:" in captured.out
    assert "warning_only_advice" in captured.out
    assert "schema_version" not in captured.out
    assert output.exists()



def test_nightly_adaptive_scheduler_can_invoke_llm_advice(monkeypatch, tmp_path: Path) -> None:
    original_build = scheduler.llm_provider_adapter.build_nightly_scheduler_advice

    def fake_build(provider, config, **kwargs):
        payload = original_build(
            "deterministic",
            config,
            changed_files=kwargs.get("changed_files"),
            recent_failure_modules=kwargs.get("recent_failure_modules"),
            codegraph_freshness=kwargs.get("codegraph_freshness", "fresh"),
            resource_budget_seconds=kwargs.get("resource_budget_seconds"),
        )
        payload["provider"] = provider
        payload["model"] = "deepseek/deepseek-r1"
        payload["llm_invocation_evidence"]["invoked"] = True
        payload["llm_invocation_evidence"]["reason"] = "nightly_scheduler_advice_live_provider_json"
        payload["llm_advice"] = {"suggested_plan_keys": ["l0"], "advisory_only": True}
        return payload

    monkeypatch.setattr(scheduler.llm_provider_adapter, "build_nightly_scheduler_advice", fake_build)

    report = scheduler.build_report(
        provider="github_models",
        config_path=scheduler.llm_provider_adapter.DEFAULT_CONFIG_PATH,
        changed_files=["scripts/llm_provider_adapter.py"],
        statuses={},
        codegraph={"freshness": "fresh", "source": "test"},
        resource_budget_seconds=900,
        invoke_llm=True,
    )

    assert report["provider"] == "github_models"
    assert report["llm_invoked"] is True
    assert report["llm_invocation_evidence"]["invoked"] is True


def test_nightly_adaptive_scheduler_public_artifact_keeps_llm_consumption_evidence(
    monkeypatch, tmp_path: Path
) -> None:
    original_build = scheduler.llm_provider_adapter.build_nightly_scheduler_advice

    def fake_build(provider, config, **kwargs):
        payload = original_build(
            "deterministic",
            config,
            changed_files=kwargs.get("changed_files"),
            recent_failure_modules=kwargs.get("recent_failure_modules"),
            codegraph_freshness=kwargs.get("codegraph_freshness", "fresh"),
            resource_budget_seconds=kwargs.get("resource_budget_seconds"),
        )
        payload["provider"] = provider
        payload["effective_provider"] = "deepseek_api"
        payload["effective_model"] = "deepseek-v4-pro"
        payload["llm_invocation_evidence"]["provider"] = "deepseek_api"
        payload["llm_invocation_evidence"]["model"] = "deepseek-v4-pro"
        payload["llm_invocation_evidence"]["invoked"] = True
        payload["llm_invocation_evidence"]["fallback_used"] = True
        payload["llm_invocation_evidence"]["provider_chain"] = [
            {"provider": "github_models", "status": "failed"},
            {"provider": "deepseek_api", "status": "invoked"},
        ]
        payload["llm_advice"] = {"suggested_plan_keys": ["l0"], "advisory_only": True}
        payload["advised_plan_keys"] = ["l0"]
        payload["advice_consumption"] = {
            "advice_consumed": True,
            "consumption_mode": "allowlisted_queue_intersection_only",
            "warning_only": True,
        }
        return payload

    monkeypatch.setattr(scheduler.llm_provider_adapter, "build_nightly_scheduler_advice", fake_build)
    output = tmp_path / "scheduler.json"

    exit_code = scheduler.main(
        [
            "--provider",
            "github_models",
            "--codegraph-freshness",
            "fresh",
            "--changed-file",
            "scripts/llm_provider_adapter.py",
            "--invoke-llm",
            "--output",
            str(output),
        ]
    )
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["public_artifact"] is True
    assert payload["effective_provider"] == "deepseek_api"
    assert payload["llm_invoked"] is True
    assert payload["llm_invocation_evidence"]["fallback_used"] is True
    assert payload["advised_plan_keys"] == ["l0"]
    assert payload["advice_consumption"]["advice_consumed"] is True
    assert payload["llm_gate"] == "ready"


def test_nightly_adaptive_scheduler_fail_on_llm_error_returns_nonzero(monkeypatch, tmp_path: Path) -> None:
    def fake_build(*args, **kwargs):
        raise scheduler.llm_provider_adapter.ProviderAdapterError("deepseek_api inference failed status=429")

    monkeypatch.setattr(scheduler.llm_provider_adapter, "build_nightly_scheduler_advice", fake_build)

    output = tmp_path / "scheduler.json"
    exit_code = scheduler.main(
        [
            "--provider",
            "deepseek_api",
            "--codegraph-freshness",
            "fresh",
            "--invoke-llm",
            "--fail-on-llm-error",
            "--output",
            str(output),
        ]
    )
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert exit_code == 2
    assert payload["workflow_gate"] == "blocked"
    assert payload["llm_gate"] == "degraded"
    assert payload["llm_invoked"] is False
    assert "429" in payload["error"]


def test_nightly_adaptive_scheduler_includes_code_intelligence_refs_in_public_artifact(
    tmp_path: Path,
) -> None:
    code_refs = tmp_path / "code-intelligence-summary.json"
    code_refs.write_text(
        json.dumps(
            {
                "schema_version": "aistock_code_intelligence_summary_v1",
                "context_ref": "tmp/validation/code-intelligence/9005/codegraph-context.md",
                "affected_tests_ref": "tmp/validation/code-intelligence/9005/affected-tests.json",
                "affected_tests_count": 3,
                "understand_anything_summary_ref": "tmp/validation/code-intelligence/9005/ua-validation-summary.md",
                "understand_anything_status": "available",
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "scheduler.json"

    exit_code = scheduler.main(
        [
            "--provider",
            "deterministic",
            "--codegraph-freshness",
            "fresh",
            "--code-intelligence-json",
            str(code_refs),
            "--output",
            str(output),
            "--json",
        ]
    )
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["input_refs"]["code_intelligence_refs"]["affected_tests_count"] == 3
    assert payload["public_artifact"] is True


def test_collect_changed_files_filters_diff_headers_and_bom(tmp_path: Path) -> None:
    changed_files_file = tmp_path / "changed.txt"
    changed_files_file.write_text(
        "\ufeff--- Changes ---\n"
        "+++ b/scripts/llm_provider_adapter.py\n"
        "@@ -1,1 +1,1 @@\n"
        "scripts/llm_provider_adapter.py\n"
        "F:/Dev/AIstock/scripts/nightly_adaptive_scheduler.py\n",
        encoding="utf-8",
    )

    collected = scheduler.collect_changed_files(
        changed_files=["./scripts/llm_provider_adapter.py", "https://example.com/nope"],
        changed_files_file=changed_files_file,
        base_ref=None,
        root=scheduler.ROOT,
    )

    assert collected == ["scripts/llm_provider_adapter.py"]


def test_nightly_execution_plan_uses_catalog_and_deduplicates_sessions() -> None:
    plan = scheduler.build_nightly_execution_plan(
        ["backend/services/simulation_runtime/ops.py", "backend/services/simulation_runtime/ops.py"],
        watermark="abc123",
        head_commit="def456",
    )

    assert plan["selected_sessions"].count("simulation_core_l2") == 1
    assert "paper_v2_l3" not in plan["selected_sessions"]
    assert plan["watermark"] == "abc123"
    assert plan["advance_change_window_on_durable_receipt"] is True
    assert plan["retry_failed_sessions_from_receipt"] is True
    assert plan["session_positional_args"]["l0"] == ["backend/services/simulation_runtime/ops.py"]


def test_nightly_execution_plan_retries_only_failed_sessions_plus_new_impact() -> None:
    plan = scheduler.build_nightly_execution_plan(
        ["frontend/src/app/watchlist/page.tsx"],
        watermark="previous-head",
        head_commit="current-head",
        retry_context={
            "failed_sessions": ["qe_archive_backend"],
            "change_scoped_files": [],
            "source_head": "previous-head",
        },
    )

    assert "qe_archive_backend" in plan["selected_sessions"]
    assert "watchlist_ui" in plan["selected_sessions"]
    assert plan["retry_sessions"] == ["qe_archive_backend"]
    assert plan["retry_source_head"] == "previous-head"
    assert "simulation_core_l2" not in plan["retry_sessions"]


def test_nightly_execution_plan_preserves_failed_l0_scope_without_new_changes() -> None:
    plan = scheduler.build_nightly_execution_plan(
        [],
        watermark="previous-head",
        head_commit="current-head",
        retry_context={
            "failed_sessions": ["l0"],
            "change_scoped_files": ["scripts/nightly_session_runner.py"],
            "source_head": "previous-head",
        },
    )

    assert plan["selected_sessions"] == ["l0"]
    assert plan["changed_files"] == []
    assert plan["session_positional_args"]["l0"] == ["scripts/nightly_session_runner.py"]


def test_load_retry_context_binds_results_to_previous_plan_head(tmp_path: Path) -> None:
    results = tmp_path / "session-results.json"
    previous_plan = tmp_path / "execution-plan.json"
    results.write_text(
        json.dumps(
            [
                {"session": "l0", "result": "failure"},
                {"session": "watchlist_ui", "result": "success"},
            ]
        ),
        encoding="utf-8",
    )
    previous_plan.write_text(
        json.dumps(
            {
                "schema_version": "aistock_nightly_execution_plan_v1",
                "head_commit": "previous-head",
                "changed_files": ["noxfile.py"],
                "selected_sessions": ["l0", "watchlist_ui"],
            }
        ),
        encoding="utf-8",
    )

    context = scheduler.load_retry_context(
        results_path=results,
        plan_path=previous_plan,
        expected_head="previous-head",
    )

    assert context == {
        "failed_sessions": ["l0"],
        "change_scoped_files": ["noxfile.py"],
        "source_head": "previous-head",
    }


def test_load_retry_context_retries_planned_sessions_missing_from_partial_receipt(tmp_path: Path) -> None:
    results = tmp_path / "session-results.json"
    previous_plan = tmp_path / "execution-plan.json"
    results.write_text(json.dumps([{"session": "l0", "result": "success"}]), encoding="utf-8")
    previous_plan.write_text(
        json.dumps(
            {
                "schema_version": "aistock_nightly_execution_plan_v1",
                "head_commit": "previous-head",
                "changed_files": ["noxfile.py"],
                "selected_sessions": ["l0", "watchlist_ui"],
            }
        ),
        encoding="utf-8",
    )

    context = scheduler.load_retry_context(
        results_path=results,
        plan_path=previous_plan,
        expected_head="previous-head",
    )

    assert context["failed_sessions"] == ["watchlist_ui"]
    assert context["change_scoped_files"] == []


def test_nightly_full_run_excludes_changed_file_only_plans() -> None:
    plan = scheduler.build_nightly_execution_plan([], full_run=True, head_commit="def456")

    assert plan["full_run"] is True
    assert plan["excluded_change_file_only_plans"] == ["l0"]
    assert "l0" not in plan["selected_plan_keys"]
    assert "l0" not in plan["selected_sessions"]
    assert plan["selected_sessions"]


def test_nightly_execution_plan_fails_closed_for_unmapped_code(monkeypatch) -> None:
    monkeypatch.setattr(
        scheduler.issue_flow,
        "select_validation",
        lambda _paths: {"ownership": {"unmatched_files": ["backend/unknown_runtime.py"]}},
    )

    try:
        scheduler.build_nightly_execution_plan(["backend/unknown_runtime.py"])
    except ValueError as exc:
        assert "unmapped executable code" in str(exc)
    else:
        raise AssertionError("Nightly must fail closed for unmapped executable code")


def test_nightly_workflow_wires_warning_only_adaptive_scheduler_job() -> None:
    import yaml

    workflow = (scheduler.ROOT / ".github" / "workflows" / "nightly.yml").read_text(encoding="utf-8")
    parsed = yaml.safe_load(workflow)
    dispatch = parsed.get("on", parsed.get(True))["workflow_dispatch"]["inputs"]

    assert "AISTOCK_LLM_ENV_FILE: F:/Dev/AIstock/.env" in workflow
    assert "Build compact code intelligence refs for LLM advice" in workflow
    assert "validate-config `\n            --provider deepseek_api `\n            --require-api-key" in workflow
    assert "Build LLM nightly discovery hypotheses" in workflow
    assert "nightly-discovery-hypothesis" in workflow
    assert "llm-hypotheses.json" in workflow
    assert "selected-plans.json" in workflow
    assert "Build Nightly LLM design drift audit" in workflow
    assert "scripts/nightly_design_drift_audit.py --json" in workflow
    assert "--invoke-llm `\n            --fail-on-llm-error `\n            --run-id" in workflow
    assert "--fail-on-llm-error" in workflow
    assert "design-drift-audit.json" in workflow
    assert "design-drift-audit.md" in workflow
    assert "LLM design drift audit failed; retrying deterministic fallback." not in workflow
    assert "retrying deterministic fallback" not in workflow
    assert "Build Nightly LLM silent degradation audit" in workflow
    assert "scripts/nightly_silent_degradation_audit.py --json" in workflow
    assert "silent-degradation-audit.json" in workflow
    assert "silent-degradation-audit.md" in workflow
    assert "LLM silent degradation audit failed; retrying deterministic fallback." not in workflow
    assert 'nightly_discovery_input_pack.py `\n            --run-id "${{ github.run_id }}"' in workflow
    assert '--codegraph-freshness-json "$outDir/codegraph-freshness.json"' in workflow
    assert "Build Nightly BugCandidate draft queue" in workflow
    assert "scripts/nightly_bug_candidate_queue.py --json build" in workflow
    assert "bug-candidates" in workflow
    assert "Promote ready Nightly BugCandidates to Issue workflow" in workflow
    assert "github.event_name == 'schedule' ||" in workflow
    assert "inputs.llm_triage_mode == 'opt_in_auto_file'" in workflow
    assert "inputs.llm_auto_file_opt_in" in workflow
    assert "issues: write" in workflow
    assert "promote-nightly-candidate" in workflow
    assert '$extraArgs = @()' in workflow
    assert "--opt-in-auto-file" in workflow
    assert "--create-registry-worktree" in workflow
    assert "Build Nightly adaptive scheduler warning report" in workflow
    assert "scripts/nightly_adaptive_scheduler.py --json" in workflow
    assert "scripts/nightly_adaptive_scheduler.py --json `\n            --provider deepseek_api `" in workflow
    assert "--codegraph-freshness-json" in workflow
    assert "--code-intelligence-json" in workflow
    assert "--invoke-llm" in workflow
    assert "llm-nightly-adaptive-scheduler.json" in workflow
    assert "llm-value-summary" in workflow
    assert "llm-value-summary.md" in workflow
    assert "LLM + Code Intelligence Value" in workflow
    assert 'cat "${LLM_VALUE_MD}" >> "${SUMMARY_DIR}/nightly_${RUN_ID}.md"' in workflow
    assert "full_nightly_run:" in workflow
    assert dispatch["run_nightly_l3"]["default"] is True
    assert dispatch["full_nightly_run"]["default"] is False
    assert "Build receipt-watermark Nightly execution plan" in workflow
    assert '--frontend-node-modules-source "${env:AISTOCK_SELF_HOSTED_SOURCE}/frontend/node_modules"' in workflow
    assert "Verify prebuilt AIstock-CI and frontend dependencies" in workflow
    assert "conda run -n AIstock-CI python scripts/ci_environment_verify.py" in workflow
    assert "frontend/node_modules/@playwright/test/cli.js" in workflow
    assert "frontend/node_modules/typescript/bin/tsc" in workflow
    assert "frontend/node_modules/next/dist/bin/next" in workflow
    assert "dependency installation is prohibited in Nightly" in workflow
    assert "conda run -n AIstock-CI python scripts/nightly_adaptive_scheduler.py" in workflow
    assert "conda run -n AIstock-CI python scripts/nightly_session_runner.py" in workflow
    assert "Select prior durable Nightly L3 receipt" in workflow
    assert "retry_run_id: ${{ steps.select_nightly_receipt.outputs.run_id }}" in workflow
    assert "retry_source_head: ${{ steps.select_nightly_receipt.outputs.head_sha }}" in workflow
    assert "--json databaseId,headSha" in workflow
    assert "--jq '.[] | [.databaseId, .headSha] | @tsv'" in workflow
    assert 'gh run download "${candidate_run_id}"' in workflow
    assert "ConvertFrom-Json -ErrorAction Stop" not in workflow
    assert "Upload selected Nightly L3 retry receipt" in workflow
    assert "Download selected Nightly L3 retry receipt" in workflow
    assert "nightly-l3-retry-source-${{ github.run_id }}" in workflow
    assert "RETRY_SOURCE_RUN_ID: ${{ needs.runner-preflight.outputs.retry_run_id }}" in workflow
    assert "RETRY_SOURCE_HEAD: ${{ needs.runner-preflight.outputs.retry_source_head }}" in workflow
    assert 'gh run list --repo "${env:GITHUB_REPOSITORY}"' not in workflow
    assert 'if (-not $retryRunId -or -not $watermark)' in workflow
    assert "Runner preflight Nightly receipt artifact is incomplete" in workflow
    assert "No prior scheduled Nightly run with a durable session receipt is available" in workflow
    assert 'if ($env:FULL_NIGHTLY_RUN -eq "true")' in workflow
    assert "--plan-selection-output" in workflow
    assert "--fail-on-blocked" in workflow
    assert 'git cat-file -e "$watermark^{commit}"' in workflow
    assert "Receipt-bound scheduled Nightly watermark is unavailable locally" in workflow
    assert '"--retry-results-json", $retryResults' in workflow
    assert '"--retry-plan-json", $retryPlan' in workflow
    assert '"--retry-source-head", $watermark' in workflow
    assert "using explicit full-run fallback" not in workflow
    assert workflow.count('$extraArgs += "--full-run"') == 1
    assert "scripts/nightly_session_runner.py" in workflow
    assert '--plan "$outDir/execution-plan.json"' in workflow
    assert "--session-timeout-seconds 1200" in workflow
    assert "--total-timeout-seconds 6300" in workflow
    assert "foreach ($session in $plan.selected_sessions)" not in workflow
    assert "nox -s paper_v2_l3" not in workflow
    assert "id: upload_nightly_l3" in workflow
    assert "steps.upload_nightly_l3.outcome == 'failure'" in workflow
    assert workflow.count("name: nightly-l3-results-${{ github.run_id }}") == 3
    assert "overwrite: true" in workflow


def test_nightly_workflow_always_materializes_discovery_input_pack_handoff() -> None:
    workflow = (scheduler.ROOT / ".github" / "workflows" / "nightly.yml").read_text(encoding="utf-8")
    blocks = [block for block in workflow.split("      - name: ") if "nightly-changed-files.txt" in block]

    assert len(blocks) == 2
    assert workflow.count("scripts/nightly_discovery_input_pack.py") == 2
    assert workflow.count("--run-date") == 2
    assert workflow.count("--allowed-plan-key validation_catalog_integrity") >= 2
    assert workflow.count("--allowed-plan-key workflow_discovery_root_clean_guard") >= 2
    assert workflow.count("--allowed-plan-key validation_semantic_drift_discovery_readonly") >= 2
    assert workflow.count("--previous-candidate-manifest") == 2
    assert workflow.count("tmp/validation/nightly_failure_issue/candidate_history/latest/manifest.json") == 2
    assert "--default-plan-key validation_semantic_drift_discovery_readonly" in workflow
    assert "git diff --name-only HEAD~1 HEAD" not in workflow
    for block in blocks:
        assert "--base-ref HEAD~1" in block
        assert '--output "$outDir/discovery-input-pack.json"' in block
        assert "--changed-files-output $changedFile" in block
        assert "New-Item -ItemType File -Force -Path $changedFile | Out-Null" not in block
        assert "Clear-Content -Path $changedFile" not in block
