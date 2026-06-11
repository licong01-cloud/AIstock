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
    assert payload["schema_version"] == scheduler.REPORT_SCHEMA_VERSION
    assert payload["workflow_gate"] == "ready"
    assert payload["execution_mode"] == "warning_only_advice"
    assert payload["queue_summary"]["allowed_plan_keys"] == ["l0"]
    assert payload["issue_creation_policy"]["allowed"] is False
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
    assert "queue_count=" in captured.out
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
    assert report["llm_invocation_summary"]["invoked"] is True
    assert report["llm_invocation_summary"]["reason"] == "nightly_scheduler_advice_live_provider_json"


def test_nightly_workflow_wires_warning_only_adaptive_scheduler_job() -> None:
    workflow = (scheduler.ROOT / ".github" / "workflows" / "nightly.yml").read_text(encoding="utf-8")

    assert "Build Nightly adaptive scheduler warning report" in workflow
    assert "scripts/nightly_adaptive_scheduler.py --json" in workflow
    assert "--codegraph-freshness-json" in workflow
    assert "--invoke-llm" in workflow
    assert "llm-nightly-adaptive-scheduler.json" in workflow
