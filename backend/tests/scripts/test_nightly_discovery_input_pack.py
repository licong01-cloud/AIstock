from __future__ import annotations

import json
from pathlib import Path

from scripts import nightly_discovery_input_pack as pack


def test_collect_changed_files_filters_bom_diff_headers_and_log_noise(tmp_path: Path) -> None:
    changed_file = tmp_path / "changed.txt"
    changed_file.write_text(
        "\ufeffChanges:\n"
        "--- Changes ---\n"
        "+++ b/scripts/llm_provider_adapter.py\n"
        "@@ -1,1 +1,1 @@\n"
        "a/scripts/nightly_discovery_input_pack.py\n"
        "b/.github/workflows/nightly.yml\n"
        "F:/Dev/AIstock/scripts/absolute-should-drop.py\n"
        "https://example.invalid/not-a-path\n"
        "scripts/nightly_discovery_input_pack.py\n",
        encoding="utf-8",
    )

    changed = pack.collect_changed_files(
        changed_files=[
            "./scripts/llm_provider_adapter.py",
            "Changes:",
            "\u9518\u7e2fests/aistock_validation/history/noisy.md",
        ],
        changed_files_file=changed_file,
        base_ref=None,
        root=tmp_path,
    )

    assert changed == [
        "scripts/llm_provider_adapter.py",
        "tests/aistock_validation/history/noisy.md",
        "scripts/nightly_discovery_input_pack.py",
        ".github/workflows/nightly.yml",
    ]


def test_build_discovery_input_pack_writes_compact_contract(tmp_path: Path) -> None:
    changed_file = tmp_path / "changed.txt"
    changed_file.write_text("\ufeffChanges:\nscripts/nightly_discovery_input_pack.py\n", encoding="utf-8")

    payload = pack.build_discovery_input_pack(
        run_id="27720422313",
        changed_files_file=changed_file,
        base_ref=None,
        root=tmp_path,
    )

    assert payload["schema_version"] == "aistock_discovery_input_pack_v1"
    assert payload["run_id"] == "27720422313"
    assert payload["changed_files"] == ["scripts/nightly_discovery_input_pack.py"]
    assert payload["input_quality"]["noise_filtered"] is True
    assert payload["production_gates"]["production_ddl_gate"] == "noop"
    assert "no_production_db_write" in payload["stop_conditions"]
    assert payload["rotation"]["readonly_only"] is True
    assert payload["discovery_statistics"]["candidate_count"] == 0


def test_rotation_uses_weekly_focus_and_changed_module_priority(tmp_path: Path) -> None:
    payload = pack.build_discovery_input_pack(
        run_id="rotation-test",
        run_date="2026-06-19",
        changed_files=["scripts/nightly_discovery_plans.py"],
        allowed_plan_keys=[
            "validation_discovery_issue_intake_readonly",
            "workflow_discovery_root_clean_guard",
            "code_intelligence_discovery_affected_tests_quality",
            "validation_center_discovery_run_record_integrity",
        ],
        base_ref=None,
        root=tmp_path,
    )

    rotation = payload["rotation"]
    assert rotation["focus_key"] == "code_intelligence_llm"
    assert rotation["changed_modules"] == ["code_intelligence"]
    assert rotation["selected_plan_keys"][0] == "code_intelligence_discovery_affected_tests_quality"
    assert len(rotation["selected_plan_keys"]) <= rotation["budget_plan_limit"]
    assert payload["discovery_statistics"]["planned_plan_count"] == len(rotation["selected_plan_keys"])


def test_rotation_explains_no_allowlisted_discovery_plan(tmp_path: Path) -> None:
    payload = pack.build_discovery_input_pack(
        run_id="rotation-empty",
        run_date="2026-06-15",
        allowed_plan_keys=["l0"],
        base_ref=None,
        root=tmp_path,
    )

    assert payload["rotation"]["focus_key"] == "workflow_validation"
    assert payload["rotation"]["selected_plan_keys"] == []
    assert payload["rotation"]["no_candidate_reason"] == "no_allowlisted_readonly_discovery_plan_selected"


def test_cli_writes_input_pack_and_changed_files(tmp_path: Path, capsys) -> None:
    source = tmp_path / "raw.txt"
    output = tmp_path / "pack.json"
    changed_output = tmp_path / "nightly-changed-files.txt"
    source.write_text("Changes:\nscripts/code_intelligence_adapter.py\n", encoding="utf-8")

    exit_code = pack.main(
        [
            "--run-id",
            "run-1",
            "--changed-files-file",
            str(source),
            "--output",
            str(output),
            "--changed-files-output",
            str(changed_output),
            "--root",
            str(tmp_path),
        ]
    )
    stdout = capsys.readouterr().out
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert stdout.startswith("PASS discovery-input-pack")
    assert payload["changed_files"] == ["scripts/code_intelligence_adapter.py"]
    assert changed_output.read_text(encoding="utf-8") == "scripts/code_intelligence_adapter.py\n"
