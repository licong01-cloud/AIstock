from __future__ import annotations

import json

from scripts import aistock_validate


def _run_cli(args: list[str]) -> int:
    parsed = aistock_validate.build_parser().parse_args(args)
    return parsed.func(parsed)


def test_record_writes_markdown_and_json_metadata(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("BACKEND_PORT", "8011")
    monkeypatch.setenv("FRONTEND_PORT", "3011")
    rc = _run_cli([
        "record",
        "--module",
        "QE Data Completeness",
        "--level",
        "L3",
        "--title",
        "Metadata Smoke",
        "--history-root",
        str(tmp_path),
    ])

    assert rc == 0
    module_dir = tmp_path / "qe-data-completeness"
    records = list(module_dir.glob("*_l3_metadata-smoke.md"))
    metadata_files = list(module_dir.glob("*_l3_metadata-smoke.json"))
    assert len(records) == 1
    assert len(metadata_files) == 1

    metadata = json.loads(metadata_files[0].read_text(encoding="utf-8"))
    assert metadata["schema_version"] == aistock_validate.RUN_METADATA_SCHEMA_VERSION
    assert metadata["module"] == "QE Data Completeness"
    assert metadata["module_slug"] == "qe-data-completeness"
    assert metadata["level"] == "L3"
    assert metadata["status"] == "created"
    assert metadata["environment"]["backend_port"] == "8011"
    assert metadata["environment"]["frontend_port"] == "3011"
    assert metadata["markdown_path"].endswith("_l3_metadata-smoke.md")
    assert metadata["coverage"] == {
        "schema_version": aistock_validate.COVERAGE_SNAPSHOT_SCHEMA_VERSION,
        "status": "not_collected",
        "line": None,
        "branch": None,
        "diff_line": None,
        "diff_branch": None,
        "snapshot_path": None,
        "quality_gates": [],
    }


def test_record_can_keep_legacy_markdown_only(tmp_path) -> None:
    rc = _run_cli([
        "record",
        "--module",
        "QE",
        "--level",
        "L1",
        "--title",
        "Markdown Only",
        "--history-root",
        str(tmp_path),
        "--no-json",
    ])

    assert rc == 0
    module_dir = tmp_path / "qe"
    assert len(list(module_dir.glob("*_l1_markdown-only.md"))) == 1
    assert not list(module_dir.glob("*.json"))


def test_record_refuses_default_history_in_canonical_root_main(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(aistock_validate, "ROOT", tmp_path)
    monkeypatch.setattr(
        aistock_validate,
        "DEFAULT_HISTORY_ROOT",
        tmp_path / "tests" / "aistock_validation" / "history",
    )
    monkeypatch.setattr(aistock_validate, "_canonical_root", lambda: tmp_path.resolve())
    monkeypatch.setattr(aistock_validate, "_git_branch", lambda cwd=None: "main")
    monkeypatch.delenv(aistock_validate.ROOT_HISTORY_OVERRIDE_ENV, raising=False)

    try:
        _run_cli([
            "record",
            "--module",
            "QE",
            "--level",
            "L3",
            "--title",
            "Root Pollution",
        ])
    except SystemExit as exc:
        assert "canonical root main" in str(exc)
    else:
        raise AssertionError("canonical root main validation history write should fail fast")

    assert not (tmp_path / "tests" / "aistock_validation" / "history").exists()


def test_record_allows_isolated_worktree_default_history(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(aistock_validate, "ROOT", tmp_path / "worktree")
    monkeypatch.setattr(
        aistock_validate,
        "DEFAULT_HISTORY_ROOT",
        tmp_path / "worktree" / "tests" / "aistock_validation" / "history",
    )
    monkeypatch.setattr(aistock_validate, "_canonical_root", lambda: (tmp_path / "canonical").resolve())
    monkeypatch.setattr(aistock_validate, "_git_branch", lambda cwd=None: "main")

    rc = _run_cli([
        "record",
        "--module",
        "QE",
        "--level",
        "L3",
        "--title",
        "Worktree Evidence",
    ])

    assert rc == 0
    assert list((tmp_path / "worktree" / "tests" / "aistock_validation" / "history" / "qe").glob("*.md"))


def test_record_allows_canonical_root_when_history_root_is_outside_default(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(aistock_validate, "ROOT", tmp_path)
    monkeypatch.setattr(
        aistock_validate,
        "DEFAULT_HISTORY_ROOT",
        tmp_path / "tests" / "aistock_validation" / "history",
    )
    monkeypatch.setattr(aistock_validate, "_canonical_root", lambda: tmp_path.resolve())
    monkeypatch.setattr(aistock_validate, "_git_branch", lambda cwd=None: "main")
    external_history = tmp_path / "tmp" / "validation-history"

    rc = _run_cli([
        "record",
        "--module",
        "QE",
        "--level",
        "L3",
        "--title",
        "External Evidence",
        "--history-root",
        str(external_history),
    ])

    assert rc == 0
    assert list((external_history / "qe").glob("*.md"))


def test_record_allows_canonical_root_with_explicit_override(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(aistock_validate, "ROOT", tmp_path)
    monkeypatch.setattr(
        aistock_validate,
        "DEFAULT_HISTORY_ROOT",
        tmp_path / "tests" / "aistock_validation" / "history",
    )
    monkeypatch.setattr(aistock_validate, "_canonical_root", lambda: tmp_path.resolve())
    monkeypatch.setattr(aistock_validate, "_git_branch", lambda cwd=None: "main")
    monkeypatch.setenv(
        aistock_validate.ROOT_HISTORY_OVERRIDE_ENV,
        aistock_validate.ROOT_HISTORY_OVERRIDE_VALUE,
    )

    rc = _run_cli([
        "record",
        "--module",
        "QE",
        "--level",
        "L3",
        "--title",
        "Override Evidence",
    ])

    assert rc == 0
    assert list((tmp_path / "tests" / "aistock_validation" / "history" / "qe").glob("*.md"))


def test_evidence_manifest_records_hash_and_missing_files(tmp_path) -> None:
    evidence_file = tmp_path / "smoke.json"
    evidence_file.write_text('{"status":"ok"}\n', encoding="utf-8")
    missing_file = tmp_path / "missing.json"
    output = tmp_path / "evidence.json"

    rc = _run_cli([
        "evidence",
        "--module",
        "qe_data_completeness",
        "--level",
        "L1",
        "--title",
        "Evidence Smoke",
        "--output",
        str(output),
        "--smoke-json",
        str(evidence_file),
        "--item",
        f"missing={missing_file}",
    ])

    assert rc == 0
    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == aistock_validate.EVIDENCE_MANIFEST_SCHEMA_VERSION
    assert manifest["module"] == "qe_data_completeness"
    assert manifest["level"] == "L1"
    assert manifest["missing_count"] == 1
    smoke = next(item for item in manifest["evidence"] if item["kind"] == "smoke_json")
    assert smoke["exists"] is True
    assert smoke["size_bytes"] == evidence_file.stat().st_size
    assert len(smoke["sha256"]) == 64
    missing = next(item for item in manifest["evidence"] if item["kind"] == "missing")
    assert missing["exists"] is False


def test_evidence_manifest_can_fail_on_missing_files(tmp_path) -> None:
    output = tmp_path / "evidence.json"
    rc = _run_cli([
        "evidence",
        "--module",
        "qe_data_completeness",
        "--output",
        str(output),
        "--include",
        str(tmp_path / "missing.txt"),
        "--fail-missing",
    ])

    assert rc == 1
    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert manifest["missing_count"] == 1
