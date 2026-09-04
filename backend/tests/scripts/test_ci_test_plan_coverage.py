from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from scripts import ci_plan_coverage as coverage


def _write_test(root: Path, relative_path: str) -> Path:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("def test_contract():\n    assert True\n", encoding="utf-8")
    return path


def test_verify_changed_test_coverage_requires_actual_collection(tmp_path: Path) -> None:
    first = "backend/tests/example/test_first.py"
    second = "tests/aistock_validation/test_second.py"
    _write_test(tmp_path, first)
    _write_test(tmp_path, second)

    payload = coverage.verify_changed_test_coverage(
        [first, second],
        collected_tests=[first],
        repo_root=tmp_path,
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["required_changed_test_files"] == [first, second]
    assert payload["missing_changed_test_files"] == [second]


def test_verify_changed_test_coverage_ignores_deleted_test(tmp_path: Path) -> None:
    deleted = "backend/tests/example/test_deleted.py"

    payload = coverage.verify_changed_test_coverage(
        [deleted],
        collected_tests=[],
        repo_root=tmp_path,
    )

    assert payload["workflow_gate"] == "passed"
    assert payload["required_changed_test_files"] == []
    assert payload["missing_changed_test_files"] == []


def test_pytest_collection_hook_appends_repo_relative_receipt(monkeypatch, tmp_path: Path) -> None:
    first = _write_test(tmp_path, "backend/tests/example/test_first.py")
    second = _write_test(tmp_path, "tests/aistock_validation/test_second.py")
    outside = _write_test(tmp_path.parent, "outside/test_external.py")
    receipt = tmp_path / "tmp" / "coverage" / "collected.txt"
    classifier_summary = tmp_path / "summary.json"
    classifier_summary.write_text(
        json.dumps({"backend_changed_test_files": ["backend/tests/example/test_first.py"]}),
        encoding="utf-8",
    )
    monkeypatch.setenv(coverage.RECEIPT_ENV, "tmp/coverage/collected.txt")
    monkeypatch.setenv(coverage.REPO_ROOT_ENV, str(tmp_path))
    monkeypatch.setenv(coverage.CLASSIFIER_SUMMARY_ENV, str(classifier_summary))
    session = SimpleNamespace(
        items=[SimpleNamespace(path=second), SimpleNamespace(path=first), SimpleNamespace(path=outside)]
    )

    coverage.pytest_collection_finish(session)

    assert receipt.read_text(encoding="utf-8").splitlines() == [
        "backend/tests/example/test_first.py",
    ]


def test_main_fails_closed_and_writes_diagnostic_json(tmp_path: Path) -> None:
    test_path = "backend/tests/example/test_missing.py"
    _write_test(tmp_path, test_path)
    changed = tmp_path / "changed.txt"
    changed.write_text(test_path + "\n", encoding="utf-8")
    receipt = tmp_path / "collected.txt"
    receipt.write_text("", encoding="utf-8")
    output = tmp_path / "result.json"

    result = coverage.main(
        [
            "--changed-files-file",
            str(changed),
            "--receipt",
            str(receipt),
            "--repo-root",
            str(tmp_path),
            "--output-json",
            str(output),
        ]
    )

    assert result == 2
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["missing_changed_test_files"] == [test_path]


def test_ci_backend_step_verifies_actual_changed_test_collection() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    step = next(
        item
        for item in workflow["jobs"]["ci-verdict"]["steps"]
        if item.get("id") == "backend_validation"
    )
    env = step["env"]
    run = str(step["run"])

    assert env["AISTOCK_CI_CLASSIFIER_SUMMARY"].endswith("summary.json")
    assert env["AISTOCK_CI_TEST_COLLECTION_RECEIPT"].endswith("collected_tests.txt")
    assert env["PYTEST_ADDOPTS"] == "-p scripts.ci_plan_coverage"
    assert "python scripts/ci_plan_coverage.py" in run
    assert '--classifier-summary "${AISTOCK_CI_CLASSIFIER_SUMMARY}"' in run
    assert '--receipt "${AISTOCK_CI_TEST_COLLECTION_RECEIPT}"' in run
    assert 'backend_failures+=("changed_test_plan_coverage")' in run
