from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import ci_plan_coverage as coverage


def test_execution_metrics_record_real_runs_without_changing_coverage(monkeypatch, tmp_path):
    monkeypatch.setattr(coverage, "_metrics", {})
    monkeypatch.delenv(coverage.METRICS_ENV, raising=False)
    receipt = tmp_path / "collected.txt"
    monkeypatch.setenv(coverage.RECEIPT_ENV, str(receipt))
    monkeypatch.setenv("AISTOCK_TEST_SOURCE_HEAD", "a" * 40)
    monkeypatch.setenv("AISTOCK_TEST_CHECKOUT_HEAD", "b" * 40)
    monkeypatch.setenv("AISTOCK_CI_ENV_FINGERPRINT", "prebuilt-identity")
    monkeypatch.setenv("AISTOCK_TEST_PLAN", "example")
    session = SimpleNamespace(items=[SimpleNamespace(nodeid="test_a.py::test_x")],
                              config=SimpleNamespace(invocation_params=SimpleNamespace(args=("-q",))))
    coverage.pytest_sessionstart(session)
    coverage.pytest_runtest_logreport(SimpleNamespace(nodeid="test_a.py::test_x", when="call", duration=.2))
    coverage.pytest_sessionfinish(session, 0)
    metrics = json.loads(receipt.with_suffix(".metrics.jsonl").read_text(encoding="utf-8"))
    assert metrics["executed_items"] == 1 and metrics["test_phase_seconds"] == .2
    assert metrics["source_head"] == "a" * 40 and metrics["exitstatus"] == 0
    report = coverage.summarize_execution_metrics([metrics, dict(metrics, stage="local", execution_id="other")])
    assert report["repeated_successful_runs"] == 1
    assert coverage.summarize_execution_metrics([metrics, metrics])["observed_runs"] == 1
    assert coverage.summarize_execution_metrics([metrics, dict(metrics, environment_fingerprint=None, execution_id="other")])["repeated_successful_runs"] == 0
    assert coverage._digest(["-k", "smoke"], ordered=True) != coverage._digest(["smoke", "-k"], ordered=True)
    monkeypatch.setattr(coverage, "_write_metrics", lambda *args: (_ for _ in ()).throw(OSError("disk unavailable")))
    coverage.pytest_sessionfinish(session, 0)  # telemetry must never replace a real test verdict


def _write_test(root: Path, relative_path: str) -> Path:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("def test_contract():\n    assert True\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("deleted", [False, True])
def test_verify_changed_test_coverage_requires_live_collection(tmp_path: Path, deleted: bool) -> None:
    first = "backend/tests/example/test_first.py"
    second = "tests/aistock_validation/test_second.py"
    if not deleted:
        _write_test(tmp_path, first)
        _write_test(tmp_path, second)
    payload = coverage.verify_changed_test_coverage(
        [first, second], collected_tests=[first], repo_root=tmp_path,
    )
    assert payload["workflow_gate"] == ("passed" if deleted else "blocked")
    assert payload["required_changed_test_files"] == ([] if deleted else [first, second])
    assert payload["missing_changed_test_files"] == ([] if deleted else [second])


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


@pytest.mark.parametrize("test_path,expected", [("", 0), ("backend/tests/example/test_missing.py", 2)])
def test_main_keeps_actual_coverage_verdict_when_metrics_missing(tmp_path, test_path, expected):
    if test_path:
        _write_test(tmp_path, test_path)
    changed = tmp_path / "changed.txt"
    changed.write_text(test_path + "\n", encoding="utf-8")
    receipt = tmp_path / "collected.txt"
    receipt.write_text("", encoding="utf-8")
    output = tmp_path / "result.json"
    result = coverage.main(["--changed-files-file", str(changed), "--receipt", str(receipt),
                            "--repo-root", str(tmp_path), "--output-json", str(output),
                            "--metrics-receipt", str(tmp_path / "missing.jsonl")])
    assert result == expected
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["missing_changed_test_files"] == ([test_path] if test_path else [])
    assert payload["execution_metrics"]["status"] == "not_recorded"


def test_ci_backend_step_verifies_actual_changed_test_collection() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    step = next(item for item in workflow["jobs"]["ci-verdict"]["steps"] if item.get("id") == "backend_validation")
    assert step["env"]["PYTEST_ADDOPTS"] == "-p scripts.ci_plan_coverage"
    assert all(fragment in step["run"] for fragment in (
        "python scripts/ci_plan_coverage.py", '--classifier-summary "${AISTOCK_CI_CLASSIFIER_SUMMARY}"',
        '--receipt "${AISTOCK_CI_TEST_COLLECTION_RECEIPT}"', 'backend_failures+=("changed_test_plan_coverage")'))
