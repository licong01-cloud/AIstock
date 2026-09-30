"""Record and verify changed Python tests collected by selected CI plans."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable


RECEIPT_ENV = "AISTOCK_CI_TEST_COLLECTION_RECEIPT"
REPO_ROOT_ENV = "AISTOCK_CI_REPO_ROOT"
CLASSIFIER_SUMMARY_ENV = "AISTOCK_CI_CLASSIFIER_SUMMARY"


def _normalize_path(value: str) -> str:
    normalized = value.strip().lstrip("\ufeff").replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _is_python_test_path(path: str) -> bool:
    normalized = _normalize_path(path)
    name = normalized.rsplit("/", 1)[-1]
    return (
        normalized.startswith(("backend/tests/", "tests/"))
        and name.startswith("test_")
        and name.endswith(".py")
    )


def _read_lines(path: Path) -> list[str]:
    if not path.is_file():
        return []
    return [_normalize_path(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _read_classifier_changed_tests(path: Path) -> list[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid classifier summary {path}: {exc}") from exc
    changed_tests = payload.get("backend_changed_test_files") if isinstance(payload, dict) else None
    if not isinstance(changed_tests, list) or not all(isinstance(item, str) for item in changed_tests):
        raise RuntimeError(f"invalid classifier summary {path}: backend_changed_test_files must be a string list")
    return [_normalize_path(value) for value in changed_tests]


def _repo_relative(path: Path, repo_root: Path) -> str | None:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except (OSError, ValueError):
        return None


def record_collected_tests(
    paths: Iterable[Path],
    *,
    receipt_path: Path,
    repo_root: Path,
    required_paths: set[str] | None = None,
) -> list[str]:
    collected = sorted(
        {
            relative
            for path in paths
            if (relative := _repo_relative(path, repo_root)) is not None
            and _is_python_test_path(relative)
            and (required_paths is None or relative in required_paths)
        }
    )
    if not collected:
        return []
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    with receipt_path.open("a", encoding="utf-8", newline="\n") as handle:
        for path in collected:
            handle.write(path + "\n")
    return collected


def pytest_collection_finish(session: Any) -> None:
    """Pytest hook enabled by nox only when CI requests a collection receipt."""

    receipt_value = os.environ.get(RECEIPT_ENV, "").strip()
    if not receipt_value:
        return
    repo_root = Path(os.environ.get(REPO_ROOT_ENV, "").strip() or Path.cwd())
    receipt_path = Path(receipt_value)
    if not receipt_path.is_absolute():
        receipt_path = repo_root / receipt_path
    classifier_summary_value = os.environ.get(CLASSIFIER_SUMMARY_ENV, "").strip()
    required_paths: set[str] | None = None
    if classifier_summary_value:
        classifier_summary_path = Path(classifier_summary_value)
        if not classifier_summary_path.is_absolute():
            classifier_summary_path = repo_root / classifier_summary_path
        required_paths = {
            path for path in _read_classifier_changed_tests(classifier_summary_path) if _is_python_test_path(path)
        }
    item_paths = [Path(str(item.path)) for item in session.items if getattr(item, "path", None) is not None]
    record_collected_tests(
        item_paths,
        receipt_path=receipt_path,
        repo_root=repo_root,
        required_paths=required_paths,
    )


def verify_changed_test_coverage(
    changed_files: Iterable[str],
    *,
    collected_tests: Iterable[str],
    repo_root: Path,
) -> dict[str, Any]:
    required = sorted(
        {
            normalized
            for value in changed_files
            if (normalized := _normalize_path(value))
            and _is_python_test_path(normalized)
            and (repo_root / normalized).is_file()
        }
    )
    collected = sorted({_normalize_path(value) for value in collected_tests if _normalize_path(value)})
    collected_set = set(collected)
    missing = [path for path in required if path not in collected_set]
    return {
        "schema_version": "aistock_ci_test_plan_coverage_v1",
        "required_changed_test_files": required,
        "collected_test_files": collected,
        "missing_changed_test_files": missing,
        "workflow_gate": "blocked" if missing else "passed",
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--changed-file", action="append", default=[])
    parser.add_argument("--changed-files-file")
    parser.add_argument("--classifier-summary")
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-json")
    args = parser.parse_args(argv)

    changed_files = list(args.changed_file)
    if args.changed_files_file:
        changed_files.extend(_read_lines(Path(args.changed_files_file)))
    if args.classifier_summary:
        changed_files.extend(_read_classifier_changed_tests(Path(args.classifier_summary)))
    payload = verify_changed_test_coverage(
        changed_files,
        collected_tests=_read_lines(Path(args.receipt)),
        repo_root=Path(args.repo_root),
    )
    if args.output_json:
        _write_json(Path(args.output_json), payload)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["workflow_gate"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
