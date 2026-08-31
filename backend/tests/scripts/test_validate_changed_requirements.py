from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "validate_changed_requirements.py"


def _load_subject():
    spec = importlib.util.spec_from_file_location("validate_changed_requirements", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


subject = _load_subject()


def test_delta_validates_changed_requirement_without_replaying_unchanged_conda_paths() -> None:
    base = "\n".join(
        (
            "anaconda-anon-usage @ file:///C:/b/abs/work",
            "numpy==2.3.3",
            "scipy==1.16.3",
        )
    )
    head = base + "\nhmmlearn==0.3.3\n"

    result = subject.build_delta(base, head, label="requirements.txt")

    assert result["changed_requirements"] == ["hmmlearn==0.3.3"]
    assert result["portable_constraints"] == ["hmmlearn==0.3.3", "numpy==2.3.3", "scipy==1.16.3"]
    assert result["excluded_nonportable_constraint_identities"] == ["distribution:anaconda-anon-usage"]


def test_delta_keeps_new_nonportable_requirement_visible_and_does_not_install_removals() -> None:
    changed_local = subject.build_delta("numpy==2.3.3", "numpy==2.3.3\nprivate @ file:///C:/private", label="req")
    removed = subject.build_delta("numpy==2.3.3\nscipy==1.16.3", "numpy==2.3.3", label="req")

    assert changed_local["changed_requirements"] == ["private @ file:///C:/private"]
    assert "private @ file:///C:/private" not in changed_local["portable_constraints"]
    assert removed["changed_requirements"] == []
    assert removed["removed_identities"] == ["distribution:scipy"]


@pytest.mark.parametrize(
    "text",
    (
        "numpy==2.3.3\nnumpy==2.2.0",
        "not a valid requirement ???",
        "-r nested.txt",
    ),
)
def test_requirement_contract_failures_are_explicit(text: str) -> None:
    with pytest.raises(subject.RequirementDeltaError):
        subject.parse_requirements(text, label="requirements.txt")


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def test_cli_reads_utf16_git_blobs_and_writes_portable_delta(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "test@example.invalid")
    _git(tmp_path, "config", "user.name", "test")
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(
        "local-build @ file:///C:/conda/work\nnumpy==2.3.3\n",
        encoding="utf-16",
    )
    _git(tmp_path, "add", "requirements.txt")
    _git(tmp_path, "commit", "-qm", "base")
    base = _git(tmp_path, "rev-parse", "HEAD")
    requirements.write_text(
        "local-build @ file:///C:/conda/work\nnumpy==2.3.3\nhmmlearn==0.3.3\n",
        encoding="utf-16",
    )
    _git(tmp_path, "add", "requirements.txt")
    _git(tmp_path, "commit", "-qm", "head")
    head = _git(tmp_path, "rev-parse", "HEAD")
    changed = tmp_path / "out" / "changed.txt"
    constraints = tmp_path / "out" / "constraints.txt"
    summary = tmp_path / "out" / "summary.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--repo-root",
            str(tmp_path),
            "--base-commit",
            base,
            "--head-commit",
            head,
            "--file",
            "requirements.txt",
            "--requirements-output",
            str(changed),
            "--constraints-output",
            str(constraints),
            "--summary-output",
            str(summary),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert changed.read_text(encoding="utf-8") == "hmmlearn==0.3.3\n"
    assert constraints.read_text(encoding="utf-8") == "hmmlearn==0.3.3\nnumpy==2.3.3\n"
    assert json.loads(summary.read_text(encoding="utf-8"))["changed_requirement_count"] == 1
    assert json.loads(completed.stdout)["excluded_nonportable_constraint_identities"] == ["distribution:local-build"]

    with pytest.raises(subject.RequirementDeltaError, match="revision is unavailable"):
        subject._git_blob(tmp_path, "missing-revision", "requirements.txt", allow_missing=True)
