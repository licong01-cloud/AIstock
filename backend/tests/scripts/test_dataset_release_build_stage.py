from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import dataset_release_build_stage as build_stage_script


class _CAS:
    def __init__(self, _root: Path) -> None:
        pass

    def get_json_bounded(self, _reference: str, *, max_bytes: int):
        assert max_bytes == 32 * 1024 * 1024
        return {"schema_version": "fixture-plan"}


class _Checkpoint:
    def __init__(self, **_identity) -> None:
        pass

    def checkpoint(self) -> None:
        return None


def _args(tmp_path: Path, *, staging_relative_path: str) -> argparse.Namespace:
    control = tmp_path / "control"
    candidate = tmp_path / "candidate"
    result_parent = control / "attempt_runs" / "attempt-a-7" / "build-prepare"
    result_parent.mkdir(parents=True)
    candidate.mkdir()
    return argparse.Namespace(
        stage="prepare",
        control_root=str(control),
        candidate_root=str(candidate),
        profile=str(tmp_path / "profile.yaml"),
        plan_ref="a" * 64,
        run_id="run-a",
        attempt_id="attempt-a",
        attempt_fence=7,
        pressure_rung=0,
        stage_timeout_seconds=300,
        release_id="release-a",
        release_digest="b" * 64,
        staging_relative_path=staging_relative_path,
        result_path=str(result_parent / "semantic_result.json"),
        prerequisite_ref=[],
    )


def test_build_stage_binds_staging_to_attempt_and_fence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    args = _args(tmp_path, staging_relative_path=".staging/attempt-a/7")
    profile = SimpleNamespace(
        control_root=Path(args.control_root),
        candidate_root=Path(args.candidate_root),
    )
    observed = {}
    monkeypatch.setattr(build_stage_script, "load_dataset_profile", lambda _path: profile)
    monkeypatch.setattr(build_stage_script, "CASStore", _CAS)
    monkeypatch.setattr(build_stage_script, "ChildResourceCheckpoint", _Checkpoint)

    def _run(invocation, *, checkpoint):
        observed["invocation"] = invocation
        checkpoint()
        return {"schema_version": "fixture-result"}

    monkeypatch.setattr(build_stage_script, "run_build_stage", _run)

    assert build_stage_script._run(args) == 0
    invocation = observed["invocation"]
    assert invocation.release_id == "release-a"
    assert invocation.staging_relative_path == ".staging/attempt-a/7"
    assert invocation.staging_root == (Path(args.candidate_root) / ".staging" / "attempt-a" / "7").resolve(strict=False)


def test_build_stage_rejects_legacy_release_scoped_staging_before_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    args = _args(tmp_path, staging_relative_path=".staging/release-a")
    profile = SimpleNamespace(
        control_root=Path(args.control_root),
        candidate_root=Path(args.candidate_root),
    )
    monkeypatch.setattr(build_stage_script, "load_dataset_profile", lambda _path: profile)

    with pytest.raises(ValueError, match="staging identity differs"):
        build_stage_script._run(args)


def _create_windows_junction(link: Path, target: Path) -> None:
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            (
                "& { param([string]$link,[string]$target) "
                "New-Item -ItemType Junction -Path $link -Target $target | Out-Null }"
            ),
            str(link),
            str(target),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 0 or not link.exists():
        pytest.skip(f"host junction creation is unavailable: {result.stderr.strip()}")


@pytest.mark.skipif(os.name != "nt", reason="Windows junction contract")
def test_build_stage_plain_root_rejects_logical_junction(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    junction = tmp_path / "candidate-junction"
    _create_windows_junction(junction, target)
    try:
        with pytest.raises(ValueError, match="link/reparse point"):
            build_stage_script._plain_root(junction)
    finally:
        os.rmdir(junction)


@pytest.mark.skipif(os.name != "nt", reason="Windows junction contract")
def test_build_stage_staging_chain_rejects_nested_junction(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    target = tmp_path / "outside-staging"
    candidate.mkdir()
    target.mkdir()
    junction = candidate / ".staging"
    _create_windows_junction(junction, target)
    try:
        with pytest.raises(ValueError, match="link/reparse point"):
            build_stage_script._assert_existing_plain_chain(junction / "attempt-a" / "7")
    finally:
        os.rmdir(junction)
