from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path, PureWindowsPath
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.lease import ClaimedAttempt
from backend.services.dataset_release.profile import load_dataset_profile
from backend.services.dataset_release.resource_budget import ResourceAdmissionClass
from backend.services.dataset_release.worker import (
    LeaseOwnerSnapshot,
    PublishRecoveryConflict,
    WorkResourceSpec,
    WorkerError,
)
from scripts.dataset_release_control_store import main as control_store_main
from scripts.dataset_release_worker import _read_repo_code_sha, build_default_registry
from scripts.dataset_release_worker import main as worker_main
import scripts.dataset_release_worker as worker_cli


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
PROFILE = REPOSITORY_ROOT / "configs" / "datasets" / "qe_backtest_monthly_v1.yaml"


def test_resource_gate_factory_forwards_approved_workload_class(monkeypatch) -> None:
    profile = load_dataset_profile(PROFILE)
    observed = {}

    def fake_gate(bound_profile, **kwargs):
        observed["profile"] = bound_profile
        observed.update(kwargs)
        return "gate"

    monkeypatch.setattr(worker_cli, "ResourceGate", fake_gate)
    factory = worker_cli.build_resource_gate_factory(profile, lambda: None)
    resources = WorkResourceSpec(
        profile.resource_policy,
        hybrid_wsl=False,
        admission_class=ResourceAdmissionClass.RESOLUTION_LIGHT,
    )

    assert factory(resources, "resolution-admission") == "gate"
    assert observed["profile"] is profile
    assert observed["admission_class"] is ResourceAdmissionClass.RESOLUTION_LIGHT


def test_worker_help_exposes_only_bounded_modes(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        worker_main(["--help"])
    assert exc.value.code == 0
    output = capsys.readouterr().out
    assert "--once" in output
    assert "--drain" in output and "--max-jobs" in output
    assert "--serve" in output
    assert "--reconcile" in output
    assert "--preflight" in output
    assert "--control-root" in output and "--profile" in output
    assert "--code-sha" not in output


def test_drain_requires_positive_max_jobs() -> None:
    with pytest.raises(SystemExit) as exc:
        worker_main(
            [
                "--drain",
                "--control-root",
                "X:/missing",
                "--profile",
                str(PROFILE),
            ]
        )
    assert exc.value.code == 2


def test_runtime_worker_does_not_initialize_missing_control_root(tmp_path) -> None:
    root = tmp_path / "must-remain-missing"
    result = worker_main(
        [
            "--once",
            "--control-root",
            str(root),
            "--profile",
            str(PROFILE),
        ]
    )
    assert result == 2
    assert not root.exists()


def test_worker_rejects_same_schema_profile_outside_repo_allowlist(
    tmp_path,
    capsys,
) -> None:
    copied_profile = tmp_path / "qe_backtest_monthly_v1.yaml"
    copied_profile.write_text(PROFILE.read_text(encoding="utf-8"), encoding="utf-8")
    root = tmp_path / "control"

    result = worker_main(
        [
            "--once",
            "--control-root",
            str(root),
            "--profile",
            str(copied_profile),
        ]
    )

    error = json.loads(capsys.readouterr().err)
    assert result == 2
    assert error["error_code"] == "BLOCKED_PROFILE_NOT_ALLOWLISTED"
    assert not root.exists()


def test_default_production_registry_wires_every_real_contract(tmp_path) -> None:
    control_root = tmp_path / "control"
    candidate_root = tmp_path / "candidates"
    store = ControlStore.initialize(control_root)
    candidate_root.mkdir()
    profile = replace(
        load_dataset_profile(PROFILE),
        control_root=PureWindowsPath(str(control_root)),
        candidate_root=PureWindowsPath(str(candidate_root)),
    )

    registry = build_default_registry(profile, store=store)

    registry.assert_production_ready()
    assert registry.missing == ()
    assert registry.resolution is not None and registry.build is not None
    assert registry.publish_recovery is not None
    assert registry.wsl_quiescence is not None
    assert "scripts/dataset_release_build_stage.py" in registry.dependency_paths
    with pytest.raises(PublishRecoveryConflict):
        registry.publish_recovery.recover_and_finalize(run={}, claim=ClaimedAttempt("dsa_fixture", 1, 1))
    assert (
        registry.wsl_quiescence(
            LeaseOwnerSnapshot(
                attempt_id="dsa_missing",
                attempt_kind="BUILD",
                owner_identity="fixture",
                host=None,
                owner_pid=None,
                owner_create_time=None,
                worker_instance_id=None,
                code_sha=None,
                capability_digest=None,
                hybrid_wsl=True,
                expires_at="2026-08-11T00:00:00+00:00",
                lease_state="ORPHAN_HOLD",
            )
        )
        == "unknown"
    )


def test_worker_code_identity_comes_from_git_and_dirty_dependencies_block() -> None:
    calls = []
    responses = iter(
        [
            SimpleNamespace(returncode=0, stdout="a" * 40 + "\n"),
            SimpleNamespace(returncode=0, stdout=""),
        ]
    )

    def clean_runner(command, **kwargs):
        calls.append((command, kwargs))
        return next(responses)

    dependency = "backend/services/dataset_release/worker.py"
    assert _read_repo_code_sha((dependency,), runner=clean_runner) == ("a" * 40)
    assert calls[0][0][-3:] == ["rev-parse", "--verify", "HEAD"]
    assert dependency in calls[1][0]
    assert "--untracked-files=all" in calls[1][0]

    dirty_responses = iter(
        [
            SimpleNamespace(returncode=0, stdout="b" * 40 + "\n"),
            SimpleNamespace(returncode=0, stdout=f" M {dependency}\n"),
        ]
    )
    with pytest.raises(WorkerError) as exc:
        _read_repo_code_sha(
            (dependency,),
            runner=lambda _command, **_kwargs: next(dirty_responses),
        )
    assert exc.value.code == "BLOCKED_WORKER_CODE_DIRTY"


def test_explicit_reconcile_is_control_only_and_does_not_start_worker_or_git(
    tmp_path,
    capsys,
    monkeypatch,
) -> None:
    root = tmp_path / "control"
    ControlStore.initialize(root)
    forbidden_calls = []

    def forbidden(*_args, **_kwargs):
        forbidden_calls.append(True)
        raise AssertionError("reconcile must not enter Worker/git startup")

    monkeypatch.setattr(worker_cli, "_same_windows_path", lambda *_args: True)
    monkeypatch.setattr(
        worker_cli,
        "resolve_previous_month_trading_cutoff",
        lambda value: date(value.year, value.month, 1).replace(day=1) - timedelta(days=1),
        raising=False,
    )
    monkeypatch.setattr(worker_cli, "_read_repo_code_sha", forbidden)
    monkeypatch.setattr(worker_cli, "DatasetReleaseWorker", forbidden)

    result = worker_main(
        [
            "--reconcile",
            "--control-root",
            str(root),
            "--profile",
            str(PROFILE),
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert result == 0 and output["ok"] is True
    assert output["mode"] == "reconcile" and len(output["items"]) == 3
    assert forbidden_calls == []
    assert list((root / "worker_heartbeats").iterdir()) == []


def test_preflight_validates_real_registry_without_claim_heartbeat_or_worker(
    tmp_path,
    capsys,
    monkeypatch,
) -> None:
    control_root = tmp_path / "control"
    candidate_root = tmp_path / "candidates"
    ControlStore.initialize(control_root)
    candidate_root.mkdir()
    profile = replace(
        load_dataset_profile(PROFILE),
        control_root=PureWindowsPath(str(control_root)),
        candidate_root=PureWindowsPath(str(candidate_root)),
    )
    before = {
        path.relative_to(control_root).as_posix(): path.read_bytes()
        for path in control_root.rglob("*")
        if path.is_file()
    }

    def forbidden(*_args, **_kwargs):
        raise AssertionError("preflight must not start Worker/heartbeat")

    monkeypatch.setattr(worker_cli, "load_dataset_profile", lambda _path: profile)
    monkeypatch.setattr(worker_cli, "DatasetReleaseWorker", forbidden)
    monkeypatch.setattr(worker_cli, "WorkerHeartbeatStore", forbidden)
    monkeypatch.setattr(worker_cli, "_read_repo_code_sha", lambda _paths: "a" * 40)

    result = worker_main(
        [
            "--preflight",
            "--control-root",
            str(control_root),
            "--profile",
            str(PROFILE),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    after = {
        path.relative_to(control_root).as_posix(): path.read_bytes()
        for path in control_root.rglob("*")
        if path.is_file()
    }
    assert result == 0 and payload["registry_gate"] == "ready"
    assert payload["code_sha"] == "a" * 40
    assert payload["safety"] == {
        "claims": 0,
        "heartbeat_writes": 0,
        "worker_started": False,
        "data_process_started": False,
        "database_writes": 0,
        "candidate_writes": 0,
        "production_writes": 0,
        "service_process_controls": 0,
    }
    assert before == after
    assert list((control_root / "worker_heartbeats").iterdir()) == []
    assert not (control_root / "guardian_states").exists()


def test_control_store_init_is_explicit_idempotent_and_status_is_read_only(
    tmp_path,
    capsys,
) -> None:
    root = tmp_path / "control"
    profile = replace(load_dataset_profile(PROFILE), control_root=root)
    common = [
        "--profile",
        str(PROFILE),
        "--control-root",
        str(root),
        "--expected-version",
        "1",
    ]

    assert control_store_main(["init", *common], _profile_override=profile) == 0
    initialized = json.loads(capsys.readouterr().out)
    identity = ControlStore(root).identity
    assert initialized["ok"] is True
    assert initialized["control_store_id"] == identity["control_store_id"]

    before = sorted(path.relative_to(root).as_posix() for path in root.rglob("*"))
    assert control_store_main(["status", *common], _profile_override=profile) == 0
    status = json.loads(capsys.readouterr().out)
    after = sorted(path.relative_to(root).as_posix() for path in root.rglob("*"))
    assert status["ok"] is True
    assert before == after

    assert control_store_main(["migrate", *common], _profile_override=profile) == 0
    migrated = json.loads(capsys.readouterr().out)
    assert migrated["migration_applied"] is False
    assert migrated["from_version"] == migrated["to_version"] == 1


def test_control_store_admin_rejects_non_profile_root_without_creating_it(tmp_path, capsys) -> None:
    allowed = tmp_path / "allowed"
    supplied = tmp_path / "not-allowed"
    profile = replace(load_dataset_profile(PROFILE), control_root=allowed)

    result = control_store_main(
        [
            "init",
            "--profile",
            str(PROFILE),
            "--control-root",
            str(supplied),
            "--expected-version",
            "1",
        ],
        _profile_override=profile,
    )

    assert result == 2
    assert not supplied.exists()
    assert json.loads(capsys.readouterr().err)["ok"] is False


def test_control_store_admin_rejects_unknown_migration_without_mutation(tmp_path, capsys) -> None:
    root = tmp_path / "control"
    profile = replace(load_dataset_profile(PROFILE), control_root=root)
    common = ["--profile", str(PROFILE), "--control-root", str(root)]
    assert (
        control_store_main(
            ["init", *common, "--expected-version", "1"],
            _profile_override=profile,
        )
        == 0
    )
    capsys.readouterr()
    before = (root / "control.sqlite3").stat().st_mtime_ns

    result = control_store_main(
        ["migrate", *common, "--expected-version", "2"],
        _profile_override=profile,
    )

    assert result == 2
    assert (root / "control.sqlite3").stat().st_mtime_ns == before
    assert json.loads(capsys.readouterr().err)["ok"] is False
