from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import noxfile  # noqa: E402


def _reset_nox_env_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(noxfile, "_VALIDATION_ENV_LOADED", False)


def test_l0_scan_paths_use_explicit_scope_without_git(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(noxfile.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("git should not run"))

    assert noxfile._l0_scan_paths(["scripts\\issue_flow.py", "scripts/issue_flow.py"]) == [
        "scripts/issue_flow.py"
    ]


def test_l0_scan_paths_use_nightly_scope_file_without_git(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    scope_file = tmp_path / "l0-scope.json"
    scope_file.write_text(
        json.dumps(["scripts\\issue_flow.py", "backend/tests/test_example.py"]),
        encoding="utf-8",
    )
    monkeypatch.setenv(noxfile.NIGHTLY_SESSION_ARGS_FILE_ENV, str(scope_file))
    monkeypatch.setattr(noxfile.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("git should not run"))

    assert noxfile._l0_scan_paths([]) == ["scripts/issue_flow.py", "backend/tests/test_example.py"]


def test_l0_scan_paths_default_to_branch_and_worktree_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []
    outputs = iter(
        [
            "scripts/issue_flow.py\n",
            "noxfile.py\nscripts/issue_flow.py\n",
            "backend/tests/test_noxfile_validation_env.py\n",
            "backend/services/new_feature.py\n",
        ]
    )

    def fake_run(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(args)
        return subprocess.CompletedProcess(args, 0, stdout=next(outputs), stderr="")

    monkeypatch.setattr(noxfile.subprocess, "run", fake_run)

    assert noxfile._l0_scan_paths([]) == [
        "scripts/issue_flow.py",
        "noxfile.py",
        "backend/tests/test_noxfile_validation_env.py",
        "backend/services/new_feature.py",
    ]
    assert commands[0] == [
        "git",
        "diff",
        "--name-only",
        "--diff-filter=ACMRT",
        "origin/main...HEAD",
        "--",
    ]


def test_l0_skill_validation_stays_within_changed_path_scope() -> None:
    paths = [".codex/skills/verify-aistock-feature/SKILL.md", "scripts/issue_flow.py"]

    assert noxfile._path_scope_includes(paths, ".codex/skills/verify-aistock-feature") is True
    assert noxfile._path_scope_includes(paths, ".codex/skills/fix-aistock-issue") is False


def test_validation_registry_l0_keeps_business_dependencies_out(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest_args: list[str] = []

    class DummySession:
        def run(self, *_args: object, **_kwargs: object) -> None:
            return None

    def capture_pytest(_session: object, *args: str) -> None:
        pytest_args.extend(args)

    monkeypatch.setattr(noxfile, "_run_pytest", capture_pytest)
    noxfile.validation_module_registry_l0(DummySession())  # type: ignore[arg-type]

    assert "backend/tests/test_validation_module_ownership.py" in pytest_args
    assert "backend/tests/test_validation_ui_target_catalog.py" not in pytest_args


def _configure_hmm_pr_targets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    changed_files: list[str],
    existing_paths: list[str] | None = None,
) -> None:
    for relative in [*noxfile.HMM_RISK_PR_SMOKE_TESTS, *(existing_paths or [])]:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# test fixture\n", encoding="utf-8")
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"changed_files": changed_files}), encoding="utf-8")
    monkeypatch.setattr(noxfile, "ROOT", tmp_path)
    monkeypatch.setenv("AISTOCK_CI_CLASSIFIER_SUMMARY", str(summary))


def _configure_direct_neighbor_targets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    changed_files: list[str],
    existing_paths: list[str],
) -> None:
    for relative in existing_paths:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# fixture\n", encoding="utf-8")
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"changed_files": changed_files}), encoding="utf-8")
    monkeypatch.setattr(noxfile, "ROOT", tmp_path)
    monkeypatch.setenv("AISTOCK_CI_CLASSIFIER_SUMMARY", str(summary))


def test_direct_neighbor_pr_targets_select_smoke_changed_test_source_neighbor_and_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    smoke = "backend/tests/example/test_contract.py"
    changed_test = "backend/tests/example/test_reader.py"
    source = "backend/services/example/writer.py"
    source_neighbor = "backend/tests/example/test_writer.py"
    router = "backend/routers/example.py"
    router_neighbor = "backend/tests/example/test_api.py"
    _configure_direct_neighbor_targets(
        monkeypatch,
        tmp_path,
        changed_files=[changed_test, source, router],
        existing_paths=[smoke, changed_test, source, source_neighbor, router, router_neighbor],
    )

    assert noxfile._direct_neighbor_pr_targets(
        smoke_tests=(smoke,),
        source_test_roots=(("backend/services/example/", "backend/tests/example/"),),
        test_globs=("backend/tests/example/test_*.py",),
        overrides={router: router_neighbor},
    ) == [smoke, changed_test, source_neighbor, router_neighbor]


def test_direct_neighbor_pr_targets_falls_back_for_unmapped_live_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = "backend/services/example/unmapped.py"
    _configure_direct_neighbor_targets(
        monkeypatch,
        tmp_path,
        changed_files=[source],
        existing_paths=[source],
    )

    assert noxfile._direct_neighbor_pr_targets(
        smoke_tests=(),
        source_test_roots=(("backend/services/example/", "backend/tests/example/"),),
        test_globs=("backend/tests/example/test_*.py",),
    ) is None


def test_direct_neighbor_pr_targets_preserves_full_plan_without_ci_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AISTOCK_CI_CLASSIFIER_SUMMARY", raising=False)

    assert noxfile._direct_neighbor_pr_targets(
        smoke_tests=(),
        source_test_roots=(),
        test_globs=(),
    ) is None


@pytest.mark.parametrize(
    "session_name",
    [
        "qlib_data_backend",
        "advisory_phase0b_backend",
        "qe_read_backend",
        "position_timing_backend",
    ],
)
def test_direct_neighbor_sessions_execute_selected_pr_slice(
    monkeypatch: pytest.MonkeyPatch,
    session_name: str,
) -> None:
    selected = ["backend/tests/example/test_selected.py"]
    pytest_args: list[str] = []

    class DummySession:
        def run(self, *_args: object, **_kwargs: object) -> None:
            return None

    monkeypatch.setattr(noxfile, "_direct_neighbor_pr_targets", lambda **_kwargs: selected)
    monkeypatch.setattr(
        noxfile,
        "_run_pytest",
        lambda _session, *args: pytest_args.extend(args),
    )

    getattr(noxfile, session_name)(DummySession())

    assert selected[0] in pytest_args


def test_factor_research_session_executes_selected_pr_slice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = "backend/tests/factor_research/test_selected.py"
    calls: list[tuple[object, ...]] = []

    class DummySession:
        def run(self, *args: object, **_kwargs: object) -> None:
            calls.append(args)

    monkeypatch.setattr(noxfile, "_direct_neighbor_pr_targets", lambda **_kwargs: [selected])

    noxfile.factor_research_backend(DummySession())

    assert selected in calls[0]


def test_hmm_risk_pr_targets_use_changed_tests_and_direct_neighbors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    targets = [
        *noxfile.HMM_RISK_PR_SMOKE_TESTS,
        "backend/tests/hmm_risk/test_rotation_l1_prediction.py",
        "backend/tests/hmm_risk/test_rotation_l1_gbdt.py",
    ]
    _configure_hmm_pr_targets(
        monkeypatch,
        tmp_path,
        changed_files=[
            "backend/services/hmm_risk/rotation_l1_prediction.py",
            "backend/tests/hmm_risk/test_rotation_l1_prediction.py",
            "scripts/hmm_risk/run_rotation_l1_g2a.py",
        ],
        existing_paths=targets[3:],
    )

    assert noxfile._hmm_risk_pr_test_targets() == targets


def test_hmm_risk_pr_targets_default_to_smoke_without_classifier_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AISTOCK_CI_CLASSIFIER_SUMMARY", raising=False)

    assert noxfile._hmm_risk_pr_test_targets() == list(noxfile.HMM_RISK_PR_SMOKE_TESTS)


def test_hmm_risk_pr_targets_map_router_and_schema_to_direct_contracts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    targets = [
        *noxfile.HMM_RISK_PR_SMOKE_TESTS,
        "backend/tests/hmm_risk/test_rotation_l1_api.py",
    ]
    _configure_hmm_pr_targets(
        monkeypatch,
        tmp_path,
        changed_files=["backend/routers/hmm_risk.py"],
        existing_paths=targets[3:],
    )

    assert noxfile._hmm_risk_pr_test_targets() == targets


def test_hmm_risk_pr_targets_fail_closed_without_neighbor_mapping(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    changed_source = "backend/services/hmm_risk/new_contract.py"
    _configure_hmm_pr_targets(
        monkeypatch,
        tmp_path,
        changed_files=[changed_source],
        existing_paths=[changed_source],
    )

    with pytest.raises(ValueError, match="lacks a direct-neighbor test mapping"):
        noxfile._hmm_risk_pr_test_targets()


@pytest.mark.parametrize(
    ("deleted_source", "deleted_test"),
    [
        (
            "backend/services/hmm_risk/retired_contract.py",
            "backend/tests/hmm_risk/test_retired_contract.py",
        ),
        (
            "scripts/hmm_risk/retired_contract.py",
            "backend/tests/hmm_risk/test_retired_contract.py",
        ),
        (
            "scripts/hmm_risk/prepare_state_model_set.py",
            "backend/tests/hmm_risk/test_prepare_state_model_set_b3.py",
        ),
    ],
)
def test_hmm_risk_pr_targets_allow_atomic_source_and_neighbor_retirement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    deleted_source: str,
    deleted_test: str,
) -> None:
    _configure_hmm_pr_targets(
        monkeypatch,
        tmp_path,
        changed_files=[deleted_source, deleted_test],
    )

    assert noxfile._hmm_risk_pr_test_targets() == list(noxfile.HMM_RISK_PR_SMOKE_TESTS)


def test_hmm_risk_pr_targets_run_remaining_neighbor_for_deleted_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    neighbor = "backend/tests/hmm_risk/test_prepare_state_model_set_b3.py"
    targets = [*noxfile.HMM_RISK_PR_SMOKE_TESTS, neighbor]
    _configure_hmm_pr_targets(
        monkeypatch,
        tmp_path,
        changed_files=["scripts/hmm_risk/prepare_state_model_set.py"],
        existing_paths=[neighbor],
    )

    assert noxfile._hmm_risk_pr_test_targets() == targets


def test_hmm_risk_pr_targets_fail_closed_when_existing_override_loses_its_test(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    changed_source = "scripts/hmm_risk/prepare_state_model_set.py"
    _configure_hmm_pr_targets(
        monkeypatch,
        tmp_path,
        changed_files=[changed_source],
        existing_paths=[changed_source],
    )

    with pytest.raises(ValueError, match="mapped direct-neighbor test is missing"):
        noxfile._hmm_risk_pr_test_targets()


@pytest.mark.parametrize(
    ("live_source", "deleted_test"),
    [
        (
            "backend/services/hmm_risk/new_contract.py",
            "backend/tests/hmm_risk/test_new_contract.py",
        ),
        (
            "scripts/hmm_risk/prepare_state_model_set.py",
            "backend/tests/hmm_risk/test_prepare_state_model_set_b3.py",
        ),
    ],
)
def test_hmm_risk_pr_targets_reject_test_only_deletion_for_live_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    live_source: str,
    deleted_test: str,
) -> None:
    _configure_hmm_pr_targets(
        monkeypatch,
        tmp_path,
        changed_files=[deleted_test],
        existing_paths=[live_source],
    )

    with pytest.raises(ValueError, match="deleted HMM direct-neighbor test still covers live source"):
        noxfile._hmm_risk_pr_test_targets()


def test_changed_file_guardrail_uses_committed_branch_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[tuple[object, ...]] = []

    class DummySession:
        posargs = ["--changed-only"]

        def run(self, *args: object, **_kwargs: object) -> None:
            commands.append(args)

        def error(self, message: str) -> None:
            raise AssertionError(message)

    monkeypatch.setattr(
        noxfile,
        "_l0_scan_paths",
        lambda _posargs: ["scripts/issue_flow.py", "backend/tests/scripts/test_issue_flow.py"],
    )
    noxfile.guardrail_changed_files(DummySession())  # type: ignore[arg-type]

    assert len(commands) == 2
    for command in commands:
        assert "--changed-only" not in command
        assert "scripts/issue_flow.py" in command
        assert "backend/tests/scripts/test_issue_flow.py" in command


def test_env_prefers_self_hosted_source_dotenv_without_copying(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_nox_env_loader(monkeypatch)
    source = tmp_path / "source"
    source.mkdir()
    (source / ".env").write_text(
        "TDX_DB_HOST=127.0.0.1\nTDX_DB_PASSWORD=secret-for-test\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AISTOCK_SELF_HOSTED_SOURCE", str(source))
    monkeypatch.delenv("AISTOCK_ENV_FILE", raising=False)
    monkeypatch.delenv("TDX_DB_PASSWORD", raising=False)

    env = noxfile._env()

    assert env["AISTOCK_ENV_FILE"] == str(source / ".env")
    assert env["TDX_DB_PASSWORD"] == "secret-for-test"
    assert os.environ["TDX_DB_PASSWORD"] == "secret-for-test"


def test_env_uses_canonical_root_dotenv_for_worktree(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_nox_env_loader(monkeypatch)
    canonical = tmp_path / "canonical"
    worktree = tmp_path / "worktree"
    canonical.mkdir()
    worktree.mkdir()
    (canonical / ".env").write_text(
        "TDX_DB_HOST=127.0.0.1\nTDX_DB_PASSWORD=canonical-secret-for-test\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("AISTOCK_ENV_FILE", raising=False)
    monkeypatch.delenv("AISTOCK_SELF_HOSTED_SOURCE", raising=False)
    monkeypatch.delenv("TDX_DB_PASSWORD", raising=False)
    monkeypatch.setattr(noxfile, "CANONICAL_ROOT", canonical)
    monkeypatch.setattr(noxfile, "ROOT", worktree)

    env = noxfile._env()

    assert env["AISTOCK_ENV_FILE"] == str(canonical / ".env")
    assert env["TDX_DB_PASSWORD"] == "canonical-secret-for-test"


def test_managed_backend_refuses_production_port() -> None:
    class DummySession:
        def error(self, message: str) -> None:
            raise RuntimeError(message)

    with pytest.raises(RuntimeError, match="production port 8001"):
        with noxfile._managed_validation_backend(DummySession(), "8001"):
            pass


def test_frontend_node_modules_install_runs_only_when_direct_entrypoints_are_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    frontend = tmp_path / "frontend"
    frontend.mkdir()
    caller_cwd = tmp_path / "caller"
    caller_cwd.mkdir()
    monkeypatch.chdir(caller_cwd)
    monkeypatch.setattr(noxfile, "ROOT", tmp_path)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("AISTOCK_CI_INSTALL_FORBIDDEN", raising=False)
    calls: list[tuple[tuple[str, ...], Path, dict[str, object]]] = []

    class DummySession:
        def log(self, message: str) -> None:
            calls.append((("log", message), Path.cwd(), {}))

        def run(self, *args: str, **kwargs: object) -> None:
            calls.append((tuple(args), Path.cwd(), dict(kwargs)))

    noxfile._ensure_frontend_node_modules(DummySession())
    assert (("npm", "ci"), frontend, {"external": True}) in calls
    assert Path.cwd() == caller_cwd

    calls.clear()
    for relative in noxfile.FRONTEND_DIRECT_ENTRYPOINTS:
        direct_cli = frontend / "node_modules" / relative
        direct_cli.parent.mkdir(parents=True, exist_ok=True)
        direct_cli.write_text("// pinned CLI\n", encoding="utf-8")

    noxfile._ensure_frontend_node_modules(DummySession())
    assert calls == []


def test_frontend_sessions_invoke_direct_entrypoints_without_npm_bin_shims(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    frontend = tmp_path / "frontend"
    frontend.mkdir()
    for relative in noxfile.FRONTEND_DIRECT_ENTRYPOINTS:
        entrypoint = frontend / "node_modules" / relative
        entrypoint.parent.mkdir(parents=True, exist_ok=True)
        entrypoint.write_text("// pinned CLI\n", encoding="utf-8")
    monkeypatch.setattr(noxfile, "ROOT", tmp_path)
    calls: list[tuple[str, ...]] = []

    class DummySession:
        def run(self, *args: str, **_kwargs: object) -> None:
            calls.append(tuple(args))

    noxfile.frontend_type_lint(DummySession())  # type: ignore[arg-type]
    noxfile._run_mocked_frontend_target(DummySession(), "tests/watchlist")  # type: ignore[arg-type]

    assert calls[0] == (
        "node",
        "node_modules/typescript/bin/tsc",
        "--noEmit",
        "--incremental",
        "false",
    )
    assert calls[1] == ("node", "node_modules/next/dist/bin/next", "lint")
    assert calls[2][:4] == (
        "node",
        "node_modules/@playwright/test/cli.js",
        "test",
        "tests/watchlist",
    )
    assert all(command[0] != "npm" for command in calls)


def test_terminate_process_tree_uses_taskkill_on_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, ...]] = []

    class DummyProc:
        pid = 12345

        def poll(self) -> None:
            return None

        def wait(self, timeout: int) -> int:
            return 0

    monkeypatch.setattr(noxfile.os, "name", "nt")
    monkeypatch.setattr(
        noxfile.subprocess,
        "run",
        lambda args, **_kwargs: calls.append(tuple(args)),
    )

    noxfile._terminate_process_tree(DummyProc())  # type: ignore[arg-type]

    assert calls == [("taskkill", "/PID", "12345", "/T", "/F")]


def test_kill_windows_listeners_on_port_only_targets_listening(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, ...]] = []

    class Result:
        stdout = (
            "  TCP    0.0.0.0:3012   0.0.0.0:0   LISTENING   111\n"
            "  TCP    127.0.0.1:3012 127.0.0.1:50000 TIME_WAIT 0\n"
        )
        returncode = 0

    def fake_run(args: list[str], **_kwargs: object) -> Result:
        calls.append(tuple(args))
        return Result()

    monkeypatch.setattr(noxfile.os, "name", "nt")
    monkeypatch.setattr(noxfile.subprocess, "run", fake_run)

    noxfile._kill_windows_listeners_on_port("3012")

    assert calls[0] == ("netstat", "-ano")
    assert ("taskkill", "/PID", "111", "/T", "/F") in calls


def test_kill_windows_listeners_falls_back_when_taskkill_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, ...]] = []

    class NetstatResult:
        stdout = "  TCP    127.0.0.1:3012   0.0.0.0:0   LISTENING   222\n"
        returncode = 0

    class FailedKillResult:
        stdout = ""
        returncode = 1

    def fake_run(args: list[str], **_kwargs: object) -> NetstatResult | FailedKillResult:
        calls.append(tuple(args))
        return NetstatResult() if args[0] == "netstat" else FailedKillResult()

    monkeypatch.setattr(noxfile.os, "name", "nt")
    monkeypatch.setattr(noxfile.subprocess, "run", fake_run)

    noxfile._kill_windows_listeners_on_port("3012")

    assert ("taskkill", "/PID", "222", "/T", "/F") in calls
    assert any(call[:4] == ("powershell", "-NoProfile", "-Command", "Stop-Process -Id 222 -Force -ErrorAction SilentlyContinue") for call in calls)


def test_reclaim_validation_ports_enabled_in_github_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AISTOCK_RECLAIM_VALIDATION_PORTS", raising=False)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert noxfile._reclaim_validation_ports() is True


def test_paper_v2_live_skips_realtime_gate_by_default_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PAPER_V2_SKIP_REALTIME", "1")
    monkeypatch.delenv("PAPER_V2_FORCE_REALTIME", raising=False)
    calls: list[tuple[str, ...]] = []
    logs: list[str] = []

    class DummySession:
        posargs: list[str] = []

        def skip(self, message: str) -> None:
            logs.append(message)
            raise RuntimeError("skipped")

        def run(self, *args: str, **_kwargs: object) -> None:
            calls.append(tuple(args))

    with pytest.raises(RuntimeError, match="skipped"):
        noxfile.paper_v2_live(DummySession())  # type: ignore[arg-type]

    assert calls == []
    assert any("PAPER_V2_SKIP_REALTIME=1" in message for message in logs)


def test_paper_v2_live_force_realtime_still_runs_service_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PAPER_V2_SKIP_REALTIME", "1")
    monkeypatch.setenv("PAPER_V2_FORCE_REALTIME", "1")
    calls: list[tuple[str, ...]] = []

    @contextmanager
    def fake_backend(_session: object, _port: str):
        yield

    monkeypatch.setattr(noxfile, "_managed_validation_backend", fake_backend)

    class DummySession:
        posargs: list[str] = []

        def run(self, *args: str, **_kwargs: object) -> None:
            calls.append(tuple(args))

    noxfile.paper_v2_live(DummySession())  # type: ignore[arg-type]

    assert calls[0][:3] == ("python", "scripts/aistock_validate.py", "services")
    assert calls[1][:2] == ("python", "scripts/paper_v2_live_validation.py")
