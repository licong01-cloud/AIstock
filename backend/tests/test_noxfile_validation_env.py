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


def test_budget_contract_is_not_nested_in_catalog_but_remains_in_nightly():
    from scripts.ci_change_classifier import _selected_nox_test_targets
    targets, error = _selected_nox_test_targets(repo_root=ROOT,
        sessions=['validation_catalog_integrity', 'validation_workflow_automation'])
    assert error is None
    budget = 'backend/tests/scripts/test_aistock_validation_budget.py'
    assert budget not in targets['validation_catalog_integrity']
    assert budget in targets['validation_workflow_automation']


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
        fallback_tests=("backend/tests/example",),
        smoke_tests=(smoke,),
        source_test_roots=(("backend/services/example/", "backend/tests/example/"),),
        test_globs=("backend/tests/example/test_*.py",),
        overrides={router: router_neighbor},
    ) == [smoke, changed_test, source_neighbor, router_neighbor]


@pytest.mark.parametrize("source", ["unmapped.py", "__init__.py", "deleted.py", "override.py"])
def test_direct_neighbor_pr_targets_fallback_preserves_changed_tests(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, source: str
) -> None:
    source_path = f"backend/services/example/{source}"
    changed_test = "backend/tests/example/test_changed.py"
    deleted_test = "backend/tests/example/test_deleted.py"
    full = ("backend/tests/example/test_full.py",)
    _configure_direct_neighbor_targets(
        monkeypatch,
        tmp_path,
        changed_files=[source_path, changed_test, deleted_test, changed_test],
        existing_paths=[changed_test, *full, *([source_path] if source != "deleted.py" else [])],
    )

    assert noxfile._direct_neighbor_pr_targets(
        fallback_tests=full,
        smoke_tests=(),
        source_test_roots=(("backend/services/example/", "backend/tests/example/"),),
        test_globs=("backend/tests/example/test_*.py",),
        overrides={source_path: "backend/tests/example/test_missing_override.py"} if source == "override.py" else {},
    ) == [*full, changed_test]


def test_direct_neighbor_pr_targets_skip_deleted_tests_without_hiding_live_changes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    smoke = "backend/tests/example/test_contract.py"
    deleted_test = "backend/tests/example/test_retired.py"
    changed_test = "backend/tests/example/test_reader.py"
    _configure_direct_neighbor_targets(
        monkeypatch,
        tmp_path,
        changed_files=[deleted_test, changed_test],
        existing_paths=[smoke, changed_test],
    )

    assert noxfile._direct_neighbor_pr_targets(
        fallback_tests=("backend/tests/example",),
        smoke_tests=(smoke,),
        source_test_roots=(("backend/services/example/", "backend/tests/example/"),),
        test_globs=("backend/tests/example/test_*.py",),
    ) == [smoke, changed_test]


def test_direct_neighbor_pr_targets_preserves_full_plan_without_ci_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AISTOCK_CI_CLASSIFIER_SUMMARY", raising=False)

    assert noxfile._direct_neighbor_pr_targets(
        fallback_tests=("backend/tests/example",),
        smoke_tests=(),
        source_test_roots=(),
        test_globs=(),
    ) is None


@pytest.mark.parametrize("missing", [False, True])
def test_direct_neighbor_multi_consumer_override_requires_all_targets(monkeypatch, tmp_path, missing):
    source = "backend/services/example/contract.py"
    consumers = ("backend/tests/example/test_reader.py", "backend/tests/example/test_writer.py")
    changed = "backend/tests/example/test_changed.py"
    _configure_direct_neighbor_targets(
        monkeypatch, tmp_path, changed_files=[source, changed],
        existing_paths=[source, changed, *(consumers[:1] if missing else consumers)],
    )
    targets = noxfile._direct_neighbor_pr_targets(
        fallback_tests=("backend/tests/example",), smoke_tests=(),
        source_test_roots=(("backend/services/example/", "backend/tests/example/"),),
        test_globs=("backend/tests/example/test_*.py",), overrides={source: consumers},
    )
    assert targets == (["backend/tests/example", changed] if missing else [*consumers, changed])


@pytest.mark.parametrize("case", ["contracts", "inference", "mixed_unknown", "shared", "nightly"])
def test_advisory_5td_explicit_consumer_mapping_preserves_safety(monkeypatch, tmp_path, case):
    src = "backend/services/advisory_model_first/"
    tests = "backend/tests/advisory_model_first/"
    consumers = [tests + f"test_generic_price_5td_{name}_v1.py" for name in ("labels", "models", "pipeline")]
    contract, inference, unknown, shared = [src + name for name in (
        "generic_price_5td_contracts_v1.py", "generic_price_5td_inference_v1.py",
        "unmapped.py", "contracts.py",
    )]
    changed = {"contracts": [contract], "inference": [inference],
               "mixed_unknown": [contract, unknown, consumers[0]], "shared": [contract, shared]}
    if case == "nightly":
        monkeypatch.delenv("AISTOCK_CI_CLASSIFIER_SUMMARY", raising=False)
    else:
        _configure_direct_neighbor_targets(monkeypatch, tmp_path, changed_files=changed[case],
                                           existing_paths=[contract, inference, unknown, shared, *consumers])
    calls = []
    monkeypatch.setattr(noxfile, "_run_pytest", lambda _session, *args: calls.append(args))
    noxfile.advisory_modeling_backend(object())
    targets = calls[0][:-3]
    full = ("backend/tests/advisory_modeling", "backend/tests/advisory_model_first")
    if case in {"mixed_unknown", "shared", "nightly"}:
        assert targets == ((*full, consumers[0]) if case == "mixed_unknown" else full)
    else:
        assert not set(full).intersection(targets)
        assert set(consumers if case == "contracts" else [consumers[1]]).issubset(targets)
        assert len(targets) <= 6


@pytest.mark.parametrize(
    "session_name",
    [
        "qlib_data_backend",
        "advisory_phase0b_backend",
        "advisory_modeling_backend",
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


@pytest.mark.parametrize("source_first", [False, True])
def test_qe_read_fallback_preserves_full_plan_and_changed_test(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, source_first: bool
) -> None:
    changed_test = "backend/tests/unified_engine/test_label_horizon.py"
    sources = [
        "backend/services/quantevolver/config_composer.py",
        "backend/services/quantevolver/qe_custom_loaders.py",
    ]
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(noxfile, "ROOT", tmp_path)
    monkeypatch.delenv("AISTOCK_CI_CLASSIFIER_SUMMARY", raising=False)
    monkeypatch.setattr(noxfile, "_run_pytest", lambda _session, *args: calls.append(args))
    noxfile.qe_read_backend(object())
    full_targets = calls.pop()[:-3]
    _configure_direct_neighbor_targets(
        monkeypatch,
        tmp_path,
        changed_files=[*sources, changed_test] if source_first else [changed_test, *sources],
        existing_paths=[changed_test, *sources],
    )

    noxfile.qe_read_backend(object())

    assert calls == [(*full_targets, changed_test, "-q", "-p", "no:cacheprovider")]


@pytest.mark.parametrize("case", ["source", "test", "override", "unknown", "offline", "shared"])
def test_advisory_modeling_neighbor_or_complete_fallback(monkeypatch, tmp_path, case):
    prefix = "backend/tests/advisory_model_first/"
    source = "backend/services/advisory_model_first/economic_moneyflow_price_v1.py"
    neighbor = prefix + "test_economic_moneyflow_price_v1.py"
    changed = prefix + "test_changed_contract.py"
    override = "backend/services/advisory_modeling/bundle_store.py"
    unknown = "backend/services/advisory_model_first/unmapped_contract.py"
    shared = "backend/services/advisory_model_first/research_control_contracts.py"
    calls = []
    monkeypatch.setattr(noxfile, "ROOT", tmp_path)
    monkeypatch.setattr(noxfile, "_run_pytest", lambda _session, *args: calls.append(args))
    monkeypatch.delenv("AISTOCK_CI_CLASSIFIER_SUMMARY", raising=False)
    noxfile.advisory_modeling_backend(object())
    full = calls.pop()[:-3]
    assert full == ("backend/tests/advisory_modeling", "backend/tests/advisory_model_first")
    if case != "offline":
        paths = {"source": [source], "test": [changed], "override": [override], "unknown": [unknown, changed], "shared": [shared, changed]}[case]
        _configure_direct_neighbor_targets(monkeypatch, tmp_path, changed_files=paths, existing_paths=[source, neighbor, changed, override, unknown, shared, prefix + "test_research_control_contracts.py", "backend/tests/advisory_modeling/test_artifacts_shadow_isolation.py"])
    noxfile.advisory_modeling_backend(object())
    targets = calls.pop()[:-3]
    if case in {"unknown", "offline", "shared"}:
        assert targets == ((*full, changed) if case == "unknown" else full)
    else:
        assert not set(full).intersection(targets) and len(targets) <= 4
        assert {"source": neighbor, "test": changed, "override": "backend/tests/advisory_modeling/test_artifacts_shadow_isolation.py"}[case] in targets
        assert prefix + "test_evidence_level_boundaries.py" in targets


def test_direct_neighbor_fallback_passes_actual_collection_gate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from scripts import ci_plan_coverage as coverage

    full_test = "backend/tests/example/test_full.py"
    changed_test = "backend/tests/example/test_changed.py"
    source = "backend/services/example/unmapped.py"
    _configure_direct_neighbor_targets(
        monkeypatch, tmp_path, changed_files=[source, changed_test], existing_paths=[source, full_test, changed_test]
    )
    for path in (full_test, changed_test):
        (tmp_path / path).write_text("def test_contract():\n    assert True\n", encoding="utf-8")
    targets = noxfile._direct_neighbor_pr_targets(
        fallback_tests=(full_test,), smoke_tests=(),
        source_test_roots=(("backend/services/example/", "backend/tests/example/"),),
        test_globs=("backend/tests/example/test_*.py",),
    )
    assert targets == [full_test, changed_test]
    receipt = tmp_path / "collected.txt"
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    env[coverage.RECEIPT_ENV] = str(receipt)
    env[coverage.REPO_ROOT_ENV] = str(tmp_path)
    env.pop("PYTEST_ADDOPTS", None)
    monkeypatch.delenv(coverage.CLASSIFIER_SUMMARY_ENV, raising=False)
    env.pop(coverage.CLASSIFIER_SUMMARY_ENV, None)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", *targets, "-q", "-p", "no:cacheprovider", "-p", "scripts.ci_plan_coverage"],
        cwd=tmp_path, env=env, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert coverage.verify_changed_test_coverage(
        [changed_test], collected_tests=receipt.read_text(encoding="utf-8").splitlines(), repo_root=tmp_path
    )["workflow_gate"] == "passed"


def test_factor_research_session_executes_compact_full_suite_with_dev_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = "backend/tests/factor_research/test_selected.py"
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class DummySession:
        def run(self, *args: object, **kwargs: object) -> None:
            calls.append((args, kwargs))

    monkeypatch.setattr(noxfile, "_direct_neighbor_pr_targets", lambda **_kwargs: [selected])

    noxfile.factor_research_backend(DummySession())

    pytest_args, pytest_kwargs = calls[0]
    assert selected not in pytest_args
    assert "backend/tests/factor_research/test_contracts.py" in pytest_args
    assert "backend/tests/factor_research/test_repository_dev.py" in pytest_args
    pytest_env = pytest_kwargs["env"]
    assert isinstance(pytest_env, dict)
    assert pytest_env["AISTOCK_DEV_DB_E2E"] == "0"
    assert pytest_env["FACTOR_RESEARCH_DEV_ENV_FILE"] == ""


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


def test_mcp_gateway_phase5_invokes_prebuilt_frontend_entrypoints_directly(
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

        def chdir(self, path: str) -> None:
            calls.append(("chdir", path))

    noxfile.mcp_gateway_phase5_assistant(DummySession())  # type: ignore[arg-type]

    assert ("node", "node_modules/next/dist/bin/next", "lint") in calls
    assert ("node", "node_modules/next/dist/bin/next", "build") in calls
    assert (
        "node",
        "node_modules/@playwright/test/cli.js",
        "test",
        "tests/research-assistant/phase5-mcp-gateway-ui.spec.ts",
        "--project",
        "chromium",
    ) in calls
    assert not any(command[:2] in {("npm", "run"), ("npx", "playwright")} for command in calls)


def test_ra_phase7_invokes_prebuilt_frontend_entrypoints_directly(
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

        def chdir(self, path: str) -> None:
            calls.append(("chdir", path))

    noxfile.ra_phase7_full_accept(DummySession())  # type: ignore[arg-type]

    assert ("node", "node_modules/next/dist/bin/next", "lint") in calls
    assert ("node", "node_modules/next/dist/bin/next", "build") in calls
    assert sum(
        command[:3]
        == ("node", "node_modules/@playwright/test/cli.js", "test")
        for command in calls
    ) == 2
    assert not any(command[:2] in {("npm", "run"), ("npx", "playwright")} for command in calls)


def test_nightly_ra_sessions_reuse_only_successful_same_run_prerequisites(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    frontend = tmp_path / "frontend"
    frontend.mkdir()
    for relative in noxfile.FRONTEND_DIRECT_ENTRYPOINTS:
        entrypoint = frontend / "node_modules" / relative
        entrypoint.parent.mkdir(parents=True, exist_ok=True)
        entrypoint.write_text("// pinned CLI\n", encoding="utf-8")
    monkeypatch.setattr(noxfile, "ROOT", tmp_path)
    monkeypatch.setenv(noxfile.NIGHTLY_SESSION_RUNNER_ENV, "1")
    monkeypatch.setenv(
        noxfile.NIGHTLY_COMPLETED_SESSIONS_ENV,
        "mcp_gateway_phase5_assistant",
    )
    calls: list[tuple[str, ...]] = []
    logs: list[str] = []

    class DummySession:
        def run(self, *args: str, **_kwargs: object) -> None:
            calls.append(tuple(args))

        def chdir(self, path: str) -> None:
            calls.append(("chdir", path))

        def log(self, message: str) -> None:
            logs.append(message)

    noxfile.ra_phase7_full_accept(DummySession())  # type: ignore[arg-type]

    assert any("backend/tests/research_assistant" in command for command in calls)
    assert ("node", "node_modules/next/dist/bin/next", "lint") not in calls
    assert ("node", "node_modules/next/dist/bin/next", "build") not in calls
    assert any("mcp_gateway_phase5_assistant" in message for message in logs)

    calls.clear()
    noxfile.mcp_gateway_phase5_assistant(DummySession())  # type: ignore[arg-type]

    assert any("tests/mcp" in command for command in calls)
    assert ("node", "node_modules/next/dist/bin/next", "lint") in calls
    assert ("node", "node_modules/next/dist/bin/next", "build") in calls

    calls.clear()
    monkeypatch.setenv(
        noxfile.NIGHTLY_COMPLETED_SESSIONS_ENV,
        "ra_phase7_full_accept",
    )
    noxfile.research_assistant_backend(DummySession())  # type: ignore[arg-type]

    assert any("backend/services/research_assistant" in command for command in calls)
    assert not any("backend/tests/research_assistant" in command for command in calls)
    assert any("ra_phase7_full_accept" in message for message in logs)


def test_research_assistant_backend_runs_full_without_nightly_predecessor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(noxfile.NIGHTLY_SESSION_RUNNER_ENV, raising=False)
    monkeypatch.delenv(noxfile.NIGHTLY_COMPLETED_SESSIONS_ENV, raising=False)
    calls: list[tuple[str, ...]] = []

    class DummySession:
        def run(self, *args: str, **_kwargs: object) -> None:
            calls.append(tuple(args))

    noxfile.research_assistant_backend(DummySession())  # type: ignore[arg-type]

    assert any("backend/tests/research_assistant" in command for command in calls)


def test_nightly_session_reuse_is_disabled_without_runner_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        noxfile.NIGHTLY_COMPLETED_SESSIONS_ENV,
        "research_assistant_backend,ra_phase7_full_accept",
    )
    monkeypatch.delenv(noxfile.NIGHTLY_SESSION_RUNNER_ENV, raising=False)

    assert noxfile._nightly_session_completed("research_assistant_backend") is False

    monkeypatch.setenv(noxfile.NIGHTLY_SESSION_RUNNER_ENV, "1")
    monkeypatch.setenv(noxfile.NIGHTLY_COMPLETED_SESSIONS_ENV, "")

    assert noxfile._nightly_session_completed("research_assistant_backend") is False


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
