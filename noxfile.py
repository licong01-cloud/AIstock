from __future__ import annotations

import os
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

import nox


ROOT = Path(__file__).parent
COMMON_ENV = {
    "PYTHONIOENCODING": "utf-8",
    "PYTHONDONTWRITEBYTECODE": "1",
}

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ["l0"]


def _env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env.update(COMMON_ENV)
    if extra:
        env.update(extra)
    return env


def _run_pytest(session: nox.Session, *args: str) -> None:
    session.run(
        "python",
        "-m",
        "pytest",
        *args,
        env=_env(),
        external=True,
    )


def _is_port_open(port: str) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex(("127.0.0.1", int(port))) == 0


def _codex_quick_validate_script() -> Path:
    codex_home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")
    return codex_home / "skills" / ".system" / "skill-creator" / "scripts" / "quick_validate.py"


def _guardrail_baseline_json(session: nox.Session) -> str:
    baseline_json = os.environ.get(
        "AISTOCK_GUARDRAIL_BASELINE_JSON",
        "tmp/validation/guardrails/baseline_20260504.json",
    )
    if not (ROOT / baseline_json).exists():
        session.error(
            "Missing guardrail baseline JSON. Run "
            "`python scripts/aistock_guardrail_scan.py --baseline "
            "--output-json tmp/validation/guardrails/baseline_20260504.json` first."
        )
    return baseline_json


@nox.session(venv_backend="none")
def l0(session: nox.Session) -> None:
    """Run local static gates that do not start AIstock services."""
    scan_paths = session.posargs or [
        "noxfile.py",
        "scripts/aistock_validate.py",
        "scripts/aistock_guardrail_scan.py",
        "scripts/aistock_module_ownership_scan.py",
        "scripts/validation_center_readonly_smoke.py",
        "scripts/aistock_data_quality_smoke.py",
        "scripts/paper_v2_live_validation.py",
        "backend/services/audit_backed_data_health.py",
        "backend/services/data_refresh_audit.py",
        "backend/services/quantevolver/completion_contract.py",
        "backend/tests/test_dataset_refresh_audit.py",
        "backend/tests/test_aistock_validate_metadata.py",
        "backend/tests/test_aistock_validate_coverage.py",
        "backend/tests/test_aistock_guardrail_scan.py",
        "backend/tests/test_validation_git_status_provider.py",
        "backend/tests/test_validation_module_ownership.py",
        "backend/tests/test_validation_center_api.py",
        "backend/tests/unified_engine/test_qe_completion_contract.py",
        "backend/tests/paper_trading_v2",
        "backend/tests/selection_center",
        "backend/tests/strategy_package",
        "frontend/playwright.config.ts",
        "frontend/src/app/validation-center",
        "frontend/src/lib/validation",
        "frontend/tests/validation-center",
        "frontend/tests/paper-v2",
        "tests/aistock_validation/catalog/module_registry.yaml",
        "tests/aistock_validation/catalog/file_ownership.yaml",
        "tests/aistock_validation/modules/development_guardrails.md",
    ]
    quick_validate = _codex_quick_validate_script()
    if not quick_validate.exists():
        session.error(f"Missing Codex skill validator: {quick_validate}")
    session.run(
        "python",
        str(quick_validate),
        ".codex/skills/verify-aistock-feature",
        external=True,
    )
    session.run(
        "python",
        ".codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py",
        *scan_paths,
        "--fail-on",
        "HIGH",
        external=True,
    )
    session.run(
        "python",
        "scripts/aistock_guardrail_scan.py",
        *scan_paths,
        "--baseline-json",
        _guardrail_baseline_json(session),
        "--fail-new-only",
        "--fail-on-severity",
        "P1",
        "--output-json",
        "tmp/validation/guardrails/l0_paths.json",
        "--summary-md",
        "tmp/validation/guardrails/l0_paths.md",
        external=True,
    )


@nox.session(venv_backend="none")
def guardrail_changed_files(session: nox.Session) -> None:
    """Run P0/P1 development-standard guardrails on staged or changed files."""
    mode_flag = session.posargs[0] if session.posargs else "--staged-only"
    if mode_flag not in {"--staged-only", "--changed-only"}:
        session.error("First optional argument must be --staged-only or --changed-only.")
    session.run(
        "python",
        "scripts/aistock_guardrail_scan.py",
        mode_flag,
        "--baseline-json",
        _guardrail_baseline_json(session),
        "--fail-new-only",
        "--fail-on-severity",
        "P1",
        "--output-json",
        "tmp/validation/guardrails/changed_files.json",
        "--summary-md",
        "tmp/validation/guardrails/changed_files.md",
        external=True,
    )
    session.run(
        sys.executable,
        "scripts/aistock_module_ownership_scan.py",
        mode_flag,
        "--fail-on-unmapped",
        "--fail-on-ambiguous",
        "--output-json",
        "tmp/validation/module_ownership/changed_files.json",
        "--summary-md",
        "tmp/validation/module_ownership/changed_files.md",
        external=True,
    )


@nox.session(venv_backend="none")
def paper_v2_backend(session: nox.Session) -> None:
    """Run Paper v2 + Selection Center backend regression tests."""
    _run_pytest(
        session,
        "backend/tests/paper_trading_v2",
        "backend/tests/selection_center",
        "backend/tests/strategy_package",
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def paper_v2_data_quality(session: nox.Session) -> None:
    """Run read-only Paper v2 + Selection Center data-quality smoke checks."""
    args = [
        "scripts/aistock_data_quality_smoke.py",
        "--scope",
        "paper_v2_selection_center",
        "--output",
        "tmp/paper_v2_data_quality_smoke.json",
    ]
    if session.posargs:
        args.extend(session.posargs)
    session.run("python", *args, env=_env(), external=True)


@nox.session(venv_backend="none")
def local_data_management_audit(session: nox.Session) -> None:
    """Run local data-management audit schema and repository checks."""
    _run_pytest(
        session,
        "backend/tests/test_dataset_refresh_audit.py",
        "-q",
        "-p",
        "no:cacheprovider",
    )
    session.run(
        "python",
        "scripts/aistock_data_quality_smoke.py",
        "--scope",
        "local_data_management",
        "--audit-schema-only",
        "--output",
        "tmp/local_data_management_audit_smoke.json",
        env=_env(),
        external=True,
    )


@nox.session(venv_backend="none")
def paper_v2_ui(session: nox.Session) -> None:
    """Run Paper v2/Selection UI E2E tests on dev ports only."""
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8012")
    frontend_port = session.posargs[1] if len(session.posargs) > 1 else os.environ.get("FRONTEND_PORT", "3012")
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "ports",
        "--allow-occupied",
        backend_port,
        frontend_port,
        external=True,
    )
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "services",
        "--backend-port",
        backend_port,
        "--tdx-port",
        os.environ.get("TDX_HTTP_PORT", "19080"),
        *(["--skip-tdx"] if os.environ.get("PAPER_V2_SKIP_REALTIME") == "1" else []),
        external=True,
    )
    old_cwd = Path.cwd()
    os.chdir(ROOT / "frontend")
    try:
        session.run(
            "npm",
            "run",
            "test:e2e",
            "--",
            "tests/paper-v2",
            env=_env(
                {
                    "BACKEND_PORT": backend_port,
                    "FRONTEND_PORT": frontend_port,
                    "PAPER_V2_API_BASE": f"http://127.0.0.1:{backend_port}/api/v1",
                    "NEXT_PUBLIC_API_BASE": f"http://127.0.0.1:{backend_port}/api/v1",
                    "PLAYWRIGHT_SKIP_WEBSERVER": "1" if _is_port_open(frontend_port) else "0",
                    "PAPER_V2_E2E_SKIP_REALTIME": os.environ.get(
                        "PAPER_V2_E2E_SKIP_REALTIME",
                        os.environ.get("PAPER_V2_SKIP_REALTIME", "0"),
                    ),
                }
            ),
            external=True,
        )
    finally:
        os.chdir(old_cwd)


@nox.session(venv_backend="none")
def paper_v2_l3(session: nox.Session) -> None:
    """Run the first-stage Paper v2 + Selection Center L3 local suite."""
    session.run("python", "scripts/aistock_validate.py", "record", "--module", "paper_v2_selection_center", "--level", "L3", "--title", "Paper v2 Selection Center L3 regression", external=True)
    session.notify("l0")
    session.notify("paper_v2_backend")
    session.notify("paper_v2_data_quality")
    if os.environ.get("PAPER_V2_L3_SKIP_UI") != "1":
        session.notify("paper_v2_ui")


@nox.session(venv_backend="none")
def qe_read_backend(session: nox.Session) -> None:
    """Run QE read-path backend regression tests only."""
    _run_pytest(
        session,
        "backend/tests/unified_engine/test_qe_evolution_read_paths.py",
        "backend/tests/unified_engine/test_qe_experiment_read_paths.py",
        "backend/tests/unified_engine/test_qe_experiment_log_terminal.py",
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def qe_read_ui(session: nox.Session) -> None:
    """Run QE read-only UI E2E tests on dev ports only."""
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8011")
    frontend_port = session.posargs[1] if len(session.posargs) > 1 else os.environ.get("FRONTEND_PORT", "3011")
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "ports",
        "--allow-occupied",
        backend_port,
        frontend_port,
        external=True,
    )
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "services",
        "--backend-port",
        backend_port,
        "--skip-tdx",
        external=True,
    )
    old_cwd = Path.cwd()
    os.chdir(ROOT / "frontend")
    try:
        session.run(
            "npm",
            "exec",
            "tsc",
            "--",
            "--noEmit",
            "--incremental",
            "false",
            external=True,
        )
        session.run(
            "npm",
            "run",
            "test:e2e",
            "--",
            "tests/qe/qe-evolution-read-only.spec.ts",
            "tests/qe/qe-experiment-read-only.spec.ts",
            env=_env(
                {
                    "BACKEND_PORT": backend_port,
                    "FRONTEND_PORT": frontend_port,
                    "QE_API_BASE": f"http://127.0.0.1:{backend_port}/api/v1",
                    "NEXT_PUBLIC_API_BASE": f"http://127.0.0.1:{backend_port}/api/v1",
                    "PLAYWRIGHT_SKIP_WEBSERVER": "1" if _is_port_open(frontend_port) else "0",
                    "QE_READ_TASK_ID": os.environ.get("QE_READ_TASK_ID", "qe_20260414_173338_d1c5"),
                    "QE_READ_EXPERIMENT_ID": os.environ.get("QE_READ_EXPERIMENT_ID", "qe_20260501_011054_c90a_L1"),
                }
            ),
            external=True,
        )
    finally:
        os.chdir(old_cwd)


@nox.session(venv_backend="none")
def qe_read_l3(session: nox.Session) -> None:
    """Run the QE read-only L3 local validation suite."""
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "record",
        "--module",
        "qe",
        "--level",
        "L3",
        "--title",
        "QE read-only workspace access regression",
        external=True,
    )
    quick_validate = _codex_quick_validate_script()
    if not quick_validate.exists():
        session.error(f"Missing Codex skill validator: {quick_validate}")
    session.run("python", str(quick_validate), ".codex/skills/verify-aistock-feature", external=True)
    session.run(
        "python",
        ".codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py",
        "backend/routers/quantevolver_evolution.py",
        "backend/routers/quantevolver.py",
        "backend/tests/unified_engine/test_qe_evolution_read_paths.py",
        "backend/tests/unified_engine/test_qe_experiment_read_paths.py",
        "backend/tests/unified_engine/test_qe_experiment_log_terminal.py",
        "frontend/src/app/quantevolver/evolution/page.tsx",
        "frontend/src/app/quantevolver/experiments/[id]/page.tsx",
        "frontend/src/app/quantevolver/components/useExperimentSSE.ts",
        "frontend/tests/qe/qe-evolution-read-only.spec.ts",
        "frontend/tests/qe/qe-experiment-read-only.spec.ts",
        "tests/aistock_validation/modules/qe.md",
        "docs/architecture/qe_worker_workspace_read_refactor_validation_plan_20260502.md",
        "noxfile.py",
        "--fail-on",
        "HIGH",
        external=True,
    )
    session.notify("qe_read_backend")
    if os.environ.get("QE_READ_L3_SKIP_UI") != "1":
        session.notify("qe_read_ui")


@nox.session(venv_backend="none")
def qe_archive_backend(session: nox.Session) -> None:
    """Run QE archive backend/schema regression tests without starting services.

    Includes the QE archive handler contract module (T14a) and its regression
    tests when those paths are present on the active branch. The handler module
    + tests live on origin/claude/dw-foundation-20260510 and are pulled into
    main via merge; this session is forward-compatible with both states.
    """
    compileall_targets = [
        "backend/db/init_qe_archive_schema.py",
        "backend/routers/qe_archive.py",
        "backend/routers/quantevolver.py",
        "backend/services/qe_archive",
        "backend/services/quantevolver/completion_contract.py",
        "backend/services/quantevolver/qe_evolution_service.py",
        "scripts/qe_archive_backfill.py",
        "scripts/qe_archive_data_quality_smoke.py",
    ]
    handlers_dir = ROOT / "backend" / "services" / "qe_archive" / "handlers"
    if handlers_dir.exists():
        compileall_targets.append("backend/services/qe_archive/handlers")
    session.run(
        "python",
        "-m",
        "compileall",
        *compileall_targets,
        external=True,
    )
    pytest_targets = [
        "backend/tests/test_qe_archive_schema.py",
        "backend/tests/test_qe_archive_repository_static.py",
        "backend/tests/unified_engine/test_qe_completion_contract.py",
    ]
    handler_contract_test = ROOT / "backend" / "tests" / "qe_archive" / "test_handler_contract.py"
    if handler_contract_test.exists():
        pytest_targets.append("backend/tests/qe_archive/test_handler_contract.py")
    _run_pytest(
        session,
        *pytest_targets,
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def model_registry_backend(session: nox.Session) -> None:
    """Run Model Registry backend regression tests without starting services.

    The model_registry module + tests live on origin/codex/qe-governance-integration-20260509
    until that branch merges to main. Session skips gracefully when sources are
    not yet merged.
    """
    services_dir = ROOT / "backend" / "services" / "model_registry"
    tests_dir = ROOT / "backend" / "tests" / "model_registry"
    if not services_dir.exists() and not tests_dir.exists():
        session.skip(
            "Model Registry module not yet merged to main. Skipped pending "
            "origin/codex/qe-governance-integration-20260509 merge."
        )
    compileall_targets: list[str] = []
    if services_dir.exists():
        compileall_targets.append("backend/services/model_registry")
    for router in (ROOT / "backend" / "routers").glob("model_registry*.py"):
        compileall_targets.append(f"backend/routers/{router.name}")
    if compileall_targets:
        session.run(
            "python",
            "-m",
            "compileall",
            *compileall_targets,
            external=True,
        )
    pytest_targets: list[str] = []
    for candidate in (
        "test_governance_migration_smoke.py",
        "test_model_registry_phase5.py",
    ):
        path = tests_dir / candidate
        if path.exists():
            pytest_targets.append(f"backend/tests/model_registry/{candidate}")
    if not pytest_targets:
        session.skip("Model Registry tests not yet present.")
    _run_pytest(
        session,
        *pytest_targets,
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def market_regime_label(session: nox.Session) -> None:
    """Run market.regime_label data-pipeline tests without starting services.

    DDL + cron script live on origin/claude/dw-foundation-20260510. Test
    `backend/tests/market/test_regime_label.py` (relocated from
    scripts/test_regime_label.py) lands here once the dw-foundation merge
    completes. Session skips when sources are not yet present.
    """
    test_path = ROOT / "backend" / "tests" / "market" / "test_regime_label.py"
    cron_script = ROOT / "scripts" / "regime_label_daily.py"
    sql_init = ROOT / "backend" / "db" / "init_market_regime_label_20260510.sql"
    if not test_path.exists() and not cron_script.exists():
        session.skip(
            "market.regime_label sources not yet merged to main. Skipped pending "
            "origin/claude/dw-foundation-20260510 merge."
        )
    compileall_targets: list[str] = []
    if cron_script.exists():
        compileall_targets.append("scripts/regime_label_daily.py")
    fetch_script = ROOT / "scripts" / "regime_label_fetch_percentile.py"
    if fetch_script.exists():
        compileall_targets.append("scripts/regime_label_fetch_percentile.py")
    if compileall_targets:
        session.run(
            "python",
            "-m",
            "compileall",
            *compileall_targets,
            external=True,
        )
    if not test_path.exists():
        session.skip("backend/tests/market/test_regime_label.py not yet present.")
    _run_pytest(
        session,
        "backend/tests/market/test_regime_label.py",
        "-q",
        "-p",
        "no:cacheprovider",
    )
    # Note: SQL DDL init_market_regime_label_20260510.sql is text-only and not
    # exercised here; its application is owned by dw-foundation per
    # cross-tool drawer eb441503881c1c0f680ca7ac.
    _ = sql_init  # documented dependency


@nox.session(venv_backend="none")
def rl_execution_smoke(session: nox.Session) -> None:
    """Module-visibility smoke for backend.services.rl_execution.

    The rl_execution module + visibility regression test live on
    origin/fix/rl_execution_module_visibility-20260510 (and via direct
    contributions when that branch merges to main). The .gitignore-mask
    regression test runs unconditionally because the .gitignore is on every
    branch; the import-path tests skip when the module is not yet merged.
    """
    services_dir = ROOT / "backend" / "services" / "rl_execution"
    test_path = ROOT / "backend" / "tests" / "test_rl_execution_module_visibility.py"
    if test_path.exists():
        if services_dir.exists():
            session.run(
                "python",
                "-m",
                "compileall",
                "backend/services/rl_execution",
                external=True,
            )
        _run_pytest(
            session,
            "backend/tests/test_rl_execution_module_visibility.py",
            "-q",
            "-p",
            "no:cacheprovider",
        )
    else:
        session.skip(
            "backend/tests/test_rl_execution_module_visibility.py not yet present "
            "on this branch."
        )


@nox.session(venv_backend="none")
def validation_coverage_backend(session: nox.Session) -> None:
    """Run validation coverage contract and gate parser tests."""
    coverage_dir = ROOT / "tmp" / "validation" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    coverage_xml = coverage_dir / "validation_coverage_backend.xml"
    coverage_json = coverage_dir / "validation_coverage_backend.json"
    coverage_snapshot = coverage_dir / "validation_coverage_backend_snapshot.json"
    session.run(
        sys.executable,
        "-m",
        "compileall",
        "scripts/aistock_validate.py",
        external=True,
    )
    session.run(
        sys.executable,
        "-m",
        "pytest",
        "backend/tests/test_aistock_validate_metadata.py",
        "backend/tests/test_aistock_validate_coverage.py",
        "--cov=scripts.aistock_validate",
        "--cov-branch",
        f"--cov-report=xml:{coverage_xml}",
        f"--cov-report=json:{coverage_json}",
        "-q",
        "-p",
        "no:cacheprovider",
        env=_env(),
        external=True,
    )
    session.run(
        sys.executable,
        "scripts/aistock_validate.py",
        "coverage",
        "--module",
        "validation_center",
        "--level",
        "L2",
        "--coverage-xml",
        str(coverage_xml),
        "--output",
        str(coverage_snapshot),
        "--line-threshold",
        "70",
        "--branch-threshold",
        "55",
        external=True,
    )


@nox.session(venv_backend="none")
def validation_module_registry_l0(session: nox.Session) -> None:
    """Validate module registry, file ownership rules, and ownership scanner."""
    scan_paths = [
        "backend/services/validation/module_registry.py",
        "backend/services/validation/file_ownership.py",
        "backend/tests/test_validation_module_ownership.py",
        "backend/tests/test_validation_ui_target_catalog.py",
        "scripts/aistock_module_ownership_scan.py",
        "tests/aistock_validation/catalog/module_registry.yaml",
        "tests/aistock_validation/catalog/file_ownership.yaml",
        "noxfile.py",
        "backend/services/validation/plan_catalog.py",
        "backend/services/validation/ui_target_catalog.py",
        "tests/aistock_validation/catalog/test_plans.yaml",
        "tests/aistock_validation/catalog/ui_targets.yaml",
    ]
    session.run(
        sys.executable,
        "-m",
        "compileall",
        "backend/services/validation/module_registry.py",
        "backend/services/validation/file_ownership.py",
        "backend/services/validation/ui_target_catalog.py",
        "scripts/aistock_module_ownership_scan.py",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/test_validation_module_ownership.py",
        "-q",
        "-p",
        "no:cacheprovider",
    )
    session.run(
        sys.executable,
        "scripts/aistock_module_ownership_scan.py",
        "--output-json",
        "tmp/validation/module_ownership/l0_paths.json",
        "--summary-md",
        "tmp/validation/module_ownership/l0_paths.md",
        "--fail-on-unmapped",
        "--fail-on-ambiguous",
        *scan_paths,
        external=True,
    )


@nox.session(venv_backend="none")
def validation_center_backend(session: nox.Session) -> None:
    """Run Validation Center API, coverage, and controlled-runner contract tests."""
    coverage_dir = ROOT / "tmp" / "validation" / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    coverage_xml = coverage_dir / "validation_center_backend.xml"
    coverage_json = coverage_dir / "validation_center_backend.json"
    coverage_snapshot = coverage_dir / "validation_center_backend_snapshot.json"
    session.run(
        sys.executable,
        "-m",
        "compileall",
        "backend/routers/validation.py",
        "backend/services/validation",
        "scripts/aistock_validate.py",
        "scripts/validation_center_readonly_smoke.py",
        "scripts/validation_center_runner_smoke.py",
        external=True,
    )
    session.run(
        sys.executable,
        "-m",
        "pytest",
        "backend/tests/test_validation_center_api.py",
        "backend/tests/test_validation_center_readonly_smoke.py",
        "backend/tests/test_validation_center_runner_smoke.py",
        "backend/tests/test_validation_execution_runner.py",
        "backend/tests/test_validation_git_activity_provider.py",
        "backend/tests/test_validation_git_status_provider.py",
        "backend/tests/test_validation_module_ownership.py",
        "backend/tests/test_validation_ui_target_catalog.py",
        "backend/tests/test_aistock_validate_metadata.py",
        "backend/tests/test_aistock_validate_coverage.py",
        "--cov=backend.services.validation",
        "--cov=backend.routers.validation",
        "--cov=scripts.validation_center_readonly_smoke",
        "--cov=scripts.validation_center_runner_smoke",
        "--cov-branch",
        f"--cov-report=xml:{coverage_xml}",
        f"--cov-report=json:{coverage_json}",
        "-q",
        "-p",
        "no:cacheprovider",
        env=_env(),
        external=True,
    )
    session.run(
        sys.executable,
        "scripts/aistock_validate.py",
        "coverage",
        "--module",
        "validation_center",
        "--level",
        "L2",
        "--title",
        "Validation Center read-only API",
        "--coverage-xml",
        str(coverage_xml),
        "--output",
        str(coverage_snapshot),
        "--line-threshold",
        "75",
        "--branch-threshold",
        "55",
        external=True,
    )


@nox.session(venv_backend="none")
def validation_center_ui(session: nox.Session) -> None:
    """Run Validation Center UI checks on dev ports with mocked APIs."""
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8011")
    frontend_port = session.posargs[1] if len(session.posargs) > 1 else os.environ.get("FRONTEND_PORT", "3011")
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "ports",
        "--allow-occupied",
        backend_port,
        frontend_port,
        external=True,
    )
    old_cwd = Path.cwd()
    os.chdir(ROOT / "frontend")
    try:
        session.run(
            "npm",
            "exec",
            "tsc",
            "--",
            "--noEmit",
            "--incremental",
            "false",
            external=True,
        )
        session.run(
            "npm",
            "run",
            "test:e2e",
            "--",
            "tests/validation-center/validation-center.spec.ts",
            env=_env(
                {
                    "BACKEND_PORT": backend_port,
                    "FRONTEND_PORT": frontend_port,
                    "NEXT_PUBLIC_API_BASE": f"http://127.0.0.1:{backend_port}/api/v1",
                    "PLAYWRIGHT_SKIP_WEBSERVER": "1" if _is_port_open(frontend_port) else "0",
                }
            ),
            external=True,
        )
    finally:
        os.chdir(old_cwd)


@nox.session(venv_backend="none")
def validation_center_real_port_ui(session: nox.Session) -> None:
    """Run Validation Center UI against a real dev backend and frontend."""
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8012")
    frontend_port = session.posargs[1] if len(session.posargs) > 1 else os.environ.get("FRONTEND_PORT", "3012")
    if str(backend_port) == "8001":
        session.error("Refusing to validate against production backend port 8001.")
    if str(frontend_port) == "3000":
        session.error("Refusing to validate against production frontend port 3000.")
    api_base = os.environ.get("VALIDATION_CENTER_API_BASE", f"http://127.0.0.1:{backend_port}/api/v1")
    parsed_api_base = urlparse(api_base)
    if parsed_api_base.port == 8001:
        session.error("Refusing to validate against production backend port 8001.")
    if parsed_api_base.hostname not in {"127.0.0.1", "localhost", "::1"}:
        session.error("Refusing to validate against a non-localhost Validation Center API.")
    output = ROOT / "tmp" / "validation" / "validation_center" / "ui_real_port_smoke.json"
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "ports",
        "--allow-occupied",
        backend_port,
        frontend_port,
        external=True,
    )
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "services",
        "--backend-port",
        backend_port,
        "--skip-tdx",
        external=True,
    )
    old_cwd = Path.cwd()
    os.chdir(ROOT / "frontend")
    try:
        session.run(
            "npm",
            "exec",
            "tsc",
            "--",
            "--noEmit",
            "--incremental",
            "false",
            external=True,
        )
        session.run(
            "npm",
            "run",
            "test:e2e",
            "--",
            "tests/validation-center/validation-center-real-port.spec.ts",
            env=_env(
                {
                    "BACKEND_PORT": backend_port,
                    "FRONTEND_PORT": frontend_port,
                    "NEXT_PUBLIC_API_BASE": "/api/v1",
                    "PAPER_V2_API_PROXY_TARGET": api_base,
                    "VALIDATION_CENTER_API_BASE": api_base,
                    "VALIDATION_CENTER_UI_SMOKE_OUTPUT": str(output),
                    "PLAYWRIGHT_SKIP_WEBSERVER": "1" if _is_port_open(frontend_port) else "0",
                }
            ),
            external=True,
        )
    finally:
        os.chdir(old_cwd)
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "evidence",
        "--module",
        "validation_center",
        "--level",
        "L3",
        "--title",
        "Validation Center real-port Git and module quality UI smoke",
        "--output",
        "tmp/validation/validation_center/ui_real_port_smoke_evidence.json",
        "--smoke-json",
        str(output),
        "--playwright-report",
        "tmp/playwright-report",
        "--item",
        "frontend/tests/validation-center/validation-center-real-port.spec.ts",
        "--item",
        "frontend/src/app/validation-center/page.tsx",
        "--item",
        "frontend/src/lib/validation/api.ts",
        "--item",
        "backend/services/validation/git_activity_provider.py",
        "--item",
        "backend/services/validation/module_quality.py",
        env=_env({"VALIDATION_CENTER_API_BASE": api_base}),
        external=True,
    )


@nox.session(venv_backend="none")
def validation_center_live_readonly(session: nox.Session) -> None:
    """Probe a running dev Validation Center API with read-only GET requests only."""
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8011")
    api_base = os.environ.get("VALIDATION_CENTER_API_BASE", f"http://127.0.0.1:{backend_port}/api/v1")
    output = ROOT / "tmp" / "validation" / "validation_center" / "readonly_smoke.json"
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "ports",
        "--allow-occupied",
        backend_port,
        external=True,
    )
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "services",
        "--backend-port",
        backend_port,
        "--skip-tdx",
        external=True,
    )
    session.run(
        "python",
        "scripts/validation_center_readonly_smoke.py",
        "--api-base",
        api_base,
        "--output",
        str(output),
        env=_env({"VALIDATION_CENTER_API_BASE": api_base}),
        external=True,
    )
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "evidence",
        "--module",
        "validation_center",
        "--level",
        "L3",
        "--title",
        "Validation Center live read-only API smoke",
        "--output",
        "tmp/validation/validation_center/readonly_smoke_evidence.json",
        "--smoke-json",
        str(output),
        "--item",
        "script=scripts/validation_center_readonly_smoke.py",
        env=_env({"VALIDATION_CENTER_API_BASE": api_base}),
        external=True,
    )


@nox.session(venv_backend="none")
def validation_center_runner_smoke(session: nox.Session) -> None:
    """Start one safe allowlisted runner job on a running dev backend and verify archive output."""
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8012")
    api_base = os.environ.get("VALIDATION_CENTER_API_BASE", f"http://127.0.0.1:{backend_port}/api/v1")
    output = ROOT / "tmp" / "validation" / "validation_center" / "runner_smoke.json"
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "ports",
        "--allow-occupied",
        backend_port,
        external=True,
    )
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "services",
        "--backend-port",
        backend_port,
        "--skip-tdx",
        external=True,
    )
    session.run(
        "python",
        "scripts/validation_center_runner_smoke.py",
        "--api-base",
        api_base,
        "--output",
        str(output),
        "--plan-key",
        "guardrail_changed_files",
        env=_env({"VALIDATION_CENTER_API_BASE": api_base}),
        external=True,
    )
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "evidence",
        "--module",
        "validation_center",
        "--level",
        "L3",
        "--title",
        "Validation Center controlled runner live smoke",
        "--output",
        "tmp/validation/validation_center/runner_smoke_evidence.json",
        "--smoke-json",
        str(output),
        "--item",
        "script=scripts/validation_center_runner_smoke.py",
        env=_env({"VALIDATION_CENTER_API_BASE": api_base}),
        external=True,
    )


@nox.session(venv_backend="none")
def qe_data_contract_backend(session: nox.Session) -> None:
    """Run validation-tool metadata and QE completion contract tests."""
    session.run(
        "python",
        "-m",
        "compileall",
        "scripts/aistock_validate.py",
        "backend/services/quantevolver/completion_contract.py",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/test_aistock_validate_metadata.py",
        "backend/tests/test_aistock_validate_coverage.py",
        "backend/tests/unified_engine/test_qe_completion_contract.py",
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def qe_archive_data_quality(session: nox.Session) -> None:
    """Run read-only QE archive DB metadata and schema smoke checks."""
    args = [
        "scripts/qe_archive_data_quality_smoke.py",
        "--output",
        "tmp/qe_archive_data_quality_smoke.json",
    ]
    if session.posargs:
        args.extend(session.posargs)
    session.run("python", *args, env=_env(), external=True)


@nox.session(venv_backend="none")
def qe_archive_ui(session: nox.Session) -> None:
    """Run QE archive UI E2E tests on dev ports when UI tests exist."""
    test_dir = ROOT / "frontend" / "tests" / "qe-archive"
    if not test_dir.exists():
        session.skip("QE archive UI tests are not implemented yet.")
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8011")
    frontend_port = session.posargs[1] if len(session.posargs) > 1 else os.environ.get("FRONTEND_PORT", "3011")
    mock_api = os.environ.get("QE_ARCHIVE_UI_MOCK_API") == "1"
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "ports",
        "--allow-occupied",
        backend_port,
        frontend_port,
        external=True,
    )
    if not mock_api:
        session.run(
            "python",
            "scripts/aistock_validate.py",
            "services",
            "--backend-port",
            backend_port,
            "--skip-tdx",
            external=True,
        )
    old_cwd = Path.cwd()
    os.chdir(ROOT / "frontend")
    try:
        session.run(
            "npm",
            "exec",
            "tsc",
            "--",
            "--noEmit",
            "--incremental",
            "false",
            external=True,
        )
        session.run(
            "npm",
            "run",
            "test:e2e",
            "--",
            "tests/qe-archive",
            env=_env(
                {
                    "BACKEND_PORT": backend_port,
                    "FRONTEND_PORT": frontend_port,
                    "QE_ARCHIVE_UI_MOCK_API": "1" if mock_api else "0",
                    "QE_ARCHIVE_API_BASE": f"http://127.0.0.1:{backend_port}/api/v1",
                    "NEXT_PUBLIC_API_BASE": f"http://127.0.0.1:{backend_port}/api/v1",
                    "PLAYWRIGHT_SKIP_WEBSERVER": "1" if _is_port_open(frontend_port) else "0",
                }
            ),
            external=True,
        )
    finally:
        os.chdir(old_cwd)


@nox.session(venv_backend="none")
def qe_archive_l3(session: nox.Session) -> None:
    """Run the QE archive local validation suite, keeping production QE untouched."""
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "record",
        "--module",
        "qe_archive",
        "--level",
        "L3",
        "--title",
        "QE archive realtime warehouse validation",
        external=True,
    )
    quick_validate = _codex_quick_validate_script()
    if not quick_validate.exists():
        session.error(f"Missing Codex skill validator: {quick_validate}")
    session.run("python", str(quick_validate), ".codex/skills/verify-aistock-feature", external=True)
    session.run(
        "python",
        ".codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py",
        "backend/db/init_qe_archive_schema.py",
        "backend/routers/qe_archive.py",
        "backend/routers/quantevolver.py",
        "backend/services/qe_archive",
        "backend/services/quantevolver/completion_contract.py",
        "backend/services/quantevolver/qe_evolution_service.py",
        "backend/tests/test_aistock_validate_metadata.py",
        "backend/tests/test_qe_archive_schema.py",
        "backend/tests/test_qe_archive_repository_static.py",
        "backend/tests/unified_engine/test_qe_completion_contract.py",
        "docs/architecture/qe_data_completeness_phase1_development_plan_20260504.md",
        "scripts/qe_archive_backfill.py",
        "scripts/qe_archive_data_quality_smoke.py",
        "scripts/aistock_validate.py",
        "frontend/src/app/qe-archive",
        "frontend/src/lib/qe-archive",
        "frontend/tests/qe-archive",
        "tests/aistock_validation/modules/qe_archive.md",
        "docs/architecture/qe_realtime_experiment_warehouse_detailed_design_20260502.md",
        "noxfile.py",
        "--fail-on",
        "HIGH",
        external=True,
    )
    session.notify("qe_archive_backend")
    session.notify("qe_archive_data_quality")
    if os.environ.get("QE_ARCHIVE_L3_SKIP_UI") != "1":
        session.notify("qe_archive_ui")


@nox.session(venv_backend="none")
def paper_v2_live(session: nox.Session) -> None:
    """Run Paper v2 catch-up-to-live validation against dev backend and TDX."""
    backend_port = os.environ.get("BACKEND_PORT", "8012")
    tdx_port = os.environ.get("TDX_HTTP_PORT", "19080")
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "services",
        "--backend-port",
        backend_port,
        "--tdx-port",
        tdx_port,
        external=True,
    )
    session.run(
        "python",
        "scripts/paper_v2_live_validation.py",
        "--api-base",
        os.environ.get("PAPER_V2_API_BASE", f"http://127.0.0.1:{backend_port}/api/v1"),
        "--tdx-base-url",
        os.environ.get("TDX_BASE_URL", f"http://127.0.0.1:{tdx_port}"),
        *session.posargs,
        env=_env(),
        external=True,
    )
