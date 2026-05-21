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


def _hosted_ci() -> bool:
    return os.environ.get("GITHUB_ACTIONS") == "true" or os.environ.get("AISTOCK_HOSTED_CI") == "1"


def _is_port_open(port: str) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex(("127.0.0.1", int(port))) == 0


def _codex_quick_validate_script() -> Path:
    codex_home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")
    return codex_home / "skills" / ".system" / "skill-creator" / "scripts" / "quick_validate.py"


def _validate_in_tree_codex_skill(session: nox.Session, skill_path: str) -> None:
    """Validate the checked-in skill even when hosted CI lacks Codex system skills."""
    quick_validate = _codex_quick_validate_script()
    if quick_validate.exists():
        session.run("python", str(quick_validate), skill_path, external=True)
        return

    root = ROOT / skill_path
    required = [
        root / "SKILL.md",
        root / "scripts" / "scan_quality_guardrails.py",
    ]
    missing = [path for path in required if not path.exists()]
    if missing:
        session.error("Missing in-tree Codex skill files: " + ", ".join(str(path) for path in missing))
    session.log(f"Codex system skill validator unavailable; basic in-tree skill check passed for {skill_path}.")


def _guardrail_baseline_json(session: nox.Session) -> str:
    """Locate the guardrail baseline JSON.

    Default points at the in-tree baseline under tests/aistock_validation/.
    Falls back to the legacy ``tmp/validation/guardrails/baseline_20260504.json``
    location for compatibility with older worktrees that haven't migrated yet.
    """
    candidates = [
        os.environ.get("AISTOCK_GUARDRAIL_BASELINE_JSON"),
        "tests/aistock_validation/guardrails_baseline_20260511.json",
        "tests/aistock_validation/guardrails_baseline_20260504.json",
        "tmp/validation/guardrails/baseline_20260504.json",
    ]
    for candidate in candidates:
        if candidate and (ROOT / candidate).exists():
            return candidate
    session.error(
        "Missing guardrail baseline JSON. Run "
        "`python scripts/aistock_guardrail_scan.py --baseline "
        "--output-json tests/aistock_validation/guardrails_baseline_<YYYYMMDD>.json` "
        "first, or set AISTOCK_GUARDRAIL_BASELINE_JSON to an existing file."
    )


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
        "backend/services/data_sync_targets.py",
        "backend/services/tushare_dataset_specs.py",
        "backend/services/tushare_sync_engine.py",
        "backend/ingestion/tdx_scheduler.py",
        "backend/routers/ingestion.py",
        "backend/services/quantevolver/completion_contract.py",
        "backend/tests/test_dataset_refresh_audit.py",
        "backend/tests/test_data_sync_targets.py",
        "backend/tests/test_tushare_sync_engine.py",
        "backend/tests/ingestion/test_tdx_scheduler_cyq_engine_routing.py",
        "backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py",
        "backend/tests/test_ingestion_data_stats_readiness_api.py",
        "backend/tests/test_aistock_validate_metadata.py",
        "backend/tests/test_aistock_validate_coverage.py",
        "backend/tests/test_aistock_guardrail_scan.py",
        "backend/tests/test_validation_git_status_provider.py",
        "backend/tests/test_validation_module_ownership.py",
        "backend/tests/test_validation_center_api.py",
        "backend/services/validation/plan_catalog.py",
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
        "tests/aistock_validation/catalog/test_plans.yaml",
        "tests/aistock_validation/modules/local_data_management.md",
        "tests/aistock_validation/catalog/file_ownership.yaml",
        "tests/aistock_validation/modules/development_guardrails.md",
    ]
    _validate_in_tree_codex_skill(session, ".codex/skills/verify-aistock-feature")
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
    args = [
        "backend/tests/paper_trading_v2",
        "backend/tests/selection_center",
        "backend/tests/strategy_package",
    ]
    if _hosted_ci():
        # Hosted Linux runners have an ephemeral DB, not the pre-seeded local
        # Windows dev DB required by these integration modules.
        args.extend(
            [
                "--ignore-glob=backend/tests/paper_trading_v2/*dev_db*.py",
                "--ignore=backend/tests/paper_trading_v2/test_runtime_enable_paper_compat.py",
                "-k",
                "not test_model_asset_resolver_uses_aistock_cache_without_wsl_unc_probe",
            ]
        )
    _run_pytest(
        session,
        *args,
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def paper_v2_qe_candidate_devdb_e2e(session: nox.Session) -> None:
    """Run the guarded DEV-DB QE candidate -> Paper v2 cross-module E2E gate."""
    session.run(
        "python",
        "-m",
        "compileall",
        "scripts/dev_db/seed_paper_v2_qe_candidate_flow.py",
        "backend/tests/e2e/test_paper_v2_qe_candidate_platform_devdb.py",
        external=True,
    )
    session.run(
        "python",
        "-m",
        "pytest",
        "backend/tests/e2e/test_paper_v2_qe_candidate_platform_devdb.py",
        "-q",
        "-p",
        "no:cacheprovider",
        env=_env({"AISTOCK_DEV_DB_E2E": "1"}),
        external=True,
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
        "backend/tests/test_data_sync_targets.py",
        "backend/tests/test_data_quality_smoke_env.py",
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
        "--use-dev-db",
        "--allow-offline-schema-review",
        "--output",
        "tmp/local_data_management_audit_smoke.json",
        env=_env(),
        external=True,
    )


@nox.session(venv_backend="none")
def data_sync_autonomy_backend(session: nox.Session) -> None:
    """Run backend regression for autonomous local data sync control-plane changes."""
    session.run(
        "python",
        "-m",
        "compileall",
        "backend/services/tushare_sync_engine.py",
        "backend/services/tushare_dataset_specs.py",
        "backend/services/data_sync_targets.py",
        "backend/ingestion/tdx_scheduler.py",
        "backend/routers/ingestion.py",
        "scripts/seed_dataset_refresh_audit.py",
        "scripts/aistock_data_quality_smoke.py",
        "backend/services/validation/plan_catalog.py",
        "noxfile.py",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/test_tushare_sync_engine.py",
        "backend/tests/test_data_sync_targets.py",
        "backend/tests/ingestion/test_tdx_scheduler_cyq_engine_routing.py",
        "backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py",
        "backend/tests/test_ingestion_data_stats_readiness_api.py",
        "backend/tests/test_dataset_refresh_audit.py",
        "backend/tests/test_validation_center_api.py",
        "backend/tests/test_validation_execution_runner.py",
        "backend/tests/test_data_quality_smoke_env.py",
        "-q",
        "-p",
        "no:cacheprovider",
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
    session.notify("data_quality_deep")
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
    _validate_in_tree_codex_skill(session, ".codex/skills/verify-aistock-feature")
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
def dr_validate(session: nox.Session) -> None:
    """Stage 7.4 DR validation against E:/DEV backup/aistock_pg_snapshots/.

    Three families (Stage 7.4 §1-§3):
      - dump file validity (pg_restore --list / plain SQL header scan)
      - dump schema vs dev DB schema diff (one-way subset)
      - retention policy compliance (30 day rolling + monthly permanent)

    Skips cleanly when the backup directory is empty or pg_restore /
    docker postgres container are not available, so the session is safe
    on fresh CI hosts without local DR state.
    """
    session.run(
        "python",
        "-m",
        "compileall",
        "backend/tests/dr",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/dr",
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def data_quality_deep(session: nox.Session) -> None:
    """Stage 7.3 deep data-quality assertions against dev DB.

    5 assertion families (≥15 tests) covering field-level consistency,
    JSONB structural validation, derived-field correctness vs the
    canonical reference implementation, cross-table cardinality, and
    time-series monotonicity. Tests skip cleanly when the dev DB
    credentials are unavailable, so the session is also safe to run on
    fresh CI hosts without the dev DB loaded.
    """
    session.run(
        "python",
        "-m",
        "compileall",
        "backend/tests/data_quality",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/data_quality",
        "-q",
        "-p",
        "no:cacheprovider",
    )


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
        "backend/db/init_qe_execution_templates_schema.py",
        "backend/routers/qe_archive.py",
        "backend/routers/qe_templates.py",
        "backend/routers/quantevolver.py",
        "backend/services/qe_archive",
        "backend/services/qe_templates",
        "backend/services/quantevolver/completion_contract.py",
        "backend/services/quantevolver/qe_evolution_service.py",
        "scripts/aistock_mcp_common.py",
        "scripts/aistock_qe_experiment_mcp_server.py",
        "scripts/aistock_qe_archive_mcp_server.py",
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
        "backend/tests/test_qe_execution_templates_schema.py",
        "backend/tests/test_qe_archive_repository_static.py",
        "backend/tests/qe_templates/test_template_validator.py",
        "backend/tests/test_aistock_qe_mcp_servers.py",
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
def qe_mcp_backend(session: nox.Session) -> None:
    """Run QE MCP/template/archive backend contract tests without services."""
    session.run(
        "python",
        "-m",
        "compileall",
        "backend/db/init_qe_execution_templates_schema.py",
        "backend/routers/qe_templates.py",
        "backend/services/qe_templates",
        "scripts/aistock_mcp_common.py",
        "scripts/aistock_qe_experiment_mcp_server.py",
        "scripts/aistock_qe_archive_mcp_server.py",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/test_qe_execution_templates_schema.py",
        "backend/tests/qe_templates/test_template_validator.py",
        "backend/tests/test_aistock_qe_mcp_servers.py",
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def research_pipeline_backend(session: nox.Session) -> None:
    """Run Research Pipeline backend/schema/router tests without services."""
    session.run(
        "python",
        "-m",
        "compileall",
        "backend/db/init_research_pipeline_schema.py",
        "backend/services/research_pipeline",
        "backend/routers/research_pipeline.py",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/research_pipeline",
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def research_mcp_contract(session: nox.Session) -> None:
    """Run Research MCP gateway contract tests for the phased platform."""
    session.run(
        "python",
        "-m",
        "compileall",
        "backend/mcp",
        "scripts/aistock_mcp_gateway.py",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/mcp",
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def research_assistant_backend(session: nox.Session) -> None:
    """Run Research Assistant backend/schema/router tests without services."""
    session.run(
        "python",
        "-m",
        "compileall",
        "-b",
        "backend/db/init_research_assistant_schema_20260521.py",
        "backend/services/research_assistant",
        "backend/routers/research_assistant.py",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/research_assistant",
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def research_assistant_mcp_contract(session: nox.Session) -> None:
    """Run Research Assistant MCP gateway contract tests."""
    session.run(
        "python",
        "-m",
        "compileall",
        "-b",
        "backend/mcp",
        "scripts/aistock_mcp_gateway.py",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/mcp/test_research_assistant_module.py",
        "backend/tests/mcp/test_profiles_registry_gateway.py",
        "-q",
        "-p",
        "no:cacheprovider",
    )


@nox.session(venv_backend="none")
def research_assistant_ui(session: nox.Session) -> None:
    """Run Research Assistant mocked UI regression."""
    session.chdir("frontend")
    session.run("npx", "playwright", "test", "tests/research-assistant/research-assistant.spec.ts", "--project", "chromium", external=True)


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
def validation_catalog_integrity(session: nox.Session) -> None:
    """Validate cross-catalog plan/module/UI/resource consistency."""
    session.run(
        sys.executable,
        "-m",
        "compileall",
        "backend/services/validation/catalog_integrity.py",
        "scripts/aistock_validation_catalog_integrity.py",
        external=True,
    )
    _run_pytest(
        session,
        "backend/tests/test_validation_catalog_integrity.py",
        "-q",
        "-p",
        "no:cacheprovider",
    )
    session.run(
        sys.executable,
        "scripts/aistock_validation_catalog_integrity.py",
        "--output-json",
        "tmp/validation/catalog/integrity_report.json",
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
        "scripts/aistock_mcp_server.py",
        "scripts/aistock_validation_catalog_integrity.py",
        "scripts/validation_failure_event_to_bug.py",
        "scripts/validation_center_readonly_smoke.py",
        "scripts/validation_center_runner_smoke.py",
        external=True,
    )
    session.run(
        sys.executable,
        "-m",
        "pytest",
        "backend/tests/test_validation_center_api.py",
        "backend/tests/test_validation_catalog_integrity.py",
        "backend/tests/test_validation_center_readonly_smoke.py",
        "backend/tests/test_validation_pipeline_center_phase1.py",
        "backend/tests/test_validation_platform_health.py",
        "backend/tests/test_validation_center_runner_smoke.py",
        "backend/tests/test_validation_execution_runner.py",
        "backend/tests/test_validation_git_activity_provider.py",
        "backend/tests/test_validation_git_status_provider.py",
        "backend/tests/test_validation_module_ownership.py",
        "backend/tests/test_validation_ui_target_catalog.py",
        "backend/tests/test_aistock_validate_metadata.py",
        "backend/tests/test_aistock_validate_coverage.py",
        "backend/tests/test_aistock_mcp_server.py",
        "backend/tests/scripts/test_aistock_mcp_github_issue_tools.py",
        "backend/tests/scripts/test_validation_failure_event_to_bug.py",
        "backend/tests/scripts/test_bug_github_sync.py",
        "--cov=backend.services.validation",
        "--cov=backend.routers.validation",
        "--cov=scripts.aistock_mcp_server",
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
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8012")
    frontend_port = session.posargs[1] if len(session.posargs) > 1 else os.environ.get("FRONTEND_PORT", "3012")
    # Default to mock-first; live mode requires QE_ARCHIVE_UI_MOCK_API=0
    # AND live-data spec assertions (current spec encodes mock fixtures).
    mock_api = os.environ.get("QE_ARCHIVE_UI_MOCK_API", "1") != "0"
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
    _validate_in_tree_codex_skill(session, ".codex/skills/verify-aistock-feature")
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
    session.notify("data_quality_deep")
    if os.environ.get("QE_ARCHIVE_L3_SKIP_UI") != "1":
        session.notify("qe_archive_ui")


@nox.session(venv_backend="none")
def qe_template_ui(session: nox.Session) -> None:
    """Run QE template management UI tests on non-production dev ports."""
    test_dir = ROOT / "frontend" / "tests" / "qe-templates"
    if not test_dir.exists():
        session.skip("QE template UI tests are not implemented yet.")
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
            "tests/qe-templates",
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
def qe_mcp_l3(session: nox.Session) -> None:
    """Run QE MCP v1 local validation gates on non-production dev ports."""
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8011")
    frontend_port = session.posargs[1] if len(session.posargs) > 1 else os.environ.get("FRONTEND_PORT", "3011")
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "record",
        "--module",
        "qe_mcp",
        "--level",
        "L3",
        "--title",
        "QE MCP v1 backend and MCP contract validation",
        external=True,
    )
    _validate_in_tree_codex_skill(session, ".codex/skills/verify-aistock-feature")
    session.run(
        "python",
        ".codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py",
        "backend/db/init_qe_execution_templates_schema.py",
        "backend/routers/qe_templates.py",
        "backend/routers/qe_archive.py",
        "backend/routers/quantevolver_evolution.py",
        "backend/services/qe_archive/backfill_service.py",
        "backend/services/qe_archive/bootstrap_marker.py",
        "backend/services/qe_archive/event_capture.py",
        "backend/services/qe_archive/ingest_history.py",
        "backend/services/qe_archive/models.py",
        "backend/services/qe_archive/policy.py",
        "backend/services/qe_archive/realtime_ingestion.py",
        "backend/services/qe_archive/repository.py",
        "backend/services/qe_archive/skip_registry.py",
        "backend/services/qe_archive/worker_loop.py",
        "backend/services/qe_archive/worker_service.py",
        "backend/services/qe_templates",
        "scripts/aistock_mcp_common.py",
        "scripts/aistock_qe_experiment_mcp_server.py",
        "scripts/aistock_qe_archive_mcp_server.py",
        "backend/tests/test_aistock_qe_mcp_servers.py",
        "backend/tests/test_qe_execution_templates_schema.py",
        "backend/tests/qe_templates",
        "frontend/src/app/quantevolver/templates",
        "frontend/src/lib/qe-templates",
        "frontend/tests/qe-templates",
        "noxfile.py",
        "--fail-on",
        "HIGH",
        external=True,
    )
    session.notify("qe_mcp_backend")
    session.notify("qe_archive_backend")
    if os.environ.get("QE_MCP_L3_SKIP_TEMPLATE_UI") != "1":
        session.notify("qe_template_ui")
    session.run(
        "python",
        "scripts/aistock_validate.py",
        "ports",
        "--allow-occupied",
        backend_port,
        frontend_port,
        external=True,
    )


@nox.session(venv_backend="none")
def market_regime_ui(session: nox.Session) -> None:
    """Run Market Regime UI E2E tests on dev ports.

    Uses Playwright route mocks for /api/v1/market/regime-label/*; the
    backend router (backend/routers/market_regime.py) is read-only and
    safe to query against dev_db once available.
    """
    test_dir = ROOT / "frontend" / "tests" / "market-regime"
    if not test_dir.exists():
        session.skip("Market Regime UI tests are not implemented yet.")
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8012")
    frontend_port = session.posargs[1] if len(session.posargs) > 1 else os.environ.get("FRONTEND_PORT", "3012")
    # Default mock-first; set MARKET_REGIME_UI_MOCK_API=0 for live mode.
    mock_api = os.environ.get("MARKET_REGIME_UI_MOCK_API", "1") != "0"
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
        session.run("npm", "exec", "tsc", "--", "--noEmit", "--incremental", "false", external=True)
        session.run(
            "npm",
            "run",
            "test:e2e",
            "--",
            "tests/market-regime",
            env=_env(
                {
                    "BACKEND_PORT": backend_port,
                    "FRONTEND_PORT": frontend_port,
                    "MARKET_REGIME_UI_MOCK_API": "1" if mock_api else "0",
                    "NEXT_PUBLIC_API_BASE": f"http://127.0.0.1:{backend_port}/api/v1",
                    "PLAYWRIGHT_SKIP_WEBSERVER": "1" if _is_port_open(frontend_port) else "0",
                }
            ),
            external=True,
        )
    finally:
        os.chdir(old_cwd)


@nox.session(venv_backend="none")
def rl_execution_ui(session: nox.Session) -> None:
    """Run RL Execution UI E2E tests on dev ports.

    Mocks /api/v1/rl-execution/*; backend router already exists on main
    so live wiring is one env-var flip away (MOCK_API=0).
    """
    test_dir = ROOT / "frontend" / "tests" / "rl-execution"
    if not test_dir.exists():
        session.skip("RL Execution UI tests are not implemented yet.")
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8012")
    frontend_port = session.posargs[1] if len(session.posargs) > 1 else os.environ.get("FRONTEND_PORT", "3012")
    # Default mock-first; set RL_EXECUTION_UI_MOCK_API=0 for live mode.
    mock_api = os.environ.get("RL_EXECUTION_UI_MOCK_API", "1") != "0"
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
        session.run("npm", "exec", "tsc", "--", "--noEmit", "--incremental", "false", external=True)
        session.run(
            "npm",
            "run",
            "test:e2e",
            "--",
            "tests/rl-execution",
            env=_env(
                {
                    "BACKEND_PORT": backend_port,
                    "FRONTEND_PORT": frontend_port,
                    "RL_EXECUTION_UI_MOCK_API": "1" if mock_api else "0",
                    "NEXT_PUBLIC_API_BASE": f"http://127.0.0.1:{backend_port}/api/v1",
                    "PLAYWRIGHT_SKIP_WEBSERVER": "1" if _is_port_open(frontend_port) else "0",
                }
            ),
            external=True,
        )
    finally:
        os.chdir(old_cwd)


@nox.session(venv_backend="none")
def strategy_package_governance_ui(session: nox.Session) -> None:
    """Run Strategy Package Governance UI E2E tests on dev ports.

    Mocks /api/v1/strategy-packages/* responses; safe to run without the
    governance backend branch merged. Live integration is wired once the
    Codex governance branch lands on main.
    """
    test_dir = ROOT / "frontend" / "tests" / "strategy-package-governance"
    if not test_dir.exists():
        session.skip("Strategy Package Governance UI tests are not implemented yet.")
    backend_port = session.posargs[0] if session.posargs else os.environ.get("BACKEND_PORT", "8012")
    frontend_port = session.posargs[1] if len(session.posargs) > 1 else os.environ.get("FRONTEND_PORT", "3012")
    # Default mock-first; live mode requires Codex governance branch merge.
    mock_api = os.environ.get("STRATEGY_PACKAGE_GOVERNANCE_UI_MOCK_API", "1") != "0"
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
            "tests/strategy-package-governance",
            env=_env(
                {
                    "BACKEND_PORT": backend_port,
                    "FRONTEND_PORT": frontend_port,
                    "STRATEGY_PACKAGE_GOVERNANCE_UI_MOCK_API": "1" if mock_api else "0",
                    "NEXT_PUBLIC_API_BASE": f"http://127.0.0.1:{backend_port}/api/v1",
                    "PLAYWRIGHT_SKIP_WEBSERVER": "1" if _is_port_open(frontend_port) else "0",
                }
            ),
            external=True,
        )
    finally:
        os.chdir(old_cwd)


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
