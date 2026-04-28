from __future__ import annotations

import os
from pathlib import Path

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


@nox.session(venv_backend="none")
def l0(session: nox.Session) -> None:
    """Run local static gates that do not start AIstock services."""
    scan_paths = session.posargs or [
        "noxfile.py",
        "scripts/aistock_validate.py",
        "backend/tests/paper_trading_v2",
        "backend/tests/selection_center",
        "backend/tests/strategy_package",
        "frontend/playwright.config.ts",
        "frontend/tests/paper-v2",
    ]
    session.run(
        "python",
        "C:/Users/lc999/.codex/skills/.system/skill-creator/scripts/quick_validate.py",
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
            }
        ),
        cwd=ROOT / "frontend",
        external=True,
    )


@nox.session(venv_backend="none")
def paper_v2_l3(session: nox.Session) -> None:
    """Run the first-stage Paper v2 + Selection Center L3 local suite."""
    session.run("python", "scripts/aistock_validate.py", "record", "--module", "paper_v2_selection_center", "--level", "L3", "--title", "Paper v2 Selection Center L3 regression", external=True)
    session.notify("l0")
    session.notify("paper_v2_backend")
    if "--with-ui" in session.posargs:
        session.notify("paper_v2_ui")
