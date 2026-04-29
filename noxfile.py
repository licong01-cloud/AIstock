from __future__ import annotations

import os
import socket
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


def _is_port_open(port: str) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex(("127.0.0.1", int(port))) == 0


def _codex_quick_validate_script() -> Path:
    codex_home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")
    return codex_home / "skills" / ".system" / "skill-creator" / "scripts" / "quick_validate.py"


@nox.session(venv_backend="none")
def l0(session: nox.Session) -> None:
    """Run local static gates that do not start AIstock services."""
    scan_paths = session.posargs or [
        "noxfile.py",
        "scripts/aistock_validate.py",
        "scripts/paper_v2_live_validation.py",
        "backend/tests/paper_trading_v2",
        "backend/tests/selection_center",
        "backend/tests/strategy_package",
        "frontend/playwright.config.ts",
        "frontend/tests/paper-v2",
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
        "python",
        "scripts/aistock_validate.py",
        "services",
        "--backend-port",
        backend_port,
        "--tdx-port",
        os.environ.get("TDX_HTTP_PORT", "19080"),
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
    if os.environ.get("PAPER_V2_L3_SKIP_UI") != "1":
        session.notify("paper_v2_ui")


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
