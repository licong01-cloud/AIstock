"""Regression tests for the rl_execution module visibility issue.

Background: a broad ``backend/services/*.py`` rule in .gitignore previously
masked ``backend/services/rl_execution/__init__.py`` and downstream files,
which hid the entire module from the working tree on fresh clones and broke
``backend.main`` import (and any backend dev-port smoke). Fix tracked at
origin/fix/rl_execution_module_visibility-20260510 and re-confirmed via
mempalace drawers across cross-tool/codex-claude-coord on 2026-05-10.

These tests guard:

- Visibility: the module is importable when present (skipped on branches
  where rl_execution is not yet merged).
- Routing: the FastAPI app loads ``/api/v1/rl-execution/*`` routes when the
  module is present.
- Gitignore: no .gitignore rule masks ``backend/services/rl_execution`` files
  (this test runs unconditionally so it catches regressions in main even
  before the module itself is merged in this worktree).
"""

from __future__ import annotations

import importlib
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
RL_EXECUTION_DIR = REPO_ROOT / "backend" / "services" / "rl_execution"
RL_EXECUTION_INIT = RL_EXECUTION_DIR / "__init__.py"


def test_gitignore_does_not_mask_rl_execution() -> None:
    """``.gitignore`` must not mask backend/services/rl_execution/*.py.

    Runs unconditionally — the regression risk is a broad pattern landing in
    .gitignore even before the rl_execution module returns to main.
    """
    init_relpath = "backend/services/rl_execution/__init__.py"
    proc = subprocess.run(
        ["git", "check-ignore", "-v", init_relpath],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 1, (
        "git check-ignore matched a .gitignore rule for "
        f"{init_relpath}; this masks the rl_execution module. "
        f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )


def test_rl_execution_module_importable() -> None:
    """``backend.services.rl_execution`` must import without ModuleNotFoundError."""
    if not RL_EXECUTION_INIT.exists():
        pytest.skip(
            "backend/services/rl_execution/__init__.py not present on this branch; "
            "module visibility import test deferred until merge."
        )
    module = importlib.import_module("backend.services.rl_execution")
    assert module is not None


def test_rl_execution_router_loaded_in_main() -> None:
    """``backend.main`` must register the rl-execution router when module is present.

    Uses FastAPI app introspection — does not start a server. Asserts at least
    one route under ``/api/v1/rl-execution`` exists when the module is present.
    """
    if not RL_EXECUTION_INIT.exists():
        pytest.skip(
            "backend/services/rl_execution/__init__.py not present on this branch; "
            "router-loaded test deferred until merge."
        )
    main = importlib.import_module("backend.main")
    app = getattr(main, "app", None)
    assert app is not None, "backend.main.app is missing"
    paths = {
        getattr(route, "path", "")
        for route in getattr(app, "routes", [])
    }
    rl_paths = [p for p in paths if p and p.startswith("/api/v1/rl-execution")]
    assert rl_paths, (
        "Expected at least one /api/v1/rl-execution* route registered on backend.main.app"
    )
