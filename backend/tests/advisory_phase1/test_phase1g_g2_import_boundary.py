from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys


G2_MODULES = (
    Path("backend/services/advisory_phase1/phase1g_source_replay.py"),
    Path("backend/services/advisory_phase1/phase1g_source_replay_postgres.py"),
    Path("backend/services/advisory_phase1/phase1g_historical_trace_contract.py"),
    Path("backend/services/advisory_phase1/historical_trace_projection_postgres.py"),
)
FORBIDDEN_PREFIXES = (
    "backend.services.selection_center",
    "backend.services.simulation_runtime",
    "backend.services.paper_trading",
    "backend.services.quantevolver",
    "backend.services.rdagent",
    "backend.services.strategy_package.runtime",
    "backend.services.strategy_package.live_inference",
    "backend.infra.qmt",
    "backend.routers",
)


def test_g2_direct_imports_and_source_do_not_cross_runtime_or_control_boundaries() -> (
    None
):
    prohibited_terms = (
        "approval",
        "rbac",
        "manual_bypass",
        "database_backup",
        "role_authorization",
    )
    for path in G2_MODULES:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(item.name for item in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert not [
            name for name in imports if name.startswith(FORBIDDEN_PREFIXES)
        ], path
        lowered = source.lower()
        assert not [term for term in prohibited_terms if term in lowered], path
        assert "get_conn(" not in source
        assert "load_dotenv" not in source


def test_g2_runtime_import_does_not_transitively_load_selection_simulation_or_trading_runtime() -> (
    None
):
    script = r"""
import builtins
real_import = builtins.__import__
forbidden = (
    "backend.services.selection_center",
    "backend.services.simulation_runtime",
    "backend.services.paper_trading",
    "backend.services.quantevolver",
    "backend.services.rdagent",
    "backend.services.strategy_package.runtime",
    "backend.services.strategy_package.live_inference",
    "backend.routers",
)
def guarded(name, globals=None, locals=None, fromlist=(), level=0):
    if name.startswith(forbidden):
        raise AssertionError(f"forbidden runtime import: {name}")
    return real_import(name, globals, locals, fromlist, level)
builtins.__import__ = guarded
import backend.services.advisory_phase1.phase1g_source_replay
import backend.services.advisory_phase1.phase1g_source_replay_postgres
import backend.services.advisory_phase1.phase1g_historical_trace_contract
import backend.services.advisory_phase1.historical_trace_projection_postgres
"""
    completed = subprocess.run(
        (sys.executable, "-c", script),
        cwd=Path.cwd(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
