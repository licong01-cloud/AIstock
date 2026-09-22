from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]


def _run_fresh_import(source: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    return subprocess.run(
        [sys.executable, "-c", source],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


@pytest.mark.parametrize(
    "source",
    (
        "from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient; "
        "assert QEWorkspaceClient.__name__ == 'QEWorkspaceClient'",
        "from backend.services.hmm_data_source.backtest_source import BacktestDataSource; "
        "assert BacktestDataSource.__name__ == 'BacktestDataSource'",
        "from backend.routers import hmm_evolution; assert hmm_evolution.router is not None",
    ),
)
def test_shared_qe_dependencies_import_in_fresh_process(source: str) -> None:
    result = _run_fresh_import(source)

    assert result.returncode == 0, result.stderr


def test_strategy_package_public_component_export_remains_available() -> None:
    result = _run_fresh_import(
        "from backend.services.strategy_package import StrategyPackageComponentService; "
        "assert StrategyPackageComponentService.__name__ == 'StrategyPackageComponentService'"
    )

    assert result.returncode == 0, result.stderr


def test_package_asset_store_import_does_not_eager_load_runtime_components() -> None:
    result = _run_fresh_import(
        "import sys; "
        "from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore; "
        "assert LocalPackageAssetStore.__name__ == 'LocalPackageAssetStore'; "
        "assert 'backend.services.strategy_package.components' not in sys.modules"
    )

    assert result.returncode == 0, result.stderr
