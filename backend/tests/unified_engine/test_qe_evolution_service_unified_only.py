"""Static guards for retiring QE legacy execution paths."""

import inspect
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler


def test_standard_evolution_loop_uses_unified_executor_only():
    source = inspect.getsource(AutoEvolutionScheduler.submit_next_loop)

    assert "BacktestExecutor" in source
    assert "build_config_from_evolution_loop" in source
    assert "compose_experiment_in_memory(" not in source
    assert "create_and_run_loop(" not in source


def test_strategy_evolution_loop_uses_unified_executor_only():
    source = inspect.getsource(AutoEvolutionScheduler.submit_strategy_evo_loop)

    assert "BacktestExecutor" in source
    assert "build_config_from_strategy_evo_loop" in source
    assert "mode=BacktestMode.BACKTEST_ONLY" in source
    assert "compose_experiment_in_memory(" not in source
    assert "create_and_run_loop(" not in source


def test_qe_evolution_service_has_no_direct_legacy_submission_calls():
    source = inspect.getsource(AutoEvolutionScheduler)

    assert "compose_experiment_in_memory(" not in source
    assert "client.create_and_run_loop(" not in source
