from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from backend.execution_algos import ALGO_REGISTRY, get_algo
from backend.services.trading_core.execution_algo_retirement import (
    RETIRED_EXECUTION_ALGO_CODES,
    V25_EXECUTION_ALGO_RETIRED,
    ExecutionAlgoRetiredError,
    execution_algo_retirement_projection,
    require_execution_algo_active,
    require_strategy_manifest_execution_algos_active,
)


ROOT = Path(__file__).resolve().parents[3]
RETIRED_STARTUP_CLIS = (
    "scripts/add_v25_to_db.py",
    "scripts/add_v25_to_catalog.py",
    "scripts/compare_v25_vs_v25_1_1y.py",
    "scripts/qlib_v25_limit_state_smoke.py",
    "scripts/v25_1_smoke_backtest.py",
    "scripts/v24_v25_test.py",
    "scripts/v24_v25_real_test.py",
    "scripts/v25_verify_final.py",
    "scripts/v25_minute_test_final.py",
    "scripts/v25_minute_test.py",
    "scripts/v25_mini_backtest.py",
    "scripts/verify_v25_integration.py",
    "scripts/verify_v25_minute_execution.py",
    "scripts/paper_v2_live_validation.py",
)


@pytest.mark.parametrize("algo_code", sorted(RETIRED_EXECUTION_ALGO_CODES))
def test_active_registry_rejects_retired_v25_without_fallback(algo_code: str) -> None:
    assert algo_code not in ALGO_REGISTRY
    with pytest.raises(ExecutionAlgoRetiredError) as exc_info:
        get_algo(algo_code, config={})
    assert exc_info.value.error_code == V25_EXECUTION_ALGO_RETIRED
    assert exc_info.value.context == {
        "reason_code": V25_EXECUTION_ALGO_RETIRED,
        "algo_code": algo_code,
        "operation": "execution_algo_registry_construct",
        "semantic_path": "registry.algo_code",
        "fallback_used": False,
        "side_effect_started": False,
        "historical_artifacts_readable": True,
    }


def test_historical_artifacts_remain_read_only_without_new_activation() -> None:
    two_stage = importlib.import_module("backend.execution_algos.v25_two_stage_algo")
    small_cap = importlib.import_module("backend.execution_algos.v25_1_small_cap_algo")
    assert two_stage.V25TwoStageAlgo.ALGO_CODE == "V25_TWO_STAGE"
    assert small_cap.V25_1SmallCapAlgo.ALGO_CODE == "V25_1_SMALL_CAP"
    assert RETIRED_EXECUTION_ALGO_CODES.isdisjoint(ALGO_REGISTRY)


def test_retirement_projection_keeps_history_visible_but_not_selectable() -> None:
    assert execution_algo_retirement_projection("V25_TWO_STAGE") == {
        "retired": True,
        "selectable": False,
        "activatable": False,
        "retirement_reason_code": V25_EXECUTION_ALGO_RETIRED,
    }
    assert execution_algo_retirement_projection("TWAP")["selectable"] is True


def test_semantic_field_only_matcher_does_not_reject_labels_or_universe() -> None:
    require_strategy_manifest_execution_algos_active(
        {
            "package_name": "V25_TWO_STAGE historical report",
            "universe_policy": {"stock_pool": "V25_1_SMALL_CAP"},
            "backtest_context": {
                "execution": {"execution_algo": "TWAP"},
                "report_file": "V25_TWO_STAGE_metrics.json",
            },
            "minute_execution_policy": {"algo_code": "TWAP"},
        },
        operation="semantic_match_test",
    )


def test_manifest_execution_semantic_field_is_rejected() -> None:
    with pytest.raises(ExecutionAlgoRetiredError) as exc_info:
        require_strategy_manifest_execution_algos_active(
            {"backtest_context": {"execution": {"execution_algo": "V25_1_SMALL_CAP"}}},
            operation="semantic_match_test",
        )
    assert exc_info.value.context["semantic_path"] == "manifest.backtest_context.execution.execution_algo"


def test_non_retired_execution_algorithms_remain_available() -> None:
    assert require_execution_algo_active("TWAP", operation="positive_control") == "TWAP"
    assert get_algo("TWAP", config={}).ALGO_CODE == "TWAP"
    assert "V24_PLAN" in ALGO_REGISTRY


@pytest.mark.parametrize("relative_path", RETIRED_STARTUP_CLIS)
def test_v25_startup_cli_has_retirement_guard_before_side_effects(relative_path: str) -> None:
    source = (ROOT / relative_path).read_text(encoding="utf-8")
    guard_offset = source.index("require_execution_algo_active(")
    side_effect_offsets = [
        offset
        for marker in (
            "from backend.db.pg_pool import get_conn",
            "from backend.execution_algos.registry",
            "qlib.init(",
            "from rl_execution",
            "def main(",
        )
        if (offset := source.find(marker)) >= 0
    ]
    assert side_effect_offsets
    assert guard_offset < min(side_effect_offsets)


def test_historical_v25_artifact_audit_cli_remains_read_only() -> None:
    source = (ROOT / "scripts/qe_v25_existing_artifact_audit.py").read_text(encoding="utf-8")
    assert "require_execution_algo_active(" not in source
    assert "artifact" in source.lower()
