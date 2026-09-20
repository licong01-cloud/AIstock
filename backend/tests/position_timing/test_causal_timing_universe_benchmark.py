from __future__ import annotations

import inspect
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.position_timing.causal_timing_contracts import BH, CYCLE5_GBDT
from backend.services.position_timing.causal_timing_universe_benchmark import (
    ALL_PIT,
    CONTRACT,
    FORMAL_ENDPOINT_COUNT,
    LARGE,
    MODEL_SHA256,
    POOL_IDS,
    SMALL,
    U0_BRIDGE,
    _bridge,
    _capacity,
    _first_candidate,
    _summary,
    enrollment_by_pool,
)
from backend.services.position_timing.r8_proxy_screen import BAK_COLUMNS, DAILY_COLUMNS


DATES = pd.DatetimeIndex([
    "2024-06-27",
    "2024-06-28",
    "2024-07-01",
    "2026-08-27",
])


def _inputs(market_caps: list[float]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, np.ndarray]:
    bars = pd.DataFrame({"pit_active": [True] * len(DATES)}, index=DATES)
    daily = pd.DataFrame(index=DATES, columns=list(DAILY_COLUMNS), dtype=float)
    daily["db_total_mv"] = market_caps
    daily["db_turnover_rate_f"] = 1.0
    daily["db_pe_ttm"] = 10.0
    daily["db_pb"] = 1.0
    bak = pd.DataFrame(1.0, index=DATES, columns=list(BAK_COLUMNS))
    return bars, daily, bak, np.ones(len(DATES), dtype=bool)


def test_pool_boundaries_use_lagged_market_cap_and_all_does_not_require_cap() -> None:
    bars, daily, bak, ready = _inputs([5_000_001.0, 499_999.0, np.nan, 1_000_000.0])
    enrollments, audits = enrollment_by_pool(bars=bars, daily=daily, bak=bak, ready=ready)

    assert enrollments[LARGE] == 1
    assert enrollments[SMALL] == 2
    assert enrollments[ALL_PIT] == 1
    assert enrollments[U0_BRIDGE] is None
    assert audits[ALL_PIT]["market_cap_unknown_session_count"] == 1


def test_u0_bridge_preserves_parent_unknown_before_enrollment_semantics() -> None:
    bars, daily, bak, ready = _inputs([np.nan, 1_000_000.0, 1_000_000.0, 1_000_000.0])
    enrollments, audits = enrollment_by_pool(bars=bars, daily=daily, bak=bak, ready=ready)

    assert enrollments[U0_BRIDGE] is None
    assert audits[U0_BRIDGE]["status"] == "ENROLLMENT_UNKNOWN"
    assert enrollments[ALL_PIT] == 1


def test_first_candidate_obeys_frozen_decision_bounds() -> None:
    mask = np.array([True, False, True, True, True])
    assert _first_candidate(mask, 1, 3) == 2
    assert _first_candidate(mask, 4, 3) is None


def test_contract_freezes_models_family_and_zero_side_effects() -> None:
    assert tuple(CONTRACT["pools"]) == POOL_IDS
    assert FORMAL_ENDPOINT_COUNT == 4
    assert MODEL_SHA256 == {
        "ridge": "aac2246286a863e02ecc985236c1ff6d52e22bf99277c996671cb5543ea4c450",
        "gbdt": "52b27c56494b4e72add859dbfd1555a72531aa96586ad2a8e5001280a9c2503a",
    }
    assert CONTRACT["models_refit"] is False
    assert CONTRACT["minute_execution_read"] is False
    assert CONTRACT["database_read"] is False
    assert CONTRACT["database_write"] is False
    assert CONTRACT["runtime_action_performed"] is False


def test_benchmark_has_no_training_call_surface() -> None:
    from backend.services.position_timing import causal_timing_universe_benchmark as module

    source = inspect.getsource(module)
    assert "fit_ridge" not in source
    assert "fit_gbdt" not in source


def test_capacity_is_disclosure_not_liquidity_filter() -> None:
    fills = pd.DataFrame({"status": ["FILLED", "REJECTED", "FILLED"], "notional": [100.0, 500.0, 300.0]})
    result = _capacity(fills)

    assert result["status"] == "CAPACITY_NOT_EVALUATED_NO_AUTHORITATIVE_TURNOVER_NOTIONAL"
    assert result["market_impact_simulated"] is False
    assert result["filled_parent_order_count"] == 2
    assert result["parent_order_notional_cny"]["median"] == 200.0


def test_summary_limits_formal_family_to_all_and_large(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.services.position_timing.causal_timing_universe_benchmark._joint_block_intervals",
        lambda *_args, **_kwargs: {
            "terminal_excess_bps": {"lower": -1.0, "upper": 2.0},
            "mdd_improvement_bps": {"lower": -2.0, "upper": 3.0},
        },
    )
    rows = []
    stocks = []
    for pool in POOL_IDS:
        for policy, path in ((BH, [100.0, 101.0, 102.0]), (CYCLE5_GBDT, [100.0, 102.0, 103.0])):
            for ordinal, nav in enumerate(path):
                rows.append({
                    "pool_id": pool,
                    "policy_id": policy,
                    "ordinal": ordinal,
                    "nav": nav,
                    "expected_account_count": 1,
                    "unknown_account_count": 0,
                })
            stocks.append({
                "pool_id": pool,
                "policy_id": policy,
                "status": "COMPLETE",
                "terminal_nav_cny": nav,
                "max_drawdown": 0.0,
                "joint_success": policy == CYCLE5_GBDT,
                "average_exposure": 1.0,
                "conditional_exposure": 1.0,
                "turnover_ratio": 1.0,
                "fees_cny": 1.0,
                "model_rejected_count": 0,
                "model_unavailable_count": 0,
            })
    result = _summary(pd.DataFrame(stocks), pd.DataFrame(rows))

    assert [item["pool_id"] for item in result["formal_comparisons"]] == [ALL_PIT, LARGE]
    assert all(item["effect_evidence"] == "INCONCLUSIVE" for item in result["formal_comparisons"])
    assert result["selected_for_live"] == 0


def test_bridge_accepts_exact_parent_e0_content(tmp_path: Path) -> None:
    stock = {
        "symbol": "000001.SZ", "execution_view": "DAILY_CLOSE", "policy_id": BH,
        "terminal_nav_cny": 101.0, "max_drawdown": -0.1, "fees_cny": 2.0,
        "turnover_ratio": 1.0, "average_exposure": 0.9, "conditional_exposure": 0.9,
        "model_rejected_count": 0, "model_unavailable_count": 0,
    }
    pd.DataFrame([stock]).to_parquet(tmp_path / "stocks.parquet", index=False)
    pool = {
        "execution_view": "DAILY_CLOSE", "policy_id": BH, "pool_id": "stock_universe",
        "ordinal": 1, "nav": 101.0,
    }
    pd.DataFrame([pool]).to_parquet(tmp_path / "pool_daily.parquet", index=False)
    fill = {
        "symbol": "000001.SZ", "execution_view": "DAILY_CLOSE", "policy_id": BH,
        "execution_ordinal": 1, "authority": "COMMON_INITIAL_ENTRY", "side": "BUY",
        "status": "FILLED", "notional": 100.0,
    }
    pd.DataFrame([fill]).to_parquet(tmp_path / "fills.parquet", index=False)

    new_stock = pd.DataFrame([{**stock, "pool_id": U0_BRIDGE}])
    new_pool = pd.DataFrame([{**pool, "pool_id": U0_BRIDGE}])
    new_fill = pd.DataFrame([{**fill, "pool_id": U0_BRIDGE}])
    result = _bridge(parent_root=tmp_path, stocks=new_stock, pool_daily=new_pool, fills=new_fill)

    assert result["status"] == "EXACT"
    assert result["pool_max_abs_nav_difference_cny"] == 0.0
    assert result["fills_canonical_equal"] is True
