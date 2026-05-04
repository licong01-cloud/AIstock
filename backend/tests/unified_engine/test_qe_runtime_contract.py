from __future__ import annotations

import pytest

from backend.services.quantevolver.runtime_contract import (
    build_qe_minute_runtime_contract,
    merge_qe_minute_runtime_contract,
    runtime_contract_missing,
)


def test_new_qe_generation_gets_default_minute_contract() -> None:
    merged = merge_qe_minute_runtime_contract(
        {"topk": 20},
        source="unit_new_generation",
        allow_default_execution_algo=True,
    )

    assert merged["topk"] == 20
    assert merged["runtime_mode"] == "minute"
    assert merged["bar_freq"] == "1m"
    assert merged["backtest_freq"] == "1min"
    assert merged["execution_algo"] == "TWAP"
    assert merged["execution_algo_params"] == {}
    assert merged["runtime_contract_version"] == "qe_minute_runtime_contract_v1"
    assert runtime_contract_missing(merged) is False


def test_historical_unknown_experiment_is_not_silently_backfilled() -> None:
    merged = merge_qe_minute_runtime_contract(
        {"topk": 20},
        source="unit_history_backfill",
        allow_default_execution_algo=False,
    )

    assert merged == {"topk": 20}
    assert runtime_contract_missing(merged) is True


def test_daily_close_price_does_not_declare_minute_contract() -> None:
    contract = build_qe_minute_runtime_contract(
        config={"execution_algo": "CLOSE_PRICE", "backtest_freq": "day"},
        source="unit_daily",
        allow_default_execution_algo=False,
    )
    assert contract is None

    with pytest.raises(ValueError, match="daily legacy"):
        build_qe_minute_runtime_contract(
            config={"execution_algo": "CLOSE_PRICE", "backtest_freq": "day"},
            source="unit_daily_required",
            require_minute=True,
        )


def test_explicit_future_minute_algo_is_supported_without_v25_hardcode() -> None:
    merged = merge_qe_minute_runtime_contract(
        {"learning_rate": 0.05},
        config={
            "execution_algo": "V26_ADAPTIVE_MINUTE",
            "execution_algo_params": {"risk_budget": 0.2},
        },
        source="unit_future_algo",
        allow_default_execution_algo=False,
    )

    assert merged["learning_rate"] == 0.05
    assert merged["backtest_freq"] == "1min"
    assert merged["execution_algo"] == "V26_ADAPTIVE_MINUTE"
    assert merged["execution_algo_params"] == {"risk_budget": 0.2}
