from __future__ import annotations

from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.causal_recovery_benchmark import (
    BH,
    CAPITAL,
    CONTRACT,
    GBDT,
    LABEL_HORIZON_SESSIONS,
    RIDGE,
    _formal_report,
    _research_diagnostics,
    _small_enrollment,
    recovery_labels_from_fills,
    replay_policy,
)
from backend.services.position_timing.causal_recovery_model import (
    FEATURE_ORDER,
    LABEL,
    fit_ridge,
    predict,
    training_split,
)
from backend.services.position_timing.causal_timing_contracts import (
    FEATURE_ORDER as MARKET_FEATURE_ORDER,
)


def _bars(periods: int = 50, *, rising: bool = False) -> pd.DataFrame:
    dates = pd.date_range("2022-01-03", periods=periods, freq="B")
    close = np.linspace(10.0, 15.0, periods) if rising else np.full(periods, 10.0)
    return pd.DataFrame(
        {
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": np.full(periods, 1_000_000.0),
            "factor": np.ones(periods),
            "up_limit": close * 1.1,
            "down_limit": close * 0.9,
            "is_suspended": np.zeros(periods, dtype=bool),
            "pit_active": np.ones(periods, dtype=bool),
        },
        index=dates,
    )


def _model_rows() -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for offset in range(12):
        train = offset < 8
        decision = (
            pd.Timestamp("2022-10-03") + pd.offsets.BDay(offset)
            if train
            else pd.Timestamp("2024-03-01") + pd.offsets.BDay(offset)
        )
        records.append(
            {
                "symbol": f"000{offset:03d}.SZ",
                "event_id": f"event-{offset}",
                "decision_date": decision.date().isoformat(),
                "label_available_at": (decision + pd.offsets.BDay(21)).date().isoformat(),
                LABEL: float(offset * 2 - 5),
                **{
                    name: float(offset + feature_number / 10)
                    for feature_number, name in enumerate(FEATURE_ORDER)
                },
            }
        )
    return pd.DataFrame(records)


def test_contract_freezes_independent_account_and_disallows_oracle_access() -> None:
    assert CONTRACT["capital_cny"] == "5000000"
    assert CONTRACT["account"] == "INDEPENDENT_CASH_NO_LEVERAGE_NO_INJECTION"
    assert CONTRACT["population"]["id"] == "SMALL_LT_50B"
    assert (
        CONTRACT["recovery"]["label_event_source"]
        == "CAUSAL_RULE_CANDIDATE_FILLED_BEFORE_PARENT_MODEL_GATE"
    )
    assert CONTRACT["recovery"]["oracle_policy_access"] is False
    assert CONTRACT["market_impact_simulated"] is False
    assert CONTRACT["database_read"] is False
    assert CONTRACT["market_network_accessed"] is False


def test_temporal_split_and_train_only_preprocessing() -> None:
    frame = _model_rows()
    _, valid, audit = training_split(frame)
    assert audit == {
        "input_rows": 12,
        "train_rows": 8,
        "validation_rows": 4,
        "immature_or_outside_rows": 0,
    }
    first = fit_ridge(frame, source_sha256="1" * 64, request_sha256="2" * 64)
    changed = frame.copy()
    changed.loc[changed.index[-4:], LABEL] += 10_000
    second = fit_ridge(changed, source_sha256="1" * 64, request_sha256="2" * 64)
    assert first["coefficients"] == second["coefficients"]
    assert first["intercept"] == second["intercept"]
    assert first["validation"] != second["validation"]
    assert len(predict(first, valid)) == 4


def test_oracle_columns_are_ignored_and_frozen_features_are_required() -> None:
    frame = _model_rows()
    frame["restricted_oracle_future_value"] = np.arange(len(frame)) * 1_000_000.0
    first = fit_ridge(frame, source_sha256="3" * 64, request_sha256="4" * 64)
    frame["restricted_oracle_future_value"] *= -100
    second = fit_ridge(frame, source_sha256="3" * 64, request_sha256="4" * 64)
    assert first["coefficients"] == second["coefficients"]
    broken = frame.drop(columns=[FEATURE_ORDER[0]])
    with pytest.raises(ActionValueError, match="RECOVERY_MODEL_COLUMNS_MISSING"):
        fit_ridge(broken, source_sha256="3" * 64, request_sha256="4" * 64)


def test_small_cap_enrollment_uses_known_t_minus_one_market_cap() -> None:
    bars = _bars(5)
    ready = np.ones(5, dtype=bool)
    lagged_mv = np.asarray([np.nan, 600_000.0, 499_999.0, 700_000.0, 400_000.0])
    enrollment, audit = _small_enrollment(
        bars=bars, ready=ready, lagged_mv=lagged_mv, first=0
    )
    assert enrollment == 2
    assert audit["status"] == "ENROLLED"
    assert audit["market_cap_unknown_session_count"] == 1
    assert audit["eligible_session_count"] == 2


def test_recovery_label_compares_immediate_and_waiting_arms_from_same_cash() -> None:
    bars = _bars(50, rising=True)
    base = pd.DataFrame(0.0, index=bars.index, columns=MARKET_FEATURE_ORDER)
    fills = pd.DataFrame(
        [
            {
                "side": "SELL",
                "status": "FILLED",
                "authority": "R0_TACTICAL_20_TRIM",
                "execution_ordinal": 2,
                "notional": 1_000_000.0,
                "fee": 10.0,
            }
        ]
    )
    labels = recovery_labels_from_fills(
        symbol="000001.SZ",
        bars=bars,
        adjusted=bars,
        base_features=base,
        lagged_mv=np.full(len(bars), 100_000.0),
        fills=fills,
    )
    assert len(labels) == 20
    first = labels.iloc[0]
    assert first[LABEL] > 0
    assert first.label_available_at == str(bars.index[2 + LABEL_HORIZON_SESSIONS].date())
    assert first.recover_now_execution_ordinal == 3
    assert first.wait_execution_ordinal == 7
    # The action-value label is normalized to the frozen full 5m account,
    # not to the approximately 1m cash released by the trim.
    assert CAPITAL == Decimal("5000000")


def test_immediate_recovery_retries_after_directional_limit_block() -> None:
    bars = _bars(50, rising=True)
    bars.loc[bars.index[3], "up_limit"] = bars.loc[bars.index[3], "close"]
    base = pd.DataFrame(0.0, index=bars.index, columns=MARKET_FEATURE_ORDER)
    fills = pd.DataFrame(
        [
            {
                "side": "SELL",
                "status": "FILLED",
                "authority": "R0_TACTICAL_20_TRIM",
                "execution_ordinal": 2,
                "notional": 1_000_000.0,
                "fee": 10.0,
            }
        ]
    )
    labels = recovery_labels_from_fills(
        symbol="000001.SZ",
        bars=bars,
        adjusted=bars,
        base_features=base,
        lagged_mv=np.full(len(bars), 100_000.0),
        fills=fills,
    )
    assert labels.iloc[0].recover_now_status == "FILLED"
    assert labels.iloc[0].recover_now_execution_ordinal == 4


def test_buy_hold_replay_does_not_require_a_trim_model() -> None:
    bars = _bars(4)
    features = pd.DataFrame(0.0, index=bars.index, columns=MARKET_FEATURE_ORDER)
    days, fills, detail = replay_policy(
        symbol="000001.SZ",
        bars=bars,
        adjusted=bars,
        pattern=pd.DataFrame(index=bars.index),
        trend=pd.DataFrame(index=bars.index),
        ready=np.zeros(len(bars), dtype=bool),
        base_features=features,
        lagged_mv=np.full(len(bars), 100_000.0),
        policy_id=BH,
        enrollment_ordinal=0,
        terminal_ordinal=3,
        trim_model={},
        recovery_models={},
    )
    assert fills.authority.tolist() == ["COMMON_INITIAL_ENTRY", "TERMINAL_LIQUIDATION"]
    assert detail["terminal_cash_cny"] < float(CAPITAL)
    assert detail["terminal_virtual_units"] == 0.0
    assert not days.nav.isna().any()


def test_formal_report_normalizes_aggregate_wealth_by_five_million_accounts() -> None:
    rows: list[dict[str, object]] = []
    paths = {
        BH: [10_000_000.0, 10_100_000.0, 10_200_000.0, 10_300_000.0],
        RIDGE: [10_000_000.0, 10_120_000.0, 10_240_000.0, 10_360_000.0],
        GBDT: [10_000_000.0, 10_110_000.0, 10_230_000.0, 10_350_000.0],
    }
    for policy, values in paths.items():
        rows.extend(
            {
                "policy_id": policy,
                "ordinal": ordinal,
                "nav": nav,
                "account_count": 2,
            }
            for ordinal, nav in enumerate(values)
        )
    report = _formal_report(pd.DataFrame(rows))
    assert len(report) == 2
    assert report[0]["terminal_excess_bps"] == pytest.approx(60.0)
    assert report[1]["terminal_excess_bps"] == pytest.approx(50.0)


def test_research_diagnostics_pairs_stocks_and_completed_recovery_cycles() -> None:
    stocks = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "policy_id": policy,
                "terminal_nav_cny": terminal,
                "total_return": terminal / 5_000_000 - 1,
                "max_drawdown": drawdown,
                "average_exposure": exposure,
                "fees_cny": fees,
                "forced_recovery_count": forced,
            }
            for symbol, policy, terminal, drawdown, exposure, fees, forced in (
                ("000001.SZ", BH, 6_000_000.0, -0.30, 0.99, 100.0, 0),
                ("000002.SZ", BH, 4_500_000.0, -0.40, 0.99, 100.0, 0),
                ("000001.SZ", RIDGE, 6_100_000.0, -0.25, 0.95, 200.0, 1),
                ("000002.SZ", RIDGE, 4_400_000.0, -0.35, 0.95, 220.0, 0),
            )
        ]
    )
    fills = pd.DataFrame(
        [
            {
                "symbol": "000001.SZ", "policy_id": RIDGE,
                "side": "SELL", "status": "FILLED",
                "authority": "R0_TACTICAL_20_TRIM", "execution_ordinal": 10,
            },
            {
                "symbol": "000001.SZ", "policy_id": RIDGE,
                "side": "BUY", "status": "FILLED",
                "authority": "MODEL_RECOVERY", "execution_ordinal": 14,
            },
        ]
    )
    result = _research_diagnostics(stocks, fills)
    ridge = result["paired_stock"][0]
    assert ridge["terminal_win_rate"] == 0.5
    assert ridge["mdd_win_rate"] == 1.0
    assert ridge["dual_win_rate"] == 0.5
    waits = {row["policy_id"]: row for row in result["recovery_wait"]}
    assert waits[RIDGE]["completed_cycle_count"] == 1
    assert waits[RIDGE]["median_sessions"] == 4.0
    assert waits[RIDGE]["unmatched_filled_trim_count"] == 0
