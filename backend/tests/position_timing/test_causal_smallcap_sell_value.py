from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.causal_smallcap_sell_benchmark import (
    BH,
    CAPITAL,
    CONTRACT,
    GBDT,
    LABEL_HORIZON_SESSIONS,
    PARENT,
    RIDGE,
    _formal_report,
    _research_diagnostics,
    _run_policy,
    _small_enrollment,
    _standardized_label,
    sell_labels_from_fills,
)
from backend.services.position_timing.causal_smallcap_sell_model import (
    FEATURE_ORDER,
    LABEL,
    LABEL_CONTRACT_ID,
    POPULATION_ID,
    fit_ridge,
    predict,
    training_split,
    validate_model,
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
    rows: list[dict[str, object]] = []
    for offset in range(12):
        decision = (
            pd.Timestamp("2022-10-03") + pd.offsets.BDay(offset)
            if offset < 8
            else pd.Timestamp("2024-03-01") + pd.offsets.BDay(offset)
        )
        rows.append(
            {
                "symbol": f"000{offset:03d}.SZ",
                "decision_date": decision.date().isoformat(),
                "label_available_at": (decision + pd.offsets.BDay(21)).date().isoformat(),
                LABEL: float(offset - 5),
                **{name: float(offset + index / 10) for index, name in enumerate(FEATURE_ORDER)},
            }
        )
    return pd.DataFrame(rows)


def test_contract_changes_only_smallcap_sell_gate_and_keeps_fixed5() -> None:
    assert CONTRACT["capital_cny"] == "5000000"
    assert CONTRACT["population"]["id"] == POPULATION_ID
    assert CONTRACT["only_variable"] == "SELL_GATE_MODEL"
    assert CONTRACT["recovery"]["wait_sessions"] == 5
    assert CONTRACT["label"]["id"] == LABEL_CONTRACT_ID
    assert CONTRACT["label"]["parent_model_policy_access"] is False
    assert CONTRACT["label"]["oracle_policy_access"] is False
    assert CONTRACT["statistics"]["family_size"] == 8
    assert CONTRACT["market_impact_simulated"] is False
    assert CONTRACT["database_read"] is False
    assert CONTRACT["runtime_action_performed"] is False


def test_smallcap_cohort_uses_known_t_minus_one_market_cap() -> None:
    bars = _bars(5)
    enrollment, audit = _small_enrollment(
        bars=bars,
        ready=np.ones(5, dtype=bool),
        lagged_mv=np.asarray([np.nan, 600_000.0, 499_999.0, 700_000.0, 400_000.0]),
        first=0,
    )
    assert enrollment == 2
    assert audit["market_cap_unknown_session_count"] == 1
    assert audit["eligible_session_count"] == 2


def test_sell_fixed5_label_uses_five_million_and_matures_at_t21() -> None:
    bars = _bars(50)
    label = _standardized_label(
        symbol="000001.SZ", bars=bars, adjusted=bars, decision=2, sell_execution=3
    )
    assert label["status"] == "MATURED"
    assert label["buy_execution_ordinal"] == 8
    assert label["label_available_at"] == str(
        bars.index[2 + LABEL_HORIZON_SESSIONS].date()
    )
    assert label[LABEL] < 0  # flat price leaves only the extra sell/buy costs
    assert label["action_wealth_cny"] < label["hold_wealth_cny"] <= float(CAPITAL)


def test_sell_label_retries_fixed5_buy_after_directional_limit() -> None:
    bars = _bars(50)
    bars.loc[bars.index[8], "up_limit"] = bars.loc[bars.index[8], "close"]
    label = _standardized_label(
        symbol="000001.SZ", bars=bars, adjusted=bars, decision=2, sell_execution=3
    )
    assert label["status"] == "MATURED"
    assert label["buy_status"] == "FILLED"
    assert label["buy_execution_ordinal"] == 9


def test_rule_candidate_labels_ignore_unregistered_future_columns() -> None:
    bars = _bars(50, rising=True)
    features = pd.DataFrame(0.0, index=bars.index, columns=FEATURE_ORDER)
    features["oracle_future_return"] = np.arange(len(features)) * 1_000_000.0
    fills = pd.DataFrame(
        [
            {
                "side": "SELL",
                "status": "FILLED",
                "authority": "R0_TACTICAL_20_TRIM",
                "decision_ordinal": 2,
                "execution_ordinal": 3,
            }
        ]
    )
    labels = sell_labels_from_fills(
        symbol="000001.SZ",
        bars=bars,
        adjusted=bars,
        features=features,
        fills=fills,
    )
    assert len(labels) == 1
    assert "oracle_future_return" not in labels
    assert labels.iloc[0].reference_sell_execution_ordinal == 3
    assert labels.iloc[0].decision_ordinal == 2


def test_model_fit_uses_train_only_and_binds_smallcap_identity() -> None:
    frame = _model_rows()
    train, valid, audit = training_split(frame)
    assert audit["train_rows"] == 8
    assert audit["validation_rows"] == 4
    first = fit_ridge(frame, source_sha256="1" * 64, request_sha256="2" * 64)
    changed = frame.copy()
    changed.loc[changed.index[-4:], LABEL] += 10_000
    second = fit_ridge(changed, source_sha256="1" * 64, request_sha256="2" * 64)
    validate_model(first)
    assert first["training_population_id"] == POPULATION_ID
    assert first["label_contract_id"] == LABEL_CONTRACT_ID
    assert first["coefficients"] == second["coefficients"]
    assert first["intercept"] == second["intercept"]
    assert first["validation"] != second["validation"]
    assert len(predict(first, valid)) == 4
    broken = dict(first)
    broken["training_population_id"] = "OTHER"
    with pytest.raises(ActionValueError, match="SMALLCAP_SELL_MODEL_IDENTITY_DRIFT"):
        validate_model(broken)
    assert len(train) == 8


def test_policy_adapter_uses_parent_or_smallcap_model_and_relabels(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[dict[str, Any]] = []

    def fake_replay(**kwargs: Any) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        observed.append(kwargs)
        days = pd.DataFrame([{"policy_id": kwargs["policy_id"], "ordinal": 1}])
        fills = pd.DataFrame([{"policy_id": kwargs["policy_id"], "status": "FILLED"}])
        return days, fills, {"policy_id": kwargs["policy_id"], "fees_cny": 0.0}

    monkeypatch.setattr(
        "backend.services.position_timing.causal_smallcap_sell_benchmark._recovery_replay_policy",
        fake_replay,
    )
    bars = _bars(3)
    values = {
        "adjusted": bars,
        "pattern": pd.DataFrame(index=bars.index),
        "trend": pd.DataFrame(index=bars.index),
        "ready": np.zeros(len(bars), dtype=bool),
        "base": pd.DataFrame(0.0, index=bars.index, columns=FEATURE_ORDER),
        "lagged_mv": np.full(len(bars), 100_000.0),
    }
    parent_model = {"identity": "parent"}
    ridge_model = {"identity": "ridge"}
    days, fills, detail = _run_policy(
        symbol="000001.SZ",
        bars=bars,
        values=values,
        policy_id=RIDGE,
        enrollment=0,
        terminal=2,
        parent_model=parent_model,
        sell_models={"ridge": ridge_model},
    )
    assert observed[0]["trim_model"] is ridge_model
    assert observed[0]["policy_id"] == "PARENT_FIXED5_GBDT_V1"
    assert days.policy_id.tolist() == [RIDGE]
    assert fills.policy_id.tolist() == [RIDGE]
    assert detail["policy_id"] == RIDGE


def test_formal_family_contains_eight_endpoints_and_uses_five_million_denominator() -> None:
    paths = {
        BH: [10_000_000.0, 10_100_000.0, 10_200_000.0, 10_300_000.0],
        PARENT: [10_000_000.0, 10_110_000.0, 10_220_000.0, 10_340_000.0],
        RIDGE: [10_000_000.0, 10_120_000.0, 10_240_000.0, 10_360_000.0],
        GBDT: [10_000_000.0, 10_115_000.0, 10_235_000.0, 10_350_000.0],
    }
    rows = [
        {"policy_id": policy, "ordinal": ordinal, "nav": nav, "account_count": 2}
        for policy, values in paths.items()
        for ordinal, nav in enumerate(values)
    ]
    report = _formal_report(pd.DataFrame(rows))
    assert len(report) == 4
    assert sum(len(item["endpoint_states"]) for item in report) == 8
    ridge_bh = next(item for item in report if item["candidate"] == RIDGE and item["baseline"] == BH)
    ridge_parent = next(
        item for item in report if item["candidate"] == RIDGE and item["baseline"] == PARENT
    )
    assert ridge_bh["terminal_excess_bps"] == pytest.approx(60.0)
    assert ridge_parent["terminal_excess_bps"] == pytest.approx(20.0)


def test_diagnostics_distinguish_bh_and_parent_and_fixed5_cycles() -> None:
    stocks = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "policy_id": policy,
                "terminal_nav_cny": terminal,
                "max_drawdown": drawdown,
                "average_exposure": 0.98,
                "fees_cny": 100.0,
                "trim_model_rejected_count": rejected,
                "trim_model_unavailable_count": 0,
            }
            for symbol, policy, terminal, drawdown, rejected in (
                ("000001.SZ", BH, 6_000_000.0, -0.30, 0),
                ("000002.SZ", BH, 4_500_000.0, -0.40, 0),
                ("000001.SZ", PARENT, 6_050_000.0, -0.28, 3),
                ("000002.SZ", PARENT, 4_550_000.0, -0.38, 2),
                ("000001.SZ", RIDGE, 6_100_000.0, -0.25, 4),
                ("000002.SZ", RIDGE, 4_400_000.0, -0.35, 5),
                ("000001.SZ", GBDT, 6_075_000.0, -0.26, 3),
                ("000002.SZ", GBDT, 4_525_000.0, -0.36, 4),
            )
        ]
    )
    fills = pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "policy_id": RIDGE,
                "side": "SELL",
                "status": "FILLED",
                "authority": "R0_TACTICAL_20_TRIM",
                "execution_ordinal": 10,
            },
            {
                "symbol": "000001.SZ",
                "policy_id": RIDGE,
                "side": "BUY",
                "status": "FILLED",
                "authority": "FIXED_5_SESSION_RECOVERY",
                "execution_ordinal": 15,
            },
        ]
    )
    result = _research_diagnostics(stocks, fills)
    ridge = [item for item in result["paired_stock"] if item["candidate"] == RIDGE]
    assert {item["baseline"] for item in ridge} == {BH, PARENT}
    cycle = next(item for item in result["fixed5_cycles"] if item["policy_id"] == RIDGE)
    assert cycle["completed_cycle_count"] == 1
    assert cycle["median_sessions"] == 5.0
    assert cycle["minimum_sessions"] == 5
