from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.price_range_inference import (
    score_price_range_bundle,
)
from backend.services.advisory_model_first.price_range_regulatory import (
    resolve_regulatory_price_range,
)
from backend.services.advisory_model_first.price_range_runtime_bundle import (
    LoadedAdvisoryPriceRangeBundle,
)
from backend.services.advisory_model_first.realtime_feature_source import (
    PriceRangeRealtimeContext,
    _target_raw_price_multiplier,
)


class _Head:
    def __init__(self, values):
        self.values = np.asarray(values, dtype=float)

    def feature_name(self):
        return list(MODEL_FEATURE_COLUMNS)

    def predict(self, matrix):
        return self.values[: len(matrix)]


def _features() -> pd.DataFrame:
    frame = pd.DataFrame(
        [{column: 0.0 for column in MODEL_FEATURE_COLUMNS} for _ in range(2)]
    )
    frame["instrument"] = ["000001.SZ", "000002.SZ"]
    frame["l2_code_id"] = [1, 2]
    return frame


def _bundle() -> LoadedAdvisoryPriceRangeBundle:
    return LoadedAdvisoryPriceRangeBundle(
        price_range_bundle_id="b" * 64,
        bundle_path=Path("/model/price"),
        manifest={"request_id": "advprreq_runtime"},
        feature_schema={"categorical_vocabulary": {"l2_code_id": [1, 2]}},
        models={
            "entry_executable_probability": _Head([0.62, 0.41]),
            "entry_gap_q10": _Head([-0.01, -0.02]),
            "entry_gap_q50": _Head([0.0, 0.0]),
            "entry_gap_q90": _Head([0.01, 0.02]),
        },
    )


def _context(symbol: str, *, board_type: str = "MAIN", target_is_st: bool = False):
    return PriceRangeRealtimeContext(
        symbol=symbol,
        decision_raw_close=10.0,
        decision_price_trade_date=date(2026, 7, 20),
        decision_price_source="market.kline_daily_raw.close_li",
        price_unit_divisor=1000.0,
        target_raw_price_multiplier=1.0,
        corporate_action_source="market.dividend:target_ex_date:no_visible_action",
        board_type=board_type,
        list_date=date(2020, 1, 1),
        listed_trading_days=99,
        target_is_st=target_is_st,
        tick_size=0.01,
    )


def _outcome(symbol: str):
    return {
        "symbol": symbol,
        "holding_period": {
            "mode_days": 5,
            "range_low_days": 3,
            "range_high_days": 10,
        },
        "horizons": [
            {
                "horizon_days": 5,
                "path_mfe_q50": 0.10,
                "path_mfe_q90": 0.20,
                "path_mae_loss_q50": 0.04,
                "path_mae_loss_q90": 0.12,
            }
        ],
    }


def test_price_range_projects_real_heads_m3_paths_and_hard_stop() -> None:
    contexts = {symbol: _context(symbol) for symbol in ("000001.SZ", "000002.SZ")}
    result = score_price_range_bundle(
        _bundle(),
        _features(),
        contexts=contexts,
        context_unavailable=(),
        outcome_candidates=[_outcome("000001.SZ"), _outcome("000002.SZ")],
        review_policy={
            "stop_loss_bps": 800,
            "take_profit_bps": 1800,
            "trailing_stop_bps": 700,
            "take_profit_mode": "trailing",
        },
        review_policy_sha256="a" * 64,
        target_trade_date=date(2026, 7, 21),
    )

    first = result[0]
    assert first["status"] == "EXPERIMENTAL_SHADOW"
    assert first["entry_price"] == {
        "condition": "ENTRY_EXECUTABLE",
        "low": 9.9,
        "mid": 10.0,
        "high": 10.1,
    }
    assert first["take_profit_price"]["horizon_trade_days"] == 5
    assert first["stop_loss_price"]["hard_stop_price"] == 9.2
    assert first["stop_loss_price"]["low"] >= 9.2
    assert first["protective_price"]["status"] == "AVAILABLE_CONDITIONAL_ON_POLICY_ACTIVATION"
    assert first["regulatory_price_range"]["rule_id"] == "MAIN_10PCT_V1"


def test_context_failure_is_visible_per_candidate_without_removing_group() -> None:
    result = score_price_range_bundle(
        _bundle(),
        _features(),
        contexts={"000001.SZ": _context("000001.SZ")},
        context_unavailable=(
            {
                "symbol": "000002.SZ",
                "reason_code": "ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE",
                "message": "missing target ex-date refresh",
            },
        ),
        outcome_candidates=[_outcome("000001.SZ"), _outcome("000002.SZ")],
        review_policy={
            "stop_loss_bps": 800,
            "take_profit_bps": 1800,
            "trailing_stop_bps": 700,
            "take_profit_mode": "trailing",
        },
        review_policy_sha256="a" * 64,
        target_trade_date=date(2026, 7, 21),
    )
    assert [item["symbol"] for item in result] == ["000001.SZ", "000002.SZ"]
    assert result[1]["status"] == "PRICE_RANGE_UNAVAILABLE"
    assert result[1]["reason_code"] == "ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE"


def test_regulatory_rules_distinguish_st_and_no_limit_ipo() -> None:
    st = resolve_regulatory_price_range(
        _context("000001.SZ", target_is_st=True),
        target_trade_date=date(2026, 7, 21),
    )
    assert (st.low, st.high, st.rule_id) == (9.5, 10.5, "MAIN_ST_5PCT_V1")

    ipo = _context("688001.SH", board_type="STAR")
    ipo = PriceRangeRealtimeContext(**{**ipo.__dict__, "listed_trading_days": 3})
    result = resolve_regulatory_price_range(ipo, target_trade_date=date(2026, 7, 21))
    assert result.status == "NO_DAILY_LIMIT"
    assert result.low is None and result.high is None


def test_regulatory_rules_keep_pre_reform_chinext_legacy_ipo_limits() -> None:
    context = _context("300001.SZ", board_type="CHINEXT")
    context = PriceRangeRealtimeContext(
        **{
            **context.__dict__,
            "list_date": date(2020, 8, 20),
            "listed_trading_days": 1,
        }
    )
    result = resolve_regulatory_price_range(
        context,
        target_trade_date=date(2020, 8, 20),
    )
    assert result.status == "LIMITED"
    assert (result.low, result.high) == (6.4, 14.4)
    assert result.rule_id == "LEGACY_CHINEXT_IPO_FIRST_DAY_44_36_V1"


def test_regulatory_rules_enforce_minimum_tick_distance() -> None:
    context = _context("000001.SZ", target_is_st=True)
    context = PriceRangeRealtimeContext(
        **{**context.__dict__, "decision_raw_close": 0.1}
    )
    result = resolve_regulatory_price_range(
        context,
        target_trade_date=date(2026, 7, 21),
    )
    assert (result.low, result.high) == (0.09, 0.11)


def test_corporate_action_multiplier_uses_tax_cash_and_share_distribution() -> None:
    row = (
        "000001.SZ",
        date(2025, 12, 31),
        date(2026, 7, 1),
        "实施",
        0.1,
        0.04,
        0.06,
        0.16,
        0.2,
        date(2026, 7, 15),
    )
    multiplier, source = _target_raw_price_multiplier(
        symbol="000001.SZ",
        decision_raw_close=10.0,
        rows=[row],
        decision_as_of_trade_date=date(2026, 7, 20),
    )
    assert np.isclose(multiplier, 9.8 / 11.0)
    assert source.endswith("implemented_action")


def test_corporate_action_multiplier_ignores_actions_not_known_at_decision() -> None:
    row = (
        "000001.SZ",
        date(2025, 12, 31),
        date(2026, 7, 1),
        "实施",
        None,
        None,
        None,
        None,
        None,
        date(2026, 7, 21),
    )
    multiplier, source = _target_raw_price_multiplier(
        symbol="000001.SZ",
        decision_raw_close=10.0,
        rows=[row],
        decision_as_of_trade_date=date(2026, 7, 20),
    )
    assert multiplier == 1.0
    assert source.endswith("no_visible_action")


def test_corporate_action_multiplier_rejects_missing_implementation_knowledge_date() -> None:
    row = (
        "000001.SZ",
        date(2025, 12, 31),
        date(2026, 7, 1),
        "实施",
        0.1,
        0.04,
        0.06,
        None,
        0.2,
        None,
    )
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        _target_raw_price_multiplier(
            symbol="000001.SZ",
            decision_raw_close=10.0,
            rows=[row],
            decision_as_of_trade_date=date(2026, 7, 20),
        )
    assert exc_info.value.reason_code == "ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE"
