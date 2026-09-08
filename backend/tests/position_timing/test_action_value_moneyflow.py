from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.data_service.moneyflow_contract import (
    MONEYFLOW_FACTOR_COLUMNS,
    derive_moneyflow_factors,
)
from backend.services.position_timing.action_value import (
    FEATURE_ORDER,
    FEATURE_SPEC_SHA256,
    POLICY_SHA256,
    MONEYFLOW_FEATURE_ORDER,
    MONEYFLOW_FEATURE_SPEC_SHA256,
    MONEYFLOW_INFORMATION_BLOCK,
    feature_contract,
    market_features,
    policy_sha256_for,
)
from backend.services.position_timing.action_value_incremental import (
    PIPELINE_ID,
    REQUEST_SCHEMA,
    _load_request,
    _require_matched_source_coverage,
)
from backend.services.position_timing.action_value_moneyflow import (
    MONEYFLOW_FEATURE,
    MoneyflowAugmentedCandidate,
    MoneyflowDataSource,
)
from backend.services.position_timing.contracts import canonical_sha256


class BaseCandidate:
    def __init__(self, root: Path, calendar: pd.DatetimeIndex) -> None:
        self.root = root
        self.calendar = calendar
        self.symbols = ("000001.SZ", "000002.SZ")
        self.references = {"daily": {"path": "fixture", "sha256": "d" * 64, "size_bytes": 1}}

    def bars(self, symbol: str) -> pd.DataFrame:
        trend = 10 + np.arange(len(self.calendar), dtype=float) * 0.1
        if symbol == "000300.SH":
            trend += 90
        frame = pd.DataFrame(index=self.calendar)
        frame["open"] = trend
        frame["high"] = trend * 1.01
        frame["low"] = trend * 0.99
        frame["close"] = trend
        frame["volume"] = 1_000_000
        frame["factor"] = 1.0
        frame["pit_active"] = symbol != "000300.SH"
        return frame


def _write_moneyflow_source(
    root: Path,
    *,
    missing_day: int | None = None,
    spike_day: int | None = None,
) -> pd.DatetimeIndex:
    component = root / "components" / "factor_h5_static_candidate_v2"
    component.mkdir(parents=True)
    calendar = pd.bdate_range("2024-01-02", periods=35)
    index = pd.MultiIndex.from_product(
        [calendar, ("000001.SZ", "000002.SZ")], names=["datetime", "instrument"]
    )
    moneyflow = pd.DataFrame(0.0, index=index, columns=MONEYFLOW_FACTOR_COLUMNS)
    moneyflow["mf_lg_buy_amt"] = 10.0
    moneyflow["mf_elg_buy_amt"] = 5.0
    if missing_day is not None:
        moneyflow.loc[(calendar[missing_day], "000001.SZ"), "mf_lg_buy_amt"] = np.nan
    if spike_day is not None:
        moneyflow.loc[(calendar[spike_day], "000001.SZ"), "mf_lg_buy_amt"] = 1_010.0
    daily = pd.DataFrame(index=index)
    daily["amount"] = 100.0
    daily["volume"] = 1_000.0
    daily["factor"] = 1.0
    moneyflow.to_hdf(
        component / "moneyflow.h5",
        key="data",
        format="table",
        data_columns=["datetime", "instrument"],
    )
    daily.to_hdf(
        component / "daily_pv.h5",
        key="data",
        format="table",
        data_columns=["datetime", "instrument"],
    )
    (component / "meta.json").write_text(
        json.dumps(
            {
                "schema_version": "qe_direct_factor_h5_static_v2",
                "rows_by_file": {
                    "moneyflow.h5": len(moneyflow),
                    "daily_pv.h5": len(daily),
                },
            }
        ),
        encoding="utf-8",
    )
    (component / "static_factors_schema.json").write_text(
        json.dumps(
            {
                "schema_version": "qe_static_factors_121_v1",
                # Production static schema declares moneyflow factor fields;
                # daily_pv raw columns are validated from their own HDF table.
                "columns": list(MONEYFLOW_FACTOR_COLUMNS),
            }
        ),
        encoding="utf-8",
    )
    return calendar


def test_moneyflow_contract_is_one_lagged_feature_without_changing_core() -> None:
    assert feature_contract(MONEYFLOW_INFORMATION_BLOCK)[1] == MONEYFLOW_FEATURE_ORDER
    assert MONEYFLOW_FEATURE_ORDER[-10] == MONEYFLOW_FEATURE
    assert MONEYFLOW_FEATURE_SPEC_SHA256 != FEATURE_SPEC_SHA256
    assert policy_sha256_for(MONEYFLOW_INFORMATION_BLOCK) != policy_sha256_for()


def test_moneyflow_uses_complete_global_five_day_window_and_lag_one(tmp_path: Path) -> None:
    calendar = _write_moneyflow_source(tmp_path)
    source = MoneyflowDataSource.open(tmp_path)
    observed = source.feature_for("000001.SZ", calendar)
    component = tmp_path / "components" / "factor_h5_static_candidate_v2"
    canonical = derive_moneyflow_factors(
        pd.read_hdf(component / "moneyflow.h5", key="data"),
        pd.read_hdf(component / "daily_pv.h5", key="data"),
    )
    expected = (
        canonical.xs("000001.SZ", level="instrument")["mf_main_net_amt_ratio_5d"]
        .reindex(calendar)
        .shift(1)
        * 10_000
    )

    assert observed.iloc[:5].isna().all()
    assert observed.iloc[5] == pytest.approx(1_500.0)
    pd.testing.assert_series_equal(observed, expected.rename(MONEYFLOW_FEATURE))


def test_moneyflow_missing_session_is_not_forward_filled(tmp_path: Path) -> None:
    calendar = _write_moneyflow_source(tmp_path, missing_day=3)
    source = MoneyflowDataSource.open(tmp_path)
    observed = source.feature_for("000001.SZ", calendar)

    assert observed.iloc[5:9].isna().all()
    assert observed.iloc[9] == pytest.approx(1_500.0)


def test_moneyflow_same_day_value_enters_only_next_decision(tmp_path: Path) -> None:
    calendar = _write_moneyflow_source(tmp_path, spike_day=5)
    source = MoneyflowDataSource.open(tmp_path)
    observed = source.feature_for("000001.SZ", calendar)

    assert observed.iloc[5] == pytest.approx(1_500.0)
    assert observed.iloc[6] == pytest.approx(21_500.0)


def test_moneyflow_features_join_core_and_source_coverage_is_outcome_free(tmp_path: Path) -> None:
    calendar = _write_moneyflow_source(tmp_path)
    base = BaseCandidate(tmp_path, calendar)
    candidate = MoneyflowAugmentedCandidate.open(base, minimum_sessions=6)
    bars = candidate.bars("000001.SZ")
    features = market_features(
        bars,
        base.bars("000300.SH")["close"],
        information_block=MONEYFLOW_INFORMATION_BLOCK,
    )

    assert tuple(features.columns) == feature_contract(MONEYFLOW_INFORMATION_BLOCK)[0]
    assert features[MONEYFLOW_FEATURE].iloc[5] == pytest.approx(1_500.0)
    coverage = candidate.coverage(candidate.symbols)
    assert coverage["outcomes_read"] is False
    assert coverage["historical_ingestion_timestamps_verified"] is False
    assert coverage["source_available_at_policy"].startswith("T_MINUS_1")
    assert coverage["moneyflow_unit_contract"]["factor_amount_unit"] == "cny"
    assert all(
        row["complete_selected_feature_sessions"] <= row["complete_core_sessions"]
        for row in coverage["coverage"].values()
    )


def test_moneyflow_request_binds_single_matched_core_hypothesis(tmp_path: Path) -> None:
    coverage = {
        "information_block": MONEYFLOW_INFORMATION_BLOCK,
        "outcomes_read": False,
        "coverage": {
            "000001.SZ": {
                "complete_core_sessions": 100,
                "complete_selected_feature_sessions": 95,
                "feature_nonmissing": {MONEYFLOW_FEATURE: 95},
            }
        },
    }
    _require_matched_source_coverage(coverage, information_block=MONEYFLOW_INFORMATION_BLOCK)
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "information_block": MONEYFLOW_INFORMATION_BLOCK,
        "hypothesis": "CORE_PLUS_MAIN_NET_FLOW_RATIO_5D_LAG1_POLICY_MINUS_MATCHED_CORE_POLICY",
        "planned_trial_count": 1,
        "feature_contract": {
            "block_id": MONEYFLOW_INFORMATION_BLOCK,
            "added_features": [MONEYFLOW_FEATURE],
            "feature_order": MONEYFLOW_FEATURE_ORDER,
            "feature_spec_sha256": MONEYFLOW_FEATURE_SPEC_SHA256,
            "policy_sha256": policy_sha256_for(MONEYFLOW_INFORMATION_BLOCK),
        },
        "matched_core_contract": {
            "information_block": "CORE_ONLY",
            "feature_order": FEATURE_ORDER,
            "feature_spec_sha256": FEATURE_SPEC_SHA256,
            "policy_sha256": POLICY_SHA256,
        },
        "training_spec": {
            "main_comparison": "CORE_PLUS_MAIN_NET_FLOW_RATIO_5D_LAG1_MINUS_MATCHED_CORE",
            "economic_threshold_bps": 0.0,
        },
        "population_spec": {"selection": "SHA256_SEED_MONEYFLOW_SOURCE_COVERAGE_ONLY"},
    }
    request["request_sha256"] = canonical_sha256(request)
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request), encoding="utf-8")
    assert canonical_sha256(_load_request(path)) == canonical_sha256(request)
