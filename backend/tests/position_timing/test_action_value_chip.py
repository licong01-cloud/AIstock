from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import (
    CHIP_COST_FEATURE_ORDER,
    CHIP_COST_FEATURE_SPEC_SHA256,
    CHIP_COST_INFORMATION_BLOCK,
    FEATURE_ORDER,
    FEATURE_SPEC_SHA256,
    POLICY_SHA256,
    ActionValueError,
    feature_contract,
    market_features,
    policy_sha256_for,
)
from backend.services.position_timing.action_value_chip import (
    CHIP_FEATURE,
    ChipCostAugmentedCandidate,
    ChipCostDataSource,
)
from backend.services.position_timing.action_value_incremental import (
    PIPELINE_ID,
    REQUEST_SCHEMA,
    _load_request,
    _require_matched_source_coverage,
)
from backend.services.position_timing.action_value_research import (
    EXOGENOUS_INITIAL_HOLDING_POLICY,
    EXOGENOUS_INITIAL_HOLDING_POLICY_SHA256,
)
from backend.services.position_timing.contracts import canonical_sha256


class BaseCandidate:
    def __init__(self, root: Path, calendar: pd.DatetimeIndex) -> None:
        self.root = root
        self.calendar = calendar
        self.symbols = ("000001.SZ", "000002.SZ")
        self.references = {"daily": {"path": "fixture", "sha256": "d" * 64, "size_bytes": 1}}

    def bars(self, symbol: str) -> pd.DataFrame:
        close = 10 + np.arange(len(self.calendar), dtype=float) * 0.1
        if symbol == "000300.SH":
            close += 90
        frame = pd.DataFrame(index=self.calendar)
        frame["open"] = close
        frame["high"] = close * 1.01
        frame["low"] = close * 0.99
        frame["close"] = close
        frame["volume"] = 1_000_000
        frame["factor"] = 1.0
        frame["pit_active"] = symbol != "000300.SH"
        return frame


def _write_chip_source(
    root: Path,
    *,
    invalid_day: int | None = None,
    changed_cost_day: int | None = None,
) -> pd.DatetimeIndex:
    component = root / "components" / "factor_h5_static_candidate_v2"
    component.mkdir(parents=True)
    calendar = pd.bdate_range("2024-01-02", periods=35)
    index = pd.MultiIndex.from_product(
        [calendar, ("000001.SZ", "000002.SZ")], names=["datetime", "instrument"]
    )
    chip = pd.DataFrame(index=index)
    chip["cp_cost_15pct"] = 8.0
    chip["cp_cost_50pct"] = 10.0
    chip["cp_cost_85pct"] = 12.0
    if invalid_day is not None:
        chip.loc[(calendar[invalid_day], "000001.SZ"), "cp_cost_50pct"] = 20.0
    if changed_cost_day is not None:
        chip.loc[(calendar[changed_cost_day], "000001.SZ"), "cp_cost_50pct"] = 11.0
        chip.loc[(calendar[changed_cost_day], "000001.SZ"), "cp_cost_85pct"] = 13.0
    chip.to_hdf(
        component / "cyq_perf.h5",
        key="data",
        format="table",
        data_columns=["datetime", "instrument"],
    )
    (component / "meta.json").write_text(
        json.dumps(
            {
                "schema_version": "qe_direct_factor_h5_static_v2",
                "rows_by_file": {"cyq_perf.h5": len(chip)},
            }
        ),
        encoding="utf-8",
    )
    (component / "static_factors_schema.json").write_text(
        json.dumps(
            {
                "schema_version": "qe_static_factors_121_v1",
                "columns": list(chip.columns),
            }
        ),
        encoding="utf-8",
    )
    return calendar


def test_chip_contract_is_one_lagged_field_without_changing_core() -> None:
    assert feature_contract(CHIP_COST_INFORMATION_BLOCK)[1] == CHIP_COST_FEATURE_ORDER
    assert CHIP_COST_FEATURE_ORDER[-10] == CHIP_FEATURE
    assert CHIP_COST_FEATURE_SPEC_SHA256 != FEATURE_SPEC_SHA256
    assert policy_sha256_for(CHIP_COST_INFORMATION_BLOCK) != policy_sha256_for()


def test_chip_distance_uses_same_source_day_raw_close_then_lags_one(tmp_path: Path) -> None:
    calendar = _write_chip_source(tmp_path, changed_cost_day=1)
    source = ChipCostDataSource.open(tmp_path)
    raw_close = pd.Series([10.0, 22.0, 33.0] + [10.0] * 32, index=calendar)
    observed = source.feature_for("000001.SZ", calendar, raw_close)

    assert pd.isna(observed.iloc[0])
    assert observed.iloc[1] == pytest.approx(0.0)
    assert observed.iloc[2] == pytest.approx(10_000.0)


def test_chip_invalid_cost_order_is_unavailable_and_not_forward_filled(tmp_path: Path) -> None:
    calendar = _write_chip_source(tmp_path, invalid_day=3)
    source = ChipCostDataSource.open(tmp_path)
    raw_close = pd.Series(10.0, index=calendar)
    observed = source.feature_for("000001.SZ", calendar, raw_close)

    assert source.invalid_source_rows == 1
    assert pd.isna(observed.iloc[4])
    assert observed.iloc[5] == pytest.approx(0.0)


def test_chip_source_index_validation_is_fail_closed() -> None:
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), "000001.SZ")] * 2,
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame(1.0, index=index, columns=["cp_cost_50pct"])

    with pytest.raises(ActionValueError, match="CHIP_COST_SOURCE_INDEX_INVALID"):
        ChipCostDataSource._validate_index(frame, expected_rows=2)


def test_chip_source_rejects_noncanonical_symbol(tmp_path: Path) -> None:
    _write_chip_source(tmp_path)
    path = tmp_path / "components" / "factor_h5_static_candidate_v2" / "cyq_perf.h5"
    frame = pd.read_hdf(path, key="data").reset_index()
    frame.loc[0, "instrument"] = "BAD"
    frame = frame.set_index(["datetime", "instrument"]).sort_index()
    frame.to_hdf(
        path,
        key="data",
        format="table",
        data_columns=["datetime", "instrument"],
    )

    with pytest.raises(ActionValueError, match="CHIP_COST_SYMBOL_INVALID"):
        ChipCostDataSource.open(tmp_path)


def test_chip_features_join_core_and_coverage_is_outcome_free(tmp_path: Path) -> None:
    calendar = _write_chip_source(tmp_path)
    base = BaseCandidate(tmp_path, calendar)
    candidate = ChipCostAugmentedCandidate.open(base, minimum_sessions=2)
    bars = candidate.bars("000001.SZ")
    features = market_features(
        bars,
        base.bars("000300.SH")["close"],
        information_block=CHIP_COST_INFORMATION_BLOCK,
    )

    assert tuple(features.columns) == feature_contract(CHIP_COST_INFORMATION_BLOCK)[0]
    assert features[CHIP_FEATURE].iloc[1] == pytest.approx(0.0)
    coverage = candidate.coverage(candidate.symbols)
    assert coverage["outcomes_read"] is False
    assert coverage["historical_ingestion_timestamps_verified"] is False
    assert coverage["source_available_at_policy"].startswith("T_MINUS_1")
    assert coverage["invalid_chip_source_rows"] == 0
    assert all(
        row["complete_selected_feature_sessions"] <= row["complete_core_sessions"]
        for row in coverage["coverage"].values()
    )


def test_chip_request_binds_single_matched_core_hypothesis(tmp_path: Path) -> None:
    coverage = {
        "information_block": CHIP_COST_INFORMATION_BLOCK,
        "outcomes_read": False,
        "coverage": {
            "000001.SZ": {
                "complete_core_sessions": 100,
                "complete_selected_feature_sessions": 95,
                "feature_nonmissing": {CHIP_FEATURE: 95},
            }
        },
    }
    _require_matched_source_coverage(coverage, information_block=CHIP_COST_INFORMATION_BLOCK)
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "information_block": CHIP_COST_INFORMATION_BLOCK,
        "hypothesis": "CORE_PLUS_CHIP_MEDIAN_COST_DISTANCE_LAG1_POLICY_MINUS_MATCHED_CORE_POLICY",
        "planned_trial_count": 1,
        "source_correction": "EXPLICIT_DB_SUSPENSION_UNION_V1",
        "suspension_snapshot": {"path": "snapshot.json", "sha256": "a" * 64, "size_bytes": 1},
        "initial_holding_contract": {
            "policy": EXOGENOUS_INITIAL_HOLDING_POLICY,
            "policy_sha256": EXOGENOUS_INITIAL_HOLDING_POLICY_SHA256,
        },
        "feature_contract": {
            "block_id": CHIP_COST_INFORMATION_BLOCK,
            "added_features": [CHIP_FEATURE],
            "feature_order": CHIP_COST_FEATURE_ORDER,
            "feature_spec_sha256": CHIP_COST_FEATURE_SPEC_SHA256,
            "policy_sha256": policy_sha256_for(CHIP_COST_INFORMATION_BLOCK),
        },
        "matched_core_contract": {
            "information_block": "CORE_ONLY",
            "feature_order": FEATURE_ORDER,
            "feature_spec_sha256": FEATURE_SPEC_SHA256,
            "policy_sha256": POLICY_SHA256,
        },
        "training_spec": {
            "main_comparison": "CORE_PLUS_CHIP_MEDIAN_COST_DISTANCE_LAG1_MINUS_MATCHED_CORE",
            "economic_threshold_bps": 0.0,
        },
        "population_spec": {"selection": "SHA256_SEED_CHIP_COST_SOURCE_COVERAGE_ONLY"},
    }
    request["request_sha256"] = canonical_sha256(request)
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request), encoding="utf-8")
    assert canonical_sha256(_load_request(path)) == canonical_sha256(request)
