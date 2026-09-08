from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import (
    FEATURE_SPEC_SHA256,
    FEATURE_ORDER,
    POLICY_SHA256,
    SW_L2_FEATURE_ORDER,
    SW_L2_FEATURE_SPEC_SHA256,
    SW_L2_INFORMATION_BLOCK,
    ActionValueError,
    feature_contract,
    market_features,
    policy_sha256_for,
)
from backend.services.position_timing.action_value_sector import (
    SectorAugmentedCandidate,
    SectorDataSource,
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


def _write_sector_source(root: Path, *, conflict: bool = False) -> pd.DatetimeIndex:
    component = root / "components" / "factor_h5_static_candidate_v2"
    component.mkdir(parents=True)
    calendar = pd.bdate_range("2024-01-02", periods=25)
    records = []
    for ordinal, day in enumerate(calendar):
        # 000001 changes classification after 20 sessions.  000002 keeps the
        # new sector's independent history available before the switch.
        first_sector = 0 if ordinal < 20 else 1
        records.extend(
            [
                (day, "000001.SZ", first_sector, 100.0 + ordinal if first_sector == 0 else 200.0 + ordinal),
                (day, "000002.SZ", 1, 200.0 + ordinal),
            ]
        )
    if conflict:
        day, _symbol, code, close = records[-1]
        records[-1] = (day, "000002.SZ", code, close + 1.0)
    frame = pd.DataFrame(
        records,
        columns=["datetime", "instrument", "l2_code_id", "sw2_close"],
    ).set_index(["datetime", "instrument"]).sort_index()
    frame.to_hdf(
        component / "sector_data.h5",
        key="data",
        format="table",
        data_columns=["datetime", "instrument"],
    )
    (component / "meta.json").write_text(
        json.dumps(
            {
                "schema_version": "qe_direct_factor_h5_static_v2",
                "sector_authority": "classification_pit_to_published_l2_v2",
                "rows_by_file": {"sector_data.h5": len(frame)},
            }
        ),
        encoding="utf-8",
    )
    (component / "static_factors_schema.json").write_text(
        json.dumps(
            {
                "schema_version": "qe_static_factors_121_v1",
                "columns": ["l2_code_id", "sw2_close"],
                "l2_code_id_missing": -1,
            }
        ),
        encoding="utf-8",
    )
    return calendar


def test_sw_l2_contract_is_distinct_without_changing_core_identity() -> None:
    assert feature_contract(SW_L2_INFORMATION_BLOCK)[1] == SW_L2_FEATURE_ORDER
    assert SW_L2_FEATURE_ORDER[-11:-9] == (
        "sw_l2_return_20d_bps",
        "relative_sw_l2_return_20d_bps",
    )
    assert SW_L2_FEATURE_SPEC_SHA256 != FEATURE_SPEC_SHA256
    assert policy_sha256_for(SW_L2_INFORMATION_BLOCK) != policy_sha256_for()


def test_sector_switch_uses_current_industry_independent_history(tmp_path: Path) -> None:
    calendar = _write_sector_source(tmp_path)
    source = SectorDataSource.open(tmp_path)
    observed = source.feature_for("000001.SZ", calendar)

    switch_day = calendar[20]
    expected = ((220.0 / 200.0) - 1.0) * 10_000
    assert observed.loc[switch_day] == pytest.approx(expected)
    # A stock-local shift would compare new-sector 220 to old-sector 100 and
    # create 12,000 bps; the independent sector panel must not do that.
    assert observed.loc[switch_day] != pytest.approx(12_000.0)
    assert observed.iloc[:20].isna().all()


def test_sector_features_join_core_without_using_code_as_feature(tmp_path: Path) -> None:
    calendar = _write_sector_source(tmp_path)
    base = BaseCandidate(tmp_path, calendar)
    candidate = SectorAugmentedCandidate.open(base, minimum_sessions=21)
    bars = candidate.bars("000001.SZ")
    features = market_features(
        bars,
        base.bars("000300.SH")["close"],
        information_block=SW_L2_INFORMATION_BLOCK,
    )

    assert "l2_code_id" not in features
    assert tuple(features.columns) == feature_contract(SW_L2_INFORMATION_BLOCK)[0]
    expected = features["return_20d_bps"] - features["sw_l2_return_20d_bps"]
    pd.testing.assert_series_equal(
        features["relative_sw_l2_return_20d_bps"],
        expected,
        check_names=False,
    )
    coverage = candidate.coverage(candidate.symbols)
    assert coverage["outcomes_read"] is False
    assert coverage["selection_inputs"].endswith("SECTOR_CLOSE_ONLY")
    assert coverage["information_block"] == SW_L2_INFORMATION_BLOCK
    assert all(
        row["complete_selected_feature_sessions"] <= row["complete_core_sessions"]
        for row in coverage["coverage"].values()
    )


def test_sector_day_conflict_fails_closed(tmp_path: Path) -> None:
    _write_sector_source(tmp_path, conflict=True)
    with pytest.raises(ActionValueError, match="SW_L2_SECTOR_DAY_VALUE_CONFLICT"):
        SectorDataSource.open(tmp_path)


def test_missing_sector_source_is_typed() -> None:
    dates = pd.bdate_range("2024-01-02", periods=30)
    bars = BaseCandidate(Path("."), dates).bars("000001.SZ")
    with pytest.raises(ActionValueError, match="SW_L2_FEATURE_SOURCE_MISSING"):
        market_features(
            bars,
            BaseCandidate(Path("."), dates).bars("000300.SH")["close"],
            information_block=SW_L2_INFORMATION_BLOCK,
        )


def test_sector_request_allows_declared_coverage_loss_but_binds_contract(tmp_path: Path) -> None:
    coverage = {
        "information_block": SW_L2_INFORMATION_BLOCK,
        "outcomes_read": False,
        "coverage": {
            "000001.SZ": {
                "complete_core_sessions": 100,
                "complete_selected_feature_sessions": 80,
                "feature_nonmissing": {
                    "sw_l2_return_20d_bps": 80,
                    "relative_sw_l2_return_20d_bps": 80,
                },
            }
        },
    }
    _require_matched_source_coverage(coverage, information_block=SW_L2_INFORMATION_BLOCK)

    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "information_block": SW_L2_INFORMATION_BLOCK,
        "hypothesis": "CORE_PLUS_SW_L2_RELATIVE_MOMENTUM_POLICY_MINUS_MATCHED_CORE_POLICY",
        "planned_trial_count": 1,
        "source_correction": "EXPLICIT_DB_SUSPENSION_UNION_V1",
        "suspension_snapshot": {"path": "snapshot.json", "sha256": "a" * 64, "size_bytes": 1},
        "initial_holding_contract": {
            "policy": EXOGENOUS_INITIAL_HOLDING_POLICY,
            "policy_sha256": EXOGENOUS_INITIAL_HOLDING_POLICY_SHA256,
        },
        "feature_contract": {
            "block_id": SW_L2_INFORMATION_BLOCK,
            "added_features": ["sw_l2_return_20d_bps", "relative_sw_l2_return_20d_bps"],
            "feature_order": SW_L2_FEATURE_ORDER,
            "feature_spec_sha256": SW_L2_FEATURE_SPEC_SHA256,
            "policy_sha256": policy_sha256_for(SW_L2_INFORMATION_BLOCK),
        },
        "matched_core_contract": {
            "information_block": "CORE_ONLY",
            "feature_order": FEATURE_ORDER,
            "feature_spec_sha256": FEATURE_SPEC_SHA256,
            "policy_sha256": POLICY_SHA256,
        },
        "training_spec": {
            "main_comparison": "CORE_PLUS_SW_L2_RELATIVE_MOMENTUM_MINUS_MATCHED_CORE",
            "economic_threshold_bps": 0.0,
        },
        "population_spec": {"selection": "SHA256_SEED_SECTOR_SOURCE_COVERAGE_ONLY"},
    }
    request["request_sha256"] = canonical_sha256(request)
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request), encoding="utf-8")
    assert canonical_sha256(_load_request(path)) == canonical_sha256(request)
    request["training_spec"]["main_comparison"] = "RESULT_SELECTED_BRANCH"
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    path.write_text(json.dumps(request), encoding="utf-8")
    with pytest.raises(ActionValueError, match="INCREMENT_REQUEST_IDENTITY_MISMATCH"):
        _load_request(path)
