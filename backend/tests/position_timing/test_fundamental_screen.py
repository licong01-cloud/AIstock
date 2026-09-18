"""Direct contracts for the PT-NEXT-022 PIT screen adapter."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.fundamental_screen import (
    FINANCIAL_SCHEMA,
    SCREEN_CONTRACT_SHA256,
    first_enrollment_ordinal,
    open_financial_pit_source,
    p1_mask_for_symbol,
    unavailable_financial_groups,
)


def test_p1_uses_previous_global_session_and_inclusive_boundaries():
    dates = pd.bdate_range("2024-01-02", periods=5)
    market_cap = pd.Series([499_999, 500_000, 5_000_000, 5_000_001, 600_000], index=dates)
    mask, counts = p1_mask_for_symbol(
        market_cap,
        dates=dates,
        pit_active=np.ones(5, dtype=bool),
        feature_ready=np.ones(5, dtype=bool),
    )
    assert mask.tolist() == [False, False, True, True, False]
    assert counts["market_cap_unknown"] == 1
    assert counts["pass"] == 2
    assert first_enrollment_ordinal(mask, final_decision_ordinal=5) == 2
    assert len(SCREEN_CONTRACT_SHA256) == 64


def test_p1_does_not_fill_missing_market_cap_or_ignore_pit_and_features():
    dates = pd.bdate_range("2024-01-02", periods=4)
    market_cap = pd.Series([600_000, np.nan, 700_000, 800_000], index=dates)
    mask, counts = p1_mask_for_symbol(
        market_cap,
        dates=dates,
        pit_active=np.array([True, True, True, False]),
        feature_ready=np.array([True, True, False, True]),
    )
    assert mask.tolist() == [False, True, False, False]
    assert counts["market_cap_unknown"] == 2
    assert first_enrollment_ordinal(mask, final_decision_ordinal=1) is None


def _write_financial_source(root: Path, frame: pd.DataFrame, *, candidate: str) -> Path:
    data = root / "financial.parquet"
    frame.to_parquet(data, index=False)
    raw = data.read_bytes()
    manifest = {
        "schema": FINANCIAL_SCHEMA,
        "candidate_dataset_sha256": candidate,
        "data_file": {
            "path": data.name,
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size_bytes": len(raw),
        },
    }
    path = root / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def test_financial_reader_is_typed_hash_bound_and_has_no_fallback(tmp_path: Path):
    required = {
        "symbol": ["600001.SH"],
        "report_end_date": ["2023-12-31"],
        "statement_scope": ["CONSOLIDATED"],
        "source_record_id": ["row-1"],
        "revision_id": ["v1"],
        "row_sha256": ["a" * 64],
        "available_at": ["2024-03-30T12:00:00+08:00"],
        "availability_precision": ["TIMESTAMP"],
        "quarterly_deducted_parent_profit_cny": [1.0],
        "quarterly_revenue_cny": [2.0],
        "quarterly_parent_profit_cny": [1.0],
        "parent_equity_cny": [10.0],
        "quarterly_operating_cash_flow_cny": [1.0],
        "financial_classification": ["NON_FINANCIAL"],
    }
    manifest = _write_financial_source(tmp_path, pd.DataFrame(required), candidate="c" * 64)
    source = open_financial_pit_source(manifest, expected_candidate_sha256="c" * 64)
    assert len(source.frame) == 1
    assert unavailable_financial_groups()["P2"]["status"] == "INPUT_UNAVAILABLE"

    payload = json.loads(manifest.read_text())
    payload["candidate_dataset_sha256"] = "d" * 64
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ActionValueError, match="FUNDAMENTAL_FINANCIAL_CANDIDATE_DRIFT"):
        open_financial_pit_source(manifest, expected_candidate_sha256="c" * 64)

