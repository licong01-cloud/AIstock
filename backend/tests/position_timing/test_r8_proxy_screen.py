"""Direct source and state contracts for PT-NEXT-023 proxy screens."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.position_timing import r8_proxy_screen as screen
from backend.services.position_timing.action_value_data import file_reference
from backend.services.position_timing.fundamental_screen import CandidateIdentity


def _frame(dates: pd.DatetimeIndex, values: dict[str, list[float]]) -> pd.DataFrame:
    return pd.DataFrame(values, index=dates)


def test_proxy_screens_are_lagged_nested_and_keep_unknown_distinct():
    dates = pd.bdate_range("2024-01-02", periods=6)
    daily = _frame(dates, {
        "db_total_mv": [600_000, 600_000, 600_000, 600_000, 600_000, 600_000],
        "db_turnover_rate_f": [1.0, 1.0, np.nan, 1.0, 1.0, 1.0],
        "db_pe_ttm": [20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
        "db_pb": [2.0, 2.0, 2.0, 2.0, 2.0, 2.0],
    })
    bak = _frame(dates, {
        "bb_rev_yoy": [10.0, 10.0, 10.0, -1.0, 10.0, 10.0],
        "bb_profit_yoy": [10.0] * 6,
        "bb_gpr": [20.0] * 6,
        "bb_npr": [5.0] * 6,
    })
    masks, coverage, unknown = screen.screen_masks_for_symbol(
        dates=dates,
        pit_active=np.ones(6, dtype=bool),
        feature_ready=np.ones(6, dtype=bool),
        daily=daily,
        bak=bak,
    )
    assert masks[screen.U0].tolist() == [False, True, True, True, True, True]
    assert masks[screen.U1].tolist() == [False, True, True, False, True, True]
    assert masks[screen.U2].tolist() == [False, True, True, False, False, True]
    assert unknown[screen.U1].tolist() == [True, False, False, True, False, False]
    assert coverage[screen.U2]["UNKNOWN"] == 2
    assert not np.any(masks[screen.U2] & ~masks[screen.U1])
    assert not np.any(masks[screen.U1] & ~masks[screen.U0])
    assert len(screen.SCREEN_CONTRACT_SHA256) == 64


def test_proxy_boundaries_are_inclusive_and_missing_is_not_fail():
    dates = pd.bdate_range("2024-01-02", periods=4)
    daily = _frame(dates, {
        "db_total_mv": [screen.MIN_TOTAL_MV, screen.MAX_TOTAL_MV, 600_000, 600_000],
        "db_turnover_rate_f": [screen.MIN_TURNOVER_RATE_F, screen.MAX_TURNOVER_RATE_F, 1.0, 1.0],
        "db_pe_ttm": [screen.MAX_PE_TTM, 20.0, 20.0, 20.0],
        "db_pb": [screen.MAX_PB, 2.0, 2.0, 2.0],
    })
    bak = _frame(dates, {column: [1.0, 1.0, 1.0, 1.0] for column in screen.BAK_COLUMNS})
    masks, coverage, _ = screen.screen_masks_for_symbol(
        dates=dates,
        pit_active=np.ones(4, dtype=bool),
        feature_ready=np.ones(4, dtype=bool),
        daily=daily,
        bak=bak,
    )
    assert masks[screen.U2].tolist() == [False, True, True, True]
    assert coverage[screen.U0]["UNKNOWN"] == 1
    assert coverage[screen.U0]["FAIL"] == 0


def test_reader_binds_both_hdf_files_without_financial_pit_fallback(tmp_path: Path, monkeypatch):
    root = tmp_path.resolve()
    component = root / "components"
    component.mkdir()
    dates = pd.MultiIndex.from_product(
        [pd.bdate_range("2024-01-02", periods=2), ["600001.SH"]],
        names=["datetime", "instrument"],
    )
    daily_path = component / "daily_basic.h5"
    bak_path = component / "bak_basic.h5"
    pd.DataFrame({column: 1.0 for column in screen.DAILY_COLUMNS}, index=dates).to_hdf(
        daily_path, key="data", format="table", data_columns=["datetime", "instrument"]
    )
    pd.DataFrame({column: 1.0 for column in screen.BAK_COLUMNS}, index=dates).to_hdf(
        bak_path, key="data", format="table", data_columns=["datetime", "instrument"]
    )
    daily_ref = file_reference(daily_path)
    bak_ref = file_reference(bak_path)
    inventory = {
        "files": [
            {"path": "components/daily_basic.h5", "sha256": daily_ref["sha256"], "size": daily_ref["size_bytes"]},
            {"path": "components/bak_basic.h5", "sha256": bak_ref["sha256"], "size": bak_ref["size_bytes"]},
        ]
    }
    inventory_path = root / "inventory.json"
    inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
    inventory_ref = file_reference(inventory_path)
    manifest = {
        "dataset_manifest_sha256": "a" * 64,
        "components": {
            "factor_content_manifest": {
                "path": "inventory.json",
                "sha256": inventory_ref["sha256"],
                "size": inventory_ref["size_bytes"],
            }
        },
    }
    candidate = CandidateIdentity(root, manifest, {"sha256": "m" * 64}, daily_ref)
    monkeypatch.setattr(screen, "open_r8_candidate_identity", lambda _root: candidate)

    identity = screen.open_r8_proxy_sources(root)
    frames = screen.read_proxy_frames(identity)
    audit = screen.source_audit(identity, frames)
    assert identity.bak_basic_reference["sha256"] == hashlib.sha256(bak_path.read_bytes()).hexdigest()
    assert audit["bak_semantics"] == "BAK_BASIC_DAILY_SNAPSHOT_PROXY_NOT_FINANCIAL_PIT"
    assert audit["daily_basic"]["row_count"] == 2
    assert audit["bak_basic"]["row_count"] == 2

