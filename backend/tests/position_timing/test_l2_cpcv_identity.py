from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.position_timing.learnability_pipeline import build_l2_cpcv_paths


def _cpcv_rows() -> pd.DataFrame:
    calendar = pd.bdate_range("2018-08-01", periods=1700)
    cohorts = calendar[::20][:80]
    records = []
    for cohort in cohorts:
        position = calendar.get_loc(cohort)
        for sign in (-1.0, 1.0):
            records.append(
                {
                    "entry_decision_date": cohort,
                    "entry_trade_date": calendar[position + 1],
                    "effective_terminal_trade_date": calendar[position + 25],
                    "target_available": True,
                    "full_exit_incremental_net_value_bps": sign,
                }
            )
    frame = pd.DataFrame(records)
    frame.attrs["trading_calendar"] = [value.isoformat() for value in calendar]
    return frame


def test_l2_cpcv_has_28_paths_and_seven_validation_memberships() -> None:
    rows = _cpcv_rows()
    paths = build_l2_cpcv_paths(rows, request_sha256="1" * 64)
    assert len(paths) == 28
    assert all(path["status"] == "READY" for path in paths)
    memberships: dict[str, int] = {}
    for path in paths:
        assert not set(path["train_dates"]) & set(path["validation_dates"])
        for value in path["validation_dates"]:
            memberships[value] = memberships.get(value, 0) + 1
    assert set(memberships.values()) == {7}
    assert len(memberships) == rows["entry_decision_date"].nunique()


def test_l2_cpcv_identity_is_deterministic() -> None:
    rows = _cpcv_rows()
    first = build_l2_cpcv_paths(rows, request_sha256="2" * 64)
    second = build_l2_cpcv_paths(rows.sample(frac=1.0, random_state=7), request_sha256="2" * 64)
    assert [path["path_id"] for path in first] == [path["path_id"] for path in second]
    assert np.array_equal(
        [path["validation_blocks"] for path in first],
        [path["validation_blocks"] for path in second],
    )
