from __future__ import annotations

import logging

import pandas as pd

from backend.data_service import qe_data_service as qe


def _members():
    # 000004.SZ 行业迁移：801010 (2018-2020) -> 801020 (2020-)
    # 000006.SZ 已迁出且无新归属（out_date 早于查询日）
    return pd.DataFrame(
        [
            ("000004.SZ", "801010.SI", "2018-01-01", "2020-01-01"),
            ("000004.SZ", "801020.SI", "2020-01-01", None),
            ("000006.SZ", "801030.SI", "2018-01-01", "2019-06-30"),
        ],
        columns=["ts_code", "l2_code", "in_date", "out_date"],
    )


def test_asof_l2_codes_pit_migration_and_unmatched():
    trade_date = pd.Series(["2019-06-01", "2021-03-01", "2021-03-01", "2021-03-01"])
    ts_code = pd.Series(["000004.SZ", "000004.SZ", "000005.SZ", "000006.SZ"])

    got = qe._asof_l2_codes(trade_date, ts_code, _members())

    # 迁移前取旧码，迁移后取新码
    assert got[0] == "801010.SI"
    assert got[1] == "801020.SI"
    # 000005.SZ 无任何归属 -> None
    assert got[2] is None
    # 000006.SZ 区间已在 2019-06-30 结束，2021 无有效归属 -> None
    assert got[3] is None


def test_asof_l2_codes_empty_members_all_none():
    trade_date = pd.Series(["2021-01-04", "2021-01-05"])
    ts_code = pd.Series(["000001.SZ", "000002.SZ"])
    empty = pd.DataFrame(columns=["ts_code", "l2_code", "in_date", "out_date"])
    assert qe._asof_l2_codes(trade_date, ts_code, empty) == [None, None]


def test_warn_low_l2_code_coverage_is_loud(caplog):
    idx = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), f"00000{i}.SZ") for i in range(10)],
        names=["datetime", "instrument"],
    )
    # 1 matched, 9 unknown(-1) -> coverage 0.10 < 0.90
    df = pd.DataFrame({"l2_code_id": [0] + [qe.UNKNOWN_L2_CODE_ID] * 9}, index=idx).astype("int16")

    with caplog.at_level(logging.WARNING, logger="aistock.qe_data_service"):
        qe._warn_low_l2_code_coverage(df)

    assert "reason_code=sector_data_l2_code_id_low_coverage" in caplog.text
    assert "missing_count=9" in caplog.text
    assert "total_count=10" in caplog.text


def test_warn_low_l2_code_coverage_silent_when_full(caplog):
    idx = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), f"00000{i}.SZ") for i in range(10)],
        names=["datetime", "instrument"],
    )
    df = pd.DataFrame({"l2_code_id": list(range(10))}, index=idx).astype("int16")
    with caplog.at_level(logging.WARNING, logger="aistock.qe_data_service"):
        qe._warn_low_l2_code_coverage(df)
    assert "sector_data_l2_code_id_low_coverage" not in caplog.text
