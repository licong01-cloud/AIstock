from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[3]
FACTOR_PATH = (
    ROOT
    / "scripts/qe_alpha_candidates/sector_rotation/m_sector_participation_gap_v2.py"
)
SPEC = importlib.util.spec_from_file_location("m_sector_participation_gap_v2", FACTOR_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _inputs(days: int = 30) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.bdate_range("2025-01-02", periods=days)
    members = {101: ("000001.SZ", "000002.SZ"), 202: ("600001.SH", "600002.SH")}
    rows: list[dict[str, object]] = []
    membership_rows: list[dict[str, object]] = []
    index: list[tuple[pd.Timestamp, str]] = []
    for offset, date in enumerate(dates):
        late = offset >= 15
        values = {
            101: {"large": 30.0 if late else 10.0, "small": 10.0},
            202: {"large": 10.0 if late else 30.0, "small": 10.0},
        }
        for l2_code_id, instruments in members.items():
            for instrument in instruments:
                index.append((date, instrument))
                value = values[l2_code_id]
                rows.append(
                    {
                        "sw2_amount": 100.0,
                        "sw2_mf_buy_sm_amt": 15.0,
                        "sw2_mf_sell_sm_amt": 5.0,
                        "sw2_mf_buy_lg_amt": value["large"] + 4.0,
                        "sw2_mf_sell_lg_amt": 4.0,
                        "sw2_mf_buy_elg_amt": 3.0,
                        "sw2_mf_sell_elg_amt": 3.0,
                    }
                )
                membership_rows.append({"l2_code_id": l2_code_id})
    multi_index = pd.MultiIndex.from_tuples(index, names=["datetime", "instrument"])
    return (
        pd.DataFrame(rows, index=multi_index),
        pd.DataFrame(membership_rows, index=multi_index),
    )


def test_factor_maps_one_sector_signal_to_every_pit_member() -> None:
    sector, membership = _inputs()

    result = MODULE.calculate_factor(sector, membership)

    last_date = result.index.get_level_values("datetime").max()
    latest = result.xs(last_date, level="datetime")[MODULE.FACTOR_NAME]
    assert latest["000001.SZ"] == latest["000002.SZ"]
    assert latest["600001.SH"] == latest["600002.SH"]
    assert latest["000001.SZ"] > 0
    assert latest["600001.SH"] < 0
    assert list(result.columns) == [MODULE.FACTOR_NAME]
    assert list(result.index.names) == ["datetime", "instrument"]
    assert result.index.is_unique
    assert result[MODULE.FACTOR_NAME].dtype == np.dtype("float32")


def test_non_positive_amount_is_missing_without_zero_fill() -> None:
    sector, membership = _inputs()
    broken_date = sector.index.get_level_values("datetime").unique()[12]
    l2_101 = membership["l2_code_id"].eq(101)
    on_date = sector.index.get_level_values("datetime") == broken_date
    sector.loc[l2_101 & on_date, "sw2_amount"] = 0.0

    result = MODULE.calculate_factor(sector, membership)

    sector_101 = result.index.get_level_values("instrument").isin(
        ["000001.SZ", "000002.SZ"]
    )
    affected = result.index.get_level_values("datetime").isin(
        sector.index.get_level_values("datetime").unique()[19:30]
    )
    assert result.loc[sector_101 & affected].empty
    assert not result.loc[~sector_101 & affected].empty
    assert not result[MODULE.FACTOR_NAME].eq(0).all()


def test_inconsistent_repeated_sector_values_fail_loud() -> None:
    sector, membership = _inputs()
    first_date = sector.index.get_level_values("datetime").min()
    sector.loc[(first_date, "000002.SZ"), "sw2_amount"] = 101.0

    with pytest.raises(ValueError, match="values differ across members"):
        MODULE.calculate_factor(sector, membership)


def test_non_integer_l2_code_fails_loud() -> None:
    sector, membership = _inputs()
    membership = membership.astype({"l2_code_id": "float64"})
    membership.iloc[0, 0] = 101.5

    with pytest.raises(ValueError, match="integer category"):
        MODULE.calculate_factor(sector, membership)


def test_non_finite_l2_code_fails_loud() -> None:
    sector, membership = _inputs()
    membership = membership.astype({"l2_code_id": "float64"})
    membership.iloc[0, 0] = np.inf

    with pytest.raises(ValueError, match="integer category"):
        MODULE.calculate_factor(sector, membership)


def test_non_positive_l2_code_is_missing() -> None:
    sector, membership = _inputs()
    membership.loc[
        membership.index.get_level_values("instrument") == "000001.SZ",
        "l2_code_id",
    ] = 0

    result = MODULE.calculate_factor(sector, membership)

    assert "000001.SZ" not in result.index.get_level_values("instrument")
    assert "000002.SZ" in result.index.get_level_values("instrument")


def test_point_in_time_truncation_does_not_change_past_values() -> None:
    sector, membership = _inputs(days=35)
    cutoff = sector.index.get_level_values("datetime").unique()[27]

    full = MODULE.calculate_factor(sector, membership)
    truncated_mask = sector.index.get_level_values("datetime") <= cutoff
    truncated = MODULE.calculate_factor(
        sector.loc[truncated_mask], membership.loc[truncated_mask]
    )

    expected = full.loc[full.index.get_level_values("datetime") <= cutoff]
    pd.testing.assert_frame_equal(truncated, expected)


def test_missing_required_file_column_fails_loud() -> None:
    sector, membership = _inputs()

    with pytest.raises(ValueError, match="missing required columns"):
        MODULE.calculate_factor(sector.drop(columns=["sw2_amount"]), membership)


def test_non_finite_sector_value_fails_loud() -> None:
    sector, membership = _inputs()
    sector.iloc[0, sector.columns.get_loc("sw2_amount")] = np.inf

    with pytest.raises(ValueError, match="non-finite values"):
        MODULE.calculate_factor(sector, membership)


def test_compute_factor_rejects_worktree_output_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="outside the repository"):
        MODULE.compute_factor(
            data_dir=tmp_path,
            output_path=ROOT / "result.h5",
        )


def test_bounded_file_read_preserves_rolling_warmup(tmp_path: Path) -> None:
    sector, membership = _inputs(days=35)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    sector.to_hdf(data_dir / "sector_data.h5", key="data", mode="w")
    membership.to_parquet(data_dir / "static_factors.parquet")
    dates = sector.index.get_level_values("datetime").unique()
    start_date = dates[24]
    end_date = dates[30]
    output_path = tmp_path / "output" / "result.h5"

    MODULE.compute_factor(
        data_dir=data_dir,
        output_path=output_path,
        start_date=start_date,
        end_date=end_date,
    )

    actual = pd.read_hdf(output_path)
    full = MODULE.calculate_factor(sector, membership)
    expected = full.loc[
        full.index.get_level_values("datetime").to_series(index=full.index).between(
            start_date, end_date
        )
    ]
    pd.testing.assert_frame_equal(actual, expected)


def test_compute_factor_rejects_reversed_date_window(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="start_date must not be after"):
        MODULE.compute_factor(
            data_dir=tmp_path,
            output_path=tmp_path / "result.h5",
            start_date="2025-02-01",
            end_date="2025-01-01",
        )


def test_compute_factor_fails_before_hdf_load_when_membership_column_missing(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _, membership = _inputs()
    membership.drop(columns=["l2_code_id"]).to_parquet(
        data_dir / "static_factors.parquet"
    )

    with pytest.raises(ValueError, match="static_factors missing required columns"):
        MODULE.compute_factor(
            data_dir=data_dir,
            output_path=tmp_path / "result.h5",
        )
