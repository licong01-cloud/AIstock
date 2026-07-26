from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.quantevolver.sector_risk_overlay import (
    COMPONENT_COLUMNS,
    QESectorRiskOverlayError,
    build_sector_risk_runtime,
)


def _frames(days: int = 110, sectors: int = 5, members: int = 6) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.bdate_range("2025-01-02", periods=days)
    daily_rows = []
    sector_rows = []
    for sector_id in range(sectors):
        sector_close = 1000.0 * np.cumprod(1.0 + 0.0005 * (sector_id + 1) + 0.002 * np.sin(np.arange(days) / 7 + sector_id))
        amount = 1_000_000_000.0 * (1.0 + sector_id / 10 + 0.1 * np.cos(np.arange(days) / 9))
        flow = amount * (0.02 * np.sin(np.arange(days) / 11 + sector_id) - 0.005 * sector_id)
        for member in range(members):
            instrument = f"{sector_id:02d}{member:04d}.SZ"
            member_close = 10.0 * np.cumprod(
                1.0
                + 0.0004 * (sector_id + 1)
                + 0.003 * np.sin(np.arange(days) / (5 + member) + member)
            )
            for idx, date in enumerate(dates):
                daily_rows.append((date, instrument, member_close[idx]))
                sector_rows.append(
                    (date, instrument, sector_close[idx], amount[idx], flow[idx], sector_id)
                )
    daily = pd.DataFrame(daily_rows, columns=["datetime", "instrument", "close"]).set_index(
        ["datetime", "instrument"]
    )
    sector = pd.DataFrame(
        sector_rows,
        columns=["datetime", "instrument", "sw2_close", "sw2_amount", "sw2_mf_net_amt", "l2_code_id"],
    ).set_index(["datetime", "instrument"])
    return daily, sector


def test_builder_shifts_signal_to_next_trade_date_and_emits_all_components() -> None:
    daily, sector = _frames()
    dates = daily.index.get_level_values("datetime").unique().sort_values()
    result = build_sector_risk_runtime(
        daily,
        sector,
        output_start=str(dates[70].date()),
        output_end=str(dates[-1].date()),
        dataset_identity="fixture-dataset-v1",
    )

    assert not result.runtime.empty
    assert set(COMPONENT_COLUMNS).issubset(result.runtime.columns)
    pairs = result.runtime.loc[:, ["signal_date", "effective_trade_date"]].drop_duplicates()
    calendar = {dates[i]: dates[i + 1] for i in range(len(dates) - 1)}
    assert all(calendar[row.signal_date] == row.effective_trade_date for row in pairs.itertuples())
    mature = result.runtime.loc[result.runtime["risk_state"] != "UNMAPPED"]
    assert mature["risk_score"].notna().all()
    assert set(mature["risk_state"].unique()).issubset({"NORMAL", "CAUTION", "HIGH", "CRITICAL"})
    assert result.summary["effective_shift_trading_days"] == 1


def test_builder_is_point_in_time_stable_when_future_rows_are_removed() -> None:
    daily, sector = _frames()
    dates = daily.index.get_level_values("datetime").unique().sort_values()
    cutoff = dates[94]
    full = build_sector_risk_runtime(
        daily,
        sector,
        output_start=str(dates[70].date()),
        output_end=str(cutoff.date()),
        dataset_identity="fixture-dataset-v1",
    ).runtime
    daily_truncated = daily.loc[daily.index.get_level_values("datetime") <= cutoff]
    sector_truncated = sector.loc[sector.index.get_level_values("datetime") <= cutoff]
    truncated = build_sector_risk_runtime(
        daily_truncated,
        sector_truncated,
        output_start=str(dates[70].date()),
        output_end=str(cutoff.date()),
        dataset_identity="fixture-dataset-v1",
    ).runtime

    pd.testing.assert_frame_equal(full.reset_index(drop=True), truncated.reset_index(drop=True))


def test_builder_rejects_conflicting_repeated_sector_values() -> None:
    daily, sector = _frames(days=80)
    first_index = sector.index[0]
    sector.loc[first_index, "sw2_close"] = float(sector.loc[first_index, "sw2_close"]) + 10.0
    dates = daily.index.get_level_values("datetime").unique().sort_values()
    with pytest.raises(QESectorRiskOverlayError, match="conflicting repeated") as exc:
        build_sector_risk_runtime(
            daily,
            sector,
            output_start=str(dates[60].date()),
            output_end=str(dates[-1].date()),
            dataset_identity="fixture-dataset-v1",
        )
    assert exc.value.reason_code == "qe_sector_risk_sector_repeat_conflict"


def test_builder_rejects_low_pit_mapping_coverage() -> None:
    daily, sector = _frames(days=80)
    sector.loc[:, "l2_code_id"] = -1
    dates = daily.index.get_level_values("datetime").unique().sort_values()
    with pytest.raises(QESectorRiskOverlayError, match="coverage") as exc:
        build_sector_risk_runtime(
            daily,
            sector,
            output_start=str(dates[60].date()),
            output_end=str(dates[-1].date()),
            dataset_identity="fixture-dataset-v1",
        )
    assert exc.value.reason_code == "qe_sector_risk_mapping_coverage_low"


def test_builder_rejects_mapped_rows_with_incomplete_components() -> None:
    daily, sector = _frames(days=80)
    dates = daily.index.get_level_values("datetime").unique().sort_values()
    with pytest.raises(QESectorRiskOverlayError, match="incomplete component") as exc:
        build_sector_risk_runtime(
            daily,
            sector,
            output_start=str(dates[20].date()),
            output_end=str(dates[30].date()),
            dataset_identity="fixture-dataset-v1",
        )
    assert exc.value.reason_code == "qe_sector_risk_components_incomplete"
