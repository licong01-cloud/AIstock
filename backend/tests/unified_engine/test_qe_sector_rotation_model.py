from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.quantevolver.sector_rotation_model import (
    REAL_SECTOR_MODEL_CLASSIFICATION,
    SectorModelConfig,
    aggregate_factor_to_sector,
    build_sector_base,
    engineer_sector_panel,
    train_sector_model_suite,
)


def _static_frame(periods: int = 48) -> pd.DataFrame:
    records = []
    dates = pd.date_range("2025-01-02", periods=periods, freq="B")
    for day_index, date in enumerate(dates):
        for sector in range(1, 4):
            close = 100.0 + day_index * (0.2 + 0.1 * sector) + np.sin(day_index / 3 + sector)
            for stock in range(4):
                records.append(
                    {
                        "datetime": date,
                        "instrument": f"{sector}{stock:05d}.SZ",
                        "l2_code_id": sector,
                        "sw2_close": close,
                        "sw2_amount": 1e8 * (1 + sector / 10 + day_index / 100),
                        "sw2_vol": 1e6 * (1 + sector / 10),
                        "sw2_mf_net_amt": 1e6 * (sector - 2) + day_index * 1e4,
                        "sw2_mf_net_vol": 1e4 * (sector - 2),
                        "sw2_total_mv": 1e11 * sector,
                        "sw2_pb": 1.0 + sector / 10,
                        "sw2_pe": 10.0 + sector,
                    }
                )
    return pd.DataFrame.from_records(records)


def _factor_frame(static: pd.DataFrame) -> pd.DataFrame:
    frame = static.loc[:, ["datetime", "instrument", "l2_code_id"]].copy()
    stock_number = frame["instrument"].str.slice(1, 2).astype(int)
    frame["value"] = frame["l2_code_id"] * 0.5 + stock_number * 0.1
    return frame.loc[:, ["datetime", "instrument", "value"]]


def test_sector_base_and_factor_aggregation_preserve_pit_identity_and_audit():
    static = _static_frame(10)
    conflicting = static.iloc[[0]].copy()
    conflicting["sw2_close"] += 1.0
    static = pd.concat([static, conflicting], ignore_index=True)
    built = build_sector_base(static)

    assert built.audit["sector_count"] == 3
    assert built.audit["repeated_sector_value_conflicts"]["sw2_close"] == 1
    aggregate, audit = aggregate_factor_to_sector(
        _factor_frame(static), built.membership, factor_name="synthetic_factor"
    )
    assert audit["matched_sector_days"] == 30
    assert {
        "synthetic_factor__mean_rank",
        "synthetic_factor__rank_dispersion",
        "synthetic_factor__top_quintile_share",
    }.issubset(aggregate.columns)


def test_sector_base_accepts_parquet_style_datetime_instrument_multiindex():
    static = _static_frame(4).set_index(["datetime", "instrument"])
    built = build_sector_base(static)
    assert built.audit["membership_rows"] == 48
    assert built.audit["sector_day_rows"] == 12


def test_negative_sector_sentinel_is_audited_and_excluded():
    static = _static_frame(4)
    sentinel = static.iloc[[0]].copy()
    sentinel["instrument"] = "999999.SZ"
    sentinel["l2_code_id"] = -1
    sentinel.loc[:, [column for column in sentinel if column.startswith("sw2_")]] = np.nan
    built = build_sector_base(pd.concat([static, sentinel], ignore_index=True))
    assert built.audit["negative_sector_id_rows"] == 1
    assert -1 not in set(built.membership["l2_code_id"])
    assert -1 not in set(built.sector_base["l2_code_id"])


def test_engineered_target_is_t_plus_1_to_t_plus_h_plus_1_and_features_are_causal():
    static = _static_frame(20)
    built = build_sector_base(static)
    aggregate, _ = aggregate_factor_to_sector(
        _factor_frame(static), built.membership, factor_name="synthetic_factor"
    )
    panel, features, audit = engineer_sector_panel(
        built.sector_base, {"synthetic_factor": aggregate}, horizon=3
    )
    sector = panel.loc[panel["l2_code_id"].eq(1)].sort_values("datetime").reset_index(drop=True)
    expected = sector.loc[4, "sw2_close"] / sector.loc[1, "sw2_close"] - 1.0
    assert np.isclose(sector.loc[0, "target_return"], expected)
    assert sector.loc[0, "entry_date"] == sector.loc[1, "datetime"]
    assert sector.loc[0, "label_end_date"] == sector.loc[4, "datetime"]
    assert "target_return" not in features
    assert "target_rank" not in features
    assert "sw2_amount" not in features
    assert "sw2_total_mv" not in features
    assert "sector_log_amount" not in features
    assert "sw2_pb__cs_rank" in features
    assert "member_count__cs_rank" in features
    assert audit["feature_policy"].startswith("causal_scale_stable")
    assert audit["horizon"] == 3


def test_target_uses_global_trading_calendar_when_a_sector_day_is_missing():
    static = _static_frame(12)
    dates = sorted(static["datetime"].unique())
    static = static.loc[
        ~(static["l2_code_id"].eq(2) & static["datetime"].eq(dates[3]))
    ]
    built = build_sector_base(static)
    panel, _, audit = engineer_sector_panel(built.sector_base, {}, horizon=2)
    sector = panel.loc[panel["l2_code_id"].eq(2)].sort_values("datetime").reset_index(drop=True)
    assert len(sector) == len(dates)
    assert pd.isna(sector.loc[3, "sw2_close"])
    assert sector.loc[0, "label_end_date"] == pd.Timestamp(dates[3])
    assert pd.isna(sector.loc[0, "target_return"])
    assert audit["missing_sector_calendar_rows"] >= 1


def test_real_sector_model_suite_emits_complete_multimodel_oos_scores():
    pytest.importorskip("lightgbm")
    static = _static_frame(64)
    built = build_sector_base(static)
    aggregate, _ = aggregate_factor_to_sector(
        _factor_frame(static), built.membership, factor_name="synthetic_factor"
    )
    panel, features, _ = engineer_sector_panel(
        built.sector_base, {"synthetic_factor": aggregate}, horizon=3
    )
    dates = sorted(panel["datetime"].unique())
    config = SectorModelConfig(
        horizon=3,
        train_start=str(pd.Timestamp(dates[0]).date()),
        train_end=str(pd.Timestamp(dates[25]).date()),
        valid_start=str(pd.Timestamp(dates[26]).date()),
        valid_end=str(pd.Timestamp(dates[43]).date()),
        test_start=str(pd.Timestamp(dates[44]).date()),
        test_end=str(pd.Timestamp(dates[-1]).date()),
        top_m=1,
        n_estimators=20,
        early_stopping_rounds=5,
        min_child_samples=2,
    )
    result = train_sector_model_suite(
        panel,
        features,
        config=config,
        seeds=[7],
        model_kinds=["lgbm_regression", "lambdarank"],
    )

    assert set(result.predictions["model_kind"]) == {"lgbm_regression", "lambdarank"}
    assert result.ensemble_scores["complete_ensemble"].all()
    assert result.ensemble_scores["component_count"].eq(2).all()
    assert result.data_audit["purge_contract"].startswith("label_end_date")
    assert any(
        metric["classification"] == REAL_SECTOR_MODEL_CLASSIFICATION
        for metric in result.metrics
    )
