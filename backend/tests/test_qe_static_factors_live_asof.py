from __future__ import annotations

import pandas as pd

from backend.data_service import qe_data_service


def _idx(*dates: str) -> pd.MultiIndex:
    return pd.MultiIndex.from_product(
        [pd.to_datetime(list(dates)), ["000001.SZ"]],
        names=["datetime", "instrument"],
    )


def test_live_asof_fill_only_slow_static_prefixes(monkeypatch) -> None:
    daily_index = _idx("2026-05-12", "2026-05-13")
    slow_index = _idx("2026-05-12")

    monkeypatch.setattr(
        qe_data_service,
        "load_daily_basic",
        lambda *_args, **_kwargs: pd.DataFrame(
            {
                "db_turnover_rate": [1.1, 1.2],
                "db_volume_ratio": [2.1, 2.2],
                "db_pb": [1.5, 1.6],
                "db_circ_mv": [100.0, 101.0],
                "db_dv_ratio": [7.0, float("nan")],
            },
            index=daily_index,
        ),
    )
    moneyflow_cols = [
        "mf_sm_buy_amt",
        "mf_sm_sell_amt",
        "mf_md_buy_amt",
        "mf_md_sell_amt",
        "mf_lg_buy_amt",
        "mf_lg_sell_amt",
        "mf_elg_buy_amt",
        "mf_elg_sell_amt",
        "mf_sm_buy_vol",
        "mf_sm_sell_vol",
        "mf_md_buy_vol",
        "mf_md_sell_vol",
        "mf_lg_buy_vol",
        "mf_lg_sell_vol",
        "mf_elg_buy_vol",
        "mf_elg_sell_vol",
        "mf_net_amt",
        "mf_net_vol",
    ]
    monkeypatch.setattr(
        qe_data_service,
        "load_moneyflow",
        lambda *_args, **_kwargs: pd.DataFrame({col: [10.0] for col in moneyflow_cols}, index=slow_index),
    )
    monkeypatch.setattr(
        qe_data_service,
        "load_bak_basic",
        lambda *_args, **_kwargs: pd.DataFrame({"bb_npr": [3.0]}, index=slow_index),
    )
    monkeypatch.setattr(
        qe_data_service,
        "load_cyq_perf",
        lambda *_args, **_kwargs: pd.DataFrame({"cp_winner_rate": [4.0]}, index=slow_index),
    )
    monkeypatch.setattr(
        qe_data_service,
        "load_sector_data",
        lambda *_args, **_kwargs: pd.DataFrame({"sw2_close": [5.0]}, index=slow_index),
    )
    monkeypatch.setattr(
        qe_data_service,
        "load_margin_detail",
        lambda *_args, **_kwargs: pd.DataFrame({"md_rzye": [6.0]}, index=slow_index),
    )
    monkeypatch.setattr(
        qe_data_service,
        "load_daily_pv",
        lambda *_args, **_kwargs: pd.DataFrame(
            {
                "close": [10.0, 11.0],
                "amount": [100.0, 101.0],
                "volume": [1000.0, 1001.0],
                "factor": [1.0, 1.0],
            },
            index=daily_index,
        ),
    )

    default_df = qe_data_service.build_static_factors(["000001.SZ"], "2026-05-12", "2026-05-13")
    live_df = qe_data_service.build_static_factors(
        ["000001.SZ"],
        "2026-05-12",
        "2026-05-13",
        asof_fill_slow_static=True,
    )
    latest_key = (pd.Timestamp("2026-05-13"), "000001.SZ")

    assert pd.isna(default_df.loc[latest_key, "cp_winner_rate"])
    assert live_df.loc[latest_key, "cp_winner_rate"] == 4.0
    assert live_df.loc[latest_key, "bb_npr"] == 3.0
    assert live_df.loc[latest_key, "sw2_close"] == 5.0
    assert live_df.loc[latest_key, "md_rzye"] == 6.0
    assert live_df.loc[latest_key, "db_dv_ratio"] == 7.0
    assert pd.isna(live_df.loc[latest_key, "mf_elg_buy_amt"])
    assert live_df.loc[latest_key, "db_turnover_rate"] == 1.2
