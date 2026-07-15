from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.quantevolver.sector_oracle import (
    ORACLE_CLASSIFICATION,
    SectorOracleConfig,
    compute_sector_oracle_grid,
)


def _observations() -> pd.DataFrame:
    records = []
    for day_index, date in enumerate(pd.date_range("2026-01-05", periods=4, freq="B")):
        for sector in range(1, 4):
            for stock in range(4):
                instrument = f"{sector}{stock:05d}.SZ"
                future_return = 0.02 * sector + 0.01 * stock + 0.001 * day_index
                # Reality score intentionally prefers low-return sector 1.
                score = 1.0 / sector + 0.001 * stock
                records.append(
                    {
                        "signal_date": date,
                        "instrument": instrument,
                        "score": score,
                        "l2_code_id": sector,
                        "entry_close_qfq": 10.0,
                        "entry_suspension_diagnostic": False,
                        "return_40": future_return,
                        "maturity_40": "mature",
                    }
                )
    records[0]["entry_suspension_diagnostic"] = True
    return pd.DataFrame.from_records(records)


def test_four_cell_oracle_and_soft_gating_preserve_trial_evidence_without_decision():
    result = compute_sector_oracle_grid(
        _observations(),
        config=SectorOracleConfig(
            horizon=40,
            sector_top_m=1,
            stock_top_k=3,
            round_trip_cost_bps=8.45,
            bootstrap_samples=20,
            bootstrap_seed=7,
        ),
    )

    assert result.eligibility["input_rows"] == 48
    assert result.eligibility["eligible_rows"] == 47
    combinations = set(zip(result.daily["cell"], result.daily["mode"]))
    assert ("reality_sector__reality_stock", "hard") in combinations
    assert ("oracle_sector__oracle_stock", "hard") in combinations
    assert ("oracle_sector__oracle_stock", "soft") in combinations
    assert ("one_layer_reality", "one_layer") in combinations

    by_key = {(item["cell"], item["mode"]): item for item in result.summaries}
    baseline = by_key[("reality_sector__reality_stock", "hard")]
    ceiling = by_key[("oracle_sector__oracle_stock", "hard")]
    assert ceiling["classification"] == ORACLE_CLASSIFICATION
    assert ceiling["mean_gross_forward_return"] > baseline["mean_gross_forward_return"]
    assert ceiling["research_decision"] is None
    assert "no GO/STOP" in ceiling["research_note"]
    assert np.isfinite(ceiling["mean_incremental_net_return_vs_hard_reality_reality"])
    assert not result.selections.empty


def test_oracle_reports_current_horizon_maturity_gap_instead_of_using_unmature_rows():
    observations = _observations()
    observations.loc[observations.index[:5], "maturity_40"] = "right_censored"
    result = compute_sector_oracle_grid(
        observations,
        config=SectorOracleConfig(
            horizon=40,
            sector_top_m=2,
            stock_top_k=4,
            round_trip_cost_bps=0.0,
            bootstrap_samples=10,
        ),
    )

    assert result.eligibility["criterion_pass_counts"]["mature"] == 43
    assert result.eligibility["eligible_rows"] == 43
    assert all(item["research_decision"] is None for item in result.summaries)


def test_negative_sector_sentinel_is_not_treated_as_pit_membership():
    observations = _observations()
    observations.loc[observations.index[1], "l2_code_id"] = -1
    result = compute_sector_oracle_grid(
        observations,
        config=SectorOracleConfig(
            horizon=40,
            sector_top_m=2,
            stock_top_k=4,
            round_trip_cost_bps=0.0,
            bootstrap_samples=10,
        ),
    )
    assert result.eligibility["criterion_pass_counts"]["pit_sector_present"] == 47
    assert result.eligibility["eligible_rows"] == 46


def test_external_sector_scores_replace_reality_sector_source_without_fallback():
    observations = _observations()
    dates = sorted(pd.to_datetime(observations["signal_date"].unique()))
    external = pd.DataFrame.from_records(
        [
            {
                "signal_date": date,
                "l2_code_id": sector,
                "sector_score": float(sector),
            }
            for date in dates[:-1]
            for sector in range(1, 4)
        ]
    )
    result = compute_sector_oracle_grid(
        observations,
        config=SectorOracleConfig(
            horizon=40,
            sector_top_m=1,
            stock_top_k=3,
            round_trip_cost_bps=8.45,
            bootstrap_samples=10,
        ),
        reality_sector_scores=external,
        reality_sector_score_name="qe_sector_lgbm_h40_v1",
    )

    assert result.eligibility["reality_sector_score_source"] == "qe_sector_lgbm_h40_v1"
    assert result.eligibility["eligible_sector_score_coverage"] < 1.0
    missing_date = pd.Timestamp(dates[-1])
    missing_day = result.daily.loc[
        result.daily["signal_date"].eq(missing_date)
        & result.daily["cell"].eq("reality_sector__reality_stock")
        & result.daily["mode"].eq("hard")
    ]
    assert len(missing_day) == 1
    assert int(missing_day.iloc[0]["selected_count"]) == 0
    assert float(missing_day.iloc[0]["turnover_proxy"]) == 1.0

    first_date = pd.Timestamp(dates[0])
    selected = result.selections.loc[
        result.selections["signal_date"].eq(first_date)
        & result.selections["cell"].eq("reality_sector__reality_stock")
        & result.selections["mode"].eq("hard")
    ]
    assert set(selected["l2_code_id"]) == {3}


def test_empty_selection_turnover_is_zero_until_a_position_set_exists():
    observations = _observations()
    external = pd.DataFrame(
        {
            "signal_date": [pd.Timestamp("2025-01-01")],
            "l2_code_id": [1],
            "sector_score": [1.0],
        }
    )
    result = compute_sector_oracle_grid(
        observations,
        config=SectorOracleConfig(
            horizon=40,
            sector_top_m=1,
            stock_top_k=3,
            round_trip_cost_bps=8.45,
            bootstrap_samples=10,
        ),
        reality_sector_scores=external,
    )
    empty_path = result.daily.loc[
        result.daily["cell"].eq("reality_sector__reality_stock")
        & result.daily["mode"].eq("hard")
    ]
    assert empty_path["selected_count"].eq(0).all()
    assert empty_path["turnover_proxy"].eq(0.0).all()
