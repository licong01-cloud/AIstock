from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.quantevolver.sector_regime_router import (
    SectorAgreementRouterConfig,
    SectorWalkForwardRouterConfig,
    build_observable_sector_state,
    compute_sector_agreement_router,
    compute_sector_walk_forward_router,
)


def _scores(*, aligned: bool) -> pd.DataFrame:
    records = []
    for day_index, date in enumerate(pd.date_range("2026-01-01", periods=10, freq="B")):
        for sector in range(4):
            value = float(sector)
            if not aligned and day_index % 2:
                value = -value
            records.append(
                {
                    "signal_date": date,
                    "l2_code_id": sector,
                    "sector_score": value,
                }
            )
    return pd.DataFrame.from_records(records)


def _oracle_daily() -> pd.DataFrame:
    records = []
    for index, date in enumerate(pd.date_range("2026-01-01", periods=10, freq="B")):
        records.extend(
            [
                {
                    "signal_date": date,
                    "cell": "one_layer_reality",
                    "mode": "one_layer",
                    "net_forward_return_proxy": 0.01,
                },
                {
                    "signal_date": date,
                    "cell": "reality_sector__reality_stock",
                    "mode": "soft",
                    "net_forward_return_proxy": 0.02 if index >= 3 else 0.0,
                },
            ]
        )
    return pd.DataFrame.from_records(records)


def test_router_uses_shifted_trailing_agreement_and_explicit_cold_start_baseline():
    result = compute_sector_agreement_router(
        _scores(aligned=True),
        _scores(aligned=True),
        _oracle_daily(),
        config=SectorAgreementRouterConfig(
            horizon=3,
            lookback=4,
            min_periods=2,
            agreement_quantile=0.75,
            bootstrap_samples=10,
        ),
    )
    assert result.daily.loc[:1, "route_evidence_available"].eq(False).all()
    assert result.daily.loc[:1, "routed_net_forward_return_proxy"].eq(0.01).all()
    assert result.daily.loc[2:, "route_to_sector_soft"].all()
    assert result.audit["cold_start_or_missing_evidence_days"] == 2
    assert "no future outcome" in result.audit["causal_contract"]
    assert np.isfinite(result.metrics[0]["mean_incremental_net_return_proxy"])


def _walk_forward_scores(*, family: str, periods: int = 80) -> pd.DataFrame:
    records = []
    for day_index, date in enumerate(pd.date_range("2025-01-01", periods=periods, freq="B")):
        positive_regime = (day_index // 8) % 2 == 0
        for sector in range(8):
            if family == "regression":
                score = float(sector) + day_index * 0.001
            elif family == "breadth":
                score = float(sector if positive_regime else 7 - sector)
            elif family == "momentum":
                score = float(sector) + (0.2 if positive_regime else -0.2) * (sector % 2)
            else:
                raise AssertionError(family)
            records.append(
                {
                    "signal_date": date,
                    "l2_code_id": sector,
                    "sector_score": score,
                }
            )
    return pd.DataFrame.from_records(records)


def _walk_forward_oracle(*, periods: int = 80) -> pd.DataFrame:
    records = []
    for day_index, date in enumerate(pd.date_range("2025-01-01", periods=periods, freq="B")):
        positive_regime = (day_index // 8) % 2 == 0
        baseline = 0.01
        overlay = baseline + (0.03 if positive_regime else -0.03)
        records.extend(
            [
                {
                    "signal_date": date,
                    "cell": "one_layer_reality",
                    "mode": "one_layer",
                    "net_forward_return_proxy": baseline,
                },
                {
                    "signal_date": date,
                    "cell": "reality_sector__reality_stock",
                    "mode": "soft",
                    "net_forward_return_proxy": overlay,
                },
            ]
        )
    return pd.DataFrame.from_records(records)


def test_walk_forward_router_uses_only_mature_history_and_keeps_cold_start():
    momentum_scores = _walk_forward_scores(family="momentum")
    momentum_scores.loc[0, "sector_score"] = np.nan
    state = build_observable_sector_state(
        {
            "regression": _walk_forward_scores(family="regression"),
            "breadth": _walk_forward_scores(family="breadth"),
            "momentum": momentum_scores,
        },
        top_m=3,
    )
    assert state.loc[0, "source_sector_count"] == 8
    assert state.loc[0, "matched_sector_count"] == 7
    assert state.loc[0, "score_coverage_ratio"] == 0.875
    config = SectorWalkForwardRouterConfig(
        horizon=3,
        top_m=3,
        min_train_days=10,
        ridge_alpha=2.0,
        bootstrap_samples=10,
    )
    original_oracle = _walk_forward_oracle()
    result = compute_sector_walk_forward_router(
        state,
        original_oracle,
        config=config,
    )

    assert result.daily.loc[:12, "route_evidence_available"].eq(False).all()
    assert result.daily.loc[13:, "route_evidence_available"].all()
    assert result.daily.loc[:12, "routed_net_forward_return_proxy"].eq(0.01).all()
    assert len(result.coefficients) == len(result.daily) - 13
    assert result.audit["maturity_delay_trading_days"] == 4
    assert "only outcomes" in result.audit["causal_contract"]

    changed_oracle = original_oracle.copy()
    changed_dates = sorted(changed_oracle["signal_date"].unique())[40:]
    changed_mask = (
        changed_oracle["signal_date"].isin(changed_dates)
        & changed_oracle["cell"].eq("reality_sector__reality_stock")
    )
    changed_oracle.loc[changed_mask, "net_forward_return_proxy"] += 10.0
    changed = compute_sector_walk_forward_router(
        state,
        changed_oracle,
        config=config,
    )
    np.testing.assert_allclose(
        result.daily.loc[:43, "predicted_incremental_net_return_proxy"],
        changed.daily.loc[:43, "predicted_incremental_net_return_proxy"],
        equal_nan=True,
    )
