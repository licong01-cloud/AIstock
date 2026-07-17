from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.services.hmm_evolution.evaluator import CandidateCoefficients, evaluate_candidate
from scripts.diagnostics.hmm_offline_diagnostic import compute_replacements


def test_pure_evaluator_matches_legacy_non_tie_replacement_oracle() -> None:
    trade_date = pd.Timestamp("2026-01-05")
    index = pd.MultiIndex.from_tuples(
        [(trade_date, symbol) for symbol in ("A", "B", "C", "D")],
        names=["datetime", "instrument"],
    )
    pred = pd.Series([4.0, 3.0, 2.0, 1.0], index=index)
    label = pd.Series([-0.1, -0.2, 0.3, 0.0], index=index)
    payload = {
        "daily_coefficients": {"2026-01-05": {"S1": 1.0, "S2": 2.0}},
        "stock_sector_map": {"A": "S1", "B": "S1", "C": "S2", "D": "S2"},
    }
    legacy_rows, legacy_days, _legacy_sectors = compute_replacements(
        pred,
        label,
        payload,
        2,
        "oracle",
    )
    pure = evaluate_candidate(
        candidate_id="hmmc_oracle",
        predictions=pd.DataFrame(
            [(date(2026, 1, 5), symbol, score) for symbol, score in zip(("A", "B", "C", "D"), pred)],
            columns=["trade_date", "symbol", "score"],
        ),
        labels=pd.DataFrame(
            [(date(2026, 1, 5), symbol, 10, value) for symbol, value in zip(("A", "B", "C", "D"), label)],
            columns=["trade_date", "symbol", "horizon_days", "future_return"],
        ),
        coefficients=CandidateCoefficients.from_payload(payload),
        evaluation_dates=[date(2026, 1, 5)],
        label_horizon_days=10,
        topk=2,
        market_forward_return_mode="disabled",
    )

    assert [
        (row.symbol, row.replacement_type) for row in legacy_rows.itertuples(index=False)
    ] == [
        (row["symbol"], row["replacement_type"]) for row in pure.replacement_rows
    ]
    assert legacy_days.iloc[0]["net_enter_minus_drop_label_10d"] == pytest.approx(
        pure.result["net_label_return"]
    )
