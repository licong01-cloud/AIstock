from __future__ import annotations

import pandas as pd

from backend.services.advisory_model_first.policy_rank_source import build_policy_rankings


def test_policy_rank_source_reconstructs_exact_top40_for_explicit_leg_roster() -> None:
    dates = pd.to_datetime(["2026-01-02", "2026-01-05"])
    calendar = pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06"])
    frames = {}
    for leg, sign in (("leg_a", 1), ("leg_b", -1)):
        frames[leg] = pd.DataFrame(
            [
                {"trade_date": date, "instrument": f"{index:06d}.SZ", "score": sign * index}
                for date in dates
                for index in range(1, 51)
            ]
        )
    result = build_policy_rankings(
        leg_frames=frames,
        terminal_weights={"leg_a": 0.7, "leg_b": 0.3},
        decision_dates=dates,
        trading_calendar=calendar,
        identity={"program_id": "advp_test"},
    )
    assert len(result.rankings) == 80
    assert result.rankings.groupby("decision_as_of_trade_date").size().tolist() == [40, 40]
    assert result.coverage["status"].tolist() == ["COMPLETE", "COMPLETE"]
