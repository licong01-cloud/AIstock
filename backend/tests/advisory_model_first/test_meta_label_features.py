from __future__ import annotations

from pathlib import Path


def test_meta_label_walk_forward_hmm_never_uses_block_or_future_dates() -> None:
    path = Path(__file__).resolve().parents[2] / "services" / "advisory_model_first" / "meta_label_features.py"
    text = path.read_text(encoding="utf-8")
    assert "train_dates = hmm_calendar[hmm_calendar < start]" in text
    assert "state = result.states[result.states[\"decision_as_of_trade_date\"].isin(dates)]" in text
    assert "precomputed_observations=observations" in text
