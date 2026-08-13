from __future__ import annotations

from pathlib import Path



def test_meta_label_walk_forward_hmm_never_uses_block_or_future_dates() -> None:
    path = Path(__file__).resolve().parents[2] / "services" / "advisory_model_first" / "meta_label_features.py"
    text = path.read_text(encoding="utf-8")
    assert "train_dates = hmm_calendar[hmm_calendar < start]" in text
    assert "state = result.states[result.states[\"decision_as_of_trade_date\"].isin(dates)]" in text
    assert "precomputed_observations=observations" in text


def test_shared_feature_builder_keeps_legacy_drop_date_default_and_supports_meta_candidate_drop() -> None:
    source = Path(__file__).resolve().parents[2] / "services" / "advisory_model_first" / "shared_feature_builder.py"
    text = source.read_text(encoding="utf-8")
    assert 'incomplete_candidate_policy: str = "drop_date"' in text
    assert 'incomplete_candidate_policy="drop_candidate"' in (
        Path(__file__).resolve().parents[2]
        / "services"
        / "advisory_model_first"
        / "meta_label_features.py"
    ).read_text(encoding="utf-8")
