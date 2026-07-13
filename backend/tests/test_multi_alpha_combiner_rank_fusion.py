from __future__ import annotations

import pandas as pd
import pytest

from backend.services.multi_alpha.combiner import CombinerLeg, MultiAlphaCombiner, MultiAlphaCombinerError


def _frame(rows: list[tuple[str, str, float | None]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"trade_date": pd.Timestamp(trade_date).date(), "instrument": instrument, "score": score}
            for trade_date, instrument, score in rows
        ]
    )


def _scores_by_instrument(frame: pd.DataFrame, trade_date: str) -> dict[str, float]:
    selected = frame[frame["trade_date"] == pd.Timestamp(trade_date).date()]
    return dict(zip(selected["instrument"], selected["combined_score"], strict=True))


def _ranking(frame: pd.DataFrame, trade_date: str) -> list[str]:
    selected = frame[frame["trade_date"] == pd.Timestamp(trade_date).date()]
    return selected.sort_values(["combined_score", "instrument"], ascending=[False, True])["instrument"].tolist()


def test_rrf_rank_fusion_combines_known_small_example() -> None:
    legs = [
        CombinerLeg(
            "momentum",
            _frame(
                [
                    ("2026-02-01", "A", 3.0),
                    ("2026-02-01", "B", 2.0),
                    ("2026-02-01", "C", 1.0),
                ]
            ),
        ),
        CombinerLeg(
            "quality",
            _frame(
                [
                    ("2026-02-01", "A", 1.0),
                    ("2026-02-01", "B", 3.0),
                    ("2026-02-01", "C", 2.0),
                ]
            ),
        ),
    ]

    result = MultiAlphaCombiner().combine_rank_fusion(legs=legs, method="rrf", rrf_k=60)

    assert _ranking(result.combined_score_frame, "2026-02-01") == ["B", "A", "C"]
    scores = _scores_by_instrument(result.combined_score_frame, "2026-02-01")
    assert scores["A"] == pytest.approx((1 / 61) + (1 / 63))
    assert scores["B"] == pytest.approx((1 / 62) + (1 / 61))
    assert scores["C"] == pytest.approx((1 / 63) + (1 / 62))
    assert result.summary["rank_fusion_method"] == "rrf"
    assert result.summary["dropped_dates"] == []


def test_borda_rank_fusion_combines_known_small_example() -> None:
    legs = [
        CombinerLeg(
            "momentum",
            _frame(
                [
                    ("2026-02-01", "A", 3.0),
                    ("2026-02-01", "B", 2.0),
                    ("2026-02-01", "C", 1.0),
                ]
            ),
        ),
        CombinerLeg(
            "quality",
            _frame(
                [
                    ("2026-02-01", "A", 1.0),
                    ("2026-02-01", "B", 3.0),
                    ("2026-02-01", "C", 2.0),
                ]
            ),
        ),
    ]

    result = MultiAlphaCombiner().rank_fusion(legs=legs, method="borda")

    assert _ranking(result.combined_score_frame, "2026-02-01") == ["B", "A", "C"]
    assert _scores_by_instrument(result.combined_score_frame, "2026-02-01") == {"A": 2.0, "B": 3.0, "C": 1.0}
    assert result.summary["rank_fusion_method"] == "borda"
    assert result.summary["rrf_k"] is None


def test_rank_fusion_is_deterministic_for_identical_input() -> None:
    legs = [
        CombinerLeg(
            "left",
            _frame(
                [
                    ("2026-02-01", "A", 2.0),
                    ("2026-02-01", "B", 1.0),
                    ("2026-02-02", "A", 1.0),
                    ("2026-02-02", "B", 2.0),
                ]
            ),
        ),
        CombinerLeg(
            "right",
            _frame(
                [
                    ("2026-02-01", "A", 1.0),
                    ("2026-02-01", "B", 2.0),
                    ("2026-02-02", "A", 2.0),
                    ("2026-02-02", "B", 1.0),
                ]
            ),
        ),
    ]

    first = MultiAlphaCombiner().combine_rank_fusion(legs=legs, method="rrf")
    second = MultiAlphaCombiner().combine_rank_fusion(legs=legs, method="rrf")

    pd.testing.assert_frame_equal(first.combined_score_frame, second.combined_score_frame)
    assert first.summary == second.summary
    assert first.weights == second.weights


def test_rank_fusion_tie_break_missing_legs_and_dropped_dates() -> None:
    legs = [
        CombinerLeg(
            "left",
            _frame(
                [
                    ("2026-02-01", "A", 10.0),
                    ("2026-02-01", "B", 10.0),
                    ("2026-02-01", "C", 5.0),
                    ("2026-02-02", "A", None),
                    ("2026-02-03", "A", 2.0),
                    ("2026-02-03", "B", 1.0),
                ]
            ),
        ),
        CombinerLeg(
            "right",
            _frame(
                [
                    ("2026-02-01", "A", 7.0),
                    ("2026-02-01", "C", 6.0),
                    ("2026-02-02", "A", None),
                    ("2026-02-03", "C", 5.0),
                ]
            ),
        ),
    ]

    result = MultiAlphaCombiner().combine_rank_fusion(legs=legs, method="rrf", rrf_k=60)

    assert _ranking(result.combined_score_frame, "2026-02-01") == ["A", "C", "B"]
    scores = _scores_by_instrument(result.combined_score_frame, "2026-02-01")
    assert scores["A"] == pytest.approx((1 / 61) + (1 / 61))
    assert scores["B"] == pytest.approx(1 / 62)
    assert scores["C"] == pytest.approx((1 / 63) + (1 / 62))
    assert _ranking(result.combined_score_frame, "2026-02-03") == ["A", "C", "B"]
    assert "2026-02-02" not in set(result.combined_score_frame["trade_date"].astype(str))
    assert result.summary["dropped_dates"] == ["2026-02-02"]


def test_rank_fusion_refuses_label_only_source() -> None:
    label_only = pd.DataFrame(
        [
            {"trade_date": pd.Timestamp("2026-02-01").date(), "instrument": "A", "label": 0.1},
            {"trade_date": pd.Timestamp("2026-02-01").date(), "instrument": "B", "label": 0.2},
        ]
    )
    score_leg = _frame([("2026-02-01", "A", 1.0), ("2026-02-01", "B", 2.0)])

    with pytest.raises(MultiAlphaCombinerError, match="refuses label-only"):
        MultiAlphaCombiner().combine_rank_fusion(
            legs=[CombinerLeg("label_only", label_only), CombinerLeg("score_leg", score_leg)],
            method="rrf",
        )
