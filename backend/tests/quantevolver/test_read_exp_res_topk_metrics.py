from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]


def _load_topk_helpers() -> dict[str, object]:
    source = (REPO_ROOT / "backend" / "services" / "quantevolver" / "templates" / "read_exp_res.py").read_text(encoding="utf-8")
    start = source.index("_TOPK_NUMERIC_KEYS")
    end = source.index("def _extract_prediction_diagnostics")
    namespace = {"pd": pd, "_np": np}
    exec(source[start:end], namespace)
    return namespace


class _FakeRecorder:
    def __init__(self, label: pd.DataFrame) -> None:
        self._label = label

    def load_object(self, name: str):  # type: ignore[no-untyped-def]
        if name == "label.pkl":
            return self._label
        raise FileNotFoundError(name)


def _frame_for_dates(values_by_date: dict[str, list[float]], column: str) -> pd.DataFrame:
    rows: list[tuple[pd.Timestamp, str, float]] = []
    for date, values in values_by_date.items():
        for idx, value in enumerate(values, start=1):
            rows.append((pd.Timestamp(date), f"{idx:06d}.SZ", value))
    index = pd.MultiIndex.from_tuples([(dt, inst) for dt, inst, _ in rows], names=["datetime", "instrument"])
    return pd.DataFrame({column: [value for _, _, value in rows]}, index=index)


def test_read_exp_res_topk_metrics_use_prediction_rank_and_label_return() -> None:
    helpers = _load_topk_helpers()
    compute = helpers["_extract_tier1_topk_metrics"]
    scores = list(reversed(range(60)))
    pred = _frame_for_dates({"2025-01-02": scores, "2025-01-03": scores}, "score")
    label = _frame_for_dates(
        {
            "2025-01-02": [0.02] * 20 + [0.01] * 40,
            "2025-01-03": [-0.01] * 20 + [0.02] * 40,
        },
        "LABEL0",
    )

    result = compute(_FakeRecorder(label), pred)

    assert result["topk_quality_status"] == "ok"
    assert result["topk_return_20"] == 0.005
    assert result["topk_hit_rate_20"] == 0.5
    assert result["topk_return_50"] == 0.011
    assert result["topk_decay"] == -0.006
    assert result["within_portfolio_rankic_method"] == "negated_spearman_rank_vs_label_positive_good"
    assert result["topk_observation_count_20"] == 40
    assert result["topk_observation_count_50"] == 100


def test_read_exp_res_within_rankic_uses_positive_good_direction() -> None:
    helpers = _load_topk_helpers()
    compute = helpers["_extract_tier1_topk_metrics"]
    scores = list(reversed(range(60)))
    pred = _frame_for_dates({"2025-01-02": scores}, "score")
    label = _frame_for_dates({"2025-01-02": [0.03] * 20 + [0.01] * 20 + [-0.01] * 20}, "LABEL0")

    result = compute(_FakeRecorder(label), pred)

    assert result["within_portfolio_rankic"] > 0
    assert result["within_portfolio_rankic_method"] == "negated_spearman_rank_vs_label_positive_good"


def test_read_exp_res_topk_metrics_marks_missing_label_with_nulls() -> None:
    helpers = _load_topk_helpers()
    compute = helpers["_extract_tier1_topk_metrics"]
    pred = _frame_for_dates({"2025-01-02": list(reversed(range(30)))}, "score")

    class MissingLabelRecorder:
        def load_object(self, name: str):  # type: ignore[no-untyped-def]
            raise FileNotFoundError(name)

    result = compute(MissingLabelRecorder(), pred)

    assert result["topk_quality_status"] == "missing_label"
    assert result["topk_return_20"] is None
    assert result["topk_hit_rate_20"] is None
