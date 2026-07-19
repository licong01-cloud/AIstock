from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.services.hmm_evolution.evaluator import (
    CandidateCoefficients,
    EvaluationInputError,
    evaluate_candidate,
    resolve_batch_common_dates,
)
from backend.services.hmm_evolution.errors import (
    LabelHorizonMismatchError,
    MarketDataUnavailableError,
    NoCommonDatesError,
)


def _predictions(rows):
    return pd.DataFrame(rows, columns=["trade_date", "symbol", "score"])


def _labels(rows, *, horizon=10):
    return pd.DataFrame(
        [(trade_date, symbol, horizon, value) for trade_date, symbol, value in rows],
        columns=["trade_date", "symbol", "horizon_days", "future_return"],
    )


def _returns(rows):
    return pd.DataFrame(rows, columns=["trade_date", "symbol", "future_return"])


def _coefficients(dates, *, missing_symbol=False):
    mapping = {"A": "S1", "B": "S1", "C": "S2", "D": "S2"}
    if missing_symbol:
        mapping.pop("C")
    return CandidateCoefficients.from_payload(
        {
            "daily_coefficients": {
                item.isoformat(): {"S1": 1.0, "S2": 2.0} for item in dates
            },
            "stock_sector_map": mapping,
        }
    )


def test_evaluator_matches_non_tie_replacement_oracle_and_day_weighting() -> None:
    d1 = date(2026, 1, 5)
    d2 = date(2026, 1, 6)
    predictions = _predictions(
        [
            (d1, "A", 4.0),
            (d1, "B", 3.0),
            (d1, "C", 2.0),
            (d1, "D", 1.0),
            (d2, "A", 4.0),
            (d2, "B", 3.0),
            (d2, "C", 2.5),
            (d2, "D", 1.0),
        ]
    )
    labels = _labels(
        [
            (d1, "A", -0.1),
            (d1, "B", -0.2),
            (d1, "C", 0.3),
            (d1, "D", 0.0),
            (d2, "A", -0.2),
            (d2, "B", -0.1),
            (d2, "C", 0.4),
            (d2, "D", 0.0),
        ]
    )
    db_returns = _returns(
        [
            (d1, "A", -0.05),
            (d1, "B", -0.1),
            (d1, "C", 0.2),
            (d1, "D", 0.0),
            (d2, "A", -0.1),
            (d2, "B", -0.05),
            (d2, "C", 0.3),
            (d2, "D", 0.0),
        ]
    )

    computation = evaluate_candidate(
        candidate_id="hmmc_oracle",
        predictions=predictions,
        labels=labels,
        coefficients=_coefficients([d1, d2]),
        evaluation_dates=[d1, d2],
        label_horizon_days=10,
        topk=2,
        db_forward_returns=db_returns,
    )

    replacements = [
        (row["date"], row["symbol"], row["replacement_type"])
        for row in computation.replacement_rows
    ]
    assert replacements == [
        (d1.isoformat(), "C", "entered_by_hmm"),
        (d1.isoformat(), "B", "dropped_by_hmm"),
        (d2.isoformat(), "C", "entered_by_hmm"),
        (d2.isoformat(), "B", "dropped_by_hmm"),
    ]
    assert computation.result["net_label_return"] == pytest.approx(0.5)
    assert computation.result["net_db_10d"] == pytest.approx(0.325)
    assert computation.result["positive_net_label_day_ratio"] == 1.0
    assert computation.result["evidence_quality"] == "complete"


def test_tie_break_is_score_desc_symbol_asc_independent_of_input_order() -> None:
    trade_date = date(2026, 1, 5)
    rows = [(trade_date, "B", 1.0), (trade_date, "A", 1.0), (trade_date, "C", 0.9)]
    labels = _labels([(trade_date, "A", 0.1), (trade_date, "B", 0.2), (trade_date, "C", 0.3)])
    returns = _returns([(trade_date, "A", 0.1), (trade_date, "B", 0.2), (trade_date, "C", 0.3)])
    coefficients = CandidateCoefficients.from_payload(
        {
            "daily_coefficients": {trade_date.isoformat(): {"S": 1.0}},
            "stock_sector_map": {"A": "S", "B": "S", "C": "S"},
        }
    )

    first = evaluate_candidate(
        candidate_id="hmmc_tie",
        predictions=_predictions(rows),
        labels=labels,
        coefficients=coefficients,
        evaluation_dates=[trade_date],
        label_horizon_days=10,
        topk=1,
        db_forward_returns=returns,
    )
    second = evaluate_candidate(
        candidate_id="hmmc_tie",
        predictions=_predictions(list(reversed(rows))),
        labels=labels,
        coefficients=coefficients,
        evaluation_dates=[trade_date],
        label_horizon_days=10,
        topk=1,
        db_forward_returns=returns,
    )

    assert first.result["result_hash"] == second.result["result_hash"]
    assert first.result["changed_day_count"] == 0
    assert first.result["net_label_return"] is None
    assert first.result["evidence_quality"] == "insufficient"
    assert first.result["metrics_json"]["daily_summary"][0]["calculation_status"] == "no_adjustment"


def test_neutral_fallback_is_explicit_and_degrades_evidence() -> None:
    trade_date = date(2026, 1, 5)
    computation = evaluate_candidate(
        candidate_id="hmmc_fallback",
        predictions=_predictions(
            [(trade_date, "A", 3.0), (trade_date, "B", 2.0), (trade_date, "C", 1.5)]
        ),
        labels=_labels(
            [(trade_date, "A", -0.1), (trade_date, "B", -0.2), (trade_date, "C", 0.3)]
        ),
        coefficients=_coefficients([trade_date], missing_symbol=True),
        evaluation_dates=[trade_date],
        label_horizon_days=10,
        topk=2,
        db_forward_returns=_returns(
            [(trade_date, "A", -0.1), (trade_date, "B", -0.2), (trade_date, "C", 0.3)]
        ),
    )

    metrics = computation.result["metrics_json"]
    assert metrics["missing_sector_occurrence_count"] == 1
    assert computation.result["evidence_quality"] == "degraded"
    assert any(
        warning["code"] == "hmm_evolution_missing_sector_neutral_fallback"
        for warning in computation.result["warnings_json"]
    )


def test_label_horizon_mismatch_fails_loudly() -> None:
    trade_date = date(2026, 1, 5)
    with pytest.raises(LabelHorizonMismatchError, match="label horizon") as exc_info:
        evaluate_candidate(
            candidate_id="hmmc_horizon",
            predictions=_predictions([(trade_date, "A", 1.0)]),
            labels=_labels([(trade_date, "A", 0.1)], horizon=20),
            coefficients=_coefficients([trade_date]),
            evaluation_dates=[trade_date],
            label_horizon_days=10,
            topk=1,
            db_forward_returns=_returns([(trade_date, "A", 0.1)]),
        )
    assert exc_info.value.reason_code == "hmm_evolution_label_horizon_mismatch"


def test_label_horizon_twenty_is_preserved_as_generic_net_label_return() -> None:
    trade_date = date(2026, 1, 5)
    computation = evaluate_candidate(
        candidate_id="hmmc_h20",
        predictions=_predictions(
            [(trade_date, "A", 3.0), (trade_date, "B", 2.0), (trade_date, "C", 1.5)]
        ),
        labels=_labels(
            [(trade_date, "A", -0.1), (trade_date, "B", -0.2), (trade_date, "C", 0.3)],
            horizon=20,
        ),
        coefficients=_coefficients([trade_date]),
        evaluation_dates=[trade_date],
        label_horizon_days=20,
        topk=2,
        db_forward_returns=_returns(
            [(trade_date, "A", -0.1), (trade_date, "B", -0.2), (trade_date, "C", 0.3)]
        ),
    )
    assert computation.result["metrics_json"]["label_horizon_days"] == 20
    assert "net_label_10d" not in computation.result["metrics_json"]


def test_required_market_returns_cannot_succeed_without_comparable_days() -> None:
    trade_date = date(2026, 1, 5)
    with pytest.raises(MarketDataUnavailableError, match="no comparable") as exc_info:
        evaluate_candidate(
            candidate_id="hmmc_db",
            predictions=_predictions(
                [(trade_date, "A", 3.0), (trade_date, "B", 2.0), (trade_date, "C", 1.5)]
            ),
            labels=_labels(
                [(trade_date, "A", -0.1), (trade_date, "B", -0.2), (trade_date, "C", 0.3)]
            ),
            coefficients=_coefficients([trade_date]),
            evaluation_dates=[trade_date],
            label_horizon_days=10,
            topk=2,
            db_forward_returns=_returns([]),
        )
    assert exc_info.value.reason_code == "hmm_evolution_market_data_unavailable"


def test_partial_symbol_returns_never_compute_a_partial_daily_net() -> None:
    trade_date = date(2025, 5, 9)
    complete_date = date(2025, 5, 12)
    coefficients = CandidateCoefficients.from_payload(
        {
            "daily_coefficients": {
                item.isoformat(): {"S1": 1.0, "S2": 5.0}
                for item in (trade_date, complete_date)
            },
            "stock_sector_map": {"A": "S1", "B": "S1", "C": "S2", "D": "S2"},
        }
    )
    computation = evaluate_candidate(
        candidate_id="hmmc_missing_symbol_return",
        predictions=_predictions(
            [
                (trade_date, "A", 4.0),
                (trade_date, "B", 3.0),
                (trade_date, "C", 2.0),
                (trade_date, "D", 1.0),
                (complete_date, "A", 4.0),
                (complete_date, "B", 3.0),
                (complete_date, "C", 2.0),
                (complete_date, "D", 1.0),
            ]
        ),
        labels=_labels(
            [
                (trade_date, "A", 0.1),
                (trade_date, "B", 0.2),
                (trade_date, "C", 0.3),
                (trade_date, "D", 0.4),
                (complete_date, "A", 0.1),
                (complete_date, "B", 0.2),
                (complete_date, "C", 0.3),
                (complete_date, "D", 0.4),
            ]
        ),
        coefficients=coefficients,
        evaluation_dates=[trade_date, complete_date],
        label_horizon_days=10,
        topk=2,
        db_forward_returns=_returns(
            [
                (trade_date, "A", 0.1),
                (trade_date, "B", 0.2),
                (trade_date, "C", 0.3),
                (complete_date, "A", 0.1),
                (complete_date, "B", 0.2),
                (complete_date, "C", 0.3),
                (complete_date, "D", 0.4),
            ]
        ),
        market_missing_evidence=(
            {
                "trade_date": trade_date.isoformat(),
                "symbol": "D",
                "label_date": "2025-05-23",
                "reason": "horizon_price_missing",
            },
        ),
    )

    daily = computation.result["metrics_json"]["daily_summary"][0]
    assert daily["entered_db_count"] == 1
    assert daily["dropped_db_count"] == 2
    assert daily["daily_net_db_10d"] is None
    assert daily["calculation_status"] == "incomplete_evidence"
    assert computation.result["metrics_json"]["incomplete_return_evidence"] == [
        {
            "date": trade_date.isoformat(),
            "symbol": "D",
            "replacement_type": "entered_by_hmm",
            "evidence_type": "market_return",
            "horizon_trading_days": 10,
            "required_start_date": trade_date.isoformat(),
            "required_label_date": "2025-05-23",
            "reason": "horizon_price_missing",
        }
    ]
    assert computation.result["evidence_quality"] == "degraded"


def test_batch_common_intersection_records_dropped_dates_and_strict_rejects() -> None:
    d1 = date(2026, 1, 5)
    d2 = date(2026, 1, 6)
    predictions = _predictions([(d1, "A", 1.0), (d2, "A", 1.0)])
    labels = _labels([(d1, "A", 0.1)])
    candidate = _coefficients([d1, d2])

    plan = resolve_batch_common_dates(
        predictions=predictions,
        labels=labels,
        candidates={"hmmc": candidate},
        window_start=d1,
        window_end=d2,
    )
    assert plan.evaluation_dates == (d1,)
    assert plan.dropped_prediction_dates == (d2,)
    assert plan.degraded is True

    with pytest.raises(EvaluationInputError, match="strict_full"):
        resolve_batch_common_dates(
            predictions=predictions,
            labels=labels,
            candidates={"hmmc": candidate},
            window_start=d1,
            window_end=d2,
            policy="strict_full",
        )


def test_batch_common_dates_fail_when_coefficient_and_prediction_do_not_overlap() -> None:
    pred_date = date(2026, 1, 5)
    coefficient_date = date(2026, 1, 6)
    with pytest.raises(NoCommonDatesError):
        resolve_batch_common_dates(
            predictions=_predictions([(pred_date, "A", 1.0)]),
            labels=_labels([(pred_date, "A", 0.1)]),
            candidates={"hmmc": _coefficients([coefficient_date])},
            window_start=pred_date,
            window_end=coefficient_date,
        )


def test_non_finite_prediction_is_excluded_with_warning_not_silently() -> None:
    trade_date = date(2026, 1, 5)
    computation = evaluate_candidate(
        candidate_id="hmmc_nan",
        predictions=_predictions(
            [(trade_date, "A", 3.0), (trade_date, "B", 2.0), (trade_date, "C", float("nan"))]
        ),
        labels=_labels([(trade_date, "A", 0.1), (trade_date, "B", 0.2)]),
        coefficients=_coefficients([trade_date]),
        evaluation_dates=[trade_date],
        label_horizon_days=10,
        topk=1,
        db_forward_returns=_returns([(trade_date, "A", 0.1), (trade_date, "B", 0.2)]),
    )
    assert computation.result["evidence_quality"] == "degraded"
    assert any(
        warning["code"] == "hmm_evolution_non_finite_prediction_scores_excluded"
        for warning in computation.result["warnings_json"]
    )
