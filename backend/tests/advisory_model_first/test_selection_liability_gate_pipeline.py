from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import policy_utility_pipeline
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.selection_liability_gate_pipeline import (
    _assert_outer_daily_completeness_not_worse,
    _attach_labels,
    _train_p0d_oof,
    _verify_prediction_dates,
    evaluate_liability_gate_constraint_blocks,
)


def _priorities(dates) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": day,
                "instrument": f"{rank:06d}.SZ",
                "entry_priority_rank": rank,
            }
            for day in dates
            for rank in range(1, 21)
        ]
    )


def test_prediction_date_identity_requires_exact_top20_and_exact_date_set() -> None:
    dates = pd.bdate_range("2026-01-05", periods=2)
    priorities = _priorities(dates)
    _verify_prediction_dates(priorities, dates)
    with pytest.raises(AdvisoryModelFirstError) as exc:
        _verify_prediction_dates(priorities.iloc[:-1], dates)
    assert exc.value.reason_code == "ADVISORY_P0K_OOF_DATE_MISMATCH"


def test_p0d_oof_uses_each_fold_train_and_score_dates_once(monkeypatch) -> None:
    calls: list[tuple[tuple[pd.Timestamp, ...], tuple[pd.Timestamp, ...]]] = []

    def fake_train(**kwargs):
        train = tuple(kwargs["train_dates"])
        score = tuple(kwargs["score_dates"])
        calls.append((train, score))
        return _priorities(score)

    monkeypatch.setattr(
        "backend.services.advisory_model_first.selection_liability_gate_pipeline."
        "train_fixed_p0d_reference_predictions",
        fake_train,
    )
    dates = pd.bdate_range("2026-01-05", periods=4)
    folds = (
        SimpleNamespace(train_dates=tuple(dates[2:]), score_dates=tuple(dates[:2])),
        SimpleNamespace(train_dates=tuple(dates[:2]), score_dates=tuple(dates[2:])),
    )
    result = _train_p0d_oof(
        features=pd.DataFrame(),
        labels=pd.DataFrame(),
        folds=folds,
        family=object(),
        seed=1,
        boost_rounds=2,
    )
    assert len(result) == 80
    assert len(calls) == 2
    assert set(calls[0][0]).isdisjoint(calls[0][1])
    assert set(calls[1][0]).isdisjoint(calls[1][1])


def test_attach_labels_leaves_non_matured_liability_missing() -> None:
    date = pd.Timestamp("2026-01-05")
    predictions = pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": date,
                "target_trade_date": date + pd.offsets.BDay(1),
                "instrument": "000001.SZ",
                "selection_effective_rank": 1,
                "predicted_turnover_liability_fraction_per_day": 0.1,
            },
            {
                "decision_as_of_trade_date": date,
                "target_trade_date": date + pd.offsets.BDay(1),
                "instrument": "000002.SZ",
                "selection_effective_rank": 2,
                "predicted_turnover_liability_fraction_per_day": 0.2,
            },
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": date,
                "target_trade_date": date + pd.offsets.BDay(1),
                "instrument": "000001.SZ",
                "label_status": "MATURED",
                "net_excess_return_bps": 1.0,
                "holding_trading_days": 4,
            },
            {
                "decision_as_of_trade_date": date,
                "target_trade_date": date + pd.offsets.BDay(1),
                "instrument": "000002.SZ",
                "label_status": "NOT_ENTERED_LIMIT_UP",
                "net_excess_return_bps": None,
                "holding_trading_days": None,
            },
        ]
    )
    attached = _attach_labels(predictions, labels)
    assert attached.iloc[0]["turnover_liability_fraction_per_day"] == 0.1
    assert pd.isna(attached.iloc[1]["turnover_liability_fraction_per_day"])


def test_constraint_evaluation_resets_blocks_and_reports_coverage(monkeypatch) -> None:
    dates = pd.bdate_range("2026-01-05", periods=2)
    calls: list[tuple[pd.Timestamp, ...]] = []

    def fake_replay(**kwargs):
        block_dates = tuple(kwargs["candidate_decision_dates"])
        calls.append(block_dates)
        return SimpleNamespace(
            daily=pd.DataFrame(
                {
                    "decision_as_of_trade_date": [block_dates[0]],
                    "turnover_fraction": [0.1],
                    "active_count": [5],
                    "cash_slot_count": [0],
                }
            )
        )

    monkeypatch.setattr(
        "backend.services.advisory_model_first.selection_liability_gate_pipeline."
        "replay_shadow_portfolio",
        fake_replay,
    )
    metrics = evaluate_liability_gate_constraint_blocks(
        rankings=_priorities(dates),
        entry_priorities=_priorities(dates),
        calibration_dates=dates,
        block_by_date={dates[0].date().isoformat(): 0, dates[1].date().isoformat(): 1},
        candidate_daily=pd.DataFrame(),
        benchmark=pd.DataFrame(),
        suspend=pd.DataFrame(),
        calendar=dates,
        policy=SimpleNamespace(target_count=5),
        policy_sha256="a" * 64,
        cost=object(),
        request_id="test",
    )
    assert calls == [(dates[0],), (dates[1],)]
    assert metrics["mean_turnover_fraction"] == 0.1
    assert metrics["active_slot_coverage"] == 1.0
    assert metrics["cash_day_count"] == 0


def test_constraint_evaluation_excludes_post_calibration_tail(monkeypatch) -> None:
    dates = pd.bdate_range("2026-01-05", periods=2)
    tail = dates[-1] + pd.offsets.BDay(1)
    rank_context = pd.DataFrame(
        {"decision_as_of_trade_date": [dates[0], dates[1], tail]}
    )

    def fake_replay(**kwargs):
        block_dates = tuple(kwargs["candidate_decision_dates"])
        replay_rank_dates = set(
            pd.to_datetime(kwargs["rankings"]["decision_as_of_trade_date"]).dt.normalize()
        )
        assert replay_rank_dates == set(block_dates)
        return SimpleNamespace(
            daily=pd.DataFrame(
                {
                    "decision_as_of_trade_date": [*block_dates, tail],
                    "turnover_fraction": [0.1] * len(block_dates) + [1.0],
                    "active_count": [5] * len(block_dates) + [0],
                    "cash_slot_count": [0] * len(block_dates) + [5],
                }
            )
        )

    monkeypatch.setattr(
        "backend.services.advisory_model_first.selection_liability_gate_pipeline."
        "replay_shadow_portfolio",
        fake_replay,
    )
    metrics = evaluate_liability_gate_constraint_blocks(
        rankings=rank_context,
        entry_priorities=_priorities(dates),
        calibration_dates=dates,
        block_by_date={day.date().isoformat(): 0 for day in dates},
        candidate_daily=pd.DataFrame(),
        benchmark=pd.DataFrame(),
        suspend=pd.DataFrame(),
        calendar=dates,
        policy=SimpleNamespace(target_count=5),
        policy_sha256="a" * 64,
        cost=object(),
        request_id="test",
    )
    assert metrics["day_count"] == 2
    assert metrics["mean_turnover_fraction"] == 0.1
    assert {row["decision_as_of_trade_date"] for row in metrics["daily_completeness"]} == {
        day.date().isoformat() for day in dates
    }


def test_constraint_evaluation_rejects_missing_matched_day(monkeypatch) -> None:
    dates = pd.bdate_range("2026-01-05", periods=2)

    def fake_replay(**kwargs):
        return SimpleNamespace(
            daily=pd.DataFrame(
                {
                    "decision_as_of_trade_date": [dates[0]],
                    "turnover_fraction": [0.1],
                    "active_count": [5],
                    "cash_slot_count": [0],
                }
            )
        )

    monkeypatch.setattr(
        "backend.services.advisory_model_first.selection_liability_gate_pipeline."
        "replay_shadow_portfolio",
        fake_replay,
    )
    with pytest.raises(AdvisoryModelFirstError) as exc:
        evaluate_liability_gate_constraint_blocks(
            rankings=_priorities(dates),
            entry_priorities=_priorities(dates),
            calibration_dates=dates,
            block_by_date={day.date().isoformat(): 0 for day in dates},
            candidate_daily=pd.DataFrame(),
            benchmark=pd.DataFrame(),
            suspend=pd.DataFrame(),
            calendar=dates,
            policy=SimpleNamespace(target_count=5),
            policy_sha256="a" * 64,
            cost=object(),
            request_id="test",
        )
    assert exc.value.reason_code == "ADVISORY_P0K_COVERAGE_INVALID"


def test_outer_completeness_fails_if_any_single_day_is_worse() -> None:
    dates = pd.bdate_range("2026-01-05", periods=2)
    selection = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "active_count": [5, 4],
            "cash_slot_count": [0, 1],
        }
    )
    gate = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "active_count": [4, 5],
            "cash_slot_count": [1, 0],
        }
    )
    with pytest.raises(AdvisoryModelFirstError) as exc:
        _assert_outer_daily_completeness_not_worse(gate, selection)
    assert exc.value.reason_code == "ADVISORY_P0K_OUTER_COMPLETENESS_FAILED"


def test_pipeline_imports_only_public_shared_orchestration_signatures() -> None:
    path = Path("backend/services/advisory_model_first/selection_liability_gate_pipeline.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    protected_modules = {
        "backend.services.advisory_model_first.policy_utility_pipeline",
        "backend.services.advisory_model_first.dual_head_output_constraint_training",
    }
    private = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module in protected_modules
        for alias in node.names
        if alias.name.startswith("_")
    ]
    assert private == []


def test_p0k_modules_do_not_import_private_predecessor_helpers() -> None:
    roots = [
        Path("backend/services/advisory_model_first") / name
        for name in (
            "selection_liability_gate_bundle.py",
            "selection_liability_gate_contracts.py",
            "selection_liability_gate_pipeline.py",
            "selection_liability_gate_training.py",
        )
    ]
    private: list[str] = []
    for path in roots:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        private.extend(
            f"{path.name}:{alias.name}"
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and (node.module or "").startswith("backend.services.advisory_model_first")
            for alias in node.names
            if alias.name.startswith("_")
        )
    assert private == []


def test_public_shared_orchestration_aliases_preserve_existing_behavior_identity() -> None:
    assert (
        policy_utility_pipeline.evaluate_policy_validation_blocks
        is policy_utility_pipeline._evaluate
    )
    assert policy_utility_pipeline.policy_episode_metrics is policy_utility_pipeline._episode_metrics
    assert policy_utility_pipeline.paired_policy_metrics is policy_utility_pipeline._paired_metrics
