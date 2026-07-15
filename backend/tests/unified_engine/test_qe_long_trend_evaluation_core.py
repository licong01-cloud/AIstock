from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import backend.services.quantevolver.long_trend_evaluation as long_trend_module
from backend.services.quantevolver.long_trend_evaluation import (
    ExecutionEvidenceBundle,
    QELongTrendEvaluationEngine,
    _daily_recall_uplift,
    _execution_family_statuses,
    _safe_nan_extreme,
    attach_episode_entry_evidence,
    attach_entry_execution_evidence,
    attach_exit_execution_evidence,
    benjamini_hochberg,
    compute_episode_metrics,
    compute_execution_metrics,
    compute_portfolio_metrics,
    moving_block_bootstrap_mean,
    newey_west_mean_test,
    reconstruct_holding_episodes,
)
from backend.services.quantevolver.long_trend_evaluation_contract import (
    FamilyComputationStatus,
    QEDatasetSnapshotIdentity,
    QELongTrendEvaluationContext,
    QELongTrendError,
    QELongTrendReason,
    QE_LONG_TREND_PROFILE_V1,
    SnapshotOverlapParityReceipt,
)


def _price_frame(*, missing_path: bool = False) -> pd.DataFrame:
    dates = pd.date_range("2025-08-01", periods=205, freq="B")
    values = {
        "000001.SZ": [10.0, 10.0] + [10.0 + 0.15 * index for index in range(1, len(dates) - 1)],
        "000002.SZ": [10.0, 10.0] + [10.0 - 0.02 * index for index in range(1, len(dates) - 1)],
    }
    records = []
    for instrument, closes in values.items():
        for index, (date, close) in enumerate(zip(dates, closes)):
            high = float(close + 0.5)
            if index == 1:
                high = 100.0  # entry-day high must never enter future path MFE
            records.append(
                {
                    "datetime": date,
                    "instrument": instrument,
                    "close_qfq": float(close),
                    "high_qfq": high,
                    "low_qfq": float(close - 0.5),
                    "volume_qfq": 1000.0,
                }
            )
    frame = pd.DataFrame.from_records(records).set_index(["datetime", "instrument"]).sort_index()
    if missing_path:
        frame.loc[(dates[2], "000001.SZ"), "close_qfq"] = np.nan
    return frame


def _sector_frame(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices.loc[:, []].copy()
    instruments = frame.index.get_level_values("instrument")
    frame["l2_code_id"] = np.where(instruments == "000001.SZ", 81, 104)
    return frame


def _prediction_frame() -> pd.DataFrame:
    dates = pd.date_range("2025-08-01", periods=205, freq="B")
    index = pd.MultiIndex.from_tuples(
        [
            (dates[0], "000002.SZ"),
            (dates[0], "000001.SZ"),
            (dates[-2], "000001.SZ"),
            (dates[-2], "000002.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    return pd.DataFrame({"score": [0.5, 0.5, 0.9, 0.1]}, index=index)


def _evaluation_context(prices: pd.DataFrame) -> QELongTrendEvaluationContext:
    dates = prices.index.get_level_values("datetime")
    start = pd.Timestamp(dates.min()).date().isoformat()
    end = pd.Timestamp(dates.max()).date().isoformat()
    feature = QEDatasetSnapshotIdentity(
        snapshot_id="qe_feature_fixture",
        manifest_sha256="feature-fixture-sha",
        start_date=start,
        end_date=end,
    )
    outcome = QEDatasetSnapshotIdentity(
        snapshot_id=feature.snapshot_id,
        manifest_sha256=feature.manifest_sha256,
        start_date=start,
        end_date=end,
    )
    receipt = SnapshotOverlapParityReceipt(
        feature_snapshot_id=feature.snapshot_id,
        outcome_snapshot_id=outcome.snapshot_id,
        overlap_start=start,
        overlap_end=end,
        row_count=len(prices),
        column_count=4,
        overlap_price_parity_sha256="overlap-fixture-sha",
        relation="same_snapshot",
    )
    return QELongTrendEvaluationContext(
        run_id="qe_fixture_run",
        evaluator_source_sha256="evaluator-fixture-sha",
        feature_snapshot=feature,
        outcome_snapshot=outcome,
        overlap_receipt=receipt,
        input_artifact_hashes={"prediction_sha256": "prediction-fixture-sha"},
    )


def test_signal_formula_stable_rank_barrier_and_censoring() -> None:
    prices = _price_frame()
    engine = QELongTrendEvaluationEngine()
    observations = engine.build_signal_observations(
        predictions=_prediction_frame(),
        prices=prices,
        sectors=_sector_frame(prices),
        signal_dates_per_chunk=1,
    )

    first_signal_date = _prediction_frame().index.get_level_values("datetime").min()
    first_date = observations.loc[observations["signal_date"] == first_signal_date]
    stock_a = first_date.loc[first_date["instrument"] == "000001.SZ"].iloc[0]
    stock_b = first_date.loc[first_date["instrument"] == "000002.SZ"].iloc[0]
    assert stock_a["stable_rank"] == 1  # tie broken by instrument ASC
    assert stock_b["stable_rank"] == 2
    expected_entry_date = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))[1]
    assert stock_a["entry_date"] == expected_entry_date
    assert stock_a["return_20"] == pytest.approx(0.3)
    assert stock_a["maturity_20"] == "matured"
    assert stock_a["time_to_close_hit_30"] == 20
    assert stock_a["path_mfe_20"] == pytest.approx(0.35)
    assert stock_a["path_mfe_20"] < 1.0  # entry-day high=100 was excluded
    assert stock_a["l2_code_id"] == 81

    tail_signal_date = _prediction_frame().index.get_level_values("datetime").max()
    tail = observations.loc[observations["signal_date"] == tail_signal_date]
    assert set(tail["maturity_20"]) == {"right_censored"}
    assert tail["return_20"].isna().all()
    assert tail["close_hit_30"].isna().all()


def test_path_gap_is_not_forward_filled_or_counted_as_mature() -> None:
    engine = QELongTrendEvaluationEngine()
    observations = engine.build_signal_observations(
        predictions=_prediction_frame().iloc[:2],
        prices=_price_frame(missing_path=True),
    )
    stock_a = observations.loc[observations["instrument"] == "000001.SZ"].iloc[0]
    assert stock_a["maturity_20"] == "path_incomplete"
    assert pd.isna(stock_a["return_20"])
    assert stock_a["observed_steps_20"] == 19


def test_evaluate_reports_label_parity_family_local_status_and_fdr() -> None:
    prices = _price_frame()
    predictions = _prediction_frame()
    observations = QELongTrendEvaluationEngine().build_signal_observations(
        predictions=predictions,
        prices=prices,
    )
    labels = observations.set_index(["signal_date", "instrument"])["return_20"].rename("label")

    result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(prices),
        predictions=predictions,
        prices=prices,
        labels=labels,
        label_horizon=20,
    )
    parity = next(metric for metric in result.metrics if metric["metric_key"] == "label_parity")
    assert parity["value_json"]["mismatch_count"] == 0
    assert result.family_status["signal_path"].status in {
        FamilyComputationStatus.COMPUTED,
        FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS,
    }
    assert result.family_status["position_episode"].status == FamilyComputationStatus.NOT_COMPUTABLE
    assert result.family_status["execution_cause"].status == FamilyComputationStatus.NOT_VERIFIABLE
    barrier = next(
        metric
        for metric in result.metrics
        if metric["metric_key"] == "barrier_capture"
        and metric["slice"] == "all_oos"
        and metric["horizon"] == 20
        and metric["barrier"] == 0.3
        and metric["k"] == 50
    )
    assert "bh_fdr_q_value" in barrier["value_json"]
    stage_survival = next(
        metric
        for metric in result.metrics
        if metric["metric_key"] == "ordered_trend_stage_survival"
        and metric["slice"] == "all_oos"
        and metric["horizon"] == 20
    )
    probabilities = stage_survival["value_json"]["direct_hit_probabilities"]
    assert probabilities["70"] <= probabilities["50"] <= probabilities["30"]
    km = stage_survival["value_json"]["kaplan_meier_hit_probabilities"]
    assert km["30"]["sample_count"] >= stage_survival["value_json"]["mature_count"]
    assert result.receipt["no_training"] is True
    assert result.receipt["no_backtest"] is True
    assert result.receipt["no_live_data_access"] is True
    assert result.evaluation_id.startswith("qelt_")
    assert result.receipt["evaluation_context"]["evaluation_id"] == result.evaluation_id
    assert result.receipt["platform_delivery_status"]["core_compute"] == "verified_phase1"

    mismatched = labels.copy()
    mismatched.iloc[0] += 0.01
    mismatch_result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(prices),
        predictions=predictions,
        prices=prices,
        labels=mismatched,
        label_horizon=20,
    )
    assert QELongTrendReason.LABEL_PARITY_FAILED.value in mismatch_result.family_status["signal_path"].reason_codes


def test_invalid_optional_family_inputs_do_not_discard_signal_path() -> None:
    prices = _price_frame()
    invalid_sector = prices.loc[:, []].copy()
    invalid_sector["wrong_sector_field"] = 1
    invalid_positions = pd.DataFrame(
        {
            "datetime": [pd.Timestamp("2026-01-06")],
            "instrument": ["000001.SZ"],
            "amount": [-1],
        }
    )
    invalid_indicator = pd.DataFrame(
        {
            "datetime": [pd.Timestamp("2026-01-06")],
            "instrument": ["000001.SZ"],
            "amount": [100],
        }
    )
    result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(prices),
        predictions=_prediction_frame(),
        prices=prices,
        sectors=invalid_sector,
        positions=invalid_positions,
        execution_evidence=ExecutionEvidenceBundle(indicator=invalid_indicator),
    )
    assert not result.signal_observations.empty
    assert result.family_status["signal_path"].status in {
        FamilyComputationStatus.COMPUTED,
        FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS,
    }
    assert result.family_status["sector_regime"].status == FamilyComputationStatus.NOT_COMPUTABLE
    assert result.family_status["position_episode"].status == FamilyComputationStatus.NOT_COMPUTABLE
    assert result.family_status["order_fill"].status == FamilyComputationStatus.NOT_COMPUTABLE
    assert result.family_status["execution_cause"].status == FamilyComputationStatus.NOT_VERIFIABLE


def test_missing_price_family_does_not_discard_authoritative_portfolio_report() -> None:
    identity_prices = _price_frame()
    portfolio_report = pd.DataFrame(
        {"return": [0.01, -0.005], "cost": [0.001, 0.001], "turnover": [0.2, 0.1]},
        index=pd.date_range("2026-01-05", periods=2, freq="B"),
    )
    result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(identity_prices),
        predictions=_prediction_frame(),
        prices=None,
        positions=pd.DataFrame(
            {
                "datetime": [pd.Timestamp("2026-01-05")],
                "instrument": ["000001.SZ"],
                "amount": [1.0],
            }
        ),
        portfolio_report=portfolio_report,
    )
    assert result.family_status["signal_path"].status == FamilyComputationStatus.NOT_COMPUTABLE
    assert result.family_status["position_episode"].status == FamilyComputationStatus.NOT_COMPUTABLE
    assert result.family_status["portfolio_result"].status == FamilyComputationStatus.COMPUTED
    assert any(metric["metric_scope"] == "portfolio_result" for metric in result.metrics)


def test_invalid_execution_activity_does_not_discard_authoritative_portfolio_report() -> None:
    prices = _price_frame()
    portfolio_report = pd.DataFrame(
        {"return": [0.01, -0.005], "cost": [0.001, 0.001], "turnover": [0.2, 0.1]},
        index=pd.date_range("2026-01-05", periods=2, freq="B"),
    )
    invalid_trades = pd.DataFrame(
        {
            "datetime": ["not-a-date"],
            "instrument": ["000001.SZ"],
            "quantity": [100.0],
        }
    )

    result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(prices),
        predictions=None,
        prices=prices,
        portfolio_report=portfolio_report,
        execution_evidence=ExecutionEvidenceBundle(trades=invalid_trades),
    )

    portfolio = result.family_status["portfolio_result"]
    assert portfolio.status == FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
    assert "valid_execution_activity_evidence" in portfolio.missing_inputs
    assert QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value in portfolio.reason_codes
    assert any(metric["metric_scope"] == "portfolio_result" for metric in result.metrics)


def test_exit_reconciliation_error_preserves_valid_entry_execution_evidence() -> None:
    prices = _price_frame()
    dates = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))
    positions = pd.DataFrame(
        {
            "datetime": dates,
            "instrument": "000001.SZ",
            "amount": [0, 1, 1, 0] + [0] * (len(dates) - 4),
        }
    )
    indicator = pd.DataFrame(
        {
            "datetime": [dates[1], dates[3]],
            "instrument": ["000001.SZ", "000001.SZ"],
            "side": ["buy", "sell"],
            "amount": [100.0, 100.0],
            "deal_amount": [100.0, 100.0],
            "ffr": [1.0, 1.0],
        }
    )
    trades = pd.DataFrame(
        {
            "datetime": [dates[1], dates[3]],
            "instrument": ["000001.SZ", "000001.SZ"],
            "side": ["buy", "sell"],
            "quantity": [100.0, 50.0],
            "price": [10.0, 10.3],
        }
    )
    result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(prices),
        predictions=_prediction_frame(),
        prices=prices,
        positions=positions,
        execution_evidence=ExecutionEvidenceBundle(
            indicator=indicator,
            trades=trades,
            exit_signals=pd.DataFrame({"datetime": [dates[3]], "instrument": ["000001.SZ"]}),
        ),
    )
    assert result.family_status["order_fill"].status == FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
    assert result.signal_observations["entry_execution_status"].eq("filled_t1").any()
    assert any(metric["metric_scope"] == "order_fill" for metric in result.metrics)


def test_full_evaluate_computes_each_available_family_independently() -> None:
    prices = _price_frame()
    dates = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))
    position_records = []
    for index, date in enumerate(dates):
        position_records.extend(
            [
                {
                    "datetime": date,
                    "instrument": "000001.SZ",
                    "amount": 1 if 1 <= index <= 4 else 0,
                },
                {
                    "datetime": date,
                    "instrument": "000002.SZ",
                    "amount": 1 if index >= 2 else 0,
                },
            ]
        )
    positions = pd.DataFrame.from_records(position_records)
    indicator = pd.DataFrame(
        {
            "datetime": [dates[1], dates[1]],
            "instrument": ["000001.SZ", "000002.SZ"],
            "amount": [100, 100],
            "deal_amount": [100, 0],
            "ffr": [1.0, 0.0],
            "reason_code": [None, "blocked_limit_up"],
        }
    )
    trades = pd.DataFrame(
        {
            "datetime": [dates[1]],
            "instrument": ["000001.SZ"],
            "side": ["buy"],
            "quantity": [100],
            "price": [10.0],
        }
    )
    result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(prices),
        predictions=_prediction_frame(),
        prices=prices,
        sectors=_sector_frame(prices),
        positions=positions,
        execution_evidence=ExecutionEvidenceBundle(indicator=indicator, trades=trades),
    )
    assert result.family_status["sector_regime"].status == FamilyComputationStatus.COMPUTED
    assert result.family_status["position_episode"].status == FamilyComputationStatus.COMPUTED
    assert result.family_status["order_fill"].status == FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
    assert result.family_status["execution_cause"].status == FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
    scopes = {metric["metric_scope"] for metric in result.metrics}
    assert {"signal_path", "sector_regime", "position_episode", "order_fill"}.issubset(scopes)


def test_missing_prediction_does_not_discard_position_episode() -> None:
    prices = _price_frame()
    dates = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))
    positions = pd.DataFrame(
        {
            "datetime": dates,
            "instrument": "000001.SZ",
            "amount": [0, 1, 1, 0] + [0] * (len(dates) - 4),
        }
    )
    result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(prices),
        predictions=None,
        prices=prices,
        positions=positions,
        execution_evidence=ExecutionEvidenceBundle(
            exit_signals=pd.DataFrame({"datetime": [dates[3]], "instrument": ["000001.SZ"]})
        ),
    )
    assert result.signal_observations.empty
    assert result.family_status["signal_path"].status == FamilyComputationStatus.NOT_COMPUTABLE
    assert result.family_status["position_episode"].status == FamilyComputationStatus.COMPUTED
    assert result.family_status["order_fill"].status == FamilyComputationStatus.COMPUTED
    assert result.holding_episodes.loc[0, "exit_execution_status"] == "filled_on_exit_signal_day"


def test_invalid_prediction_schema_is_family_local() -> None:
    invalid = _prediction_frame().copy()
    invalid.iloc[0, 0] = np.nan
    prices = _price_frame()
    result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(prices),
        predictions=invalid,
        prices=prices,
    )
    assert result.signal_observations.empty
    assert result.family_status["signal_path"].status == FamilyComputationStatus.NOT_COMPUTABLE
    assert result.family_status["signal_path"].reason_codes == ("QELT_PREDICTION_SCHEMA_INVALID",)


def test_prediction_dates_cannot_escape_feature_snapshot_into_outcome_extension() -> None:
    prices = _price_frame()
    dates = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))
    feature = QEDatasetSnapshotIdentity(
        snapshot_id="feature-v1",
        manifest_sha256="feature-sha",
        start_date=dates[0].date().isoformat(),
        end_date=dates[10].date().isoformat(),
    )
    outcome = QEDatasetSnapshotIdentity(
        snapshot_id="outcome-v2",
        manifest_sha256="outcome-sha",
        start_date=dates[0].date().isoformat(),
        end_date=dates[-1].date().isoformat(),
        lineage_parent_ids=(feature.snapshot_id,),
    )
    context = QELongTrendEvaluationContext(
        run_id="qe-extension-fixture",
        evaluator_source_sha256="source-sha",
        feature_snapshot=feature,
        outcome_snapshot=outcome,
        overlap_receipt=SnapshotOverlapParityReceipt(
            feature_snapshot_id=feature.snapshot_id,
            outcome_snapshot_id=outcome.snapshot_id,
            overlap_start=feature.start_date,
            overlap_end=feature.end_date,
            row_count=22,
            column_count=4,
            overlap_price_parity_sha256="overlap-sha",
            relation="verified_extension",
        ),
        input_artifact_hashes={"prediction_sha256": "prediction-sha"},
    )
    escaped_prediction = pd.DataFrame(
        {"score": [1.0]},
        index=pd.MultiIndex.from_tuples(
            [(dates[11], "000001.SZ")],
            names=["datetime", "instrument"],
        ),
    )
    result = QELongTrendEvaluationEngine().evaluate(
        context=context,
        predictions=escaped_prediction,
        prices=prices,
    )
    assert result.signal_observations.empty
    assert result.family_status["signal_path"].status == FamilyComputationStatus.NOT_COMPUTABLE
    assert result.family_status["signal_path"].reason_codes == (QELongTrendReason.PREDICTION_SCHEMA_INVALID.value,)


def test_entry_execution_status_uses_archived_evidence_without_daily_guessing() -> None:
    entry_date = pd.Timestamp("2026-01-06")
    instruments = [f"00000{index}.SZ" for index in range(1, 7)]
    observations = pd.DataFrame(
        {
            "signal_date": pd.Timestamp("2026-01-05"),
            "instrument": instruments,
            "entry_date": entry_date,
        }
    )
    indicator = pd.DataFrame(
        {
            "datetime": [entry_date, entry_date, entry_date],
            "instrument": [instruments[0], instruments[1], instruments[3]],
            "amount": [100, 100, 100],
            "deal_amount": [100, 50, 0],
            "ffr": [1.0, 0.5, 0.0],
        }
    )
    trades = pd.DataFrame(
        {
            "datetime": [entry_date, pd.Timestamp("2026-01-07")],
            "instrument": [instruments[0], instruments[2]],
            "side": ["buy", "buy"],
            "quantity": [100, 100],
            "price": [10.0, 10.5],
        }
    )
    orders = pd.DataFrame(
        {
            "datetime": [entry_date],
            "instrument": [instruments[4]],
            "attempted": [False],
        }
    )
    enriched = attach_entry_execution_evidence(
        observations,
        evidence=ExecutionEvidenceBundle(indicator=indicator, trades=trades, orders=orders),
        calendar=pd.date_range("2026-01-05", periods=4, freq="B"),
    )
    assert enriched["entry_execution_status"].tolist() == [
        "filled_t1",
        "partial_fill_t1",
        "delayed_fill",
        "never_filled",
        "not_attempted_by_strategy",
        "not_verifiable",
    ]
    assert enriched.loc[2, "entry_delay_days"] == 1
    assert enriched["entry_block_reason"].isna().all()

    conflicting_order = pd.DataFrame(
        {
            "datetime": [entry_date],
            "instrument": [instruments[0]],
            "attempted": [False],
        }
    )
    with pytest.raises(QELongTrendError) as exc_info:
        attach_entry_execution_evidence(
            observations.iloc[:1],
            evidence=ExecutionEvidenceBundle(
                indicator=indicator.iloc[:1],
                trades=trades.iloc[:1],
                orders=conflicting_order,
            ),
            calendar=pd.date_range("2026-01-05", periods=4, freq="B"),
        )
    assert exc_info.value.reason_code == "QELT_EXECUTION_BRIDGE_RECONCILIATION_FAILED"


def test_hierarchical_indicator_overfill_preserves_qlib_execution_semantics() -> None:
    """Qlib ffr is measured against the outer target and may legitimately exceed one."""

    dates = pd.date_range("2026-01-05", periods=4, freq="B")
    observations = pd.DataFrame(
        {
            "signal_date": [dates[0]],
            "instrument": ["600510.SH"],
            "entry_date": [dates[1]],
        }
    )
    indicator = pd.DataFrame(
        {
            "datetime": [dates[1]],
            "instrument": ["600510.SH"],
            "amount": [420_300.0],
            "inner_amount": [429_800.0],
            "deal_amount": [429_800.0],
            "ffr": [429_800.0 / 420_300.0],
        }
    )
    trade = pd.DataFrame(
        {
            "datetime": [dates[1]],
            "instrument": ["600510.SH"],
            "side": ["buy"],
            "quantity": [429_800.0],
            "price": [6.70],
        }
    )

    enriched = attach_entry_execution_evidence(
        observations,
        evidence=ExecutionEvidenceBundle(indicator=indicator, trades=trade),
        calendar=dates,
    )

    assert enriched.loc[0, "entry_execution_status"] == "filled_t1"
    assert enriched.loc[0, "entry_execution_evidence_level"] == "indicator_and_trade_reconciled"
    assert enriched.loc[0, "entry_target_amount"] == 420_300.0
    assert enriched.loc[0, "entry_inner_target_amount"] == 429_800.0
    assert enriched.loc[0, "entry_deal_amount"] == 429_800.0
    assert enriched.loc[0, "entry_fill_ratio"] == pytest.approx(429_800.0 / 420_300.0)
    assert enriched.loc[0, "entry_overfill_amount"] == 9_500.0

    execution_metric = compute_execution_metrics(enriched, pd.DataFrame())[0]
    assert execution_metric["value_json"]["entry_overfill_count"] == 1
    assert execution_metric["value_json"]["entry_fill_ratio"]["mean"] > 1.0

    impossible_inner_target = indicator.assign(inner_amount=429_700.0)
    with pytest.raises(QELongTrendError) as exc_info:
        attach_entry_execution_evidence(
            observations,
            evidence=ExecutionEvidenceBundle(indicator=impossible_inner_target),
            calendar=dates,
        )
    assert exc_info.value.reason_code == QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value

    inconsistent_ffr = indicator.assign(ffr=1.0)
    with pytest.raises(QELongTrendError) as exc_info:
        attach_entry_execution_evidence(
            observations,
            evidence=ExecutionEvidenceBundle(indicator=inconsistent_ffr),
            calendar=dates,
        )
    assert exc_info.value.reason_code == QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value


def test_hierarchical_indicator_overfill_is_preserved_for_exit_evidence() -> None:
    dates = pd.date_range("2026-01-05", periods=4, freq="B")
    instrument = "600510.SH"
    episodes = pd.DataFrame(
        {
            "instrument": [instrument],
            "entry_date": [dates[0]],
            "exit_date": [dates[2]],
        }
    )
    prices = pd.DataFrame(
        {
            "datetime": dates,
            "instrument": instrument,
            "close_qfq": [10.0, 10.5, 11.0, 10.8],
            "high_qfq": [10.2, 10.7, 11.2, 11.0],
            "low_qfq": [9.8, 10.3, 10.8, 10.6],
        }
    ).set_index(["datetime", "instrument"])
    indicator = pd.DataFrame(
        {
            "datetime": [dates[2]],
            "instrument": [instrument],
            "side": ["sell"],
            "amount": [100.0],
            "inner_amount": [105.0],
            "deal_amount": [105.0],
            "ffr": [1.05],
        }
    )
    trade = pd.DataFrame(
        {
            "datetime": [dates[2]],
            "instrument": [instrument],
            "side": ["sell"],
            "quantity": [105.0],
            "price": [11.0],
        }
    )
    exit_signal = pd.DataFrame({"datetime": [dates[2]], "instrument": [instrument]})

    enriched = attach_exit_execution_evidence(
        episodes,
        evidence=ExecutionEvidenceBundle(
            indicator=indicator,
            trades=trade,
            exit_signals=exit_signal,
        ),
        prices=prices,
        calendar=dates,
        evaluation_asof=dates[-1],
    )

    assert enriched.loc[0, "exit_execution_status"] == "filled_on_exit_signal_day"
    assert enriched.loc[0, "exit_execution_evidence_level"] == "indicator_and_exit_reconciled"
    assert enriched.loc[0, "exit_target_amount"] == 100.0
    assert enriched.loc[0, "exit_inner_target_amount"] == 105.0
    assert enriched.loc[0, "exit_deal_amount"] == 105.0
    assert enriched.loc[0, "exit_fill_ratio"] == 1.05
    assert enriched.loc[0, "exit_overfill_amount"] == 5.0


def test_episode_reconstruction_handles_exit_reentry_open_and_false_exit() -> None:
    dates = pd.date_range("2025-01-02", periods=190, freq="B")
    records = []
    amount_a = [0] * len(dates)
    amount_a[1:3] = [1, 1]
    amount_a[5:7] = [1, 1]
    amount_b = [0, 0] + [1] * (len(dates) - 2)
    for date, first, second in zip(dates, amount_a, amount_b):
        records.extend(
            [
                {"datetime": date, "instrument": "000001.SZ", "amount": first},
                {"datetime": date, "instrument": "000002.SZ", "amount": second},
            ]
        )
    positions = pd.DataFrame.from_records(records)

    price_records = []
    closes_a = [10.0 + 0.1 * index for index in range(len(dates))]
    closes_b = [10.0 + 0.05 * index for index in range(len(dates))]
    for instrument, closes in (("000001.SZ", closes_a), ("000002.SZ", closes_b)):
        for date, close in zip(dates, closes):
            price_records.append(
                {
                    "datetime": date,
                    "instrument": instrument,
                    "close_qfq": close,
                    "high_qfq": close + 0.2,
                    "low_qfq": close - 0.2,
                }
            )
    prices = pd.DataFrame.from_records(price_records).set_index(["datetime", "instrument"])
    episodes = reconstruct_holding_episodes(
        positions=positions,
        prices=prices,
        evaluation_asof=dates[-1],
        profile=QE_LONG_TREND_PROFILE_V1,
    )
    stock_a = episodes.loc[episodes["instrument"] == "000001.SZ"].sort_values("episode_seq")
    stock_b = episodes.loc[episodes["instrument"] == "000002.SZ"]
    assert stock_a["episode_seq"].tolist() == [1, 2]
    assert stock_a.iloc[0]["false_early_exit"] is True
    assert bool(stock_b.iloc[0]["open_censored"]) is True
    assert stock_b.iloc[0]["episode_maturity_state"] == "open_event_censored"
    assert pd.isna(stock_b.iloc[0]["episode_close_return_qfq"])


def test_zero_overlap_label_and_missing_high_low_are_explicit_limitations() -> None:
    prices = _price_frame()
    predictions = _prediction_frame().iloc[:2]
    engine = QELongTrendEvaluationEngine()
    observations = engine.build_signal_observations(predictions=predictions, prices=prices)
    empty_labels = observations.set_index(["signal_date", "instrument"])["return_20"].iloc[0:0]
    result = engine.evaluate(
        context=_evaluation_context(prices),
        predictions=predictions,
        prices=prices,
        labels=empty_labels,
        label_horizon=20,
    )
    parity = next(metric for metric in result.metrics if metric["metric_key"] == "label_parity")
    assert parity["quality_flag"] == "not_computable"
    assert parity["value_json"]["reason_code"] == QELongTrendReason.LABEL_PARITY_NO_OVERLAP.value

    missing_path_prices = prices.copy()
    missing_path_prices.loc[:, ["high_qfq", "low_qfq"]] = np.nan
    path_result = engine.evaluate(
        context=_evaluation_context(missing_path_prices),
        predictions=predictions,
        prices=missing_path_prices,
    )
    assert path_result.family_status["signal_path"].status == FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
    assert path_result.family_status["signal_path"].coverage["max_horizon_high_low_coverage"] == 0.0
    assert set(path_result.signal_observations["maturity_20"]) == {"matured"}
    assert set(path_result.signal_observations["path_quality_20"]) == {"path_incomplete"}
    assert path_result.signal_observations["path_mfe_20"].isna().all()


def test_trade_matching_is_one_to_one_and_conflicts_fail_fast() -> None:
    dates = pd.date_range("2026-01-05", periods=5, freq="B")
    observations = pd.DataFrame(
        {
            "signal_date": dates[:2],
            "instrument": ["000001.SZ", "000001.SZ"],
            "entry_date": dates[1:3],
        }
    )
    single_trade = pd.DataFrame(
        {
            "datetime": [dates[3]],
            "instrument": ["000001.SZ"],
            "side": ["buy"],
            "quantity": [100],
            "price": [10.0],
        }
    )
    ambiguous = attach_entry_execution_evidence(
        observations,
        evidence=ExecutionEvidenceBundle(trades=single_trade),
        calendar=dates,
    )
    assert ambiguous["entry_execution_status"].tolist() == ["not_verifiable", "not_verifiable"]
    assert ambiguous["entry_execution_evidence_level"].tolist() == [
        "ambiguous_trade_match",
        "ambiguous_trade_match",
    ]

    indicator = pd.DataFrame(
        {
            "datetime": [dates[1]],
            "instrument": ["000001.SZ"],
            "amount": [100],
            "deal_amount": [0],
            "ffr": [0.0],
        }
    )
    same_day_trade = single_trade.assign(datetime=dates[1])
    with pytest.raises(QELongTrendError) as exc_info:
        attach_entry_execution_evidence(
            observations.iloc[:1],
            evidence=ExecutionEvidenceBundle(indicator=indicator, trades=same_day_trade),
            calendar=dates,
        )
    assert exc_info.value.reason_code == QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value

    early_explicit_trade = pd.DataFrame(
        {
            "datetime": [dates[0]],
            "signal_date": [dates[0]],
            "instrument": ["000001.SZ"],
            "side": ["buy"],
            "quantity": [100],
        }
    )
    with pytest.raises(QELongTrendError) as exc_info:
        attach_entry_execution_evidence(
            observations.iloc[:1],
            evidence=ExecutionEvidenceBundle(trades=early_explicit_trade),
            calendar=dates,
        )
    assert exc_info.value.reason_code == QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value


def test_suspension_and_limit_state_remain_diagnostic_until_direct_order_reason() -> None:
    prices = _price_frame()
    dates = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))
    prices["suspend_d"] = False
    prices["limit_state"] = pd.NA
    prices.loc[(dates[1], "000001.SZ"), "volume_qfq"] = 0.0
    prices.loc[(dates[1], "000001.SZ"), "suspend_d"] = True
    prices.loc[(dates[1], "000001.SZ"), "limit_state"] = "limit_up"
    observations = QELongTrendEvaluationEngine().build_signal_observations(
        predictions=_prediction_frame().iloc[:2],
        prices=prices,
    )
    stock = observations.loc[observations["instrument"] == "000001.SZ"].iloc[[0]]
    assert bool(stock.iloc[0]["entry_suspension_diagnostic"]) is True
    assert stock.iloc[0]["entry_limit_state_diagnostic"] == "limit_up"
    assert stock.iloc[0]["entry_execution_status"] == "not_verifiable"

    indicator = pd.DataFrame(
        {
            "datetime": [dates[1]],
            "instrument": ["000001.SZ"],
            "amount": [100],
            "deal_amount": [0],
            "ffr": [0.0],
            "reason_code": ["blocked_limit_up"],
        }
    )
    enriched = attach_entry_execution_evidence(
        stock,
        evidence=ExecutionEvidenceBundle(indicator=indicator),
        calendar=dates,
    )
    assert enriched.iloc[0]["entry_execution_status"] == "never_filled"
    assert enriched.iloc[0]["entry_block_reason"] == "blocked_limit_up"
    assert pd.notna(enriched.iloc[0]["missed_mfe_due_to_entry_block"])
    assert bool(enriched.iloc[0]["missed_barrier_winner_due_to_entry_block"]) is True

    unknown_reason = indicator.assign(reason_code="guessed_from_daily_limit")
    with pytest.raises(QELongTrendError) as exc_info:
        attach_entry_execution_evidence(
            stock,
            evidence=ExecutionEvidenceBundle(indicator=unknown_reason),
            calendar=dates,
        )
    assert exc_info.value.reason_code == QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value


def test_exit_bridge_is_symmetric_and_records_block_loss() -> None:
    prices = _price_frame()
    dates = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))
    positions = pd.DataFrame(
        {
            "datetime": dates,
            "instrument": "000001.SZ",
            "amount": [0] + [1] * 4 + [0] * (len(dates) - 5),
        }
    )
    episodes = reconstruct_holding_episodes(
        positions=positions,
        prices=prices,
        evaluation_asof=dates[-1],
    )
    exit_signal = pd.DataFrame({"datetime": [dates[3]], "instrument": ["000001.SZ"]})
    orders = pd.DataFrame(
        {
            "datetime": [dates[3]],
            "instrument": ["000001.SZ"],
            "side": ["sell"],
            "attempted": [True],
            "reason_code": ["blocked_limit_down"],
        }
    )
    trades = pd.DataFrame(
        {
            "datetime": [dates[5]],
            "instrument": ["000001.SZ"],
            "side": ["sell"],
            "quantity": [100],
            "price": [10.6],
        }
    )
    enriched = attach_exit_execution_evidence(
        episodes,
        evidence=ExecutionEvidenceBundle(
            trades=trades,
            orders=orders,
            exit_signals=exit_signal,
        ),
        prices=prices,
        calendar=dates,
        evaluation_asof=dates[-1],
    )
    assert enriched.loc[0, "exit_execution_status"] == "delayed_exit"
    assert enriched.loc[0, "exit_delay_days"] == 2
    assert enriched.loc[0, "exit_block_reason"] == "blocked_limit_down"
    assert enriched.loc[0, "blocked_exit_extra_holding_days"] == 2
    assert pd.notna(enriched.loc[0, "blocked_exit_extra_drawdown"])


def test_instrument_exit_and_position_snapshot_gaps_are_not_silenced() -> None:
    prices = _price_frame()
    dates = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))
    prices.loc[(dates[21:], "000001.SZ"), ["close_qfq", "high_qfq", "low_qfq"]] = np.nan
    observations = QELongTrendEvaluationEngine().build_signal_observations(
        predictions=_prediction_frame().iloc[:2],
        prices=prices,
    )
    first = observations.loc[observations["instrument"] == "000001.SZ"].iloc[0]
    assert first["maturity_20"] == "instrument_exit_unresolved"

    complete_prices = _price_frame()
    complete_dates = pd.DatetimeIndex(sorted(complete_prices.index.get_level_values("datetime").unique()))
    positions = pd.DataFrame(
        {
            "datetime": complete_dates.delete(10),
            "instrument": "000001.SZ",
            "amount": 1,
        }
    )
    with pytest.raises(QELongTrendError) as exc_info:
        reconstruct_holding_episodes(
            positions=positions,
            prices=complete_prices,
            evaluation_asof=complete_dates[-1],
        )
    assert exc_info.value.reason_code == QELongTrendReason.EPISODE_RECONCILIATION_FAILED.value


def test_position_history_uses_its_own_asof_and_marks_left_censoring() -> None:
    prices = _price_frame()
    dates = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))
    position_dates = dates[:10]
    positions = pd.DataFrame(
        {
            "datetime": position_dates,
            "instrument": "000001.SZ",
            "amount": [1, 1, 1, 0, 0, 1, 1, 1, 1, 1],
        }
    )
    episodes = reconstruct_holding_episodes(
        positions=positions,
        prices=prices,
        evaluation_asof=dates[-1],
    )
    assert len(episodes) == 2
    assert bool(episodes.loc[0, "left_censored"])
    assert episodes.loc[0, "episode_maturity_state"] == "left_censored"
    assert pd.isna(episodes.loc[0, "entry_close_qfq"])
    assert not bool(episodes.loc[1, "left_censored"])
    assert bool(episodes.loc[1, "open_censored"])
    assert episodes.loc[1, "position_observation_end_date"] == position_dates[-1]
    assert episodes.loc[1, "extended_path_coverage"] == 1.0

    all_zero = positions.copy()
    all_zero["amount"] = 0
    no_episodes = reconstruct_holding_episodes(
        positions=all_zero,
        prices=prices,
        evaluation_asof=dates[-1],
    )
    assert no_episodes.empty
    assert set(("left_censored", "episode_quality_flags")).issubset(no_episodes.columns)
    no_position_result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(prices),
        predictions=None,
        prices=prices,
        positions=all_zero,
    )
    assert no_position_result.holding_episodes.empty
    assert no_position_result.family_status["position_episode"].status == FamilyComputationStatus.COMPUTED

    with pytest.raises(QELongTrendError) as exc_info:
        reconstruct_holding_episodes(
            positions=positions.iloc[0:0],
            prices=prices,
            evaluation_asof=dates[-1],
        )
    assert exc_info.value.reason_code == QELongTrendReason.EPISODE_RECONCILIATION_FAILED.value


def test_episode_capture_ratio_is_undefined_when_mfe_is_not_positive() -> None:
    dates = pd.date_range("2025-01-02", periods=190, freq="B")
    closes = np.linspace(10.0, 5.0, len(dates))
    prices = pd.DataFrame(
        {
            "datetime": dates,
            "instrument": "000001.SZ",
            "close_qfq": closes,
            "high_qfq": closes + 0.001,
            "low_qfq": closes - 0.001,
        }
    ).set_index(["datetime", "instrument"])
    positions = pd.DataFrame(
        {
            "datetime": dates,
            "instrument": "000001.SZ",
            "amount": [0.0] + [1.0] * 20 + [0.0] * (len(dates) - 21),
        }
    )

    episodes = reconstruct_holding_episodes(
        positions=positions,
        prices=prices,
        evaluation_asof=dates[-1],
    )
    assert len(episodes) == 1
    assert episodes.loc[0, "episode_mfe"] <= 0.0
    assert pd.isna(episodes.loc[0, "episode_capture_ratio"])
    assert "episode_capture_ratio_denominator_not_positive" in episodes.loc[
        0, "episode_quality_flags"
    ]


def test_execution_cause_coverage_only_requires_reasons_for_failed_events() -> None:
    filled_observations = pd.DataFrame(
        {
            "entry_execution_status": ["filled_t1"],
            "entry_block_reason": [None],
            "missed_mfe_due_to_entry_block": [np.nan],
        }
    )
    filled_episodes = pd.DataFrame(
        {
            "exit_execution_status": ["filled_on_exit_signal_day"],
            "exit_block_reason": [None],
            "blocked_exit_extra_drawdown": [np.nan],
        }
    )
    _, cause = _execution_family_statuses(filled_observations, filled_episodes)
    assert cause.status == FamilyComputationStatus.COMPUTED
    assert cause.coverage["cause_required_event_count"] == 0
    assert cause.coverage["direct_cause_coverage"] is None

    failed = filled_observations.copy()
    failed["entry_execution_status"] = "never_filled"
    _, unresolved = _execution_family_statuses(failed, filled_episodes)
    assert unresolved.status == FamilyComputationStatus.NOT_VERIFIABLE
    assert unresolved.coverage["unresolved_cause_count"] == 1

    direct_reason = filled_observations.copy()
    direct_reason["entry_execution_status"] = "not_verifiable"
    direct_reason["entry_block_reason"] = "blocked_limit_up"
    direct_reason["missed_mfe_due_to_entry_block"] = 0.2
    _, direct_cause = _execution_family_statuses(direct_reason, filled_episodes)
    assert direct_cause.status == FamilyComputationStatus.COMPUTED
    assert direct_cause.coverage["direct_cause_coverage"] == 1.0


def test_one_entry_signal_cannot_attach_to_two_position_episodes() -> None:
    dates = pd.date_range("2026-01-05", periods=4, freq="B")
    episodes = pd.DataFrame(
        {
            "instrument": ["000001.SZ", "000001.SZ"],
            "entry_date": [dates[1], dates[3]],
            "left_censored": [False, False],
            "entry_execution_status": ["not_verifiable", "not_verifiable"],
            "entry_execution_evidence_level": ["none", "none"],
            "actual_entry_date": [pd.NaT, pd.NaT],
            "actual_entry_price": [np.nan, np.nan],
            "entry_delay_days": [np.nan, np.nan],
            "entry_block_reason": [None, None],
        }
    )
    observations = pd.DataFrame(
        {
            "signal_date": [dates[0]],
            "instrument": ["000001.SZ"],
            "entry_date": [dates[1]],
            "actual_entry_date": [dates[3]],
            "stable_rank": [1],
            "entry_execution_status": ["delayed_fill"],
            "entry_execution_evidence_level": ["reconciled_trade"],
            "actual_entry_price": [10.5],
            "entry_delay_days": [2.0],
            "entry_block_reason": [None],
        }
    )
    enriched = attach_episode_entry_evidence(episodes, observations)
    assert enriched.loc[0, "entry_execution_status"] == "not_verifiable"
    assert enriched.loc[1, "entry_execution_status"] == "delayed_fill"


def test_indicator_and_trade_quantities_must_reconcile_on_both_sides() -> None:
    prices = _price_frame()
    dates = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))
    observations = pd.DataFrame(
        {
            "signal_date": [dates[0]],
            "instrument": ["000001.SZ"],
            "entry_date": [dates[1]],
        }
    )
    indicator = pd.DataFrame(
        {
            "datetime": [dates[1]],
            "instrument": ["000001.SZ"],
            "amount": [100.0],
            "deal_amount": [100.0],
            "ffr": [1.0],
        }
    )
    mismatched_buy = pd.DataFrame(
        {
            "datetime": [dates[1]],
            "instrument": ["000001.SZ"],
            "side": ["buy"],
            "quantity": [50.0],
        }
    )
    with pytest.raises(QELongTrendError) as exc_info:
        attach_entry_execution_evidence(
            observations,
            evidence=ExecutionEvidenceBundle(
                indicator=indicator,
                trades=mismatched_buy,
            ),
            calendar=dates,
        )
    assert exc_info.value.reason_code == QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value

    positions = pd.DataFrame(
        {
            "datetime": dates,
            "instrument": "000001.SZ",
            "amount": [0, 1, 1, 0] + [0] * (len(dates) - 4),
        }
    )
    episodes = reconstruct_holding_episodes(
        positions=positions,
        prices=prices,
        evaluation_asof=dates[-1],
    )
    exit_signal = pd.DataFrame({"datetime": [dates[3]], "instrument": ["000001.SZ"]})
    sell_indicator = indicator.assign(
        datetime=dates[3],
        side="sell",
    )
    mismatched_sell = mismatched_buy.assign(
        datetime=dates[3],
        side="sell",
    )
    with pytest.raises(QELongTrendError) as exc_info:
        attach_exit_execution_evidence(
            episodes,
            evidence=ExecutionEvidenceBundle(
                indicator=sell_indicator,
                trades=mismatched_sell,
                exit_signals=exit_signal,
            ),
            prices=prices,
            calendar=dates,
            evaluation_asof=dates[-1],
        )
    assert exc_info.value.reason_code == QELongTrendReason.EXECUTION_BRIDGE_RECONCILIATION_FAILED.value


def test_late_exit_is_excluded_from_false_early_exit_denominator() -> None:
    prices = _price_frame()
    dates = pd.DatetimeIndex(sorted(prices.index.get_level_values("datetime").unique()))
    amounts = [0] + [1] * 184 + [0] * (len(dates) - 185)
    positions = pd.DataFrame({"datetime": dates, "instrument": "000001.SZ", "amount": amounts})
    episodes = reconstruct_holding_episodes(
        positions=positions,
        prices=prices,
        evaluation_asof=dates[-1],
    )
    assert pd.isna(episodes.loc[0, "false_early_exit"])
    summary = compute_episode_metrics(episodes)[0]
    assert summary["value_json"]["false_early_exit_denominator"] == 0


def test_recall_baseline_uses_actual_eligible_topk_and_sector_switch_is_reported() -> None:
    frame = pd.DataFrame(
        {
            "signal_date": [pd.Timestamp("2026-01-05")] * 2,
            "stable_rank": [1, 3],
        }
    )
    assert _daily_recall_uplift(frame, pd.Series([True, True]), k=2) == [0.0]

    rows: list[dict[str, object]] = []
    dates = [pd.Timestamp("2026-01-05"), pd.Timestamp("2026-01-06")]
    sectors = [(1, 2), (3, 2)]
    for position, (date, sector_pair) in enumerate(zip(dates, sectors)):
        for rank, sector in enumerate(sector_pair, start=1):
            row: dict[str, object] = {
                "signal_date": date,
                "instrument": f"00000{rank}.SZ",
                "stable_rank": rank,
                "l2_code_id": sector,
                "signal_calendar_position": position,
                "evaluation_calendar_position": 1,
            }
            for horizon in QE_LONG_TREND_PROFILE_V1.horizons:
                row[f"maturity_{horizon}"] = "matured"
                row[f"return_{horizon}"] = 0.1
                row[f"close_mfe_{horizon}"] = 0.2
                row[f"close_mae_{horizon}"] = -0.1
                row[f"path_mfe_{horizon}"] = 0.25
                row[f"path_mae_{horizon}"] = -0.12
            for barrier in QE_LONG_TREND_PROFILE_V1.barriers:
                row[f"time_to_close_hit_{int(barrier * 100)}"] = 1.0
            rows.append(row)
    sector_metrics = QELongTrendEvaluationEngine().compute_sector_metrics(pd.DataFrame.from_records(rows))
    concentration = next(metric for metric in sector_metrics if metric["metric_key"] == "top50_sector_concentration")
    assert concentration["value_json"]["top1_sector_switch_rate"] == 1.0


def test_registered_profile_and_authoritative_portfolio_report_contract() -> None:
    unregistered = replace(QE_LONG_TREND_PROFILE_V1, profile_id="unregistered_override")
    with pytest.raises(QELongTrendError) as exc_info:
        QELongTrendEvaluationEngine(unregistered)
    assert exc_info.value.reason_code == QELongTrendReason.PROFILE_INVALID.value

    prices = _price_frame()
    for invalid_parameters in (
        {"strategy_topk": 51},
        {"label_horizon": 10},
    ):
        with pytest.raises(QELongTrendError) as exc_info:
            QELongTrendEvaluationEngine().evaluate(
                context=_evaluation_context(prices),
                predictions=_prediction_frame(),
                prices=prices,
                **invalid_parameters,
            )
        assert exc_info.value.reason_code == QELongTrendReason.PROFILE_INVALID.value

    report = pd.DataFrame(
        {
            "return": [0.01, -0.005, 0.02],
            "cost": [0.001, 0.001, 0.001],
            "turnover": [0.2, 0.1, 0.3],
        },
        index=pd.date_range("2026-01-05", periods=3, freq="B"),
    )
    metrics = compute_portfolio_metrics(report)
    assert metrics[0]["metric_scope"] == "portfolio_result"
    assert metrics[0]["value_json"]["trading_day_count"] == 3

    partial_metrics = compute_portfolio_metrics(report.loc[:, ["return"]])
    assert partial_metrics[0]["quality_flag"] == "computed_with_limitations"
    assert partial_metrics[0]["value_json"]["total_cost"] is None
    partial_result = QELongTrendEvaluationEngine().evaluate(
        context=_evaluation_context(prices),
        predictions=None,
        prices=prices,
        portfolio_report=report.loc[:, ["return"]],
    )
    assert partial_result.family_status["portfolio_result"].status == FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS
    assert partial_result.family_status["portfolio_result"].coverage["cost_coverage"] == 0.0

    zero_diagnostic_report = report.assign(cost=0.0, turnover=0.0)
    conflicted = compute_portfolio_metrics(
        zero_diagnostic_report,
        executed_trade_count=3,
    )[0]
    assert conflicted["quality_flag"] == "computed_with_limitations"
    assert conflicted["value_json"]["zero_diagnostics_conflict"] is True
    assert conflicted["value_json"]["total_cost"] is None
    assert conflicted["value_json"]["average_turnover"] is None

    empty = report.iloc[0:0]
    with pytest.raises(QELongTrendError) as exc_info:
        compute_portfolio_metrics(empty)
    assert exc_info.value.reason_code == QELongTrendReason.PORTFOLIO_REPORT_INVALID.value


def test_statistical_helpers_are_deterministic_and_monotone() -> None:
    values = [0.1, 0.2, -0.05, 0.3, 0.1, 0.2]
    first = moving_block_bootstrap_mean(values, block_length=2, samples=100, seed=7)
    second = moving_block_bootstrap_mean(values, block_length=2, samples=100, seed=7)
    assert first == second
    hac = newey_west_mean_test(values, lag=2)
    assert hac["n"] == len(values)
    assert hac["se"] is not None
    assert benjamini_hochberg([0.01, 0.04, 0.03, None]) == [0.03, 0.04, 0.04, None]
    assert benjamini_hochberg([None, float("nan")]) == [None, None]
    assert moving_block_bootstrap_mean([], block_length=2, samples=10, seed=1)["mean"] is None
    singleton = moving_block_bootstrap_mean([0.2], block_length=2, samples=10, seed=1)
    assert singleton["ci_low"] == singleton["ci_high"] == 0.2
    constant = newey_west_mean_test([0.1, 0.1, 0.1], lag=10)
    assert constant["p_value"] == 0.0
    assert newey_west_mean_test([], lag=2)["mean"] is None
    assert newey_west_mean_test([0.0, 0.0], lag=2)["p_value"] == 1.0
    matrix = np.asarray([[np.nan, 1.0], [np.nan, 2.0]])
    assert np.isnan(_safe_nan_extreme(matrix, kind="max")[0])
    assert _safe_nan_extreme(matrix, kind="min")[1] == 1.0
    with pytest.raises(ValueError):
        _safe_nan_extreme(matrix, kind="median")

    empty_execution = attach_entry_execution_evidence(
        pd.DataFrame(),
        evidence=None,
        calendar=pd.DatetimeIndex([]),
    )
    assert "entry_execution_status" in empty_execution.columns


def test_core_source_has_no_non_qe_runtime_imports() -> None:
    module_root = Path(long_trend_module.__file__).parent
    source = "\n".join(
        (module_root / name).read_text(encoding="utf-8")
        for name in (
            "long_trend_evaluation_contract.py",
            "long_trend_data_reader.py",
            "long_trend_evaluation.py",
        )
    )
    forbidden = (
        "backend.services.selection_center",
        "backend.services.advisory",
        "backend.services.paper_trading",
        "backend.infra.qmt_client",
        "backend.services.strategy_package",
        "PredictionArtifactStore",
    )
    for token in forbidden:
        assert token not in source
