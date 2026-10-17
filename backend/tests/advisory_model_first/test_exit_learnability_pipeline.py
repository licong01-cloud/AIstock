from __future__ import annotations

from itertools import combinations

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.entry_exit_formal_contracts import (
    ActionSupportSpecV1,
)
from backend.services.advisory_model_first.exit_learnability_contracts import (
    EXIT_CATEGORICAL_FEATURE_COLUMNS,
    EXIT_FEATURE_COLUMNS,
    ExitLearnabilityInferenceSpecV1,
    ExitLearnabilityModelSpecV1,
    FrozenAdvisoryN2ExitLearnabilityRequestV1,
)
from backend.services.advisory_model_first.exit_learnability_pipeline import (
    build_exit_feature_matrix,
    evaluate_exit_policy,
    run_exit_crossfit,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    Tier1EvidenceState,
)


def _request() -> FrozenAdvisoryN2ExitLearnabilityRequestV1:
    return FrozenAdvisoryN2ExitLearnabilityRequestV1.model_construct(
        shadow_policy_sha256="a" * 64,
        cost_policy_sha256="b" * 64,
        intervention_policy_sha256="c" * 64,
        model_spec=ExitLearnabilityModelSpecV1(),
        inference_spec=ExitLearnabilityInferenceSpecV1(),
        support_spec=ActionSupportSpecV1(),
    )


def _market_frame(
    dates: pd.DatetimeIndex,
    instrument: str,
    closes: np.ndarray,
) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "datetime": dates,
            "instrument": instrument,
            "open": closes * 0.995,
            "high": closes * 1.01,
            "low": closes * 0.99,
            "close": closes,
            "volume": np.linspace(1_000_000.0, 2_000_000.0, len(dates)),
        }
    )
    return frame.set_index(["datetime", "instrument"])


def test_feature_builder_is_invariant_to_post_review_market_poison() -> None:
    dates = pd.bdate_range("2024-06-03", periods=45)
    entry = dates[5]
    review = dates[30]
    stock = _market_frame(dates, "000001.SZ", np.linspace(10.0, 12.0, len(dates)))
    benchmark = _market_frame(dates, "SH000300", np.linspace(100.0, 103.0, len(dates)))[["open", "close"]]
    labels = pd.DataFrame(
        [
            {
                "label_id": "label-1",
                "episode_id": "episode-1",
                "decision_date": review,
                "target_action_date": dates[31],
                "instrument": "000001.SZ",
                "status": "AVAILABLE",
                "incremental_net_value_bps": 25.0,
                "baseline_policy_sha256": "a" * 64,
                "intervention_policy_sha256": "c" * 64,
                "cost_policy_sha256": "b" * 64,
            }
        ]
    )
    metadata = pd.DataFrame(
        [
            {
                "episode_id": "episode-1",
                "entry_decision_date": dates[4],
                "entry_trade_date": entry,
                "instrument": "000001.SZ",
                "entry_price": 10.1,
                "selection_rank": 1,
                "selection_score": 2.5,
            }
        ]
    )
    baseline = build_exit_feature_matrix(
        exit_labels=labels,
        episode_metadata=metadata,
        daily=stock,
        benchmark_daily=benchmark,
        request=_request(),
    )
    poisoned_stock = stock.copy()
    poisoned_benchmark = benchmark.copy()
    poisoned_stock.loc[(slice(dates[31], None), slice(None)), :] = 999999.0
    poisoned_benchmark.loc[(slice(dates[31], None), slice(None)), :] = 999999.0
    poisoned = build_exit_feature_matrix(
        exit_labels=labels,
        episode_metadata=metadata,
        daily=poisoned_stock,
        benchmark_daily=poisoned_benchmark,
        request=_request(),
    )

    pd.testing.assert_frame_equal(baseline, poisoned)
    assert baseline.loc[0, "holding_trading_days_elapsed"] == 26
    assert baseline.loc[0, "market_regime"] == "UP_OR_FLAT"


def test_crossfit_uses_all_28_paths_and_seven_predictions_per_row() -> None:
    dates = pd.bdate_range("2025-01-02", periods=8)
    feature_rows: list[dict[str, object]] = []
    label_rows: list[dict[str, object]] = []
    for day_index, entry_day in enumerate(dates):
        episode = f"episode-{day_index}"
        for review_index in range(2):
            label_id = f"label-{day_index}-{review_index}"
            row: dict[str, object] = {
                "label_id": label_id,
                "episode_id": episode,
                "entry_decision_date": entry_day,
                "entry_trade_date": entry_day,
                "review_decision_date": entry_day + pd.Timedelta(days=review_index),
                "target_action_date": entry_day + pd.Timedelta(days=review_index + 1),
                "instrument": f"0000{day_index:02d}.SZ",
                "label_status": "AVAILABLE",
                "missing_numeric_feature_count": 0,
            }
            for feature_index, column in enumerate(EXIT_FEATURE_COLUMNS):
                row[column] = (
                    "UP_OR_FLAT"
                    if column in EXIT_CATEGORICAL_FEATURE_COLUMNS
                    else float(day_index + review_index + feature_index / 100.0)
                )
            feature_rows.append(row)
            label_rows.append(
                {
                    "label_id": label_id,
                    "incremental_net_value_bps": float(day_index * 10 + review_index),
                    "status": "AVAILABLE",
                }
            )
    paths = []
    for path_index, validation_indexes in enumerate(combinations(range(8), 2)):
        validation = [dates[index].date().isoformat() for index in validation_indexes]
        train = [value.date().isoformat() for index, value in enumerate(dates) if index not in validation_indexes]
        paths.append(
            {
                "path_id": f"path-{path_index}",
                "status": "READY",
                "train_dates": train,
                "validation_dates": validation,
            }
        )
    # A legitimate unavailable review row remains in the OOF matrix but must
    # never be used as a training target or silently converted to zero.
    label_rows[0]["incremental_net_value_bps"] = None
    label_rows[0]["status"] = "DATA_UNAVAILABLE"
    feature_rows[0]["label_status"] = "DATA_UNAVAILABLE"

    oof = run_exit_crossfit(
        features=pd.DataFrame(feature_rows),
        exit_labels=pd.DataFrame(label_rows),
        cpcv_payload={"paths": paths},
        request=_request(),
    )

    assert len(oof) == 16
    assert oof["oof_prediction_count"].eq(7).all()
    assert np.isfinite(oof["predicted_exit_advantage_bps"]).all()
    assert pd.isna(oof.loc[oof["label_id"].eq("label-0-0"), "incremental_net_value_bps"]).all()


def test_policy_uses_first_threshold_crossing_not_max_prediction(monkeypatch) -> None:
    entry_days = pd.bdate_range("2025-01-02", periods=60)
    rows: list[dict[str, object]] = []
    for day_index, entry_day in enumerate(entry_days):
        for slot in range(5):
            episode = f"episode-{day_index}-{slot}"
            first_review = entry_day + pd.Timedelta(hours=1)
            second_review = entry_day + pd.Timedelta(hours=2)
            rows.extend(
                [
                    {
                        "label_id": f"{episode}-first",
                        "episode_id": episode,
                        "entry_decision_date": entry_day,
                        "review_decision_date": first_review,
                        "target_action_date": entry_day + pd.Timedelta(days=1),
                        "instrument": f"0000{slot:02d}.SZ",
                        "predicted_exit_advantage_bps": 10.0,
                        "oof_prediction_count": 7,
                        "incremental_net_value_bps": -20.0,
                        "label_status": "AVAILABLE",
                    },
                    {
                        "label_id": f"{episode}-second",
                        "episode_id": episode,
                        "entry_decision_date": entry_day,
                        "review_decision_date": second_review,
                        "target_action_date": entry_day + pd.Timedelta(days=2),
                        "instrument": f"0000{slot:02d}.SZ",
                        "predicted_exit_advantage_bps": 100.0,
                        "oof_prediction_count": 7,
                        "incremental_net_value_bps": 200.0,
                        "label_status": "AVAILABLE",
                    },
                ]
            )

    def fake_regimes(_frame, decisions):
        ordered = sorted(pd.DatetimeIndex(decisions).normalize().unique())
        return {value: "UP_OR_FLAT" if index % 2 == 0 else "DOWN" for index, value in enumerate(ordered)}

    monkeypatch.setattr(
        "backend.services.advisory_model_first.exit_learnability_pipeline.build_tier1_benchmark_regimes",
        fake_regimes,
    )
    episode, daily, inference, support, diagnostics = evaluate_exit_policy(
        oof=pd.DataFrame(rows),
        benchmark_daily=pd.DataFrame(),
        oracle_summary={"mean_oracle_lift_bps": 386.0},
        request=_request(),
    )

    assert episode["selected_label_id"].str.endswith("-first").all()
    assert episode["realized_incremental_net_value_bps"].eq(-20.0).all()
    assert daily["policy_lift_bps"].eq(-20.0).all()
    assert inference.evidence_state == Tier1EvidenceState.LOW
    assert support.support_sufficient is True
    assert diagnostics["episode"]["intervened_mean_realized_lift_bps"] == -20.0
