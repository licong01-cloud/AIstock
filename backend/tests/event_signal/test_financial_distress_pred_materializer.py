import datetime as dt
import pickle

import pandas as pd

from backend.services.event_signal.financial_distress_pred_materializer import (
    build_rank_date_penalties,
    candidate_scores_from_prediction,
    load_prediction_pickle,
    materialize_from_files,
    materialize_score_down_prediction,
    trading_days_from_prediction,
)


def _prediction_frame():
    index = pd.MultiIndex.from_product(
        [pd.to_datetime(["2024-01-02", "2024-01-03"]), ["A", "BAD", "C", "D"]],
        names=["datetime", "instrument"],
    )
    return pd.DataFrame({"score": [0.9, 0.8, 0.7, 0.6, 0.9, 0.8, 0.7, 0.6]}, index=index)


def _overlay_frame():
    return pd.DataFrame(
        [
            {
                "trade_date": dt.date(2024, 1, 3),
                "ts_code": "BAD",
                "can_buy": False,
                "force_exit": False,
                "active_trading_days": 5,
                "active_signal_count": 1,
                "max_loss_to_market_cap": None,
                "max_miss_gap": None,
                "loss_report_count_730d_max": 0,
                "prior_loss_report_count_730d_max": 0,
                "min_active_age_trading_days": 0,
                "earliest_effective_trade_date": dt.date(2024, 1, 3),
                "industries": "software",
                "market_cap_buckets": "mv_10bn_to_30bn_yuan",
                "source_signal_ids": "101",
                "event_types": "financial_indicator_large_decline",
            }
        ]
    )


def test_materialize_score_down_prediction_reorders_scores_and_traces_topk_drop():
    pred = _prediction_frame()

    adjusted, trace, metrics = materialize_score_down_prediction(
        pred,
        rank_date_penalties={dt.date(2024, 1, 2): {"BAD": 0.50}},
        top_k=3,
    )

    by_symbol = adjusted.loc[pd.Timestamp("2024-01-02")]["score"].to_dict()
    assert by_symbol == {"A": 0.9, "BAD": 0.6, "C": 0.8, "D": 0.7}
    bad_trace = next(row for row in trace if row["ts_code"] == "BAD")
    assert bad_trace["original_rank"] == 2
    assert bad_trace["adjusted_rank"] == 4
    assert bad_trace["dropped_from_topk"] is True
    assert metrics["topk_drop_count"] == 1
    assert metrics["rank_dates_touched"] == 1


def test_materialize_score_down_prediction_supports_non_string_score_column():
    pred = _prediction_frame().rename(columns={"score": 0})

    adjusted, trace, metrics = materialize_score_down_prediction(
        pred,
        rank_date_penalties={dt.date(2024, 1, 2): {"BAD": 0.50}},
        top_k=3,
    )

    assert float(adjusted.loc[(pd.Timestamp("2024-01-02"), "BAD"), 0]) == 0.6
    assert next(row for row in trace if row["ts_code"] == "BAD")["dropped_from_topk"] is True
    assert metrics["score_column"] == 0


def test_build_rank_date_penalties_maps_trade_date_to_previous_prediction_date():
    pred = _prediction_frame()
    overlay = _overlay_frame()
    candidate_scores = candidate_scores_from_prediction(pred)
    trading_days = trading_days_from_prediction(pred)

    rank_date_penalties, penalty_trace = build_rank_date_penalties(
        overlay,
        candidate_scores=candidate_scores,
        trading_days=trading_days,
        context_profile_key="rank_decay_balanced",
        top_k=3,
        ranking_date_mode="previous",
    )

    assert set(rank_date_penalties) == {dt.date(2024, 1, 2)}
    assert rank_date_penalties[dt.date(2024, 1, 2)]["BAD"] > 0
    assert penalty_trace[0]["trade_date"] == "2024-01-03"
    assert penalty_trace[0]["rank_date"] == "2024-01-02"


def test_materialize_from_files_writes_adjusted_prediction_and_audit_files(tmp_path):
    pred_path = tmp_path / "pred.pkl"
    overlay_path = tmp_path / "overlay.csv"
    output_path = tmp_path / "adjusted_pred.pkl"
    trace_path = tmp_path / "trace.csv"
    meta_path = tmp_path / "meta.json"
    report_path = tmp_path / "report.md"
    with pred_path.open("wb") as fh:
        pickle.dump(_prediction_frame(), fh)
    _overlay_frame().to_csv(overlay_path, index=False)

    payload = materialize_from_files(
        prediction_pkl=pred_path,
        overlay_csv=overlay_path,
        output_pkl=output_path,
        trace_csv=trace_path,
        meta_json=meta_path,
        report_md=report_path,
        context_profile_key="rank_decay_balanced",
        top_k=3,
        ranking_date_mode="previous",
    )

    adjusted = load_prediction_pickle(output_path)
    assert output_path.exists()
    assert trace_path.exists()
    assert meta_path.exists()
    assert report_path.exists()
    assert payload["metrics"]["penalized_symbol_count"] == 1
    assert float(adjusted.loc[(pd.Timestamp("2024-01-02"), "BAD"), "score"]) < 0.8
