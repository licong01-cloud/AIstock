from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.candidate_group import build_runtime_equivalent_candidates
from backend.services.advisory_model_first.contracts import build_frozen_training_request
from backend.services.advisory_model_first.diagnostics import _ensemble_mean
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.labels import filter_labels_for_purged_split
from backend.services.advisory_model_first.model_bundle import publish_model_bundle
from backend.services.advisory_model_first.reranker_training import (
    RerankerTrainingResult,
    _coerce_numeric_feature_dtypes,
)
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID, TERMINAL_WEIGHTS
from backend.services.advisory_model_first.time_split import PurgedDateSplit


def _leg(values: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": pd.Timestamp("2024-07-04"),
            "instrument": [f"{index:06d}.SZ" for index in range(1, len(values) + 1)],
            "score": values,
        }
    )


def test_candidate_identity_does_not_depend_on_mapping_insertion_order() -> None:
    common = {
        "terminal_weights": {FUND_LEG_ID: TERMINAL_WEIGHTS[FUND_LEG_ID], LSTM_LEG_ID: TERMINAL_WEIGHTS[LSTM_LEG_ID]},
        "decision_dates": [pd.Timestamp("2024-07-04")],
        "trading_calendar": pd.to_datetime(["2024-07-04", "2024-07-05"]),
        "identity": {"program_id": "program"},
        "raw_top_k": 3,
        "target_count": 3,
    }
    reversed_result = build_runtime_equivalent_candidates(
        leg_frames={FUND_LEG_ID: _leg([1.0, 2.0, 3.0]), LSTM_LEG_ID: _leg([3.0, 2.0, 1.0])},
        **common,
    )
    normal_result = build_runtime_equivalent_candidates(
        leg_frames={LSTM_LEG_ID: _leg([3.0, 2.0, 1.0]), FUND_LEG_ID: _leg([1.0, 2.0, 3.0])},
        **common,
    )
    assert reversed_result.candidates["instrument"].tolist() == normal_result.candidates["instrument"].tolist()
    assert reversed_result.candidates["combined_score"].tolist() == normal_result.candidates["combined_score"].tolist()


class _IntersectingPredictionSource:
    def load_scores(self, run_id: str, **_: object) -> pd.DataFrame:
        rows = {
            "run-a": [("000001.SZ", 1.0), ("000002.SZ", 2.0), ("000003.SZ", 3.0)],
            "run-b": [("000002.SZ", 4.0), ("000003.SZ", 5.0), ("000004.SZ", 6.0)],
        }[run_id]
        return pd.DataFrame(
            {
                "trade_date": pd.Timestamp("2024-07-04"),
                "instrument": [item[0] for item in rows],
                "score": [item[1] for item in rows],
            }
        )


def test_full_seed_diagnostic_uses_formal_inner_intersection() -> None:
    result = _ensemble_mean(
        _IntersectingPredictionSource(),
        run_ids=["run-a", "run-b"],
        decision_dates=[pd.Timestamp("2024-07-04")],
    )
    assert result["instrument"].tolist() == ["000002.SZ", "000003.SZ"]
    assert result["score"].tolist() == [3.0, 4.0]


def test_purge_boundary_exclusion_recomputes_group_relevance() -> None:
    decision = pd.Timestamp("2024-07-04")
    labels = pd.DataFrame(
        {
            "decision_as_of_trade_date": decision,
            "target_trade_date": pd.Timestamp("2024-07-05"),
            "instrument": [f"{index:06d}.SZ" for index in range(1, 7)],
            "label_status": "MATURE_EXECUTABLE",
            "actual_exit_date": pd.to_datetime(
                ["2024-07-08", "2024-07-08", "2024-07-08", "2024-07-08", "2024-07-08", "2024-07-15"]
            ),
            "utility_5": [0.0, 1.0, 2.0, 3.0, 4.0, 100.0],
            "group_label_status": "AVAILABLE",
            "relevance": [0, 0, 1, 2, 3, 4],
        }
    )
    split = PurgedDateSplit(
        train=(decision,),
        purge_1=(pd.Timestamp("2024-07-09"),),
        validation=(pd.Timestamp("2024-07-10"),),
        purge_2=(pd.Timestamp("2024-07-11"),),
        test=(pd.Timestamp("2024-07-12"),),
    )
    result = filter_labels_for_purged_split(labels, split, data_cutoff="2024-07-31")
    eligible = result[result["group_label_status"] == "AVAILABLE"]
    assert eligible["relevance"].astype(int).tolist() == [0, 1, 2, 3, 4]
    assert result.iloc[-1]["group_label_status"] == "CROSSES_PURGE_BOUNDARY"


class _FakeBooster:
    best_iteration = 1

    def save_model(self, path: str, *, num_iteration: int) -> None:
        assert num_iteration == 1
        Path(path).write_text("fake-lightgbm-model", encoding="ascii")


def _bundle_request(output_root: Path):
    return build_frozen_training_request(
        package_id="pkg",
        manifest_sha256="a" * 64,
        package_asset_closure_hash="b" * 64,
        program_id="program",
        binding_version_id="binding",
        style_profile_id="style",
        style_profile_hash="c" * 64,
        effective_package_oos_cutoff="2026-07-20",
        selection_runtime_semantics_id="runtime",
        selection_runtime_semantics_hash="d" * 64,
        selection_runtime_semantics={"normalization_method": "zscore"},
        representative_seed_run_ids={"leg": "run"},
        representative_model_asset_sha256={"leg": "e" * 64},
        full_seed_roster={"leg": ("run",)},
        prediction_artifacts={},
        terminal_weights={"leg": 1.0},
        combined_reference_path="/data/combined.pkl",
        combined_reference_sha256="f" * 64,
        qlib_daily_root="/data/qlib",
        factor_data_root="/data/factors",
        suspend_data_root="/data/suspend",
        prediction_store_root="/data/prediction-store",
        repository_root="/repo",
        repository_commit="1" * 40,
        output_root=str(output_root),
        created_at="2026-08-08T00:00:00Z",
    )


def test_existing_bundle_reuse_rejects_corrupt_file(tmp_path: Path) -> None:
    request = _bundle_request(tmp_path)
    split = PurgedDateSplit(
        train=(pd.Timestamp("2024-07-04"),),
        purge_1=(pd.Timestamp("2024-07-05"),),
        validation=(pd.Timestamp("2024-07-08"),),
        purge_2=(pd.Timestamp("2024-07-09"),),
        test=(pd.Timestamp("2024-07-10"),),
    )
    training = RerankerTrainingResult(
        booster=_FakeBooster(),
        feature_names=("feature",),
        categorical_vocabulary={},
        evaluation_history={"validation": {"ndcg@5": [1.0]}},
        metrics={"status": "available", "test_date_count": 1},
        test_predictions=pd.DataFrame({"instrument": ["000001.SZ"]}),
        baseline_comparison={"model_top5": {"status": "available"}},
    )
    arguments = {
        "model_root": tmp_path,
        "request": request,
        "split": split,
        "hmm_models": {"models": {"sector": {"state": 1}}},
        "hmm_unavailable": (),
        "training": training,
        "diagnostics": {"status": "available"},
        "schema_receipt": {"schema_hash": "schema"},
        "environment_report": {"lightgbm_version": "test"},
        "resource_report": {"peak_rss_bytes": 1},
    }
    bundle_id, bundle_path, manifest = publish_model_bundle(**arguments)
    assert manifest["bundle_id"] == bundle_id
    assert manifest["full_seed_roster"] == {"leg": ["run"]}
    (bundle_path / "model.txt").write_text("corrupt", encoding="ascii")

    with pytest.raises(AdvisoryModelFirstError) as error:
        publish_model_bundle(**arguments)
    assert error.value.reason_code == "ADVISORY_MODEL_BUNDLE_INVALID"


def test_numeric_object_feature_is_strictly_normalized_for_lightgbm() -> None:
    numeric = _coerce_numeric_feature_dtypes(
        pd.DataFrame({"market_up_ratio": [Decimal("0.5"), Decimal("0.75")]})
    )
    assert pd.api.types.is_float_dtype(numeric["market_up_ratio"])

    with pytest.raises(AdvisoryModelFirstError) as error:
        _coerce_numeric_feature_dtypes(pd.DataFrame({"market_up_ratio": ["not-a-number"]}))
    assert error.value.reason_code == "ADVISORY_MODEL_QE_SCHEMA_MISMATCH"
