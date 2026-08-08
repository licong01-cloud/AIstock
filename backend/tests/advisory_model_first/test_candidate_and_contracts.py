from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.candidate_group import build_runtime_equivalent_candidates
from backend.services.advisory_model_first.contracts import build_frozen_training_request
from backend.services.advisory_model_first.prediction_source import ExactPredictionSource
from backend.services.advisory_model_first.target_binding import (
    FUND_LEG_ID,
    LSTM_LEG_ID,
    RUNTIME_SEMANTICS_HASH,
    TERMINAL_WEIGHTS,
)
from backend.services.model_store.artifact_store import PredictionArtifactStore


def _frame(values: list[float], *, day: str = "2024-07-04") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": pd.to_datetime([day] * len(values)),
            "instrument": [f"{index:06d}.SZ" for index in range(1, len(values) + 1)],
            "score": values,
        }
    )


def test_runtime_equivalent_candidate_uses_population_zscore_and_stable_ties() -> None:
    result = build_runtime_equivalent_candidates(
        leg_frames={
            LSTM_LEG_ID: _frame([3.0, 2.0, 1.0]),
            FUND_LEG_ID: _frame([1.0, 2.0, 3.0]),
        },
        terminal_weights=TERMINAL_WEIGHTS,
        decision_dates=pd.to_datetime(["2024-07-04"]),
        trading_calendar=pd.to_datetime(["2024-07-04", "2024-07-05"]),
        identity={"selection_runtime_semantics_hash": RUNTIME_SEMANTICS_HASH},
        raw_top_k=3,
        target_count=3,
    )
    candidates = result.candidates
    assert candidates["instrument"].tolist() == ["000001.SZ", "000002.SZ", "000003.SZ"]
    assert candidates["selection_source_rank"].tolist() == [1, 2, 3]
    assert candidates["target_trade_date"].dt.strftime("%Y-%m-%d").unique().tolist() == ["2024-07-05"]
    assert result.coverage["candidate_count"].tolist() == [3]


def test_exact_prediction_source_reads_only_manifest_bound_blob(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {"score": [0.1, 0.2]},
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-07-04"), "000001.SZ"), (pd.Timestamp("2024-07-04"), "000002.SZ")],
            names=["datetime", "instrument"],
        ),
    )
    payload = io.BytesIO()
    frame.to_pickle(payload)
    payload.seek(0)
    store = PredictionArtifactStore(tmp_path)
    store.write_artifacts(run_key="run-a", files={"prediction": ("pred.pkl", payload)})

    source = ExactPredictionSource(tmp_path)
    descriptor = source.describe("run-a")
    loaded = source.load_scores("run-a")
    assert descriptor.row_count == 2
    assert loaded["instrument"].tolist() == ["000001.SZ", "000002.SZ"]


def test_frozen_request_hash_excludes_created_at_and_output_root() -> None:
    common = {
        "package_id": "pkg",
        "manifest_sha256": "a" * 64,
        "package_asset_closure_hash": "b" * 64,
        "program_id": "program",
        "binding_version_id": "binding",
        "style_profile_id": "style",
        "style_profile_hash": "c" * 64,
        "effective_package_oos_cutoff": "2026-07-20",
        "selection_runtime_semantics_id": "runtime",
        "selection_runtime_semantics_hash": "d" * 64,
        "selection_runtime_semantics": {"version": 1},
        "representative_seed_run_ids": {"leg": "run"},
        "representative_model_asset_sha256": {"leg": "e" * 64},
        "full_seed_roster": {"leg": ("run",)},
        "prediction_artifacts": {},
        "terminal_weights": {"leg": 1.0},
        "combined_reference_path": "/data/combined.pkl",
        "combined_reference_sha256": "f" * 64,
        "qlib_daily_root": "/data/qlib",
        "factor_data_root": "/data/factors",
        "suspend_data_root": "/data/suspend",
        "prediction_store_root": "/data/prediction-store",
        "repository_root": "/repo",
        "repository_commit": "1" * 40,
    }
    first = build_frozen_training_request(**common, output_root="/out/a", created_at="2026-08-08T00:00:00Z")
    second = build_frozen_training_request(**common, output_root="/out/b", created_at="2026-08-08T01:00:00Z")
    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    with pytest.raises(ValueError, match="request_sha256 mismatch"):
        first.model_copy(update={"request_sha256": "0" * 64}).model_validate(
            first.model_copy(update={"request_sha256": "0" * 64}).model_dump()
        )
