from __future__ import annotations

import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.quality_contracts import QUALITY_SEEDS
from backend.services.advisory_model_first.quality_tournament import (
    prepare_model_matrix,
    run_quality_tournament,
    validate_quality_projection,
)


class _Dataset:
    def __init__(self, data, label, **kwargs) -> None:
        self.data = data
        self.label = label


class _Booster:
    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.best_iteration = 3

    def predict(self, matrix, num_iteration=None):
        return -np.asarray(matrix["parent_rank_pct"], dtype=float) + (self.seed % 7) * 1e-8


def _fake_lightgbm():
    def train(parameters, train_set, **kwargs):
        callbacks = kwargs["callbacks"]
        assert len(callbacks) == 3
        return _Booster(int(parameters["seed"]))

    return SimpleNamespace(
        Dataset=_Dataset,
        train=train,
        early_stopping=lambda **kwargs: ("early", kwargs),
        record_evaluation=lambda history: ("record", history),
        log_evaluation=lambda **kwargs: ("log", kwargs),
    )


def _projection() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-01", periods=251)
    rows = []
    for date_index, decision in enumerate(dates):
        split = "train" if date_index < 191 else "validation"
        for rank in range(1, 6):
            row = {column: 0.1 for column in MODEL_FEATURE_COLUMNS}
            row.update(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": decision + pd.offsets.BDay(1),
                    "instrument": f"{rank:06d}.SZ",
                    "program_id": "program",
                    "binding_version_id": "binding",
                    "package_id": "pkg",
                    "manifest_sha256": "1" * 64,
                    "selection_runtime_semantics_hash": "2" * 64,
                    "split": split,
                    "selection_effective_rank": rank,
                    "candidate_group_size": 5,
                    "parent_combined_score": float(6 - rank),
                    "parent_rank_pct": float(rank),
                    "l2_code_id": 1,
                    "relevance": 5 - rank,
                    "utility_5": float(6 - rank) / 100.0,
                    "stock_net_return_5": float(6 - rank) / 100.0,
                    "excess_return_5": float(6 - rank) / 100.0,
                    "path_mfe_5": 0.1,
                    "path_mae_loss_5": 0.01,
                }
            )
            rows.append(row)
    return pd.DataFrame(rows)


def test_tournament_executes_all_45_boosters_and_all_fusion_weights(monkeypatch) -> None:
    calls: list[int] = []
    fake = _fake_lightgbm()
    original_train = fake.train

    def counted_train(parameters, *args, **kwargs):
        calls.append(int(parameters["seed"]))
        return original_train(parameters, *args, **kwargs)

    fake.train = counted_train
    monkeypatch.setitem(sys.modules, "lightgbm", fake)
    progress = []
    result = run_quality_tournament(_projection(), progress_callback=progress.append)
    assert len(calls) == 45
    assert set(calls) == set(QUALITY_SEEDS)
    assert result.report["trial_count"] == 45
    assert result.report["weighted_candidate_count"] == 36
    assert len(result.report["candidates"]) == 37
    assert len(progress) == 9
    assert all(item["seed_count"] == 5 for item in progress)
    assert result.winner_row["candidate_id"] == "SELECTION_PRIOR_ONLY"
    assert result.winning_family is None


def test_projection_rejects_non_finite_target_instead_of_emitting_nan_metrics() -> None:
    projection = _projection()
    projection.loc[projection.index[0], "excess_return_5"] = np.nan

    with pytest.raises(AdvisoryModelFirstError) as raised:
        validate_quality_projection(projection, allowed_splits=("train", "validation"))

    assert raised.value.reason_code == "ADVISORY_M5_INPUT_IDENTITY_MISMATCH"


def test_model_matrix_rejects_infinite_feature_instead_of_passing_it_to_lightgbm() -> None:
    projection = _projection().iloc[:10].copy()
    projection.loc[projection.index[0], "parent_rank_pct"] = np.inf

    with pytest.raises(AdvisoryModelFirstError) as raised:
        prepare_model_matrix(projection, train_mask=pd.Series(True, index=projection.index))

    assert raised.value.reason_code == "ADVISORY_M5_INPUT_IDENTITY_MISMATCH"
