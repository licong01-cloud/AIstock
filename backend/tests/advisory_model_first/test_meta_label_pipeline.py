from __future__ import annotations

import os
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_pipeline import (
    _compare_reference_challenger,
    _verify_environment,
)
from backend.tests.advisory_model_first.test_meta_label_contracts import (
    _request,
    _return_aware_request,
)


def test_meta_label_pipeline_requires_wsl_without_import_failure() -> None:
    if os.name != "nt":
        pytest.skip("Windows fail-closed contract")
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        _verify_environment(_request())
    assert excinfo.value.reason_code == "ADVISORY_MODEL_TRAINING_REQUIRES_WSL"


def test_meta_label_pipeline_rejects_tracked_dirty_repository(monkeypatch) -> None:
    request = _request(repository_commit="a" * 40)
    monkeypatch.setenv("CONDA_DEFAULT_ENV", "rdagent-gpu")
    monkeypatch.setattr(
        "backend.services.advisory_model_first.meta_label_pipeline.os",
        SimpleNamespace(name="posix", getenv=os.getenv),
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.meta_label_pipeline.platform.release",
        lambda: "microsoft-standard-WSL2",
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.meta_label_pipeline.importlib.metadata.version",
        lambda _name: "4.6.0",
    )
    responses = iter(
        [
            SimpleNamespace(stdout="a" * 40 + "\n"),
            SimpleNamespace(stdout=" M backend/services/advisory_model_first/meta_label_pipeline.py\n"),
        ]
    )
    monkeypatch.setattr(
        "backend.services.advisory_model_first.meta_label_pipeline.subprocess.run",
        lambda *_args, **_kwargs: next(responses),
    )
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        _verify_environment(request)
    assert excinfo.value.reason_code == "ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH"


def test_reference_comparison_requires_and_pairs_exact_28_paths() -> None:
    paths = [f"path_{index:02d}" for index in range(28)]
    candidate = pd.DataFrame(
        {
            "family_id": ["FAMILY_CORE_HMM"] * 28,
            "seed": [20260813] * 28,
            "path_id": paths,
            "policy_mean_daily_net_excess_return_bps": [float(index + 1) for index in range(28)],
        }
    )
    reference_metrics = pd.DataFrame(
        {
            "family_id": ["FAMILY_CORE_HMM"] * 28,
            "seed": [20260817] * 28,
            "path_id": paths,
            "policy_mean_daily_net_excess_return_bps": [float(index) for index in range(28)],
        }
    )
    comparison = _compare_reference_challenger(
        request=_return_aware_request(),
        trial_metrics=candidate,
        winner_family_id="FAMILY_CORE_HMM",
        winner_seed=20260813,
        reference={
            "winner": {"family_id": "FAMILY_CORE_HMM", "seed": 20260817},
            "trial_metrics": reference_metrics,
        },
    )
    assert comparison["path_count"] == 28
    assert comparison["candidate_minus_reference_mean_primary_metric_bps"] == pytest.approx(1.0)
    assert comparison["candidate_path_win_rate"] == 1.0
    assert comparison["research_improvement"] is True

    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        _compare_reference_challenger(
            request=_return_aware_request(),
            trial_metrics=candidate.iloc[:-1],
            winner_family_id="FAMILY_CORE_HMM",
            winner_seed=20260813,
            reference={
                "winner": {"family_id": "FAMILY_CORE_HMM", "seed": 20260817},
                "trial_metrics": reference_metrics,
            },
        )
    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_REFERENCE_INVALID"
