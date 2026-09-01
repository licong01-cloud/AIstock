from __future__ import annotations

import os
from pathlib import Path

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_dataset_pipeline import _verify_environment
from backend.tests.advisory_model_first.test_policy_contracts import _request


def test_policy_dataset_pipeline_imports_on_windows_but_execution_requires_wsl(tmp_path) -> None:
    request = _request(repository_root=str(tmp_path), output_root=str(tmp_path / "output"))
    if os.name != "nt":
        pytest.skip("Windows-specific diagnostic import contract")
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        _verify_environment(request)
    assert excinfo.value.reason_code == "ADVISORY_MODEL_TRAINING_REQUIRES_WSL"


def test_policy_dataset_pipeline_has_no_feature_or_database_stage() -> None:
    source = Path(__file__).resolve().parents[2] / "services" / "advisory_model_first" / "policy_dataset_pipeline.py"
    text = source.read_text(encoding="utf-8")
    assert "build_advisory_feature_matrix" not in text
    assert "fit_fresh_sector_hmm" not in text
    assert "pg_pool" not in text
    assert "validate_factor_file_schemas" not in text
