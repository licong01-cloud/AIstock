from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_pipeline import _verify_environment
from backend.tests.advisory_model_first.test_meta_label_contracts import _request


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
