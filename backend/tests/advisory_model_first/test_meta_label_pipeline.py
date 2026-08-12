from __future__ import annotations

import os

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
