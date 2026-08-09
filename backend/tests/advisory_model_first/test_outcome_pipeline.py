from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.outcome_contracts import OutcomeInputArtifactV1
from backend.services.advisory_model_first.outcome_pipeline import _read_bound_parquet
from backend.services.advisory_model_first.prediction_source import sha256_file


def test_outcome_parquet_readback_requires_exact_frozen_identity(tmp_path: Path) -> None:
    path = tmp_path / "features.parquet"
    frame = pd.DataFrame({"instrument": ["000001.SZ"], "score": [0.5]})
    frame.to_parquet(path, index=False)
    descriptor = OutcomeInputArtifactV1(
        path=str(path),
        sha256=sha256_file(path),
        size_bytes=path.stat().st_size,
        row_count=1,
        columns=tuple(frame.columns),
    )

    pd.testing.assert_frame_equal(_read_bound_parquet(descriptor), frame)
    path.write_bytes(path.read_bytes() + b"tamper")
    with pytest.raises(AdvisoryModelFirstError) as error:
        _read_bound_parquet(descriptor)
    assert error.value.reason_code == "ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH"
