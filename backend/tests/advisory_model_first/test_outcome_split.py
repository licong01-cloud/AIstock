from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.outcome_split import fixed_406_outcome_split


def test_outcome_split_is_226_25_50_25_80_without_overlap() -> None:
    dates = pd.bdate_range("2024-01-01", periods=406)
    split = fixed_406_outcome_split(dates)

    assert [len(split.train), len(split.purge_1), len(split.validation), len(split.purge_2), len(split.test)] == [
        226,
        25,
        50,
        25,
        80,
    ]
    flattened = [*split.train, *split.purge_1, *split.validation, *split.purge_2, *split.test]
    assert flattened == list(dates)
    assert len(set(flattened)) == 406


def test_outcome_split_rejects_any_other_date_count() -> None:
    with pytest.raises(AdvisoryModelFirstError) as error:
        fixed_406_outcome_split(pd.bdate_range("2024-01-01", periods=405))
    assert error.value.reason_code == "ADVISORY_OUTCOME_REQUEST_INVALID"
