from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


@dataclass(frozen=True)
class OutcomeDateSplit:
    train: tuple[pd.Timestamp, ...]
    purge_1: tuple[pd.Timestamp, ...]
    validation: tuple[pd.Timestamp, ...]
    purge_2: tuple[pd.Timestamp, ...]
    test: tuple[pd.Timestamp, ...]

    def as_dict(self) -> dict[str, list[str]]:
        return {
            name: [pd.Timestamp(value).date().isoformat() for value in getattr(self, name)]
            for name in ("train", "purge_1", "validation", "purge_2", "test")
        }


def fixed_406_outcome_split(decision_dates: Sequence[object]) -> OutcomeDateSplit:
    dates = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize().sort_values().unique()
    if len(dates) != 406:
        raise AdvisoryModelFirstError(
            "outcome training requires exactly 406 decision dates",
            reason_code="ADVISORY_OUTCOME_REQUEST_INVALID",
            context={"decision_date_count": len(dates)},
        )
    lengths = (226, 25, 50, 25, 80)
    offsets = [0]
    for length in lengths:
        offsets.append(offsets[-1] + length)
    if offsets[-1] != len(dates):
        raise AssertionError("outcome split lengths do not cover the decision dates")
    return OutcomeDateSplit(
        train=tuple(dates[offsets[0] : offsets[1]]),
        purge_1=tuple(dates[offsets[1] : offsets[2]]),
        validation=tuple(dates[offsets[2] : offsets[3]]),
        purge_2=tuple(dates[offsets[3] : offsets[4]]),
        test=tuple(dates[offsets[4] : offsets[5]]),
    )
