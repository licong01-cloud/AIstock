from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


@dataclass(frozen=True)
class PurgedDateSplit:
    train: tuple[pd.Timestamp, ...]
    purge_1: tuple[pd.Timestamp, ...]
    validation: tuple[pd.Timestamp, ...]
    purge_2: tuple[pd.Timestamp, ...]
    test: tuple[pd.Timestamp, ...]

    def as_dict(self) -> dict[str, list[str]]:
        return {
            name: [item.date().isoformat() for item in getattr(self, name)]
            for name in ("train", "purge_1", "validation", "purge_2", "test")
        }


def fixed_406_date_split(decision_dates: Sequence[pd.Timestamp]) -> PurgedDateSplit:
    dates = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize().sort_values().unique()
    if len(dates) != 406:
        raise AdvisoryModelFirstError(
            "the frozen model-first split requires exactly 406 decision dates",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"actual_date_count": len(dates)},
        )
    split = PurgedDateSplit(
        train=tuple(dates[:246]),
        purge_1=tuple(dates[246:256]),
        validation=tuple(dates[256:316]),
        purge_2=tuple(dates[316:326]),
        test=tuple(dates[326:406]),
    )
    expected = {
        "train": ("2024-07-04", "2025-07-09"),
        "purge_1": ("2025-07-10", "2025-07-23"),
        "validation": ("2025-07-24", "2025-10-23"),
        "purge_2": ("2025-10-24", "2025-11-06"),
        "test": ("2025-11-07", "2026-03-10"),
    }
    for name, (start, end) in expected.items():
        values = getattr(split, name)
        actual = (values[0].date().isoformat(), values[-1].date().isoformat())
        if actual != (start, end):
            raise AdvisoryModelFirstError(
                "decision dates do not match the approved purged split",
                reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
                context={"split": name, "expected": [start, end], "actual": list(actual)},
            )
    return split
