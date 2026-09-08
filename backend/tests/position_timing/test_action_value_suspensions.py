from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import (
    ActionPlan,
    ActionValueError,
    daily_fill,
)
from backend.services.position_timing.action_value_data import DailyCandidate
from backend.services.position_timing.action_value_suspensions import (
    SuspensionSnapshotBook,
    _snapshot_payload,
    freeze_suspension_snapshot,
)


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.query = None
        self.arguments = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, query, arguments):
        self.query = query
        self.arguments = arguments

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self.cursor_instance = _Cursor(rows)

    def cursor(self):
        return self.cursor_instance


def _candidate() -> DailyCandidate:
    return DailyCandidate(
        root=Path("candidate"),
        calendar=pd.DatetimeIndex(["2022-05-05"]),
        spans=pd.DataFrame(
            [{"symbol": "002211.SZ", "start": pd.Timestamp("2020-01-01"), "end": pd.Timestamp("2025-01-01")}]
        ),
        suspension_keys={("002211.SZ", date(2025, 4, 24))},
        references={"candidate": {"path": "candidate", "sha256": "a" * 64, "size_bytes": 1}},
    )


def test_snapshot_freezes_only_explicit_scoped_s_rows_and_unions_candidate(tmp_path: Path) -> None:
    connection = _Connection(
        [
            ("002211.SZ", date(2022, 5, 5), "S", None),
            ("002211.SZ", date(2023, 6, 20), "S", ""),
        ]
    )
    path = freeze_suspension_snapshot(
        connection,
        symbols=("002211.SZ", "002211.SZ"),
        start=date(2018, 8, 1),
        end=date(2026, 8, 31),
        timing_root=tmp_path,
    )

    book = SuspensionSnapshotBook.open(path)
    augmented = book.apply(_candidate(), snapshot_path=path)

    assert "suspend_type = 'S'" in connection.cursor_instance.query
    assert connection.cursor_instance.arguments == (
        ["002211.SZ"],
        date(2018, 8, 1),
        date(2026, 8, 31),
    )
    assert book.keys == (
        ("002211.SZ", date(2022, 5, 5)),
        ("002211.SZ", date(2023, 6, 20)),
    )
    assert augmented.suspension_keys == {
        ("002211.SZ", date(2022, 5, 5)),
        ("002211.SZ", date(2023, 6, 20)),
        ("002211.SZ", date(2025, 4, 24)),
    }
    assert path.name == f"{book.snapshot_sha256}.json"
    assert "timing_suspension_snapshot" in augmented.references


def test_empty_snapshot_is_valid_but_nan_bar_is_not_inferred_as_suspension(tmp_path: Path) -> None:
    path = freeze_suspension_snapshot(
        _Connection([]),
        symbols=("002211.SZ",),
        start=date(2022, 1, 1),
        end=date(2022, 12, 31),
        timing_root=tmp_path,
    )
    assert SuspensionSnapshotBook.open(path).keys == ()

    plan = ActionPlan("002211.SZ", 100, Decimal("3.00"))
    missing = {
        "open": np.nan,
        "high": np.nan,
        "low": np.nan,
        "close": np.nan,
        "up_limit": np.nan,
        "down_limit": np.nan,
        "is_suspended": False,
    }
    assert daily_fill(plan, missing, sellable=0).reason == "TARGET_BAR_INVALID"
    assert daily_fill(plan, {**missing, "is_suspended": True}, sellable=0).reason == "TARGET_DAY_SUSPENDED"


def test_snapshot_rejects_duplicate_or_out_of_scope_rows() -> None:
    row = ("002211.SZ", date(2022, 5, 5), "S", None)
    with pytest.raises(ActionValueError, match="SUSPENSION_SOURCE_SCOPE_INVALID"):
        _snapshot_payload(
            [row, row],
            symbols=("002211.SZ",),
            start=date(2022, 1, 1),
            end=date(2022, 12, 31),
        )
    with pytest.raises(ActionValueError, match="SUSPENSION_SOURCE_SCOPE_INVALID"):
        _snapshot_payload(
            [("002211.SZ", date(2021, 12, 31), "S", None)],
            symbols=("002211.SZ",),
            start=date(2022, 1, 1),
            end=date(2022, 12, 31),
        )


def test_snapshot_tamper_fails_closed(tmp_path: Path) -> None:
    path = freeze_suspension_snapshot(
        _Connection([("002211.SZ", date(2022, 5, 5), "S", None)]),
        symbols=("002211.SZ",),
        start=date(2022, 1, 1),
        end=date(2022, 12, 31),
        timing_root=tmp_path,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["suspensions"][0]["trade_date"] = "2022-05-06"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ActionValueError, match="SUSPENSION_SNAPSHOT_IDENTITY_MISMATCH"):
        SuspensionSnapshotBook.open(path)
