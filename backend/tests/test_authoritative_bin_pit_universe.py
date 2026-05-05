from __future__ import annotations

import datetime as dt

import pandas as pd

from backend.qlib_exporter.authoritative_bin_exporter import _build_pit_all_txt_lines


def test_build_pit_all_txt_lines_keeps_multi_segment_ranges() -> None:
    existing = {
        "000001.SZ": ("000001.SZ", "2018-08-01", "2026-05-04", "000001.SZ"),
        "600000.SH": ("600000.SH", "2018-08-01", "2026-05-04", "600000.SH"),
    }
    pit_spans = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": dt.date(2019, 8, 1),
                "eligible_end": dt.date(2021, 4, 30),
                "entry_reason": "ipo_365d",
                "exit_reason": "st_negative",
            },
            {
                "ts_code": "000001.SZ",
                "eligible_start": dt.date(2022, 5, 10),
                "eligible_end": dt.date(2026, 5, 4),
                "entry_reason": "st_restore",
                "exit_reason": "generation_end",
            },
            {
                "ts_code": "600000.SH",
                "eligible_start": dt.date(2018, 1, 1),
                "eligible_end": dt.date(2026, 12, 31),
                "entry_reason": "ipo_365d",
                "exit_reason": "generation_end",
            },
        ]
    )

    lines, skipped = _build_pit_all_txt_lines(
        existing_ranges=existing,
        pit_spans=pit_spans,
        start=dt.date(2018, 8, 1),
        end=dt.date(2026, 5, 4),
    )

    assert skipped == []
    assert lines == [
        "000001.SZ\t2019-08-01\t2021-04-30",
        "000001.SZ\t2022-05-10\t2026-05-04",
        "600000.SH\t2018-08-01\t2026-05-04",
    ]


def test_build_pit_all_txt_lines_preserves_minute_time_suffix() -> None:
    existing = {
        "000001.SZ": ("000001.SZ", "2018-08-01 09:31:00", "2026-05-04 15:00:00", "000001.SZ"),
    }
    pit_spans = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": dt.date(2019, 8, 1),
                "eligible_end": dt.date(2021, 4, 30),
                "entry_reason": "ipo_365d",
                "exit_reason": "st_negative",
            },
        ]
    )

    lines, skipped = _build_pit_all_txt_lines(
        existing_ranges=existing,
        pit_spans=pit_spans,
        start=dt.date(2018, 8, 1),
        end=dt.date(2026, 5, 4),
    )

    assert skipped == []
    assert lines == ["000001.SZ\t2019-08-01 09:31:00\t2021-04-30 15:00:00"]
