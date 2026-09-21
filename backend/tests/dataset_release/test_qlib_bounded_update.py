from __future__ import annotations

import csv
import math
from pathlib import Path
import struct

import pytest

from backend.services.dataset_release.qlib_bounded_update import (
    QlibBoundedUpdateError,
    extend_qlib_dataset,
    rewrite_feature_values,
    sha256_file,
)


F32 = struct.Struct("<f")


def _feature(path: Path, *, start: int, values: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(F32.pack(float(start)))
        for value in values:
            handle.write(F32.pack(value))


def _values(path: Path) -> list[float]:
    payload = path.read_bytes()
    return [F32.unpack(payload[index : index + 4])[0] for index in range(0, len(payload), 4)]


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    baseline = tmp_path / "baseline"
    (baseline / "calendars").mkdir(parents=True)
    (baseline / "instruments").mkdir()
    (baseline / "calendars" / "day.txt").write_text("2026-08-28\n2026-08-31\n", encoding="utf-8")
    (baseline / "instruments" / "all.txt").write_text(
        "sh600000\t2018-08-01\t2026-08-31\n", encoding="utf-8"
    )
    _feature(baseline / "features" / "sh600000" / "close.day.bin", start=0, values=[10.0, 11.0])
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    with (csv_dir / "sh600000.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["datetime", "close"])
        writer.writeheader()
        writer.writerow({"datetime": "2026-09-01", "close": "12"})
        writer.writerow({"datetime": "2026-09-03", "close": "13"})
    calendar = tmp_path / "day.txt"
    calendar.write_text(
        "2026-08-28\n2026-08-31\n2026-09-01\n2026-09-02\n2026-09-03\n",
        encoding="utf-8",
    )
    instruments = tmp_path / "all.txt"
    instruments.write_text(
        "sh600000\t2018-08-01\t2026-09-03\nsh600000\t2026-09-10\t2026-09-20\n",
        encoding="utf-8",
    )
    return baseline, csv_dir, calendar, instruments


def test_bounded_writer_appends_private_features_and_preserves_baseline(tmp_path: Path) -> None:
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    source_feature = baseline / "features" / "sh600000" / "close.day.bin"
    source_sha = sha256_file(source_feature)
    target = tmp_path / "successor"
    receipt = extend_qlib_dataset(
        baseline_root=baseline,
        target_root=target,
        csv_dir=csv_dir,
        new_calendar_path=calendar,
        frequency="day",
        instruments_all_path=instruments,
        allowed_fields=("close",),
        expected_instruments=("sh600000",),
    )
    result = _values(target / "features" / "sh600000" / "close.day.bin")
    assert result[:3] == pytest.approx([0.0, 10.0, 11.0])
    assert result[3] == pytest.approx(12.0)
    assert math.isnan(result[4])
    assert result[5] == pytest.approx(13.0)
    assert sha256_file(source_feature) == source_sha
    assert receipt["bounded_memory_unit"] == "one_instrument_csv"
    assert receipt["calendar_append_count"] == 3
    assert receipt["baseline_mutated"] is False
    assert instruments.read_bytes() == (target / "instruments" / "all.txt").read_bytes()


def test_writer_rejects_historical_calendar_insertion(tmp_path: Path) -> None:
    baseline, csv_dir, _, instruments = _fixture(tmp_path)
    calendar = tmp_path / "inserted-day.txt"
    calendar.write_text("2026-08-28\n2026-08-29\n2026-08-31\n", encoding="utf-8")
    with pytest.raises(QlibBoundedUpdateError, match="strict append-only"):
        extend_qlib_dataset(
            baseline_root=baseline,
            target_root=tmp_path / "successor",
            csv_dir=csv_dir,
            new_calendar_path=calendar,
            frequency="day",
            instruments_all_path=instruments,
            allowed_fields=("close",),
            expected_instruments=("sh600000",),
        )


def test_writer_rejects_payload_that_overlaps_frozen_prefix(tmp_path: Path) -> None:
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    with (csv_dir / "sh600000.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["datetime", "close"])
        writer.writeheader()
        writer.writerow({"datetime": "2026-08-31", "close": "99"})
    with pytest.raises(QlibBoundedUpdateError, match="overlaps frozen calendar"):
        extend_qlib_dataset(
            baseline_root=baseline,
            target_root=tmp_path / "successor",
            csv_dir=csv_dir,
            new_calendar_path=calendar,
            frequency="day",
            instruments_all_path=instruments,
            allowed_fields=("close",),
            expected_instruments=("sh600000",),
        )


def test_writer_rejects_infinite_values_but_preserves_explicit_nan(tmp_path: Path) -> None:
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    with (csv_dir / "sh600000.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["datetime", "close"])
        writer.writeheader()
        writer.writerow({"datetime": "2026-09-01", "close": "inf"})
    with pytest.raises(QlibBoundedUpdateError, match="infinite"):
        extend_qlib_dataset(
            baseline_root=baseline,
            target_root=tmp_path / "successor",
            csv_dir=csv_dir,
            new_calendar_path=calendar,
            frequency="day",
            instruments_all_path=instruments,
            allowed_fields=("close",),
            expected_instruments=("sh600000",),
        )


def test_selective_rewrite_changes_only_requested_indices_and_keeps_source_immutable(tmp_path: Path) -> None:
    baseline, _, _, _ = _fixture(tmp_path)
    source = baseline / "features" / "sh600000" / "close.day.bin"
    source_sha = sha256_file(source)
    target = tmp_path / "private" / "close.day.bin"
    receipt = rewrite_feature_values(
        instrument="sh600000",
        feature="close",
        source_path=source,
        target_path=target,
        values_by_index={1: 21.0, 3: 23.0},
        calendar_size=4,
    )
    result = _values(target)
    assert result[:3] == pytest.approx([0.0, 10.0, 21.0])
    assert math.isnan(result[3])
    assert result[4] == pytest.approx(23.0)
    assert sha256_file(source) == source_sha
    assert receipt["rewritten_index_count"] == 2


def test_bounded_writer_rejects_target_nested_under_baseline(tmp_path: Path) -> None:
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    with pytest.raises(QlibBoundedUpdateError, match="separate from the baseline"):
        extend_qlib_dataset(
            baseline_root=baseline,
            target_root=baseline / "successor",
            csv_dir=csv_dir,
            new_calendar_path=calendar,
            frequency="day",
            instruments_all_path=instruments,
            allowed_fields=("close",),
            expected_instruments=("sh600000",),
        )


def test_bounded_writer_rejects_linked_csv_or_baseline_entry(tmp_path: Path) -> None:
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    linked = baseline / "linked"
    try:
        linked.symlink_to(baseline / "calendars", target_is_directory=True)
    except OSError:
        pytest.skip("test environment cannot create a directory symlink")
    with pytest.raises(QlibBoundedUpdateError, match="contains a link or junction"):
        extend_qlib_dataset(
            baseline_root=baseline,
            target_root=tmp_path / "successor",
            csv_dir=csv_dir,
            new_calendar_path=calendar,
            frequency="day",
            instruments_all_path=instruments,
            allowed_fields=("close",),
            expected_instruments=("sh600000",),
        )
