from __future__ import annotations

import csv
from datetime import date
import hashlib
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
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_legacy_prefix import load_legacy_prefix


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


def _bound_prefix(baseline):
    metadata = b"{}\n"
    (baseline / "meta_export.json").write_bytes(metadata)
    manifest = {
        "release_id": "qe_hmm_full_v2_20260831",
        "cutoff_trade_date": "2026-08-31",
        "components": {"day_meta_export": {"path": "meta_export.json", "sha256": hashlib.sha256(metadata).hexdigest(), "size": len(metadata)}},
    }
    identity = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    manifest["dataset_manifest_sha256"] = identity
    payload = canonical_json_bytes(manifest) + b"\n"
    (baseline / "qe_dataset_manifest.json").write_bytes(payload)
    return load_legacy_prefix(
        baseline, expected_manifest_sha256=identity,
        expected_file_sha256=hashlib.sha256(payload).hexdigest(),
        expected_cutoff=date(2026, 8, 31), expected_release_id=manifest["release_id"],
    )


def test_monthly_bound_prefix_uses_existing_writer_without_history_hash_rechecks(tmp_path, monkeypatch):
    import backend.services.dataset_release.qlib_bounded_update as writer
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    prefix = _bound_prefix(baseline)
    original = writer.sha256_file
    def metadata_only(path):
        if path.suffix == ".bin":
            pytest.fail("a second historical content hash pass was requested")
        return original(path)
    monkeypatch.setattr(writer, "sha256_file", metadata_only)
    target = tmp_path / "new"
    receipt = extend_qlib_dataset(
        baseline_root=baseline, target_root=target, csv_dir=csv_dir,
        new_calendar_path=calendar, frequency="day", instruments_all_path=instruments,
        allowed_fields=("close",), expected_instruments=("sh600000",),
        inherited_prefix=prefix,
    )
    feature = target / "features/sh600000/close.day.bin"
    assert _values(feature)[1:4] == pytest.approx([10.0, 11.0, 12.0])
    row = receipt["feature_receipts"][0]
    assert row["predecessor_manifest_sha256"] == prefix.manifest_sha256
    assert row["predecessor_sha256"] is None
    assert row["target_sha256"] == hashlib.sha256(feature.read_bytes()).hexdigest()
    assert receipt["historical_content_revalidated"] is False


def test_qfq_new_anchor_rescales_ohlc_factor_volume_not_raw_limits_or_amount(tmp_path):
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    fields = ("open", "high", "low", "close", "factor", "volume", "amount", "prev_close", "up_limit_price", "down_limit_price", "limit_up", "limit_down")
    values = dict.fromkeys(fields, 10.0)
    values.update(factor=1.0, volume=100.0, amount=1200.0, limit_up=0.0, limit_down=0.0)
    for field in fields:
        _feature(baseline / f"features/sh600000/{field}.day.bin", start=0, values=[values[field], values[field]])
    with (csv_dir / "sh600000.csv").open("w", newline="") as output:
        rows = csv.DictWriter(output, fieldnames=("datetime", *fields))
        rows.writeheader()
        rows.writerow({"datetime": "2026-09-01", **values})
    target = tmp_path / "new"
    result = extend_qlib_dataset(
        baseline_root=baseline, target_root=target, csv_dir=csv_dir,
        new_calendar_path=calendar, frequency="day", instruments_all_path=instruments,
        allowed_fields=fields, expected_instruments=("sh600000",),
        qfq_basis_changes={"sh600000": (2.0, 4.0)},
    )
    for field in ("open", "high", "low", "close"):
        assert _values(target / f"features/sh600000/{field}.day.bin")[1:3] == pytest.approx([5.0, 5.0])
    assert _values(target / "features/sh600000/factor.day.bin")[1:3] == pytest.approx([0.5, 0.5])
    assert _values(target / "features/sh600000/volume.day.bin")[1:3] == pytest.approx([200.0, 200.0])
    for field in ("amount", "prev_close", "up_limit_price", "down_limit_price", "limit_up", "limit_down"):
        assert _values(target / f"features/sh600000/{field}.day.bin")[1:3] == pytest.approx([values[field], values[field]])
    assert _values(target / "features/sh600000/close.day.bin")[3] == pytest.approx(10.0)
    assert _values(baseline / "features/sh600000/close.day.bin")[1:3] == pytest.approx([10.0, 10.0])
    assert result["qfq_basis_changed_instrument_count"] == 1


def test_changed_basis_rejects_a_partial_feature_contract(tmp_path):
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    with pytest.raises(QlibBoundedUpdateError, match="QFQ"):
        extend_qlib_dataset(
            baseline_root=baseline, target_root=tmp_path / "new", csv_dir=csv_dir,
            new_calendar_path=calendar, frequency="day", instruments_all_path=instruments,
            allowed_fields=("close",), expected_instruments=("sh600000",),
            qfq_basis_changes={"sh600000": (2.0, 4.0)},
        )
    assert not (tmp_path / "new").exists()


@pytest.mark.parametrize("old,new", [(0, 1), (-1, 1), (float("nan"), 1), (1, float("inf")), (1e-300, 1e300), (1e300, 1e-300)])
def test_invalid_qfq_scale_is_rejected_before_any_copy(tmp_path, old, new):
    from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    with pytest.raises(QlibBoundedUpdateError, match="QFQ"):
        extend_qlib_dataset(
            baseline_root=baseline, target_root=tmp_path / "new", csv_dir=csv_dir,
            new_calendar_path=calendar, frequency="day", instruments_all_path=instruments,
            allowed_fields=QLIB_STOCK_FIELDS, expected_instruments=("sh600000",),
            qfq_basis_changes={"sh600000": (old, new)},
        )
    assert not (tmp_path / "new").exists()


def test_canonical_date_symbol_csv_is_consumed_without_a_private_symbol_mapping(tmp_path):
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    original = csv_dir / "sh600000.csv"
    # Formal shared canonical CSV producer uses date/symbol, not datetime.
    original.rename(csv_dir / "600000.SH.csv")
    with (csv_dir / "600000.SH.csv").open("w", newline="") as output:
        rows = csv.DictWriter(output, fieldnames=("date", "symbol", "close"))
        rows.writeheader()
        rows.writerow({"date": "2026-09-01", "symbol": "600000.SH", "close": 12.0})
    result = extend_qlib_dataset(
        baseline_root=baseline, target_root=tmp_path / "new", csv_dir=csv_dir,
        new_calendar_path=calendar, frequency="day", instruments_all_path=instruments,
        allowed_fields=("close",), expected_instruments=("600000.SH",), datetime_field="date",
    )
    assert result["instrument_csv_count"] == 1
    assert _values(tmp_path / "new/features/600000.sh/close.day.bin") == pytest.approx([2.0, 12.0])


def test_canonical_csv_symbol_mismatch_never_produces_success(tmp_path):
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    with (csv_dir / "sh600000.csv").open("w", newline="") as output:
        rows = csv.DictWriter(output, fieldnames=("date", "symbol", "close"))
        rows.writeheader()
        rows.writerow({"date": "2026-09-01", "symbol": "000001.SZ", "close": 12.0})
    with pytest.raises(QlibBoundedUpdateError, match="symbol"):
        extend_qlib_dataset(
            baseline_root=baseline, target_root=tmp_path / "new", csv_dir=csv_dir,
            new_calendar_path=calendar, frequency="day", instruments_all_path=instruments,
            allowed_fields=("close",), expected_instruments=("sh600000",), datetime_field="date",
        )


def test_manifest_bound_prefix_rejects_drift_before_cloning(tmp_path):
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    prefix = _bound_prefix(baseline)
    (baseline / "meta_export.json").write_bytes(b"unexpected metadata")
    with pytest.raises(QlibBoundedUpdateError, match="metadata"):
        extend_qlib_dataset(
            baseline_root=baseline, target_root=tmp_path / "new", csv_dir=csv_dir,
            new_calendar_path=calendar, frequency="day", instruments_all_path=instruments,
            allowed_fields=("close",), expected_instruments=("sh600000",), inherited_prefix=prefix,
        )
    assert not (tmp_path / "new").exists()


def test_monthly_cancellation_before_copy_preserves_source_and_creates_no_target(tmp_path):
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    class Cancelled(RuntimeError):
        pass
    def cancel():
        raise Cancelled("operator cancellation")
    with pytest.raises(Cancelled):
        extend_qlib_dataset(
            baseline_root=baseline, target_root=tmp_path / "new", csv_dir=csv_dir,
            new_calendar_path=calendar, frequency="day", instruments_all_path=instruments,
            allowed_fields=("close",), expected_instruments=("sh600000",), checkpoint=cancel,
        )
    assert not (tmp_path / "new").exists()
    assert _values(baseline / "features/sh600000/close.day.bin")[1:] == pytest.approx([10.0, 11.0])


def test_bound_prefix_cannot_target_a_sibling_inside_the_active_release(tmp_path):
    baseline, csv_dir, calendar, instruments = _fixture(tmp_path)
    prefix = _bound_prefix(baseline)
    # Point the valid manifest at a nested provider, as in a real release.
    from dataclasses import replace
    root = tmp_path / "release"
    root.mkdir()
    baseline.rename(root / "day")
    baseline = root / "day"
    import json
    manifest_path = baseline / "qe_dataset_manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["components"]["day_meta_export"]["path"] = "day/meta_export.json"
    manifest.pop("dataset_manifest_sha256")
    identity = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    manifest["dataset_manifest_sha256"] = identity
    payload = canonical_json_bytes(manifest) + b"\n"
    (root / "qe_dataset_manifest.json").write_bytes(payload)
    prefix = replace(prefix, root=root, manifest=manifest, manifest_sha256=identity, manifest_file_sha256=hashlib.sha256(payload).hexdigest())
    target = root / "wrong-sibling"
    with pytest.raises(QlibBoundedUpdateError, match="immutable release"):
        extend_qlib_dataset(
            baseline_root=baseline, target_root=target, csv_dir=csv_dir,
            new_calendar_path=calendar, frequency="day", instruments_all_path=instruments,
            allowed_fields=("close",), expected_instruments=("sh600000",), inherited_prefix=prefix,
        )
    assert not target.exists()


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
