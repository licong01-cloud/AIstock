"""Legacy prefix reuse tests use only tiny task-owned files, never datasets."""

from datetime import date
import errno
import hashlib
import json
import os
from pathlib import Path
import csv
import struct

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release import monthly_legacy_prefix as subject
from backend.services.dataset_release.monthly_legacy_prefix import (
    LegacyMonthlyPrefixError,
    clone_legacy_prefix,
    load_legacy_prefix,
)


def prefix(tmp_path, *, meta_filename="meta.json"):
    root = tmp_path / "old"
    (root / "components/day/features").mkdir(parents=True)
    (root / "components/day/features/close.bin").write_bytes(b"old-close")
    (root / "components/day/features/retired.bin").write_bytes(b"old-retired")
    (root / "components/day" / meta_filename).write_bytes(b"{}\n")
    body = {
        "release_id": "qe_hmm_full_v2_20260831",
        "cutoff_trade_date": "2026-08-31",
        "components": {"day_meta_export": {"path": f"components/day/{meta_filename}", "sha256": hashlib.sha256(b"{}\n").hexdigest(), "size": 3}},
    }
    identity = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    body["dataset_manifest_sha256"] = identity
    raw = canonical_json_bytes(body) + b"\n"
    (root / "qe_dataset_manifest.json").write_bytes(raw)
    return load_legacy_prefix(
        root, expected_manifest_sha256=identity,
        expected_file_sha256=hashlib.sha256(raw).hexdigest(),
        expected_cutoff=date(2026, 8, 31), expected_release_id=body["release_id"],
    )


def test_prefix_clone_copies_mutable_files_before_any_writer_can_run(tmp_path):
    source = prefix(tmp_path)
    target = tmp_path / "new"
    mutable = ("components/day/features/close.bin", "components/day/meta.json")
    receipt = clone_legacy_prefix(source, target, relative_roots=("components/day",), mutable_paths=mutable)
    assert receipt["copied_file_count"] == 2
    assert receipt["hardlinked_file_count"] == 1
    assert receipt["full_content_revalidated"] is False
    assert receipt["predecessor_manifest_sha256"] == source.manifest_sha256
    assert not os.path.samefile(source.root / mutable[0], target / mutable[0])
    assert os.path.samefile(source.root / "components/day/features/retired.bin", target / "components/day/features/retired.bin")
    with (target / mutable[0]).open("ab") as handle:
        handle.write(b"-september")
    assert (source.root / mutable[0]).read_bytes() == b"old-close"
    assert (target / mutable[0]).read_bytes() == b"old-close-september"
    assert json.loads((source.root / "qe_dataset_manifest.json").read_text()) == source.manifest


def test_cross_volume_prefix_copy_does_not_require_hardlink_capability(tmp_path, monkeypatch):
    source = prefix(tmp_path)
    def different_volume(*_args, **_kwargs):
        raise OSError(errno.EXDEV, "different volume")
    monkeypatch.setattr(os, "link", different_volume)
    result = clone_legacy_prefix(source, tmp_path / "new", relative_roots=("components/day",), mutable_paths=())
    assert result["hardlinked_file_count"] == 0
    assert result["copied_file_count"] == 3


@pytest.mark.parametrize("relative_roots,mutable", [
    (("../old",), ()), (("components/day",), ("../escape",)),
    (("components/day", "components/day/features"), ()),
    (("components/day",), ("components/missing.bin",)),
    (("components/day",), ("components/day/not-present.bin",)),
])
def test_invalid_clone_scope_cannot_publish_success(tmp_path, relative_roots, mutable):
    source = prefix(tmp_path)
    with pytest.raises(LegacyMonthlyPrefixError):
        clone_legacy_prefix(source, tmp_path / "new", relative_roots=relative_roots, mutable_paths=mutable)


def test_existing_target_and_source_self_copy_are_rejected(tmp_path):
    source = prefix(tmp_path)
    for target in (source.root, source.root / "nested", tmp_path):
        with pytest.raises(LegacyMonthlyPrefixError):
            clone_legacy_prefix(source, target, relative_roots=("components/day",), mutable_paths=())


def test_manifest_drift_is_detected_before_clone(tmp_path):
    source = prefix(tmp_path)
    (source.root / "qe_dataset_manifest.json").write_bytes(b"changed")
    with pytest.raises(LegacyMonthlyPrefixError, match="manifest"):
        clone_legacy_prefix(source, tmp_path / "new", relative_roots=("components/day",), mutable_paths=())


def test_clone_never_reads_component_contents_to_calculate_history_hashes(tmp_path, monkeypatch):
    source = prefix(tmp_path)
    original = Path.open
    def only_identity_reads(path, *args, **kwargs):
        if path.name.endswith(".bin") and (not args or "r" in args[0]):
            pytest.fail("historical bin content read")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "open", only_identity_reads)
    result = clone_legacy_prefix(source, tmp_path / "new", relative_roots=("components/day",), mutable_paths=())
    assert result["file_count"] == 3


def test_copy_fallback_does_not_overwrite_a_concurrently_created_target(tmp_path, monkeypatch):
    source = prefix(tmp_path)
    def raced_link(_source, destination):
        Path(destination).write_bytes(b"other writer")
        raise OSError(errno.EXDEV, "different volume")
    monkeypatch.setattr(os, "link", raced_link)
    with pytest.raises(FileExistsError):
        clone_legacy_prefix(source, tmp_path / "new", relative_roots=("components/day",), mutable_paths=())
    assert (source.root / "components/day/meta.json").read_bytes() == b"{}\n"
    target_files = list((tmp_path / "new").rglob("*"))
    assert [path.read_bytes() for path in target_files if path.is_file()] == [b"other writer"]


def test_link_in_a_selected_component_never_produces_a_success_receipt(tmp_path):
    source = prefix(tmp_path)
    link = source.root / "components/day/escape"
    try:
        link.symlink_to(tmp_path, target_is_directory=True)
    except OSError:
        pytest.skip("filesystem does not allow symlinks")
    with pytest.raises(LegacyMonthlyPrefixError, match="link"):
        clone_legacy_prefix(source, tmp_path / "new", relative_roots=("components/day",), mutable_paths=())


def test_large_private_file_keeps_the_existing_cancellation_checkpoint(tmp_path, monkeypatch):
    source = prefix(tmp_path)
    ticks = iter((0.0, 3.0))
    monkeypatch.setattr(subject.time, "monotonic", lambda: next(ticks))
    observed = []
    subject._copy_exclusive(source.root / "components/day/features/close.bin", tmp_path / "copy.bin", lambda: observed.append(True))
    assert observed == [True]


def test_prefix_maps_legacy_provider_into_shared_monthly_layout(tmp_path):
    source = prefix(tmp_path)
    target = tmp_path / "new"
    mutable = "components/day/features/close.bin"
    result = clone_legacy_prefix(
        source, target, relative_roots=("components/day",),
        target_roots={"components/day": "daily_bin/qlib"},
        mutable_paths=(mutable,),
    )
    assert not (target / "components").exists()
    assert (target / "daily_bin/qlib/features/close.bin").read_bytes() == b"old-close"
    assert not os.path.samefile(source.root / mutable, target / "daily_bin/qlib/features/close.bin")
    assert os.path.samefile(
        source.root / "components/day/features/retired.bin",
        target / "daily_bin/qlib/features/retired.bin",
    )
    assert result["target_roots"] == {"components/day": "daily_bin/qlib"}


@pytest.mark.parametrize("destinations", [
    {}, {"unexpected": "daily_bin/qlib"},
    {"components/day": "../escape"},
    {"components/day": "daily_bin/qlib", "extra": "other"},
])
def test_invalid_destination_map_rejected_before_target_creation(tmp_path, destinations):
    source = prefix(tmp_path)
    target = tmp_path / "new"
    with pytest.raises(LegacyMonthlyPrefixError):
        clone_legacy_prefix(
            source, target, relative_roots=("components/day",),
            target_roots=destinations, mutable_paths=(),
        )
    assert not target.exists()


@pytest.mark.parametrize("destinations", [
    {"components/day/features": "daily_bin", "components/day/meta.json": "daily_bin/meta.json"},
    {"components/day/features": "DAY", "components/day/meta.json": "day"},
])
def test_mapped_destination_overlap_is_rejected_case_insensitively(tmp_path, destinations):
    source = prefix(tmp_path)
    target = tmp_path / "new"
    with pytest.raises(LegacyMonthlyPrefixError, match="overlap"):
        clone_legacy_prefix(
            source, target, relative_roots=tuple(destinations),
            target_roots=destinations, mutable_paths=(),
        )
    assert not target.exists()


def test_individual_file_root_can_be_mapped_without_renaming_component_identity(tmp_path):
    source = prefix(tmp_path)
    target = tmp_path / "new"
    old = "components/day/meta.json"
    result = clone_legacy_prefix(
        source, target, relative_roots=(old,),
        target_roots={old: "daily_bin/meta_export.json"}, mutable_paths=(old,),
    )
    assert (target / "daily_bin/meta_export.json").read_bytes() == b"{}\n"
    assert result["predecessor_manifest_sha256"] == source.manifest_sha256
    assert result["file_count"] == 1
    assert result["copied_file_count"] == 1


def qlib_prefix(tmp_path, *, meta_filename="meta.json"):
    source = prefix(tmp_path, meta_filename=meta_filename)
    provider = source.root / "components/day"
    (provider / "calendars").mkdir()
    (provider / "calendars/day.txt").write_text("2026-08-31\n")
    (provider / "instruments").mkdir()
    (provider / "instruments/all.txt").write_text("000001.SZ\t2026-08-31\t2026-08-31\n")
    (provider / "features/000001.sz").mkdir()
    (provider / "features/000001.sz/close.day.bin").write_bytes(b"real-qlib-file")
    return source


def test_append_preparation_uses_the_manifest_pin_not_historical_metadata_paths(tmp_path):
    source = qlib_prefix(tmp_path)
    result = subject.prepare_legacy_qlib_append(
        source, tmp_path / "new", dataset="daily_bin",
        instruments=("000001.SZ", "000002.SZ"), fields=("close",),
    )
    provider = tmp_path / "new/daily_bin/qlib"
    assert result["qlib_relative_path"] == "daily_bin/qlib"
    assert result["copy_receipt"]["predecessor_manifest_sha256"] == source.manifest_sha256
    for relative in (
        "features/000001.sz/close.day.bin", "calendars/day.txt", "instruments/all.txt", "meta.json",
    ):
        assert not os.path.samefile(source.root / "components/day" / relative, provider / relative)
    assert result["existing_mutable_feature_file_count"] == 1
    assert result["new_instrument_count"] == 1
    assert result["publication_allowed"] is False


@pytest.mark.parametrize("dataset,instruments,fields", [
    ("factor", ("000001.SZ",), ("close",)),
    ("daily_bin", ("../outside",), ("close",)),
    ("daily_bin", ("000001.SZ", "000001.sz"), ("close",)),
    ("daily_bin", ("000001.SZ",), ("../close",)),
    ("daily_bin", (), ("close",)),
    ("daily_bin", ("000001.SZ",), ()),
])
def test_append_preparation_rejects_undeclared_writer_targets(tmp_path, dataset, instruments, fields):
    source = qlib_prefix(tmp_path)
    target = tmp_path / "new"
    with pytest.raises(LegacyMonthlyPrefixError):
        subject.prepare_legacy_qlib_append(
            source, target, dataset=dataset, instruments=instruments, fields=fields,
        )
    assert not target.exists()


def test_append_preparation_rejects_a_changed_pinned_metadata_file(tmp_path):
    source = qlib_prefix(tmp_path)
    (source.root / "components/day/meta.json").write_bytes(b"changed metadata")
    with pytest.raises(LegacyMonthlyPrefixError, match="metadata"):
        subject.prepare_legacy_qlib_append(
            source, tmp_path / "new", dataset="daily_bin",
            instruments=("000001.SZ",), fields=("close",),
        )
    assert not (tmp_path / "new").exists()


def test_prepared_append_writer_cannot_mutate_the_predecessor(tmp_path):
    source = qlib_prefix(tmp_path)
    target = tmp_path / "new"
    result = subject.prepare_legacy_qlib_append(
        source, target, dataset="daily_bin",
        instruments=("000001.SZ",), fields=("close",),
    )
    provider = target / result["qlib_relative_path"]
    before = (source.root / "components/day/features/000001.sz/close.day.bin").read_bytes()
    with (provider / "features/000001.sz/close.day.bin").open("ab") as writer:
        writer.write(b"-delta")
    (provider / "calendars/day.txt").write_text("2026-08-31\n2026-09-01\n")
    (provider / "instruments/all.txt").write_text("000001.SZ\t2026-08-31\t2026-09-01\n")
    assert (source.root / "components/day/features/000001.sz/close.day.bin").read_bytes() == before
    assert (source.root / "components/day/calendars/day.txt").read_text() == "2026-08-31\n"
    assert (source.root / "components/day/instruments/all.txt").read_text().endswith("2026-08-31\n")


def test_preparation_checkpoint_is_not_dropped_during_writer_scope_enumeration(tmp_path):
    source = qlib_prefix(tmp_path)
    target = tmp_path / "new"
    class Cancelled(RuntimeError):
        pass
    def cancel():
        raise Cancelled("operator requested cancellation")
    with pytest.raises(Cancelled):
        subject.prepare_legacy_qlib_append(
            source, target, dataset="daily_bin", instruments=("000001.SZ",),
            fields=("close",), checkpoint=cancel,
        )
    assert not target.exists()


def test_monthly_adapter_produces_real_shared_bins_using_canonical_codes(tmp_path):
    from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS
    source = qlib_prefix(tmp_path, meta_filename="meta_export.json")
    for field in QLIB_STOCK_FIELDS:
        (source.root / f"components/day/features/000001.sz/{field}.day.bin").write_bytes(struct.pack("<ff", 0.0, 10.0))
    csv_root = tmp_path / "csv"
    csv_root.mkdir()
    with (csv_root / "000001.SZ.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=("date", "symbol", *QLIB_STOCK_FIELDS))
        writer.writeheader()
        writer.writerow({"date": "2026-09-01", "symbol": "000001.SZ", **dict.fromkeys(QLIB_STOCK_FIELDS, 12.0)})
    calendar = tmp_path / "calendar.txt"
    calendar.write_text("2026-08-31\n2026-09-01\n")
    instruments = tmp_path / "all.txt"
    instruments.write_text("000001.SZ\t2026-08-31\t2026-09-01\n")
    staging = tmp_path / "staging"
    staging.mkdir()
    result = subject.append_legacy_qlib_month(
        source, staging, dataset="daily_bin", target_cutoff=date(2026, 9, 1),
        csv_root=csv_root, calendar_path=calendar, instruments_all_path=instruments,
        instruments=("000001.SZ",), source_receipt_sha256="a" * 64,
    )
    feature = staging / "daily_bin/qlib/features/000001.sz/close.day.bin"
    assert struct.unpack("<fff", feature.read_bytes()) == pytest.approx((0.0, 10.0, 12.0))
    assert struct.unpack("<ff", (source.root / "components/day/features/000001.sz/close.day.bin").read_bytes()) == pytest.approx((0.0, 10.0))
    assert result["append_performed"] is True
    assert result["publication_allowed"] is False
    assert result["historical_source_rows_read"] == 0
    assert result["receipt"]["feature_file_count"] == 12


def test_monthly_minute_append_preserves_prefix_and_adds_exactly_240_native_bars(tmp_path):
    from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS
    from backend.services.dataset_release.minute_overlay import canonical_session_times
    source = prefix(tmp_path, meta_filename="meta_export.json")
    provider = source.root / "components/minute"
    (provider / "calendars").mkdir(parents=True)
    (provider / "instruments").mkdir()
    (provider / "features/000001.sz").mkdir(parents=True)
    (provider / "meta_export.json").write_bytes(b"{}\n")
    old_times = [value.strftime("%Y-%m-%d %H:%M:%S") for value in canonical_session_times(date(2026, 8, 31))]
    new_times = [value.strftime("%Y-%m-%d %H:%M:%S") for value in canonical_session_times(date(2026, 9, 1))]
    (provider / "calendars/1min.txt").write_text("".join(f"{value}\n" for value in old_times))
    (provider / "instruments/all.txt").write_text("000001.SZ\t2026-08-31\t2026-08-31\n")
    for field in QLIB_STOCK_FIELDS:
        (provider / f"features/000001.sz/{field}.1min.bin").write_bytes(struct.pack("<241f", 0.0, *([10.0] * 240)))
    body = dict(source.manifest)
    body["components"] = {**body["components"], "minute_meta_export": {
        "path": "components/minute/meta_export.json", "sha256": hashlib.sha256(b"{}\n").hexdigest(), "size": 3,
    }}
    body.pop("dataset_manifest_sha256")
    identity = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    body["dataset_manifest_sha256"] = identity
    payload = canonical_json_bytes(body) + b"\n"
    (source.root / "qe_dataset_manifest.json").write_bytes(payload)
    source = load_legacy_prefix(
        source.root, expected_manifest_sha256=identity,
        expected_file_sha256=hashlib.sha256(payload).hexdigest(),
        expected_cutoff=source.cutoff, expected_release_id=source.release_id,
    )
    csv_root = tmp_path / "csv"
    csv_root.mkdir()
    with (csv_root / "000001.SZ.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=("date", "symbol", *QLIB_STOCK_FIELDS))
        writer.writeheader()
        for stamp in new_times:
            writer.writerow({"date": stamp, "symbol": "000001.SZ", **dict.fromkeys(QLIB_STOCK_FIELDS, 12.0)})
    calendar = tmp_path / "calendar.txt"
    calendar.write_text("".join(f"{value}\n" for value in (*old_times, *new_times)))
    instruments = tmp_path / "all.txt"
    instruments.write_text("000001.SZ\t2026-08-31\t2026-09-01\n")
    staging = tmp_path / "staging"
    staging.mkdir()
    result = subject.append_legacy_qlib_month(
        source, staging, dataset="minute_bin", target_cutoff=date(2026, 9, 1),
        csv_root=csv_root, calendar_path=calendar, instruments_all_path=instruments,
        instruments=("000001.SZ",), source_receipt_sha256="b" * 64,
    )
    feature = staging / "minute_bin/qlib/features/000001.sz/close.1min.bin"
    values = struct.unpack("<481f", feature.read_bytes())
    assert values[1:241] == pytest.approx([10.0] * 240)
    assert values[241:] == pytest.approx([12.0] * 240)
    assert all(stamp[11:16] not in {"13:00", "09:00"} for stamp in new_times)
    assert result["receipt"]["calendar_append_count"] == 240
    assert result["historical_source_rows_read"] == 0
