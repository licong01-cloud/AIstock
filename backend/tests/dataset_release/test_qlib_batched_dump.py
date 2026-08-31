from __future__ import annotations

import argparse
import csv
import json
import struct
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import dataset_release_qlib_batched_dump as batched


DAY0 = "2026-07-30"
DAY1 = "2026-07-31"


def _write_csv(path: Path, code: str, dates: list[str], *, corrected: bool = False) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "symbol", *batched.FIELDS])
        writer.writeheader()
        for ordinal, value in enumerate(dates, start=1):
            numeric = float(ordinal + (10 if corrected and value == DAY0 else 0))
            writer.writerow({"date": value, "symbol": code, **{field: numeric for field in batched.FIELDS}})
    return {
        "code": code,
        "role": "index" if code.endswith(".CSI") else "stock",
        "relative_path": path.name,
        "rows": len(dates),
        "sha256": batched._sha256(path),
        "start": dates[0],
        "end": dates[-1],
    }


def _manifest(
    root: Path,
    *,
    phases: list[tuple[str, str, list[str]]],
    batch_size: int = 20,
) -> Path:
    output = []
    total_rows = 0
    total_writes = 0
    for phase_id, kind, codes in phases:
        by_role = {
            "stock": [code for code in codes if not code.endswith(".CSI")],
            "index": [code for code in codes if code.endswith(".CSI")],
        }
        batches = []
        for role in ("stock", "index"):
            for offset in range(0, len(by_role[role]), batch_size):
                ordinal = len(batches)
                batch_root = root / phase_id / f"batch-{ordinal:04d}-{role}"
                files = [
                    _write_csv(
                        batch_root / f"{code.casefold()}.csv",
                        code,
                        ([DAY0, DAY1] if kind == "override" or (kind == "full" and int(code[:6]) % 2) else [DAY1]),
                        corrected=kind == "override",
                    )
                    for code in by_role[role][offset : offset + batch_size]
                ]
                total_rows += sum(item["rows"] for item in files)
                total_writes += len(files)
                batches.append(
                    {
                        "ordinal": ordinal,
                        "role": role,
                        "mode": "dump_update" if kind == "tail" else "dump_fix",
                        "relative_path": batch_root.relative_to(root).as_posix(),
                        "files": files,
                    }
                )
        output.append({"phase_id": phase_id, "kind": kind, "batches": batches})
    value = {
        "schema_version": batched.SCHEMA,
        "dataset": "daily_bin",
        "freq": "day",
        "fields": list(batched.FIELDS),
        "max_codes_per_batch": batch_size,
        "per_batch_timeout_seconds": 30,
        "resource_checkpoint_identity": {
            "attempt_id": "attempt-1",
            "fence": 1,
            "execution_id": "build-dump-daily",
        },
        "phases": output,
        "expected_total_code_writes": total_writes,
        "expected_total_rows": total_rows,
    }
    value["manifest_identity"] = batched._identity(value)
    path = root / "batch_manifest.json"
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    return path


def _write_feature(path: Path, start: int, values: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(struct.pack(f"<{len(values) + 1}f", float(start), *values))


def _qlib(root: Path, stock_codes: list[str], index_codes: list[str] | None = None) -> Path:
    (root / "calendars").mkdir(parents=True)
    (root / "instruments").mkdir()
    (root / "calendars" / "day.txt").write_text(DAY0 + "\n", encoding="utf-8")
    (root / "instruments" / "all.txt").write_text(
        "".join(f"{code}\t{DAY0}\t{DAY0}\n" for code in stock_codes),
        encoding="utf-8",
    )
    if index_codes:
        (root / "instruments" / "index.txt").write_text(
            "".join(f"{code}\t{DAY0}\t{DAY0}\n" for code in index_codes),
            encoding="utf-8",
        )
    for code in [*stock_codes, *(index_codes or [])]:
        for field in batched.FIELDS:
            _write_feature(root / "features" / code.casefold() / f"{field}.day.bin", 0, [1.0])
    return root


class _FakeDump:
    def __init__(self, *, returncode: int = 0, skip_field: str | None = None, partial: bool = False):
        self.returncode = returncode
        self.skip_field = skip_field
        self.partial = partial
        self.calls: list[dict[str, object]] = []

    def __call__(self, command, *, check, timeout):
        assert check is False and timeout <= 1800
        mode = command[2]
        data = Path(command[command.index("--data_path") + 1])
        qlib = Path(command[command.index("--qlib_dir") + 1])
        self.calls.append({"mode": mode, "data": data, "timeout": timeout})
        if self.returncode:
            return SimpleNamespace(returncode=self.returncode)
        calendar_path = qlib / "calendars" / "day.txt"
        calendar = [value for value in calendar_path.read_text().splitlines() if value]
        rows_by_code = {}
        for path in sorted(data.glob("*.csv")):
            with path.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            rows_by_code[rows[0]["symbol"].upper()] = rows
            if mode == "dump_update":
                calendar = sorted(set(calendar).union(row["date"] for row in rows))
        calendar_path.write_text("\n".join(calendar) + "\n")
        positions = {value: ordinal for ordinal, value in enumerate(calendar)}
        all_path = qlib / "instruments" / "all.txt"
        all_lines = [value for value in all_path.read_text().splitlines() if value]
        all_codes = {value.split("\t", 1)[0] for value in all_lines}
        for code, rows in rows_by_code.items():
            if code not in all_codes:
                all_lines.append(f"{code}\t{rows[0]['date']}\t{rows[-1]['date']}")
                all_codes.add(code)
            else:
                all_lines = [
                    (
                        f"{code}\t{line.split(chr(9))[1]}\t{rows[-1]['date']}"
                        if line.startswith(code + "\t") and mode == "dump_update"
                        else line
                    )
                    for line in all_lines
                ]
            for ordinal, field in enumerate(batched.FIELDS):
                if field == self.skip_field or (self.partial and ordinal >= 6):
                    continue
                target = qlib / "features" / code.casefold() / f"{field}.day.bin"
                if mode == "dump_update" and target.exists():
                    values = list(struct.unpack(f"<{target.stat().st_size // 4}f", target.read_bytes()))
                    values.extend(float(row[field]) for row in rows if row["date"] == DAY1)
                    target.write_bytes(struct.pack(f"<{len(values)}f", *values))
                else:
                    start = positions[rows[0]["date"]]
                    values = [float(row[field]) for row in rows]
                    _write_feature(target, start, values)
        all_path.write_text("\n".join(all_lines) + "\n")
        return SimpleNamespace(returncode=0)


def _args(manifest: Path, dump_script: Path, qlib: Path) -> argparse.Namespace:
    return argparse.Namespace(
        manifest=str(manifest),
        dump_script=str(dump_script),
        qlib_dir=str(qlib),
        freq="day",
        max_workers=4,
    )


def _script(tmp_path: Path) -> Path:
    path = tmp_path / "dump_bin.py"
    path.write_text("# fixture\n", encoding="utf-8")
    return path


def _codes(count: int) -> list[str]:
    return [f"{ordinal:06d}.SZ" for ordinal in range(1, count + 1)]


def test_override_second_batch_remains_dump_fix_and_preserves_index_authority(tmp_path, monkeypatch):
    stocks = _codes(21)
    index = ["000985.CSI"]
    qlib = _qlib(tmp_path / "qlib", stocks, index)
    (qlib / "calendars" / "day.txt").write_text(f"{DAY0}\n{DAY1}\n", encoding="utf-8")
    original_all = (qlib / "instruments" / "all.txt").read_bytes()
    original_index = (qlib / "instruments" / "index.txt").read_bytes()
    manifest = _manifest(
        tmp_path / "batches",
        phases=[("override", "override", [*stocks, *index])],
    )
    fake = _FakeDump()
    monkeypatch.setattr(batched.subprocess, "run", fake)

    receipt = batched.run(_args(manifest, _script(tmp_path), qlib))

    assert receipt["status"] == "PASS"
    assert [item["mode"] for item in fake.calls] == ["dump_fix", "dump_fix", "dump_fix"]
    assert (qlib / "instruments" / "all.txt").read_bytes() == original_all
    assert (qlib / "instruments" / "index.txt").read_bytes() == original_index


def test_three_hundred_batches_stay_serial_and_receipt_is_bounded(tmp_path, monkeypatch):
    codes = _codes(300)
    qlib = _qlib(tmp_path / "qlib", codes)
    manifest = _manifest(
        tmp_path / "batches",
        phases=[("tail", "tail", codes)],
        batch_size=1,
    )
    fake = _FakeDump()
    monkeypatch.setattr(batched.subprocess, "run", fake)

    receipt = batched.run(_args(manifest, _script(tmp_path), qlib))

    assert receipt["completed_batch_count"] == 300
    assert receipt["peak_codes_per_batch"] == 1
    assert receipt["all_market_frames_retained"] == 0
    assert len(fake.calls) == 300
    assert (tmp_path / "batches" / "batched_dump_receipt.json").stat().st_size < 512_000


def test_tail_journal_never_full_hashes_existing_feature_history(
    tmp_path,
    monkeypatch,
):
    codes = _codes(3)
    qlib = _qlib(tmp_path / "qlib", codes)
    manifest = _manifest(
        tmp_path / "batches",
        phases=[("tail", "tail", codes)],
    )
    feature_hashes = []
    original_sha256 = batched._sha256

    def counted_sha256(path):
        if "features" in Path(path).parts:
            feature_hashes.append(Path(path))
        return original_sha256(path)

    monkeypatch.setattr(batched, "_sha256", counted_sha256)
    fake = _FakeDump()
    monkeypatch.setattr(batched.subprocess, "run", fake)

    receipt = batched.run(_args(manifest, _script(tmp_path), qlib))

    assert receipt["status"] == "PASS"
    assert feature_hashes == []


def test_full_uses_preseeded_global_calendar_and_only_bounded_dump_fix_batches(
    tmp_path,
    monkeypatch,
):
    stocks = _codes(45)
    index = ["000985.CSI"]
    qlib = _qlib(tmp_path / "qlib", stocks, index)
    (qlib / "calendars" / "day.txt").write_text(f"{DAY0}\n{DAY1}\n", encoding="utf-8")
    # FULL pre-seeds authority only; per-code features do not exist before
    # the first dump_fix batch.
    for path in sorted(
        (qlib / "features").rglob("*"),
        key=lambda item: len(item.parts),
        reverse=True,
    ):
        path.unlink() if path.is_file() else path.rmdir()
    original_calendar = (qlib / "calendars" / "day.txt").read_bytes()
    original_all = (qlib / "instruments" / "all.txt").read_bytes()
    original_index = (qlib / "instruments" / "index.txt").read_bytes()
    manifest = _manifest(
        tmp_path / "batches",
        phases=[("full", "full", [*stocks, *index])],
    )
    fake = _FakeDump()
    monkeypatch.setattr(batched.subprocess, "run", fake)

    receipt = batched.run(_args(manifest, _script(tmp_path), qlib))

    assert receipt["status"] == "PASS"
    assert receipt["completed_batch_count"] == 4
    assert receipt["peak_codes_per_batch"] == 20
    assert [item["mode"] for item in fake.calls] == [
        "dump_fix",
        "dump_fix",
        "dump_fix",
        "dump_fix",
    ]
    assert all(item["instrument_authority_restored"] for item in receipt["completed_batches"])
    assert (qlib / "calendars" / "day.txt").read_bytes() == original_calendar
    assert (qlib / "instruments" / "all.txt").read_bytes() == original_all
    assert (qlib / "instruments" / "index.txt").read_bytes() == original_index
    # Even-numbered codes list on DAY1; their feature start stays aligned to
    # position 1 of the preseeded global calendar rather than a batch calendar.
    start, _floats = batched._float_file(qlib / "features" / "000002.sz" / "close.day.bin")
    assert start == 1


@pytest.mark.parametrize("returncode,skip", [(9, None), (0, "close")])
def test_upstream_failure_and_rc_zero_silent_field_failure_are_rejected(tmp_path, monkeypatch, returncode, skip):
    codes = _codes(1)
    qlib = _qlib(tmp_path / "qlib", codes)
    manifest = _manifest(tmp_path / "batches", phases=[("tail", "tail", codes)])
    fake = _FakeDump(returncode=returncode, skip_field=skip)
    monkeypatch.setattr(batched.subprocess, "run", fake)

    with pytest.raises(batched.BatchedDumpError):
        batched.run(_args(manifest, _script(tmp_path), qlib))


def test_extra_and_symlink_batch_entries_are_rejected(tmp_path, monkeypatch):
    codes = _codes(1)
    qlib = _qlib(tmp_path / "qlib", codes)
    manifest = _manifest(tmp_path / "batches", phases=[("tail", "tail", codes)])
    batch = next((tmp_path / "batches" / "tail").iterdir())
    (batch / "extra.txt").write_text("unexpected")
    fake = _FakeDump()
    monkeypatch.setattr(batched.subprocess, "run", fake)
    with pytest.raises(batched.BatchedDumpError, match="extra/non-CSV"):
        batched.run(_args(manifest, _script(tmp_path), qlib))
    assert not fake.calls


def test_timeout_is_typed_without_sleep(tmp_path, monkeypatch):
    codes = _codes(1)
    qlib = _qlib(tmp_path / "qlib", codes)
    manifest = _manifest(tmp_path / "batches", phases=[("tail", "tail", codes)])

    def timeout(command, *, check, timeout):
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(batched.subprocess, "run", timeout)
    with pytest.raises(batched.BatchedDumpTimeout):
        batched.run(_args(manifest, _script(tmp_path), qlib))


def test_full_slow_first_batch_never_submits_later_batches(tmp_path, monkeypatch):
    codes = _codes(41)
    qlib = _qlib(tmp_path / "qlib", codes)
    (qlib / "calendars" / "day.txt").write_text(f"{DAY0}\n{DAY1}\n", encoding="utf-8")
    for path in sorted(
        (qlib / "features").rglob("*"),
        key=lambda item: len(item.parts),
        reverse=True,
    ):
        path.unlink() if path.is_file() else path.rmdir()
    manifest = _manifest(
        tmp_path / "batches",
        phases=[("full", "full", codes)],
    )
    calls = []

    def timeout(command, *, check, timeout):
        calls.append(tuple(command))
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(batched.subprocess, "run", timeout)

    with pytest.raises(batched.BatchedDumpTimeout):
        batched.run(_args(manifest, _script(tmp_path), qlib))

    assert len(calls) == 1


def test_checkpoint_and_recovery_resume_only_after_audited_batch(tmp_path, monkeypatch):
    codes = _codes(2)
    qlib = _qlib(tmp_path / "qlib", codes)
    manifest = _manifest(tmp_path / "batches", phases=[("tail", "tail", codes)], batch_size=1)
    signal = tmp_path / "checkpoint.json"
    signal.write_text(
        json.dumps(
            {
                "schema_version": batched.RESOURCE_SIGNAL_SCHEMA,
                "attempt_id": "attempt-1",
                "fence": 1,
                "execution_id": "build-dump-daily",
            }
        )
    )
    monkeypatch.setenv("DATASET_RESOURCE_CHECKPOINT_FILE", str(signal))
    fake = _FakeDump()
    monkeypatch.setattr(batched.subprocess, "run", fake)
    args = _args(manifest, _script(tmp_path), qlib)
    with pytest.raises(batched.BatchedDumpCheckpoint):
        batched.run(args)
    assert len(fake.calls) == 1
    signal.unlink()
    receipt = batched.run(args)
    assert receipt["status"] == "PASS"
    assert len(fake.calls) == 2


def test_crash_after_child_before_receipt_recovers_without_double_append(tmp_path, monkeypatch):
    codes = _codes(1)
    qlib = _qlib(tmp_path / "qlib", codes)
    manifest = _manifest(tmp_path / "batches", phases=[("tail", "tail", codes)])
    fake = _FakeDump()
    monkeypatch.setattr(batched.subprocess, "run", fake)
    original = batched._atomic_json
    crashed = False

    def crash_receipt(path, payload):
        nonlocal crashed
        if payload.get("status") == "IN_PROGRESS" and not crashed:
            crashed = True
            raise RuntimeError("fixture crash after child")
        return original(path, payload)

    monkeypatch.setattr(batched, "_atomic_json", crash_receipt)
    args = _args(manifest, _script(tmp_path), qlib)
    with pytest.raises(RuntimeError, match="fixture crash"):
        batched.run(args)
    monkeypatch.setattr(batched, "_atomic_json", original)

    receipt = batched.run(args)

    assert receipt["status"] == "PASS"
    assert len(fake.calls) == 1
    assert receipt["completed_batches"][0]["recovered_from_inflight"] is True


def test_same_size_override_crash_recovers_from_content_bound_state(
    tmp_path,
    monkeypatch,
):
    codes = _codes(1)
    qlib = _qlib(tmp_path / "qlib", codes)
    (qlib / "calendars" / "day.txt").write_text(f"{DAY0}\n{DAY1}\n", encoding="utf-8")
    for field in batched.FIELDS:
        _write_feature(
            qlib / "features" / codes[0].casefold() / f"{field}.day.bin",
            0,
            [1.0, 2.0],
        )
    manifest = _manifest(
        tmp_path / "batches",
        phases=[("override", "override", codes)],
    )
    fake = _FakeDump()
    monkeypatch.setattr(batched.subprocess, "run", fake)
    original = batched._atomic_json
    crashed = False

    def crash_receipt(path, payload):
        nonlocal crashed
        if payload.get("status") == "IN_PROGRESS" and not crashed:
            crashed = True
            raise RuntimeError("fixture crash after same-size override")
        return original(path, payload)

    monkeypatch.setattr(batched, "_atomic_json", crash_receipt)
    args = _args(manifest, _script(tmp_path), qlib)
    with pytest.raises(RuntimeError, match="same-size override"):
        batched.run(args)
    monkeypatch.setattr(batched, "_atomic_json", original)

    receipt = batched.run(args)

    assert len(fake.calls) == 1
    assert receipt["completed_batches"][0]["recovered_from_inflight"] is True


def test_partial_post_child_state_fails_closed_without_rerunning(tmp_path, monkeypatch):
    codes = _codes(1)
    qlib = _qlib(tmp_path / "qlib", codes)
    manifest = _manifest(tmp_path / "batches", phases=[("tail", "tail", codes)])
    fake = _FakeDump(partial=True)
    monkeypatch.setattr(batched.subprocess, "run", fake)
    args = _args(manifest, _script(tmp_path), qlib)
    with pytest.raises(batched.BatchedDumpError):
        batched.run(args)
    with pytest.raises(batched.BatchedDumpError):
        batched.run(args)
    assert len(fake.calls) == 1


def test_manifest_rejects_workers_and_batch_size_above_hard_bounds(tmp_path):
    codes = _codes(1)
    qlib = _qlib(tmp_path / "qlib", codes)
    manifest = _manifest(tmp_path / "batches", phases=[("tail", "tail", codes)])
    args = _args(manifest, _script(tmp_path), qlib)
    args.max_workers = 9
    with pytest.raises(batched.BatchedDumpError, match="workers"):
        batched.run(args)
