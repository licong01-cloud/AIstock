from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backend.services.dataset_release.log_store import (
    LogCapacityExceeded,
    LogSegmentBudget,
    LogStoreError,
    MAX_LOG_SEGMENTS,
    MAX_LOG_TOTAL_BYTES,
    RotatingLogWriter,
    bounded_tail,
    read_log_page,
)


def test_default_log_contract_keeps_128_segments_and_explicit_two_gib_cap() -> None:
    budget = LogSegmentBudget()

    assert MAX_LOG_SEGMENTS == 128
    assert MAX_LOG_TOTAL_BYTES == 2 * 1024**3
    assert budget.max_segments == MAX_LOG_SEGMENTS
    assert budget.max_total_bytes == MAX_LOG_TOTAL_BYTES


def test_log_segments_rotate_without_accumulating_full_output(tmp_path: Path) -> None:
    writer = RotatingLogWriter(tmp_path, "stdout", segment_limit_bytes=1024)
    writer.write(b"a" * 2500)
    writer.close()
    assert [item.size_bytes for item in writer.segments] == [1024, 1024, 452]
    for item in writer.segments:
        path = tmp_path / item.path
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item.sha256
    index = json.loads((tmp_path / "stdout.index.json").read_text(encoding="utf-8"))
    assert len(index["segments"]) == 3
    assert not list(tmp_path.glob("*.partial"))


def test_writer_blocks_before_opening_segment_beyond_shared_capacity(
    tmp_path: Path,
) -> None:
    budget = LogSegmentBudget(max_segments=2)
    stdout = RotatingLogWriter(tmp_path, "stdout", segment_limit_bytes=1024, segment_budget=budget)
    stderr = RotatingLogWriter(tmp_path, "stderr", segment_limit_bytes=1024, segment_budget=budget)
    stdout.write(b"a" * 2048)

    with pytest.raises(LogCapacityExceeded) as exc:
        stderr.write(b"b")

    stdout.close()
    stderr.close()
    assert exc.value.code == "CONTROL_ROOT_CAPACITY_EXCEEDED"
    assert budget.count == 2
    assert len(stdout.segments) == 2
    assert not list(tmp_path.glob("stderr.*.partial"))


def test_writer_blocks_before_exceeding_shared_total_byte_capacity(
    tmp_path: Path,
) -> None:
    budget = LogSegmentBudget(max_segments=10, max_total_bytes=1500)
    stdout = RotatingLogWriter(tmp_path, "stdout", segment_limit_bytes=1024, segment_budget=budget)
    stderr = RotatingLogWriter(tmp_path, "stderr", segment_limit_bytes=1024, segment_budget=budget)
    stdout.write(b"a" * 1024)

    with pytest.raises(LogCapacityExceeded) as exc:
        stderr.write(b"b" * 477)

    stdout.close()
    stderr.close()
    assert exc.value.code == "CONTROL_ROOT_CAPACITY_EXCEEDED"
    assert budget.bytes_written == 1024
    assert budget.count == 1
    assert sum(path.stat().st_size for path in tmp_path.glob("*.log")) == 1024
    assert not list(tmp_path.glob("stderr.*.partial"))


def test_tail_is_bounded_by_bytes_and_lines(tmp_path: Path) -> None:
    writer = RotatingLogWriter(tmp_path, "stderr", segment_limit_bytes=1024)
    writer.write(b"".join(f"line-{index:04d}\n".encode() for index in range(500)))
    writer.close()
    tail = bounded_tail(tmp_path, "stderr", max_bytes=200, max_lines=5)
    assert len(tail) <= 200
    assert tail.count(b"\n") == 5
    assert tail.endswith(b"line-0499\n")


@pytest.mark.parametrize(
    ("max_bytes", "max_lines"),
    [(0, 10), (1024**2 + 1, 10), (100, 0), (100, 1001)],
)
def test_tail_rejects_unbounded_requests(tmp_path: Path, max_bytes: int, max_lines: int) -> None:
    tmp_path.mkdir(exist_ok=True)
    with pytest.raises(LogStoreError, match="tail bounds"):
        bounded_tail(tmp_path, "worker", max_bytes=max_bytes, max_lines=max_lines)


def test_log_writer_rejects_non_allowlisted_stream(tmp_path: Path) -> None:
    with pytest.raises(LogStoreError, match="allowlisted"):
        RotatingLogWriter(tmp_path, "../../escape")


def test_log_page_uses_forward_generation_and_byte_cursor(tmp_path: Path) -> None:
    writer = RotatingLogWriter(tmp_path, "worker", segment_limit_bytes=1024)
    writer.write(b"".join(f"line-{index:04d}\n".encode() for index in range(250)))
    writer.close()

    first = read_log_page(tmp_path, "worker", max_bytes=256, max_lines=5)
    assert first.data.count(b"\n") == 5
    assert first.has_more is True
    assert first.next_generation == 1
    assert first.next_byte_offset == len(first.data)
    second = read_log_page(
        tmp_path,
        "worker",
        generation=first.next_generation,
        byte_offset=first.next_byte_offset,
        max_bytes=256,
        max_lines=5,
    )
    assert second.data.startswith(b"line-0005")


def test_log_page_missing_root_is_empty_and_cursor_is_validated(tmp_path: Path) -> None:
    page = read_log_page(tmp_path / "missing", "stdout")
    assert page.data == b""
    assert page.has_more is False
    with pytest.raises(LogStoreError, match="cursor"):
        read_log_page(tmp_path / "missing", "stdout", generation=0)
