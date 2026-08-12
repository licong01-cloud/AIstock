from __future__ import annotations

import sys
from pathlib import Path

from backend.services.dataset_release.log_store import bounded_tail
from backend.services.dataset_release.subprocess_runner import run_streamed


def test_runner_streams_large_stdout_and_stderr_to_bounded_segments(tmp_path: Path) -> None:
    result = run_streamed(
        [
            sys.executable,
            "-c",
            "import sys; sys.stdout.write('o'*3000); sys.stderr.write('e'*2500)",
        ],
        cwd=tmp_path,
        log_root=tmp_path / "logs",
        segment_limit_bytes=1024,
    )
    assert result.returncode == 0
    stdout_segments = [item for item in result.log_segments if item["stream"] == "stdout"]
    stderr_segments = [item for item in result.log_segments if item["stream"] == "stderr"]
    assert [item["size_bytes"] for item in stdout_segments] == [1024, 1024, 952]
    assert [item["size_bytes"] for item in stderr_segments] == [1024, 1024, 452]
    assert len(bounded_tail(tmp_path / "logs", "stdout", max_bytes=100, max_lines=10)) == 100


def test_runner_preserves_nonzero_status_without_calling_shell(tmp_path: Path) -> None:
    marker = tmp_path / "marker.txt"
    result = run_streamed(
        [sys.executable, "-c", "import sys; sys.exit(7)", ";touch", str(marker)],
        cwd=tmp_path,
        log_root=tmp_path / "logs",
    )
    assert result.returncode == 7
    assert marker.exists() is False


def test_runner_reports_shared_log_capacity_without_unbounded_segment_list(
    tmp_path: Path,
) -> None:
    result = run_streamed(
        [sys.executable, "-c", "import sys; sys.stdout.write('x'*5000)"],
        cwd=tmp_path,
        log_root=tmp_path / "logs",
        segment_limit_bytes=1024,
        max_log_segments=2,
    )

    assert result.cooperative_reason == "log_capacity"
    assert len(result.log_segments) == 2
