from __future__ import annotations

import datetime as dt
import json
import subprocess
from pathlib import Path

import pytest

from scripts import dr_snapshot_prod_db as snapshot


def _prod_cfg() -> dict[str, str]:
    return {
        "TDX_DB_HOST": "127.0.0.1",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "aistock",
    }


def test_parse_args_uses_dr_pg_container_env(monkeypatch) -> None:
    monkeypatch.setenv("DR_PG_CONTAINER", "timescaledb")

    args = snapshot.parse_args([])

    assert args.container == "timescaledb"


def test_parse_args_container_cli_overrides_env(monkeypatch) -> None:
    monkeypatch.setenv("DR_PG_CONTAINER", "timescaledb")

    args = snapshot.parse_args(["--container", "aistock-pg-prod"])

    assert args.container == "aistock-pg-prod"


def test_dry_run_uses_env_container_without_docker(monkeypatch, tmp_path: Path, capsys) -> None:
    monkeypatch.setenv("DR_PG_CONTAINER", "timescaledb")
    monkeypatch.setattr(snapshot, "parse_env", lambda: _prod_cfg())

    rc = snapshot.main(
        [
            "--dry-run",
            "--snapshot-date",
            "2026-06-03",
            "--target-dir",
            str(tmp_path),
        ]
    )

    out = capsys.readouterr().out
    assert rc == 0
    assert "container         : timescaledb" in out
    assert str(tmp_path / "aistock_pg_20260603.dump") in out
    assert "compression level : 1" in out
    assert "DR_STAGE " not in out and "DR_SNAPSHOT_RECEIPT " not in out


@pytest.mark.parametrize("level", [1, 0, 9])
def test_custom_format_pg_dump_does_not_use_parallel_jobs(monkeypatch, tmp_path: Path, level) -> None:
    calls: list[list[str]] = []
    plan = snapshot.SnapshotPlan(
        container="timescaledb",
        pg_user="postgres",
        pg_dbname="aistock",
        target_path=tmp_path / "aistock_pg_20260603.dump",
        in_container_path="/tmp/aistock_pg_20260603.dump",
        is_permanent=False,
        snapshot_date=dt.date(2026, 6, 3),
        compression_level=level,
    )

    monkeypatch.setattr(snapshot, "run", lambda cmd, **kwargs: calls.append(list(cmd)))

    snapshot.execute_pg_dump_inside_container(plan)

    assert calls
    assert "--format=custom" in calls[0]
    assert f"--compress={level}" in calls[0]
    assert not any(arg.startswith("--jobs=") for arg in calls[0])


def test_compression_cli_defaults_fast_and_rejects_invalid_levels():
    assert snapshot.parse_args([]).compression_level == 1
    assert snapshot.parse_args(["--compression-level", "9"]).compression_level == 9
    for value in ("-1", "10", "true"):
        with pytest.raises(SystemExit):
            snapshot.parse_args(["--compression-level", value])


@pytest.mark.parametrize("result", ["valid", "empty", "restore_failed"])
def test_validate_dump_streams_local_file_to_pg_restore(monkeypatch, tmp_path: Path, result: str) -> None:
    dump_path = tmp_path / "aistock_pg_20260603.dump"
    dump_path.write_bytes(b"x" * snapshot.MIN_EXPECTED_DUMP_BYTES)
    plan = snapshot.SnapshotPlan(
        container="timescaledb",
        pg_user="postgres",
        pg_dbname="aistock",
        target_path=dump_path,
        in_container_path="/tmp/aistock_pg_20260603.dump",
        is_permanent=False,
        snapshot_date=dt.date(2026, 6, 3),
    )
    subprocess_calls: list[tuple[list[str], Path]] = []
    streams = []

    def _forbid_bare_restore(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("validate_dump should stream the dump file directly to pg_restore")

    class _Proc:
        stdout = b"123; 0 0 TABLE DATA public example postgres\n" if result == "valid" else b""

    def _fake_subprocess_run(cmd, *, stdin, capture_output=False, check=False):  # noqa: ANN001
        assert not stdin.closed
        streams.append(stdin)
        subprocess_calls.append((list(cmd), Path(stdin.name)))
        assert capture_output is True
        assert check is True
        if result == "restore_failed":
            raise subprocess.CalledProcessError(1, cmd)
        return _Proc()

    monkeypatch.setattr(snapshot, "run", _forbid_bare_restore)
    monkeypatch.setattr(snapshot.subprocess, "run", _fake_subprocess_run)

    if result == "valid":
        receipt = snapshot.validate_dump(plan)
        assert receipt["table_data_entries"] == 1
        assert receipt["validation_scope"] == "toc_structure_only"
        assert receipt["toc_checked"] is True
        assert receipt["archive_payload_verified"] is receipt["restore_exercised"] is False
    else:
        error = SystemExit if result == "empty" else subprocess.CalledProcessError
        with pytest.raises(error):
            snapshot.validate_dump(plan)
    assert streams[0].closed
    assert subprocess_calls == [
        (
            ["docker", "exec", "-i", "timescaledb", "pg_restore", "--list"],
            dump_path,
        )
    ]


@pytest.mark.parametrize("failed_stage", [None, "pg_dump", "copy", "toc", "cleanup", "cleanup_warning"])
def test_main_records_stage_timing_without_claiming_restore(monkeypatch, tmp_path, capsys, failed_stage):
    monkeypatch.setattr(snapshot, "parse_env", lambda: _prod_cfg())
    calls = []
    def stage(name):
        def execute(*args, **kwargs):
            calls.append(name)
            if name == failed_stage:
                raise SystemExit("stage failed")
            if name == "cleanup" and failed_stage == "cleanup_warning":
                return False
            return {"toc_checked": True, "archive_payload_verified": False, "restore_exercised": False}
        return execute
    for name, function in [("pg_dump", "execute_pg_dump_inside_container"), ("copy", "copy_dump_out"),
                           ("toc", "validate_dump"), ("cleanup", "cleanup_in_container")]:
        monkeypatch.setattr(snapshot, function, stage(name))
    args = ["--target-dir", str(tmp_path), "--snapshot-date", "2026-10-13", "--compression-level", "9"]
    failed = failed_stage is not None and failed_stage != "cleanup_warning"
    if failed:
        with pytest.raises(SystemExit, match="stage failed"):
            snapshot.main(args)
    else:
        assert snapshot.main(args) == 0
    output = capsys.readouterr().out.splitlines()
    receipt = json.loads(next(line.removeprefix("DR_SNAPSHOT_RECEIPT ") for line in output
                              if line.startswith("DR_SNAPSHOT_RECEIPT ")))
    assert receipt["status"] == ("failed" if failed else "completed")
    assert receipt["dump_format"] == "custom" and receipt["dump_compress"] == "9"
    assert set(receipt["stages"]) == set(calls) and calls[-1] == "cleanup"
    assert all(item["duration_seconds"] >= 0 for item in receipt["stages"].values())
    expected = "failed" if failed else "warning" if failed_stage == "cleanup_warning" else "completed"
    assert receipt["stages"]["cleanup" if failed_stage == "cleanup_warning" else failed_stage or "toc"]["status"] == expected
    assert receipt["archive_payload_verified"] is receipt["restore_exercised"] is False
