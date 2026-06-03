from __future__ import annotations

import datetime as dt
from pathlib import Path

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


def test_custom_format_pg_dump_does_not_use_parallel_jobs(monkeypatch, tmp_path: Path) -> None:
    calls: list[list[str]] = []
    plan = snapshot.SnapshotPlan(
        container="timescaledb",
        pg_user="postgres",
        pg_dbname="aistock",
        target_path=tmp_path / "aistock_pg_20260603.dump",
        in_container_path="/tmp/aistock_pg_20260603.dump",
        is_permanent=False,
        snapshot_date=dt.date(2026, 6, 3),
    )

    monkeypatch.setattr(snapshot, "run", lambda cmd, **kwargs: calls.append(list(cmd)))

    snapshot.execute_pg_dump_inside_container(plan)

    assert calls
    assert "--format=custom" in calls[0]
    assert not any(arg.startswith("--jobs=") for arg in calls[0])


def test_validate_dump_streams_local_file_to_pg_restore(monkeypatch, tmp_path: Path) -> None:
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
    subprocess_calls: list[tuple[list[str], bytes | None]] = []

    def _forbid_bare_restore(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("validate_dump should stream the dump file directly to pg_restore")

    class _Proc:
        stdout = b"; table data\n123; 0 0 TABLE DATA public example postgres\n"

    def _fake_subprocess_run(cmd, *, input=None, capture_output=False, check=False):  # noqa: ANN001
        subprocess_calls.append((list(cmd), input))
        assert capture_output is True
        assert check is True
        return _Proc()

    monkeypatch.setattr(snapshot, "run", _forbid_bare_restore)
    monkeypatch.setattr(snapshot.subprocess, "run", _fake_subprocess_run)

    summary = snapshot.validate_dump(plan)

    assert summary["table_data_entries"] == 1
    assert subprocess_calls == [
        (
            ["docker", "exec", "-i", "timescaledb", "pg_restore", "--list"],
            b"x" * snapshot.MIN_EXPECTED_DUMP_BYTES,
        )
    ]
