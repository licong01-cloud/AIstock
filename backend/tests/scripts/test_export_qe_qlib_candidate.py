from __future__ import annotations

import importlib.util
import sys
from argparse import Namespace
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = ROOT / "scripts" / "export_qe_qlib_candidate.py"
SPEC = importlib.util.spec_from_file_location("export_qe_qlib_candidate", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_legacy_daily_loader_refuses_unbounded_batches_before_query(monkeypatch) -> None:
    start = date(2018, 8, 1)
    end = date(2026, 6, 30)
    pool = pd.DataFrame(
        {
            "ts_code": [f"{code:06d}.SZ" for code in range(1000)] + ["000000.SZ"],
            "list_date": [start + timedelta(days=offset) for offset in range(1000)] + [start],
        }
    )
    calls: list[tuple[list[str], date, date, bool, dict[str, date]]] = []

    class FakeReader:
        def load_qlib_daily_data(
            self,
            codes,
            call_start,
            call_end,
            use_tushare_adj,
            instrument_start_dates,
        ):
            batch = list(codes)
            calls.append((batch, call_start, call_end, use_tushare_adj, instrument_start_dates))
            index = pd.MultiIndex.from_tuples(
                [(pd.Timestamp(call_start), batch[0])],
                names=["datetime", "instrument"],
            )
            return pd.DataFrame({"$close": [1.0]}, index=index)

    monkeypatch.setattr(MODULE, "DBReader", FakeReader)

    with pytest.raises(
        MODULE.LegacyUnboundedExportDisabled,
        match="LEGACY_UNBOUNDED_EXPORT_DISABLED",
    ):
        MODULE.load_daily_data(pool, start, end, batch_size=400)

    assert calls == []


def test_legacy_daily_loader_is_bounded_to_sample_batches(monkeypatch) -> None:
    start = date(2018, 8, 1)
    end = date(2026, 6, 30)
    pool = pd.DataFrame(
        {
            "ts_code": [f"{code:06d}.SZ" for code in range(500)],
            "list_date": [start + timedelta(days=offset) for offset in range(500)],
        }
    )
    calls: list[tuple[list[str], date, date, bool, dict[str, date]]] = []

    class FakeReader:
        def load_qlib_daily_data(
            self,
            codes,
            call_start,
            call_end,
            use_tushare_adj,
            instrument_start_dates,
        ):
            batch = list(codes)
            calls.append((batch, call_start, call_end, use_tushare_adj, instrument_start_dates))
            index = pd.MultiIndex.from_tuples(
                [(pd.Timestamp(call_start), batch[0])],
                names=["datetime", "instrument"],
            )
            return pd.DataFrame({"$close": [1.0]}, index=index)

    monkeypatch.setattr(MODULE, "DBReader", FakeReader)
    result = MODULE.load_daily_data(pool, start, end, batch_size=400)

    assert len(calls) == 2
    assert [len(call[0]) for call in calls] == [400, 100]
    assert all(call[1:4] == (start, end, True) for call in calls)
    assert all(set(call[0]) == set(call[4]) for call in calls)
    assert calls[0][4]["000000.SZ"] == start
    assert calls[-1][4]["000499.SZ"] == start + timedelta(days=499)
    assert sum(len(call[0]) for call in calls) == 500
    assert not result.empty


def test_legacy_main_refuses_full_scope_before_any_source_query(monkeypatch) -> None:
    monkeypatch.setattr(
        MODULE,
        "parse_args",
        lambda: Namespace(log_level="INFO", limit_instruments=None),
    )
    monkeypatch.setattr(
        MODULE,
        "get_h5_universe",
        lambda *_args, **_kwargs: pytest.fail("source query must not run"),
    )
    with pytest.raises(
        MODULE.LegacyUnboundedExportDisabled,
        match="LEGACY_UNBOUNDED_EXPORT_DISABLED",
    ):
        MODULE.main()


def test_legacy_main_refuses_candidate_overwrite_before_source_query(monkeypatch) -> None:
    monkeypatch.setattr(
        MODULE,
        "parse_args",
        lambda: Namespace(
            log_level="INFO",
            limit_instruments=10,
            overwrite_candidate=True,
        ),
    )
    monkeypatch.setattr(
        MODULE,
        "get_h5_universe",
        lambda *_args, **_kwargs: pytest.fail("source query must not run"),
    )
    with pytest.raises(
        MODULE.LegacyUnboundedExportDisabled,
        match="LEGACY_CANDIDATE_OVERWRITE_DISABLED",
    ):
        MODULE.main()


def test_legacy_prepare_dir_never_removes_existing_candidate(tmp_path) -> None:
    candidate = tmp_path / "sample_candidate"
    candidate.mkdir()
    sentinel = candidate / "existing.bin"
    sentinel.write_bytes(b"keep")

    with pytest.raises(FileExistsError, match="never overwrites or removes"):
        MODULE.prepare_dir(
            candidate,
            allowed_root=tmp_path,
            overwrite=True,
        )

    assert sentinel.read_bytes() == b"keep"


def test_legacy_wsl_copy_command_refuses_existing_target(monkeypatch, tmp_path) -> None:
    observed: dict[str, object] = {}

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return Namespace(returncode=0, stderr="")

    monkeypatch.setattr(MODULE.subprocess, "run", fake_run)
    MODULE.copy_bin_to_wsl(
        Namespace(
            wsl_copy_dir="/home/test/new_candidate",
            wsl_distro="Ubuntu",
        ),
        tmp_path,
    )

    command = observed["command"]
    assert isinstance(command, list)
    shell = command[-1]
    assert "rm -rf" not in shell
    assert "if [ -e /home/test/new_candidate ]" in shell
    assert "refusing overwrite" in shell


def test_legacy_wsl_paths_must_be_explicit(tmp_path) -> None:
    with pytest.raises(
        MODULE.LegacyUnboundedExportDisabled,
        match="LEGACY_WSL_PATHS_REQUIRED",
    ):
        MODULE.run_wsl_script(
            Namespace(wsl_conda_sh="", rdagent_root_wsl=""),
            "dump_bin.py",
            [],
        )
    with pytest.raises(
        MODULE.LegacyUnboundedExportDisabled,
        match="LEGACY_WSL_COPY_PATH_REQUIRED",
    ):
        MODULE.copy_bin_to_wsl(
            Namespace(wsl_copy_dir="", wsl_distro="Ubuntu"),
            tmp_path,
        )


def test_legacy_snapshot_helpers_bind_the_guarded_custom_root(monkeypatch, tmp_path) -> None:
    snapshot_root = tmp_path / "custom-snapshots"
    snapshot_root.mkdir()
    observed: dict[str, object] = {}

    class FakeWriter:
        def __init__(self, *, root):
            observed["writer_root"] = root

    def fake_field_map(**kwargs):
        observed["field_map"] = kwargs
        return {"ok": True}

    monkeypatch.setattr(MODULE, "SnapshotWriter", FakeWriter)
    monkeypatch.setattr(MODULE, "export_field_map_for_snapshot", fake_field_map)

    MODULE.candidate_snapshot_writer(snapshot_root)
    result = MODULE.candidate_field_map(
        snapshot_root=snapshot_root,
        snapshot_id="isolated_candidate",
    )

    assert observed["writer_root"] == snapshot_root.resolve(strict=True)
    assert observed["field_map"] == {
        "snapshot_id": "isolated_candidate",
        "snapshot_root": snapshot_root.resolve(strict=True),
        "write_to_h5": True,
    }
    assert result == {"ok": True}


def test_read_static_schema_columns_accepts_explicit_source(tmp_path) -> None:
    from backend.services.dataset_release.static_schema import STATIC_COLUMN_DTYPES, STATIC_ORDERED_COLUMNS

    source = tmp_path / "schema.parquet"
    pd.DataFrame(
        {column: pd.Series(dtype=STATIC_COLUMN_DTYPES[column]) for column in STATIC_ORDERED_COLUMNS}
    ).to_parquet(source)

    assert MODULE.read_static_schema_columns(source) == list(STATIC_ORDERED_COLUMNS)


def test_resolve_static_schema_columns_defaults_to_canonical_121() -> None:
    from backend.services.dataset_release.static_schema import STATIC_ORDERED_COLUMNS

    assert MODULE.resolve_static_schema_columns(None) == list(STATIC_ORDERED_COLUMNS)
    assert len(MODULE.resolve_static_schema_columns(None)) == 121


def test_resolve_static_schema_columns_keeps_explicit_file_validation(tmp_path) -> None:
    from backend.services.dataset_release.static_schema import STATIC_COLUMN_DTYPES, STATIC_ORDERED_COLUMNS

    source = tmp_path / "schema.parquet"
    pd.DataFrame(
        {column: pd.Series(dtype=STATIC_COLUMN_DTYPES[column]) for column in STATIC_ORDERED_COLUMNS}
    ).to_parquet(source)

    assert MODULE.resolve_static_schema_columns(source) == list(STATIC_ORDERED_COLUMNS)


def test_read_static_schema_columns_rejects_noncanonical_subset(tmp_path) -> None:
    source = tmp_path / "subset.parquet"
    pd.DataFrame({"beta": [1.0], "l2_code_id": [1], "alpha": [2.0]}).to_parquet(source)

    with pytest.raises(ValueError, match="ordered 121-column contract"):
        MODULE.read_static_schema_columns(source)


def test_read_static_schema_columns_rejects_stale_source(tmp_path) -> None:
    source = tmp_path / "stale.parquet"
    pd.DataFrame({"beta": [1.0]}).to_parquet(source)

    with pytest.raises(ValueError, match="lacks l2_code_id"):
        MODULE.read_static_schema_columns(source)


def test_align_static_schema_preserves_l2_int16_and_unknown_code() -> None:
    frame = pd.DataFrame(
        {"alpha": [1, 2], "l2_code_id": [3, None]},
        index=pd.Index(["a", "b"]),
    )

    result = MODULE.align_static_schema(
        frame,
        ["alpha", "l2_code_id", "missing_factor"],
    )

    assert str(result["alpha"].dtype) == "float32"
    assert str(result["missing_factor"].dtype) == "float32"
    assert str(result["l2_code_id"].dtype) == "int16"
    assert result["l2_code_id"].tolist() == [3, -1]
