"""BUG-989 continuation: frozen suspend_d candidate dataset -> qe_suspend_filter.json.

The suspend artifact must be rebuilt on the compute node exclusively from
frozen, hash-pinned files (``qe_frozen_build_spec.json`` suspend section +
``suspend_d.parquet``/``manifest.json`` + the pinned frozen calendar).  Any
pin, identity, window-coverage, field or date-completeness mismatch fails
closed with a stable reason_code; there is no database fallback, no online
backfill and no silent degradation (a missing pandas/pyarrow also fails loud
instead of disabling the suspend filter).
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import qe_build_frozen_suspend_filter as suspend_builder  # noqa: E402
from qe_suspend_filter import QESuspendFilter  # noqa: E402

CALENDAR_DAYS = ["2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07", "2021-01-08"]
DATASET_ID = "suspend_d_daily_candidate_20180801_20260630"
UNIVERSE_KEY = "shsz_st_pit_active_v1"
SOURCE_CONTRACT = "tushare_suspend_d_shsz_S_v1"

SUSPEND_ROWS = [
    # continuous suspension: 600000.SH suspended 01-04..01-05, resumed 01-06
    ("2021-01-04", "600000.SH", "S", None),
    ("2021-01-05", "600000.SH", "S", None),
    ("2021-01-05", "000001.SZ", "S", "13:00-15:00"),  # intraday suspension
    ("2021-01-07", "000002.SZ", "S", None),
    ("2021-01-07", "000002.SZ", "R", None),  # resumption rows must be ignored
    # 2021-01-06 and 2021-01-08: zero-suspension days (exported, verified)
]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_bin_calendar(root: Path, days=CALENDAR_DAYS) -> str:
    calendars = root / "calendars"
    calendars.mkdir(parents=True)
    calendar_file = calendars / "day.txt"
    calendar_file.write_text("".join(f"{day}\n" for day in days), encoding="utf-8")
    return _sha256(calendar_file)


def _write_suspend_dataset(
    root: Path,
    *,
    rows=SUSPEND_ROWS,
    days=CALENDAR_DAYS,
    start="2021-01-04",
    end="2021-01-08",
    daily_row_counts_override: dict | None = None,
    drop_manifest: bool = False,
) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=["trade_date", "ts_code", "suspend_type", "suspend_timing"])
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    parquet_path = root / "suspend_d.parquet"
    frame.to_parquet(parquet_path, index=False)
    s_counts = (
        frame[frame["suspend_type"] == "S"].groupby(frame["trade_date"].dt.strftime("%Y-%m-%d")).size().to_dict()
    )
    daily_row_counts = {day: int(s_counts.get(day, 0)) for day in days}
    if daily_row_counts_override is not None:
        daily_row_counts = daily_row_counts_override
    manifest = {
        "schema_version": suspend_builder.MANIFEST_SCHEMA_VERSION,
        "kind": "qe_suspend_filter_source",
        "dataset_id": DATASET_ID,
        "start": start,
        "end": end,
        "cutoff": end,
        "universe_key": UNIVERSE_KEY,
        "source": {"schema": "market", "table": "suspend_d", "contract": SOURCE_CONTRACT},
        "daily_row_counts": daily_row_counts,
    }
    manifest_path = root / "manifest.json"
    if not drop_manifest:
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "parquet_sha256": _sha256(parquet_path),
        "manifest_sha256": _sha256(manifest_path) if manifest_path.exists() else "0" * 64,
    }


def _write_spec(
    workspace: Path,
    provider_dir: Path,
    suspend_dir: Path,
    pins: dict,
    *,
    start="2021-01-04",
    end="2021-01-08",
    suspend_section: dict | None = None,
    schema_version: str = suspend_builder.SPEC_SCHEMA_VERSION,
) -> Path:
    if suspend_section is None:
        suspend_section = {
            "dataset_id": DATASET_ID,
            "provider_uri": str(suspend_dir),
            "universe_key": UNIVERSE_KEY,
            "parquet_sha256": pins["parquet_sha256"],
            "manifest_sha256": pins["manifest_sha256"],
            "source_contract": SOURCE_CONTRACT,
        }
    spec = {
        "schema_version": schema_version,
        "kind": "qe_event_risk_policy",
        "provider_uri_day": str(provider_dir),
        "start_date": start,
        "end_date": end,
        "pins": {"calendar_sha256": pins["calendar_sha256"]},
        "suspend": suspend_section,
    }
    workspace.mkdir(parents=True, exist_ok=True)
    spec_path = workspace / suspend_builder.SPEC_FILE
    spec_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")
    return spec_path


def _make_workspace(tmp_path: Path, **dataset_kwargs):
    provider_dir = tmp_path / "bin"
    calendar_sha256 = _write_bin_calendar(provider_dir)
    suspend_dir = tmp_path / "suspend"
    suspend_pins = _write_suspend_dataset(suspend_dir, **dataset_kwargs)
    pins = {"calendar_sha256": calendar_sha256, **suspend_pins}
    workspace = tmp_path / "ws"
    return provider_dir, suspend_dir, pins, workspace


def test_build_artifact_covers_every_calendar_day_including_zero_suspension_days(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    _write_spec(workspace, provider_dir, suspend_dir, pins)

    artifact = suspend_builder.ensure_frozen_suspend_filter_artifact(
        cwd=workspace, print_fn=lambda *_a, **_k: None
    )
    assert artifact == workspace / "qe_suspend_filter.json"
    payload = json.loads(artifact.read_text(encoding="utf-8"))

    assert payload["enabled"] is True
    assert payload["source"] == "frozen:suspend_d.parquet"
    assert payload["audit_dataset"] == DATASET_ID
    assert payload["source_contract"] == SOURCE_CONTRACT
    assert payload["start_date"] == "2021-01-04"
    assert payload["end_date"] == "2021-01-08"
    assert payload["trade_date_count"] == 5
    assert payload["suspend_d_parquet_sha256"] == pins["parquet_sha256"]
    assert payload["suspend_d_manifest_sha256"] == pins["manifest_sha256"]

    by_date = payload["suspended_by_date"]
    # Strict contract: one key per pinned calendar day; zero-suspension days
    # are explicit empty lists, never missing keys.
    assert sorted(by_date) == CALENDAR_DAYS
    assert by_date["2021-01-04"] == ["600000.SH"]
    assert by_date["2021-01-05"] == ["000001.SZ", "600000.SH"]
    assert by_date["2021-01-06"] == []  # exported zero-suspension day
    assert by_date["2021-01-07"] == ["000002.SZ"]  # 'R' resumption row excluded
    assert by_date["2021-01-08"] == []
    assert payload["suspended_row_count"] == 4


def test_rebuild_is_deterministic(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    _write_spec(workspace, provider_dir, suspend_dir, pins)
    first = suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)
    first_bytes = first.read_bytes()
    second = suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)
    assert first_bytes == second.read_bytes()


def test_artifact_feeds_strict_runtime_filter_end_to_end(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    _write_spec(workspace, provider_dir, suspend_dir, pins)
    artifact = suspend_builder.ensure_frozen_suspend_filter_artifact(
        cwd=workspace, print_fn=lambda *_a, **_k: None
    )
    runtime = QESuspendFilter(enabled=True, suspend_filter_file=str(artifact), strict=True)

    # tushare and qlib symbol spellings both match (bidirectional aliases).
    assert runtime.is_suspended("600000.SH", "2021-01-04") is True
    assert runtime.is_suspended("SH600000", "2021-01-05") is True
    assert runtime.is_suspended("000001.SZ", "2021-01-05") is True
    assert runtime.is_suspended("000001.SZ", "2021-01-04") is False
    # Continuous suspension: still suspended on day 2, free after resumption.
    assert runtime.is_suspended("600000.SH", "2021-01-05") is True
    assert runtime.is_suspended("600000.SH", "2021-01-06") is False
    # Zero-suspension days are valid keys with empty sets (strict mode OK).
    assert runtime.suspended_symbols("2021-01-08") == set()
    # A date outside the artifact window is a missing key -> strict raise.
    with pytest.raises(RuntimeError, match="no entry for trade date 2021-01-11"):
        runtime.suspended_symbols("2021-01-11")


def test_window_subrange_only_emits_days_inside_window(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    _write_spec(workspace, provider_dir, suspend_dir, pins, start="2021-01-05", end="2021-01-07")
    artifact = suspend_builder.ensure_frozen_suspend_filter_artifact(
        cwd=workspace, print_fn=lambda *_a, **_k: None
    )
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert sorted(payload["suspended_by_date"]) == ["2021-01-05", "2021-01-06", "2021-01-07"]
    assert payload["trade_date_count"] == 3


def test_missing_spec_is_legacy_noop(tmp_path):
    assert (
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=tmp_path, print_fn=lambda *_a, **_k: None)
        is None
    )


def test_spec_without_suspend_section_is_legacy_noop(tmp_path):
    spec_path = tmp_path / suspend_builder.SPEC_FILE
    spec_path.write_text(
        json.dumps({"schema_version": suspend_builder.SPEC_SCHEMA_VERSION, "kind": "qe_event_risk_policy"}),
        encoding="utf-8",
    )
    assert (
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=tmp_path, print_fn=lambda *_a, **_k: None)
        is None
    )


def test_parquet_pin_mismatch_fails_closed(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    _write_spec(workspace, provider_dir, suspend_dir, pins)
    with (suspend_dir / "suspend_d.parquet").open("ab") as handle:
        handle.write(b"tampered")
    with pytest.raises(RuntimeError, match="qe_frozen_suspend_pin_mismatch"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_missing_manifest_fails_closed(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path, drop_manifest=True)
    _write_spec(workspace, provider_dir, suspend_dir, pins)
    with pytest.raises(RuntimeError, match="qe_frozen_suspend_file_missing"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_missing_suspend_dir_fails_closed(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    _write_spec(workspace, provider_dir, tmp_path / "absent_suspend", pins)
    with pytest.raises(RuntimeError, match="qe_frozen_suspend_dir_missing"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_calendar_pin_mismatch_fails_closed(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    _write_spec(workspace, provider_dir, suspend_dir, pins)
    with (provider_dir / "calendars" / "day.txt").open("a", encoding="utf-8") as handle:
        handle.write("2021-01-11\n")
    with pytest.raises(RuntimeError, match="qe_frozen_universe_pin_mismatch"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_identity_mismatch_fails_closed(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    spec_path = _write_spec(workspace, provider_dir, suspend_dir, pins)
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    spec["suspend"]["dataset_id"] = "suspend_d_OTHER_dataset"
    spec_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")
    with pytest.raises(RuntimeError, match="qe_frozen_suspend_identity_mismatch"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_window_not_covered_fails_closed(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    _write_spec(workspace, provider_dir, suspend_dir, pins, end="2021-02-01")
    with pytest.raises(RuntimeError, match="qe_frozen_suspend_window_not_covered"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_incomplete_date_completeness_receipt_fails_closed(tmp_path):
    # A manifest that silently drops one calendar day from the receipt cannot
    # distinguish "zero suspensions" from "day not exported" -> fail closed.
    counts = {day: 0 for day in CALENDAR_DAYS if day != "2021-01-06"}
    counts["2021-01-04"] = 1
    counts["2021-01-05"] = 2
    counts["2021-01-07"] = 1
    provider_dir, suspend_dir, pins, workspace = _make_workspace(
        tmp_path, daily_row_counts_override=counts
    )
    _write_spec(workspace, provider_dir, suspend_dir, pins)
    with pytest.raises(RuntimeError, match="qe_frozen_suspend_coverage_receipt_incomplete"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_coverage_count_mismatch_fails_closed(tmp_path):
    counts = {day: 0 for day in CALENDAR_DAYS}
    counts["2021-01-04"] = 2  # parquet has 1 row that day
    counts["2021-01-05"] = 2
    counts["2021-01-07"] = 1
    provider_dir, suspend_dir, pins, workspace = _make_workspace(
        tmp_path, daily_row_counts_override=counts
    )
    _write_spec(workspace, provider_dir, suspend_dir, pins)
    with pytest.raises(RuntimeError, match="qe_frozen_suspend_coverage_mismatch"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_missing_required_field_fails_closed(tmp_path):
    rows = [("2021-01-04", "600000.SH", "S", None)]
    frame = pd.DataFrame(rows, columns=["trade_date", "ts_code", "suspend_type", "suspend_timing"])
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    frame = frame.drop(columns=["suspend_type"])
    provider_dir = tmp_path / "bin"
    calendar_sha256 = _write_bin_calendar(provider_dir)
    suspend_dir = tmp_path / "suspend"
    suspend_dir.mkdir(parents=True)
    parquet_path = suspend_dir / "suspend_d.parquet"
    frame.to_parquet(parquet_path, index=False)
    manifest = {
        "schema_version": suspend_builder.MANIFEST_SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "start": "2021-01-04",
        "end": "2021-01-08",
        "universe_key": UNIVERSE_KEY,
        "source": {"contract": SOURCE_CONTRACT},
        "daily_row_counts": {day: 0 for day in CALENDAR_DAYS},
    }
    manifest_path = suspend_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pins = {
        "calendar_sha256": calendar_sha256,
        "parquet_sha256": _sha256(parquet_path),
        "manifest_sha256": _sha256(manifest_path),
    }
    workspace = tmp_path / "ws"
    _write_spec(workspace, provider_dir, suspend_dir, pins)
    with pytest.raises(RuntimeError, match="qe_frozen_suspend_field_missing"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_non_canonical_or_bj_symbol_fails_closed(tmp_path):
    rows = [("2021-01-04", "430047.BJ", "S", None)]
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path, rows=rows)
    _write_spec(workspace, provider_dir, suspend_dir, pins)
    with pytest.raises(RuntimeError, match="qe_frozen_suspend_symbol_invalid"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_spec_schema_version_mismatch_fails_closed(tmp_path):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    _write_spec(workspace, provider_dir, suspend_dir, pins, schema_version="qe_frozen_build_spec_v0")
    with pytest.raises(RuntimeError, match="qe_frozen_build_spec_invalid"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)


def test_missing_pandas_fails_loud_instead_of_disabling_filter(tmp_path, monkeypatch):
    provider_dir, suspend_dir, pins, workspace = _make_workspace(tmp_path)
    _write_spec(workspace, provider_dir, suspend_dir, pins)
    monkeypatch.setitem(sys.modules, "pandas", None)
    with pytest.raises(RuntimeError, match="qe_frozen_suspend_dependency_missing"):
        suspend_builder.ensure_frozen_suspend_filter_artifact(cwd=workspace, print_fn=lambda *_a, **_k: None)
