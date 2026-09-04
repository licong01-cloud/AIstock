from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import build_core_index_membership_authority as subject


def _write_current(path: Path, symbols: list[str]) -> None:
    frame = pd.DataFrame(
        [["20260904", "000300", "CSI 300", "CSI 300", symbol] for symbol in symbols],
        columns=["date", "index", "name", "english_name", "constituent"],
    )
    frame.to_excel(path, index=False)


def _write_event(path: Path, additions: list[str], removals: list[str]) -> None:
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(
            [["000300", "CSI 300", symbol] for symbol in additions],
            columns=["index", "name", "symbol"],
        ).to_excel(writer, sheet_name="adds", index=False)
        pd.DataFrame(
            [["000300", "CSI 300", symbol] for symbol in removals],
            columns=["index", "name", "symbol"],
        ).to_excel(writer, sheet_name="removes", index=False)


def _manifest(tmp_path: Path, *, current: list[str], additions: list[str], removals: list[str]) -> Path:
    current_path = tmp_path / "current.xlsx"
    event_path = tmp_path / "event.xlsx"
    _write_current(current_path, current)
    _write_event(event_path, additions, removals)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": subject.SCHEMA_VERSION,
                "window_start": "2018-08-01",
                "cutoff": "2026-08-31",
                "pools": [
                    {
                        "pool_id": "csi300",
                        "current_workbook": str(current_path),
                        "current_as_of": "2026-09-04",
                        "current_code_column": 4,
                        "current_expected_count": len(current),
                        "baseline_source_reference": "CSI:reconstructed-baseline",
                        "events": [
                            {
                                "effective_from": "2024-06-17",
                                "source_reference": "CSI:notice-test",
                                "workbook": str(event_path),
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return manifest


def test_builder_reconstructs_half_open_history_from_official_event(tmp_path: Path) -> None:
    manifest = _manifest(
        tmp_path,
        current=["000001", "600000"],
        additions=["600000"],
        removals=["000002"],
    )

    rows = subject.build_authority(manifest)

    assert rows == [
        {
            "pool_id": "csi300",
            "index_code": "000300.SH",
            "ts_code": "000001.SZ",
            "effective_from": "2018-08-01",
            "effective_to_exclusive": None,
            "source_provider": "CSI",
            "source_reference": "CSI:reconstructed-baseline",
        },
        {
            "pool_id": "csi300",
            "index_code": "000300.SH",
            "ts_code": "000002.SZ",
            "effective_from": "2018-08-01",
            "effective_to_exclusive": "2024-06-17",
            "source_provider": "CSI",
            "source_reference": "CSI:reconstructed-baseline",
        },
        {
            "pool_id": "csi300",
            "index_code": "000300.SH",
            "ts_code": "600000.SH",
            "effective_from": "2024-06-17",
            "effective_to_exclusive": None,
            "source_provider": "CSI",
            "source_reference": "CSI:notice-test",
        },
    ]


def test_builder_is_order_invariant_and_accepts_inline_official_event(tmp_path: Path) -> None:
    current_path = tmp_path / "current.xlsx"
    _write_current(current_path, ["600000", "000001"])
    payload = {
        "schema_version": subject.SCHEMA_VERSION,
        "window_start": "2024-01-02",
        "cutoff": "2026-08-31",
        "pools": [
            {
                "pool_id": "csi300",
                "current_workbook": str(current_path),
                "current_as_of": "2026-09-04",
                "current_expected_count": 2,
                "baseline_source_reference": "CSI:baseline",
                "events": [
                    {
                        "effective_from": "2024-06-17",
                        "source_reference": "CSI:notice",
                        "additions": ["600000"],
                        "removals": ["000002"],
                    }
                ],
            }
        ],
    }
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text(json.dumps(payload), encoding="utf-8")
    payload["pools"][0]["events"][0]["additions"].reverse()
    payload["pools"][0]["events"][0]["removals"].reverse()
    second.write_text(json.dumps(payload), encoding="utf-8")

    assert subject.build_authority(first) == subject.build_authority(second)


def test_builder_fails_closed_on_reverse_continuity_break(tmp_path: Path) -> None:
    manifest = _manifest(
        tmp_path,
        current=["000001"],
        additions=["600000"],
        removals=["000002"],
    )

    with pytest.raises(subject.AuthorityBuildError, match="reverse continuity failed"):
        subject.build_authority(manifest)


def test_builder_rejects_same_symbol_on_both_sides(tmp_path: Path) -> None:
    manifest = _manifest(
        tmp_path,
        current=["000001"],
        additions=["000001"],
        removals=["000001"],
    )

    with pytest.raises(subject.AuthorityBuildError, match="same symbols"):
        subject.build_authority(manifest)


def test_builder_rejects_event_outside_window_and_stale_snapshot(tmp_path: Path) -> None:
    manifest = _manifest(
        tmp_path,
        current=["000001", "600000"],
        additions=["600000"],
        removals=["000002"],
    )
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["pools"][0]["events"][0]["effective_from"] = "2027-01-04"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(subject.AuthorityBuildError, match="outside"):
        subject.build_authority(manifest)

    payload["pools"][0]["events"][0]["effective_from"] = "2024-06-17"
    payload["pools"][0]["current_as_of"] = "2026-06-30"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(subject.AuthorityBuildError, match="predates cutoff"):
        subject.build_authority(manifest)


def test_builder_rejects_mislabeled_current_snapshot(tmp_path: Path) -> None:
    manifest = _manifest(
        tmp_path,
        current=["000001", "600000"],
        additions=["600000"],
        removals=["000002"],
    )
    current = pd.read_excel(tmp_path / "current.xlsx", dtype=object)
    current.loc[:, "index"] = "000905"
    current.to_excel(tmp_path / "current.xlsx", index=False)

    with pytest.raises(subject.AuthorityBuildError, match="index differs"):
        subject.build_authority(manifest)
