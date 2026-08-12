from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.services.dataset_release.pit import (
    PitSpanInvalid,
    PitSnapshotError,
    PitStateNotReady,
    filter_frame_to_pit_spans,
    frozen_pit_snapshot_from_mapping,
    freeze_pit_snapshot,
    write_frozen_pit_snapshot,
    write_pit_all_txt,
)


SHA_A = "a" * 64
SHA_B = "b" * 64


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("000001.SZ", "2026-07-08", "2026-08-03", "restore", None),
            ("000001.SZ", "2026-06-01", "2026-07-03", "listed", "st"),
            ("600000.SH", "2020-01-01", "2099-12-31", "listed", None),
        ],
        columns=[
            "ts_code",
            "eligible_start",
            "eligible_end",
            "entry_reason",
            "exit_reason",
        ],
    )


def _freeze(rows: pd.DataFrame):
    return freeze_pit_snapshot(
        rows,
        universe_key="shsz_st_pit_active_v1",
        rule_version="st_pub_next_trade_restore_active_l_v1",
        scope_start=date(2026, 7, 1),
        cutoff=date(2026, 7, 31),
        state_identity="state-17",
        source_fingerprint_sha256=SHA_A,
        parameter_hash=SHA_B,
        state_start=date(2018, 8, 1),
        state_end=date(2026, 7, 31),
    )


def test_frozen_pit_is_order_independent_scope_clipped_and_canonical() -> None:
    first = _freeze(_rows())
    second = _freeze(_rows().sample(frac=1.0, random_state=7))

    assert first.spans_sha256 == second.spans_sha256
    assert first.canonical_bytes() == second.canonical_bytes()
    assert first.unique_instruments == 2
    frame = first.to_frame()
    assert frame.iloc[0]["eligible_start"] == date(2026, 7, 1)
    assert frame.iloc[-1]["eligible_end"] == date(2026, 7, 31)
    assert list(frame["ts_code"]) == ["000001.SZ", "000001.SZ", "600000.SH"]
    assert frozen_pit_snapshot_from_mapping(first.as_dict()) == first

    tampered = first.as_dict()
    tampered["spans_sha256"] = "0" * 64
    with pytest.raises(PitSnapshotError, match="identity/digest differs"):
        frozen_pit_snapshot_from_mapping(tampered)


def test_frozen_pit_rejects_stale_state_overlap_and_non_shsz() -> None:
    with pytest.raises(PitStateNotReady, match="not ready"):
        freeze_pit_snapshot(
            _rows(),
            universe_key="u",
            rule_version="r",
            scope_start=date(2026, 7, 1),
            cutoff=date(2026, 7, 31),
            state_identity="state",
            source_fingerprint_sha256=SHA_A,
            parameter_hash=SHA_B,
            state_dirty=True,
        )

    overlap = _rows().iloc[:2].copy()
    overlap.loc[1, "eligible_end"] = "2026-07-10"
    with pytest.raises(PitSpanInvalid, match="overlapping"):
        _freeze(overlap)

    bad_exchange = _rows().iloc[[0]].copy()
    bad_exchange.loc[:, "ts_code"] = "430047.BJ"
    with pytest.raises(PitSpanInvalid, match="invalid SH/SZ"):
        _freeze(bad_exchange)


def test_frozen_pit_filters_multiple_spans_and_writes_new_artifacts(tmp_path) -> None:
    snapshot = _freeze(_rows())
    index = pd.MultiIndex.from_product(
        [
            pd.date_range("2026-07-01", "2026-07-10", freq="D"),
            ["000001.SZ", "600000.SH", "000002.SZ"],
        ],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame({"value": range(len(index))}, index=index)

    masked, receipt = filter_frame_to_pit_spans(frame, snapshot)

    assert len(masked.xs("000001.SZ", level="instrument")) == 6
    assert len(masked.xs("600000.SH", level="instrument")) == 10
    assert "000002.SZ" not in set(masked.index.get_level_values("instrument"))
    assert receipt["rows_removed"] == 14
    frozen_path = tmp_path / "pit_snapshot.json"
    frozen_receipt = write_frozen_pit_snapshot(frozen_path, snapshot)
    assert frozen_receipt["spans_sha256"] == snapshot.spans_sha256
    all_path = tmp_path / "instruments" / "all.txt"
    all_receipt = write_pit_all_txt(all_path, snapshot, masked)
    assert all_receipt["span_lines"] == 3
    assert all_receipt["multi_span_lines"] == 1
    assert all_path.read_text(encoding="utf-8").count("000001.SZ") == 2

    with pytest.raises(FileExistsError):
        write_frozen_pit_snapshot(frozen_path, snapshot)
