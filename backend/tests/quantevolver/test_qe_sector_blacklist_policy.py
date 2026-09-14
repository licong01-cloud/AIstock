from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.quantevolver.qe_sector_blacklist_policy import (
    QESectorBlacklistPolicyError,
    materialize_sector_blacklist_universe,
    requested_sector_codes,
)


CALENDAR = [
    dt.date(2026, 8, 3),
    dt.date(2026, 8, 4),
    dt.date(2026, 8, 5),
    dt.date(2026, 8, 6),
]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _frozen_inputs(
    tmp_path: Path,
    *,
    unknown: bool = False,
    omit_last: bool = False,
    reverse_rows: bool = False,
):
    code_map = {
        "schema_version": "qe_sw_l2_code_map_v1",
        "ordered_codes": ["801010.SI", "801020.SI"],
    }
    code_map["code_map_digest"] = digest_named_fields(
        "dataset_release_sw_l2_code_map_v1",
        {"ordered_codes": code_map["ordered_codes"]},
    )
    map_path = tmp_path / "sector_code_map.json"
    map_path.write_text(json.dumps(code_map, sort_keys=True), encoding="utf-8")
    rows = [
        {
            "instrument": "000001.SZ",
            "start_date": "2026-08-03",
            "end_date": "2026-08-04",
            "l2_code_id": 0,
        },
        {
            "instrument": "000001.SZ",
            "start_date": "2026-08-05",
            "end_date": "2026-08-06",
            "l2_code_id": -1 if unknown else 1,
        },
        {
            "instrument": "000002.SZ",
            "start_date": "2026-08-03",
            "end_date": "2026-08-05" if omit_last else "2026-08-06",
            "l2_code_id": 1,
        },
    ]
    if reverse_rows:
        rows.reverse()
    membership_path = tmp_path / "sector_membership_spans.parquet"
    pd.DataFrame(rows).to_parquet(membership_path, index=False)
    pins = {
        "schema_version": "qe_sector_policy_input_v1",
        "membership_file": membership_path.name,
        "membership_sha256": _sha(membership_path),
        "code_map_file": map_path.name,
        "code_map_sha256": _sha(map_path),
        "start": CALENDAR[0].isoformat(),
        "end": CALENDAR[-1].isoformat(),
        "universe_key": "aistock_equity_pit_canonical_v2",
    }
    return pins


def test_materialize_sector_blacklist_preserves_pit_transitions(tmp_path: Path) -> None:
    result = materialize_sector_blacklist_universe(
        base_intervals=[
            ("000001.SZ", CALENDAR[0], CALENDAR[-1]),
            ("000002.SZ", CALENDAR[0], CALENDAR[-1]),
        ],
        calendar=CALENDAR,
        window_start=CALENDAR[0],
        window_end=CALENDAR[-1],
        factor_root=tmp_path,
        pins=_frozen_inputs(tmp_path),
        blacklist_codes=["801010.SI"],
    )

    assert result.instruments_content == (
        "000001.SZ\t2026-08-05\t2026-08-06\n"
        "000002.SZ\t2026-08-03\t2026-08-06\n"
    )
    assert result.diagnostics["blacklist_excluded_count"] == 1
    assert result.diagnostics["blacklist_excluded_membership_days"] == 2
    assert result.diagnostics["effective"] is True


@pytest.mark.parametrize(
    ("fixture_kwargs", "reason_code"),
    [
        ({"unknown": True}, "qe_sector_blacklist_membership_unknown"),
        ({"omit_last": True}, "qe_sector_blacklist_membership_incomplete"),
    ],
)
def test_materialize_sector_blacklist_fails_closed_on_membership_gaps(
    tmp_path: Path,
    fixture_kwargs: dict,
    reason_code: str,
) -> None:
    pins = _frozen_inputs(tmp_path, **fixture_kwargs)
    with pytest.raises(QESectorBlacklistPolicyError, match=reason_code):
        materialize_sector_blacklist_universe(
            base_intervals=[("000001.SZ", CALENDAR[0], CALENDAR[-1]), ("000002.SZ", CALENDAR[0], CALENDAR[-1])],
            calendar=CALENDAR,
            window_start=CALENDAR[0],
            window_end=CALENDAR[-1],
            factor_root=tmp_path,
            pins=pins,
            blacklist_codes=["801010.SI"],
        )


def test_materialize_sector_blacklist_rejects_hash_drift(tmp_path: Path) -> None:
    pins = _frozen_inputs(tmp_path)
    (tmp_path / "sector_code_map.json").write_text("{}", encoding="utf-8")
    with pytest.raises(QESectorBlacklistPolicyError, match="qe_sector_blacklist_frozen_mapping_hash_mismatch"):
        materialize_sector_blacklist_universe(
            base_intervals=[("000001.SZ", CALENDAR[0], CALENDAR[-1])],
            calendar=CALENDAR,
            window_start=CALENDAR[0],
            window_end=CALENDAR[-1],
            factor_root=tmp_path,
            pins=pins,
            blacklist_codes=["801010.SI"],
        )


def test_request_is_canonical_and_snapshot_must_match() -> None:
    assert requested_sector_codes({"sector_blacklist": ["801020.si", "801010.SI"]}) == (
        "801010.SI",
        "801020.SI",
    )
    with pytest.raises(QESectorBlacklistPolicyError, match="snapshot differs"):
        requested_sector_codes(
            {
                "sector_blacklist": ["801010.SI"],
                "sector_blacklist_snapshot": {"items": [{"sw2_code": "801020.SI"}]},
            }
        )

    with pytest.raises(QESectorBlacklistPolicyError, match="requires at least one"):
        requested_sector_codes(
            {"sector_blacklist_enabled": True, "sector_blacklist": []}
        )


def test_materialize_sector_blacklist_rejects_noncanonical_membership_order(
    tmp_path: Path,
) -> None:
    pins = _frozen_inputs(tmp_path, reverse_rows=True)
    with pytest.raises(QESectorBlacklistPolicyError, match="canonical instrument/date order"):
        materialize_sector_blacklist_universe(
            base_intervals=[("000001.SZ", CALENDAR[0], CALENDAR[-1])],
            calendar=CALENDAR,
            window_start=CALENDAR[0],
            window_end=CALENDAR[-1],
            factor_root=tmp_path,
            pins=pins,
            blacklist_codes=["801010.SI"],
        )
