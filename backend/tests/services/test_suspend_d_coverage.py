from __future__ import annotations

import datetime as dt

import pytest

from backend.services import suspend_d_coverage as coverage


@pytest.mark.parametrize("symbol,day,raw,effective", [
    ("600955.SH", "2021-09-15", "09:00-10:00", "09:30-10:00"),
    ("605567.SH", "2021-10-13", "09:00-10:00", "09:30-10:00"),
    ("688607.SH", "2021-02-01", "09:33-09:43,11:10-11:10", "09:33-09:43,11:00-11:10"),
])
def test_exact_notice_retains_raw_clock_and_proves_effective_session(symbol, day, raw, effective):
    from backend.data_service.suspension_day_authority import project_suspension_days

    rows = [dict(ts_code=symbol, trade_date=day, suspend_type="S", suspend_timing=raw)]
    resolution = project_suspension_days(rows)[0]
    assert resolution["state"] == "INTRADAY_SUSPENDED"
    assert resolution["effective_suspend_timing"] == effective
    assert rows[0]["suspend_timing"] == raw
    assert resolution["historical_publication_time_proven"] is False
    rows[0]["ts_code"] = "000001.SZ"
    assert project_suspension_days(rows)[0]["state"] == "UNRESOLVED"


def test_new_authority_does_not_rewrite_previously_approved_entries():
    import json
    from backend.data_service import suspension_day_authority as public

    old = json.loads(public.DEFAULT_PATH.with_name("suspension_day_authority_v2.json").read_bytes())
    current = public.load_authority()
    assert all(entry in current["entries"] for entry in old["entries"])
    assert len(current["entries"]) == len(old["entries"]) + 3


def _afternoon_authority():
    from backend.data_service.suspension_day_authority import POLICY, SCHEMA

    return {"schema_version": SCHEMA, "policy": POLICY, "entries": [{
        "ts_code": "603056.SH", "trade_date": "2026-01-09", "state": "INTRADAY_SUSPENDED",
        "announcement_id": "2026-003", "disclosure_date": "2026-01-09",
        "source_url": "https://static.cninfo.com.cn/finalpage/2026-01-09/1224926625.PDF",
        "source_content_sha256": "5841c3a2fa73d5bc29cc8d49eb8ae86c89e5248d8f1b8c1895883a0966a8dcb3",
        "source_file_size": 81790, "historical_publication_time_proven": False,
        "provider_suspend_timing": "13:00-9:30", "effective_suspend_timing": "13:00-15:00",
    }]}


def test_new_authority_does_not_replace_old_process_payload():
    import json
    from pathlib import Path
    from backend.data_service import suspension_day_authority as public

    legacy_path = Path(public.__file__).with_name("suspension_day_authority_v1.json")
    assert public.DEFAULT_PATH != legacy_path
    legacy = json.loads(legacy_path.read_bytes())["entries"]
    current = public.load_authority()["entries"]
    assert all(entry["state"] == "FULL_DAY_SUSPENDED" for entry in legacy)
    assert all(entry in current for entry in legacy)
    assert any(entry["state"] == "INTRADAY_SUSPENDED" for entry in current)


@pytest.mark.parametrize("timing", ["13:00-9:30", "13:00-15:00"])
def test_official_afternoon_resolution_preserves_raw_cross_session_marker(timing):
    from backend.data_service.suspension_day_authority import full_day_keys, project_suspension_days

    row = dict(ts_code="603056.SH", trade_date="2026-01-09", suspend_type="S", suspend_timing=timing)
    result = project_suspension_days([row], authority=_afternoon_authority())[0]
    assert result["state"] == "INTRADAY_SUSPENDED"
    assert result["basis"] == "OFFICIAL_DISCLOSURE"
    assert result["effective_suspend_timing"] == "13:00-15:00"
    assert result["announcement_id"] == "2026-003"
    assert not full_day_keys([row], authority=_afternoon_authority())
    assert row["suspend_timing"] == timing


@pytest.mark.parametrize("events", [
    [("S", "09:30-09:30")], [("S", "14:00-9:30")], [("R", "13:00-9:30")],
    [("S", "13:00-9:30"), ("S", "13:00-9:30")],
    [("S", "13:00-9:30"), ("R", None)], [("S", None)],
])
def test_afternoon_resolution_does_not_hide_changed_or_ambiguous_events(events):
    from backend.data_service.suspension_day_authority import project_suspension_days

    rows = [dict(ts_code="603056.SH", trade_date="2026-01-09", suspend_type=kind, suspend_timing=timing)
            for kind, timing in events]
    assert project_suspension_days(rows, authority=_afternoon_authority())[0]["state"] == "UNRESOLVED"


def test_cross_session_timing_is_not_generally_inferred_or_borrowed_from_new_authority():
    from backend.data_service.suspension_day_authority import POLICY, SCHEMA, project_suspension_days

    row = dict(ts_code="603056.SH", trade_date="2026-01-09", suspend_type="S", suspend_timing="13:00-9:30")
    empty = {"schema_version": SCHEMA, "policy": POLICY, "entries": []}
    assert project_suspension_days([row], authority=empty)[0]["state"] == "UNRESOLVED"
    for change in ({"ts_code": "000001.SZ"}, {"trade_date": "2026-01-08"}):
        assert project_suspension_days([{**row, **change}], authority=_afternoon_authority())[0]["state"] == "UNRESOLVED"


@pytest.mark.parametrize("timing", ["13:00-13:00", "15:00-13:00", "08:00-15:00"])
def test_official_intraday_authority_rejects_invalid_effective_span(timing):
    from backend.data_service.suspension_day_authority import load_authority

    authority = _afternoon_authority()
    authority["entries"][0]["effective_suspend_timing"] = timing
    with pytest.raises(ValueError, match="intraday"):
        load_authority(authority)


def test_monthly_frozen_afternoon_export_and_reader_share_the_same_authority(tmp_path):
    import json
    from types import SimpleNamespace
    import pandas as pd
    from backend.data_service.suspension_day_authority import project_suspension_days
    from backend.services.dataset_release.monthly_shared_components import _build_suspend

    day = dt.date(2026, 1, 9)
    raw = dict(ts_code="603056.SH", trade_date=day, suspend_type="S", suspend_timing="13:00-9:30")
    parquet, meta, keys, count = _build_suspend(root=tmp_path, rows=[raw], pit_rows=[], calendar=(day,),
        profile=SimpleNamespace(start_date=day, universe_key="pit"), cutoff=day,
        suspension_authority=_afternoon_authority())
    frame, receipt = pd.read_parquet(parquet), json.loads(meta.read_bytes())
    assert count == 1 and keys == {("603056.SH", day)}
    assert frame.iloc[0].suspend_timing == "13:00-9:30"
    assert receipt["raw_provider_events"][0]["suspend_timing"] == "13:00-9:30"
    result = project_suspension_days(frame.to_dict("records"), authority=receipt["suspension_day_authority"])[0]
    assert result["state"] == "INTRADAY_SUSPENDED"
    assert result["effective_suspend_timing"] == "13:00-15:00"


def test_disclosure_resolves_mixed_events_without_deleting_raw_records():
    from backend.data_service.suspension_day_authority import project_suspension_days

    rows = [dict(ts_code="603616.SH", trade_date="20260122", suspend_type=kind,
                 suspend_timing=None) for kind in ("R", "S")]
    result = project_suspension_days(rows)
    assert rows[0]["suspend_type"] == "R" and len(rows) == 2
    assert result[0]["state"] == "FULL_DAY_SUSPENDED"
    assert result[0]["basis"] == "OFFICIAL_DISCLOSURE"
    assert result[0]["historical_publication_time_proven"] is False
    assert len(result[0]["source_content_sha256"]) == 64


@pytest.mark.parametrize("rows, expected", [
    ([('S', None)], "FULL_DAY_SUSPENDED"),
    ([('R', None)], "RESUMED"),
    ([('S', '09:30-10:00')], "INTRADAY_SUSPENDED"),
    ([('S', None), ('R', None)], "UNRESOLVED"),
    ([('S', '09:30-09:30')], "UNRESOLVED"),
    ([('S', None), ('S', None)], "UNRESOLVED"),
    ([('S', '00:00-23:00')], "UNRESOLVED"),
    ([('S', '9:34-9:44')], "INTRADAY_SUSPENDED"),
    ([('S', '13:13-13:23,13:23-13:33')], "INTRADAY_SUSPENDED"),
    ([('S', '9:30-9:40,9:41-9:51')], "INTRADAY_SUSPENDED"),
    ([('S', '11:13-11:23'), ('R', None)], "INTRADAY_SUSPENDED"),
    ([('S', '13:20-13:40,13:30-13:50')], "UNRESOLVED"),
    ([('S', '13:20-13:30,10:30-10:40')], "UNRESOLVED"),
])
def test_shared_day_state_does_not_infer_event_order(rows, expected):
    from backend.data_service.suspension_day_authority import project_suspension_days

    raw = [dict(ts_code="000001.SZ", trade_date="20260901", suspend_type=kind,
                suspend_timing=timing) for kind, timing in rows]
    assert project_suspension_days(raw)[0]["state"] == expected


def test_frozen_legacy_source_cannot_borrow_new_disclosure_rules():
    from types import SimpleNamespace
    from backend.data_service.suspension_day_authority import frozen_authority, project_suspension_days
    rows = [dict(ts_code="603616.SH", trade_date="20260122", suspend_type=kind,
                 suspend_timing=None) for kind in ("S", "R")]
    cas = SimpleNamespace(get_json_bounded=lambda *_args, **_kwargs: {})
    frozen = frozen_authority(cas, SimpleNamespace(source_manifest_ref="frozen"))
    assert frozen["entries"] == []
    assert project_suspension_days(rows, authority=frozen)[0]["state"] == "UNRESOLVED"


def test_official_resolution_does_not_hide_duplicate_or_intraday_conflicts():
    from backend.data_service.suspension_day_authority import project_suspension_days
    base = dict(ts_code="603616.SH", trade_date="20260122", suspend_type="S", suspend_timing=None)
    for rows in ([base, base], [{**base, "suspend_timing": "09:30-10:00"}]):
        assert project_suspension_days(rows)[0]["state"] == "UNRESOLVED"


@pytest.mark.parametrize("symbol, day, events", [
    ("002731.SZ", "2026-07-06", [("S", None), ("R", None)]),
    ("603056.SH", "2026-01-12", [("S", None), ("R", None)]),
    ("603897.SH", "2025-11-04", [("S", None), ("R", None)]),
    ("688005.SH", "2026-01-16", [("S", "09:30-09:30")]),
])
def test_verified_full_day_facts_resolve_exact_keys_only(symbol, day, events):
    from backend.data_service.suspension_day_authority import load_authority, project_suspension_days

    rows = [dict(ts_code=symbol, trade_date=day, suspend_type=kind,
                 suspend_timing=timing) for kind, timing in events]
    original = [dict(row) for row in rows]
    resolved = project_suspension_days(rows)[0]
    assert resolved["state"] == "FULL_DAY_SUSPENDED"
    assert resolved["basis"] == "OFFICIAL_DISCLOSURE"
    assert resolved["raw_event_count"] == len(rows)
    assert resolved["historical_publication_time_proven"] is False
    assert rows == original

    # A later checkout cannot lend an exception to an older frozen SOURCE.
    empty = load_authority()
    empty["entries"] = []
    assert project_suspension_days(rows, authority=empty)[0]["state"] == "UNRESOLVED"


@pytest.mark.parametrize("events", [
    [("S", "9:30-9:30")],
    [("S", "09:30-10:00")],
    [("S", "09:31-09:31")],
    [("S", [])],
    [("R", "09:30-09:30")],
    [("S", "09:30-09:30"), ("S", "09:30-09:30")],
    [("S", "09:30-09:30"), ("R", "10:00-11:00")],
])
def test_official_opening_sentinel_is_narrow_and_never_hides_conflicts(events):
    from backend.data_service.suspension_day_authority import project_suspension_days

    rows = [dict(ts_code="688005.SH", trade_date="2026-01-16", suspend_type=kind,
                 suspend_timing=timing) for kind, timing in events]
    expected = "FULL_DAY_SUSPENDED" if events == [("S", "9:30-9:30")] else "UNRESOLVED"
    assert project_suspension_days(rows)[0]["state"] == expected


def test_verifier_rejects_changed_disclosure_bytes_before_writing(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import verify_suspension_day_authority as verifier
    monkeypatch.setattr(verifier.requests, "get", lambda *_args, **_kwargs:
                        SimpleNamespace(content=b"changed", raise_for_status=lambda: None))
    entry = dict(ts_code="603616.SH", trade_date="2026-01-22", source_url="https://example.invalid/original",
                 source_content_sha256="0" * 64, source_file_size=7)
    with pytest.raises(ValueError, match="bytes drifted"):
        verifier._download(entry, tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (
            dict(
                missing_days=10,
                full_day_suspend_days=10,
                minute_positive_days=0,
                intraday_suspend_days=0,
            ),
            coverage.FULLY_COVERED,
        ),
        (
            dict(
                missing_days=10,
                full_day_suspend_days=3,
                minute_positive_days=0,
                intraday_suspend_days=0,
            ),
            coverage.PARTIAL_AUTHORITY_GAP,
        ),
        (
            dict(
                missing_days=3,
                full_day_suspend_days=3,
                minute_positive_days=1,
                intraday_suspend_days=0,
            ),
            coverage.MINUTE_WITHOUT_DAILY,
        ),
        (
            dict(
                missing_days=1,
                full_day_suspend_days=0,
                minute_positive_days=0,
                intraday_suspend_days=1,
            ),
            coverage.INTRADAY_WITHOUT_DAILY,
        ),
        (
            dict(
                missing_days=2,
                full_day_suspend_days=0,
                minute_positive_days=0,
                intraday_suspend_days=0,
            ),
            coverage.UNEXPLAINED_NO_TRADE,
        ),
    ],
)
def test_classify_missing_run_is_fail_closed(evidence, expected) -> None:
    assert coverage.classify_missing_run(**evidence) == expected


def test_classify_missing_run_rejects_impossible_counts() -> None:
    with pytest.raises(coverage.SuspendCoverageError, match="within missing_days"):
        coverage.classify_missing_run(
            missing_days=2,
            full_day_suspend_days=3,
            minute_positive_days=0,
            intraday_suspend_days=0,
        )


def _run(
    *,
    symbol: str,
    start: dt.date,
    end: dt.date,
    missing: int,
    full: int,
    minute: int = 0,
    intraday: int = 0,
) -> dict:
    return {
        "ts_code": symbol,
        "start_date": start,
        "end_date": end,
        "missing_days": missing,
        "full_day_suspend_days": full,
        "minute_positive_days": minute,
        "intraday_suspend_days": intraday,
        "list_date": dt.date(2020, 1, 1),
        "delist_date": None,
        "list_status": "L",
    }


def test_receipt_keeps_sparse_and_unexplained_runs_unresolved() -> None:
    window = coverage.CoverageWindow(
        start_date=dt.date(2025, 11, 1),
        end_date=dt.date(2025, 12, 31),
        trading_day_count=43,
    )
    receipt = coverage.build_coverage_receipt(
        window=window,
        raw_runs=[
            _run(
                symbol="688766.SH",
                start=dt.date(2025, 11, 25),
                end=dt.date(2025, 12, 8),
                missing=10,
                full=3,
            ),
            _run(
                symbol="600984.SH",
                start=dt.date(2025, 12, 1),
                end=dt.date(2025, 12, 5),
                missing=5,
                full=5,
            ),
            _run(
                symbol="000001.SZ",
                start=dt.date(2025, 12, 9),
                end=dt.date(2025, 12, 9),
                missing=1,
                full=0,
            ),
        ],
        raw_conflicts=[
            {"ts_code": "000002.SZ", "trade_date": dt.date(2025, 12, 10)}
        ],
        max_findings=10,
    )

    assert receipt["summary"]["coverage_complete"] is False
    assert receipt["summary"]["covered_run_count"] == 1
    assert receipt["summary"]["unresolved_run_count"] == 2
    assert receipt["summary"]["unresolved_symbol_count"] == 2
    assert receipt["summary"]["uncovered_day_count"] == 8
    assert receipt["summary"]["full_day_suspend_price_conflict_count"] == 1
    assert receipt["semantics"]["missing_market_data_implies_suspension"] is False
    assert receipt["database_write_performed"] is False


def test_receipt_is_complete_only_when_all_runs_have_exact_full_day_authority() -> None:
    receipt = coverage.build_coverage_receipt(
        window=coverage.CoverageWindow(
            start_date=dt.date(2026, 8, 1),
            end_date=dt.date(2026, 8, 31),
            trading_day_count=21,
        ),
        raw_runs=[
            _run(
                symbol="002155.SZ",
                start=dt.date(2026, 8, 20),
                end=dt.date(2026, 8, 26),
                missing=5,
                full=5,
            )
        ],
        raw_conflicts=[],
        max_findings=10,
    )

    assert receipt["summary"]["coverage_complete"] is True
    assert receipt["summary"]["unresolved_run_count"] == 0
    assert receipt["summary"]["uncovered_day_count"] == 0


class _Cursor:
    def __init__(self, rows):
        self._rows = iter(rows)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return next(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Conn:
    def __init__(self, cursors):
        self._cursors = iter(cursors)

    def cursor(self, *args, **kwargs):
        return next(self._cursors)


def test_resolve_rolling_window_uses_exact_trading_day_count() -> None:
    cursor = _Cursor(
        [
            (dt.date(2026, 6, 8),),
            (dt.date(2026, 6, 8), dt.date(2026, 8, 31), 60),
        ]
    )
    window = coverage.resolve_coverage_window(
        _Conn([cursor]),
        end_date=dt.date(2026, 8, 31),
        lookback_trading_days=60,
    )

    assert window == coverage.CoverageWindow(
        start_date=dt.date(2026, 6, 8),
        end_date=dt.date(2026, 8, 31),
        trading_day_count=60,
    )
    assert cursor.executed[0][1] == (dt.date(2026, 8, 31), 59)


def test_audit_sets_read_only_before_querying(monkeypatch) -> None:
    setup = _Cursor([])
    conn = _Conn([setup])
    window = coverage.CoverageWindow(
        start_date=dt.date(2026, 8, 1),
        end_date=dt.date(2026, 8, 31),
        trading_day_count=21,
    )
    monkeypatch.setattr(coverage, "resolve_coverage_window", lambda *_args, **_kwargs: window)
    monkeypatch.setattr(coverage, "_fetch_missing_runs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        coverage, "_fetch_full_day_suspend_price_conflicts", lambda *_args, **_kwargs: []
    )

    receipt = coverage.audit_suspend_d_coverage(
        conn,
        start_date=window.start_date,
        end_date=window.end_date,
    )

    assert setup.executed[0][0] == "SET TRANSACTION READ ONLY"
    assert setup.executed[1] == ("SET LOCAL statement_timeout = %s", (300_000,))
    assert receipt["summary"]["coverage_complete"] is True
