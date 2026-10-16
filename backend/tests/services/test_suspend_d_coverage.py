from __future__ import annotations

import datetime as dt

import pytest

from backend.services import suspend_d_coverage as coverage


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
