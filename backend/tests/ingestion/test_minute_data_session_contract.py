import datetime as dt

import pytest

from backend.services.minute_data_session_contract import CHINA_TZ, guard_minute_values


def row(time, volume=0, amount=0):
    return (dt.datetime(2026, 9, 29, *time, tzinfo=CHINA_TZ), "000016.SZ", "1m",
            10, 10, 10, 10, volume, amount, "none", "tdx_api")


class Conn:
    def __init__(self, evidence):
        self.evidence = list(evidence)
    def cursor(self):
        return self
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def execute(self, sql, params):
        assert sql.startswith("\nSELECT")
        self.current = self.evidence.pop(0)
    def fetchone(self):
        return self.current


def test_real_regular_and_auction_bars_unchanged_without_db_probe():
    values = [row((9, 0), 1, 2), row((11, 30), 1, 2), row((13, 1), 1, 2)]
    assert guard_minute_values(None, "000016.SZ", dt.date(2026, 9, 29), values) is values


def test_independently_confirmed_suspension_is_suppressed_without_timestamp_rewrite():
    values = [row((13, 0)), row((15, 0))]
    assert guard_minute_values(Conn([(True, True, False), (False,)]), "000016.SZ",
                               dt.date(2026, 9, 29), values) == []
    assert values[0][0].hour == 13


@pytest.mark.parametrize("evidence", [[(False, True, False)],
                                    [(True, True, True)], [(True, True, False), (True,)]])
def test_intraday_unproven_or_traded_data_fails_closed(evidence):
    with pytest.raises(ValueError, match="suspension_unproven"):
        guard_minute_values(Conn(evidence), "000016.SZ", dt.date(2026, 9, 29), [row((13, 0))])


def test_full_day_suspension_without_daily_row_needs_no_fabricated_daily_placeholder():
    assert guard_minute_values(Conn([(True, False, False), (False,)]), "000016.SZ",
                               dt.date(2026, 9, 29), [row((13, 0))]) == []


@pytest.mark.parametrize("volume,amount", [(1, 0), (0, 1), (None, 0), (0, None), (float('nan'), 0)])
def test_invalid_1300_turnover_never_silently_discarded(volume, amount):
    with pytest.raises(ValueError, match="with_turnover"):
        guard_minute_values(None, "000016.SZ", dt.date(2026, 9, 29), [row((13, 0), volume, amount)])


@pytest.mark.parametrize("producer", ["incremental", "full_upsert", "full_init"])
def test_all_ingestion_entrypoints_suppress_confirmed_placeholders(producer):
    from scripts import ingest_full_minute, ingest_incremental
    method = {"incremental": ingest_incremental.upsert_minute,
              "full_upsert": ingest_full_minute.upsert_minute,
              "full_init": ingest_full_minute.insert_minute_init}[producer]
    payload = [{"Time": "2026-09-29T13:00:00+08:00", "Open": 10, "High": 10,
                "Low": 10, "Close": 10, "Volume": 0, "Amount": 0}]
    assert method(Conn([(True, True, False), (False,)]), "000016.SZ", dt.date(2026, 9, 29), payload) == (0, None)


def test_repair_requires_exact_plan_and_refuses_production_before_lock():
    from scripts.repair_suspended_minute_placeholders import apply_dev
    class Production(Conn):
        def execute(self, sql, params=()):
            assert sql == "SELECT current_database()"
        def fetchone(self):
            return ("aistock",)
    with pytest.raises(ValueError, match="DEV-only"):
        apply_dev(Production([]), {})


def test_repair_plan_drift_never_deletes(monkeypatch):
    from scripts import repair_suspended_minute_placeholders as repair
    class Dev(Conn):
        def execute(self, sql, params=()):
            assert sql.startswith(("SELECT", "LOCK"))
        def fetchone(self):
            return ("aistock_dev",)
    plan = {"start": "2026-09-29", "end": "2026-09-29", "targets": []}
    monkeypatch.setattr(repair, "build_plan", lambda *args: {**plan, "unresolved": []})
    with pytest.raises(ValueError, match="plan drift"):
        repair.apply_dev(Dev([]), plan)
