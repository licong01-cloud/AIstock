import datetime as dt

import scripts.validate_etf_share_size_source as subject


class _Cursor:
    def __init__(self) -> None:
        self.description = []
        self.row = None
        self.executed: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        if sql == subject.SUMMARY_SQL:
            names = [
                "row_count", "symbol_count", "trade_date_count", "min_date", "max_date",
                "total_share_nulls", "total_size_nulls", "nav_nulls", "close_nulls",
                "non_finite_rows", "negative_rows",
            ]
            self.description = [(name,) for name in names]
            self.row = (100, 10, 10, dt.date(2026, 9, 1), dt.date(2026, 9, 14), 1, 2, 3, 4, 0, 0)
        elif sql == subject.ANOMALY_SQL:
            self.description = [(name,) for name in ("missing_trade_dates", "non_trading_dates", "duplicate_keys")]
            self.row = (0, 0, 0)
        elif sql == subject.DAILY_SQL:
            self.description = [(name,) for name in ("min_rows_per_day", "max_rows_per_day", "avg_rows_per_day")]
            self.row = (8, 12, 10)

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(self) -> None:
        self.cur = _Cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cur


def test_full_validator_is_read_only_and_does_not_fail_on_source_nulls(monkeypatch) -> None:
    conn = _Connection()
    monkeypatch.setattr(subject, "get_conn", lambda: conn)

    result = subject.validate()

    assert result["ok"] is True
    assert result["read_only"] is True
    assert result["summary"]["nav_nulls"] == 3
    assert result["blocking_counts"] == {
        "missing_trade_dates": 0,
        "non_trading_dates": 0,
        "duplicate_keys": 0,
        "non_finite_rows": 0,
        "negative_rows": 0,
    }
    assert conn.cur.executed[0] == "SET TRANSACTION READ ONLY"
    assert all(not sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for sql in conn.cur.executed)
