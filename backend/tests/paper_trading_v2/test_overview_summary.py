from __future__ import annotations

from backend.services.paper_trading_v2.repository import PaperTradingV2Repository


class _Cursor:
    def __init__(self) -> None:
        self.sql: list[str] = []
        self.params: list[tuple] = []
        self.rows = [
            {
                "total_packages": 5,
                "active_packages": 4,
                "retired_packages": 1,
                "portfolio_count": 2,
                "active_portfolio_count": 2,
                "package_count": 3,
                "latest_selection_run_count": 3,
            },
        ]

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self.sql.append(sql)
        self.params.append(params or ())

    def fetchone(self) -> dict:
        return self.rows.pop(0)


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def cursor(self, **_kwargs: object) -> _Cursor:
        return self._cursor


def test_overview_summary_uses_lightweight_database_aggregates_only() -> None:
    cursor = _Cursor()
    repo = PaperTradingV2Repository(conn_factory=lambda: _Connection(cursor))

    summary = repo.overview_summary()

    assert summary == {
        "package_counts": {"total": 5, "active": 4, "retired": 1},
        "selection_counts": {"packages_with_latest_run": 3, "latest_run_count": 3},
        "portfolio_counts": {"total": 2, "active": 2},
    }
    assert len(cursor.sql) == 1
    joined_sql = "\n".join(cursor.sql).lower()
    assert "manifest_json" not in joined_sql
    assert "model_state" not in joined_sql
    assert "selectable" not in joined_sql
    assert "strategy_pkg.package" in joined_sql
    assert "paper_v2.portfolio" in joined_sql
    assert "selection.run" in joined_sql
