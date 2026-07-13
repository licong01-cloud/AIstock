from __future__ import annotations

from datetime import UTC, date, datetime

from backend.services.paper_trading_v2.repository import PaperTradingV2Repository
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


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


class _PortfolioPageCursor:
    def __init__(self) -> None:
        self.sql: list[str] = []
        self.params: list[tuple] = []
        self._last = ""

    def __enter__(self) -> "_PortfolioPageCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self._last = sql.lower()
        self.sql.append(sql)
        self.params.append(params or ())

    def fetchone(self) -> dict:
        assert "count(*)" in self._last
        return {"total": 2}

    def fetchall(self) -> list[dict]:
        assert "select *" in self._last
        manifest = freeze_manifest(
            make_manifest().model_copy(
                update={
                    "package_id": "pkg_a",
                    "package_name": "Package A",
                    "package_status": PackageStatus.PAPER_ENABLED,
                }
            )
        )
        return [
            {
                "portfolio_id": "pf_minqmt",
                "portfolio_name": "MiniQMT",
                "package_id": manifest.package_id,
                "manifest_sha256": manifest.manifest_sha256,
                "frozen_manifest_json": manifest.model_dump(mode="json"),
                "initial_cash": 100000,
                "start_date": date(2024, 1, 2),
                "data_source": "MINIQMT_REALTIME",
                "broker_backend": "minqmt_sim",
                "fee_policy": {},
                "risk_policy": {},
                "execution_policy": {},
                "status": "RUNNING",
                "auto_run_enabled": True,
                "auto_run_config": {},
                "auto_run_config_sha256": None,
                "auto_run_updated_at": None,
                "auto_run_updated_by": None,
                "created_at": datetime(2024, 1, 2, tzinfo=UTC),
                "updated_at": datetime(2024, 1, 2, tzinfo=UTC),
            }
        ]


def test_portfolio_page_uses_database_pagination_and_broker_filter() -> None:
    cursor = _PortfolioPageCursor()
    repo = PaperTradingV2Repository(conn_factory=lambda: _Connection(cursor))

    page = repo.list_portfolios_page(page=2, page_size=25, statuses=["RUNNING"], broker_backend="minqmt_sim")

    assert page["pagination"]["total"] == 2
    assert page["pagination"]["broker_backend"] == "minqmt_sim"
    assert page["portfolios"][0]["broker_backend"] == "minqmt_sim"
    joined_sql = "\n".join(cursor.sql).lower()
    assert "from paper_v2.portfolio" in joined_sql
    assert "broker_backend = %s" in joined_sql
    assert "limit %s offset %s" in joined_sql
    assert "select portfolio_id" not in joined_sql
