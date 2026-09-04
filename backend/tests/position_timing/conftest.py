from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from backend.services.position_timing.artifact_store import PositionTimingArtifactStore
from backend.services.position_timing.contracts import canonical_sha256
from backend.services.position_timing.service import PositionTimingDependencies, PositionTimingService


CHINA_TZ = ZoneInfo("Asia/Shanghai")


class FakeCalendar:
    def __init__(self, *, today: date = date(2026, 9, 3), is_trading_day: bool = True) -> None:
        self.today = today
        self.is_trading_day = is_trading_day

    def status(self, *, as_of_date: date) -> dict[str, Any]:
        assert as_of_date == self.today
        return {
            "is_trading_day": self.is_trading_day,
            "previous_trading_day": "2026-09-02",
            "latest_completed_trading_day": "2026-09-03" if self.is_trading_day else "2026-09-02",
            "source": "fake-calendar",
            "timezone": "Asia/Shanghai",
            "cache": {"checksum": "calendar-fixture"},
        }

    def next_trading_day(self, anchor_date: date) -> date:
        mapping = {
            date(2026, 9, 2): date(2026, 9, 3),
            date(2026, 9, 3): date(2026, 9, 4),
        }
        return mapping[anchor_date]


def daily_loader(symbols: list[str], trade_date: date) -> dict[str, Any]:
    closes = {"000001.SZ": 9.0, "600000.SH": 12.0, "688001.SH": 20.0}
    rows = {
        symbol: {
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "open": closes.get(symbol, 10.0),
            "high": closes.get(symbol, 10.0),
            "low": closes.get(symbol, 10.0),
            "close": closes.get(symbol, 10.0),
            "volume": 100000.0,
            "amount": 1000000.0,
            "price_basis": "raw_cny",
            "feature_available_at": f"{trade_date.isoformat()}T15:00:00+08:00",
        }
        for symbol in symbols
    }
    identity = {"source": "fake-daily", "rows_sha256": canonical_sha256(rows)}
    identity["identity_sha256"] = canonical_sha256(identity)
    return {"rows": rows, "identity": identity}


def supporting_loader(symbols: list[str], trade_date: date) -> dict[str, Any]:
    st = {
        symbol: {
            "is_st": symbol == "600000.SH",
            "source": "fake-st",
            "evidence_hash": canonical_sha256({"symbol": symbol, "st": symbol == "600000.SH"}),
            "feature_available_at": f"{trade_date.isoformat()}T09:15:00+08:00",
        }
        for symbol in symbols
    }
    suspend = {
        symbol: {
            "is_suspended": False,
            "suspend_type": None,
            "suspend_timing": None,
            "feature_available_at": f"{trade_date.isoformat()}T09:15:00+08:00",
        }
        for symbol in symbols
    }
    identity = {
        "source": "fake-supporting",
        "trade_date": trade_date.isoformat(),
        "stock_st_facts_sha256": canonical_sha256(st),
        "suspend_facts_sha256": canonical_sha256(suspend),
    }
    identity["identity_sha256"] = canonical_sha256(identity)
    return {"stock_st_facts": st, "suspend_facts": suspend, "identity": identity}


def delist_loader(symbols: list[str], trade_date: date) -> dict[str, Any]:
    rows = {
        symbol: {
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "delist_flag": False,
            "feature_available_at": f"{trade_date.isoformat()}T15:00:00+08:00",
            "evidence_hash": canonical_sha256({"symbol": symbol, "delist_flag": False}),
        }
        for symbol in symbols
    }
    identity = {"source": "fake-delist", "rows_sha256": canonical_sha256(rows)}
    identity["identity_sha256"] = canonical_sha256(identity)
    return {"rows": rows, "identity": identity}


@pytest.fixture
def holding_rows() -> list[dict[str, Any]]:
    return [
        {
            "id": 1,
            "code": "000001",
            "name": "平安银行",
            "cost_price": 10.0,
            "quantity": 1000,
            "updated_at": "2026-09-03T14:00:00+08:00",
        }
    ]


@pytest.fixture
def watchlist_rows() -> list[dict[str, Any]]:
    return [
        {
            "id": 11,
            "code": "000001.SZ",
            "name": "平安银行",
            "advisory_enabled": True,
            "lifecycle_status": "HOLDING",
        },
        {
            "id": 12,
            "code": "600000.SH",
            "name": "浦发银行",
            "advisory_enabled": True,
            "lifecycle_status": "CANDIDATE",
        },
        {
            "id": 13,
            "code": "600519.SH",
            "name": "已退出",
            "advisory_enabled": True,
            "lifecycle_status": "EXITED",
        },
    ]


@pytest.fixture
def service_factory(tmp_path: Path, holding_rows: list[dict[str, Any]], watchlist_rows: list[dict[str, Any]]):
    def build(
        *,
        now: datetime | None = None,
        holdings: list[dict[str, Any]] | None = None,
        watchlist: list[dict[str, Any]] | None = None,
        calendar: Any | None = None,
        daily: Any = daily_loader,
        supporting: Any = supporting_loader,
        delist: Any = delist_loader,
    ) -> PositionTimingService:
        hold = holding_rows if holdings is None else holdings
        watch = watchlist_rows if watchlist is None else watchlist

        def watchlist_page(page: int, page_size: int) -> dict[str, Any]:
            start = (page - 1) * page_size
            return {"total": len(watch), "items": watch[start : start + page_size]}

        dependencies = PositionTimingDependencies(
            holdings_loader=lambda: hold,
            watchlist_page_loader=watchlist_page,
            calendar_service=calendar or FakeCalendar(),
            daily_snapshot_loader=daily,
            supporting_facts_loader=supporting,
            delist_snapshot_loader=delist,
            now_provider=lambda: now or datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ),
            source_commit_provider=lambda: "a" * 40,
        )
        return PositionTimingService(
            store=PositionTimingArtifactStore(tmp_path / "position-timing"),
            dependencies=dependencies,
        )

    return build
