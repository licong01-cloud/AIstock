"""CLI entry point: drive a single sim run end-to-end and print summary.

Usage::

    python -m backend.services.paper_trading_v2.daemon.demo_run

The demo uses the same ``make_paper_enabled_manifest`` fixture that the
integration tests rely on, plus a deterministic in-memory market-data
provider, so the run is reproducible without DB access.

The event log lands in ``var/paper_v2_sim/daemon_events.db`` (worktree-local
SQLite, gitignored). The script prints the run_id + every event_seq /
event_type pair in chronological order so the operator can manually verify
the lifecycle without opening the DB.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from backend.services.paper_trading_v2.broker import LocalSimBackend
from backend.services.paper_trading_v2.daemon import (
    DaemonEventLog,
    PaperV2SimRunner,
)
from backend.services.paper_trading_v2.market_data import (
    MinuteDataSource,
    MinuteExecutionMarketInput,
    PaperV2MinuteMarketDataProvider,
)
from backend.services.trading_core.models import (
    MinuteBar,
    OrderIntent,
    OrderSide,
    OrderType,
)
from backend.services.trading_core.sim_gateway import SimGateway

from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest


_TRADE_DATE = date(2024, 1, 2)
_SYMBOL = "600519.SH"
_OPEN = 1700.0


class _FakeMarketDataProvider(PaperV2MinuteMarketDataProvider):
    """Hard-coded provider so the demo runs without TDX / DB."""

    def __init__(self) -> None:
        super().__init__()

    def load_symbol_input(  # type: ignore[override]
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        min_bars: int = 1,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        start = datetime.combine(trade_date, datetime.min.time()).replace(hour=9, minute=31)
        bars = [
            MinuteBar(
                symbol=symbol,
                bar_time=start + timedelta(minutes=i),
                open=_OPEN + i * 0.5,
                high=_OPEN + 1.0 + i * 0.5,
                low=_OPEN - 0.5 + i * 0.5,
                close=_OPEN + 0.3 + i * 0.5,
                volume=200_000,
                amount=400_000_000.0,
                limit_up=_OPEN * 1.1,
                limit_down=_OPEN * 0.9,
            )
            for i in range(8)
        ]
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=bars,
            market_context={
                "stock_id": symbol,
                "trade_date": trade_date.isoformat(),
                "data_source": source.value,
                "prev_close": _OPEN,
                "limit_up": _OPEN * 1.1,
                "limit_down": _OPEN * 0.9,
                "suspend_status": {"is_suspended": False},
            },
        )


def main() -> int:
    manifest = make_paper_enabled_manifest()
    portfolio_id = f"paper_demo_{uuid4().hex[:8]}"

    # Determine where to put the event-log DB. Default: worktree-local var/.
    repo_root = Path(__file__).resolve().parents[4]
    db_path = repo_root / "var" / "paper_v2_sim" / "daemon_events.db"

    event_log = DaemonEventLog(
        db_path=db_path,
        portfolio_id=portfolio_id,
        package_id=manifest.package_id,
    )
    backend = LocalSimBackend(
        portfolio_id=portfolio_id,
        initial_cash=10_000_000.0,
        data_source=MinuteDataSource.DB_HISTORICAL,
        manifest=manifest,
        market_data_provider=_FakeMarketDataProvider(),
    )
    gateway = SimGateway.from_local_sim(backend)
    gateway.connect()

    intents = [
        OrderIntent(
            package_id=manifest.package_id,
            portfolio_id=portfolio_id,
            symbol=_SYMBOL,
            side=OrderSide.BUY,
            quantity=1000,
            order_type=OrderType.MARKET,
            target_trade_date=_TRADE_DATE,
        ),
    ]

    runner = PaperV2SimRunner(
        gateway=gateway,
        event_log=event_log,
        manifest=manifest,
    )
    try:
        result = runner.run_intents(intents)
    finally:
        gateway.close()

    print("=== Demo run summary ===")
    print(f"run_id:        {result.run_id}")
    print(f"portfolio_id:  {result.portfolio_id}")
    print(f"package_id:    {result.package_id}")
    print(f"submitted:     {len(result.handles)}")
    print(f"rejected:      {len(result.rejected_intents)}")
    print(f"fills:         {result.fills_received}")
    print(f"db_path:       {event_log.db_path}")
    print()
    print("=== Event log (chronological) ===")
    for record in event_log.read_all():
        print(
            f"  seq={record.event_seq:02d}  type={record.event_type.value:<18}"
            f"  intent={record.intent_id or '-'}  symbol={record.symbol or '-'}"
        )
    print()
    last = event_log.read_all()[-1]
    print(f"final event payload:\n{json.dumps(last.payload, indent=2, ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
