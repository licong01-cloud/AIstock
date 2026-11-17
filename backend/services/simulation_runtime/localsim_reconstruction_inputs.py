"""Sealed inputs for isolated account correction; no inference or database writes."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from types import MappingProxyType
from zoneinfo import ZoneInfo

from backend.services.simulation_data.contracts import MinuteDataSource, MinuteExecutionMarketInput
from backend.services.simulation_data.daily_context import DailyTradingContextV1, canonical_json_sha256
from backend.services.trading_core.models import MinuteBar
from backend.services.trading_core.errors import DataUnavailableError

TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class HistoricalMarketSnapshot:
    """LocalSIM-owned observation; the public TDX-only snapshot stays strict."""
    trade_date: date
    as_of_time: datetime
    source: MinuteDataSource
    market_inputs: object
    errors: object
    snapshot_id: str
    snapshot_hash: str
    schema_version: str = "localsim_frozen_historical_observation_v1"


def _freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    return value


def require(condition, reason):
    if not condition:
        raise ValueError("LOCALSIM_RECONSTRUCTION_" + reason)


def money(value):
    require(not isinstance(value, bool), "INVALID_MONEY")
    result = Decimal(str(value))
    require(result.is_finite(), "INVALID_MONEY")
    return result


def file_ref(path):
    path = Path(path).resolve()
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(2**20), b""):
            digest.update(block)
    return {"path": str(path), "sha256": digest.hexdigest(), "bytes": path.stat().st_size}


def read(pin):
    require(Path(pin["path"]).is_absolute(), "ABSOLUTE_SOURCE_REQUIRED")
    require(file_ref(pin["path"]) == pin, "SOURCE_CHANGED")
    return json.loads(Path(pin["path"]).read_text(encoding="utf-8"))


def publish(path, body):
    """Publish under the caller's file lock; existing bytes are immutable."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(body, sort_keys=True, ensure_ascii=False, indent=2) + "\n").encode()
    if path.exists():
        require(path.read_bytes() == raw, "ARTIFACT_CONFLICT")
    else:
        # A partial write is evidence of an interrupted publication and fails
        # inspection. It can never be consumed as a completed account day.
        with path.open("xb") as stream:
            stream.write(raw)
    return file_ref(path)


def trusted_opening(facts, trade_date, *, require_close=True):
    """Prove the same-date cash, lots and capital bridge before continuation."""
    snapshots = [s for s in facts["snapshots"] if str(s["trade_date"]) == trade_date]
    require(bool(snapshots), "OPENING_MISSING")
    snapshot = max(snapshots, key=lambda s: str(s["snapshot_time"]))
    stamp = datetime.fromisoformat(str(snapshot["snapshot_time"])).astimezone(TZ)
    require(stamp.date().isoformat() == trade_date
            and (not require_close or stamp.time() >= time(15)), "OPENING_NOT_CLOSE")
    if not require_close:
        require(snapshot.get("metadata", {}).get("terminal") is True, "VALUATION_SOURCE_NOT_TERMINAL")
    rows = [p for p in facts["positions"] if p["run_id"] == snapshot["run_id"]
            and str(p["trade_date"]) == trade_date]
    positions = {p["symbol"]: p for p in rows}
    require(len(positions) == len(rows) == snapshot["position_count"], "OPENING_LOTS_INCOMPLETE")
    require(all(0 <= p["available_quantity"] <= p["quantity"] and money(p["avg_cost"]) >= 0
                for p in rows), "OPENING_LOTS_INVALID")
    value = sum((money(p["market_price"]) * p["quantity"] for p in rows), Decimal(0))
    require(abs(value - money(snapshot["market_value"])) <= Decimal("0.02")
            and abs(money(snapshot["cash"]) + value - money(snapshot["nav"])) <= Decimal("0.02"),
            "OPENING_NAV_BRIDGE")
    initial = money(facts["account"]["initial_capital"])
    flows = [c for c in facts["cash"] if str(c["trade_date"]) <= trade_date]
    require(abs(initial + sum((money(c["cash_delta"]) for c in flows), Decimal(0))
                - money(snapshot["cash"])) <= Decimal("0.02"), "OPENING_CASH_BRIDGE")
    dates = {r["run_id"]: str(r["trade_date"]) for r in facts["runs"]}
    expected = {}
    fills = [f for f in facts["fills"] if dates.get(f["run_id"], "9999") <= trade_date]
    require(all(datetime.fromisoformat(str(f["trade_time"])).astimezone(TZ) <= stamp
                for f in fills if dates[f["run_id"]] == trade_date and "trade_time" in f), "FILLS_AFTER_SNAPSHOT")
    require(len({f["fill_id"] for f in fills}) == len(fills)
            and {c["fill_id"] for c in flows} == {f["fill_id"] for f in fills}, "OPENING_FILL_CASH_SET")
    for fill in fills:
        require(fill["side"] in {"BUY", "SELL"}, "OPENING_FILL_SIDE")
        expected[fill["symbol"]] = expected.get(fill["symbol"], 0) + fill["quantity"] * (
            1 if fill["side"] == "BUY" else -1)
    require(all(expected.get(s, 0) == positions.get(s, {}).get("quantity", 0)
                for s in set(expected) | set(positions)), "OPENING_INVENTORY_BRIDGE")
    return {"schema_version": "localsim_reconstruction_opening_v1", "source_account_id": facts["account"]["account_id"],
            "source_ledger_scope": facts["scope"], "trade_date": trade_date, "source_snapshot": snapshot,
            "origin_initial_cash": str(initial), "cash": str(snapshot["cash"]), "nav": str(snapshot["nav"]),
            "cumulative_pnl": str(money(snapshot["nav"]) - initial), "positions": rows,
            "cash_bridge_residual": "0", "inventory_mismatch_count": 0, "new_capital": "0"}


class FrozenHistoricalMarketProvider:
    """Feed native incremental matching with only completed, sealed observations.

    The physical source stays DB_HISTORICAL. This object has no network, DB or
    selection client. Full-day OHLC/volume are excluded from observable inputs.
    """
    requires_frozen_daily_context = True

    def freeze_snapshot(self, *, market_inputs, errors, as_of_time):
        require(all(m.source == MinuteDataSource.DB_HISTORICAL and m.trade_date == self.context.trade_date
                    for m in market_inputs.values()), "SNAPSHOT_SCOPE")
        digest = canonical_json_sha256({"context_hash": self.context.context_hash,
            "as_of_time": as_of_time.isoformat(), "errors": errors, "source": MinuteDataSource.DB_HISTORICAL.value,
            "markets": {s: {"bars": [b.model_dump(mode="json") for b in m.minute_bars],
                             "context": m.market_context} for s, m in market_inputs.items()}})
        frozen = MappingProxyType({s: replace(m, minute_bars=tuple(m.minute_bars), market_context=_freeze(m.market_context))
                                   for s, m in market_inputs.items()})
        return HistoricalMarketSnapshot(trade_date=self.context.trade_date, as_of_time=as_of_time,
            source=MinuteDataSource.DB_HISTORICAL, market_inputs=frozen, errors=_freeze(errors),
            snapshot_id="lshist_" + digest[:16], snapshot_hash=digest)

    def __init__(self, day_input):
        self.context = DailyTradingContextV1.model_validate(day_input["daily_context"])
        self.markets = deepcopy(day_input["markets"])
        require(set(self.markets) == set(self.context.symbol_set), "MARKET_SYMBOL_SET")
        for symbol, value in self.markets.items():
            value["bars"] = [MinuteBar.model_validate(b) for b in value["bars"]]
            stamps = [b.bar_time for b in value["bars"]]
            require(stamps == sorted(set(stamps)) and all(b.symbol == symbol
                    and b.bar_time.astimezone(TZ).date() == self.context.trade_date for b in value["bars"]),
                    "BAR_ORDER_OR_SCOPE")
            fact = self.context.symbols[symbol]
            require((bool(stamps) and stamps[-1].astimezone(TZ).time() == time(15))
                    or (not stamps and fact.is_suspended), "MISSING_CLOSE_BAR")
            if not fact.is_suspended:
                expected = [datetime.combine(self.context.trade_date, time(9, 30), TZ) + timedelta(minutes=i)
                            for i in [*range(1, 121), *range(211, 331)]]
                require(stamps == expected, "INCOMPLETE_MINUTE_SESSION")
            require(all(b.limit_up == fact.up_limit and b.limit_down == fact.down_limit
                        and b.is_suspended == fact.is_suspended for b in value["bars"]), "MARKET_FACT_CONFLICT")

    def load_observed_intraday(self, *, symbol, trade_date, source, until_time,
                               require_suspend_status=True, frozen_daily_fact=None):
        require(source == MinuteDataSource.DB_HISTORICAL and trade_date == self.context.trade_date
                and until_time.astimezone(TZ).date() == trade_date and require_suspend_status,
                "OBSERVED_SOURCE_SCOPE")
        require(frozen_daily_fact is not None and frozen_daily_fact["context_hash"] == self.context.context_hash
                and frozen_daily_fact["symbol_fact"] == self.context.symbols[symbol].canonical_payload(),
                "OBSERVED_FACT_IDENTITY")
        market = self.markets[symbol]
        bars = [b for b in market["bars"] if b.bar_time <= until_time]
        context = {k: deepcopy(v) for k, v in market["market_context"].items()
                   if not k.startswith("full_day_")}
        context.update(observed_only=True, observed_until=until_time.isoformat(), observed_bar_count=len(bars),
                       generated_at=until_time.isoformat(), daily_trading_context=deepcopy(frozen_daily_fact))
        return MinuteExecutionMarketInput(symbol=symbol, trade_date=trade_date, source=source,
                                           minute_bars=bars, market_context=context)


def freeze_day_input(*, selected, opening_symbols, all_selected_symbols, daily_context_provider,
                     minute_provider, calendar_snapshot, binding_identity, package_identity,
                     release_identity):
    """Caller supplies read-only providers; finish the whole preflight before run."""
    day = date.fromisoformat(selected["target_trade_date"])
    require(day < datetime.now(TZ).date(), "CURRENT_DAY_FORBIDDEN")
    symbols = sorted(set(opening_symbols) | set(all_selected_symbols))
    context = daily_context_provider.load(symbols=symbols, trade_date=day,
        as_of_time=datetime.combine(day, time(9, 30), TZ), calendar_service_snapshot=calendar_snapshot,
        binding_identity=binding_identity, package_identity=package_identity, release_identity=release_identity)
    markets = {}
    for symbol in symbols:
        try:
            market = minute_provider.load_symbol_input(symbol=symbol, trade_date=day,
                source=MinuteDataSource.DB_HISTORICAL, require_suspend_status=True)
            markets[symbol] = {"bars": [b.model_dump(mode="json") for b in market.minute_bars],
                               "market_context": market.market_context}
        except DataUnavailableError as exc:
            if not (context.symbols[symbol].is_suspended and exc.message == "historical DB returned no minute bars"):
                raise
            fact = context.symbols[symbol]
            markets[symbol] = {"bars": [], "market_context": {
                "data_source": MinuteDataSource.DB_HISTORICAL.value, "prev_close": fact.pre_close,
                "limit_up": fact.up_limit, "limit_down": fact.down_limit,
                "suspend_status": {"is_suspended": True, "source": fact.suspend_source}}}
    value = {"schema_version": "localsim_reconstruction_day_input_v1", "trade_date": day.isoformat(),
             "selection": selected, "daily_context": context.model_dump(mode="json"), "markets": markets}
    FrozenHistoricalMarketProvider(value)
    value["input_hash"] = canonical_json_sha256(value)
    return value
