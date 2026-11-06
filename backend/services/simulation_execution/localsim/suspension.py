"""LocalSIM historical halt eligibility and separately identified valuation references."""

from __future__ import annotations

from copy import deepcopy
from datetime import date
import hashlib
from math import isfinite
from pathlib import Path
import re
from typing import Any, Mapping

from backend.data_service import suspension_day_authority as public_suspension
from backend.db.pg_pool import get_conn
from backend.services.simulation_data.daily_context import canonical_json_sha256
from backend.services.simulation_data.trading_calendar import TradeCalendarProvider
from backend.services.strategy_package.reference_prices import REFERENCE_PRICE_CONTRACT, load_reference_prices
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.limit_price_provider import StkLimitPriceProvider


REFERENCE_KEY = "localsim_suspension_reference_v1"
SUSPENSION_KEY = "localsim_historical_suspension_v1"
NATIVE_PRE_CLOSE_KEY = "localsim_suspended_native_pre_close_v1"
DAY_AUTHORITY_KEY = "localsim_suspension_day_authority_v1"
# A running consumer keeps the rules it actually loaded; a main checkout update
# must not silently replace its authority between request identity and execution.
_LOADED_AUTHORITY = public_suspension.load_authority()
_PUBLIC_IMPLEMENTATION_SHA256 = hashlib.sha256(Path(public_suspension.__file__).read_bytes()).hexdigest()


def localsim_suspension_policy_identity(authority: Mapping[str, Any] | None = None) -> dict[str, str]:
    payload = _LOADED_AUTHORITY if authority is None else authority
    return {"schema_version": payload["schema_version"], "policy": payload["policy"],
            "authority_sha256": public_suspension.digest(payload),
            "implementation_sha256": _PUBLIC_IMPLEMENTATION_SHA256}


def _public_rows(facts):
    return [{"ts_code": f["symbol"], "trade_date": f["trade_date"],
             "suspend_type": f["suspend_type"], "suspend_timing": f["suspend_timing"]} for f in facts]


def _evidence_source(value, database_source, file_source):
    if "file_binding_hash" not in value:
        return database_source
    if not re.fullmatch(r"[0-9a-f]{64}", str(value["file_binding_hash"])):
        raise ValueError("invalid frozen file binding identity")
    return file_source


def _validate_frozen_day_authority(value, *, symbol, trade_date):
    frozen = value[DAY_AUTHORITY_KEY]
    if not all(f.get("symbol") == symbol and f.get("trade_date") == trade_date.isoformat()
               for f in value["suspension_facts"]):
        return False
    authority = public_suspension.load_authority(frozen["authority_payload"])
    projected = public_suspension.project_suspension_days(
        _public_rows(value["suspension_facts"]), authority=authority, required_dates=[trade_date],
    )
    day = next((r for r in projected if r["ts_code"] == symbol and r["trade_date"] == trade_date.isoformat()), None)
    return (frozen["schema_version"] == DAY_AUTHORITY_KEY
            and frozen["effective_trade_date"] == trade_date.isoformat()
            and frozen["cutoff_date"] == value["cutoff_date"]
            and frozen["usage"] == "D_EFFECTIVE_MARKET_STATE"
            and frozen["policy_identity"] == localsim_suspension_policy_identity(authority)
            and frozen["projected_day"] == day and day is not None and day["state"] == "FULL_DAY_SUSPENDED"
            and frozen["data_identity"] == canonical_json_sha256(
                {k: v for k, v in frozen.items() if k != "data_identity"}))


def validate_suspension_fact(evidence: Mapping[str, Any], *, symbol: str, trade_date: date) -> dict[str, Any]:
    value = dict(evidence)
    facts = value.get("suspension_facts", [])
    try:
        valid = (
            value.get("schema_version") in {REFERENCE_KEY, SUSPENSION_KEY, NATIVE_PRE_CLOSE_KEY}
            and value.get("symbol") == symbol and value.get("trade_date") == trade_date.isoformat()
            and value.get("suspension_source") == _evidence_source(value, "market.suspend_d", "dataset_files.suspend_d")
            and date.fromisoformat(value["cutoff_date"]) < trade_date
            and (_validate_frozen_day_authority(value, symbol=symbol, trade_date=trade_date)
                 if DAY_AUTHORITY_KEY in value else (
                     len(facts) == 1 and all(f.get("symbol") == symbol and f.get("trade_date") == trade_date.isoformat()
                         and f.get("suspend_type") == "S" and f.get("suspend_timing") in (None, "") for f in facts)))
            and value.get("evidence_sha256") == canonical_json_sha256(
                {k: v for k, v in value.items() if k != "evidence_sha256"})
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        valid = False
    if not valid:
        raise DataUnavailableError("LocalSIM full-day halt evidence is invalid",
                                   context={"reason_code": "LOCALSIM_SUSPENSION_FACT_INVALID", "symbol": symbol})
    return value


def validate_suspended_native_pre_close(
    evidence: Mapping[str, Any], *, symbol: str, trade_date: date, price: float | None = None,
) -> dict[str, Any]:
    value = validate_suspension_fact(evidence, symbol=symbol, trade_date=trade_date)
    try:
        previous, upper, lower = (float(value[k]) for k in ("pre_close", "up_limit", "down_limit"))
        valid = (value.get("schema_version") == NATIVE_PRE_CLOSE_KEY
                 and value.get("price_source") == _evidence_source(value, "market.stk_limit.pre_close", "dataset_files.prev_close")
                 and value.get("price_basis") == "raw"
                 and all(isfinite(p) and p > 0 for p in (previous, upper, lower))
                 and lower < previous < upper and (price is None or price == previous))
    except (KeyError, ValueError, TypeError):
        valid = False
    if not valid:
        raise DataUnavailableError("LocalSIM suspended native pre_close evidence is invalid",
                                   context={"reason_code": "LOCALSIM_SUSPENDED_PRE_CLOSE_INVALID", "symbol": symbol})
    return value


def validate_suspension_reference(
    evidence: Mapping[str, Any], *, symbol: str, trade_date: date, price: float | None = None,
) -> dict[str, Any]:
    """Validate the frozen valuation carrier without treating its close as pre_close."""
    value = dict(evidence)
    validate_suspension_fact(value, symbol=symbol, trade_date=trade_date)
    observation = value.get("reference_price_availability", {})
    try:
        cutoff = date.fromisoformat(value["cutoff_date"])
        quote_date = date.fromisoformat(observation["quote_date"])
        reference = float(value["reference_close"])
        valid = (
            value.get("schema_version") == REFERENCE_KEY
            and value.get("symbol") == symbol and value.get("trade_date") == trade_date.isoformat()
            and value.get("price_basis") == "raw"
            and value.get("reference_source") == _evidence_source(value, "market.kline_daily_raw", "dataset_files.daily_pv")
            and value.get("suspension_source") == _evidence_source(value, "market.suspend_d", "dataset_files.suspend_d")
            and observation.get("contract_version") == REFERENCE_PRICE_CONTRACT
            and observation.get("as_of_date") == cutoff.isoformat()
            and observation.get("state") in {"EXACT_CLOSE", "SUSPENSION_LAST_OBSERVED_CLOSE"}
            and observation.get("reason_code") is None
            and quote_date <= cutoff < trade_date
            and isfinite(reference) and reference > 0
            and (price is None or reference == price)
            and value.get("evidence_sha256") == canonical_json_sha256(
                {k: v for k, v in value.items() if k != "evidence_sha256"})
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        valid = False
    if not valid:
        raise DataUnavailableError(
            "LocalSIM suspended valuation requires complete PIT reference evidence",
            context={"reason_code": "LOCALSIM_SUSPENSION_REFERENCE_INVALID", "symbol": symbol,
                     "trade_date": trade_date.isoformat()},
        )
    return value


class LocalSimHistoricalSuspensionProvider:
    """Read each D independently; reuse the public reference-price implementation."""

    def __init__(self, *, conn_factory=None, calendar=None, reference_loader=None, limit_provider=None,
                 authority: Mapping[str, Any] | None = None, event_loader=None, file_binding_hash=None) -> None:
        self.conn_factory = conn_factory or get_conn
        self.calendar = calendar or TradeCalendarProvider(conn_factory=self.conn_factory)
        self.reference_loader = reference_loader or load_reference_prices
        self.limit_provider = limit_provider or StkLimitPriceProvider(conn_factory=self.conn_factory)
        self.authority = deepcopy(public_suspension.load_authority(_LOADED_AUTHORITY if authority is None else authority))
        self.event_loader = event_loader
        self.file_binding_hash = file_binding_hash
        if (event_loader is None) != (file_binding_hash is None):
            raise ValueError("file suspension events require an explicit frozen binding")

    def load_statuses(self, *, symbols: list[str], trade_date: date,
                      valuation_symbols: list[str] | None = None) -> dict[str, dict[str, Any]]:
        normalized = sorted(set(symbols))
        if not normalized:
            return {}
        snapshot = self.calendar.snapshot(trade_date)
        cutoff = snapshot.previous_trading_date
        if not snapshot.is_trading_day or cutoff is None or cutoff >= trade_date:
            raise DataUnavailableError(
                "LocalSIM historical halt reference requires the native previous trading date",
                context={"reason_code": "LOCALSIM_SUSPENSION_CALENDAR_INVALID"},
            )
        facts: dict[str, list[dict[str, Any]]] = {s: [] for s in normalized}
        if self.event_loader is not None:
            rows = self.event_loader(normalized, trade_date)
        else:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT ts_code, trade_date, suspend_type, suspend_timing FROM market.suspend_d "
                        "WHERE trade_date = %s AND ts_code = ANY(%s) ORDER BY ts_code, suspend_type, suspend_timing",
                        (trade_date, normalized),
                    )
                    rows = cur.fetchall()
        for symbol, day, kind, timing in rows:
            facts[symbol].append({"symbol": symbol, "trade_date": day.isoformat(),
                                  "suspend_type": kind, "suspend_timing": timing})
        source = "dataset_files.suspend_d" if self.file_binding_hash else "market.suspend_d"
        projected = public_suspension.project_suspension_days(
            _public_rows([row for rows in facts.values() for row in rows]),
            authority=self.authority, required_dates=[trade_date],
        )
        by_symbol = {row["ts_code"]: row for row in projected if row["ts_code"] in facts}
        for symbol, day in by_symbol.items():
            if day["state"] == "UNRESOLVED":
                raise DataUnavailableError(
                    "LocalSIM historical suspension facts conflict for the same symbol and day",
                    context={"reason_code": "LOCALSIM_SUSPENSION_FACT_CONFLICT", "symbol": symbol,
                             "trade_date": trade_date.isoformat(), "suspension_facts": facts[symbol],
                             "public_projection": day},
                )
        halted = [symbol for symbol, day in by_symbol.items() if day["state"] == "FULL_DAY_SUSPENDED"]
        required = sorted(set(halted) & set(valuation_symbols or []))
        limits = self.limit_provider.get_limit_prices(required, trade_date) if required else {}
        native = []
        for symbol in required:
            limit = limits[symbol]
            if limit.symbol != symbol or limit.trade_date != trade_date:
                raise DataUnavailableError("LocalSIM suspended native limit identity conflicts",
                                           context={"reason_code": "LOCALSIM_SUSPENDED_PRE_CLOSE_INVALID"})
            if limit.pre_close is not None and isfinite(float(limit.pre_close)) and float(limit.pre_close) > 0:
                native.append(symbol)
        references = sorted(set(required) - set(native))
        prices = self.reference_loader(symbols=references, as_of_date=cutoff,
                                       conn_factory=self.conn_factory) if references else {}
        statuses = {}
        for symbol in normalized:
            suspended = symbol in halted
            authority_record = {
                "schema_version": DAY_AUTHORITY_KEY, "effective_trade_date": trade_date.isoformat(),
                "cutoff_date": cutoff.isoformat(), "usage": "D_EFFECTIVE_MARKET_STATE",
                "authority_payload": deepcopy(self.authority),
                "policy_identity": localsim_suspension_policy_identity(self.authority),
                "projected_day": deepcopy(by_symbol.get(symbol)),
            }
            authority_record["data_identity"] = canonical_json_sha256(authority_record)
            status: dict[str, Any] = {
                "symbol": symbol, "trade_date": trade_date.isoformat(), "is_tradable": not suspended,
                "reason_code": "SUSPENDED_BY_SUSPEND_D" if suspended else "OK",
                "source": source,
                DAY_AUTHORITY_KEY: authority_record,
                "suspend_status": {"is_suspended": suspended, "suspend_type": "S" if suspended else None,
                                   "suspend_timing": None, "source": source},
            }
            if suspended:
                fact = {
                    "schema_version": SUSPENSION_KEY, "symbol": symbol, "trade_date": trade_date.isoformat(),
                    "cutoff_date": cutoff.isoformat(), "suspension_source": source,
                    "suspension_facts": facts[symbol],
                    DAY_AUTHORITY_KEY: authority_record,
                }
                if self.file_binding_hash:
                    fact["file_binding_hash"] = self.file_binding_hash
                fact["evidence_sha256"] = canonical_json_sha256(fact)
                status[SUSPENSION_KEY] = validate_suspension_fact(fact, symbol=symbol, trade_date=trade_date)
            if symbol in native:
                limit = limits[symbol]
                evidence = {**status[SUSPENSION_KEY], "schema_version": NATIVE_PRE_CLOSE_KEY,
                            "price_source": "dataset_files.prev_close" if self.file_binding_hash else "market.stk_limit.pre_close", "price_basis": "raw",
                            "pre_close": float(limit.pre_close), "up_limit": float(limit.up_limit),
                            "down_limit": float(limit.down_limit)}
                evidence.pop("evidence_sha256")
                evidence["evidence_sha256"] = canonical_json_sha256(evidence)
                status[NATIVE_PRE_CLOSE_KEY] = validate_suspended_native_pre_close(
                    evidence, symbol=symbol, trade_date=trade_date,
                )
            if symbol in references:
                evidence = {
                    **status[SUSPENSION_KEY],
                    "schema_version": REFERENCE_KEY, "symbol": symbol, "trade_date": trade_date.isoformat(),
                    "cutoff_date": cutoff.isoformat(), "price_basis": "raw",
                    "reference_source": "dataset_files.daily_pv" if self.file_binding_hash else "market.kline_daily_raw", "suspension_source": source,
                    "suspension_facts": facts[symbol], "reference_close": prices.get(symbol),
                    "reference_price_availability": getattr(prices, "observations", {}).get(symbol, {}),
                }
                evidence.pop("evidence_sha256")
                evidence["evidence_sha256"] = canonical_json_sha256(evidence)
                status[REFERENCE_KEY] = validate_suspension_reference(evidence, symbol=symbol, trade_date=trade_date)
            statuses[symbol] = status
        return statuses
