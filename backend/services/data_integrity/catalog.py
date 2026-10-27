"""Obligations from the reviewed schedule catalogue and DatasetSpec, not jobs."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from backend.db.init_tushare_schedules import get_default_schedule_catalog
from backend.services.data_sync_targets import PUBLICATION_LAG_DAYS, PUBLICATION_READY_AT
from backend.services.tushare_dataset_specs import DATASET_REGISTRY
from .contracts import digest

CN = ZoneInfo("Asia/Shanghai")
SCOPE_VERSION = "full_provider_fact_scope_v1"
RULE_VERSION = "existing_dataset_quality_contracts_v1"
ALIASES = {"stock_moneyflow_ts": "moneyflow_ts"}
REFERENCE = frozenset({"stock_basic", "sw_index_member", "sw_index_classify", "etf_basic_snapshots"})
EVENT = frozenset({"suspend_d", "stock_st", "stock_st_events", "dividend", "anns_metadata",
                   "tushare_forecast_raw", "tushare_express_raw", "tushare_fina_indicator_raw"})


class IntegrityCatalogueUnavailable(RuntimeError):
    """A server-owned obligation contract is missing, not a client input error."""


def canonical_dataset(name: str) -> str:
    return ALIASES.get(name, name)


@dataclass(frozen=True)
class Obligation:
    dataset: str
    category: str
    ready_time: str
    lag_trading_days: int = 0
    scope_version: str = SCOPE_VERSION
    rule_version: str = RULE_VERSION

    def as_dict(self):
        return asdict(self)

    def due_at(self, day: date, calendar: Mapping[date, bool]) -> datetime | None:
        following = [d for d in sorted(calendar) if d > day and calendar[d]]
        if self.lag_trading_days > len(following):
            return None  # missing official future calendar, not an assumed weekday
        due_day = following[self.lag_trading_days - 1] if self.lag_trading_days else day
        return datetime.combine(due_day, time.fromisoformat(self.ready_time), CN)


def obligations(extra_schedules: Iterable[Mapping[str, Any]] = (),
                historical: Iterable[Mapping[str, Any]] = ()) -> tuple[list[Obligation], str]:
    catalogue = get_default_schedule_catalog()
    if catalogue.get("errors"):
        raise IntegrityCatalogueUnavailable("reviewed obligation catalogue is invalid")
    schedules = list(catalogue["templates"]) + [s for s in extra_schedules if s.get("enabled")]
    entries: dict[str, Obligation] = {}
    for schedule in schedules:
        name = str(schedule["dataset"])
        if name.startswith("_"):
            continue  # maintenance/testing jobs aren't source datasets
        names = ("sw_index_classify", "sw_index_member", "sw_daily") if name == "sw_sector" else (canonical_dataset(name),)
        for dataset in names:
            spec_name = "stock_moneyflow_ts" if dataset == "moneyflow_ts" else dataset
            known = spec_name in DATASET_REGISTRY or dataset in {
                "kline_daily_raw", "kline_minute_raw", "moneyflow_ts", "sector_data", "anns_metadata"}
            if not known:
                raise IntegrityCatalogueUnavailable(f"enabled dataset has no formal obligation contract: {dataset}")
            ready = PUBLICATION_READY_AT.get(dataset)
            clock = ready.isoformat() if ready else str((schedule.get("options") or {}).get("at") or "23:59")
            category = "reference" if dataset in REFERENCE else "event" if dataset in EVENT else "daily_series"
            entries[dataset] = Obligation(dataset, category, clock, PUBLICATION_LAG_DAYS.get(dataset, 0))
    # Once persisted, disabling a schedule cannot erase its historical obligation.
    for row in historical:
        obligation = Obligation(**dict(row))
        entries.setdefault(obligation.dataset, obligation)
    result = sorted(entries.values(), key=lambda item: item.dataset)
    if not result:
        raise IntegrityCatalogueUnavailable("obligation catalogue cannot be empty")
    return result, digest([item.as_dict() for item in result])
