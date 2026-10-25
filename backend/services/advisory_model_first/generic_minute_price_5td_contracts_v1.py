"""One additional D-minute information block; unchanged fixed-five-session policy."""
from pydantic import BaseModel, ConfigDict
import time

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import (
    ARMS, FEATURES, KEY, PARAMETERS, POLICY, POLICY_SHA256, GenericPrice5TDConfigurationV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

MINUTE_FEATURES = (
    "opening_30m_return_bps", "closing_30m_return_bps", "realized_volatility_bps",
    "directional_efficiency", "close_to_vwap_bps", "opening_30m_amount_share",
    "closing_30m_amount_share", "limit_pressure",
)
FIELDS = ("open", "high", "low", "close", "volume", "amount", "limit_up", "limit_down")
SCHEMA = "generic_minute_price_5td_v1"
SCHEMA_SHA256 = sha({"schema": SCHEMA, "daily": FEATURES, "minute": MINUTE_FEATURES,
                    "matched_dimensions": 19, "candidate_dimensions": 35,
                    "coverage_denominator": "ORIGINAL_D_CALENDAR", "minimum_ohlc_coverage": .8,
                    "efficiency": "SUM_ADJACENT_LOG_RETURNS_OVER_ABSOLUTE_SUM"})
PINS = {"meta_export_sha256": "meta_export.json", "calendar_sha256": "calendars/1min.txt",
        "instruments_sha256": "instruments/all.txt"}


class MinuteSourceIdentityV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    generation: str
    minute_root: str
    pins: dict[str, str]


def check_resource_budget_v1(started):
    import psutil
    if time.monotonic()-started > 1800 or psutil.Process().memory_info().rss > 2*1024**3:
        raise ValueError("minute study exceeds declared 30-minute/2GiB resource budget")


__all__ = ["ARMS", "FEATURES", "FIELDS", "KEY", "MINUTE_FEATURES", "PARAMETERS", "PINS",
           "POLICY", "POLICY_SHA256", "SCHEMA", "SCHEMA_SHA256", "GenericPrice5TDConfigurationV1",
           "MinuteSourceIdentityV1", "check_resource_budget_v1"]
