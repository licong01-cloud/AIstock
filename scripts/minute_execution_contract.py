"""Pure minute-execution contract shared by Qlib strategy adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
import json
from pathlib import Path
from typing import Any, Callable, Sequence

import math


TOTAL_TRADING_MINUTES = 240
TAIL_START_OFFSET = 210
REALLOC_OFFSET = 235
V25_EARLY_MINUTES = 30
V24_WARMUP_MINUTES = 30


class MinuteExecutionContractError(RuntimeError):
    """Raised when market data or execution state cannot be used safely."""


class MarketAction(str, Enum):
    TRADE = "TRADE"
    NO_FILL = "NO_FILL"
    P0_FORCE = "P0_FORCE"


@dataclass(frozen=True)
class MarketState:
    action: MarketAction
    reason: str


class DayFeatureArtifact:
    """Strict V25 model-input artifact keyed by trade date and symbol."""

    def __init__(self, path: str | Path, *, expected_schema_version: str) -> None:
        artifact_path = Path(path)
        if not artifact_path.is_file():
            raise MinuteExecutionContractError(f"day_features_file_missing_config_error: {artifact_path}")
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise MinuteExecutionContractError(
                f"day_features_artifact_invalid_data_error: {artifact_path}"
            ) from exc
        if not isinstance(payload, dict):
            raise MinuteExecutionContractError("day_features_artifact_invalid_data_error: expected object")
        schema_version = str(payload.get("schema_version") or "").strip()
        if not expected_schema_version or schema_version != str(expected_schema_version).strip():
            raise MinuteExecutionContractError(
                "day_features_schema_mismatch_config_error: "
                f"expected={expected_schema_version} actual={schema_version}"
            )
        features = payload.get("features_by_date")
        if not isinstance(features, dict):
            raise MinuteExecutionContractError("day_features_artifact_invalid_data_error: missing features_by_date")
        self.path = artifact_path
        self.schema_version = schema_version
        self.features_by_date = features

    @staticmethod
    def _date_key(value: Any) -> str:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        text = str(value or "").strip()
        if len(text) < 10:
            raise MinuteExecutionContractError(f"day_features_trade_date_invalid_data_error: {value}")
        return text[:10]

    @staticmethod
    def _symbol_aliases(symbol: Any) -> tuple[str, ...]:
        text = str(symbol or "").strip().upper()
        code = text.split(".")[0]
        aliases = [text, code]
        if code.isdigit() and len(code) == 6:
            suffix = "SH" if code.startswith(("5", "6", "9")) else "SZ"
            aliases.append(f"{code}.{suffix}")
        return tuple(dict.fromkeys(aliases))

    def vector(self, *, trade_date: Any, symbol: Any) -> list[float]:
        date_key = self._date_key(trade_date)
        daily = self.features_by_date.get(date_key)
        if not isinstance(daily, dict):
            raise MinuteExecutionContractError(
                f"day_features_missing_data_error: trade_date={date_key} symbol={symbol}"
            )
        raw_vector = None
        for alias in self._symbol_aliases(symbol):
            if alias in daily:
                raw_vector = daily[alias]
                break
        if not isinstance(raw_vector, list) or len(raw_vector) != 10:
            raise MinuteExecutionContractError(
                f"day_features_missing_data_error: trade_date={date_key} symbol={symbol}"
            )
        vector = []
        for item in raw_vector:
            try:
                number = float(item)
            except (TypeError, ValueError) as exc:
                raise MinuteExecutionContractError(
                    f"day_features_invalid_data_error: trade_date={date_key} symbol={symbol} value={item}"
                ) from exc
            if not math.isfinite(number):
                raise MinuteExecutionContractError(
                    f"day_features_invalid_data_error: trade_date={date_key} symbol={symbol} value={item}"
                )
            vector.append(number)
        return vector


def _positive_finite(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0


def raw_price(adjusted_price: Any, factor: Any) -> float:
    """Convert Qlib adjusted OHLC to the raw RMB basis used by limits."""

    if not _positive_finite(adjusted_price):
        raise MinuteExecutionContractError(f"adjusted_price_invalid_data_error: {adjusted_price}")
    if not _positive_finite(factor):
        raise MinuteExecutionContractError(f"factor_missing_data_error: {factor}")
    value = float(adjusted_price) / float(factor)
    if not _positive_finite(value):
        raise MinuteExecutionContractError(
            f"raw_price_invalid_data_error: adjusted_price={adjusted_price} factor={factor}"
        )
    return value


def normalize_trade_step(*, trade_step: int, start_idx: int, end_idx: int) -> int | None:
    """Normalize 240 bars and 241 bars with a leading 09:25 auction bar.

    The returned index always uses the 09:30-15:00 240-minute grid. A return
    value of ``None`` identifies the optional auction bar, which is a WAIT/no
    order step rather than a trading minute.
    """

    trade_len = int(end_idx) - int(start_idx) + 1
    if trade_len not in {TOTAL_TRADING_MINUTES, TOTAL_TRADING_MINUTES + 1}:
        raise MinuteExecutionContractError(
            f"minute_calendar_length_data_error: expected=240_or_241 actual={trade_len}"
        )
    relative = int(trade_step) - int(start_idx)
    if relative < 0 or relative >= trade_len:
        raise MinuteExecutionContractError(
            f"minute_calendar_step_data_error: step={trade_step} start={start_idx} end={end_idx}"
        )
    if trade_len == TOTAL_TRADING_MINUTES + 1:
        if relative == 0:
            return None
        relative -= 1
    if relative < 0 or relative >= TOTAL_TRADING_MINUTES:
        raise MinuteExecutionContractError(f"normalized_minute_step_data_error: step={relative}")
    return relative


def v25_stage(normalized_step: int) -> str:
    """Return the V25 two-stage state for one normalized trading minute."""

    if normalized_step < 0 or normalized_step >= TOTAL_TRADING_MINUTES:
        raise MinuteExecutionContractError(f"v25_stage_step_data_error: {normalized_step}")
    return "EARLY" if normalized_step < V25_EARLY_MINUTES else "LATE"


def v24_stage(normalized_step: int) -> str:
    """Return the V24 warmup/plan state for one normalized trading minute."""

    if normalized_step < 0 or normalized_step >= TOTAL_TRADING_MINUTES:
        raise MinuteExecutionContractError(f"v24_stage_step_data_error: {normalized_step}")
    return "WARMUP" if normalized_step < V24_WARMUP_MINUTES else "PLAN"


def select_tail_substitute_candidates(
    backup_candidates: Sequence[tuple[str, float]],
    *,
    blocked_count: int,
    current_holdings: set[str],
    already_added: set[str],
    selling_count: int,
    topk: int,
    is_tradable: Callable[[str], bool],
) -> list[tuple[str, float]]:
    """Select ranked substitutes while preserving candidate-switch semantics."""

    if blocked_count < 0 or selling_count < 0 or topk <= 0:
        raise MinuteExecutionContractError(
            "tail_substitute_config_error: "
            f"blocked_count={blocked_count} selling_count={selling_count} topk={topk}"
        )
    effective_count = len(current_holdings) + len(already_added) - selling_count
    max_new = max(0, min(blocked_count, topk - effective_count))
    selected: list[tuple[str, float]] = []
    for raw_symbol, score in backup_candidates:
        symbol = str(raw_symbol)
        if len(selected) >= max_new:
            break
        if symbol in current_holdings or symbol in already_added:
            continue
        if not is_tradable(symbol):
            continue
        selected.append((symbol, float(score)))
    return selected


def classify_market_state(
    *,
    side: str,
    price: Any,
    prev_close: Any,
    limit_up: Any,
    limit_down: Any,
    volume: Any | None = None,
    suspended_by_exchange: bool = False,
    suspended_by_suspend_d: bool = False,
) -> MarketState:
    """Classify explicit market states without hiding data failures."""

    normalized_side = str(side or "").upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise MinuteExecutionContractError(f"unsupported_side_config_error: {side}")
    if suspended_by_suspend_d:
        return MarketState(MarketAction.NO_FILL, "suspended_by_suspend_d")
    if suspended_by_exchange:
        return MarketState(MarketAction.NO_FILL, "suspended_by_exchange")

    if volume is not None:
        try:
            parsed_volume = float(volume)
        except (TypeError, ValueError) as exc:
            raise MinuteExecutionContractError(f"volume_invalid_data_error: {volume}") from exc
        if not math.isfinite(parsed_volume) or parsed_volume < 0:
            raise MinuteExecutionContractError(f"volume_invalid_data_error: {volume}")
        if parsed_volume == 0:
            return MarketState(MarketAction.NO_FILL, "intraday_halt_or_no_bar")

    if not _positive_finite(price):
        raise MinuteExecutionContractError(f"price_missing_data_error: {price}")
    if not _positive_finite(prev_close):
        raise MinuteExecutionContractError(f"prev_close_missing_data_error: {prev_close}")
    if not _positive_finite(limit_up) or not _positive_finite(limit_down):
        raise MinuteExecutionContractError(
            f"limit_price_missing_data_error: limit_up={limit_up} limit_down={limit_down}"
        )

    current = float(price)
    upper = float(limit_up)
    lower = float(limit_down)
    epsilon = 1e-6
    if normalized_side == "BUY" and current >= upper * (1 - epsilon):
        return MarketState(MarketAction.NO_FILL, "limit_up_buy_blocked")
    if normalized_side == "SELL" and current <= lower * (1 + epsilon):
        return MarketState(MarketAction.NO_FILL, "limit_down_sell_blocked")
    if normalized_side == "BUY" and current <= lower * (1 + epsilon):
        return MarketState(MarketAction.P0_FORCE, "p0_limit_buy_at_down_limit")
    if normalized_side == "SELL" and current >= upper * (1 - epsilon):
        return MarketState(MarketAction.P0_FORCE, "p0_limit_sell_at_up_limit")
    return MarketState(MarketAction.TRADE, "tradable")


def strict_remaining_fraction(plan: Sequence[float], plan_index: int, *, expected_length: int) -> float:
    """Return the current conditional plan fraction or fail; never use TWAP."""

    if len(plan) != expected_length:
        raise MinuteExecutionContractError(
            f"execution_plan_length_error: expected={expected_length} actual={len(plan)}"
        )
    if plan_index < 0 or plan_index >= expected_length:
        raise MinuteExecutionContractError(
            f"execution_plan_index_error: index={plan_index} length={expected_length}"
        )
    values = []
    for item in plan:
        try:
            value = float(item)
        except (TypeError, ValueError) as exc:
            raise MinuteExecutionContractError(f"execution_plan_value_error: {item}") from exc
        if not math.isfinite(value) or value < 0:
            raise MinuteExecutionContractError(f"execution_plan_value_error: {item}")
        values.append(value)
    remaining_weight = sum(values[plan_index:])
    if remaining_weight <= 0:
        raise MinuteExecutionContractError(
            f"execution_plan_remaining_weight_error: index={plan_index}"
        )
    return values[plan_index] / remaining_weight
