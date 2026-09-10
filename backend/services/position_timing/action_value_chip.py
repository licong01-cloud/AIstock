"""Candidate-local chip-cost context for offline position-timing research.

The source is read-only and content-bound.  A source-day raw close is compared
with the same source-day median chip cost before the result is shifted by one
global trading session, so corporate actions cannot mix two price bases.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from .action_value import (
    CHIP_COST_INFORMATION_BLOCK,
    CORE_INFORMATION_BLOCK,
    MARKET_FEATURES,
    ActionValueError,
    feature_contract,
    market_features,
)
from .action_value_data import BENCHMARK, DailyCandidate, file_reference
from .contracts import canonical_sha256


CHIP_COMPONENT = "factor_h5_static_candidate_v2"
CHIP_DATA_FILE = "cyq_perf.h5"
CHIP_META_SCHEMA = "qe_direct_factor_h5_static_v2"
CHIP_SCHEMA = "qe_static_factors_121_v1"
CHIP_FEATURE = "chip_median_cost_distance_lag1_bps"
REQUIRED_CHIP_FIELDS = ("cp_cost_15pct", "cp_cost_50pct", "cp_cost_85pct")
MIN_SOURCE_SESSIONS = 1_008


@dataclass(frozen=True)
class ChipCostDataSource:
    root: Path
    median_cost: pd.Series
    valid_session_counts: pd.Series
    invalid_source_rows: int
    references: dict[str, dict[str, Any]]
    calendar_start: str
    calendar_end: str

    @classmethod
    def open(cls, candidate_root: Path) -> "ChipCostDataSource":
        root = candidate_root.resolve() / "components" / CHIP_COMPONENT
        paths = {
            "chip_data": root / CHIP_DATA_FILE,
            "chip_meta": root / "meta.json",
            "chip_schema": root / "static_factors_schema.json",
        }
        references = {role: file_reference(path) for role, path in paths.items()}
        try:
            meta = json.loads(paths["chip_meta"].read_text(encoding="utf-8"))
            schema = json.loads(paths["chip_schema"].read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ActionValueError("CHIP_COST_SOURCE_METADATA_INVALID") from exc
        if (
            meta.get("schema_version") != CHIP_META_SCHEMA
            or meta.get("rows_by_file", {}).get(CHIP_DATA_FILE) is None
            or schema.get("schema_version") != CHIP_SCHEMA
            or not set(REQUIRED_CHIP_FIELDS).issubset(schema.get("columns", ()))
        ):
            raise ActionValueError("CHIP_COST_SOURCE_AUTHORITY_MISMATCH")
        try:
            with pd.HDFStore(paths["chip_data"], mode="r") as store:
                if store.keys() != ["/data"]:
                    raise ActionValueError("CHIP_COST_SOURCE_KEY_INVALID")
                rows = store.select("/data", columns=list(REQUIRED_CHIP_FIELDS))
        except ActionValueError:
            raise
        except (OSError, ValueError, KeyError) as exc:
            raise ActionValueError("CHIP_COST_SOURCE_UNAVAILABLE") from exc
        cls._validate_index(
            rows,
            expected_rows=int(meta["rows_by_file"][CHIP_DATA_FILE]),
        )

        instruments = rows.index.get_level_values("instrument").unique().astype(str)
        source_dates = pd.to_datetime(
            rows.index.get_level_values("datetime").unique(), errors="coerce"
        )
        if not pd.Series(instruments).str.fullmatch(r"\d{6}\.(SH|SZ)").all():
            raise ActionValueError("CHIP_COST_SYMBOL_INVALID")
        if source_dates.isna().any():
            raise ActionValueError("CHIP_COST_DATETIME_INVALID")

        numeric = rows.loc[:, REQUIRED_CHIP_FIELDS].apply(pd.to_numeric, errors="coerce")
        valid = (
            np.isfinite(numeric).all(axis=1)
            & numeric["cp_cost_15pct"].gt(0)
            & numeric["cp_cost_15pct"].le(numeric["cp_cost_50pct"])
            & numeric["cp_cost_50pct"].le(numeric["cp_cost_85pct"])
        )
        median_cost = numeric["cp_cost_50pct"].where(valid)
        valid_counts = valid.groupby(level="instrument").sum().astype(int)
        if any(file_reference(paths[role]) != reference for role, reference in references.items()):
            raise ActionValueError("SOURCE_CHANGED_WHILE_READING")
        return cls(
            root=root,
            median_cost=median_cost,
            valid_session_counts=valid_counts,
            invalid_source_rows=int((~valid).sum()),
            references=references,
            calendar_start=pd.Timestamp(rows.index.get_level_values("datetime").min())
            .date()
            .isoformat(),
            calendar_end=pd.Timestamp(rows.index.get_level_values("datetime").max())
            .date()
            .isoformat(),
        )

    @staticmethod
    def _validate_index(frame: pd.DataFrame, *, expected_rows: int) -> None:
        if (
            frame.empty
            or len(frame) != expected_rows
            or not isinstance(frame.index, pd.MultiIndex)
            or tuple(frame.index.names) != ("datetime", "instrument")
            or frame.index.has_duplicates
            or not frame.index.is_monotonic_increasing
        ):
            raise ActionValueError("CHIP_COST_SOURCE_INDEX_INVALID")

    def eligible_symbols(self, *, minimum_sessions: int = MIN_SOURCE_SESSIONS) -> tuple[str, ...]:
        if minimum_sessions <= 1:
            raise ActionValueError("CHIP_COST_MINIMUM_SOURCE_SESSIONS_INVALID")
        return tuple(
            sorted(self.valid_session_counts[self.valid_session_counts.ge(minimum_sessions)].index)
        )

    def feature_for(
        self,
        symbol: str,
        calendar: pd.DatetimeIndex,
        raw_close: pd.Series,
    ) -> pd.Series:
        try:
            cost = self.median_cost.xs(symbol, level="instrument").reindex(calendar)
        except KeyError:
            return pd.Series(np.nan, index=calendar, name=CHIP_FEATURE)
        close = pd.to_numeric(raw_close.reindex(calendar), errors="coerce")
        same_day_distance = ((close / cost - 1) * 10_000).where(close.gt(0) & cost.gt(0))
        result = same_day_distance.shift(1)
        result.name = CHIP_FEATURE
        return result.replace([np.inf, -np.inf], np.nan)


@dataclass
class ChipCostAugmentedCandidate:
    base: DailyCandidate
    chip: ChipCostDataSource
    symbols: tuple[str, ...]
    minimum_source_sessions: int

    @classmethod
    def open(
        cls,
        base: DailyCandidate,
        *,
        minimum_sessions: int = MIN_SOURCE_SESSIONS,
    ) -> "ChipCostAugmentedCandidate":
        chip = ChipCostDataSource.open(base.root)
        symbols = tuple(
            sorted(set(base.symbols) & set(chip.eligible_symbols(minimum_sessions=minimum_sessions)))
        )
        if not symbols:
            raise ActionValueError("CHIP_COST_RESEARCH_POPULATION_EMPTY")
        return cls(base=base, chip=chip, symbols=symbols, minimum_source_sessions=minimum_sessions)

    @property
    def root(self) -> Path:
        return self.base.root

    @property
    def calendar(self) -> pd.DatetimeIndex:
        return self.base.calendar

    def bars(self, symbol: str) -> pd.DataFrame:
        bars = self.base.bars(symbol)
        if symbol != BENCHMARK:
            bars[CHIP_FEATURE] = self.chip.feature_for(symbol, bars.index, bars["close"])
        return bars

    def coverage(self, symbols: Sequence[str]) -> dict[str, Any]:
        selected = tuple(symbols)
        if not selected or len(set(selected)) != len(selected) or not set(selected).issubset(
            self.symbols
        ):
            raise ActionValueError("RESEARCH_POPULATION_INVALID")
        benchmark = self.bars(BENCHMARK).close
        selected_market_features, feature_order, feature_spec_sha256 = feature_contract(
            CHIP_COST_INFORMATION_BLOCK
        )
        counts: dict[str, dict[str, Any]] = {}
        for symbol in selected:
            bars = self.bars(symbol)
            core = market_features(bars, benchmark, information_block=CORE_INFORMATION_BLOCK)
            augmented = market_features(
                bars,
                benchmark,
                information_block=CHIP_COST_INFORMATION_BLOCK,
            )
            active = bars.pit_active
            complete_core = active & core.loc[:, MARKET_FEATURES].notna().all(axis=1)
            complete_selected = active & augmented.notna().all(axis=1)
            counts[symbol] = {
                "pit_sessions": int(active.sum()),
                "valid_chip_source_sessions": int(
                    self.chip.valid_session_counts.get(symbol, 0)
                ),
                "complete_core_sessions": int(complete_core.sum()),
                "complete_selected_feature_sessions": int(complete_selected.sum()),
                "feature_nonmissing": {
                    name: int(augmented.loc[active, name].notna().sum())
                    for name in selected_market_features
                },
            }
        references = {**deepcopy(self.base.references), **deepcopy(self.chip.references)}
        return {
            "schema_version": "position_timing_chip_cost_source_coverage_v1",
            "information_block": CHIP_COST_INFORMATION_BLOCK,
            "symbols": selected,
            "calendar_start": self.calendar[0].date().isoformat(),
            "calendar_end": self.calendar[-1].date().isoformat(),
            "chip_calendar_start": self.chip.calendar_start,
            "chip_calendar_end": self.chip.calendar_end,
            "minimum_chip_source_sessions": self.minimum_source_sessions,
            "eligible_symbol_count": len(self.symbols),
            "invalid_chip_source_rows": self.chip.invalid_source_rows,
            "coverage": counts,
            "feature_order": feature_order,
            "feature_spec_sha256": feature_spec_sha256,
            "source_references": references,
            "source_sha256": canonical_sha256(references),
            "source_available_at_policy": "T_MINUS_1_GLOBAL_SESSION_END_OF_DAY_CONSERVATIVE",
            "historical_ingestion_timestamps_verified": False,
            "optional_blocks": [CHIP_COST_INFORMATION_BLOCK],
            "selection_inputs": "PIT_KEYS_VALID_MONOTONE_POSITIVE_CHIP_COST_SOURCE_ONLY",
            "outcomes_read": False,
        }


__all__ = [
    "CHIP_FEATURE",
    "MIN_SOURCE_SESSIONS",
    "ChipCostAugmentedCandidate",
    "ChipCostDataSource",
]
