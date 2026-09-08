"""Candidate-local PIT Shenwan L2 context for offline position-timing research.

The industry code is only a join key.  This reader never turns its ordinal id
into a model feature and never reads or writes the database.
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
    CORE_INFORMATION_BLOCK,
    MARKET_FEATURES,
    SW_L2_INFORMATION_BLOCK,
    ActionValueError,
    feature_contract,
    market_features,
)
from .action_value_data import BENCHMARK, DailyCandidate, file_reference
from .contracts import canonical_sha256


SECTOR_COMPONENT = "factor_h5_static_candidate_v2"
SECTOR_DATA_FILE = "sector_data.h5"
SECTOR_META_SCHEMA = "qe_direct_factor_h5_static_v2"
SECTOR_SCHEMA = "qe_static_factors_121_v1"
SECTOR_AUTHORITY = "classification_pit_to_published_l2_v2"
UNKNOWN_L2_CODE_ID = -1
MIN_SOURCE_SESSIONS = 1_008


@dataclass(frozen=True)
class SectorDataSource:
    root: Path
    membership: pd.Series
    sector_return_20d_bps: pd.Series
    valid_session_counts: pd.Series
    references: dict[str, dict[str, Any]]
    calendar_start: str
    calendar_end: str

    @classmethod
    def open(cls, candidate_root: Path) -> "SectorDataSource":
        root = candidate_root.resolve() / "components" / SECTOR_COMPONENT
        paths = {
            "sector_data": root / SECTOR_DATA_FILE,
            "sector_meta": root / "meta.json",
            "sector_schema": root / "static_factors_schema.json",
        }
        references = {role: file_reference(path) for role, path in paths.items()}
        try:
            meta = json.loads(paths["sector_meta"].read_text(encoding="utf-8"))
            schema = json.loads(paths["sector_schema"].read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ActionValueError("SW_L2_SOURCE_METADATA_INVALID") from exc
        if (
            meta.get("schema_version") != SECTOR_META_SCHEMA
            or meta.get("sector_authority") != SECTOR_AUTHORITY
            or meta.get("rows_by_file", {}).get(SECTOR_DATA_FILE) is None
            or schema.get("schema_version") != SECTOR_SCHEMA
            or schema.get("l2_code_id_dtype") not in (None, "int16")
            or schema.get("l2_code_id_missing") != UNKNOWN_L2_CODE_ID
            or not {"l2_code_id", "sw2_close"}.issubset(schema.get("columns", ()))
        ):
            raise ActionValueError("SW_L2_SOURCE_AUTHORITY_MISMATCH")
        try:
            with pd.HDFStore(paths["sector_data"], mode="r") as store:
                if store.keys() != ["/data"]:
                    raise ActionValueError("SW_L2_SOURCE_KEY_INVALID")
                rows = store.select("/data", columns=["l2_code_id", "sw2_close"])
        except ActionValueError:
            raise
        except (OSError, ValueError, KeyError) as exc:
            raise ActionValueError("SW_L2_SOURCE_UNAVAILABLE") from exc
        if (
            rows.empty
            or len(rows) != int(meta["rows_by_file"][SECTOR_DATA_FILE])
            or not isinstance(rows.index, pd.MultiIndex)
            or tuple(rows.index.names) != ("datetime", "instrument")
            or rows.index.has_duplicates
            or not rows.index.is_monotonic_increasing
        ):
            raise ActionValueError("SW_L2_SOURCE_INDEX_INVALID")

        frame = rows.reset_index()
        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
        frame["instrument"] = frame["instrument"].astype(str).str.upper()
        encoded = pd.to_numeric(frame["l2_code_id"], errors="coerce")
        if (
            frame["datetime"].isna().any()
            or not frame["instrument"].str.fullmatch(r"\d{6}\.(SH|SZ)").all()
            or encoded.isna().any()
            or not np.equal(encoded, np.floor(encoded)).all()
        ):
            raise ActionValueError("SW_L2_MEMBERSHIP_INVALID")
        frame["l2_code_id"] = encoded.astype(np.int16)
        close = pd.to_numeric(frame["sw2_close"], errors="coerce")
        frame["sw2_close"] = close.where(np.isfinite(close) & close.gt(0))
        frame.loc[frame["l2_code_id"].eq(UNKNOWN_L2_CODE_ID), "sw2_close"] = np.nan

        usable = frame.loc[frame["l2_code_id"].ne(UNKNOWN_L2_CODE_ID)]
        by_sector_day = usable.groupby(["datetime", "l2_code_id"], sort=True)["sw2_close"]
        bounds = by_sector_day.agg(["min", "max"])
        conflicts = bounds["min"].notna() & bounds["max"].notna() & bounds["min"].ne(bounds["max"])
        if conflicts.any():
            raise ActionValueError("SW_L2_SECTOR_DAY_VALUE_CONFLICT")
        sector_close = by_sector_day.first().unstack("l2_code_id")
        sector_close = sector_close.reindex(sorted(frame["datetime"].unique()))
        complete = sector_close.rolling(21, min_periods=21).count().eq(21)
        sector_returns = ((sector_close / sector_close.shift(20) - 1) * 10_000).where(complete)
        sector_returns = sector_returns.stack(future_stack=True)
        sector_returns.index.names = ["datetime", "l2_code_id"]
        sector_returns.name = "sw_l2_return_20d_bps"

        membership_frame = frame.set_index(["datetime", "instrument"]).sort_index()
        membership = membership_frame["l2_code_id"].copy()
        valid = membership_frame["l2_code_id"].ne(UNKNOWN_L2_CODE_ID) & membership_frame[
            "sw2_close"
        ].notna()
        valid_counts = valid.groupby(level="instrument").sum().astype(int)
        if any(file_reference(paths[role]) != reference for role, reference in references.items()):
            raise ActionValueError("SOURCE_CHANGED_WHILE_READING")
        return cls(
            root=root,
            membership=membership,
            sector_return_20d_bps=sector_returns,
            valid_session_counts=valid_counts,
            references=references,
            calendar_start=pd.Timestamp(frame["datetime"].min()).date().isoformat(),
            calendar_end=pd.Timestamp(frame["datetime"].max()).date().isoformat(),
        )

    def eligible_symbols(self, *, minimum_sessions: int = MIN_SOURCE_SESSIONS) -> tuple[str, ...]:
        if minimum_sessions <= 20:
            raise ActionValueError("SW_L2_MINIMUM_SOURCE_SESSIONS_INVALID")
        return tuple(sorted(self.valid_session_counts[self.valid_session_counts.ge(minimum_sessions)].index))

    def feature_for(self, symbol: str, calendar: pd.DatetimeIndex) -> pd.Series:
        try:
            membership = self.membership.xs(symbol, level="instrument").reindex(calendar)
        except KeyError:
            return pd.Series(np.nan, index=calendar, name="sw_l2_return_20d_bps")
        keys = pd.MultiIndex.from_arrays(
            [calendar, membership.fillna(UNKNOWN_L2_CODE_ID).astype(int)],
            names=["datetime", "l2_code_id"],
        )
        values = self.sector_return_20d_bps.reindex(keys).to_numpy(float)
        return pd.Series(values, index=calendar, name="sw_l2_return_20d_bps")


@dataclass
class SectorAugmentedCandidate:
    base: DailyCandidate
    sector: SectorDataSource
    symbols: tuple[str, ...]
    minimum_source_sessions: int

    @classmethod
    def open(
        cls,
        base: DailyCandidate,
        *,
        minimum_sessions: int = MIN_SOURCE_SESSIONS,
    ) -> "SectorAugmentedCandidate":
        sector = SectorDataSource.open(base.root)
        symbols = tuple(sorted(set(base.symbols) & set(sector.eligible_symbols(minimum_sessions=minimum_sessions))))
        if not symbols:
            raise ActionValueError("SW_L2_RESEARCH_POPULATION_EMPTY")
        return cls(
            base=base,
            sector=sector,
            symbols=symbols,
            minimum_source_sessions=minimum_sessions,
        )

    @property
    def root(self) -> Path:
        return self.base.root

    @property
    def calendar(self) -> pd.DatetimeIndex:
        return self.base.calendar

    def bars(self, symbol: str) -> pd.DataFrame:
        bars = self.base.bars(symbol)
        if symbol != BENCHMARK:
            bars["sw_l2_return_20d_bps"] = self.sector.feature_for(symbol, bars.index)
        return bars

    def coverage(self, symbols: Sequence[str]) -> dict[str, Any]:
        selected = tuple(symbols)
        if not selected or len(set(selected)) != len(selected) or not set(selected).issubset(self.symbols):
            raise ActionValueError("RESEARCH_POPULATION_INVALID")
        benchmark = self.bars(BENCHMARK).close
        selected_market_features, feature_order, feature_spec_sha256 = feature_contract(
            SW_L2_INFORMATION_BLOCK
        )
        counts: dict[str, dict[str, Any]] = {}
        for symbol in selected:
            bars = self.bars(symbol)
            core = market_features(bars, benchmark, information_block=CORE_INFORMATION_BLOCK)
            augmented = market_features(bars, benchmark, information_block=SW_L2_INFORMATION_BLOCK)
            active = bars.pit_active
            complete_core = active & core.loc[:, MARKET_FEATURES].notna().all(axis=1)
            complete_selected = active & augmented.notna().all(axis=1)
            counts[symbol] = {
                "pit_sessions": int(active.sum()),
                "valid_sector_source_sessions": int(self.sector.valid_session_counts.get(symbol, 0)),
                "complete_core_sessions": int(complete_core.sum()),
                "complete_selected_feature_sessions": int(complete_selected.sum()),
                "feature_nonmissing": {
                    name: int(augmented.loc[active, name].notna().sum())
                    for name in selected_market_features
                },
            }
        references = {**deepcopy(self.base.references), **deepcopy(self.sector.references)}
        return {
            "schema_version": "position_timing_sw_l2_source_coverage_v1",
            "information_block": SW_L2_INFORMATION_BLOCK,
            "symbols": selected,
            "calendar_start": self.calendar[0].date().isoformat(),
            "calendar_end": self.calendar[-1].date().isoformat(),
            "sector_calendar_start": self.sector.calendar_start,
            "sector_calendar_end": self.sector.calendar_end,
            "minimum_sector_source_sessions": self.minimum_source_sessions,
            "eligible_symbol_count": len(self.symbols),
            "coverage": counts,
            "feature_order": feature_order,
            "feature_spec_sha256": feature_spec_sha256,
            "source_references": references,
            "source_sha256": canonical_sha256(references),
            "source_available_at_policy": "DAILY_EXPORT_EOD_CUTOFF_ASSUMPTION",
            "historical_ingestion_timestamps_verified": False,
            "optional_blocks": [SW_L2_INFORMATION_BLOCK],
            "selection_inputs": "PIT_KEYS_VALID_L2_AND_NONMISSING_POSITIVE_SECTOR_CLOSE_ONLY",
            "outcomes_read": False,
        }


__all__ = [
    "MIN_SOURCE_SESSIONS",
    "SectorAugmentedCandidate",
    "SectorDataSource",
]
