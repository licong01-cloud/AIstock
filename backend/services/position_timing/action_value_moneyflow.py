"""Candidate-local lagged moneyflow context for offline position-timing research.

The candidate HDF already contains canonical CNY amounts.  This reader applies
no unit conversion, uses no same-day value, and never reads or writes the DB.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from backend.data_service.moneyflow_contract import (
    MONEYFLOW_UNIT_CONTRACT_VERSION,
    moneyflow_unit_contract_receipt,
)

from .action_value import (
    CORE_INFORMATION_BLOCK,
    MARKET_FEATURES,
    MONEYFLOW_INFORMATION_BLOCK,
    ActionValueError,
    feature_contract,
    market_features,
)
from .action_value_data import BENCHMARK, DailyCandidate, file_reference
from .contracts import canonical_sha256


MONEYFLOW_COMPONENT = "factor_h5_static_candidate_v2"
MONEYFLOW_DATA_FILE = "moneyflow.h5"
DAILY_PV_FILE = "daily_pv.h5"
MONEYFLOW_META_SCHEMA = "qe_direct_factor_h5_static_v2"
MONEYFLOW_STATIC_SCHEMA = "qe_static_factors_121_v1"
MONEYFLOW_FEATURE = "main_net_flow_ratio_5d_lag1_bps"
MONEYFLOW_WINDOW = 5
MIN_SOURCE_SESSIONS = 1_008
REQUIRED_MONEYFLOW_FIELDS = (
    "mf_lg_buy_amt",
    "mf_lg_sell_amt",
    "mf_elg_buy_amt",
    "mf_elg_sell_amt",
)


@dataclass(frozen=True)
class MoneyflowDataSource:
    root: Path
    amounts: pd.DataFrame
    valid_session_counts: pd.Series
    references: dict[str, dict[str, Any]]
    calendar_start: str
    calendar_end: str
    unit_contract: dict[str, Any]

    @classmethod
    def open(cls, candidate_root: Path) -> "MoneyflowDataSource":
        root = candidate_root.resolve() / "components" / MONEYFLOW_COMPONENT
        paths = {
            "moneyflow_data": root / MONEYFLOW_DATA_FILE,
            "daily_pv_data": root / DAILY_PV_FILE,
            "moneyflow_meta": root / "meta.json",
            "moneyflow_schema": root / "static_factors_schema.json",
        }
        references = {role: file_reference(path) for role, path in paths.items()}
        try:
            meta = json.loads(paths["moneyflow_meta"].read_text(encoding="utf-8"))
            schema = json.loads(paths["moneyflow_schema"].read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ActionValueError("MONEYFLOW_SOURCE_METADATA_INVALID") from exc
        columns = set(schema.get("columns", ()))
        if (
            meta.get("schema_version") != MONEYFLOW_META_SCHEMA
            or meta.get("rows_by_file", {}).get(MONEYFLOW_DATA_FILE) is None
            or meta.get("rows_by_file", {}).get(DAILY_PV_FILE) is None
            or schema.get("schema_version") != MONEYFLOW_STATIC_SCHEMA
            or not set(REQUIRED_MONEYFLOW_FIELDS).issubset(columns)
            or MONEYFLOW_UNIT_CONTRACT_VERSION != "tushare_moneyflow_shares_yuan_v1"
        ):
            raise ActionValueError("MONEYFLOW_SOURCE_AUTHORITY_MISMATCH")
        try:
            with pd.HDFStore(paths["moneyflow_data"], mode="r") as store:
                if store.keys() != ["/data"]:
                    raise ActionValueError("MONEYFLOW_SOURCE_KEY_INVALID")
                moneyflow = store.select("/data", columns=list(REQUIRED_MONEYFLOW_FIELDS))
            with pd.HDFStore(paths["daily_pv_data"], mode="r") as store:
                if store.keys() != ["/data"]:
                    raise ActionValueError("MONEYFLOW_DAILY_PV_KEY_INVALID")
                daily_amount = store.select("/data", columns=["amount"])
        except ActionValueError:
            raise
        except (OSError, ValueError, KeyError) as exc:
            raise ActionValueError("MONEYFLOW_SOURCE_UNAVAILABLE") from exc
        cls._validate_index(
            moneyflow,
            expected_rows=int(meta["rows_by_file"][MONEYFLOW_DATA_FILE]),
            error_code="MONEYFLOW_SOURCE_INDEX_INVALID",
        )
        cls._validate_index(
            daily_amount,
            expected_rows=int(meta["rows_by_file"][DAILY_PV_FILE]),
            error_code="MONEYFLOW_DAILY_PV_INDEX_INVALID",
        )
        instruments = moneyflow.index.get_level_values("instrument").unique().astype(str)
        daily_instruments = daily_amount.index.get_level_values("instrument").unique().astype(str)
        source_dates = pd.to_datetime(
            moneyflow.index.get_level_values("datetime").unique(), errors="coerce"
        )
        daily_dates = pd.to_datetime(
            daily_amount.index.get_level_values("datetime").unique(), errors="coerce"
        )
        if not pd.Series(instruments).str.fullmatch(r"\d{6}\.(SH|SZ)").all() or not pd.Series(
            daily_instruments
        ).str.fullmatch(r"\d{6}\.(SH|SZ)").all():
            raise ActionValueError("MONEYFLOW_SYMBOL_INVALID")
        if source_dates.isna().any() or daily_dates.isna().any():
            raise ActionValueError("MONEYFLOW_DATETIME_INVALID")

        numeric = moneyflow.loc[:, REQUIRED_MONEYFLOW_FIELDS].apply(pd.to_numeric, errors="coerce")
        main_net_amount = (
            numeric["mf_lg_buy_amt"]
            + numeric["mf_elg_buy_amt"]
            - numeric["mf_lg_sell_amt"]
            - numeric["mf_elg_sell_amt"]
        )
        amount = pd.to_numeric(daily_amount["amount"], errors="coerce").reindex(moneyflow.index)
        amounts = pd.DataFrame(
            {"main_net_amount_cny": main_net_amount, "daily_amount_cny": amount},
            index=moneyflow.index,
        )
        valid = (
            np.isfinite(amounts["main_net_amount_cny"])
            & np.isfinite(amounts["daily_amount_cny"])
            & amounts["daily_amount_cny"].gt(0)
        )
        amounts = amounts.where(valid)
        valid_counts = valid.groupby(level="instrument").sum().astype(int)
        if any(file_reference(paths[role]) != reference for role, reference in references.items()):
            raise ActionValueError("SOURCE_CHANGED_WHILE_READING")
        return cls(
            root=root,
            amounts=amounts,
            valid_session_counts=valid_counts,
            references=references,
            calendar_start=pd.Timestamp(moneyflow.index.get_level_values("datetime").min()).date().isoformat(),
            calendar_end=pd.Timestamp(moneyflow.index.get_level_values("datetime").max()).date().isoformat(),
            unit_contract=moneyflow_unit_contract_receipt(),
        )

    @staticmethod
    def _validate_index(frame: pd.DataFrame, *, expected_rows: int, error_code: str) -> None:
        if (
            frame.empty
            or len(frame) != expected_rows
            or not isinstance(frame.index, pd.MultiIndex)
            or tuple(frame.index.names) != ("datetime", "instrument")
            or frame.index.has_duplicates
            or not frame.index.is_monotonic_increasing
        ):
            raise ActionValueError(error_code)

    def eligible_symbols(self, *, minimum_sessions: int = MIN_SOURCE_SESSIONS) -> tuple[str, ...]:
        if minimum_sessions <= MONEYFLOW_WINDOW:
            raise ActionValueError("MONEYFLOW_MINIMUM_SOURCE_SESSIONS_INVALID")
        return tuple(sorted(self.valid_session_counts[self.valid_session_counts.ge(minimum_sessions)].index))

    def feature_for(self, symbol: str, calendar: pd.DatetimeIndex) -> pd.Series:
        try:
            source = self.amounts.xs(symbol, level="instrument").reindex(calendar)
        except KeyError:
            return pd.Series(np.nan, index=calendar, name=MONEYFLOW_FEATURE)
        complete = source.notna().all(axis=1).rolling(
            MONEYFLOW_WINDOW, min_periods=MONEYFLOW_WINDOW
        ).sum().eq(MONEYFLOW_WINDOW)
        numerator = source["main_net_amount_cny"].rolling(
            MONEYFLOW_WINDOW, min_periods=MONEYFLOW_WINDOW
        ).sum()
        denominator = source["daily_amount_cny"].rolling(
            MONEYFLOW_WINDOW, min_periods=MONEYFLOW_WINDOW
        ).sum()
        ratio = (numerator / denominator * 10_000).where(complete & denominator.gt(0)).shift(1)
        ratio.name = MONEYFLOW_FEATURE
        return ratio.replace([np.inf, -np.inf], np.nan)


@dataclass
class MoneyflowAugmentedCandidate:
    base: DailyCandidate
    moneyflow: MoneyflowDataSource
    symbols: tuple[str, ...]
    minimum_source_sessions: int

    @classmethod
    def open(
        cls,
        base: DailyCandidate,
        *,
        minimum_sessions: int = MIN_SOURCE_SESSIONS,
    ) -> "MoneyflowAugmentedCandidate":
        moneyflow = MoneyflowDataSource.open(base.root)
        symbols = tuple(
            sorted(set(base.symbols) & set(moneyflow.eligible_symbols(minimum_sessions=minimum_sessions)))
        )
        if not symbols:
            raise ActionValueError("MONEYFLOW_RESEARCH_POPULATION_EMPTY")
        return cls(base, moneyflow, symbols, minimum_sessions)

    @property
    def root(self) -> Path:
        return self.base.root

    @property
    def calendar(self) -> pd.DatetimeIndex:
        return self.base.calendar

    def bars(self, symbol: str) -> pd.DataFrame:
        bars = self.base.bars(symbol)
        if symbol != BENCHMARK:
            bars[MONEYFLOW_FEATURE] = self.moneyflow.feature_for(symbol, bars.index)
        return bars

    def coverage(self, symbols: Sequence[str]) -> dict[str, Any]:
        selected = tuple(symbols)
        if not selected or len(set(selected)) != len(selected) or not set(selected).issubset(self.symbols):
            raise ActionValueError("RESEARCH_POPULATION_INVALID")
        benchmark = self.bars(BENCHMARK).close
        selected_market_features, feature_order, feature_spec_sha256 = feature_contract(
            MONEYFLOW_INFORMATION_BLOCK
        )
        counts: dict[str, dict[str, Any]] = {}
        for symbol in selected:
            bars = self.bars(symbol)
            core = market_features(bars, benchmark, information_block=CORE_INFORMATION_BLOCK)
            augmented = market_features(bars, benchmark, information_block=MONEYFLOW_INFORMATION_BLOCK)
            active = bars.pit_active
            complete_core = active & core.loc[:, MARKET_FEATURES].notna().all(axis=1)
            complete_selected = active & augmented.notna().all(axis=1)
            counts[symbol] = {
                "pit_sessions": int(active.sum()),
                "valid_moneyflow_source_sessions": int(
                    self.moneyflow.valid_session_counts.get(symbol, 0)
                ),
                "complete_core_sessions": int(complete_core.sum()),
                "complete_selected_feature_sessions": int(complete_selected.sum()),
                "feature_nonmissing": {
                    name: int(augmented.loc[active, name].notna().sum())
                    for name in selected_market_features
                },
            }
        references = {**deepcopy(self.base.references), **deepcopy(self.moneyflow.references)}
        return {
            "schema_version": "position_timing_moneyflow_source_coverage_v1",
            "information_block": MONEYFLOW_INFORMATION_BLOCK,
            "symbols": selected,
            "calendar_start": self.calendar[0].date().isoformat(),
            "calendar_end": self.calendar[-1].date().isoformat(),
            "moneyflow_calendar_start": self.moneyflow.calendar_start,
            "moneyflow_calendar_end": self.moneyflow.calendar_end,
            "minimum_moneyflow_source_sessions": self.minimum_source_sessions,
            "eligible_symbol_count": len(self.symbols),
            "coverage": counts,
            "feature_order": feature_order,
            "feature_spec_sha256": feature_spec_sha256,
            "moneyflow_unit_contract": self.moneyflow.unit_contract,
            "source_references": references,
            "source_sha256": canonical_sha256(references),
            "source_available_at_policy": "T_MINUS_1_GLOBAL_SESSION_END_OF_DAY_CONSERVATIVE",
            "historical_ingestion_timestamps_verified": False,
            "optional_blocks": [MONEYFLOW_INFORMATION_BLOCK],
            "selection_inputs": "PIT_KEYS_VALID_CANONICAL_MONEYFLOW_AND_POSITIVE_DAILY_AMOUNT_ONLY",
            "outcomes_read": False,
        }


__all__ = [
    "MIN_SOURCE_SESSIONS",
    "MONEYFLOW_FEATURE",
    "MoneyflowAugmentedCandidate",
    "MoneyflowDataSource",
]
