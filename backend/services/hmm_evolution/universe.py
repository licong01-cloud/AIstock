"""Resolve the exact source-loop stock pool and immutable QE ST-PIT universe."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Protocol

import numpy as np
import pandas as pd
from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.factor_universe_mask_service import FactorUniverseMaskService
from backend.services.quantevolver.qe_dataset_contract import (
    QE_DATASET_CONTRACT_ID,
    QE_ST_PIT_UNIVERSE_KEY,
)
from backend.services.quantevolver.stock_pool_sync import (
    StockPoolSnapshot,
    read_stock_pool_snapshot,
)
from backend.services.stock_universe_pit_service import DEFAULT_ST_PIT_RULE_VERSION

from .errors import InvalidSpecError
from .models import EvaluationSpec


SOURCE_LOOP_UNIVERSE_TYPE = "source_loop_stock_pool_st_pit"
_LOOP_RE = re.compile(r"^Loop(?P<index>[1-9][0-9]*)$")


@dataclass(frozen=True)
class SourceLoopUniverseContract:
    task_id: str
    loop_name: str
    stock_pool: str
    risk_policy: Mapping[str, Any]


@dataclass(frozen=True)
class ResolvedEvaluationUniverse:
    predictions: pd.DataFrame
    labels: pd.DataFrame
    evidence: Mapping[str, Any]


class SourceLoopUniverseRepository(Protocol):
    def load(self, base_loop_ref: str) -> SourceLoopUniverseContract: ...


class StockPoolSnapshotLoader(Protocol):
    def __call__(self, stock_pool: str) -> StockPoolSnapshot: ...


class QELoopUniverseRepository:
    """Read the source loop's persisted selection contract without modifying QE state."""

    def load(self, base_loop_ref: str) -> SourceLoopUniverseContract:
        task_id, loop_name, loop_index = _parse_loop_ref(base_loop_ref)
        with get_conn(autocommit=False, manage_transaction=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute(
                    """
                    SELECT config_json
                    FROM qe_evolution_loops
                    WHERE task_id = %s AND loop_index = %s AND status = 'completed'
                    """,
                    (task_id, loop_index),
                )
                row = cursor.fetchone()
        if row is None:
            raise InvalidSpecError(
                "source QE loop has no completed persisted selection contract",
                context={"base_loop_ref": base_loop_ref},
            )
        config = dict(row.get("config_json") or {})
        config_sections = [
            config,
            *(
                dict(config.get(key) or {})
                for key in ("model_params", "strategy_params")
                if isinstance(config.get(key), Mapping)
            ),
        ]
        stock_pools = {
            str(section.get("stock_pool") or "").strip()
            for section in config_sections
            if str(section.get("stock_pool") or "").strip()
        }
        if not stock_pools:
            raise InvalidSpecError(
                "source QE loop does not declare an explicit stock_pool",
                context={"base_loop_ref": base_loop_ref},
            )
        if len(stock_pools) != 1:
            raise InvalidSpecError(
                "source QE loop contains conflicting stock_pool declarations",
                context={"base_loop_ref": base_loop_ref},
            )
        stock_pool = next(iter(stock_pools))
        risk_candidates = [
            dict(section.get("risk_policy") or {})
            for section in config_sections
            if isinstance(section.get("risk_policy"), Mapping)
        ]
        if not risk_candidates:
            raise InvalidSpecError(
                "source QE loop does not declare an ST-PIT risk policy",
                context={"base_loop_ref": base_loop_ref},
            )
        if any(candidate != risk_candidates[0] for candidate in risk_candidates[1:]):
            raise InvalidSpecError(
                "source QE loop contains conflicting risk policy declarations",
                context={"base_loop_ref": base_loop_ref},
            )
        risk_policy = risk_candidates[0]
        if (
            risk_policy.get("enabled") is not True
            or "st_pit" not in set(risk_policy.get("providers") or ())
            or risk_policy.get("strict_data_ready") is not True
        ):
            raise InvalidSpecError(
                "source QE loop ST-PIT policy is not enabled in strict mode",
                context={"base_loop_ref": base_loop_ref},
            )
        configured_universe_key = str(risk_policy.get("st_universe_key") or "").strip()
        if configured_universe_key and configured_universe_key != QE_ST_PIT_UNIVERSE_KEY:
            raise InvalidSpecError(
                "source QE loop ST-PIT universe differs from its immutable dataset contract",
                context={"base_loop_ref": base_loop_ref},
            )
        return SourceLoopUniverseContract(
            task_id=task_id,
            loop_name=loop_name,
            stock_pool=stock_pool,
            risk_policy=risk_policy,
        )


class QEExecutionUniverseResolver:
    """Intersect source-loop pool intervals with the immutable QE ST-PIT mask."""

    def __init__(
        self,
        *,
        loop_repository: SourceLoopUniverseRepository | None = None,
        pit_service: FactorUniverseMaskService | None = None,
        stock_pool_loader: StockPoolSnapshotLoader = read_stock_pool_snapshot,
    ) -> None:
        self._loop_repository = loop_repository or QELoopUniverseRepository()
        self._pit_service = pit_service or FactorUniverseMaskService()
        self._stock_pool_loader = stock_pool_loader

    def resolve(
        self,
        *,
        evaluation_spec: EvaluationSpec,
        predictions: pd.DataFrame,
        labels: pd.DataFrame,
    ) -> ResolvedEvaluationUniverse:
        if evaluation_spec.schema_version != "hmm_evaluation_spec_v2":
            raise InvalidSpecError("new HMM evaluations require hmm_evaluation_spec_v2")
        if evaluation_spec.universe != {"type": SOURCE_LOOP_UNIVERSE_TYPE}:
            raise InvalidSpecError(
                "HMM evaluation universe must be resolved from the source loop stock pool and ST-PIT contract"
            )
        contract = self._loop_repository.load(evaluation_spec.base_loop_ref)
        try:
            pool_snapshot = self._stock_pool_loader(contract.stock_pool)
        except (OSError, RuntimeError, ValueError) as exc:
            raise InvalidSpecError(
                "source QE stock_pool cannot be read and verified",
                context={"stock_pool": contract.stock_pool, "error_type": type(exc).__name__},
            ) from exc

        normalized_predictions = _window_pairs(
            _normalize_pairs(predictions, required_value="score"),
            start=evaluation_spec.window_start,
            end=evaluation_spec.window_end,
        )
        normalized_labels = _window_pairs(
            _normalize_pairs(labels, required_value="future_return"),
            start=evaluation_spec.window_start,
            end=evaluation_spec.window_end,
        )
        dates = tuple(sorted(normalized_predictions["trade_date"].unique()))
        symbols = tuple(sorted(normalized_predictions["symbol"].unique()))
        if not dates or not symbols:
            raise InvalidSpecError("source prediction artifact has no universe rows")

        pit_metadata = self._pit_service.metadata(
            start_date=evaluation_spec.window_start,
            end_date=evaluation_spec.window_end,
            universe_key=QE_ST_PIT_UNIVERSE_KEY,
            ensure=False,
        )
        if pit_metadata.get("universe_rule_version") != DEFAULT_ST_PIT_RULE_VERSION:
            raise InvalidSpecError(
                "QE ST-PIT rule version differs from the authoritative platform rule",
                context={
                    "expected": DEFAULT_ST_PIT_RULE_VERSION,
                    "actual": pit_metadata.get("universe_rule_version"),
                },
            )
        date_index = pd.DatetimeIndex(dates)
        base_mask = _build_pool_mask(date_index, symbols, pool_snapshot)
        pit_mask = self._pit_service.build_eligible_mask(
            date_index,
            symbols,
            start_date=evaluation_spec.window_start,
            end_date=evaluation_spec.window_end,
            universe_key=QE_ST_PIT_UNIVERSE_KEY,
            ensure=False,
        )
        if pit_mask.shape != base_mask.shape:
            raise InvalidSpecError("QE ST-PIT mask shape does not match the source prediction universe")
        combined_mask = base_mask & pit_mask
        filtered_predictions = _apply_mask(normalized_predictions, date_index, symbols, combined_mask)
        per_day = (
            filtered_predictions.groupby("trade_date", sort=True)["symbol"]
            .nunique()
            .reindex(dates, fill_value=0)
        )
        insufficient = per_day.loc[per_day < evaluation_spec.topk]
        if not insufficient.empty:
            first_date = insufficient.index[0]
            raise InvalidSpecError(
                "source-loop eligible universe is smaller than TopK",
                context={
                    "date": first_date.isoformat(),
                    "eligible_count": int(insufficient.iloc[0]),
                    "topk": evaluation_spec.topk,
                },
            )
        eligible_pairs = filtered_predictions[["trade_date", "symbol"]].drop_duplicates()
        filtered_labels = normalized_labels.merge(
            eligible_pairs,
            on=["trade_date", "symbol"],
            how="inner",
            validate="many_to_one",
        )
        evidence = {
            "type": SOURCE_LOOP_UNIVERSE_TYPE,
            "universe_id": f"{pool_snapshot.instrument_name}:{QE_ST_PIT_UNIVERSE_KEY}",
            "universe_hash": _eligible_pair_hash(eligible_pairs),
            "symbol_count": int(filtered_predictions["symbol"].nunique()),
            "eligible_pair_count": int(len(eligible_pairs)),
            "prediction_row_count_before": int(len(normalized_predictions)),
            "prediction_row_count_after": int(len(filtered_predictions)),
            "excluded_prediction_row_count": int(len(normalized_predictions) - len(filtered_predictions)),
            "source_loop": {"task_id": contract.task_id, "loop_name": contract.loop_name},
            "stock_pool": {
                "name": pool_snapshot.instrument_name,
                "filename": pool_snapshot.filename,
                "sha256": pool_snapshot.sha256,
                "interval_count": len(pool_snapshot.intervals),
            },
            "st_pit": {
                "dataset_contract_id": QE_DATASET_CONTRACT_ID,
                "universe_key": QE_ST_PIT_UNIVERSE_KEY,
                "rule_version": pit_metadata.get("universe_rule_version"),
                "scope": pit_metadata.get("universe_scope"),
                "source_fingerprint_sha256": pit_metadata.get("universe_fingerprint_sha256"),
                "index_policy": pit_metadata.get("index_policy"),
                "coverage_semantics": pit_metadata.get("coverage_semantics"),
            },
        }
        return ResolvedEvaluationUniverse(
            predictions=filtered_predictions,
            labels=filtered_labels,
            evidence=evidence,
        )


def _parse_loop_ref(base_loop_ref: str) -> tuple[str, str, int]:
    parts = str(base_loop_ref or "").split("/")
    if len(parts) != 2 or not parts[0]:
        raise InvalidSpecError("base_loop_ref must use '<task_id>/LoopN'")
    match = _LOOP_RE.fullmatch(parts[1])
    if match is None:
        raise InvalidSpecError("base_loop_ref must use '<task_id>/LoopN'")
    return parts[0], parts[1], int(match.group("index"))


def _normalize_pairs(frame: pd.DataFrame, *, required_value: str) -> pd.DataFrame:
    required = {"trade_date", "symbol", required_value}
    if not required.issubset(frame.columns):
        raise InvalidSpecError(
            "universe input frame is missing required columns",
            context={"required": sorted(required), "actual": sorted(map(str, frame.columns))},
        )
    result = frame.copy()
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="raise").dt.date
    result["symbol"] = result["symbol"].astype(str).str.strip().str.upper()
    return result


def _window_pairs(frame: pd.DataFrame, *, start: date, end: date) -> pd.DataFrame:
    return frame.loc[
        frame["trade_date"].between(start, end, inclusive="both")
    ].reset_index(drop=True)


def _build_pool_mask(
    date_index: pd.DatetimeIndex,
    symbols: tuple[str, ...],
    snapshot: StockPoolSnapshot,
) -> np.ndarray:
    mask = np.zeros((len(date_index), len(symbols)), dtype=bool)
    column_by_symbol = {symbol: index for index, symbol in enumerate(symbols)}
    for interval in snapshot.intervals:
        column = column_by_symbol.get(interval.ts_code)
        if column is None:
            continue
        active = (date_index >= pd.Timestamp(interval.eligible_start)) & (
            date_index <= pd.Timestamp(interval.eligible_end)
        )
        if active.any():
            mask[active, column] = True
    return mask


def _apply_mask(
    frame: pd.DataFrame,
    dates: pd.DatetimeIndex,
    symbols: tuple[str, ...],
    mask: np.ndarray,
) -> pd.DataFrame:
    row_by_date = {item.date(): index for index, item in enumerate(dates)}
    column_by_symbol = {symbol: index for index, symbol in enumerate(symbols)}
    keep = np.fromiter(
        (
            mask[row_by_date[trade_date], column_by_symbol[symbol]]
            for trade_date, symbol in frame[["trade_date", "symbol"]].itertuples(index=False, name=None)
        ),
        dtype=bool,
        count=len(frame),
    )
    return frame.loc[keep].reset_index(drop=True)


def _eligible_pair_hash(pairs: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    ordered = pairs.sort_values(["trade_date", "symbol"], kind="mergesort")
    for trade_date, symbol in ordered.itertuples(index=False, name=None):
        digest.update(f"{trade_date.isoformat()}\t{symbol}\n".encode("utf-8"))
    return digest.hexdigest()
