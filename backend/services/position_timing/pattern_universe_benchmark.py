"""PT-NEXT-020 candidate-bound universe benchmark research.

The module is deliberately offline and timing-owned.  It has no API, database,
scheduler, registry/current, card, alert, order, or serving integration.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .action_value_data import BENCHMARK, DailyCandidate, file_reference
from .action_value_pipeline import _clean_repository_commit
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256
from .pattern_research import (
    BLOCK_SESSIONS,
    INITIAL_TRAINING_SESSIONS,
    REFERENCE_CAPITAL_CNY,
    TERMINAL_MAX_DEFER,
    inspect_pattern_bundle,
    mean_interval,
    replay_full_policy_symbol,
)
from .pattern_adj_factor_restatement import (
    audit_candidate_adj_factor_restatement,
    open_adj_factor_restatement_authority,
)
from .pattern_strategy import PATTERN_FEATURE_COLUMNS, pattern_feature_frame


PIPELINE_ID = "POSITION_TIMING_PATTERN_UNIVERSE_BENCHMARK_V1"
ARTIFACT_FOLDER = "pattern_universe_benchmark_v1"
REQUEST_SCHEMA = "position_timing_pattern_universe_benchmark_request_v4"
RECEIPT_SCHEMA = "position_timing_pattern_universe_benchmark_receipt_v4"
BUNDLE_SCHEMA = "position_timing_pattern_universe_benchmark_bundle_v4"
CHUNK_SCHEMA = "position_timing_pattern_universe_benchmark_chunk_v1"
MEMBERSHIP_SCHEMA = "position_timing_candidate_bound_pool_membership_v1"
RESULT_CLASS = "EXPLORATORY_CROSS_SYMBOL_EXTERNAL_VALIDITY_NOT_TEMPORAL_HOLDOUT"

EXPECTED_CANDIDATE_MANIFEST_SHA256 = (
    "7b5402c38b4b279140375fa6517595f88bdfb472e617faf8b032c04f0d33d1c1"
)
EXPECTED_CANDIDATE_DATASET_SHA256 = (
    "59b92120a4fb52fdde8a3db57337eb9af3810d28881861db7d9e3d028987407f"
)
EXPECTED_PARENT_MANIFEST_SHA256 = (
    "7481afad8bcb6bde45cfc4fbe90fc53ac14719ef048aa464d91498dce0da9541"
)
EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256 = (
    "c40f3c991ac31b570e7a739bb1898a59f12e202f2e96e9bcd8399211e5323edd"
)
POOL_IDS = (
    "stock_universe",
    "csi300",
    "csi500",
    "csi1000",
    "star50",
    "star100",
)
EXPECTED_POOL_FILES: Mapping[str, Mapping[str, Any]] = {
    "stock_universe": {
        "path": "stock_pools/stock_universe.txt",
        "sha256": "8979d78a7c3b322a3d9696f21e880dcd79c9a0bff0aa59d160f6a1f957640392",
        "size_bytes": 179817,
    },
    "csi300": {
        "path": "stock_pools/index_pool__csi300.txt",
        "sha256": "c3158b8205e6cfd1af265c756a0be81df3fc4b28e95f22e2f854991695508807",
        "size_bytes": 18912,
    },
    "csi500": {
        "path": "stock_pools/index_pool__csi500.txt",
        "sha256": "bf6f74990d55cf880c179731ab9ff8080c09a4383e6652eb06f21ab353dc24f8",
        "size_bytes": 41600,
    },
    "csi1000": {
        "path": "stock_pools/index_pool__csi1000.txt",
        "sha256": "3821b96179a4e8fd07727347dbd8b9b91ac120e0a76fc8097ab1e46dcbffae79",
        "size_bytes": 84128,
    },
    "star50": {
        "path": "stock_pools/index_pool__star50.txt",
        "sha256": "5eeba8833a5b2be3523815718d0cbae2656a316e6da11980193cfd434c58ff39",
        "size_bytes": 4064,
    },
    "star100": {
        "path": "stock_pools/index_pool__star100.txt",
        "sha256": "7906e7ea7fc5ad9caa602a6b11166454d855e5ad84c96c5cd62758618ceddc54",
        "size_bytes": 6112,
    },
}
PRIMARY_STRATEGY = "R0_P"
STRATEGY_IDS = (PRIMARY_STRATEGY,)
FAMILY_SIZE = len(POOL_IDS)
FAMILYWISE_CONFIDENCE = 1.0 - 0.05 / FAMILY_SIZE
NOMINAL_CONFIDENCE = 0.95
BOOTSTRAP_SAMPLES = 5000
INFERENCE_SEED = 20260914
DEFAULT_CHUNK_SIZE = 32
SOURCE_CODE_FILES: Mapping[str, str] = {
    "pipeline": "pattern_universe_benchmark.py",
    "pattern_research": "pattern_research.py",
    "pattern_strategy": "pattern_strategy.py",
    "adj_factor_restatement": "pattern_adj_factor_restatement.py",
    "daily_candidate": "action_value_data.py",
    "cost_and_fill": "action_value.py",
}
EXTERNAL_WRITE_RECEIPT_FLAGS: Mapping[str, bool] = {
    "registry_written": False,
    "current_written": False,
    "serving_model_artifact_written": False,
    "card_written": False,
    "alert_written": False,
    "order_written": False,
    "database_written": False,
    "database_read": False,
    "network_accessed": False,
    "live_market_read": False,
    "runtime_action_performed": False,
    "runtime_written": False,
}

QLIB_ADJUSTED_FACTOR_CONTRACT: Mapping[str, Any] = {
    "schema_version": "position_timing_qlib_adjusted_factor_contract_v1",
    "valuation_mode": "QLIB_ADJUSTED_PRICE",
    "price_transform": "ADJUSTED_OHLC=RAW_OHLC_X_FACTOR",
    "volume_transform": "ADJUSTED_VOLUME=RAW_VOLUME_DIV_FACTOR",
    "position_semantics": "VIRTUAL_ADJUSTED_UNITS_NOT_BROKER_SHARES",
    "corporate_action_accounting": "NOT_SIMULATED",
    "cash_dividend_accounting": "NOT_SIMULATED",
    "rights_subscription_accounting": "NOT_SIMULATED",
    "share_arrival_accounting": "NOT_SIMULATED",
    "execution_authority": "SIGNAL_RESEARCH_ONLY",
    "factor_restatement_gate": "REQUIRED",
}
QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256 = canonical_sha256(
    QLIB_ADJUSTED_FACTOR_CONTRACT
)

BENCHMARK_CONTRACT: Mapping[str, Any] = {
    "schema_version": "position_timing_pattern_universe_benchmark_contract_v2",
    "candidate_manifest_sha256": EXPECTED_CANDIDATE_MANIFEST_SHA256,
    "candidate_dataset_manifest_sha256": EXPECTED_CANDIDATE_DATASET_SHA256,
    "parent_manifest_sha256": EXPECTED_PARENT_MANIFEST_SHA256,
    "adj_factor_restatement_authority_canonical_sha256": (
        EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256
    ),
    "qlib_adjusted_factor_contract_sha256": QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256,
    "pool_ids": POOL_IDS,
    "effective_index_population": "INDEX_MEMBERSHIP_AND_STOCK_UNIVERSE_PIT",
    "primary_strategy": PRIMARY_STRATEGY,
    "primary_comparator": "SAME_STOCK_BUY_AND_HOLD",
    "diagnostic_strategies": (),
    "terminal": {
        "mode": "TERMINAL_LIQUIDATED",
        "max_defer_trading_days": TERMINAL_MAX_DEFER,
    },
    "main_family_size": FAMILY_SIZE,
    "economic_threshold_bps": 0.0,
    "bootstrap": {
        "method": "CIRCULAR_MOVING_BLOCK",
        "block_sessions": BLOCK_SESSIONS,
        "samples": BOOTSTRAP_SAMPLES,
        "seed": INFERENCE_SEED,
        "nominal_confidence": NOMINAL_CONFIDENCE,
        "familywise_confidence": FAMILYWISE_CONFIDENCE,
    },
    "selection": False,
    "serving": False,
}
BENCHMARK_CONTRACT_SHA256 = canonical_sha256(BENCHMARK_CONTRACT)


def _same_canonical_identity(left: Any, right: Any) -> bool:
    return canonical_json_bytes(left) == canonical_json_bytes(right)


def _without_path(reference: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in reference.items() if key != "path"}


def _assert_file_reference(reference: Mapping[str, Any], *, code: str) -> Path:
    try:
        path = Path(str(reference["path"])).resolve()
    except (KeyError, TypeError, ValueError) as exc:
        raise ActionValueError(code) from exc
    if file_reference(path) != reference:
        raise ActionValueError(code, path=path.as_posix())
    return path


@dataclass(frozen=True)
class CandidatePoolMemberships:
    candidate_root: Path
    candidate_manifest_reference: Mapping[str, Any]
    candidate_dataset_manifest_sha256: str
    intervals: Mapping[str, pd.DataFrame]
    references: Mapping[str, Mapping[str, Any]]

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(self.intervals["stock_universe"]["symbol"].unique())

    def pools_for(self, symbol: str) -> tuple[str, ...]:
        normalized = symbol.upper()
        return tuple(
            pool_id
            for pool_id in POOL_IDS
            if self.intervals[pool_id]["symbol"].eq(normalized).any()
        )

    def effective_mask(
        self,
        *,
        pool_id: str,
        symbol: str,
        calendar: pd.DatetimeIndex,
        stock_pit_mask: Sequence[bool],
    ) -> np.ndarray:
        if pool_id not in POOL_IDS or len(calendar) != len(stock_pit_mask):
            raise ActionValueError("PATTERN_POOL_MASK_SPEC_INVALID")
        active = np.zeros(len(calendar), dtype=bool)
        selected = self.intervals[pool_id]
        for row in selected.loc[selected["symbol"].eq(symbol.upper())].itertuples():
            active |= (calendar >= row.start) & (calendar <= row.end)
        return active & np.asarray(stock_pit_mask, dtype=bool)


def _validate_interval_frame(
    frame: pd.DataFrame,
    *,
    pool_id: str,
    calendar: pd.DatetimeIndex,
) -> pd.DataFrame:
    if frame.empty or list(frame.columns) != ["symbol", "start", "end"]:
        raise ActionValueError("PATTERN_POOL_SIDECAR_SCHEMA_INVALID", pool_id=pool_id)
    result = frame.copy()
    result["symbol"] = result["symbol"].astype(str).str.upper()
    if not result["symbol"].str.fullmatch(r"\d{6}\.(SH|SZ)").all():
        raise ActionValueError("PATTERN_POOL_SYMBOL_INVALID", pool_id=pool_id)
    try:
        result["start"] = pd.to_datetime(result["start"], format="%Y-%m-%d")
        result["end"] = pd.to_datetime(result["end"], format="%Y-%m-%d")
    except (TypeError, ValueError) as exc:
        raise ActionValueError("PATTERN_POOL_DATE_INVALID", pool_id=pool_id) from exc
    ordered = result.sort_values(["symbol", "start", "end"], kind="stable").reset_index(drop=True)
    if (
        result.isna().any(axis=None)
        or (result["start"] > result["end"]).any()
        or not result.reset_index(drop=True).equals(ordered)
        or result["start"].min() < calendar[0]
        or result["end"].max() > calendar[-1]
    ):
        raise ActionValueError("PATTERN_POOL_INTERVAL_INVALID", pool_id=pool_id)
    for symbol, group in result.groupby("symbol", sort=False):
        starts = group["start"].to_numpy()
        ends = group["end"].to_numpy()
        if len(group) > 1 and any(starts[1:] <= ends[:-1]):
            raise ActionValueError(
                "PATTERN_POOL_INTERVAL_OVERLAP",
                pool_id=pool_id,
                symbol=symbol,
            )
    return result


def open_candidate_pool_memberships(candidate: DailyCandidate) -> CandidatePoolMemberships:
    root = candidate.root.resolve()
    manifest_path = root / "qe_dataset_manifest.json"
    manifest_reference = file_reference(manifest_path)
    if manifest_reference["sha256"] != EXPECTED_CANDIDATE_MANIFEST_SHA256:
        raise ActionValueError("PATTERN_BENCHMARK_CANDIDATE_IDENTITY_MISMATCH")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("PATTERN_BENCHMARK_CANDIDATE_MANIFEST_INVALID") from exc
    manifest_identity = {
        key: value for key, value in manifest.items() if key != "dataset_manifest_sha256"
    }
    declared_dataset_sha256 = str(manifest.get("dataset_manifest_sha256") or "")
    sidecars = ((manifest.get("st_pit_manifest") or {}).get("index_membership_sidecars") or {})
    if (
        manifest.get("availability_status") != "CANDIDATE_READY"
        or declared_dataset_sha256 != EXPECTED_CANDIDATE_DATASET_SHA256
        or declared_dataset_sha256 != canonical_sha256(manifest_identity)
        or set(sidecars) != set(POOL_IDS)
    ):
        raise ActionValueError("PATTERN_BENCHMARK_CANDIDATE_CONTRACT_MISMATCH")

    intervals: dict[str, pd.DataFrame] = {}
    references: dict[str, Mapping[str, Any]] = {}
    for pool_id in POOL_IDS:
        expected = EXPECTED_POOL_FILES[pool_id]
        declared = sidecars[pool_id]
        if (
            declared.get("path") != expected["path"]
            or declared.get("sha256") != expected["sha256"]
            or int(declared.get("size", -1)) != expected["size_bytes"]
        ):
            raise ActionValueError(
                "PATTERN_POOL_MANIFEST_IDENTITY_MISMATCH", pool_id=pool_id
            )
        path = (root / expected["path"]).resolve()
        if not path.is_relative_to(root):
            raise ActionValueError("PATTERN_POOL_PATH_OUTSIDE_CANDIDATE", pool_id=pool_id)
        reference = file_reference(path)
        if _without_path(reference) != {
            "sha256": expected["sha256"],
            "size_bytes": expected["size_bytes"],
        }:
            raise ActionValueError("PATTERN_POOL_FILE_IDENTITY_MISMATCH", pool_id=pool_id)
        frame = pd.read_csv(
            path,
            sep="\t",
            names=["symbol", "start", "end"],
            dtype=str,
            keep_default_na=False,
        )
        intervals[pool_id] = _validate_interval_frame(
            frame, pool_id=pool_id, calendar=candidate.calendar
        )
        references[pool_id] = reference
        if file_reference(path) != reference:
            raise ActionValueError("PATTERN_POOL_CHANGED_WHILE_READING", pool_id=pool_id)

    candidate_intervals = candidate.spans.loc[:, ["symbol", "start", "end"]].copy()
    candidate_intervals = candidate_intervals.sort_values(
        ["symbol", "start", "end"], kind="stable"
    ).reset_index(drop=True)
    if not intervals["stock_universe"].equals(candidate_intervals):
        raise ActionValueError("PATTERN_POOL_STOCK_UNIVERSE_DRIFT")
    return CandidatePoolMemberships(
        root,
        manifest_reference,
        declared_dataset_sha256,
        intervals,
        references,
    )


@dataclass(frozen=True)
class FrozenParentEvidence:
    bundle: Path
    manifest_reference: Mapping[str, Any]
    request_sha256: str


@dataclass(frozen=True)
class _NoAccountActions:
    """Compatibility view proving this signal replay has no account actions."""

    actions: tuple[()] = ()

    @staticmethod
    def on(_symbol: str, _trade_date: date) -> None:
        return None

    @staticmethod
    def between(
        _symbol: str, _start_exclusive: date, _end_inclusive: date
    ) -> tuple[()]:
        return ()


NO_ACCOUNT_ACTIONS = _NoAccountActions()


def load_frozen_parent_evidence(bundle: Path) -> FrozenParentEvidence:
    root = bundle.resolve()
    inspected = inspect_pattern_bundle(root)
    manifest = inspected["manifest"]
    if manifest.get("manifest_sha256") != EXPECTED_PARENT_MANIFEST_SHA256:
        raise ActionValueError("PATTERN_BENCHMARK_PARENT_IDENTITY_MISMATCH")
    manifest_reference = file_reference(root / "manifest.json")
    return FrozenParentEvidence(
        bundle=root,
        manifest_reference=manifest_reference,
        request_sha256=inspected["request"]["request_sha256"],
    )


def _source_code_references(repository_root: Path) -> Mapping[str, Mapping[str, Any]]:
    source_root = repository_root / "backend" / "services" / "position_timing"
    return {
        role: file_reference((source_root / filename).resolve())
        for role, filename in SOURCE_CODE_FILES.items()
    }


def _publish_source_diagnostic(
    *, timing_root: Path, payload: Mapping[str, Any]
) -> Path:
    diagnostic = dict(payload)
    diagnostic["diagnostic_sha256"] = canonical_sha256(diagnostic)
    path = (
        timing_root.resolve()
        / "research"
        / ARTIFACT_FOLDER
        / "source_diagnostics"
        / f"{diagnostic['diagnostic_sha256']}.json"
    )
    PositionTimingArtifactStore._publish_immutable(
        path, canonical_json_bytes(diagnostic)
    )
    return path


def _qlib_adjusted_bars(bars: pd.DataFrame, *, symbol: str) -> pd.DataFrame:
    """Return Qlib-style adjusted units without account-level actions.

    ``DailyCandidate`` exposes raw OHLC/volume plus the frozen adjustment
    factor.  Signal research consumes the equivalent Qlib representation:
    prices are multiplied by the factor and volume is divided by it.  The
    resulting frame has a unit factor so downstream replay cannot apply the
    source adjustment twice.
    """

    required = {"open", "high", "low", "close", "volume", "factor"}
    if not required.issubset(bars):
        raise ActionValueError("PATTERN_QLIB_ADJUSTED_SOURCE_SCHEMA_INVALID", symbol=symbol)
    result = bars.copy()
    factor = pd.to_numeric(result["factor"], errors="coerce")
    numeric = result.loc[:, ["open", "high", "low", "close", "volume"]].apply(
        pd.to_numeric, errors="coerce"
    )
    present = numeric[["open", "high", "low", "close"]].notna().any(axis=1)
    invalid_factor = present & (~np.isfinite(factor) | factor.le(0))
    if invalid_factor.any():
        raise ActionValueError(
            "PATTERN_QLIB_ADJUSTED_FACTOR_INVALID",
            symbol=symbol,
            invalid_row_count=int(invalid_factor.sum()),
        )
    result.loc[:, ["open", "high", "low", "close"]] = numeric[
        ["open", "high", "low", "close"]
    ].mul(factor, axis=0)
    result["volume"] = numeric["volume"].div(factor)
    for field in ("up_limit", "down_limit"):
        if field in result:
            result[field] = pd.to_numeric(result[field], errors="coerce").mul(
                factor
            )
    result["source_adjustment_factor"] = factor
    result["factor"] = 1.0
    return result


def _audit_qlib_adjusted_factor_integrity(
    candidate: DailyCandidate,
    *,
    symbols: Sequence[str],
    start: date,
    end: date,
    candidate_source_sha256: str,
) -> Mapping[str, Any]:
    """Validate every in-scope factor series without inferring account events."""

    normalized = tuple(sorted({str(symbol).upper() for symbol in symbols}))
    if not normalized or start > end or len(candidate_source_sha256) != 64:
        raise ActionValueError("PATTERN_QLIB_ADJUSTED_FACTOR_SCOPE_INVALID")
    invalid_symbols: list[Mapping[str, Any]] = []
    insufficient_symbols: list[str] = []
    material_change_count = 0
    observed_row_count = 0
    series_digest = hashlib.sha256()
    for symbol in normalized:
        bars = candidate.bars(symbol)
        in_scope = (
            bars["pit_active"].astype(bool)
            & (bars.index.date >= start)
            & (bars.index.date <= end)
        )
        numeric = bars.loc[:, ["open", "high", "low", "close", "volume", "factor"]].apply(
            pd.to_numeric, errors="coerce"
        )
        present = in_scope & numeric[["open", "high", "low", "close"]].notna().any(axis=1)
        factor = numeric["factor"]
        invalid = present & (~np.isfinite(factor) | factor.le(0))
        valid = factor.where(present & np.isfinite(factor) & factor.gt(0)).dropna()
        adjusted_prices = numeric[["open", "high", "low", "close"]].mul(
            factor, axis=0
        )
        invalid_adjusted = present & (
            ~np.isfinite(adjusted_prices).all(axis=1)
            | adjusted_prices.le(0).any(axis=1)
        )
        invalid_volume = present & (
            ~np.isfinite(numeric["volume"]) | numeric["volume"].lt(0)
        )
        invalid_count = int((invalid | invalid_adjusted | invalid_volume).sum())
        if invalid_count:
            invalid_symbols.append(
                {"symbol": symbol, "invalid_row_count": invalid_count}
            )
        if len(valid) < 2:
            insufficient_symbols.append(symbol)
        ratios = valid.div(valid.shift(1)).dropna()
        material_change_count += int(
            ((ratios - 1.0).abs() * 10000.0 > 10.0).sum()
        )
        observed_row_count += len(valid)
        symbol_digest = hashlib.sha256()
        for stamp, value in valid.items():
            symbol_digest.update(
                f"{pd.Timestamp(stamp).date().isoformat()}|{float(value).hex()}\n".encode(
                    "ascii"
                )
            )
        series_digest.update(
            f"{symbol}|{len(valid)}|{symbol_digest.hexdigest()}\n".encode("ascii")
        )
    identity: dict[str, Any] = {
        "schema_version": "position_timing_qlib_adjusted_factor_integrity_audit_v1",
        "contract_sha256": QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256,
        "candidate_source_sha256": candidate_source_sha256,
        "scope": {
            "symbols_sha256": canonical_sha256(normalized),
            "symbol_count": len(normalized),
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
        "observed_factor_row_count": observed_row_count,
        "material_factor_change_count": material_change_count,
        "factor_series_sha256": series_digest.hexdigest(),
        "invalid_factor_symbol_count": len(invalid_symbols),
        "invalid_factor_symbols": invalid_symbols,
        "insufficient_factor_symbol_count": len(insufficient_symbols),
        "insufficient_factor_symbols": insufficient_symbols,
        "coverage_complete": not invalid_symbols and not insufficient_symbols,
        "corporate_action_authority_read": False,
        "account_economics_simulated": False,
        "broker_account_clearing": False,
        "outcomes_read": False,
        "database_read": False,
        "database_write": False,
        "network_accessed": False,
        "runtime_action_performed": False,
    }
    return {**identity, "audit_sha256": canonical_sha256(identity)}


def _first_evaluable_ordinal(
    *,
    features: pd.DataFrame,
    membership_mask: np.ndarray,
    minimum_ordinal: int,
    terminal_ordinal: int,
) -> int | None:
    required = tuple(PATTERN_FEATURE_COLUMNS)
    ready = features.loc[:, required].notna().all(axis=1).to_numpy(bool)
    eligible = ready & membership_mask
    candidates = np.flatnonzero(eligible)
    candidates = candidates[
        (candidates >= minimum_ordinal) & (candidates < terminal_ordinal)
    ]
    return int(candidates[0]) if len(candidates) else None


def _path_daily_partials(
    rows: Sequence[Mapping[str, Any]],
    *,
    pool_id: str,
    strategy_id: str,
    terminal_status: str,
) -> tuple[pd.DataFrame, Mapping[str, Any]]:
    if terminal_status not in {
        "TERMINAL_LIQUIDATED",
        "TERMINAL_LIQUIDATION_NOT_REQUIRED",
    }:
        raise ActionValueError("PATTERN_BENCHMARK_TERMINAL_STATUS_INVALID")
    frame = pd.DataFrame(
        item for item in rows if item.get("comparison") == "P_MINUS_BUY_AND_HOLD"
    )
    if frame.empty:
        raise ActionValueError(
            "PATTERN_BENCHMARK_PATH_EMPTY",
            pool_id=pool_id,
            strategy_id=strategy_id,
        )
    frame = frame.sort_values("valuation_date", kind="stable").reset_index(drop=True)
    if frame["valuation_date"].duplicated().any():
        raise ActionValueError("PATTERN_BENCHMARK_PATH_DATE_DUPLICATE")
    prior_policy = frame["policy_wealth_cny"].shift(1, fill_value=float(REFERENCE_CAPITAL_CNY))
    prior_baseline = frame["baseline_wealth_cny"].shift(
        1, fill_value=float(REFERENCE_CAPITAL_CNY)
    )
    policy_change = (
        (frame["policy_wealth_cny"] - prior_policy)
        / float(REFERENCE_CAPITAL_CNY)
        * 10000.0
    )
    baseline_change = (
        (frame["baseline_wealth_cny"] - prior_baseline)
        / float(REFERENCE_CAPITAL_CNY)
        * 10000.0
    )
    observed_increment = frame["incremental_net_value_bps"].to_numpy(float)
    if not np.allclose(
        policy_change.to_numpy(float) - baseline_change.to_numpy(float),
        observed_increment,
        rtol=0,
        atol=1e-7,
    ):
        raise ActionValueError("PATTERN_BENCHMARK_INCREMENT_IDENTITY_MISMATCH")
    exposed = frame["policy_exposure"].to_numpy(float)
    missed_upside = np.where(
        (exposed < 0.01) & (baseline_change.to_numpy(float) > 0),
        baseline_change.to_numpy(float),
        0.0,
    )
    partial = pd.DataFrame(
        {
            "pool_id": pool_id,
            "strategy_id": strategy_id,
            "valuation_date": pd.to_datetime(frame["valuation_date"]),
            "active_sleeve_count": 1,
            "policy_change_bps_sum": policy_change.to_numpy(float),
            "buy_hold_change_bps_sum": baseline_change.to_numpy(float),
            "timing_increment_bps_sum": observed_increment,
            "policy_exposure_sum": exposed,
            "policy_cash_fraction_sum": 1.0 - exposed,
            "missed_upside_bps_sum": missed_upside,
            "post_exit_upside_bps_sum": np.where(
                (frame["policy_quantity"].to_numpy(int) == 0)
                & (frame["baseline_quantity"].to_numpy(int) > 0)
                & (baseline_change.to_numpy(float) > 0),
                baseline_change.to_numpy(float),
                0.0,
            ),
        }
    )
    policy_wealth = frame["policy_wealth_cny"].to_numpy(float)
    buy_hold_wealth = frame["baseline_wealth_cny"].to_numpy(float)
    if not np.isfinite(partial.select_dtypes(include=[np.number])).all(axis=None):
        raise ActionValueError("PATTERN_BENCHMARK_PATH_NON_FINITE")
    summary = {
        "pool_id": pool_id,
        "strategy_id": strategy_id,
        "symbol": str(frame.iloc[0]["symbol"]),
        "evaluation_start": pd.Timestamp(frame.iloc[0]["valuation_date"]).date().isoformat(),
        "evaluation_end": pd.Timestamp(frame.iloc[-1]["valuation_date"]).date().isoformat(),
        "session_count": len(frame),
        "terminal_status": terminal_status,
        "policy_terminal_wealth_cny": float(policy_wealth[-1]),
        "buy_hold_terminal_wealth_cny": float(buy_hold_wealth[-1]),
        "policy_total_return_bps": float(
            (policy_wealth[-1] / float(REFERENCE_CAPITAL_CNY) - 1.0) * 10000.0
        ),
        "buy_hold_total_return_bps": float(
            (buy_hold_wealth[-1] / float(REFERENCE_CAPITAL_CNY) - 1.0) * 10000.0
        ),
        "timing_increment_total_bps": float(
            (policy_wealth[-1] - buy_hold_wealth[-1])
            / float(REFERENCE_CAPITAL_CNY)
            * 10000.0
        ),
        "policy_max_drawdown_bps": _max_drawdown_bps(
            policy_wealth, initial_value=float(REFERENCE_CAPITAL_CNY)
        ),
        "buy_hold_max_drawdown_bps": _max_drawdown_bps(
            buy_hold_wealth, initial_value=float(REFERENCE_CAPITAL_CNY)
        ),
    }
    return partial, summary


def _terminal_status_from_counts(counts: Mapping[str, int]) -> str:
    liquidated = int(counts.get("TERMINAL_LIQUIDATED", 0))
    not_required = int(counts.get("TERMINAL_LIQUIDATION_NOT_REQUIRED", 0))
    if liquidated == 1 and not_required == 0:
        return "TERMINAL_LIQUIDATED"
    if liquidated == 0 and not_required == 1:
        return "TERMINAL_LIQUIDATION_NOT_REQUIRED"
    raise ActionValueError("PATTERN_BENCHMARK_TERMINAL_STATUS_INVALID")


def _max_drawdown_bps(values: np.ndarray, *, initial_value: float) -> float:
    values = np.asarray(values, dtype=float)
    if (
        values.ndim != 1
        or not len(values)
        or not np.isfinite(values).all()
        or not np.isfinite(initial_value)
        or initial_value <= 0
    ):
        raise ActionValueError("PATTERN_BENCHMARK_DRAWDOWN_INPUT_INVALID")
    values = np.concatenate(([float(initial_value)], values))
    peaks = np.maximum.accumulate(values)
    return float(np.min(values / peaks - 1.0) * 10000.0)


def _run_strategy_path(
    *,
    strategy_id: str,
    symbol: str,
    pool_bars: pd.DataFrame,
    pattern_features: pd.DataFrame,
    calendar_dates: Sequence[date],
    corporate_actions: _NoAccountActions,
    start_ordinal: int,
    terminal_ordinal: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Counter[str]]:
    if strategy_id != PRIMARY_STRATEGY:
        raise ActionValueError(
            "PATTERN_BENCHMARK_STRATEGY_INVALID", strategy_id=strategy_id
        )
    return replay_full_policy_symbol(
        symbol=symbol,
        bars=pool_bars,
        features=pattern_features,
        calendar_dates=calendar_dates,
        corporate_actions=corporate_actions,
        start_ordinal=start_ordinal,
        terminal_ordinal=terminal_ordinal,
        template_id="R0",
        terminal_liquidation_enabled=True,
    )


def _chunk_paths(chunk_root: Path) -> Mapping[str, Path]:
    return {
        "daily": chunk_root / "daily_partial.parquet",
        "fills": chunk_root / "fills.parquet",
        "symbols": chunk_root / "symbol_summary.parquet",
        "diagnostics": chunk_root / "diagnostics.json",
        "manifest": chunk_root / "manifest.json",
    }


def _chunk_manifest(root: Path, *, chunk_identity: Mapping[str, Any]) -> Mapping[str, Any]:
    files = {
        path.relative_to(root).as_posix(): _without_path(file_reference(path))
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    }
    identity = {
        "schema_version": CHUNK_SCHEMA,
        "chunk_identity": chunk_identity,
        "files": files,
    }
    return {**identity, "manifest_sha256": canonical_sha256(identity)}


def inspect_chunk(chunk_root: Path, *, expected_identity: Mapping[str, Any]) -> Mapping[str, Any]:
    root = chunk_root.resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("PATTERN_BENCHMARK_CHUNK_UNAVAILABLE") from exc
    identity = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    actual_files = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    }
    expected_files = set(manifest.get("files") or {}) | {"manifest.json"}
    if (
        manifest.get("schema_version") != CHUNK_SCHEMA
        or not _same_canonical_identity(manifest.get("chunk_identity"), expected_identity)
        or manifest.get("manifest_sha256") != canonical_sha256(identity)
        or actual_files != expected_files
    ):
        raise ActionValueError("PATTERN_BENCHMARK_CHUNK_IDENTITY_MISMATCH")
    for name, reference in manifest["files"].items():
        if _without_path(file_reference(root / name)) != reference:
            raise ActionValueError(
                "PATTERN_BENCHMARK_CHUNK_FILE_IDENTITY_MISMATCH", file=name
            )
    return manifest


def _publish_chunk(
    *,
    target: Path,
    chunk_identity: Mapping[str, Any],
    daily: pd.DataFrame,
    fills: pd.DataFrame,
    summaries: pd.DataFrame,
    diagnostics: Mapping[str, Any],
) -> Mapping[str, Any]:
    target = target.resolve()
    if target.exists():
        return inspect_chunk(target, expected_identity=chunk_identity)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        paths = _chunk_paths(staging)
        daily.to_parquet(paths["daily"], index=False)
        fills.to_parquet(paths["fills"], index=False)
        summaries.to_parquet(paths["symbols"], index=False)
        paths["diagnostics"].write_bytes(canonical_json_bytes(diagnostics))
        paths["manifest"].write_bytes(
            canonical_json_bytes(
                _chunk_manifest(staging, chunk_identity=chunk_identity)
            )
        )
        with _exclusive_file_lock(target.parent / f".{target.name}.lock"):
            if target.exists():
                shutil.rmtree(staging)
                return inspect_chunk(target, expected_identity=chunk_identity)
            os.replace(staging, target)
        return inspect_chunk(target, expected_identity=chunk_identity)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _run_chunk(
    *,
    request: Mapping[str, Any],
    chunk_ordinal: int,
    symbols: Sequence[str],
    candidate: DailyCandidate,
    memberships: CandidatePoolMemberships,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Mapping[str, Any]]:
    # The replay engine retains a read-only empty-book compatibility input,
    # but this research pipeline has no authority or code path for applying
    # account-level corporate actions.
    no_account_actions = NO_ACCOUNT_ACTIONS
    calendar_dates = tuple(item.date() for item in candidate.calendar)
    minimum_ordinal = INITIAL_TRAINING_SESSIONS
    terminal_ordinal = len(calendar_dates) - 1 - TERMINAL_MAX_DEFER
    daily_parts: list[pd.DataFrame] = []
    fill_parts: list[pd.DataFrame] = []
    summaries: list[Mapping[str, Any]] = []
    diagnostic_rows: list[Mapping[str, Any]] = []
    source_references: dict[str, Mapping[str, Any]] = {}

    for symbol in symbols:
        try:
            before_refs = set(candidate.references)
            bars = _qlib_adjusted_bars(candidate.bars(symbol), symbol=symbol)
            source_references.update(
                {
                    key: value
                    for key, value in candidate.references.items()
                    if key not in before_refs
                }
            )
            features = pattern_feature_frame(
                bars, symbol=symbol, corporate_actions=no_account_actions
            )
            for pool_id in memberships.pools_for(symbol):
                membership_mask = memberships.effective_mask(
                    pool_id=pool_id,
                    symbol=symbol,
                    calendar=candidate.calendar,
                    stock_pit_mask=bars["pit_active"].to_numpy(bool),
                )
                start_ordinal = _first_evaluable_ordinal(
                    features=features,
                    membership_mask=membership_mask,
                    minimum_ordinal=minimum_ordinal,
                    terminal_ordinal=terminal_ordinal,
                )
                if start_ordinal is None:
                    diagnostic_rows.append(
                        {
                            "symbol": symbol,
                            "pool_id": pool_id,
                            "strategy_id": None,
                            "status": "NOT_EVALUABLE",
                            "reason_code": "NO_PIT_MEMBER_PATTERN_READY_SESSION",
                        }
                    )
                    continue
                pool_bars = bars.copy()
                pool_bars["pit_active"] = membership_mask
                for strategy_id in STRATEGY_IDS:
                    strategy_start = start_ordinal
                    if strategy_start >= terminal_ordinal:
                        diagnostic_rows.append(
                            {
                                "symbol": symbol,
                                "pool_id": pool_id,
                                "strategy_id": strategy_id,
                                "status": "NOT_EVALUABLE",
                                "reason_code": "STRATEGY_CLOCK_AFTER_TERMINAL",
                            }
                        )
                        continue
                    try:
                        rows, fills, counts = _run_strategy_path(
                            strategy_id=strategy_id,
                            symbol=symbol,
                            pool_bars=pool_bars,
                            pattern_features=features,
                            calendar_dates=calendar_dates,
                            corporate_actions=no_account_actions,
                            start_ordinal=strategy_start,
                            terminal_ordinal=terminal_ordinal,
                        )
                        partial, summary = _path_daily_partials(
                            rows,
                            pool_id=pool_id,
                            strategy_id=strategy_id,
                            terminal_status=_terminal_status_from_counts(counts),
                        )
                        daily_parts.append(partial)
                        summaries.append(summary)
                        fill_frame = pd.DataFrame(fills)
                        if not fill_frame.empty:
                            fill_frame.insert(0, "strategy_id", strategy_id)
                            fill_frame.insert(0, "pool_id", pool_id)
                            fill_parts.append(fill_frame)
                        diagnostic_rows.append(
                            {
                                "symbol": symbol,
                                "pool_id": pool_id,
                                "strategy_id": strategy_id,
                                "status": "COMPLETED",
                                "reason_code": None,
                                "replay_counts": dict(sorted(counts.items())),
                            }
                        )
                    except ActionValueError as exc:
                        diagnostic_rows.append(
                            {
                                "symbol": symbol,
                                "pool_id": pool_id,
                                "strategy_id": strategy_id,
                                "status": "EXCLUDED",
                                "reason_code": exc.code,
                                "details": exc.details,
                            }
                        )
        except ActionValueError as exc:
            diagnostic_rows.append(
                {
                    "symbol": symbol,
                    "pool_id": None,
                    "strategy_id": None,
                    "status": "SOURCE_EXCLUDED",
                    "reason_code": exc.code,
                    "details": exc.details,
                }
            )

    daily = (
        pd.concat(daily_parts, ignore_index=True)
        .groupby(["pool_id", "strategy_id", "valuation_date"], as_index=False)
        .sum(numeric_only=True)
        if daily_parts
        else pd.DataFrame(
            columns=(
                "pool_id",
                "strategy_id",
                "valuation_date",
                "active_sleeve_count",
                "policy_change_bps_sum",
                "buy_hold_change_bps_sum",
                "timing_increment_bps_sum",
                "policy_exposure_sum",
                "policy_cash_fraction_sum",
                "missed_upside_bps_sum",
                "post_exit_upside_bps_sum",
            )
        )
    )
    fills = (
        pd.concat(fill_parts, ignore_index=True)
        if fill_parts
        else pd.DataFrame(
            columns=(
                "pool_id",
                "strategy_id",
                "symbol",
                "path_role",
                "anchor_date",
                "decision_date",
                "decision_as_of",
                "feature_available_at",
                "target_date",
                "planned_delta_qty",
                "fill_status",
                "fill_reason",
                "fill_delta_qty",
                "fill_price_raw",
                "fill_fee_cny",
            )
        )
    )
    summary_frame = (
        pd.DataFrame(summaries)
        if summaries
        else pd.DataFrame(
            columns=(
                "pool_id",
                "strategy_id",
                "symbol",
                "evaluation_start",
                "evaluation_end",
                "session_count",
                "terminal_status",
                "policy_terminal_wealth_cny",
                "buy_hold_terminal_wealth_cny",
                "policy_total_return_bps",
                "buy_hold_total_return_bps",
                "timing_increment_total_bps",
                "policy_max_drawdown_bps",
                "buy_hold_max_drawdown_bps",
            )
        )
    )
    diagnostics = {
        "schema_version": "position_timing_pattern_universe_chunk_diagnostics_v1",
        "request_sha256": request["request_sha256"],
        "chunk_ordinal": chunk_ordinal,
        "symbols": tuple(symbols),
        "source_references": source_references,
        "source_references_sha256": canonical_sha256(source_references),
        "rows": diagnostic_rows,
        "status_counts": dict(
            sorted(Counter(item["status"] for item in diagnostic_rows).items())
        ),
    }
    diagnostics["diagnostics_sha256"] = canonical_sha256(diagnostics)
    return daily, fills, summary_frame, diagnostics


def _effect_evidence(
    inference: Mapping[str, Any], *, coverage_complete: bool
) -> str:
    lower = inference.get("lower_bps")
    upper = inference.get("upper_bps")
    if not coverage_complete or lower is None or upper is None:
        return "INCONCLUSIVE"
    if float(lower) > 0:
        return "SUPPORTED"
    if float(upper) < 0:
        return "NEGATIVE"
    return "INCONCLUSIVE"


def _annualized_return(total_return_bps: float, sessions: int) -> float | None:
    wealth_ratio = 1.0 + total_return_bps / 10000.0
    if sessions <= 0 or wealth_ratio <= 0:
        return None
    return float((wealth_ratio ** (252.0 / sessions) - 1.0) * 10000.0)


def _aggregate_results(
    *,
    request: Mapping[str, Any],
    chunk_roots: Sequence[Path],
    benchmark_close: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Mapping[str, Any]]:
    daily_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    symbol_frames: list[pd.DataFrame] = []
    diagnostic_rows: list[Mapping[str, Any]] = []
    for root in chunk_roots:
        paths = _chunk_paths(root)
        daily_frames.append(pd.read_parquet(paths["daily"]))
        fill_frames.append(pd.read_parquet(paths["fills"]))
        symbol_frames.append(pd.read_parquet(paths["symbols"]))
        payload = json.loads(paths["diagnostics"].read_text(encoding="utf-8"))
        identity = {
            key: value for key, value in payload.items() if key != "diagnostics_sha256"
        }
        if payload.get("diagnostics_sha256") != canonical_sha256(identity):
            raise ActionValueError("PATTERN_BENCHMARK_CHUNK_DIAGNOSTICS_INVALID")
        diagnostic_rows.extend(payload["rows"])
    daily = pd.concat(daily_frames, ignore_index=True)
    if daily.empty:
        raise ActionValueError("PATTERN_BENCHMARK_NO_DAILY_RESULTS")
    daily = (
        daily.groupby(["pool_id", "strategy_id", "valuation_date"], as_index=False)
        .sum(numeric_only=True)
        .sort_values(["pool_id", "strategy_id", "valuation_date"], kind="stable")
        .reset_index(drop=True)
    )
    for column in (
        "policy_change_bps",
        "buy_hold_change_bps",
        "timing_increment_bps",
        "policy_exposure",
        "policy_cash_fraction",
        "missed_upside_bps",
        "post_exit_upside_bps",
    ):
        source = {
            "policy_change_bps": "policy_change_bps_sum",
            "buy_hold_change_bps": "buy_hold_change_bps_sum",
            "timing_increment_bps": "timing_increment_bps_sum",
            "policy_exposure": "policy_exposure_sum",
            "policy_cash_fraction": "policy_cash_fraction_sum",
            "missed_upside_bps": "missed_upside_bps_sum",
            "post_exit_upside_bps": "post_exit_upside_bps_sum",
        }[column]
        daily[column] = daily[source] / daily["active_sleeve_count"]

    fills = pd.concat(fill_frames, ignore_index=True)
    symbols = pd.concat(symbol_frames, ignore_index=True)
    diagnostic_frame = pd.DataFrame(diagnostic_rows)
    source_failures = (
        int(diagnostic_frame["status"].eq("SOURCE_EXCLUDED").sum())
        if not diagnostic_frame.empty
        else 0
    )
    summaries: list[Mapping[str, Any]] = []
    comparisons: dict[str, Mapping[str, Any]] = {}
    for pool_offset, pool_id in enumerate(POOL_IDS):
        for strategy_id in STRATEGY_IDS:
            subset = daily.loc[
                daily["pool_id"].eq(pool_id)
                & daily["strategy_id"].eq(strategy_id)
            ].copy()
            completed = diagnostic_frame.loc[
                diagnostic_frame["pool_id"].eq(pool_id)
                & diagnostic_frame["strategy_id"].eq(strategy_id)
                & diagnostic_frame["status"].eq("COMPLETED")
            ]
            excluded = diagnostic_frame.loc[
                diagnostic_frame["pool_id"].eq(pool_id)
                & diagnostic_frame["strategy_id"].eq(strategy_id)
                & diagnostic_frame["status"].eq("EXCLUDED")
            ]
            not_evaluable = diagnostic_frame.loc[
                diagnostic_frame["pool_id"].eq(pool_id)
                & diagnostic_frame["strategy_id"].isna()
                & diagnostic_frame["status"].eq("NOT_EVALUABLE")
            ]
            if subset.empty:
                summaries.append(
                    {
                        "pool_id": pool_id,
                        "strategy_id": strategy_id,
                        "status": "NO_EVALUABLE_PATHS",
                        "completed_symbol_count": len(completed),
                        "excluded_symbol_count": len(excluded),
                        "not_evaluable_symbol_count": len(not_evaluable),
                    }
                )
                continue
            coverage_complete = not len(excluded) and not source_failures
            values = subset["timing_increment_bps"].to_numpy(float)
            familywise = mean_interval(
                values,
                samples=BOOTSTRAP_SAMPLES,
                seed=INFERENCE_SEED + pool_offset,
                confidence_level=(
                    FAMILYWISE_CONFIDENCE
                    if strategy_id == PRIMARY_STRATEGY
                    else NOMINAL_CONFIDENCE
                ),
            )
            nominal = mean_interval(
                values,
                samples=BOOTSTRAP_SAMPLES,
                seed=INFERENCE_SEED + pool_offset,
                confidence_level=NOMINAL_CONFIDENCE,
            )
            policy_total = float(subset["policy_change_bps"].sum())
            buy_hold_total = float(subset["buy_hold_change_bps"].sum())
            policy_curve = 1.0 + subset["policy_change_bps"].cumsum().to_numpy(float) / 10000.0
            buy_hold_curve = 1.0 + subset["buy_hold_change_bps"].cumsum().to_numpy(float) / 10000.0
            start_day = pd.Timestamp(subset.iloc[0]["valuation_date"])
            end_day = pd.Timestamp(subset.iloc[-1]["valuation_date"])
            context = benchmark_close.loc[
                (benchmark_close.index >= start_day)
                & (benchmark_close.index <= end_day)
            ].dropna()
            context_return = (
                float((context.iloc[-1] / context.iloc[0] - 1.0) * 10000.0)
                if len(context) >= 2
                else None
            )
            filled = fills.loc[
                fills["pool_id"].eq(pool_id)
                & fills["strategy_id"].eq(strategy_id)
                & fills["fill_status"].eq("FILLED")
                & fills["path_role"].isin(
                    (
                        "FULL_POLICY",
                        "FULL_POLICY_TERMINAL",
                        "FULL_BUY_AND_HOLD",
                        "FULL_BUY_AND_HOLD_TERMINAL",
                    )
                )
            ].copy()
            if not filled.empty:
                filled["notional_cny"] = (
                    filled["fill_delta_qty"].abs()
                    * filled["fill_price_raw"]
                )
            policy_fills = filled.loc[
                filled["path_role"].str.startswith("FULL_POLICY", na=False)
            ]
            bh_fills = filled.loc[
                filled["path_role"].str.startswith("FULL_BUY_AND_HOLD", na=False)
            ]
            daily_std = float(subset["policy_change_bps"].std(ddof=1))
            summary = {
                "pool_id": pool_id,
                "strategy_id": strategy_id,
                "status": "COMPLETE" if coverage_complete else "COVERAGE_INCOMPLETE",
                "evidence_role": (
                    "PRIMARY_FAMILY" if strategy_id == PRIMARY_STRATEGY else "DIAGNOSTIC_ONLY"
                ),
                "evaluation_start": start_day.date().isoformat(),
                "evaluation_end": end_day.date().isoformat(),
                "evaluation_session_count": len(subset),
                "completed_symbol_count": len(completed),
                "excluded_symbol_count": len(excluded),
                "not_evaluable_symbol_count": len(not_evaluable),
                "coverage_complete": coverage_complete,
                "policy_total_return_bps": policy_total,
                "buy_hold_total_return_bps": buy_hold_total,
                "timing_increment_total_bps": policy_total - buy_hold_total,
                "policy_annualized_return_bps": _annualized_return(policy_total, len(subset)),
                "buy_hold_annualized_return_bps": _annualized_return(buy_hold_total, len(subset)),
                "csi300_price_index_context_return_bps": context_return,
                "market_context_semantics": "PRICE_INDEX_NOT_INVESTABLE_TOTAL_RETURN",
                "policy_max_drawdown_bps": _max_drawdown_bps(
                    policy_curve, initial_value=1.0
                ),
                "buy_hold_max_drawdown_bps": _max_drawdown_bps(
                    buy_hold_curve, initial_value=1.0
                ),
                "policy_annualized_volatility_bps": float(
                    daily_std * np.sqrt(252.0)
                ),
                "policy_sharpe_zero_rate": (
                    float(subset["policy_change_bps"].mean() / daily_std * np.sqrt(252.0))
                    if daily_std > 0
                    else None
                ),
                "average_policy_exposure": float(subset["policy_exposure"].mean()),
                "average_policy_cash_fraction": float(subset["policy_cash_fraction"].mean()),
                "missed_upside_bps": float(subset["missed_upside_bps"].sum()),
                "post_exit_upside_bps": float(subset["post_exit_upside_bps"].sum()),
                "policy_fill_count": len(policy_fills),
                "buy_hold_fill_count": len(bh_fills),
                "policy_fee_cny": float(policy_fills["fill_fee_cny"].sum()),
                "buy_hold_fee_cny": float(bh_fills["fill_fee_cny"].sum()),
                "policy_turnover_cny": float(policy_fills.get("notional_cny", pd.Series(dtype=float)).sum()),
                "buy_hold_turnover_cny": float(bh_fills.get("notional_cny", pd.Series(dtype=float)).sum()),
                "nominal_inference": nominal,
                "familywise_inference": familywise,
                "effect_evidence": _effect_evidence(
                    familywise, coverage_complete=coverage_complete
                ),
                "power_status": "NOT_COMPUTABLE",
                "power_reason_code": "NO_PREREGISTERED_SAME_ESTIMAND_EFFECT_SCALE",
            }
            summaries.append(summary)
            if strategy_id == PRIMARY_STRATEGY:
                comparisons[f"{pool_id}:P_MINUS_BUY_AND_HOLD"] = summary
    pool_summary = pd.DataFrame(summaries)
    coverage = {
        "schema_version": "position_timing_pattern_universe_benchmark_coverage_v1",
        "request_sha256": request["request_sha256"],
        "source_preflight_complete": request["qlib_adjusted_factor_integrity_audit"][
            "coverage_complete"
        ],
        "source_excluded_symbol_count": source_failures,
        "diagnostic_status_counts": dict(
            sorted(Counter(diagnostic_frame["status"]).items())
        ),
        "primary_comparison_count": len(comparisons),
        "primary_family_complete": len(comparisons) == FAMILY_SIZE
        and all(item["coverage_complete"] for item in comparisons.values()),
        "diagnostic_rows": diagnostic_rows,
    }
    coverage["coverage_sha256"] = canonical_sha256(coverage)
    return daily, pool_summary, symbols, {"coverage": coverage, "comparisons": comparisons}


def _bundle_manifest(root: Path, receipt: Mapping[str, Any]) -> Mapping[str, Any]:
    files = {
        path.relative_to(root).as_posix(): _without_path(file_reference(path))
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    }
    identity = {
        "schema_version": BUNDLE_SCHEMA,
        "request_sha256": receipt["request_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "files": files,
    }
    return {**identity, "manifest_sha256": canonical_sha256(identity)}


def _publish_bundle(
    *,
    bundle: Path,
    request_path: Path,
    chunk_roots: Sequence[Path],
    daily: pd.DataFrame,
    pool_summary: pd.DataFrame,
    symbol_summary: pd.DataFrame,
    coverage: Mapping[str, Any],
    receipt: Mapping[str, Any],
) -> None:
    bundle = bundle.resolve()
    bundle.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}.", dir=bundle.parent))
    try:
        shutil.copy2(request_path, staging / "request.json")
        daily.to_parquet(staging / "daily_pool.parquet", index=False)
        pool_summary.to_parquet(staging / "pool_summary.parquet", index=False)
        symbol_summary.to_parquet(staging / "symbol_summary.parquet", index=False)
        (staging / "coverage.json").write_bytes(canonical_json_bytes(coverage))
        chunk_refs = {
            "schema_version": "position_timing_pattern_universe_chunk_references_v1",
            "chunks": [
                {
                    "path": root.resolve().as_posix(),
                    "manifest": file_reference(root / "manifest.json"),
                }
                for root in chunk_roots
            ],
        }
        chunk_refs["chunk_references_sha256"] = canonical_sha256(chunk_refs)
        (staging / "chunk_references.json").write_bytes(
            canonical_json_bytes(chunk_refs)
        )
        (staging / "receipt.json").write_bytes(canonical_json_bytes(receipt))
        (staging / "manifest.json").write_bytes(
            canonical_json_bytes(_bundle_manifest(staging, receipt))
        )
        with _exclusive_file_lock(bundle.parent / f".{bundle.name}.lock"):
            if bundle.exists():
                raise ActionValueError("PATTERN_BENCHMARK_BUNDLE_ALREADY_EXISTS")
            os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def prepare_request(
    *,
    timing_root: Path,
    repository_root: Path,
    parent_pattern_bundle: Path,
    candidate_root: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Path:
    """Freeze a Qlib adjusted-factor signal-research request.

    This path intentionally does not read or bind corporate-action, rights-
    issue, cash-ledger, share-arrival, or broker-account authorities.
    """

    if not timing_root.is_absolute() or not repository_root.is_absolute():
        raise ActionValueError("PATTERN_BENCHMARK_PREPARE_SPEC_INVALID")
    repository = repository_root.resolve()
    root = timing_root.resolve()
    candidate_root = candidate_root.resolve()
    if (
        root == repository
        or root.is_relative_to(repository)
        or isinstance(chunk_size, bool)
        or not 1 <= chunk_size <= 256
    ):
        raise ActionValueError("PATTERN_BENCHMARK_PREPARE_SPEC_INVALID")
    repository_commit = _clean_repository_commit(repository)
    candidate = DailyCandidate.open(candidate_root)
    memberships = open_candidate_pool_memberships(candidate)
    parent = load_frozen_parent_evidence(parent_pattern_bundle)
    symbols = memberships.symbols
    if symbols != candidate.symbols:
        raise ActionValueError("PATTERN_BENCHMARK_POPULATION_IDENTITY_MISMATCH")
    start = candidate.calendar[0].date()
    end = candidate.calendar[-1].date()
    if (start, end, len(candidate.calendar)) != (
        date(2018, 8, 1),
        date(2026, 8, 31),
        1961,
    ):
        raise ActionValueError("PATTERN_BENCHMARK_CALENDAR_IDENTITY_MISMATCH")
    candidate_identity_sha256 = canonical_sha256(
        {
            "candidate_manifest": memberships.candidate_manifest_reference,
            "candidate_dataset_manifest_sha256": memberships.candidate_dataset_manifest_sha256,
            "pool_sidecars": memberships.references,
        }
    )
    restatement_authority = open_adj_factor_restatement_authority(
        candidate_root=candidate_root,
        expected_candidate_manifest_sha256=EXPECTED_CANDIDATE_MANIFEST_SHA256,
        expected_authority_canonical_sha256=(
            EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256
        ),
    )
    restatement_audit = audit_candidate_adj_factor_restatement(
        candidate, restatement_authority
    )
    if (
        restatement_authority.candidate_manifest_reference
        != memberships.candidate_manifest_reference
        or restatement_authority.candidate_dataset_manifest_sha256
        != memberships.candidate_dataset_manifest_sha256
    ):
        raise ActionValueError(
            "PATTERN_BENCHMARK_ADJ_FACTOR_RESTATEMENT_CANDIDATE_IDENTITY_MISMATCH"
        )
    factor_audit = _audit_qlib_adjusted_factor_integrity(
        candidate,
        symbols=symbols,
        start=start,
        end=end,
        candidate_source_sha256=candidate_identity_sha256,
    )
    candidate.bars(BENCHMARK)
    source_preflight_identity = {
        "schema_version": "position_timing_pattern_adjusted_source_preflight_audit_v1",
        "candidate_manifest_sha256": EXPECTED_CANDIDATE_MANIFEST_SHA256,
        "qlib_adjusted_factor_contract_sha256": (
            QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256
        ),
        "qlib_adjusted_factor_integrity_audit_sha256": factor_audit[
            "audit_sha256"
        ],
        "adj_factor_restatement_audit_sha256": restatement_audit[
            "audit_sha256"
        ],
        "invalid_factor_symbol_count": factor_audit[
            "invalid_factor_symbol_count"
        ],
        "insufficient_factor_symbol_count": factor_audit[
            "insufficient_factor_symbol_count"
        ],
        "corporate_action_authority_read": False,
        "account_economics_simulated": False,
        "broker_account_clearing": False,
        "outcomes_read": False,
    }
    source_preflight = {
        **source_preflight_identity,
        "audit_sha256": canonical_sha256(source_preflight_identity),
    }
    if factor_audit["coverage_complete"] is not True:
        diagnostic_path = _publish_source_diagnostic(
            timing_root=root,
            payload={
                "schema_version": "position_timing_pattern_adjusted_source_diagnostic_v1",
                "pipeline_id": PIPELINE_ID,
                "candidate_manifest": memberships.candidate_manifest_reference,
                "qlib_adjusted_factor_contract": QLIB_ADJUSTED_FACTOR_CONTRACT,
                "qlib_adjusted_factor_integrity_audit": factor_audit,
                "adj_factor_restatement_audit": restatement_audit,
                "corporate_action_authority_read": False,
                "outcomes_read": False,
                "database_written": False,
                "runtime_action_performed": False,
            },
        )
        raise ActionValueError(
            "PATTERN_QLIB_ADJUSTED_FACTOR_COVERAGE_INCOMPLETE",
            diagnostic_path=diagnostic_path.as_posix(),
            invalid_factor_symbol_count=factor_audit[
                "invalid_factor_symbol_count"
            ],
            insufficient_factor_symbol_count=factor_audit[
                "insufficient_factor_symbol_count"
            ],
        )
    request: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "repository_root": repository.as_posix(),
        "repository_commit": repository_commit,
        "timing_root": root.as_posix(),
        "candidate_root": candidate_root.as_posix(),
        "candidate_manifest": memberships.candidate_manifest_reference,
        "candidate_manifest_sha256": EXPECTED_CANDIDATE_MANIFEST_SHA256,
        "candidate_dataset_manifest_sha256": memberships.candidate_dataset_manifest_sha256,
        "candidate_identity_sha256": candidate_identity_sha256,
        "candidate_data_reference_count": len(candidate.references),
        "candidate_data_references_sha256": canonical_sha256(candidate.references),
        "candidate_calendar": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "session_count": len(candidate.calendar),
        },
        "pool_sidecars": memberships.references,
        "pool_ids": POOL_IDS,
        "population_symbols": symbols,
        "population_symbols_sha256": canonical_sha256(symbols),
        "parent_pattern_bundle": parent.bundle.as_posix(),
        "parent_manifest": parent.manifest_reference,
        "parent_manifest_sha256": EXPECTED_PARENT_MANIFEST_SHA256,
        "parent_request_sha256": parent.request_sha256,
        "qlib_adjusted_factor_contract": QLIB_ADJUSTED_FACTOR_CONTRACT,
        "qlib_adjusted_factor_contract_sha256": (
            QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256
        ),
        "qlib_adjusted_factor_integrity_audit": factor_audit,
        "qlib_adjusted_factor_integrity_audit_sha256": factor_audit[
            "audit_sha256"
        ],
        "adj_factor_restatement_authority": (
            restatement_authority.authority_reference
        ),
        "adj_factor_restatement_authority_canonical_sha256": (
            restatement_authority.authority_canonical_sha256
        ),
        "adj_factor_restatement_diagnosis_sha256": (
            restatement_authority.diagnosis_sha256
        ),
        "adj_factor_restatement_series": [
            {
                "symbol": series.symbol,
                "start": series.start.isoformat(),
                "end": series.end.isoformat(),
                "row_count": series.row_count,
                "ordered_rows_sha256": series.ordered_rows_sha256,
            }
            for series in restatement_authority.series
        ],
        "adj_factor_restatement_audit": restatement_audit,
        "adj_factor_restatement_audit_sha256": restatement_audit[
            "audit_sha256"
        ],
        "source_preflight_audit": source_preflight,
        "source_preflight_audit_sha256": source_preflight["audit_sha256"],
        "benchmark_contract": BENCHMARK_CONTRACT,
        "benchmark_contract_sha256": BENCHMARK_CONTRACT_SHA256,
        "chunk_size": chunk_size,
        "source_code": _source_code_references(repository),
        "result_class": RESULT_CLASS,
        "selected_trial_count": 0,
        "registry_write": False,
        "current_write": False,
        "serving_model_artifact_write": False,
        "card_write": False,
        "alert_write": False,
        "order_write": False,
        "database_write": False,
        "runtime_write": False,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = (
        root
        / "research"
        / ARTIFACT_FOLDER
        / "requests"
        / f"{request['request_sha256']}.json"
    )
    PositionTimingArtifactStore._publish_immutable(
        path, canonical_json_bytes(request)
    )
    return path


def _load_request(path: Path) -> dict[str, Any]:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("PATTERN_BENCHMARK_REQUEST_UNAVAILABLE") from exc
    identity = {key: value for key, value in request.items() if key != "request_sha256"}
    factor_audit = request.get("qlib_adjusted_factor_integrity_audit")
    factor_identity = (
        {key: value for key, value in factor_audit.items() if key != "audit_sha256"}
        if isinstance(factor_audit, Mapping)
        else {}
    )
    restatement_audit = request.get("adj_factor_restatement_audit")
    restatement_identity = (
        {key: value for key, value in restatement_audit.items() if key != "audit_sha256"}
        if isinstance(restatement_audit, Mapping)
        else {}
    )
    preflight = request.get("source_preflight_audit")
    preflight_identity = (
        {key: value for key, value in preflight.items() if key != "audit_sha256"}
        if isinstance(preflight, Mapping)
        else {}
    )
    candidate_manifest = request.get("candidate_manifest")
    pool_sidecars = request.get("pool_sidecars")
    source_code = request.get("source_code")
    false_flags = (
        "registry_write",
        "current_write",
        "serving_model_artifact_write",
        "card_write",
        "alert_write",
        "order_write",
        "database_write",
        "runtime_write",
    )
    forbidden = {
        "corporate_action_snapshot",
        "corporate_action_full_scope_authority",
        "corporate_action_application_audit",
        "rights_issue_authority",
        "rights_issue_participation_policy",
        "factor_action_coverage_audit",
    }
    forbidden_prefixes = ("corporate_action_", "rights_issue_")
    repository_root = Path(str(request.get("repository_root", "")))
    timing_root = Path(str(request.get("timing_root", "")))
    if (
        request.get("schema_version") != REQUEST_SCHEMA
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("request_sha256") != canonical_sha256(identity)
        or request.get("candidate_manifest_sha256")
        != EXPECTED_CANDIDATE_MANIFEST_SHA256
        or request.get("candidate_dataset_manifest_sha256")
        != EXPECTED_CANDIDATE_DATASET_SHA256
        or request.get("parent_manifest_sha256")
        != EXPECTED_PARENT_MANIFEST_SHA256
        or not _same_canonical_identity(
            request.get("benchmark_contract"), BENCHMARK_CONTRACT
        )
        or request.get("benchmark_contract_sha256")
        != BENCHMARK_CONTRACT_SHA256
        or not _same_canonical_identity(
            request.get("qlib_adjusted_factor_contract"),
            QLIB_ADJUSTED_FACTOR_CONTRACT,
        )
        or request.get("qlib_adjusted_factor_contract_sha256")
        != QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256
        or forbidden.intersection(request)
        or any(str(key).startswith(forbidden_prefixes) for key in request)
        or not isinstance(factor_audit, Mapping)
        or factor_audit.get("audit_sha256") != canonical_sha256(factor_identity)
        or request.get("qlib_adjusted_factor_integrity_audit_sha256")
        != factor_audit.get("audit_sha256")
        or factor_audit.get("contract_sha256")
        != QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256
        or factor_audit.get("candidate_source_sha256")
        != request.get("candidate_identity_sha256")
        or factor_audit.get("coverage_complete") is not True
        or factor_audit.get("invalid_factor_symbol_count") != 0
        or factor_audit.get("insufficient_factor_symbol_count") != 0
        or factor_audit.get("corporate_action_authority_read") is not False
        or factor_audit.get("account_economics_simulated") is not False
        or factor_audit.get("broker_account_clearing") is not False
        or request.get("adj_factor_restatement_authority_canonical_sha256")
        != EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256
        or not isinstance(restatement_audit, Mapping)
        or restatement_audit.get("audit_sha256")
        != canonical_sha256(restatement_identity)
        or request.get("adj_factor_restatement_audit_sha256")
        != restatement_audit.get("audit_sha256")
        or restatement_audit.get("coverage_complete") is not True
        or not isinstance(preflight, Mapping)
        or preflight.get("audit_sha256") != canonical_sha256(preflight_identity)
        or request.get("source_preflight_audit_sha256")
        != preflight.get("audit_sha256")
        or preflight.get("candidate_manifest_sha256")
        != request.get("candidate_manifest_sha256")
        or preflight.get("qlib_adjusted_factor_contract_sha256")
        != request.get("qlib_adjusted_factor_contract_sha256")
        or preflight.get("qlib_adjusted_factor_integrity_audit_sha256")
        != request.get("qlib_adjusted_factor_integrity_audit_sha256")
        or preflight.get("adj_factor_restatement_audit_sha256")
        != request.get("adj_factor_restatement_audit_sha256")
        or preflight.get("invalid_factor_symbol_count") != 0
        or preflight.get("insufficient_factor_symbol_count") != 0
        or preflight.get("corporate_action_authority_read") is not False
        or preflight.get("account_economics_simulated") is not False
        or preflight.get("broker_account_clearing") is not False
        or preflight.get("outcomes_read") is not False
        or not isinstance(candidate_manifest, Mapping)
        or candidate_manifest.get("sha256")
        != request.get("candidate_manifest_sha256")
        or not isinstance(pool_sidecars, Mapping)
        or set(pool_sidecars) != set(POOL_IDS)
        or request.get("candidate_identity_sha256")
        != canonical_sha256(
            {
                "candidate_manifest": candidate_manifest,
                "candidate_dataset_manifest_sha256": request.get(
                    "candidate_dataset_manifest_sha256"
                ),
                "pool_sidecars": pool_sidecars,
            }
        )
        or not isinstance(source_code, Mapping)
        or set(source_code) != set(SOURCE_CODE_FILES)
        or not all(isinstance(item, Mapping) for item in source_code.values())
        or isinstance(request.get("chunk_size"), bool)
        or not isinstance(request.get("chunk_size"), int)
        or not 1 <= request["chunk_size"] <= 256
        or not repository_root.is_absolute()
        or not timing_root.is_absolute()
        or timing_root == repository_root
        or timing_root.is_relative_to(repository_root)
        or tuple(request.get("pool_ids") or ()) != POOL_IDS
        or tuple(request.get("population_symbols") or ())
        != tuple(sorted(set(request.get("population_symbols") or ())))
        or request.get("population_symbols_sha256")
        != canonical_sha256(tuple(request.get("population_symbols") or ()))
        or request.get("result_class") != RESULT_CLASS
        or request.get("selected_trial_count") != 0
        or any(request.get(flag) is not False for flag in false_flags)
    ):
        raise ActionValueError("PATTERN_BENCHMARK_REQUEST_IDENTITY_MISMATCH")
    return request


def inspect_bundle(bundle: Path) -> Mapping[str, Any]:
    root = bundle.resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((root / "receipt.json").read_text(encoding="utf-8"))
        coverage = json.loads((root / "coverage.json").read_text(encoding="utf-8"))
        chunk_refs = json.loads(
            (root / "chunk_references.json").read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise ActionValueError("PATTERN_BENCHMARK_BUNDLE_UNAVAILABLE") from exc
    request = _load_request(root / "request.json")
    manifest_identity = {
        key: value for key, value in manifest.items() if key != "manifest_sha256"
    }
    receipt_identity = {
        key: value for key, value in receipt.items() if key != "receipt_sha256"
    }
    coverage_identity = {
        key: value for key, value in coverage.items() if key != "coverage_sha256"
    }
    chunk_identity = {
        key: value
        for key, value in chunk_refs.items()
        if key != "chunk_references_sha256"
    }
    false_flags = tuple(EXTERNAL_WRITE_RECEIPT_FLAGS)
    request_bound_receipt_fields = (
        "repository_commit",
        "benchmark_contract_sha256",
        "candidate_manifest_sha256",
        "candidate_dataset_manifest_sha256",
        "candidate_data_references_sha256",
        "parent_manifest_sha256",
        "qlib_adjusted_factor_contract_sha256",
        "qlib_adjusted_factor_integrity_audit_sha256",
        "adj_factor_restatement_authority_canonical_sha256",
        "adj_factor_restatement_audit_sha256",
        "source_preflight_audit_sha256",
    )
    actual_files = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    }
    expected_files = set(manifest.get("files") or {}) | {"manifest.json"}
    if (
        manifest.get("schema_version") != BUNDLE_SCHEMA
        or manifest.get("manifest_sha256") != canonical_sha256(manifest_identity)
        or receipt.get("schema_version") != RECEIPT_SCHEMA
        or receipt.get("pipeline_id") != PIPELINE_ID
        or receipt.get("request_sha256") != request["request_sha256"]
        or receipt.get("receipt_sha256") != canonical_sha256(receipt_identity)
        or receipt.get("result_class") != RESULT_CLASS
        or receipt.get("selected_trial_count") != 0
        or receipt.get("corporate_action_authority_read") is not False
        or receipt.get("account_economics_simulated") is not False
        or receipt.get("broker_account_clearing") is not False
        or any(receipt.get(flag) is not False for flag in false_flags)
        or any(
            receipt.get(field) != request.get(field)
            for field in request_bound_receipt_fields
        )
        or coverage.get("coverage_sha256") != canonical_sha256(coverage_identity)
        or chunk_refs.get("chunk_references_sha256")
        != canonical_sha256(chunk_identity)
        or manifest.get("request_sha256") != request["request_sha256"]
        or manifest.get("receipt_sha256") != receipt["receipt_sha256"]
        or root.name != request["request_sha256"]
        or actual_files != expected_files
    ):
        raise ActionValueError("PATTERN_BENCHMARK_BUNDLE_IDENTITY_MISMATCH")
    for name, reference in manifest["files"].items():
        if _without_path(file_reference(root / name)) != reference:
            raise ActionValueError(
                "PATTERN_BENCHMARK_BUNDLE_FILE_IDENTITY_MISMATCH", file=name
            )
    for item in chunk_refs["chunks"]:
        chunk_root = Path(item["path"])
        if file_reference(chunk_root / "manifest.json") != item["manifest"]:
            raise ActionValueError("PATTERN_BENCHMARK_EXTERNAL_CHUNK_CHANGED")
        try:
            external_manifest = json.loads(
                (chunk_root / "manifest.json").read_text(encoding="utf-8")
            )
            inspect_chunk(
                chunk_root, expected_identity=external_manifest["chunk_identity"]
            )
        except (KeyError, OSError, ValueError) as exc:
            raise ActionValueError(
                "PATTERN_BENCHMARK_EXTERNAL_CHUNK_CHANGED"
            ) from exc
    return {
        "manifest": manifest,
        "request": request,
        "receipt": receipt,
        "coverage": coverage,
    }


def run_request(request_path: Path) -> Mapping[str, Any]:
    request_path = request_path.resolve()
    request = _load_request(request_path)
    if request_path.name != f"{request['request_sha256']}.json":
        raise ActionValueError("PATTERN_BENCHMARK_REQUEST_PATH_MISMATCH")
    repository = Path(request["repository_root"]).resolve()
    if _clean_repository_commit(repository) != request["repository_commit"]:
        raise ActionValueError("PATTERN_BENCHMARK_CODE_IDENTITY_MISMATCH")
    timing_root = Path(request["timing_root"]).resolve()
    bundle = (
        timing_root
        / "research"
        / ARTIFACT_FOLDER
        / "bundles"
        / request["request_sha256"]
    )
    if bundle.exists():
        return {
            "status": "ALREADY_MATERIALIZED",
            "bundle": bundle.as_posix(),
            **inspect_bundle(bundle),
        }
    for reference in request["source_code"].values():
        _assert_file_reference(
            reference, code="PATTERN_BENCHMARK_SOURCE_CODE_CHANGED"
        )
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    memberships = open_candidate_pool_memberships(candidate)
    if (
        memberships.candidate_manifest_reference != request["candidate_manifest"]
        or memberships.candidate_dataset_manifest_sha256
        != request["candidate_dataset_manifest_sha256"]
        or not _same_canonical_identity(
            memberships.references, request["pool_sidecars"]
        )
        or memberships.symbols != tuple(request["population_symbols"])
    ):
        raise ActionValueError("PATTERN_BENCHMARK_POPULATION_DRIFT")
    parent = load_frozen_parent_evidence(Path(request["parent_pattern_bundle"]))
    if (
        parent.manifest_reference != request["parent_manifest"]
        or parent.request_sha256 != request["parent_request_sha256"]
    ):
        raise ActionValueError("PATTERN_BENCHMARK_PARENT_DRIFT")
    restatement_authority = open_adj_factor_restatement_authority(
        candidate_root=candidate.root,
        expected_candidate_manifest_sha256=request["candidate_manifest_sha256"],
        expected_authority_canonical_sha256=request[
            "adj_factor_restatement_authority_canonical_sha256"
        ],
    )
    restatement_audit = audit_candidate_adj_factor_restatement(
        candidate, restatement_authority
    )
    declared_restatement_series = [
        {
            "symbol": series.symbol,
            "start": series.start.isoformat(),
            "end": series.end.isoformat(),
            "row_count": series.row_count,
            "ordered_rows_sha256": series.ordered_rows_sha256,
        }
        for series in restatement_authority.series
    ]
    if (
        restatement_authority.authority_reference
        != request["adj_factor_restatement_authority"]
        or restatement_authority.candidate_manifest_reference
        != request["candidate_manifest"]
        or restatement_authority.candidate_dataset_manifest_sha256
        != request["candidate_dataset_manifest_sha256"]
        or restatement_authority.diagnosis_sha256
        != request["adj_factor_restatement_diagnosis_sha256"]
        or declared_restatement_series != request["adj_factor_restatement_series"]
        or restatement_audit != request["adj_factor_restatement_audit"]
    ):
        raise ActionValueError(
            "PATTERN_BENCHMARK_ADJ_FACTOR_RESTATEMENT_AUTHORITY_DRIFT"
        )
    factor_audit = _audit_qlib_adjusted_factor_integrity(
        candidate,
        symbols=memberships.symbols,
        start=candidate.calendar[0].date(),
        end=candidate.calendar[-1].date(),
        candidate_source_sha256=request["candidate_identity_sha256"],
    )
    if factor_audit != request["qlib_adjusted_factor_integrity_audit"]:
        raise ActionValueError("PATTERN_BENCHMARK_ADJUSTED_FACTOR_DRIFT")
    observed_preflight_identity = {
        key: value
        for key, value in request["source_preflight_audit"].items()
        if key != "audit_sha256"
    }
    observed_preflight = {
        **observed_preflight_identity,
        "qlib_adjusted_factor_integrity_audit_sha256": factor_audit[
            "audit_sha256"
        ],
        "adj_factor_restatement_audit_sha256": restatement_audit[
            "audit_sha256"
        ],
    }
    observed_preflight["audit_sha256"] = canonical_sha256(observed_preflight)
    if observed_preflight != request["source_preflight_audit"]:
        raise ActionValueError("PATTERN_BENCHMARK_SOURCE_PREFLIGHT_DRIFT")
    benchmark_close = candidate.bars(BENCHMARK)["close"]
    observed_candidate_references = dict(candidate.references)
    symbols = tuple(request["population_symbols"])
    chunk_size = int(request["chunk_size"])
    chunk_roots: list[Path] = []
    chunk_reuse_count = 0
    for chunk_ordinal, offset in enumerate(range(0, len(symbols), chunk_size)):
        chunk_symbols = symbols[offset : offset + chunk_size]
        chunk_identity = {
            "request_sha256": request["request_sha256"],
            "repository_commit": request["repository_commit"],
            "chunk_ordinal": chunk_ordinal,
            "symbol_start": chunk_symbols[0],
            "symbol_end": chunk_symbols[-1],
            "symbol_count": len(chunk_symbols),
            "symbols_sha256": canonical_sha256(chunk_symbols),
            "strategy_ids": STRATEGY_IDS,
            "pool_ids": POOL_IDS,
            "valuation_mode": "QLIB_ADJUSTED_PRICE",
        }
        chunk_root = (
            timing_root
            / "research"
            / ARTIFACT_FOLDER
            / "chunks"
            / request["request_sha256"]
            / f"{chunk_ordinal:04d}"
        )
        if chunk_root.exists():
            inspect_chunk(chunk_root, expected_identity=chunk_identity)
            chunk_reuse_count += 1
        else:
            daily, fills, summaries, diagnostics = _run_chunk(
                request=request,
                chunk_ordinal=chunk_ordinal,
                symbols=chunk_symbols,
                candidate=candidate,
                memberships=memberships,
            )
            _publish_chunk(
                target=chunk_root,
                chunk_identity=chunk_identity,
                daily=daily,
                fills=fills,
                summaries=summaries,
                diagnostics=diagnostics,
            )
        diagnostics = json.loads(
            (chunk_root / "diagnostics.json").read_text(encoding="utf-8")
        )
        observed_candidate_references.update(diagnostics["source_references"])
        chunk_roots.append(chunk_root)
    if (
        len(observed_candidate_references)
        != request["candidate_data_reference_count"]
        or canonical_sha256(observed_candidate_references)
        != request["candidate_data_references_sha256"]
    ):
        raise ActionValueError("PATTERN_BENCHMARK_CANDIDATE_DATA_DRIFT")
    daily, pool_summary, symbol_summary, aggregate = _aggregate_results(
        request=request,
        chunk_roots=chunk_roots,
        benchmark_close=benchmark_close,
    )
    coverage = aggregate["coverage"]
    comparisons = aggregate["comparisons"]
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "request_sha256": request["request_sha256"],
        "repository_commit": request["repository_commit"],
        "result_class": RESULT_CLASS,
        "benchmark_contract_sha256": BENCHMARK_CONTRACT_SHA256,
        "candidate_manifest_sha256": request["candidate_manifest_sha256"],
        "candidate_dataset_manifest_sha256": request[
            "candidate_dataset_manifest_sha256"
        ],
        "candidate_data_references_sha256": request[
            "candidate_data_references_sha256"
        ],
        "parent_manifest_sha256": request["parent_manifest_sha256"],
        "qlib_adjusted_factor_contract_sha256": request[
            "qlib_adjusted_factor_contract_sha256"
        ],
        "qlib_adjusted_factor_integrity_audit_sha256": request[
            "qlib_adjusted_factor_integrity_audit_sha256"
        ],
        "adj_factor_restatement_authority_canonical_sha256": request[
            "adj_factor_restatement_authority_canonical_sha256"
        ],
        "adj_factor_restatement_audit_sha256": request[
            "adj_factor_restatement_audit_sha256"
        ],
        "source_preflight_audit_sha256": request[
            "source_preflight_audit_sha256"
        ],
        "source_preflight_outcomes_read": False,
        "outcomes_read_after_source_preflight": True,
        "corporate_action_authority_read": False,
        "account_economics_simulated": False,
        "broker_account_clearing": False,
        "pool_sidecars": request["pool_sidecars"],
        "main_family_size": FAMILY_SIZE,
        "primary_comparisons": comparisons,
        "primary_family_complete": coverage["primary_family_complete"],
        "effect_evidence_by_pool": {
            key.split(":", 1)[0]: value["effect_evidence"]
            for key, value in comparisons.items()
        },
        "selected_trial_count": 0,
        "chunk_count": len(chunk_roots),
        "chunk_reuse_count": chunk_reuse_count,
        "coverage_sha256": coverage["coverage_sha256"],
        **EXTERNAL_WRITE_RECEIPT_FLAGS,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    _publish_bundle(
        bundle=bundle,
        request_path=request_path,
        chunk_roots=chunk_roots,
        daily=daily,
        pool_summary=pool_summary,
        symbol_summary=symbol_summary,
        coverage=coverage,
        receipt=receipt,
    )
    return {
        "status": "MATERIALIZED",
        "bundle": bundle.as_posix(),
        **inspect_bundle(bundle),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--timing-root", required=True, type=Path)
    prepare.add_argument("--repository-root", required=True, type=Path)
    prepare.add_argument("--parent-pattern-bundle", required=True, type=Path)
    prepare.add_argument("--candidate-root", required=True, type=Path)
    prepare.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    run = commands.add_parser("run")
    run.add_argument("--request", required=True, type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "prepare":
            path = prepare_request(
                timing_root=arguments.timing_root,
                repository_root=arguments.repository_root,
                parent_pattern_bundle=arguments.parent_pattern_bundle,
                candidate_root=arguments.candidate_root,
                chunk_size=arguments.chunk_size,
            )
            output: Mapping[str, Any] = {
                "status": "REQUEST_PREPARED",
                "request": path.as_posix(),
            }
        elif arguments.command == "run":
            output = run_request(arguments.request)
        else:
            output = {"status": "BUNDLE_VALID", **inspect_bundle(arguments.bundle)}
    except ActionValueError as exc:
        print(json.dumps({"status": "ERROR", "reason_code": exc.code, **exc.details}))
        return 2
    print(json.dumps(output, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
