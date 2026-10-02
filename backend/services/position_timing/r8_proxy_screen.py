"""Frozen R8-only proxy screens for PT-NEXT-023.

The ``bak_basic`` screen is explicitly a lagged daily-snapshot proxy.  It is
not a quarterly, revision-aware financial PIT source and must never satisfy
the PT-NEXT-022 P2/P3 contract.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .action_value_data import file_reference
from .contracts import canonical_sha256
from .fundamental_screen import CandidateIdentity, open_r8_candidate_identity


U0 = "SIZE_50_500B_V1"
U1 = "VALUE_LIQUIDITY_PROXY_V1"
U2 = "BAK_GROWTH_PROXY_V1"
SCREEN_IDS = (U0, U1, U2)
SCREEN_STATES = ("PASS", "FAIL", "UNKNOWN", "NOT_APPLICABLE")

MIN_TOTAL_MV = 500_000.0
MAX_TOTAL_MV = 5_000_000.0
MIN_TURNOVER_RATE_F = 0.5
MAX_TURNOVER_RATE_F = 15.0
MAX_PE_TTM = 60.0
MAX_PB = 8.0

DAILY_COLUMNS = (
    "db_total_mv",
    "db_turnover_rate_f",
    "db_pe_ttm",
    "db_pb",
)
BAK_COLUMNS = (
    "bb_rev_yoy",
    "bb_profit_yoy",
    "bb_gpr",
    "bb_npr",
)


@dataclass(frozen=True)
class ProxySourceIdentity:
    candidate: CandidateIdentity
    inventory_reference: Mapping[str, Any]
    daily_basic_reference: Mapping[str, Any]
    bak_basic_reference: Mapping[str, Any]


@dataclass(frozen=True)
class ProxyFrames:
    daily: pd.DataFrame
    bak: pd.DataFrame


def _read_json(path: Path) -> dict[str, Any]:
    import json

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("CORE_TACTICAL_PROXY_JSON_INVALID", path=str(path)) from exc
    if not isinstance(value, dict):
        raise ActionValueError("CORE_TACTICAL_PROXY_JSON_INVALID", path=str(path))
    return value


def _component_reference(root: Path, inventory: Mapping[str, Any], suffix: str) -> Mapping[str, Any]:
    matches = [
        item
        for item in inventory.get("files", [])
        if str(item.get("path") or "").replace("\\", "/").endswith(suffix)
    ]
    if len(matches) != 1:
        raise ActionValueError("CORE_TACTICAL_PROXY_COMPONENT_INVALID", suffix=suffix, count=len(matches))
    item = matches[0]
    path = (root / str(item.get("path") or "")).resolve()
    if not path.is_relative_to(root):
        raise ActionValueError("CORE_TACTICAL_PROXY_COMPONENT_SCOPE_DRIFT", suffix=suffix)
    reference = file_reference(path)
    if reference["sha256"] != item.get("sha256") or reference["size_bytes"] != item.get("size"):
        raise ActionValueError("CORE_TACTICAL_PROXY_COMPONENT_DRIFT", suffix=suffix)
    return reference


def open_r8_proxy_sources(candidate_root: Path) -> ProxySourceIdentity:
    candidate = open_r8_candidate_identity(candidate_root)
    spec = (candidate.manifest.get("components") or {}).get("factor_content_manifest")
    if not isinstance(spec, Mapping):
        raise ActionValueError("CORE_TACTICAL_FACTOR_INVENTORY_MISSING")
    inventory_path = (candidate.root / str(spec.get("path") or "")).resolve()
    if not inventory_path.is_relative_to(candidate.root):
        raise ActionValueError("CORE_TACTICAL_FACTOR_INVENTORY_SCOPE_DRIFT")
    inventory_reference = file_reference(inventory_path)
    if (
        inventory_reference["sha256"] != spec.get("sha256")
        or inventory_reference["size_bytes"] != spec.get("size")
    ):
        raise ActionValueError("CORE_TACTICAL_FACTOR_INVENTORY_DRIFT")
    inventory = _read_json(inventory_path)
    daily = _component_reference(candidate.root, inventory, "/daily_basic.h5")
    bak = _component_reference(candidate.root, inventory, "/bak_basic.h5")
    if daily != candidate.daily_basic_reference:
        raise ActionValueError("CORE_TACTICAL_DAILY_BASIC_IDENTITY_DRIFT")
    return ProxySourceIdentity(candidate, inventory_reference, daily, bak)


def _read_hdf(reference: Mapping[str, Any], columns: tuple[str, ...], source: str) -> pd.DataFrame:
    try:
        frame = pd.read_hdf(str(reference["path"]), key="data", columns=list(columns))
    except (OSError, KeyError, ValueError) as exc:
        raise ActionValueError("CORE_TACTICAL_PROXY_READ_FAILED", source=source) from exc
    if (
        not isinstance(frame.index, pd.MultiIndex)
        or frame.index.nlevels != 2
        or not frame.index.is_unique
    ):
        raise ActionValueError("CORE_TACTICAL_PROXY_INDEX_INVALID", source=source)
    frame.index = frame.index.set_names(["datetime", "instrument"])
    if not frame.index.is_monotonic_increasing:
        frame = frame.sort_index()
    normalized = frame.loc[:, list(columns)].apply(pd.to_numeric, errors="coerce")
    return normalized


def read_proxy_frames(identity: ProxySourceIdentity) -> ProxyFrames:
    return ProxyFrames(
        daily=_read_hdf(identity.daily_basic_reference, DAILY_COLUMNS, "daily_basic"),
        bak=_read_hdf(identity.bak_basic_reference, BAK_COLUMNS, "bak_basic"),
    )


def source_audit(identity: ProxySourceIdentity, frames: ProxyFrames) -> dict[str, Any]:
    def audit_frame(frame: pd.DataFrame, columns: tuple[str, ...], reference: Mapping[str, Any]) -> dict[str, Any]:
        dates = frame.index.get_level_values("datetime")
        instruments = frame.index.get_level_values("instrument")
        return {
            "reference": reference,
            "row_count": int(len(frame)),
            "symbol_count": int(instruments.nunique()),
            "start": str(pd.Timestamp(dates.min()).date()),
            "end": str(pd.Timestamp(dates.max()).date()),
            "index_unique": bool(frame.index.is_unique),
            "columns": list(columns),
            "finite_count": {
                column: int(np.isfinite(frame[column].to_numpy(float)).sum())
                for column in columns
            },
        }

    audit = {
        "schema_version": "position_timing_r8_proxy_source_audit_v1",
        "candidate_dataset_sha256": identity.candidate.dataset_sha256,
        "availability": "PREVIOUS_GLOBAL_TRADING_SESSION",
        "bak_semantics": "BAK_BASIC_DAILY_SNAPSHOT_PROXY_NOT_FINANCIAL_PIT",
        "inventory_reference": identity.inventory_reference,
        "daily_basic": audit_frame(frames.daily, DAILY_COLUMNS, identity.daily_basic_reference),
        "bak_basic": audit_frame(frames.bak, BAK_COLUMNS, identity.bak_basic_reference),
    }
    audit["audit_sha256"] = canonical_sha256(audit)
    return audit


def _lagged(frame: pd.DataFrame, dates: pd.Index, columns: tuple[str, ...]) -> pd.DataFrame:
    aligned = frame.reindex(pd.DatetimeIndex(dates)).loc[:, list(columns)]
    return aligned.shift(1)


def screen_masks_for_symbol(
    *,
    dates: pd.Index,
    pit_active: np.ndarray,
    feature_ready: np.ndarray,
    daily: pd.DataFrame,
    bak: pd.DataFrame,
) -> tuple[dict[str, np.ndarray], dict[str, dict[str, Any]], dict[str, np.ndarray]]:
    """Return enrollment masks, coverage summaries and daily UNKNOWN masks."""

    size = len(dates)
    if len(pit_active) != size or len(feature_ready) != size:
        raise ActionValueError("CORE_TACTICAL_SCREEN_ALIGNMENT_INVALID")
    lag_daily = _lagged(daily, dates, DAILY_COLUMNS)
    lag_bak = _lagged(bak, dates, BAK_COLUMNS)
    pit = np.asarray(pit_active, dtype=bool)
    ready = np.asarray(feature_ready, dtype=bool)

    mv = lag_daily["db_total_mv"].to_numpy(float)
    u0_known = np.isfinite(mv)
    u0_rule = u0_known & (mv >= MIN_TOTAL_MV) & (mv <= MAX_TOTAL_MV)
    u0_pass = pit & u0_rule
    u0_unknown = pit & ~u0_known

    turnover = lag_daily["db_turnover_rate_f"].to_numpy(float)
    pe = lag_daily["db_pe_ttm"].to_numpy(float)
    pb = lag_daily["db_pb"].to_numpy(float)
    u1_fields_known = np.isfinite(turnover) & np.isfinite(pe) & np.isfinite(pb)
    u1_rule = (
        u1_fields_known
        & (turnover >= MIN_TURNOVER_RATE_F)
        & (turnover <= MAX_TURNOVER_RATE_F)
        & (pe > 0)
        & (pe <= MAX_PE_TTM)
        & (pb > 0)
        & (pb <= MAX_PB)
    )
    u1_pass = u0_pass & u1_rule
    u1_unknown = u0_unknown | (u0_pass & ~u1_fields_known)

    bak_values = lag_bak.loc[:, list(BAK_COLUMNS)].to_numpy(float)
    u2_fields_known = np.isfinite(bak_values).all(axis=1)
    u2_rule = u2_fields_known & (bak_values > 0).all(axis=1)
    u2_pass = u1_pass & u2_rule
    u2_unknown = u1_unknown | (u1_pass & ~u2_fields_known)

    passed = {U0: u0_pass, U1: u1_pass, U2: u2_pass}
    unknown = {U0: u0_unknown, U1: u1_unknown, U2: u2_unknown}
    masks = {screen: values & ready for screen, values in passed.items()}
    coverage: dict[str, dict[str, Any]] = {}
    for screen in SCREEN_IDS:
        screen_pass = passed[screen]
        screen_unknown = unknown[screen]
        screen_fail = ~(screen_pass | screen_unknown)
        coverage[screen] = {
            "expected": size,
            "PASS": int(screen_pass.sum()),
            "FAIL": int(screen_fail.sum()),
            "UNKNOWN": int(screen_unknown.sum()),
            "NOT_APPLICABLE": 0,
            "feature_unready": int((~ready).sum()),
            "enrollment_eligible": int((screen_pass & ready).sum()),
            "enrollment_unknown": int((screen_unknown & ready).sum()),
        }
    if np.any(u2_pass & ~u1_pass) or np.any(u1_pass & ~u0_pass):
        raise ActionValueError("CORE_TACTICAL_SCREEN_NESTING_INVALID")
    return masks, coverage, unknown


SCREEN_CONTRACT = {
    "schema": "position_timing_r8_proxy_screen_contract_v1",
    "screen_ids": list(SCREEN_IDS),
    "availability": "PREVIOUS_GLOBAL_TRADING_SESSION",
    "u0": {"total_mv_wanyuan": [MIN_TOTAL_MV, MAX_TOTAL_MV]},
    "u1": {
        "turnover_rate_f": [MIN_TURNOVER_RATE_F, MAX_TURNOVER_RATE_F],
        "pe_ttm": ["GT_0", MAX_PE_TTM],
        "pb": ["GT_0", MAX_PB],
    },
    "u2": {column: "GT_0" for column in BAK_COLUMNS},
    "bak_semantics": "BAK_BASIC_DAILY_SNAPSHOT_PROXY_NOT_FINANCIAL_PIT",
    "missing": "UNKNOWN_NO_FILL_NO_BACKFILL",
}
SCREEN_CONTRACT_SHA256 = canonical_sha256(SCREEN_CONTRACT)


__all__ = [
    "BAK_COLUMNS",
    "DAILY_COLUMNS",
    "ProxyFrames",
    "ProxySourceIdentity",
    "SCREEN_CONTRACT",
    "SCREEN_CONTRACT_SHA256",
    "SCREEN_IDS",
    "U0",
    "U1",
    "U2",
    "open_r8_proxy_sources",
    "read_proxy_frames",
    "screen_masks_for_symbol",
    "source_audit",
]
