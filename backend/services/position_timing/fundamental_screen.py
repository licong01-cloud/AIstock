"""PIT screen inputs for the PT-NEXT-022 fundamental timing research.

The adapter is deliberately source-only.  It never queries a database and it
does not turn the legacy ``bak_basic`` fields into a financial PIT history.
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


SCREEN_IDS = ("P1", "P2", "P3")
SCREEN_STATES = ("PASS", "FAIL", "UNKNOWN", "NOT_APPLICABLE")
FINANCIAL_SCHEMA = "position_timing_fundamental_pit_source_v1"
R8_MANIFEST_FILE_SHA256 = "07db01d8c24108ebfe2b85bc42f18f33c77fb0e09451999d7520faa08ebe1b18"
R8_DATASET_SHA256 = "6bb6096aada59541f58d05e1c44d5dabd8838661936d98539adcb79d7ac39283"
R8_DEPLOYMENT_CONTENT_SHA256 = "c45f67da4f2e96f17fe24c043666783761be102550e9af94395786eabc51b9d0"
R8_REVISION = "20260918-r8"
P1_MIN_TOTAL_MV_WANYUAN = 500_000.0
P1_MAX_TOTAL_MV_WANYUAN = 5_000_000.0

FINANCIAL_COLUMNS = {
    "symbol",
    "report_end_date",
    "statement_scope",
    "source_record_id",
    "revision_id",
    "row_sha256",
    "available_at",
    "availability_precision",
    "quarterly_deducted_parent_profit_cny",
    "quarterly_revenue_cny",
    "quarterly_parent_profit_cny",
    "parent_equity_cny",
    "quarterly_operating_cash_flow_cny",
    "financial_classification",
}


@dataclass(frozen=True)
class CandidateIdentity:
    root: Path
    manifest: Mapping[str, Any]
    manifest_reference: Mapping[str, Any]
    daily_basic_reference: Mapping[str, Any]

    @property
    def dataset_sha256(self) -> str:
        return str(self.manifest["dataset_manifest_sha256"])


@dataclass(frozen=True)
class FinancialPitSource:
    frame: pd.DataFrame
    manifest: Mapping[str, Any]
    manifest_reference: Mapping[str, Any]
    data_reference: Mapping[str, Any]


def _read_json(path: Path) -> dict[str, Any]:
    import json

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("FUNDAMENTAL_SOURCE_JSON_INVALID", path=str(path)) from exc
    if not isinstance(value, dict):
        raise ActionValueError("FUNDAMENTAL_SOURCE_JSON_INVALID", path=str(path))
    return value


def open_r8_candidate_identity(candidate_root: Path) -> CandidateIdentity:
    root = candidate_root.resolve()
    manifest_path = root / "qe_dataset_manifest.json"
    manifest_ref = file_reference(manifest_path)
    if manifest_ref["sha256"] != R8_MANIFEST_FILE_SHA256:
        raise ActionValueError("FUNDAMENTAL_R8_MANIFEST_FILE_DRIFT")
    manifest = _read_json(manifest_path)
    canonical = canonical_sha256({k: v for k, v in manifest.items() if k != "dataset_manifest_sha256"})
    if (
        manifest.get("dataset_manifest_sha256") != R8_DATASET_SHA256
        or canonical != R8_DATASET_SHA256
        or manifest.get("deployment_content_sha256") != R8_DEPLOYMENT_CONTENT_SHA256
        or manifest.get("revision") != R8_REVISION
        or manifest.get("availability_status") != "CANDIDATE_READY"
    ):
        raise ActionValueError("FUNDAMENTAL_R8_CANONICAL_IDENTITY_DRIFT")
    inventory_spec = (manifest.get("components") or {}).get("factor_content_manifest")
    if not isinstance(inventory_spec, Mapping):
        raise ActionValueError("FUNDAMENTAL_FACTOR_INVENTORY_MISSING")
    inventory_path = (root / str(inventory_spec.get("path") or "")).resolve()
    inventory_ref = file_reference(inventory_path)
    if inventory_ref["sha256"] != inventory_spec.get("sha256") or inventory_ref["size_bytes"] != inventory_spec.get("size"):
        raise ActionValueError("FUNDAMENTAL_FACTOR_INVENTORY_DRIFT")
    inventory = _read_json(inventory_path)
    component = next(
        (
            item for item in inventory.get("files", [])
            if str(item.get("path") or "").replace("\\", "/").endswith("/daily_basic.h5")
        ),
        None,
    )
    if not isinstance(component, Mapping):
        raise ActionValueError("FUNDAMENTAL_DAILY_BASIC_COMPONENT_MISSING")
    path = (root / str(component.get("path") or "")).resolve()
    if not path.is_relative_to(root):
        raise ActionValueError("FUNDAMENTAL_DAILY_BASIC_SCOPE_DRIFT")
    reference = file_reference(path)
    if reference["sha256"] != component.get("sha256") or reference["size_bytes"] != component.get("size"):
        raise ActionValueError("FUNDAMENTAL_DAILY_BASIC_IDENTITY_DRIFT")
    return CandidateIdentity(root, manifest, manifest_ref, reference)


def read_total_market_cap(identity: CandidateIdentity) -> pd.Series:
    path = Path(str(identity.daily_basic_reference["path"]))
    try:
        frame = pd.read_hdf(path, key="data", columns=["db_total_mv"])
    except (OSError, KeyError, ValueError) as exc:
        raise ActionValueError("FUNDAMENTAL_DAILY_BASIC_READ_FAILED") from exc
    if not isinstance(frame.index, pd.MultiIndex) or frame.index.nlevels != 2 or not frame.index.is_unique:
        raise ActionValueError("FUNDAMENTAL_DAILY_BASIC_INDEX_INVALID")
    index = frame.index.set_names(["datetime", "instrument"])
    values = pd.to_numeric(frame["db_total_mv"], errors="coerce")
    if (~np.isfinite(values) | (values <= 0)).any():
        raise ActionValueError("FUNDAMENTAL_TOTAL_MV_INVALID")
    values.index = index
    values.name = "db_total_mv"
    return values.sort_index()


def p1_mask_for_symbol(
    market_cap: pd.Series,
    *,
    dates: pd.Index,
    pit_active: np.ndarray,
    feature_ready: np.ndarray,
) -> tuple[np.ndarray, dict[str, int]]:
    """Return the daily P1 mask using only the previous global session value."""

    if len(dates) != len(pit_active) or len(dates) != len(feature_ready):
        raise ActionValueError("FUNDAMENTAL_P1_ALIGNMENT_INVALID")
    aligned = pd.to_numeric(market_cap.reindex(pd.DatetimeIndex(dates)), errors="coerce")
    lagged = aligned.shift(1).to_numpy(float)
    known = np.isfinite(lagged)
    in_band = known & (lagged >= P1_MIN_TOTAL_MV_WANYUAN) & (lagged <= P1_MAX_TOTAL_MV_WANYUAN)
    passed = in_band & np.asarray(pit_active, dtype=bool) & np.asarray(feature_ready, dtype=bool)
    counts = {
        "expected": int(len(dates)),
        "market_cap_unknown": int((~known).sum()),
        "market_cap_pass": int(in_band.sum()),
        "pit_inactive": int((~np.asarray(pit_active, dtype=bool)).sum()),
        "feature_unready": int((~np.asarray(feature_ready, dtype=bool)).sum()),
        "pass": int(passed.sum()),
    }
    return passed, counts


def first_enrollment_ordinal(mask: np.ndarray, *, final_decision_ordinal: int) -> int | None:
    eligible = np.flatnonzero(np.asarray(mask, dtype=bool))
    eligible = eligible[eligible < final_decision_ordinal]
    return int(eligible[0]) if len(eligible) else None


def unavailable_financial_groups(reason: str = "FINANCIAL_PIT_INPUT_NOT_DELIVERED") -> dict[str, Any]:
    return {
        "P2": {"status": "INPUT_UNAVAILABLE", "reason": reason},
        "P3": {"status": "INPUT_UNAVAILABLE", "reason": reason},
    }


def open_financial_pit_source(
    manifest_path: Path,
    *,
    expected_candidate_sha256: str = R8_DATASET_SHA256,
) -> FinancialPitSource:
    """Open a future formal source; no legacy or database fallback is allowed."""

    manifest_path = manifest_path.resolve()
    manifest_ref = file_reference(manifest_path)
    manifest = _read_json(manifest_path)
    if manifest.get("schema") != FINANCIAL_SCHEMA:
        raise ActionValueError("FUNDAMENTAL_FINANCIAL_SCHEMA_INVALID")
    if manifest.get("candidate_dataset_sha256") != expected_candidate_sha256:
        raise ActionValueError("FUNDAMENTAL_FINANCIAL_CANDIDATE_DRIFT")
    data_spec = manifest.get("data_file")
    if not isinstance(data_spec, Mapping):
        raise ActionValueError("FUNDAMENTAL_FINANCIAL_DATA_REFERENCE_MISSING")
    data_path = (manifest_path.parent / str(data_spec.get("path") or "")).resolve()
    if not data_path.is_relative_to(manifest_path.parent):
        raise ActionValueError("FUNDAMENTAL_FINANCIAL_SCOPE_DRIFT")
    data_ref = file_reference(data_path)
    if data_ref["sha256"] != data_spec.get("sha256") or data_ref["size_bytes"] != data_spec.get("size_bytes"):
        raise ActionValueError("FUNDAMENTAL_FINANCIAL_DATA_DRIFT")
    frame = pd.read_parquet(data_path)
    if not FINANCIAL_COLUMNS.issubset(frame.columns):
        raise ActionValueError("FUNDAMENTAL_FINANCIAL_COLUMNS_MISSING")
    if frame[["source_record_id", "revision_id"]].duplicated().any():
        raise ActionValueError("FUNDAMENTAL_FINANCIAL_VERSION_CONFLICT")
    available = pd.to_datetime(frame["available_at"], utc=True, errors="coerce")
    report_end = pd.to_datetime(frame["report_end_date"], errors="coerce")
    if available.isna().any() or report_end.isna().any():
        raise ActionValueError("FUNDAMENTAL_FINANCIAL_TIME_INVALID")
    if not frame["row_sha256"].astype(str).str.fullmatch(r"[0-9a-f]{64}").all():
        raise ActionValueError("FUNDAMENTAL_FINANCIAL_ROW_IDENTITY_INVALID")
    normalized = frame.copy()
    normalized["available_at"] = available
    normalized["report_end_date"] = report_end
    return FinancialPitSource(normalized, manifest, manifest_ref, data_ref)


SCREEN_CONTRACT = {
    "schema": "position_timing_fundamental_screen_contract_v1",
    "screen_ids": list(SCREEN_IDS),
    "p1": {
        "unit": "CNY_10000",
        "lower_inclusive": P1_MIN_TOTAL_MV_WANYUAN,
        "upper_inclusive": P1_MAX_TOTAL_MV_WANYUAN,
        "availability": "PREVIOUS_GLOBAL_TRADING_SESSION",
    },
    "p2_p3_source_schema": FINANCIAL_SCHEMA,
    "missing_financial_source": "GROUP_LEVEL_INPUT_UNAVAILABLE_NO_FALLBACK",
}
SCREEN_CONTRACT_SHA256 = canonical_sha256(SCREEN_CONTRACT)
