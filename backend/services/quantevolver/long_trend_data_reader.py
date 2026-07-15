"""Strict, QE-only daily price and sector reader for long-trend evaluation."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable, Iterable

import numpy as np
import pandas as pd

from backend.services.quantevolver.long_trend_evaluation_contract import (
    QEDatasetSnapshotIdentity,
    QELongTrendError,
    QELongTrendReason,
    SnapshotOverlapParityReceipt,
    canonical_sha256,
)
from backend.services.quantevolver.qe_dataset_contract import (
    QE_DATASET_CONTRACT_ID,
    QE_DATASET_SIGNAL_END_DATE,
    QE_DATASET_START_DATE,
    require_qe_dataset_window,
)


_TS_CODE_RE = re.compile(r"^(?P<code>\d{6})\.(?P<exchange>SH|SZ|BJ)$")
_QLIB_CODE_RE = re.compile(r"^(?P<exchange>SH|SZ|BJ)(?P<code>\d{6})$")
_PRICE_FILE = "daily_pv.h5"
_SECTOR_FILE = "sector_data.h5"
_META_FILE = "meta.json"
_IDENTITY_FILES = (_META_FILE, _PRICE_FILE, _SECTOR_FILE)


def canonicalize_instrument(value: object) -> str:
    text = str(value).strip().upper()
    match = _TS_CODE_RE.fullmatch(text)
    if match:
        return f"{match.group('code')}.{match.group('exchange')}"
    match = _QLIB_CODE_RE.fullmatch(text)
    if match:
        return f"{match.group('code')}.{match.group('exchange')}"
    raise ValueError(f"unsupported A-share instrument identity {value!r}")


@dataclass(frozen=True)
class QELongTrendDatasetFrames:
    prices: pd.DataFrame
    sectors: pd.DataFrame | None
    price_path: Path
    sector_path: Path | None


class QELongTrendDatasetReader:
    """Load only daily_pv.h5 and sector_data.h5 from an explicit QE snapshot.

    The reader intentionally does not reuse the official factor cache loader:
    that loader permits eight data files and swallows date-slicing exceptions,
    while F-014 requires a two-file allowlist and structured fail-fast errors.
    """

    def __init__(
        self,
        *,
        factor_data_dir: str | os.PathLike[str],
        qe_workspace_root: str | os.PathLike[str],
        qe_dataset_contract_id: str,
        snapshot_identity: QEDatasetSnapshotIdentity,
        hdf_reader: Callable[..., pd.DataFrame] | None = None,
    ) -> None:
        if not qe_dataset_contract_id or qe_dataset_contract_id != QE_DATASET_CONTRACT_ID:
            raise QELongTrendError(
                QELongTrendReason.NON_QE_SOURCE_REJECTED,
                "long-trend reader requires the deployed QE dataset contract identity",
                context={
                    "requested_dataset_contract_id": qe_dataset_contract_id or None,
                    "expected_dataset_contract_id": QE_DATASET_CONTRACT_ID,
                },
            )
        workspace = Path(qe_workspace_root).expanduser().resolve()
        if not workspace.is_dir():
            raise QELongTrendError(
                QELongTrendReason.NON_QE_SOURCE_REJECTED,
                f"QE workspace root does not exist: {workspace}",
            )
        root = Path(factor_data_dir).expanduser().resolve()
        if not root.is_dir():
            raise QELongTrendError(
                QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
                f"QE factor_data snapshot does not exist: {root}",
            )
        actual_identity = inspect_qe_snapshot_identity(root)
        requested_identity = (
            snapshot_identity.snapshot_id,
            snapshot_identity.manifest_sha256,
            snapshot_identity.start_date,
            snapshot_identity.end_date,
            snapshot_identity.lineage_parent_ids,
        )
        actual_identity_core = (
            actual_identity.snapshot_id,
            actual_identity.manifest_sha256,
            actual_identity.start_date,
            actual_identity.end_date,
            actual_identity.lineage_parent_ids,
        )
        if requested_identity != actual_identity_core:
            raise QELongTrendError(
                QELongTrendReason.DATASET_ROOT_IDENTITY_MISMATCH,
                "factor_data root does not match the supplied QE snapshot identity",
                context={
                    "requested_snapshot_id": snapshot_identity.snapshot_id,
                    "actual_snapshot_id": actual_identity.snapshot_id,
                    "requested_manifest_sha256": snapshot_identity.manifest_sha256,
                    "actual_manifest_sha256": actual_identity.manifest_sha256,
                },
            )
        self.factor_data_dir = root
        self.qe_workspace_root = workspace
        self.qe_dataset_contract_id = qe_dataset_contract_id
        self.snapshot_identity = actual_identity
        self._hdf_reader = hdf_reader or pd.read_hdf

    def load(
        self,
        *,
        start_date: str,
        end_date: str,
        instruments: Iterable[str] | None = None,
        include_sector: bool = True,
    ) -> QELongTrendDatasetFrames:
        start_ts = pd.Timestamp(start_date).normalize()
        end_ts = pd.Timestamp(end_date).normalize()
        try:
            require_qe_dataset_window(start_date=start_ts.date(), end_date=end_ts.date())
        except ValueError as exc:
            raise QELongTrendError(
                QELongTrendReason.DATASET_ROOT_IDENTITY_MISMATCH,
                str(exc),
                context={
                    "snapshot_id": self.snapshot_identity.snapshot_id,
                    "start_date": str(start_ts.date()),
                    "end_date": str(end_ts.date()),
                },
            ) from exc

        price_path = self._resolve_allowed_file(_PRICE_FILE, required=True)
        raw_prices = self._read_hdf(price_path, QELongTrendReason.DAILY_PV_SCHEMA_INVALID)
        prices = self._normalize_prices(raw_prices, start_ts, end_ts, instruments)
        del raw_prices

        sectors: pd.DataFrame | None = None
        sector_path: Path | None = None
        if include_sector:
            sector_path = self._resolve_allowed_file(_SECTOR_FILE, required=True)
            raw_sectors = self._read_hdf(sector_path, QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID)
            sectors = self._normalize_sectors(raw_sectors, start_ts, end_ts, instruments)
            del raw_sectors
        return QELongTrendDatasetFrames(
            prices=prices,
            sectors=sectors,
            price_path=price_path,
            sector_path=sector_path,
        )

    def verify_workspace_binding(self, *, include_sector: bool = True) -> dict[str, Path]:
        """Verify that a QE workspace is bound to the declared snapshot files.

        Historical batch evaluation loads one immutable dataset once and then
        evaluates many recorder workspaces.  This method validates each
        workspace's file identity without re-reading the full H5 payload.
        """

        paths = {_PRICE_FILE: self._resolve_allowed_file(_PRICE_FILE, required=True)}
        if include_sector:
            paths[_SECTOR_FILE] = self._resolve_allowed_file(_SECTOR_FILE, required=True)
        return paths

    def load_prices(
        self,
        *,
        start_date: str,
        end_date: str,
        instruments: Iterable[str] | None = None,
    ) -> pd.DataFrame:
        return self.load(
            start_date=start_date,
            end_date=end_date,
            instruments=instruments,
            include_sector=False,
        ).prices

    def _resolve_allowed_file(self, name: str, *, required: bool) -> Path:
        if name not in {_PRICE_FILE, _SECTOR_FILE}:
            raise QELongTrendError(
                QELongTrendReason.NON_QE_SOURCE_REJECTED,
                f"file {name!r} is outside the F-014 reader allowlist",
            )
        logical_path = self.factor_data_dir / name
        path = logical_path.resolve()
        if required and not path.is_file():
            reason = (
                QELongTrendReason.DAILY_PV_SCHEMA_INVALID
                if name == _PRICE_FILE
                else QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID
            )
            raise QELongTrendError(reason, f"required QE dataset file is missing: {path}")
        workspace_path = self.qe_workspace_root / name
        if required and not workspace_path.is_file():
            raise QELongTrendError(
                QELongTrendReason.DATASET_ROOT_IDENTITY_MISMATCH,
                f"QE workspace is not bound to required snapshot file: {workspace_path}",
            )
        if required:
            try:
                same_file = os.path.samefile(path, workspace_path)
            except OSError as exc:
                raise QELongTrendError(
                    QELongTrendReason.DATASET_ROOT_IDENTITY_MISMATCH,
                    f"failed to verify QE workspace snapshot binding for {name}: {exc}",
                ) from exc
            if not same_file:
                raise QELongTrendError(
                    QELongTrendReason.DATASET_ROOT_IDENTITY_MISMATCH,
                    f"QE workspace file {name} is not the registered snapshot file",
                    context={
                        "snapshot_file": str(path),
                        "workspace_file": str(workspace_path.resolve()),
                    },
                )
        return path

    def _read_hdf(self, path: Path, reason: QELongTrendReason) -> pd.DataFrame:
        try:
            frame = self._hdf_reader(path)
        except (OSError, ValueError, KeyError) as exc:
            raise QELongTrendError(reason, f"failed to read {path.name}: {exc}") from exc
        if not isinstance(frame, pd.DataFrame):
            raise QELongTrendError(reason, f"{path.name} must contain a pandas DataFrame")
        return frame

    @staticmethod
    def _normalize_index(frame: pd.DataFrame, *, reason: QELongTrendReason) -> pd.DataFrame:
        if not isinstance(frame.index, pd.MultiIndex):
            if {"datetime", "instrument"}.issubset(frame.columns):
                frame = frame.set_index(["datetime", "instrument"])
            else:
                raise QELongTrendError(
                    reason,
                    "dataset frame requires a (datetime, instrument) MultiIndex",
                )
        names = list(frame.index.names)
        if "datetime" not in names or "instrument" not in names:
            if len(names) == 2:
                frame = frame.copy(deep=False)
                frame.index = frame.index.set_names(["datetime", "instrument"])
            else:
                raise QELongTrendError(reason, f"invalid dataset index names: {names}")
        if not frame.index.is_unique:
            raise QELongTrendError(reason, "dataset index must be unique by datetime/instrument")

        reset = frame.reset_index()
        reset["datetime"] = pd.to_datetime(reset["datetime"], errors="coerce").dt.normalize()
        if reset["datetime"].isna().any():
            raise QELongTrendError(reason, "dataset contains invalid datetime values")
        try:
            reset["instrument"] = reset["instrument"].map(canonicalize_instrument)
        except ValueError as exc:
            raise QELongTrendError(reason, str(exc)) from exc
        normalized = reset.set_index(["datetime", "instrument"]).sort_index()
        if not normalized.index.is_unique:
            raise QELongTrendError(
                reason,
                "instrument canonicalization produced duplicate datetime/instrument rows",
            )
        return normalized

    def _normalize_prices(
        self,
        frame: pd.DataFrame,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        instruments: Iterable[str] | None,
    ) -> pd.DataFrame:
        frame = self._normalize_index(frame, reason=QELongTrendReason.DAILY_PV_SCHEMA_INVALID)
        columns = {str(column).lstrip("$").lower(): column for column in frame.columns}
        required = {"open", "close", "high", "low", "volume"}
        missing = sorted(required - set(columns))
        if missing:
            raise QELongTrendError(
                QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
                f"daily_pv.h5 is missing qfq price columns: {missing}",
            )
        selected = frame.loc[
            :,
            [
                columns["open"],
                columns["close"],
                columns["high"],
                columns["low"],
                columns["volume"],
            ],
        ].rename(
            columns={
                columns["open"]: "open_qfq",
                columns["close"]: "close_qfq",
                columns["high"]: "high_qfq",
                columns["low"]: "low_qfq",
                columns["volume"]: "volume_qfq",
            }
        )
        selected = self._slice(selected, start_ts, end_ts, instruments)
        for column in ("open_qfq", "close_qfq", "high_qfq", "low_qfq", "volume_qfq"):
            selected[column] = pd.to_numeric(selected[column], errors="coerce").astype("float64")
        for column in ("open_qfq", "close_qfq", "high_qfq", "low_qfq"):
            selected.loc[selected[column] <= 0.0, column] = np.nan
        selected.loc[selected["volume_qfq"] < 0.0, "volume_qfq"] = np.nan
        finite_ohlc = selected[["open_qfq", "close_qfq", "high_qfq", "low_qfq"]].notna().all(axis=1)
        invalid_ohlc = finite_ohlc & (
            (selected["high_qfq"] < selected["low_qfq"])
            | (selected["open_qfq"] > selected["high_qfq"])
            | (selected["open_qfq"] < selected["low_qfq"])
            | (selected["close_qfq"] > selected["high_qfq"])
            | (selected["close_qfq"] < selected["low_qfq"])
        )
        if bool(invalid_ohlc.any()):
            raise QELongTrendError(
                QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
                "daily_pv.h5 contains inconsistent qfq OHLC rows",
                context={"invalid_row_count": int(invalid_ohlc.sum())},
            )
        if selected.empty:
            raise QELongTrendError(
                QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
                "daily price selection is empty for the requested evaluation window",
            )
        return selected

    def _normalize_sectors(
        self,
        frame: pd.DataFrame,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        instruments: Iterable[str] | None,
    ) -> pd.DataFrame:
        frame = self._normalize_index(frame, reason=QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID)
        if "l2_code_id" not in frame.columns:
            raise QELongTrendError(
                QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID,
                "sector_data.h5 is missing l2_code_id",
            )
        selected = self._slice(frame.loc[:, ["l2_code_id"]], start_ts, end_ts, instruments)
        numeric = pd.to_numeric(selected["l2_code_id"], errors="coerce")
        finite = numeric.dropna().to_numpy(dtype="float64")
        if finite.size and not np.allclose(finite, np.rint(finite), rtol=0.0, atol=0.0):
            raise QELongTrendError(
                QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID,
                "l2_code_id must contain integer category ids",
            )
        if finite.size and bool((finite < -1.0).any()):
            raise QELongTrendError(
                QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID,
                "l2_code_id contains values below the registered -1 unknown sentinel",
            )
        selected = selected.copy()
        selected["l2_code_id"] = numeric.mask(numeric.eq(-1.0)).astype("Int16")
        return selected

    @staticmethod
    def _slice(
        frame: pd.DataFrame,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        instruments: Iterable[str] | None,
    ) -> pd.DataFrame:
        dates = frame.index.get_level_values("datetime")
        mask = (dates >= start_ts) & (dates <= end_ts)
        if instruments is not None:
            try:
                wanted = {canonicalize_instrument(value) for value in instruments}
            except ValueError as exc:
                raise QELongTrendError(QELongTrendReason.PREDICTION_SCHEMA_INVALID, str(exc)) from exc
            mask &= frame.index.get_level_values("instrument").isin(wanted)
        return frame.loc[mask].copy(deep=True)


def verify_outcome_snapshot_extension(
    *,
    feature_identity: QEDatasetSnapshotIdentity,
    outcome_identity: QEDatasetSnapshotIdentity,
    feature_prices: pd.DataFrame,
    outcome_prices: pd.DataFrame,
) -> SnapshotOverlapParityReceipt:
    """Prove same-snapshot or extension-only qfq OHLC overlap identity."""

    feature_start = pd.Timestamp(feature_identity.start_date).normalize()
    feature_end = pd.Timestamp(feature_identity.end_date).normalize()
    outcome_start = pd.Timestamp(outcome_identity.start_date).normalize()
    outcome_end = pd.Timestamp(outcome_identity.end_date).normalize()
    same_snapshot = (
        feature_identity.snapshot_id == outcome_identity.snapshot_id
        and feature_identity.manifest_sha256 == outcome_identity.manifest_sha256
        and feature_identity.start_date == outcome_identity.start_date
        and feature_identity.end_date == outcome_identity.end_date
    )
    if not same_snapshot:
        valid_lineage = feature_identity.snapshot_id in set(outcome_identity.lineage_parent_ids)
        if not (outcome_start <= feature_start and outcome_end > feature_end and valid_lineage):
            raise QELongTrendError(
                QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
                "outcome snapshot does not declare a strict extension of the feature snapshot",
                context={
                    "feature_snapshot_id": feature_identity.snapshot_id,
                    "outcome_snapshot_id": outcome_identity.snapshot_id,
                    "feature_window": [str(feature_start.date()), str(feature_end.date())],
                    "outcome_window": [str(outcome_start.date()), str(outcome_end.date())],
                    "lineage_parent_ids": list(outcome_identity.lineage_parent_ids),
                },
            )

    feature = _normalize_overlap_price_frame(feature_prices, feature_start, feature_end)
    outcome = _normalize_overlap_price_frame(outcome_prices, feature_start, feature_end)
    if feature.empty or outcome.empty:
        raise QELongTrendError(
            QELongTrendReason.SNAPSHOT_OVERLAP_EMPTY,
            "feature/outcome snapshot overlap contains no qfq OHLC rows",
            context={
                "feature_rows": int(len(feature)),
                "outcome_rows": int(len(outcome)),
            },
        )
    if not feature.index.equals(outcome.index) or list(feature.columns) != list(outcome.columns):
        raise QELongTrendError(
            QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
            "feature/outcome qfq OHLC overlap identities differ",
            context={
                "feature_rows": int(len(feature)),
                "outcome_rows": int(len(outcome)),
            },
        )
    feature_values = feature.to_numpy(dtype="float64")
    outcome_values = outcome.to_numpy(dtype="float64")
    if not np.array_equal(feature_values, outcome_values, equal_nan=True):
        difference = np.abs(feature_values - outcome_values)
        finite = difference[np.isfinite(difference)]
        raise QELongTrendError(
            QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
            "feature/outcome qfq OHLC overlap values differ",
            context={
                "max_abs_diff": float(finite.max()) if finite.size else None,
                "required_comparison": "exact_float64_equal_nan",
            },
        )
    value_hash = pd.util.hash_pandas_object(feature, index=True).to_numpy(dtype="uint64")
    parity_sha = canonical_sha256(
        {
            "feature_snapshot_id": feature_identity.snapshot_id,
            "outcome_snapshot_id": outcome_identity.snapshot_id,
            "feature_manifest_sha256": feature_identity.manifest_sha256,
            "outcome_manifest_sha256": outcome_identity.manifest_sha256,
            "overlap_start": feature_start.date().isoformat(),
            "overlap_end": feature_end.date().isoformat(),
            "columns": list(feature.columns),
            "row_hash_sha256": hashlib.sha256(value_hash.tobytes()).hexdigest(),
        }
    )
    return SnapshotOverlapParityReceipt(
        feature_snapshot_id=feature_identity.snapshot_id,
        outcome_snapshot_id=outcome_identity.snapshot_id,
        overlap_start=feature_start.date().isoformat(),
        overlap_end=feature_end.date().isoformat(),
        row_count=int(len(feature)),
        column_count=int(len(feature.columns)),
        overlap_price_parity_sha256=parity_sha,
        relation="same_snapshot" if same_snapshot else "verified_extension",
    )


def inspect_qe_snapshot_identity(
    factor_data_dir: str | os.PathLike[str],
) -> QEDatasetSnapshotIdentity:
    """Build the immutable QE snapshot identity from meta plus file content hashes."""

    root = Path(factor_data_dir).expanduser().resolve()
    if not root.is_dir():
        raise QELongTrendError(
            QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
            f"QE snapshot root does not exist: {root}",
        )
    meta_path = root / _META_FILE
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise QELongTrendError(
            QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
            f"failed to read QE snapshot meta.json: {exc}",
        ) from exc
    snapshot_id = str(meta.get("snapshot_id") or "").strip().lower()
    start_date = str(meta.get("start") or "")
    end_date = str(meta.get("end") or "")
    lineage_raw = meta.get("lineage_parent_ids", [])
    if not isinstance(lineage_raw, (list, tuple)):
        raise QELongTrendError(
            QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
            "meta.json lineage_parent_ids must be an array when present",
        )
    lineage_parent_ids = tuple(str(value).strip() for value in lineage_raw)
    if snapshot_id != QE_DATASET_CONTRACT_ID:
        raise QELongTrendError(
            QELongTrendReason.DATASET_ROOT_IDENTITY_MISMATCH,
            "meta.json snapshot_id differs from the deployed QE dataset contract",
            context={
                "meta_snapshot_id": snapshot_id,
                "qe_dataset_contract_id": QE_DATASET_CONTRACT_ID,
            },
        )
    if start_date != QE_DATASET_START_DATE.isoformat() or end_date != QE_DATASET_SIGNAL_END_DATE.isoformat():
        raise QELongTrendError(
            QELongTrendReason.DATASET_ROOT_IDENTITY_MISMATCH,
            "meta.json date window differs from the deployed QE dataset contract",
            context={
                "meta_window": [start_date, end_date],
                "contract_window": [
                    QE_DATASET_START_DATE.isoformat(),
                    QE_DATASET_SIGNAL_END_DATE.isoformat(),
                ],
            },
        )
    file_manifest: dict[str, dict[str, int | str]] = {}
    for name in _IDENTITY_FILES:
        path = (root / name).resolve()
        if not path.is_file():
            raise QELongTrendError(
                QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
                f"QE snapshot identity file is missing: {path}",
            )
        stat = path.stat()
        file_manifest[name] = {
            "size": int(stat.st_size),
            "sha256": _cached_file_sha256(str(path), int(stat.st_size), int(stat.st_mtime_ns)),
        }
    manifest_sha256 = canonical_sha256(
        {
            "snapshot_id": snapshot_id,
            "start_date": start_date,
            "end_date": end_date,
            "meta": meta,
            "files": file_manifest,
        }
    )
    return QEDatasetSnapshotIdentity(
        snapshot_id=snapshot_id,
        manifest_sha256=manifest_sha256,
        start_date=start_date,
        end_date=end_date,
        lineage_parent_ids=lineage_parent_ids,
    )


@lru_cache(maxsize=16)
def _cached_file_sha256(path_text: str, size: int, mtime_ns: int) -> str:
    del size, mtime_ns
    digest = hashlib.sha256()
    with Path(path_text).open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_overlap_price_frame(
    frame: pd.DataFrame,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.DataFrame:
    normalized = QELongTrendDatasetReader._normalize_index(
        frame,
        reason=QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
    )
    aliases = {str(column).lstrip("$").lower(): column for column in normalized.columns}
    required = ("open", "high", "low", "close")
    if not set(required).issubset(aliases):
        qfq_required = ("open_qfq", "high_qfq", "low_qfq", "close_qfq")
        if not set(qfq_required).issubset(normalized.columns):
            raise QELongTrendError(
                QELongTrendReason.DAILY_PV_SCHEMA_INVALID,
                "snapshot overlap parity requires qfq OHLC columns",
            )
        selected = normalized.loc[:, list(qfq_required)].rename(
            columns={name: name.removesuffix("_qfq") for name in qfq_required}
        )
    else:
        selected = normalized.loc[:, [aliases[name] for name in required]].rename(
            columns={aliases[name]: name for name in required}
        )
    dates = selected.index.get_level_values("datetime")
    selected = selected.loc[(dates >= start_ts) & (dates <= end_ts)].sort_index()
    for column in required:
        selected[column] = pd.to_numeric(selected[column], errors="coerce").astype("float64")
    return selected
