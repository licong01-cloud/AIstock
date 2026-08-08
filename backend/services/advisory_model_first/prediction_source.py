from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.model_store.artifact_store import PredictionArtifactStore, PredictionStoreError


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class ExactPredictionSource:
    """Resolve only explicitly named Prediction Store runs and artifacts."""

    def __init__(self, prediction_store_root: str | Path) -> None:
        self.store = PredictionArtifactStore(prediction_store_root)

    def describe(self, run_id: str) -> PredictionArtifactDescriptor:
        try:
            manifest = self.store.load_manifest(run_id)
        except (PredictionStoreError, OSError) as exc:
            raise AdvisoryModelFirstError(
                "prediction manifest cannot be read",
                reason_code="ADVISORY_MODEL_PREDICTION_MANIFEST_MISSING",
                context={"run_id": run_id, "error_type": type(exc).__name__},
            ) from exc
        matches = [
            item
            for item in manifest.get("artifacts", [])
            if item.get("artifact_type") == "prediction" or item.get("artifact_name") == "pred.pkl"
        ]
        if len(matches) != 1:
            raise AdvisoryModelFirstError(
                "prediction manifest must contain exactly one pred.pkl artifact",
                reason_code="ADVISORY_MODEL_PREDICTION_MANIFEST_MISSING",
                context={"run_id": run_id, "match_count": len(matches)},
            )
        item = matches[0]
        try:
            path = self.store.resolve_artifact_path(run_id, artifact_type="prediction", artifact_name="pred.pkl")
        except (PredictionStoreError, OSError) as exc:
            raise AdvisoryModelFirstError(
                "prediction artifact cannot be resolved",
                reason_code="ADVISORY_MODEL_PREDICTION_MANIFEST_MISSING",
                context={"run_id": run_id, "error_type": type(exc).__name__},
            ) from exc
        expected_sha = str(item.get("sha256") or "").lower()
        actual_sha = sha256_file(path)
        if actual_sha != expected_sha:
            raise AdvisoryModelFirstError(
                "prediction artifact hash does not match its manifest",
                reason_code="ADVISORY_MODEL_PREDICTION_HASH_MISMATCH",
                context={"run_id": run_id, "expected_sha256": expected_sha, "actual_sha256": actual_sha},
            )
        return PredictionArtifactDescriptor(
            run_id=run_id,
            run_key=str(manifest.get("run_key_safe") or manifest.get("run_key") or run_id),
            artifact_uri=str(item.get("uri") or ""),
            artifact_sha256=expected_sha,
            size_bytes=int(item.get("size_bytes") or path.stat().st_size),
            row_count=int(item.get("row_count") or 0),
            date_start=str(item.get("date_start") or ""),
            date_end=str(item.get("date_end") or ""),
        )

    def describe_all(self, run_ids: Iterable[str]) -> dict[str, PredictionArtifactDescriptor]:
        return {run_id: self.describe(run_id) for run_id in run_ids}

    def load_scores(
        self,
        run_id: str,
        *,
        decision_dates: Iterable[pd.Timestamp] | None = None,
        verify_artifact: bool = True,
    ) -> pd.DataFrame:
        descriptor = self.describe(run_id) if verify_artifact else None
        path = self.store.resolve_artifact_path(run_id, artifact_type="prediction", artifact_name="pred.pkl")
        try:
            raw = pd.read_pickle(path)
        except Exception as exc:
            raise AdvisoryModelFirstError(
                "prediction artifact cannot be deserialized",
                reason_code="ADVISORY_MODEL_PREDICTION_MANIFEST_MISSING",
                context={"run_id": run_id, "error_type": type(exc).__name__},
            ) from exc
        frame = _normalize_prediction_frame(raw, run_id=run_id)
        if decision_dates is not None:
            wanted = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize()
            frame = frame[frame["trade_date"].isin(wanted)].copy()
        if frame.empty:
            raise AdvisoryModelFirstError(
                "prediction artifact has no rows in the requested decision range",
                reason_code="ADVISORY_MODEL_PREDICTION_MANIFEST_MISSING",
                context={
                    "run_id": run_id,
                    "artifact_sha256": descriptor.artifact_sha256 if descriptor is not None else None,
                },
            )
        return frame


def _normalize_prediction_frame(raw: object, *, run_id: str) -> pd.DataFrame:
    if isinstance(raw, pd.Series):
        raw = raw.to_frame("score")
    if not isinstance(raw, pd.DataFrame) or not isinstance(raw.index, pd.MultiIndex):
        raise AdvisoryModelFirstError(
            "prediction artifact must be a DataFrame with a MultiIndex",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"run_id": run_id, "actual_type": type(raw).__name__},
        )
    if set(raw.index.names) != {"datetime", "instrument"}:
        raise AdvisoryModelFirstError(
            "prediction artifact index must be datetime,instrument",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"run_id": run_id, "index_names": list(raw.index.names)},
        )
    if "score" not in raw.columns:
        raise AdvisoryModelFirstError(
            "prediction artifact is missing score",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"run_id": run_id, "columns": list(map(str, raw.columns))},
        )
    frame = raw[["score"]].reset_index()
    frame["trade_date"] = pd.to_datetime(frame.pop("datetime")).dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str).str.upper()
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    bad = ~np.isfinite(frame["score"].to_numpy(dtype=float, copy=False))
    if bad.any():
        raise AdvisoryModelFirstError(
            "prediction artifact contains non-finite scores",
            reason_code="ADVISORY_MODEL_PREDICTION_HASH_MISMATCH",
            context={"run_id": run_id, "non_finite_count": int(bad.sum())},
        )
    if frame.duplicated(["trade_date", "instrument"]).any():
        raise AdvisoryModelFirstError(
            "prediction artifact contains duplicate date-symbol rows",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"run_id": run_id},
        )
    return frame[["trade_date", "instrument", "score"]].sort_values(
        ["trade_date", "instrument"]
    ).reset_index(drop=True)


def validate_prediction_descriptors(
    expected_run_ids: Iterable[str],
    descriptors: Mapping[str, PredictionArtifactDescriptor],
) -> None:
    expected = tuple(expected_run_ids)
    missing = sorted(set(expected) - set(descriptors))
    extra = sorted(set(descriptors) - set(expected))
    if missing or extra:
        raise AdvisoryModelFirstError(
            "prediction descriptor roster does not match the frozen request",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"missing_run_ids": missing, "extra_run_ids": extra},
        )
