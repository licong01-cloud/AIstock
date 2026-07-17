"""Strict coefficient parsing and content-addressed candidate identity."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Protocol

from .errors import ArtifactHashMismatchError, ArtifactManifestInvalidError
from .models import (
    AssetTrustLevel,
    CandidateCoverage,
    CandidateManifest,
    CandidatePreview,
    CandidateSourceType,
    CoefficientStats,
    normalize_asset_path,
)
from .qe_asset_reader import QEExperimentAssetReader


class _DuplicateKeyError(ValueError):
    pass


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


class CandidateArtifactParser:
    """Parse precomputed HMM coefficients without generating or mutating assets."""

    def preview_bytes(
        self,
        payload_bytes: bytes,
        *,
        source_type: CandidateSourceType,
        source_ref: dict[str, Any],
        artifact_uri: str,
        expected_sha256: str | None = None,
        trust_level: AssetTrustLevel = AssetTrustLevel.TRUSTED_COMPUTATIONAL_INPUT,
    ) -> CandidatePreview:
        if not payload_bytes:
            raise ArtifactManifestInvalidError("coefficient artifact is empty")
        actual_sha256 = hashlib.sha256(payload_bytes).hexdigest()
        if expected_sha256 and expected_sha256.lower() != actual_sha256:
            raise ArtifactHashMismatchError(
                "coefficient artifact hash does not match its manifest",
                context={"expected_sha256": expected_sha256, "actual_sha256": actual_sha256},
            )
        if (
            source_type is CandidateSourceType.QE_EXPERIMENT
            and trust_level is not AssetTrustLevel.TRUSTED_COMPUTATIONAL_INPUT
        ):
            raise ArtifactManifestInvalidError(
                "unverified QE evidence cannot be registered as a computational candidate",
                context={"trust_level": trust_level.value},
            )
        try:
            text = payload_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ArtifactManifestInvalidError("coefficient artifact must be UTF-8 JSON") from exc
        try:
            payload = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
        except (json.JSONDecodeError, _DuplicateKeyError) as exc:
            raise ArtifactManifestInvalidError(
                "coefficient artifact is not valid deterministic JSON",
                context={"error": str(exc)},
            ) from exc
        if not isinstance(payload, dict):
            raise ArtifactManifestInvalidError("coefficient artifact root must be a JSON object")

        daily = payload.get("daily_coefficients")
        stock_sector_map = payload.get("stock_sector_map")
        if not isinstance(daily, dict) or not daily:
            raise ArtifactManifestInvalidError("daily_coefficients must be a non-empty object")
        if not isinstance(stock_sector_map, dict) or not stock_sector_map:
            raise ArtifactManifestInvalidError("stock_sector_map must be a non-empty object")

        parsed_dates: list[date] = []
        sector_counts: list[int] = []
        coefficients: list[float] = []
        prior_date: date | None = None
        for raw_date, sector_values in daily.items():
            try:
                parsed_date = date.fromisoformat(str(raw_date))
            except ValueError as exc:
                raise ArtifactManifestInvalidError(
                    "daily coefficient date must be ISO YYYY-MM-DD",
                    context={"date": raw_date},
                ) from exc
            if parsed_date.isoformat() != raw_date:
                raise ArtifactManifestInvalidError(
                    "daily coefficient dates must already be normalized ISO dates",
                    context={"date": raw_date},
                )
            if prior_date is not None and parsed_date <= prior_date:
                raise ArtifactManifestInvalidError(
                    "daily coefficient dates must be strictly increasing",
                    context={"date": raw_date},
                )
            prior_date = parsed_date
            parsed_dates.append(parsed_date)
            if not isinstance(sector_values, dict) or not sector_values:
                raise ArtifactManifestInvalidError(
                    "each daily coefficient entry must contain at least one sector",
                    context={"date": raw_date},
                )
            sector_counts.append(len(sector_values))
            for sector_code, raw_value in sector_values.items():
                if not isinstance(sector_code, str) or not sector_code.strip():
                    raise ArtifactManifestInvalidError("sector codes must be non-empty strings")
                if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                    raise ArtifactManifestInvalidError(
                        "coefficients must be numeric",
                        context={"date": raw_date, "sector": sector_code},
                    )
                value = float(raw_value)
                if not math.isfinite(value) or value <= 0:
                    raise ArtifactManifestInvalidError(
                        "coefficients must be finite and greater than zero",
                        context={"date": raw_date, "sector": sector_code},
                    )
                coefficients.append(value)

        for symbol, sector_code in stock_sector_map.items():
            if not isinstance(symbol, str) or not symbol.strip():
                raise ArtifactManifestInvalidError("stock_sector_map symbols must be non-empty strings")
            if not isinstance(sector_code, str) or not sector_code.strip():
                raise ArtifactManifestInvalidError(
                    "stock_sector_map sector codes must be non-empty strings",
                    context={"symbol": symbol},
                )

        manifest = CandidateManifest(
            source_type=source_type,
            source_ref=dict(source_ref),
            artifact_uri=artifact_uri,
            artifact_sha256=actual_sha256,
            size_bytes=len(payload_bytes),
            detected_format="hmm_sector_coefficients_legacy_v1",
            coverage=CandidateCoverage(
                start_date=parsed_dates[0],
                end_date=parsed_dates[-1],
                date_count=len(parsed_dates),
                sector_count_min=min(sector_counts),
                sector_count_max=max(sector_counts),
                stock_sector_map_count=len(stock_sector_map),
            ),
            coefficient_stats=CoefficientStats(min=min(coefficients), max=max(coefficients)),
        )
        return CandidatePreview(
            candidate_id=manifest.candidate_id,
            manifest_hash=manifest.manifest_hash,
            manifest=manifest,
        )


class SnapshotCoefficientProvider(Protocol):
    """Read-only snapshot metadata/artifact boundary supplied by existing services."""

    def get_snapshot_metadata(self, snapshot_id: str) -> Mapping[str, Any]: ...

    def read_coefficient_bytes(self, snapshot_id: str, artifact_name: str) -> bytes: ...


class CandidateArtifactResolver:
    """Resolve all approved coefficient sources without generation or configuration writes."""

    def __init__(
        self,
        *,
        parser: CandidateArtifactParser | None = None,
        artifact_roots: Mapping[str, str | Path] | None = None,
        snapshot_provider: SnapshotCoefficientProvider | None = None,
        qe_asset_reader: QEExperimentAssetReader | None = None,
        max_artifact_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if max_artifact_bytes < 1:
            raise ValueError("max_artifact_bytes must be positive")
        self._parser = parser or CandidateArtifactParser()
        self._artifact_roots = {
            str(alias): Path(os.path.abspath(path))
            for alias, path in dict(artifact_roots or {}).items()
        }
        self._snapshot_provider = snapshot_provider
        self._qe_asset_reader = qe_asset_reader
        self._max_artifact_bytes = max_artifact_bytes

    def preview_existing_snapshot(
        self,
        *,
        snapshot_id: str,
        artifact_name: str,
    ) -> CandidatePreview:
        if self._snapshot_provider is None:
            raise ArtifactManifestInvalidError("snapshot coefficient provider is not configured")
        metadata = dict(self._snapshot_provider.get_snapshot_metadata(snapshot_id))
        status = str(metadata.get("status") or "").lower()
        if status not in {"completed", "ready"}:
            raise ArtifactManifestInvalidError(
                "snapshot must be completed or ready before coefficient inspection",
                context={"snapshot_id": snapshot_id, "status": status},
            )
        safe_name = normalize_asset_path(artifact_name)
        data = self._snapshot_provider.read_coefficient_bytes(snapshot_id, safe_name)
        self._assert_size(data)
        return self._parser.preview_bytes(
            data,
            source_type=CandidateSourceType.EXISTING_SNAPSHOT,
            source_ref={
                "snapshot_id": snapshot_id,
                "config_id": metadata.get("config_id"),
                "artifact_name": safe_name,
            },
            artifact_uri=f"snapshot://{snapshot_id}/{safe_name}",
            expected_sha256=metadata.get("artifact_sha256"),
        )

    def preview_configured_local(
        self,
        *,
        root_alias: str,
        relative_path: str,
    ) -> CandidatePreview:
        if root_alias not in self._artifact_roots:
            raise ArtifactManifestInvalidError(
                "configured coefficient root alias is not available",
                context={"root_alias": root_alias},
            )
        safe_path = normalize_asset_path(relative_path)
        root = self._artifact_roots[root_alias]
        self._assert_no_reparse(root)
        target = root.joinpath(*safe_path.split("/"))
        self._assert_contained(root, target)
        self._assert_no_reparse_path(root, target)
        try:
            data = target.read_bytes()
        except OSError as exc:
            raise ArtifactManifestInvalidError(
                "configured coefficient artifact cannot be read",
                context={"root_alias": root_alias, "relative_path": safe_path},
            ) from exc
        self._assert_size(data)
        return self._parser.preview_bytes(
            data,
            source_type=CandidateSourceType.CONFIGURED_LOCAL,
            source_ref={"root_alias": root_alias, "relative_path": safe_path},
            artifact_uri=f"configured-local://{root_alias}/{safe_path}",
        )

    async def preview_qe_experiment(
        self,
        *,
        task_id: str,
        loop_name: str,
        relative_path: str,
    ) -> CandidatePreview:
        if self._qe_asset_reader is None:
            raise ArtifactManifestInvalidError("QE asset reader is not configured")
        safe_path = normalize_asset_path(relative_path)
        entry = await self._qe_asset_reader.stat_asset(task_id, loop_name, safe_path)
        if entry.trust_level is not AssetTrustLevel.TRUSTED_COMPUTATIONAL_INPUT:
            raise ArtifactManifestInvalidError(
                "QE coefficient asset is inspection-only and cannot enter evaluation",
                context={"task_id": task_id, "loop_name": loop_name, "relative_path": safe_path},
            )
        if not entry.sha256 or not entry.schema_version or not entry.parser_contract:
            raise ArtifactManifestInvalidError(
                "QE coefficient asset lacks the trusted manifest/parser receipt",
                context={"task_id": task_id, "loop_name": loop_name, "relative_path": safe_path},
            )
        content = await self._qe_asset_reader.read_asset(
            task_id,
            loop_name,
            safe_path,
            declared_entry=entry,
        )
        self._assert_size(content.data)
        return self._parser.preview_bytes(
            content.data,
            source_type=CandidateSourceType.QE_EXPERIMENT,
            source_ref={
                "task_id": task_id,
                "loop_name": loop_name,
                "asset_path": safe_path,
                "schema_version": entry.schema_version,
                "parser_contract": entry.parser_contract,
            },
            artifact_uri=f"qe://{task_id}/{loop_name}/{safe_path}",
            expected_sha256=entry.sha256,
            trust_level=content.receipt.trust_level,
        )

    def _assert_size(self, data: bytes) -> None:
        if len(data) > self._max_artifact_bytes:
            raise ArtifactManifestInvalidError(
                "coefficient artifact exceeds the configured size limit",
                context={"size_bytes": len(data), "max_artifact_bytes": self._max_artifact_bytes},
            )

    @staticmethod
    def _assert_contained(root: Path, target: Path) -> None:
        root_text = os.path.normcase(os.path.abspath(root))
        target_text = os.path.normcase(os.path.abspath(target))
        try:
            common = os.path.normcase(os.path.commonpath((root_text, target_text)))
        except ValueError as exc:
            raise ArtifactManifestInvalidError("configured coefficient path crosses filesystem roots") from exc
        if common != root_text:
            raise ArtifactManifestInvalidError("configured coefficient path escapes its approved root")

    @classmethod
    def _assert_no_reparse_path(cls, root: Path, target: Path) -> None:
        current = root
        relative_parts = target.relative_to(root).parts
        for part in relative_parts:
            current = current / part
            if current.exists() or current.is_symlink():
                cls._assert_no_reparse(current)

    @staticmethod
    def _assert_no_reparse(path: Path) -> None:
        try:
            result = path.lstat()
        except OSError as exc:
            raise ArtifactManifestInvalidError("configured coefficient path is unavailable") from exc
        attributes = getattr(result, "st_file_attributes", 0)
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        if path.is_symlink() or bool(attributes & reparse_flag):
            raise ArtifactManifestInvalidError("configured coefficient paths cannot traverse symlinks/reparse points")
