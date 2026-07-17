"""Read-only resolver for immutable QE artifacts in Prediction Store."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from backend.services.model_store import ModelStoreService, PredictionStoreError

from .exceptions import DataSourceError


ARTIFACT_TYPES = {
    "pred.pkl": "prediction",
    "label.pkl": "label",
}
_LOOP_NAME_RE = re.compile(r"^Loop(?P<index>[1-9][0-9]*)$")


@dataclass(frozen=True)
class ResolvedPredictionStoreArtifact:
    """Trusted immutable artifact resolved without creating a second copy."""

    path: Path
    run_key: str
    artifact_name: str
    artifact_type: str
    uri: str
    sha256: str
    size_bytes: int
    row_count: int

    def source_info(self) -> dict[str, Any]:
        return {
            "source": "prediction_store",
            "run_key": self.run_key,
            "artifact_name": self.artifact_name,
            "artifact_type": self.artifact_type,
            "uri": self.uri,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "row_count": self.row_count,
            "path": str(self.path),
        }


class PredictionStoreArtifactResolver:
    """Resolve task/LoopN artifacts from the existing content-addressed store.

    A missing manifest or missing requested artifact returns ``None`` so the
    caller may use its explicitly configured workspace fallback.  A present but
    corrupt/untrusted manifest raises instead; integrity failures must never be
    hidden by silently downloading a different copy from a workspace.
    """

    def __init__(self, model_store: ModelStoreService | None = None) -> None:
        self._model_store = model_store or ModelStoreService()

    def resolve(
        self,
        *,
        loop_ref: str,
        artifact_name: str,
    ) -> ResolvedPredictionStoreArtifact | None:
        artifact_type = ARTIFACT_TYPES.get(artifact_name)
        if artifact_type is None:
            raise DataSourceError(f"Prediction Store lookup refused unsupported artifact: {artifact_name!r}")

        task_id, loop_name = _parse_loop_ref(loop_ref)
        match = _LOOP_NAME_RE.fullmatch(loop_name)
        if match is None:
            return None
        loop_index = int(match.group("index"))
        run_key = f"{task_id}_L{loop_index}"

        try:
            resolved = self._model_store.resolve_archive_manifest(
                run_id=run_key,
                task_id=task_id,
                loop_index=loop_index,
                verify_sha256=True,
            )
        except (PredictionStoreError, OSError) as exc:
            raise DataSourceError(
                f"Prediction Store resolution failed for {loop_ref}/{artifact_name}: {type(exc).__name__}: {exc}"
            ) from exc

        status = str(resolved.get("status") or "missing")
        if status == "missing":
            return None
        if status == "corrupt":
            raise DataSourceError(
                f"Prediction Store manifest is corrupt for {loop_ref}/{artifact_name}: {resolved.get('errors') or []}"
            )

        manifest = resolved.get("manifest")
        if not isinstance(manifest, Mapping):
            raise DataSourceError(f"Prediction Store returned no trusted manifest for {loop_ref}/{artifact_name}")

        item = _find_artifact(manifest, artifact_type=artifact_type)
        if item is None:
            selected_run_key = str(resolved.get("selected_run_key") or run_key)
            try:
                raw_manifest = self._model_store.artifact_store.load_manifest(selected_run_key)
            except (PredictionStoreError, OSError) as exc:
                raise DataSourceError(
                    "Prediction Store raw manifest inspection failed for "
                    f"{loop_ref}/{artifact_name}: {type(exc).__name__}: {exc}"
                ) from exc
            if (
                isinstance(raw_manifest, Mapping)
                and _find_artifact(raw_manifest, artifact_type=artifact_type) is not None
            ):
                raise DataSourceError(
                    "Prediction Store target artifact is present but invalid for "
                    f"{loop_ref}/{artifact_name}: {resolved.get('errors') or []}"
                )
            return None
        _validate_hmm_artifact_item(
            item,
            loop_ref=loop_ref,
            artifact_name=artifact_name,
        )

        manifest_uri = str(manifest.get("uri") or manifest.get("mlflow_artifact_uri") or run_key)
        try:
            path = self._model_store.artifact_store.resolve_artifact_path(
                manifest_uri,
                artifact_type=artifact_type,
                artifact_name=artifact_name,
            )
        except (PredictionStoreError, OSError) as exc:
            raise DataSourceError(
                "Prediction Store artifact path is unreadable for "
                f"{loop_ref}/{artifact_name}: {type(exc).__name__}: {exc}"
            ) from exc

        return ResolvedPredictionStoreArtifact(
            path=path,
            run_key=str(resolved.get("selected_run_key") or run_key),
            artifact_name=artifact_name,
            artifact_type=artifact_type,
            uri=str(item.get("uri") or manifest_uri),
            sha256=str(item["sha256"]),
            size_bytes=int(item["size_bytes"]),
            row_count=int(item["row_count"]),
        )


def _parse_loop_ref(loop_ref: str) -> tuple[str, str]:
    value = str(loop_ref or "").strip()
    parts = value.split("/")
    if len(parts) != 2 or any(not part or part in {".", ".."} for part in parts):
        raise DataSourceError(f"Invalid base_loop_ref format: {loop_ref!r}; expected '<task_id>/<loop_name>'")
    if any("\\" in part or "/" in part for part in parts):
        raise DataSourceError(f"Unsafe base_loop_ref path segment: {loop_ref!r}")
    return parts[0], parts[1]


def _find_artifact(
    manifest: Mapping[str, Any],
    *,
    artifact_type: str,
) -> Mapping[str, Any] | None:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        return None
    for item in artifacts:
        if isinstance(item, Mapping) and str(item.get("artifact_type") or "") == artifact_type:
            return item
    return None


def _validate_hmm_artifact_item(
    item: Mapping[str, Any],
    *,
    loop_ref: str,
    artifact_name: str,
) -> None:
    errors: list[str] = []
    if str(item.get("artifact_name") or "") != artifact_name:
        errors.append(f"artifact_name={item.get('artifact_name')!r}")
    if str(item.get("collection_status") or "") != "available":
        errors.append(f"collection_status={item.get('collection_status')!r}")
    if str(item.get("parser_status") or "") != "parsed":
        errors.append(f"parser_status={item.get('parser_status')!r}")
    try:
        row_count = int(item.get("row_count"))
    except (TypeError, ValueError):
        row_count = 0
    if row_count <= 0:
        errors.append(f"row_count={item.get('row_count')!r}")
    if errors:
        raise DataSourceError(
            "Prediction Store artifact does not meet the HMM trust contract for "
            f"{loop_ref}/{artifact_name}: {', '.join(errors)}"
        )
