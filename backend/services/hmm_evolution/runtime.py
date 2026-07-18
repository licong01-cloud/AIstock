"""Runtime wiring for the isolated HMM evolution research service.

The module deliberately exposes only read-only QE/snapshot adapters and the
``hmm_evolution.*`` repository.  It does not start a worker, scheduler, model
training flow, or any production trading integration at import time.
"""

from __future__ import annotations

import asyncio
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote

import httpx
from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceFileNotFound,
)

from .candidate_artifact import CandidateArtifactResolver, SnapshotCoefficientProvider
from .errors import (
    ArtifactManifestInvalidError,
    InvalidSpecError,
    QEAssetUnavailableError,
    RuntimeDisabledError,
)
from .executor import HMMEvaluationExecutor
from .input_adapter import HMMEvaluationInputAdapter
from .models import normalize_asset_path
from .qe_asset_reader import QEExperimentAssetReader, WorkspaceReadClient
from .repository import HMMEvolutionRepository
from .service import HMMEvolutionService


RUNTIME_MODES = frozenset({"disabled", "api_only", "api_worker"})


class ManagedQEWorkspaceReadClient(WorkspaceReadClient):
    """Open a short-lived client on the task/loop's authoritative QE node."""

    @staticmethod
    async def _client_for(task_id: str, loop_id: str) -> QEWorkspaceClient:
        return await asyncio.to_thread(QEWorkspaceClient.for_task_loop, task_id, loop_id)

    async def list_workspace_files(self, task_id: str, loop_id: str) -> Mapping[str, Any]:
        async with await self._client_for(task_id, loop_id) as client:
            return await client.list_workspace_files(task_id, loop_id)

    async def stat_workspace_file(
        self,
        task_id: str,
        loop_id: str,
        file_path: str,
    ) -> Mapping[str, Any]:
        async with await self._client_for(task_id, loop_id) as client:
            return await client.stat_workspace_file(task_id, loop_id, file_path)

    async def download_workspace_file_bytes(
        self,
        task_id: str,
        loop_id: str,
        file_path: str,
    ) -> bytes:
        async with await self._client_for(task_id, loop_id) as client:
            return await client.download_workspace_file_bytes(task_id, loop_id, file_path)

    async def open_workspace_file_range(
        self,
        task_id: str,
        loop_id: str,
        file_path: str,
        *,
        start: int,
        end: int,
    ) -> tuple[httpx.AsyncClient, httpx.Response]:
        """Open one bounded HTTP Range without buffering the remote asset."""

        qe_client = await self._client_for(task_id, loop_id)
        client = qe_client.client
        safe_task = quote(task_id, safe="")
        safe_loop = quote(QEWorkspaceClient._to_rdagent_loop_id(task_id, loop_id), safe="")
        safe_path = quote(normalize_asset_path(file_path), safe="/")
        url = f"{qe_client.base_url}/tasks/{safe_task}/loops/{safe_loop}/files/{safe_path}"
        request = client.build_request("GET", url, headers={"Range": f"bytes={start}-{end}"})
        try:
            response = await client.send(request, stream=True)
            if response.status_code == 404:
                await response.aclose()
                await client.aclose()
                raise QEWorkspaceFileNotFound(task_id, loop_id, file_path, url)
            if response.status_code != 206:
                await response.aclose()
                await client.aclose()
                raise QEAssetUnavailableError(
                    "QE node did not honor the required bounded Range request",
                    context={
                        "task_id": task_id,
                        "loop_name": loop_id,
                        "relative_path": file_path,
                        "http_status": response.status_code,
                    },
                )
            return client, response
        except Exception:
            if not client.is_closed:
                await client.aclose()
            raise


class ReadOnlySnapshotCoefficientProvider(SnapshotCoefficientProvider):
    """Read snapshot identity and an existing sibling coefficient artifact only."""

    def get_snapshot_metadata(self, snapshot_id: str) -> Mapping[str, Any]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT snapshot_id, config_id, status, model_path
                    FROM model_train_snapshots
                    WHERE snapshot_id = %s
                    """,
                    (snapshot_id,),
                )
                row = cursor.fetchone()
        if row is None:
            raise ArtifactManifestInvalidError(
                "snapshot does not exist",
                context={"snapshot_id": snapshot_id},
            )
        return dict(row)

    def read_coefficient_bytes(self, snapshot_id: str, artifact_name: str) -> bytes:
        metadata = dict(self.get_snapshot_metadata(snapshot_id))
        safe_name = normalize_asset_path(artifact_name)
        model_path = Path(str(metadata.get("model_path") or ""))
        if not model_path.is_absolute():
            raise ArtifactManifestInvalidError(
                "snapshot model path is not an absolute local path",
                context={"snapshot_id": snapshot_id},
            )
        model_dir = model_path.parent
        target = model_dir.joinpath(*safe_name.split("/"))
        try:
            if os.path.commonpath((str(model_dir.resolve()), str(target.resolve()))) != str(
                model_dir.resolve()
            ):
                raise ArtifactManifestInvalidError(
                    "snapshot coefficient artifact escapes the snapshot directory",
                    context={"snapshot_id": snapshot_id, "artifact_name": safe_name},
                )
        except OSError as exc:
            raise ArtifactManifestInvalidError(
                "snapshot coefficient path cannot be resolved",
                context={"snapshot_id": snapshot_id, "artifact_name": safe_name},
            ) from exc
        self._assert_plain_file(model_dir)
        self._assert_plain_file(target)
        try:
            return target.read_bytes()
        except OSError as exc:
            raise ArtifactManifestInvalidError(
                "snapshot coefficient artifact cannot be read",
                context={"snapshot_id": snapshot_id, "artifact_name": safe_name},
            ) from exc

    @staticmethod
    def _assert_plain_file(path: Path) -> None:
        try:
            info = path.lstat()
        except OSError as exc:
            raise ArtifactManifestInvalidError("snapshot artifact path is unavailable") from exc
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        file_attributes = int(getattr(info, "st_file_attributes", 0))
        if path.is_symlink() or file_attributes & reparse_flag:
            raise ArtifactManifestInvalidError(
                "snapshot artifact paths may not traverse links or reparse points"
            )


@dataclass(frozen=True)
class HMMEvolutionRuntime:
    repository: HMMEvolutionRepository
    service: HMMEvolutionService
    qe_asset_reader: QEExperimentAssetReader
    qe_read_client: ManagedQEWorkspaceReadClient
    artifact_resolver: CandidateArtifactResolver
    input_adapter: HMMEvaluationInputAdapter
    executor: HMMEvaluationExecutor


def runtime_mode() -> str:
    mode = os.getenv("HMM_EVOLUTION_RUNTIME_MODE", "disabled").strip().lower()
    if mode not in RUNTIME_MODES:
        raise InvalidSpecError(
            "unsupported HMM evolution runtime mode",
            context={"runtime_mode": mode},
        )
    return mode


def require_api_runtime() -> str:
    mode = runtime_mode()
    if mode not in {"api_only", "api_worker"}:
        raise RuntimeDisabledError(
            "HMM evolution API runtime is disabled",
            context={"runtime_mode": mode, "required_modes": ["api_only", "api_worker"]},
        )
    return mode


def require_worker_runtime() -> str:
    mode = runtime_mode()
    if mode != "api_worker":
        raise RuntimeDisabledError(
            "HMM evolution worker runtime is disabled",
            context={"runtime_mode": mode, "required_mode": "api_worker"},
        )
    return mode


def build_runtime() -> HMMEvolutionRuntime:
    artifact_roots = _artifact_roots_from_env()
    qe_client = ManagedQEWorkspaceReadClient()
    qe_reader = QEExperimentAssetReader(
        qe_client,
        max_read_bytes=_positive_int_env(
            "HMM_EVOLUTION_QE_MAX_READ_BYTES",
            64 * 1024 * 1024,
        ),
    )
    resolver = CandidateArtifactResolver(
        artifact_roots=artifact_roots,
        snapshot_provider=ReadOnlySnapshotCoefficientProvider(),
        qe_asset_reader=qe_reader,
        max_artifact_bytes=_positive_int_env(
            "HMM_EVOLUTION_CANDIDATE_MAX_BYTES",
            64 * 1024 * 1024,
        ),
    )
    repository = HMMEvolutionRepository()
    input_adapter = HMMEvaluationInputAdapter(candidate_resolver=resolver)
    service = HMMEvolutionService(
        repository,
        artifact_resolver=resolver,
        input_adapter=input_adapter,
    )
    return HMMEvolutionRuntime(
        repository=repository,
        service=service,
        qe_asset_reader=qe_reader,
        qe_read_client=qe_client,
        artifact_resolver=resolver,
        input_adapter=input_adapter,
        executor=HMMEvaluationExecutor(input_adapter),
    )


def _artifact_roots_from_env() -> dict[str, str]:
    raw = os.getenv("HMM_EVOLUTION_ARTIFACT_ROOTS_JSON", "{}").strip() or "{}"
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise InvalidSpecError("HMM evolution artifact roots JSON is invalid") from exc
    if not isinstance(payload, dict):
        raise InvalidSpecError("HMM evolution artifact roots must be a JSON object")
    roots: dict[str, str] = {}
    for raw_alias, raw_path in payload.items():
        alias = str(raw_alias or "").strip()
        path = str(raw_path or "").strip()
        if not alias or not path:
            raise InvalidSpecError("HMM evolution artifact root aliases and paths must be non-empty")
        roots[alias] = path
    return roots


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise InvalidSpecError(f"{name} must be an integer") from exc
    if value < 1:
        raise InvalidSpecError(f"{name} must be positive")
    return value
