"""Remote dispatch primitives for multi-alpha combine-backtest Phase 1."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import requests

from backend.db.pg_pool import get_conn
from backend.services.multi_alpha.combine_backtest import (
    DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS,
    MultiAlphaCombineBacktestError,
    _pred_backtest_error_context,
    apply_pred_backtest_overrides,
    ingest_enhanced_metrics,
    prepare_pred_backtest_workspace,
)
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient


_LOCAL_HOSTS = {"", "localhost", "127.0.0.1", "::1"}
_REMOTE_TASK_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")
_CHUNK_SIZE = 1024 * 1024
_QE_FILE_SYNC_MAX_FILE_SIZE = 10 * 1024 * 1024
_REMOTE_SMALL_TEXT_SUFFIXES = {".csv", ".json", ".py", ".txt", ".yaml", ".yml"}
_REMOTE_SMALL_BINARY_SUFFIXES = {".parquet"}
_REMOTE_SMALL_EXCLUDED_NAMES = {
    "combined_factors_df.parquet",
    "combined_prediction.pkl",
    "pred_backtest_stderr.log",
    "pred_backtest_stdout.log",
    "qe_current_recorder.json",
    "qe_extracted_recorder.json",
    "qe_prediction_store_upload.json",
    "qe_recorder_isolation.json",
    "qlib_res.csv",
    "qlib_results_enhanced.json",
    "qlib_results_llm.json",
    "ret.pkl",
    "ret_schema.json",
    "ret_schema.parquet",
    "signals.json",
    "signals.parquet",
}
_REMOTE_SMALL_EXCLUDED_PREFIXES = ("minute_trades", "minute_summary")


@dataclass(frozen=True)
class ComputeNodeInfo:
    node_id: str
    api_base_url: str | None = None
    factor_cache_dir: str | None = None
    factor_data_dir: str | None = None
    qlib_data_path: str | None = None
    qlib_minute_path: str | None = None
    workspace_base: str | None = None

    @property
    def is_remote(self) -> bool:
        parsed = urlparse(str(self.api_base_url or ""))
        return (parsed.hostname or "").lower() not in _LOCAL_HOSTS


def get_compute_node_info(node_id: str) -> ComputeNodeInfo:
    """Resolve node metadata from infra.compute_nodes without guessing host strings."""

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT node_id, api_base_url, factor_cache_dir, factor_data_dir,
                       qlib_data_path, qlib_minute_path, workspace_base
                FROM infra.compute_nodes
                WHERE node_id = %s
                """,
                (node_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise MultiAlphaCombineBacktestError(
                    f"compute node does not exist: {node_id}",
                    reason_code="remote_node_not_found",
                    context={"node_id": node_id},
                )
            cols = [desc[0] for desc in cur.description]
    payload = dict(zip(cols, row, strict=True))
    return ComputeNodeInfo(**payload)


def is_remote_compute_node(node_id: str) -> bool:
    node = get_compute_node_info(node_id)
    if not str(node.api_base_url or "").strip():
        raise MultiAlphaCombineBacktestError(
            f"compute node api_base_url is not configured: {node_id}",
            reason_code="remote_node_api_base_missing",
            context={"node_id": node_id},
        )
    return node.is_remote


async def _close_async_client(client: QEWorkspaceClient) -> None:
    await client.close()


def sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(_CHUNK_SIZE), b""):
                if not chunk:
                    continue
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise MultiAlphaCombineBacktestError(
            f"failed to read workspace artifact for sha256: {type(exc).__name__}: {exc}",
            reason_code="workspace_artifact_read_failed",
            context={"path": str(path)},
        ) from exc
    if size <= 0:
        raise MultiAlphaCombineBacktestError(
            f"workspace artifact is empty: {path}",
            reason_code="workspace_artifact_empty",
            context={"path": str(path)},
        )
    return digest.hexdigest(), size


class WorkspaceArtifactSyncClient:
    """Content-addressed large artifact sync client for RDAgent QE workspace nodes."""

    def __init__(self, *, base_url: str, timeout_seconds: float = 600.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = float(timeout_seconds)

    @classmethod
    def for_node(cls, node_id: str, *, timeout_seconds: float = 600.0) -> "WorkspaceArtifactSyncClient":
        client = QEWorkspaceClient.for_node(node_id)
        try:
            return cls(base_url=client.base_url, timeout_seconds=timeout_seconds)
        finally:
            if not client.client.is_closed:
                # Client was created only for node base_url resolution; close it synchronously outside any event loop.
                try:
                    running_loop = asyncio.get_running_loop()
                except RuntimeError:
                    asyncio.run(_close_async_client(client))
                else:
                    running_loop.create_task(_close_async_client(client))

    def ensure_artifact(self, path: Path, *, node_id: str, verify_after_upload: bool = True) -> dict[str, Any]:
        sha256, size = sha256_file(path)
        status = self.get_artifact_status(sha256=sha256, node_id=node_id)
        if status.get("exists") and int(status.get("size") or -1) == size:
            return {"sha256": sha256, "size": size, "uploaded": False, "status": status}
        if status.get("exists") and int(status.get("size") or -1) != size:
            raise MultiAlphaCombineBacktestError(
                "remote workspace artifact exists with same sha but unexpected size",
                reason_code="workspace_artifact_remote_size_mismatch",
                context={"node_id": node_id, "sha256": sha256, "local_size": size, "remote_status": status},
            )
        self.upload_artifact(path=path, sha256=sha256, size=size, node_id=node_id)
        if verify_after_upload:
            verified = self.get_artifact_status(sha256=sha256, node_id=node_id)
            if not verified.get("exists") or int(verified.get("size") or -1) != size:
                raise MultiAlphaCombineBacktestError(
                    "remote workspace artifact verification failed after upload",
                    reason_code="workspace_artifact_verify_failed",
                    context={"node_id": node_id, "sha256": sha256, "size": size, "remote_status": verified},
                )
            status = verified
        return {"sha256": sha256, "size": size, "uploaded": True, "status": status}

    def get_artifact_status(self, *, sha256: str, node_id: str) -> dict[str, Any]:
        url = self._artifact_url(sha256)
        try:
            response = requests.head(url, timeout=self.timeout_seconds)
            if response.status_code == 200:
                exists = response.headers.get("X-Artifact-Exists") == "1"
                size = int(response.headers.get("X-Artifact-Size") or 0)
                return {
                    "exists": exists,
                    "size": size,
                    "sha256": sha256,
                    "url": url,
                    "artifact_store_root": response.headers.get("X-Artifact-Store-Root"),
                }
            response = requests.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise MultiAlphaCombineBacktestError(
                f"workspace artifact HEAD failed: {type(exc).__name__}: {exc}",
                reason_code="workspace_artifact_head_failed",
                context={"node_id": node_id, "sha256": sha256, "url": url},
            ) from exc
        if not isinstance(payload, Mapping):
            raise MultiAlphaCombineBacktestError(
                f"workspace artifact HEAD returned invalid payload: {payload!r}",
                reason_code="workspace_artifact_head_failed",
                context={"node_id": node_id, "sha256": sha256, "url": url},
            )
        return dict(payload)

    def upload_artifact(self, *, path: Path, sha256: str, size: int, node_id: str) -> None:
        url = self._artifact_url(sha256)
        try:
            with path.open("rb") as fh:
                response = requests.post(url, data=fh, timeout=self.timeout_seconds)
            response.raise_for_status()
            payload = response.json()
        except (OSError, requests.RequestException, ValueError) as exc:
            raise MultiAlphaCombineBacktestError(
                f"workspace artifact upload failed: {type(exc).__name__}: {exc}",
                reason_code="workspace_artifact_upload_failed",
                context={"node_id": node_id, "sha256": sha256, "size": size, "url": url, "path": str(path)},
            ) from exc
        if not isinstance(payload, Mapping) or payload.get("ok") is not True:
            raise MultiAlphaCombineBacktestError(
                f"workspace artifact upload returned invalid payload: {payload!r}",
                reason_code="workspace_artifact_upload_failed",
                context={"node_id": node_id, "sha256": sha256, "size": size, "url": url, "path": str(path)},
            )

    def _artifact_url(self, sha256: str) -> str:
        if self.base_url.endswith("/artifacts"):
            return f"{self.base_url}/{sha256}"
        return f"{self.base_url}/artifacts/{sha256}"


class RemotePredBacktestExecutor:
    """Run a combine pred-backtest on a remote RDAgent QE workspace node."""

    def __init__(
        self,
        *,
        node_resolver=get_compute_node_info,
        artifact_client_factory: Any | None = None,
        workspace_client_factory: Any | None = None,
        small_file_syncer: Any | None = None,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self._node_resolver = node_resolver
        self._artifact_client_factory = artifact_client_factory
        self._workspace_client_factory = workspace_client_factory or QEWorkspaceClient.for_node
        self._small_file_syncer = small_file_syncer
        self._poll_interval_seconds = float(poll_interval_seconds)

    def execute_pred_backtest(
        self,
        *,
        workspace: Path,
        pred_pkl: Path,
        node_id: str,
        backtest_config: Mapping[str, Any],
    ) -> dict[str, Any]:
        workspace.mkdir(parents=True, exist_ok=True)
        if not pred_pkl.exists():
            raise MultiAlphaCombineBacktestError(
                f"prediction pickle does not exist: {pred_pkl}",
                reason_code="pred_pkl_missing",
                context={"workspace": str(workspace), "node_id": node_id},
            )
        if bool(backtest_config.get("parse_only")):
            return ingest_enhanced_metrics(workspace / "qlib_results_enhanced.json")

        node = self._node_resolver(node_id)
        _require_remote_api_base(node)
        if not node.is_remote:
            raise MultiAlphaCombineBacktestError(
                "RemotePredBacktestExecutor received a local compute node",
                reason_code="remote_dispatch_node_not_remote",
                context={"node_id": node_id, "api_base_url": node.api_base_url},
            )
        prepare_pred_backtest_workspace(workspace=workspace, backtest_config=backtest_config)
        apply_pred_backtest_overrides(workspace=workspace, backtest_config=backtest_config)
        l2_path = _resolve_l2_artifact_path(workspace=workspace, backtest_config=backtest_config)
        artifact_client = self._artifact_client_factory(node_id) if self._artifact_client_factory else WorkspaceArtifactSyncClient.for_node(node_id)
        artifact_manifest = artifact_client.ensure_artifact(l2_path, node_id=node_id)
        remote_paths = _remote_paths(
            node=node,
            backtest_config=backtest_config,
            artifact_sha256=str(artifact_manifest["sha256"]),
            artifact_manifest=artifact_manifest,
        )
        task_id = _remote_task_id(backtest_config=backtest_config, workspace=workspace)
        loop_index = _remote_loop_index(backtest_config=backtest_config)
        timeout_seconds = int(backtest_config.get("timeout_seconds", DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS))
        small_files = _remote_small_files(workspace=workspace, pred_pkl=pred_pkl)
        self._sync_small_files(node=node, task_id=task_id, loop_index=loop_index, files=small_files, timeout_seconds=timeout_seconds)
        wsl_command = _remote_wsl_command(workspace=workspace, remote_paths=remote_paths, backtest_config=backtest_config)
        try:
            loop_id, enhanced = asyncio.run(
                self._run_remote_loop(
                    node_id=node_id,
                    task_id=task_id,
                    loop_index=loop_index,
                    config={
                        "source": "multi_alpha_combine_backtest_remote_dispatch_v1",
                        "workspace": str(workspace),
                        "backtest_name": str(backtest_config.get("backtest_name") or workspace.name),
                        "artifact_manifest": artifact_manifest,
                        "remote_paths": remote_paths,
                    },
                    experiment_files={},
                    wsl_command=wsl_command,
                    timeout_seconds=timeout_seconds,
                )
            )
        except MultiAlphaCombineBacktestError:
            raise
        except Exception as exc:
            raise MultiAlphaCombineBacktestError(
                f"remote pred-backtest dispatch failed: {type(exc).__name__}: {exc}",
                reason_code="remote_pred_backtest_dispatch_failed",
                context={**_pred_backtest_error_context(workspace=workspace, node_id=node_id, backtest_config=backtest_config), "stage": "remote_dispatch"},
            ) from exc
        result_path = workspace / "qlib_results_enhanced.json"
        result_path.write_text(json.dumps(enhanced, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        metrics = ingest_enhanced_metrics(result_path)
        metrics["remote_dispatch"] = {
            "node_id": node_id,
            "task_id": task_id,
            "loop_id": loop_id,
            "artifact_manifest": artifact_manifest,
        }
        return metrics

    def _sync_small_files(
        self,
        *,
        node: ComputeNodeInfo,
        task_id: str,
        loop_index: int,
        files: Mapping[str, str],
        timeout_seconds: int,
    ) -> None:
        if self._small_file_syncer is not None:
            self._small_file_syncer(
                node=node,
                task_id=task_id,
                loop_index=loop_index,
                files=dict(files),
                timeout_seconds=timeout_seconds,
            )
            return
        base = str(node.api_base_url or "").rstrip("/")
        if not base:
            raise MultiAlphaCombineBacktestError(
                "remote node api_base_url is required for qe_file_sync small-file upload",
                reason_code="remote_node_api_base_missing",
                context={"node_id": node.node_id},
            )
        url = f"{base}/api/qe/experiments/{task_id}/files/batch"
        loop_files = {f"Loop{loop_index}/{name}": content for name, content in files.items()}
        try:
            response = requests.post(url, json={"files": loop_files}, timeout=timeout_seconds)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise MultiAlphaCombineBacktestError(
                f"remote small-file qe_file_sync upload failed: {type(exc).__name__}: {exc}",
                reason_code="remote_small_file_sync_failed",
                context={"node_id": node.node_id, "task_id": task_id, "loop_index": loop_index, "url": url, "file_count": len(files)},
            ) from exc
        if not isinstance(payload, Mapping) or payload.get("success") is not True:
            raise MultiAlphaCombineBacktestError(
                f"remote small-file qe_file_sync upload returned failure: {payload!r}",
                reason_code="remote_small_file_sync_failed",
                context={"node_id": node.node_id, "task_id": task_id, "url": url, "payload": payload},
            )

    async def _run_remote_loop(
        self,
        *,
        node_id: str,
        task_id: str,
        loop_index: int,
        config: dict[str, Any],
        experiment_files: dict[str, str],
        wsl_command: str,
        timeout_seconds: int,
    ) -> tuple[str, dict[str, Any]]:
        client = self._workspace_client_factory(node_id)
        try:
            loop_id = await client.create_and_run_loop(
                task_id=task_id,
                loop_index=loop_index,
                config=config,
                experiment_files=experiment_files,
                wsl_command=wsl_command,
            )
            deadline = time.monotonic() + timeout_seconds
            last_status: Mapping[str, Any] | None = None
            while time.monotonic() <= deadline:
                status_payload = await client.get_loop_status(task_id, loop_id)
                last_status = dict(status_payload or {})
                status = str(last_status.get("status") or "").lower()
                if status == "completed":
                    enhanced = await client.get_workspace_file(task_id, loop_id, "qlib_results_enhanced.json")
                    if not isinstance(enhanced, dict):
                        raise MultiAlphaCombineBacktestError(
                            "remote pred-backtest returned non-JSON enhanced metrics",
                            reason_code="remote_pred_backtest_result_invalid",
                            context={
                                "node_id": node_id,
                                "task_id": task_id,
                                "loop_id": loop_id,
                                "result_type": type(enhanced).__name__,
                            },
                        )
                    return loop_id, enhanced
                if status in {"failed", "cancelled"}:
                    stderr_tail = ""
                    try:
                        stderr_tail = str(await client.get_workspace_file(task_id, loop_id, "run.log"))[-2000:]
                    except Exception as log_exc:
                        stderr_tail = f"<run.log unavailable: {type(log_exc).__name__}: {log_exc}>"
                    raise MultiAlphaCombineBacktestError(
                        f"remote pred-backtest failed on node={node_id} status={status}",
                        reason_code="remote_pred_backtest_failed",
                        context={"node_id": node_id, "task_id": task_id, "loop_id": loop_id, "status": last_status, "stderr_tail": stderr_tail},
                    )
                await asyncio.sleep(self._poll_interval_seconds)
            raise MultiAlphaCombineBacktestError(
                f"remote pred-backtest timed out after {timeout_seconds}s on node={node_id}",
                reason_code="remote_pred_backtest_timeout",
                context={"node_id": node_id, "task_id": task_id, "loop_id": loop_id, "last_status": dict(last_status or {})},
            )
        finally:
            close = getattr(client, "close", None)
            if close is not None:
                await close()


def _resolve_l2_artifact_path(*, workspace: Path, backtest_config: Mapping[str, Any]) -> Path:
    raw = backtest_config.get("combined_factors_path") or backtest_config.get("l2_artifact_path")
    if raw is None:
        raw = workspace / "combined_factors_df.parquet"
    path = Path(str(raw))
    if not path.is_absolute():
        path = workspace / path
    if not path.exists() or not path.is_file():
        raise MultiAlphaCombineBacktestError(
            f"L2 combined factors parquet is missing for remote dispatch: {path}",
            reason_code="remote_l2_artifact_missing",
            context={"workspace": str(workspace), "combined_factors_path": str(path)},
        )
    return path


def _require_remote_api_base(node: ComputeNodeInfo) -> None:
    if not str(node.api_base_url or "").strip():
        raise MultiAlphaCombineBacktestError(
            "remote node api_base_url is required for combine pred-backtest dispatch",
            reason_code="remote_node_api_base_missing",
            context={"node_id": node.node_id},
        )


def _remote_paths(
    *,
    node: ComputeNodeInfo,
    backtest_config: Mapping[str, Any],
    artifact_sha256: str,
    artifact_manifest: Mapping[str, Any],
) -> dict[str, str]:
    status = artifact_manifest.get("status") if isinstance(artifact_manifest.get("status"), Mapping) else {}
    artifact_root = (
        str(backtest_config.get("remote_artifact_store_root") or "").strip()
        or str((status or {}).get("artifact_store_root") or "").strip()
        or os.getenv("AISTOCK_REMOTE_QE_WORKSPACE_ARTIFACT_STORE", "").strip()
    )
    qlib_data_path = str(backtest_config.get("remote_qlib_data_path") or node.qlib_data_path or "").strip()
    factor_cache_dir = str(
        backtest_config.get("remote_factor_cache_dir")
        or node.factor_cache_dir
        or node.factor_data_dir
        or ""
    ).strip()
    if not artifact_root:
        raise MultiAlphaCombineBacktestError(
            "remote artifact_store_root is required for combine pred-backtest dispatch",
            reason_code="remote_artifact_store_root_missing",
            context={"node_id": node.node_id, "api_base_url": node.api_base_url, "artifact_manifest": dict(artifact_manifest)},
        )
    if not qlib_data_path:
        raise MultiAlphaCombineBacktestError(
            "remote qlib_data_path is required for combine pred-backtest dispatch",
            reason_code="remote_qlib_data_path_missing",
            context={"node_id": node.node_id, "api_base_url": node.api_base_url},
        )
    if not factor_cache_dir:
        raise MultiAlphaCombineBacktestError(
            "remote factor cache dir is required for combine pred-backtest dispatch",
            reason_code="remote_factor_cache_dir_missing",
            context={"node_id": node.node_id, "api_base_url": node.api_base_url},
        )
    _require_remote_linux_path(path_name="artifact_store_root", value=artifact_root, node=node)
    _require_remote_linux_path(path_name="qlib_data_path", value=qlib_data_path, node=node)
    _require_remote_linux_path(path_name="factor_cache_dir", value=factor_cache_dir, node=node)
    paths = {
        "artifact_path": f"{artifact_root.rstrip('/')}/{artifact_sha256}",
        "qlib_data_path": qlib_data_path,
        "factor_cache_dir": factor_cache_dir,
    }
    workspace_base = str(backtest_config.get("remote_workspace_base") or node.workspace_base or "").strip()
    if workspace_base:
        _require_remote_linux_path(path_name="workspace_base", value=workspace_base, node=node)
        paths["workspace_base"] = workspace_base.rstrip("/")
    return paths


def _require_remote_linux_path(*, path_name: str, value: str, node: ComputeNodeInfo) -> None:
    invalid = "\\" in value or re.match(r"^[A-Za-z]:", value) is not None or not value.startswith("/") or value.startswith("/mnt/")
    if invalid:
        raise MultiAlphaCombineBacktestError(
            f"remote {path_name} must be a node-local Linux absolute path, got: {value}",
            reason_code="remote_path_invalid",
            context={"node_id": node.node_id, "api_base_url": node.api_base_url, "path_name": path_name, "value": value},
        )


def _remote_task_id(*, backtest_config: Mapping[str, Any], workspace: Path) -> str:
    raw = str(backtest_config.get("remote_task_id") or f"macb_remote_{workspace.parent.name}_{workspace.name}").strip()
    cleaned = _REMOTE_TASK_ID_RE.sub("_", raw).strip("._-")
    if not cleaned:
        raise MultiAlphaCombineBacktestError("remote task id is empty", reason_code="remote_task_id_invalid")
    return cleaned[:180]


def _remote_loop_index(*, backtest_config: Mapping[str, Any]) -> int:
    raw = backtest_config.get("remote_loop_index") or 1
    try:
        loop_index = int(raw)
    except (TypeError, ValueError) as exc:
        raise MultiAlphaCombineBacktestError(
            "remote_loop_index must be an integer",
            reason_code="remote_loop_index_invalid",
            context={"remote_loop_index": raw},
        ) from exc
    if loop_index < 1:
        raise MultiAlphaCombineBacktestError(
            "remote_loop_index must be positive",
            reason_code="remote_loop_index_invalid",
            context={"remote_loop_index": raw},
        )
    return loop_index


def _remote_small_files(*, workspace: Path, pred_pkl: Path) -> dict[str, str]:
    files: dict[str, str] = {}
    for name in sorted(_iter_remote_small_file_names(workspace=workspace)):
        path = workspace / name
        if name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py") and not path.exists():
            raise MultiAlphaCombineBacktestError(
                f"remote pred-backtest workspace missing required file: {name}",
                reason_code="remote_workspace_file_missing",
                context={"workspace": str(workspace), "file": name},
        )
        if path.suffix.lower() in _REMOTE_SMALL_BINARY_SUFFIXES:
            files[f"{name}.b64"] = _b64_file(path)
        else:
            try:
                files[name] = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as exc:
                raise MultiAlphaCombineBacktestError(
                    f"failed to package remote text workspace file: {type(exc).__name__}: {exc}",
                    reason_code="remote_workspace_file_packaging_failed",
                    context={"path": str(path), "workspace": str(workspace)},
                ) from exc
    files[f"{pred_pkl.name}.b64"] = _b64_file(pred_pkl)
    too_large = {name: len(content.encode("utf-8")) for name, content in files.items() if len(content.encode("utf-8")) > _QE_FILE_SYNC_MAX_FILE_SIZE}
    if too_large:
        raise MultiAlphaCombineBacktestError(
            "remote small-file payload exceeds qe_file_sync 10MB limit",
            reason_code="remote_small_file_too_large",
            context={"workspace": str(workspace), "too_large": too_large, "limit_bytes": _QE_FILE_SYNC_MAX_FILE_SIZE},
        )
    return files


def _iter_remote_small_file_names(*, workspace: Path) -> set[str]:
    required = {"conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"}
    names: set[str] = set(required)
    for path in workspace.iterdir():
        if not path.is_file():
            continue
        name = path.name
        suffix = path.suffix.lower()
        if name in _REMOTE_SMALL_EXCLUDED_NAMES or name.startswith(_REMOTE_SMALL_EXCLUDED_PREFIXES):
            continue
        if suffix in _REMOTE_SMALL_TEXT_SUFFIXES or suffix in _REMOTE_SMALL_BINARY_SUFFIXES:
            names.add(name)
    return names


def _b64_file(path: Path) -> str:
    try:
        return base64.b64encode(path.read_bytes()).decode("ascii")
    except OSError as exc:
        raise MultiAlphaCombineBacktestError(
            f"failed to package remote workspace file: {type(exc).__name__}: {exc}",
            reason_code="remote_workspace_file_packaging_failed",
            context={"path": str(path)},
        ) from exc


def _remote_wsl_command(*, workspace: Path, remote_paths: Mapping[str, str], backtest_config: Mapping[str, Any]) -> str:
    conda_env = str(backtest_config.get("remote_conda_env") or backtest_config.get("conda_env") or "AIstock")
    artifact_path = _shell_quote(remote_paths["artifact_path"])
    qlib_path = _shell_quote(remote_paths["qlib_data_path"])
    factor_cache = _shell_quote(remote_paths["factor_cache_dir"])
    env_exports = _remote_env_exports(backtest_config)
    workspace_cd = _remote_workspace_cd(workspace=workspace, remote_paths=remote_paths, backtest_config=backtest_config)
    command = "".join(
        [
            "set -euo pipefail; ",
            workspace_cd,
            "test -f conf.yaml; test -f qrun_limit_minute.py; test -f read_exp_res.py; ",
            "python -c \"import base64,pathlib; [p.with_suffix('').write_bytes(base64.b64decode(p.read_text())) for p in pathlib.Path('.').glob('*.b64')]\"; ",
            "ln -sfn " + artifact_path + " combined_factors_df.parquet; ",
            "test -f combined_factors_df.parquet; ",
            "test -d " + qlib_path + "; ",
            "test -d " + factor_cache + "; ",
            "export QLIB_DATA_PATH=" + qlib_path + "; ",
            "export QLIB_DATA_PATH_WSL=" + qlib_path + "; ",
            "export FACTOR_CACHE_DIR=" + factor_cache + "; ",
            "export RDAGENT_FACTOR_DATA_WSL=" + factor_cache + "; ",
            "export FACTOR_CACHE_DATA_MODE='backtest_factor_data_dir'; ",
            env_exports,
            "source ~/miniconda3/etc/profile.d/conda.sh; ",
            "conda activate " + _shell_quote(conda_env) + "; ",
            "python qrun_limit_minute.py conf.yaml --pred-backtest combined_prediction.pkl; ",
            "QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py",
        ]
    )
    return "bash -lc " + _shell_quote(command)


def _remote_workspace_cd(*, workspace: Path, remote_paths: Mapping[str, str], backtest_config: Mapping[str, Any]) -> str:
    workspace_base = str(remote_paths.get("workspace_base") or "").strip()
    if not workspace_base:
        return ""
    task_id = _remote_task_id(backtest_config=backtest_config, workspace=workspace)
    loop_index = _remote_loop_index(backtest_config=backtest_config)
    return "cd " + _shell_quote(f"{workspace_base.rstrip('/')}/{task_id}/Loop{loop_index}") + "; "


def _remote_env_exports(backtest_config: Mapping[str, Any]) -> str:
    raw = backtest_config.get("remote_env")
    env: dict[str, Any] = dict(raw) if isinstance(raw, Mapping) else {}
    for key in ("num_features", "num_timesteps"):
        if key in backtest_config and key not in env:
            env[key] = backtest_config[key]
    exports: list[str] = []
    for key, value in sorted(env.items()):
        key_str = str(key)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key_str):
            raise MultiAlphaCombineBacktestError(
                "remote_env contains an invalid shell variable name",
                reason_code="remote_env_invalid",
                context={"key": key_str},
            )
        exports.append(f"export {key_str}={_shell_quote(str(value))}; ")
    return "".join(exports)


def _shell_quote(value: str) -> str:
    return "'" + str(value).replace("'", "'\"'\"'") + "'"
