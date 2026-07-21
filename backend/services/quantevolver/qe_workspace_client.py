import logging
import os
import aiofiles
import zipfile
import re
import hashlib
import json
from dataclasses import dataclass
from typing import Dict, Any, Mapping, Optional
import httpx

from backend.services.qe_archive.models import normalize_json

logger = logging.getLogger(__name__)

_QE_SUBMISSION_RECEIPT_SCHEMA = "qe_submission_receipt_v1"
_QE_SUBMISSION_RECEIPT_STATUSES = frozenset(
    {"not_reserved", "reserved", "started", "running", "completed", "failed", "cancelled"}
)
_QE_TYPED_KILL_RECEIPT_SCHEMA = "qe_kill_receipt_v1"
_QE_TYPED_KILL_RECEIPT_STATUSES = frozenset(
    {"requested", "signal_sent", "reconciling", "completed", "cancelled", "failed"}
)
_QE_EXECUTION_ENVIRONMENT_SCHEMA = "qe_execution_environment_manifest_v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_LOOP_ID_RE = re.compile(r"^Loop[1-9][0-9]*$")
_COMMAND_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,256}$")
_ENVIRONMENT_SNAPSHOT_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,256}$")


class QEWorkspaceSubmissionError(RuntimeError):
    """Base error for the durable QE Workspace submission contract."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class QEWorkspaceSubmissionContractError(QEWorkspaceSubmissionError):
    """The QE node returned a response that violates the receipt contract."""


class QEWorkspaceSubmissionRejected(QEWorkspaceSubmissionError):
    """The QE node authoritatively rejected a submission request."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message, reason_code=reason_code, context=context)
        self.status_code = int(status_code)


class QEWorkspaceSubmissionTransportError(QEWorkspaceSubmissionError):
    """Submission transport failed, so remote acceptance remains unknown."""


class QEWorkspaceTypedKillError(RuntimeError):
    """Base error for the explicit PID-reuse-safe QE cancellation contract."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class QEWorkspaceTypedKillContractError(QEWorkspaceTypedKillError):
    """The QE node returned a malformed typed cancellation receipt."""


class QEWorkspaceTypedKillRejected(QEWorkspaceTypedKillError):
    """The QE node authoritatively rejected the typed cancellation request."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message, reason_code=reason_code, context=context)
        self.status_code = int(status_code)


class QEWorkspaceTypedKillTransportError(QEWorkspaceTypedKillError):
    """Typed kill transport failed, so remote delivery remains unknown."""


class QEWorkspaceExecutionEnvironmentError(RuntimeError):
    """The QE node could not supply a trustworthy cached deployment identity."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class QEWorkspaceDatasetIdentityError(RuntimeError):
    """The QE node returned an invalid dataset identity/evidence report."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True)
class QEWorkspaceSubmissionReceipt:
    task_id: str
    loop_id: str
    submission_intent_hash: str
    request_digest: str
    receipt_status: str
    duplicate_replay: bool
    execution_identity_hash: str | None = None
    execution_environment_snapshot_id: str | None = None
    execution_environment_manifest_sha256: str | None = None


@dataclass(frozen=True)
class QEWorkspaceSubmissionInspection:
    schema_version: str
    task_id: str
    loop_id: str
    status: str
    submission_intent_hash: str | None = None
    request_digest: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    started_at: str | None = None
    running_at: str | None = None
    finished_at: str | None = None
    pid: int | None = None
    process_identity: dict[str, int] | None = None
    execution_identity_hash: str | None = None
    execution_environment_snapshot_id: str | None = None
    execution_environment_manifest_sha256: str | None = None

    @property
    def is_reserved(self) -> bool:
        return self.status != "not_reserved"


@dataclass(frozen=True)
class QEWorkspaceTypedKillReceipt:
    schema_version: str
    task_id: str
    loop_id: str
    command_id: str
    kill_intent_generation: int
    kill_intent_hash: str
    expected_submission_intent_hash: str
    expected_process_identity: dict[str, int] | None
    expected_phase: str | None
    process_identity: dict[str, int] | None
    status: str
    signal_attempt_count: int
    signal_sent_at: str | None
    signal_sent: bool
    process_observation: dict[str, Any] | None
    result_observation: dict[str, Any] | None
    submission_receipt_status: str | None
    terminal_reason: str | None
    error: dict[str, Any] | None
    created_at: str
    updated_at: str
    completed_at: str | None


@dataclass(frozen=True)
class QEWorkspaceExecutionEnvironment:
    schema_version: str
    execution_environment_snapshot_id: str
    execution_environment_manifest_sha256: str
    manifest: dict[str, Any]


@dataclass(frozen=True)
class QEWorkspaceDatasetIdentity:
    schema_version: str
    complete: bool
    reason_code: str | None
    missing: tuple[str, ...]
    acquisition_suggestions: tuple[str, ...]
    dataset: dict[str, str] | None
    detail: str | None = None


class QELoopWorkspaceCleanupUnavailable(RuntimeError):
    """Raised when RD-Agent cannot service loop-scoped workspace cleanup."""


class QEWorkspaceFileNotFound(RuntimeError):
    """Raised when a requested loop-scoped workspace file is missing."""

    def __init__(self, task_id: str, loop_id: str, file_path: str, url: str) -> None:
        self.task_id = task_id
        self.loop_id = loop_id
        self.file_path = file_path
        self.url = url
        super().__init__(
            f"workspace file not found: task={task_id} loop={loop_id} file={file_path} url={url}"
        )


class QEWorkspaceCatalogUnavailable(RuntimeError):
    """Raised when the QE node lacks the Phase 1 read-only catalog contract."""


class QEWorkspaceCatalogInvalid(QEWorkspaceCatalogUnavailable):
    """Raised when a QE node exposes the catalog endpoint but violates its contract."""


class QEWorkspaceClient:
    """
    专门负责与被物理隔离的 RDAgent 端进行网络交互的客户端
    封装了诸如触发任务、获取回测指标、获取日志流、下载模型资产等操作。
    """
    def __init__(self, base_url: str = "http://localhost:9000/api/v1/qe_workspace"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0),
            trust_env=False,
        )

    @staticmethod
    def _to_rdagent_loop_id(task_id: str, loop_id: str) -> str:
        """DB 中 loop_id 格式为 '{task_id}_{LoopN}'，RDAgent 文件系统期望 'LoopN'"""
        if loop_id.startswith(task_id + "_"):
            return loop_id[len(task_id) + 1:]
        return loop_id

    @classmethod
    def for_node(cls, node_id: str) -> "QEWorkspaceClient":
        """根据 node_id 从 compute_nodes 表获取 api_base_url 创建客户端。"""
        from ...db.pg_pool import get_conn
        from psycopg2.extras import RealDictCursor
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT api_base_url FROM infra.compute_nodes WHERE node_id = %s", (node_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"节点不存在: {node_id}")
                base = row["api_base_url"].rstrip("/")
                return cls(base_url=f"{base}/api/v1/qe_workspace")

    @classmethod
    def for_task_loop(cls, task_id: str, loop_id: str | None = None) -> "QEWorkspaceClient":
        """Resolve the authoritative QE compute node for one task/loop.

        Loop-level placement wins when it is present.  Otherwise the task's
        node is authoritative.  Missing task/node metadata is an explicit
        contract failure; callers must never fall back to localhost or a
        process-wide default node.
        """

        normalized_task_id = str(task_id or "").strip()
        normalized_loop_id = str(loop_id or "").strip()
        if not normalized_task_id:
            raise ValueError("QE task_id is required for workspace node resolution")

        from ...db.pg_pool import get_conn
        from psycopg2.extras import RealDictCursor

        full_loop_id = (
            normalized_loop_id
            if normalized_loop_id.startswith(f"{normalized_task_id}_")
            else f"{normalized_task_id}_{normalized_loop_id}"
        )
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT COALESCE(NULLIF(l.node_id, ''), NULLIF(t.node_id, '')) AS node_id
                    FROM qe_evolution_tasks t
                    LEFT JOIN qe_evolution_loops l
                      ON l.task_id = t.task_id
                     AND (%s <> '' AND l.loop_id IN (%s, %s))
                    WHERE t.task_id = %s
                    ORDER BY CASE WHEN NULLIF(l.node_id, '') IS NOT NULL THEN 0 ELSE 1 END
                    LIMIT 1
                    """,
                    (
                        normalized_loop_id,
                        normalized_loop_id,
                        full_loop_id,
                        normalized_task_id,
                    ),
                )
                row = cur.fetchone()
        node_id = str(row["node_id"] or "").strip() if row else ""
        if not node_id:
            raise ValueError(
                f"QE task/loop has no authoritative compute node: "
                f"task={normalized_task_id} loop={normalized_loop_id or '<task>'}"
            )
        return cls.for_node(node_id)

    async def close(self):
        """显式关闭内部 httpx 客户端，释放连接池资源。"""
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def get_execution_environment(self) -> QEWorkspaceExecutionEnvironment:
        """Read the owning node's cached deployment identity exactly once per caller.

        The endpoint itself is deployment-cached.  This client method performs no
        GPU/VRAM/resource telemetry and never synthesizes an environment identity
        from the local AIstock process when the QE node is unavailable.
        """

        url = f"{self.base_url}/execution-environment"
        try:
            response = await self.client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise QEWorkspaceExecutionEnvironmentError(
                "QE Workspace rejected execution environment identity lookup",
                reason_code="qe_workspace_execution_environment_rejected",
                context={
                    "url": url,
                    "status_code": exc.response.status_code,
                    "body": exc.response.text[:1000],
                },
            ) from exc
        except httpx.RequestError as exc:
            raise QEWorkspaceExecutionEnvironmentError(
                "QE Workspace execution environment identity is unavailable",
                reason_code="qe_workspace_execution_environment_unavailable",
                context={"url": url, "error_type": type(exc).__name__, "message": str(exc)},
            ) from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise QEWorkspaceExecutionEnvironmentError(
                "QE Workspace execution environment response is not valid JSON",
                reason_code="qe_workspace_execution_environment_invalid",
                context={"url": url},
            ) from exc
        return self._parse_execution_environment(payload)

    async def get_dataset_identity(
        self,
        *,
        node_id: str,
        data_root_uri: str | None,
    ) -> QEWorkspaceDatasetIdentity:
        """Read the node-owned immutable dataset manifest/evidence report."""

        normalized_node_id = str(node_id or "").strip()
        if not normalized_node_id:
            raise QEWorkspaceDatasetIdentityError(
                "QE dataset identity requires a node_id",
                reason_code="qe_workspace_dataset_identity_invalid",
            )
        url = f"{self.base_url}/dataset-identity"
        params: dict[str, str] = {"node_id": normalized_node_id}
        if data_root_uri is not None:
            params["data_root_uri"] = str(data_root_uri)
        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise QEWorkspaceDatasetIdentityError(
                "QE Workspace rejected dataset identity lookup",
                reason_code="qe_workspace_dataset_identity_rejected",
                context={"url": url, "status_code": exc.response.status_code, "body": exc.response.text[:1000]},
            ) from exc
        except httpx.RequestError as exc:
            raise QEWorkspaceDatasetIdentityError(
                "QE Workspace dataset identity is unavailable",
                reason_code="qe_workspace_dataset_identity_unavailable",
                context={"url": url, "error_type": type(exc).__name__, "message": str(exc)},
            ) from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise QEWorkspaceDatasetIdentityError(
                "QE Workspace dataset identity response is not valid JSON",
                reason_code="qe_workspace_dataset_identity_invalid",
            ) from exc
        return self._parse_dataset_identity(payload)
        
    async def submit_loop(
        self,
        task_id: str,
        loop_index: int,
        config: Dict[str, Any],
        experiment_files: Dict[str, str] | None = None,
        wsl_command: str = "",
        model_source: Dict[str, Any] | None = None,
        callback_url: str | None = None,
        *,
        submission_intent_hash: str,
        execution_identity_hash: str | None = None,
        execution_environment_snapshot_id: str | None = None,
        execution_environment_manifest_sha256: str | None = None,
    ) -> QEWorkspaceSubmissionReceipt:
        """Submit one loop and validate the durable server-side receipt.

        A transport failure is intentionally distinct from a server rejection:
        callers must reconcile the receipt before deciding whether a second POST
        is safe.  The client never retries or falls back to the legacy request
        schema on its own.
        """
        normalized_task_id = self._validate_task_id(task_id)
        normalized_loop_index = self._validate_loop_index(loop_index)
        normalized_intent_hash = self._validate_sha256(
            submission_intent_hash,
            field_name="submission_intent_hash",
        )
        execution_binding = self._validate_execution_binding(
            execution_identity_hash=execution_identity_hash,
            execution_environment_snapshot_id=execution_environment_snapshot_id,
            execution_environment_manifest_sha256=execution_environment_manifest_sha256,
        )
        expected_loop_id = f"Loop{normalized_loop_index}"
        url = f"{self.base_url}/tasks/{normalized_task_id}/loops"
        payload = {
            "loop_index": normalized_loop_index,
            "config": config,
            "experiment_files": experiment_files or {},
            "wsl_command": wsl_command,
            "submission_intent_hash": normalized_intent_hash,
        }
        payload.update(execution_binding)
        if model_source:
            payload["model_source"] = model_source
        if callback_url:
            payload["callback_url"] = callback_url

        try:
            response = await self.client.post(url, json=normalize_json(payload))
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise self._submission_rejected(exc) from exc
        except httpx.RequestError as exc:
            raise QEWorkspaceSubmissionTransportError(
                "QE Workspace submission transport failed; remote acceptance is unknown",
                reason_code="qe_workspace_submission_transport_unknown",
                context={
                    "task_id": normalized_task_id,
                    "loop_id": expected_loop_id,
                    "url": url,
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
            ) from exc

        try:
            data = response.json()
        except ValueError as exc:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace submission response is not valid JSON",
                reason_code="qe_workspace_submission_response_invalid",
                context={"task_id": normalized_task_id, "loop_id": expected_loop_id},
            ) from exc
        return self._parse_submission_receipt(
            data,
            task_id=normalized_task_id,
            expected_loop_id=expected_loop_id,
            expected_intent_hash=normalized_intent_hash,
            expected_execution_binding=execution_binding,
        )

    async def create_and_run_loop(
        self,
        task_id: str,
        loop_index: int,
        config: Dict[str, Any],
        experiment_files: Dict[str, str] | None = None,
        wsl_command: str = "",
        model_source: Dict[str, Any] | None = None,
        callback_url: str | None = None,
        *,
        submission_intent_hash: str,
        execution_identity_hash: str | None = None,
        execution_environment_snapshot_id: str | None = None,
        execution_environment_manifest_sha256: str | None = None,
    ) -> str:
        """
        通知 RDAgent 根据配置生成代码并启动执行 QLib 回测
        返回 RDAgent 端生成的 loop_id

        model_source: 策略演进时传入模型来源信息；backtest-only 应使用打包 payload，
            避免目标 loop 通过 mlruns 符号链接写回 source recorder
            {
                "source_task_id": "qe_xxx",
                "source_loop": "Loop3",
            }
        callback_url: Loop 完成后回调 AIstock 的 URL（远端节点主动通知）
        """
        receipt = await self.submit_loop(
            task_id,
            loop_index,
            config,
            experiment_files,
            wsl_command,
            model_source=model_source,
            callback_url=callback_url,
            submission_intent_hash=submission_intent_hash,
            execution_identity_hash=execution_identity_hash,
            execution_environment_snapshot_id=execution_environment_snapshot_id,
            execution_environment_manifest_sha256=execution_environment_manifest_sha256,
        )
        return receipt.loop_id

    async def inspect_loop_submission(
        self,
        task_id: str,
        loop_id: str,
        *,
        submission_intent_hash: str | None = None,
    ) -> QEWorkspaceSubmissionInspection:
        """Read the authoritative receipt, including explicit ``not_reserved``."""
        normalized_task_id = self._validate_task_id(task_id)
        normalized_loop_id = self._validate_loop_id(
            self._to_rdagent_loop_id(normalized_task_id, loop_id)
        )
        url = (
            f"{self.base_url}/tasks/{normalized_task_id}/loops/"
            f"{normalized_loop_id}/submission"
        )
        params = None
        if submission_intent_hash is not None:
            params = {
                "submission_intent_hash": self._validate_sha256(
                    submission_intent_hash,
                    field_name="submission_intent_hash",
                )
            }
        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise self._submission_rejected(exc) from exc
        except httpx.RequestError as exc:
            raise QEWorkspaceSubmissionTransportError(
                "QE Workspace receipt inspection transport failed",
                reason_code="qe_workspace_submission_inspection_unavailable",
                context={
                    "task_id": normalized_task_id,
                    "loop_id": normalized_loop_id,
                    "url": url,
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
            ) from exc
        try:
            data = response.json()
        except ValueError as exc:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace receipt inspection response is not valid JSON",
                reason_code="qe_workspace_submission_receipt_invalid",
                context={"task_id": normalized_task_id, "loop_id": normalized_loop_id},
            ) from exc
        return self._parse_submission_inspection(
            data,
            task_id=normalized_task_id,
            expected_loop_id=normalized_loop_id,
        )

    @staticmethod
    def _validate_task_id(task_id: str) -> str:
        normalized = str(task_id or "").strip()
        if not normalized:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace task_id is required",
                reason_code="qe_workspace_submission_identity_invalid",
                context={"field": "task_id"},
            )
        return normalized

    @staticmethod
    def _validate_loop_index(loop_index: int) -> int:
        if isinstance(loop_index, bool):
            value = 0
        else:
            try:
                value = int(loop_index)
            except (TypeError, ValueError):
                value = 0
        if value < 1:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace loop_index must be a positive integer",
                reason_code="qe_workspace_submission_identity_invalid",
                context={"field": "loop_index", "value": loop_index},
            )
        return value

    @staticmethod
    def _validate_loop_id(loop_id: str) -> str:
        normalized = str(loop_id or "").strip()
        if not _LOOP_ID_RE.fullmatch(normalized):
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace loop_id must use the canonical LoopN identity",
                reason_code="qe_workspace_submission_identity_invalid",
                context={"field": "loop_id", "value": loop_id},
            )
        return normalized

    @staticmethod
    def _validate_sha256(value: Any, *, field_name: str) -> str:
        normalized = str(value or "").strip().lower()
        if not _SHA256_RE.fullmatch(normalized):
            raise QEWorkspaceSubmissionContractError(
                f"{field_name} must be a lowercase SHA-256 hex digest",
                reason_code="qe_workspace_submission_receipt_invalid",
                context={"field": field_name},
            )
        return normalized

    @classmethod
    def _validate_execution_binding(
        cls,
        *,
        execution_identity_hash: str | None,
        execution_environment_snapshot_id: str | None,
        execution_environment_manifest_sha256: str | None,
    ) -> dict[str, str | None]:
        values = (
            execution_identity_hash,
            execution_environment_snapshot_id,
            execution_environment_manifest_sha256,
        )
        if all(value is None for value in values):
            return {
                "execution_identity_hash": None,
                "execution_environment_snapshot_id": None,
                "execution_environment_manifest_sha256": None,
            }
        if any(value is None or not str(value).strip() for value in values):
            raise QEWorkspaceSubmissionContractError(
                "execution identity binding requires identity hash, environment snapshot id, and environment manifest hash together",
                reason_code="qe_workspace_execution_identity_invalid",
            )
        snapshot_id = str(execution_environment_snapshot_id).strip()
        if not _ENVIRONMENT_SNAPSHOT_ID_RE.fullmatch(snapshot_id):
            raise QEWorkspaceSubmissionContractError(
                "execution_environment_snapshot_id has an invalid format",
                reason_code="qe_workspace_execution_identity_invalid",
                context={"execution_environment_snapshot_id": execution_environment_snapshot_id},
            )
        return {
            "execution_identity_hash": cls._validate_sha256(
                execution_identity_hash,
                field_name="execution_identity_hash",
            ),
            "execution_environment_snapshot_id": snapshot_id,
            "execution_environment_manifest_sha256": cls._validate_sha256(
                execution_environment_manifest_sha256,
                field_name="execution_environment_manifest_sha256",
            ),
        }

    @staticmethod
    def _validate_process_identity(value: Any) -> dict[str, int]:
        if not isinstance(value, Mapping):
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace process_identity must be an object",
                reason_code="qe_workspace_submission_receipt_invalid",
            )
        required = {"pid", "pgid", "start_time_ticks"}
        if set(value) != required:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace process_identity must contain exactly pid, pgid, start_time_ticks",
                reason_code="qe_workspace_submission_receipt_invalid",
                context={"actual_fields": sorted(str(key) for key in value)},
            )
        normalized: dict[str, int] = {}
        for field_name in sorted(required):
            raw = value[field_name]
            if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
                raise QEWorkspaceSubmissionContractError(
                    f"QE Workspace process_identity.{field_name} must be a positive integer",
                    reason_code="qe_workspace_submission_receipt_invalid",
                    context={"field": field_name, "value": raw},
                )
            normalized[field_name] = raw
        return normalized

    @classmethod
    def _parse_execution_environment(cls, payload: Any) -> QEWorkspaceExecutionEnvironment:
        if not isinstance(payload, Mapping):
            raise QEWorkspaceExecutionEnvironmentError(
                "QE Workspace execution environment response must be a JSON object",
                reason_code="qe_workspace_execution_environment_invalid",
            )
        required = {
            "schema_version",
            "execution_environment_snapshot_id",
            "execution_environment_manifest_sha256",
            "manifest",
        }
        missing = sorted(required.difference(payload))
        if missing:
            raise QEWorkspaceExecutionEnvironmentError(
                "QE Workspace execution environment response is missing required fields",
                reason_code="qe_workspace_execution_environment_invalid",
                context={"missing_fields": missing},
            )
        if payload.get("schema_version") != _QE_EXECUTION_ENVIRONMENT_SCHEMA:
            raise QEWorkspaceExecutionEnvironmentError(
                "QE Workspace execution environment schema is unsupported",
                reason_code="qe_workspace_execution_environment_schema_unsupported",
                context={"schema_version": payload.get("schema_version")},
            )
        snapshot_id = str(payload.get("execution_environment_snapshot_id") or "").strip()
        if not _ENVIRONMENT_SNAPSHOT_ID_RE.fullmatch(snapshot_id):
            raise QEWorkspaceExecutionEnvironmentError(
                "QE Workspace execution environment snapshot id is invalid",
                reason_code="qe_workspace_execution_environment_invalid",
            )
        manifest_hash = cls._validate_sha256(
            payload.get("execution_environment_manifest_sha256"),
            field_name="execution_environment_manifest_sha256",
        )
        manifest = payload.get("manifest")
        if not isinstance(manifest, Mapping):
            raise QEWorkspaceExecutionEnvironmentError(
                "QE Workspace execution environment manifest must be an object",
                reason_code="qe_workspace_execution_environment_invalid",
            )
        try:
            canonical = json.dumps(
                dict(manifest),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise QEWorkspaceExecutionEnvironmentError(
                "QE Workspace execution environment manifest is not canonical JSON compatible",
                reason_code="qe_workspace_execution_environment_invalid",
            ) from exc
        actual_hash = hashlib.sha256(canonical).hexdigest()
        if actual_hash != manifest_hash:
            raise QEWorkspaceExecutionEnvironmentError(
                "QE Workspace execution environment manifest hash mismatch",
                reason_code="qe_workspace_execution_environment_hash_mismatch",
                context={"expected": manifest_hash, "actual": actual_hash},
            )
        return QEWorkspaceExecutionEnvironment(
            schema_version=_QE_EXECUTION_ENVIRONMENT_SCHEMA,
            execution_environment_snapshot_id=snapshot_id,
            execution_environment_manifest_sha256=manifest_hash,
            manifest=dict(manifest),
        )

    @classmethod
    def _parse_dataset_identity(cls, payload: Any) -> QEWorkspaceDatasetIdentity:
        if not isinstance(payload, Mapping):
            raise QEWorkspaceDatasetIdentityError(
                "QE Workspace dataset identity response must be a JSON object",
                reason_code="qe_workspace_dataset_identity_invalid",
            )
        required = {"schema_version", "complete", "reason_code", "missing", "acquisition_suggestions", "dataset"}
        missing_fields = sorted(required.difference(payload))
        if missing_fields:
            raise QEWorkspaceDatasetIdentityError(
                "QE Workspace dataset identity response is missing required fields",
                reason_code="qe_workspace_dataset_identity_invalid",
                context={"missing_fields": missing_fields},
            )
        complete = payload.get("complete")
        if not isinstance(complete, bool):
            raise QEWorkspaceDatasetIdentityError(
                "QE Workspace dataset identity complete must be boolean",
                reason_code="qe_workspace_dataset_identity_invalid",
            )
        raw_missing = payload.get("missing")
        raw_suggestions = payload.get("acquisition_suggestions")
        if (
            not isinstance(raw_missing, list)
            or not all(isinstance(item, str) and item.strip() for item in raw_missing)
            or not isinstance(raw_suggestions, list)
            or not all(isinstance(item, str) and item.strip() for item in raw_suggestions)
        ):
            raise QEWorkspaceDatasetIdentityError(
                "QE Workspace dataset identity evidence lists are invalid",
                reason_code="qe_workspace_dataset_identity_invalid",
            )
        reason_code = payload.get("reason_code")
        if reason_code is not None and (not isinstance(reason_code, str) or not reason_code.strip()):
            raise QEWorkspaceDatasetIdentityError(
                "QE Workspace dataset identity reason_code is invalid",
                reason_code="qe_workspace_dataset_identity_invalid",
            )
        raw_dataset = payload.get("dataset")
        if complete:
            if payload.get("schema_version") != "qe_dataset_identity_v1" or not isinstance(raw_dataset, Mapping):
                raise QEWorkspaceDatasetIdentityError(
                    "QE Workspace complete dataset identity is malformed",
                    reason_code="qe_workspace_dataset_identity_invalid",
                )
            required_dataset_fields = {
                "deployment_snapshot_id",
                "dataset_manifest_sha256",
                "cutoff_trade_date",
                "qlib_calendar_sha256",
                "qlib_instruments_sha256",
                "st_pit_snapshot_id",
                "st_pit_manifest_sha256",
                "resolved_node_id",
                "resolved_data_root_uri",
            }
            dataset_missing = sorted(required_dataset_fields.difference(raw_dataset))
            if dataset_missing or any(
                not isinstance(raw_dataset.get(field), str) or not str(raw_dataset[field]).strip()
                for field in required_dataset_fields
            ):
                raise QEWorkspaceDatasetIdentityError(
                    "QE Workspace complete dataset identity is missing required immutable fields",
                    reason_code="qe_workspace_dataset_identity_invalid",
                    context={"missing_fields": dataset_missing},
                )
            for field in (
                "dataset_manifest_sha256",
                "qlib_calendar_sha256",
                "qlib_instruments_sha256",
                "st_pit_manifest_sha256",
            ):
                cls._validate_sha256(raw_dataset[field], field_name=f"dataset.{field}")
            dataset = {field: str(raw_dataset[field]).strip() for field in required_dataset_fields}
            return QEWorkspaceDatasetIdentity(
                schema_version="qe_dataset_identity_v1",
                complete=True,
                reason_code=None,
                missing=(),
                acquisition_suggestions=(),
                dataset=dataset,
                detail=None,
            )
        if payload.get("schema_version") != "qe_dataset_identity_evidence_v1" or raw_dataset is not None:
            raise QEWorkspaceDatasetIdentityError(
                "QE Workspace incomplete dataset identity evidence is malformed",
                reason_code="qe_workspace_dataset_identity_invalid",
            )
        return QEWorkspaceDatasetIdentity(
            schema_version="qe_dataset_identity_evidence_v1",
            complete=False,
            reason_code=str(reason_code or "qe_workspace_dataset_identity_incomplete"),
            missing=tuple(raw_missing),
            acquisition_suggestions=tuple(raw_suggestions),
            dataset=None,
            detail=str(payload.get("detail")) if payload.get("detail") is not None else None,
        )

    @classmethod
    def _parse_submission_receipt(
        cls,
        payload: Any,
        *,
        task_id: str,
        expected_loop_id: str,
        expected_intent_hash: str,
        expected_execution_binding: Mapping[str, str | None],
    ) -> QEWorkspaceSubmissionReceipt:
        if not isinstance(payload, Mapping):
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace submission response must be a JSON object",
                reason_code="qe_workspace_submission_response_invalid",
                context={"task_id": task_id, "loop_id": expected_loop_id},
            )
        required = {
            "loop_id",
            "status",
            "submission_intent_hash",
            "request_digest",
            "receipt_status",
            "duplicate_replay",
        }
        missing = sorted(required.difference(payload))
        if missing:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace submission response is missing receipt fields",
                reason_code="qe_workspace_submission_response_invalid",
                context={
                    "task_id": task_id,
                    "loop_id": expected_loop_id,
                    "missing_fields": missing,
                },
            )
        actual_loop_id = cls._validate_loop_id(str(payload.get("loop_id") or ""))
        if actual_loop_id != expected_loop_id:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace returned a different loop identity",
                reason_code="qe_workspace_submission_identity_mismatch",
                context={"expected_loop_id": expected_loop_id, "actual_loop_id": actual_loop_id},
            )
        actual_intent_hash = cls._validate_sha256(
            payload.get("submission_intent_hash"),
            field_name="submission_intent_hash",
        )
        if actual_intent_hash != expected_intent_hash:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace returned a different submission intent hash",
                reason_code="qe_workspace_submission_identity_mismatch",
                context={
                    "task_id": task_id,
                    "loop_id": expected_loop_id,
                    "expected_submission_intent_hash": expected_intent_hash,
                    "actual_submission_intent_hash": actual_intent_hash,
                },
            )
        request_digest = cls._validate_sha256(
            payload.get("request_digest"),
            field_name="request_digest",
        )
        receipt_status = str(payload.get("receipt_status") or "").strip().lower()
        if receipt_status not in _QE_SUBMISSION_RECEIPT_STATUSES - {"not_reserved"}:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace returned an invalid receipt status",
                reason_code="qe_workspace_submission_receipt_invalid",
                context={"receipt_status": receipt_status},
            )
        if str(payload.get("status") or "").strip().lower() != "accepted":
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace submission response did not acknowledge acceptance",
                reason_code="qe_workspace_submission_response_invalid",
                context={"status": payload.get("status")},
            )
        duplicate_replay = payload.get("duplicate_replay")
        if not isinstance(duplicate_replay, bool):
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace duplicate_replay must be boolean",
                reason_code="qe_workspace_submission_response_invalid",
            )
        actual_execution_binding = cls._validate_execution_binding(
            execution_identity_hash=payload.get("execution_identity_hash"),
            execution_environment_snapshot_id=payload.get("execution_environment_snapshot_id"),
            execution_environment_manifest_sha256=payload.get("execution_environment_manifest_sha256"),
        )
        if dict(actual_execution_binding) != dict(expected_execution_binding):
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace returned a different execution identity binding",
                reason_code="qe_workspace_execution_identity_mismatch",
                context={
                    "expected_execution_binding": dict(expected_execution_binding),
                    "actual_execution_binding": dict(actual_execution_binding),
                },
            )
        return QEWorkspaceSubmissionReceipt(
            task_id=task_id,
            loop_id=actual_loop_id,
            submission_intent_hash=actual_intent_hash,
            request_digest=request_digest,
            receipt_status=receipt_status,
            duplicate_replay=duplicate_replay,
            execution_identity_hash=actual_execution_binding["execution_identity_hash"],
            execution_environment_snapshot_id=actual_execution_binding[
                "execution_environment_snapshot_id"
            ],
            execution_environment_manifest_sha256=actual_execution_binding[
                "execution_environment_manifest_sha256"
            ],
        )

    @classmethod
    def _parse_submission_inspection(
        cls,
        payload: Any,
        *,
        task_id: str,
        expected_loop_id: str,
    ) -> QEWorkspaceSubmissionInspection:
        if not isinstance(payload, Mapping):
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace receipt inspection must be a JSON object",
                reason_code="qe_workspace_submission_receipt_invalid",
            )
        required = {"schema_version", "task_id", "loop_id", "status"}
        missing = sorted(required.difference(payload))
        if missing:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace receipt inspection is missing required fields",
                reason_code="qe_workspace_submission_receipt_invalid",
                context={"missing_fields": missing},
            )
        schema_version = str(payload.get("schema_version") or "")
        if schema_version != _QE_SUBMISSION_RECEIPT_SCHEMA:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace receipt schema is unsupported",
                reason_code="qe_workspace_submission_receipt_schema_unsupported",
                context={"schema_version": schema_version},
            )
        actual_task_id = str(payload.get("task_id") or "").strip()
        actual_loop_id = cls._validate_loop_id(str(payload.get("loop_id") or ""))
        if actual_task_id != task_id or actual_loop_id != expected_loop_id:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace receipt identity does not match the requested loop",
                reason_code="qe_workspace_submission_identity_mismatch",
                context={
                    "expected_task_id": task_id,
                    "actual_task_id": actual_task_id,
                    "expected_loop_id": expected_loop_id,
                    "actual_loop_id": actual_loop_id,
                },
            )
        status = str(payload.get("status") or "").strip().lower()
        if status not in _QE_SUBMISSION_RECEIPT_STATUSES:
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace receipt status is invalid",
                reason_code="qe_workspace_submission_receipt_invalid",
                context={"status": status},
            )
        intent_hash: str | None = None
        request_digest: str | None = None
        if status == "not_reserved":
            unexpected = [
                field
                for field in ("submission_intent_hash", "request_digest")
                if payload.get(field) not in (None, "")
            ]
            if unexpected:
                raise QEWorkspaceSubmissionContractError(
                    "not_reserved receipt must not claim a persisted submission identity",
                    reason_code="qe_workspace_submission_receipt_invalid",
                    context={"unexpected_fields": unexpected},
                )
        else:
            intent_hash = cls._validate_sha256(
                payload.get("submission_intent_hash"),
                field_name="submission_intent_hash",
            )
            request_digest = cls._validate_sha256(
                payload.get("request_digest"),
                field_name="request_digest",
            )
        execution_binding = cls._validate_execution_binding(
            execution_identity_hash=payload.get("execution_identity_hash"),
            execution_environment_snapshot_id=payload.get("execution_environment_snapshot_id"),
            execution_environment_manifest_sha256=payload.get("execution_environment_manifest_sha256"),
        )
        pid = payload.get("pid")
        if pid is not None and (isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0):
            raise QEWorkspaceSubmissionContractError(
                "QE Workspace receipt pid must be a positive integer when present",
                reason_code="qe_workspace_submission_receipt_invalid",
                context={"pid": pid},
            )
        process_identity = payload.get("process_identity")
        if process_identity is not None:
            normalized_identity = cls._validate_process_identity(process_identity)
            if pid != normalized_identity["pid"]:
                raise QEWorkspaceSubmissionContractError(
                    "QE Workspace receipt pid does not match process_identity.pid",
                    reason_code="qe_workspace_submission_receipt_invalid",
                    context={"pid": pid, "process_identity": normalized_identity},
                )
        else:
            normalized_identity = None
        return QEWorkspaceSubmissionInspection(
            schema_version=schema_version,
            task_id=actual_task_id,
            loop_id=actual_loop_id,
            status=status,
            submission_intent_hash=intent_hash,
            request_digest=request_digest,
            created_at=payload.get("created_at"),
            updated_at=payload.get("updated_at"),
            started_at=payload.get("started_at"),
            running_at=payload.get("running_at"),
            finished_at=payload.get("finished_at"),
            pid=pid,
            process_identity=normalized_identity,
            execution_identity_hash=execution_binding["execution_identity_hash"],
            execution_environment_snapshot_id=execution_binding[
                "execution_environment_snapshot_id"
            ],
            execution_environment_manifest_sha256=execution_binding[
                "execution_environment_manifest_sha256"
            ],
        )

    @classmethod
    def _parse_typed_kill_receipt(
        cls,
        payload: Any,
        *,
        task_id: str,
        loop_id: str,
        command_id: str,
        kill_intent_generation: int,
        kill_intent_hash: str,
        expected_submission_intent_hash: str,
        expected_process_identity: dict[str, int] | None,
        expected_phase: str | None,
    ) -> QEWorkspaceTypedKillReceipt:
        if not isinstance(payload, Mapping):
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt must be a JSON object",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        required = {
            "schema_version",
            "task_id",
            "loop_id",
            "command_id",
            "kill_intent_generation",
            "kill_intent_hash",
            "expected_submission_intent_hash",
            "expected_process_identity",
            "expected_phase",
            "process_identity",
            "status",
            "signal_attempt_count",
            "signal_sent_at",
            "signal_sent",
            "process_observation",
            "result_observation",
            "submission_receipt_status",
            "terminal_reason",
            "error",
            "created_at",
            "updated_at",
            "completed_at",
        }
        missing = sorted(required.difference(payload))
        if missing:
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt is missing required fields",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
                context={"missing_fields": missing},
            )
        if payload.get("schema_version") != _QE_TYPED_KILL_RECEIPT_SCHEMA:
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt schema is unsupported",
                reason_code="qe_workspace_typed_kill_receipt_schema_unsupported",
                context={"schema_version": payload.get("schema_version")},
            )
        actual_task_id = str(payload.get("task_id") or "").strip()
        actual_loop_id = str(payload.get("loop_id") or "").strip()
        actual_command_id = str(payload.get("command_id") or "").strip()
        if not _COMMAND_ID_RE.fullmatch(actual_command_id):
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt command_id is invalid",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        actual_generation = payload.get("kill_intent_generation")
        if isinstance(actual_generation, bool) or not isinstance(actual_generation, int) or actual_generation < 1:
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt generation is invalid",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        try:
            actual_kill_hash = cls._validate_sha256(
                payload.get("kill_intent_hash"),
                field_name="kill_intent_hash",
            )
            actual_submission_hash = cls._validate_sha256(
                payload.get("expected_submission_intent_hash"),
                field_name="expected_submission_intent_hash",
            )
        except QEWorkspaceSubmissionContractError as exc:
            raise QEWorkspaceTypedKillContractError(
                str(exc),
                reason_code="qe_workspace_typed_kill_receipt_invalid",
                context=exc.context,
            ) from exc
        receipt_expected_phase = payload.get("expected_phase")
        if receipt_expected_phase is not None and receipt_expected_phase != "pre_process_start":
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt expected_phase is invalid",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        raw_expected_identity = payload.get("expected_process_identity")
        raw_process_identity = payload.get("process_identity")
        try:
            receipt_expected_identity = (
                None
                if raw_expected_identity is None
                else cls._validate_process_identity(raw_expected_identity)
            )
            receipt_process_identity = (
                None if raw_process_identity is None else cls._validate_process_identity(raw_process_identity)
            )
        except QEWorkspaceSubmissionContractError as exc:
            raise QEWorkspaceTypedKillContractError(
                str(exc),
                reason_code="qe_workspace_typed_kill_receipt_invalid",
                context=exc.context,
            ) from exc
        if (
            actual_task_id != task_id
            or actual_loop_id != loop_id
            or actual_command_id != command_id
            or actual_generation != kill_intent_generation
            or actual_kill_hash != kill_intent_hash
            or actual_submission_hash != expected_submission_intent_hash
            or receipt_expected_identity != expected_process_identity
            or receipt_expected_phase != expected_phase
        ):
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt identity does not match its request",
                reason_code="qe_workspace_typed_kill_identity_mismatch",
                context={
                    "expected_task_id": task_id,
                    "actual_task_id": actual_task_id,
                    "expected_loop_id": loop_id,
                    "actual_loop_id": actual_loop_id,
                    "expected_command_id": command_id,
                    "actual_command_id": actual_command_id,
                },
            )
        receipt_status = str(payload.get("status") or "").strip()
        if receipt_status not in _QE_TYPED_KILL_RECEIPT_STATUSES:
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt status is invalid",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
                context={"status": payload.get("status")},
            )
        signal_attempt_count = payload.get("signal_attempt_count")
        if isinstance(signal_attempt_count, bool) or not isinstance(signal_attempt_count, int) or signal_attempt_count < 0:
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt signal_attempt_count is invalid",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        signal_sent = payload.get("signal_sent")
        if not isinstance(signal_sent, bool):
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt signal_sent must be boolean",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        signal_sent_at = payload.get("signal_sent_at")
        if signal_sent_at is not None and not isinstance(signal_sent_at, str):
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt signal_sent_at is invalid",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        completed_at = payload.get("completed_at")
        if completed_at is not None and not isinstance(completed_at, str):
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt completed_at is invalid",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        for field_name in ("process_observation", "result_observation", "error"):
            value = payload.get(field_name)
            if value is not None and not isinstance(value, Mapping):
                raise QEWorkspaceTypedKillContractError(
                    f"QE Workspace typed kill receipt {field_name} must be an object or null",
                    reason_code="qe_workspace_typed_kill_receipt_invalid",
                )
        submission_receipt_status = payload.get("submission_receipt_status")
        terminal_reason = payload.get("terminal_reason")
        if submission_receipt_status is not None and not isinstance(submission_receipt_status, str):
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt submission_receipt_status is invalid",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        if terminal_reason is not None and not isinstance(terminal_reason, str):
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt terminal_reason is invalid",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        created_at = str(payload.get("created_at") or "").strip()
        updated_at = str(payload.get("updated_at") or "").strip()
        if not created_at or not updated_at:
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill receipt must carry timestamps",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            )
        return QEWorkspaceTypedKillReceipt(
            schema_version=_QE_TYPED_KILL_RECEIPT_SCHEMA,
            task_id=actual_task_id,
            loop_id=actual_loop_id,
            command_id=actual_command_id,
            kill_intent_generation=actual_generation,
            kill_intent_hash=actual_kill_hash,
            expected_submission_intent_hash=actual_submission_hash,
            expected_process_identity=receipt_expected_identity,
            expected_phase=receipt_expected_phase,
            process_identity=receipt_process_identity,
            status=receipt_status,
            signal_attempt_count=signal_attempt_count,
            signal_sent_at=signal_sent_at,
            signal_sent=signal_sent,
            process_observation=(
                dict(payload["process_observation"])
                if isinstance(payload.get("process_observation"), Mapping)
                else None
            ),
            result_observation=(
                dict(payload["result_observation"])
                if isinstance(payload.get("result_observation"), Mapping)
                else None
            ),
            submission_receipt_status=submission_receipt_status,
            terminal_reason=terminal_reason,
            error=dict(payload["error"]) if isinstance(payload.get("error"), Mapping) else None,
            created_at=created_at,
            updated_at=updated_at,
            completed_at=completed_at,
        )

    @staticmethod
    def _typed_kill_rejected(exc: httpx.HTTPStatusError) -> QEWorkspaceTypedKillRejected:
        response = exc.response
        reason_code = "qe_workspace_typed_kill_rejected"
        message = f"QE Workspace rejected typed kill with HTTP {response.status_code}"
        detail: Any = None
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if isinstance(payload, Mapping):
            detail = payload.get("detail")
            if isinstance(detail, Mapping):
                reason_code = str(detail.get("reason_code") or reason_code)
                message = str(detail.get("message") or message)
            elif detail not in (None, ""):
                message = str(detail)
        return QEWorkspaceTypedKillRejected(
            message,
            status_code=response.status_code,
            reason_code=reason_code,
            context={"response_detail": detail},
        )

    @staticmethod
    def _submission_rejected(exc: httpx.HTTPStatusError) -> QEWorkspaceSubmissionRejected:
        response = exc.response
        reason_code = "qe_workspace_submission_rejected"
        message = f"QE Workspace rejected the submission with HTTP {response.status_code}"
        detail: Any = None
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if isinstance(payload, Mapping):
            detail = payload.get("detail")
            if isinstance(detail, Mapping):
                reason_code = str(detail.get("reason_code") or reason_code)
                message = str(detail.get("message") or message)
            elif detail not in (None, ""):
                message = str(detail)
        return QEWorkspaceSubmissionRejected(
            message,
            status_code=response.status_code,
            reason_code=reason_code,
            context={"response_detail": detail},
        )
        
    async def get_loop_status(self, task_id: str, loop_id: str) -> Dict[str, Any]:
        """
        查询 WSL 侧 QLib 任务执行的状态（双参数：task_id + loop_id）
        """
        url = f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/status"
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to get status for task {task_id} loop {loop_id}: {str(e)}")
            raise
        
    async def get_loop_metrics(self, task_id: str, loop_id: str) -> Dict[str, Any]:
        """
        获取某个 LOOP 跑完后的各项指标（双参数：task_id + loop_id）。
        404 时重试一次（等待 5s，可能 read_exp_res.py 还未完成），最终仍失败则抛异常。
        """
        url = f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/metrics"
        import asyncio
        for attempt in range(2):
            try:
                response = await self.client.get(url)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not payload:
                    raise RuntimeError(
                        f"回测指标响应为空或格式错误: task={task_id} loop={loop_id} payload={payload}"
                    )
                return payload
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404 and attempt == 0:
                    logger.warning(f"Metrics not ready yet for {task_id}/{loop_id}, retrying in 5s...")
                    await asyncio.sleep(5)
                    continue
                raise RuntimeError(f"Failed to get metrics for task {task_id} loop {loop_id}: {e}") from e
            except httpx.HTTPError as e:
                raise RuntimeError(f"Failed to get metrics for task {task_id} loop {loop_id}: {e}") from e

    async def kill_loop(self, task_id: str, loop_id: str) -> Dict[str, Any]:
        """终止 RDAgent 侧正在运行的 Loop 进程。"""
        url = f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/kill"
        try:
            response = await self.client.post(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            raise RuntimeError(f"Failed to kill loop {task_id}/{loop_id}: {e}") from e

    async def kill_loop_typed(
        self,
        task_id: str,
        loop_id: str,
        *,
        command_id: str,
        kill_intent_generation: int,
        kill_intent_hash: str,
        expected_submission_intent_hash: str,
        expected_process_identity: Mapping[str, Any] | None,
        expected_phase: str | None,
    ) -> QEWorkspaceTypedKillReceipt:
        """Deliver one durable PID-reuse-safe cancellation intent.

        This is intentionally separate from :meth:`kill_loop`: legacy callers keep
        their PID-only endpoint and response contract, while durable multi-alpha
        control receives a typed receipt for reconciliation and never retries a
        remote signal implicitly.
        """

        normalized_task_id = self._validate_task_id(task_id)
        normalized_loop_id = self._validate_loop_id(
            self._to_rdagent_loop_id(normalized_task_id, loop_id),
        )
        normalized_command_id = str(command_id or "").strip()
        if not _COMMAND_ID_RE.fullmatch(normalized_command_id):
            raise QEWorkspaceTypedKillContractError(
                "command_id must be a stable safe command identity",
                reason_code="qe_workspace_typed_kill_request_invalid",
                context={"command_id": command_id},
            )
        if isinstance(kill_intent_generation, bool):
            generation = 0
        else:
            try:
                generation = int(kill_intent_generation)
            except (TypeError, ValueError):
                generation = 0
        if generation < 1:
            raise QEWorkspaceTypedKillContractError(
                "kill_intent_generation must be an integer >= 1",
                reason_code="qe_workspace_typed_kill_request_invalid",
            )
        try:
            normalized_kill_hash = self._validate_sha256(
                kill_intent_hash,
                field_name="kill_intent_hash",
            )
            normalized_submission_hash = self._validate_sha256(
                expected_submission_intent_hash,
                field_name="expected_submission_intent_hash",
            )
        except QEWorkspaceSubmissionContractError as exc:
            raise QEWorkspaceTypedKillContractError(
                str(exc),
                reason_code="qe_workspace_typed_kill_request_invalid",
                context=exc.context,
            ) from exc
        if expected_phase is not None and expected_phase != "pre_process_start":
            raise QEWorkspaceTypedKillContractError(
                "expected_phase must be null or pre_process_start",
                reason_code="qe_workspace_typed_kill_request_invalid",
            )
        if expected_phase == "pre_process_start":
            if expected_process_identity is not None:
                raise QEWorkspaceTypedKillContractError(
                    "expected_phase pre_process_start cannot include expected_process_identity",
                    reason_code="qe_workspace_typed_kill_request_invalid",
                )
            normalized_identity = None
        else:
            if expected_process_identity is None:
                raise QEWorkspaceTypedKillContractError(
                    "typed running-process cancellation requires expected_process_identity",
                    reason_code="qe_workspace_typed_kill_request_invalid",
                )
            try:
                normalized_identity = self._validate_process_identity(
                    expected_process_identity,
                )
            except QEWorkspaceSubmissionContractError as exc:
                raise QEWorkspaceTypedKillContractError(
                    str(exc),
                    reason_code="qe_workspace_typed_kill_request_invalid",
                    context=exc.context,
                ) from exc

        url = (
            f"{self.base_url}/tasks/{normalized_task_id}/loops/"
            f"{normalized_loop_id}/kill-intents"
        )
        payload = {
            "command_id": normalized_command_id,
            "kill_intent_generation": generation,
            "kill_intent_hash": normalized_kill_hash,
            "expected_submission_intent_hash": normalized_submission_hash,
            "expected_process_identity": normalized_identity,
            "expected_phase": expected_phase,
        }
        try:
            response = await self.client.post(url, json=normalize_json(payload))
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise self._typed_kill_rejected(exc) from exc
        except httpx.RequestError as exc:
            raise QEWorkspaceTypedKillTransportError(
                "QE Workspace typed kill transport failed; remote delivery is unknown",
                reason_code="qe_workspace_typed_kill_transport_unknown",
                context={
                    "task_id": normalized_task_id,
                    "loop_id": normalized_loop_id,
                    "command_id": normalized_command_id,
                    "url": url,
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
            ) from exc
        try:
            response_payload = response.json()
        except ValueError as exc:
            raise QEWorkspaceTypedKillContractError(
                "QE Workspace typed kill response is not valid JSON",
                reason_code="qe_workspace_typed_kill_receipt_invalid",
            ) from exc
        return self._parse_typed_kill_receipt(
            response_payload,
            task_id=normalized_task_id,
            loop_id=normalized_loop_id,
            command_id=normalized_command_id,
            kill_intent_generation=generation,
            kill_intent_hash=normalized_kill_hash,
            expected_submission_intent_hash=normalized_submission_hash,
            expected_process_identity=normalized_identity,
            expected_phase=expected_phase,
        )

    async def get_enhanced_metrics(self, task_id: str, loop_id: str) -> Dict[str, Any]:
        """
        获取增强诊断指标（训练曲线、IC 时间序列、收益曲线等）。
        Loop 已完成时调用，数据必须存在。404 时重试一次（read_exp_res.py 可能尚未写完）。
        """
        rdagent_loop_id = self._to_rdagent_loop_id(task_id, loop_id)
        url = f"{self.base_url}/tasks/{task_id}/loops/{rdagent_loop_id}/enhanced-metrics"
        import asyncio
        for attempt in range(2):
            try:
                response = await self.client.get(url)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not payload:
                    raise RuntimeError(
                        f"增强指标响应为空或格式错误: task={task_id} loop={loop_id} payload={payload}"
                    )
                return payload
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404 and attempt == 0:
                    logger.warning(f"Enhanced metrics not ready yet for {task_id}/{loop_id}, retrying in 5s...")
                    await asyncio.sleep(5)
                    continue
                raise RuntimeError(f"Failed to get enhanced metrics for task {task_id} loop {loop_id}: {e}") from e
            except httpx.HTTPError as e:
                raise RuntimeError(f"Failed to get enhanced metrics for task {task_id} loop {loop_id}: {e}") from e

    async def stream_task_logs(self, task_id: str):
        """
        从 RDAgent 侧实时拉取任务日志流。
        SSE 长连接不设 read timeout — 实验可能运行数小时，训练期间可能长时间无输出。
        """
        url = f"{self.base_url}/tasks/{task_id}/logs"
        stream_timeout = httpx.Timeout(connect=30.0, read=None, write=10.0, pool=10.0)
        async with httpx.AsyncClient(timeout=stream_timeout, trust_env=False) as stream_client:
            async with stream_client.stream("GET", url) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    yield line

    async def download_mlruns_params(self, task_id: str, loop_id: str) -> Optional[bytes]:
        """从节点下载指定 loop 的 mlruns params.pkl（tar.gz 打包，保留目录结构）。

        Returns: tar.gz bytes.
        """
        url = f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/mlruns-params"
        try:
            response = await self.client.get(url, timeout=60.0)
            response.raise_for_status()
            return response.content
        except httpx.HTTPError as e:
            raise RuntimeError(f"download_mlruns_params failed for {task_id}/{loop_id}: {e}") from e

    async def download_loop_assets(self, task_id: str, loop_id: str, dest_dir: str) -> str:
        """
        调用 API 将 models/*.pkl 和 features_order.txt 打包下载，并解压到 AIstock 本地的 dest_dir
        （双参数：task_id + loop_id）
        """
        url = f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/assets/download"
        zip_path = os.path.join(dest_dir, f"{loop_id}_assets.zip")
        
        try:
            os.makedirs(dest_dir, exist_ok=True)
            async with self.client.stream("GET", url) as response:
                response.raise_for_status()
                async with aiofiles.open(zip_path, 'wb') as f:
                    async for chunk in response.aiter_bytes():
                        await f.write(chunk)

            with zipfile.ZipFile(zip_path, "r") as zf:
                real_dest = os.path.realpath(dest_dir) + os.sep
                for info in zf.infolist():
                    target = os.path.realpath(os.path.join(dest_dir, info.filename))
                    if not target.startswith(real_dest) and target != real_dest.rstrip(os.sep):
                        raise ValueError(f"ZIP 路径遍历攻击: {info.filename}")
                zf.extractall(dest_dir)

            logger.info(f"Successfully downloaded assets for {loop_id} to {zip_path}")
            return dest_dir
        except httpx.HTTPError as e:
            logger.error(f"Failed to download assets for {loop_id}: {str(e)}")
            raise
        
    async def get_workspace_config(self) -> Dict[str, Any]:
        """
        获取 RDAgent 侧的工作区配置（路径等），用于动态生成 WSL 命令。
        """
        url = f"{self.base_url}/config"
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to get workspace config: {e}")
            raise

    async def download_group_predictions(
        self, task_id: str, loop_id: str, group_name: str
    ) -> bytes:
        """从节点下载指定组的 pred.pkl（用于多Alpha跨节点预测收集）。"""
        rdagent_loop_id = self._to_rdagent_loop_id(task_id, loop_id)
        url = f"{self.base_url}/tasks/{task_id}/loops/{rdagent_loop_id}/groups/{group_name}/predictions"
        try:
            response = await self.client.get(url, timeout=60.0)
            response.raise_for_status()
            return response.content
        except httpx.HTTPError as e:
            raise RuntimeError(f"下载组预测失败: task={task_id} loop={loop_id} group={group_name}: {e}") from e

    async def download_workspace_file_bytes(
        self, task_id: str, loop_id: str, file_path: str
    ) -> bytes:
        """下载 workspace 中的任意文件原始字节。"""
        rdagent_loop_id = self._to_rdagent_loop_id(task_id, loop_id)
        url = f"{self.base_url}/tasks/{task_id}/loops/{rdagent_loop_id}/files/{file_path}"
        try:
            response = await self.client.get(url, timeout=60.0)
            response.raise_for_status()
            return response.content
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise QEWorkspaceFileNotFound(task_id, loop_id, file_path, url) from e
            raise RuntimeError(
                f"download workspace file failed: task={task_id} loop={loop_id} file={file_path}: {e}"
            ) from e
        except httpx.HTTPError as e:
            raise RuntimeError(f"下载 workspace 文件失败: task={task_id} loop={loop_id} file={file_path}: {e}") from e

    async def list_workspace_files(self, task_id: str, loop_id: str) -> Dict[str, Any]:
        """List a loop's complete read-only asset catalog.

        The node response must explicitly declare ``catalog_completeness``.
        Older nodes do not expose this endpoint; callers must surface that as a
        partial/unavailable catalog instead of guessing common file names.
        """
        rdagent_loop_id = self._to_rdagent_loop_id(task_id, loop_id)
        url = f"{self.base_url}/tasks/{task_id}/loops/{rdagent_loop_id}/files"
        try:
            response = await self.client.get(url, timeout=60.0)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise QEWorkspaceCatalogInvalid(
                    f"invalid workspace catalog response for {task_id}/{rdagent_loop_id}"
                )
            rows = payload.get("files")
            if rows is None:
                rows = payload.get("assets")
            if not isinstance(rows, list):
                raise QEWorkspaceCatalogInvalid(
                    f"invalid workspace catalog response for {task_id}/{rdagent_loop_id}"
                )
            if payload.get("catalog_completeness") not in {"complete", "partial"}:
                raise QEWorkspaceCatalogInvalid(
                    "workspace catalog response must declare catalog_completeness"
                )
            return payload
        except httpx.HTTPStatusError as e:
            if e.response.status_code in {404, 405, 501}:
                raise QEWorkspaceCatalogUnavailable(
                    f"QE node has no loop asset catalog endpoint: {task_id}/{rdagent_loop_id}"
                ) from e
            raise RuntimeError(
                f"list workspace files failed: task={task_id} loop={rdagent_loop_id}: {e}"
            ) from e
        except httpx.HTTPError as e:
            raise RuntimeError(
                f"list workspace files failed: task={task_id} loop={rdagent_loop_id}: {e}"
            ) from e

    async def stat_workspace_file(
        self, task_id: str, loop_id: str, file_path: str
    ) -> Dict[str, Any]:
        """Return catalog metadata for one known loop-relative asset path."""
        payload = await self.list_workspace_files(task_id, loop_id)
        normalized = str(file_path).replace("\\", "/")
        rows = payload.get("files") or payload.get("assets") or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_path = row.get("relative_path") or row.get("path") or row.get("filename")
            if str(row_path or "").replace("\\", "/") == normalized:
                result = dict(row)
                result["catalog_completeness"] = payload["catalog_completeness"]
                return result
        raise QEWorkspaceFileNotFound(
            task_id,
            loop_id,
            file_path,
            f"{self.base_url}/tasks/{task_id}/loops/{self._to_rdagent_loop_id(task_id, loop_id)}/files",
        )

    async def get_workspace_file(self, task_id: str, loop_id: str, file_path: str) -> Dict[str, Any] | str:
        """读取 workspace 中的指定文件内容。"""
        rdagent_loop_id = self._to_rdagent_loop_id(task_id, loop_id)
        url = f"{self.base_url}/tasks/{task_id}/loops/{rdagent_loop_id}/files/{file_path}"
        try:
            response = await self.client.get(url, timeout=30.0)
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            if "json" in content_type:
                payload = response.json()
                if payload is None:
                    raise RuntimeError(
                        f"workspace 文件 JSON 为空: task={task_id} loop={loop_id} file={file_path}"
                    )
                return payload
            if not response.text:
                raise RuntimeError(
                    f"workspace 文件内容为空: task={task_id} loop={loop_id} file={file_path}"
                )
            return response.text
        except httpx.HTTPError as e:
            raise RuntimeError(f"读取 workspace 文件失败: task={task_id} loop={loop_id} file={file_path}: {e}") from e

    async def cleanup_task_workspace(self, task_id: str) -> bool:
        """
        要求 RDAgent 彻底删除任务工作区
        """
        url = f"{self.base_url}/tasks/{task_id}"
        try:
            response = await self.client.delete(url)
            response.raise_for_status()
            return True
        except httpx.HTTPError as e:
            logger.error(f"Failed to cleanup workspace for task {task_id}: {str(e)}")
            raise

    async def cleanup_loop_workspace(self, task_id: str, loop_id: str) -> bool:
        """
        Delete one Loop workspace only. Rerun must not use task-level cleanup,
        otherwise sibling custom_evo loops would lose their artifacts.
        """
        rdagent_loop_id = self._to_rdagent_loop_id(task_id, loop_id)
        url = f"{self.base_url}/tasks/{task_id}/loops/{rdagent_loop_id}"
        try:
            response = await self.client.delete(url)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and payload.get("ok") is False:
                raise RuntimeError(f"Loop cleanup returned ok=false: {payload}")
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise QELoopWorkspaceCleanupUnavailable(
                    "RD-Agent QE workspace API does not expose loop-level cleanup "
                    f"or the loop path is unavailable: {task_id}/{rdagent_loop_id}"
                ) from e
            raise RuntimeError(f"Failed to cleanup loop workspace {task_id}/{rdagent_loop_id}: {e}") from e
        except httpx.HTTPError as e:
            raise RuntimeError(f"Failed to cleanup loop workspace {task_id}/{rdagent_loop_id}: {e}") from e
