"""
回测数据源实现

从 QE workspace 下载 artifact（pred.pkl, label.pkl），缓存到本地，
提供标准化的数据访问接口。

特性:
- 首次下载并缓存 artifact
- 后续访问使用缓存（无重复下载）
- 内存缓存（避免重复加载 pickle）
- 并发安全（asyncio.Lock）
- 使用真实交易日历（market.trading_calendar）
"""

import ast
import asyncio
import hashlib
import json
import re
from datetime import date
from typing import Any, Literal, Mapping, Optional, Tuple

import pandas as pd

from backend.db.pg_pool import get_conn
from backend.services.model_store import ModelStoreService
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient

from .base import HMMDataSourceInterface
from .artifact_manifest import RemoteArtifactManifest
from .cache_manager import ArtifactCacheManager
from .db_repository import HMMDataRepository
from .exceptions import DataSourceError, DateRangeError, HorizonError, DataNotFoundError
from .legacy_qe_artifact_manifests import (
    LegacyQEArtifactManifest,
    find_legacy_qe_artifact_manifest,
)
from .prediction_store_resolver import PredictionStoreArtifactResolver


ArtifactSourcePreference = Literal[
    "prediction_store_first",
    "prediction_store_only",
    "workspace_only",
]
ARTIFACT_SOURCE_PREFERENCES: frozenset[str] = frozenset(
    {"prediction_store_first", "prediction_store_only", "workspace_only"}
)
_RECORDER_START_LOG_RE = re.compile(
    r"Recorder (?P<recorder_id>[0-9a-f]+) starts running under Experiment "
    r"(?P<experiment_id>[0-9]+)"
)
_HISTORICAL_RECORDER_LOG_MAX_BYTES = 2 * 1024 * 1024


class BacktestDataSource(HMMDataSourceInterface):
    """
    回测数据源

    使用 QE workspace 的 artifact（pred.pkl, label.pkl）作为数据源。
    第一次访问时下载并缓存，后续访问使用缓存。

    Example:
        source = BacktestDataSource(
            base_loop_ref="qe_20260502_131502_9b54/Loop1",
            cache_dir="tmp/hmm_evolution_cache/",
        )
        pred_df = await source.get_predictions(
            start_date=date(2024, 7, 1),
            end_date=date(2024, 7, 5),
        )
    """

    # 隔离约束：回测数据源只允许下载 QE 实验产物，禁止下载任何配置文件。
    # 这是「完全隔离，不干扰」的代码级强制：只读 pred/label 数据，不触碰 QE/模拟盘配置。
    ALLOWED_ARTIFACTS: frozenset[str] = frozenset({"pred.pkl", "label.pkl"})

    def __init__(
        self,
        base_loop_ref: str,
        cache_dir: str = "tmp/hmm_evolution_cache/",
        qe_client: Optional[QEWorkspaceClient] = None,
        repository: Optional[HMMDataRepository] = None,
        max_artifact_bytes: int = ArtifactCacheManager.DEFAULT_MAX_ARTIFACT_BYTES,
        max_cache_bytes: int = ArtifactCacheManager.DEFAULT_MAX_CACHE_BYTES,
        cache_ttl_seconds: int = ArtifactCacheManager.DEFAULT_TTL_SECONDS,
        artifact_source_preference: ArtifactSourcePreference = "prediction_store_first",
        model_store: Optional[ModelStoreService] = None,
        label_horizon_days: int = 10,
    ):
        """
        Args:
            base_loop_ref: QE loop 引用（如 "qe_20260502_131502_9b54/Loop1"）
            cache_dir: 缓存目录
            qe_client: QE workspace 客户端（用于测试注入）
            repository: 只读 canonical market repository（用于测试注入）
            max_artifact_bytes: 单个 artifact 最大字节数
            max_cache_bytes: artifact 缓存总字节上限
            cache_ttl_seconds: 缓存有效期（秒）
            artifact_source_preference: Prediction Store / QE workspace 读取策略
            model_store: Prediction Store service（用于测试注入）
            label_horizon_days: label.pkl 对应的显式预测周期
        """
        if artifact_source_preference not in ARTIFACT_SOURCE_PREFERENCES:
            raise ValueError(
                "artifact_source_preference must be one of "
                f"{sorted(ARTIFACT_SOURCE_PREFERENCES)}, got {artifact_source_preference!r}"
            )
        if not 1 <= int(label_horizon_days) <= 30:
            raise ValueError(f"label_horizon_days must be between 1 and 30, got {label_horizon_days}")
        self.base_loop_ref = base_loop_ref
        self.artifact_source_preference = artifact_source_preference
        self.label_horizon_days = int(label_horizon_days)
        self.cache_manager = ArtifactCacheManager(
            cache_dir,
            max_artifact_bytes=max_artifact_bytes,
            max_cache_bytes=max_cache_bytes,
            ttl_seconds=cache_ttl_seconds,
        )
        # Injected clients are owned by the caller.  The default client is
        # resolved lazily from the QE task's authoritative compute node and is
        # closed by this data source.
        self.qe_client = qe_client
        self._owns_qe_client = False
        self.repository = repository or HMMDataRepository()
        self._prediction_store_resolver = PredictionStoreArtifactResolver(model_store)
        self._artifact_source_info: dict[str, dict[str, Any]] = {}

        # 内存缓存（避免重复加载 pickle）
        self._pred_cache: Optional[pd.DataFrame] = None
        self._label_cache: Optional[pd.DataFrame] = None
        self._pred_cache_source: Optional[str] = None
        self._label_cache_source: Optional[str] = None

        # 并发锁（防止重复下载）
        self._download_lock = asyncio.Lock()

    @property
    def mode(self) -> str:
        return "backtest"

    async def __aenter__(self) -> "BacktestDataSource":
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close an internally created QE client and release its HTTP pool."""
        if self._owns_qe_client and self.qe_client is not None:
            await self.qe_client.close()
            self.qe_client = None
            self._owns_qe_client = False

    def get_artifact_source_info(self) -> dict[str, dict[str, Any]]:
        """Return a copy of the source decision for each loaded artifact."""

        return {artifact_name: dict(info) for artifact_name, info in self._artifact_source_info.items()}

    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """获取预测分数"""
        # 验证日期范围
        is_valid, error_msg = await self.validate_date_range(start_date, end_date)
        if not is_valid:
            raise DateRangeError(error_msg)

        # 加载完整预测数据
        pred_df = await self._load_predictions_from_cache()

        # 过滤日期范围
        mask = (pred_df["trade_date"] >= start_date) & (pred_df["trade_date"] <= end_date)
        result_df = pred_df[mask].copy()

        if result_df.empty:
            raise DataNotFoundError(f"No predictions found for date range [{start_date}, {end_date}]")

        return result_df

    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame:
        """获取未来收益标签"""
        # 验证 horizon
        if not 1 <= horizon_days <= 30:
            raise HorizonError(f"horizon_days must be between 1 and 30, got {horizon_days}")

        # 验证日期范围
        is_valid, error_msg = await self.validate_date_range(start_date, end_date)
        if not is_valid:
            raise DateRangeError(error_msg)

        # 加载完整标签数据
        label_df = await self._load_labels_from_cache()

        # 过滤日期范围和 horizon
        mask = (
            (label_df["trade_date"] >= start_date)
            & (label_df["trade_date"] <= end_date)
            & (label_df["horizon_days"] == horizon_days)
        )
        result_df = label_df[mask].copy()

        if result_df.empty:
            raise DataNotFoundError(
                f"No labels found for date range [{start_date}, {end_date}] with horizon_days={horizon_days}"
            )

        return result_df

    async def get_sector_mapping(self, trade_date: date) -> dict[str, str]:
        """
        获取股票板块映射（申万 L2）

        Notes:
            回测模式下，从 market.sw_index_member 查询历史板块映射
        """
        try:
            return await asyncio.to_thread(
                self.repository.get_sector_mapping,
                trade_date,
            )

        except Exception as e:
            raise DataSourceError(f"Failed to query sector mapping: {e}")

    async def get_available_date_range(self) -> Tuple[date, date]:
        """获取数据源可用的日期范围"""
        # 加载预测数据
        pred_df = await self._load_predictions_from_cache()

        min_date = pred_df["trade_date"].min()
        max_date = pred_df["trade_date"].max()

        return min_date, max_date

    async def _load_predictions_from_cache(self) -> pd.DataFrame:
        """
        从缓存加载预测数据

        Returns:
            DataFrame with columns: trade_date, symbol, score, rank
        """
        # 检查内存缓存
        if self._pred_cache is not None:
            if self._pred_cache_source == "prediction_store" or self.cache_manager.is_fresh(
                self.base_loop_ref,
                "pred.pkl",
            ):
                return self._pred_cache
            self._pred_cache = None
            self._pred_cache_source = None

        store_obj = await self._load_prediction_store_artifact("pred.pkl")
        if store_obj is not None:
            try:
                df = self._normalize_prediction_data(store_obj)
            except Exception as e:
                raise DataSourceError(f"Failed to normalize predictions from Prediction Store: {e}") from e
            self._pred_cache = df
            self._pred_cache_source = "prediction_store"
            return df

        # 检查本地缓存
        if not self.cache_manager.is_cached(self.base_loop_ref, "pred.pkl"):
            # 需要下载
            async with self._download_lock:
                # 双重检查（可能其他协程已下载）
                if not self.cache_manager.is_cached(self.base_loop_ref, "pred.pkl"):
                    await self._download_artifact("pred.pkl")

        # 加载 pickle
        try:
            pred_obj = self.cache_manager.load_pickle(self.base_loop_ref, "pred.pkl")

            # 标准化为 DataFrame
            df = self._normalize_prediction_data(pred_obj)

            # 缓存到内存
            self._pred_cache = df
            self._pred_cache_source = "qe_workspace_cache"
            self._artifact_source_info["pred.pkl"] = self._cache_source_info("pred.pkl")

            return df

        except Exception as e:
            raise DataSourceError(f"Failed to load predictions from cache: {e}")

    async def _load_labels_from_cache(self) -> pd.DataFrame:
        """
        从缓存加载标签数据

        Returns:
            DataFrame with columns: trade_date, symbol, horizon_days, future_return, label_date
        """
        # 检查内存缓存
        if self._label_cache is not None:
            if self._label_cache_source == "prediction_store" or self.cache_manager.is_fresh(
                self.base_loop_ref,
                "label.pkl",
            ):
                return self._label_cache
            self._label_cache = None
            self._label_cache_source = None

        store_obj = await self._load_prediction_store_artifact("label.pkl")
        if store_obj is not None:
            try:
                df = await self._normalize_label_data(store_obj)
            except Exception as e:
                raise DataSourceError(f"Failed to normalize labels from Prediction Store: {e}") from e
            self._label_cache = df
            self._label_cache_source = "prediction_store"
            return df

        # 检查本地缓存
        if not self.cache_manager.is_cached(self.base_loop_ref, "label.pkl"):
            # 需要下载
            async with self._download_lock:
                # 双重检查
                if not self.cache_manager.is_cached(self.base_loop_ref, "label.pkl"):
                    await self._download_artifact("label.pkl")

        # 加载 pickle
        try:
            label_obj = self.cache_manager.load_pickle(self.base_loop_ref, "label.pkl")

            # 标准化为 DataFrame
            df = await self._normalize_label_data(label_obj)

            # 缓存到内存
            self._label_cache = df
            self._label_cache_source = "qe_workspace_cache"
            self._artifact_source_info["label.pkl"] = self._cache_source_info("label.pkl")

            return df

        except Exception as e:
            raise DataSourceError(f"Failed to load labels from cache: {e}")

    async def _load_prediction_store_artifact(self, artifact_name: str) -> Any | None:
        """Load an immutable artifact directly from Prediction Store without copying it."""

        if self.artifact_source_preference == "workspace_only":
            return None

        resolved = await asyncio.to_thread(
            self._prediction_store_resolver.resolve,
            loop_ref=self.base_loop_ref,
            artifact_name=artifact_name,
        )
        if resolved is None:
            self._artifact_source_info[artifact_name] = {
                "source": "prediction_store",
                "status": "missing",
                "artifact_name": artifact_name,
                "loop_ref": self.base_loop_ref,
                "fallback": self.artifact_source_preference == "prediction_store_first",
            }
            if self.artifact_source_preference == "prediction_store_only":
                raise DataSourceError(
                    f"Prediction Store artifact is required but missing for {self.base_loop_ref}/{artifact_name}"
                )
            return None

        try:
            artifact_obj = await asyncio.to_thread(pd.read_pickle, resolved.path)
        except Exception as exc:
            raise DataSourceError(
                "Failed to deserialize Prediction Store artifact for "
                f"{self.base_loop_ref}/{artifact_name}: {type(exc).__name__}: {exc}"
            ) from exc

        try:
            actual_row_count = len(artifact_obj)
        except TypeError as exc:
            raise DataSourceError(
                f"Prediction Store artifact has no row count for {self.base_loop_ref}/{artifact_name}"
            ) from exc
        if actual_row_count != resolved.row_count:
            raise DataSourceError(
                "Prediction Store artifact row count mismatch for "
                f"{self.base_loop_ref}/{artifact_name}: "
                f"manifest={resolved.row_count}, actual={actual_row_count}"
            )

        self._artifact_source_info[artifact_name] = {
            **resolved.source_info(),
            "status": "available",
            "loop_ref": self.base_loop_ref,
            "zero_copy": True,
        }
        return artifact_obj

    def _cache_source_info(self, artifact_name: str) -> dict[str, Any]:
        manifest = self.cache_manager.get_artifact_manifest(self.base_loop_ref, artifact_name)
        provenance = manifest.provenance
        return {
            "source": "qe_workspace_cache",
            "artifact_name": artifact_name,
            "loop_ref": self.base_loop_ref,
            "uri": (
                f"qe://{provenance.task_id}/{provenance.loop_name}/{provenance.workspace_path}"
                if provenance.source == "qe_workspace"
                else f"cache://{manifest.cache_key}/{artifact_name}"
            ),
            "sha256": manifest.sha256,
            "size_bytes": manifest.file_size,
            "row_count": provenance.remote_row_count,
            "remote_schema_version": provenance.remote_schema_version,
            "trust_level": "trusted_computational_input",
            "zero_copy": False,
            "fallback": self.artifact_source_preference == "prediction_store_first",
        }

    async def _download_artifact(self, artifact_name: str):
        """
        从 QE workspace 下载 artifact

        Args:
            artifact_name: artifact 名称（pred.pkl 或 label.pkl）

        Raises:
            DataSourceError: 下载失败，或 artifact 名称不在允许白名单内
        """
        # 隔离约束强制：只允许下载数据产物，拒绝配置文件等其他内容
        if artifact_name not in self.ALLOWED_ARTIFACTS:
            raise DataSourceError(
                f"Refused to download '{artifact_name}': only "
                f"{sorted(self.ALLOWED_ARTIFACTS)} are permitted. "
                f"HMM evolution must not fetch QE/paper config files."
            )

        # 解析 base_loop_ref
        # 格式: "qe_20260502_131502_9b54/Loop1"
        parts = self.base_loop_ref.split("/")
        if len(parts) != 2:
            raise DataSourceError(
                f"Invalid base_loop_ref format: {self.base_loop_ref}. Expected format: 'task_id/loop_name'"
            )

        task_id, loop_name = parts

        try:
            client = await self._get_qe_client(task_id)
            artifact_path = await self._resolve_workspace_artifact_path(
                client,
                task_id=task_id,
                loop_name=loop_name,
                artifact_name=artifact_name,
            )
            remote_manifest, remote_manifest_path = await self._resolve_remote_artifact_manifest(
                client,
                task_id=task_id,
                loop_name=loop_name,
                artifact_name=artifact_name,
                artifact_path=artifact_path,
            )

            # 下载 artifact（带重试）
            artifact_bytes = await self._download_with_retry(
                client,
                task_id,
                loop_name,
                artifact_path,
            )

            # 保存到缓存
            self.cache_manager.save_artifact(
                self.base_loop_ref,
                artifact_name,
                artifact_bytes,
                metadata={
                    "source": "qe_workspace",
                    "task_id": task_id,
                    "loop_name": loop_name,
                    "workspace_path": artifact_path,
                    "remote_manifest_path": remote_manifest_path,
                    "remote_schema_version": remote_manifest.schema_version,
                    "remote_sha256": remote_manifest.sha256,
                    "remote_size_bytes": remote_manifest.size_bytes,
                    "remote_row_count": remote_manifest.row_count,
                    "remote_quality_status": remote_manifest.quality_status,
                },
            )

        except Exception as e:
            raise DataSourceError(f"Failed to download {artifact_name} from QE workspace: {e}")

    async def _download_with_retry(
        self,
        client: QEWorkspaceClient,
        task_id: str,
        loop_name: str,
        artifact_path: str,
        max_retries: int = 3,
    ) -> bytes:
        """
        下载 artifact（带重试）

        Args:
            task_id: QE 任务 ID
            loop_name: Loop 名称
            artifact_path: workspace 内经 recorder metadata 解析的 artifact 路径
            max_retries: 最大重试次数

        Returns:
            artifact 内容（bytes）

        Raises:
            DataSourceError: 重试耗尽仍失败
        """
        last_error = None

        for attempt in range(max_retries):
            try:
                artifact_bytes = await client.download_workspace_file_bytes(
                    task_id,
                    loop_name,
                    artifact_path,
                )
                return artifact_bytes

            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    # 指数退避
                    wait_time = 2**attempt
                    await asyncio.sleep(wait_time)

        raise DataSourceError(f"Failed to download {artifact_path} after {max_retries} retries: {last_error}")

    async def _get_qe_client(self, task_id: str) -> QEWorkspaceClient:
        """Return the injected client or lazily resolve the task's node client."""
        if self.qe_client is not None:
            return self.qe_client

        node_id = await asyncio.to_thread(self._resolve_task_node_id, task_id)
        try:
            client = await asyncio.to_thread(QEWorkspaceClient.for_node, node_id)
        except Exception as exc:
            raise DataSourceError(
                f"Failed to create QE workspace client for task={task_id}, node={node_id}: {exc}"
            ) from exc
        self.qe_client = client
        self._owns_qe_client = True
        return client

    @staticmethod
    def _resolve_task_node_id(task_id: str) -> str:
        """Resolve the authoritative compute node recorded for a QE task."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT node_id FROM qe_evolution_tasks WHERE task_id = %s",
                        (task_id,),
                    )
                    row = cur.fetchone()
        except Exception as exc:
            raise DataSourceError(f"Failed to resolve compute node for QE task {task_id}: {exc}") from exc

        node_id = str(row[0]).strip() if row and row[0] else ""
        if not node_id:
            raise DataSourceError(f"QE task {task_id} has no authoritative compute node")
        return node_id

    @classmethod
    async def _resolve_workspace_artifact_path(
        cls,
        client: QEWorkspaceClient,
        *,
        task_id: str,
        loop_name: str,
        artifact_name: str,
    ) -> str:
        """Resolve an allowlisted MLflow artifact from recorder metadata."""
        attempts: dict[str, str] = {}
        invalid_sidecars: dict[str, str] = {}
        sidecar_identities: set[tuple[str, str]] = set()
        for ref_name in ("qe_current_recorder.json", "qe_extracted_recorder.json"):
            try:
                payload: Any = await client.get_workspace_file(
                    task_id,
                    loop_name,
                    ref_name,
                )
            except Exception as exc:
                attempts[ref_name] = f"{type(exc).__name__}: {exc}"
                continue
            try:
                if isinstance(payload, str):
                    payload = json.loads(payload)
                if not isinstance(payload, dict):
                    raise ValueError(f"expected JSON object, got {type(payload).__name__}")

                recorder_id = str(payload.get("recorder_id") or payload.get("selected_recorder_id") or "").strip()
                experiment_id = str(payload.get("experiment_id") or payload.get("selected_experiment_id") or "").strip()
                for field_name, value in (
                    ("recorder_id", recorder_id),
                    ("experiment_id", experiment_id),
                ):
                    if not value or "/" in value or "\\" in value or value in {".", ".."}:
                        raise ValueError(f"invalid {field_name}: {value!r}")
                sidecar_identities.add((experiment_id, recorder_id))
            except Exception as exc:
                invalid_sidecars[ref_name] = f"{type(exc).__name__}: {exc}"

        if invalid_sidecars:
            raise DataSourceError(
                f"Invalid QE recorder sidecar for task={task_id}, loop={loop_name}: "
                f"{invalid_sidecars}"
            )
        if len(sidecar_identities) > 1:
            raise DataSourceError(
                f"Conflicting QE recorder sidecars for task={task_id}, loop={loop_name}: "
                f"{sorted(sidecar_identities)}"
            )
        if sidecar_identities:
            experiment_id, recorder_id = next(iter(sidecar_identities))
            return f"mlruns/{experiment_id}/{recorder_id}/artifacts/{artifact_name}"

        legacy_manifest = find_legacy_qe_artifact_manifest(f"{task_id}/{loop_name}")
        if legacy_manifest is None:
            raise DataSourceError(
                f"QE recorder metadata unavailable for task={task_id}, loop={loop_name}: {attempts}"
            )
        return await cls._resolve_legacy_workspace_artifact_path(
            client,
            task_id=task_id,
            loop_name=loop_name,
            artifact_name=artifact_name,
            manifest=legacy_manifest,
        )

    @staticmethod
    async def _resolve_legacy_workspace_artifact_path(
        client: QEWorkspaceClient,
        *,
        task_id: str,
        loop_name: str,
        artifact_name: str,
        manifest: LegacyQEArtifactManifest,
    ) -> str:
        """Resolve a pre-sidecar recorder only from corroborated immutable evidence."""

        try:
            catalog = await client.list_workspace_files(task_id, loop_name)
        except Exception as exc:
            raise DataSourceError(
                f"Failed to read the complete QE catalog for legacy recorder resolution: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        if catalog.get("catalog_completeness") != "complete":
            raise DataSourceError(
                f"Legacy QE recorder resolution requires a complete catalog for {task_id}/{loop_name}"
            )
        rows = catalog.get("files") or catalog.get("assets") or []
        if not isinstance(rows, list):
            raise DataSourceError(f"Legacy QE catalog rows are invalid for {task_id}/{loop_name}")
        rows_by_path: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            path = str(
                row.get("relative_path") or row.get("path") or row.get("filename") or ""
            ).replace("\\", "/")
            rows_by_path.setdefault(path, []).append(row)

        for required_name in ("pred.pkl", "label.pkl"):
            required_path = manifest.artifact(required_name).workspace_path
            if len(rows_by_path.get(required_path, [])) != 1:
                raise DataSourceError(
                    f"Legacy QE recorder lacks one unique {required_name} catalog entry for "
                    f"{task_id}/{loop_name}"
                )

        evidence = manifest.recorder_evidence
        log_rows = rows_by_path.get(evidence.workspace_path, [])
        if len(log_rows) != 1:
            raise DataSourceError(
                f"Legacy QE recorder requires one cataloged run.log for {task_id}/{loop_name}"
            )
        log_row = log_rows[0]
        if str(log_row.get("access_mode") or "") != "inspection_only":
            raise DataSourceError(
                f"Legacy QE recorder log is not inspection-only for {task_id}/{loop_name}"
            )
        try:
            catalog_size = int(log_row.get("size_bytes"))
        except (TypeError, ValueError) as exc:
            raise DataSourceError(
                f"Legacy QE recorder log has no valid catalog size for {task_id}/{loop_name}"
            ) from exc
        if catalog_size != evidence.size_bytes or catalog_size > _HISTORICAL_RECORDER_LOG_MAX_BYTES:
            raise DataSourceError(
                f"Legacy QE recorder log size differs from its immutable receipt for {task_id}/{loop_name}"
            )

        try:
            payload = await client.get_workspace_file(
                task_id,
                loop_name,
                evidence.workspace_path,
            )
        except Exception as exc:
            raise DataSourceError(
                f"Failed to inspect legacy QE recorder log for {task_id}/{loop_name}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        if not isinstance(payload, str) or not payload:
            raise DataSourceError(f"Legacy QE recorder log is empty for {task_id}/{loop_name}")
        payload_bytes = payload.encode("utf-8")
        if len(payload_bytes) != evidence.size_bytes or hashlib.sha256(payload_bytes).hexdigest() != evidence.sha256:
            raise DataSourceError(
                f"Legacy QE recorder log differs from its immutable receipt for {task_id}/{loop_name}"
            )

        terminal_identities: set[tuple[str, str]] = set()
        started_identities: set[tuple[str, str]] = set()
        for line_number, line in enumerate(payload.splitlines(), start=1):
            start_match = _RECORDER_START_LOG_RE.search(line)
            if start_match is not None:
                started_identities.add(
                    (start_match.group("experiment_id"), start_match.group("recorder_id"))
                )
            if not line.startswith("Latest recorder: "):
                continue
            try:
                terminal = ast.literal_eval(line.partition(":")[2].strip())
            except (SyntaxError, ValueError) as exc:
                raise DataSourceError(
                    f"Invalid terminal recorder evidence at run.log:{line_number}"
                ) from exc
            if not isinstance(terminal, Mapping):
                raise DataSourceError(
                    f"Terminal recorder evidence is not an object at run.log:{line_number}"
                )
            if terminal.get("class") != "Recorder" or terminal.get("status") != "FINISHED":
                raise DataSourceError(
                    f"Terminal recorder evidence is not finished at run.log:{line_number}"
                )
            terminal_identities.add(
                (
                    str(terminal.get("experiment_id") or "").strip(),
                    str(terminal.get("id") or "").strip(),
                )
            )

        expected_identity = (manifest.recorder_experiment_id, manifest.recorder_id)
        if terminal_identities != {expected_identity} or expected_identity not in started_identities:
            raise DataSourceError(
                f"Legacy QE recorder log does not corroborate the immutable identity for "
                f"{task_id}/{loop_name}: terminal={sorted(terminal_identities)}"
            )
        return manifest.artifact(artifact_name).workspace_path

    @classmethod
    async def _resolve_remote_artifact_manifest(
        cls,
        client: QEWorkspaceClient,
        *,
        task_id: str,
        loop_name: str,
        artifact_name: str,
        artifact_path: str,
    ) -> tuple[RemoteArtifactManifest, str]:
        """Load and validate the QE-authoritative manifest before downloading bytes."""
        attempts: dict[str, str] = {}
        manifest_paths = (
            f"{artifact_path}.manifest.json",
            "hmm_artifact_manifest.json",
            "qe_completion_payload.json",
        )
        for manifest_path in manifest_paths:
            try:
                payload: Any = await client.get_workspace_file(
                    task_id,
                    loop_name,
                    manifest_path,
                )
                if isinstance(payload, str):
                    payload = json.loads(payload)
                manifest = RemoteArtifactManifest.from_remote_payload(
                    payload,
                    artifact_name=artifact_name,
                )
                return manifest, manifest_path
            except Exception as exc:
                attempts[manifest_path] = f"{type(exc).__name__}: {exc}"
        legacy_manifest = find_legacy_qe_artifact_manifest(f"{task_id}/{loop_name}")
        if legacy_manifest is not None:
            try:
                catalog = await client.list_workspace_files(task_id, loop_name)
            except Exception as exc:
                raise DataSourceError(
                    f"Cannot prove remote manifests are absent for legacy artifact resolution: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            if catalog.get("catalog_completeness") != "complete":
                raise DataSourceError(
                    f"Legacy artifact manifest fallback requires a complete catalog for "
                    f"{task_id}/{loop_name}"
                )
            catalog_rows = catalog.get("files") or catalog.get("assets") or []
            catalog_paths = {
                str(
                    row.get("relative_path")
                    or row.get("path")
                    or row.get("filename")
                    or ""
                ).replace("\\", "/")
                for row in catalog_rows
                if isinstance(row, Mapping)
            }
            cataloged_remote_manifests = sorted(set(manifest_paths) & catalog_paths)
            if cataloged_remote_manifests:
                raise DataSourceError(
                    "A remote artifact manifest is cataloged but could not be validated; "
                    f"refusing legacy fallback for {task_id}/{loop_name}/{artifact_name}: "
                    f"{cataloged_remote_manifests}"
                )
            receipt = legacy_manifest.artifact(artifact_name)
            if receipt.workspace_path != artifact_path:
                raise DataSourceError(
                    f"Legacy QE artifact path differs from its immutable receipt for "
                    f"{task_id}/{loop_name}/{artifact_name}"
                )
            return (
                RemoteArtifactManifest.model_validate(
                    {
                        "artifact_name": receipt.artifact_name,
                        "schema_version": receipt.schema_version,
                        "sha256": receipt.sha256,
                        "size_bytes": receipt.size_bytes,
                        "row_count": receipt.row_count,
                        "quality_status": receipt.quality_status,
                    }
                ),
                "legacy_qe_artifact_manifests.py",
            )
        raise DataSourceError(
            f"Trusted remote artifact manifest unavailable for task={task_id}, "
            f"loop={loop_name}, artifact={artifact_name}: {attempts}"
        )

    def _normalize_prediction_data(self, pred_obj: Any) -> pd.DataFrame:
        """
        标准化预测数据为 DataFrame

        Args:
            pred_obj: pred.pkl 反序列化后的对象

        Returns:
            DataFrame with columns: trade_date, symbol, score, rank
        """
        if isinstance(pred_obj, pd.Series):
            frame = pred_obj.to_frame(name="score")
        elif isinstance(pred_obj, pd.DataFrame):
            frame = pred_obj.copy()
        elif isinstance(pred_obj, dict):
            # Dict[date, pd.Series] 或 Dict[date, Dict[symbol, score]]
            rows = []
            for trade_date, data in pred_obj.items():
                if isinstance(data, pd.Series):
                    for symbol, score in data.items():
                        rows.append(
                            {
                                "trade_date": trade_date,
                                "symbol": symbol,
                                "score": score,
                            }
                        )
                elif isinstance(data, dict):
                    for symbol, score in data.items():
                        rows.append(
                            {
                                "trade_date": trade_date,
                                "symbol": symbol,
                                "score": score,
                            }
                        )

            frame = pd.DataFrame(rows)
        else:
            raise DataSourceError(f"Unsupported pred.pkl format: {type(pred_obj)}. Expected Series, DataFrame or Dict.")

        score_col = _find_value_column(
            frame,
            preferred=("score", "prediction", "pred"),
            value_label="prediction score",
        )
        result = _normalize_indexed_frame(
            frame,
            value_column=score_col,
            output_value_column="score",
            source_label="pred.pkl",
        )
        result["score"] = pd.to_numeric(result["score"], errors="coerce")
        result = result.dropna(subset=["trade_date", "symbol", "score"])
        if result.empty:
            raise DataSourceError("pred.pkl contains no valid prediction rows")
        _raise_on_duplicate_keys(
            result,
            keys=("trade_date", "symbol"),
            source_label="pred.pkl",
        )
        result = result.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
        result["rank"] = result.groupby("trade_date")["score"].rank(method="first", ascending=False).astype(int)
        return result

    async def _normalize_label_data(self, label_obj: Any) -> pd.DataFrame:
        """
        标准化标签数据为 DataFrame

        Args:
            label_obj: label.pkl 反序列化后的对象

        Returns:
            DataFrame with columns: trade_date, symbol, horizon_days, future_return, label_date
        """
        if isinstance(label_obj, pd.Series):
            frame = label_obj.to_frame(name="future_return")
        elif isinstance(label_obj, pd.DataFrame):
            frame = label_obj.copy()
        elif isinstance(label_obj, dict):
            # Dict[date, pd.Series]
            rows = []
            for trade_date, data in label_obj.items():
                if isinstance(data, pd.Series):
                    for symbol, future_return in data.items():
                        rows.append(
                            {
                                "trade_date": trade_date,
                                "symbol": symbol,
                                "horizon_days": self.label_horizon_days,
                                "future_return": future_return,
                            }
                        )
            frame = pd.DataFrame(rows)
        else:
            raise DataSourceError(
                f"Unsupported label.pkl format: {type(label_obj)}. Expected Series, DataFrame or Dict."
            )

        label_col = _find_value_column(
            frame,
            preferred=("future_return", "label", "label0", "return", "ret"),
            value_label="forward return",
        )
        result = _normalize_indexed_frame(
            frame,
            value_column=label_col,
            output_value_column="future_return",
            source_label="label.pkl",
        )
        result["future_return"] = pd.to_numeric(
            result["future_return"],
            errors="coerce",
        )
        if "horizon_days" in frame.columns and not isinstance(frame.index, pd.MultiIndex):
            result["horizon_days"] = pd.to_numeric(
                frame.loc[result.index, "horizon_days"],
                errors="coerce",
            )
        if "horizon_days" not in result.columns:
            result["horizon_days"] = self.label_horizon_days
        result["horizon_days"] = pd.to_numeric(
            result["horizon_days"],
            errors="coerce",
        )
        result = result.dropna(subset=["trade_date", "symbol", "future_return", "horizon_days"])
        if result.empty:
            raise DataSourceError("label.pkl contains no valid label rows")
        result["horizon_days"] = result["horizon_days"].astype(int)
        _raise_on_duplicate_keys(
            result,
            keys=("trade_date", "symbol", "horizon_days"),
            source_label="label.pkl",
        )
        result = result.sort_values(["trade_date", "symbol", "horizon_days"]).reset_index(drop=True)
        result["label_date"] = await self._calculate_label_dates(
            result["trade_date"],
            result["horizon_days"],
        )
        return result

    async def _calculate_label_dates(
        self,
        trade_dates: pd.Series,
        horizon_days: pd.Series,
    ) -> pd.Series:
        """
        计算 label_date（使用真实交易日历）

        Args:
            trade_dates: 交易日期序列
            horizon_days: horizon 天数序列

        Returns:
            label_date 序列
        """
        # 获取唯一的 (trade_date, horizon) 组合
        unique_pairs = pd.DataFrame(
            {
                "trade_date": trade_dates,
                "horizon_days": horizon_days,
            }
        ).drop_duplicates()

        # 查询交易日历
        label_date_map = {}
        for _, row in unique_pairs.iterrows():
            trade_date = row["trade_date"]
            horizon = row["horizon_days"]
            label_date = await self._get_nth_trading_day(trade_date, horizon)
            label_date_map[(trade_date, horizon)] = label_date

        # 映射回原 Series
        result = pd.Series(
            [label_date_map[(td, h)] for td, h in zip(trade_dates, horizon_days)], index=trade_dates.index
        )

        return result

    async def _get_nth_trading_day(self, start_date: date, n_days: int) -> date:
        """
        获取 start_date 后的第 N 个交易日

        Args:
            start_date: 起始日期
            n_days: 需要前进的交易日数

        Returns:
            第 N 个交易日的日期

        Raises:
            DataSourceError: 查询失败或数据不足
        """
        try:
            return await asyncio.to_thread(
                self.repository.get_nth_trading_day,
                start_date,
                n_days,
            )
        except DataSourceError:
            raise
        except Exception as e:
            raise DataSourceError(f"Failed to query trading calendar: {e}") from e


def _find_value_column(
    frame: pd.DataFrame,
    *,
    preferred: tuple[str, ...],
    value_label: str,
) -> Any:
    lowered = {str(column).lower(): column for column in frame.columns}
    for name in preferred:
        if name.lower() in lowered:
            return lowered[name.lower()]
    numeric = [column for column in frame.columns if pd.api.types.is_numeric_dtype(frame[column])]
    if len(numeric) == 1:
        return numeric[0]
    if len(frame.columns) == 1:
        return frame.columns[0]
    raise DataSourceError(f"artifact DataFrame has no unambiguous {value_label} column: {frame.columns.tolist()}")


def _normalize_indexed_frame(
    frame: pd.DataFrame,
    *,
    value_column: Any,
    output_value_column: str,
    source_label: str,
) -> pd.DataFrame:
    if isinstance(frame.index, pd.MultiIndex):
        index_names = [str(name or "").lower() for name in frame.index.names]
        date_level = _find_index_level(
            index_names,
            ("datetime", "date", "trade_date", "time"),
        )
        instrument_level = _find_index_level(
            index_names,
            ("instrument", "symbol", "ts_code", "code"),
        )
        if date_level is None or instrument_level is None:
            if frame.index.nlevels < 2:
                raise DataSourceError(f"{source_label} MultiIndex lacks date/instrument levels")
            date_level = 0 if date_level is None else date_level
            instrument_level = 1 if instrument_level is None else instrument_level
        result = pd.DataFrame(
            {
                "trade_date": frame.index.get_level_values(date_level),
                "symbol": frame.index.get_level_values(instrument_level),
                output_value_column: frame[value_column].to_numpy(),
            }
        )
    else:
        date_column = _find_named_column(
            frame,
            ("trade_date", "datetime", "date", "time"),
        )
        symbol_column = _find_named_column(
            frame,
            ("symbol", "instrument", "ts_code", "code"),
        )
        if date_column is None or symbol_column is None:
            raise DataSourceError(f"{source_label} DataFrame lacks date/symbol columns: {frame.columns.tolist()}")
        result = frame[[date_column, symbol_column, value_column]].rename(
            columns={
                date_column: "trade_date",
                symbol_column: "symbol",
                value_column: output_value_column,
            }
        )

    result = result.copy()
    result["trade_date"] = pd.to_datetime(
        result["trade_date"],
        errors="coerce",
    ).dt.date
    result = result.dropna(subset=["trade_date", "symbol"])
    result["symbol"] = result["symbol"].astype(str)
    return result


def _find_named_column(
    frame: pd.DataFrame,
    names: tuple[str, ...],
) -> Any | None:
    lowered = {str(column).lower(): column for column in frame.columns}
    for name in names:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


def _find_index_level(
    index_names: list[str],
    tokens: tuple[str, ...],
) -> int | None:
    for index, name in enumerate(index_names):
        if any(token in name for token in tokens):
            return index
    return None


def _raise_on_duplicate_keys(
    frame: pd.DataFrame,
    *,
    keys: tuple[str, ...],
    source_label: str,
) -> None:
    duplicate_mask = frame.duplicated(subset=list(keys), keep=False)
    if not duplicate_mask.any():
        return
    samples = frame.loc[duplicate_mask, list(keys)].head(5).to_dict(orient="records")
    raise DataSourceError(f"{source_label} contains duplicate identity keys {keys}: samples={samples}")
