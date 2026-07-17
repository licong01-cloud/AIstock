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

import asyncio
import json
from datetime import date
from typing import Any, Optional, Tuple

import pandas as pd

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient

from .base import HMMDataSourceInterface
from .cache_manager import ArtifactCacheManager
from .db_repository import HMMDataRepository
from .exceptions import DataSourceError, DateRangeError, HorizonError, DataNotFoundError


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
    ):
        """
        Args:
            base_loop_ref: QE loop 引用（如 "qe_20260502_131502_9b54/Loop1"）
            cache_dir: 缓存目录
            qe_client: QE workspace 客户端（用于测试注入）
            repository: 只读 canonical market repository（用于测试注入）
        """
        self.base_loop_ref = base_loop_ref
        self.cache_manager = ArtifactCacheManager(cache_dir)
        # Injected clients are owned by the caller.  The default client is
        # resolved lazily from the QE task's authoritative compute node and is
        # closed by this data source.
        self.qe_client = qe_client
        self._owns_qe_client = False
        self.repository = repository or HMMDataRepository()

        # 内存缓存（避免重复加载 pickle）
        self._pred_cache: Optional[pd.DataFrame] = None
        self._label_cache: Optional[pd.DataFrame] = None

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
        mask = (pred_df['trade_date'] >= start_date) & (pred_df['trade_date'] <= end_date)
        result_df = pred_df[mask].copy()

        if result_df.empty:
            raise DataNotFoundError(
                f"No predictions found for date range [{start_date}, {end_date}]"
            )

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
            raise HorizonError(
                f"horizon_days must be between 1 and 30, got {horizon_days}"
            )

        # 验证日期范围
        is_valid, error_msg = await self.validate_date_range(start_date, end_date)
        if not is_valid:
            raise DateRangeError(error_msg)

        # 加载完整标签数据
        label_df = await self._load_labels_from_cache()

        # 过滤日期范围和 horizon
        mask = (
            (label_df['trade_date'] >= start_date)
            & (label_df['trade_date'] <= end_date)
            & (label_df['horizon_days'] == horizon_days)
        )
        result_df = label_df[mask].copy()

        if result_df.empty:
            raise DataNotFoundError(
                f"No labels found for date range [{start_date}, {end_date}] "
                f"with horizon_days={horizon_days}"
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

        min_date = pred_df['trade_date'].min()
        max_date = pred_df['trade_date'].max()

        return min_date, max_date

    async def _load_predictions_from_cache(self) -> pd.DataFrame:
        """
        从缓存加载预测数据

        Returns:
            DataFrame with columns: trade_date, symbol, score, rank
        """
        # 检查内存缓存
        if self._pred_cache is not None:
            return self._pred_cache

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
            return self._label_cache

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

            return df

        except Exception as e:
            raise DataSourceError(f"Failed to load labels from cache: {e}")

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
                f"Invalid base_loop_ref format: {self.base_loop_ref}. "
                f"Expected format: 'task_id/loop_name'"
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
                    "task_id": task_id,
                    "loop_name": loop_name,
                    "workspace_path": artifact_path,
                },
            )

        except Exception as e:
            raise DataSourceError(
                f"Failed to download {artifact_name} from QE workspace: {e}"
            )

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
                    wait_time = 2 ** attempt
                    await asyncio.sleep(wait_time)

        raise DataSourceError(
            f"Failed to download {artifact_path} after {max_retries} retries: {last_error}"
        )

    async def _get_qe_client(self, task_id: str) -> QEWorkspaceClient:
        """Return the injected client or lazily resolve the task's node client."""
        if self.qe_client is not None:
            return self.qe_client

        node_id = await asyncio.to_thread(self._resolve_task_node_id, task_id)
        try:
            client = await asyncio.to_thread(QEWorkspaceClient.for_node, node_id)
        except Exception as exc:
            raise DataSourceError(
                f"Failed to create QE workspace client for task={task_id}, "
                f"node={node_id}: {exc}"
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
            raise DataSourceError(
                f"Failed to resolve compute node for QE task {task_id}: {exc}"
            ) from exc

        node_id = str(row[0]).strip() if row and row[0] else ""
        if not node_id:
            raise DataSourceError(
                f"QE task {task_id} has no authoritative compute node"
            )
        return node_id

    @staticmethod
    async def _resolve_workspace_artifact_path(
        client: QEWorkspaceClient,
        *,
        task_id: str,
        loop_name: str,
        artifact_name: str,
    ) -> str:
        """Resolve an allowlisted MLflow artifact from recorder metadata."""
        attempts: dict[str, str] = {}
        for ref_name in ("qe_current_recorder.json", "qe_extracted_recorder.json"):
            try:
                payload: Any = await client.get_workspace_file(
                    task_id,
                    loop_name,
                    ref_name,
                )
                if isinstance(payload, str):
                    payload = json.loads(payload)
                if not isinstance(payload, dict):
                    raise ValueError(
                        f"expected JSON object, got {type(payload).__name__}"
                    )

                recorder_id = str(
                    payload.get("recorder_id")
                    or payload.get("selected_recorder_id")
                    or ""
                ).strip()
                experiment_id = str(
                    payload.get("experiment_id")
                    or payload.get("selected_experiment_id")
                    or ""
                ).strip()
                for field_name, value in (
                    ("recorder_id", recorder_id),
                    ("experiment_id", experiment_id),
                ):
                    if not value or "/" in value or "\\" in value or value in {".", ".."}:
                        raise ValueError(f"invalid {field_name}: {value!r}")

                return (
                    f"mlruns/{experiment_id}/{recorder_id}/artifacts/"
                    f"{artifact_name}"
                )
            except Exception as exc:
                attempts[ref_name] = f"{type(exc).__name__}: {exc}"

        raise DataSourceError(
            f"QE recorder metadata unavailable for task={task_id}, "
            f"loop={loop_name}: {attempts}"
        )

    def _normalize_prediction_data(self, pred_obj: any) -> pd.DataFrame:
        """
        标准化预测数据为 DataFrame

        Args:
            pred_obj: pred.pkl 反序列化后的对象

        Returns:
            DataFrame with columns: trade_date, symbol, score, rank
        """
        # pred.pkl 的格式可能是:
        # 1. DataFrame
        # 2. Dict[date, pd.Series]
        # 3. Dict[date, Dict[symbol, score]]

        if isinstance(pred_obj, pd.DataFrame):
            # 已经是 DataFrame，检查列名
            required_cols = ['trade_date', 'symbol', 'score']
            if all(col in pred_obj.columns for col in required_cols):
                return pred_obj
            else:
                raise DataSourceError(
                    f"pred.pkl DataFrame missing required columns. "
                    f"Expected: {required_cols}, got: {pred_obj.columns.tolist()}"
                )

        elif isinstance(pred_obj, dict):
            # Dict[date, pd.Series] 或 Dict[date, Dict[symbol, score]]
            rows = []
            for trade_date, data in pred_obj.items():
                if isinstance(data, pd.Series):
                    for symbol, score in data.items():
                        rows.append({
                            'trade_date': trade_date,
                            'symbol': symbol,
                            'score': score,
                        })
                elif isinstance(data, dict):
                    for symbol, score in data.items():
                        rows.append({
                            'trade_date': trade_date,
                            'symbol': symbol,
                            'score': score,
                        })

            df = pd.DataFrame(rows)
            return df

        else:
            raise DataSourceError(
                f"Unsupported pred.pkl format: {type(pred_obj)}. "
                f"Expected DataFrame or Dict."
            )

    async def _normalize_label_data(self, label_obj: any) -> pd.DataFrame:
        """
        标准化标签数据为 DataFrame

        Args:
            label_obj: label.pkl 反序列化后的对象

        Returns:
            DataFrame with columns: trade_date, symbol, horizon_days, future_return, label_date
        """
        # label.pkl 的格式可能是:
        # 1. DataFrame
        # 2. Dict[date, pd.Series]  (假设 horizon 固定为 10)

        if isinstance(label_obj, pd.DataFrame):
            # 已经是 DataFrame
            required_cols = ['trade_date', 'symbol', 'future_return']
            if all(col in label_obj.columns for col in required_cols):
                # 如果没有 horizon_days，默认设为 10
                if 'horizon_days' not in label_obj.columns:
                    label_obj['horizon_days'] = 10

                # 计算 label_date（使用真实交易日历）
                if 'label_date' not in label_obj.columns:
                    label_obj['label_date'] = await self._calculate_label_dates(
                        label_obj['trade_date'],
                        label_obj['horizon_days']
                    )

                return label_obj
            else:
                raise DataSourceError(
                    f"label.pkl DataFrame missing required columns. "
                    f"Expected: {required_cols}, got: {label_obj.columns.tolist()}"
                )

        elif isinstance(label_obj, dict):
            # Dict[date, pd.Series]
            rows = []
            for trade_date, data in label_obj.items():
                if isinstance(data, pd.Series):
                    for symbol, future_return in data.items():
                        rows.append({
                            'trade_date': trade_date,
                            'symbol': symbol,
                            'horizon_days': 10,  # 默认
                            'future_return': future_return,
                        })

            df = pd.DataFrame(rows)

            # 计算 label_date
            df['label_date'] = await self._calculate_label_dates(
                df['trade_date'],
                df['horizon_days']
            )

            return df

        else:
            raise DataSourceError(
                f"Unsupported label.pkl format: {type(label_obj)}. "
                f"Expected DataFrame or Dict."
            )

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
        unique_pairs = pd.DataFrame({
            'trade_date': trade_dates,
            'horizon_days': horizon_days,
        }).drop_duplicates()

        # 查询交易日历
        label_date_map = {}
        for _, row in unique_pairs.iterrows():
            trade_date = row['trade_date']
            horizon = row['horizon_days']
            label_date = await self._get_nth_trading_day(trade_date, horizon)
            label_date_map[(trade_date, horizon)] = label_date

        # 映射回原 Series
        result = pd.Series(
            [label_date_map[(td, h)] for td, h in zip(trade_dates, horizon_days)],
            index=trade_dates.index
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
