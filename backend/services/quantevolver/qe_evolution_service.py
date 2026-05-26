import json
import logging
import os
import asyncio
import uuid
import threading
import base64
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import aiofiles
from psycopg2.extras import RealDictCursor
from ...db.pg_pool import get_conn
from ..qe_archive.models import sha256_json

from .factor_official_evaluation_service import CALC_ENGINE
from .qe_workspace_client import QEWorkspaceClient, QELoopWorkspaceCleanupUnavailable
from .qe_evolution_agents import EvolutionAgents, EvolutionFactorAgent, EvolutionModelAgent, AnalystResult
from .callback_urls import build_aistock_callback_url
from .experiment_config import DEFAULT_LABEL_HORIZON, normalize_label_horizon, normalize_qe_random_seed
from .runtime_contract import build_qe_minute_runtime_contract, merge_qe_minute_runtime_contract
from .seed_contract import ensure_loop_fixed_seed
from .payload_summary import compact_loop_row, compact_task_row
from .node_execution import (
    normalize_node_parallelism,
    preflight_qe_node,
    resolve_custom_loop_nodes,
    resolve_default_qe_node_id,
)
from ..strategy_package.workspace_policy import (
    remove_aistock_artifact_tree,
    unlink_aistock_artifact_files,
)

logger = logging.getLogger(__name__)

_ACTIVE_RATING_JOIN_SQL = """
JOIN aistock_factor_catalog cat
  ON cat.factor_name = c.factor_name AND cat.source = c.factor_source
LEFT JOIN LATERAL (
    SELECT official_grade, official_score, rule_version
    FROM qe_factor_official_ratings r
    WHERE r.factor_catalog_id = cat.id
      AND r.rule_version = (
          SELECT rule_version FROM qe_rating_rule_versions
          WHERE status = 'active'
          ORDER BY activated_at DESC NULLS LAST, created_at DESC
          LIMIT 1
      )
    ORDER BY r.graded_at DESC
    LIMIT 1
) fr ON TRUE
"""

SOTA_ASSETS_DIR = os.environ.get(
    "QE_SOTA_ASSETS_DIR",
    str(Path(__file__).resolve().parents[3] / "rdagent_assets" / "qe_sota_assets"),
)
QE_EVOLUTION_LOG_TERMINAL_STATUSES = {
    "completed",
    "failed",
    "cancelled",
    "canceled",
    "interrupted",
    "timeout",
    "paused",
    "stopped",
}
QE_LOG_TAIL_DEFAULT_LINES = 500

QE_LOOP_RETRY_MODE_AUTO = "auto"
QE_LOOP_RETRY_MODE_BACKTEST_ONLY = "backtest_only"
QE_LOOP_RETRY_MODE_FULL_TRAIN = "full_train"
QE_LOOP_RETRY_MODES = {
    QE_LOOP_RETRY_MODE_AUTO,
    QE_LOOP_RETRY_MODE_BACKTEST_ONLY,
    QE_LOOP_RETRY_MODE_FULL_TRAIN,
}
_QE_LOOP_RETRY_MODE_ALIASES = {
    "backtest": QE_LOOP_RETRY_MODE_BACKTEST_ONLY,
    "only_backtest": QE_LOOP_RETRY_MODE_BACKTEST_ONLY,
    "train": QE_LOOP_RETRY_MODE_FULL_TRAIN,
    "full": QE_LOOP_RETRY_MODE_FULL_TRAIN,
}


def normalize_qe_loop_retry_mode(mode: Optional[str]) -> str:
    """Normalize explicit loop retry mode from API/UI callers."""
    normalized = (mode or QE_LOOP_RETRY_MODE_AUTO).strip().lower().replace("-", "_")
    normalized = _QE_LOOP_RETRY_MODE_ALIASES.get(normalized, normalized)
    if normalized not in QE_LOOP_RETRY_MODES:
        raise ValueError(
            "Invalid retry mode: "
            f"{mode!r}. Expected one of: {sorted(QE_LOOP_RETRY_MODES)}"
        )
    return normalized


def _read_text_tail_lines(path: Path, max_lines: int = QE_LOG_TAIL_DEFAULT_LINES) -> List[str]:
    """Read a bounded tail from a potentially large UTF-8-ish log file."""
    if max_lines <= 0 or not path.exists() or not path.is_file():
        return []
    chunk_size = 8192
    data = b""
    with path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell()
        while pos > 0 and data.count(b"\n") <= max_lines:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            data = f.read(read_size) + data
    return data.decode("utf-8", errors="replace").splitlines()[-max_lines:]


def derive_custom_evo_final_status(expected_count: int, status_counts: Dict[str, int]) -> str:
    """Return completed only when every configured custom loop completed."""
    active_status_counts = {status: int(count) for status, count in status_counts.items() if int(count) > 0}
    completed_count = int(status_counts.get("completed", 0))
    failed_count = int(status_counts.get("failed", 0))
    cancelled_count = int(status_counts.get("cancelled", 0)) + int(status_counts.get("canceled", 0))
    if (
        expected_count > 0
        and completed_count == expected_count
        and failed_count == 0
        and cancelled_count == 0
        and set(active_status_counts) == {"completed"}
    ):
        return "completed"
    return "failed"


class AutoEvolutionScheduler:
    """
    自动演进任务的核心调度器与状态机
    负责在 AIstock 侧控制演进流程的流转 (LOOP)，并调用 Agent 和 RDAgent API。
    """
    def __init__(self):
        self.workspace_client = QEWorkspaceClient()
        self.agents = EvolutionAgents()
        self.factor_agent = EvolutionFactorAgent(agents=self.agents)
        self.model_agent = EvolutionModelAgent(agents=self.agents)
        self._node_clients: Dict[str, QEWorkspaceClient] = {}
        self._log_stream_lock = threading.RLock()
        self._active_log_stream_counts: Dict[str, int] = {}
        self._log_stream_stop_requested: set[str] = set()

    def _archive_completed_loop_best_effort(self, task_id: str, loop_id: str, loop_index: int | None = None) -> None:
        """Best-effort archive hook; archive failures must not affect QE status."""

        try:
            from backend.services.qe_archive.realtime_ingestion import safe_archive_loop_completed

            safe_archive_loop_completed(task_id=task_id, loop_id=loop_id, loop_index=loop_index)
        except Exception as exc:  # pragma: no cover - defensive isolation.
            logger.warning(
                "QE archive realtime hook failed without changing QE status: task=%s loop=%s error=%s",
                task_id,
                loop_id,
                exc,
                exc_info=True,
            )

    def _record_research_backtest_best_effort(
        self,
        task_id: str,
        loop_id: str,
        loop_index: int | None = None,
        experiment_id: str | None = None,
    ) -> None:
        """Best-effort Research Pipeline hook; failures must not affect QE status."""

        try:
            from backend.services.research_pipeline.realtime_ingestion import safe_record_hmm_backtest_completed

            safe_record_hmm_backtest_completed(
                task_id=task_id,
                loop_id=loop_id,
                loop_index=loop_index,
                experiment_id=experiment_id,
            )
        except Exception as exc:  # pragma: no cover - defensive isolation.
            logger.warning(
                "Research Pipeline realtime hook failed without changing QE status: task=%s loop=%s error=%s",
                task_id,
                loop_id,
                exc,
                exc_info=True,
            )

    def _ensure_log_stream_state(self) -> None:
        """Initialize log-stream bookkeeping for tests that construct via __new__."""
        if not hasattr(self, "_log_stream_lock"):
            self._log_stream_lock = threading.RLock()
        if not hasattr(self, "_active_log_stream_counts"):
            self._active_log_stream_counts = {}
        if not hasattr(self, "_log_stream_stop_requested"):
            self._log_stream_stop_requested = set()

    def _get_workspace_client_for_node_id(self, node_id: Optional[str]) -> QEWorkspaceClient:
        if node_id:
            if node_id not in self._node_clients:
                self._node_clients[node_id] = QEWorkspaceClient.for_node(node_id)
            return self._node_clients[node_id]
        return self.workspace_client

    def _get_workspace_client_for_task(self, task_id: str) -> QEWorkspaceClient:
        """根据 task 的 node_id 返回对应节点的 workspace 客户端。无 node_id 时返回默认客户端。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT node_id FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                row = cur.fetchone()
        return self._get_workspace_client_for_node_id(row.get("node_id") if row else None)

    @staticmethod
    def _log_stream_node_label(node_id: Optional[str]) -> str:
        return (str(node_id).strip() if node_id else "") or "local"

    @staticmethod
    def _log_stream_node_key(node_id: Optional[str]) -> str:
        return (str(node_id).strip() if node_id else "") or "__local__"

    def _get_log_stream_node_plan_for_task(self, task_id: str) -> Dict[str, Any]:
        """
        Return every execution node that can own logs for a task.

        Custom evolution can submit different loops to different nodes; the
        task-level node only represents Loop1/default submission and is not
        sufficient for distributed realtime logs.
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT task_id, task_type, node_id, strategy_evo_config
                    FROM qe_evolution_tasks
                    WHERE task_id = %s
                    """,
                    (task_id,),
                )
                task = cur.fetchone()
                cur.execute(
                    """
                    SELECT loop_index, node_id
                    FROM qe_evolution_loops
                    WHERE task_id = %s
                      AND node_id IS NOT NULL
                      AND BTRIM(node_id) <> ''
                    ORDER BY loop_index ASC
                    """,
                    (task_id,),
                )
                loop_rows = [dict(row) for row in cur.fetchall()]

        if not task:
            return {
                "task_id": task_id,
                "node_ids": [None],
                "warnings": [f"Task {task_id} not found while resolving log nodes."],
            }

        node_ids: List[Optional[str]] = []
        seen: set[str] = set()
        warnings: List[str] = []
        default_node_id = resolve_default_qe_node_id()

        def add_node(raw_node_id: Optional[str]) -> None:
            normalized = (str(raw_node_id).strip() if raw_node_id else "") or None
            key = self._log_stream_node_key(normalized)
            if normalized == default_node_id and "__local__" in seen:
                return
            if normalized is None and default_node_id in seen:
                return
            if key not in seen:
                seen.add(key)
                node_ids.append(normalized)

        add_node(task.get("node_id"))

        if task.get("task_type") == "custom_evo":
            try:
                strategy_config = self._parse_custom_evo_strategy_config(
                    task.get("strategy_evo_config"),
                    task_id=task_id,
                )
                resolved_loops, _loop1_node_id, _selected = resolve_custom_loop_nodes(
                    [dict(loop_cfg) for loop_cfg in strategy_config["loops"]],
                    task.get("node_id"),
                )
                for loop_cfg in resolved_loops:
                    add_node(loop_cfg.get("node_id"))
            except Exception as exc:
                warning = (
                    f"Failed to resolve custom_evo log nodes from strategy_evo_config "
                    f"for task {task_id}: {exc}"
                )
                warnings.append(warning)
                logger.warning(warning)

        for row in loop_rows:
            add_node(row.get("node_id"))

        if not node_ids:
            add_node(None)

        return {
            "task_id": task_id,
            "task_type": task.get("task_type"),
            "node_ids": node_ids,
            "warnings": warnings,
        }

    @staticmethod
    def _normalize_loop_db_id(task_id: str, loop_id_or_index: str | int) -> str:
        if isinstance(loop_id_or_index, int):
            return f"{task_id}_Loop{loop_id_or_index}"
        loop_value = str(loop_id_or_index)
        if loop_value.startswith(task_id + "_"):
            return loop_value
        if loop_value.startswith("Loop"):
            return f"{task_id}_{loop_value}"
        return loop_value

    def _get_loop_node_id(self, task_id: str, loop_id_or_index: str | int) -> str:
        loop_db_id = self._normalize_loop_db_id(task_id, loop_id_or_index)
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT l.node_id AS loop_node_id, t.node_id AS task_node_id
                    FROM qe_evolution_tasks t
                    LEFT JOIN qe_evolution_loops l ON l.task_id = t.task_id AND l.loop_id = %s
                    WHERE t.task_id = %s
                    """,
                    (loop_db_id, task_id),
                )
                row = cur.fetchone()
        if row:
            return row.get("loop_node_id") or row.get("task_node_id") or resolve_default_qe_node_id()
        return resolve_default_qe_node_id()

    def _get_workspace_client_for_loop(self, task_id: str, loop_id_or_index: str | int) -> QEWorkspaceClient:
        return self._get_workspace_client_for_node_id(self._get_loop_node_id(task_id, loop_id_or_index))

    async def _build_backtest_only_model_payload(
        self,
        source_client: QEWorkspaceClient,
        source_task_id: str,
        source_loop_index: int,
        *,
        reason: str,
    ) -> tuple[Dict[str, Any], Dict[str, str]]:
        """Package reusable model params through the QE node API for backtest-only runs."""
        source_loop = f"Loop{source_loop_index}"
        mlruns_tar = await source_client.download_mlruns_params(source_task_id, source_loop)
        if not mlruns_tar:
            raise RuntimeError(
                f"{reason} missing source model params: {source_task_id}/{source_loop}"
            )
        model_source = {
            "source_task_id": source_task_id,
            "source_loop": source_loop,
            # Reuse the existing node contract: when true, the API extracts the
            # packaged mlruns tar from experiment_files instead of probing links.
            "cross_node": True,
            "source_transport": "mlruns_params_tar",
        }
        source_ref = {
            "schema_version": "qe_backtest_source_ref_v1",
            "source_task_id": source_task_id,
            "source_loop": source_loop,
            "source_transport": "mlruns_params_tar",
            "reason": reason,
        }
        extra_experiment_files = {
            "mlruns_params.tar.gz.b64": base64.b64encode(mlruns_tar).decode("ascii"),
            "qe_backtest_source_ref.json": json.dumps(source_ref, ensure_ascii=False, indent=2),
        }
        logger.info(
            "%s packaged source model params via node API: source=%s/%s bytes=%s",
            reason,
            source_task_id,
            source_loop,
            len(mlruns_tar),
        )
        return model_source, extra_experiment_files

    async def _require_backtest_retry_isolation_passed(
        self,
        client: QEWorkspaceClient,
        task_id: str,
        loop_id: str,
        node_id: str,
    ) -> Dict[str, Any]:
        """Ensure retry does not mask a failed backtest-only recorder isolation gate."""
        try:
            isolation_payload = await client.get_workspace_file(
                task_id,
                loop_id,
                "qe_recorder_isolation.json",
            )
        except Exception as exc:
            raise ValueError(
                "QE_BACKTEST_RETRY_REQUIRES_ISOLATION_PASSED: "
                f"{task_id}/{loop_id} has no readable qe_recorder_isolation.json on node {node_id}"
            ) from exc

        if isinstance(isolation_payload, str):
            try:
                isolation_payload = json.loads(isolation_payload)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    "QE_BACKTEST_RETRY_REQUIRES_ISOLATION_PASSED: "
                    f"{task_id}/{loop_id} isolation manifest is not valid JSON"
                ) from exc

        if not isinstance(isolation_payload, dict):
            raise ValueError(
                "QE_BACKTEST_RETRY_REQUIRES_ISOLATION_PASSED: "
                f"{task_id}/{loop_id} isolation manifest has invalid type"
            )
        if isolation_payload.get("recorder_isolation_status") != "passed":
            raise ValueError(
                "QE_BACKTEST_RETRY_REQUIRES_ISOLATION_PASSED: "
                f"{task_id}/{loop_id} recorder_isolation_status="
                f"{isolation_payload.get('recorder_isolation_status')!r}"
            )
        return isolation_payload

    def _get_callback_url_for_node(self, node_id: Optional[str]) -> Optional[str]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT callback_url FROM infra.compute_nodes WHERE node_id = %s", (node_id,))
                row = cur.fetchone()
        return build_aistock_callback_url(
            endpoint_path="/api/v1/quantevolver/evolution/webhook/loop-completed",
            full_url_env="AISTOCK_QE_EVOLUTION_LOOP_CALLBACK_URL",
            node_id=node_id,
            node_callback_url=row.get("callback_url") if row else None,
            require_env_base=True,
        )

    def _get_task_status(self, task_id: str) -> Optional[str]:
        """Read latest task status so background submit loops can stop cleanly."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                row = cur.fetchone()
        return row[0] if row else None

    def task_exists(self, task_id: str) -> bool:
        return self._get_task_status(task_id) is not None

    def is_terminal_log_status(self, status: Optional[str]) -> bool:
        return str(status or "").lower() in QE_EVOLUTION_LOG_TERMINAL_STATUSES

    def get_task_log_tail(self, task_id: str, tail_lines: int = QE_LOG_TAIL_DEFAULT_LINES) -> Dict[str, Any]:
        status = self._get_task_status(task_id)
        log_path = Path(SOTA_ASSETS_DIR) / task_id / "logs" / "evolution.log"
        lines = _read_text_tail_lines(log_path, max(1, min(int(tail_lines or QE_LOG_TAIL_DEFAULT_LINES), 5000)))
        return {
            "task_id": task_id,
            "task_status": status,
            "terminal": self.is_terminal_log_status(status),
            "log_path": str(log_path) if log_path.exists() else None,
            "logs": lines,
        }

    def _request_stop_log_streams(self, task_id: str) -> None:
        self._ensure_log_stream_state()
        with self._log_stream_lock:
            self._log_stream_stop_requested.add(task_id)

    def _is_log_stream_stop_requested(self, task_id: str) -> bool:
        self._ensure_log_stream_state()
        with self._log_stream_lock:
            return task_id in self._log_stream_stop_requested

    def _register_log_stream(self, task_id: str) -> None:
        self._ensure_log_stream_state()
        with self._log_stream_lock:
            self._active_log_stream_counts[task_id] = self._active_log_stream_counts.get(task_id, 0) + 1

    def _unregister_log_stream(self, task_id: str) -> None:
        self._ensure_log_stream_state()
        with self._log_stream_lock:
            count = self._active_log_stream_counts.get(task_id, 0) - 1
            if count > 0:
                self._active_log_stream_counts[task_id] = count
            else:
                self._active_log_stream_counts.pop(task_id, None)

    def _active_log_stream_count(self, task_id: str) -> int:
        self._ensure_log_stream_state()
        with self._log_stream_lock:
            return self._active_log_stream_counts.get(task_id, 0)

    async def _wait_for_log_streams_closed(self, task_id: str, timeout_seconds: float = 10.0) -> int:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while self._active_log_stream_count(task_id) > 0 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        return self._active_log_stream_count(task_id)

    def _get_callback_url_for_task(self, task_id: str) -> Optional[str]:
        """查询任务关联节点的 callback_url，用于 Loop 完成后主动回调。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT t.node_id, cn.callback_url
                    FROM qe_evolution_tasks t
                    LEFT JOIN infra.compute_nodes cn ON t.node_id = cn.node_id
                    WHERE t.task_id = %s
                """, (task_id,))
                row = cur.fetchone()
        return build_aistock_callback_url(
            endpoint_path="/api/v1/quantevolver/evolution/webhook/loop-completed",
            full_url_env="AISTOCK_QE_EVOLUTION_LOOP_CALLBACK_URL",
            node_id=row.get("node_id") if row else None,
            node_callback_url=row.get("callback_url") if row else None,
            require_env_base=True,
        )
        
    async def create_task(
        self,
        task_name: str,
        target_desc: str,
        max_loops: int,
        base_experiment_id: str,
        allow_created: bool = False,
        start_from_loop_zero: bool = False,
        node_id: Optional[str] = None,
        stock_pool: Optional[str] = None,
        label_horizon: Optional[int] = None,
        random_seed: Optional[int] = None,
    ) -> str:
        """
        创建演进任务并写入数据库。
        支持三种场景：
        1. 从单次实验开始演进（base_experiment_id 是主实验）
        2. 从之前演进的某个子 Loop 继续演进（base_experiment_id 是子实验如 qe_xxx_L3）
        3. 恢复已有演进任务（使用 resume_task 方法）

        task_id 复用根实验ID，current_loop 从已有最大 loop_index 开始。
        allow_created: 为 True 时允许 status='created' 的实验（rdagent_task_sota 场景，Loop 1 即为初始回测）。
        start_from_loop_zero: 为 True 时从 Loop0 开始（rdagent_task_sota 场景，Loop1 为初始回测）。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT experiment_id, status, parent_experiment_id, is_evolution_loop, loop_index FROM qe_experiments WHERE experiment_id = %s", (base_experiment_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"base_experiment_id not found: {base_experiment_id}")
                allowed_statuses = {'completed', 'created'} if allow_created else {'completed'}
                if row['status'] not in allowed_statuses:
                    raise ValueError(f"基础实验尚未完成（当前状态: {row['status']}），无法开始演进")

        # 确定根实验ID（如果 base 是子 Loop，追溯到根）
        effective_label_horizon = self._resolve_new_task_label_horizon(
            base_experiment_id,
            explicit_label_horizon=label_horizon,
        )
        effective_random_seed = normalize_qe_random_seed(
            random_seed,
            field_name="create_task.random_seed",
        )
        root_experiment_id = base_experiment_id
        # rdagent_task_sota: 从 0 开始，Loop1 为初始回测
        # qe_experiment: 从 1 开始，基础实验已完成相当于 Loop1
        start_loop_index = 0 if start_from_loop_zero else 1
        if row.get('is_evolution_loop') and row.get('parent_experiment_id'):
            root_experiment_id = row['parent_experiment_id']
            start_loop_index = row.get('loop_index', 1)

        # 检查是否已有该根实验的演进任务
        task_id = root_experiment_id
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT task_id, status, current_loop, strategy_params FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                existing_task = cur.fetchone()

        if existing_task:
            if existing_task['status'] == 'running':
                raise ValueError(f"该实验已有正在运行的演进任务: {task_id}")
            # 已有任务但已完成/暂停/失败 → 更新为新一轮
            if start_from_loop_zero:
                actual_start = 0  # rdagent_task_sota: 从头开始，Loop1 为初始回测
            else:
                actual_start = max(existing_task['current_loop'], start_loop_index)
            current_strategy_params = self._parse_json_field(
                existing_task.get("strategy_params"),
                "create_task.strategy_params",
            ) if existing_task.get("strategy_params") not in (None, "") else {}
            current_strategy_params["random_seed"] = effective_random_seed
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_tasks
                        SET task_name = %s, target_desc = %s, max_loops = %s,
                            current_loop = %s, status = 'pending',
                            base_experiment_id = %s, node_id = COALESCE(%s, node_id),
                            strategy_params = %s,
                            label_horizon = %s, updated_at = NOW()
                        WHERE task_id = %s
                    """, (
                        task_name, target_desc, actual_start + max_loops,
                        actual_start, base_experiment_id, node_id,
                        json.dumps(current_strategy_params),
                        effective_label_horizon, task_id,
                    ))
                conn.commit()
            logger.info(f"Updated existing evolution task {task_id}: start_loop={actual_start}, max_loops={actual_start + max_loops}")
        else:
            # 查找已有的最大 loop_index（可能之前有手动演进的子 Loop）
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(MAX(loop_index), 0) FROM qe_experiments WHERE parent_experiment_id = %s",
                        (root_experiment_id,),
                    )
                    max_existing_loop = cur.fetchone()[0]

            actual_start = max(max_existing_loop, start_loop_index)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO qe_evolution_tasks
                        (task_id, task_name, target_desc, max_loops, current_loop, status,
                         base_experiment_id, node_id, stock_pool, strategy_params, label_horizon)
                        VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s, %s, %s, %s)
                    """, (
                        task_id, task_name, target_desc, actual_start + max_loops,
                        actual_start, base_experiment_id, node_id, stock_pool,
                        json.dumps({"random_seed": effective_random_seed}),
                        effective_label_horizon,
                    ))
                conn.commit()
            logger.info(f"Created evolution task {task_id}: start_loop={actual_start}, max_loops={actual_start + max_loops}")
            
        return task_id

    def _parse_json_field(self, value: Any, field_name: str) -> Dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        raise ValueError(f"Invalid JSON field for {field_name}: {value}")

    def _extract_label_horizon_from_params(self, params: Any, *, context: str) -> int:
        parsed = self._parse_json_field(params, context) if params not in (None, "") else {}
        return normalize_label_horizon(parsed.get("label_horizon"), field_name=f"{context}.label_horizon")

    def _extract_label_horizon_from_config(self, config: Any, *, context: str) -> int:
        cfg = self._parse_json_field(config, context) if config not in (None, "") else {}
        top_level = cfg.get("label_horizon")
        model_params = cfg.get("model_params") or cfg.get("custom_params") or {}
        params = self._parse_json_field(
            model_params,
            f"{context}.model_params",
        ) if model_params not in (None, "") else {}
        param_value = params.get("label_horizon")
        if top_level not in (None, ""):
            top_horizon = normalize_label_horizon(
                top_level,
                field_name=f"{context}.label_horizon",
            )
            if param_value not in (None, ""):
                param_horizon = normalize_label_horizon(
                    param_value,
                    field_name=f"{context}.model_params.label_horizon",
                )
                if param_horizon != top_horizon:
                    raise ValueError(
                        f"{context}: label_horizon={top_horizon} conflicts with "
                        f"model_params.label_horizon={param_horizon}"
                    )
            return top_horizon
        return normalize_label_horizon(
            param_value,
            field_name=f"{context}.model_params.label_horizon",
        )

    def _apply_label_horizon_to_model_params(self, params: Any, label_horizon: Any) -> Dict[str, Any]:
        parsed = self._parse_json_field(params, "model_params") if params not in (None, "") else {}
        effective = normalize_label_horizon(label_horizon)
        existing = parsed.get("label_horizon")
        if existing not in (None, ""):
            existing_horizon = normalize_label_horizon(existing, field_name="model_params.label_horizon")
            if existing_horizon != effective:
                raise ValueError(
                    f"model_params.label_horizon={existing_horizon} conflicts with task label_horizon={effective}"
                )
        if effective == DEFAULT_LABEL_HORIZON:
            parsed.pop("label_horizon", None)
        else:
            parsed["label_horizon"] = effective
        return parsed

    def _resolve_new_task_label_horizon(
        self,
        base_experiment_id: str,
        *,
        explicit_label_horizon: Any = None,
    ) -> int:
        if explicit_label_horizon not in (None, ""):
            return normalize_label_horizon(
                explicit_label_horizon,
                field_name="label_horizon",
            )
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT custom_params FROM qe_experiments WHERE experiment_id = %s",
                    (base_experiment_id,),
                )
                row = cur.fetchone()
        if not row:
            raise ValueError(f"base_experiment_id not found: {base_experiment_id}")
        return self._extract_label_horizon_from_params(
            row.get("custom_params"),
            context=f"base_experiment[{base_experiment_id}].custom_params",
        )

    def _get_source_loop_label_horizon(self, task_id: str, loop_index: int) -> int:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT config_json FROM qe_evolution_loops WHERE task_id = %s AND loop_index = %s",
                    (task_id, loop_index),
                )
                row = cur.fetchone()
                if row and row.get("config_json"):
                    cfg = self._parse_json_field(
                        row.get("config_json"),
                        f"source_loop[{task_id}/Loop{loop_index}].config_json",
                    )
                    model_params = cfg.get("model_params") or {}
                    if isinstance(model_params, str):
                        model_params = self._parse_json_field(
                            model_params,
                            f"source_loop[{task_id}/Loop{loop_index}].model_params",
                        )
                    if "label_horizon" in cfg or (
                        isinstance(model_params, dict) and "label_horizon" in model_params
                    ):
                        return self._extract_label_horizon_from_config(
                            cfg,
                            context=f"source_loop[{task_id}/Loop{loop_index}].config_json",
                        )

                cur.execute(
                    "SELECT label_horizon FROM qe_evolution_tasks WHERE task_id = %s",
                    (task_id,),
                )
                task_row = cur.fetchone()
                if task_row and task_row.get("label_horizon") not in (None, ""):
                    return normalize_label_horizon(
                        task_row.get("label_horizon"),
                        field_name=f"source_task[{task_id}].label_horizon",
                    )

                cur.execute(
                    """
                    SELECT custom_params FROM qe_experiments
                    WHERE qe_task_id = %s AND loop_index = %s
                    ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (task_id, loop_index),
                )
                exp_row = cur.fetchone()
        if exp_row and exp_row.get("custom_params"):
            return self._extract_label_horizon_from_params(
                exp_row.get("custom_params"),
                context=f"source_experiment[{task_id}/Loop{loop_index}].custom_params",
            )
        raise ValueError(f"source loop not found for label_horizon: {task_id}/Loop{loop_index}")

    def _enforce_config_label_horizon(self, config: Dict[str, Any], label_horizon: Any, *, context: str) -> Dict[str, Any]:
        effective = normalize_label_horizon(label_horizon, field_name=f"{context}.label_horizon")
        next_config = dict(config or {})
        next_config["model_params"] = self._apply_label_horizon_to_model_params(
            next_config.get("model_params") or {},
            effective,
        )
        return next_config

    def _load_base_config_from_experiment(self, base_experiment_id: str) -> Dict[str, Any]:
        """
        从基础实验加载配置。支持主实验和子 Loop 实验。
        如果 base_experiment_id 是子 Loop（有 parent_experiment_id），
        则从该子 Loop 的配置加载（继承其演进结果）。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT factor_names, model_id, strategy_id, data_split, custom_params,
                           parent_experiment_id, is_evolution_loop
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    """,
                    (base_experiment_id,),
                )
                row = cur.fetchone()

        if not row:
            raise ValueError(f"Base experiment not found: {base_experiment_id}")

        factor_names = row.get("factor_names")
        if isinstance(factor_names, str):
            factor_names = json.loads(factor_names)
        if not isinstance(factor_names, list) or len(factor_names) == 0:
            raise ValueError(f"Base experiment {base_experiment_id} has invalid factor_names")

        data_split = self._parse_json_field(row.get("data_split"), "data_split")
        custom_params = self._parse_json_field(row.get("custom_params"), "custom_params")

        # 如果基础实验是子 Loop（演进产物），action_type 应为 param_tune（继续演进）
        # 如果是主实验（首次演进），action_type 为 initial
        action_type = "param_tune" if row.get("is_evolution_loop") else "initial"

        return {
            "action_type": action_type,
            "factor_list": factor_names,
            "model_id": row.get("model_id"),
            "strategy_id": row.get("strategy_id"),
            "data_split": data_split,
            "model_params": custom_params,
            "base_experiment_id": base_experiment_id,
        }

    # ================================================================
    # 因子可用性验证 + 因子黑名单
    # ================================================================

    def _query_factor_correlation_pairs(self, factor_names: List[str], threshold: float = 0.0) -> List[Dict]:
        """
        查询因子列表中所有已计算的相关性对。
        threshold: |correlation| 过滤阈值，默认 0 返回全部。
        返回 [{factor_a, factor_b, correlation}]，按 |correlation| DESC 排序。
        """
        if len(factor_names) < 2:
            return []

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # factor_name → catalog_id 映射
                ph = ",".join(["%s"] * len(factor_names))
                cur.execute(f"""
                    SELECT DISTINCT ON (factor_name) factor_name, id
                    FROM aistock_factor_catalog
                    WHERE factor_name IN ({ph})
                    ORDER BY factor_name, id
                """, factor_names)
                id_map = {row["factor_name"]: row["id"] for row in cur.fetchall()}
                id_to_name = {v: k for k, v in id_map.items()}
                ids = list(id_map.values())

                if len(ids) < 2:
                    return []

                cur.execute("""
                    SELECT factor_a_id, factor_b_id, correlation
                    FROM qe_factor_correlations
                    WHERE factor_a_id = ANY(%s)
                      AND factor_b_id = ANY(%s)
                      AND ABS(correlation) >= %s
                    ORDER BY ABS(correlation) DESC
                """, (ids, ids, threshold))

                pairs = []
                for row in cur.fetchall():
                    pairs.append({
                        "factor_a": id_to_name.get(row["factor_a_id"], f"id={row['factor_a_id']}"),
                        "factor_b": id_to_name.get(row["factor_b_id"], f"id={row['factor_b_id']}"),
                        "correlation": float(row["correlation"]),
                    })
                return pairs

    def validate_factor_availability(self, factor_list: List[str]) -> Dict[str, Any]:
        """
        检查因子列表中的可用性问题:
        1. 已删除因子（不在 catalog 中）
        2. 不可用因子（is_available=FALSE）
        3. 高相关性因子对（|corr| > 0.7，仅警告）
        """
        if not factor_list:
            return {"has_issues": False, "deleted_factors": [], "unavailable_factors": [],
                    "valid_factors": [], "high_corr_pairs": [], "warnings": []}

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 查询所有存在且可用的因子
                placeholders = ",".join(["%s"] * len(factor_list))
                cur.execute(f"""
                    SELECT factor_name, is_available
                    FROM aistock_factor_catalog
                    WHERE factor_name IN ({placeholders})
                """, factor_list)
                catalog_map = {row["factor_name"]: row["is_available"] for row in cur.fetchall()}

                deleted_factors = [f for f in factor_list if f not in catalog_map]
                unavailable_factors = [f for f in factor_list if catalog_map.get(f) is False]
                valid_factors = [f for f in factor_list
                                 if f in catalog_map and catalog_map[f] is not False]

        # 复用 _query_factor_correlation_pairs，只取 |corr| > 0.7
        high_corr_pairs = self._query_factor_correlation_pairs(valid_factors, threshold=0.7)

        warnings = []
        if high_corr_pairs:
            warnings.append(
                f"存在 {len(high_corr_pairs)} 对高相关因子 (|corr| > 0.7)"
            )

        has_issues = len(deleted_factors) > 0 or len(unavailable_factors) > 0
        if deleted_factors:
            warnings.insert(0, f"{len(deleted_factors)} 个因子已被删除: {deleted_factors}")
        if unavailable_factors:
            warnings.insert(0, f"{len(unavailable_factors)} 个因子已标记不可用: {unavailable_factors}")

        return {
            "has_issues": has_issues,
            "deleted_factors": deleted_factors,
            "unavailable_factors": unavailable_factors,
            "valid_factors": valid_factors,
            "high_corr_pairs": high_corr_pairs,
            "warnings": warnings,
        }

    def _add_to_factor_blacklist(self, task_id: str, factor_names: List[str]):
        """将因子加入任务黑名单，后续演进不得引入"""
        if not factor_names:
            return
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_evolution_tasks
                    SET factor_blacklist = (
                        SELECT jsonb_agg(DISTINCT elem)
                        FROM (
                            SELECT jsonb_array_elements(COALESCE(factor_blacklist, '[]'::jsonb)) AS elem
                            UNION ALL
                            SELECT jsonb_array_elements(%s::jsonb)
                        ) sub
                    ),
                    updated_at = NOW()
                    WHERE task_id = %s
                """, (json.dumps(factor_names), task_id))
            conn.commit()
        logger.info(f"因子黑名单更新: task={task_id}, added={factor_names}")

    def _get_factor_blacklist(self, task_id: str) -> set:
        """获取任务的因子黑名单"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT factor_blacklist FROM qe_evolution_tasks WHERE task_id = %s",
                    (task_id,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    bl = row[0] if isinstance(row[0], list) else json.loads(row[0])
                    return set(bl)
        return set()

    def _compute_factor_context(self, factor_list: List[str]) -> Dict[str, Any]:
        """计算因子组合特征，供 ModelAgent 参考因子-模型兼容性。"""
        if not factor_list:
            return {"factor_count": 0, "category_distribution": {}, "ts_ratio": 0.0}
        cats: Dict[str, int] = {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                ph = ",".join(["%s"] * len(factor_list))
                cur.execute(f"""
                    SELECT factor_name, category
                    FROM qe_factor_classification
                    WHERE factor_name IN ({ph})
                """, factor_list)
                for name, cat in cur.fetchall():
                    cat_key = cat or "unknown"
                    cats[cat_key] = cats.get(cat_key, 0) + 1
        total = len(factor_list)
        ts_count = cats.get("ts_momentum", 0) + cats.get("ts_mean_reversion", 0) + cats.get("ts_volatility", 0)
        return {
            "factor_count": total,
            "category_distribution": cats,
            "ts_ratio": round(ts_count / max(total, 1), 2),
        }

    def _build_full_evolution_history(self, task_id: str) -> Dict[str, Any]:
        """
        构建完整演进历史（查询所有已完成 loop，不再 LIMIT 3）。
        供所有 Agent 做全局最优决策。
        如果 task 是 fork 且 inherit_history=True，先加载源 task 截止到 fork point 的历史。
        """
        inherited_rows = []
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 检查是否为 fork task 且需要继承历史
                cur.execute(
                    "SELECT fork_from_task_id, fork_from_loop_index, inherit_history "
                    "FROM qe_evolution_tasks WHERE task_id = %s",
                    (task_id,),
                )
                task_meta = cur.fetchone()
                if (
                    task_meta
                    and task_meta.get("inherit_history")
                    and task_meta.get("fork_from_task_id")
                    and task_meta.get("fork_from_loop_index") is not None
                ):
                    cur.execute("""
                        SELECT loop_index, action_type, config_json, metrics_json,
                               agent_analysis, is_sota
                        FROM qe_evolution_loops
                        WHERE task_id = %s AND status = 'completed'
                              AND loop_index <= %s
                        ORDER BY loop_index ASC
                    """, (task_meta["fork_from_task_id"], task_meta["fork_from_loop_index"]))
                    inherited_rows = cur.fetchall()

                cur.execute("""
                    SELECT loop_index, action_type, config_json, metrics_json,
                           agent_analysis, is_sota
                    FROM qe_evolution_loops
                    WHERE task_id = %s AND status = 'completed'
                    ORDER BY loop_index ASC
                """, (task_id,))
                rows = cur.fetchall()

        loops = []
        sota_loop_index = None
        sota_metrics = None
        ic_trend = []
        action_type_deltas: Dict[str, list] = {}
        failed_approaches = []
        prev_ic = None

        # 先处理继承的历史 rows（标记 inherited）
        all_rows = [(r, True) for r in inherited_rows] + [(r, False) for r in rows]

        for row, is_inherited in all_rows:
            config = row.get("config_json") or {}
            if isinstance(config, str):
                config = json.loads(config)
            metrics = row.get("metrics_json") or {}
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            analysis = row.get("agent_analysis") or {}
            if isinstance(analysis, str):
                analysis = json.loads(analysis)

            cur_ic = metrics.get("IC")

            delta_vs_previous = None
            if prev_ic is not None and cur_ic is not None:
                delta_vs_previous = {
                    "IC": round(cur_ic - prev_ic, 6),
                }

            action = row.get("action_type") or "initial"
            # 剥离 enhanced_metrics（含 top_stocks/stock_trades/return_curves 等大量数据）
            # 避免多 loop 累积后 context 超限；reviewer 只需核心指标
            metrics_summary = {k: v for k, v in metrics.items() if k != "enhanced_metrics"}
            loop_entry = {
                "loop_index": row["loop_index"],
                "action_type": action,
                "config_summary": {
                    "factors": config.get("factor_list", []),
                    "model_id": config.get("model_id"),
                    "model_params": config.get("model_params", {}),
                },
                "metrics": metrics_summary,
                "is_sota": bool(row.get("is_sota")),
                "analyst_report": (
                    analysis.get("analyst", {}).get("report_text", "")
                    if isinstance(analysis.get("analyst"), dict)
                    else analysis.get("analyst", "")
                ),
                "delta_vs_previous": delta_vs_previous,
            }
            if is_inherited:
                loop_entry["inherited"] = True
                if task_meta:
                    loop_entry["source_task_id"] = task_meta.get("fork_from_task_id", "")
            loops.append(loop_entry)

            # ── 以下统计指标只计算当前 task 的 loops（不含 inherited） ──
            # inherited rows 仅供 Agent 参考历史上下文，不影响决策统计
            if not is_inherited:
                ic_trend.append(cur_ic)

                if row.get("is_sota"):
                    sota_loop_index = row["loop_index"]
                    sota_metrics = metrics

                if delta_vs_previous and cur_ic is not None:
                    action_type_deltas.setdefault(action, []).append(
                        delta_vs_previous["IC"]
                    )

                if delta_vs_previous and delta_vs_previous["IC"] < 0:
                    failed_approaches.append({
                        "loop_index": row["loop_index"],
                        "action_type": action,
                        "ic_delta": delta_vs_previous["IC"],
                    })

            prev_ic = cur_ic

        # 汇总 action_type 统计 (S2: 新增 win_rate)
        action_type_stats = {}
        for at, deltas in action_type_deltas.items():
            wins = sum(1 for d in deltas if d > 0)
            action_type_stats[at] = {
                "count": len(deltas),
                "avg_ic_delta": round(sum(deltas) / len(deltas), 6) if deltas else 0,
                "win_rate": round(wins / len(deltas), 4) if deltas else 0,
            }

        # S2: 计算 consecutive_same_action（只统计当前 task 的 loops，不含 inherited）
        consecutive_same_action = {"action_type": None, "count": 0, "recent_ic_deltas": []}
        current_loops = [loop_entry for loop_entry in loops if not loop_entry.get("inherited")]
        if current_loops:
            last_action = current_loops[-1]["action_type"]
            consecutive = 0
            recent_deltas = []
            for loop_entry in reversed(current_loops):
                if loop_entry["action_type"] == last_action:
                    consecutive += 1
                    delta = loop_entry.get("delta_vs_previous")
                    if delta and delta.get("IC") is not None:
                        recent_deltas.append(delta["IC"])
                else:
                    break
            consecutive_same_action = {
                "action_type": last_action,
                "count": consecutive,
                "recent_ic_deltas": list(reversed(recent_deltas)),
            }

        # S2: 计算 unexplored_directions
        all_possible = {"factor_adjust", "param_tune", "model_switch"}
        explored = set(action_type_stats.keys())
        unexplored = list(all_possible - explored)

        # S2: 提取最近一轮的训练诊断
        latest_training = {}
        if loops:
            latest_metrics = loops[-1].get("metrics", {})
            em = latest_metrics.get("enhanced_metrics", {})
            td = em.get("training_diagnostics", {})
            if td:
                latest_training = {
                    "best_epoch": td.get("best_epoch"),
                    "convergence_ratio": td.get("convergence_ratio"),
                    "overfit_ratio": td.get("overfit_ratio"),
                    "training_failed": td.get("training_failed", False),
                }

        valid_ics = [x for x in ic_trend if x is not None]
        return {
            "total_loops": len(loops),
            "sota_loop_index": sota_loop_index,
            "sota_metrics": sota_metrics,
            "loops": loops,
            "trend_summary": {
                "ic_trend": ic_trend,
                "best_ic": max(valid_ics) if valid_ics else None,
                "action_type_stats": action_type_stats,
                "consecutive_same_action": consecutive_same_action,
                "unexplored_directions": unexplored,
            },
            "failed_approaches": failed_approaches,
            "latest_training_diagnostics": latest_training,
        }

    def _build_analysis_context(self, task_id: str, factor_names: List[str]) -> Dict[str, Any]:
        """
        S7: 构建 Agent 分析上下文（供 process_completed_loop 使用）。
        包含: factor_profiles, target_desc, evolution_guidance
        """
        analysis_context = {}

        # 因子画像
        factor_profiles = self._get_factor_profile(factor_names)
        if factor_profiles:
            analysis_context["factor_profiles"] = factor_profiles

        # 演进目标和指引
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT target_desc, evolution_guidance FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task_row = cur.fetchone()
        target_desc = (task_row or {}).get("target_desc", "")
        evolution_guidance = (task_row or {}).get("evolution_guidance", "")
        if target_desc:
            analysis_context["target_desc"] = target_desc
        if evolution_guidance:
            analysis_context["evolution_guidance"] = evolution_guidance

        return analysis_context

    def _get_evolution_mode(self, task_id: str) -> str:
        """获取任务的 evolution_mode 设置。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT evolution_mode FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                row = cur.fetchone()
                return (row[0] if row and row[0] else "auto")

    def _get_sota_loop_config(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取 SOTA Loop 的完整配置（用于退化回滚）。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT l.config_json
                    FROM qe_sota_registry r
                    JOIN qe_evolution_loops l ON r.loop_id = l.loop_id
                    WHERE l.task_id = %s
                    ORDER BY r.created_at DESC LIMIT 1
                """, (task_id,))
                row = cur.fetchone()
                if row and row.get('config_json'):
                    cfg = row['config_json']
                    if isinstance(cfg, str):
                        cfg = json.loads(cfg)
                    return cfg
        return None

    def _check_sota_surpassed(self, evolution_history: Dict[str, Any]) -> bool:
        """
        检查 SOTA 是否已被超越。
        Returns: True if any post-SOTA loop has IC > SOTA IC + 0.002
        """
        sota_idx = evolution_history.get("sota_loop_index")
        sota_metrics = evolution_history.get("sota_metrics")
        if sota_idx is None or not sota_metrics:
            return False  # 无 SOTA 记录
        sota_ic = sota_metrics.get("IC", 0) or 0

        for lp in evolution_history.get("loops", []):
            if lp["loop_index"] <= sota_idx:
                continue
            lp_ic = (lp.get("metrics") or {}).get("IC")
            if lp_ic is not None and lp_ic > sota_ic + 0.002:
                return True
        return False

    def _decide_evolution_direction(
        self, analyst_result: 'AnalystResult', evolution_mode: str, metrics: Dict[str, Any],
        is_sota: bool = False, config: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Phase 7: 方向控制器。
        Level 1: evolution_mode（用户设置） → 如非 auto 直接返回
        Level 1.5: auto 模式下因子数 < 15 → 强制 factor_expand
        Level 2: Analyst Step 2 LLM 决策结果 → auto 模式时使用

        注意: factor_adjust 方向始终允许通过，SOTA 因子保护由 Factor Agent
        的 add-only 机制实现（sota_protected_factors 参数），而非在此层阻止。
        """
        # Level 1: 用户指定模式
        if evolution_mode == "factor_only":
            return "factor_adjust"
        if evolution_mode == "model_only":
            diag = metrics.get("enhanced_metrics", {}).get("training_diagnostics", {})
            return "model_switch" if diag.get("training_failed") else "param_tune"
        if evolution_mode == "joint":
            return "factor_model_joint"

        # Level 1.5: auto 模式 — 因子数 < 15 时强制 factor_expand
        current_factor_count = len((config or {}).get("factor_list", []))
        if evolution_mode == "auto" and current_factor_count < 15:
            logger.info(f"因子数={current_factor_count} < 15，强制 factor_expand 扩充因子组合")
            return "factor_expand"

        # Level 2: auto — 使用 Analyst Step 2 的方向决策
        direction = analyst_result.direction
        recommended = direction.get("recommended_direction", "")
        valid_directions = {"factor_adjust", "param_tune", "model_switch", "factor_model_joint", "factor_expand"}
        if recommended in valid_directions:
            if is_sota and recommended in ("model_switch", "factor_model_joint"):
                logger.info(f"SOTA 轮选择了激进方向 {recommended}，回滚机制保障安全")
            return recommended

        logger.error(
            f"Analyst 返回无效的 recommended_direction: '{recommended}'，"
            f"有效值: {valid_directions}"
        )
        raise ValueError(
            f"无效的演进方向 '{recommended}'，Analyst 决策异常，"
            f"请检查 LLM 返回内容"
        )

    @staticmethod
    def _compute_training_diagnostics(training_curves: Dict[str, Any]) -> Dict[str, Any]:
        """从训练曲线数据派生收敛诊断指标。"""
        train_loss = training_curves.get("train_loss", [])
        val_loss = training_curves.get("val_loss", [])

        if not train_loss:
            return {"training_failed": True, "best_epoch": 0}

        best_epoch = 0
        best_val = float("inf")
        for i, v in enumerate(val_loss or train_loss):
            if v is not None and v < best_val:
                best_val = v
                best_epoch = i + 1

        total_epochs = len(train_loss)
        convergence_ratio = best_epoch / total_epochs if total_epochs > 0 else 0
        overfit_ratio = 0.0
        if val_loss and train_loss and len(val_loss) == len(train_loss):
            final_train = train_loss[-1] or 0
            final_val = val_loss[-1] or 0
            if final_train > 0:
                overfit_ratio = max(0, (final_val - final_train) / final_train)

        training_failed = best_epoch == 0 or (best_epoch == 1 and total_epochs > 3)
        loss_plateau = False
        if len(val_loss or train_loss) >= 5:
            recent = (val_loss or train_loss)[-5:]
            recent_valid = [x for x in recent if x is not None]
            if recent_valid and max(recent_valid) - min(recent_valid) < 0.001:
                loss_plateau = True

        return {
            "best_epoch": best_epoch,
            "total_epochs": total_epochs,
            "convergence_ratio": round(convergence_ratio, 4),
            "overfit_ratio": round(overfit_ratio, 4),
            "training_failed": training_failed,
            "loss_plateau": loss_plateau,
            "train_loss_final": train_loss[-1] if train_loss else None,
            "val_loss_final": val_loss[-1] if val_loss else None,
        }

    def _write_loop_factor_records(
        self, cur, task_id: str, loop_id: str, loop_index: int,
        config: Dict, metrics: Dict, action_type: str, is_sota: bool,
    ):
        """写入 qe_loop_factor_records，含 action_role 计算（S4）。"""
        curr_factors = set(config.get("factor_list", []))
        if not curr_factors:
            return

        # 获取上一轮因子列表以计算 action_role
        prev_factors: set = set()
        if loop_index > 1:
            prev_loop_id = f"{task_id}_Loop{loop_index - 1}"
            cur.execute(
                "SELECT config_json FROM qe_evolution_loops WHERE loop_id = %s",
                (prev_loop_id,),
            )
            prev_row = cur.fetchone()
            if prev_row:
                prev_config = prev_row[0] if isinstance(prev_row[0], dict) else json.loads(prev_row[0] or "{}")
                prev_factors = set(prev_config.get("factor_list", []))

        combo_ic = metrics.get("IC")
        combo_icir = metrics.get("ICIR")
        combo_sharpe = metrics.get("sharpe")
        combo_ann_return = metrics.get("annualized_return")
        combo_max_drawdown = metrics.get("max_drawdown")
        model_id = config.get("model_id")
        all_factors = list(curr_factors)

        # 解析 factor_catalog_id 和 model_catalog_id
        all_factor_names = curr_factors | (prev_factors - curr_factors)
        factor_catalog_map: Dict[str, tuple] = {}
        if all_factor_names:
            ph = ",".join(["%s"] * len(all_factor_names))
            cur.execute(f"""
                SELECT DISTINCT ON (factor_name) factor_name, id, source
                FROM aistock_factor_catalog
                WHERE factor_name IN ({ph})
                ORDER BY factor_name,
                         CASE WHEN source = 'rdagent_task_sync' THEN 0 ELSE 1 END,
                         id
            """, list(all_factor_names))
            for row in cur.fetchall():
                factor_catalog_map[row[0]] = (row[1], row[2])

        model_catalog_id = None
        cur.execute("""
            SELECT mc.id FROM qe_evolution_tasks et
            JOIN aistock_model_catalog mc ON mc.task_run_id = et.source_task_id
            WHERE et.task_id = %s LIMIT 1
        """, (task_id,))
        mc_row = cur.fetchone()
        if mc_row:
            model_catalog_id = mc_row[0]

        # 当前因子：kept / added
        for factor in curr_factors:
            fc_id, fc_source = factor_catalog_map.get(factor, (None, None))
            if fc_id is None:
                logger.warning(f"因子 {factor} 未在 catalog 中找到，跳过记录写入")
                continue
            role = "added" if factor not in prev_factors else "kept"
            other = [f for f in all_factors if f != factor]
            cur.execute("""
                INSERT INTO qe_loop_factor_records
                (task_id, loop_id, loop_index, factor_name, action_role,
                 combo_ic, combo_icir, combo_sharpe, combo_ann_return, combo_max_drawdown,
                 model_id, action_type, is_sota, other_factors,
                 factor_catalog_id, factor_source, model_catalog_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s)
                ON CONFLICT (loop_id, factor_name) DO NOTHING
            """, (
                task_id, loop_id, loop_index, factor, role,
                combo_ic, combo_icir, combo_sharpe, combo_ann_return, combo_max_drawdown,
                model_id, action_type, is_sota, json.dumps(other),
                fc_id, fc_source, model_catalog_id,
            ))

        # 被移除的因子
        for factor in prev_factors - curr_factors:
            fc_id, fc_source = factor_catalog_map.get(factor, (None, None))
            if fc_id is None:
                continue
            cur.execute("""
                INSERT INTO qe_loop_factor_records
                (task_id, loop_id, loop_index, factor_name, action_role,
                 model_id, action_type, is_sota,
                 factor_catalog_id, factor_source, model_catalog_id)
                VALUES (%s, %s, %s, %s, 'removed', %s, %s, %s,
                        %s, %s, %s)
                ON CONFLICT (loop_id, factor_name) DO NOTHING
            """, (task_id, loop_id, loop_index, factor, model_id, action_type, is_sota,
                  fc_id, fc_source, model_catalog_id))

    def _write_loop_model_records(
        self, cur, task_id: str, loop_id: str, loop_index: int,
        config: Dict, metrics: Dict, action_type: str, is_sota: bool,
    ):
        """写入 qe_loop_model_records，含训练诊断数据。"""
        model_id = config.get("model_id")
        if not model_id:
            return

        # 解析 model_catalog_id
        model_catalog_id = None
        cur.execute("""
            SELECT mc.id FROM qe_evolution_tasks et
            JOIN aistock_model_catalog mc ON mc.task_run_id = et.source_task_id
            WHERE et.task_id = %s LIMIT 1
        """, (task_id,))
        mc_row = cur.fetchone()
        if mc_row:
            model_catalog_id = mc_row[0]

        if model_catalog_id is None:
            logger.warning(f"模型 catalog 未找到 (task={task_id})，跳过 model_records 写入")
            return

        em = metrics.get("enhanced_metrics", {})
        td = em.get("training_diagnostics", {})
        tc = em.get("training_curves", {})

        factor_list = config.get("factor_list", [])
        cur.execute("""
            INSERT INTO qe_loop_model_records
            (task_id, loop_id, loop_index, model_id, model_type,
             combo_ic, combo_icir, combo_sharpe, combo_ann_return, combo_max_drawdown,
             model_params, best_epoch, total_epochs, convergence_ratio, overfit_ratio,
             training_failed, train_loss_final, val_loss_final,
             train_loss_curve, val_loss_curve,
             action_type, is_sota, factor_count, factor_list,
             model_catalog_id)
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s)
            ON CONFLICT (loop_id, model_id) DO NOTHING
        """, (
            task_id, loop_id, loop_index, model_id, config.get("model_type"),
            metrics.get("IC"), metrics.get("ICIR"), metrics.get("sharpe"),
            metrics.get("annualized_return"), metrics.get("max_drawdown"),
            json.dumps(config.get("model_params", {})),
            td.get("best_epoch"), td.get("total_epochs"),
            td.get("convergence_ratio"), td.get("overfit_ratio"),
            td.get("training_failed", False),
            td.get("train_loss_final"), td.get("val_loss_final"),
            json.dumps(tc.get("train_loss")) if tc.get("train_loss") else None,
            json.dumps(tc.get("val_loss")) if tc.get("val_loss") else None,
            action_type, is_sota, len(factor_list), json.dumps(factor_list),
            model_catalog_id,
        ))

    def _get_factor_profile(self, factor_names: List[str]) -> Dict[str, Any]:
        """获取因子画像数据（factor_profile）供演进Agent使用。"""
        if not factor_names:
            return {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                ph = ",".join(["%s"] * len(factor_names))
                cur.execute(f"""
                    SELECT factor_name, factor_profile
                    FROM qe_factor_classification
                    WHERE factor_name IN ({ph}) AND factor_profile IS NOT NULL
                """, factor_names)
                return {row[0]: row[1] for row in cur.fetchall()}

    def _get_relevant_correlations(self, factor_names: List[str]) -> Dict[str, Any]:
        """查询当前因子组合的相关性矩阵（如果存在）。"""
        if not factor_names or len(factor_names) < 2:
            return {}
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 先获取 factor_name -> catalog_id 映射
                    ph = ",".join(["%s"] * len(factor_names))
                    cur.execute(f"""
                        SELECT DISTINCT ON (factor_name) factor_name, id
                        FROM aistock_factor_catalog
                        WHERE factor_name IN ({ph})
                        ORDER BY factor_name, id
                    """, factor_names)
                    id_map = {row[0]: row[1] for row in cur.fetchall()}
                    id_to_name = {v: k for k, v in id_map.items()}
                    factor_ids = list(id_map.values())

                    if len(factor_ids) < 2:
                        return {}

                    cur.execute("""
                        SELECT factor_a_id, factor_b_id, correlation
                        FROM qe_factor_correlations
                        WHERE factor_a_id = ANY(%s) AND factor_b_id = ANY(%s)
                    """, (factor_ids, factor_ids))
                    correlations = {}
                    for row in cur.fetchall():
                        name_a = id_to_name.get(row[0], "")
                        name_b = id_to_name.get(row[1], "")
                        key = f"{name_a}_{name_b}"
                        correlations[key] = row[2]
                    return correlations
        except Exception as e:
            logger.error(f"_get_relevant_correlations 查询失败: {e}", exc_info=True)
            raise RuntimeError(f"查询因子相关性失败: {e}") from e

    def _get_factor_library_summary(self) -> Dict[str, Any]:
        """获取因子库摘要（轻量版，每因子~30 tokens），供 researcher 做方向决策。"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT c.category, fr.official_grade,
                               COUNT(*) as cnt,
                               AVG(m.ic_mean) as avg_ic,
                               AVG(m.top_excess_sharpe) as avg_sharpe
                        FROM qe_factor_classification c
                        {_ACTIVE_RATING_JOIN_SQL}
                        LEFT JOIN LATERAL (
                            SELECT ic_mean, top_excess_sharpe
                            FROM aistock_factor_metrics
                            WHERE factor_name = c.factor_name
                              AND eval_window = 'full'
                              AND calc_engine = %s
                            ORDER BY calculated_at DESC
                            LIMIT 1
                        ) m ON TRUE
                        WHERE fr.official_grade IS NOT NULL
                        GROUP BY c.category, fr.official_grade
                        ORDER BY c.category, fr.official_grade
                    """, (CALC_ENGINE,))
                    rows = cur.fetchall()

            summary: Dict[str, Any] = {}
            for row in rows:
                cat = row[0] or "UNKNOWN"
                grade = row[1] or "D"
                cnt = row[2]
                avg_ic = round(float(row[3]), 4) if row[3] is not None else None
                avg_sharpe = round(float(row[4]), 4) if row[4] is not None else None
                if cat not in summary:
                    summary[cat] = {"total": 0, "grades": {}}
                summary[cat]["total"] += cnt
                summary[cat]["grades"][grade] = {
                    "count": cnt,
                    "avg_ic": avg_ic,
                    "avg_sharpe": avg_sharpe,
                }
            return summary
        except Exception as e:
            raise RuntimeError(f"Failed to get factor library summary: {e}") from e

    async def submit_next_loop(self, task_id: str) -> Optional[str]:
        """
        提交下一个 Loop 到 RDAgent，立即返回（不阻塞等待执行完成）。
        Returns: evolution_loop_db_id if submitted, None if task is complete/stopped.
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            logger.error(f"Task {task_id} not found")
            return None

        if task['status'] not in ('pending', 'running'):
            logger.info(f"Task {task_id} is in state {task['status']}, cannot submit next loop.")
            return None

        if task.get("task_type") == "custom_evo":
            next_loop_index = (task["current_loop"] or 0) + 1
            return await self.submit_custom_evo_loop(task_id, next_loop_index)

        current_loop = task['current_loop']
        max_loops = task['max_loops']

        if current_loop >= max_loops:
            logger.info(f"Task {task_id} has reached max_loops ({max_loops}), marking completed.")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE qe_evolution_tasks SET status = 'completed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                conn.commit()
            return None

        # Mark as running (CAS: only if task is in pending or running state)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE qe_evolution_tasks SET status = 'running', updated_at = NOW() WHERE task_id = %s AND status IN ('pending', 'running')", (task_id,))
                if cur.rowcount == 0:
                    logger.warning(f"Task {task_id} CAS update failed: status not in (pending, running)")
                    return None
            conn.commit()

        loop_index = current_loop + 1
        loop_id = f"Loop{loop_index}"
        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        logger.info(f"Submitting Loop {loop_index} for task {task_id}")

        # ── Multi-Alpha 分流 (Phase 3) ──────────────────────────────
        # 从任务记录或基础实验检查 alpha_mode
        _alpha_mode = self._detect_alpha_mode(task)
        if _alpha_mode == "multi":
            return await self._submit_multi_alpha_loop(
                task, task_id, loop_index, evolution_loop_db_id
            )

        # 创建 LOOP 记录
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_evolution_loops
                    (loop_id, task_id, loop_index, status)
                    VALUES (%s, %s, %s, 'running')
                    ON CONFLICT (loop_id) DO UPDATE SET status = 'running', updated_at = NOW()
                """, (evolution_loop_db_id, task_id, loop_index))
            conn.commit()

        # 加载配置：查看是否有上一轮 reviewer 输出的 next_config
        # F3: 统一异常捕获覆盖 config 加载 + ConfigComposer + RDAgent 提交
        try:
            config = None
            if loop_index > 1:
                prev_loop_db_id = f"{task_id}_Loop{loop_index - 1}"
                with get_conn() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT agent_analysis FROM qe_evolution_loops
                            WHERE loop_id = %s AND status = 'completed'
                        """, (prev_loop_db_id,))
                        prev_row = cur.fetchone()
                        if prev_row and prev_row.get('agent_analysis'):
                            analysis = prev_row['agent_analysis']
                            if isinstance(analysis, str):
                                analysis = json.loads(analysis)
                            reviewer_config = (analysis.get("reviewer") or {}).get("validated_config")
                            if isinstance(reviewer_config, dict):
                                config = reviewer_config
                                logger.info(f"Loop {loop_index}: 配置来源 = reviewer_config (prev loop {prev_loop_db_id})")
                            else:
                                logger.warning(f"Loop {loop_index}: 上轮 loop {prev_loop_db_id} 缺少 reviewer.validated_config")

            if config is None:
                config = self._load_base_config_from_experiment(task["base_experiment_id"])
                logger.info(f"Loop {loop_index}: 配置来源 = base_experiment {task['base_experiment_id']}")

            action_type = config.get("action_type", "initial" if current_loop == 0 else "param_tune")

            # ── 每轮 Loop 因子可用性检查 ──
            factor_list = config.get("factor_list", [])
            validation = self.validate_factor_availability(factor_list)
            if validation["has_issues"]:
                # 严格模式：因子不可用时暂停任务，不静默移除
                logger.error(
                    f"Loop {loop_index} 因子可用性检查失败。"
                    f"已删除: {validation['deleted_factors']}, "
                    f"不可用: {validation['unavailable_factors']}。"
                    f"任务暂停，请修复因子后恢复。"
                )
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE qe_evolution_tasks SET status = 'paused', updated_at = NOW()
                            WHERE task_id = %s
                        """, (task_id,))
                    conn.commit()
                raise ValueError(
                    f"Loop {loop_index}: 因子可用性检查失败。"
                    f"已删除: {validation['deleted_factors']}, "
                    f"不可用: {validation['unavailable_factors']}。"
                    f"任务已暂停，请通过 /evolution/tasks/{task_id}/resolve-factors 修复。"
                )

            from .experiment_config_builders import build_config_from_evolution_loop
            from .config_composer import ConfigComposer
            from .executors.backtest import BacktestExecutor, BacktestMode
            from .executors.base import ExecutionContext

            experiment_name = f"{task_id}/{loop_id}"
            cfg = build_config_from_evolution_loop(
                config,
                task,
                experiment_name=experiment_name,
            )
            config = dict(config)
            loop_model_params = merge_qe_minute_runtime_contract(
                cfg.build_custom_params(),
                config=config,
                execution_algo=cfg.execution_algo,
                execution_algo_params=cfg.execution_algo_params,
                source="evolution_loop_config",
                allow_default_execution_algo=True,
            )
            config["model_params"] = loop_model_params
            runtime_contract = build_qe_minute_runtime_contract(
                custom_params=loop_model_params,
                execution_algo=cfg.execution_algo,
                execution_algo_params=cfg.execution_algo_params,
                source="evolution_loop_config",
                allow_default_execution_algo=True,
            )
            if runtime_contract:
                config.update(runtime_contract)

            # Persist the loop config for reviewer output and the next evolution loop.
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_loops SET config_json = %s, action_type = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(config), action_type, evolution_loop_db_id))
                conn.commit()

            client = self._get_workspace_client_for_task(task_id)
            executor = BacktestExecutor(ConfigComposer(), client)
            ctx = ExecutionContext(
                task_id=task_id,
                loop_index=loop_index,
                experiment_name=experiment_name,
                node_id=task.get("node_id"),
                callback_url=self._get_callback_url_for_task(task_id),
                require_fixed_seed=True,
            )
            await executor.submit(cfg, ctx, mode=BacktestMode.FULL_TRAIN)
        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"Failed to submit loop {loop_index} for task {task_id}: {e}\n{tb_str}")
            # 幂等保护：独立 try/except 确保 DB 状态不泄漏
            try:
                with get_conn() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
                        cur.execute("UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s", (error_detail, evolution_loop_db_id))
                        # custom_evo/strategy_evo：单个 loop 失败不标记整个 task，只有所有 loop 都失败才标记
                        cur.execute("SELECT task_type FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                        task_row = cur.fetchone()
                        if not task_row or task_row.get("task_type") not in ("strategy_evo", "custom_evo"):
                            cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                        else:
                            cur.execute("SELECT COUNT(*) FROM qe_evolution_loops WHERE task_id = %s AND status != 'failed'", (task_id,))
                            non_failed = cur.fetchone()
                            if non_failed and non_failed[0] == 0:
                                cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"FATAL: Failed to mark loop/task as failed for {task_id}: {db_err}")
            return None

        return evolution_loop_db_id

    async def process_completed_loop(self, task_id: str, loop_id_str: str) -> bool:
        """
        Loop 完成后处理：获取 metrics → Agent 分析 → 更新 DB → 判断是否继续。
        CAS 幂等保护：只有 status='running' 的 loop 会被处理。
        Returns: True if processing succeeded, False if skipped/failed.
        """
        # 检查任务类型：策略演进走简化流程
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT task_type, base_experiment_id, label_horizon FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task_row = cur.fetchone()

        if task_row and task_row.get("task_type") in ("strategy_evo", "custom_evo"):
            return await self.process_strategy_evo_completed_loop(task_id, loop_id_str)

        # 检查是否为多Alpha实验：先收集组级结果再走后续流程
        if task_row and task_row.get("base_experiment_id"):
            _alpha_mode = self._detect_alpha_mode({"base_experiment_id": task_row["base_experiment_id"]})
            if _alpha_mode == "multi":
                from .multi_alpha_result_collector import MultiAlphaResultCollector
                # experiment_id 格式: "{task_id}_L{loop_index}"
                _parts = loop_id_str.rsplit("_Loop", 1)
                if len(_parts) == 2:
                    _exp_id = f"{_parts[0]}_L{_parts[1]}"
                else:
                    # 无法从 loop_id_str 推断，查询 DB
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT experiment_id FROM qe_evolution_loops WHERE loop_id = %s",
                                (loop_id_str,),
                            )
                            _eid_row = cur.fetchone()
                    _exp_id = _eid_row[0] if _eid_row and _eid_row[0] else loop_id_str

                try:
                    collector = MultiAlphaResultCollector()
                    result = await collector.collect_and_persist(_exp_id)
                    if result.get("ok"):
                        logger.info(f"多Alpha结果收集完成: {_exp_id}")
                    elif result.get("reason") == "pending_groups":
                        # 还有组未完成训练，等待下一个组完成时再次触发
                        logger.info(
                            f"多Alpha结果收集跳过: {_exp_id}, "
                            f"等待 {len(result.get('pending', []))} 组完成: "
                            f"{result.get('pending')}"
                        )
                    else:
                        logger.error(f"��Alpha结果收集返回异常: {_exp_id}: {result}")
                except Exception as e:
                    # 不静默：记录 ERROR 级别，但不阻断演进流程（演进可能需要继续下一轮）
                    logger.error(f"多Alpha结果收集失败: {_exp_id}: {e}", exc_info=True)

        # 提取 loop_index from loop_id_str (format: "{task_id}_Loop{N}")
        loop_suffix = loop_id_str.rsplit("_Loop", 1)
        if len(loop_suffix) != 2:
            logger.error(f"Invalid loop_id format: {loop_id_str}")
            return False
        try:
            loop_index = int(loop_suffix[1])
        except ValueError:
            logger.error(f"Invalid loop_index in loop_id: {loop_id_str}")
            return False

        evolution_loop_db_id = loop_id_str
        loop_id = f"Loop{loop_index}"

        # 每轮开始前清空 LLM trace
        self.agents.reset_trace()

        # CAS 幂等保护: 只有 status='running' 的 loop 才能被处理
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    UPDATE qe_evolution_loops SET status = 'processing', updated_at = NOW()
                    WHERE loop_id = %s AND status = 'running'
                    RETURNING loop_id
                """, (evolution_loop_db_id,))
                cas_row = cur.fetchone()
            conn.commit()

        if not cas_row:
            logger.info(f"Loop {evolution_loop_db_id} is not in 'running' state, skipping (idempotent).")
            return False

        try:
            # 读取配置
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT config_json FROM qe_evolution_loops WHERE loop_id = %s", (evolution_loop_db_id,))
                    loop_row = cur.fetchone()
            if not loop_row or not loop_row.get('config_json'):
                raise ValueError(f"Loop {evolution_loop_db_id} config_json 为空，数据完整性异常")
            config = loop_row['config_json']
            if isinstance(config, str):
                config = json.loads(config)
            action_type = config.get("action_type", "initial")

            # 获取回测结果
            client = self._get_workspace_client_for_task(task_id)
            metrics = await client.get_loop_metrics(task_id, loop_id)

            # Normalize QLib metric keys → frontend-expected short keys
            _METRIC_ALIASES = {
                "Rank IC": "Rank_IC",
                "1day.excess_return_with_cost.information_ratio": "sharpe",
                "1day.excess_return_with_cost.annualized_return": "annualized_return",
                "1day.excess_return_without_cost.annualized_return": "annualized_return_no_cost",
                "1day.excess_return_with_cost.max_drawdown": "max_drawdown",
                "1day.excess_return_without_cost.information_ratio": "sharpe_no_cost",
                "1day.excess_return_without_cost.max_drawdown": "max_drawdown_no_cost",
                "1day.excess_return_with_cost.mean": "daily_return",
                "1day.excess_return_without_cost.mean": "daily_return_no_cost",
            }
            for src, dst in _METRIC_ALIASES.items():
                if src in metrics and dst not in metrics:
                    metrics[dst] = metrics[src]

            # 拉取增强诊断指标（训练曲线、收敛诊断等）
            enhanced_data = await client.get_enhanced_metrics(task_id, loop_id)
            # 计算训练诊断派生指标
            td = enhanced_data.get("training_diagnostics", {})
            if not td and "training_curves" in enhanced_data:
                td = self._compute_training_diagnostics(enhanced_data.get("training_curves", {}))
                enhanced_data["training_diagnostics"] = td
            metrics["enhanced_metrics"] = enhanced_data

            # S3: enhanced_metrics 先写入 DB，确保后续 _build_full_evolution_history 可读取
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_loops
                        SET metrics_json = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(metrics), evolution_loop_db_id))
                conn.commit()

            # Agent 分析（传递完整演进历史）
            logger.info(f"Running Agent analysis for loop {loop_index} of task {task_id}")
            evolution_history = self._build_full_evolution_history(task_id)

            # 使用共享方法构建分析上下文
            factor_names = config.get("factor_list", [])
            analysis_context = self._build_analysis_context(task_id, factor_names)
            # 注入 evolution_mode 供 Analyst Step 2 使用
            evolution_mode = self._get_evolution_mode(task_id)
            analysis_context["evolution_mode"] = evolution_mode

            # Analyst 两步 LLM — 返回 AnalystResult
            analyst_result = await self.agents.run_analyst(
                loop_index, config, metrics,
                analysis_context=analysis_context,
                evolution_history=evolution_history,
            )

            historical_sota_metrics = None
            sota_loop_id_ref = None
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT r.loop_id, l.metrics_json
                        FROM qe_sota_registry r
                        JOIN qe_evolution_loops l ON r.loop_id = l.loop_id
                        WHERE l.task_id = %s
                        ORDER BY r.created_at DESC LIMIT 1
                    """, (task_id,))
                    sota_row = cur.fetchone()
                    if sota_row and sota_row['metrics_json']:
                        historical_sota_metrics = sota_row['metrics_json']
                        sota_loop_id_ref = sota_row['loop_id']

            eval_result = await self.agents.run_evaluator(
                metrics, historical_sota_metrics,
                evolution_history=evolution_history,
            )
            is_sota = eval_result["is_sota"]
            eval_reason = eval_result.get("reason", "")
            eval_method = eval_result.get("method", "unknown")

            # ── SOTA 回滚 ──
            # 策略: 非 SOTA 轮回滚到 SOTA 配置为基础继续演进。
            # 若尚无 SOTA（首轮未达基线），sota_config=None，不回滚，Agent 自由探索。
            actual_config = config.copy()
            # 因子保护已由 importance-based 机制替代，不再传递 SOTA 因子列表
            sota_protected_factors = None
            sota_context = None
            if evolution_history and not is_sota:
                sota_config = self._get_sota_loop_config(task_id)
                if sota_config:
                    logger.info(
                        "SOTA 回滚: 当前轮非 SOTA，回滚到 SOTA 配置为基础继续演进。"
                        "因子保护由 importance-based 机制管理。"
                    )
                    config = sota_config
                    # 更新 factor_names 以匹配回滚后的配置
                    factor_names = config.get("factor_list", [])
                    # 构建双组上下文，让 Agent 同时看到本轮和 SOTA 两组数据
                    sota_context = {
                        "current_run": {
                            "config": actual_config,
                            "metrics": metrics,
                        },
                        "sota_baseline": {
                            "config": sota_config,
                            "metrics": historical_sota_metrics or {},
                        },
                        "rollback_applied": True,
                    }

            # Phase 7: 方向控制器 — 决定 action_type
            decided_action_type = self._decide_evolution_direction(
                analyst_result, evolution_mode, metrics, is_sota=is_sota, config=config,
            )
            logger.info(f"Direction decision: {decided_action_type} (mode={evolution_mode}, sota_protected={sota_protected_factors is not None})")

            # Phase 7: 多 Agent 分发
            correlations = self._get_relevant_correlations(factor_names)
            factor_library_summary = self._get_factor_library_summary()
            researcher_context = {}
            if correlations:
                researcher_context["correlations"] = correlations
            if factor_library_summary:
                researcher_context["factor_library_summary"] = factor_library_summary
            # 传递因子黑名单给 Agent
            factor_blacklist = self._get_factor_blacklist(task_id)
            if factor_blacklist:
                researcher_context["factor_blacklist"] = list(factor_blacklist)
            # 传递 loop_index 用于 SOTA 渐进淘汰
            researcher_context["loop_index"] = loop_index
            researcher_context["max_retire_per_loop"] = 3

            if decided_action_type in ("factor_adjust", "factor_expand"):
                next_config_draft = await self.factor_agent.run(
                    analyst_result, is_sota, config,
                    evolution_history=evolution_history,
                    researcher_context=researcher_context if researcher_context else None,
                    sota_protected_factors=sota_protected_factors,
                    sota_context=sota_context,
                )
                if not isinstance(next_config_draft, dict):
                    raise ValueError(f"Factor Agent 返回无效输出: type={type(next_config_draft)}")
                if "factor_list" not in next_config_draft:
                    raise ValueError("Factor Agent 输出缺少 'factor_list' 字段")
            elif decided_action_type in ("param_tune", "model_switch"):
                next_config_draft = await self.model_agent.run(
                    analyst_result, is_sota, config,
                    evolution_history=evolution_history,
                    sota_context=sota_context,
                    task_id=task_id,
                )
                if not isinstance(next_config_draft, dict):
                    raise ValueError(f"Model Agent 返回无效输出: type={type(next_config_draft)}")
                # 模型轮保留因子
                next_config_draft["factor_list"] = config.get("factor_list", [])
            elif decided_action_type == "factor_model_joint":
                # Phase 1: Factor Agent 先决定因子组合
                factor_draft = await self.factor_agent.run(
                    analyst_result, is_sota, config,
                    evolution_history=evolution_history,
                    researcher_context=researcher_context if researcher_context else None,
                    sota_protected_factors=sota_protected_factors,
                    sota_context=sota_context,
                )
                # Phase 2: 计算因子组合特征，传递给 Model Agent
                factor_list = factor_draft.get("factor_list", config.get("factor_list", []))
                factor_context = self._compute_factor_context(factor_list)
                merged_config = {**config, "factor_list": factor_list}
                model_draft = await self.model_agent.run(
                    analyst_result, is_sota, merged_config,
                    evolution_history=evolution_history,
                    factor_context=factor_context,
                    sota_context=sota_context,
                    task_id=task_id,
                )
                next_config_draft = {
                    "factor_list": factor_list,
                    "model_id": model_draft.get("model_id", config.get("model_id")),
                    "model_params": model_draft.get("model_params", config.get("model_params", {})),
                    "rationale": f"Factor: {factor_draft.get('rationale', '')} | Model: {model_draft.get('rationale', '')}",
                }
            else:
                next_config_draft = await self.factor_agent.run(
                    analyst_result, is_sota, config,
                    evolution_history=evolution_history,
                    researcher_context=researcher_context if researcher_context else None,
                    sota_protected_factors=sota_protected_factors,
                    sota_context=sota_context,
                )

            # 注入 action_type（由分发层统一设置）
            next_config_draft["action_type"] = decided_action_type

            # 将当前 Loop 的信息补充到 evolution_history 中，
            # 使 Reviewer 能看到最新的 config 状态（当前 Loop 还处于 processing，
            # _build_full_evolution_history 查询不到它）
            # 注意：config_summary 使用 config（rollback 后的基准 config）而非 actual_config，
            # 因为 draft 的 factor_list 也来自 config。如果用 actual_config，
            # rollback 场景下 Reviewer 会误判 factor_list 发生了变化而拒绝。
            reviewer_history = dict(evolution_history) if evolution_history else {}
            rollback_applied = not is_sota and evolution_history is not None and config != actual_config
            current_loop_entry = {
                "loop_index": loop_index,
                "action_type": decided_action_type,
                "config_summary": {
                    "factors": config.get("factor_list", []),
                    "model_id": config.get("model_id"),
                    "model_params": config.get("model_params", {}),
                },
                "metrics": {k: v for k, v in metrics.items() if k != "enhanced_metrics"},
                "is_sota": is_sota,
                "rollback_applied": rollback_applied,
            }
            reviewer_history_loops = list(reviewer_history.get("loops", []))
            reviewer_history_loops.append(current_loop_entry)
            reviewer_history["loops"] = reviewer_history_loops
            reviewer_history["total_loops"] = len(reviewer_history_loops)

            next_config = await self.agents.run_reviewer(
                next_config_draft,
                evolution_history=reviewer_history,
            )

            # 确保关键基础字段不会因为 Agent 漏输出而丢失
            for key in ["model_id", "strategy_id", "data_split", "base_experiment_id"]:
                if key not in next_config and key in config:
                    next_config[key] = config[key]
            next_config = self._enforce_config_label_horizon(
                next_config,
                (task_row or {}).get("label_horizon"),
                context=f"{task_id}/Loop{loop_index}.reviewer",
            )

            agent_analysis = {
                "analyst": analyst_result.to_dict(),
                "evaluator": {
                    "is_sota": is_sota,
                    "reason": eval_reason,
                    "method": eval_method,
                    "path": eval_result.get("path", ""),
                    "sota_loop_id": sota_loop_id_ref,
                },
                "direction": {
                    "decided_action_type": decided_action_type,
                    "analyst_recommendation": analyst_result.direction,
                    "evolution_mode": evolution_mode,
                },
                "researcher": {
                    "draft": {k: v for k, v in next_config_draft.items() if k != "_optuna_trial"},
                    "action_type": decided_action_type,
                },
                "reviewer": {
                    "approved": True,
                    "validated_config": next_config,
                },
                "llm_trace": self.agents.get_trace(),
            }

            # 更新 action_type 为实际决策的方向
            action_type = decided_action_type
            experiment_custom_params = merge_qe_minute_runtime_contract(
                actual_config.get("model_params", {}),
                config=actual_config,
                source="evolution_loop_completion",
                allow_default_execution_algo=False,
            )

            # 更新 LOOP 记录
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 先写入 qe_experiments（外键目标），再更新 loop 的 experiment_id
                    experiment_id = f"{task_id}_L{loop_index}"
                    cur.execute("""
                        INSERT INTO qe_experiments
                        (experiment_id, experiment_name, qe_task_id, qe_loop_id,
                         loop_index, parent_experiment_id,
                         is_evolution_loop, factor_names, model_id, strategy_id,
                         data_split, custom_params,
                         result_metrics, status, is_sota)
                        VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s, %s, 'completed', %s)
                        ON CONFLICT (experiment_id) DO UPDATE SET
                            result_metrics = EXCLUDED.result_metrics,
                            status = EXCLUDED.status,
                            is_sota = EXCLUDED.is_sota,
                            qe_task_id = EXCLUDED.qe_task_id,
                            qe_loop_id = EXCLUDED.qe_loop_id,
                            custom_params = EXCLUDED.custom_params
                    """, (
                        experiment_id,
                        f"{task_id} Loop{loop_index}",
                        task_id,
                        loop_id,
                        loop_index,
                        task_id,
                        json.dumps(actual_config.get("factor_list", [])),
                        actual_config.get("model_id"),
                        actual_config.get("strategy_id"),
                        json.dumps(actual_config.get("data_split", {})),
                        json.dumps(experiment_custom_params),
                        json.dumps(metrics),
                        is_sota,
                    ))

                    cur.execute("""
                        UPDATE qe_evolution_loops
                        SET action_type = %s, config_json = %s, metrics_json = %s,
                            agent_analysis = %s, is_sota = %s, status = 'completed',
                            experiment_id = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (
                        action_type,
                        json.dumps(actual_config),
                        json.dumps(metrics),
                        json.dumps(agent_analysis),
                        is_sota,
                        experiment_id,
                        evolution_loop_db_id
                    ))

                    if is_sota:
                        logger.info(
                            "QE evaluator marked %s as an automatic candidate only; "
                            "formal SOTA approval requires a manual promotion_review record.",
                            evolution_loop_db_id,
                        )

                    # 写入 qe_loop_factor_records（action_role 计算）
                    self._write_loop_factor_records(
                        cur, task_id, evolution_loop_db_id, loop_index,
                        actual_config, metrics, action_type, is_sota,
                    )

                    # 写入 qe_loop_model_records
                    self._write_loop_model_records(
                        cur, task_id, evolution_loop_db_id, loop_index,
                        actual_config, metrics, action_type, is_sota,
                    )

                conn.commit()

            # Optuna 反馈：param_tune 方向时将 IC 反馈给 Optuna
            self._archive_completed_loop_best_effort(task_id, evolution_loop_db_id, loop_index)
            self._record_research_backtest_best_effort(task_id, evolution_loop_db_id, loop_index)

            if decided_action_type == "param_tune":
                _optuna_trial = next_config_draft.get("_optuna_trial") if next_config_draft else None
                if _optuna_trial is not None:
                    try:
                        from .optuna_optimizer import OptunaHyperparamOptimizer
                        ic_value = metrics.get("IC")
                        if ic_value is not None:
                            model_type = next_config_draft.get("model_type", "")
                            optimizer = OptunaHyperparamOptimizer(task_id, model_type)
                            optimizer.get_or_create_study()
                            optimizer.tell(_optuna_trial, float(ic_value))
                            logger.info(f"Optuna tell() 成功: task={task_id}, IC={ic_value}")
                    except Exception as e:
                        logger.error(f"Optuna tell() 失败: {e}, 不影响演进流程")

            # 更新总进度
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE qe_evolution_tasks SET current_loop = %s, updated_at = NOW() WHERE task_id = %s", (loop_index, task_id))
                conn.commit()

            # 判断是否还有剩余 Loop → 提交下一轮
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT current_loop, max_loops, status FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                    task = cur.fetchone()

            if task and task['status'] == 'running' and task['current_loop'] < task['max_loops']:
                _next_task = asyncio.create_task(self.submit_next_loop(task_id))
                _next_task.add_done_callback(
                    lambda t: logger.error(f"submit_next_loop failed: {t.exception()}") if t.exception() else None
                )
            elif task and task['current_loop'] >= task['max_loops']:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'completed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
                logger.info(f"Task {task_id} completed successfully.")

            return True

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"Error processing completed loop {evolution_loop_db_id} for task {task_id}: {e}\n{tb_str}")
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # 将完整错误信息写入 agent_analysis 以便调试（保留已收集的 LLM trace）
                    error_detail = json.dumps({"_error": str(e), "_traceback": tb_str, "llm_trace": self.agents.get_trace()}, ensure_ascii=False)
                    cur.execute("UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s", (error_detail, evolution_loop_db_id))
                    # custom_evo/strategy_evo：单个 loop 失败不标记整个 task，只有所有 loop 都失败才标记
                    cur.execute("SELECT task_type FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                    task_row = cur.fetchone()
                    if not task_row or task_row.get("task_type") not in ("strategy_evo", "custom_evo"):
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    else:
                        cur.execute("SELECT COUNT(*) FROM qe_evolution_loops WHERE task_id = %s AND status != 'failed'", (task_id,))
                        non_failed = cur.fetchone()
                        if non_failed and non_failed[0] == 0:
                            cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                conn.commit()
            return False

    async def scan_running_loops(self):
        """
        定时扫描兜底：
        1. 查询 status='running' 的 loops，检查 RDAgent 侧状态
        2. F4: 检测卡在 processing 超过 30 分钟的 loop
        3. F5: 检测 running 的 task 但没有任何活跃 loop（僵尸 task）
        """
        try:
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # 原有：扫描 running 状态的 loop
                    cur.execute("""
                        SELECT l.loop_id, l.task_id, l.loop_index
                        FROM qe_evolution_loops l
                        JOIN qe_evolution_tasks t ON l.task_id = t.task_id
                        WHERE l.status = 'running' AND t.status = 'running'
                    """)
                    running_loops = cur.fetchall()

                    # F4: 检测卡在 processing 超过 30 分钟的 loop
                    cur.execute("""
                        SELECT loop_id, task_id FROM qe_evolution_loops
                        WHERE status = 'processing'
                          AND updated_at < NOW() - INTERVAL '30 minutes'
                    """)
                    stuck_processing = cur.fetchall()

                    # F5: 检测僵尸 task（running 但无活跃 loop）
                    # 排除最近10分钟内有 failed loop 的 task，防止无限重试循环
                    cur.execute("""
                        SELECT t.task_id FROM qe_evolution_tasks t
                        WHERE t.status = 'running'
                          AND NOT EXISTS (
                              SELECT 1 FROM qe_evolution_loops l
                              WHERE l.task_id = t.task_id AND l.status IN ('running', 'processing')
                          )
                          AND t.updated_at < NOW() - INTERVAL '5 minutes'
                          AND NOT EXISTS (
                              SELECT 1 FROM qe_evolution_loops l2
                              WHERE l2.task_id = t.task_id
                                AND l2.status = 'failed'
                                AND l2.updated_at > NOW() - INTERVAL '10 minutes'
                          )
                    """)
                    zombie_tasks = cur.fetchall()

            # F4: 处理 processing 超时
            for row in stuck_processing:
                logger.error(f"Loop {row['loop_id']} stuck in processing for >30min, marking as failed")
                try:
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s AND status = 'processing'", (row['loop_id'],))
                        conn.commit()
                except Exception as e:
                    raise RuntimeError(f"Failed to mark stuck loop {row['loop_id']} as failed: {e}") from e

            # F5: 处理僵尸 task — 尝试提交下一轮
            for row in zombie_tasks:
                logger.warning(f"Zombie task detected: {row['task_id']} is running but has no active loops, attempting recovery")
                asyncio.create_task(self._safe_submit_or_fail(row['task_id']))

            # 原有逻辑：检查 running 的 loop 在 RDAgent 侧的状态
            for loop_row in running_loops:
                task_id = loop_row['task_id']
                loop_index = loop_row['loop_index']
                loop_id = f"Loop{loop_index}"
                evolution_loop_db_id = loop_row['loop_id']

                try:
                    client = self._get_workspace_client_for_loop(task_id, evolution_loop_db_id)
                    status_resp = await client.get_loop_status(task_id, loop_id)
                    rd_status = status_resp.get("status")

                    if rd_status == "completed":
                        logger.info(f"Timer scan: Loop {evolution_loop_db_id} completed on RDAgent side, processing.")
                        asyncio.create_task(self._safe_process_completed_loop(task_id, evolution_loop_db_id))
                    elif rd_status in ("failed", "error"):
                        logger.warning(f"Timer scan: Loop {evolution_loop_db_id} failed on RDAgent side.")
                        with get_conn() as conn:
                            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                                cur.execute("UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s", (evolution_loop_db_id,))
                                # 策略演进任务：单个 loop 失败不标记整个 task 为 failed（其他 loop 可能还在跑）
                                cur.execute("SELECT task_type FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                                task_row = cur.fetchone()
                                if not task_row or task_row.get("task_type") not in ("strategy_evo", "custom_evo"):
                                    cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                            conn.commit()
                except Exception as e:
                    # 单个 loop 检查失败（超时/网络等）不应中断整个扫描
                    logger.warning(f"Timer scan: Failed to check loop {evolution_loop_db_id}: {e}")

        except Exception as e:
            logger.error(f"Timer scan error: {e}", exc_info=True)
            raise  # 让调用方（定时器框架）感知调度异常，避免静默失效

    async def _safe_submit_or_fail(self, task_id: str):
        """F5: 尝试为僵尸 task 提交下一轮，失败则标记 task 为 failed。"""
        try:
            result = await self.submit_next_loop(task_id)
            if result:
                logger.info(f"Zombie task {task_id} recovered: submitted {result}")
            else:
                logger.info(f"Zombie task {task_id}: submit_next_loop returned None (completed or stopped)")
        except Exception as e:
            logger.error(f"Zombie task {task_id} recovery failed, marking as failed: {e}")
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"FATAL: Failed to mark zombie task {task_id} as failed: {db_err}")
                raise RuntimeError(f"Failed to mark zombie task {task_id} as failed: {db_err}") from db_err

    _LLM_TRANSIENT_KEYWORDS = ("disconnected", "timeout", "rate_limit", "503", "502", "429", "connection", "reset by peer")

    async def _safe_process_completed_loop(self, task_id: str, loop_id: str):
        """process_completed_loop with retry for transient LLM errors (for use with asyncio.create_task)."""
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                await self.process_completed_loop(task_id, loop_id)
                return  # 成功
            except Exception as e:
                err_lower = str(e).lower()
                is_transient = any(kw in err_lower for kw in self._LLM_TRANSIENT_KEYWORDS)
                if is_transient and attempt < max_retries:
                    wait_sec = 30 * attempt
                    logger.warning(f"Transient LLM error in process_completed_loop({task_id}, {loop_id}), "
                                   f"attempt {attempt}/{max_retries}, retrying in {wait_sec}s: {e}")
                    await asyncio.sleep(wait_sec)
                    continue
                # 非瞬态错误 或 重试耗尽 → 标记失败
                logger.error(f"Error in process_completed_loop({task_id}, {loop_id}) "
                             f"(attempt {attempt}/{max_retries}, transient={is_transient}): {e}", exc_info=True)
                try:
                    with get_conn() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s", (loop_id,))
                            # custom_evo/strategy_evo：单个 loop 失败不标记整个 task，只有所有 loop 都失败才标记
                            cur.execute("SELECT task_type FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                            task_row = cur.fetchone()
                            if not task_row or task_row.get("task_type") not in ("strategy_evo", "custom_evo"):
                                cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                            else:
                                cur.execute("SELECT COUNT(*) FROM qe_evolution_loops WHERE task_id = %s AND status != 'failed'", (task_id,))
                                non_failed = cur.fetchone()
                                if non_failed and non_failed[0] == 0:
                                    cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                        conn.commit()
                    logger.error(f"Marked loop {loop_id} as failed due to process_completed_loop error")
                except Exception as db_err:
                    logger.critical(f"FATAL: Failed to mark loop/task as failed after process_completed_loop error: {db_err}")
                    raise RuntimeError(f"Failed to mark loop/task as failed: {db_err}") from db_err
                return

    async def get_all_tasks(self, detail: str = "summary") -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if detail == "full":
                    cur.execute("SELECT * FROM qe_evolution_tasks ORDER BY created_at DESC")
                else:
                    cur.execute("""
                        SELECT task_id, task_name, target_desc, max_loops, current_loop,
                               status, base_experiment_id, node_id, label_horizon,
                               task_type, source_type, strategy_id, execution_algo,
                               strategy_evo_execution_mode, created_at, updated_at
                        FROM qe_evolution_tasks
                        ORDER BY created_at DESC
                    """)
                rows = [dict(row) for row in cur.fetchall()]
        return rows if detail == "full" else [compact_task_row(row) for row in rows]

    async def resume_task(self, task_id: str, additional_loops: int = 0, force_full_train: bool = False) -> dict:
        """
        恢复已暂停/已完成/已失败的演进任务，继续从上次的 current_loop 开始。
        如果 additional_loops > 0，则增加 max_loops。
        force_full_train=True 时，忽略各 loop 的 backtest_only，强制完整训练。
        Returns: {"task_id": str, "task_type": str, "force_full_train": bool}
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            raise ValueError(f"演进任务不存在: {task_id}")

        if task['status'] == 'running':
            raise ValueError(f"演进任务正在运行中: {task_id}")

        task_type = task.get('task_type', 'evolution')
        if force_full_train and task_type == "custom_evo":
            strategy_evo_config = task.get("strategy_evo_config") or {}
            if isinstance(strategy_evo_config, str):
                strategy_evo_config = json.loads(strategy_evo_config)
            backtest_only_loops = [
                loop.get("loop_index")
                for loop in strategy_evo_config.get("loops", [])
                if loop.get("backtest_only")
            ]
            if backtest_only_loops:
                raise ValueError(
                    "force_full_train=True would override backtest_only loop config "
                    f"for custom_evo loops {backtest_only_loops}; refusing to silently "
                    "change the UI-defined comparison. Resume with force_full_train=false."
                )

        new_max = task['max_loops']
        if additional_loops > 0:
            new_max = task['current_loop'] + additional_loops

        # 策略演进/自定义演进恢复：将失败的 loop 状态重置为 pending
        if task_type in ('strategy_evo', 'custom_evo'):
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops SET status = 'pending', updated_at = NOW() WHERE task_id = %s AND status IN ('failed', 'cancelled')",
                        (task_id,),
                    )
                    reset_count = cur.rowcount
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET status = 'pending', max_loops = %s, updated_at = NOW() WHERE task_id = %s",
                        (new_max, task_id),
                    )
                conn.commit()
            logger.info(f"Resumed {task_type} task {task_id}, reset {reset_count} failed loops, max_loops={new_max}")
        else:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET status = 'pending', max_loops = %s, updated_at = NOW() WHERE task_id = %s",
                        (new_max, task_id),
                    )
                conn.commit()
            logger.info(f"Resumed evolution task {task_id}, max_loops={new_max}")

        return {"task_id": task_id, "task_type": task_type, "force_full_train": force_full_train}

    async def fork_task(
        self,
        source_task_id: str,
        from_loop_index: int,
        task_name: Optional[str] = None,
        max_loops: int = 10,
        evolution_guidance: Optional[str] = None,
        evolution_mode: str = "auto",
        inherit_history: bool = False,
        strategy_id: Optional[str] = None,
        strategy_params: Optional[Dict[str, Any]] = None,
        execution_algo: Optional[str] = None,
        execution_algo_params: Optional[Dict[str, Any]] = None,
        unfilled_handler: Optional[str] = None,
        unfilled_handler_params: Optional[Dict[str, Any]] = None,
        node_id: Optional[str] = None,
        label_horizon: Optional[int] = None,
        random_seed: Optional[int] = None,
    ) -> str:
        """
        从指定 task 的某个已完成 loop 分叉出新的演进任务。
        以该 loop 的因子+模型配置为基础，创建新 task 做全新演进。
        """
        # 1. 验证源 task 和 loop
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (source_task_id,))
                source_task = cur.fetchone()
                if not source_task:
                    raise ValueError(f"源任务不存在: {source_task_id}")

                cur.execute(
                    "SELECT * FROM qe_evolution_loops WHERE task_id = %s AND loop_index = %s AND status = 'completed'",
                    (source_task_id, from_loop_index),
                )
                source_loop = cur.fetchone()
                if not source_loop:
                    raise ValueError(
                        f"源任务 {source_task_id} 中不存在已完成的 Loop {from_loop_index}"
                    )

        # 2. 读取源 loop 的配置
        config = source_loop.get("config_json") or {}
        if isinstance(config, str):
            config = json.loads(config)

        metrics = source_loop.get("metrics_json") or {}
        if isinstance(metrics, str):
            metrics = json.loads(metrics)

        factor_list = config.get("factor_list", [])
        model_id = config.get("model_id")
        # 继承源任务的 strategy/execution_algo，入参非 None 时覆盖
        effective_strategy_id = strategy_id if strategy_id is not None else source_task.get("strategy_id")
        effective_strategy_params = strategy_params if strategy_params is not None else (source_task.get("strategy_params") or {})
        effective_execution_algo = execution_algo if execution_algo is not None else source_task.get("execution_algo")
        effective_execution_algo_params = execution_algo_params if execution_algo_params is not None else (source_task.get("execution_algo_params") or {})
        effective_unfilled_handler = unfilled_handler if unfilled_handler is not None else source_task.get("unfilled_handler")
        effective_unfilled_handler_params = unfilled_handler_params if unfilled_handler_params is not None else (source_task.get("unfilled_handler_params") or {})
        if isinstance(effective_strategy_params, str):
            effective_strategy_params = json.loads(effective_strategy_params)
        if isinstance(effective_execution_algo_params, str):
            effective_execution_algo_params = json.loads(effective_execution_algo_params)
        effective_strategy_params = dict(effective_strategy_params or {})
        seed_source = (
            random_seed
            if random_seed not in (None, "")
            else effective_strategy_params.get("random_seed")
            or config.get("random_seed")
            or (config.get("runtime_flags") or {}).get("random_seed")
            or (config.get("execution_manifest") or {}).get("random_seed")
        )
        effective_random_seed = normalize_qe_random_seed(
            seed_source,
            field_name="fork_task.random_seed",
        )
        effective_strategy_params["random_seed"] = effective_random_seed
        strategy_id = effective_strategy_id
        data_split = config.get("data_split", {})
        model_params = config.get("model_params", {})
        source_label_horizon = self._extract_label_horizon_from_config(
            config,
            context=f"fork_source[{source_task_id}/Loop{from_loop_index}].config_json",
        )
        effective_label_horizon = normalize_label_horizon(
            label_horizon if label_horizon not in (None, "") else source_label_horizon,
            field_name="fork_task.label_horizon",
        )
        if label_horizon not in (None, ""):
            # Full-train fork is allowed to change the training target; it will
            # retrain instead of reusing the source model.
            model_params = self._parse_json_field(model_params, "fork_task.model_params")
            model_params.pop("label_horizon", None)
        model_params = self._apply_label_horizon_to_model_params(
            model_params,
            effective_label_horizon,
        )
        model_params = merge_qe_minute_runtime_contract(
            model_params,
            config=config,
            execution_algo=effective_execution_algo,
            execution_algo_params=effective_execution_algo_params,
            source="fork_base_experiment",
            allow_default_execution_algo=True,
        )

        if not factor_list:
            raise ValueError(f"源 Loop {from_loop_index} 的因子列表为空，无法分叉")
        # model_id / strategy_id 允许为空 — ConfigComposer 会用默认值补全

        # 3. 生成新 task_id 和名称（微秒精度 + 4位随机后缀防并发冲突）
        import uuid
        from datetime import datetime
        suffix = uuid.uuid4().hex[:4]
        new_task_id = f"qe_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{suffix}"
        if not task_name:
            source_name = source_task.get("task_name", source_task_id)
            task_name = f"{source_name}_from_L{from_loop_index}"
        target_desc = source_task.get("target_desc", "")

        # 4+5. 在同一事务中创建 base experiment + evolution task（防止孤儿记录）
        base_exp_id = f"{new_task_id}_base"
        # node_id: 入参优先，否则继承源任务
        effective_node_id = node_id if node_id is not None else source_task.get("node_id")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_experiments
                    (experiment_id, experiment_name, parent_experiment_id,
                     factor_names, model_id, strategy_id, data_split, custom_params,
                     result_metrics, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'created')
                """, (
                    base_exp_id,
                    f"Fork base from {source_task_id} L{from_loop_index}",
                    None,
                    json.dumps(factor_list),
                    model_id,
                    strategy_id,
                    json.dumps(data_split) if isinstance(data_split, dict) else data_split,
                    json.dumps(model_params) if isinstance(model_params, dict) else model_params,
                    json.dumps(metrics),
                ))
                cur.execute("""
                    INSERT INTO qe_evolution_tasks
                    (task_id, task_name, target_desc, max_loops, current_loop, status,
                     base_experiment_id, node_id, source_type,
                     evolution_guidance, evolution_mode,
                     fork_from_task_id, fork_from_loop_index, inherit_history,
                     strategy_id, strategy_params, execution_algo, execution_algo_params,
                     unfilled_handler, unfilled_handler_params, label_horizon)
                    VALUES (%s, %s, %s, %s, 0, 'pending', %s, %s, 'fork', %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s)
                """, (
                    new_task_id, task_name, target_desc, max_loops,
                    base_exp_id, effective_node_id,
                    evolution_guidance, evolution_mode,
                    source_task_id, from_loop_index, inherit_history,
                    effective_strategy_id,
                    json.dumps(effective_strategy_params) if effective_strategy_params else None,
                    effective_execution_algo,
                    json.dumps(effective_execution_algo_params) if effective_execution_algo_params else None,
                    effective_unfilled_handler,
                    json.dumps(effective_unfilled_handler_params) if effective_unfilled_handler_params else None,
                    effective_label_horizon,
                ))
            conn.commit()

        logger.info(
            f"Forked new task {new_task_id} from {source_task_id} Loop {from_loop_index}, "
            f"max_loops={max_loops}, inherit_history={inherit_history}"
        )
        return new_task_id

    async def get_task_detail(self, task_id: str, detail: str = "summary") -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if detail == "full":
                    cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                else:
                    cur.execute("""
                        SELECT task_id, task_name, target_desc, max_loops, current_loop,
                               status, base_experiment_id, node_id, label_horizon,
                               task_type, source_type, strategy_id, strategy_params,
                               execution_algo, execution_algo_params, unfilled_handler,
                               unfilled_handler_params, strategy_evo_execution_mode,
                               created_at, updated_at
                        FROM qe_evolution_tasks WHERE task_id = %s
                    """, (task_id,))
                task = cur.fetchone()
                if not task:
                    return None

                if detail == "full":
                    cur.execute("SELECT * FROM qe_evolution_loops WHERE task_id = %s ORDER BY loop_index ASC", (task_id,))
                else:
                    cur.execute("""
                        SELECT loop_id, task_id, loop_index, action_type,
                               config_json->'factor_list' AS factor_list,
                               config_json->'factor_names' AS factor_names,
                               config_json->>'model_id' AS model_id,
                               config_json->>'strategy_id' AS strategy_id,
                               config_json->>'label_horizon' AS label_horizon,
                               config_json->>'execution_algo' AS execution_algo,
                               metrics_json->>'IC' AS ic,
                               metrics_json->>'ICIR' AS icir,
                               COALESCE(metrics_json->>'Rank_IC', metrics_json->>'Rank IC') AS rank_ic,
                               COALESCE(metrics_json->>'Rank_ICIR', metrics_json->>'Rank ICIR') AS rank_icir,
                               COALESCE(
                                   metrics_json->>'annualized_return',
                                   metrics_json->>'excess_return_with_cost_annualized',
                                   metrics_json#>>'{summary,annualized_return}'
                               ) AS annualized_return,
                               COALESCE(
                                   metrics_json->>'max_drawdown',
                                   metrics_json->>'excess_return_with_cost_max_drawdown',
                                   metrics_json#>>'{summary,max_drawdown}'
                               ) AS max_drawdown,
                               COALESCE(
                                   metrics_json->>'information_ratio',
                                   metrics_json->>'sharpe',
                                   metrics_json->>'excess_return_with_cost_IR',
                                   metrics_json#>>'{summary,information_ratio}'
                               ) AS information_ratio,
                               is_sota, status,
                               node_id, experiment_id, created_at, updated_at
                        FROM qe_evolution_loops
                        WHERE task_id = %s
                        ORDER BY loop_index ASC
                    """, (task_id,))
                loops = cur.fetchall()

                result = dict(task)
                result['loops'] = [dict(loop_row) for loop_row in loops]

        # Live status 检查：对 running 状态的 loop 查询 RDAgent 侧真实状态
        # 对于 custom_evo/strategy_evo 并行调度任务，不能直接修改 loop status（会破坏
        # submit_custom_evo_all_loops 的 run_with_sem 调度循环），改为触发完整的
        # process_completed_loop 流程。
        task_type = result.get('task_type')
        any_synced = False
        for loop_data in result['loops']:
            if loop_data.get('status') not in ('running', 'processing'):
                continue
            loop_id = loop_data['loop_id']
            loop_index = loop_data['loop_index']
            try:
                client = self._get_workspace_client_for_loop(task_id, loop_id)
                live = await client.get_loop_status(task_id, f"Loop{loop_index}")
                rd_status = live.get("status")
            except Exception as e:
                logger.warning(f"[get_task_detail] live status check failed for {loop_id}: {e}")
                loop_data["live_status_error"] = str(e)
                continue

            if task_type in ("custom_evo", "strategy_evo") and rd_status == "not_found":
                logger.info(
                    "[get_task_detail] Loop %s not visible on RD-Agent yet; "
                    "keeping DB status=%s instead of treating it as terminal",
                    loop_id,
                    loop_data.get("status"),
                )
                continue

            if rd_status in ("completed", "failed", "error", "not_found"):
                if task_type in ("custom_evo", "strategy_evo"):
                    if rd_status == "completed":
                        try:
                            logger.info(f"[get_task_detail] processing completed loop: {loop_id}")
                            await self._safe_process_completed_loop(task_id, loop_id)
                            with get_conn() as conn:
                                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                                    cur.execute("SELECT * FROM qe_evolution_loops WHERE loop_id = %s", (loop_id,))
                                    updated = cur.fetchone()
                                    if updated:
                                        for i, lp in enumerate(result['loops']):
                                            if lp.get('loop_id') == loop_id:
                                                result['loops'][i] = dict(updated)
                                                break
                            any_synced = True
                        except Exception as e:
                            logger.error(f"[get_task_detail] completed-loop processing failed for {loop_id}: {e}")
                    else:
                        new_status = "failed" if rd_status in ("failed", "error", "not_found") else rd_status
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE qe_evolution_loops SET status = %s, updated_at = NOW() "
                                    "WHERE loop_id = %s AND status IN ('running', 'processing')",
                                    (new_status, loop_id),
                                )
                            conn.commit()
                        loop_data["status"] = new_status
                        any_synced = True
                        logger.info(f"[get_task_detail] synced loop {loop_id}: rd_status={rd_status} -> {new_status}")
                else:
                    # 标准演进任务：保留原有快速同步逻辑
                    new_status = "failed" if rd_status in ("failed", "not_found") else "completed"
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE qe_evolution_loops SET status = %s, updated_at = NOW() WHERE loop_id = %s AND status IN ('running', 'processing')",
                                (new_status, loop_id),
                            )
                        conn.commit()
                    loop_data['status'] = new_status
                    any_synced = True
                    logger.info(f"[get_task_detail] auto-synced loop {loop_id}: {rd_status} -> {new_status}")

        # 仅对标准演进任务（非 custom_evo/strategy_evo）自动更新 task 状态。
        # custom_evo/strategy_evo 的 task 状态由 process_strategy_evo_completed_loop 或
        # submit_custom_evo_all_loops 的 final status check 管理。
        if any_synced and task_type not in ("custom_evo", "strategy_evo"):
            all_terminal = all(
                lp.get('status') in ('completed', 'failed', 'cancelled')
                for lp in result['loops']
            )
            if all_terminal and result.get('status') == 'running':
                has_failed = any(lp.get('status') == 'failed' for lp in result['loops'])
                new_task_status = 'failed' if has_failed else 'completed'
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE qe_evolution_tasks SET status = %s, updated_at = NOW() WHERE task_id = %s AND status = 'running'",
                            (new_task_status, task_id),
                        )
                    conn.commit()
                result['status'] = new_task_status
                logger.info(f"[get_task_detail] auto-synced task {task_id} -> {new_task_status}")

        if detail == "full":
            try:
                from .blacklist_snapshot import enrich_blacklist_snapshot_for_display
                for loop_data in result.get("loops", []):
                    config_json = loop_data.get("config_json")
                    if not isinstance(config_json, dict):
                        continue
                    model_params = config_json.get("model_params")
                    if isinstance(model_params, dict):
                        config_json["model_params"] = enrich_blacklist_snapshot_for_display(model_params)
                    else:
                        config_json["model_params"] = enrich_blacklist_snapshot_for_display(config_json)
            except Exception as e:
                raise RuntimeError(f"演进 Loop 行业黑名单快照解析失败: {e}") from e
        else:
            result["loops"] = [compact_loop_row(loop_data) for loop_data in result.get("loops", [])]
            result["detail"] = "summary"

        return result

    def get_loop_comparison(self, task_id: str) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT task_id, task_name, status, max_loops, current_loop, created_at, updated_at
                    FROM qe_evolution_tasks
                    WHERE task_id = %s
                """, (task_id,))
                task = cur.fetchone()
                if not task:
                    return None
                cur.execute("""
                    SELECT loop_id, task_id, loop_index, action_type,
                           config_json->'factor_list' AS factor_list,
                           config_json->'factor_names' AS factor_names,
                           config_json->>'model_id' AS model_id,
                           config_json->>'strategy_id' AS strategy_id,
                           config_json->>'label_horizon' AS label_horizon,
                           config_json->>'execution_algo' AS execution_algo,
                           metrics_json->>'IC' AS ic,
                           metrics_json->>'ICIR' AS icir,
                           COALESCE(metrics_json->>'Rank_IC', metrics_json->>'Rank IC') AS rank_ic,
                           COALESCE(metrics_json->>'Rank_ICIR', metrics_json->>'Rank ICIR') AS rank_icir,
                           COALESCE(
                               metrics_json->>'annualized_return',
                               metrics_json->>'excess_return_with_cost_annualized',
                               metrics_json#>>'{summary,annualized_return}'
                           ) AS annualized_return,
                           COALESCE(
                               metrics_json->>'max_drawdown',
                               metrics_json->>'excess_return_with_cost_max_drawdown',
                               metrics_json#>>'{summary,max_drawdown}'
                           ) AS max_drawdown,
                           COALESCE(
                               metrics_json->>'information_ratio',
                               metrics_json->>'sharpe',
                               metrics_json->>'excess_return_with_cost_IR',
                               metrics_json#>>'{summary,information_ratio}'
                           ) AS information_ratio,
                           is_sota, status,
                           node_id, experiment_id, created_at, updated_at
                    FROM qe_evolution_loops
                    WHERE task_id = %s
                    ORDER BY loop_index ASC
                """, (task_id,))
                loops = [compact_loop_row(dict(row)) for row in cur.fetchall()]
        result = compact_task_row(dict(task))
        result["loops"] = loops
        result["detail"] = "comparison"
        return result

    def get_loop_payload(self, task_id: str, loop_index: int, payload: str) -> Optional[Dict[str, Any]]:
        if payload not in {"config", "metrics", "analysis"}:
            raise ValueError(f"Unsupported loop payload: {payload}")
        projection = {
            "config": "config_json",
            "metrics": "metrics_json",
            "analysis": "agent_analysis",
        }[payload]
        sql = (
            "SELECT loop_id, task_id, loop_index, status, action_type, "
            + projection
            + " FROM qe_evolution_loops WHERE task_id = %s AND loop_index = %s"
        )
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, (task_id, loop_index))
                row = cur.fetchone()
        if not row:
            return None
        result = dict(row)
        result["payload_type"] = payload
        return result
        
    async def stop_task(self, task_id: str) -> dict:
        """Stop a QE evolution task and all non-terminal loops.

        The stop action is task-scoped: it pauses the task first so background
        submit loops stop, marks every pending/running/processing loop as
        cancelled, and then asks RD-Agent to kill every non-completed loop
        because DB state can lag behind workspace process state.  It never
        stops only the latest loop.
        """
        result = {
            "task_id": task_id,
            "paused": False,
            "loops_killed": [],
            "loops_cancelled": [],
            # Backward-compatible alias; callers should prefer loops_killed.
            "loop_killed": None,
        }

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT task_id, node_id FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task_row = cur.fetchone()
                if not task_row:
                    raise ValueError(f"Task not found: {task_id}")
                cur.execute(
                    "SELECT loop_id, loop_index, status, node_id FROM qe_evolution_loops "
                    "WHERE task_id = %s AND status <> 'completed' "
                    "ORDER BY loop_index ASC",
                    (task_id,),
                )
                loops_to_stop = [dict(row) for row in cur.fetchall()]

                # Pause first.  Long-running submit loops check task status and
                # will stop before submitting any later loops.
                cur.execute(
                    "UPDATE qe_evolution_tasks SET status = 'paused', updated_at = NOW() WHERE task_id = %s",
                    (task_id,),
                )
                cur.execute(
                    "UPDATE qe_evolution_loops SET status = 'cancelled', updated_at = NOW() "
                    "WHERE task_id = %s AND status IN ('running', 'processing', 'pending') "
                    "RETURNING loop_id, loop_index",
                    (task_id,),
                )
                result["loops_cancelled"] = [dict(row) for row in cur.fetchall()]
            conn.commit()
        result["paused"] = True
        logger.info(
            "Task %s stopped: cancelled %d non-terminal loops",
            task_id,
            len(result["loops_cancelled"]),
        )

        for loop_row in loops_to_stop:
            loop_index = loop_row["loop_index"]
            loop_id = f"Loop{loop_index}"
            loop_db_id = loop_row["loop_id"]
            loop_node_id = loop_row.get("node_id") or task_row.get("node_id") or resolve_default_qe_node_id()
            kill_success = False
            kill_error = None
            kill_result = None
            try:
                client = self._get_workspace_client_for_node_id(loop_node_id)
                kill_result = await client.kill_loop(task_id, loop_id)
                kill_success = bool(kill_result.get("killed", False))
                kill_error = kill_result.get("error")
                if kill_error:
                    logger.warning("Kill loop %s returned error: %s", loop_db_id, kill_error)
                else:
                    logger.info("Kill loop %s result: %s", loop_db_id, kill_result)
            except Exception as e:
                kill_error = str(e)
                if "404" in kill_error or "No pid.txt" in kill_error:
                    kill_error = None
                    kill_result = {"status": "no_process", "detail": "pid file not found"}
                    logger.info("Loop %s has no remote pid; treated as already stopped", loop_db_id)
                else:
                    logger.error("Failed to kill loop process for %s: %s", loop_db_id, e)

            item = {
                "loop_id": loop_db_id,
                "loop_index": loop_index,
                "previous_status": loop_row.get("status"),
                "process_killed": kill_success,
                "db_cancelled": any(
                    row.get("loop_id") == loop_db_id for row in result["loops_cancelled"]
                ),
                "error": kill_error,
                "kill_result": kill_result,
            }
            result["loops_killed"].append(item)
            if result["loop_killed"] is None:
                result["loop_killed"] = item

        return result

    async def stop_multi_alpha_experiment(self, experiment_id: str) -> dict:
        """停止多Alpha实验：终止所有节点正在执行的训练/回测。

        遍历 qe_multi_alpha_groups 中所有 running 状态的组，
        逐节点调用 kill_loop 终止进程，更新状态为 cancelled。
        """
        from .qe_workspace_client import QEWorkspaceClient

        result = {
            "experiment_id": experiment_id,
            "groups_stopped": [],
            "groups_failed": [],
        }

        # 1. 获取所有 running 状态的组
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """SELECT group_name, assigned_node_id, qe_loop_id
                       FROM qe_multi_alpha_groups
                       WHERE parent_experiment_id = %s
                         AND status = 'running'""",
                    (experiment_id,),
                )
                running_groups = cur.fetchall()

        if not running_groups:
            # 没有 running 的组，直接更新实验状态
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_experiments SET status = 'cancelled', completed_at = NOW() "
                        "WHERE experiment_id = %s",
                        (experiment_id,),
                    )
                conn.commit()
            result["message"] = "no running groups found, experiment marked cancelled"
            return result

        # 2. 获取实验的 qe_task_id（从 qe_experiments 表获取，组表不存此字段）
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT qe_task_id FROM qe_experiments WHERE experiment_id = %s",
                    (experiment_id,),
                )
                row = cur.fetchone()
        qe_task_id = row[0] if row else None
        if not qe_task_id:
            raise ValueError(f"实验 {experiment_id} 缺少 qe_task_id，无法终止节点进程")

        # 3. 逐组终止
        for g in running_groups:
            g_name = g["group_name"]
            node_id = g["assigned_node_id"]
            loop_id = g["qe_loop_id"]

            if not node_id or not loop_id:
                result["groups_failed"].append({
                    "group_name": g_name,
                    "error": f"missing node_id/loop_id: {node_id}/{loop_id}",
                })
                continue

            try:
                client = QEWorkspaceClient.for_node(node_id)
                async with client:
                    kill_result = await client.kill_loop(qe_task_id, loop_id)
                    kill_success = kill_result.get("killed", False)
                    logger.info(f"Kill group {g_name} @ {node_id}: {kill_result}")
                    result["groups_stopped"].append({
                        "group_name": g_name,
                        "node_id": node_id,
                        "killed": kill_success,
                    })
            except Exception as e:
                logger.error(f"Failed to kill group {g_name} @ {node_id}: {e}")
                result["groups_failed"].append({
                    "group_name": g_name,
                    "node_id": node_id,
                    "error": str(e),
                })

        # 4. 更新所有组状态为 cancelled
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE qe_multi_alpha_groups
                       SET status = 'cancelled'
                       WHERE parent_experiment_id = %s AND status = 'running'""",
                    (experiment_id,),
                )
                # 更新实验状态
                cur.execute(
                    "UPDATE qe_experiments SET status = 'cancelled', completed_at = NOW() "
                    "WHERE experiment_id = %s",
                    (experiment_id,),
                )
            conn.commit()

        # 如果所有组都 kill 失败，抛异常（不静默）
        if result["groups_failed"] and not result["groups_stopped"]:
            raise RuntimeError(
                f"多Alpha实验 {experiment_id} 停止失败: 所有 {len(result['groups_failed'])} 组终止失败. "
                f"详情: {result['groups_failed']}"
            )

        logger.info(
            f"多Alpha实验 {experiment_id} 已停止: "
            f"{len(result['groups_stopped'])} 组成功, {len(result['groups_failed'])} 组失败"
        )
        return result

    async def retry_loop(
        self,
        task_id: str,
        loop_index: int,
        retry_mode: str = QE_LOOP_RETRY_MODE_AUTO,
    ) -> Dict[str, Any]:
        """重试失败的 Loop：自动判断训练是否已完成，决定从训练或回测恢复.

        判断逻辑：
        - workspace 中 mlruns 有 params.pkl → 训练完成，使用 --backtest-only
        - 无 params.pkl → 训练未完成，全量重跑

        Returns: {"loop_id": str, "mode": "backtest_only"|"full_train"}
        """
        requested_retry_mode = normalize_qe_loop_retry_mode(retry_mode)

        from .config_composer import (
            PRECOMPUTED_HMM_COEFF_JSON_PARAM,
            ConfigComposer,
        )

        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        loop_id = f"Loop{loop_index}"
        # 1. 验证 loop 存在且状态为 failed
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT loop_id, status, config_json, node_id FROM qe_evolution_loops WHERE loop_id = %s",
                    (evolution_loop_db_id,),
                )
                loop_row = cur.fetchone()

        if not loop_row:
            raise ValueError(f"Loop {evolution_loop_db_id} 不存在")
        if loop_row["status"] not in ("failed", "cancelled"):
            raise ValueError(
                f"Loop {evolution_loop_db_id} 状态为 '{loop_row['status']}'，"
                f"只有 failed 或 cancelled 状态的 loop 可以重试"
            )

        # 2. 获取 task 信息（retry 需要 task 元数据，但不再因 task running 而拒绝）
        # parallel custom_evo 任务中，一个 loop 失败时其他 loop 可能仍在运行，
        # 此时 task 状态为 running 是正常的，不应阻塞失败 loop 的重试。
        # loop 级别的状态校验已在步骤 1 完成（只允许 failed/cancelled）。
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        effective_node_id = loop_row.get("node_id") or task.get("node_id") or resolve_default_qe_node_id()
        if not loop_row.get("node_id"):
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops SET node_id = %s, updated_at = NOW() WHERE loop_id = %s",
                        (effective_node_id, evolution_loop_db_id),
                    )
                conn.commit()
        await preflight_qe_node(effective_node_id)

        client = self._get_workspace_client_for_node_id(effective_node_id)
        config = loop_row["config_json"]
        if isinstance(config, str):
            config = json.loads(config)
        original_backtest_only = bool(isinstance(config, dict) and config.get("backtest_only"))

        # 3. Resolve retry mode. UI/API callers may force full training or
        # backtest-only; auto preserves the old artifact-based behavior.
        retry_mode_name = requested_retry_mode
        retry_model_source = None
        retry_extra_experiment_files = None
        if requested_retry_mode in (
            QE_LOOP_RETRY_MODE_AUTO,
            QE_LOOP_RETRY_MODE_BACKTEST_ONLY,
        ):
            if original_backtest_only:
                await self._require_backtest_retry_isolation_passed(
                    client,
                    task_id,
                    loop_id,
                    effective_node_id,
                )
            try:
                retry_model_source, retry_extra_experiment_files = await self._build_backtest_only_model_payload(
                    client,
                    task_id,
                    loop_index,
                    reason="retry backtest-only",
                )
                retry_mode_name = QE_LOOP_RETRY_MODE_BACKTEST_ONLY
                logger.info(
                    "Retry loop %s: params.pkl verified on node %s, using backtest-only mode "
                    "(requested=%s)",
                    evolution_loop_db_id,
                    effective_node_id,
                    requested_retry_mode,
                )
            except Exception as e:
                if requested_retry_mode == QE_LOOP_RETRY_MODE_BACKTEST_ONLY:
                    raise ValueError(
                        f"Loop {evolution_loop_db_id} was requested as backtest-only, "
                        f"but reusable mlruns params.pkl is not available on node {effective_node_id}."
                    ) from e
                retry_mode_name = QE_LOOP_RETRY_MODE_FULL_TRAIN
                logger.warning(
                    "Retry loop %s: no reusable mlruns params on node %s; "
                    "falling back to full train+backtest retry. reason=%s",
                    evolution_loop_db_id,
                    effective_node_id,
                    e,
                )
        else:
            retry_mode_name = QE_LOOP_RETRY_MODE_FULL_TRAIN
            logger.info(
                "Retry loop %s: using forced full train+backtest mode on node %s",
                evolution_loop_db_id,
                effective_node_id,
            )

        # 4. 更新 loop 状态为 running
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE qe_evolution_loops SET status = 'running', agent_analysis = NULL, updated_at = NOW() WHERE loop_id = %s",
                    (evolution_loop_db_id,),
                )
                cur.execute(
                    "UPDATE qe_evolution_tasks SET status = 'running', updated_at = NOW() WHERE task_id = %s",
                    (task_id,),
                )
            conn.commit()

        # 5. 使用统一引擎重新 compose 并提交
        try:
            from .experiment_config_builders import build_config_from_retry_loop
            from .executors.backtest import BacktestExecutor, BacktestMode
            from .executors.base import ExecutionContext

            task_for_retry = dict(task)
            task_for_retry["node_id"] = effective_node_id
            cfg = build_config_from_retry_loop(config, task_for_retry, experiment_name=f"{task_id}/{loop_id}")
            if retry_mode_name != QE_LOOP_RETRY_MODE_BACKTEST_ONLY and cfg.build_runtime_flags().get("random_seed") is None:
                raise ValueError(
                    f"Loop {evolution_loop_db_id}: runtime_flags.random_seed is required for full-train retry"
                )

            composer = ConfigComposer()
            executor = BacktestExecutor(composer, client)
            if retry_mode_name == "backtest_only" and cfg.hmm and cfg.hmm.enable_sector_hmm:
                try:
                    hmm_coeff_json = await client.get_workspace_file(
                        task_id,
                        loop_id,
                        "hmm_sector_coefficients.json",
                    )
                except Exception as e:
                    raise ValueError(
                        f"Loop {evolution_loop_db_id} is HMM-enabled but "
                        f"hmm_sector_coefficients.json is not readable on node {effective_node_id}."
                    ) from e
                if not isinstance(hmm_coeff_json, str):
                    hmm_coeff_json = json.dumps(hmm_coeff_json, ensure_ascii=False)
                extra_params = dict(cfg.extra_params or {})
                extra_params[PRECOMPUTED_HMM_COEFF_JSON_PARAM] = hmm_coeff_json
                cfg = cfg.model_copy(update={"extra_params": extra_params})
                logger.info(
                    "Retry loop %s: reusing HMM coefficients artifact from node %s",
                    evolution_loop_db_id,
                    effective_node_id,
                )
            ctx = ExecutionContext(
                task_id=task_id,
                loop_index=loop_index,
                experiment_name=f"{task_id}/{loop_id}",
                node_id=effective_node_id,
                callback_url=self._get_callback_url_for_node(effective_node_id),
                model_source=retry_model_source,
                extra_experiment_files=retry_extra_experiment_files,
                require_fixed_seed=(retry_mode_name != "backtest_only"),
            )
            retry_mode = BacktestMode.BACKTEST_ONLY if retry_mode_name == "backtest_only" else BacktestMode.FULL_TRAIN
            result = await executor.submit(cfg, ctx, mode=retry_mode)
            logger.info("Retry in %s mode via unified engine: %s", retry_mode_name, result.wsl_command)

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"Retry loop failed for {evolution_loop_db_id}: {e}\n{tb_str}")
            error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s",
                        (error_detail, evolution_loop_db_id),
                    )
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s",
                        (task_id,),
                    )
                conn.commit()
            raise

        return {"loop_id": evolution_loop_db_id, "mode": retry_mode_name}

    async def delete_task(self, task_id: str) -> Dict[str, Any]:
        """
        Delete an evolution task, its DB records, remote worker workspace, and local AIstock caches.
        Running tasks must be stopped before deletion.
        """
        task_node_id: Optional[str] = None
        task: Dict[str, Any]
        dependent_forks: List[Dict[str, Any]] = []
        sub_experiment_ids: List[str] = []

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Block new log streams before any destructive cleanup starts.
                cur.execute("SELECT task_id, task_name, status, node_id FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()
                if not task:
                    raise ValueError(f"任务不存在: {task_id}")
                if task["status"] == "running":
                    raise ValueError("运行中的任务不能删除，请先停止任务")
                task_node_id = task.get("node_id")
                self._request_stop_log_streams(task_id)

                cur.execute(
                    "SELECT task_id, task_name FROM qe_evolution_tasks "
                    "WHERE fork_from_task_id = %s AND inherit_history = TRUE",
                    (task_id,),
                )
                dependent_forks = cur.fetchall()

                cur.execute(
                    "SELECT experiment_id FROM qe_experiments WHERE qe_task_id = %s",
                    (task_id,),
                )
                sub_experiment_ids = [r["experiment_id"] for r in cur.fetchall()]

        # Worker workspaces must be cleaned through the QE node API only.
        try:
            client = self._get_workspace_client_for_node_id(task_node_id)
            await client.cleanup_task_workspace(task_id)
            logger.info("Cleaned remote QE workspace through node API: %s", task_id)
        except Exception as e:
            logger.exception("QE evolution task remote workspace cleanup failed before DB delete: %s", task_id)
            raise RuntimeError(
                "QE执行节点workspace清理失败，数据库记录和本地SOTA缓存未删除；"
                f"请确认执行节点API可用后重试。原始错误: {e}"
            ) from e

        remaining_streams = await self._wait_for_log_streams_closed(task_id, timeout_seconds=10.0)
        if remaining_streams:
            raise RuntimeError(
                f"任务 {task_id} 仍有 {remaining_streams} 个日志流未关闭；"
                "本地SOTA日志缓存和数据库记录未删除。请关闭日志面板后重试。"
            )

        # Clean AIstock-owned local artifacts only; worker workspace cleanup is API-only.
        from .config_composer import QE_EXPERIMENTS_ROOT

        local_cleanup_roots = [QE_EXPERIMENTS_ROOT, Path(SOTA_ASSETS_DIR)]

        async def _remove_tree_with_log_stream_retry(dir_path: Path) -> bool:
            last_error: Optional[BaseException] = None
            for attempt in range(5):
                try:
                    return remove_aistock_artifact_tree(
                        dir_path,
                        purpose=f"QE evolution task local artifact cleanup: {task_id}",
                        allowed_roots=local_cleanup_roots,
                        ignore_errors=False,
                    )
                except PermissionError as exc:
                    last_error = exc
                    self._request_stop_log_streams(task_id)
                    await self._wait_for_log_streams_closed(task_id, timeout_seconds=5.0)
                    logger.warning(
                        "Local artifact cleanup hit a locked file for task %s path=%s attempt=%s/5: %s",
                        task_id,
                        dir_path,
                        attempt + 1,
                        exc,
                    )
                    await asyncio.sleep(0.25 * (attempt + 1))
            if last_error:
                raise last_error
            return False

        cleaned_dirs = []
        for dir_path in [
            QE_EXPERIMENTS_ROOT / task_id,    # AIstock-side experiment copy
            Path(SOTA_ASSETS_DIR) / task_id,  # SOTA assets + log stream cache
        ]:
            if await _remove_tree_with_log_stream_retry(dir_path):
                cleaned_dirs.append(str(dir_path))
                logger.info("Cleaned local AIstock artifact directory: %s", dir_path)

        optuna_deleted = unlink_aistock_artifact_files(
            Path(SOTA_ASSETS_DIR) / "optuna_studies",
            f"{task_id}_*.db",
            purpose=f"QE evolution task Optuna study cleanup: {task_id}",
            allowed_roots=[Path(SOTA_ASSETS_DIR)],
        )
        if optuna_deleted:
            logger.info("Cleaned %s Optuna study file(s) for task %s", optuna_deleted, task_id)

        # Delete DB records only after remote and local cleanup have succeeded.
        deleted_counts: Dict[str, Any] = {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                if dependent_forks:
                    fork_ids = [f["task_id"] for f in dependent_forks]
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET inherit_history = FALSE "
                        "WHERE fork_from_task_id = %s AND inherit_history = TRUE",
                        (task_id,),
                    )
                    logger.warning(
                        "Source task %s was deleted; inherit_history disabled for %s fork task(s): %s",
                        task_id,
                        len(fork_ids),
                        fork_ids,
                    )

                if sub_experiment_ids:
                    cur.execute(
                        "DELETE FROM qe_factor_experiment_metrics WHERE experiment_id = ANY(%s)",
                        (sub_experiment_ids,),
                    )
                    deleted_counts["qe_factor_experiment_metrics"] = cur.rowcount

                cur.execute("DELETE FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                deleted_counts["qe_evolution_tasks"] = cur.rowcount

                if sub_experiment_ids:
                    cur.execute(
                        "DELETE FROM qe_experiments WHERE experiment_id = ANY(%s)",
                        (sub_experiment_ids,),
                    )
                    deleted_counts["qe_experiments"] = cur.rowcount

            conn.commit()

        deleted_counts["cleaned_dirs"] = len(cleaned_dirs)
        deleted_counts["optuna_files_deleted"] = optuna_deleted

        logger.info("Task %s (%s) deleted. Counts: %s", task_id, task["task_name"], deleted_counts)
        return {
            "task_id": task_id,
            "task_name": task["task_name"],
            "deleted_counts": deleted_counts,
            "cleaned_dirs": cleaned_dirs,
        }
        
    async def stream_task_logs(self, task_id: str):
        """Forward RDAgent SSE logs, including all loop nodes for distributed custom_evo tasks."""
        current_status = self._get_task_status(task_id)
        if self._is_log_stream_stop_requested(task_id) or current_status is None:
            payload = {
                "status": "deleted",
                "event": "task_deleted",
                "logs": [f"Task {task_id} no longer exists; log stream closed."],
            }
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            return

        if self.is_terminal_log_status(current_status):
            snapshot = self.get_task_log_tail(task_id)
            logs = snapshot["logs"] or [
                f"Task {task_id} is {current_status}; no local evolution.log tail is available."
            ]
            for log_line in logs:
                yield f"data: {json.dumps({'status': current_status, 'event': 'task_log_tail', 'logs': [log_line]}, ensure_ascii=False)}\n\n"
            closed = {
                "status": current_status,
                "event": "task_log_terminal",
                "logs": [
                    f"Task {task_id} is terminal ({current_status}); no live log stream was opened."
                ],
            }
            yield f"data: {json.dumps(closed, ensure_ascii=False)}\n\n"
            return

        self._register_log_stream(task_id)
        log_dir = os.path.join(SOTA_ASSETS_DIR, task_id, "logs")
        log_path = os.path.join(log_dir, "evolution.log")
        node_plan = self._get_log_stream_node_plan_for_task(task_id)
        node_ids = list(node_plan.get("node_ids") or [])
        if not node_ids:
            node_ids = [None]
            node_plan.setdefault("warnings", []).append(
                f"Task {task_id} log node plan was empty; using local log stream only."
            )
        distributed_stream = len(node_ids) > 1

        async def append_local_log(text: str) -> None:
            os.makedirs(log_dir, exist_ok=True)
            async with aiofiles.open(log_path, "a", encoding="utf-8") as log_file:
                await log_file.write(text)

        def as_sse(raw_line: str) -> str:
            if raw_line.startswith("data:"):
                return f"{raw_line}\n\n"
            return f"data: {raw_line}\n\n"

        def parse_payload(text: str) -> Optional[Dict[str, Any]]:
            try:
                payload = json.loads(text)
            except Exception as exc:
                logger.debug("Skipping non-JSON QE log payload for task %s: %s", task_id, exc)
                return None
            return payload if isinstance(payload, dict) else None

        def normalize_logs(value: Any) -> List[str]:
            if value is None:
                return []
            if isinstance(value, list):
                return [str(item) for item in value]
            return [str(value)]

        def node_prefix_log_line(log_line: str, node_id: Optional[str]) -> str:
            label = self._log_stream_node_label(node_id)
            prefix = f"[{label}]"
            text = str(log_line)
            return text if text.startswith(prefix) else f"{prefix} {text}"

        def prepare_sse_line(raw_line: str, node_id: Optional[str], *, decorate_node: bool) -> tuple[str, str, Optional[Dict[str, Any]]]:
            text = raw_line[len("data:"):].strip() if raw_line.startswith("data:") else raw_line
            payload = parse_payload(text) if text else None
            if not decorate_node:
                return as_sse(raw_line), text, payload

            label = self._log_stream_node_label(node_id)
            if payload is None:
                decorated_payload: Dict[str, Any] = {
                    "status": "running",
                    "event": "node_log",
                    "node_id": label,
                    "logs": [node_prefix_log_line(text or raw_line, node_id)],
                }
            else:
                decorated_payload = dict(payload)
                decorated_payload["node_id"] = label
                decorated_payload["source_node_id"] = label
                decorated_payload.setdefault("event", "node_log")
                logs = normalize_logs(decorated_payload.get("logs"))
                if logs:
                    decorated_payload["logs"] = [
                        node_prefix_log_line(log_line, node_id)
                        for log_line in logs
                    ]

            decorated_text = json.dumps(decorated_payload, ensure_ascii=False)
            return f"data: {decorated_text}\n\n", decorated_text, decorated_payload

        def warning_sse(message: str, *, event: str, node_id: Optional[str] = None) -> tuple[str, str]:
            label = self._log_stream_node_label(node_id) if node_id is not None else None
            payload: Dict[str, Any] = {
                "status": "warning",
                "event": event,
                "logs": [message],
            }
            if label is not None:
                payload["node_id"] = label
            text = json.dumps(payload, ensure_ascii=False)
            return f"data: {text}\n\n", text

        def is_workspace_waiting(payload: Optional[Dict[str, Any]]) -> bool:
            if not payload or payload.get("status") != "waiting":
                return False
            logs = payload.get("logs") or []
            if not isinstance(logs, list):
                logs = [logs]
            return any("Task directory not found yet" in str(item) for item in logs)

        try:
            node_labels = ", ".join(self._log_stream_node_label(node_id) for node_id in node_ids)
            session_header = (
                f"\n{'='*60}\n"
                f"[Session Start] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"[Log Nodes] {node_labels}\n"
                f"{'='*60}\n"
            )
            await append_local_log(session_header)

            for warning in node_plan.get("warnings") or []:
                sse, text = warning_sse(f"[System] {warning}", event="log_node_resolution_warning")
                await append_local_log(text + "\n")
                yield sse

            async def forward_single_node(node_id: Optional[str]):
                client = self._get_workspace_client_for_node_id(node_id)
                async for line in client.stream_task_logs(task_id):
                    text = line[len("data:"):].strip() if line.startswith("data:") else line
                    payload = parse_payload(text) if text else None

                    if self._is_log_stream_stop_requested(task_id):
                        closed = {
                            "status": "deleted",
                            "event": "task_deleted",
                            "logs": [f"Task {task_id} is being deleted; log stream closed."],
                        }
                        yield f"data: {json.dumps(closed, ensure_ascii=False)}\n\n"
                        return

                    if is_workspace_waiting(payload):
                        latest_status = self._get_task_status(task_id)
                        if latest_status is None:
                            deleted = {
                                "status": "deleted",
                                "event": "task_deleted",
                                "logs": [f"Task {task_id} no longer exists; log stream closed."],
                            }
                            yield f"data: {json.dumps(deleted, ensure_ascii=False)}\n\n"
                            return
                        if latest_status in {"completed", "failed", "cancelled", "paused"}:
                            missing = {
                                "status": "missing",
                                "event": "task_log_workspace_missing",
                                "logs": [
                                    f"Task {task_id} is {latest_status}, but its RDAgent workspace is missing; log stream closed."
                                ],
                            }
                            yield f"data: {json.dumps(missing, ensure_ascii=False)}\n\n"
                            return
                        # Transient pre-start wait: show it in UI but do not persist one line per second.
                        yield as_sse(line)
                        continue

                    if text:
                        await append_local_log(text + "\n")
                    yield as_sse(line)

            if not distributed_stream:
                node_id = node_ids[0] if node_ids else None
                try:
                    async for chunk in forward_single_node(node_id):
                        yield chunk
                except Exception as exc:
                    logger.exception("QE log stream failed for task %s node %s", task_id, self._log_stream_node_label(node_id))
                    sse, text = warning_sse(
                        f"[{self._log_stream_node_label(node_id)}] log stream failed: {exc}",
                        event="node_log_stream_error",
                        node_id=node_id,
                    )
                    await append_local_log(text + "\n")
                    yield sse
                return

            queue: asyncio.Queue = asyncio.Queue()

            async def read_node_stream(node_id: Optional[str]) -> None:
                node_key = self._log_stream_node_key(node_id)
                try:
                    client = self._get_workspace_client_for_node_id(node_id)
                    async for line in client.stream_task_logs(task_id):
                        await queue.put({"kind": "line", "node_id": node_id, "node_key": node_key, "line": line})
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logger.exception("QE distributed log stream failed for task %s node %s", task_id, self._log_stream_node_label(node_id))
                    await queue.put({"kind": "error", "node_id": node_id, "node_key": node_key, "error": str(exc)})
                finally:
                    queue.put_nowait({"kind": "done", "node_id": node_id, "node_key": node_key})

            worker_tasks: Dict[str, asyncio.Task] = {
                self._log_stream_node_key(node_id): asyncio.create_task(read_node_stream(node_id))
                for node_id in node_ids
            }
            active_workers = len(worker_tasks)
            terminal_missing_nodes: set[str] = set()
            try:
                while active_workers > 0:
                    if self._is_log_stream_stop_requested(task_id):
                        closed = {
                            "status": "deleted",
                            "event": "task_deleted",
                            "logs": [f"Task {task_id} is being deleted; distributed log stream closed."],
                        }
                        yield f"data: {json.dumps(closed, ensure_ascii=False)}\n\n"
                        return

                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        if self._get_task_status(task_id) is None:
                            deleted = {
                                "status": "deleted",
                                "event": "task_deleted",
                                "logs": [f"Task {task_id} no longer exists; distributed log stream closed."],
                            }
                            yield f"data: {json.dumps(deleted, ensure_ascii=False)}\n\n"
                            return
                        continue

                    kind = item.get("kind")
                    node_id = item.get("node_id")
                    node_key = item.get("node_key")
                    if kind == "done":
                        active_workers -= 1
                        continue
                    if kind == "error":
                        sse, text = warning_sse(
                            f"[{self._log_stream_node_label(node_id)}] log stream failed: {item.get('error')}",
                            event="node_log_stream_error",
                            node_id=node_id,
                        )
                        await append_local_log(text + "\n")
                        yield sse
                        continue

                    sse_line, persist_text, payload = prepare_sse_line(
                        item.get("line") or "",
                        node_id,
                        decorate_node=True,
                    )

                    if is_workspace_waiting(payload):
                        latest_status = self._get_task_status(task_id)
                        if latest_status is None:
                            deleted = {
                                "status": "deleted",
                                "event": "task_deleted",
                                "logs": [f"Task {task_id} no longer exists; distributed log stream closed."],
                            }
                            yield f"data: {json.dumps(deleted, ensure_ascii=False)}\n\n"
                            return
                        if latest_status in {"completed", "failed", "cancelled", "paused"}:
                            if node_key not in terminal_missing_nodes:
                                terminal_missing_nodes.add(node_key)
                                sse, text = warning_sse(
                                    f"[{self._log_stream_node_label(node_id)}] Task {task_id} is {latest_status}, "
                                    "but this node's RDAgent workspace is missing; stopping this node log stream.",
                                    event="node_log_workspace_missing",
                                    node_id=node_id,
                                )
                                await append_local_log(text + "\n")
                                yield sse
                            task = worker_tasks.get(node_key)
                            if task:
                                task.cancel()
                            continue
                        # Transient pre-start wait: show it in UI but do not persist one line per second.
                        yield sse_line
                        continue

                    if persist_text:
                        await append_local_log(persist_text + "\n")
                    yield sse_line
            finally:
                for task in worker_tasks.values():
                    if not task.done():
                        task.cancel()
                if worker_tasks:
                    await asyncio.gather(*worker_tasks.values(), return_exceptions=True)
        finally:
            self._unregister_log_stream(task_id)

    async def get_sota_registry(self) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT r.*, l.config_json, l.metrics_json, t.task_name 
                    FROM qe_sota_registry r
                    JOIN qe_evolution_loops l ON r.loop_id = l.loop_id
                    JOIN qe_evolution_tasks t ON l.task_id = t.task_id
                    ORDER BY r.created_at DESC
                """)
                return [dict(row) for row in cur.fetchall()]
        
    async def sync_loop_assets(self, task_id: str, loop_id: str) -> str:
        """
        同步 RDAgent 侧的物理资产到本地（双参数：task_id + loop_id）
        注意：loop_id 来自路由参数（如 "Loop2"），DB 中存储的是 "{task_id}_Loop{N}" 格式
        """
        dest_dir = os.path.join(SOTA_ASSETS_DIR, task_id, loop_id)
        client = self._get_workspace_client_for_loop(task_id, loop_id)
        synced_path = await client.download_loop_assets(task_id, loop_id, dest_dir)
        
        # 更新 DB SOTA registry：loop_id 在 DB 中是 "{task_id}_{loop_id}" 格式
        evolution_loop_db_id = f"{task_id}_{loop_id}"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_sota_registry 
                    SET model_assets_synced = TRUE, local_asset_path = %s 
                    WHERE loop_id = %s
                """, (synced_path, evolution_loop_db_id))
            conn.commit()
            
        return synced_path

    async def get_task_sota_assets(self, task_id: str, include_alpha_baseline: bool = False) -> Dict[str, Any]:
        """
        查询指定 RDAgent task 的 SOTA 因子和模型资产。
        用于 Phase 4 多入口演进：从 RDAgent task 创建演进任务。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 获取 SOTA 因子（排除已删除/不可用）+ LEFT JOIN 独立指标
                cur.execute("""
                    SELECT c.factor_name, c.source,
                           m.ic_mean AS task_ic, m.top_excess_annual_return AS task_ann_ret,
                           m.top_max_drawdown AS task_drawdown,
                           m.ic_mean, m.rank_ic_mean, m.icir,
                           m.top_excess_sharpe, m.top_excess_annual_return,
                           m.top_max_drawdown,
                           c.is_sota_factor, c.catalog_source
                    FROM aistock_factor_catalog c
                    LEFT JOIN LATERAL (
                        SELECT ic_mean, rank_ic_mean, icir,
                               top_excess_sharpe, top_excess_annual_return,
                               top_max_drawdown
                        FROM aistock_factor_metrics
                        WHERE factor_name = c.factor_name AND eval_window = 'full' AND calc_engine = %s
                        ORDER BY calculated_at DESC
                        LIMIT 1
                    ) m ON TRUE
                    WHERE c.source_task_id = %s
                      AND c.is_sota_factor = TRUE
                      AND c.is_available = TRUE
                    ORDER BY m.ic_mean DESC NULLS LAST
                """, (CALC_ENGINE, task_id))
                sota_factors = [dict(r) for r in cur.fetchall()]

                # 获取 SOTA 模型
                cur.execute("""
                    SELECT model_id, model_name, model_type, ic, annualized_return,
                           max_drawdown, is_sota, task_run_id
                    FROM aistock_model_catalog
                    WHERE task_run_id = %s AND is_sota = TRUE
                    ORDER BY ic DESC NULLS LAST
                """, (task_id,))
                sota_models = [dict(r) for r in cur.fetchall()]

                # 可选：获取 Alpha 基准因子
                alpha_factors = []
                if include_alpha_baseline:
                    cur.execute("""
                        SELECT c.factor_name, c.source, m.ic_mean AS ic
                        FROM aistock_factor_catalog c
                        JOIN LATERAL (
                            SELECT ic_mean
                            FROM aistock_factor_metrics
                            WHERE factor_name = c.factor_name
                              AND eval_window = 'full'
                              AND calc_engine = %s
                            ORDER BY calculated_at DESC
                            LIMIT 1
                        ) m ON TRUE
                        WHERE c.source IN ('alpha158', 'alpha360')
                          AND c.is_available = TRUE
                        ORDER BY m.ic_mean DESC NULLS LAST
                        LIMIT 50
                    """, (CALC_ENGINE,))
                    alpha_factors = [dict(r) for r in cur.fetchall()]

                # 统计该 task 的全部因子数（含非 SOTA），用于判断是否有演进因子
                cur.execute("""
                    SELECT COUNT(*) AS cnt
                    FROM aistock_factor_catalog
                    WHERE source_task_id = %s
                """, (task_id,))
                total_task_factors = cur.fetchone()["cnt"]

        return {
            "task_id": task_id,
            "sota_factors": sota_factors,
            "sota_models": sota_models,
            "alpha_factors": alpha_factors,
            "total_sota_factors": len(sota_factors),
            "total_sota_models": len(sota_models),
            "total_task_factors": total_task_factors,
        }

    async def create_experiment_from_task_sota(
        self,
        task_id: str,
        experiment_name: str,
        include_alpha_baseline: bool = False,
        model_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        factor_keys: Optional[List[str]] = None,
        custom_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        基于 RDAgent task 的 SOTA 资产创建真实 QE 实验。
        用于入口 D (rdagent_task_sota)。

        - factor_keys: 用户手动选择的因子 key 列表（格式 'name||source'）
                       若提供则使用用户选择的因子，否则使用 SOTA 因子
        - model_id: 用户手动选择的模型ID
                    Factor Task 必须由前端传入；Model Task 使用 SOTA 第一个模型
        """
        assets = await self.get_task_sota_assets(task_id, include_alpha_baseline)

        # 确定因子列表
        if factor_keys and len(factor_keys) > 0:
            # 用户手动选择了因子（Model Task 场景）
            factor_names = [k.split("||")[0] for k in factor_keys]
        else:
            # 使用 SOTA 因子（Factor Task 场景）
            factor_names = [f["factor_name"] for f in assets["sota_factors"]]
            if include_alpha_baseline:
                alpha_names = [f["factor_name"] for f in assets["alpha_factors"]]
                factor_names = list(dict.fromkeys(factor_names + alpha_names))

        if not factor_names:
            raise ValueError(f"RDAgent task {task_id} 没有可用因子（SOTA 因子为空且未手动选择因子）")

        # 确定模型
        # model_id 由调用方传入（Factor Task 由前端传入，Model Task 自动使用 SOTA 第一个模型）
        if not model_id and assets["sota_models"]:
            model_id = assets["sota_models"][0]["model_id"]
        params = dict(custom_params or {})
        params["random_seed"] = normalize_qe_random_seed(
            params.get("random_seed"),
            field_name="create_experiment_from_task_sota.random_seed",
        )

        # 使用 ConfigComposer 创建实验
        from .config_composer import ConfigComposer
        composer = ConfigComposer()
        result = composer.compose_experiment(
            factor_names=factor_names,
            model_id=model_id,
            strategy_id=strategy_id,
            experiment_name=experiment_name,
            custom_params=params,
        )

        return {
            "ok": True,
            "experiment_id": result.get("experiment_id"),
            "factor_count": len(factor_names),
            "model_id": model_id,
            "source_task_id": task_id,
        }

    async def get_available_source_tasks(self) -> List[Dict[str, Any]]:
        """获取所有已同步的 RDAgent task 列表（从 model/factor catalog 聚合）。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT task_id,
                           COALESCE(SUM(sota_factor_count), 0)::int AS sota_factor_count,
                           COALESCE(SUM(sota_model_count), 0)::int  AS sota_model_count,
                           MAX(best_ic) AS best_ic,
                           MAX(best_sharpe) AS best_sharpe,
                           MAX(best_annualized_return) AS best_annualized_return,
                           MIN(worst_max_drawdown) AS worst_max_drawdown,
                           COALESCE(SUM(total_loops), 0)::int AS total_loops
                    FROM (
                        SELECT c.source_task_id AS task_id,
                               COUNT(*) FILTER (WHERE is_sota_factor = TRUE) AS sota_factor_count,
                               0 AS sota_model_count,
                               MAX(m.ic_mean) AS best_ic,
                               MAX(m.top_excess_sharpe) AS best_sharpe,
                               MAX(m.top_excess_annual_return) AS best_annualized_return,
                               MIN(m.top_max_drawdown) AS worst_max_drawdown,
                               0 AS total_loops
                        FROM aistock_factor_catalog c
                        LEFT JOIN LATERAL (
                            SELECT ic_mean, top_excess_sharpe,
                                   top_excess_annual_return, top_max_drawdown
                            FROM aistock_factor_metrics
                            WHERE factor_name = c.factor_name
                              AND eval_window = 'full'
                              AND calc_engine = %s
                            ORDER BY calculated_at DESC
                            LIMIT 1
                        ) m ON TRUE
                        WHERE c.source_task_id IS NOT NULL AND c.source_task_id != ''
                        GROUP BY c.source_task_id
                        UNION ALL
                        SELECT task_run_id AS task_id,
                               0 AS sota_factor_count,
                               COUNT(*) FILTER (WHERE is_sota = TRUE) AS sota_model_count,
                               MAX(ic) AS best_ic,
                               MAX(sharpe) AS best_sharpe,
                               MAX(annualized_return) AS best_annualized_return,
                               MIN(max_drawdown) AS worst_max_drawdown,
                               COUNT(DISTINCT loop_id) AS total_loops
                        FROM aistock_model_catalog
                        WHERE task_run_id IS NOT NULL
                        GROUP BY task_run_id
                    ) sub
                    GROUP BY task_id
                    HAVING COALESCE(SUM(sota_factor_count), 0) > 0
                        OR COALESCE(SUM(sota_model_count), 0) > 0
                    ORDER BY best_ic DESC NULLS LAST
                """, (CALC_ENGINE,))
                tasks = [dict(r) for r in cur.fetchall()]
                for t in tasks:
                    t["has_sota"] = t["sota_model_count"] > 0 or t["sota_factor_count"] > 0

        return tasks

    async def get_completed_experiments(self) -> List[Dict[str, Any]]:
        """获取所有已完成的 QE 实验列表，用于作为演进起点选择。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT e.experiment_id, e.experiment_name, e.status,
                           e.factor_names, e.model_id, e.strategy_id,
                           e.ic, e.icir, e.rank_ic, e.annualized_return,
                           e.max_drawdown, e.information_ratio,
                           e.is_sota, e.is_evolution_loop,
                           e.created_at, e.completed_at,
                           COALESCE(jsonb_array_length(e.factor_names), 0) AS factor_count
                    FROM qe_experiments e
                    WHERE e.status = 'completed'
                    ORDER BY e.completed_at DESC NULLS LAST
                    LIMIT 200
                """)
                experiments = [dict(r) for r in cur.fetchall()]
        return experiments

    # ================================================================
    # 策略演进（Strategy Evolution）- 跳过训练的批量策略回测
    # ================================================================

    async def strategy_fork_task(
        self,
        source_task_id: str,
        from_loop_index: int,
        task_name: Optional[str] = None,
        loops_config: List[Dict[str, Any]] = None,
        execution_mode: str = "serial",
        inherit_history: bool = False,
        node_id: Optional[str] = None,
    ) -> str:
        """
        从指定 task 的某个已完成 loop 创建策略演进任务。
        复用源 loop 的模型，仅修改策略参数进行批量回测。
        """
        if not loops_config or len(loops_config) == 0:
            raise ValueError("loops_config 不能为空，至少需要配置一个 Loop")

        # 1. 验证源 task 和 loop
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (source_task_id,))
                source_task = cur.fetchone()
                if not source_task:
                    raise ValueError(f"源任务不存在: {source_task_id}")

                cur.execute(
                    "SELECT * FROM qe_evolution_loops WHERE task_id = %s AND loop_index = %s AND status = 'completed'",
                    (source_task_id, from_loop_index),
                )
                source_loop = cur.fetchone()
                if not source_loop:
                    raise ValueError(
                        f"源任务 {source_task_id} 中不存在已完成的 Loop {from_loop_index}"
                    )

        # 2. 验证模型文件存在
        client = self._get_workspace_client_for_task(source_task_id)
        source_loop_id = f"Loop{from_loop_index}"
        workspace_status = await client.get_loop_status(source_task_id, source_loop_id)
        if workspace_status.get("status") not in ("completed",):
            logger.warning(
                f"源 Loop {source_task_id} L{from_loop_index} 在 workspace 侧状态为 {workspace_status.get('status')}，"
                "模型文件可能不完整"
            )

        # 3. 读取源 loop 的基础配置（因子、模型、data_split 等）
        config = source_loop.get("config_json") or {}
        if isinstance(config, str):
            config = json.loads(config)

        base_factor_list = config.get("factor_list", [])
        base_model_id = config.get("model_id")
        base_strategy_id = config.get("strategy_id")
        base_data_split = config.get("data_split", {})
        base_model_params = config.get("model_params", {})
        source_label_horizon = self._extract_label_horizon_from_config(
            config,
            context=f"strategy_fork_source[{source_task_id}/Loop{from_loop_index}].config_json",
        )
        base_model_params = self._apply_label_horizon_to_model_params(
            base_model_params,
            source_label_horizon,
        )
        base_model_params = merge_qe_minute_runtime_contract(
            base_model_params,
            config=config,
            source="strategy_fork_base_experiment",
            allow_default_execution_algo=False,
        )
        for idx, loop_cfg in enumerate(loops_config, start=1):
            loop_horizon = loop_cfg.get("label_horizon")
            if loop_horizon not in (None, ""):
                requested_horizon = normalize_label_horizon(
                    loop_horizon,
                    field_name=f"strategy_fork.loops[{idx}].label_horizon",
                )
                if requested_horizon != source_label_horizon:
                    raise ValueError(
                        "strategy_fork is backtest-only and cannot change label_horizon: "
                        f"loop {idx} requested {requested_horizon}, source is {source_label_horizon}"
                    )
            loop_cfg["source_label_horizon"] = source_label_horizon

        if not base_factor_list:
            raise ValueError(f"源 Loop {from_loop_index} 的因子列表为空，无法创建策略演进")
        if not base_model_id:
            raise ValueError(f"源 Loop {from_loop_index} 的 model_id 为空，无法复用模型")

        # 4. 生成新 task_id
        suffix = uuid.uuid4().hex[:4]
        new_task_id = f"qe_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{suffix}"
        if not task_name:
            source_name = source_task.get("task_name", source_task_id)
            task_name = f"{source_name}_策略演进_L{from_loop_index}"

        target_desc = source_task.get("target_desc", "")

        # 5. 创建 base_experiment 记录
        base_exp_id = f"{new_task_id}_base"
        metrics = source_loop.get("metrics_json") or {}
        if isinstance(metrics, str):
            metrics = json.loads(metrics)

        # node_id: 入参优先，否则继承源任务
        effective_node_id = node_id if node_id is not None else source_task.get("node_id")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_experiments
                    (experiment_id, experiment_name, qe_task_id, qe_loop_id,
                     loop_index, parent_experiment_id,
                     is_evolution_loop, factor_names, model_id, strategy_id,
                     data_split, custom_params,
                     result_metrics, status, is_sota)
                    VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s, %s, %s, %s, %s, %s, 'created', FALSE)
                """, (
                    base_exp_id,
                    f"策略演进基准 from {source_task_id} L{from_loop_index}",
                    new_task_id,
                    source_loop_id,
                    0,
                    None,
                    json.dumps(base_factor_list),
                    base_model_id,
                    base_strategy_id,
                    json.dumps(base_data_split) if isinstance(base_data_split, dict) else base_data_split,
                    json.dumps(base_model_params) if isinstance(base_model_params, dict) else base_model_params,
                    json.dumps(metrics),
                ))

                # 6. 创建策略演进任务
                cur.execute("""
                    INSERT INTO qe_evolution_tasks
                    (task_id, task_name, target_desc, max_loops, current_loop, status,
                     base_experiment_id, node_id, source_type,
                     task_type, strategy_evo_config, strategy_evo_execution_mode,
                     model_source_task_id, model_source_loop_index,
                     fork_from_task_id, fork_from_loop_index, inherit_history, label_horizon)
                    VALUES (%s, %s, %s, %s, 0, 'pending', %s, %s, 'strategy_fork',
                            'strategy_evo', %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    new_task_id, task_name, target_desc, len(loops_config),
                    base_exp_id, effective_node_id,
                    json.dumps({"loops": loops_config}),
                    execution_mode,
                    source_task_id, from_loop_index,
                    source_task_id, from_loop_index, inherit_history,
                    source_label_horizon,
                ))
            conn.commit()

        logger.info(
            f"创建策略演进任务 {new_task_id} 从 {source_task_id} L{from_loop_index}, "
            f"共 {len(loops_config)} 个 Loop, 执行方式={execution_mode}"
        )

        # 7. 异步启动批量调度
        bg_task = asyncio.create_task(self.submit_strategy_evo_all_loops(new_task_id))
        bg_task.add_done_callback(
            lambda t: logger.error(f"submit_strategy_evo_all_loops failed: {t.exception()}") if t.exception() else None
        )

        return new_task_id

    async def submit_strategy_evo_loop(self, task_id: str, loop_index: int) -> Optional[str]:
        """
        提交单个策略回测 Loop（跳过训练）。
        使用 --backtest-only 模式，复用源 loop 的模型。
        """
        # 读取 task 的 strategy_evo_config
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            logger.error(f"Task {task_id} not found")
            return None

        strategy_evo_config = task.get("strategy_evo_config")
        if not strategy_evo_config:
            logger.error(f"Task {task_id} strategy_evo_config 为空，数据完整性异常")
            return None
        if isinstance(strategy_evo_config, str):
            strategy_evo_config = json.loads(strategy_evo_config)

        loops_config = strategy_evo_config.get("loops")
        if not loops_config:
            logger.error(f"Task {task_id} strategy_evo_config.loops 为空")
            return None

        loop_config = None
        for cfg in loops_config:
            if cfg.get("loop_index") == loop_index:
                loop_config = cfg
                break

        if not loop_config:
            logger.error(f"Loop {loop_index} 的配置未找到")
            raise ValueError(f"Loop configuration not found for loop_index={loop_index} in task {task_id}")

        # 加载源 Loop 的基础配置
        source_task_id = task.get("model_source_task_id")
        source_loop_idx = task.get("model_source_loop_index")

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT config_json FROM qe_evolution_loops WHERE task_id = %s AND loop_index = %s",
                    (source_task_id, source_loop_idx),
                )
                source_loop_row = cur.fetchone()

        if not source_loop_row:
            logger.error(f"源 Loop {source_task_id} L{source_loop_idx} 未找到")
            return None

        source_config = source_loop_row.get("config_json") or {}
        if isinstance(source_config, str):
            source_config = json.loads(source_config)

        # 合并策略参数
        base_config = {
            "action_type": "strategy_backtest",
            "factor_list": source_config.get("factor_list", []),
            "model_id": source_config.get("model_id"),
            "model_params": source_config.get("model_params", {}),
            "data_split": source_config.get("data_split", {}),
            "strategy_id": loop_config.get("strategy_id") or source_config.get("strategy_id"),
        }

        # 策略参数覆盖
        strategy_params = loop_config.get("strategy_params", {})
        if strategy_params:
            base_config["model_params"] = dict(source_config.get("model_params", {}))
            base_config["model_params"].update(strategy_params)

        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        loop_id = f"Loop{loop_index}"

        # 创建 LOOP 记录
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_evolution_loops
                    (loop_id, task_id, loop_index, status, action_type)
                    VALUES (%s, %s, %s, 'running', 'strategy_backtest')
                    ON CONFLICT (loop_id) DO UPDATE SET status = 'running', updated_at = NOW()
                """, (evolution_loop_db_id, task_id, loop_index))
            conn.commit()

        # Unified execution layer: ExperimentConfig + BacktestExecutor.
        try:
            from .experiment_config_builders import build_config_from_strategy_evo_loop
            from .config_composer import ConfigComposer
            from .executors.backtest import BacktestExecutor, BacktestMode
            from .executors.base import ExecutionContext

            experiment_name = f"{task_id}/{loop_id}"
            cfg = build_config_from_strategy_evo_loop(
                base_config,
                loop_config,
                task,
                experiment_name=experiment_name,
            )
            base_config = dict(base_config)
            loop_model_params = merge_qe_minute_runtime_contract(
                cfg.build_custom_params(),
                config=base_config,
                execution_algo=cfg.execution_algo,
                execution_algo_params=cfg.execution_algo_params,
                source="strategy_evo_loop_config",
                allow_default_execution_algo=True,
            )
            base_config["model_params"] = loop_model_params
            runtime_contract = build_qe_minute_runtime_contract(
                custom_params=loop_model_params,
                execution_algo=cfg.execution_algo,
                execution_algo_params=cfg.execution_algo_params,
                source="strategy_evo_loop_config",
                allow_default_execution_algo=True,
            )
            if runtime_contract:
                base_config.update(runtime_contract)

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_loops SET config_json = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(base_config), evolution_loop_db_id))
                conn.commit()

            target_node_id = task.get("node_id")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT node_id FROM qe_evolution_tasks WHERE task_id = %s", (source_task_id,))
                    src_row = cur.fetchone()
            source_node_id = src_row[0] if src_row else None
            source_client = (
                self.workspace_client
                if not source_node_id
                else self._node_clients.get(source_node_id) or QEWorkspaceClient.for_node(source_node_id)
            )
            if source_node_id and source_node_id not in self._node_clients:
                self._node_clients[source_node_id] = source_client
            model_source, extra_experiment_files = await self._build_backtest_only_model_payload(
                source_client,
                source_task_id,
                int(source_loop_idx),
                reason="strategy evolution backtest-only",
            )
            logger.info(
                "strategy evolution backtest-only packaged source model: source=%s/%s source_node=%s target_node=%s",
                source_task_id,
                f"Loop{source_loop_idx}",
                source_node_id or "local",
                target_node_id or "local",
            )

            client = self._get_workspace_client_for_task(task_id)
            executor = BacktestExecutor(ConfigComposer(), client)
            ctx = ExecutionContext(
                task_id=task_id,
                loop_index=loop_index,
                experiment_name=experiment_name,
                node_id=task.get("node_id"),
                callback_url=self._get_callback_url_for_task(task_id),
                model_source=model_source,
                extra_experiment_files=extra_experiment_files or None,
            )
            await executor.submit(cfg, ctx, mode=BacktestMode.BACKTEST_ONLY)

            logger.info(f"[unified] strategy evolution Loop {loop_index} submitted (backtest-only)")
            return evolution_loop_db_id

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"策略演进 Loop {loop_index} 提交失败: {e}\n{tb_str}")
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
                        cur.execute("UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s", (error_detail, evolution_loop_db_id))
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"策略演进 Loop {loop_index} 失败标记失败: {db_err}")
            return None

    async def submit_strategy_evo_all_loops(self, task_id: str):
        """
        批量调度策略演进 Loops，支持串行/并行模式。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            logger.error(f"Task {task_id} not found")
            return

        strategy_evo_config = task.get("strategy_evo_config")
        if not strategy_evo_config:
            logger.error(f"Task {task_id} strategy_evo_config 为空，数据完整性异常")
            return
        if isinstance(strategy_evo_config, str):
            strategy_evo_config = json.loads(strategy_evo_config)

        loops_config = strategy_evo_config.get("loops", [])
        if not loops_config:
            logger.error(f"策略演进任务 {task_id} 没有 loops 配置")
            return

        execution_mode_raw = task.get("strategy_evo_execution_mode", "serial")
        if execution_mode_raw.startswith("parallel"):
            mode = "parallel"
            parts = execution_mode_raw.split("_")
            parallelism = int(parts[1]) if len(parts) > 1 else 2
        else:
            mode = "serial"
            parallelism = 1

        # Mark task as running only if it has not been stopped since enqueue.
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE qe_evolution_tasks SET status = 'running', updated_at = NOW() "
                    "WHERE task_id = %s AND status IN ('pending', 'running')",
                    (task_id,),
                )
                if cur.rowcount == 0:
                    logger.info(f"Task {task_id} is no longer pending/running; abort loop submission")
                    conn.commit()
                    return
            conn.commit()

        # 恢复场景：过滤掉已完成的 loop，只提交 pending/failed 的
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT loop_index, status FROM qe_evolution_loops WHERE task_id = %s",
                    (task_id,),
                )
                existing_loops = {row["loop_index"]: row["status"] for row in cur.fetchall()}

        loops_to_run = []
        for loop_config in loops_config:
            loop_index = loop_config.get("loop_index", len(loops_config))
            status = existing_loops.get(loop_index)
            if status == "completed":
                logger.info(f"策略演进 Loop {loop_index} 已完成，跳过")
                continue
            loops_to_run.append(loop_config)

        if not loops_to_run:
            logger.info(f"策略演进任务 {task_id} 所有 Loop 已完成")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE qe_evolution_tasks SET status = 'completed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                conn.commit()
            return

        if mode == "serial":
            # 串行模式：逐个提交并等待完成
            for loop_config in loops_to_run:
                loop_index = loop_config.get("loop_index", len(loops_config))
                loop_id = await self.submit_strategy_evo_loop(task_id, loop_index)
                if not loop_id:
                    logger.error(f"策略演进 Loop {loop_index} 提交失败，停止后续 Loops")
                    break

                # 等待 Loop 完成（超时 2 小时）
                max_wait = 7200  # 2 小时
                waited = 0
                interval = 10
                while waited < max_wait:
                    await asyncio.sleep(interval)
                    waited += interval

                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT status FROM qe_evolution_loops WHERE loop_id = %s",
                                (loop_id,),
                            )
                            row = cur.fetchone()
                    if not row:
                        break

                    status = row[0]
                    if status in ("completed", "failed", "cancelled"):
                        break

                # 超时检查
                if waited >= max_wait:
                    logger.error(f"策略演进 Loop {loop_index} 等待超时（{max_wait}s），标记为 failed")
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s AND status = 'running'",
                                (loop_id,),
                            )
                        conn.commit()
                    continue

                # 处理完成的 Loop
                if loop_id:
                    await self._safe_process_completed_loop(task_id, loop_id)

        else:
            # 并行模式：使用 semaphore 控制并行度（持有到 loop 完成）
            sem = asyncio.Semaphore(parallelism)
            logger.info(f"策略演进 {task_id} 并行模式启动，并行度={parallelism}，共 {len(loops_to_run)} 个 Loop（跳过 {len(loops_config) - len(loops_to_run)} 个已完成）")

            async def run_with_sem(loop_config):
                loop_index = loop_config.get("loop_index", len(loops_config))
                async with sem:
                    loop_id = await self.submit_strategy_evo_loop(task_id, loop_index)
                    if not loop_id:
                        return None
                    # 等待 loop 完成后才释放 semaphore
                    max_wait = 7200
                    waited = 0
                    interval = 10
                    while waited < max_wait:
                        await asyncio.sleep(interval)
                        waited += interval
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute("SELECT status FROM qe_evolution_loops WHERE loop_id = %s", (loop_id,))
                                row = cur.fetchone()
                        if not row or row[0] in ("completed", "failed", "cancelled"):
                            break
                    if waited >= max_wait:
                        logger.error(f"并行模式 Loop {loop_index} 等待超时（{max_wait}s）")
                    if loop_id:
                        await self._safe_process_completed_loop(task_id, loop_id)
                    return loop_id

            tasks = [run_with_sem(lc) for lc in loops_to_run]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"并行模式 Loop {loops_to_run[i].get('loop_index', i+1)} 提交失败: {result}")

    async def process_strategy_evo_completed_loop(self, task_id: str, loop_id_str: str) -> bool:
        """
        处理策略演进 Loop 的完成事件（简化版 process_completed_loop）。
        跳过 Agent 分析、SOTA 判定，只收集 metrics。
        """
        # 提取 loop_index
        loop_suffix = loop_id_str.rsplit("_Loop", 1)
        if len(loop_suffix) != 2:
            logger.error(f"Invalid loop_id format: {loop_id_str}")
            return False
        try:
            loop_index = int(loop_suffix[1])
        except ValueError:
            logger.error(f"Invalid loop_index in loop_id: {loop_id_str}")
            return False

        evolution_loop_db_id = loop_id_str

        # CAS 幂等保护
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    UPDATE qe_evolution_loops SET status = 'processing', updated_at = NOW()
                    WHERE loop_id = %s AND status = 'running'
                    RETURNING loop_id
                """, (evolution_loop_db_id,))
                cas_row = cur.fetchone()
            conn.commit()

        if not cas_row:
            logger.info(f"Loop {evolution_loop_db_id} is not in 'running' state, skipping.")
            return False

        try:
            # 获取回测结果
            execution_node_id = self._get_loop_node_id(task_id, evolution_loop_db_id)
            client = self._get_workspace_client_for_loop(task_id, evolution_loop_db_id)
            loop_id = f"Loop{loop_index}"
            metrics = await client.get_loop_metrics(task_id, loop_id)
            metrics.setdefault("execution_trace", {})["node_id"] = execution_node_id
            metrics["execution_node_id"] = execution_node_id

            # Normalize metric keys
            _METRIC_ALIASES = {
                "Rank IC": "Rank_IC",
                "1day.excess_return_with_cost.information_ratio": "sharpe",
                "1day.excess_return_with_cost.annualized_return": "annualized_return",
                "1day.excess_return_without_cost.annualized_return": "annualized_return_no_cost",
                "1day.excess_return_with_cost.max_drawdown": "max_drawdown",
                "1day.excess_return_without_cost.information_ratio": "sharpe_no_cost",
                "1day.excess_return_without_cost.max_drawdown": "max_drawdown_no_cost",
                "1day.excess_return_with_cost.mean": "daily_return",
                "1day.excess_return_without_cost.mean": "daily_return_no_cost",
            }
            for src, dst in _METRIC_ALIASES.items():
                if src in metrics and dst not in metrics:
                    metrics[dst] = metrics[src]

            # 拉取增强诊断指标（与 process_completed_loop:1227-1234 对齐）
            # 失败时 raise — 前端 on-demand API 依赖 metrics_json 中的 enhanced_metrics 字段
            enhanced_data = await client.get_enhanced_metrics(task_id, loop_id)
            td = enhanced_data.get("training_diagnostics", {})
            if not td and "training_curves" in enhanced_data:
                td = self._compute_training_diagnostics(enhanced_data.get("training_curves", {}))
                enhanced_data["training_diagnostics"] = td
            metrics["enhanced_metrics"] = enhanced_data

            # 读取配置
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT config_json FROM qe_evolution_loops WHERE loop_id = %s", (evolution_loop_db_id,))
                    loop_row = cur.fetchone()
            if not loop_row or not loop_row.get('config_json'):
                raise ValueError(f"Loop {evolution_loop_db_id} config_json 为空，数据完整性异常")
            config = loop_row['config_json']
            if isinstance(config, str):
                config = json.loads(config)
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT base_experiment_id FROM qe_evolution_tasks WHERE task_id = %s",
                        (task_id,),
                    )
                    task_parent_row = cur.fetchone()
            history_parent_experiment_id = (
                task_parent_row.get("base_experiment_id")
                if task_parent_row and task_parent_row.get("base_experiment_id")
                else task_id
            )
            experiment_custom_params = merge_qe_minute_runtime_contract(
                config.get("model_params", {}),
                config=config,
                source="strategy_evo_loop_completion",
                allow_default_execution_algo=False,
            )

            # 更新 LOOP 记录
            with get_conn() as conn:
                with conn.cursor() as cur:
                    experiment_id = f"{task_id}_L{loop_index}"
                    cur.execute("""
                        INSERT INTO qe_experiments
                        (experiment_id, experiment_name, qe_task_id, qe_loop_id,
                         loop_index, parent_experiment_id,
                         is_evolution_loop, factor_names, model_id, strategy_id,
                         data_split, custom_params,
                         result_metrics, status, is_sota)
                        VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s, %s, 'completed', FALSE)
                        ON CONFLICT (experiment_id) DO UPDATE SET
                            result_metrics = EXCLUDED.result_metrics,
                            status = EXCLUDED.status,
                            parent_experiment_id = EXCLUDED.parent_experiment_id,
                            qe_task_id = EXCLUDED.qe_task_id,
                            qe_loop_id = EXCLUDED.qe_loop_id,
                            custom_params = EXCLUDED.custom_params
                    """, (
                        experiment_id,
                        f"{task_id} 策略回测{loop_index}",
                        task_id, loop_id, loop_index, history_parent_experiment_id,
                        json.dumps(config.get("factor_list", [])),
                        config.get("model_id"), config.get("strategy_id"),
                        json.dumps(config.get("data_split", {})),
                        json.dumps(experiment_custom_params),
                        json.dumps(metrics),
                    ))

                    cur.execute("""
                        UPDATE qe_evolution_loops
                        SET metrics_json = %s, status = 'completed',
                            experiment_id = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(metrics), experiment_id, evolution_loop_db_id))
                conn.commit()

            # 更新任务进度
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT current_loop, max_loops FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                    task_row = cur.fetchone()

            self._archive_completed_loop_best_effort(task_id, evolution_loop_db_id, loop_index)
            self._record_research_backtest_best_effort(task_id, evolution_loop_db_id, loop_index)

            current_loop = task_row.get("current_loop", 0) if task_row else 0
            max_loops = task_row.get("max_loops", 0) if task_row else 0

            if loop_index > current_loop:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE qe_evolution_tasks SET current_loop = %s, updated_at = NOW() WHERE task_id = %s", (loop_index, task_id))
                    conn.commit()

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT status, COUNT(*) FROM qe_evolution_loops WHERE task_id = %s GROUP BY status",
                        (task_id,),
                    )
                    status_counts = {status: count for status, count in cur.fetchall()}
            terminal_statuses = {"completed", "failed", "cancelled", "canceled"}
            completed_count = int(status_counts.get("completed", 0))
            failed_count = int(status_counts.get("failed", 0))
            cancelled_count = int(status_counts.get("cancelled", 0)) + int(status_counts.get("canceled", 0))
            terminal_count = completed_count + failed_count + cancelled_count
            nonterminal_count = sum(
                int(count) for status, count in status_counts.items()
                if status not in terminal_statuses
            )

            # Parallel loops can finish out of order. Only finalize the task
            # after every configured loop is terminal, then require all loops
            # to be completed before declaring task success.
            if max_loops and terminal_count >= max_loops and nonterminal_count == 0:
                final_status = derive_custom_evo_final_status(max_loops, status_counts)
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE qe_evolution_tasks SET status = %s, updated_at = NOW() "
                            "WHERE task_id = %s AND status = 'running'",
                            (final_status, task_id),
                        )
                    conn.commit()
                logger.info(
                    "Strategy evolution task %s final status=%s "
                    "(expected=%s, terminal=%s, completed=%s, failed=%s, cancelled=%s)",
                    task_id,
                    final_status,
                    max_loops,
                    terminal_count,
                    completed_count,
                    failed_count,
                    cancelled_count,
                )
            else:
                # 串行模式：提交下一个 Loop
                execution_mode = task_row.get("strategy_evo_execution_mode", "serial") if task_row else "serial"
                if execution_mode == "serial":
                    next_loop_index = loop_index + 1
                    next_task = asyncio.create_task(self.submit_strategy_evo_loop(task_id, next_loop_index))
                    next_task.add_done_callback(
                        lambda t: logger.error(f"submit_strategy_evo_loop failed: {t.exception()}") if t.exception() else None
                    )

            return True

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"策略演进 Loop {evolution_loop_db_id} 处理失败: {e}\n{tb_str}")
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
                        cur.execute("UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s", (error_detail, evolution_loop_db_id))
                        cur.execute("UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s", (task_id,))
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"FATAL: 策略演进 Loop {evolution_loop_db_id} 失败标记失败: {db_err}")
            return False

    # ═══════════════════════════════════════════════════════════════════
    # 自定义演进 (Custom Evolution) — 每个 Loop 完全自定义因子+模型+策略
    # ═══════════════════════════════════════════════════════════════════

    async def create_custom_evo_task(
        self,
        task_name: str,
        target_desc: str = "",
        loops_config: List[Dict[str, Any]] = None,
        execution_mode: str = "serial",
        node_id: Optional[str] = None,
        node_parallelism: Optional[Dict[str, int]] = None,
        engine_mode: str = "unified",
        clone_from_task_id: Optional[str] = None,
        auto_start: bool = True,
    ) -> str:
        """
        创建自定义演进任务。每个 Loop 都可以完全自定义因子、模型、策略配置，
        执行完整的训练+回测流程。
        """
        if not loops_config or len(loops_config) == 0:
            raise ValueError("loops_config 不能为空，至少需要配置一个 Loop")
        if (engine_mode or "unified") != "unified":
            raise ValueError(
                "QE legacy execution engine has been removed; only engine_mode='unified' is supported."
            )
        loops_config, loop1_node_id, selected_node_ids = resolve_custom_loop_nodes(
            [dict(loop_cfg) for loop_cfg in loops_config],
            node_id,
        )
        node_parallelism = normalize_node_parallelism(selected_node_ids, node_parallelism)

        normalized_loops = []
        for idx, loop_cfg in enumerate(loops_config, start=1):
            cfg = dict(loop_cfg)
            ensure_loop_fixed_seed(cfg, context=f"custom_evo.loops[{idx}]")
            if cfg.get("backtest_only") and "source_label_horizon" not in cfg:
                if not cfg.get("model_source_task_id") or cfg.get("model_source_loop_index") is None:
                    raise ValueError(f"Loop {idx}: backtest-only requires model_source before label_horizon validation")
                cfg["source_label_horizon"] = self._get_source_loop_label_horizon(
                    cfg["model_source_task_id"],
                    int(cfg["model_source_loop_index"]),
                )
            label_horizon = normalize_label_horizon(
                cfg.get("label_horizon"),
                field_name=f"custom_evo.loops[{idx}].label_horizon",
            )
            if cfg.get("backtest_only"):
                source_horizon = normalize_label_horizon(
                    cfg.get("source_label_horizon"),
                    field_name=f"custom_evo.loops[{idx}].source_label_horizon",
                )
                if label_horizon != source_horizon:
                    raise ValueError(
                        f"Loop {idx}: backtest-only label_horizon={label_horizon} "
                        f"does not match source model label_horizon={source_horizon}"
                    )
            cfg["label_horizon"] = label_horizon
            normalized_loops.append(cfg)
        loops_config = normalized_loops
        node_id = loop1_node_id

        # 生成 task_id
        suffix = uuid.uuid4().hex[:4]
        new_task_id = f"qe_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{suffix}"

        # 用第一个 Loop 的配置创建 base experiment 记录
        first_loop = loops_config[0]
        factor_names = [k.split("||")[0] for k in first_loop.get("factor_keys", [])]
        base_exp_id = f"{new_task_id}_base"
        first_custom_params = dict(first_loop.get("strategy_params") or {})
        if first_loop.get("label_type"):
            first_custom_params["label_type"] = first_loop["label_type"]
        if bool(first_loop.get("disable_alpha158", False)):
            first_custom_params["disable_alpha158"] = True
        first_label_horizon = normalize_label_horizon(first_loop.get("label_horizon"))
        if first_label_horizon != DEFAULT_LABEL_HORIZON:
            first_custom_params["label_horizon"] = first_label_horizon
        first_custom_params = merge_qe_minute_runtime_contract(
            first_custom_params,
            config=first_loop,
            execution_algo=first_loop.get("execution_algo"),
            execution_algo_params=first_loop.get("execution_algo_params"),
            source="custom_evo_base_experiment",
            allow_default_execution_algo=True,
        )

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_experiments
                    (experiment_id, experiment_name, qe_task_id,
                     is_evolution_loop, factor_names, model_id, strategy_id,
                     data_split, custom_params, status, is_sota)
                    VALUES (%s, %s, %s, FALSE, %s, %s, %s, %s, %s, 'created', FALSE)
                """, (
                    base_exp_id,
                    f"自定义演进基准 {task_name}",
                    new_task_id,
                    json.dumps(factor_names),
                    first_loop.get("model_id"),
                    first_loop.get("strategy_id"),
                    json.dumps(first_loop.get("data_split") or {}),
                    json.dumps(first_custom_params),
                ))

                # 创建自定义演进任务
                cur.execute("""
                    INSERT INTO qe_evolution_tasks
                    (task_id, task_name, target_desc, max_loops, current_loop, status,
                     base_experiment_id, node_id, source_type,
                     task_type, strategy_evo_config, strategy_evo_execution_mode, label_horizon)
                    VALUES (%s, %s, %s, %s, 0, 'pending', %s, %s, 'custom',
                            'custom_evo', %s, %s, %s)
                """, (
                    new_task_id, task_name, target_desc, len(loops_config),
                    base_exp_id, node_id,
                    json.dumps({
                        "loops": loops_config,
                        "engine_mode": engine_mode,
                        "node_parallelism": node_parallelism,
                        "node_resolution_policy": "loop1_inherit_v1",
                        "clone_from_task_id": clone_from_task_id,
                    }),
                    execution_mode,
                    first_label_horizon,
                ))
            conn.commit()

        logger.info(
            f"创建自定义演进任务 {new_task_id}, "
            f"共 {len(loops_config)} 个 Loop, 执行方式={execution_mode}"
        )

        # Template materialization can create the DB task without starting execution.
        if auto_start:
            bg_task = asyncio.create_task(self.submit_custom_evo_all_loops(new_task_id))
            bg_task.add_done_callback(
                lambda t: logger.error(f"submit_custom_evo_all_loops failed: {t.exception()}") if t.exception() else None
            )

        return new_task_id


    def _parse_custom_evo_strategy_config(self, raw_config: Any, *, task_id: str) -> Dict[str, Any]:
        if raw_config in (None, ""):
            raise ValueError(f"custom_evo task {task_id} has empty strategy_evo_config")
        if isinstance(raw_config, str):
            parsed = json.loads(raw_config)
        elif isinstance(raw_config, dict):
            parsed = dict(raw_config)
        else:
            raise ValueError(f"custom_evo task {task_id} has invalid strategy_evo_config type: {type(raw_config).__name__}")
        loops = parsed.get("loops")
        if not isinstance(loops, list) or not loops:
            raise ValueError(f"custom_evo task {task_id} has no editable strategy_evo_config.loops")
        normalized_loops: List[Dict[str, Any]] = []
        for pos, loop_cfg in enumerate(loops, start=1):
            if not isinstance(loop_cfg, dict):
                raise ValueError(f"custom_evo task {task_id} loop config at position {pos} is not an object")
            cfg = dict(loop_cfg)
            cfg["loop_index"] = int(cfg.get("loop_index") or pos)
            normalized_loops.append(cfg)
        parsed["loops"] = sorted(normalized_loops, key=lambda item: int(item.get("loop_index") or 0))
        return parsed

    async def get_custom_evo_editable_config(self, task_id: str) -> Dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()
                if not task:
                    raise ValueError(f"custom_evo task not found: {task_id}")
                if task.get("task_type") != "custom_evo":
                    raise ValueError(f"task {task_id} is not a custom_evo task")
                cur.execute(
                    """
                    SELECT loop_index, loop_id, status, node_id, experiment_id, updated_at
                    FROM qe_evolution_loops
                    WHERE task_id = %s
                    ORDER BY loop_index ASC
                    """,
                    (task_id,),
                )
                loop_rows = [dict(row) for row in cur.fetchall()]

        strategy_config = self._parse_custom_evo_strategy_config(
            task.get("strategy_evo_config"),
            task_id=task_id,
        )
        loop_status_by_index = {int(row["loop_index"]): row for row in loop_rows}
        editable_loops: List[Dict[str, Any]] = []
        for loop_cfg in strategy_config["loops"]:
            cfg = dict(loop_cfg)
            status_row = loop_status_by_index.get(int(cfg.get("loop_index") or 0))
            if status_row and not cfg.get("node_id") and status_row.get("node_id"):
                cfg["node_id"] = status_row.get("node_id")
            editable_loops.append(cfg)

        failed_loop_indexes = [
            row["loop_index"]
            for row in loop_rows
            if row.get("status") in ("failed", "cancelled", "canceled")
        ]
        return {
            "task_id": task_id,
            "task_name": task.get("task_name"),
            "target_desc": task.get("target_desc") or "",
            "task_type": task.get("task_type"),
            "status": task.get("status"),
            "node_id": task.get("node_id"),
            "execution_mode": task.get("strategy_evo_execution_mode") or "serial",
            "engine_mode": strategy_config.get("engine_mode") or "unified",
            "node_parallelism": strategy_config.get("node_parallelism") or {},
            "node_resolution_policy": strategy_config.get("node_resolution_policy") or "loop1_inherit_v1",
            "loops": editable_loops,
            "loop_statuses": loop_rows,
            "failed_loop_indexes": failed_loop_indexes,
            "config_source": "strategy_evo_config.loops",
        }

    def _acquire_custom_evo_mutation_lock(self, conn, task_id: str) -> None:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (f"qe_custom_evo:{task_id}",))
            row = cur.fetchone()
            if not row or not row[0]:
                raise ValueError(f"custom_evo task {task_id} is already being modified; please retry later")
        conn.commit()

    def _release_custom_evo_mutation_lock(self, conn, task_id: str) -> None:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (f"qe_custom_evo:{task_id}",))
            conn.commit()
        except Exception as exc:
            logger.error("Failed to release custom_evo mutation lock for %s: %s", task_id, exc)

    def _cleanup_local_custom_evo_loop_dirs(self, task_id: str, loop_index: int) -> List[str]:
        from .config_composer import QE_EXPERIMENTS_ROOT

        loop_name = f"Loop{loop_index}"
        cleanup_roots = [QE_EXPERIMENTS_ROOT, Path(SOTA_ASSETS_DIR)]
        targets = [
            QE_EXPERIMENTS_ROOT / task_id / loop_name,
            Path(SOTA_ASSETS_DIR) / task_id / loop_name,
        ]
        cleaned: List[str] = []
        for target in targets:
            if remove_aistock_artifact_tree(
                target,
                purpose=f"QE custom_evo local loop artifact cleanup: {task_id}/{loop_name}",
                allowed_roots=cleanup_roots,
            ):
                cleaned.append(str(target))
        return cleaned

    async def delete_custom_evo_loop_result(self, task_id: str, loop_index: int) -> Dict[str, Any]:
        loop_db_id = f"{task_id}_Loop{loop_index}"
        loop_name = f"Loop{loop_index}"
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()
                if not task:
                    raise ValueError(f"custom_evo task not found: {task_id}")
                if task.get("task_type") != "custom_evo":
                    raise ValueError(f"task {task_id} is not a custom_evo task")
                strategy_config = self._parse_custom_evo_strategy_config(task.get("strategy_evo_config"), task_id=task_id)
                target_cfg = next((cfg for cfg in strategy_config["loops"] if int(cfg.get("loop_index") or 0) == loop_index), None)
                if not target_cfg:
                    raise ValueError(f"Loop {loop_index} is not configured in custom_evo task {task_id}")
                cur.execute("SELECT * FROM qe_evolution_loops WHERE loop_id = %s", (loop_db_id,))
                loop_row = cur.fetchone()

        old_node_id = (
            (loop_row or {}).get("node_id")
            or (target_cfg or {}).get("node_id")
            or task.get("node_id")
            or resolve_default_qe_node_id()
        )
        old_status = (loop_row or {}).get("status")
        old_experiment_id = (loop_row or {}).get("experiment_id") or f"{task_id}_L{loop_index}"
        client = self._get_workspace_client_for_node_id(old_node_id)

        if old_status in ("running", "processing"):
            await client.kill_loop(task_id, loop_name)

        try:
            await client.cleanup_loop_workspace(task_id, loop_name)
            remote_cleanup = {
                "ok": True,
                "method": "rdagent_api",
                "node_id": old_node_id,
            }
        except QELoopWorkspaceCleanupUnavailable as exc:
            logger.error(
                "Loop-level RD-Agent cleanup unavailable for %s/%s on node %s; "
                "direct node filesystem fallback is disabled.",
                task_id,
                loop_name,
                old_node_id,
            )
            raise RuntimeError(
                "RD-Agent node API must expose loop-level cleanup before this "
                "custom_evo loop can be deleted; direct worker filesystem cleanup is forbidden"
            ) from exc
        cleaned_dirs = self._cleanup_local_custom_evo_loop_dirs(task_id, loop_index)

        deleted_counts: Dict[str, int] = {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                if old_experiment_id:
                    cur.execute(
                        "DELETE FROM qe_factor_experiment_metrics WHERE experiment_id = %s",
                        (old_experiment_id,),
                    )
                    deleted_counts["qe_factor_experiment_metrics"] = cur.rowcount
                cur.execute("DELETE FROM qe_evolution_loops WHERE loop_id = %s", (loop_db_id,))
                deleted_counts["qe_evolution_loops"] = cur.rowcount
                if old_experiment_id:
                    cur.execute("DELETE FROM qe_experiments WHERE experiment_id = %s", (old_experiment_id,))
                    deleted_counts["qe_experiments"] = cur.rowcount
            conn.commit()

        return {
            "loop_id": loop_db_id,
            "old_status": old_status,
            "old_node_id": old_node_id,
            "old_experiment_id": old_experiment_id,
            "remote_cleanup": remote_cleanup,
            "deleted_counts": deleted_counts,
            "cleaned_dirs": cleaned_dirs,
        }

    def _normalize_full_custom_evo_nodes(
        self,
        loops_config: List[Dict[str, Any]],
        task_node_id: Optional[str],
        raw_node_parallelism: Optional[Dict[str, int]],
    ) -> tuple[List[Dict[str, Any]], str, Dict[str, int]]:
        loops_config, loop1_node_id, selected_node_ids = resolve_custom_loop_nodes(
            [dict(loop_cfg) for loop_cfg in loops_config],
            task_node_id,
        )
        node_parallelism = normalize_node_parallelism(selected_node_ids, raw_node_parallelism)
        return loops_config, loop1_node_id, node_parallelism

    async def rerun_custom_evo_loop(
        self,
        task_id: str,
        loop_index: int,
        loop_config: Dict[str, Any],
        execution_mode: str = "serial",
        node_id: Optional[str] = None,
        node_parallelism: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Any]:
        lock_cm = get_conn()
        lock_conn = lock_cm.__enter__()
        try:
            self._acquire_custom_evo_mutation_lock(lock_conn, task_id)
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                    task = cur.fetchone()
                    if not task:
                        raise ValueError(f"custom_evo task not found: {task_id}")
                    if task.get("task_type") != "custom_evo":
                        raise ValueError(f"task {task_id} is not a custom_evo task")
                    strategy_config = self._parse_custom_evo_strategy_config(task.get("strategy_evo_config"), task_id=task_id)
                    replaced = False
                    next_loops: List[Dict[str, Any]] = []
                    replacement = dict(loop_config)
                    replacement["loop_index"] = loop_index
                    for cfg in strategy_config["loops"]:
                        if int(cfg.get("loop_index") or 0) == loop_index:
                            next_loops.append(replacement)
                            replaced = True
                        else:
                            next_loops.append(dict(cfg))
                    if not replaced:
                        raise ValueError(f"Loop {loop_index} is not configured in custom_evo task {task_id}")

                    resolved_loops, loop1_node_id, full_node_parallelism = self._normalize_full_custom_evo_nodes(
                        next_loops,
                        node_id if node_id is not None else task.get("node_id"),
                        node_parallelism,
                    )

            cleanup_result = await self.delete_custom_evo_loop_result(task_id, loop_index)
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    current_loop = max(int(cfg.get("loop_index") or 0) for cfg in resolved_loops)
                    strategy_config["loops"] = resolved_loops
                    strategy_config["engine_mode"] = "unified"
                    strategy_config["node_parallelism"] = full_node_parallelism
                    strategy_config["node_resolution_policy"] = "loop1_inherit_v1"
                    cur.execute(
                        """
                        UPDATE qe_evolution_tasks
                        SET strategy_evo_config = %s,
                            strategy_evo_execution_mode = %s,
                            node_id = %s,
                            max_loops = %s,
                            current_loop = GREATEST(COALESCE(current_loop, 0), %s),
                            status = 'running',
                            updated_at = NOW()
                        WHERE task_id = %s
                        """,
                        (
                            json.dumps(strategy_config),
                            execution_mode,
                            loop1_node_id,
                            len(resolved_loops),
                            min(loop_index, current_loop),
                            task_id,
                        ),
                    )
                conn.commit()

            return {
                "task_id": task_id,
                "loop_index": loop_index,
                "loop_id": f"{task_id}_Loop{loop_index}",
                "execution_mode": execution_mode,
                "node_parallelism": full_node_parallelism,
                "cleanup": cleanup_result,
                "message": f"Custom evolution Loop {loop_index} rerun queued.",
            }
        finally:
            try:
                self._release_custom_evo_mutation_lock(lock_conn, task_id)
            finally:
                lock_cm.__exit__(None, None, None)

    async def append_custom_evo_loops(
        self,
        task_id: str,
        loops_config: List[Dict[str, Any]],
        execution_mode: str = "serial",
        node_id: Optional[str] = None,
        node_parallelism: Optional[Dict[str, int]] = None,
        ack_failed_loop_warning: bool = False,
    ) -> Dict[str, Any]:
        if not loops_config:
            raise ValueError("append_custom_evo_loops requires at least one loop config")
        lock_cm = get_conn()
        lock_conn = lock_cm.__enter__()
        try:
            self._acquire_custom_evo_mutation_lock(lock_conn, task_id)
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                    task = cur.fetchone()
                    if not task:
                        raise ValueError(f"custom_evo task not found: {task_id}")
                    if task.get("task_type") != "custom_evo":
                        raise ValueError(f"task {task_id} is not a custom_evo task")
                    cur.execute(
                        """
                        SELECT loop_index, status FROM qe_evolution_loops
                        WHERE task_id = %s AND status IN ('failed', 'cancelled', 'canceled')
                        ORDER BY loop_index ASC
                        """,
                        (task_id,),
                    )
                    failed_rows = [dict(row) for row in cur.fetchall()]
                    if failed_rows and not ack_failed_loop_warning:
                        raise ValueError(
                            "Existing failed/cancelled loops require ack_failed_loop_warning=true before appending: "
                            + ", ".join(f"Loop{row['loop_index']}={row['status']}" for row in failed_rows)
                        )

                    strategy_config = self._parse_custom_evo_strategy_config(task.get("strategy_evo_config"), task_id=task_id)
                    existing_loops = [dict(cfg) for cfg in strategy_config["loops"]]
                    max_loop_index = max(int(cfg.get("loop_index") or 0) for cfg in existing_loops)
                    new_loop_indexes: List[int] = []
                    assigned_new_loops: List[Dict[str, Any]] = []
                    for offset, cfg in enumerate(loops_config, start=1):
                        next_cfg = dict(cfg)
                        next_index = max_loop_index + offset
                        next_cfg["loop_index"] = next_index
                        new_loop_indexes.append(next_index)
                        assigned_new_loops.append(next_cfg)

                    combined_loops = existing_loops + assigned_new_loops
                    resolved_loops, loop1_node_id, full_node_parallelism = self._normalize_full_custom_evo_nodes(
                        combined_loops,
                        node_id if node_id is not None else task.get("node_id"),
                        node_parallelism,
                    )
                    strategy_config["loops"] = resolved_loops
                    strategy_config["engine_mode"] = "unified"
                    strategy_config["node_parallelism"] = full_node_parallelism
                    strategy_config["node_resolution_policy"] = "loop1_inherit_v1"
                    cur.execute(
                        """
                        UPDATE qe_evolution_tasks
                        SET strategy_evo_config = %s,
                            strategy_evo_execution_mode = %s,
                            node_id = %s,
                            max_loops = %s,
                            current_loop = GREATEST(COALESCE(current_loop, 0), %s),
                            status = 'running',
                            updated_at = NOW()
                        WHERE task_id = %s
                        """,
                        (
                            json.dumps(strategy_config),
                            execution_mode,
                            loop1_node_id,
                            len(resolved_loops),
                            max_loop_index,
                            task_id,
                        ),
                    )
                conn.commit()
            return {
                "task_id": task_id,
                "new_loop_indexes": new_loop_indexes,
                "total_loops": len(resolved_loops),
                "execution_mode": execution_mode,
                "node_parallelism": full_node_parallelism,
                "existing_failed_loop_indexes": [row["loop_index"] for row in failed_rows],
                "message": f"Appended {len(new_loop_indexes)} custom evolution loops.",
            }
        finally:
            try:
                self._release_custom_evo_mutation_lock(lock_conn, task_id)
            finally:
                lock_cm.__exit__(None, None, None)

    async def _wait_and_process_custom_evo_loop(self, task_id: str, loop_index: int, loop_id: str) -> None:
        max_wait = 14400
        waited = 0
        interval = 15
        final_status = None
        while waited < max_wait:
            await asyncio.sleep(interval)
            waited += interval
            if self._get_task_status(task_id) != "running":
                logger.info("Custom evolution task %s stopped while waiting for Loop %s", task_id, loop_index)
                return
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT status FROM qe_evolution_loops WHERE loop_id = %s", (loop_id,))
                    row = cur.fetchone()
            final_status = row[0] if row else None
            if not row or final_status in ("completed", "failed", "cancelled", "canceled"):
                break
        if waited >= max_wait:
            logger.error("Custom evolution Loop %s wait timed out (%ss); marking failed", loop_index, max_wait)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s AND status = 'running'",
                        (loop_id,),
                    )
                conn.commit()
            return
        if final_status == "completed":
            await self._safe_process_completed_loop(task_id, loop_id)
        else:
            logger.info("Custom evolution Loop %s ended with status=%s; skip completed-loop processing", loop_index, final_status)

    def recompute_custom_evo_task_status(self, task_id: str) -> Optional[str]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT strategy_evo_config FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()
                if not task:
                    return None
                strategy_config = self._parse_custom_evo_strategy_config(task.get("strategy_evo_config"), task_id=task_id)
                expected_count = len(strategy_config["loops"])
                cur.execute(
                    "SELECT status, COUNT(*) FROM qe_evolution_loops WHERE task_id = %s GROUP BY status",
                    (task_id,),
                )
                status_counts = {status: count for status, count in cur.fetchall()}
                terminal_statuses = {"completed", "failed", "cancelled", "canceled"}
                active_count = sum(
                    int(count) for status, count in status_counts.items()
                    if status not in terminal_statuses
                )
                terminal_count = sum(
                    int(count) for status, count in status_counts.items()
                    if status in terminal_statuses
                )
                if active_count > 0:
                    final_status = "running"
                elif terminal_count >= expected_count:
                    final_status = derive_custom_evo_final_status(expected_count, status_counts)
                else:
                    final_status = "failed"
                cur.execute(
                    "UPDATE qe_evolution_tasks SET status = %s, updated_at = NOW() WHERE task_id = %s",
                    (final_status, task_id),
                )
            conn.commit()
        return final_status

    async def submit_custom_evo_selected_loops(
        self,
        task_id: str,
        loop_indexes: List[int],
        force_full_train: bool = False,
    ) -> Dict[str, Any]:
        selected_indexes = sorted({int(idx) for idx in loop_indexes})
        if not selected_indexes:
            raise ValueError("submit_custom_evo_selected_loops requires at least one loop index")
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()
        if not task:
            raise ValueError(f"Task {task_id} not found")
        if task.get("task_type") != "custom_evo":
            raise ValueError(f"task {task_id} is not a custom_evo task")
        strategy_config = self._parse_custom_evo_strategy_config(task.get("strategy_evo_config"), task_id=task_id)
        engine_mode = strategy_config.get("engine_mode") or "unified"
        if engine_mode != "unified":
            raise ValueError("QE legacy execution engine has been removed; only engine_mode='unified' is supported.")

        loops_config, loop1_node_id, selected_node_ids = resolve_custom_loop_nodes(
            [dict(loop_cfg) for loop_cfg in strategy_config["loops"]],
            task.get("node_id"),
        )
        node_parallelism = normalize_node_parallelism(
            selected_node_ids,
            strategy_config.get("node_parallelism"),
        )
        selected_configs = [cfg for cfg in loops_config if int(cfg.get("loop_index") or 0) in selected_indexes]
        found_indexes = {int(cfg.get("loop_index") or 0) for cfg in selected_configs}
        missing = sorted(set(selected_indexes) - found_indexes)
        if missing:
            raise ValueError(f"Loop configuration missing for selected custom_evo loops: {missing}")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE qe_evolution_tasks SET status = 'running', updated_at = NOW() WHERE task_id = %s",
                    (task_id,),
                )
            conn.commit()
        task = dict(task)
        task["node_id"] = loop1_node_id

        execution_mode_raw = task.get("strategy_evo_execution_mode") or "serial"
        if execution_mode_raw.startswith("parallel"):
            mode = "parallel"
        else:
            mode = "serial"

        submitted: List[str] = []
        if mode == "serial":
            for loop_config in selected_configs:
                loop_index = int(loop_config.get("loop_index"))
                if self._get_task_status(task_id) != "running":
                    logger.info("Custom evolution task %s stopped before selected Loop %s", task_id, loop_index)
                    break
                loop_id = await self.submit_custom_evo_loop(task_id, loop_index, force_full_train=force_full_train)
                if not loop_id:
                    logger.error("Custom evolution selected Loop %s submit failed; stop serial batch", loop_index)
                    break
                submitted.append(loop_id)
                await self._wait_and_process_custom_evo_loop(task_id, loop_index, loop_id)
        else:
            semaphores = {node: asyncio.Semaphore(limit) for node, limit in node_parallelism.items()}

            async def run_selected(loop_config: Dict[str, Any]) -> Optional[str]:
                loop_index = int(loop_config.get("loop_index"))
                loop_node_id = loop_config.get("node_id") or loop1_node_id
                sem = semaphores[loop_node_id]
                async with sem:
                    if self._get_task_status(task_id) != "running":
                        logger.info("Custom evolution task %s stopped before selected Loop %s", task_id, loop_index)
                        return None
                    loop_id = await self.submit_custom_evo_loop(task_id, loop_index, force_full_train=force_full_train)
                    if not loop_id:
                        return None
                    await self._wait_and_process_custom_evo_loop(task_id, loop_index, loop_id)
                    return loop_id

            results = await asyncio.gather(*(run_selected(cfg) for cfg in selected_configs), return_exceptions=True)
            for cfg, result in zip(selected_configs, results):
                if isinstance(result, Exception):
                    logger.error("Selected custom_evo Loop %s failed in parallel batch: %s", cfg.get("loop_index"), result)
                elif result:
                    submitted.append(result)

        final_status = self.recompute_custom_evo_task_status(task_id)
        return {
            "task_id": task_id,
            "selected_loop_indexes": selected_indexes,
            "submitted_loop_ids": submitted,
            "final_status": final_status,
        }

    async def submit_custom_evo_loop(self, task_id: str, loop_index: int, force_full_train: bool = False) -> Optional[str]:
        """
        提交单个自定义演进 Loop（完整训练+回测）。
        从 strategy_evo_config.loops 中读取该 Loop 的完整因子+模型+策略配置。
        force_full_train=True 时，忽略 backtest_only，强制完整训练。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            logger.error(f"Task {task_id} not found")
            return None

        custom_evo_config = task.get("strategy_evo_config")
        if not custom_evo_config:
            logger.error(f"Task {task_id} strategy_evo_config 为空")
            return None
        if isinstance(custom_evo_config, str):
            custom_evo_config = json.loads(custom_evo_config)

        loops_config = custom_evo_config.get("loops", [])
        loop_config = None
        for cfg in loops_config:
            if cfg.get("loop_index") == loop_index:
                loop_config = cfg
                break

        if not loop_config:
            logger.error(f"Loop {loop_index} 的配置未找到")
            raise ValueError(f"Loop configuration not found for loop_index={loop_index} in task {task_id}")

        # The legacy custom-evolution executor has been retired; only the unified path may run.
        _engine_mode = custom_evo_config.get("engine_mode") or "unified"
        if _engine_mode != "unified":
            raise ValueError(
                "QE legacy execution engine has been removed; "
                "only engine_mode='unified' is supported."
            )
        return await self._submit_custom_evo_loop_unified(
            task_id,
            loop_index,
            loop_config,
            task,
            force_full_train=force_full_train,
        )

    async def _submit_custom_evo_loop_unified(
        self,
        task_id: str,
        loop_index: int,
        loop_config: Dict[str, Any],
        task: Dict[str, Any],
        force_full_train: bool = False,
    ) -> Optional[str]:
        """
        统一引擎路径：使用 ExperimentConfig + BacktestExecutor 提交自定义演进 Loop。
        替代 submit_custom_evo_loop 中的手动参数组装代码。
        """
        from .experiment_config_builders import build_config_from_custom_evo_loop
        from .executors.backtest import BacktestExecutor, BacktestMode
        from .executors.base import ExecutionContext
        from .config_composer import ConfigComposer

        evolution_loop_db_id = f"{task_id}_Loop{loop_index}"
        loop_id = f"Loop{loop_index}"
        effective_node_id = loop_config.get("node_id") or task.get("node_id") or resolve_default_qe_node_id()

        if self._get_task_status(task_id) != "running":
            logger.info(f"Custom evolution task {task_id} is not running; skip submitting Loop {loop_index}")
            return None

        # 创建 LOOP 记录
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT node_id FROM qe_evolution_loops WHERE loop_id = %s", (evolution_loop_db_id,))
                existing_loop = cur.fetchone()
                existing_node_id = existing_loop.get("node_id") if existing_loop else None
                if existing_node_id and existing_node_id != effective_node_id:
                    raise ValueError(
                        f"Loop {evolution_loop_db_id} is locked to node {existing_node_id}; "
                        f"refusing to submit on {effective_node_id}"
                    )
                cur.execute("""
                    INSERT INTO qe_evolution_loops
                    (loop_id, task_id, loop_index, status, action_type, node_id)
                    VALUES (%s, %s, %s, 'running', 'custom_config', %s)
                    ON CONFLICT (loop_id) DO UPDATE SET
                        status = 'running',
                        node_id = COALESCE(qe_evolution_loops.node_id, EXCLUDED.node_id),
                        updated_at = NOW()
                """, (evolution_loop_db_id, task_id, loop_index, effective_node_id))
            conn.commit()

        try:
            loop_config = dict(loop_config)
            ensure_loop_fixed_seed(loop_config, context=f"custom_evo.task[{task_id}].Loop{loop_index}")
            if loop_config.get("backtest_only") and "source_label_horizon" not in loop_config:
                loop_config = dict(loop_config)
                loop_config["source_label_horizon"] = self._get_source_loop_label_horizon(
                    loop_config.get("model_source_task_id"),
                    int(loop_config.get("model_source_loop_index")),
                )
            # 1. 构建 ExperimentConfig（配置层）
            experiment_name = f"{task_id}/{loop_id}"
            cfg = build_config_from_custom_evo_loop(
                loop_config=loop_config,
                task=task,
                experiment_name=experiment_name,
            )

            # 2. 保存 config 记录到 loop
            runtime_flags = cfg.build_runtime_flags()
            requested_seed = runtime_flags.get("random_seed")
            if requested_seed is None and not cfg.backtest_only:
                raise ValueError(f"Loop {loop_index}: runtime_flags.random_seed is required before config persistence")

            config_record = {
                "action_type": "custom_config",
                "label": loop_config.get("label"),
                "factor_list": cfg.factor_names,
                "model_id": cfg.model_id,
                "strategy_id": cfg.strategy_id,
                "strategy_params": cfg.build_strategy_params(),
                "runtime_flags": runtime_flags,
                "execution_algo": cfg.execution_algo,
                "execution_algo_params": cfg.execution_algo_params,
                "disable_alpha158": bool(loop_config.get("disable_alpha158", False)),
                "backtest_only": cfg.backtest_only,
                "model_source_task_id": cfg.model_source_task_id,
                "model_source_loop_index": cfg.model_source_loop_index,
                "source_label_horizon": loop_config.get("source_label_horizon"),
                "stock_pool": cfg.stock_pool,
                "filter_suspended_on_signal": cfg.filter_suspended_on_signal,
                "suspend_filter_strict": cfg.suspend_filter_strict,
                "alpha_mode": loop_config.get("alpha_mode") or "single",
                "multi_alpha_config": loop_config.get("multi_alpha_config"),
                "model_params": cfg.build_custom_params(),
                "data_split": cfg.data_split or {},
                "label_type": cfg.label_type,
                "label_horizon": cfg.label_horizon,
                "node_id": effective_node_id,
                "execution_node_id": effective_node_id,
            }
            loop_model_params = merge_qe_minute_runtime_contract(
                cfg.build_custom_params(),
                config=config_record,
                execution_algo=cfg.execution_algo,
                execution_algo_params=cfg.execution_algo_params,
                source="custom_evo_loop_config",
                allow_default_execution_algo=True,
            )
            config_record["model_params"] = loop_model_params
            runtime_contract = build_qe_minute_runtime_contract(
                custom_params=loop_model_params,
                execution_algo=cfg.execution_algo,
                execution_algo_params=cfg.execution_algo_params,
                source="custom_evo_loop_config",
                allow_default_execution_algo=True,
            )
            if runtime_contract:
                config_record.update(runtime_contract)
            config_record["execution_manifest"] = {
                "schema_version": "qe_execution_manifest_v1",
                "task_id": task_id,
                "loop_index": loop_index,
                "factor_list": list(cfg.factor_names),
                "factor_count": len(cfg.factor_names),
                "model_id": cfg.model_id,
                "strategy_id": cfg.strategy_id,
                "strategy_params": cfg.build_strategy_params(),
                "model_params": loop_model_params,
                "data_split": cfg.data_split or {},
                "label_type": cfg.label_type,
                "label_horizon": cfg.label_horizon,
                "execution_algo": cfg.execution_algo,
                "execution_algo_params": cfg.execution_algo_params or {},
                "runtime_flags": runtime_flags,
                "random_seed": requested_seed,
                "node_id": effective_node_id,
                "backtest_only": cfg.backtest_only,
            }
            config_record["execution_manifest_sha256"] = sha256_json(config_record["execution_manifest"])
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE qe_evolution_loops SET config_json = %s, updated_at = NOW()
                        WHERE loop_id = %s
                    """, (json.dumps(config_record), evolution_loop_db_id))
                conn.commit()

            # 3. 执行层提交
            if self._get_task_status(task_id) != "running":
                logger.info(f"Custom evolution task {task_id} stopped before executor submit for Loop {loop_index}")
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE qe_evolution_loops SET status = 'cancelled', updated_at = NOW() WHERE loop_id = %s AND status = 'running'",
                            (evolution_loop_db_id,),
                        )
                    conn.commit()
                return None

            await preflight_qe_node(effective_node_id)

            composer = ConfigComposer()
            client = self._get_workspace_client_for_node_id(effective_node_id)
            executor = BacktestExecutor(composer, client)
            ctx = ExecutionContext(
                task_id=task_id,
                loop_index=loop_index,
                experiment_name=experiment_name,
                node_id=effective_node_id,
                callback_url=self._get_callback_url_for_node(effective_node_id),
                require_fixed_seed=True,
            )
            # backtest-only 模式：注入 model_source 并切换执行模式
            # force_full_train 可覆盖 backtest_only 配置，用于恢复时源模型不可用的场景
            if cfg.backtest_only and not force_full_train:
                if not cfg.model_source_task_id or cfg.model_source_loop_index is None:
                    raise ValueError(
                        f"Loop {loop_index}: backtest_only=True 但未指定 model_source"
                    )
                source_node_id = self._get_loop_node_id(
                    cfg.model_source_task_id,
                    int(cfg.model_source_loop_index),
                )
                source_client = self._get_workspace_client_for_node_id(source_node_id)
                model_source, extra_experiment_files = await self._build_backtest_only_model_payload(
                    source_client,
                    cfg.model_source_task_id,
                    int(cfg.model_source_loop_index),
                    reason="custom evolution backtest-only",
                )
                logger.info(
                    "custom evolution backtest-only packaged source model: source=%s/Loop%s source_node=%s target_node=%s",
                    cfg.model_source_task_id,
                    cfg.model_source_loop_index,
                    source_node_id or "local",
                    effective_node_id or "local",
                )

                ctx = ExecutionContext(
                    task_id=task_id,
                    loop_index=loop_index,
                    experiment_name=experiment_name,
                    node_id=effective_node_id,
                    callback_url=self._get_callback_url_for_node(effective_node_id),
                    model_source=model_source,
                    extra_experiment_files=extra_experiment_files or None,
                    require_fixed_seed=False,
                )
                mode = BacktestMode.BACKTEST_ONLY
                logger.info(
                    f"[unified] 自定义演进 Loop {loop_index} 使用 backtest-only 模式，"
                    f"model_source={cfg.model_source_task_id}/Loop{cfg.model_source_loop_index}"
                )
            else:
                if force_full_train and cfg.backtest_only:
                    logger.info(
                        f"[unified] 自定义演进 Loop {loop_index} force_full_train=True，"
                        f"忽略 backtest_only 配置，执行完整训练"
                    )
                mode = BacktestMode.FULL_TRAIN
            await executor.submit(cfg, ctx, mode=mode)

            logger.info(f"[unified] 自定义演进 Loop {loop_index} 已提交")
            return evolution_loop_db_id

        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(f"[unified] 自定义演进 Loop {loop_index} 提交失败: {e}\n{tb_str}")
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        error_detail = json.dumps({"_error": str(e), "_traceback": tb_str}, ensure_ascii=False)
                        cur.execute(
                            "UPDATE qe_evolution_loops SET status = 'failed', agent_analysis = %s, updated_at = NOW() WHERE loop_id = %s",
                            (error_detail, evolution_loop_db_id),
                        )
                    conn.commit()
            except Exception as db_err:
                logger.critical(f"[unified] Loop {loop_index} 失败标记失败: {db_err}")
            return None

    async def submit_custom_evo_all_loops(self, task_id: str, force_full_train: bool = False):
        """
        批量调度自定义演进 Loops，支持串行/并行模式。
        与 submit_strategy_evo_all_loops 逻辑相同，但调用 submit_custom_evo_loop。
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                task = cur.fetchone()

        if not task:
            logger.error(f"Task {task_id} not found")
            return

        custom_evo_config = task.get("strategy_evo_config")
        if not custom_evo_config:
            logger.error(f"Task {task_id} strategy_evo_config 为空")
            return
        if isinstance(custom_evo_config, str):
            custom_evo_config = json.loads(custom_evo_config)

        loops_config = custom_evo_config.get("loops", [])
        if not loops_config:
            logger.error(f"自定义演进任务 {task_id} 没有 loops 配置")
            return
        engine_mode = custom_evo_config.get("engine_mode") or "unified"
        if engine_mode != "unified":
            error_msg = (
                "QE legacy execution engine has been removed; "
                "only engine_mode='unified' is supported."
            )
            logger.error("自定义演进任务 %s 配置了已移除的 engine_mode=%r", task_id, engine_mode)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET status = 'failed', updated_at = NOW() WHERE task_id = %s",
                        (task_id,),
                    )
                conn.commit()
            raise ValueError(error_msg)

        loops_config, loop1_node_id, selected_node_ids = resolve_custom_loop_nodes(
            [dict(loop_cfg) for loop_cfg in loops_config],
            task.get("node_id"),
        )
        node_parallelism = normalize_node_parallelism(
            selected_node_ids,
            custom_evo_config.get("node_parallelism"),
        )
        custom_evo_config["loops"] = loops_config
        custom_evo_config["node_parallelism"] = node_parallelism
        custom_evo_config["node_resolution_policy"] = "loop1_inherit_v1"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_evolution_tasks
                    SET node_id = %s, strategy_evo_config = %s, updated_at = NOW()
                    WHERE task_id = %s
                    """,
                    (loop1_node_id, json.dumps(custom_evo_config), task_id),
                )
            conn.commit()
        task = dict(task)
        task["node_id"] = loop1_node_id

        execution_mode_raw = task.get("strategy_evo_execution_mode", "serial")
        if execution_mode_raw.startswith("parallel"):
            mode = "parallel"
            parts = execution_mode_raw.split("_")
            parallelism = int(parts[1]) if len(parts) > 1 else 2
        else:
            mode = "serial"
            parallelism = 1

        # Mark task as running only if it has not been stopped since enqueue.
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE qe_evolution_tasks SET status = 'running', updated_at = NOW() "
                    "WHERE task_id = %s AND status IN ('pending', 'running')",
                    (task_id,),
                )
                if cur.rowcount == 0:
                    logger.info(f"Task {task_id} is no longer pending/running; abort loop submission")
                    conn.commit()
                    return
            conn.commit()

        # 过滤已完成的 loop
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT loop_index, status FROM qe_evolution_loops WHERE task_id = %s",
                    (task_id,),
                )
                existing_loops = {row["loop_index"]: row["status"] for row in cur.fetchall()}

        loops_to_run = []
        for loop_config in loops_config:
            loop_index = loop_config.get("loop_index")
            status = existing_loops.get(loop_index)
            if status == "completed":
                logger.info(f"自定义演进 Loop {loop_index} 已完成，跳过")
                continue
            loops_to_run.append(loop_config)

        if not loops_to_run:
            logger.info(f"自定义演进任务 {task_id} 所有 Loop 已完成")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT status, COUNT(*) FROM qe_evolution_loops WHERE task_id = %s GROUP BY status",
                        (task_id,),
                    )
                    status_counts = {status: count for status, count in cur.fetchall()}
            final_status = derive_custom_evo_final_status(len(loops_config), status_counts)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_tasks SET status = %s, updated_at = NOW() WHERE task_id = %s",
                        (final_status, task_id),
                    )
                conn.commit()
            return

        if mode == "serial":
            for loop_config in loops_to_run:
                loop_index = loop_config.get("loop_index")
                if self._get_task_status(task_id) != "running":
                    logger.info(f"Custom evolution task {task_id} is no longer running; stop submitting at Loop {loop_index}")
                    break
                loop_id = await self.submit_custom_evo_loop(task_id, loop_index, force_full_train=force_full_train)
                if not loop_id:
                    logger.error(f"Custom evolution Loop {loop_index} submit failed; stop subsequent loops")
                    break

                max_wait = 14400
                waited = 0
                interval = 15
                final_status = None
                stop_requested = False
                while waited < max_wait:
                    await asyncio.sleep(interval)
                    waited += interval
                    if self._get_task_status(task_id) != "running":
                        logger.info(f"Custom evolution task {task_id} stopped while waiting for Loop {loop_index}")
                        stop_requested = True
                        break
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT status FROM qe_evolution_loops WHERE loop_id = %s", (loop_id,))
                            row = cur.fetchone()
                    final_status = row[0] if row else None
                    if not row or final_status in ("completed", "failed", "cancelled"):
                        break

                if stop_requested:
                    break

                if waited >= max_wait:
                    logger.error(f"Custom evolution Loop {loop_index} wait timed out ({max_wait}s); marking failed")
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE qe_evolution_loops SET status = 'failed', updated_at = NOW() WHERE loop_id = %s AND status = 'running'",
                                (loop_id,),
                            )
                        conn.commit()
                    continue

                if loop_id and final_status == "completed":
                    await self._safe_process_completed_loop(task_id, loop_id)
                elif loop_id:
                    logger.info(
                        f"Custom evolution Loop {loop_index} ended with status={final_status}; "
                        "skip completed-loop metrics processing"
                    )
        else:
            node_semaphores = {
                node_id: asyncio.Semaphore(limit)
                for node_id, limit in node_parallelism.items()
            }
            logger.info(
                f"Custom evolution {task_id} parallel mode start: "
                f"legacy_parallelism={parallelism}, node_parallelism={node_parallelism}, loops={len(loops_to_run)}"
            )

            async def run_with_sem(loop_config):
                loop_index = loop_config.get("loop_index")
                loop_node_id = loop_config.get("node_id") or loop1_node_id
                sem = node_semaphores[loop_node_id]
                async with sem:
                    if self._get_task_status(task_id) != "running":
                        logger.info(f"Custom evolution task {task_id} is no longer running; skip Loop {loop_index}")
                        return None
                    loop_id = await self.submit_custom_evo_loop(task_id, loop_index, force_full_train=force_full_train)
                    if not loop_id:
                        return None
                    max_wait = 14400
                    waited = 0
                    interval = 15
                    final_status = None
                    while waited < max_wait:
                        await asyncio.sleep(interval)
                        waited += interval
                        if self._get_task_status(task_id) != "running":
                            logger.info(f"Custom evolution task {task_id} stopped while waiting for Loop {loop_index}")
                            return loop_id
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute("SELECT status FROM qe_evolution_loops WHERE loop_id = %s", (loop_id,))
                                row = cur.fetchone()
                        final_status = row[0] if row else None
                        if not row or final_status in ("completed", "failed", "cancelled"):
                            break
                    if waited >= max_wait:
                        logger.error("Custom evolution Loop %s wait timed out (%ss)", loop_index, max_wait)
                    if loop_id and final_status == "completed":
                        await self._safe_process_completed_loop(task_id, loop_id)
                    elif loop_id:
                        logger.info(f"Custom evolution Loop {loop_index} ended with status={final_status}; skip metrics processing")
                    return loop_id

            tasks = [run_with_sem(lc) for lc in loops_to_run]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"并行模式 Loop {loops_to_run[i].get('loop_index', i+1)} 提交失败: {result}")

        # ── 所有 loop 调度结束，确保 task 有最终状态 ──
        # process_strategy_evo_completed_loop 内部会在 loop_index >= max_loops 时标记 completed，
        # 但串行 break 或并行 loop 全部失败时可能漏掉，这里做兜底。
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT status FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                final_task = cur.fetchone()
        if final_task and final_task["status"] == "running":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT status, COUNT(*) FROM qe_evolution_loops WHERE task_id = %s GROUP BY status",
                        (task_id,),
                    )
                    status_counts = {status: count for status, count in cur.fetchall()}
            terminal_statuses = {"completed", "failed", "cancelled", "canceled"}
            active_count = sum(
                int(count) for status, count in status_counts.items()
                if status not in terminal_statuses
            )
            if active_count == 0:
                # 没有正在跑的 loop 了，检查结果
                completed_count = int(status_counts.get("completed", 0))
                failed_count = int(status_counts.get("failed", 0))
                cancelled_count = int(status_counts.get("cancelled", 0)) + int(status_counts.get("canceled", 0))
                expected_count = len(loops_config)
                terminal_count = completed_count + failed_count + cancelled_count
                final_status = derive_custom_evo_final_status(expected_count, status_counts)
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE qe_evolution_tasks SET status = %s, updated_at = NOW() WHERE task_id = %s AND status = 'running'",
                            (final_status, task_id),
                        )
                    conn.commit()
                logger.info(
                    "Custom evolution task %s final status: %s "
                    "(expected=%s, terminal=%s, completed=%s, failed=%s, cancelled=%s)",
                    task_id,
                    final_status,
                    expected_count,
                    terminal_count,
                    completed_count,
                    failed_count,
                    cancelled_count,
                )


    # ── Multi-Alpha Phase 3: helper methods ──────────────────────────────

    def _detect_alpha_mode(self, task: dict) -> str:
        """Detect alpha_mode from task or base experiment records.

        DB 错误不静默吞噬 — 记录日志并返回 single（安全降级，因为
        单 Alpha 路径不会破坏数据，而错误检测为 multi 反而会导致
        MultiAlphaEngine 因缺少 config 而崩溃）。
        """
        base_exp_id = task.get("base_experiment_id")
        if base_exp_id:
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT alpha_mode FROM qe_experiments WHERE experiment_id = %s",
                            (base_exp_id,),
                        )
                        row = cur.fetchone()
                        if row and row[0] and row[0] != "single":
                            return row[0]
            except Exception as e:
                logger.error(
                    "检测 alpha_mode 失败 (base_exp_id=%s): %s — 降级为 single",
                    base_exp_id, e,
                )
        return "single"

    async def _submit_multi_alpha_loop(
        self,
        task: dict,
        task_id: str,
        loop_index: int,
        evolution_loop_db_id: str,
    ):
        """Submit a Multi-Alpha evolution loop.

        Phase 3: generates sub-experiments for each alpha group, stores results.
        Actual Qlib execution dispatch is handled by the existing node infrastructure.
        """
        logger.info(f"Multi-Alpha loop {loop_index} for task {task_id}")

        # Create LOOP record
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO qe_evolution_loops "
                    "(loop_id, task_id, loop_index, status) "
                    "VALUES (%s, %s, %s, 'running') "
                    "ON CONFLICT (loop_id) DO UPDATE SET status = 'running', updated_at = NOW()",
                    (evolution_loop_db_id, task_id, loop_index),
                )
            conn.commit()

        try:
            base_exp_id = task.get("base_experiment_id")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT multi_alpha_config, factor_names, model_id, strategy_id, "
                        "data_split, custom_params, strategy_params "
                        "FROM qe_experiments WHERE experiment_id = %s",
                        (base_exp_id,),
                    )
                    exp_row = cur.fetchone()

            if not exp_row or not exp_row[0]:
                raise ValueError(f"Base experiment {base_exp_id} has no multi_alpha_config")

            from .experiment_config_builders import (
                _build_hmm_config_from_fields,
                _pop_hmm_fields,
                build_config_from_multi_alpha,
            )

            multi_alpha_raw = exp_row[0]
            if isinstance(multi_alpha_raw, str):
                multi_alpha_raw = json.loads(multi_alpha_raw)

            data_split = exp_row[4]
            if isinstance(data_split, str):
                data_split = json.loads(data_split)
            custom_params = exp_row[5] or {}
            if isinstance(custom_params, str):
                custom_params = json.loads(custom_params)
            strat_params = exp_row[6]
            if isinstance(strat_params, str):
                strat_params = json.loads(strat_params)
            task_strategy_params = self._parse_json_field(
                task.get("strategy_params"),
                f"multi_alpha_task[{task_id}].strategy_params",
            )
            custom_params_clean = dict(custom_params or {})
            strat_params_clean = dict(strat_params or {})
            task_strategy_params_clean = dict(task_strategy_params or {})
            hmm_config = _build_hmm_config_from_fields(
                _pop_hmm_fields(custom_params_clean),
                _pop_hmm_fields(strat_params_clean),
                _pop_hmm_fields(task_strategy_params_clean),
            )

            cfg = build_config_from_multi_alpha(
                multi_alpha_config=multi_alpha_raw,
                data_split=data_split,
                strategy_id=exp_row[3],
                strategy_params=strat_params_clean or None,
                hmm_config=hmm_config,
                node_id=task.get("node_id"),
                experiment_name=f"{task_id}_Loop{loop_index}",
            )

            from .multi_alpha_engine import MultiAlphaEngine

            engine = MultiAlphaEngine(cfg)
            result = engine.run()

            config_json = {
                "alpha_mode": "multi",
                "multi_alpha_config": (
                    cfg.multi_alpha_config.model_dump() if cfg.multi_alpha_config else None
                ),
                "group_configs": result.get("group_configs"),
                "meta_method": result.get("meta_method"),
            }
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops "
                        "SET status = 'completed', config_json = %s, updated_at = NOW() "
                        "WHERE loop_id = %s",
                        (json.dumps(config_json, default=str), evolution_loop_db_id),
                    )
                    cur.execute(
                        "UPDATE qe_evolution_tasks "
                        "SET current_loop = %s, updated_at = NOW() "
                        "WHERE task_id = %s",
                        (loop_index, task_id),
                    )
                conn.commit()

            logger.info(
                f"Multi-Alpha loop {loop_index} completed: "
                f"{result['total_groups']} groups"
            )
            return evolution_loop_db_id

        except Exception as e:
            logger.error(
                f"Multi-Alpha loop {loop_index} failed: {e}", exc_info=True
            )
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops "
                        "SET status = 'failed', error_message = %s, updated_at = NOW() "
                        "WHERE loop_id = %s",
                        (str(e)[:2000], evolution_loop_db_id),
                    )
                conn.commit()
            return None
