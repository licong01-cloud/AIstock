from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _row_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    if key in row.keys():
        return row[key]
    return default


@dataclass(frozen=True)
class RDTaskRun:
    task_run_id: str
    scenario: Optional[str]
    status: Optional[str]
    created_at_utc: Optional[str]
    updated_at_utc: Optional[str]


@dataclass(frozen=True)
class RDLoop:
    task_run_id: str
    loop_id: int
    action: Optional[str]
    status: Optional[str]
    has_result: int
    best_workspace_id: Optional[str]
    ic_mean: Optional[float]
    ann_return: Optional[float]
    mdd: Optional[float]
    turnover: Optional[float]
    multi_score: Optional[float]


@dataclass(frozen=True)
class RDWorkspace:
    workspace_id: str
    workspace_role: Optional[str]
    experiment_type: Optional[str]
    workspace_path: str
    meta_path: Optional[str]
    summary_path: Optional[str]
    manifest_path: Optional[str]


@dataclass(frozen=True)
class RDWorkspaceSignalHint:
    task_run_id: Optional[str]
    loop_id: Optional[int]
    action: Optional[str]
    ic_mean: Optional[float]
    ann_return: Optional[float]
    mdd: Optional[float]
    turnover: Optional[float]
    multi_score: Optional[float]
    workspace_id: str
    workspace_role: Optional[str]
    experiment_type: Optional[str]
    workspace_path: str
    manifest_path: Optional[str]
    summary_path: Optional[str]
    signals_parquet_path: Optional[str]
    signals_json_path: Optional[str]


class RDRegistryReader:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    @staticmethod
    def resolve_db_path() -> str:
        explicit = (os.getenv("RDAGENT_REGISTRY_SQLITE_PATH") or "").strip()
        if explicit:
            return explicit

        root_win = (os.getenv("QLIB_RDAGENT_ROOT_WIN") or "").strip().strip('"')
        if root_win:
            candidate = Path(root_win) / "RDagentDB" / "registry.sqlite"
            return str(candidate)

        # fallback: assume repo layout under current workspace
        candidate = Path(__file__).resolve().parents[3] / "RD-Agent-main" / "RDagentDB" / "registry.sqlite"
        return str(candidate)

    def _connect(self) -> sqlite3.Connection:
        db_path = Path(self.db_path)
        if not db_path.exists():
            raise FileNotFoundError(f"registry.sqlite not found: {db_path}")
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def list_task_runs(self, limit: int = 50) -> List[RDTaskRun]:
        sql = """
        SELECT task_run_id, scenario, status, created_at_utc, updated_at_utc
        FROM task_runs
        ORDER BY created_at_utc DESC
        LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
        return [
            RDTaskRun(
                task_run_id=row["task_run_id"],
                scenario=_row_get(row, "scenario"),
                status=_row_get(row, "status"),
                created_at_utc=_row_get(row, "created_at_utc"),
                updated_at_utc=_row_get(row, "updated_at_utc"),
            )
            for row in rows
        ]

    def list_loops(self, task_run_id: str) -> List[RDLoop]:
        sql = """
        SELECT loop_id, action, status, has_result, best_workspace_id,
               ic_mean, ann_return, mdd, turnover, multi_score
        FROM loops
        WHERE task_run_id = ?
        ORDER BY loop_id ASC
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (task_run_id,)).fetchall()
        results: List[RDLoop] = []
        for row in rows:
            results.append(
                RDLoop(
                    task_run_id=task_run_id,
                    loop_id=int(row["loop_id"]),
                    action=_row_get(row, "action"),
                    status=_row_get(row, "status"),
                    has_result=int(_row_get(row, "has_result") or 0),
                    best_workspace_id=_row_get(row, "best_workspace_id"),
                    ic_mean=_row_get(row, "ic_mean"),
                    ann_return=_row_get(row, "ann_return"),
                    mdd=_row_get(row, "mdd"),
                    turnover=_row_get(row, "turnover"),
                    multi_score=_row_get(row, "multi_score"),
                )
            )
        return results

    def get_workspace(self, workspace_id: str) -> RDWorkspace:
        sql = """
        SELECT workspace_id, workspace_role, experiment_type,
               workspace_path, meta_path, summary_path, manifest_path
        FROM workspaces
        WHERE workspace_id = ?
        """
        with self._connect() as conn:
            row = conn.execute(sql, (workspace_id,)).fetchone()
        if not row:
            raise KeyError(f"workspace_id not found: {workspace_id}")
        return RDWorkspace(
            workspace_id=row["workspace_id"],
            workspace_role=_row_get(row, "workspace_role"),
            experiment_type=_row_get(row, "experiment_type"),
            workspace_path=_row_get(row, "workspace_path") or "",
            meta_path=_row_get(row, "meta_path"),
            summary_path=_row_get(row, "summary_path"),
            manifest_path=_row_get(row, "manifest_path"),
        )

    def find_signal_files(self, workspace_id: str) -> Tuple[Optional[str], Optional[str]]:
        """Return (signals_parquet_rel, signals_json_rel) relative to workspace."""
        sql = """
        SELECT af.path
        FROM artifacts a
        JOIN artifact_files af ON af.artifact_id = a.artifact_id
        WHERE a.workspace_id = ?
          AND af.path IN ('signals.parquet', 'signals.json')
        """
        signals_parquet = None
        signals_json = None
        with self._connect() as conn:
            rows = conn.execute(sql, (workspace_id,)).fetchall()
        for row in rows:
            p = row["path"]
            if p == "signals.parquet":
                signals_parquet = p
            elif p == "signals.json":
                signals_json = p
        return signals_parquet, signals_json

    def find_backtest_metrics_file(self, workspace_id: str) -> Optional[str]:
        """Return relative path to qlib_res.csv if recorded in artifact_files.

        This relies on the RD-Agent backfill script having created an
        artifact of type backtest_metrics with an artifact_files entry
        pointing to qlib_res.csv.
        """

        sql = """
        SELECT af.path
        FROM artifacts a
        JOIN artifact_files af ON af.artifact_id = a.artifact_id
        WHERE a.workspace_id = ?
          AND af.path = 'qlib_res.csv'
        LIMIT 1
        """
        with self._connect() as conn:
            row = conn.execute(sql, (workspace_id,)).fetchone()
        if not row:
            return None
        return row["path"]

    def list_backtest_workspaces(self, task_run_id: str) -> List[Dict[str, Any]]:
        """List workspaces under a task_run_id that have backtest artifacts.

        Returns list of {loop_id, workspace_id, has_metrics, has_curve}.
        """

        sql = """
        SELECT
            a.loop_id AS loop_id,
            a.workspace_id AS workspace_id,
            MAX(CASE WHEN af.path = 'qlib_res.csv' THEN 1 ELSE 0 END) AS has_metrics,
            MAX(CASE WHEN af.path = 'ret.pkl' THEN 1 ELSE 0 END) AS has_curve
        FROM artifacts a
        JOIN artifact_files af ON af.artifact_id = a.artifact_id
        WHERE a.task_run_id = ?
          AND af.path IN ('qlib_res.csv', 'ret.pkl')
        GROUP BY a.loop_id, a.workspace_id
        ORDER BY a.loop_id ASC
        """

        with self._connect() as conn:
            rows = conn.execute(sql, (task_run_id,)).fetchall()

        return [
            {
                "loop_id": _row_get(row, "loop_id"),
                "workspace_id": _row_get(row, "workspace_id"),
                "has_metrics": int(_row_get(row, "has_metrics") or 0),
                "has_curve": int(_row_get(row, "has_curve") or 0),
            }
            for row in rows
            if _row_get(row, "workspace_id")
        ]

    def list_workspace_ids_for_task_run(self, task_run_id: str) -> List[str]:
        """Collect workspace_id for a task_run_id from artifacts/loops tables."""

        sql = """
        SELECT DISTINCT workspace_id
        FROM artifacts
        WHERE task_run_id = ?
          AND workspace_id IS NOT NULL
        """

        with self._connect() as conn:
            rows = conn.execute(sql, (task_run_id,)).fetchall()

        ws_ids = {str(row["workspace_id"]) for row in rows if row["workspace_id"]}

        try:
            sql_loops = """
            SELECT DISTINCT best_workspace_id AS workspace_id
            FROM loops
            WHERE task_run_id = ?
              AND best_workspace_id IS NOT NULL
            """
            rows2 = conn.execute(sql_loops, (task_run_id,)).fetchall()
            ws_ids.update({str(row["workspace_id"]) for row in rows2 if row["workspace_id"]})
        except Exception:
            pass

        return sorted(ws_ids)

    def find_backtest_curve_file(self, workspace_id: str) -> Optional[str]:
        """Return relative path to ret.pkl if recorded in artifact_files.

        This relies on the RD-Agent backfill script having created an
        artifact of type backtest_curve with an artifact_files entry
        pointing to ret.pkl.
        """

        sql = """
        SELECT af.path
        FROM artifacts a
        JOIN artifact_files af ON af.artifact_id = a.artifact_id
        WHERE a.workspace_id = ?
          AND af.path = 'ret.pkl'
        LIMIT 1
        """
        with self._connect() as conn:
            row = conn.execute(sql, (workspace_id,)).fetchone()
        if not row:
            return None
        return row["path"]

    def list_workspaces_with_signals(self, limit: int = 200) -> List[RDWorkspaceSignalHint]:
        """List workspaces which have signals.parquet/signals.json recorded in artifact_files.

        This does not rely on loops.has_result/best_workspace_id. It is the preferred
        discovery path when has_result is not populated.
        """

        sql = """
        SELECT
            a.task_run_id AS task_run_id,
            a.loop_id     AS loop_id,
            l.action      AS action,
            l.ic_mean     AS ic_mean,
            l.ann_return  AS ann_return,
            l.mdd         AS mdd,
            l.turnover    AS turnover,
            l.multi_score AS multi_score,
            a.workspace_id AS workspace_id,
            w.workspace_role AS workspace_role,
            w.experiment_type AS experiment_type,
            w.workspace_path AS workspace_path,
            w.manifest_path  AS manifest_path,
            w.summary_path   AS summary_path,
            MAX(CASE WHEN af.path = 'signals.parquet' THEN af.path ELSE NULL END) AS signals_parquet_path,
            MAX(CASE WHEN af.path = 'signals.json' THEN af.path ELSE NULL END) AS signals_json_path
        FROM artifacts a
        JOIN artifact_files af ON af.artifact_id = a.artifact_id
        JOIN workspaces w ON w.workspace_id = a.workspace_id
        LEFT JOIN loops l ON l.task_run_id = a.task_run_id AND l.loop_id = a.loop_id
        WHERE af.path IN ('signals.parquet', 'signals.json')
        GROUP BY a.task_run_id, a.loop_id, l.action, l.ic_mean, l.ann_return, l.mdd, l.turnover, l.multi_score,
                 a.workspace_id, w.workspace_role, w.experiment_type, w.workspace_path, w.manifest_path, w.summary_path
        ORDER BY a.task_run_id DESC, a.loop_id DESC
        LIMIT ?
        """

        with self._connect() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()

        results: List[RDWorkspaceSignalHint] = []
        for row in rows:
            results.append(
                RDWorkspaceSignalHint(
                    task_run_id=_row_get(row, "task_run_id"),
                    loop_id=int(_row_get(row, "loop_id") or 0) if _row_get(row, "loop_id") is not None else None,
                    action=_row_get(row, "action"),
                    ic_mean=_row_get(row, "ic_mean"),
                    ann_return=_row_get(row, "ann_return"),
                    mdd=_row_get(row, "mdd"),
                    turnover=_row_get(row, "turnover"),
                    multi_score=_row_get(row, "multi_score"),
                    workspace_id=row["workspace_id"],
                    workspace_role=_row_get(row, "workspace_role"),
                    experiment_type=_row_get(row, "experiment_type"),
                    workspace_path=_row_get(row, "workspace_path") or "",
                    manifest_path=_row_get(row, "manifest_path"),
                    summary_path=_row_get(row, "summary_path"),
                    signals_parquet_path=_row_get(row, "signals_parquet_path"),
                    signals_json_path=_row_get(row, "signals_json_path"),
                )
            )
        return results
