"""Background reconciliation for one-off QE experiments.

The UI and SSE log stream are not reliable lifecycle owners: browsers can be
closed, a log stream can stay open without emitting a terminal event, and
backend reloads can drop in-memory callbacks. This scanner makes the database
state converge to RD-Agent/workspace state even when no page is open.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.qe_experiment_scanner")


class QEExperimentStatusScanner:
    """Reconcile running rows in ``qe_experiments`` with RD-Agent status."""

    def __init__(self, *, batch_size: int = 50):
        self.batch_size = max(1, int(batch_size or 50))

    def _mark_malformed_running_experiment(self, experiment_id: str, error: str) -> None:
        payload = {
            "status_sync": {
                "stage": "failed",
                "source": "qe_experiment_scanner",
                "error": error,
            }
        }
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_experiments
                    SET status = 'failed',
                        result_metrics = COALESCE(result_metrics, '{}'::jsonb) || %s::jsonb,
                        completed_at = NOW()
                    WHERE experiment_id = %s
                      AND status = 'running'
                    """,
                    (json.dumps(payload, ensure_ascii=False), experiment_id),
                )
            conn.commit()

    def _load_running_experiments(self) -> list[dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT experiment_id, qe_task_id, qe_loop_id, alpha_mode
                    FROM qe_experiments
                    WHERE status = 'running'
                    ORDER BY started_at NULLS FIRST, created_at NULLS FIRST
                    LIMIT %s
                    """,
                    (self.batch_size,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]

    async def scan_once(self) -> dict[str, int]:
        """Run one reconciliation pass.

        Returns counters for monitoring/logging. Individual experiment failures
        are logged and do not abort the whole scan.
        """
        rows = self._load_running_experiments()
        stats = {
            "checked": 0,
            "synced_terminal": 0,
            "still_running": 0,
            "malformed_failed": 0,
            "errors": 0,
        }
        if not rows:
            return stats

        from ...routers.quantevolver import get_experiment_run_status

        for row in rows:
            experiment_id = row["experiment_id"]
            alpha_mode = row.get("alpha_mode") or "single"
            try:
                if not row.get("qe_task_id"):
                    self._mark_malformed_running_experiment(
                        experiment_id,
                        "running experiment is missing qe_task_id",
                    )
                    stats["malformed_failed"] += 1
                    continue
                if alpha_mode != "multi" and not row.get("qe_loop_id"):
                    self._mark_malformed_running_experiment(
                        experiment_id,
                        "running single-alpha experiment is missing qe_loop_id",
                    )
                    stats["malformed_failed"] += 1
                    continue

                stats["checked"] += 1
                result = await get_experiment_run_status(experiment_id)
                status = result.get("status")
                if status == "running":
                    stats["still_running"] += 1
                elif status in {"completed", "failed", "interrupted", "timeout"}:
                    stats["synced_terminal"] += 1
                else:
                    logger.warning(
                        "Unexpected QE experiment sync status: experiment=%s status=%s",
                        experiment_id,
                        status,
                    )
            except Exception as exc:
                stats["errors"] += 1
                logger.warning(
                    "QE experiment status scan failed: experiment=%s error=%s",
                    experiment_id,
                    exc,
                    exc_info=True,
                )
                await asyncio.sleep(0)

        return stats
