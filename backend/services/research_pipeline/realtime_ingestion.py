"""Best-effort realtime Research Pipeline ingestion hooks.

The QE runtime may call this module after its own completion transaction has
committed. Recording is feature-flagged off by default and failures must never
propagate back into QE loop processing.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from typing import Any

from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn

from .hmm_backtest_recorder import (
    HMM_RECORDING_ENABLED_ENV,
    HMMBacktestRecorder,
    as_dict,
    as_list,
    env_truthy,
)

logger = logging.getLogger("aistock.research_pipeline.realtime_ingestion")


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return env_truthy(str(value) if value is not None else None)


def _falsey(value: Any) -> bool:
    if isinstance(value, bool):
        return not value
    return str(value or "").strip().lower() in {"0", "false", "no", "n", "off"}


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
        except Exception:
            return {}
        return dict(loaded) if isinstance(loaded, Mapping) else {}
    return {}


def _loop_config_from_task(row: Mapping[str, Any], loop_index: int | None) -> dict[str, Any]:
    task_config = _json_mapping(row.get("strategy_evo_config"))
    loops = as_list(task_config.get("loops"))
    if not loops:
        return {}
    for item in loops:
        if not isinstance(item, Mapping):
            continue
        if loop_index is not None:
            try:
                if int(item.get("loop_index")) == int(loop_index):
                    return dict(item)
            except (TypeError, ValueError):
                continue
    if loop_index is None and len(loops) == 1 and isinstance(loops[0], Mapping):
        return dict(loops[0])
    return {}


def _combined_research_metadata(row: Mapping[str, Any], loop_index: int | None) -> dict[str, Any]:
    config = as_dict(row.get("config_json"))
    loop_config = _loop_config_from_task(row, loop_index)
    custom_params = as_dict(row.get("custom_params"))

    sources: list[dict[str, Any]] = [
        loop_config,
        as_dict(loop_config.get("strategy_params")),
        config,
        as_dict(config.get("model_params")),
        custom_params,
        as_dict(custom_params.get("hmm_config_json")),
    ]
    merged: dict[str, Any] = {}
    for source in sources:
        merged.update({key: value for key, value in source.items() if value is not None})
    return merged


def _research_experiment_id(metadata: Mapping[str, Any]) -> str | None:
    for key in ("research_experiment_id", "research_pipeline_experiment_id"):
        value = metadata.get(key)
        if value:
            return str(value)
    return None


def _is_hmm_research_loop(metadata: Mapping[str, Any]) -> bool:
    if not _truthy(metadata.get("record_backtest_to_research_pipeline")):
        return False

    research_domain = str(metadata.get("research_domain") or metadata.get("domain") or "").strip().lower()
    pipeline_type = str(
        metadata.get("research_pipeline_type")
        or metadata.get("pipeline_type")
        or metadata.get("pipeline")
        or ""
    ).strip().lower()
    if research_domain != "hmm":
        return False
    return pipeline_type in {"", "hmm_research"}


class ResearchPipelineRealtimeIngestion:
    """Small facade for QE completion-time Research Pipeline recording."""

    def __init__(
        self,
        *,
        recorder: HMMBacktestRecorder | None = None,
        enabled: bool | None = None,
    ) -> None:
        self._recorder = recorder
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        if self._enabled is not None:
            return self._enabled
        return env_truthy(os.getenv(HMM_RECORDING_ENABLED_ENV))

    def record_hmm_backtest_completed(
        self,
        *,
        task_id: str,
        loop_id: str,
        loop_index: int | None = None,
        experiment_id: str | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {"recorded": False, "skipped_reason": "disabled"}

        row = self._fetch_loop_context(task_id=task_id, loop_id=loop_id)
        if not row:
            return {"recorded": False, "skipped_reason": "loop_not_found"}

        resolved_loop_index = self._int_or_none(row.get("loop_index"))
        if resolved_loop_index is None:
            resolved_loop_index = loop_index

        metadata = _combined_research_metadata(row, resolved_loop_index)
        research_experiment_id = experiment_id or _research_experiment_id(metadata)
        if not research_experiment_id:
            return {"recorded": False, "skipped_reason": "missing_research_experiment_id"}
        if not _is_hmm_research_loop(metadata):
            return {"recorded": False, "skipped_reason": "not_hmm_research_loop"}

        recorder = self._recorder or HMMBacktestRecorder()
        record = self._build_record(
            recorder,
            row=row,
            research_experiment_id=str(research_experiment_id),
            task_id=task_id,
            loop_id=loop_id,
            loop_index=resolved_loop_index,
            metadata=metadata,
        )
        saved = recorder._repo.upsert_backtest_record(record)  # type: ignore[attr-defined]
        return {
            "recorded": True,
            "record_id": saved.get("record_id"),
            "record_key_sha256": saved.get("record_key_sha256"),
            "experiment_id": research_experiment_id,
        }

    def _fetch_loop_context(self, *, task_id: str, loop_id: str) -> dict[str, Any] | None:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        l.loop_id,
                        l.task_id,
                        l.loop_index,
                        l.config_json,
                        l.metrics_json,
                        l.experiment_id AS source_experiment_id,
                        l.created_at AS loop_created_at,
                        t.task_name,
                        t.created_at AS task_created_at,
                        t.strategy_evo_config,
                        e.custom_params
                    FROM qe_evolution_loops l
                    LEFT JOIN qe_evolution_tasks t ON t.task_id = l.task_id
                    LEFT JOIN qe_experiments e ON e.experiment_id = l.experiment_id
                    WHERE l.task_id = %s AND l.loop_id = %s
                    """,
                    (task_id, loop_id),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def _build_record(
        self,
        recorder: HMMBacktestRecorder,
        *,
        row: Mapping[str, Any],
        research_experiment_id: str,
        task_id: str,
        loop_id: str,
        loop_index: int | None,
        metadata: Mapping[str, Any],
    ) -> Any:
        config = as_dict(row.get("config_json"))
        custom_params = as_dict(row.get("custom_params"))
        payload = {
            "task_id": task_id,
            "task_name": row.get("task_name"),
            "task_created_at": row.get("task_created_at") or row.get("loop_created_at"),
            "loop_index": loop_index,
            "loop_id": loop_id,
            "experiment_id": row.get("source_experiment_id"),
            "config": config,
            "config_summary": recorder._config_summary_from_loop(config, custom_params),  # type: ignore[attr-defined]
            "metrics": as_dict(row.get("metrics_json")),
            "hmm_config_summary": recorder._hmm_summary_from_params(metadata),  # type: ignore[attr-defined]
        }
        return recorder.normalize_historical_record(
            payload,
            experiment_id=research_experiment_id,
            selected_representative=False,
            duplicate=False,
            recorded_by="auto_hook",
            source_type="qe_loop",
        )

    @staticmethod
    def _int_or_none(value: Any) -> int | None:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None


def safe_record_hmm_backtest_completed(
    *,
    task_id: str,
    loop_id: str,
    loop_index: int | None = None,
    experiment_id: str | None = None,
) -> dict[str, Any]:
    """Record one HMM QE loop without raising into the QE runtime path."""

    if not env_truthy(os.getenv(HMM_RECORDING_ENABLED_ENV)):
        return {"recorded": False, "skipped_reason": "disabled"}

    try:
        result = ResearchPipelineRealtimeIngestion(enabled=True).record_hmm_backtest_completed(
            task_id=task_id,
            loop_id=loop_id,
            loop_index=loop_index,
            experiment_id=experiment_id,
        )
        if result.get("skipped_reason") != "disabled":
            logger.info("Research Pipeline HMM realtime ingestion result: %s", result)
        return result
    except Exception as exc:  # pragma: no cover - runtime protection path.
        logger.warning(
            "Research Pipeline HMM realtime ingestion failed: task=%s loop=%s error=%s",
            task_id,
            loop_id,
            exc,
            exc_info=True,
        )
        return {
            "recorded": False,
            "skipped_reason": "error",
            "error": f"{type(exc).__name__}: {exc}",
        }
