
"""HMM backtest timeline recorder for Research Pipeline.

The recorder is the single write path for both future QE completion hooks and
historical backfills. It normalizes compact loop metrics, builds stable
signatures, and upserts idempotent Research Pipeline backtest records.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from backend.db.pg_pool import get_conn
from backend.services.qe_archive.models import canonical_json_dumps, normalize_json

from .models import ArtifactRefRecord, BackfillRunRecord, BacktestRecord, PipelineEventRecord, StageAttemptRecord, StagePlanRecord, utc_now
from .repository import ResearchPipelineRepository

RECORD_VERSION = "hmm_backtest_record_v1"
BACKFILL_TYPE = "hmm_backtest_timeline"
BACKTEST_RECORDING_STAGE = "backtest_recording"
HMM_BACKFILL_ENABLED_ENV = "RESEARCH_PIPELINE_HMM_BACKFILL_ENABLED"
HMM_BACKFILL_WRITE_ENABLED_ENV = "RESEARCH_PIPELINE_HMM_BACKFILL_WRITE_ENABLED"
HMM_RECORDING_ENABLED_ENV = "RESEARCH_PIPELINE_HMM_RECORDING_ENABLED"

HMM_FIELD_KEYS = {
    "enable_sector_hmm",
    "sector_hmm_model_path",
    "hmm_model_version_id",
    "hmm_signal_preset",
    "hmm_signal_presets",
    "hmm_config_json",
    "hmm_config_version",
    "hmm_config_ui_label",
    "hmm_candidate_name",
    "enable_hmm_risk_gate",
    "hmm_risk_gate_file",
    "hmm_risk_gate_model_path",
    "precomputed_hmm_risk_gate_json",
    "hmm_coefficients_file",
}
NON_HMM_CONFIG_KEYS = (
    "model_id",
    "model_source_task_id",
    "model_source_loop_index",
    "strategy_id",
    "execution_algo",
    "stock_pool",
    "label_horizon",
    "disable_alpha158",
    "factor_count",
    "factor_sig",
)
METRIC_ALIASES = {
    "ann": ("ann", "annualized_return", "annualized_return_no_cost", "1day.excess_return_with_cost.annualized_return"),
    "mdd": ("mdd", "max_drawdown", "max_drawdown_no_cost", "1day.excess_return_with_cost.max_drawdown"),
    "ir": ("ir", "information_ratio", "sharpe", "1day.excess_return_with_cost.information_ratio"),
    "ic": ("ic", "IC", "mean_ic"),
    "rank_ic": ("rank_ic", "Rank_IC", "Rank IC", "mean_rank_ic"),
    "sharpe": ("sharpe", "information_ratio", "1day.excess_return_with_cost.information_ratio"),
    "turnover": ("turnover", "turnover_actual", "avg_turnover"),
}


def env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def stable_json_hash(payload: Any, *, size: int = 12) -> str:
    text = canonical_json_dumps(normalize_json(payload))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:size]


def sha256_json(payload: Any) -> str:
    text = canonical_json_dumps(normalize_json(payload))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
            return dict(loaded) if isinstance(loaded, dict) else {}
        except Exception:
            return {}
    return {}


def as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
            return list(loaded) if isinstance(loaded, list) else []
        except Exception:
            return []
    return []


def metric_value(metrics: Mapping[str, Any], key: str) -> float | None:
    for alias in METRIC_ALIASES[key]:
        value = metrics.get(alias)
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number == number:
            return number
    return None


def normalize_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


@dataclass(frozen=True)
class PreviewResult:
    backfill_run_id: str
    counts: dict[str, int]
    sample_records: list[dict[str, Any]]


class HMMBacktestRecorder:
    def __init__(self, repository: ResearchPipelineRepository | None = None) -> None:
        self._repo = repository or ResearchPipelineRepository()

    @property
    def backfill_enabled(self) -> bool:
        return env_truthy(os.getenv(HMM_BACKFILL_ENABLED_ENV))

    @property
    def backfill_write_enabled(self) -> bool:
        return env_truthy(os.getenv(HMM_BACKFILL_WRITE_ENABLED_ENV))

    @property
    def realtime_enabled(self) -> bool:
        return env_truthy(os.getenv(HMM_RECORDING_ENABLED_ENV))

    def normalize_historical_record(
        self,
        payload: Mapping[str, Any],
        *,
        experiment_id: str,
        selected_representative: bool = False,
        duplicate: bool = False,
        excluded: bool = False,
        recorded_by: str = "backfill",
        source_type: str = "historical_file",
    ) -> BacktestRecord:
        metrics = self._extract_metrics(payload)
        config_summary = self._extract_config_summary(payload)
        hmm_summary = self._extract_hmm_summary(payload)
        source_task_id = str(payload.get("task_id") or "unknown_task")
        source_loop_id = str(payload.get("loop_id") or f"{source_task_id}_Loop{payload.get('loop_index') or 0}")
        source_loop_index = self._int_or_none(payload.get("loop_index"))
        source_experiment_id = payload.get("experiment_id")
        strict_family_sig = str(payload.get("strict_family_sig") or payload.get("config_family_sig") or "") or None
        archive_family_sig = str(payload.get("archive_family_sig") or "") or None
        non_hmm_config_sig = archive_family_sig or stable_json_hash({k: config_summary.get(k) for k in NON_HMM_CONFIG_KEYS})
        hmm_config_sig = stable_json_hash(hmm_summary) if hmm_summary else None
        dedup_status = (
            "excluded"
            if excluded
            else "primary"
            if selected_representative
            else "hmm_variant"
            if duplicate and hmm_summary
            else "duplicate_same_config"
            if duplicate
            else "primary"
        )
        rejection_reason = None
        if excluded:
            rejection_reason = str(payload.get("reason") or payload.get("rejection_reason") or "excluded_from_hmm_timeline")
        elif duplicate and not selected_representative:
            rejection_reason = "hmm_only_config_sweep_preserved_in_research_pipeline"
        record_key = self.build_record_key(
            experiment_id=experiment_id,
            source_type=source_type,
            source_task_id=source_task_id,
            source_loop_id=source_loop_id,
            source_loop_index=source_loop_index,
        )
        return BacktestRecord(
            experiment_id=experiment_id,
            pipeline_type="hmm_research",
            research_domain="hmm",
            source_type=source_type,  # type: ignore[arg-type]
            source_task_id=source_task_id,
            source_loop_id=source_loop_id,
            source_loop_index=source_loop_index,
            source_experiment_id=str(source_experiment_id) if source_experiment_id else None,
            source_created_at=normalize_dt(payload.get("task_created_at") or payload.get("created_at")),
            record_version=RECORD_VERSION,
            record_key_sha256=record_key,
            non_hmm_config_sig=non_hmm_config_sig,
            hmm_config_sig=hmm_config_sig,
            strict_family_sig=strict_family_sig,
            archive_family_sig=archive_family_sig or non_hmm_config_sig,
            dedup_status=dedup_status,  # type: ignore[arg-type]
            qe_archive_eligible=bool(selected_representative),
            qe_archive_representative=bool(selected_representative),
            rejection_reason=rejection_reason,
            ann=metric_value(metrics, "ann"),
            mdd=metric_value(metrics, "mdd"),
            ir=metric_value(metrics, "ir"),
            ic=metric_value(metrics, "ic"),
            rank_ic=metric_value(metrics, "rank_ic"),
            sharpe=metric_value(metrics, "sharpe"),
            turnover=metric_value(metrics, "turnover"),
            metrics_json=metrics,
            hmm_config_summary_json=hmm_summary,
            config_summary_json=config_summary,
            source_payload_json=self._compact_source_payload(payload),
            recorded_by=recorded_by,
        )

    def normalize_qe_loop(self, *, experiment_id: str, task_id: str, loop_id: str, loop_index: int | None = None) -> BacktestRecord | None:
        row = self._fetch_qe_loop(task_id=task_id, loop_id=loop_id, loop_index=loop_index)
        if not row:
            return None
        config = as_dict(row.get("config_json"))
        custom_params = as_dict(row.get("custom_params"))
        merged = {**config, **custom_params}
        research_experiment_id = self._research_experiment_id(merged)
        if research_experiment_id and research_experiment_id != experiment_id:
            experiment_id = research_experiment_id
        if not self._is_hmm_research_payload(merged, task_name=row.get("task_name")):
            return None
        payload = {
            "task_id": task_id,
            "task_name": row.get("task_name"),
            "task_created_at": row.get("task_created_at"),
            "loop_index": row.get("loop_index") or loop_index,
            "loop_id": loop_id,
            "experiment_id": row.get("experiment_id"),
            "config": config,
            "config_summary": self._config_summary_from_loop(config, custom_params),
            "metrics": as_dict(row.get("metrics_json")),
            "hmm_config_summary": self._hmm_summary_from_params(merged),
        }
        return self.normalize_historical_record(
            payload,
            experiment_id=experiment_id,
            selected_representative=False,
            duplicate=False,
            recorded_by="auto_hook",
            source_type="qe_loop",
        )

    def create_backfill_preview(
        self,
        experiment_id: str,
        payload: Mapping[str, Any],
        *,
        created_by: str = "codex",
        require_enabled: bool = True,
    ) -> dict[str, Any]:
        if require_enabled and not self.backfill_enabled:
            raise ValueError(f"HMM backfill preview disabled; set {HMM_BACKFILL_ENABLED_ENV}=true")
        records, excluded, fingerprint = self.load_backfill_records(experiment_id, payload)
        existing_keys = self._existing_record_keys(experiment_id)
        counts = self._preview_counts(records, excluded, existing_keys)
        run = self._repo.create_backfill_run(
            BackfillRunRecord(
                experiment_id=experiment_id,
                status="previewed",
                dry_run=True,
                source_scope_json=dict(payload.get("source_scope") or {}),
                source_fingerprint_json=fingerprint,
                counts_json=counts,
                created_by=created_by,
            )
        )
        return {"preview_id": run["backfill_run_id"], "backfill_run": run, "counts": counts, "sample_records": [r.model_dump() for r in records[:10]]}

    def execute_backfill(
        self,
        experiment_id: str,
        payload: Mapping[str, Any],
        *,
        created_by: str = "codex",
        require_enabled: bool = True,
    ) -> dict[str, Any]:
        if require_enabled and not self.backfill_enabled:
            raise ValueError(f"HMM backfill disabled; set {HMM_BACKFILL_ENABLED_ENV}=true")
        data = dict(payload)
        preview_id = data.get("preview_id")
        if preview_id and not (data.get("source_file") or dict(data.get("source_scope") or {}).get("source_file")):
            preview = self._repo.get_backfill_run(str(preview_id))
            if not preview:
                raise ValueError(f"backfill preview not found: {preview_id}")
            data["source_scope"] = {**dict(preview.get("source_scope_json") or {}), **dict(data.get("source_scope") or {})}
        dry_run = bool(data.get("dry_run", True))
        if not dry_run and require_enabled and not self.backfill_write_enabled:
            raise ValueError(f"HMM backfill write disabled; set {HMM_BACKFILL_WRITE_ENABLED_ENV}=true")
        records, excluded, fingerprint = self.load_backfill_records(experiment_id, data)
        existing_keys = self._existing_record_keys(experiment_id)
        counts = self._preview_counts(records, excluded, existing_keys)
        run = self._repo.create_backfill_run(
            BackfillRunRecord(
                experiment_id=experiment_id,
                status="running" if not dry_run else "previewed",
                dry_run=dry_run,
                source_scope_json=dict(data.get("source_scope") or {}),
                source_fingerprint_json=fingerprint,
                counts_json=counts,
                created_by=created_by,
                started_at=utc_now(),
            )
        )
        stage_attempt: dict[str, Any] | None = None
        inserted = updated = skipped = 0
        try:
            if not dry_run:
                stage = self.ensure_backtest_recording_stage(experiment_id)
                attempt_no = self._repo.next_attempt_no(experiment_id, BACKTEST_RECORDING_STAGE)
                stage_attempt = self._repo.create_stage_attempt(
                    StageAttemptRecord(
                        stage_id=str(stage["stage_id"]),
                        experiment_id=experiment_id,
                        stage_name=BACKTEST_RECORDING_STAGE,
                        attempt_no=attempt_no,
                        status="running",
                        input_json=dict(data),
                        started_at=utc_now(),
                    )
                )
                self._repo.update_stage_plan(str(stage["stage_id"]), {"status": "running", "latest_attempt_no": attempt_no})
                for record in records:
                    record = record.model_copy(update={"stage_attempt_id": str(stage_attempt["stage_attempt_id"])})
                    existed = record.record_key_sha256 in existing_keys
                    self._repo.upsert_backtest_record(record)
                    if existed:
                        updated += 1
                    else:
                        inserted += 1
                skipped = counts.get("would_skip_duplicate", 0)
                final_counts = {**counts, "inserted": inserted, "updated": updated, "skipped_duplicate": skipped, "excluded": excluded}
                self._repo.update_stage_attempt(
                    str(stage_attempt["stage_attempt_id"]),
                    {
                        "status": "passed",
                        "result_json": {"counts": final_counts, "backfill_run_id": run["backfill_run_id"], "record_version": RECORD_VERSION},
                        "completed_at": utc_now(),
                    },
                )
                self._repo.update_stage_plan(str(stage["stage_id"]), {"status": "passed", "latest_attempt_no": attempt_no})
                self._record_backfill_artifacts_and_event(
                    experiment_id=experiment_id,
                    stage_attempt_id=str(stage_attempt["stage_attempt_id"]),
                    payload=data,
                    fingerprint=fingerprint,
                    counts=final_counts,
                    created_by=created_by,
                )
                run = self._repo.update_backfill_run(
                    str(run["backfill_run_id"]),
                    {"status": "completed", "counts_json": final_counts, "stage_attempt_id": str(stage_attempt["stage_attempt_id"]), "completed_at": utc_now()},
                )
            return {"backfill_run_id": run["backfill_run_id"], "stage_attempt_id": stage_attempt and stage_attempt["stage_attempt_id"], "status": run["status"], "counts": run["counts_json"]}
        except Exception as exc:
            self._repo.update_backfill_run(str(run["backfill_run_id"]), {"status": "failed", "error_message": str(exc), "completed_at": utc_now()})
            if stage_attempt:
                self._repo.update_stage_attempt(str(stage_attempt["stage_attempt_id"]), {"status": "failed", "error_message": str(exc), "completed_at": utc_now()})
            raise

    def load_backfill_records(self, experiment_id: str, payload: Mapping[str, Any]) -> tuple[list[BacktestRecord], int, dict[str, Any]]:
        source_scope = dict(payload.get("source_scope") or {})
        source_file = source_scope.get("source_file") or payload.get("source_file")
        if not source_file:
            raise ValueError("source_file is required for historical HMM backfill")
        path = Path(str(source_file)).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        path = path.resolve()
        if not path.exists():
            raise ValueError(f"source_file does not exist: {path}")
        data = json.loads(path.read_text(encoding="utf-8"))
        selected = as_list(data.get("selected_representatives")) or as_list(data.get("qe_archive_representatives"))
        duplicates = as_list(data.get("duplicate_rejected_loops"))
        excluded_rows = as_list(data.get("excluded_loops"))
        records: list[BacktestRecord] = []
        seen: set[str] = set()
        for item in selected:
            if isinstance(item, Mapping):
                record = self.normalize_historical_record(item, experiment_id=experiment_id, selected_representative=True)
                if record.record_key_sha256 not in seen:
                    records.append(record)
                    seen.add(record.record_key_sha256)
        for item in duplicates:
            if isinstance(item, Mapping):
                record = self.normalize_historical_record(item, experiment_id=experiment_id, duplicate=True)
                if record.record_key_sha256 not in seen:
                    records.append(record)
                    seen.add(record.record_key_sha256)
        fingerprint = {
            "source_file": str(path),
            "source_file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "record_version": RECORD_VERSION,
            "selected_count": len(selected),
            "duplicate_count": len(duplicates),
            "excluded_count": len(excluded_rows),
        }
        return records, len(excluded_rows), fingerprint

    def ensure_backtest_recording_stage(self, experiment_id: str) -> dict[str, Any]:
        stage = self._repo.get_stage_plan(experiment_id, BACKTEST_RECORDING_STAGE)
        if stage:
            return stage
        stages = self._repo.list_stage_plans(experiment_id)
        max_order = max([int(row.get("stage_order") or 0) for row in stages], default=0)
        qe_shadow = next((row for row in stages if row.get("stage_name") == "qe_shadow"), None)
        order = int(qe_shadow.get("stage_order")) if qe_shadow else max_order + 1
        if qe_shadow:
            # Keep the display order stable for older experiments created before this stage existed.
            for row in sorted(stages, key=lambda item: int(item.get("stage_order") or 0), reverse=True):
                if int(row.get("stage_order") or 0) >= order:
                    self._repo.update_stage_plan(str(row["stage_id"]), {"stage_order": int(row.get("stage_order") or 0) + 1})
        return self._repo.create_stage_plan(StagePlanRecord(experiment_id=experiment_id, stage_name=BACKTEST_RECORDING_STAGE, stage_order=max(1, order), planned_config_json={"auto_added": True}))

    def record_qe_loop_completed(self, *, experiment_id: str, task_id: str, loop_id: str, loop_index: int | None = None) -> dict[str, Any]:
        record = self.normalize_qe_loop(experiment_id=experiment_id, task_id=task_id, loop_id=loop_id, loop_index=loop_index)
        if record is None:
            return {"recorded": False, "skipped_reason": "not_hmm_research_loop"}
        row = self._repo.upsert_backtest_record(record)
        return {"recorded": True, "record_id": row["record_id"], "record_key_sha256": row["record_key_sha256"]}

    def _record_backfill_artifacts_and_event(
        self,
        *,
        experiment_id: str,
        stage_attempt_id: str,
        payload: Mapping[str, Any],
        fingerprint: Mapping[str, Any],
        counts: Mapping[str, Any],
        created_by: str,
    ) -> None:
        source_file = dict(payload.get("source_scope") or {}).get("source_file") or payload.get("source_file")
        if source_file and hasattr(self._repo, "create_artifact_ref"):
            self._repo.create_artifact_ref(
                ArtifactRefRecord(
                    experiment_id=experiment_id,
                    stage_attempt_id=stage_attempt_id,
                    domain_type="file",
                    artifact_uri=str(source_file),
                    artifact_sha256=str(fingerprint.get("source_file_sha256") or "") or None,
                    status="validated",
                    metadata_json={"backfill_type": BACKFILL_TYPE, "record_version": RECORD_VERSION},
                )
            )
        if hasattr(self._repo, "create_pipeline_event"):
            self._repo.create_pipeline_event(
                PipelineEventRecord(
                    experiment_id=experiment_id,
                    stage_attempt_id=stage_attempt_id,
                    event_type="hmm_backtest_backfill_completed",
                    message="HMM backtest timeline backfill completed",
                    payload_json={"counts": dict(counts), "source_fingerprint": dict(fingerprint)},
                    created_by=created_by,
                )
            )

    def build_record_key(self, *, experiment_id: str, source_type: str, source_task_id: str, source_loop_id: str, source_loop_index: int | None) -> str:
        return sha256_json({"experiment_id": experiment_id, "source_type": source_type, "source_task_id": source_task_id, "source_loop_id": source_loop_id, "source_loop_index": source_loop_index, "record_version": RECORD_VERSION})

    def _preview_counts(self, records: list[BacktestRecord], excluded: int, existing_keys: set[str]) -> dict[str, int]:
        would_update = sum(1 for record in records if record.record_key_sha256 in existing_keys)
        would_insert = len(records) - would_update
        return {
            "candidate_count": len(records) + excluded,
            "research_timeline_count": len(records),
            "would_insert": would_insert,
            "would_update": would_update,
            "would_skip_duplicate": would_update,
            "qe_archive_representative_count": sum(1 for record in records if record.qe_archive_representative),
            "excluded": excluded,
        }

    def _existing_record_keys(self, experiment_id: str) -> set[str]:
        keys: set[str] = set()
        offset = 0
        while True:
            rows = self._repo.list_backtest_records(experiment_id, limit=500, offset=offset)
            if not rows:
                break
            keys.update(str(row["record_key_sha256"]) for row in rows if row.get("record_key_sha256"))
            if len(rows) < 500:
                break
            offset += 500
        return keys

    def _extract_metrics(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        metrics = as_dict(payload.get("metrics")) or as_dict(payload.get("metrics_json"))
        for key in ("ann", "mdd", "ir", "ic", "rank_ic", "sharpe", "turnover"):
            if key in payload and key not in metrics:
                metrics[key] = payload.get(key)
        return metrics

    def _extract_config_summary(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        summary = as_dict(payload.get("config_summary")) or as_dict(payload.get("config")) or as_dict(payload.get("family"))
        if not summary and "factor_sig" in payload:
            summary = {key: payload.get(key) for key in NON_HMM_CONFIG_KEYS if key in payload}
        if "factor_sig" not in summary and summary.get("factor_list"):
            factors = as_list(summary.get("factor_list"))
            summary["factor_count"] = len(factors)
            summary["factor_sig"] = stable_json_hash(sorted(map(str, factors)))
        return {key: summary.get(key) for key in NON_HMM_CONFIG_KEYS if summary.get(key) is not None}

    def _extract_hmm_summary(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        summary = as_dict(payload.get("hmm_config_summary"))
        if summary:
            return summary
        config = as_dict(payload.get("config"))
        return self._hmm_summary_from_params(config)

    def _hmm_summary_from_params(self, params: Mapping[str, Any]) -> dict[str, Any]:
        summary = {key: params.get(key) for key in HMM_FIELD_KEYS if params.get(key) is not None}
        hmm_config = as_dict(params.get("hmm_config_json"))
        if hmm_config:
            for key in ("config_version", "ui_label", "candidate_name", "display_name"):
                if key in hmm_config and key not in summary:
                    summary[key] = hmm_config[key]
        return summary

    def _config_summary_from_loop(self, config: Mapping[str, Any], custom_params: Mapping[str, Any]) -> dict[str, Any]:
        merged = {**dict(config), **dict(custom_params)}
        factor_list = as_list(config.get("factor_list") or config.get("factor_names"))
        return {
            "model_id": merged.get("model_id"),
            "model_source_task_id": merged.get("model_source_task_id"),
            "model_source_loop_index": merged.get("model_source_loop_index"),
            "strategy_id": merged.get("strategy_id"),
            "execution_algo": merged.get("execution_algo"),
            "stock_pool": merged.get("stock_pool"),
            "label_horizon": merged.get("label_horizon"),
            "disable_alpha158": merged.get("disable_alpha158"),
            "factor_count": len(factor_list) if factor_list else merged.get("factor_count"),
            "factor_sig": stable_json_hash(sorted(map(str, factor_list))) if factor_list else merged.get("factor_sig"),
        }

    def _compact_source_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        keys = ("task_id", "task_name", "loop_id", "loop_index", "experiment_id", "label", "archive_family_sig", "strict_family_sig", "config_family_sig", "reason")
        return {key: payload.get(key) for key in keys if key in payload}

    def _int_or_none(self, value: Any) -> int | None:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _is_hmm_research_payload(self, payload: Mapping[str, Any], *, task_name: Any = None) -> bool:
        if payload.get("record_backtest_to_research_pipeline") and payload.get("research_domain") == "hmm":
            return True
        if payload.get("enable_sector_hmm") or payload.get("hmm_model_version_id") or payload.get("hmm_config_json"):
            return True
        return "hmm" in str(task_name or "").lower()

    def _research_experiment_id(self, payload: Mapping[str, Any]) -> str | None:
        value = payload.get("research_experiment_id") or payload.get("research_pipeline_experiment_id")
        return str(value) if value else None

    def _fetch_qe_loop(self, *, task_id: str, loop_id: str, loop_index: int | None = None) -> dict[str, Any] | None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT l.loop_id, l.task_id, l.loop_index, l.config_json, l.metrics_json, l.experiment_id,
                           l.created_at, t.task_name, t.created_at AS task_created_at,
                           e.custom_params
                    FROM qe_evolution_loops l
                    LEFT JOIN qe_evolution_tasks t ON t.task_id = l.task_id
                    LEFT JOIN qe_experiments e ON e.experiment_id = l.experiment_id
                    WHERE l.task_id = %s AND l.loop_id = %s
                    """,
                    (task_id, loop_id),
                )
                row = cur.fetchone()
                if not row:
                    return None
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))


