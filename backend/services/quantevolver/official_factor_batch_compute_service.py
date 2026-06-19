"""Official WSL-only batch factor value + metric compute service."""
from __future__ import annotations

import gc
import hashlib
import json
import multiprocessing
import os
import queue
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import pandas as pd

from ...db.pg_pool import get_conn
from ...infra.wsl_qlib_runner import win_to_wsl_path
from .backtest_base_data_memory_cache import BacktestBaseDataMemoryCache
from .factor_eligibility_service import FactorEligibilityService
from .factor_universe_mask_service import OFFICIAL_FACTOR_UNIVERSE_KEY, FactorUniverseMaskService
from .factor_value_loader import FactorValueLoader
from .offline_code_text_factor_executor import FactorExecutionResult
from .offline_code_text_factor_executor import OfflineCodeTextFactorExecutor
from .wsl_runtime_guard import assert_wsl_runtime

REPO_ROOT = Path(__file__).resolve().parents[3]
OFFICIAL_FACTOR_CACHE_ROOT = REPO_ROOT / "rdagent_assets" / "factor_values"
OFFICIAL_FACTOR_CACHE_SINGLE_DIR = OFFICIAL_FACTOR_CACHE_ROOT / "single"
OFFICIAL_FACTOR_CACHE_META_PATH = OFFICIAL_FACTOR_CACHE_ROOT / "_meta.json"
OFFICIAL_FACTOR_WINDOW_START = "2018-08-01"
OFFICIAL_FACTOR_WINDOW_END = "2026-04-30"
OFFICIAL_CACHE_SCHEMA_VERSION = "official_factor_cache_v2"
OFFICIAL_CACHE_SOURCE_SYSTEM = "official_offline_backtest_factor_data"
DEFAULT_BATCH_SIZE = 16
DEFAULT_METRIC_WORKERS = 2
DEFAULT_SOFT_RSS_MB = 48 * 1024
DEFAULT_HARD_RSS_MB = 55 * 1024
DEFAULT_MIN_AVAILABLE_MB = 8 * 1024
DEFAULT_SWAP_GROWTH_HARD_STOP_MB = 1024
DEFAULT_RESOURCE_POLL_SEC = 5.0
DEFAULT_ONE_WORKER_AVAILABLE_MULTIPLIER = 5
DEFAULT_TWO_WORKER_AVAILABLE_MULTIPLIER = 8
DEFAULT_RESULT_DRAIN_CHUNK_SIZE = 1
RESOURCE_GATE_FAILED = "memory_gate_failed"
FACTOR_TIMEOUT = "factor_timeout"
FactorResultHandler = Callable[[str, FactorExecutionResult], None]


@dataclass(frozen=True)
class ResourceSnapshot:
    rss_mb: float
    uss_mb: float | None
    swap_mb: float
    available_mb: float | None
    pss_mb: float | None = None
    process_count: int = 1
    rss_raw_mb: float | None = None


@dataclass(frozen=True)
class FactorResourceLimits:
    soft_rss_mb: int = DEFAULT_SOFT_RSS_MB
    hard_rss_mb: int = DEFAULT_HARD_RSS_MB
    min_available_mb: int = DEFAULT_MIN_AVAILABLE_MB
    swap_growth_hard_stop_mb: int = DEFAULT_SWAP_GROWTH_HARD_STOP_MB

    @classmethod
    def from_env(cls) -> "FactorResourceLimits":
        return cls(
            soft_rss_mb=_env_int("AISTOCK_OFFICIAL_FACTOR_SOFT_RSS_MB", DEFAULT_SOFT_RSS_MB),
            hard_rss_mb=_env_int("AISTOCK_OFFICIAL_FACTOR_HARD_RSS_MB", DEFAULT_HARD_RSS_MB),
            min_available_mb=_env_int("AISTOCK_OFFICIAL_FACTOR_MIN_AVAILABLE_MB", DEFAULT_MIN_AVAILABLE_MB),
            swap_growth_hard_stop_mb=_env_int(
                "AISTOCK_OFFICIAL_FACTOR_SWAP_GROWTH_HARD_STOP_MB",
                DEFAULT_SWAP_GROWTH_HARD_STOP_MB,
            ),
        )


@dataclass(frozen=True)
class ResourceGateDecision:
    ok: bool
    reason: str | None
    detail: dict[str, Any]


@dataclass
class BatchComputeConfig:
    factor_names: list[str] | None
    factor_data_dir: str
    start_date: str
    end_date: str
    include_disabled: bool = False
    batch_size: int = DEFAULT_BATCH_SIZE
    workers: int = 1
    force: bool = False
    timeout_per_factor: int = 1800
    qlib_bin_path: str | None = None
    task_id: str | None = None
    validation_mode: str | None = None
    expected_factor_count: int | None = None


class OfficialFactorBatchComputeService:
    """Compute official factor values from catalog code_text using backtest data only."""

    def __init__(self, event_emitter: Optional[Callable[[dict[str, Any]], None]] = None) -> None:
        self._eligibility_service = FactorEligibilityService()
        self._universe_service = FactorUniverseMaskService()
        self._event_emitter = event_emitter
        self._active_meta_context: dict[str, Any] | None = None
        self._resource_limits = FactorResourceLimits.from_env()

    def compute(self, config: BatchComputeConfig | dict[str, Any]) -> dict[str, Any]:
        cfg = self._coerce_config(config)
        assert_wsl_runtime("official_factor_full_compute")
        self._assert_official_cache_root()
        task_id = cfg.task_id or f"official_factor_full_{int(time.time() * 1000)}"
        started = time.time()
        start_date = _normalize_date(cfg.start_date)
        end_date = _normalize_date(cfg.end_date)
        if start_date > end_date:
            raise ValueError(f"invalid factor cache window: {start_date} > {end_date}")
        self._emit("preflight", task_id=task_id, data_start=start_date, data_end=end_date)

        eligible = self._eligibility_service.list_eligible_factors(
            factor_names=cfg.factor_names,
            include_disabled=cfg.include_disabled,
            source_mode="official_offline",
        )
        requested = list(cfg.factor_names or [row["factor_name"] for row in eligible])
        eligible_names = [row["factor_name"] for row in eligible]
        skipped = sorted(set(requested) - set(eligible_names)) if cfg.factor_names else []
        if not eligible:
            return {
                "success": False,
                "status": "failed",
                "error": "no official-offline factors with catalog code_text",
                "requested_factors": requested,
                "eligible_factors": [],
                "skipped_factors": skipped,
            }

        resource_at_start = _resource_snapshot()
        base_cache = BacktestBaseDataMemoryCache.load_once(cfg.factor_data_dir, start_date, end_date)
        base_cache_manifest = base_cache.manifest()
        resource_after_base = _resource_snapshot()
        self._emit(
            "base_cache_loaded",
            task_id=task_id,
            rss_mb=resource_after_base.rss_mb,
            pss_mb=resource_after_base.pss_mb,
            uss_mb=resource_after_base.uss_mb,
            swap_mb=resource_after_base.swap_mb,
            available_mb=resource_after_base.available_mb,
            resource_limits=asdict(self._resource_limits),
            base_cache=base_cache_manifest,
        )

        universe_meta = self._universe_service.metadata(
            start_date=start_date,
            end_date=end_date,
            universe_key=OFFICIAL_FACTOR_UNIVERSE_KEY,
            refresh_policy="coverage",
        )
        universe_meta.setdefault("universe_key", OFFICIAL_FACTOR_UNIVERSE_KEY)
        universe_meta.setdefault("index_policy", "st_pit_buy_eligible_reindexed_v1")
        eligible_index = self._universe_service.build_eligible_index(
            start_date=start_date,
            end_date=end_date,
            universe_key=OFFICIAL_FACTOR_UNIVERSE_KEY,
        )

        qlib_bin_path: Path | None = None
        metrics_ctx = None
        metrics_error: str | None = None
        calc_batch_id = str(uuid.uuid4())
        try:
            from .qe_eval_v2_metric_engine import compute_single_factor_metrics, prepare_shared_context

            qlib_bin_path = self._resolve_qlib_bin_path(cfg.qlib_bin_path)
            metrics_ctx = prepare_shared_context(
                qlib_bin_path=qlib_bin_path,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:
            metrics_error = f"metrics_context_failed: {type(exc).__name__}: {exc}"
            self._emit("failed", task_id=task_id, error=metrics_error)
            compute_single_factor_metrics = None  # type: ignore[assignment]
        self._active_meta_context = {
            "data_start": start_date,
            "data_end": end_date,
            "factor_data_dir": str(Path(cfg.factor_data_dir).expanduser()),
            "qlib_bin_path": str(qlib_bin_path) if qlib_bin_path else None,
            "universe_meta": universe_meta,
            "base_cache_manifest": base_cache_manifest,
        }

        factor_ids = self._load_factor_ids()
        executor = OfflineCodeTextFactorExecutor(base_cache)
        OFFICIAL_FACTOR_CACHE_SINGLE_DIR.mkdir(parents=True, exist_ok=True)
        results: list[dict[str, Any]] = []
        db_result = {"inserted": 0, "skipped": 0, "errors": [], "save_failures": []}
        success_count = 0
        fail_count = 0
        batches = list(_chunks(eligible, max(1, int(cfg.batch_size or DEFAULT_BATCH_SIZE))))
        memory_samples: list[dict[str, Any]] = []
        resource_failures: list[dict[str, Any]] = []

        for batch_index, batch in enumerate(batches, start=1):
            batch_id = f"{task_id}_b{batch_index:04d}"
            before_decision = self._check_resource_gate(
                "before_batch",
                swap_baseline_mb=resource_at_start.swap_mb,
                task_id=task_id,
                batch_id=batch_id,
                batch_index=batch_index,
            )
            if not before_decision.ok:
                resource_failures.append(before_decision.detail)
                remaining = [item for group in batches[batch_index - 1:] for item in group]
                self._append_resource_failure_results(remaining, results, before_decision)
                fail_count += len(remaining)
                break
            resource_before_batch = _resource_snapshot()
            requested_workers = max(1, int(cfg.workers or 1))
            effective_workers = self._select_batch_workers(cfg.workers, resource_before_batch)
            limits = getattr(self, "_resource_limits", FactorResourceLimits())
            memory_samples.append({
                "event": "batch_started",
                "batch_id": batch_id,
                "batch_index": batch_index,
                "factor_count": len(batch),
                "requested_workers": requested_workers,
                "effective_workers": effective_workers,
                "rss_mb": resource_before_batch.rss_mb,
                "swap_mb": resource_before_batch.swap_mb,
                "available_mb": resource_before_batch.available_mb,
            })
            if effective_workers < requested_workers:
                self._emit(
                    "worker_throttled",
                    task_id=task_id,
                    batch_id=batch_id,
                    batch_index=batch_index,
                    requested_workers=requested_workers,
                    effective_workers=effective_workers,
                    available_mb=resource_before_batch.available_mb,
                    min_available_mb=limits.min_available_mb,
                    reason="available_memory_headroom",
                )
            self._emit(
                "batch_started",
                task_id=task_id,
                batch_id=batch_id,
                batch_index=batch_index,
                batch_count=len(batches),
                factor_count=len(batch),
                requested_workers=requested_workers,
                effective_workers=effective_workers,
                rss_mb=resource_before_batch.rss_mb,
                swap_mb=resource_before_batch.swap_mb,
                available_mb=resource_before_batch.available_mb,
            )
            pending_written_paths: list[Path] = []
            batch_resource_failure: ResourceGateDecision | None = None
            pending_success_frames: list[tuple[str, pd.DataFrame]] = []
            pending_success_meta: dict[str, dict[str, Any]] = {}
            batch_resource_failure_recorded = False
            row_by_name = {str(row.get("factor_name") or "").strip(): row for row in batch}
            handled_names: set[str] = set()

            def _flush_success_frames() -> None:
                nonlocal success_count
                if not pending_success_frames:
                    return
                self._update_meta_atomic(
                    pending_success_meta,
                    data_start=start_date,
                    data_end=end_date,
                    factor_data_dir=str(Path(cfg.factor_data_dir).expanduser()),
                    qlib_bin_path=str(qlib_bin_path) if qlib_bin_path else None,
                    universe_meta=universe_meta,
                    base_cache_manifest=base_cache_manifest,
                )
                pending_written_paths.clear()
                success_count += self._drain_success_frames(
                    pending_success_frames,
                    results,
                    db_result=db_result,
                    metrics_error=metrics_error,
                    metrics_ctx=metrics_ctx,
                    compute_single_factor_metrics=compute_single_factor_metrics,
                    calc_batch_id=calc_batch_id,
                    end_date=end_date,
                    factor_ids=factor_ids,
                    batch_id=batch_id,
                )
                pending_success_frames.clear()
                pending_success_meta.clear()
                gc.collect()

            def _handle_exec_result(name: str, exec_result: FactorExecutionResult) -> None:
                nonlocal fail_count, batch_resource_failure, batch_resource_failure_recorded
                row = row_by_name.get(name)
                handled_names.add(name)
                if row is None:
                    fail_count += 1
                    results.append({
                        "name": name,
                        "success": False,
                        "error": "unexpected_execution_result",
                        "error_type": "unexpected_execution_result",
                    })
                    return
                if not exec_result.success or exec_result.dataframe is None:
                    fail_count += 1
                    err = exec_result.error or "unknown"
                    if exec_result.error_type == RESOURCE_GATE_FAILED and not batch_resource_failure_recorded:
                        resource_failures.append({
                            "phase": "during_batch",
                            "task_id": task_id,
                            "batch_id": batch_id,
                            "batch_index": batch_index,
                            "reason": err.replace(f"{RESOURCE_GATE_FAILED}: ", "", 1),
                            "error_type": RESOURCE_GATE_FAILED,
                        })
                        batch_resource_failure = ResourceGateDecision(
                            False,
                            err.replace(f"{RESOURCE_GATE_FAILED}: ", "", 1),
                            resource_failures[-1],
                        )
                        batch_resource_failure_recorded = True
                    results.append({
                        "name": name,
                        "success": False,
                        "error": err,
                        "error_type": exec_result.error_type or "unknown",
                    })
                    self._record_error_meta(name, row.get("code_text"), err)
                    return
                df = exec_result.dataframe.reindex(eligible_index)
                exec_result.dataframe = None
                nan_rate = float(df.iloc[:, 0].isna().mean()) if len(df) else 0.0
                source_hash = _code_hash(str(row.get("code_text") or ""))
                meta_entry = {
                    "status": "ok",
                    "computed_at": datetime.now(timezone.utc).isoformat(),
                    "rows": int(len(df)),
                    "nan_rate": nan_rate,
                    "date_range": f"{start_date}~{end_date}",
                    "as_of_date": end_date,
                    "code_hash": source_hash,
                    "source_hash_raw": source_hash,
                    "code_source": "code_text",
                    "data_source_mode": "official_offline_backtest_factor_data",
                    "factor_data_dir": str(Path(cfg.factor_data_dir).expanduser()),
                    "window_train_start": start_date,
                    "window_backtest_end": end_date,
                    "batch_id": batch_id,
                }
                meta_entry.update(universe_meta)
                parquet_path = self._write_single_atomic(name, df)
                pending_written_paths.append(parquet_path)
                pending_success_meta[name] = meta_entry
                self._emit(
                    "factor_done",
                    task_id=task_id,
                    batch_id=batch_id,
                    factor_name=name,
                    rows=int(len(df)),
                    nan_rate=round(nan_rate, 6),
                    elapsed_sec=exec_result.elapsed_sec,
                )
                pending_success_frames.append((name, df))
                if len(pending_success_frames) >= self._result_drain_chunk_size():
                    _flush_success_frames()

            try:
                batch_exec = self._compute_batch_frames(
                    executor,
                    batch,
                    workers=effective_workers,
                    timeout_per_factor=cfg.timeout_per_factor,
                    task_id=task_id,
                    batch_id=batch_id,
                    swap_baseline_mb=resource_at_start.swap_mb,
                    result_handler=_handle_exec_result,
                )
                for row in batch:
                    name = str(row.get("factor_name") or "").strip()
                    if name in handled_names:
                        continue
                    _handle_exec_result(
                        name,
                        batch_exec.get(name)
                        or FactorExecutionResult(
                            factor_name=name,
                            success=False,
                            error="missing_execution_result",
                            error_type="missing_execution_result",
                        ),
                    )
                _flush_success_frames()
            except Exception:
                for path in pending_written_paths:
                    try:
                        path.unlink(missing_ok=True)
                    except OSError as unlink_exc:
                        self._emit(
                            "cleanup_warning",
                            task_id=task_id,
                            batch_id=batch_id,
                            path=str(path),
                            error=str(unlink_exc),
                        )
                raise

            FactorValueLoader.invalidate_single_cache()
            gc.collect()
            resource_after_release = _resource_snapshot()
            single_cache_entries = len(getattr(FactorValueLoader, "_single_cache", {}))
            memory_samples.append({
                "event": "batch_released",
                "batch_id": batch_id,
                "batch_index": batch_index,
                "rss_mb": resource_after_release.rss_mb,
                "swap_mb": resource_after_release.swap_mb,
                "available_mb": resource_after_release.available_mb,
                "single_cache_entries": single_cache_entries,
            })
            self._emit(
                "batch_released",
                task_id=task_id,
                batch_id=batch_id,
                rss_mb=resource_after_release.rss_mb,
                swap_mb=resource_after_release.swap_mb,
                available_mb=resource_after_release.available_mb,
                single_cache_entries=single_cache_entries,
            )
            if batch_resource_failure is not None:
                remaining = [item for group in batches[batch_index:] for item in group]
                self._append_resource_failure_results(remaining, results, batch_resource_failure)
                fail_count += len(remaining)
                self._emit(
                    "resource_gate_abort",
                    task_id=task_id,
                    batch_id=batch_id,
                    batch_index=batch_index,
                    remaining_factor_count=len(remaining),
                    reason=batch_resource_failure.reason,
                )
                break
            after_decision = self._check_resource_gate(
                "after_batch",
                swap_baseline_mb=resource_at_start.swap_mb,
                task_id=task_id,
                batch_id=batch_id,
                batch_index=batch_index,
            )
            if not after_decision.ok:
                resource_failures.append(after_decision.detail)
                remaining = [item for group in batches[batch_index:] for item in group]
                self._append_resource_failure_results(remaining, results, after_decision)
                fail_count += len(remaining)
                break

        if metrics_ctx is not None:
            del metrics_ctx
        del base_cache
        self._active_meta_context = None
        gc.collect()
        overall_success = success_count > 0 and fail_count == 0 and not db_result.get("save_failures") and not metrics_error
        summary = {
            "total": len(results),
            "success": success_count,
            "failed": fail_count,
            "success_rate": f"{(success_count / len(results) * 100):.1f}%" if results else "0.0%",
            "factor_results": results,
            "output_path": str(OFFICIAL_FACTOR_CACHE_SINGLE_DIR),
            "total_elapsed_sec": round(time.time() - started, 1),
            "batch_count": len(batches),
        }
        runtime_validation = self._build_runtime_validation_report(
            cfg=cfg,
            task_id=task_id,
            requested=requested,
            eligible_names=eligible_names,
            skipped=skipped,
            results=results,
            success_count=success_count,
            fail_count=fail_count,
            db_result=db_result,
            metrics_error=metrics_error,
            batch_count=len(batches),
            memory_samples=memory_samples,
            resource_failures=resource_failures,
            universe_meta=universe_meta,
            start_date=start_date,
            end_date=end_date,
        )
        result = {
            "success": overall_success,
            "status": "success" if overall_success else "failed",
            "task_id": task_id,
            "requested_factors": requested,
            "eligible_factors": eligible_names,
            "skipped_factors": skipped,
            "pipeline_version": "official_factor_cache_v2",
            "code_source": "code_text",
            "cache_source": OFFICIAL_CACHE_SOURCE_SYSTEM,
            "cache_root": str(OFFICIAL_FACTOR_CACHE_ROOT),
            "single_dir": str(OFFICIAL_FACTOR_CACHE_SINGLE_DIR),
            "data_start": start_date,
            "data_end": end_date,
            "snapshot_date": end_date,
            "as_of_date": end_date,
            "universe_metadata": universe_meta,
            "pipeline_summary": summary,
            "db_result": db_result,
            "success_count": success_count,
            "fail_count": fail_count,
            "total_metrics_inserted": db_result["inserted"],
            "total_metrics_skipped": db_result["skipped"],
            "runtime_validation": runtime_validation,
        }
        if metrics_error or db_result.get("errors") or fail_count:
            result["error"] = " | ".join([x for x in [metrics_error, "; ".join(db_result.get("errors", [])[:5])] if x]) or f"failed factors={fail_count}"
        self._emit("success" if overall_success else "failed", task_id=task_id, result=result)
        return result

    def _coerce_config(self, raw: BatchComputeConfig | dict[str, Any]) -> BatchComputeConfig:
        if isinstance(raw, BatchComputeConfig):
            return raw
        data = dict(raw or {})
        return BatchComputeConfig(
            factor_names=list(data.get("factor_names") or []) or None,
            factor_data_dir=str(data.get("factor_data_dir") or ""),
            start_date=str(data.get("start_date") or data.get("window_train_start") or OFFICIAL_FACTOR_WINDOW_START),
            end_date=str(data.get("end_date") or data.get("window_backtest_end") or data.get("data_date") or OFFICIAL_FACTOR_WINDOW_END),
            include_disabled=bool(data.get("include_disabled", False)),
            batch_size=int(data.get("batch_size") or DEFAULT_BATCH_SIZE),
            workers=int(data.get("workers") or data.get("max_workers") or 1),
            force=bool(data.get("force", False)),
            timeout_per_factor=int(data.get("timeout_per_factor") or 1800),
            qlib_bin_path=data.get("qlib_bin_path"),
            task_id=data.get("task_id"),
            validation_mode=(str(data.get("validation_mode")).strip() or None) if data.get("validation_mode") is not None else None,
            expected_factor_count=(
                int(data.get("expected_factor_count"))
                if data.get("expected_factor_count") is not None
                else None
            ),
        )

    def _select_batch_workers(self, requested_workers: int, snapshot: ResourceSnapshot) -> int:
        requested = max(1, int(requested_workers or 1))
        available_mb = snapshot.available_mb
        if available_mb is None:
            return requested
        limits = getattr(self, "_resource_limits", FactorResourceLimits())
        min_available_mb = max(1, int(limits.min_available_mb or DEFAULT_MIN_AVAILABLE_MB))
        if available_mb < min_available_mb * DEFAULT_ONE_WORKER_AVAILABLE_MULTIPLIER:
            return 1
        if available_mb < min_available_mb * DEFAULT_TWO_WORKER_AVAILABLE_MULTIPLIER:
            return min(requested, 2)
        return requested

    def _result_drain_chunk_size(self) -> int:
        return max(1, _env_int("AISTOCK_OFFICIAL_FACTOR_RESULT_DRAIN_CHUNK_SIZE", DEFAULT_RESULT_DRAIN_CHUNK_SIZE))

    def _drain_success_frames(
        self,
        frames: list[tuple[str, pd.DataFrame]],
        results: list[dict[str, Any]],
        *,
        db_result: dict[str, Any],
        metrics_error: str | None,
        metrics_ctx: Any,
        compute_single_factor_metrics: Any,
        calc_batch_id: str,
        end_date: str,
        factor_ids: dict[str, int],
        batch_id: str,
    ) -> int:
        success_delta = 0
        for name, df in list(frames):
            if metrics_error or metrics_ctx is None or compute_single_factor_metrics is None:
                db_result["errors"].append(f"{name}: {metrics_error or 'metrics context missing'}")
                db_result["save_failures"].append(name)
            else:
                try:
                    metric_result = compute_single_factor_metrics(name, df.rename(columns={"value": name}), metrics_ctx)
                    metrics_by_window = metric_result.get("metrics", {}) if isinstance(metric_result, dict) else {}
                    flat_metrics = []
                    for window_name, window_metrics in metrics_by_window.items():
                        rec = dict(window_metrics)
                        rec["factor_name"] = name
                        rec["eval_window"] = window_name
                        flat_metrics.append(rec)
                    if flat_metrics:
                        save_result = self._metric_writer()._save_metrics(
                            {"metrics": flat_metrics, "calc_batch_id": calc_batch_id},
                            snapshot_date=end_date,
                            factor_ids=factor_ids,
                        )
                        db_result["inserted"] += int(save_result.get("inserted") or 0)
                        db_result["skipped"] += int(save_result.get("skipped") or 0)
                        if save_result.get("errors"):
                            db_result["errors"].extend(save_result["errors"])
                        full_metrics = metrics_by_window.get("full", {})
                        monthly_series = full_metrics.get("monthly_ic_series")
                        if monthly_series:
                            self._metric_writer()._save_monthly_ic(name, end_date, monthly_series)
                    else:
                        db_result["errors"].append(f"{name}: metrics_empty")
                except Exception as exc:
                    db_result["errors"].append(f"{name}: {type(exc).__name__}: {exc}")
                    db_result["save_failures"].append(name)

            results.append({
                "name": name,
                "success": True,
                "rows": int(len(df)),
                "nan_rate": round(float(df.iloc[:, 0].isna().mean()) if len(df) else 0.0, 6),
                "batch_id": batch_id,
                "error": None,
            })
            success_delta += 1
        return success_delta

    def _compute_batch_frames(
        self,
        executor: OfflineCodeTextFactorExecutor,
        batch: list[dict[str, Any]],
        *,
        workers: int,
        timeout_per_factor: int = 1800,
        task_id: str | None = None,
        batch_id: str | None = None,
        swap_baseline_mb: float | None = None,
        result_handler: FactorResultHandler | None = None,
    ) -> dict[str, Any]:
        max_workers = max(1, min(int(workers or 1), len(batch) or 1))
        timeout_sec = max(1, int(timeout_per_factor or 1800))
        baseline_mb = _resource_snapshot().swap_mb if swap_baseline_mb is None else swap_baseline_mb
        if os.name != "nt" and "fork" in multiprocessing.get_all_start_methods():
            return self._compute_batch_frames_with_process_timeouts(
                executor,
                batch,
                max_workers=max_workers,
                timeout_sec=timeout_sec,
                task_id=task_id,
                batch_id=batch_id,
                swap_baseline_mb=baseline_mb,
                result_handler=result_handler,
            )
        if max_workers <= 1:
            results = executor.compute_batch(batch)
            if result_handler is not None:
                for name, result in list(results.items()):
                    result_handler(name, result)
                return {}
            return results

        results: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="official-factor") as pool:
            futures = {
                pool.submit(
                    executor.compute_factor,
                    str(item.get("factor_name") or "").strip(),
                    str(item.get("code_text") or ""),
                ): str(item.get("factor_name") or "").strip() or "<missing>"
                for item in batch
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # defensive; compute_factor normally captures errors
                    from .offline_code_text_factor_executor import FactorExecutionResult

                    result = FactorExecutionResult(
                        factor_name=name,
                        success=False,
                        error=f"{type(exc).__name__}: {exc}",
                        error_type=type(exc).__name__,
                    )
                if result_handler is not None:
                    result_handler(name, result)
                else:
                    results[name] = result
        return results

    def _compute_batch_frames_with_process_timeouts(
        self,
        executor: OfflineCodeTextFactorExecutor,
        batch: list[dict[str, Any]],
        *,
        max_workers: int,
        timeout_sec: int,
        task_id: str | None,
        batch_id: str | None,
        swap_baseline_mb: float,
        result_handler: FactorResultHandler | None = None,
    ) -> dict[str, FactorExecutionResult]:
        ctx = multiprocessing.get_context("fork")
        result_queue = ctx.Queue()
        pending = list(batch)
        running: dict[str, dict[str, Any]] = {}
        results: dict[str, FactorExecutionResult] = {}
        last_resource_gate_check = 0.0
        queue_safe_to_drain = True

        try:
            while pending or running:
                while pending and len(running) < max_workers:
                    item = pending.pop(0)
                    name = str(item.get("factor_name") or "").strip() or "<missing>"
                    code_text = str(item.get("code_text") or "")
                    if not code_text.strip():
                        result = FactorExecutionResult(
                            factor_name=name,
                            success=False,
                            error="missing_code_text",
                            error_type="missing_code_text",
                        )
                        if result_handler is not None:
                            result_handler(name, result)
                        else:
                            results[name] = result
                        continue
                    proc = ctx.Process(
                        target=_factor_process_worker,
                        args=(result_queue, executor, name, code_text),
                        name=f"official-factor-{name[:32]}",
                    )
                    proc.start()
                    running[name] = {
                        "process": proc,
                        "started": time.monotonic(),
                        "pid": proc.pid,
                    }
                    self._emit(
                        "factor_started",
                        task_id=task_id,
                        batch_id=batch_id,
                        factor_name=name,
                        pid=proc.pid,
                        timeout_sec=timeout_sec,
                    )

                self._drain_factor_results(result_queue, running, results, result_handler=result_handler)
                now = time.monotonic()
                for name, state in list(running.items()):
                    proc = state["process"]
                    elapsed = now - float(state["started"])
                    if not proc.is_alive():
                        proc.join(timeout=0.1)
                        self._drain_factor_results(result_queue, running, results, result_handler=result_handler)
                        if name not in results:
                            result = FactorExecutionResult(
                                factor_name=name,
                                success=False,
                                elapsed_sec=round(elapsed, 3),
                                error="factor process exited without returning a result",
                                error_type="factor_process_no_result",
                            )
                            if result_handler is not None:
                                result_handler(name, result)
                            else:
                                results[name] = result
                        running.pop(name, None)
                        continue
                    if elapsed <= timeout_sec:
                        continue
                    self._terminate_factor_process(proc)
                    if proc.is_alive():
                        proc.join(timeout=5)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(timeout=5)
                    result = FactorExecutionResult(
                        factor_name=name,
                        success=False,
                        elapsed_sec=round(elapsed, 3),
                        error=f"factor execution exceeded timeout_per_factor={timeout_sec}s",
                        error_type=FACTOR_TIMEOUT,
                    )
                    if result_handler is not None:
                        result_handler(name, result)
                    else:
                        results[name] = result
                    self._emit(
                        "factor_timeout",
                        task_id=task_id,
                        batch_id=batch_id,
                        factor_name=name,
                        pid=state.get("pid"),
                        elapsed_sec=round(elapsed, 3),
                        timeout_sec=timeout_sec,
                    )
                    running.pop(name, None)

                if running and now - last_resource_gate_check >= DEFAULT_RESOURCE_POLL_SEC:
                    last_resource_gate_check = now
                    gate = self._check_resource_gate(
                        "during_batch",
                        swap_baseline_mb=swap_baseline_mb,
                        task_id=task_id or "official_factor_batch",
                        batch_id=batch_id,
                        extra_pids=[state["pid"] for state in running.values() if state.get("pid")],
                    )
                    if not gate.ok:
                        queue_safe_to_drain = False
                        self._terminate_running_factors(running)
                        now = time.monotonic()
                        for name, state in list(running.items()):
                            elapsed = now - float(state.get("started") or now)
                            result = self._resource_failure_result(name, gate, elapsed_sec=elapsed)
                            if result_handler is not None:
                                result_handler(name, result)
                            else:
                                results[name] = result
                        running.clear()
                        for item in pending:
                            name = str(item.get("factor_name") or "").strip() or "<missing>"
                            result = self._resource_failure_result(name, gate)
                            if result_handler is not None:
                                result_handler(name, result)
                            else:
                                results[name] = result
                        pending.clear()
                        break

                if pending or running:
                    time.sleep(0.2)

            if queue_safe_to_drain:
                self._drain_factor_results(result_queue, running, results, result_handler=result_handler)
        finally:
            self._terminate_running_factors(running)
            try:
                if not queue_safe_to_drain and hasattr(result_queue, "cancel_join_thread"):
                    result_queue.cancel_join_thread()
                result_queue.close()
                result_queue.join_thread()
            except Exception:
                pass
        return results

    def _drain_factor_results(
        self,
        result_queue: multiprocessing.Queue,
        running: dict[str, dict[str, Any]],
        results: dict[str, FactorExecutionResult],
        *,
        result_handler: FactorResultHandler | None = None,
    ) -> None:
        while True:
            try:
                name, result = result_queue.get_nowait()
            except (queue.Empty, EOFError, OSError, ValueError):
                return
            if name not in results:
                if not isinstance(result, FactorExecutionResult):
                    result = FactorExecutionResult(
                        factor_name=name,
                        success=False,
                        error=f"unexpected factor worker result type: {type(result).__name__}",
                        error_type="factor_worker_result_invalid",
                    )
                if result_handler is not None:
                    result_handler(name, result)
                else:
                    results[name] = result
            state = running.pop(name, None)
            if state is not None:
                proc = state["process"]
                proc.join(timeout=1)

    def _terminate_running_factors(self, running: dict[str, dict[str, Any]]) -> None:
        for state in list(running.values()):
            proc = state.get("process")
            if proc is None:
                continue
            try:
                if proc.is_alive():
                    proc.terminate()
                    proc.join(timeout=5)
                if proc.is_alive():
                    proc.kill()
                    proc.join(timeout=5)
            except Exception:
                continue

    def _terminate_factor_process(self, proc: multiprocessing.Process) -> None:
        try:
            import signal

            os.kill(int(proc.pid), signal.SIGKILL)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass

    def _resource_failure_result(
        self,
        factor_name: str,
        decision: ResourceGateDecision,
        *,
        elapsed_sec: float = 0.0,
    ) -> FactorExecutionResult:
        reason = decision.reason or RESOURCE_GATE_FAILED
        return FactorExecutionResult(
            factor_name=factor_name,
            success=False,
            elapsed_sec=round(elapsed_sec, 3),
            error=f"{RESOURCE_GATE_FAILED}: {reason}",
            error_type=RESOURCE_GATE_FAILED,
        )

    def _check_resource_gate(
        self,
        phase: str,
        *,
        swap_baseline_mb: float,
        task_id: str,
        batch_id: str | None = None,
        batch_index: int | None = None,
        extra_pids: Iterable[int] | None = None,
    ) -> ResourceGateDecision:
        snapshot = _resource_snapshot(extra_pids=extra_pids)
        limits = getattr(self, "_resource_limits", FactorResourceLimits())
        swap_growth_mb = max(0.0, snapshot.swap_mb - swap_baseline_mb)
        detail = {
            "phase": phase,
            "task_id": task_id,
            "batch_id": batch_id,
            "batch_index": batch_index,
            "rss_mb": snapshot.rss_mb,
            "uss_mb": snapshot.uss_mb,
            "swap_mb": snapshot.swap_mb,
            "swap_growth_mb": round(swap_growth_mb, 2),
            "available_mb": snapshot.available_mb,
            "pss_mb": snapshot.pss_mb,
            "process_count": snapshot.process_count,
            "rss_raw_mb": snapshot.rss_raw_mb,
            "limits": asdict(limits),
        }
        memory_for_gate_mb = snapshot.pss_mb or snapshot.uss_mb or snapshot.rss_mb
        reason: str | None = None
        if memory_for_gate_mb >= limits.hard_rss_mb:
            reason = "hard_rss_limit_exceeded"
        elif snapshot.available_mb is not None and snapshot.available_mb < limits.min_available_mb:
            reason = "available_memory_below_minimum"
        elif swap_growth_mb >= limits.swap_growth_hard_stop_mb:
            reason = "swap_growth_hard_stop_exceeded"

        if reason:
            detail["reason"] = reason
            self._emit("resource_gate_failed", **detail)
            return ResourceGateDecision(False, reason, detail)

        if memory_for_gate_mb >= limits.soft_rss_mb:
            warning = dict(detail)
            warning["reason"] = "soft_rss_limit_exceeded"
            self._emit("resource_gate_warning", **warning)
        return ResourceGateDecision(True, None, detail)

    def _append_resource_failure_results(
        self,
        factors: list[dict[str, Any]],
        results: list[dict[str, Any]],
        decision: ResourceGateDecision,
    ) -> None:
        reason = decision.reason or RESOURCE_GATE_FAILED
        for item in factors:
            results.append({
                "name": str(item.get("factor_name") or "").strip() or "<missing>",
                "success": False,
                "error": f"{RESOURCE_GATE_FAILED}: {reason}",
                "error_type": RESOURCE_GATE_FAILED,
                "resource_gate": decision.detail,
            })

    def _resolve_qlib_bin_path(self, qlib_bin_path: str | None) -> Path | None:
        candidates = [qlib_bin_path, os.getenv("QLIB_BIN_PATH"), str(REPO_ROOT / "qlib_bin" / "qlib_bin_20260311")]
        for item in candidates:
            if not item:
                continue
            p = Path(win_to_wsl_path(str(item)) if os.name != "nt" else str(item))
            if p.exists():
                return p
        return None

    def _load_factor_ids(self) -> dict[str, int]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT factor_name, id FROM aistock_factor_catalog")
                return {str(row[0]): int(row[1]) for row in cur.fetchall()}

    def _write_single_atomic(self, factor_name: str, df: pd.DataFrame) -> Path:
        path = OFFICIAL_FACTOR_CACHE_SINGLE_DIR / f"{factor_name}.parquet"
        tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        df.rename(columns={df.columns[0]: "value"}).to_parquet(tmp, engine="pyarrow", compression="snappy")
        tmp.replace(path)
        return path

    def _update_meta_atomic(
        self,
        factor_entries: dict[str, dict[str, Any]],
        *,
        data_start: str,
        data_end: str,
        factor_data_dir: str,
        qlib_bin_path: str | None,
        universe_meta: dict[str, Any],
        base_cache_manifest: dict[str, Any],
    ) -> None:
        meta = self._load_meta()
        meta.update({
            "schema_version": OFFICIAL_CACHE_SCHEMA_VERSION,
            "source_system": OFFICIAL_CACHE_SOURCE_SYSTEM,
            "as_of_date": data_end,
            "data_start": data_start,
            "data_end": data_end,
            "factor_data_dir": factor_data_dir,
            "qlib_bin_path": qlib_bin_path,
            "base_data_cache_policy": "load_once_readonly",
            "data_source_mode": "official_offline_backtest_factor_data",
            "data_freshness_profile": "qe_backtest_coverage",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_train_start": data_start,
            "window_backtest_end": data_end,
            "base_data_manifest": base_cache_manifest,
        })
        meta.update(universe_meta)
        meta.setdefault("factors", {}).update(factor_entries)
        tmp = OFFICIAL_FACTOR_CACHE_META_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        tmp.replace(OFFICIAL_FACTOR_CACHE_META_PATH)

    def _load_meta(self) -> dict[str, Any]:
        if OFFICIAL_FACTOR_CACHE_META_PATH.exists():
            return json.loads(OFFICIAL_FACTOR_CACHE_META_PATH.read_text(encoding="utf-8"))
        return {"factors": {}}

    def _record_error_meta(self, factor_name: str, code_text: str | None, error: str) -> None:
        entry = {
            "status": "error",
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "error": str(error)[:500],
            "code_source": "code_text",
            "source_hash_raw": _code_hash(str(code_text or "")),
            "code_hash": _code_hash(str(code_text or "")),
        }
        if self._active_meta_context:
            self._update_meta_atomic({factor_name: entry}, **self._active_meta_context)
            return
        meta = self._load_meta()
        meta.setdefault("factors", {})[factor_name] = entry
        tmp = OFFICIAL_FACTOR_CACHE_META_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        tmp.replace(OFFICIAL_FACTOR_CACHE_META_PATH)

    def _metric_writer(self):
        from .factor_official_evaluation_service import FactorOfficialEvaluationService

        if not hasattr(self, "_metric_writer_instance"):
            self._metric_writer_instance = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
        return self._metric_writer_instance

    def _build_runtime_validation_report(
        self,
        *,
        cfg: BatchComputeConfig,
        task_id: str,
        requested: list[str],
        eligible_names: list[str],
        skipped: list[str],
        results: list[dict[str, Any]],
        success_count: int,
        fail_count: int,
        db_result: dict[str, Any],
        metrics_error: str | None,
        batch_count: int,
        memory_samples: list[dict[str, Any]],
        resource_failures: list[dict[str, Any]],
        universe_meta: dict[str, Any],
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        """Build a portable evidence block for WSL smoke/batch/full gates."""

        mode = _resolve_validation_mode(cfg.validation_mode, requested, cfg.factor_names)
        expected_factor_count = cfg.expected_factor_count
        factor_failures = [item for item in results if not item.get("success")]
        failure_summary: dict[str, int] = {}
        for item in factor_failures:
            reason = str(item.get("error_type") or item.get("error") or "unknown")
            failure_summary[reason] = failure_summary.get(reason, 0) + 1

        checks = {
            "wsl_runtime_entered": True,
            "official_cache_only": OFFICIAL_FACTOR_CACHE_ROOT.name == "factor_values",
            "code_text_source": True,
            "requested_count_classified": len(requested) == success_count + fail_count + len(skipped),
            "expected_factor_count_met": (
                True
                if expected_factor_count is None
                else len(eligible_names) == int(expected_factor_count)
            ),
            "cache_rows_written_for_successes": success_count > 0,
            "metrics_context_ok": metrics_error is None,
            "metrics_write_ok": not db_result.get("save_failures"),
            "batch_release_observed": any(item.get("event") == "batch_released" for item in memory_samples),
            "timeout_gate_available": cfg.timeout_per_factor > 0,
            "resource_gate_available": True,
            "resource_gate_ok": not resource_failures,
            "single_cache_released": all(
                int(item.get("single_cache_entries", 0) or 0) == 0
                for item in memory_samples
                if item.get("event") == "batch_released"
            ),
            "universe_metadata_present": bool(universe_meta.get("universe_key")) and bool(universe_meta.get("index_policy")),
            "window_declared": bool(start_date and end_date),
            "failures_classified": fail_count == 0 or bool(failure_summary),
        }
        gate_status = "passed" if all(checks.values()) and fail_count == 0 else "failed"

        return {
            "schema_version": "official_factor_runtime_validation_v1",
            "task_id": task_id,
            "mode": mode,
            "gate_status": gate_status,
            "cache_root": str(OFFICIAL_FACTOR_CACHE_ROOT),
            "single_dir": str(OFFICIAL_FACTOR_CACHE_SINGLE_DIR),
            "cache_source": OFFICIAL_CACHE_SOURCE_SYSTEM,
            "code_source": "code_text",
            "data_start": start_date,
            "data_end": end_date,
            "requested_factor_count": len(requested),
            "eligible_factor_count": len(eligible_names),
            "expected_factor_count": expected_factor_count,
            "success_factor_count": success_count,
            "failed_factor_count": fail_count,
            "skipped_factor_count": len(skipped),
            "batch_count": batch_count,
            "timeout_per_factor_sec": cfg.timeout_per_factor,
            "resource_limits": asdict(getattr(self, "_resource_limits", FactorResourceLimits())),
            "resource_failures": resource_failures[-12:],
            "checks": checks,
            "failure_summary": failure_summary,
            "failed_factors": [
                {
                    "name": item.get("name"),
                    "error_type": item.get("error_type") or "unknown",
                    "error": item.get("error"),
                }
                for item in factor_failures
            ],
            "memory_samples": memory_samples[-12:],
            "next_gates": {
                "correlation_full": "run_correlation_compute_wsl_against_same_official_cache",
                "qe_subwindow_cache_hit": "FactorValueLoader.validate_official_cache_window_hit",
            },
        }

    def _assert_official_cache_root(self) -> None:
        if OFFICIAL_FACTOR_CACHE_ROOT.name != "factor_values":
            raise RuntimeError("official factor full compute must use the factor_values cache root")

    def _emit(self, event_type: str, **payload: Any) -> None:
        event = {"type": event_type, "ts": datetime.now(timezone.utc).isoformat(), **payload}
        emitter = getattr(self, "_event_emitter", None)
        if emitter is not None:
            emitter(event)


def _normalize_date(value: str) -> str:
    raw = str(value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw


def _chunks(items: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for idx in range(0, len(items), size):
        yield items[idx: idx + size]


def _resolve_validation_mode(
    requested_mode: str | None,
    requested: list[str],
    factor_names: list[str] | None,
) -> str:
    if requested_mode:
        return requested_mode
    if factor_names is None:
        return "full_enabled"
    if len(requested) <= 2:
        return "smoke_2"
    if len(requested) <= 16:
        return "batch_16"
    return "custom_subset"


def _code_hash(code_text: str) -> str:
    return hashlib.sha256(code_text.encode("utf-8")).hexdigest()[:16]


def _factor_process_worker(
    result_queue: multiprocessing.Queue,
    executor: OfflineCodeTextFactorExecutor,
    factor_name: str,
    code_text: str,
) -> None:
    try:
        result = executor.compute_factor(factor_name, code_text)
    except Exception as exc:
        result = FactorExecutionResult(
            factor_name=factor_name,
            success=False,
            error=f"{type(exc).__name__}: {exc}",
            error_type=type(exc).__name__,
        )
    try:
        result_queue.put((factor_name, result))
    except Exception:
        # The parent process will classify a missing queue result as no-result.
        pass


def _resource_snapshot(extra_pids: Iterable[int] | None = None, *, fast: bool = False) -> ResourceSnapshot:
    pids = [os.getpid(), *[int(pid) for pid in (extra_pids or []) if int(pid) > 0]]
    total_rss = 0.0
    total_uss = 0.0
    total_swap = 0.0
    total_pss = 0.0
    saw_uss = False
    saw_pss = False
    process_count = 0
    for pid in dict.fromkeys(pids):
        snap = _process_memory_snapshot(pid, fast=fast)
        if snap is None:
            continue
        process_count += 1
        total_rss += snap.rss_mb
        total_swap += snap.swap_mb
        if snap.uss_mb is not None:
            saw_uss = True
            total_uss += snap.uss_mb
        if snap.pss_mb is not None:
            saw_pss = True
            total_pss += snap.pss_mb
    available_mb = _available_memory_mb()
    return ResourceSnapshot(
        rss_mb=round(total_pss if saw_pss else total_rss, 2),
        uss_mb=round(total_uss, 2) if saw_uss else None,
        swap_mb=round(total_swap, 2),
        available_mb=available_mb,
        pss_mb=round(total_pss, 2) if saw_pss else None,
        process_count=max(1, process_count),
        rss_raw_mb=round(total_rss, 2),
    )


def _process_memory_snapshot(pid: int, *, fast: bool = False) -> ResourceSnapshot | None:
    try:
        import psutil

        proc = psutil.Process(pid)
        rss_mb = proc.memory_info().rss / 1024 / 1024
        full = None
        if not fast:
            try:
                full = proc.memory_full_info()
            except Exception:
                full = None
        uss_mb = getattr(full, "uss", None)
        swap_mb = getattr(full, "swap", None)
        pss_mb = getattr(full, "pss", None)
        return ResourceSnapshot(
            rss_mb=round(rss_mb, 2),
            uss_mb=round(uss_mb / 1024 / 1024, 2) if uss_mb is not None else None,
            swap_mb=round(swap_mb / 1024 / 1024, 2) if swap_mb is not None else _proc_status_mb(pid, "VmSwap"),
            available_mb=None,
            pss_mb=round(pss_mb / 1024 / 1024, 2) if pss_mb is not None else None,
            process_count=1,
            rss_raw_mb=round(rss_mb, 2),
        )
    except Exception:
        if pid != os.getpid() and not Path(f"/proc/{pid}/status").exists():
            return None
        rss_mb = _proc_status_mb(pid, "VmRSS")
        if rss_mb <= 0 and pid != os.getpid():
            return None
        return ResourceSnapshot(
            rss_mb=rss_mb,
            uss_mb=None,
            swap_mb=_proc_status_mb(pid, "VmSwap"),
            available_mb=None,
            pss_mb=_proc_smaps_rollup_mb(pid, "Pss"),
            process_count=1,
            rss_raw_mb=rss_mb,
        )


def _proc_status_mb(pid: int, field: str) -> float:
    try:
        for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.startswith(f"{field}:"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                return round(float(parts[1]) / 1024, 2)
    except Exception:
        return 0.0
    return 0.0


def _proc_smaps_rollup_mb(pid: int, field: str) -> float | None:
    try:
        for line in Path(f"/proc/{pid}/smaps_rollup").read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.startswith(f"{field}:"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                return round(float(parts[1]) / 1024, 2)
    except Exception:
        return None
    return None


def _available_memory_mb() -> float | None:
    try:
        import psutil

        return round(psutil.virtual_memory().available / 1024 / 1024, 2)
    except Exception:
        pass
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("MemAvailable:"):
                return round(float(line.split()[1]) / 1024, 2)
    except Exception:
        return None
    return None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(str(raw).strip())
    except ValueError:
        return int(default)


def _rss_mb() -> float:
    try:
        import psutil

        return round(psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024, 2)
    except Exception:
        try:
            import resource

            return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2)
        except Exception:
            return 0.0
