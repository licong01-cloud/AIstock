"""Official WSL-only batch factor value + metric compute service."""
from __future__ import annotations

import gc
import hashlib
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
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

        base_cache = BacktestBaseDataMemoryCache.load_once(cfg.factor_data_dir, start_date, end_date)
        self._emit(
            "base_cache_loaded",
            task_id=task_id,
            rss_mb=_rss_mb(),
            base_cache=base_cache.manifest(),
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
            "base_cache_manifest": base_cache.manifest(),
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

        for batch_index, batch in enumerate(batches, start=1):
            batch_id = f"{task_id}_b{batch_index:04d}"
            rss_before_batch = _rss_mb()
            memory_samples.append({
                "event": "batch_started",
                "batch_id": batch_id,
                "batch_index": batch_index,
                "factor_count": len(batch),
                "rss_mb": rss_before_batch,
            })
            self._emit(
                "batch_started",
                task_id=task_id,
                batch_id=batch_id,
                batch_index=batch_index,
                batch_count=len(batches),
                factor_count=len(batch),
                rss_mb=rss_before_batch,
            )
            batch_exec = self._compute_batch_frames(executor, batch, workers=cfg.workers)
            batch_frames: dict[str, pd.DataFrame] = {}
            batch_meta: dict[str, dict[str, Any]] = {}
            written_paths: list[Path] = []
            try:
                for row in batch:
                    name = row["factor_name"]
                    exec_result = batch_exec.get(name)
                    if exec_result is None or not exec_result.success or exec_result.dataframe is None:
                        fail_count += 1
                        err = (exec_result.error if exec_result else "missing_execution_result") or "unknown"
                        results.append({
                            "name": name,
                            "success": False,
                            "error": err,
                            "error_type": exec_result.error_type if exec_result else "missing_execution_result",
                        })
                        self._record_error_meta(name, row.get("code_text"), err)
                        continue
                    df = exec_result.dataframe.reindex(eligible_index)
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
                    written_paths.append(parquet_path)
                    batch_frames[name] = df
                    batch_meta[name] = meta_entry
                    self._emit(
                        "factor_done",
                        task_id=task_id,
                        batch_id=batch_id,
                        factor_name=name,
                        rows=int(len(df)),
                        nan_rate=round(nan_rate, 6),
                        elapsed_sec=exec_result.elapsed_sec,
                    )

                if batch_meta:
                    self._update_meta_atomic(
                        batch_meta,
                        data_start=start_date,
                        data_end=end_date,
                        factor_data_dir=str(Path(cfg.factor_data_dir).expanduser()),
                        qlib_bin_path=str(qlib_bin_path) if qlib_bin_path else None,
                        universe_meta=universe_meta,
                        base_cache_manifest=base_cache.manifest(),
                    )
            except Exception:
                for path in written_paths:
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

            for name, df in batch_frames.items():
                if metrics_error or metrics_ctx is None or compute_single_factor_metrics is None:
                    db_result["errors"].append(f"{name}: {metrics_error or 'metrics context missing'}")
                    db_result["save_failures"].append(name)
                    continue
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

            for name, df in list(batch_frames.items()):
                success_count += 1
                results.append({
                    "name": name,
                    "success": True,
                    "rows": int(len(df)),
                    "nan_rate": round(float(df.iloc[:, 0].isna().mean()) if len(df) else 0.0, 6),
                    "batch_id": batch_id,
                    "error": None,
                })
            batch_frames.clear()
            FactorValueLoader.invalidate_single_cache()
            gc.collect()
            rss_after_release = _rss_mb()
            single_cache_entries = len(getattr(FactorValueLoader, "_single_cache", {}))
            memory_samples.append({
                "event": "batch_released",
                "batch_id": batch_id,
                "batch_index": batch_index,
                "rss_mb": rss_after_release,
                "single_cache_entries": single_cache_entries,
            })
            self._emit(
                "batch_released",
                task_id=task_id,
                batch_id=batch_id,
                rss_mb=rss_after_release,
                single_cache_entries=single_cache_entries,
            )

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

    def _compute_batch_frames(
        self,
        executor: OfflineCodeTextFactorExecutor,
        batch: list[dict[str, Any]],
        *,
        workers: int,
    ) -> dict[str, Any]:
        max_workers = max(1, min(int(workers or 1), len(batch) or 1))
        if max_workers <= 1:
            return executor.compute_batch(batch)

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
                    results[name] = future.result()
                except Exception as exc:  # defensive; compute_factor normally captures errors
                    from .offline_code_text_factor_executor import FactorExecutionResult

                    results[name] = FactorExecutionResult(
                        factor_name=name,
                        success=False,
                        error=f"{type(exc).__name__}: {exc}",
                        error_type=type(exc).__name__,
                    )
        return results

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
            "official_cache_only": "factor_values_realtime" not in str(OFFICIAL_FACTOR_CACHE_ROOT),
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
        if any(part.lower() == "factor_values_realtime" for part in OFFICIAL_FACTOR_CACHE_ROOT.parts):
            raise RuntimeError("official factor full compute must not use factor_values_realtime")

    def _emit(self, event_type: str, **payload: Any) -> None:
        event = {"type": event_type, "ts": datetime.now(timezone.utc).isoformat(), **payload}
        if self._event_emitter is not None:
            self._event_emitter(event)


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
