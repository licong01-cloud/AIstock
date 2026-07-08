from __future__ import annotations

import io
import json
import math
import pickle
import re
from dataclasses import dataclass, field
from typing import Any, Mapping


class ResultsOnlyGateError(ValueError):
    """Fail-closed artifact gate error for QE results-only retry."""

    def __init__(
        self,
        *,
        reason_code: str,
        artifact: str,
        task_id: str,
        loop_id: str,
        node_id: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self.reason_code = reason_code
        self.artifact = artifact
        self.task_id = task_id
        self.loop_id = loop_id
        self.node_id = node_id
        self.details = dict(details or {})
        message = (
            "QE_RESULTS_ONLY_GATE_FAILED "
            f"reason_code={reason_code} artifact={artifact} "
            f"task={task_id} loop={loop_id} node={node_id or '<unknown>'} "
            "hint=retry with backtest_only or full_train after repairing the missing artifact"
        )
        if self.details:
            message += " details=" + json.dumps(self.details, ensure_ascii=False, default=str)
        super().__init__(message)


class ResultsOnlyRegistrationError(RuntimeError):
    """Loud registration/upload failure after the results-only gate passed."""

    def __init__(
        self,
        *,
        reason_code: str,
        task_id: str,
        loop_id: str,
        node_id: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self.reason_code = reason_code
        self.task_id = task_id
        self.loop_id = loop_id
        self.node_id = node_id
        self.details = dict(details or {})
        message = (
            "QE_RESULTS_ONLY_REGISTRATION_FAILED "
            f"reason_code={reason_code} task={task_id} loop={loop_id} "
            f"node={node_id or '<unknown>'}"
        )
        if self.details:
            message += " details=" + json.dumps(self.details, ensure_ascii=False, default=str)
        super().__init__(message)


@dataclass
class PredictionGateResult:
    score_series: Any = field(repr=False)
    row_count: int = 0
    date_count: int = 0
    instrument_count: int = 0
    finite_score_count: int = 0
    score_column: str = "score"

    def stats(self) -> dict[str, Any]:
        return {
            "row_count": self.row_count,
            "date_count": self.date_count,
            "instrument_count": self.instrument_count,
            "finite_score_count": self.finite_score_count,
            "score_column": self.score_column,
        }


@dataclass
class ResultsOnlyArtifacts:
    recorder_ref: dict[str, Any]
    artifact_prefix: str
    metrics: dict[str, Any]
    enhanced_metrics: dict[str, Any]
    pred_bytes: bytes
    label_bytes: bytes | None
    params_bytes: bytes | None
    prediction: PredictionGateResult
    report_stats: dict[str, Any]
    metrics_source: str
    prediction_store_manifest: dict[str, Any] | None = None

    def attach_prediction_store_manifest(self, manifest: dict[str, Any]) -> None:
        self.prediction_store_manifest = manifest
        self.enhanced_metrics["prediction_store_manifest"] = manifest
        self.enhanced_metrics["prediction_store_upload"] = {
            "schema_version": "qe_prediction_store_upload_v1",
            "status": "success",
            "mode": "results_only",
            "run_key": manifest.get("run_key"),
        }
        self.metrics["prediction_store_manifest"] = manifest


async def collect_results_only_artifacts(
    *,
    client: Any,
    task_id: str,
    loop_id: str,
    node_id: str | None = None,
) -> ResultsOnlyArtifacts:
    """Validate existing QE loop artifacts without launching qrun/backtest."""

    recorder_ref = await _load_recorder_ref(client, task_id=task_id, loop_id=loop_id, node_id=node_id)
    try:
        recorder_id = _safe_ref_component(recorder_ref.get("recorder_id"), field_name="recorder_id")
        experiment_id = _safe_ref_component(recorder_ref.get("experiment_id"), field_name="experiment_id")
    except ValueError as exc:
        raise ResultsOnlyGateError(
            reason_code="recorder_ref_invalid",
            artifact="qe_current_recorder.json",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"error": str(exc)},
        ) from exc
    artifact_prefix = f"mlruns/{experiment_id}/{recorder_id}/artifacts"

    pred_path = f"{artifact_prefix}/pred.pkl"
    pred_bytes = await _download_required_bytes(
        client,
        task_id=task_id,
        loop_id=loop_id,
        node_id=node_id,
        file_path=pred_path,
        reason_code="pred_missing",
        empty_reason_code="pred_empty",
        artifact="pred.pkl",
    )
    pred_obj = _load_pickle_for_gate(
        pred_bytes,
        artifact="pred.pkl",
        reason_code="pred_unreadable",
        task_id=task_id,
        loop_id=loop_id,
        node_id=node_id,
    )
    prediction = _validate_prediction_object(pred_obj, task_id=task_id, loop_id=loop_id, node_id=node_id)

    report_path = f"{artifact_prefix}/portfolio_analysis/report_normal_1day.pkl"
    report_bytes = await _download_required_bytes(
        client,
        task_id=task_id,
        loop_id=loop_id,
        node_id=node_id,
        file_path=report_path,
        reason_code="portfolio_report_missing",
        empty_reason_code="portfolio_report_empty",
        artifact="portfolio_analysis/report_normal_1day.pkl",
    )
    report_obj = _load_pickle_for_gate(
        report_bytes,
        artifact="portfolio_analysis/report_normal_1day.pkl",
        reason_code="portfolio_report_unreadable",
        task_id=task_id,
        loop_id=loop_id,
        node_id=node_id,
    )
    report_stats = _validate_portfolio_report(report_obj, task_id=task_id, loop_id=loop_id, node_id=node_id)

    label_bytes, label_errors = await _download_first_optional_bytes(
        client,
        task_id=task_id,
        loop_id=loop_id,
        file_paths=[f"{artifact_prefix}/label.pkl", f"{artifact_prefix}/sig_analysis/label.pkl"],
    )
    params_bytes, params_errors = await _download_first_optional_bytes(
        client,
        task_id=task_id,
        loop_id=loop_id,
        file_paths=[f"{artifact_prefix}/params.pkl", f"{artifact_prefix}/params_pkl"],
    )

    metrics, metrics_error = await _read_existing_metrics(client, task_id=task_id, loop_id=loop_id)
    metrics_source = "existing"
    if metrics is None:
        metrics = {}
    if not _has_valid_ic_metrics(metrics):
        if not label_bytes:
            raise ResultsOnlyGateError(
                reason_code="metrics_missing_and_label_missing",
                artifact="qlib_res.csv|label.pkl",
                task_id=task_id,
                loop_id=loop_id,
                node_id=node_id,
                details={
                    "metrics_error": metrics_error or "IC/RankIC missing from existing metrics",
                    "label_attempts": label_errors,
                },
            )
        label_obj = _load_pickle_for_gate(
            label_bytes,
            artifact="label.pkl",
            reason_code="label_unreadable",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
        )
        metrics.update(
            _compute_ic_rank_ic(
                prediction.score_series,
                label_obj,
                task_id=task_id,
                loop_id=loop_id,
                node_id=node_id,
            )
        )
        metrics_source = "recomputed_from_pred_label"

    _merge_portfolio_report_metrics(metrics, report_obj)
    _normalize_metric_aliases(metrics)

    enhanced_metrics, enhanced_error = await _read_enhanced_metrics(client, task_id=task_id, loop_id=loop_id)
    if enhanced_metrics is None:
        enhanced_metrics = {
            "schema_version": "qe_results_only_enhanced_v1",
            "results_only_recovered": True,
            "enhanced_metrics_source": "reconstructed_minimal",
            "enhanced_metrics_error": enhanced_error,
        }
    enhanced_metrics["results_only_gate"] = {
        "prediction": prediction.stats(),
        "portfolio_report": report_stats,
        "metrics_source": metrics_source,
        "artifact_prefix": artifact_prefix,
        "optional_artifact_attempts": {
            "label": label_errors,
            "params": params_errors,
        },
    }
    metrics["enhanced_metrics"] = enhanced_metrics

    return ResultsOnlyArtifacts(
        recorder_ref=recorder_ref,
        artifact_prefix=artifact_prefix,
        metrics=metrics,
        enhanced_metrics=enhanced_metrics,
        pred_bytes=pred_bytes,
        label_bytes=label_bytes,
        params_bytes=params_bytes,
        prediction=prediction,
        report_stats=report_stats,
        metrics_source=metrics_source,
    )


def upload_results_only_prediction_store(
    *,
    artifacts: ResultsOnlyArtifacts,
    task_id: str,
    loop_id: str,
    loop_index: int,
    node_id: str | None = None,
    store: Any | None = None,
) -> dict[str, Any]:
    """Upload validated existing artifacts into the idempotent prediction store."""

    from backend.services.model_store import PredictionArtifactStore

    files: dict[str, tuple[str, io.BytesIO]] = {
        "prediction": ("pred.pkl", io.BytesIO(artifacts.pred_bytes)),
    }
    if artifacts.params_bytes:
        files["model_params"] = ("params.pkl", io.BytesIO(artifacts.params_bytes))
    if artifacts.label_bytes:
        files["label"] = ("label.pkl", io.BytesIO(artifacts.label_bytes))

    recorder_ref = artifacts.recorder_ref
    metadata = {
        "producer": "qe_results_only_retry",
        "mode": "results_only",
        "task_id": task_id,
        "loop_id": loop_id,
        "loop_index": loop_index,
        "source_node_id": node_id,
        "recorder_id": recorder_ref.get("recorder_id"),
        "recorder_experiment_id": recorder_ref.get("experiment_id"),
        "artifact_prefix": artifacts.artifact_prefix,
        "prediction_gate": artifacts.prediction.stats(),
        "metrics_source": artifacts.metrics_source,
    }
    run_key = f"{task_id}_L{loop_index}"
    try:
        manifest = (store or PredictionArtifactStore()).write_artifacts(
            run_key=run_key,
            files=files,
            metadata={k: v for k, v in metadata.items() if v not in (None, "", [], {})},
        )
    except Exception as exc:
        raise ResultsOnlyRegistrationError(
            reason_code="prediction_store_upload_failed",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"error": f"{type(exc).__name__}: {exc}", "run_key": run_key},
        ) from exc

    artifacts.attach_prediction_store_manifest(manifest)
    return manifest


async def _load_recorder_ref(
    client: Any,
    *,
    task_id: str,
    loop_id: str,
    node_id: str | None,
) -> dict[str, Any]:
    errors: dict[str, str] = {}
    for file_path in ("qe_current_recorder.json", "qe_extracted_recorder.json"):
        try:
            payload = await client.get_workspace_file(task_id, loop_id, file_path)
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not isinstance(payload, dict):
                errors[file_path] = f"expected JSON object, got {type(payload).__name__}"
                continue
            normalized = _normalize_recorder_ref(payload)
            if normalized.get("recorder_id") and normalized.get("experiment_id"):
                return normalized
            errors[file_path] = "missing recorder_id or experiment_id"
        except Exception as exc:
            errors[file_path] = f"{type(exc).__name__}: {exc}"
    raise ResultsOnlyGateError(
        reason_code="recorder_ref_missing",
        artifact="qe_current_recorder.json",
        task_id=task_id,
        loop_id=loop_id,
        node_id=node_id,
        details={"attempts": errors},
    )


def _normalize_recorder_ref(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    normalized["recorder_id"] = str(
        payload.get("recorder_id") or payload.get("selected_recorder_id") or ""
    ).strip()
    normalized["experiment_id"] = str(
        payload.get("experiment_id") or payload.get("selected_experiment_id") or ""
    ).strip()
    normalized["experiment_name"] = str(
        payload.get("experiment_name") or payload.get("selected_experiment_name") or ""
    ).strip()
    return normalized


def _safe_ref_component(value: Any, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text or "/" in text or "\\" in text or ".." in text.split("/"):
        raise ValueError(f"invalid recorder ref {field_name}: {value!r}")
    return text


async def _download_required_bytes(
    client: Any,
    *,
    task_id: str,
    loop_id: str,
    node_id: str | None,
    file_path: str,
    reason_code: str,
    empty_reason_code: str,
    artifact: str,
) -> bytes:
    try:
        content = await client.download_workspace_file_bytes(task_id, loop_id, file_path)
    except Exception as exc:
        raise ResultsOnlyGateError(
            reason_code=reason_code,
            artifact=artifact,
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"path": file_path, "error": f"{type(exc).__name__}: {exc}"},
        ) from exc
    if not content:
        raise ResultsOnlyGateError(
            reason_code=empty_reason_code,
            artifact=artifact,
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"path": file_path},
        )
    return content


async def _download_first_optional_bytes(
    client: Any,
    *,
    task_id: str,
    loop_id: str,
    file_paths: list[str],
) -> tuple[bytes | None, dict[str, str]]:
    errors: dict[str, str] = {}
    for file_path in file_paths:
        try:
            content = await client.download_workspace_file_bytes(task_id, loop_id, file_path)
        except Exception as exc:
            errors[file_path] = f"{type(exc).__name__}: {exc}"
            continue
        if content:
            return content, errors
        errors[file_path] = "empty file"
    return None, errors


def _load_pickle_for_gate(
    payload: bytes,
    *,
    artifact: str,
    reason_code: str,
    task_id: str,
    loop_id: str,
    node_id: str | None,
) -> Any:
    try:
        return pickle.loads(payload)
    except Exception as exc:
        raise ResultsOnlyGateError(
            reason_code=reason_code,
            artifact=artifact,
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"error": f"{type(exc).__name__}: {exc}"},
        ) from exc


def _validate_prediction_object(
    pred_obj: Any,
    *,
    task_id: str,
    loop_id: str,
    node_id: str | None,
) -> PredictionGateResult:
    import numpy as np
    import pandas as pd

    if isinstance(pred_obj, pd.Series):
        score = pred_obj
        score_column = pred_obj.name or "score"
    elif isinstance(pred_obj, pd.DataFrame):
        frame = pred_obj.copy()
        if not isinstance(frame.index, pd.MultiIndex):
            lower_columns = {str(col).lower(): col for col in frame.columns}
            dt_col = lower_columns.get("datetime") or lower_columns.get("date")
            inst_col = lower_columns.get("instrument") or lower_columns.get("symbol")
            if dt_col is not None and inst_col is not None:
                frame[dt_col] = pd.to_datetime(frame[dt_col], errors="coerce")
                frame = frame.set_index([dt_col, inst_col])
        score_column = _select_score_column(frame, task_id=task_id, loop_id=loop_id, node_id=node_id)
        score = frame[score_column]
    else:
        raise ResultsOnlyGateError(
            reason_code="pred_invalid_type",
            artifact="pred.pkl",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"type": type(pred_obj).__name__},
        )

    if score.empty:
        raise ResultsOnlyGateError(
            reason_code="pred_empty",
            artifact="pred.pkl",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
        )
    if not isinstance(score.index, pd.MultiIndex) or score.index.nlevels < 2:
        raise ResultsOnlyGateError(
            reason_code="pred_bad_index",
            artifact="pred.pkl",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"index_type": type(score.index).__name__},
        )

    numeric = pd.to_numeric(score, errors="coerce").replace([np.inf, -np.inf], np.nan)
    finite_score_count = int(numeric.notna().sum())
    if finite_score_count == 0:
        raise ResultsOnlyGateError(
            reason_code="pred_all_nan",
            artifact="pred.pkl",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"row_count": int(len(numeric))},
        )

    date_values = score.index.get_level_values(0)
    instrument_values = score.index.get_level_values(1)
    date_count = int(len(set(date_values)))
    instrument_count = int(len(set(instrument_values)))
    if date_count < 1 or instrument_count < 1:
        raise ResultsOnlyGateError(
            reason_code="pred_bad_coverage",
            artifact="pred.pkl",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"date_count": date_count, "instrument_count": instrument_count},
        )

    return PredictionGateResult(
        score_series=numeric,
        row_count=int(len(numeric)),
        date_count=date_count,
        instrument_count=instrument_count,
        finite_score_count=finite_score_count,
        score_column=str(score_column),
    )


def _select_score_column(frame: Any, *, task_id: str, loop_id: str, node_id: str | None) -> Any:
    import pandas as pd

    if "score" in frame.columns:
        return "score"
    numeric_columns = [
        col for col in frame.columns
        if pd.api.types.is_numeric_dtype(frame[col])
        and str(col).lower() not in {"datetime", "date", "instrument", "symbol"}
    ]
    if numeric_columns:
        return numeric_columns[0]
    raise ResultsOnlyGateError(
        reason_code="pred_missing_score",
        artifact="pred.pkl",
        task_id=task_id,
        loop_id=loop_id,
        node_id=node_id,
        details={"columns": [str(col) for col in frame.columns]},
    )


def _validate_portfolio_report(
    report_obj: Any,
    *,
    task_id: str,
    loop_id: str,
    node_id: str | None,
) -> dict[str, Any]:
    import pandas as pd

    if isinstance(report_obj, (pd.DataFrame, pd.Series)):
        row_count = int(len(report_obj))
        if row_count <= 0:
            raise ResultsOnlyGateError(
                reason_code="portfolio_report_empty",
                artifact="portfolio_analysis/report_normal_1day.pkl",
                task_id=task_id,
                loop_id=loop_id,
                node_id=node_id,
            )
        return {"type": type(report_obj).__name__, "row_count": row_count}
    if isinstance(report_obj, dict) and report_obj:
        return {"type": "dict", "row_count": len(report_obj)}
    raise ResultsOnlyGateError(
        reason_code="portfolio_report_invalid",
        artifact="portfolio_analysis/report_normal_1day.pkl",
        task_id=task_id,
        loop_id=loop_id,
        node_id=node_id,
        details={"type": type(report_obj).__name__},
    )


async def _read_existing_metrics(client: Any, *, task_id: str, loop_id: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        payload = await client.get_loop_metrics(task_id, loop_id)
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(payload, dict) or not payload:
        return None, f"metrics payload is empty or invalid: {payload!r}"
    return dict(payload), None


async def _read_enhanced_metrics(client: Any, *, task_id: str, loop_id: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        payload = await client.get_enhanced_metrics(task_id, loop_id)
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(payload, dict) or not payload:
        return None, f"enhanced metrics payload is empty or invalid: {payload!r}"
    return dict(payload), None


def _has_valid_ic_metrics(metrics: Mapping[str, Any]) -> bool:
    return _finite_metric(metrics, ("IC", "ic")) is not None and _finite_metric(
        metrics, ("Rank IC", "Rank_IC", "rank_ic")
    ) is not None


def _finite_metric(metrics: Mapping[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        if key not in metrics:
            continue
        try:
            value = float(metrics[key])
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            return value
    return None


def _compute_ic_rank_ic(
    pred_score: Any,
    label_obj: Any,
    *,
    task_id: str,
    loop_id: str,
    node_id: str | None,
) -> dict[str, Any]:
    import numpy as np
    import pandas as pd

    label = _label_to_series(label_obj, task_id=task_id, loop_id=loop_id, node_id=node_id)
    if not isinstance(pred_score.index, pd.MultiIndex) or not isinstance(label.index, pd.MultiIndex):
        raise ResultsOnlyGateError(
            reason_code="metric_recompute_bad_index",
            artifact="pred.pkl|label.pkl",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
        )
    common = pred_score.dropna().index.intersection(label.dropna().index)
    if len(common) < 2:
        raise ResultsOnlyGateError(
            reason_code="metric_recompute_no_overlap",
            artifact="pred.pkl|label.pkl",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"overlap_rows": int(len(common))},
        )

    pred = pred_score.loc[common].astype(float)
    actual = pd.to_numeric(label.loc[common], errors="coerce")
    frame = pd.DataFrame({"pred": pred, "label": actual}).dropna()
    ics: list[float] = []
    rank_ics: list[float] = []
    for dt, group in frame.groupby(level=0, sort=True):
        if len(group) < 2 or group["pred"].nunique() < 2 or group["label"].nunique() < 2:
            continue
        ic = group["pred"].corr(group["label"], method="pearson")
        rank_ic = group["pred"].corr(group["label"], method="spearman")
        if pd.notna(ic) and np.isfinite(ic):
            ics.append(float(ic))
        if pd.notna(rank_ic) and np.isfinite(rank_ic):
            rank_ics.append(float(rank_ic))
    if not ics or not rank_ics:
        raise ResultsOnlyGateError(
            reason_code="metric_recompute_empty",
            artifact="pred.pkl|label.pkl",
            task_id=task_id,
            loop_id=loop_id,
            node_id=node_id,
            details={"overlap_rows": int(len(frame))},
        )

    ic_mean = float(np.mean(ics))
    rank_ic_mean = float(np.mean(rank_ics))
    metrics: dict[str, Any] = {
        "IC": ic_mean,
        "Rank IC": rank_ic_mean,
        "Rank_IC": rank_ic_mean,
        "ic_days": len(ics),
        "rank_ic_days": len(rank_ics),
    }
    if len(ics) > 1 and float(np.std(ics)) > 1e-12:
        metrics["ICIR"] = float(np.mean(ics) / np.std(ics))
    if len(rank_ics) > 1 and float(np.std(rank_ics)) > 1e-12:
        metrics["Rank ICIR"] = float(np.mean(rank_ics) / np.std(rank_ics))
    return metrics


def _label_to_series(
    label_obj: Any,
    *,
    task_id: str,
    loop_id: str,
    node_id: str | None,
) -> Any:
    import pandas as pd

    if isinstance(label_obj, pd.Series):
        return pd.to_numeric(label_obj, errors="coerce")
    if isinstance(label_obj, pd.DataFrame):
        if "label" in label_obj.columns:
            return pd.to_numeric(label_obj["label"], errors="coerce")
        numeric_cols = [col for col in label_obj.columns if pd.api.types.is_numeric_dtype(label_obj[col])]
        if numeric_cols:
            return pd.to_numeric(label_obj[numeric_cols[0]], errors="coerce")
        if len(label_obj.columns) == 1:
            return pd.to_numeric(label_obj.iloc[:, 0], errors="coerce")
    raise ResultsOnlyGateError(
        reason_code="label_invalid_type",
        artifact="label.pkl",
        task_id=task_id,
        loop_id=loop_id,
        node_id=node_id,
        details={"type": type(label_obj).__name__},
    )


def _merge_portfolio_report_metrics(metrics: dict[str, Any], report_obj: Any) -> None:
    import numpy as np
    import pandas as pd

    if isinstance(report_obj, pd.Series):
        report = report_obj.to_frame()
    elif isinstance(report_obj, pd.DataFrame):
        report = report_obj
    else:
        return
    col_lower = {str(col).lower(): col for col in report.columns}
    if "return" in col_lower:
        ret = pd.to_numeric(report[col_lower["return"]], errors="coerce").fillna(0.0)
        metrics.setdefault("n_trading_days", int(len(ret)))
        _merge_series_metrics(metrics, ret, prefix="1day.return")
        bench = pd.to_numeric(report[col_lower["bench"]], errors="coerce").fillna(0.0) if "bench" in col_lower else 0.0
        excess = ret - bench
        _merge_series_metrics(metrics, excess, prefix="1day.excess_return_without_cost")
        cost_col = next(
            (
                source_col for lower, source_col in col_lower.items()
                if "cost" in lower and "excess" not in lower and "return" not in lower
            ),
            None,
        )
        if cost_col is not None:
            excess_with_cost = excess - pd.to_numeric(report[cost_col], errors="coerce").fillna(0.0).abs()
        else:
            excess_with_cost = excess
        _merge_series_metrics(metrics, excess_with_cost, prefix="1day.excess_return_with_cost")
        cumulative = (1.0 + ret).cumprod()
        if not cumulative.empty and np.isfinite(cumulative.iloc[-1]):
            metrics.setdefault("final_nav", float(cumulative.iloc[-1]))


def _merge_series_metrics(metrics: dict[str, Any], series: Any, *, prefix: str) -> None:
    clean = series.fillna(0.0)
    if clean.empty:
        return
    cumulative = (1.0 + clean).cumprod()
    final_nav = float(cumulative.iloc[-1])
    trading_days = max(int(len(clean)), 1)
    annualized = final_nav ** (252.0 / trading_days) - 1.0 if final_nav > 0 else -1.0
    std = float(clean.std())
    mean = float(clean.mean())
    information_ratio = float(mean / std * (252.0 ** 0.5)) if std > 0 else 0.0
    drawdown = cumulative / cumulative.cummax() - 1.0
    values = {
        f"{prefix}.annualized_return": float(annualized),
        f"{prefix}.information_ratio": information_ratio,
        f"{prefix}.max_drawdown": float(drawdown.min()),
        f"{prefix}.mean": mean,
        f"{prefix}.std": std,
    }
    for key, value in values.items():
        if key not in metrics and math.isfinite(value):
            metrics[key] = value


def _normalize_metric_aliases(metrics: dict[str, Any]) -> None:
    aliases = {
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
    for src, dst in aliases.items():
        if src in metrics and dst not in metrics:
            metrics[dst] = metrics[src]


def parse_results_only_loop_index(loop_id: str) -> int:
    match = re.search(r"Loop(\d+)$", str(loop_id))
    if not match:
        raise ValueError(f"invalid QE loop id for results_only retry: {loop_id!r}")
    return int(match.group(1))
