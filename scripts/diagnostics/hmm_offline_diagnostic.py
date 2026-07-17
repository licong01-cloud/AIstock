"""Offline HMM overlay diagnostics for QE loops.

The script reads QE artifacts only through QEWorkspaceClient-compatible HTTP
endpoints. It does not touch QE strategy code or remote workspace paths.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import pickle
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterator, Sequence

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.services.hmm_data_source import BacktestDataSource  # noqa: E402
from backend.services.hmm_evolution.evaluator import (  # noqa: E402
    CandidateCoefficients,
    evaluate_candidate,
    resolve_batch_common_dates,
)
from backend.services.hmm_evolution.errors import MarketDataUnavailableError  # noqa: E402
from backend.services.hmm_evolution.market_repository import (  # noqa: E402
    HMMMarketReturnRepository,
)
from backend.services.quantevolver.qe_workspace_client import (  # noqa: E402
    QEWorkspaceClient,
    QEWorkspaceFileNotFound,
)


@dataclass
class LoopInfo:
    loop_index: int
    loop_id: str
    status: str
    experiment_id: str | None
    node_id: str | None
    label: str | None
    has_hmm: bool
    snapshot_id: str | None
    model_path: str | None
    annualized_return: float | None
    max_drawdown: float | None
    sharpe: float | None
    ic: float | None
    rank_ic: float | None


@dataclass
class RecorderRef:
    experiment_id: str
    recorder_id: str


@contextmanager
def read_only_cursor() -> Iterator[Any]:
    """Use the canonical pool with an explicit read-only transaction."""

    with get_conn(autocommit=False, manage_transaction=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            yield cursor


def pct(x: Any, digits: int = 2) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "NA"
    return f"{float(x) * 100:.{digits}f}%"


def num(x: Any, digits: int = 4) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "NA"
    return f"{float(x):.{digits}f}"


def safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        converted = float(value)
        if not math.isfinite(converted):
            return None
        return converted
    except (TypeError, ValueError, OverflowError):
        return None


def first_finite(*values: Any) -> float | None:
    for value in values:
        converted = safe_float(value)
        if converted is not None:
            return converted
    return None


def extract_metrics(result_metrics: dict[str, Any] | None, metric_json: dict[str, Any] | None) -> tuple[float | None, ...]:
    rm = result_metrics or {}
    mj = metric_json or {}
    ann = first_finite(rm.get("annualized_return"), mj.get("annualized_return"), mj.get("annual_return"))
    dd = first_finite(rm.get("max_drawdown"), mj.get("max_drawdown"))
    sharpe = first_finite(rm.get("sharpe"), mj.get("sharpe"), mj.get("sharpe_ratio"))
    ic = first_finite(rm.get("IC"), rm.get("ic"), mj.get("IC"), mj.get("ic"))
    rank_ic = first_finite(
        rm.get("Rank IC"),
        rm.get("Rank_IC"),
        rm.get("rank_ic"),
        mj.get("rank_ic"),
    )
    return ann, dd, sharpe, ic, rank_ic


def load_loops(task_id: str) -> tuple[list[LoopInfo], str, str]:
    with read_only_cursor() as cur:
        cur.execute(
            """
            SELECT t.node_id, n.api_base_url
            FROM qe_evolution_tasks t
            LEFT JOIN infra.compute_nodes n ON n.node_id = t.node_id
            WHERE t.task_id = %s
            """,
            (task_id,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"QE task not found: {task_id}")
        task_node_id, api_base_url = row
        if not api_base_url:
            raise RuntimeError(f"QE task has no compute-node api_base_url: {task_id}")

        cur.execute(
            """
            SELECT l.loop_index, l.loop_id, l.status, l.experiment_id, l.node_id,
                   l.config_json, l.metrics_json,
                   e.custom_params, e.result_metrics,
                   e.annualized_return, e.max_drawdown, e.ic, e.rank_ic
            FROM qe_evolution_loops l
            LEFT JOIN qe_experiments e ON e.experiment_id = l.experiment_id
            WHERE l.task_id = %s
            ORDER BY l.loop_index
            """,
            (task_id,),
        )
        loops: list[LoopInfo] = []
        for row in cur.fetchall():
            (
                loop_index,
                loop_id,
                status,
                experiment_id,
                node_id,
                cfg,
                metrics_json,
                custom_params,
                result_metrics,
                e_ann,
                e_dd,
                e_ic,
                e_rank_ic,
            ) = row
            cfg = cfg or {}
            cp = custom_params or cfg.get("custom_params") or cfg.get("strategy_params") or {}
            ann, dd, sharpe, ic, rank_ic = extract_metrics(result_metrics, metrics_json)
            if e_ann is not None:
                ann = safe_float(e_ann)
            if e_dd is not None:
                dd = safe_float(e_dd)
            if e_ic is not None:
                ic = safe_float(e_ic)
            if e_rank_ic is not None:
                rank_ic = safe_float(e_rank_ic)
            loops.append(
                LoopInfo(
                    loop_index=int(loop_index),
                    loop_id=str(loop_id),
                    status=str(status),
                    experiment_id=experiment_id,
                    node_id=node_id or task_node_id,
                    label=cfg.get("label") or cp.get("label"),
                    has_hmm=bool(cp.get("enable_sector_hmm")),
                    snapshot_id=cp.get("hmm_model_version_id"),
                    model_path=cp.get("sector_hmm_model_path"),
                    annualized_return=ann,
                    max_drawdown=dd,
                    sharpe=sharpe,
                    ic=ic,
                    rank_ic=rank_ic,
                )
            )
    return loops, str(task_node_id), str(api_base_url).rstrip("/")


def enhanced_metrics_by_experiment(experiment_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not experiment_ids:
        return {}
    with read_only_cursor() as cur:
        cur.execute(
            "SELECT experiment_id, result_metrics FROM qe_experiments WHERE experiment_id = ANY(%s)",
            (experiment_ids,),
        )
        out = {}
        for exp_id, rm in cur.fetchall():
            if not isinstance(rm, dict):
                continue
            out[exp_id] = rm.get("enhanced_metrics") if isinstance(rm.get("enhanced_metrics"), dict) else rm
        return out


def parse_recorder_ref(run_log: str) -> RecorderRef:
    latest = re.search(
        r"Latest recorder: .*?'id': '([0-9a-f]+)'.*?'experiment_id': '(\d+)'",
        run_log,
        flags=re.S,
    )
    if latest:
        return RecorderRef(experiment_id=latest.group(2), recorder_id=latest.group(1))
    start = re.search(r"Recorder ([0-9a-f]+) starts running under Experiment (\d+)", run_log)
    if start:
        return RecorderRef(experiment_id=start.group(2), recorder_id=start.group(1))
    raise RuntimeError("Cannot locate mlflow recorder id in run.log")


def loads_pickle(data: bytes) -> Any:
    return pickle.loads(data)


def decode_utf8(payload: bytes, *, artifact_name: str) -> str:
    try:
        return payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"QE artifact is not valid UTF-8: {artifact_name}") from exc


def loads_json_artifact(payload: bytes, *, artifact_name: str) -> Any:
    try:
        return json.loads(decode_utf8(payload, artifact_name=artifact_name))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"QE artifact is not valid JSON: {artifact_name}") from exc


async def download_loop_artifacts(
    client: QEWorkspaceClient,
    task_id: str,
    loop: LoopInfo,
    out_dir: Path,
) -> dict[str, Any]:
    loop_dir = out_dir / f"L{loop.loop_index}_{loop.loop_id}"
    loop_dir.mkdir(parents=True, exist_ok=True)
    warnings: list[dict[str, str]] = []
    artifacts: dict[str, Any] = {"warnings": warnings}
    run_log_bytes = await client.download_workspace_file_bytes(
        task_id,
        loop.loop_id,
        "run.log",
    )
    run_log = decode_utf8(run_log_bytes, artifact_name="run.log")
    (loop_dir / "run.log").write_text(run_log, encoding="utf-8")
    rec = parse_recorder_ref(run_log)
    artifacts["recorder"] = rec

    try:
        enhanced_bytes = await client.download_workspace_file_bytes(
            task_id,
            loop.loop_id,
            "qlib_results_enhanced.json",
        )
    except QEWorkspaceFileNotFound as exc:
        warnings.append(
            {
                "artifact": "qlib_results_enhanced.json",
                "reason_code": "qe_workspace_file_not_found",
                "error": str(exc),
            }
        )
    else:
        enhanced = loads_json_artifact(
            enhanced_bytes,
            artifact_name="qlib_results_enhanced.json",
        )
        (loop_dir / "qlib_results_enhanced.json").write_text(
            json.dumps(enhanced, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        artifacts["enhanced"] = enhanced

    art_prefix = f"mlruns/{rec.experiment_id}/{rec.recorder_id}/artifacts"
    report_artifact = f"{art_prefix}/portfolio_analysis/report_normal_1day.pkl"
    try:
        report_bytes = await client.download_workspace_file_bytes(
            task_id,
            loop.loop_id,
            report_artifact,
        )
    except QEWorkspaceFileNotFound as exc:
        warnings.append(
            {
                "artifact": report_artifact,
                "reason_code": "qe_workspace_file_not_found",
                "error": str(exc),
            }
        )
    else:
        (loop_dir / "report_normal_1day.pkl").write_bytes(report_bytes)
        artifacts["report"] = loads_pickle(report_bytes)

    if loop.has_hmm:
        hmm_artifact = "hmm_sector_coefficients.json"
        hmm_bytes = await client.download_workspace_file_bytes(
            task_id,
            loop.loop_id,
            hmm_artifact,
        )
        hmm = loads_json_artifact(hmm_bytes, artifact_name=hmm_artifact)
        if not isinstance(hmm, dict):
            raise RuntimeError("hmm_sector_coefficients.json must contain a JSON object")
        (loop_dir / "hmm_sector_coefficients.json").write_text(
            json.dumps(hmm, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        artifacts["hmm"] = hmm
    return artifacts


def pred_to_series(pred: pd.DataFrame | pd.Series) -> pd.Series:
    if isinstance(pred, pd.DataFrame):
        if "score" in pred.columns:
            col = "score"
        elif len(pred.columns) == 1:
            col = pred.columns[0]
        else:
            raise RuntimeError("pred.pkl must expose a score column or exactly one value column")
        ser = pred[col]
    else:
        ser = pred
    if not isinstance(ser.index, pd.MultiIndex):
        raise RuntimeError("pred.pkl index must be MultiIndex(datetime, instrument)")
    result = ser.copy()
    result.index = result.index.set_levels(pd.to_datetime(result.index.levels[0]), level=0)
    return result.sort_index()


def label_to_series(label: pd.DataFrame | pd.Series) -> pd.Series:
    if isinstance(label, pd.DataFrame):
        if "LABEL0" in label.columns:
            col = "LABEL0"
        elif len(label.columns) == 1:
            col = label.columns[0]
        else:
            raise RuntimeError("label.pkl must expose LABEL0 or exactly one value column")
        ser = label[col]
    else:
        ser = label
    if not isinstance(ser.index, pd.MultiIndex):
        raise RuntimeError("label.pkl index must be MultiIndex(datetime, instrument)")
    result = ser.copy()
    result.index = result.index.set_levels(pd.to_datetime(result.index.levels[0]), level=0)
    return result.sort_index()


def _prediction_frame(pred: pd.DataFrame | pd.Series) -> pd.DataFrame:
    if isinstance(pred, pd.DataFrame) and {"trade_date", "symbol", "score"} <= set(pred.columns):
        return pred.loc[:, ["trade_date", "symbol", "score"]].copy()
    series = pred_to_series(pred)
    return pd.DataFrame(
        {
            "trade_date": pd.to_datetime(series.index.get_level_values(0)).date,
            "symbol": series.index.get_level_values(1).astype(str),
            "score": series.to_numpy(),
        }
    )


def _label_frame(
    label: pd.DataFrame | pd.Series,
    *,
    label_horizon_days: int,
) -> pd.DataFrame:
    required = {"trade_date", "symbol", "horizon_days", "future_return"}
    if isinstance(label, pd.DataFrame) and required <= set(label.columns):
        return label.loc[:, ["trade_date", "symbol", "horizon_days", "future_return"]].copy()
    series = label_to_series(label)
    return pd.DataFrame(
        {
            "trade_date": pd.to_datetime(series.index.get_level_values(0)).date,
            "symbol": series.index.get_level_values(1).astype(str),
            "horizon_days": label_horizon_days,
            "future_return": series.to_numpy(),
        }
    )


def _mean_or_nan(values: pd.Series) -> float:
    finite = pd.to_numeric(values, errors="coerce").dropna()
    if finite.empty:
        return float("nan")
    return float(finite.mean())


def compute_replacements(
    pred_ser: pd.DataFrame | pd.Series,
    label_ser: pd.DataFrame | pd.Series,
    hmm: dict[str, Any],
    topk: int,
    loop_label: str,
    label_horizon_days: int = 10,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    predictions = _prediction_frame(pred_ser)
    labels = _label_frame(label_ser, label_horizon_days=label_horizon_days)
    coefficients = CandidateCoefficients.from_payload(hmm)
    prediction_dates = pd.to_datetime(predictions["trade_date"], errors="raise").dt.date
    if prediction_dates.empty:
        raise RuntimeError("prediction artifact contains no rows")
    date_plan = resolve_batch_common_dates(
        predictions=predictions,
        labels=labels,
        candidates={loop_label: coefficients},
        window_start=min(prediction_dates),
        window_end=max(prediction_dates),
    )
    computation = evaluate_candidate(
        candidate_id=loop_label,
        predictions=predictions,
        labels=labels,
        coefficients=coefficients,
        evaluation_dates=date_plan.evaluation_dates,
        label_horizon_days=label_horizon_days,
        topk=topk,
        market_forward_return_mode="disabled",
        date_coverage_evidence=date_plan.as_evidence(),
    )

    label_column = f"label_{label_horizon_days}d"
    replacement_rows: list[dict[str, Any]] = []
    for source_row in computation.replacement_rows:
        row = dict(source_row)
        row["loop_label"] = loop_label
        row[label_column] = row.pop("label_return")
        row.pop("db_return_10d", None)
        replacement_rows.append(row)
    replacements = pd.DataFrame(replacement_rows)

    day_rows: list[dict[str, Any]] = []
    for source_day in computation.daily_summary:
        row = dict(source_day)
        date_str = str(row["date"])
        if replacements.empty:
            date_replacements = pd.DataFrame(
                columns=["date", "replacement_type", label_column]
            )
        else:
            date_replacements = replacements.loc[replacements["date"] == date_str]
        entered = date_replacements.loc[
            date_replacements["replacement_type"] == "entered_by_hmm",
            label_column,
        ]
        dropped = date_replacements.loc[
            date_replacements["replacement_type"] == "dropped_by_hmm",
            label_column,
        ]
        day_coefficients = coefficients.daily_coefficients[date.fromisoformat(date_str)]
        coefficient_values = list(day_coefficients.values())
        non_neutral = [value for value in coefficient_values if abs(value - 1.0) > 1e-9]
        row.update(
            {
                "loop_label": loop_label,
                f"mean_entered_{label_column}": _mean_or_nan(entered),
                f"mean_dropped_{label_column}": _mean_or_nan(dropped),
                f"net_enter_minus_drop_{label_column}": row.pop("daily_net_label"),
                "non_neutral_sector_count": len(non_neutral),
                "min_sector_coeff": min(coefficient_values),
                "max_sector_coeff": max(coefficient_values),
                "mean_sector_coeff": sum(coefficient_values) / len(coefficient_values),
            }
        )
        row.pop("daily_net_db_10d", None)
        day_rows.append(row)

    sector_rows = [
        {
            "loop_label": loop_label,
            "date": trade_date.isoformat(),
            "sector_code": sector_code,
            "coefficient": coefficient,
        }
        for trade_date in date_plan.evaluation_dates
        for sector_code, coefficient in sorted(
            coefficients.daily_coefficients[trade_date].items()
        )
    ]
    days = pd.DataFrame(day_rows)
    sectors = pd.DataFrame(sector_rows)
    warnings = list(computation.result["warnings_json"])
    for frame in (replacements, days, sectors):
        frame.attrs["evaluation_warnings"] = warnings
        frame.attrs["date_coverage"] = date_plan.as_evidence()
        frame.attrs["label_horizon_days"] = label_horizon_days
    return replacements, days, sectors


def enrich_db_forward_returns(
    replacements: pd.DataFrame,
    horizons: Sequence[int],
    *,
    repository: HMMMarketReturnRepository | None = None,
) -> pd.DataFrame:
    if replacements.empty:
        return replacements.copy()
    normalized_horizons = sorted(set(int(item) for item in horizons))
    if not normalized_horizons or normalized_horizons[0] < 1:
        raise ValueError("horizons must contain positive trading-day counts")
    symbols = sorted(replacements["symbol"].dropna().unique().tolist())
    trade_dates = sorted(set(pd.to_datetime(replacements["date"], errors="raise").dt.date))
    market_repository = repository or HMMMarketReturnRepository()
    watermark = market_repository.resolve_watermark(
        policy="latest_common_completed",
        requested_date=None,
    )
    enriched = replacements.copy()
    evidence: dict[str, Any] = {"watermark": watermark.as_manifest_evidence(), "horizons": {}}
    total_return_rows = 0
    for horizon in normalized_horizons:
        market_read = market_repository.read_forward_returns(
            symbols=symbols,
            trade_dates=trade_dates,
            horizon_trading_days=horizon,
            as_of_date=watermark.resolved_as_of_date,
        )
        return_column = f"db_ret_{horizon}d"
        return_frame = market_read.returns.loc[
            :, ["trade_date", "symbol", "future_return"]
        ].rename(columns={"trade_date": "date", "future_return": return_column})
        if not return_frame.empty:
            return_frame["date"] = pd.to_datetime(return_frame["date"]).dt.strftime("%Y-%m-%d")
        if return_frame.duplicated(["date", "symbol"]).any():
            raise MarketDataUnavailableError(
                "market forward returns contain duplicate date/symbol rows",
                context={"horizon_trading_days": horizon},
            )
        # The repository returns at most one row per date/symbol.  many_to_one
        # makes row explosion a hard failure while bounding output to the
        # replacement-row count for each of the three requested horizons.
        enriched = enriched.merge(
            return_frame,
            on=["date", "symbol"],
            how="left",
            validate="many_to_one",
        )
        total_return_rows += len(return_frame)
        coverage = float(enriched[return_column].notna().mean())
        evidence["horizons"][str(horizon)] = {
            **market_read.as_manifest_evidence(),
            "replacement_row_coverage_ratio": coverage,
        }
    if total_return_rows == 0:
        raise MarketDataUnavailableError(
            "no trading-day market returns cover the HMM replacement rows",
            context={
                "symbol_count": len(symbols),
                "date_count": len(trade_dates),
                "horizons": normalized_horizons,
            },
        )
    enriched.attrs.update(replacements.attrs)
    enriched.attrs["market_return_evidence"] = evidence
    return enriched


def summarize_replacements(rep: pd.DataFrame, day: pd.DataFrame) -> dict[str, Any]:
    if rep.empty:
        return {}
    label_horizon_days = int(rep.attrs.get("label_horizon_days") or 0)
    label_column = f"label_{label_horizon_days}d"
    day_label_column = f"net_enter_minus_drop_{label_column}"
    if label_horizon_days < 1 or label_column not in rep.columns or day_label_column not in day.columns:
        raise RuntimeError("replacement frames are missing explicit label-horizon metadata")
    out: dict[str, Any] = {
        "label_horizon_days": label_horizon_days,
        "days": int(day["date"].nunique()) if not day.empty else 0,
        "changed_days": int((day["replacement_count"] > 0).sum()) if not day.empty else 0,
        "avg_entered_per_day": safe_float(day["entered_count"].mean()) if not day.empty else None,
        "max_entered_per_day": safe_float(day["entered_count"].max()) if not day.empty else None,
        "total_enter_rows": int((rep["replacement_type"] == "entered_by_hmm").sum()),
        "total_drop_rows": int((rep["replacement_type"] == "dropped_by_hmm").sum()),
        "unique_enter_symbols": int(rep.loc[rep["replacement_type"] == "entered_by_hmm", "symbol"].nunique()),
        "unique_drop_symbols": int(rep.loc[rep["replacement_type"] == "dropped_by_hmm", "symbol"].nunique()),
        "mean_net_label_by_day": safe_float(day[day_label_column].mean()) if not day.empty else None,
        "positive_net_label_day_ratio": safe_float((day[day_label_column] > 0).mean()) if not day.empty else None,
    }
    for col in [label_column, "db_ret_5d", "db_ret_10d", "db_ret_20d"]:
        if col not in rep.columns:
            continue
        ent = rep.loc[rep["replacement_type"] == "entered_by_hmm", col].dropna()
        drp = rep.loc[rep["replacement_type"] == "dropped_by_hmm", col].dropna()
        out[f"coverage_{col}"] = safe_float(rep[col].notna().mean())
        out[f"entered_mean_{col}"] = safe_float(ent.mean()) if not ent.empty else None
        out[f"dropped_mean_{col}"] = safe_float(drp.mean()) if not drp.empty else None
        out[f"net_mean_{col}"] = safe_float(ent.mean() - drp.mean()) if not ent.empty and not drp.empty else None
    return out


def enhanced_stock_contribution(base: dict[str, Any], other: dict[str, Any]) -> dict[str, Any]:
    def stock_profit(em: dict[str, Any]) -> dict[str, float]:
        out = {}
        for row in em.get("all_stocks") or []:
            code = row.get("code")
            profit = safe_float(row.get("profit"))
            if code and profit is not None:
                out[code] = profit
        return out

    b = stock_profit(base)
    o = stock_profit(other)
    common = set(b) & set(o)
    only_o = set(o) - set(b)
    only_b = set(b) - set(o)
    return {
        "common_count": len(common),
        "hmm_only_count": len(only_o),
        "base_only_count": len(only_b),
        "common_profit_diff": sum(o[s] - b[s] for s in common),
        "hmm_only_profit": sum(o[s] for s in only_o),
        "base_only_profit": sum(b[s] for s in only_b),
        "total_proxy_diff": sum(o[s] - b[s] for s in common) + sum(o[s] for s in only_o) - sum(b[s] for s in only_b),
    }


def daily_return_diff(base_report: pd.DataFrame | None, other_report: pd.DataFrame | None) -> dict[str, Any]:
    if base_report is None or other_report is None:
        return {}
    if not isinstance(base_report, pd.DataFrame) or not isinstance(other_report, pd.DataFrame):
        return {}
    if base_report.index.has_duplicates or other_report.index.has_duplicates:
        raise RuntimeError("daily return reports must have unique date indexes")
    b = base_report[["return"]].rename(columns={"return": "base_return"}).copy()
    o = other_report[["return"]].rename(columns={"return": "hmm_return"}).copy()
    # Each report is one bounded daily row per date; concat cannot multiply rows.
    df = pd.concat([b, o], axis=1, join="inner")
    if df.empty:
        return {}
    df["diff"] = df["hmm_return"] - df["base_return"]
    worst = df.sort_values("diff").head(5)
    best = df.sort_values("diff", ascending=False).head(5)
    return {
        "days": len(df),
        "mean_daily_diff": safe_float(df["diff"].mean()),
        "positive_diff_ratio": safe_float((df["diff"] > 0).mean()),
        "worst_days": [(idx.strftime("%Y-%m-%d"), float(row["diff"])) for idx, row in worst.iterrows()],
        "best_days": [(idx.strftime("%Y-%m-%d"), float(row["diff"])) for idx, row in best.iterrows()],
    }


def write_markdown(
    path: Path,
    task_id: str,
    loops: list[LoopInfo],
    base_loop: LoopInfo,
    label_horizon_days: int,
    summaries: dict[str, dict[str, Any]],
    enhanced_comp: dict[str, dict[str, Any]],
    return_comp: dict[str, dict[str, Any]],
    day_paths: dict[str, Path],
    rep_paths: dict[str, Path],
    artifact_warnings: dict[str, list[dict[str, str]]],
    evaluation_warnings: dict[str, list[dict[str, Any]]],
    market_evidence: dict[str, dict[str, Any]],
) -> None:
    lines: list[str] = []
    lines.append(f"# HMM 离线诊断报告 - {task_id}")
    lines.append("")
    lines.append("## 执行边界")
    lines.append("- pred.pkl/label.pkl 只通过 Phase 0 BacktestDataSource 缓存复用；不下载或落盘 QE 配置文件。")
    lines.append("- QE loop/report/HMM 证据只通过 QEWorkspaceClient/节点 HTTP API 只读获取，没有修改 QE 策略代码。")
    lines.append("- Top50 重放口径是 raw score 与 HMM adjusted score 的排名替换，不等同于最终成交；最终成交还会受 n_drop、已有持仓、停牌/涨跌停、分钟执行影响。")
    lines.append(
        f"- label_{label_horizon_days}d 来自本次 QE 的 label.pkl；db_ret_5d/10d/20d "
        "来自 canonical HMMMarketReturnRepository 的交易日只读查询。"
    )
    lines.append("")
    lines.append("## Loop 基线")
    lines.append("```text")
    lines.append("Loop  Status     HMM  Label        AnnRet    MaxDD      Sharpe    IC        RankIC    Snapshot")
    lines.append("----  ---------  ---  -----------  --------  ---------  --------  --------  --------  ------------------------------------")
    for lp in loops:
        lines.append(
            f"L{lp.loop_index:<3}  {lp.status:<9}  {str(lp.has_hmm):<3}  {(lp.label or ''):<11}  "
            f"{pct(lp.annualized_return):>8}  {pct(lp.max_drawdown):>9}  {num(lp.sharpe):>8}  "
            f"{num(lp.ic):>8}  {num(lp.rank_ic):>8}  {(lp.snapshot_id or '')[:36]}"
        )
    lines.append("```")
    lines.append("")
    lines.append("## Top50 替换摘要")
    lines.append("```text")
    lines.append(
        "Loop/Version                 Days  ChgDays  AvgEnter  MaxEnter  EnterSym  "
        f"DropSym  NetLabel{label_horizon_days}D  NetDB5D    NetDB10D   NetDB20D"
    )
    lines.append("---------------------------  ----  -------  --------  --------  --------  -------  ----------  --------  --------  --------")
    for label, s in summaries.items():
        lines.append(
            f"{label[:27]:<27}  {s.get('days', 0):>4}  {s.get('changed_days', 0):>7}  "
            f"{num(s.get('avg_entered_per_day'), 2):>8}  {num(s.get('max_entered_per_day'), 0):>8}  "
            f"{s.get('unique_enter_symbols', 0):>8}  {s.get('unique_drop_symbols', 0):>7}  "
            f"{pct(s.get(f'net_mean_label_{label_horizon_days}d')):>10}  "
            f"{pct(s.get('net_mean_db_ret_5d')):>8}  "
            f"{pct(s.get('net_mean_db_ret_10d')):>8}  {pct(s.get('net_mean_db_ret_20d')):>8}"
        )
    lines.append("```")
    lines.append("")
    lines.append("## 交易贡献粗归因（enhanced_metrics）")
    lines.append("```text")
    lines.append("Loop/Version                 Common  HMMOnly  BaseOnly  CommonProfitDiff  HMMOnlyProfit  BaseOnlyProfit  TotalProxyDiff")
    lines.append("---------------------------  ------  -------  --------  ----------------  -------------  --------------  --------------")
    for label, c in enhanced_comp.items():
        lines.append(
            f"{label[:27]:<27}  {c.get('common_count', 0):>6}  {c.get('hmm_only_count', 0):>7}  "
            f"{c.get('base_only_count', 0):>8}  {c.get('common_profit_diff', 0):>16,.0f}  "
            f"{c.get('hmm_only_profit', 0):>13,.0f}  {c.get('base_only_profit', 0):>14,.0f}  "
            f"{c.get('total_proxy_diff', 0):>14,.0f}"
        )
    lines.append("```")
    lines.append("")
    lines.append("## 日收益差异")
    lines.append("```text")
    lines.append("Loop/Version                 Days  MeanDailyDiff  PositiveRatio")
    lines.append("---------------------------  ----  -------------  -------------")
    for label, c in return_comp.items():
        lines.append(f"{label[:27]:<27}  {c.get('days', 0):>4}  {pct(c.get('mean_daily_diff'), 4):>13}  {pct(c.get('positive_diff_ratio')):>13}")
    lines.append("```")
    lines.append("")
    for label, c in return_comp.items():
        if not c:
            continue
        lines.append(f"### {label} 最影响日")
        lines.append("- 最拖累: " + ", ".join([f"{d}({pct(v, 3)})" for d, v in c.get("worst_days", [])]))
        lines.append("- 最贡献: " + ", ".join([f"{d}({pct(v, 3)})" for d, v in c.get("best_days", [])]))
        lines.append("")
    lines.append("## 证据覆盖与显式告警")
    for label, summary in summaries.items():
        coverage = ", ".join(
            [
                f"label_{label_horizon_days}d={pct(summary.get(f'coverage_label_{label_horizon_days}d'))}",
                f"db5={pct(summary.get('coverage_db_ret_5d'))}",
                f"db10={pct(summary.get('coverage_db_ret_10d'))}",
                f"db20={pct(summary.get('coverage_db_ret_20d'))}",
            ]
        )
        lines.append(f"- {label}: {coverage}")
        evidence = market_evidence.get(label) or {}
        watermark = (evidence.get("watermark") or {}).get("resolved_as_of_date")
        if watermark:
            lines.append(f"  - market watermark: {watermark}")
    for label, warnings in artifact_warnings.items():
        for warning in warnings:
            lines.append(
                f"- {label}: artifact={warning['artifact']} reason={warning['reason_code']} "
                f"error={warning['error']}"
            )
    for label, warnings in evaluation_warnings.items():
        for warning in warnings:
            lines.append(
                f"- {label}: evaluator={warning.get('code')} message={warning.get('message')}"
            )
    if not any(artifact_warnings.values()) and not any(evaluation_warnings.values()):
        lines.append("- 无 artifact/evaluator 告警。")
    lines.append("")
    lines.append("## 产物")
    for label, p in rep_paths.items():
        lines.append(f"- {label} replacement rows: `{p}`")
    for label, p in day_paths.items():
        lines.append(f"- {label} daily summary: `{p}`")
    lines.append("")
    lines.append("## 当前判断")
    lines.append(
        f"- 如果 NetLabel{label_horizon_days}D/NetDB10D 为负，说明 HMM 替换进来的股票后验收益低于"
        "被挤出的 raw Top50 股票，是 overlay 直接拖累收益的证据。"
    )
    lines.append(
        f"- 如果 NetLabel{label_horizon_days}D 为正但交易收益差，问题更可能在 n_drop、已有持仓、"
        "执行/停牌过滤或权重分配。"
    )
    lines.append("- 如果替换数量极少但收益差明显，重点查少数高金额/长持仓股票；如果替换数量很大且净收益为负，重点查 HMM 系数方向与强度。")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def resolve_label_horizon(label: str | None, requested: int | None) -> int:
    if requested is not None:
        if not 1 <= requested <= 30:
            raise ValueError("label_horizon_days must be between 1 and 30")
        return requested
    normalized = str(label or "").strip()
    if not normalized:
        raise RuntimeError(
            "cannot infer label horizon from the base loop; pass --label-horizon-days explicitly"
        )
    named_matches = {
        int(value)
        for value in re.findall(
            r"(?i)(?:\bh(?:orizon)?|label|return)[_\- ]?([1-9]\d?)d?\b",
            normalized,
        )
    }
    if len(named_matches) > 1:
        raise RuntimeError(
            f"base loop label contains conflicting horizon values: {sorted(named_matches)}"
        )
    if named_matches:
        horizon = next(iter(named_matches))
    else:
        ref_matches = [
            int(value)
            for value in re.findall(
                r"Ref\(\s*\$close\s*,\s*-(\d{1,2})\s*\)",
                normalized,
                flags=re.IGNORECASE,
            )
        ]
        if not ref_matches:
            raise RuntimeError(
                "cannot infer label horizon from the base loop; pass --label-horizon-days explicitly"
            )
        horizon = max(ref_matches)
    if not 1 <= horizon <= 30:
        raise RuntimeError(f"inferred label horizon is outside 1..30 days: {horizon}")
    return horizon


async def run(
    task_id: str,
    out_dir: Path,
    topk: int,
    label_horizon_days: int | None = None,
) -> Path:
    loops, _node_id, api_base = load_loops(task_id)
    completed = [lp for lp in loops if lp.status == "completed" and lp.experiment_id]
    if not completed:
        raise RuntimeError(f"No completed loops found for {task_id}")
    base_loop = next((lp for lp in completed if not lp.has_hmm), completed[0])
    resolved_label_horizon = resolve_label_horizon(base_loop.label, label_horizon_days)
    client = QEWorkspaceClient(base_url=f"{api_base}/api/v1/qe_workspace")
    artifact_dir = out_dir / task_id / "artifacts"
    table_dir = out_dir / task_id / "tables"
    report_dir = ROOT / "docs" / "analysis"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    table_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    try:
        base_art = await download_loop_artifacts(client, task_id, base_loop, artifact_dir)
        loop_artifacts: dict[int, dict[str, Any]] = {base_loop.loop_index: base_art}
        for lp in completed:
            if lp.loop_index == base_loop.loop_index:
                continue
            loop_artifacts[lp.loop_index] = await download_loop_artifacts(client, task_id, lp, artifact_dir)
    finally:
        await client.close()

    source = BacktestDataSource(
        base_loop_ref=f"{task_id}/{base_loop.loop_id}",
        label_horizon_days=resolved_label_horizon,
    )
    async with source:
        source_start, source_end = await source.get_available_date_range()
        predictions = await source.get_predictions(source_start, source_end)
        labels = await source.get_labels(
            source_start,
            source_end,
            horizon_days=resolved_label_horizon,
        )

    exp_ids = [lp.experiment_id for lp in completed if lp.experiment_id]
    enhanced = enhanced_metrics_by_experiment(exp_ids)
    base_enhanced = enhanced.get(base_loop.experiment_id or "", {})
    summaries: dict[str, dict[str, Any]] = {}
    enhanced_comp: dict[str, dict[str, Any]] = {}
    return_comp: dict[str, dict[str, Any]] = {}
    rep_paths: dict[str, Path] = {}
    day_paths: dict[str, Path] = {}
    artifact_warnings: dict[str, list[dict[str, str]]] = {}
    evaluation_warnings: dict[str, list[dict[str, Any]]] = {}
    market_evidence: dict[str, dict[str, Any]] = {}

    for lp in completed:
        loop_label = f"L{lp.loop_index}_{lp.label or ('HMM' if lp.has_hmm else 'BASE')}"
        artifact_warnings[loop_label] = list(
            (loop_artifacts.get(lp.loop_index) or {}).get("warnings") or []
        )

    base_report = loop_artifacts.get(base_loop.loop_index, {}).get("report")
    for lp in completed:
        if not lp.has_hmm:
            continue
        art = loop_artifacts.get(lp.loop_index) or {}
        hmm = art.get("hmm")
        if not hmm:
            raise RuntimeError(f"completed HMM loop has no coefficient artifact: L{lp.loop_index}")
        label = f"L{lp.loop_index}_{lp.label or 'HMM'}"
        rep, day, sectors = compute_replacements(
            predictions,
            labels,
            hmm,
            topk,
            label,
            label_horizon_days=resolved_label_horizon,
        )
        evaluation_warnings[label] = list(day.attrs.get("evaluation_warnings") or [])
        rep = enrich_db_forward_returns(rep, horizons=[5, 10, 20])
        market_evidence[label] = dict(rep.attrs.get("market_return_evidence") or {})
        summary = summarize_replacements(rep, day)
        summaries[label] = summary
        rep_path = table_dir / f"{label}_top{topk}_replacements.csv"
        day_path = table_dir / f"{label}_top{topk}_daily_summary.csv"
        sector_path = table_dir / f"{label}_sector_coefficients.csv"
        rep.to_csv(rep_path, index=False, encoding="utf-8-sig")
        day.to_csv(day_path, index=False, encoding="utf-8-sig")
        sectors.to_csv(sector_path, index=False, encoding="utf-8-sig")
        rep_paths[label] = rep_path
        day_paths[label] = day_path
        other_enhanced = enhanced.get(lp.experiment_id or "", {})
        if base_enhanced and other_enhanced:
            enhanced_comp[label] = enhanced_stock_contribution(base_enhanced, other_enhanced)
        return_comp[label] = daily_return_diff(base_report, art.get("report"))

    report_path = report_dir / f"hmm_offline_diagnostic_{task_id}.md"
    write_markdown(
        report_path,
        task_id,
        loops,
        base_loop,
        resolved_label_horizon,
        summaries,
        enhanced_comp,
        return_comp,
        day_paths,
        rep_paths,
        artifact_warnings,
        evaluation_warnings,
        market_evidence,
    )
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_id")
    parser.add_argument("--out-dir", default=str(ROOT / "tmp" / "hmm_offline_diag"))
    parser.add_argument("--topk", type=int, default=50)
    parser.add_argument("--label-horizon-days", type=int)
    args = parser.parse_args()
    report = asyncio.run(
        run(
            args.task_id,
            Path(args.out_dir),
            args.topk,
            label_horizon_days=args.label_horizon_days,
        )
    )
    print(report)


if __name__ == "__main__":
    main()
