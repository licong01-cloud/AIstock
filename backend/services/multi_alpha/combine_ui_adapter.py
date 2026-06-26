"""Read-only UI adapter for multi-alpha combine-backtest results.

The adapter maps ``strategy_pkg.multi_alpha_combine_backtest_*`` rows into the
custom-evolution-shaped contract consumed by the existing QE result UI.  It does
not read or mutate QE evolution tables.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime
from typing import Any, Mapping, Protocol, Sequence

from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field

from backend.db.pg_pool import get_conn


DEFAULT_SCHEME = "ic_weighted"
SCHEME_PRIORITY = (
    "ic_weighted",
    "risk_parity",
    "orthogonality_aware",
    "equal",
    "rank_fusion_rrf",
    "rank_fusion_borda",
)
TERMINAL_RUN_STATUSES = {"succeeded", "failed", "partial_failed"}


class CombineUIAdapterError(RuntimeError):
    """Raised when macb rows cannot be adapted without ambiguity."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str = "combine_ui_adapter_error",
        context: Mapping[str, Any] | None = None,
    ) -> None:
        self.reason_code = reason_code
        self.context = dict(context or {})
        super().__init__(f"reason_code={reason_code}: {message}")


class CombineLoopMetrics(BaseModel):
    annualized_return: float | None = None
    cagr: float | None = None
    sharpe: float | None = None
    max_drawdown: float | None = None
    calmar: float | None = None
    turnover: float | None = None
    topk_return_20: float | None = None
    topk_hit_rate_20: float | None = None
    IC: None = None
    ICIR: None = None
    Rank_IC: None = None


class CombineLoopRow(BaseModel):
    loop_id: str
    task_id: str
    run_id: str
    loop_index: int
    status: str
    is_sota: bool
    action_type: str = "multi_alpha_combine"
    config_json: dict[str, Any]
    metrics_json: CombineLoopMetrics
    loo: list[dict[str, Any]] = Field(default_factory=list)
    weights_json: dict[str, Any] = Field(default_factory=dict)
    per_window_weights_json: list[Any] = Field(default_factory=list)
    oos_start: str | None = None
    oos_end: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class CombineTaskItem(BaseModel):
    task_id: str
    task_name: str
    task_type: str = "multi_alpha_combine"
    status: str
    current_loop: int
    max_loops: int
    created_at: str
    updated_at: str
    roster_hash: str
    normalize_method: str
    walk_forward_signature: str
    available_schemes: list[str] = Field(default_factory=list)
    default_scheme: str = DEFAULT_SCHEME
    phase: str | None = None
    running_count: int = 0


class CombineTrajectoryResp(BaseModel):
    trajectory: list[CombineLoopRow]
    scheme: str
    available_schemes: list[str]
    scheme_warning: dict[str, Any] | None = None


class CombineTaskDetailResp(BaseModel):
    task: CombineTaskItem
    loops: list[CombineLoopRow]
    scheme: str
    available_schemes: list[str]
    scheme_warning: dict[str, Any] | None = None


class CombineCustomEvoConfigResp(BaseModel):
    loops: list[dict[str, Any]]
    scheme: str
    available_schemes: list[str]


class CombineUIRepository(Protocol):
    def list_run_headers(self) -> list[dict[str, Any]]:
        ...

    def get_run_bundle(self, run_id: str) -> dict[str, Any] | None:
        ...


class PostgresCombineUIRepository:
    """SELECT-only repository over macb result tables."""

    def __init__(self, connection_provider=get_conn) -> None:
        self._connection_provider = connection_provider

    def list_run_headers(self) -> list[dict[str, Any]]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    ORDER BY created_at DESC
                    """
                )
                return [dict(row) for row in cur.fetchall()]

    def get_run_bundle(self, run_id: str) -> dict[str, Any] | None:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_run WHERE id = %s", (run_id,))
                run = cur.fetchone()
                if not run:
                    return None
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_scheme_result
                    WHERE run_id = %s
                    ORDER BY weighting_scheme
                    """,
                    (run_id,),
                )
                schemes = cur.fetchall()
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_loo
                    WHERE run_id = %s
                    ORDER BY weighting_scheme, dropped_leg_id
                    """,
                    (run_id,),
                )
                loo = cur.fetchall()
        return {
            "run": _normalize_run_status(dict(run)),
            "scheme_results": [dict(row) for row in schemes],
            "loo": [dict(row) for row in loo],
        }


class MultiAlphaCombineUIAdapter:
    """Build custom-evo compatible read models for combine-backtest UI pages."""

    def __init__(self, repository: CombineUIRepository | None = None) -> None:
        self._repository = repository or PostgresCombineUIRepository()

    def list_tasks(self, *, status: str | None = None, limit: int = 20, offset: int = 0) -> dict[str, Any]:
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        groups = self._load_groups()
        tasks = [
            self._task_item(
                task_key,
                rows,
                available_schemes=self._available_schemes_for_rows(task_key, rows),
            )
            for task_key, rows in groups
        ]
        if status:
            tasks = [task for task in tasks if task.status == status]
        page = tasks[offset : offset + limit]
        return {
            "tasks": [task.model_dump() for task in page],
            "count": len(page),
            "total": len(tasks),
            "limit": limit,
            "offset": offset,
        }

    def get_task(self, task_key: str, *, scheme: str | None = None) -> dict[str, Any]:
        rows = self._require_task_rows(task_key)
        loops, selected_scheme, available_schemes, warning = self._build_loops(task_key, rows, scheme=scheme)
        task = self._task_item(task_key, rows, available_schemes=available_schemes, default_scheme=selected_scheme)
        return CombineTaskDetailResp(
            task=task,
            loops=loops,
            scheme=selected_scheme,
            available_schemes=available_schemes,
            scheme_warning=warning,
        ).model_dump()

    def get_trajectory(self, task_key: str, *, scheme: str | None = None) -> dict[str, Any]:
        rows = self._require_task_rows(task_key)
        loops, selected_scheme, available_schemes, warning = self._build_loops(task_key, rows, scheme=scheme)
        return CombineTrajectoryResp(
            trajectory=loops,
            scheme=selected_scheme,
            available_schemes=available_schemes,
            scheme_warning=warning,
        ).model_dump()

    def get_custom_evo_config(self, task_key: str, *, scheme: str | None = None) -> dict[str, Any]:
        rows = self._require_task_rows(task_key)
        loops, selected_scheme, available_schemes, _warning = self._build_loops(task_key, rows, scheme=scheme)
        return CombineCustomEvoConfigResp(
            loops=[
                {
                    "loop_index": loop.loop_index,
                    "label": loop.config_json.get("label"),
                    "strategy_params": loop.config_json.get("strategy_params") or {},
                    "runtime_flags": loop.config_json.get("runtime_flags") or {},
                }
                for loop in loops
            ],
            scheme=selected_scheme,
            available_schemes=available_schemes,
        ).model_dump()

    def _available_schemes_for_rows(self, task_key: str, rows: Sequence[Mapping[str, Any]]) -> list[str]:
        bundles = []
        for row in rows:
            run_id = str(row.get("id") or "")
            bundle = self._repository.get_run_bundle(run_id)
            if bundle is None:
                raise CombineUIAdapterError(
                    "combine UI referenced run is missing while listing schemes",
                    reason_code="combine_ui_run_missing",
                    context={"task_key": task_key, "run_id": run_id},
                )
            bundle["run"] = _normalize_run_status(bundle["run"])
            bundles.append(bundle)
        return _common_schemes(bundles)

    def get_loop(self, task_key: str, loop_index: int, *, scheme: str | None = None) -> dict[str, Any]:
        trajectory = self.get_trajectory(task_key, scheme=scheme)
        for loop in trajectory["trajectory"]:
            if int(loop["loop_index"]) == int(loop_index):
                return {
                    "loop": loop,
                    "scheme": trajectory["scheme"],
                    "available_schemes": trajectory["available_schemes"],
                    "scheme_warning": trajectory.get("scheme_warning"),
                }
        raise CombineUIAdapterError(
            "combine UI loop not found",
            reason_code="combine_ui_loop_not_found",
            context={"task_key": task_key, "loop_index": loop_index},
        )

    def _load_groups(self) -> list[tuple[str, list[dict[str, Any]]]]:
        rows = [_normalize_run_status(row) for row in self._repository.list_run_headers()]
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(task_key_for_run(row), []).append(row)
        ordered = []
        for task_key, task_rows in grouped.items():
            ordered.append((task_key, _sort_run_rows(task_rows)))
        ordered.sort(key=lambda item: _parse_timestamp(_task_updated_at(item[1])), reverse=True)
        return ordered

    def _require_task_rows(self, task_key: str) -> list[dict[str, Any]]:
        for key, rows in self._load_groups():
            if key == task_key:
                return rows
        raise CombineUIAdapterError(
            "combine UI task key not found",
            reason_code="combine_ui_task_not_found",
            context={"task_key": task_key},
        )

    def _task_item(
        self,
        task_key: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        available_schemes: list[str] | None = None,
        default_scheme: str = DEFAULT_SCHEME,
    ) -> CombineTaskItem:
        statuses = [str(row.get("status") or "") for row in rows]
        status = _task_status(statuses)
        running_rows = [row for row in rows if row.get("status") == "running"]
        terminal_count = sum(1 for row in rows if row.get("status") in TERMINAL_RUN_STATUSES)
        first = rows[0]
        return CombineTaskItem(
            task_id=task_key,
            task_name=_task_name(first),
            status=status,
            current_loop=terminal_count if status == "running" else len(rows),
            max_loops=len(rows),
            created_at=_iso(_min_timestamp(row.get("created_at") for row in rows)),
            updated_at=_iso(_task_updated_at(rows)),
            roster_hash=str(first.get("roster_hash") or ""),
            normalize_method=str(first.get("normalize_method") or ""),
            walk_forward_signature=_walk_forward_signature(_as_mapping(first.get("walk_forward_json"))),
            available_schemes=available_schemes or [],
            default_scheme=default_scheme,
            phase=_running_phase(running_rows),
            running_count=len(running_rows),
        )

    def _build_loops(
        self,
        task_key: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        scheme: str | None,
    ) -> tuple[list[CombineLoopRow], str, list[str], dict[str, Any] | None]:
        bundles = []
        for row in rows:
            run_id = str(row.get("id") or "")
            bundle = self._repository.get_run_bundle(run_id)
            if bundle is None:
                raise CombineUIAdapterError(
                    "combine UI referenced run is missing",
                    reason_code="combine_ui_run_missing",
                    context={"task_key": task_key, "run_id": run_id},
                )
            bundle["run"] = _normalize_run_status(bundle["run"])
            bundles.append(bundle)
        available_schemes = _common_schemes(bundles)
        selected_scheme, warning = _select_scheme(available_schemes, requested=scheme)
        window_rank = _window_ranks([bundle["run"] for bundle in bundles])
        sorted_bundles = sorted(bundles, key=lambda bundle: _run_sort_key(bundle["run"]))
        selected_results = [_require_scheme(bundle, selected_scheme) for bundle in sorted_bundles]
        best_run_id = _best_succeeded_run_id(sorted_bundles, selected_results)

        loops: list[CombineLoopRow] = []
        for index, (bundle, scheme_result) in enumerate(zip(sorted_bundles, selected_results, strict=True), start=1):
            run = bundle["run"]
            run_id = str(run.get("id") or "")
            config = _loop_config(run, scheme_result, selected_scheme, window_rank.get(_window_key(run), index))
            loop_loo = _loo_for_scheme(bundle.get("loo") or [], selected_scheme)
            loops.append(
                CombineLoopRow(
                    loop_id=f"{task_key}_Loop{index}",
                    task_id=task_key,
                    run_id=run_id,
                    loop_index=index,
                    status=_loop_status(str(run.get("status") or "")),
                    is_sota=run_id == best_run_id,
                    config_json=config,
                    metrics_json=_loop_metrics(scheme_result),
                    loo=loop_loo,
                    weights_json=_as_mapping(scheme_result.get("weights_json")),
                    per_window_weights_json=_as_list(scheme_result.get("per_window_weights_json")),
                    oos_start=_iso(run.get("oos_start")),
                    oos_end=_iso(run.get("oos_end")),
                    created_at=_iso(run.get("created_at")),
                    updated_at=_iso(run.get("updated_at") or run.get("created_at")),
                )
            )
        return loops, selected_scheme, available_schemes, warning


def error_payload(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, CombineUIAdapterError):
        return {"reason_code": exc.reason_code, "message": str(exc), "context": exc.context}
    return {"reason_code": "combine_ui_adapter_unhandled_error", "message": str(exc)}


def task_key_for_run(row: Mapping[str, Any]) -> str:
    roster_hash = str(row.get("roster_hash") or "")
    normalize_method = str(row.get("normalize_method") or "")
    wf_signature = _walk_forward_signature(_as_mapping(row.get("walk_forward_json")))
    if not roster_hash or not normalize_method:
        raise CombineUIAdapterError(
            "combine run header is missing roster_hash or normalize_method",
            reason_code="combine_ui_invalid_run_header",
            context={"run_id": row.get("id"), "roster_hash": roster_hash, "normalize_method": normalize_method},
        )
    return f"{roster_hash}|{normalize_method}|{wf_signature}"


def _normalize_run_status(row: dict[str, Any]) -> dict[str, Any]:
    reason = _as_mapping(row.get("reason"))
    if reason.get("logical_status") == "partial_failed":
        row["status"] = "partial_failed"
    return row


def _walk_forward_signature(walk_forward: Mapping[str, Any]) -> str:
    enabled = walk_forward.get("enabled", True)
    window = walk_forward.get("window", "na")
    min_periods = walk_forward.get("min_periods", "na")
    expanding = walk_forward.get("expanding", False)
    return f"wf_w{window}_min{min_periods}_exp{str(bool(expanding)).lower()}_en{str(bool(enabled)).lower()}"


def _task_name(row: Mapping[str, Any]) -> str:
    roster = _as_list(row.get("roster_json"))
    leg_ids = []
    for item in roster:
        if isinstance(item, Mapping):
            leg_ids.append(str(item.get("leg_id") or item.get("id") or "unknown_leg"))
        else:
            leg_ids.append(str(item))
    return "+".join(leg_ids) if leg_ids else str(row.get("roster_hash") or "multi-alpha")


def _task_status(statuses: Sequence[str]) -> str:
    if any(status == "running" for status in statuses):
        return "running"
    if statuses and all(status == "succeeded" for status in statuses):
        return "completed"
    if any(status in {"failed", "partial_failed"} for status in statuses):
        return "failed"
    return statuses[0] if statuses else "pending"


def _loop_status(status: str) -> str:
    if status == "succeeded":
        return "completed"
    if status == "running":
        return "running"
    if status in {"failed", "partial_failed"}:
        return "failed"
    return status or "pending"


def _common_schemes(bundles: Sequence[Mapping[str, Any]]) -> list[str]:
    common: set[str] | None = None
    seen_any = False
    for bundle in bundles:
        run = _as_mapping(bundle.get("run"))
        schemes = {
            str(row.get("weighting_scheme"))
            for row in _as_list(bundle.get("scheme_results"))
            if row.get("weighting_scheme")
        }
        if not schemes:
            if run.get("status") == "succeeded":
                raise CombineUIAdapterError(
                    "succeeded combine run has no scheme_result rows",
                    reason_code="combine_ui_scheme_results_missing",
                    context={"run_id": run.get("id")},
                )
            continue
        seen_any = True
        common = schemes if common is None else common & schemes
    if not seen_any:
        return [DEFAULT_SCHEME]
    if not common:
        raise CombineUIAdapterError(
            "combine task has no common weighting_scheme across runs",
            reason_code="combine_ui_no_common_weighting_scheme",
            context={"run_ids": [_as_mapping(bundle.get("run")).get("id") for bundle in bundles]},
        )
    return sorted(common, key=lambda item: SCHEME_PRIORITY.index(item) if item in SCHEME_PRIORITY else len(SCHEME_PRIORITY))


def _select_scheme(available_schemes: Sequence[str], *, requested: str | None) -> tuple[str, dict[str, Any] | None]:
    if requested:
        if requested not in available_schemes:
            raise CombineUIAdapterError(
                "requested weighting_scheme is not available for this combine task",
                reason_code="combine_ui_weighting_scheme_not_found",
                context={"requested_scheme": requested, "available_schemes": list(available_schemes)},
            )
        return requested, None
    if DEFAULT_SCHEME in available_schemes:
        return DEFAULT_SCHEME, None
    selected = available_schemes[0]
    return selected, {
        "reason_code": "combine_ui_default_scheme_unavailable",
        "requested_scheme": DEFAULT_SCHEME,
        "selected_scheme": selected,
        "available_schemes": list(available_schemes),
    }


def _require_scheme(bundle: Mapping[str, Any], scheme: str) -> dict[str, Any]:
    for row in _as_list(bundle.get("scheme_results")):
        if row.get("weighting_scheme") == scheme:
            return dict(row)
    run = _as_mapping(bundle.get("run"))
    if run.get("status") != "succeeded":
        return {
            "weighting_scheme": scheme,
            "weights_json": {},
            "per_window_weights_json": [],
            "skipped": True,
            "skipped_reason": "scheme_result_not_available_for_non_succeeded_run",
        }
    raise CombineUIAdapterError(
        "weighting_scheme row disappeared while building combine UI payload",
        reason_code="combine_ui_weighting_scheme_missing_for_run",
        context={"run_id": run.get("id"), "scheme": scheme},
    )


def _best_succeeded_run_id(bundles: Sequence[Mapping[str, Any]], scheme_results: Sequence[Mapping[str, Any]]) -> str | None:
    candidates: list[tuple[float, float, str]] = []
    for bundle, scheme_result in zip(bundles, scheme_results, strict=True):
        run = _as_mapping(bundle.get("run"))
        if run.get("status") != "succeeded" or bool(scheme_result.get("skipped")):
            continue
        cagr = _finite_float(scheme_result.get("cagr"))
        if cagr is None:
            continue
        sharpe = _finite_float(scheme_result.get("sharpe")) or float("-inf")
        candidates.append((cagr, sharpe, str(run.get("id") or "")))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1]))[2]


def _loop_metrics(scheme_result: Mapping[str, Any]) -> CombineLoopMetrics:
    cagr = _finite_float(scheme_result.get("cagr"))
    return CombineLoopMetrics(
        annualized_return=cagr,
        cagr=cagr,
        sharpe=_finite_float(scheme_result.get("sharpe")),
        max_drawdown=_finite_float(scheme_result.get("max_drawdown")),
        calmar=_finite_float(scheme_result.get("calmar")),
        turnover=_finite_float(scheme_result.get("turnover")),
        topk_return_20=_finite_float(scheme_result.get("topk_return_20")),
        topk_hit_rate_20=_finite_float(scheme_result.get("topk_hit_rate_20")),
    )


def _loop_config(run: Mapping[str, Any], scheme_result: Mapping[str, Any], scheme: str, window_index: int) -> dict[str, Any]:
    backtest_config = _as_mapping(run.get("backtest_config_json"))
    topk = backtest_config.get("topk")
    label = f"win{window_index} top{topk}" if topk is not None else f"win{window_index}"
    return {
        "label": label,
        "description": f"{_iso(run.get('oos_start'))}~{_iso(run.get('oos_end'))} {scheme}",
        "action_type": "multi_alpha_combine",
        "model_id": "multi_alpha_combine",
        "model_type": "MultiAlphaCombine",
        "strategy_params": {
            "topk": topk,
            "weighting_scheme": scheme,
            "normalize_method": run.get("normalize_method"),
        },
        "runtime_flags": {
            "loop_desc": f"{_iso(run.get('oos_start'))}~{_iso(run.get('oos_end'))} {scheme}",
            "ui_label": label,
            "window": {"oos_start": _iso(run.get("oos_start")), "oos_end": _iso(run.get("oos_end"))},
            "run_id": run.get("id"),
        },
        "roster": _as_list(run.get("roster_json")),
        "weights_json": _as_mapping(scheme_result.get("weights_json")),
        "per_window_weights_json": _as_list(scheme_result.get("per_window_weights_json")),
        "baseline_leg_id": run.get("baseline_leg_id"),
        "walk_forward": _as_mapping(run.get("walk_forward_json")),
        "backtest_config": backtest_config,
    }


def _loo_for_scheme(rows: Sequence[Mapping[str, Any]], scheme: str) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        if row.get("weighting_scheme") != scheme:
            continue
        result.append(
            {
                "dropped_leg_id": row.get("dropped_leg_id"),
                "marginal_cagr": _finite_float(row.get("marginal_cagr")),
                "marginal_sharpe": _finite_float(row.get("marginal_sharpe")),
                "marginal_calmar": _finite_float(row.get("marginal_calmar")),
                "is_negative_contributor": any(
                    (value is not None and value <= 0)
                    for value in (
                        _finite_float(row.get("marginal_cagr")),
                        _finite_float(row.get("marginal_sharpe")),
                        _finite_float(row.get("marginal_calmar")),
                    )
                ),
            }
        )
    return result


def _window_ranks(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], int]:
    windows = sorted({_window_key(row) for row in rows})
    return {window: index for index, window in enumerate(windows, start=1)}


def _window_key(row: Mapping[str, Any]) -> tuple[str, str]:
    return (_iso(row.get("oos_start")) or "", _iso(row.get("oos_end")) or "")


def _sort_run_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted((dict(row) for row in rows), key=_run_sort_key)


def _run_sort_key(row: Mapping[str, Any]) -> tuple[int, str, str, str, str]:
    return (
        _extract_topk(row),
        _iso(row.get("oos_start")) or "",
        _iso(row.get("oos_end")) or "",
        _iso(row.get("created_at")) or "",
        str(row.get("id") or ""),
    )


def _extract_topk(row: Mapping[str, Any]) -> int:
    value = _as_mapping(row.get("backtest_config_json")).get("topk")
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _running_phase(rows: Sequence[Mapping[str, Any]]) -> str | None:
    for row in rows:
        reason = _as_mapping(row.get("reason"))
        phase = reason.get("phase")
        if phase:
            return str(phase)
    return None


def _task_updated_at(rows: Sequence[Mapping[str, Any]]) -> Any:
    values = [row.get("updated_at") or row.get("created_at") for row in rows]
    return _max_timestamp(values)


def _min_timestamp(values: Sequence[Any]) -> Any:
    parsed = [value for value in values if value is not None]
    if not parsed:
        return None
    return min(parsed, key=_parse_timestamp)


def _max_timestamp(values: Sequence[Any]) -> Any:
    parsed = [value for value in values if value is not None]
    if not parsed:
        return None
    return max(parsed, key=_parse_timestamp)


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    text = str(value or "")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min


def _as_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise CombineUIAdapterError(
                "combine UI expected JSON object but received invalid JSON text",
                reason_code="combine_ui_invalid_json_object",
                context={"value_preview": value[:120]},
            ) from exc
        if isinstance(parsed, Mapping):
            return dict(parsed)
    raise CombineUIAdapterError(
        "combine UI expected JSON object",
        reason_code="combine_ui_invalid_json_object",
        context={"value_type": type(value).__name__},
    )


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise CombineUIAdapterError(
                "combine UI expected JSON array but received invalid JSON text",
                reason_code="combine_ui_invalid_json_array",
                context={"value_preview": value[:120]},
            ) from exc
        if isinstance(parsed, list):
            return parsed
    raise CombineUIAdapterError(
        "combine UI expected JSON array",
        reason_code="combine_ui_invalid_json_array",
        context={"value_type": type(value).__name__},
    )


def _finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)
