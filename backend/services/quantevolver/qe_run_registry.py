"""Canonical QE run reservation and lightweight history projection.

This adapter deliberately reuses the existing QE control-plane tables.  It
does not own execution, polling, artifacts, or research data.  Its only write
boundary is reservation-before-dispatch plus idempotent state transitions.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from psycopg2.extras import RealDictCursor

from ...db.pg_pool import get_conn


QE_RUN_REGISTRATION_PARAM = "_qe_run_registration"
QE_RUN_REGISTRATION_SCHEMA = "qe_run_registration_v1"
QE_RUN_PURPOSES = frozenset({"research", "validation"})
QE_RUN_SOURCE_TYPES = frozenset({"ui", "mcp", "scheduler", "agent"})
QE_RUN_CONSUMERS = frozenset({"qe_mainline", "advisory"})
QE_RUN_DEFAULT_CONSUMER = "qe_mainline"
_CANONICAL_STATUS_ALIASES = {
    "created": "planned",
    "pending": "queued",
    "waiting": "queued",
    "waiting_capacity": "queued",
    "processing": "running",
    "success": "completed",
    "succeeded": "completed",
    "error": "failed",
    "timeout": "failed",
    "canceled": "cancelled",
}


class QERunRegistryError(RuntimeError):
    """Stable, loud failure raised when a canonical reservation is not durable."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(f"reason_code={reason_code}: {message}")
        self.reason_code = reason_code
        self.message = message


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _digest_strings(values: Iterable[Any]) -> str:
    normalized = sorted({str(value).strip() for value in values if str(value).strip()})
    payload = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _factor_names(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = [value]
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        return []
    return [str(item).split("||", 1)[0] for item in value if str(item).strip()]


def _portable_fields(source: Mapping[str, Any], keys: Sequence[str]) -> dict[str, Any]:
    return {
        key: source[key]
        for key in keys
        if source.get(key) not in (None, "", [], {})
        and isinstance(source.get(key), (str, int, float, bool))
    }


def normalize_qe_run_purpose(value: Any) -> str:
    purpose = str(value or "research").strip().lower()
    if purpose not in QE_RUN_PURPOSES:
        raise QERunRegistryError(
            "qe_run_purpose_invalid",
            f"purpose must be one of {sorted(QE_RUN_PURPOSES)}, got {value!r}",
        )
    return purpose


def normalize_qe_run_source_type(value: Any) -> str:
    source_type = str(value or "ui").strip().lower()
    if source_type not in QE_RUN_SOURCE_TYPES:
        raise QERunRegistryError(
            "qe_run_source_type_invalid",
            f"source_type must be one of {sorted(QE_RUN_SOURCE_TYPES)}, got {value!r}",
        )
    return source_type


def normalize_qe_run_consumer_id(value: Any) -> str:
    consumer_id = (
        QE_RUN_DEFAULT_CONSUMER
        if value is None
        else str(value).strip().lower()
    )
    if consumer_id not in QE_RUN_CONSUMERS:
        raise QERunRegistryError(
            "qe_run_consumer_id_invalid",
            f"consumer_id must be one of {sorted(QE_RUN_CONSUMERS)}, got {value!r}",
        )
    return consumer_id


def qe_registration_summary(value: Any) -> dict[str, Any]:
    """Normalize a persisted registration without mutating the stored payload."""

    registration = _mapping(value)
    if registration:
        registration["consumer_id"] = normalize_qe_run_consumer_id(
            registration.get("consumer_id")
        )
    return registration


def canonical_qe_status(value: Any) -> str:
    status = str(value or "unknown").strip().lower()
    return _CANONICAL_STATUS_ALIASES.get(status, status)


def build_qe_run_registration(
    *,
    run_kind: str,
    custom_params: Mapping[str, Any] | None = None,
    consumer_id: str | None = None,
    source_type: str | None = None,
    created_by_name: str | None = None,
    purpose: str | None = None,
    node_id: str | None = None,
    model_id: str | None = None,
    factor_names: Sequence[Any] | None = None,
    data_split: Mapping[str, Any] | None = None,
    strategy_id: str | None = None,
    parent_id: str | None = None,
    task_id: str | None = None,
    loop_index: int | None = None,
) -> dict[str, Any]:
    """Build the small, portable registration summary stored in JSONB."""

    params = dict(custom_params or {})
    provenance = _mapping(params.get("qe_mcp_provenance"))
    dataset = _mapping(params.get("_qe_active_dataset_summary"))
    binding = _mapping(params.get("_qe_direct_v2_dataset_binding"))
    selection = _mapping(binding.get("selection_pins"))
    strategy_params = _mapping(params.get("strategy_params"))
    runtime_flags = _mapping(params.get("runtime_flags"))
    strategy_source = {**strategy_params, **params}
    factors = _factor_names(factor_names or params.get("factor_names") or [])
    effective_source = normalize_qe_run_source_type(
        source_type or provenance.get("created_by_type")
    )
    effective_name = created_by_name or provenance.get("created_by_name")
    effective_node = node_id or params.get("execution_node_id")
    execution_algo = params.get("execution_algo")
    frequency = params.get("frequency") or params.get("freq")
    if execution_algo and not frequency:
        frequency = "1min"

    registration: dict[str, Any] = {
        "schema_version": QE_RUN_REGISTRATION_SCHEMA,
        "run_kind": str(run_kind).strip().lower(),
        "consumer_id": normalize_qe_run_consumer_id(
            consumer_id
            if consumer_id is not None
            else provenance.get("consumer_id")
        ),
        "source_type": effective_source,
        "purpose": normalize_qe_run_purpose(purpose or provenance.get("purpose")),
        "factor_count": len(factors),
        "factor_digest": _digest_strings(factors),
    }
    optional = {
        "created_by_name": effective_name,
        "node_id": effective_node,
        "model_id": model_id,
        "strategy_id": strategy_id,
        "parent_id": parent_id,
        "task_id": task_id,
        "loop_index": loop_index,
        "random_seed": _first_present(params.get("random_seed"), params.get("seed")),
        "label_horizon": params.get("label_horizon"),
        "execution_algo": execution_algo,
        "execution_frequency": frequency,
        "dataset_release_id": _first_present(dataset.get("release_id"), binding.get("release_id")),
        "dataset_generation": _first_present(
            dataset.get("generation"), binding.get("profile_generation")
        ),
        "dataset_cutoff": _first_present(dataset.get("cutoff"), binding.get("cutoff")),
        "universe_mode": selection.get("mode"),
        "universe_pool_ids": list(selection.get("pool_ids") or []),
        "benchmark": selection.get("benchmark") or params.get("benchmark"),
    }
    registration.update(
        {key: value for key, value in optional.items() if value not in (None, "", [], {})}
    )
    split_summary = _portable_fields(
        dict(data_split or {}),
        ("train_start", "train_end", "valid_start", "valid_end", "test_start", "test_end"),
    )
    strategy_summary = _portable_fields(
        strategy_source,
        ("topk", "n_drop", "hold_thresh", "risk_degree"),
    )
    cost_summary = _portable_fields(
        strategy_source,
        ("open_cost", "close_cost", "min_cost", "impact_cost"),
    )
    execution_summary = _portable_fields(
        {**runtime_flags, **params},
        (
            "runtime_mode",
            "bar_freq",
            "backtest_freq",
            "filter_suspended_on_signal",
            "suspend_filter_strict",
            "limit_threshold",
        ),
    )
    for key, value in (
        ("data_split", split_summary),
        ("strategy_summary", strategy_summary),
        ("cost_summary", cost_summary),
        ("execution_contract_summary", execution_summary),
    ):
        if value:
            registration[key] = value
    return registration


def attach_qe_run_registration(
    custom_params: Mapping[str, Any] | None,
    **registration_kwargs: Any,
) -> dict[str, Any]:
    params = dict(custom_params or {})
    params[QE_RUN_REGISTRATION_PARAM] = build_qe_run_registration(
        custom_params=params,
        **registration_kwargs,
    )
    return params


def attach_qe_planned_loop_registration(
    config: Mapping[str, Any],
    *,
    run_kind: str,
    consumer_id: str | None = None,
    source_type: str,
    created_by_name: str | None,
    purpose: str,
    node_id: str | None,
    parent_id: str,
    task_id: str,
    loop_index: int,
) -> dict[str, Any]:
    """Attach one portable identity to both a planned config and its params."""

    loop_config = dict(config)
    params = attach_qe_run_registration(
        _mapping(loop_config.get("custom_params")),
        run_kind=run_kind,
        consumer_id=consumer_id,
        source_type=source_type,
        created_by_name=created_by_name,
        purpose=purpose,
        node_id=node_id,
        model_id=loop_config.get("model_id"),
        factor_names=(
            loop_config.get("factor_list")
            or loop_config.get("factor_names")
            or loop_config.get("factor_keys")
            or []
        ),
        strategy_id=loop_config.get("strategy_id"),
        data_split=_mapping(loop_config.get("data_split")),
        parent_id=parent_id,
        task_id=task_id,
        loop_index=loop_index,
    )
    loop_config["custom_params"] = params
    loop_config[QE_RUN_REGISTRATION_PARAM] = params[QE_RUN_REGISTRATION_PARAM]
    return loop_config


@dataclass(frozen=True)
class PlannedQELoop:
    loop_index: int
    node_id: str | None = None
    config: Mapping[str, Any] | None = None
    action_type: str | None = None


class QERunRegistry:
    """Thin adapter around the current QE control-plane tables."""

    def __init__(self, connection_factory: Callable[..., Any] = get_conn) -> None:
        self._get_conn = connection_factory

    def _transaction(self) -> Any:
        """Return an atomic DB context while retaining lightweight test doubles."""

        try:
            return self._get_conn(autocommit=False, manage_transaction=True)
        except TypeError:
            return self._get_conn()

    def reserve_single(
        self,
        *,
        experiment_id: str,
        experiment_name: str,
        workspace_path: str | None,
        factor_names: Sequence[str],
        model_id: str | None,
        strategy_id: str | None,
        data_split: Mapping[str, Any] | None,
        custom_params: Mapping[str, Any] | None,
        evolution_goal: str | None = None,
        llm_hypothesis: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not experiment_id:
            raise QERunRegistryError("qe_run_identity_missing", "experiment_id is required")
        expected_registration = _mapping(
            _mapping(custom_params).get(QE_RUN_REGISTRATION_PARAM)
        )
        if not expected_registration:
            raise QERunRegistryError(
                "qe_run_registration_missing",
                f"single reservation {experiment_id} lacks registration summary",
            )
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qe_experiments
                        (experiment_id, experiment_name, status,
                         factor_names, model_id, strategy_id,
                         data_split, custom_params, workspace_path,
                         evolution_goal, llm_hypothesis, created_at)
                    VALUES (%s, %s, 'created', %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (experiment_id) DO UPDATE SET
                        experiment_name = EXCLUDED.experiment_name,
                        factor_names = EXCLUDED.factor_names,
                        model_id = EXCLUDED.model_id,
                        strategy_id = EXCLUDED.strategy_id,
                        data_split = EXCLUDED.data_split,
                        custom_params = EXCLUDED.custom_params,
                        evolution_goal = EXCLUDED.evolution_goal,
                        llm_hypothesis = EXCLUDED.llm_hypothesis
                    WHERE qe_experiments.status = 'created'
                    """,
                    (
                        experiment_id,
                        experiment_name,
                        json.dumps(list(factor_names)),
                        model_id,
                        strategy_id,
                        json.dumps(dict(data_split or {})),
                        json.dumps(dict(custom_params or {})),
                        workspace_path,
                        evolution_goal,
                        json.dumps(dict(llm_hypothesis)) if llm_hypothesis else None,
                    ),
                )
                cur.execute(
                    "SELECT experiment_id, status, custom_params FROM qe_experiments WHERE experiment_id = %s",
                    (experiment_id,),
                )
                row = cur.fetchone()
                persisted_params = _mapping(row[2] if row and len(row) > 2 else None)
                if (
                    not row
                    or str(row[0]) != experiment_id
                    or str(row[1]) != "created"
                    or _mapping(persisted_params.get(QE_RUN_REGISTRATION_PARAM))
                    != expected_registration
                ):
                    raise QERunRegistryError(
                        "qe_run_reservation_readback_failed",
                        f"single reservation not readable for {experiment_id}",
                    )
        return {"experiment_id": str(row[0]), "status": str(row[1])}

    def reserve_task(
        self,
        *,
        task_id: str,
        base_experiment_id: str,
        task_kind: str,
        planned_loops: Sequence[PlannedQELoop],
        consumer_id: str | None = None,
        source_type: str = "scheduler",
        created_by_name: str | None = None,
        purpose: str = "research",
    ) -> None:
        if not planned_loops:
            raise QERunRegistryError(
                "qe_run_planned_loops_missing", f"task {task_id} has no planned loops"
            )
        expected = sorted({planned.loop_index for planned in planned_loops})
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT factor_names, model_id, strategy_id, custom_params, data_split
                    FROM qe_experiments
                    WHERE experiment_id = %s
                    FOR UPDATE
                    """,
                    (base_experiment_id,),
                )
                parent_row = cur.fetchone()
                if not parent_row:
                    raise QERunRegistryError(
                        "qe_run_parent_reservation_missing",
                        f"base experiment {base_experiment_id} is not durable",
                    )
                first_planned = planned_loops[0]
                first_config = dict(first_planned.config or {})
                parent_params = _mapping(parent_row[3])
                parent_params.update(_mapping(first_config.get("custom_params")))
                parent_registration = qe_registration_summary(
                    parent_params.get(QE_RUN_REGISTRATION_PARAM)
                )
                effective_consumer = normalize_qe_run_consumer_id(
                    consumer_id
                    if consumer_id is not None
                    else parent_registration.get("consumer_id")
                )
                registration = build_qe_run_registration(
                    run_kind=task_kind,
                    custom_params=parent_params,
                    consumer_id=effective_consumer,
                    source_type=source_type,
                    created_by_name=created_by_name,
                    purpose=purpose,
                    node_id=first_planned.node_id,
                    model_id=first_config.get("model_id") or parent_row[1],
                    factor_names=(
                        first_config.get("factor_list")
                        or first_config.get("factor_names")
                        or first_config.get("factor_keys")
                        or _factor_names(parent_row[0])
                    ),
                    strategy_id=first_config.get("strategy_id") or parent_row[2],
                    data_split=_mapping(first_config.get("data_split") or parent_row[4]),
                    parent_id=base_experiment_id,
                    task_id=task_id,
                )
                cur.execute(
                    """
                    UPDATE qe_evolution_tasks
                    SET strategy_evo_config = COALESCE(strategy_evo_config, '{}'::jsonb)
                        || jsonb_build_object(%s, %s::jsonb),
                        updated_at = NOW()
                    WHERE task_id = %s
                    """,
                    (QE_RUN_REGISTRATION_PARAM, json.dumps(registration), task_id),
                )
                if cur.rowcount != 1:
                    raise QERunRegistryError(
                        "qe_run_task_reservation_missing",
                        f"evolution task {task_id} is not durable",
                    )
                expected_loops: dict[int, tuple[dict[str, Any], str, str]] = {}
                for planned in planned_loops:
                    if planned.loop_index < 1:
                        raise QERunRegistryError(
                            "qe_run_loop_index_invalid",
                            f"task {task_id} loop index must be positive",
                        )
                    loop_id = f"{task_id}_Loop{planned.loop_index}"
                    loop_config = attach_qe_planned_loop_registration(
                        dict(planned.config or {}),
                        run_kind=f"{task_kind}_loop",
                        consumer_id=effective_consumer,
                        source_type=source_type,
                        created_by_name=created_by_name,
                        purpose=purpose,
                        node_id=planned.node_id,
                        parent_id=base_experiment_id,
                        task_id=task_id,
                        loop_index=planned.loop_index,
                    )
                    expected_loops[planned.loop_index] = (
                        _mapping(loop_config.get(QE_RUN_REGISTRATION_PARAM)),
                        str(planned.node_id or ""),
                        str(planned.action_type or "planned"),
                    )
                    cur.execute(
                        """
                        INSERT INTO qe_evolution_loops
                            (loop_id, task_id, loop_index, action_type, config_json, status, node_id)
                        VALUES (%s, %s, %s, %s, %s, 'pending', %s)
                        ON CONFLICT (task_id, loop_index) DO NOTHING
                        """,
                        (
                            loop_id,
                            task_id,
                            planned.loop_index,
                            planned.action_type or "planned",
                            json.dumps(loop_config),
                            planned.node_id,
                        ),
                    )
                cur.execute(
                    """
                    SELECT loop_index, status, config_json, node_id, action_type
                    FROM qe_evolution_loops
                    WHERE task_id = %s AND loop_index = ANY(%s)
                    ORDER BY loop_index
                    """,
                    (task_id, [planned.loop_index for planned in planned_loops]),
                )
                rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT task_id, strategy_evo_config
                    FROM qe_evolution_tasks
                    WHERE task_id = %s
                    """,
                    (task_id,),
                )
                task_row = cur.fetchone()
                observed = sorted({int(row[0]) for row in rows})
                observed_loops = {
                    int(row[0]): (
                        str(row[1] or ""),
                        _mapping(
                            _mapping(row[2] if len(row) > 2 else None).get(
                                QE_RUN_REGISTRATION_PARAM
                            )
                        ),
                        str(row[3] or "") if len(row) > 3 else "",
                        str(row[4] or "") if len(row) > 4 else "",
                    )
                    for row in rows
                }
                loop_identity_mismatch = any(
                    observed_loops.get(loop_index)
                    != ("pending", registration, node, action_type)
                    for loop_index, (registration, node, action_type) in expected_loops.items()
                )
                task_registration = _mapping(
                    _mapping(task_row[1] if task_row and len(task_row) > 1 else None).get(
                        QE_RUN_REGISTRATION_PARAM
                    )
                )
                if (
                    observed != expected
                    or not task_row
                    or task_registration != registration
                    or loop_identity_mismatch
                ):
                    raise QERunRegistryError(
                        "qe_run_task_reservation_readback_failed",
                        f"task {task_id} planned loops mismatch: expected={expected}, observed={observed}",
                    )

    def reserve_multi_alpha(
        self,
        *,
        parent_experiment_id: str,
        experiment_name: str,
        factor_names: Sequence[str],
        model_id: str | None,
        strategy_id: str | None,
        data_split: Mapping[str, Any] | None,
        custom_params: Mapping[str, Any] | None,
        multi_alpha_config: Mapping[str, Any],
        assignments: Sequence[Any],
        parent_multi_alpha_id: str | None = None,
    ) -> None:
        if not assignments:
            raise QERunRegistryError(
                "qe_run_multi_alpha_children_missing",
                f"multi-alpha parent {parent_experiment_id} has no planned groups",
            )
        expected_registration = _mapping(
            _mapping(custom_params).get(QE_RUN_REGISTRATION_PARAM)
        )
        if not expected_registration:
            raise QERunRegistryError(
                "qe_run_multi_alpha_registration_missing",
                f"multi-alpha parent {parent_experiment_id} lacks registration summary",
            )
        expected = sorted(
            (
                str(item.group.group_name),
                sorted(_factor_names(item.group.factor_names)),
                str(item.group.model_id or ""),
                str(item.node_id or ""),
                str(item.group.reuse_mode or "retrain"),
            )
            for item in assignments
        )
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qe_experiments
                        (experiment_id, experiment_name, status, factor_names, model_id,
                         strategy_id, data_split, custom_params, alpha_mode,
                         multi_alpha_config, parent_multi_alpha_id, created_at)
                    VALUES (%s, %s, 'created', %s, %s, %s, %s, %s,
                            'multi', %s::jsonb, %s, NOW())
                    ON CONFLICT (experiment_id) DO UPDATE SET
                        experiment_name = EXCLUDED.experiment_name,
                        factor_names = EXCLUDED.factor_names,
                        model_id = EXCLUDED.model_id,
                        strategy_id = EXCLUDED.strategy_id,
                        data_split = EXCLUDED.data_split,
                        custom_params = EXCLUDED.custom_params,
                        alpha_mode = 'multi',
                        multi_alpha_config = EXCLUDED.multi_alpha_config,
                        parent_multi_alpha_id = EXCLUDED.parent_multi_alpha_id
                    WHERE qe_experiments.status = 'created'
                    """,
                    (
                        parent_experiment_id,
                        experiment_name,
                        json.dumps(list(factor_names)),
                        model_id,
                        strategy_id,
                        json.dumps(dict(data_split or {})),
                        json.dumps(dict(custom_params or {})),
                        json.dumps(dict(multi_alpha_config)),
                        parent_multi_alpha_id,
                    ),
                )
                for assignment in assignments:
                    group = assignment.group
                    cur.execute(
                        """
                        INSERT INTO qe_multi_alpha_groups
                            (parent_experiment_id, group_name, factor_names, model_id,
                             dataset_type, model_params, compute_resource,
                             assigned_node_id, qe_loop_id, status,
                             model_source_experiment_id, model_source_group_name, reuse_mode)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, 'pending', %s, %s, %s)
                        ON CONFLICT (parent_experiment_id, group_name) DO UPDATE SET
                            factor_names = EXCLUDED.factor_names,
                            model_id = EXCLUDED.model_id,
                            dataset_type = EXCLUDED.dataset_type,
                            model_params = EXCLUDED.model_params,
                            compute_resource = EXCLUDED.compute_resource,
                            assigned_node_id = EXCLUDED.assigned_node_id,
                            model_source_experiment_id = EXCLUDED.model_source_experiment_id,
                            model_source_group_name = EXCLUDED.model_source_group_name,
                            reuse_mode = EXCLUDED.reuse_mode
                        WHERE qe_multi_alpha_groups.status = 'pending'
                        """,
                        (
                            parent_experiment_id,
                            group.group_name,
                            json.dumps(list(group.factor_names)),
                            group.model_id,
                            group.dataset_type,
                            json.dumps(group.model_params) if group.model_params else None,
                            group.compute_resource,
                            assignment.node_id,
                            group.model_source_experiment_id,
                            group.model_source_group_name,
                            group.reuse_mode or "retrain",
                        ),
                    )
                cur.execute(
                    "SELECT experiment_id, status, custom_params FROM qe_experiments WHERE experiment_id = %s",
                    (parent_experiment_id,),
                )
                parent_row = cur.fetchone()
                cur.execute(
                    """
                    SELECT group_name, factor_names, model_id, assigned_node_id, reuse_mode
                    FROM qe_multi_alpha_groups
                    WHERE parent_experiment_id = %s
                    """,
                    (parent_experiment_id,),
                )
                child_rows = cur.fetchall()
                observed = sorted(
                    (
                        str(row[0]),
                        sorted(_factor_names(row[1])),
                        str(row[2] or ""),
                        str(row[3] or ""),
                        str(row[4] or "retrain"),
                    )
                    for row in child_rows
                )
                parent_params = _mapping(
                    parent_row[2] if parent_row and len(parent_row) > 2 else None
                )
                if (
                    not parent_row
                    or str(parent_row[1]) != "created"
                    or _mapping(parent_params.get(QE_RUN_REGISTRATION_PARAM))
                    != expected_registration
                    or observed != expected
                ):
                    raise QERunRegistryError(
                        "qe_run_multi_alpha_reservation_readback_failed",
                        f"parent={parent_experiment_id}, expected_groups={expected}, observed_groups={observed}",
                    )

    def reserve_durable(
        self,
        *,
        run_id: str,
        reserve: Callable[[], Mapping[str, Any]],
        readback: Callable[[str], Mapping[str, Any] | None],
    ) -> dict[str, Any]:
        """Apply the same reservation/readback invariant to durable tables.

        The durable repository keeps ownership of its existing transaction,
        task/run schema, and identity-conflict checks. This adapter only makes
        the pre-dispatch readback contract explicit without duplicating those
        tables in ``qe_experiments``.
        """

        created = dict(reserve() or {})
        if str(created.get("id") or "") != run_id:
            raise QERunRegistryError(
                "qe_run_durable_reservation_identity_mismatch",
                f"durable reservation returned a different identity for {run_id}",
            )
        persisted = readback(run_id)
        if not persisted or str(persisted.get("id") or "") != run_id:
            raise QERunRegistryError(
                "qe_run_durable_reservation_readback_failed",
                f"durable run is not readable before dispatch: {run_id}",
            )
        status = str(persisted.get("status") or "")
        if status not in {"queued", "preparing", "running", "pause_requested", "paused"}:
            raise QERunRegistryError(
                "qe_run_durable_reservation_status_invalid",
                f"durable run {run_id} has non-dispatchable status {status!r}",
            )
        return dict(persisted)

    def mark_dispatched(self, *, experiment_id: str) -> bool:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_experiments
                    SET status = 'pending', updated_at = NOW()
                    WHERE experiment_id = %s AND status = 'created'
                    """,
                    (experiment_id,),
                )
                changed = cur.rowcount == 1
        return changed

    def apply_transition(
        self,
        *,
        experiment_id: str,
        from_statuses: Sequence[str],
        to_status: str,
    ) -> bool:
        if not from_statuses:
            raise QERunRegistryError(
                "qe_run_transition_source_missing", "from_statuses cannot be empty"
            )
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_experiments
                    SET status = %s, updated_at = NOW()
                    WHERE experiment_id = %s AND status = ANY(%s)
                    """,
                    (to_status, experiment_id, list(from_statuses)),
                )
                changed = cur.rowcount == 1
        return changed

    def project_history(self, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Add small registration/progress summaries without mutating state."""

        projected = [dict(row) for row in rows]
        experiment_ids = [str(row.get("experiment_id")) for row in projected if row.get("experiment_id")]
        task_ids = [str(row.get("qe_task_id")) for row in projected if row.get("qe_task_id")]
        if not experiment_ids and not task_ids:
            return projected
        has_registered_run = any(
            QE_RUN_REGISTRATION_PARAM in _mapping(row.get("custom_params"))
            for row in projected
        )
        if not has_registered_run:
            # Batch A does not backfill historical rows.  Avoid extra aggregate
            # reads for legacy history that has no canonical registration.
            return projected

        progress: dict[str, dict[str, Any]] = {}
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT t.task_id, t.base_experiment_id, t.max_loops, t.current_loop,
                           t.status AS task_status, l.status AS loop_status, COUNT(l.loop_id) AS count
                    FROM qe_evolution_tasks t
                    LEFT JOIN qe_evolution_loops l ON l.task_id = t.task_id
                    WHERE t.task_id = ANY(%s) OR t.base_experiment_id = ANY(%s)
                    GROUP BY t.task_id, t.base_experiment_id, t.max_loops,
                             t.current_loop, t.status, l.status
                    """,
                    (list(set(task_ids + experiment_ids)) or [""], experiment_ids or [""]),
                )
                evolution_rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT task_id, base_experiment_id, strategy_evo_config
                    FROM qe_evolution_tasks
                    WHERE task_id = ANY(%s) OR base_experiment_id = ANY(%s)
                    """,
                    (list(set(task_ids + experiment_ids)) or [""], experiment_ids or [""]),
                )
                task_registration_rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT parent_experiment_id, status, COUNT(*) AS count
                    FROM qe_multi_alpha_groups
                    WHERE parent_experiment_id = ANY(%s)
                    GROUP BY parent_experiment_id, status
                    """,
                    (experiment_ids or [""],),
                )
                multi_rows = cur.fetchall()

        task_registrations: dict[str, dict[str, Any]] = {}
        for row in task_registration_rows:
            registration = qe_registration_summary(
                _mapping(row.get("strategy_evo_config")).get(QE_RUN_REGISTRATION_PARAM)
            )
            if not registration:
                continue
            task_registrations[str(row["task_id"])] = registration
            if row.get("base_experiment_id"):
                task_registrations[str(row["base_experiment_id"])] = registration

        for row in evolution_rows:
            key = str(row["base_experiment_id"] or row["task_id"])
            summary = progress.setdefault(
                key,
                {
                    "kind": "evolution",
                    "task_id": row["task_id"],
                    "total": int(row["max_loops"] or 0),
                    "current": int(row["current_loop"] or 0),
                    "status": canonical_qe_status(row["task_status"]),
                    "counts": {},
                },
            )
            if row.get("loop_status"):
                status = canonical_qe_status(row["loop_status"])
                summary["counts"][status] = (
                    int(summary["counts"].get(status) or 0) + int(row["count"] or 0)
                )
        for row in multi_rows:
            key = str(row["parent_experiment_id"])
            summary = progress.setdefault(
                key,
                {"kind": "multi_alpha", "total": 0, "counts": {}},
            )
            count = int(row["count"] or 0)
            status = canonical_qe_status(row["status"])
            summary["counts"][status] = int(summary["counts"].get(status) or 0) + count
            summary["total"] += count

        for item in projected:
            params = _mapping(item.get("custom_params"))
            registration = qe_registration_summary(params.get(QE_RUN_REGISTRATION_PARAM))
            if not item.get("is_evolution_loop"):
                registration = (
                    task_registrations.get(str(item.get("qe_task_id") or ""))
                    or task_registrations.get(str(item.get("experiment_id") or ""))
                    or registration
                )
            if registration:
                item["registration_summary"] = registration
                item["canonical_status"] = canonical_qe_status(item.get("status"))
            progress_key = str(item.get("experiment_id") or "")
            if progress_key in progress:
                item["progress_summary"] = progress[progress_key]
            elif item.get("qe_task_id"):
                parent_progress = next(
                    (
                        summary
                        for summary in progress.values()
                        if summary.get("task_id") == item.get("qe_task_id")
                    ),
                    None,
                )
                if parent_progress:
                    item["progress_summary"] = parent_progress
        return projected


def canonical_status_counts(statuses: Iterable[str]) -> dict[str, int]:
    """Pure helper used by API/UI contract tests."""

    counts = Counter(canonical_qe_status(status) for status in statuses)
    return dict(sorted(counts.items()))
