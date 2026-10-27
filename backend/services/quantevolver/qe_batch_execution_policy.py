"""Versioned placement and workload limits for new QE custom-evolution batches.

Limits apply to whole Loops: a full-fit Loop holds its slot through backtesting.
They do not claim independent CPU-training/backtest phase pipelining.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
import json
from typing import Any

from .node_execution import QENodePreflightError, normalize_node_parallelism

QE_BATCH_EXECUTION_POLICY = "qe_model_workload_v2"
LEGACY_BATCH_EXECUTION_POLICY = "qe_balanced_workload_v1"
QE_TRAINING_LOOP_CAPACITY = 2
QE_CPU_TRAINING_LOOP_CAPACITY = 4
QE_BACKTEST_LOOP_CAPACITY = 4
POLICY_KEY = "_qe_batch_execution_policy"
MODEL_BINDING_KEY = "_qe_model_execution"
CPU_NODE = "rdagent-node1"
GPU_NODE = "wsl2-5080"
DATASET_FIELDS = (
    "deployment_snapshot_id", "dataset_manifest_sha256", "cutoff_trade_date",
    "qlib_calendar_sha256", "qlib_instruments_sha256",
    "st_pit_snapshot_id", "st_pit_manifest_sha256",
)


def _object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, Mapping):
        raise ValueError("QE_BATCH_MODEL_METADATA_INVALID")
    return dict(value)


def loop_compute_kind(loop: Mapping[str, Any], model_info: Mapping[str, Any] | None) -> str:
    """Classify the model that actually executes; replay never runs a model."""
    is_pure_backtest(loop)  # Reject ambiguous reuse modes.
    if loop.get("prediction_replay"):
        return "prediction_replay"
    if not model_info:
        raise ValueError("QE_BATCH_MODEL_METADATA_REQUIRED: " + str(loop.get("model_id")))
    model_type = str(model_info.get("model_type") or "").upper()
    custom_nn = bool(model_info.get("code_text"))
    neural = custom_nn or "NN" in model_type or "GAT" in model_type
    cpu_family = any(t in model_type for t in (
        "LGB", "GBDT", "XGB", "CATBOOST", "LINEAR", "RIDGE", "LASSO", "LAMBDARANK", "LAMBDAMART",
    ))
    tabpfn = "TABPFN" in model_type
    if not (neural or cpu_family or tabpfn):
        raise ValueError("QE_BATCH_MODEL_DEVICE_UNKNOWN: " + str(loop.get("model_id")))
    # Match the composer's supported device selectors, without changing them.
    params = _object(model_info.get("model_hyperparameters"))
    training = _object(model_info.get("model_training_hyperparameters"))
    if neural and not custom_nn:
        params = {**training, **params}
    params.update(_object(loop.get("model_params")))
    if neural:
        return "gpu" if int(params.get("GPU", 0)) >= 0 else "cpu"
    selector = (
        params.get("device_type", "cpu") if "LGB" in model_type or "GBDT" in model_type
        else params.get("task_type", "CPU") if "CATBOOST" in model_type
        else params.get("device", "cuda" if tabpfn else "cpu")
    )
    device = str(selector).lower()
    if device in {"cpu", "cpu:0"}:
        return "gpu" if params.get("tree_method") == "gpu_hist" else "cpu"
    if device == "gpu" or device == "cuda" or device.startswith("cuda:"):
        return "gpu"
    raise ValueError("QE_BATCH_MODEL_DEVICE_UNKNOWN: " + device)


def bind_batch_model_nodes(
    loops: Sequence[Mapping[str, Any]], *, available_nodes: Sequence[str],
    balanced: bool, preferred_node: str | None = None, model_lookup: Any = None,
) -> list[dict[str, Any]]:
    """GPU train/inference stays on WSL; CPU models stay on the remote node.

    Immutable prediction replay is CPU work without model execution and can
    use either node. Explicit placement cannot override model compatibility.
    """
    if model_lookup is None:
        from .config_composer import ConfigComposer
        model_lookup = ConfigComposer()._get_model_info
    result = [dict(loop) for loop in loops]
    cache: dict[str, Any] = {}
    for loop in result:
        model_id = str(loop.get("model_id") or "")
        if not loop.get("prediction_replay") and model_id not in cache:
            cache[model_id] = model_lookup(model_id)
        kind = loop_compute_kind(loop, cache.get(model_id))
        required = {"cpu": CPU_NODE, "gpu": GPU_NODE}.get(kind)
        requested = loop.get("node_id") or preferred_node
        if balanced:
            loop["node_id"] = required
        elif required and requested != required:
            raise ValueError(f"QE_BATCH_MODEL_NODE_MISMATCH: {model_id} requires {required}, got {requested}")
        else:
            loop["node_id"] = requested
        if required and required not in available_nodes:
            raise ValueError("QE_BATCH_MODEL_NODE_NOT_BOUND: " + required)
        binding = {"model_id": model_id, "compute_kind": kind}
        previous = loop.get(MODEL_BINDING_KEY)
        if previous is not None and dict(previous) != binding:
            raise ValueError("QE_BATCH_MODEL_BINDING_CHANGED: " + model_id)
        loop[MODEL_BINDING_KEY] = binding
    if balanced:
        result = place_batch_loops(result, available_nodes=available_nodes)
    return result


def is_pure_backtest(loop: Mapping[str, Any]) -> bool:
    if loop.get("backtest_only") and loop.get("prediction_replay"):
        raise ValueError("QE_BATCH_WORKLOAD_AMBIGUOUS: choose one reuse mode")
    if loop.get("frozen_inference") is not None:
        if not loop.get("backtest_only") or loop.get("prediction_replay"):
            raise ValueError("QE_FROZEN_MODE_INVALID")
        return False  # A real model executes; preserve CPU/GPU affinity and whole-Loop limits.
    return bool(loop.get("backtest_only") or loop.get("prediction_replay"))


def batch_node_parallelism(
    loops: Sequence[Mapping[str, Any]],
    requested: Mapping[str, int] | None = None,
) -> dict[str, int]:
    """Remote CPU fits: 4; WSL GPU fits: 2; pure backtests: 4."""
    nodes = {str(loop["node_id"]) for loop in loops}
    defaults = {
        node: (QE_BACKTEST_LOOP_CAPACITY if all(
            is_pure_backtest(loop) for loop in loops if str(loop["node_id"]) == node
        ) else (QE_CPU_TRAINING_LOOP_CAPACITY if node == CPU_NODE else QE_TRAINING_LOOP_CAPACITY))
        for node in nodes
    }
    result = normalize_node_parallelism(nodes, {**defaults, **dict(requested or {})})
    for node, limit in result.items():
        if limit > defaults[node]:
            raise QENodePreflightError(
                "QE_BATCH_WORKLOAD_CAPACITY_EXCEEDED",
                "QE batch exceeds this node's model/workload capacity.",
                {"node_id": node, "requested": limit, "maximum": defaults[node]},
            )
    return result


def place_batch_loops(
    loops: Sequence[Mapping[str, Any]],
    *,
    available_nodes: Sequence[str],
    preferred_node: str | None = None,
    rebalance_single_node: bool = False,
) -> list[dict[str, Any]]:
    """Balance unpinned new Loops, keeping seed cohorts together when possible.

    A loop-level node is an explicit pin. A task-level node is a preference for
    automatic placement. Caller must validate all selected nodes before saving.
    """
    nodes = sorted({str(node).strip() for node in available_nodes if str(node).strip()})
    if preferred_node:
        if preferred_node not in nodes:
            raise ValueError("QE_BATCH_PREFERRED_NODE_NOT_BOUND: " + preferred_node)
        nodes.remove(preferred_node)
        nodes.insert(0, preferred_node)
    if not nodes:
        raise ValueError("QE_BATCH_ACTIVE_NODE_BINDINGS_REQUIRED")
    result = [dict(loop) for loop in loops]
    for loop in result:
        pin = str(loop.get("node_id") or "").strip()
        if pin and pin not in nodes:
            raise ValueError("QE_BATCH_PINNED_NODE_NOT_BOUND: " + pin)
        loop["node_id"] = pin or None
    pins = {loop["node_id"] for loop in result if loop["node_id"]}
    if rebalance_single_node and len(result) > 1 and len(nodes) > 1 and len(pins) == 1:
        # Older UI/templates fill every Loop with the same default node. In
        # balanced mode those defaults must not silently disable distribution.
        if not preferred_node:
            first = next(iter(pins))
            nodes.remove(first)
            nodes.insert(0, first)
        for loop in result:
            loop["node_id"] = None
    load = Counter(str(loop["node_id"]) for loop in result if loop.get("node_id"))
    groups: dict[Any, list[dict[str, Any]]] = {}
    for position, loop in enumerate(result):
        seed = (loop.get("runtime_flags") or {}).get("random_seed")
        key = ("seed", seed) if seed is not None else ("loop", position)
        groups.setdefault(key, []).append(loop)
    # A single seed must not leave the other node permanently unused.
    cohorts = list(groups.values()) if len(groups) >= len(nodes) else [[loop] for loop in result]
    for cohort in cohorts:
        pins = {str(loop["node_id"]) for loop in cohort if loop.get("node_id")}
        node = next(iter(pins)) if len(pins) == 1 else min(nodes, key=lambda n: load[n])
        for loop in cohort:
            if not loop["node_id"]:
                loop["node_id"] = node
                load[node] += 1
    return result


async def verify_batch_dataset_nodes(profile: Any, node_ids: Sequence[str]) -> dict[str, str]:
    """Read node-owned identities, compare to active authority and to each other."""
    from .qe_workspace_client import QEWorkspaceClient

    expected_manifest = str(profile.raw["components"]["dataset_manifest_sha256"])
    shared: dict[str, str] | None = None
    for node in sorted(set(node_ids)):
        root = str(profile.raw["node_bindings"][node]["candidate_root"])
        async with QEWorkspaceClient.for_node(node) as client:
            report = await client.get_dataset_identity(node_id=node, data_root_uri=root)
        dataset = report.dataset
        if not report.complete or not dataset:
            raise ValueError(f"QE_BATCH_DATASET_IDENTITY_INCOMPLETE: {node}")
        if dataset.get("resolved_node_id") != node or dataset.get("resolved_data_root_uri") != root:
            raise ValueError(f"QE_BATCH_DATASET_LOCATION_MISMATCH: {node}")
        if dataset.get("dataset_manifest_sha256") != expected_manifest:
            raise ValueError(f"QE_BATCH_ACTIVE_DATASET_MISMATCH: {node}")
        identity = {field: str(dataset.get(field) or "") for field in DATASET_FIELDS}
        if not all(identity.values()):
            raise ValueError(f"QE_BATCH_DATASET_IDENTITY_INCOMPLETE: {node}")
        if shared is not None and identity != shared:
            raise ValueError(f"QE_BATCH_CROSS_NODE_DATASET_MISMATCH: {node}")
        shared = identity
    if shared is None:
        raise ValueError("QE_BATCH_ACTIVE_NODE_BINDINGS_REQUIRED")
    return shared


def strategy_node_parallelism(
    loops: Sequence[Mapping[str, Any]], requested: Mapping[str, int] | None,
) -> dict[str, int]:
    """Enforce versioned new batches on every path; leave legacy bindings intact."""
    policies = [loop.get(POLICY_KEY) for loop in loops]
    if any(policies):
        versions = {p.get("version") for p in policies if isinstance(p, Mapping)}
        if not all(isinstance(p, Mapping) for p in policies if p) or not versions <= {
            QE_BATCH_EXECUTION_POLICY, LEGACY_BATCH_EXECUTION_POLICY,
        } or len(versions) != 1:
            raise ValueError("QE_BATCH_EXECUTION_POLICY_INVALID")
        if versions == {LEGACY_BATCH_EXECUTION_POLICY}:
            # V1 was frozen with a two-fit cap on both nodes; do not migrate it.
            defaults = {str(loop["node_id"]): (
                2 if any(not is_pure_backtest(other) for other in loops if other["node_id"] == loop["node_id"]) else 4
            ) for loop in loops}
            limits = batch_node_parallelism(loops, {**defaults, **dict(requested or {})})
            for node, limit in limits.items():
                if any(not is_pure_backtest(loop) for loop in loops if loop["node_id"] == node) and limit > 2:
                    raise ValueError("QE_BATCH_LEGACY_CAPACITY_EXCEEDED")
            return limits
        for loop in loops:
            binding = loop.get(MODEL_BINDING_KEY) or {}
            kind = binding.get("compute_kind")
            required = {"cpu": CPU_NODE, "gpu": GPU_NODE}.get(kind)
            if binding.get("model_id") != loop.get("model_id") or kind not in {"cpu", "gpu", "prediction_replay"}:
                raise ValueError("QE_BATCH_MODEL_BINDING_REQUIRED")
            if required and loop.get("node_id") != required:
                raise ValueError("QE_BATCH_MODEL_NODE_MISMATCH")
        return batch_node_parallelism(loops, requested)
    return normalize_node_parallelism({str(loop["node_id"]) for loop in loops}, requested)
