"""Versioned placement and workload limits for new QE custom-evolution batches.

Limits apply to whole Loops: a full-fit Loop holds its slot through backtesting.
They do not claim independent CPU-training/backtest phase pipelining.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from .node_execution import QENodePreflightError, normalize_node_parallelism

QE_BATCH_EXECUTION_POLICY = "qe_balanced_workload_v1"
QE_TRAINING_LOOP_CAPACITY = 2
QE_BACKTEST_LOOP_CAPACITY = 4
POLICY_KEY = "_qe_batch_execution_policy"
DATASET_FIELDS = (
    "deployment_snapshot_id", "dataset_manifest_sha256", "cutoff_trade_date",
    "qlib_calendar_sha256", "qlib_instruments_sha256",
    "st_pit_snapshot_id", "st_pit_manifest_sha256",
)


def is_pure_backtest(loop: Mapping[str, Any]) -> bool:
    if loop.get("backtest_only") and loop.get("prediction_replay"):
        raise ValueError("QE_BATCH_WORKLOAD_AMBIGUOUS: choose one reuse mode")
    return bool(loop.get("backtest_only") or loop.get("prediction_replay"))


def batch_node_parallelism(
    loops: Sequence[Mapping[str, Any]],
    requested: Mapping[str, int] | None = None,
) -> dict[str, int]:
    """Default to 2 full-fit / 4 pure-backtest Loops; explicit reductions survive."""
    nodes = {str(loop["node_id"]) for loop in loops}
    defaults = {
        node: (QE_BACKTEST_LOOP_CAPACITY if all(
            is_pure_backtest(loop) for loop in loops if str(loop["node_id"]) == node
        ) else QE_TRAINING_LOOP_CAPACITY)
        for node in nodes
    }
    result = normalize_node_parallelism(nodes, {**defaults, **dict(requested or {})})
    for node, limit in result.items():
        if limit > defaults[node]:
            raise QENodePreflightError(
                "QE_BATCH_WORKLOAD_CAPACITY_EXCEEDED",
                "Full-training batches allow at most two whole Loops per node.",
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
        if not all(isinstance(p, Mapping) and p.get("version") == QE_BATCH_EXECUTION_POLICY for p in policies if p):
            raise ValueError("QE_BATCH_EXECUTION_POLICY_INVALID")
        return batch_node_parallelism(loops, requested)
    return normalize_node_parallelism({str(loop["node_id"]) for loop in loops}, requested)
