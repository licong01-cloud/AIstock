"""
Multi-Alpha Resource Planner: assign AlphaGroups to compute nodes.

Execution modes:
  serial         — all groups on single node, sequential
  local_parallel — GPU groups serial on primary GPU, CPU groups parallel
  distributed    — split across wsl2-5080 + rdagent-node1
"""
from __future__ import annotations

import logging
from typing import Any

from .experiment_config import AlphaGroup

logger = logging.getLogger("aistock.quantevolver.multi_alpha_resource_planner")


class GroupAssignment:
    """Assignment of an AlphaGroup to a compute node."""

    def __init__(self, group: AlphaGroup, node_id: str, order: int):
        self.group = group
        self.node_id = node_id
        self.order = order  # execution order (for serial/parallel scheduling)

    def __repr__(self):
        return (f"GroupAssignment(group={self.group.group_name}, "
                f"node={self.node_id}, order={self.order})")


def plan_assignments(
    groups: list[AlphaGroup],
    execution_mode: str,
    available_nodes: list[dict[str, Any]] | None = None,
    default_node_id: str = "wsl2-5080",
) -> list[GroupAssignment]:
    """Plan which node runs each group.

    Args:
        groups: list of AlphaGroup configs
        execution_mode: "serial" / "local_parallel" / "distributed"
        available_nodes: list of node dicts from infra.compute_nodes
                         (keys: node_id, gpu_model, gpu_vram_mb, status)
        default_node_id: fallback node

    Returns:
        list of GroupAssignment, ordered by execution sequence
    """
    if execution_mode == "serial":
        return _plan_serial(groups, default_node_id)
    elif execution_mode == "local_parallel":
        return _plan_local_parallel(groups, default_node_id)
    elif execution_mode == "distributed":
        return _plan_distributed(groups, available_nodes, default_node_id)
    else:
        logger.warning(f"Unknown execution_mode '{execution_mode}', falling back to serial")
        return _plan_serial(groups, default_node_id)


def _plan_serial(groups: list[AlphaGroup], node_id: str) -> list[GroupAssignment]:
    """All groups on single node, sequential. GPU groups first."""
    gpu_groups = [g for g in groups if g.compute_resource == "gpu"]
    cpu_groups = [g for g in groups if g.compute_resource != "gpu"]
    ordered = gpu_groups + cpu_groups

    assignments = []
    for i, g in enumerate(ordered):
        # 用户 preferred_node_id 优先
        target = g.preferred_node_id or node_id
        assignments.append(GroupAssignment(group=g, node_id=target, order=i))
    return assignments


def _plan_local_parallel(groups: list[AlphaGroup], node_id: str) -> list[GroupAssignment]:
    """GPU groups serial (share 1 GPU), CPU groups all parallel.

    Order encoding:
      GPU groups: order=0,1,2... (must be sequential)
      CPU groups: order=100,100,100... (same order = can run in parallel)
    """
    assignments = []
    gpu_order = 0
    for g in groups:
        target = g.preferred_node_id or node_id
        if g.compute_resource == "gpu":
            assignments.append(GroupAssignment(group=g, node_id=target, order=gpu_order))
            gpu_order += 1
        else:
            assignments.append(GroupAssignment(group=g, node_id=target, order=100))
    return assignments


def _plan_distributed(
    groups: list[AlphaGroup],
    available_nodes: list[dict[str, Any]] | None,
    default_node_id: str,
) -> list[GroupAssignment]:
    """Distribute groups across available nodes.

    Strategy:
    - GPU groups → node with most VRAM (typically wsl2-5080 RTX 5080 16GB)
    - CPU groups → split round-robin across all online nodes
    - Respect preferred_node_id when set
    """
    if not available_nodes:
        # No node info → fall back to local_parallel
        logger.info("No available_nodes provided, falling back to local_parallel")
        return _plan_local_parallel(groups, default_node_id)

    online_nodes = [n for n in available_nodes if n.get("status") == "online"]
    if not online_nodes:
        online_nodes = available_nodes  # use all if none are online

    # Sort by GPU VRAM desc for GPU assignment
    gpu_nodes = sorted(
        [n for n in online_nodes if n.get("gpu_vram_mb", 0) > 0],
        key=lambda n: n.get("gpu_vram_mb", 0),
        reverse=True,
    )
    primary_gpu_node = gpu_nodes[0]["node_id"] if gpu_nodes else default_node_id

    # CPU round-robin
    cpu_node_ids = [n["node_id"] for n in online_nodes]
    if not cpu_node_ids:
        cpu_node_ids = [default_node_id]

    assignments = []
    gpu_order = 0
    cpu_idx = 0

    for g in groups:
        # Preferred node override
        if g.preferred_node_id:
            target = g.preferred_node_id
        elif g.compute_resource == "gpu":
            target = primary_gpu_node
        else:
            target = cpu_node_ids[cpu_idx % len(cpu_node_ids)]
            cpu_idx += 1

        if g.compute_resource == "gpu":
            order = gpu_order
            gpu_order += 1
        else:
            order = 100  # parallel

        assignments.append(GroupAssignment(group=g, node_id=target, order=order))

    return assignments
