import asyncio

import pytest

from backend.services.quantevolver.node_execution import (
    QENodePreflightError,
    normalize_node_parallelism,
    preflight_qe_node,
    resolve_custom_loop_nodes,
)


def test_custom_loop_nodes_inherit_loop1_node():
    loops, loop1_node, selected = resolve_custom_loop_nodes(
        [
            {"loop_index": 1, "node_id": "node-a"},
            {"loop_index": 2, "node_id": ""},
            {"loop_index": 3, "node_id": "node-b"},
        ],
        request_node_id=None,
    )

    assert loop1_node == "node-a"
    assert [loop["node_id"] for loop in loops] == ["node-a", "node-a", "node-b"]
    assert selected == {"node-a", "node-b"}


def test_custom_loop_nodes_use_request_node_for_blank_loop1():
    loops, loop1_node, selected = resolve_custom_loop_nodes(
        [
            {"loop_index": 1, "node_id": ""},
            {"loop_index": 2, "node_id": ""},
        ],
        request_node_id="manual-node",
    )

    assert loop1_node == "manual-node"
    assert [loop["node_id"] for loop in loops] == ["manual-node", "manual-node"]
    assert selected == {"manual-node"}


def test_node_parallelism_defaults_and_caps_selected_nodes_only():
    assert normalize_node_parallelism({"node-a", "node-b"}, {"node-b": 4}) == {
        "node-a": 1,
        "node-b": 4,
    }

    with pytest.raises(QENodePreflightError) as out_of_range:
        normalize_node_parallelism({"node-a"}, {"node-a": 5})
    assert out_of_range.value.error_code == "QE_NODE_PARALLELISM_OUT_OF_RANGE"

    with pytest.raises(QENodePreflightError) as unknown:
        normalize_node_parallelism({"node-a"}, {"node-b": 1})
    assert unknown.value.error_code == "QE_NODE_PARALLELISM_UNKNOWN_NODE"


def test_preflight_qe_node_merges_workspace_config_and_preserves_ssh_user(monkeypatch):
    node_row = {
        "node_id": "rdagent-node1",
        "api_base_url": "http://192.168.50.215:9000",
        "status": "online",
        "ssh_user": "lc999",
        "workspace_base": None,
        "factor_data_dir": None,
        "qlib_data_path": None,
        "qlib_minute_path": None,
        "qlib_rdagent_root": None,
    }
    workspace_config = {
        "workspace_base": "/home/lc999/projects/RD-Agent-main/qe_workspace",
        "factor_data_dir": "/home/lc999/data/factor_data",
        "qlib_data_path": "/home/lc999/data/qlib_bin",
        "qlib_minute_path": "/home/lc999/data/qlib_minute_bin",
        "qlib_rdagent_root": "/home/lc999/projects/RD-Agent-main",
    }

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

        async def get_workspace_config(self):
            return dict(workspace_config)

    monkeypatch.setattr(
        "backend.services.quantevolver.node_execution.get_compute_node",
        lambda node_id: dict(node_row),
    )
    monkeypatch.setattr(
        "backend.services.quantevolver.node_execution.QEWorkspaceClient.for_node",
        staticmethod(lambda node_id: FakeClient()),
    )

    node = asyncio.run(preflight_qe_node("rdagent-node1"))

    assert node["ssh_user"] == "lc999"
    for key, value in workspace_config.items():
        assert node[key] == value
    assert node["workspace_config"] == workspace_config
