import pytest

from backend.services.quantevolver.node_execution import (
    QENodePreflightError,
    normalize_node_parallelism,
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
