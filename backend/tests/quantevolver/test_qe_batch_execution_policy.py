from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from backend.services.quantevolver import qe_batch_execution_policy as policy
from backend.services.quantevolver.node_execution import QENodePreflightError


NODES = ["rdagent-node1", "wsl2-5080"]


def loop(seed=42, node=None, **kwargs):
    return {"runtime_flags": {"random_seed": seed}, "node_id": node, **kwargs}


def test_nine_loops_use_both_nodes_and_keep_each_seed_pair_together():
    source = [loop(seed, factor_keys=[arm]) for seed in (42, 2024, 2026) for arm in ("B", "R8", "R7")]
    result = policy.place_batch_loops(source, available_nodes=NODES)
    assert {item["node_id"] for item in result} == set(NODES)
    for seed in (42, 2024, 2026):
        assert len({item["node_id"] for item in result if item["runtime_flags"]["random_seed"] == seed}) == 1
    assert [item["factor_keys"] for item in result] == [item["factor_keys"] for item in source]
    assert all(item["node_id"] is None for item in source)
    assert policy.batch_node_parallelism(result) == {NODES[0]: 4, NODES[1]: 2}


def test_single_seed_still_uses_two_nodes_and_explicit_pins_survive():
    result = policy.place_batch_loops([loop(node=NODES[1]), loop(), loop(), loop()], available_nodes=NODES)
    assert result[0]["node_id"] == NODES[1]
    assert {item["node_id"] for item in result} == set(NODES)
    assert [item["node_id"] for item in result].count(NODES[0]) == 2


@pytest.mark.parametrize("mode", ["prediction_replay", "backtest_only"])
def test_pure_backtests_default_to_four_on_each_node(mode):
    loops = [loop(node=node, **{mode: True}) for node in NODES]
    assert policy.batch_node_parallelism(loops) == dict.fromkeys(NODES, 4)
    assert policy.batch_node_parallelism(loops, {NODES[0]: 2}) == {NODES[0]: 2, NODES[1]: 4}


def test_mixed_workload_is_conservative_per_node_and_excess_is_rejected():
    loops = [loop(node=NODES[0]), loop(node=NODES[0], prediction_replay=True), loop(node=NODES[1], prediction_replay=True)]
    assert policy.batch_node_parallelism(loops) == {NODES[0]: 4, NODES[1]: 4}
    with pytest.raises(QENodePreflightError):
        policy.batch_node_parallelism([loop(node=NODES[1])], {NODES[1]: 4})
    with pytest.raises(QENodePreflightError):
        policy.batch_node_parallelism(loops, {"unselected-node": 1})
    with pytest.raises(ValueError, match="AMBIGUOUS"):
        policy.batch_node_parallelism([loop(node=NODES[0], backtest_only=True, prediction_replay=True)])


def test_restart_and_retry_enforce_new_policy_without_rewriting_legacy_tasks():
    loops = [loop(node=NODES[0], **{policy.POLICY_KEY: {"version": policy.LEGACY_BATCH_EXECUTION_POLICY}})]
    assert policy.strategy_node_parallelism(loops, None) == {NODES[0]: 2}
    with pytest.raises(ValueError):
        policy.strategy_node_parallelism(loops, {NODES[0]: 4})
    legacy = [loop(node=NODES[0])]
    assert policy.strategy_node_parallelism(legacy, {NODES[0]: 4}) == {NODES[0]: 4}
    # Appending an unmarked loop to a governed batch still enforces the cap.
    assert policy.strategy_node_parallelism(loops + legacy, None) == {NODES[0]: 2}


def _model(model_id):
    return {"model_type": "PTNN" if model_id == "gpu" else "LGBModel"} if model_id != "missing" else None


def test_model_affinity_precedes_seed_cohort_and_old_prefilled_defaults():
    source = [loop(seed, node=NODES[0], model_id=kind) for seed in (42, 2024, 2026) for kind in ("cpu", "gpu")]
    bound = policy.bind_batch_model_nodes(source, available_nodes=NODES, balanced=True, model_lookup=_model)
    assert [r["node_id"] for r in bound] == NODES * 3
    assert policy.batch_node_parallelism(bound) == {NODES[0]: 4, NODES[1]: 2}
    assert all(r["node_id"] == NODES[0] for r in source)


def test_all_cpu_batch_stays_remote_even_with_idle_wsl():
    source = [loop(model_id="cpu", node=NODES[1]) for _ in range(9)]
    bound = policy.bind_batch_model_nodes(source, available_nodes=NODES, balanced=True, model_lookup=_model)
    assert {r["node_id"] for r in bound} == {NODES[0]}


@pytest.mark.parametrize("model_id,node", [("cpu", NODES[1]), ("gpu", NODES[0])])
def test_explicit_node_cannot_bypass_model_requirement(model_id, node):
    with pytest.raises(ValueError, match="MODEL_NODE_MISMATCH"):
        policy.bind_batch_model_nodes([loop(model_id=model_id, node=node)], available_nodes=NODES, balanced=False, model_lookup=_model)


def test_gpu_frozen_model_inference_stays_on_wsl_but_replay_can_use_both_nodes():
    bound = policy.bind_batch_model_nodes([loop(model_id="gpu", backtest_only=True)], available_nodes=NODES, balanced=True, model_lookup=_model)
    assert bound[0]["node_id"] == NODES[1]
    assert policy.batch_node_parallelism(bound) == {NODES[1]: 4}
    replay = policy.bind_batch_model_nodes([loop(model_id="gpu", prediction_replay=True) for _ in range(6)], available_nodes=NODES, balanced=True, model_lookup=lambda _: pytest.fail("replay must not load a model"))
    assert {r["node_id"] for r in replay} == set(NODES)


def test_unknown_model_or_unavailable_required_node_fails_closed():
    with pytest.raises(ValueError, match="METADATA_REQUIRED"):
        policy.bind_batch_model_nodes([loop(model_id="missing")], available_nodes=NODES, balanced=True, model_lookup=_model)
    with pytest.raises(ValueError, match="MODEL_NODE_NOT_BOUND"):
        policy.bind_batch_model_nodes([loop(model_id="gpu")], available_nodes=[NODES[0]], balanced=True, model_lookup=_model)
    with pytest.raises(ValueError, match="DEVICE_UNKNOWN"):
        policy.loop_compute_kind(loop(model_id="odd"), {"model_type": "unknown"})


def test_governed_restart_rejects_tampered_node_and_model_bindings():
    bound = policy.bind_batch_model_nodes([loop(model_id="cpu", node=NODES[0])], available_nodes=NODES, balanced=False, model_lookup=_model)
    bound[0][policy.POLICY_KEY] = {"version": policy.QE_BATCH_EXECUTION_POLICY}
    assert policy.strategy_node_parallelism(bound, None) == {NODES[0]: 4}
    with pytest.raises(ValueError, match="MODEL_NODE_MISMATCH"):
        policy.strategy_node_parallelism([{**bound[0], "node_id": NODES[1]}], None)
    with pytest.raises(ValueError, match="BINDING_REQUIRED"):
        policy.strategy_node_parallelism([{**bound[0], "model_id": "gpu"}], None)


@pytest.mark.parametrize("info,params,expected", [
    ({"model_type": "LGBModel"}, {"device_type": "gpu"}, "gpu"),
    ({"model_type": "XGBOOST"}, {"device": "cuda:0"}, "gpu"),
    ({"model_type": "CATBOOST"}, {}, "cpu"),
    ({"model_type": "CATBOOST"}, {"task_type": "GPU"}, "gpu"),
    ({"model_type": "PTNN"}, {}, "gpu"),
    ({"model_type": "TimeSeries", "code_text": "custom source"}, {}, "gpu"),
    ({"model_type": "PTNN"}, {"GPU": -1}, "cpu"),
    ({"model_type": "TABPFN"}, {}, "gpu"),
])
def test_device_selectors_follow_actual_model_execution(info, params, expected):
    assert policy.loop_compute_kind(loop(model_params=params), info) == expected


def test_append_to_v2_batch_binds_new_model_and_keeps_node_contract():
    from unittest.mock import patch
    from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler
    original = policy.bind_batch_model_nodes([loop(model_id="cpu", node=NODES[0])], available_nodes=NODES, balanced=False, model_lookup=_model)
    original[0][policy.POLICY_KEY] = {"version": policy.QE_BATCH_EXECUTION_POLICY}
    appended = loop(model_id="gpu", node=NODES[1])
    with patch("backend.services.quantevolver.config_composer.ConfigComposer._get_model_info", side_effect=_model):
        rows, _, slots = object.__new__(AutoEvolutionScheduler)._normalize_full_custom_evo_nodes(original + [appended], NODES[0], None)
    assert rows[1][policy.POLICY_KEY]["version"] == policy.QE_BATCH_EXECUTION_POLICY
    assert rows[1][policy.MODEL_BINDING_KEY]["compute_kind"] == "gpu"
    assert slots == {NODES[0]: 4, NODES[1]: 2}


def test_placement_rejects_unbound_nodes():
    with pytest.raises(ValueError, match="PINNED_NODE_NOT_BOUND"):
        policy.place_batch_loops([loop(node="other")], available_nodes=NODES)
    with pytest.raises(ValueError, match="BINDINGS_REQUIRED"):
        policy.place_batch_loops([loop()], available_nodes=[])


def test_runtime_smoke_requires_exact_policy_and_rejects_http_only_success():
    from backend.routers.quantevolver_evolution import get_batch_execution_policy
    from scripts.aistock_issue_workflow import _business_smoke_semantic_contract

    contract, validate = _business_smoke_semantic_contract("/api/v1/quantevolver/evolution/execution-policy")
    assert contract == "qe_batch_execution_policy"
    payload = get_batch_execution_policy()
    assert validate(payload)[0] == "passed"
    for bad in ({"status": "ok"}, {**payload, "default_node_allocation": "explicit"},
                {**payload, "per_node_workload_limits": {"full_fit_loop": 4, "pure_backtest_loop": 4}}):
        assert validate(bad)[0] == "failed"


@pytest.mark.parametrize("defect", [None, "manifest", "pit", "location", "incomplete"])
def test_cross_node_identity_is_compared_with_active_and_with_peer(monkeypatch, defect):
    profile = SimpleNamespace(raw={
        "components": {"dataset_manifest_sha256": "a" * 64},
        "node_bindings": {node: {"candidate_root": "/dataset/" + node} for node in NODES},
    })
    calls = []

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_dataset_identity(self, *, node_id, data_root_uri):
            calls.append(node_id)
            data = dict.fromkeys(policy.DATASET_FIELDS, "identity")
            data.update(dataset_manifest_sha256="a" * 64, resolved_node_id=node_id, resolved_data_root_uri=data_root_uri)
            if node_id == NODES[1]:
                if defect == "manifest":
                    data["dataset_manifest_sha256"] = "b" * 64
                if defect == "pit":
                    data["st_pit_snapshot_id"] = "wrong"
                if defect == "location":
                    data["resolved_data_root_uri"] = "/other"
            return SimpleNamespace(complete=not (defect == "incomplete" and node_id == NODES[1]), dataset=data)

    monkeypatch.setattr("backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient.for_node", lambda _: Client())
    if defect:
        with pytest.raises(ValueError, match="QE_BATCH_"):
            asyncio.run(policy.verify_batch_dataset_nodes(profile, NODES))
    else:
        result = asyncio.run(policy.verify_batch_dataset_nodes(profile, NODES))
        assert result["dataset_manifest_sha256"] == "a" * 64
    assert calls == NODES
