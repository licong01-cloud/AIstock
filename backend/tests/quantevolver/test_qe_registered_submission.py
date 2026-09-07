"""Batch A contract tests for canonical QE reservation-before-dispatch."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.quantevolver import config_composer as composer_module
from backend.services.quantevolver import multi_alpha_engine as engine_module
from backend.services.quantevolver import node_execution as node_execution_module
from backend.services.quantevolver.config_composer import (
    ConfigComposer,
    RDAGENT_DEFAULT_DATA_SPLIT,
)
from backend.services.quantevolver.experiment_config import (
    QE_RUNTIME_METADATA_KEYS,
    split_qe_runtime_metadata,
)
from backend.services.quantevolver.multi_alpha_engine import MultiAlphaEngine
from backend.services.quantevolver.node_execution import QENodePreflightError, preflight_qe_node
from backend.services.quantevolver.qe_run_registry import (
    QE_RUN_REGISTRATION_PARAM,
    PlannedQELoop,
    QERunRegistry,
    QERunRegistryError,
    attach_qe_run_registration,
    build_qe_run_registration,
)


class _WorkspaceConfigClient:
    def __init__(self, workspace_base: str):
        self.workspace_base = workspace_base

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

    async def get_workspace_config(self):
        return {"workspace_base": self.workspace_base}


def _preflight_registered_node(monkeypatch, *, configured_root: str, api_root: str):
    node = {
        "node_id": "wsl2-5080",
        "api_base_url": "http://127.0.0.1:9000",
        "status": "online",
        "workspace_base": configured_root,
        "factor_data_dir": "/mnt/x/factors",
        "qlib_data_path": "/mnt/x/day",
        "qlib_minute_path": "/mnt/x/minute",
        "qlib_rdagent_root": "/mnt/f/Dev/RD-Agent-main",
    }
    monkeypatch.setattr(node_execution_module, "get_compute_node", lambda _node_id: dict(node))
    monkeypatch.setattr(
        node_execution_module.QEWorkspaceClient,
        "for_node",
        staticmethod(lambda _node_id: _WorkspaceConfigClient(api_root)),
    )
    return asyncio.run(preflight_qe_node("wsl2-5080"))


@pytest.mark.parametrize(
    "api_root",
    ["", ".", "relative/qe_workspace", "/", "//host/qe_workspace", "/mnt/f/qe/../other", r"C:\qe"],
)
def test_registered_submission_preflight_rejects_noncanonical_api_workspace_root(monkeypatch, api_root):
    with pytest.raises(QENodePreflightError) as exc_info:
        _preflight_registered_node(monkeypatch, configured_root="/mnt/f/qe_workspace", api_root=api_root)
    assert exc_info.value.error_code == "QE_NODE_WORKSPACE_ROOT_INVALID"


def test_registered_submission_preflight_rejects_db_api_workspace_mismatch(monkeypatch):
    with pytest.raises(QENodePreflightError) as exc_info:
        _preflight_registered_node(
            monkeypatch,
            configured_root="/mnt/f/qe_workspace",
            api_root="/tmp/other-qe-workspace",
        )
    assert exc_info.value.error_code == "QE_NODE_WORKSPACE_ROOT_MISMATCH"
    assert exc_info.value.context["configured_workspace_base"] == "/mnt/f/qe_workspace"
    assert exc_info.value.context["api_workspace_base"] == "/tmp/other-qe-workspace"


def test_registered_submission_preflight_normalizes_matching_trailing_slash(monkeypatch):
    node = _preflight_registered_node(
        monkeypatch,
        configured_root="/mnt/f/qe_workspace",
        api_root="/mnt/f/qe_workspace/",
    )
    assert node["workspace_config"]["workspace_base"] == "/mnt/f/qe_workspace"


def test_registered_control_metadata_never_reaches_strategy_kwargs() -> None:
    yaml_text = ConfigComposer()._compose_conf_yaml(
        factors_info=[],
        model_info=None,
        strategy_info={
            "strategy_id": "score_weighted_topk_v2",
            "source_code": "class ScoreWeightedTopkStrategyV2:\n    pass\n",
            "portfolio_config": {
                "class": "ScoreWeightedTopkStrategyV2",
                "kwargs": {},
            },
        },
        data_split={
            "train_start": "2020-01-01",
            "train_end": "2020-12-31",
            "valid_start": "2021-01-01",
            "valid_end": "2021-06-30",
            "test_start": "2021-07-01",
            "test_end": "2021-12-31",
            "backtest_end": "2021-12-31",
        },
        custom_params={
            "topk": 20,
            "qe_mcp_provenance": {"created_by_name": "Codex"},
            "qe_factor_sources": {"alpha_a": "official"},
            "qe_pending_task_source": "mcp",
            "qe_pending_created_by": "Codex",
        },
        has_custom_factors=False,
        has_alpha158=False,
        backtest_freq="1min",
    )

    assert "topk: 20" in yaml_text
    for metadata_key in (
        "qe_mcp_provenance",
        "qe_factor_sources",
        "qe_pending_task_source",
        "qe_pending_created_by",
    ):
        assert metadata_key not in yaml_text


def test_registered_control_metadata_uses_canonical_runtime_metadata_contract() -> None:
    registered_keys = {
        "qe_mcp_provenance",
        "qe_factor_sources",
        "qe_pending_task_source",
        "qe_pending_created_by",
    }
    params = {
        "topk": 20,
        "qe_mcp_provenance": {"created_by_name": "Codex"},
        "qe_factor_sources": {"alpha_a": "official"},
        "qe_pending_task_source": "mcp",
        "qe_pending_created_by": "Codex",
    }

    executable, metadata = split_qe_runtime_metadata(params)

    assert registered_keys <= QE_RUNTIME_METADATA_KEYS
    assert executable == {"topk": 20}
    assert set(metadata) == registered_keys


def test_registered_control_metadata_remains_in_persisted_custom_params(monkeypatch) -> None:
    captured: dict = {}

    class _CaptureRegistry:
        def __init__(self, *, connection_factory):
            assert connection_factory is composer_module.get_conn

        def reserve_single(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(composer_module, "QERunRegistry", _CaptureRegistry)
    custom_params = {
        "topk": 20,
        "qe_mcp_provenance": {"created_by_name": "Codex"},
        "qe_factor_sources": {"alpha_a": "official"},
    }

    ConfigComposer()._save_experiment_record(
        experiment_id="qe_registered_metadata",
        experiment_name="registered metadata",
        exp_dir="/tmp/qe_registered_metadata",
        factor_names=["alpha_a"],
        model_id="lgbm",
        strategy_id="score_weighted_topk_v2",
        data_split={"test_end": "2026-08-31"},
        custom_params=custom_params,
    )

    persisted = captured["custom_params"]
    assert persisted["qe_mcp_provenance"] == {"created_by_name": "Codex"}
    assert persisted["qe_factor_sources"] == {"alpha_a": "official"}
    assert QE_RUN_REGISTRATION_PARAM in persisted


class _StateCursor:
    def __init__(self, state):
        self.state = state
        self.rowcount = 0
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        params = params or ()
        self.state["statements"].append(normalized)
        self.rows = []
        self.rowcount = 0
        if normalized.startswith("INSERT INTO qe_experiments"):
            experiment_id = params[0]
            custom_params = json.loads(params[6] or "{}")
            status = self.state["experiments"].get(experiment_id, {}).get("status", "created")
            self.state["experiments"][experiment_id] = {
                "status": status,
                "custom_params": custom_params,
                "factor_names": json.loads(params[2] or "[]"),
                "model_id": params[3],
                "strategy_id": params[4],
                "data_split": json.loads(params[5] or "{}"),
            }
            self.rowcount = 1
        elif normalized.startswith("SELECT experiment_id, status, custom_params"):
            row = self.state["experiments"].get(params[0])
            if row:
                self.rows = [(params[0], row["status"], row["custom_params"])]
        elif normalized.startswith("SELECT factor_names, model_id, strategy_id, custom_params"):
            row = self.state["experiments"].get(params[0])
            if row:
                self.rows = [(
                    row.get("factor_names", []),
                    row.get("model_id"),
                    row.get("strategy_id"),
                    row.get("custom_params", {}),
                    row.get("data_split", {}),
                )]
        elif normalized.startswith("UPDATE qe_evolution_tasks SET strategy_evo_config"):
            task_id = params[2]
            if task_id in self.state["tasks"]:
                self.state["tasks"][task_id]["strategy_evo_config"][params[0]] = json.loads(params[1])
                self.rowcount = 1
        elif normalized.startswith("INSERT INTO qe_evolution_loops"):
            loop_id, task_id, loop_index, action_type, config_json, node_id = params
            key = (task_id, int(loop_index))
            self.state["loops"].setdefault(
                key,
                {
                    "loop_id": loop_id,
                    "status": "pending",
                    "action_type": action_type,
                    "config_json": json.loads(config_json),
                    "node_id": node_id,
                },
            )
            self.rowcount = 1
        elif normalized.startswith("SELECT loop_index, status"):
            task_id, indexes = params
            self.rows = [
                (
                    index,
                    self.state["loops"][(task_id, index)]["status"],
                    self.state["loops"][(task_id, index)]["config_json"],
                    self.state["loops"][(task_id, index)]["node_id"],
                    self.state["loops"][(task_id, index)]["action_type"],
                )
                for index in sorted(indexes)
                if (task_id, index) in self.state["loops"]
            ]
        elif normalized.startswith("SELECT task_id, strategy_evo_config"):
            row = self.state["tasks"].get(params[0])
            if row:
                self.rows = [(params[0], row["strategy_evo_config"])]
        elif normalized.startswith("INSERT INTO qe_multi_alpha_groups"):
            parent_id, group_name, factors, model_id = params[:4]
            self.state["groups"].setdefault(
                (parent_id, group_name),
                {
                    "status": "pending",
                    "factor_names": json.loads(factors),
                    "model_id": model_id,
                    "node_id": params[7],
                    "reuse_mode": params[10],
                },
            )
            self.rowcount = 1
        elif normalized.startswith("SELECT group_name, factor_names, model_id, assigned_node_id, reuse_mode"):
            self.rows = [
                (
                    group_name,
                    group["factor_names"],
                    group["model_id"],
                    group["node_id"],
                    group["reuse_mode"],
                )
                for (parent_id, group_name), group in self.state["groups"].items()
                if parent_id == params[0]
            ]
        elif normalized.startswith("UPDATE qe_experiments SET status = 'pending'"):
            row = self.state["experiments"].get(params[0])
            if row and row["status"] == "created":
                row["status"] = "pending"
                self.rowcount = 1

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class _StateConnection:
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def cursor(self, *_args, **_kwargs):
        return _StateCursor(self.state)

    def commit(self):
        self.state["commits"] += 1


def _state_factory(initial_experiments=None, initial_tasks=None):
    state = {
        "experiments": dict(initial_experiments or {}),
        "tasks": dict(initial_tasks or {}),
        "loops": {},
        "groups": {},
        "statements": [],
        "commits": 0,
    }
    return state, lambda: _StateConnection(state)


def test_registration_summary_is_portable_and_complete() -> None:
    registration = build_qe_run_registration(
        run_kind="single",
        source_type="mcp",
        created_by_name="Codex",
        purpose="research",
        node_id="rdagent-node1",
        model_id="LSTM",
        factor_names=["factor_b", "factor_a"],
        strategy_id="TWAP",
        data_split={"test_start": "2026-07-01", "test_end": "2026-08-31"},
        custom_params={
            "random_seed": 123,
            "label_horizon": 20,
            "execution_algo": "TWAP",
            "backtest_freq": "1min",
            "topk": 50,
            "n_drop": 5,
            "open_cost": 0.0005,
            "filter_suspended_on_signal": True,
            "_qe_active_dataset_summary": {
                "generation": "g7",
                "release_id": "qe-20260831",
                "cutoff": "2026-08-31",
            },
            "_qe_direct_v2_dataset_binding": {
                "selection_pins": {"mode": "single_index", "pool_ids": ["000300.SH"]}
            },
        },
    )

    assert registration["source_type"] == "mcp"
    assert registration["purpose"] == "research"
    assert registration["dataset_release_id"] == "qe-20260831"
    assert registration["universe_pool_ids"] == ["000300.SH"]
    assert registration["execution_algo"] == "TWAP"
    assert registration["execution_frequency"] == "1min"
    assert registration["factor_count"] == 2
    assert registration["data_split"]["test_end"] == "2026-08-31"
    assert registration["strategy_summary"] == {"topk": 50, "n_drop": 5}
    assert registration["cost_summary"]["open_cost"] == 0.0005
    assert registration["execution_contract_summary"]["filter_suspended_on_signal"] is True
    assert "/mnt/" not in json.dumps(registration)


def test_registration_rejects_unknown_purpose() -> None:
    with pytest.raises(QERunRegistryError, match="qe_run_purpose_invalid"):
        build_qe_run_registration(run_kind="single", purpose="smoke")
    with pytest.raises(QERunRegistryError, match="qe_run_source_type_invalid"):
        build_qe_run_registration(run_kind="single", source_type="unknown-runner")


def test_run_registration_metadata_is_not_forwarded_to_strategy_kwargs(
    monkeypatch,
) -> None:
    monkeypatch.setattr(composer_module, "load_active_qe_profile", lambda: None)
    composer = ConfigComposer()
    monkeypatch.setattr(
        composer,
        "_get_factors_info",
        lambda *_args, **_kwargs: [
            {
                "factor_name": "DemoFactor",
                "source": "custom",
                "code_text": (
                    "def calculate_DemoFactor(instruments, start_date, end_date):\n"
                    "    return None\n"
                ),
            }
        ],
    )
    monkeypatch.setattr(composer, "_get_model_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(composer, "_get_strategy_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        composer,
        "_fetch_workspace_config",
        lambda *_args, **_kwargs: {
            "workspace_base": "/tmp/qe_workspace",
            "qlib_data_path": "/tmp/qlib_day",
            "qlib_minute_path": "/tmp/qlib_minute",
            "factor_data_dir": "/tmp/factor_data",
        },
    )
    monkeypatch.setattr(
        composer,
        "_prepare_risk_policy_runtime",
        lambda **kwargs: (kwargs["custom_params"], None),
    )
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **kwargs: (kwargs["custom_params"], None),
    )
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read")

    custom_params = attach_qe_run_registration(
        {"execution_node_id": "wsl2-5080"},
        run_kind="custom_evolution_loop",
        source_type="agent",
        purpose="research",
        task_id="qe-registration-filter",
        loop_index=1,
        node_id="wsl2-5080",
        factor_names=["DemoFactor"],
    )
    result = composer.compose_experiment_in_memory(
        factor_names=["DemoFactor"],
        model_id=None,
        data_split=dict(RDAGENT_DEFAULT_DATA_SPLIT),
        custom_params=custom_params,
        skip_db_save=True,
        execution_algo="CLOSE_PRICE",
        execution_algo_params={},
    )

    assert QE_RUN_REGISTRATION_PARAM not in result["experiment_files"]["conf.yaml"]


def test_single_reservation_rejects_missing_or_non_created_identity() -> None:
    state, factory = _state_factory(
        initial_experiments={
            "qe-complete": {
                "status": "completed",
                "custom_params": {},
                "factor_names": ["old"],
                "model_id": "LSTM",
                "strategy_id": "TWAP",
                "data_split": {},
            }
        }
    )
    registry = QERunRegistry(connection_factory=factory)
    with pytest.raises(QERunRegistryError, match="registration_missing"):
        registry.reserve_single(
            experiment_id="qe-new",
            experiment_name="missing registration",
            workspace_path=None,
            factor_names=[],
            model_id=None,
            strategy_id=None,
            data_split={},
            custom_params={},
        )

    params = attach_qe_run_registration(
        {},
        run_kind="single",
        source_type="ui",
        purpose="research",
        factor_names=["new"],
    )
    with pytest.raises(QERunRegistryError, match="reservation_readback_failed"):
        registry.reserve_single(
            experiment_id="qe-complete",
            experiment_name="must not overwrite",
            workspace_path=None,
            factor_names=["new"],
            model_id="LGBModel",
            strategy_id="TWAP",
            data_split={},
            custom_params=params,
        )
    assert state["experiments"]["qe-complete"]["status"] == "completed"


def test_single_and_task_reservations_are_read_back_before_dispatch() -> None:
    params = attach_qe_run_registration(
        {"execution_algo": "TWAP"},
        run_kind="single",
        source_type="ui",
        purpose="research",
        factor_names=["f1"],
    )
    state, factory = _state_factory(
        initial_tasks={"task-1": {"strategy_evo_config": {}}}
    )
    registry = QERunRegistry(connection_factory=factory)
    registry.reserve_single(
        experiment_id="qe-1",
        experiment_name="registered",
        workspace_path="/state/qe-1",
        factor_names=["f1"],
        model_id="LSTM",
        strategy_id="TWAP",
        data_split={"test_end": "2026-08-31"},
        custom_params=params,
    )
    registry.reserve_task(
        task_id="task-1",
        base_experiment_id="qe-1",
        task_kind="custom_evolution",
        planned_loops=[
            PlannedQELoop(1, "wsl2-5080", {"factor_list": ["f1"], "model_id": "LSTM"}),
            PlannedQELoop(2, "rdagent-node1", {"factor_list": ["f2"], "model_id": "LGBModel"}),
        ],
        source_type="mcp",
        purpose="validation",
    )

    assert state["experiments"]["qe-1"]["status"] == "created"
    assert sorted(state["loops"]) == [("task-1", 1), ("task-1", 2)]
    assert state["loops"][("task-1", 1)]["status"] == "pending"
    assert state["loops"][("task-1", 2)]["config_json"][QE_RUN_REGISTRATION_PARAM]["purpose"] == "validation"
    assert (
        state["loops"][("task-1", 2)]["config_json"]["custom_params"][
            QE_RUN_REGISTRATION_PARAM
        ]["source_type"]
        == "mcp"
    )
    parent_registration = state["experiments"]["qe-1"]["custom_params"][QE_RUN_REGISTRATION_PARAM]
    assert parent_registration["run_kind"] == "single"
    task_registration = state["tasks"]["task-1"]["strategy_evo_config"][
        QE_RUN_REGISTRATION_PARAM
    ]
    assert task_registration["model_id"] == "LSTM"
    assert task_registration["factor_count"] == 1
    assert task_registration["node_id"] == "wsl2-5080"
    state["loops"][("task-1", 2)]["node_id"] = "unexpected-node"
    with pytest.raises(QERunRegistryError, match="task_reservation_readback_failed"):
        registry.reserve_task(
            task_id="task-1",
            base_experiment_id="qe-1",
            task_kind="custom_evolution",
            planned_loops=[
                PlannedQELoop(1, "wsl2-5080", {"factor_list": ["f1"], "model_id": "LSTM"}),
                PlannedQELoop(2, "rdagent-node1", {"factor_list": ["f2"], "model_id": "LGBModel"}),
            ],
            source_type="mcp",
            purpose="validation",
        )
    assert registry.mark_dispatched(experiment_id="qe-1") is True
    assert state["experiments"]["qe-1"]["status"] == "pending"


def test_registry_requests_managed_transactions_from_real_connection_factories() -> None:
    state, _factory = _state_factory()
    calls: list[dict[str, object]] = []

    def factory(**kwargs):
        calls.append(dict(kwargs))
        return _StateConnection(state)

    registry = QERunRegistry(connection_factory=factory)
    params = attach_qe_run_registration(
        {},
        run_kind="single",
        source_type="ui",
        purpose="research",
    )
    registry.reserve_single(
        experiment_id="qe-transactional",
        experiment_name="transactional",
        workspace_path=None,
        factor_names=[],
        model_id=None,
        strategy_id=None,
        data_split={},
        custom_params=params,
    )

    assert calls == [{"autocommit": False, "manage_transaction": True}]


def _engine_for_order_test():
    group = SimpleNamespace(
        group_name="price",
        reuse_mode="retrain",
        model_source_experiment_id=None,
        model_source_group_name=None,
        factor_names=["f1"],
        model_id="LSTM",
        model_params={},
        dataset_type="DatasetH",
        compute_resource="gpu",
    )
    engine = MultiAlphaEngine.__new__(MultiAlphaEngine)
    engine.config = SimpleNamespace(
        node_id="wsl2-5080",
        experiment_name="qe-multi",
        strategy_id="TWAP",
        data_split={"test_end": "2026-08-31"},
        build_custom_params=lambda: {"execution_algo": "TWAP"},
    )
    engine.ma_config = SimpleNamespace(
        alpha_groups=[group],
        execution_mode="serial",
        meta_model=SimpleNamespace(method="equal", lookback_days=20),
        model_dump=lambda: {"alpha_groups": [{"group_name": "price"}]},
    )
    engine.available_nodes = None
    engine.composer = object()
    engine.active_dataset_profile = None
    engine.parent_custom_params = {}
    engine.registration_context = {"source_type": "mcp", "purpose": "research"}
    engine.parent_multi_alpha_id = None
    return engine, group


def test_multi_alpha_parent_and_all_children_are_durable_before_dispatch() -> None:
    state, factory = _state_factory()
    registry = QERunRegistry(connection_factory=factory)
    registration = attach_qe_run_registration(
        {"execution_algo": "TWAP"},
        run_kind="multi_alpha",
        source_type="ui",
        purpose="research",
        factor_names=["f1", "f2"],
    )
    assignments = [
        SimpleNamespace(
            group=SimpleNamespace(
                group_name=name,
                factor_names=[factor],
                model_id="LSTM",
                dataset_type="DatasetH",
                model_params={},
                compute_resource="gpu",
                model_source_experiment_id=None,
                model_source_group_name=None,
                reuse_mode="retrain",
            ),
            node_id=node,
        )
        for name, factor, node in (
            ("price", "f1", "wsl2-5080"),
            ("sector", "f2", "rdagent-node1"),
        )
    ]

    registry.reserve_multi_alpha(
        parent_experiment_id="qe-ma-1",
        experiment_name="registered multi alpha",
        factor_names=["f1", "f2"],
        model_id="LSTM",
        strategy_id="TWAP",
        data_split={"test_end": "2026-08-31"},
        custom_params=registration,
        multi_alpha_config={"execution_mode": "parallel"},
        assignments=assignments,
    )

    assert state["experiments"]["qe-ma-1"]["status"] == "created"
    assert sorted(state["groups"]) == [("qe-ma-1", "price"), ("qe-ma-1", "sector")]

    state["groups"][("qe-ma-1", "sector")]["factor_names"] = ["different-factor"]
    with pytest.raises(QERunRegistryError, match="reservation_readback_failed"):
        registry.reserve_multi_alpha(
            parent_experiment_id="qe-ma-1",
            experiment_name="registered multi alpha",
            factor_names=["f1", "f2"],
            model_id="LSTM",
            strategy_id="TWAP",
            data_split={"test_end": "2026-08-31"},
            custom_params=registration,
            multi_alpha_config={"execution_mode": "parallel"},
            assignments=assignments,
        )


def test_multi_alpha_rejects_missing_parent_registration() -> None:
    _state, factory = _state_factory()
    registry = QERunRegistry(connection_factory=factory)
    assignment = SimpleNamespace(
        group=SimpleNamespace(
            group_name="price",
            factor_names=["f1"],
            model_id="LSTM",
            dataset_type="DatasetH",
            model_params={},
            compute_resource="gpu",
            model_source_experiment_id=None,
            model_source_group_name=None,
            reuse_mode="retrain",
        ),
        node_id="wsl2-5080",
    )

    with pytest.raises(QERunRegistryError, match="registration_missing"):
        registry.reserve_multi_alpha(
            parent_experiment_id="qe-ma-unregistered",
            experiment_name="unregistered multi alpha",
            factor_names=["f1"],
            model_id="LSTM",
            strategy_id="TWAP",
            data_split={},
            custom_params={},
            multi_alpha_config={"execution_mode": "serial"},
            assignments=[assignment],
        )


def test_durable_multi_alpha_is_read_back_before_wakeup() -> None:
    calls: list[str] = []
    registry = QERunRegistry(connection_factory=lambda: None)

    def reserve() -> dict[str, str]:
        calls.append("reserve")
        return {"id": "macb-1", "status": "queued"}

    def readback(run_id: str) -> dict[str, str]:
        calls.append("readback")
        return {"id": run_id, "status": "queued"}

    row = registry.reserve_durable(
        run_id="macb-1",
        reserve=reserve,
        readback=readback,
    )

    assert row["status"] == "queued"
    assert calls == ["reserve", "readback"]


def test_multi_alpha_reserves_parent_and_children_before_materialization(monkeypatch) -> None:
    engine, group = _engine_for_order_test()
    order: list[str] = []
    assignment = SimpleNamespace(group=group, node_id="wsl2-5080", order=0)

    class _Registry:
        def __init__(self, **_kwargs):
            pass

        def reserve_multi_alpha(self, **kwargs):
            assert kwargs["parent_experiment_id"] == "qe-multi"
            assert kwargs["assignments"] == [assignment]
            order.append("reserved")

    def fail_after_reservation(*_args, **_kwargs):
        assert order == ["reserved"]
        raise RuntimeError("materialization failed")

    monkeypatch.setattr(engine_module, "QERunRegistry", _Registry)
    monkeypatch.setattr(engine_module, "plan_assignments", lambda **_kwargs: [assignment])
    monkeypatch.setattr(engine, "_compose_group_experiment", fail_after_reservation)

    with pytest.raises(RuntimeError, match="materialization failed"):
        engine.run()
    assert order == ["reserved"]


def test_multi_alpha_registration_failure_prevents_materialization(monkeypatch) -> None:
    engine, group = _engine_for_order_test()
    materialized = False
    assignment = SimpleNamespace(group=group, node_id="wsl2-5080", order=0)

    class _Registry:
        def __init__(self, **_kwargs):
            pass

        def reserve_multi_alpha(self, **_kwargs):
            raise QERunRegistryError("qe_run_multi_alpha_reservation_readback_failed", "no readback")

    def compose(*_args, **_kwargs):
        nonlocal materialized
        materialized = True
        return {}

    monkeypatch.setattr(engine_module, "QERunRegistry", _Registry)
    monkeypatch.setattr(engine_module, "plan_assignments", lambda **_kwargs: [assignment])
    monkeypatch.setattr(engine, "_compose_group_experiment", compose)

    with pytest.raises(QERunRegistryError, match="reservation_readback_failed"):
        engine.run()
    assert materialized is False


def test_evolution_entrypoints_reserve_before_async_submission() -> None:
    source = Path(
        engine_module.__file__.replace("multi_alpha_engine.py", "qe_evolution_service.py")
    ).read_text(encoding="utf-8")
    custom = source[source.index("async def create_custom_evo_task"):source.index("def _parse_custom_evo_strategy_config")]
    strategy = source[source.index("async def strategy_fork_task"):source.index("async def submit_strategy_evo_loop")]

    assert custom.index(".reserve_task(") < custom.index("asyncio.create_task(")
    assert strategy.index(".reserve_task(") < strategy.index("asyncio.create_task(")


def test_auto_and_multi_alpha_loops_preserve_planned_registration() -> None:
    source = Path(
        engine_module.__file__.replace("multi_alpha_engine.py", "qe_evolution_service.py")
    ).read_text(encoding="utf-8")
    auto_submit = source[
        source.index("async def submit_next_loop"):source.index("async def process_completed_loop")
    ]
    multi_submit = source[source.index("async def _submit_multi_alpha_loop"):]

    assert auto_submit.index("_load_planned_loop_registration(") < auto_submit.index(
        "await executor.submit("
    )
    assert "loop_model_params[QE_RUN_REGISTRATION_PARAM]" in auto_submit
    assert multi_submit.index("_load_planned_loop_registration(") < multi_submit.index(
        "result = engine.run()"
    )
    assert "config_json[QE_RUN_REGISTRATION_PARAM]" in multi_submit
