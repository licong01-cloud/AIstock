"""Service-level tests for compact QE API read projections."""

from __future__ import annotations

import asyncio

from backend.services.quantevolver.config_composer import ConfigComposer
from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler


class _Cursor:
    def __init__(self, script, *, as_dict: bool = False):
        self._script = list(script)
        self._as_dict = as_dict
        self.description = []
        self._rows = []

    def execute(self, sql, _params=None):
        next_result = self._script.pop(0)
        capture = next_result.get("capture")
        if capture is not None:
            capture.append(sql)
        self.description = [(name,) for name in next_result["cols"]]
        self._rows = next_result["rows"]

    def _dict_row(self, row):
        if isinstance(row, dict):
            return row
        return dict(zip([col[0] for col in self.description], row))

    def fetchone(self):
        if not self._rows:
            return None
        return self._dict_row(self._rows[0]) if self._as_dict else self._rows[0]

    def fetchall(self):
        return [self._dict_row(row) for row in self._rows] if self._as_dict else self._rows

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _Conn:
    def __init__(self, script):
        self._script = script

    def cursor(self, *_, **kwargs):
        return _Cursor(self._script, as_dict=kwargs.get("cursor_factory") is not None)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def _patch_conn(monkeypatch, module, script):
    monkeypatch.setattr(module, "get_conn", lambda: _Conn(script))


def test_experiment_history_full_keeps_legacy_jsonb_columns(monkeypatch):
    import backend.services.quantevolver.config_composer as module

    script = [
        {"cols": ["count"], "rows": [(1,)]},
        {"cols": ["experiment_id"], "rows": [("qe_parent",)]},
        {
            "cols": [
                "experiment_id", "experiment_name", "status", "factor_names", "model_id", "strategy_id",
                "workspace_path", "wsl_command", "result_metrics", "qe_task_id", "qe_loop_id", "loop_index",
                "parent_experiment_id", "is_evolution_loop", "ic", "icir", "rank_ic", "rank_icir",
                "annualized_return", "max_drawdown", "information_ratio", "annualized_return_no_cost",
                "max_drawdown_no_cost", "information_ratio_no_cost", "created_at", "updated_at", "custom_params",
                "alpha_mode", "multi_alpha_config", "parent_multi_alpha_id", "_evolution_base_experiment_id", "_evolution_task_type",
            ],
            "rows": [(
                "qe_parent", "Parent", "completed", ["a"], "m", "s", "/tmp/ws", "cmd",
                {"enhanced_metrics": {"stock_trades": {"x": []}}}, None, None, None, None, False,
                0.1, None, None, None, None, None, None, None, None, None, None, None, {"quick_train": True},
                "single", {"groups": []}, None, None, None,
            )],
        },
    ]
    _patch_conn(monkeypatch, module, script)

    result = ConfigComposer()._list_experiment_history(detail="full")

    item = result["items"][0]
    assert result["detail"] == "full"
    assert item["workspace_path"] == "/tmp/ws"
    assert item["wsl_command"] == "cmd"
    assert "result_metrics" in item
    assert "custom_params" in item
    assert "multi_alpha_config" in item


def test_experiment_history_summary_drops_legacy_jsonb_columns(monkeypatch):
    import backend.services.quantevolver.config_composer as module

    script = [
        {"cols": ["count"], "rows": [(1,)]},
        {"cols": ["experiment_id"], "rows": [("qe_parent",)]},
        {
            "cols": [
                "experiment_id", "experiment_name", "status", "factor_names", "model_id", "strategy_id",
                "qe_task_id", "qe_loop_id", "loop_index", "parent_experiment_id", "is_evolution_loop",
                "ic", "icir", "rank_ic", "rank_icir", "annualized_return", "max_drawdown",
                "information_ratio", "annualized_return_no_cost", "max_drawdown_no_cost", "information_ratio_no_cost",
                "created_at", "updated_at", "alpha_mode", "parent_multi_alpha_id", "_evolution_base_experiment_id", "_evolution_task_type",
            ],
            "rows": [(
                "qe_parent", "Parent", "completed", ["a"], "m", "s", None, None, None, None, False,
                0.1, None, None, None, None, None, None, None, None, None, None, None, "single", None, None, None,
            )],
        },
    ]
    _patch_conn(monkeypatch, module, script)

    result = ConfigComposer()._list_experiment_history(detail="summary")

    item = result["items"][0]
    assert result["detail"] == "summary"
    assert item["ic"] == 0.1
    assert "result_metrics" not in item
    assert "custom_params" not in item
    assert "workspace_path" not in item


def test_get_task_detail_summary_compacts_loop_jsonb(monkeypatch):
    import backend.services.quantevolver.qe_evolution_service as module

    captured_sql = []
    script = [
        {
            "cols": ["task_id", "task_name", "target_desc", "max_loops", "current_loop", "status", "base_experiment_id", "node_id", "label_horizon", "task_type", "source_type", "strategy_id", "strategy_params", "execution_algo", "execution_algo_params", "unfilled_handler", "unfilled_handler_params", "strategy_evo_execution_mode", "created_at", "updated_at"],
            "rows": [("task_1", "Task", "goal", 2, 2, "completed", "qe_base", "node", 5, "custom_evo", None, "strat", {}, "CLOSE_PRICE", {}, None, None, "serial", None, None)],
        },
        {
            "capture": captured_sql,
            "cols": ["loop_id", "task_id", "loop_index", "action_type", "factor_list", "model_id", "ic", "is_sota", "status", "node_id", "experiment_id", "created_at", "updated_at"],
            "rows": [("task_1_Loop1", "task_1", 1, "initial", ["a"], "m", "0.2", False, "completed", "node", "qe_1", None, None)],
        },
    ]
    _patch_conn(monkeypatch, module, script)
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    result = asyncio.run(scheduler.get_task_detail("task_1", detail="summary"))

    assert result["detail"] == "summary"
    assert result["loops"][0]["ic"] == 0.2
    assert result["loops"][0]["factors"] == ["a"]
    assert "config_json" not in result["loops"][0]
    assert "metrics_json" not in result["loops"][0]
    loop_sql = captured_sql[0]
    assert "config_json," not in loop_sql
    assert "metrics_json," not in loop_sql
    assert "agent_analysis" not in loop_sql
    assert "config_json->'factor_list'" in loop_sql
    assert "metrics_json->>'IC'" in loop_sql


def test_get_task_detail_full_keeps_loop_jsonb(monkeypatch):
    import backend.services.quantevolver.qe_evolution_service as module

    script = [
        {"cols": ["task_id", "task_name", "status", "task_type"], "rows": [("task_1", "Task", "completed", "custom_evo")]},
        {
            "cols": ["loop_id", "task_id", "loop_index", "action_type", "config_json", "metrics_json", "agent_analysis", "is_sota", "status"],
            "rows": [("task_1_Loop1", "task_1", 1, "initial", {"factor_list": ["a"]}, {"IC": 0.2}, {"analysis": "ok"}, False, "completed")],
        },
    ]
    _patch_conn(monkeypatch, module, script)
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    result = asyncio.run(scheduler.get_task_detail("task_1", detail="full"))

    assert result["loops"][0]["config_json"]["factor_list"] == ["a"]
    assert result["loops"][0]["metrics_json"] == {"IC": 0.2}
    assert result["loops"][0]["agent_analysis"] == {"analysis": "ok"}


def test_get_all_tasks_uses_offset_for_full_visibility(monkeypatch):
    import backend.services.quantevolver.qe_evolution_service as module

    captured = []
    script = [
        {
            "capture": captured,
            "cols": [
                "task_id", "task_name", "target_desc", "max_loops", "current_loop", "status",
                "base_experiment_id", "node_id", "label_horizon", "task_type", "source_type",
                "strategy_id", "strategy_params", "strategy_evo_config", "execution_algo", "strategy_evo_execution_mode", "created_at", "updated_at",
            ],
            "rows": [
                ("task_201", "Task 201", "goal", 1, 1, "completed", "qe_base", "node", 1, "evolution", None, None, {"enable_sector_hmm": True}, None, None, None, None),
            ],
        },
    ]
    _patch_conn(monkeypatch, module, script)
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    result = asyncio.run(scheduler.get_all_tasks(limit=200, offset=200))

    assert result[0]["task_id"] == "task_201"
    assert result[0]["hmm_enabled"] is True
    assert "strategy_params" not in result[0]
    assert "LIMIT %s OFFSET %s" in captured[0]


def test_compact_task_row_hmm_enabled_rejects_false_string():
    from backend.services.quantevolver.payload_summary import compact_task_row

    assert compact_task_row({"strategy_params": {"enable_sector_hmm": "false"}})["hmm_enabled"] is False
    assert compact_task_row({"strategy_params": {"hmm_model_version_id": "snap_001"}})["hmm_enabled"] is True
