import asyncio
from pathlib import Path

import pytest

from backend.routers import quantevolver as qt
from backend.services.quantevolver.qe_feedback_service import QEFeedbackService


class _Cursor:
    def __init__(self, row=None):
        self._row = row
        self.executed = []
        self.description = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        self.executed.append((args, kwargs))

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, row=None):
        self.cursor_obj = _Cursor(row)
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return self.cursor_obj

    def commit(self):
        self.committed = True


class _EnhancedClient:
    def __init__(self, payload=None, error=None):
        self.payload = payload or {
            "summary": {"IC": 0.052},
            "ic_diagnostics": {"dates": ["2026-01-02"], "ic_series": [0.052]},
            "return_curves": {"dates": ["2026-01-02"], "cumulative_excess_with_cost": [0.0123]},
            "all_stocks": [{"code": "000001.SZ", "profit": 10.5, "profit_pct": 0.01, "holding_days": 2, "first_date": "2026-01-02", "last_date": "2026-01-03"}],
        }
        self.error = error
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get_enhanced_metrics(self, task_id, loop_id):
        self.calls.append((task_id, loop_id))
        if self.error:
            raise self.error
        return self.payload


def test_experiment_enhanced_metrics_prefers_db_cache_and_never_opens_node(monkeypatch):
    row = (
        "Loop1",
        "task_cached",
        {
            "enhanced_metrics": {
                "summary": {"IC": 0.041},
                "ic_diagnostics": {"dates": ["2026-01-02"], "ic_series": [0.041]},
                "return_curves": {"dates": ["2026-01-02"], "cumulative_excess_with_cost": [0.01]},
                "all_stocks": [{"code": "000001.SZ"}],
            }
        },
        {"execution_node_id": "node-a"},
    )
    monkeypatch.setattr(qt, "get_conn", lambda: _Conn(row))
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient.for_node",
        lambda node_id: (_ for _ in ()).throw(AssertionError("DB cache should avoid node reads")),
    )

    result = asyncio.run(qt.get_experiment_enhanced_metrics("exp_cached"))

    assert result["dates"] == ["2026-01-02"]
    assert result["ic_series"] == [0.041]
    assert result["cumulative_excess_with_cost"] == [0.01]
    assert result["all_stocks"][0]["code"] == "000001.SZ"


def test_experiment_enhanced_metrics_uses_node_api_when_db_detail_missing(monkeypatch):
    row = ("Loop2", "task_node", {"summary": {"IC": 0.052}}, {"execution_node_id": "node-a"})
    client = _EnhancedClient()

    monkeypatch.setattr(qt, "get_conn", lambda: _Conn(row))
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient.for_node",
        lambda node_id: client,
    )

    result = asyncio.run(qt.get_experiment_enhanced_metrics("exp_node"))

    assert result["dates"] == ["2026-01-02"]
    assert result["ic_series"] == [0.052]
    assert result["return_dates"] == ["2026-01-02"]
    assert result["all_stocks"][0]["code"] == "000001.SZ"
    assert client.calls == [("task_node", "Loop2")]


def test_experiment_enhanced_metrics_node_missing_is_explicit_404(monkeypatch):
    row = ("Loop2", "task_node", {}, {"execution_node_id": "node-a"})
    client = _EnhancedClient(error=RuntimeError("404 qlib_results_enhanced.json not found"))

    monkeypatch.setattr(qt, "get_conn", lambda: _Conn(row))
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_workspace_client.QEWorkspaceClient.for_node",
        lambda node_id: client,
    )

    with pytest.raises(qt.HTTPException) as exc:
        asyncio.run(qt.get_experiment_enhanced_metrics("exp_missing"))

    assert exc.value.status_code == 404
    assert "增强指标文件尚未生成" in str(exc.value.detail)
    assert client.calls == [("task_node", "Loop2")]


def test_feedback_service_reads_db_record_not_workspace_path(monkeypatch):
    workspace_path = "F:/should/not/be/read/qe_workspace/exp_feedback"
    record = {
        "experiment_id": "exp_feedback",
        "workspace_path": workspace_path,
        "loop_index": 1,
        "factor_names": ["factor_a"],
        "model_id": "model_a",
        "strategy_id": "strategy_a",
        "custom_params": {"execution_node_id": "node-a"},
        "result_metrics": {
            "summary": {
                "IC": 0.033,
                "Rank_IC": 0.044,
                "excess_return_with_cost_annualized": 0.12,
                "excess_return_with_cost_max_drawdown": -0.08,
                "excess_return_with_cost_IR": 1.4,
            }
        },
        "ic": 0.033,
        "rank_ic": 0.044,
        "annualized_return": 0.12,
        "max_drawdown": -0.08,
        "information_ratio": 1.4,
    }
    conn = _Conn()
    monkeypatch.setattr("backend.services.quantevolver.qe_feedback_service.get_conn", lambda: conn)

    feedback = QEFeedbackService().generate_feedback(
        experiment_id="exp_feedback",
        experiment_record=record,
    )

    assert feedback.experiment_id == "exp_feedback"
    assert feedback.next_focus
    assert conn.cursor_obj.executed, "feedback must still be persisted to DB"
    assert workspace_path not in str(conn.cursor_obj.executed)


def test_experiment_read_router_has_no_worker_workspace_fallback_helpers():
    source = Path(qt.__file__).read_text(encoding="utf-8")
    forbidden = [
        "_candidate_enhanced_metric_files",
        "_load_local_enhanced_metrics_payload",
        "_wsl_to_win_path",
        "_candidate_run_log_files",
        "_load_experiment_local_log_tail",
        "local run.log",
        "qlib_results_enhanced.json candidates",
    ]
    for token in forbidden:
        assert token not in source
