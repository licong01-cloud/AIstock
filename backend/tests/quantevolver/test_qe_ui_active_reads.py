"""BUG-2012: persisted UI projections; no scheduler, DB writes or remote calls."""
from copy import deepcopy

import pytest

from backend.services.multi_alpha.combine_ui_adapter import (
    CombineUIAdapterError,
    MultiAlphaCombineUIAdapter,
    PostgresCombineUIRepository,
    task_key_for_run,
)
from backend.services.quantevolver.payload_summary import compact_loop_row
from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler


class Connection:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self, **_):
        return self

    def execute(self, sql, params):
        assert sql.lstrip().startswith("SELECT")
        self.calls.append((sql, params))

    def fetchall(self):
        return self.rows


@pytest.mark.parametrize("status", ["running", "completed", "failed", "paused"])
def test_observed_task_status_returns_terminal_snapshot_without_history(monkeypatch, status):
    import backend.services.quantevolver.qe_evolution_service as module

    row = {"task_id": "observed", "status": status, "current_loop": 2, "max_loops": 3}
    conn = Connection([row])
    monkeypatch.setattr(module, "get_conn", lambda: conn)
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    assert scheduler.get_task_status_batch(["observed", "observed"]) == [row]
    sql, params = conn.calls[0]
    assert params == (["observed"],)
    assert "WHERE task_id = ANY(%s)" in sql
    assert "config_json" not in sql and "metrics_json" not in sql
    assert scheduler.get_task_status_batch([]) == []
    assert len(conn.calls) == 1


def run_row(index, status="succeeded"):
    return {
        "id": f"run-{index}", "roster_hash": f"roster-{index}",
        "normalize_method": "rank", "walk_forward_json": {},
        "created_at": "2026-10-20T00:00:00Z", "updated_at": "2026-10-20T00:00:00Z",
        "status": status, "backtest_config_json": {"topk": 50},
    }


class Repository:
    def __init__(self):
        self.rows = [run_row(index) for index in range(6)]
        self.headers = []
        self.bundles = []

    def list_run_headers(self, *, run_ids=None):
        self.headers.append(run_ids)
        return [dict(row) for row in self.rows if run_ids is None or row["id"] in run_ids]

    def get_run_bundle(self, run_id):
        self.bundles.append(run_id)
        return {"run": next(row for row in self.rows if row["id"] == run_id), "scheme_results": [], "loo": []}


def test_combine_pagination_reads_only_page_bundles_and_status_never_reads_bundles():
    repo = Repository()
    adapter = MultiAlphaCombineUIAdapter(repo)
    page = adapter.list_tasks(limit=2, offset=2)
    assert page["total"] == 6 and page["count"] == 2
    assert set(repo.bundles) == {"run-2", "run-3"}
    repo.bundles.clear()
    repo.rows[2]["status"] = "running"
    status = adapter.get_observed_status(["run-2", "absent"])
    assert repo.headers[-1] == ["run-2", "absent"]
    assert status["missing_ids"] == ["absent"]
    assert status["tasks"][0]["status"] == "running"
    assert status["tasks"][0]["run_ids"] == ["run-2"]
    assert repo.bundles == []
    repo.rows[2]["status"] = "succeeded"
    assert adapter.get_observed_status(["run-2"])["tasks"][0]["status"] == "completed"


def test_combine_observed_detail_is_scoped_and_missing_set_is_explicit():
    repo = Repository()
    adapter = MultiAlphaCombineUIAdapter(repo)
    key = task_key_for_run(repo.rows[2])
    detail = adapter.get_task(key, run_ids=["run-2"])
    assert detail["task"]["run_ids"] == ["run-2"]
    assert repo.headers == [["run-2"]]
    with pytest.raises(CombineUIAdapterError, match="combine_ui_observed_runs_missing"):
        adapter.get_task(key, run_ids=["run-2", "absent"])


def test_combine_postgres_status_query_filters_before_child_counts():
    conn = Connection([run_row(1)])
    repo = PostgresCombineUIRepository(connection_provider=lambda: conn)
    assert repo.list_run_headers(run_ids=["run-1"]) == conn.rows
    sql, params = conn.calls[0]
    assert "run.id = ANY(%s::text[])" in sql
    assert params == (["run-1"], ["run-1"])
    assert "scheme_result" not in sql
    assert repo.list_run_headers(run_ids=[]) == []
    assert len(conn.calls) == 1


def test_light_projection_retains_metric_and_legacy_position_authority():
    raw = {
        "loop_index": 1, "status": "completed",
        "config_json": {
            "factor_list": ["a"], "model_params": {
                "enable_sector_hmm": True,
                "_qe_direct_v2_dataset_binding": {"selection_pins": {"pool_ids": ["csi500"]}},
            }, "strategy_params": {"topk": 50},
        },
        "metrics_json": {"IC": .12, "annualized_return": .2, "information_ratio": 1.1,
            "enhanced_metrics": {
                "absolute_returns": {"cagr": .3, "sharpe": 1.8, "max_drawdown": -.1, "final_cash": 20},
                "trade_diagnostics": {"avg_turnover": .05},
                "return_curves": {"dates": ["2026-01-01", "2026-01-02"]},
                "stock_trades": {"A": [{"date": "2026-01-01", "type": "buy"}]},
                "all_stocks": [{"stock": "A", "large": "x" * 10000}],
                "factor_analysis": {"feature_importance": [{"feature": "a"}]},
            }},
    }
    projected = deepcopy(raw)
    for key in ("all_stocks", "top_stocks", "bottom_stocks", "feature_importance", "factor_analysis", "ic_diagnostics"):
        projected["metrics_json"]["enhanced_metrics"].pop(key, None)
    result = compact_loop_row(projected)
    assert result == compact_loop_row(raw)
    assert result["metrics_summary"]["sharpe"] == 1.8
    assert result["metrics_summary"]["information_ratio"] == 1.1
    assert result["metrics_summary"]["enhanced_metrics"]["position_summary"]["final_stock_count"] == 1
    assert result["config_summary"]["universe"]["pool_ids"] == ["csi500"]


def test_status_routes_are_ahead_of_dynamic_details_and_report_missing(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.routers import quantevolver_evolution as qe, multi_alpha as ma

    monkeypatch.setattr(qe.scheduler, "get_task_status_batch", lambda ids: [{"task_id": ids[0], "status": "completed"}])
    monkeypatch.setattr(ma, "MultiAlphaCombineUIAdapter", lambda: MultiAlphaCombineUIAdapter(Repository()))
    app = FastAPI()
    app.include_router(qe.router)
    app.include_router(ma.router)
    client = TestClient(app)
    qe_route = next(route.path for route in qe.router.routes if route.path.endswith("/tasks/status"))
    ma_route = next(route.path for route in ma.router.routes if route.path.endswith("/tasks/status"))
    response = client.get(qe_route, params=[("task_ids", "observed"), ("task_ids", "missing")])
    assert response.status_code == 200
    assert response.json()["missing_ids"] == ["missing"]
    response = client.get(ma_route, params={"run_ids": "run-1"})
    assert response.status_code == 200
    assert response.json()["data"]["tasks"][0]["run_ids"] == ["run-1"]


def test_system_log_tail_has_bounded_io_and_explicit_truncation(tmp_path):
    from backend.routers.quantevolver_evolution import _read_system_log_tail

    log = tmp_path / "tail.log"
    log.write_text(" INFO old\n" * 100 + " ERROR first\n INFO recent\n ERROR last\n", encoding="utf-8")
    result = _read_system_log_tail(log, 2, "ERROR", max_bytes=80)
    assert result["lines"] == [" ERROR first", " ERROR last"]
    assert result["truncated"] is True and result["bytes_scanned"] == 80
    assert _read_system_log_tail(log, 0, "")["lines"] == []
    result = _read_system_log_tail(log, 2, "", max_bytes=4096)
    assert result["truncated"] is False
    assert result["lines"] == [" INFO recent", " ERROR last"]
