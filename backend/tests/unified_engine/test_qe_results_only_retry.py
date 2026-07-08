import asyncio
import inspect
import pickle
from typing import Any

import pandas as pd
import pytest

from backend.services.quantevolver import qe_evolution_service as qes
from backend.services.quantevolver.qe_evolution_service import (
    AutoEvolutionScheduler,
    QE_LOOP_RETRY_MODE_RESULTS_ONLY,
)
from backend.services.quantevolver.results_only_retry import (
    ResultsOnlyGateError,
    collect_results_only_artifacts,
)


def _pickle(obj: Any) -> bytes:
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def _valid_prediction(scores: list[float] | None = None) -> pd.DataFrame:
    idx = pd.MultiIndex.from_product(
        [pd.to_datetime(["2026-01-02", "2026-01-03"]), ["000001.SZ", "000002.SZ", "000003.SZ"]],
        names=["datetime", "instrument"],
    )
    values = scores if scores is not None else [0.1, 0.2, 0.3, 0.15, 0.25, 0.35]
    return pd.DataFrame({"score": values}, index=idx)


def _valid_label() -> pd.DataFrame:
    idx = _valid_prediction().index
    return pd.DataFrame({"label": [0.01, 0.02, 0.03, 0.015, 0.025, 0.035]}, index=idx)


def _valid_report() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "return": [0.001, 0.002, -0.001],
            "bench": [0.0005, 0.001, -0.0005],
            "cost": [0.0001, 0.0001, 0.0001],
        },
        index=pd.to_datetime(["2026-01-02", "2026-01-03", "2026-01-04"]),
    )


class FakeResultsOnlyClient:
    def __init__(
        self,
        *,
        pred_bytes: bytes | None,
        label_bytes: bytes | None = None,
        metrics: dict[str, Any] | None = None,
        enhanced: dict[str, Any] | None = None,
    ) -> None:
        self.pred_bytes = pred_bytes
        self.label_bytes = label_bytes
        self.metrics = metrics
        self.enhanced = enhanced or {"source": "existing_enhanced"}
        self.create_and_run_loop_calls: list[tuple[Any, ...]] = []

    async def get_workspace_file(self, task_id: str, loop_id: str, file_path: str) -> dict[str, Any]:
        if file_path == "qe_current_recorder.json":
            return {
                "recorder_id": "rec-123",
                "experiment_id": "exp-456",
                "experiment_name": f"{task_id}/{loop_id}",
            }
        raise FileNotFoundError(file_path)

    async def download_workspace_file_bytes(self, task_id: str, loop_id: str, file_path: str) -> bytes:
        if file_path.endswith("/pred.pkl"):
            if self.pred_bytes is None:
                raise FileNotFoundError(file_path)
            return self.pred_bytes
        if file_path.endswith("/portfolio_analysis/report_normal_1day.pkl"):
            return _pickle(_valid_report())
        if file_path.endswith("/label.pkl") and self.label_bytes is not None:
            return self.label_bytes
        raise FileNotFoundError(file_path)

    async def get_loop_metrics(self, task_id: str, loop_id: str) -> dict[str, Any]:
        if self.metrics is None:
            raise FileNotFoundError("qlib_res.csv")
        return self.metrics

    async def get_enhanced_metrics(self, task_id: str, loop_id: str) -> dict[str, Any]:
        return self.enhanced

    async def create_and_run_loop(self, *args, **kwargs):  # pragma: no cover - assertion helper
        self.create_and_run_loop_calls.append((args, kwargs))
        raise AssertionError("results_only must not submit qrun/backtest")


def test_results_only_valid_artifacts_registers_without_qrun(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    client = FakeResultsOnlyClient(
        pred_bytes=_pickle(_valid_prediction()),
        metrics={"IC": 0.10536278494785283, "Rank IC": 0.10536278494785283},
    )
    registered: dict[str, Any] = {}

    def fake_upload(*, artifacts, task_id, loop_id, loop_index, node_id):
        manifest = {"run_key": f"{task_id}_L{loop_index}", "artifacts": [{"artifact_type": "prediction"}]}
        artifacts.attach_prediction_store_manifest(manifest)
        return manifest

    def fake_register(**kwargs):
        registered.update(kwargs)
        return f"{kwargs['task_id']}_L{kwargs['loop_index']}"

    monkeypatch.setattr(qes, "upload_results_only_prediction_store", fake_upload)
    monkeypatch.setattr(scheduler, "_register_results_only_loop", fake_register)
    monkeypatch.setattr(scheduler, "_archive_completed_loop_best_effort", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler, "_record_research_backtest_best_effort", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler, "recompute_custom_evo_task_status", lambda task_id: "completed")

    result = asyncio.run(
        scheduler._retry_loop_results_only(
            task_id="qe_task",
            loop_index=15,
            task={"task_id": "qe_task", "task_type": "custom_evo", "node_id": "node-a"},
            client=client,
            effective_node_id="node-a",
            config={"factor_list": ["alpha_a"], "model_id": "m", "strategy_id": "s", "model_params": {}},
        )
    )

    assert result["mode"] == QE_LOOP_RETRY_MODE_RESULTS_ONLY
    assert result["prediction_store_run_key"] == "qe_task_L15"
    assert registered["metrics"]["Rank IC"] == pytest.approx(0.10536278494785283)
    assert registered["metrics"]["Rank_IC"] == pytest.approx(0.10536278494785283)
    assert client.create_and_run_loop_calls == []


def test_results_only_missing_pred_fails_loud_and_does_not_register(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    client = FakeResultsOnlyClient(pred_bytes=None, metrics={"IC": 0.1, "Rank IC": 0.1})
    state = {"registered": 0, "uploaded": 0, "failures": []}

    monkeypatch.setattr(qes, "upload_results_only_prediction_store", lambda **kwargs: state.__setitem__("uploaded", 1))
    monkeypatch.setattr(scheduler, "_register_results_only_loop", lambda **kwargs: state.__setitem__("registered", 1))
    monkeypatch.setattr(
        scheduler,
        "_record_results_only_retry_failure",
        lambda **kwargs: state["failures"].append(str(kwargs["error"])),
    )
    monkeypatch.setattr(scheduler, "recompute_custom_evo_task_status", lambda task_id: "failed")

    with pytest.raises(ResultsOnlyGateError, match="reason_code=pred_missing"):
        asyncio.run(
            scheduler._retry_loop_results_only(
                task_id="qe_task",
                loop_index=15,
                task={"task_id": "qe_task", "task_type": "custom_evo", "node_id": "node-a"},
                client=client,
                effective_node_id="node-a",
                config={"model_params": {}},
            )
        )

    assert state["registered"] == 0
    assert state["uploaded"] == 0
    assert "reason_code=pred_missing" in state["failures"][0]


def test_results_only_all_nan_prediction_fails_loud():
    client = FakeResultsOnlyClient(
        pred_bytes=_pickle(_valid_prediction([float("nan")] * 6)),
        metrics={"IC": 0.1, "Rank IC": 0.1},
    )

    with pytest.raises(ResultsOnlyGateError, match="reason_code=pred_all_nan"):
        asyncio.run(
            collect_results_only_artifacts(
                client=client,
                task_id="qe_task",
                loop_id="Loop15",
                node_id="node-a",
            )
        )


def test_results_only_invalid_prediction_type_fails_loud():
    client = FakeResultsOnlyClient(
        pred_bytes=_pickle({"score": [0.1, 0.2]}),
        metrics={"IC": 0.1, "Rank IC": 0.1},
    )

    with pytest.raises(ResultsOnlyGateError, match="reason_code=pred_invalid_type"):
        asyncio.run(
            collect_results_only_artifacts(
                client=client,
                task_id="qe_task",
                loop_id="Loop15",
                node_id="node-a",
            )
        )


def test_results_only_recomputes_missing_ic_from_pred_and_label():
    client = FakeResultsOnlyClient(
        pred_bytes=_pickle(_valid_prediction()),
        label_bytes=_pickle(_valid_label()),
        metrics={"1day.excess_return_with_cost.annualized_return": 0.03},
    )

    artifacts = asyncio.run(
        collect_results_only_artifacts(
            client=client,
            task_id="qe_task",
            loop_id="Loop15",
            node_id="node-a",
        )
    )

    assert artifacts.metrics_source == "recomputed_from_pred_label"
    assert artifacts.metrics["IC"] == pytest.approx(1.0)
    assert artifacts.metrics["Rank IC"] == pytest.approx(1.0)


class _RegisterCursor:
    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state
        self._fetchone: Any = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        self.state["sql"].append(normalized)
        self.state["params"].append(params)
        if normalized.startswith("UPDATE qe_evolution_loops SET status = 'processing'"):
            if self.state["loop_status"] in {"failed", "cancelled", "canceled"}:
                self.state["loop_status"] = "processing"
                self._fetchone = ("qe_task_Loop15",)
            else:
                self._fetchone = None
            return
        if normalized.startswith("INSERT INTO qe_experiments"):
            experiment_id = params[0]
            self.state["experiments"][experiment_id] = {"metrics": params[11]}
            return
        if normalized.startswith("UPDATE qe_evolution_loops SET metrics_json"):
            self.state["loop_status"] = "completed"
            self.state["loop_experiment_id"] = params[2]

    def fetchone(self):
        return self._fetchone


class _RegisterConn:
    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def cursor(self, *args, **kwargs):
        return _RegisterCursor(self.state)

    def commit(self):
        self.state["commits"] += 1


def test_results_only_registration_is_idempotent_upsert(monkeypatch):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    state = {"loop_status": "failed", "experiments": {}, "sql": [], "params": [], "commits": 0}
    monkeypatch.setattr(qes, "get_conn", lambda: _RegisterConn(state))

    kwargs = {
        "task_id": "qe_task",
        "loop_index": 15,
        "evolution_loop_db_id": "qe_task_Loop15",
        "loop_id": "Loop15",
        "task": {"task_id": "qe_task", "base_experiment_id": "base", "node_id": "node-a"},
        "config": {"factor_list": ["alpha_a"], "model_id": "m", "strategy_id": "s", "model_params": {}},
        "metrics": {"IC": 0.1, "Rank IC": 0.1},
        "artifacts_summary": {"prediction": {"row_count": 6}},
    }

    assert scheduler._register_results_only_loop(**kwargs) == "qe_task_L15"
    state["loop_status"] = "failed"
    assert scheduler._register_results_only_loop(**kwargs) == "qe_task_L15"

    assert list(state["experiments"]) == ["qe_task_L15"]
    assert len(state["experiments"]) == 1
    assert any("ON CONFLICT (experiment_id) DO UPDATE" in sql for sql in state["sql"])


def test_results_only_helper_has_no_backtest_submission_path():
    source = inspect.getsource(AutoEvolutionScheduler._retry_loop_results_only)
    assert "BacktestExecutor" not in source
    assert "qrun" not in source.lower()
    assert "create_and_run_loop" not in source
    assert "executor.submit" not in source
