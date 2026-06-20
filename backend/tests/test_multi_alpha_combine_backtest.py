from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from backend.services.multi_alpha.combine_backtest import (
    COMBINE_BACKTEST_CONFIRM,
    InMemoryCombineBacktestRepository,
    MultiAlphaCombineBacktestError,
    MultiAlphaCombineBacktestService,
    maybe_upload_combined_prediction,
    parse_request,
)
from backend.services.multi_alpha.panels import MultiAlphaPanelBuilder


DATES = [pd.Timestamp("2026-01-02").date(), pd.Timestamp("2026-01-03").date(), pd.Timestamp("2026-01-04").date()]
INSTRUMENTS = ["A", "B", "C"]


class FakeExecutor:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def execute_pred_backtest(self, *, workspace: Path, pred_pkl: Path, node_id: str, backtest_config: dict) -> dict:
        frame = pd.read_pickle(pred_pkl).reset_index()
        score_sum = float(frame["score"].sum())
        name = workspace.name
        metrics = {
            "cagr": 1.0 + score_sum / 1000.0,
            "max_drawdown": -0.15,
            "sharpe": 2.0 + score_sum / 1000.0,
            "calmar": 6.0 + score_sum / 1000.0,
            "topk_return_20": 0.05,
            "topk_hit_rate_20": 0.6,
            "turnover": 20.0,
            "name": name,
        }
        self.calls.append({"workspace": workspace, "node_id": node_id, "metrics": metrics})
        (workspace / "qlib_results_enhanced.json").write_text(json.dumps({"absolute_returns": metrics}), encoding="utf-8")
        return metrics


class FakeCapacityChecker:
    def __init__(self, active_count: int = 0) -> None:
        self.active_count = active_count
        self.calls: list[dict] = []
        self.releases: list[dict] = []

    def ensure_slot_available(self, *, node_id: str, limit: int, run_id: str, backtest_name: str) -> dict:
        self.calls.append({"node_id": node_id, "limit": limit, "run_id": run_id, "backtest_name": backtest_name})
        if self.active_count >= limit:
            raise MultiAlphaCombineBacktestError(
                "node saturated",
                reason_code="node_capacity_exhausted",
                context={"node_id": node_id, "limit": limit, "active_count": self.active_count},
            )
        return {"node_id": node_id, "limit": limit, "active_count": self.active_count, "available": True}

    def release_slot(self, capacity: dict) -> None:
        self.releases.append(capacity)


def _pred(offset: float) -> pd.DataFrame:
    rows = []
    for d_idx, trade_date in enumerate(DATES):
        for i_idx, instrument in enumerate(INSTRUMENTS):
            rows.append({"trade_date": trade_date, "instrument": instrument, "score": offset + d_idx + i_idx})
    return pd.DataFrame(rows)


def _label() -> pd.DataFrame:
    rows = []
    for d_idx, trade_date in enumerate(DATES):
        for i_idx, instrument in enumerate(INSTRUMENTS):
            rows.append(
                {
                    "trade_date": trade_date,
                    "instrument": instrument,
                    "forward_return": (0.01 * (d_idx + i_idx + 1)) + (0.003 if d_idx == 1 and instrument == "C" else 0.0),
                }
            )
    return pd.DataFrame(rows)


def _payload() -> dict:
    return {
        "roster": [
            {"leg_id": "leg_a", "seed_run_ids": ["a1", "a2"]},
            {"leg_id": "leg_b", "seed_run_ids": ["b1", "b2"]},
        ],
        "oos_start": "2026-01-02",
        "oos_end": "2026-01-04",
        "weighting_schemes": ["equal", "ic_weighted", "risk_parity"],
        "normalize_method": "rank",
        "walk_forward": {"enabled": True, "window": 2, "min_periods": 2},
        "backtest_config": {"node_id": "wsl2-5080", "node_parallelism": {"wsl2-5080": 2}},
        "baseline_leg_id": "leg_a",
        "topk": 1,
        "min_date_coverage": 1.0,
        "run_async": False,
    }


def _payload_three_legs() -> dict:
    payload = _payload()
    payload["roster"] = [
        {"leg_id": "leg_a", "seed_run_ids": ["a1", "a2"]},
        {"leg_id": "leg_b", "seed_run_ids": ["b1", "b2"]},
        {"leg_id": "leg_c", "seed_run_ids": ["c1", "c2"]},
    ]
    payload["weighting_schemes"] = ["equal"]
    return payload


def _service(
    tmp_path: Path,
    *,
    capacity_checker: FakeCapacityChecker | None = None,
) -> tuple[MultiAlphaCombineBacktestService, InMemoryCombineBacktestRepository, FakeExecutor, FakeCapacityChecker]:
    preds = {
        "a1": _pred(1.0),
        "a2": _pred(2.0),
        "b1": _pred(-1.0),
        "b2": _pred(-2.0),
        "c1": _pred(3.0),
        "c2": _pred(4.0),
    }
    labels = {run_id: _label() for run_id in preds}
    repo = InMemoryCombineBacktestRepository()
    executor = FakeExecutor()
    checker = capacity_checker or FakeCapacityChecker()
    service = MultiAlphaCombineBacktestService(
        panel_builder=MultiAlphaPanelBuilder(
            prediction_loader=lambda run_id: preds[run_id],
            label_loader=lambda run_id: labels[run_id],
        ),
        executor=executor,
        repository=repo,
        capacity_checker=checker,
        workspace_root=tmp_path / "macb",
        clock=lambda: datetime(2026, 1, 5, tzinfo=timezone.utc),
    )
    return service, repo, executor, checker


def test_combine_backtest_runs_ic_weighted_and_risk_parity_and_persists(tmp_path: Path) -> None:
    service, repo, executor, checker = _service(tmp_path)

    result = service.submit_run(_payload(), run_async=False)
    run = service.get_run(result["run_id"])

    assert run["run"]["status"] == "succeeded"
    schemes = {row["weighting_scheme"]: row for row in run["scheme_results"]}
    assert set(schemes) == {"equal", "ic_weighted", "risk_parity"}
    assert all(not row["skipped"] for row in schemes.values())
    assert schemes["ic_weighted"]["sharpe"] is not None
    assert schemes["risk_parity"]["calmar"] is not None
    assert len(run["loo"]) == 0
    assert {call["node_id"] for call in executor.calls} == {"wsl2-5080"}
    assert {call["limit"] for call in checker.calls} == {2}
    assert len(checker.releases) == len(executor.calls)
    assert repo.runs[result["run_id"]]["roster_hash"]


def test_combine_backtest_persists_loo_for_three_or_more_legs(tmp_path: Path) -> None:
    service, _repo, _executor, _checker = _service(tmp_path)

    result = service.submit_run(_payload_three_legs(), run_async=False)
    run = service.get_run(result["run_id"])

    assert len(run["loo"]) == 3
    assert {row["dropped_leg_id"] for row in run["loo"]} == {"leg_a", "leg_b", "leg_c"}


def test_combine_backtest_deterministic_combined_prediction(tmp_path: Path) -> None:
    service, _repo, _executor, _checker = _service(tmp_path)
    first = service.submit_run(_payload(), run_async=False)
    second = service.submit_run(_payload(), run_async=False)

    first_pred = pd.read_pickle(tmp_path / "macb" / first["run_id"] / "combined_equal" / "combined_prediction.pkl")
    second_pred = pd.read_pickle(tmp_path / "macb" / second["run_id"] / "combined_equal" / "combined_prediction.pkl")

    pd.testing.assert_frame_equal(first_pred, second_pred)


def test_parse_request_respects_confirmation_constant_name() -> None:
    assert COMBINE_BACKTEST_CONFIRM == "MULTI_ALPHA_COMBINE_BACKTEST_RUN"
    request = parse_request(_payload())
    assert request.weighting_schemes == ("equal", "ic_weighted", "risk_parity")


def test_node_parallelism_must_cover_selected_node(tmp_path: Path) -> None:
    service, _repo, _executor, _checker = _service(tmp_path)
    payload = _payload()
    payload["backtest_config"] = {"node_id": "rdagent-node1", "node_parallelism": {"wsl2-5080": 2}}

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        service.submit_run(payload, run_async=False)

    assert excinfo.value.reason_code == "node_parallelism_missing_node"


def test_node_capacity_exhaustion_fails_loud_before_executor(tmp_path: Path) -> None:
    service, repo, executor, _checker = _service(tmp_path, capacity_checker=FakeCapacityChecker(active_count=2))

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        service.submit_run(_payload(), run_async=False)

    assert excinfo.value.reason_code == "node_capacity_exhausted"
    run_id = next(iter(repo.runs))
    assert repo.runs[run_id]["status"] == "failed"
    assert executor.calls == []


def test_prediction_store_upload_is_explicit_and_fail_loud(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pred_path = tmp_path / "combined_prediction.pkl"
    pd.DataFrame({"score": [1.0]}).to_pickle(pred_path)
    monkeypatch.setenv("AISTOCK_PREDICTION_STORE_UPLOAD_URL", "http://backend/api/v1/prediction-store/artifacts/{run_key}")

    class Response:
        status_code = 200
        text = "{}"

        @staticmethod
        def json() -> dict:
            return {"status": "success", "data": {"manifest": {"artifacts": [{"artifact_type": "prediction"}]}}}

    def fake_post(url: str, **kwargs):
        assert url.endswith("/macb_123_combined_equal")
        assert "files" in kwargs and "pred" in kwargs["files"]
        return Response()

    monkeypatch.setattr("backend.services.multi_alpha.combine_backtest.requests.post", fake_post)

    manifest = maybe_upload_combined_prediction(
        run_id="macb_123",
        backtest_name="combined_equal",
        pred_pkl=pred_path,
        node_id="wsl2-5080",
        backtest_config={"node_id": "wsl2-5080"},
    )

    assert manifest == {"artifacts": [{"artifact_type": "prediction"}]}
