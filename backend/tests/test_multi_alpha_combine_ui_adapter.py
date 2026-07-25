from __future__ import annotations

from typing import Any

import pytest

from backend.services.multi_alpha.combine_ui_adapter import (
    CombineUIAdapterError,
    MultiAlphaCombineUIAdapter,
    task_key_for_run,
)


ROSTER = [{"leg_id": "a1_plus3_LSTM_h20"}, {"leg_id": "new_FUNDGROWTH_h20"}]
WF = {"enabled": True, "window": 60, "min_periods": 20, "expanding": False}


def _run(
    run_id: str,
    *,
    roster_hash: str = "hash_a",
    normalize_method: str = "rank",
    status: str = "succeeded",
    topk: int = 25,
    oos_start: str = "2024-07-02",
    oos_end: str = "2026-03-10",
    created_at: str = "2026-06-26T01:00:00",
    reason: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": run_id,
        "roster_hash": roster_hash,
        "roster_json": ROSTER,
        "oos_start": oos_start,
        "oos_end": oos_end,
        "normalize_method": normalize_method,
        "walk_forward_json": WF,
        "backtest_config_json": {"topk": topk, "strategy_id": "V25_1_SMALL_CAP"},
        "baseline_leg_id": "a1_plus3_LSTM_h20",
        "status": status,
        "reason": reason,
        "created_at": created_at,
        "updated_at": None,
    }


def _scheme(
    scheme: str,
    *,
    cagr: float = 1.0,
    sharpe: float = 2.0,
    max_drawdown: float = -0.15,
    calmar: float = 6.0,
    turnover: float = 10.0,
) -> dict[str, Any]:
    return {
        "weighting_scheme": scheme,
        "weights_json": {"leg_weights": {"a1_plus3_LSTM_h20": 0.6, "new_FUNDGROWTH_h20": 0.4}},
        "per_window_weights_json": [{"start": "2024-07-02", "weights": {"a1_plus3_LSTM_h20": 0.6}}],
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "calmar": calmar,
        "topk_return_20": 0.0631,
        "topk_hit_rate_20": 0.6471,
        "turnover": turnover,
    }


def _loo(scheme: str = "ic_weighted") -> list[dict[str, Any]]:
    return [
        {
            "weighting_scheme": scheme,
            "dropped_leg_id": "a1_plus3_LSTM_h20",
            "marginal_sharpe": 0.25,
            "marginal_calmar": 0.31,
            "marginal_cagr": 0.13,
        },
        {
            "weighting_scheme": scheme,
            "dropped_leg_id": "new_FUNDGROWTH_h20",
            "marginal_sharpe": -0.08,
            "marginal_calmar": -0.12,
            "marginal_cagr": -0.19,
        },
    ]


class FakeCombineUIRepository:
    def __init__(self, bundles: dict[str, dict[str, Any]]) -> None:
        self.bundles = bundles

    def list_run_headers(self) -> list[dict[str, Any]]:
        return [dict(bundle["run"]) for bundle in self.bundles.values()]

    def get_run_bundle(self, run_id: str) -> dict[str, Any] | None:
        bundle = self.bundles.get(run_id)
        if bundle is None:
            return None
        return {
            "run": dict(bundle["run"]),
            "scheme_results": [dict(row) for row in bundle.get("scheme_results", [])],
            "loo": [dict(row) for row in bundle.get("loo", [])],
        }


def _adapter(bundles: dict[str, dict[str, Any]]) -> MultiAlphaCombineUIAdapter:
    return MultiAlphaCombineUIAdapter(repository=FakeCombineUIRepository(bundles))


def test_list_tasks_groups_before_pagination_and_maps_task_status() -> None:
    run_a = _run("run_a", topk=25, created_at="2026-06-26T01:00:00")
    run_b = _run("run_b", topk=50, created_at="2026-06-26T02:00:00")
    run_other = _run("run_other", roster_hash="hash_b", topk=25, created_at="2026-06-26T03:00:00")
    adapter = _adapter({
        "run_a": {"run": run_a, "scheme_results": [_scheme("ic_weighted")]},
        "run_b": {"run": run_b, "scheme_results": [_scheme("ic_weighted")]},
        "run_other": {"run": run_other, "scheme_results": [_scheme("ic_weighted")]},
    })

    page = adapter.list_tasks(limit=1, offset=0)

    assert page["total"] == 2
    assert page["count"] == 1
    assert page["tasks"][0]["max_loops"] in {1, 2}
    assert page["tasks"][0]["task_type"] == "multi_alpha_combine"


def test_trajectory_maps_metrics_null_ic_sota_loo_and_partial_failed() -> None:
    run_top25 = _run("run_top25", topk=25, created_at="2026-06-26T01:00:00")
    run_top50 = _run("run_top50", topk=50, created_at="2026-06-26T02:00:00")
    run_failed = _run(
        "run_failed",
        status="running",
        topk=100,
        created_at="2026-06-26T03:00:00",
        reason={"logical_status": "partial_failed", "phase": "loo"},
    )
    task_key = task_key_for_run(run_top25)
    adapter = _adapter({
        "run_top25": {"run": run_top25, "scheme_results": [_scheme("ic_weighted", cagr=1.00, sharpe=2.0)], "loo": _loo()},
        "run_top50": {"run": run_top50, "scheme_results": [_scheme("ic_weighted", cagr=1.10, sharpe=1.8)], "loo": _loo()},
        "run_failed": {"run": run_failed, "scheme_results": [], "loo": []},
    })

    result = adapter.get_trajectory(task_key, scheme="ic_weighted")
    loops = result["trajectory"]

    assert result["scheme"] == "ic_weighted"
    assert [loop["config_json"]["strategy_params"]["topk"] for loop in loops] == [25, 50, 100]
    assert [loop["status"] for loop in loops] == ["completed", "completed", "failed"]
    assert [loop["is_sota"] for loop in loops] == [False, True, False]
    assert loops[1]["metrics_json"]["annualized_return"] == pytest.approx(1.10)
    assert loops[1]["metrics_json"]["cagr"] == pytest.approx(1.10)
    assert loops[1]["metrics_json"]["max_drawdown"] == pytest.approx(-0.15)
    assert loops[1]["metrics_json"]["IC"] is None
    assert loops[1]["metrics_json"]["ICIR"] is None
    assert loops[1]["metrics_json"]["Rank_IC"] is None
    assert loops[0]["loo"][1]["is_negative_contributor"] is True
    assert loops[2]["metrics_json"]["annualized_return"] is None


def test_default_scheme_fallback_is_explicit_warning() -> None:
    run = _run("run_equal")
    task_key = task_key_for_run(run)
    adapter = _adapter({"run_equal": {"run": run, "scheme_results": [_scheme("equal", cagr=0.9)], "loo": _loo("equal")}})

    result = adapter.get_task(task_key)

    assert result["scheme"] == "equal"
    assert result["scheme_warning"]["reason_code"] == "combine_ui_default_scheme_unavailable"
    assert result["available_schemes"] == ["equal"]


def test_requested_missing_scheme_fails_loud() -> None:
    run = _run("run_equal")
    task_key = task_key_for_run(run)
    adapter = _adapter({"run_equal": {"run": run, "scheme_results": [_scheme("equal")], "loo": []}})

    with pytest.raises(CombineUIAdapterError) as excinfo:
        adapter.get_trajectory(task_key, scheme="ic_weighted")

    assert excinfo.value.reason_code == "combine_ui_weighting_scheme_not_found"
    assert excinfo.value.context["requested_scheme"] == "ic_weighted"


def test_succeeded_run_missing_scheme_results_stays_visible_as_explicit_skip() -> None:
    run = _run("run_missing")
    task_key = task_key_for_run(run)
    adapter = _adapter({"run_missing": {"run": run, "scheme_results": [], "loo": []}})

    result = adapter.get_trajectory(task_key)

    assert result["available_schemes"] == ["ic_weighted"]
    assert result["trajectory"][0]["metrics_json"]["annualized_return"] is None
    assert result["trajectory"][0]["reason"] == {}


def test_running_run_without_scheme_result_uses_placeholder_not_silent_success() -> None:
    run = _run("run_running", status="running", reason={"phase": "backtest"})
    task_key = task_key_for_run(run)
    adapter = _adapter({"run_running": {"run": run, "scheme_results": [], "loo": []}})

    result = adapter.get_task(task_key)
    loop = result["loops"][0]

    assert result["task"]["status"] == "running"
    assert result["task"]["phase"] == "backtest"
    assert result["scheme"] == "ic_weighted"
    assert loop["status"] == "running"
    assert loop["metrics_json"]["annualized_return"] is None
    assert loop["config_json"]["weights_json"] == {}


def test_task_progress_uses_latest_running_run_heartbeat() -> None:
    run_old = _run(
        "run_running_old",
        status="running",
        topk=25,
        created_at="2026-06-26T01:00:00+00:00",
        reason={
            "phase": "scheme_combined",
            "heartbeat_at": "2026-06-26T01:10:00+00:00",
            "progress": {"completed": 1, "total": 4, "pending": 3},
        },
    )
    run_old["updated_at"] = "2026-06-26T01:10:00+00:00"
    run_new = _run(
        "run_running_new",
        status="running",
        topk=50,
        created_at="2026-06-26T02:00:00+00:00",
        reason={
            "phase": "backtests_running",
            "heartbeat_at": "2026-06-26T02:20:00+00:00",
            "progress": {"completed": 3, "total": 4, "pending": 1},
        },
    )
    run_new["updated_at"] = "2026-06-26T02:20:00+00:00"
    task_key = task_key_for_run(run_old)
    adapter = _adapter({
        "run_running_old": {"run": run_old, "scheme_results": [], "loo": []},
        "run_running_new": {"run": run_new, "scheme_results": [], "loo": []},
    })

    task = adapter.get_task(task_key)["task"]

    assert task["phase"] == "backtests_running"
    assert task["progress"] == {"completed": 3, "total": 4, "pending": 1}
    assert task["heartbeat_at"] == "2026-06-26T02:20:00+00:00"


def test_task_progress_distinguishes_remote_running_from_waiting_capacity() -> None:
    bundles: dict[str, dict[str, Any]] = {}
    for index in range(10):
        run = _run(
            f"run_recovery_{index}",
            status="running",
            topk=25 + index,
            created_at=f"2026-07-25T16:{index:02d}:00+00:00",
            reason={"phase": "recovery_children_published"},
        )
        run.update(
            {
                "attempt_count": 1,
                "remote_running_count": 1 if index < 4 else 0,
                "queued_count": 0 if index < 4 else 1,
                "reconciling_count": 0,
                "terminal_attempt_count": 0,
            }
        )
        bundles[run["id"]] = {"run": run, "scheme_results": [], "loo": []}
    task = _adapter(bundles).list_tasks()["tasks"][0]

    assert task["status"] == "running"
    assert task["running_count"] == 4
    assert task["queued_count"] == 6
    assert task["reconciling_count"] == 0
    assert task["progress"] == {
        "completed": 0,
        "total": 10,
        "pending": 10,
        "running": 4,
        "queued": 6,
        "reconciling": 0,
    }


def test_latest_partial_recovered_successor_is_task_truth_after_failed_history() -> None:
    failed = _run(
        "run_failed_history",
        status="failed",
        created_at="2026-07-25T16:00:00+00:00",
    )
    recovered = _run(
        "run_recovered_latest",
        status="partial_recovered",
        created_at="2026-07-25T20:00:00+00:00",
    )
    for run in (failed, recovered):
        run.update(
            {
                "attempt_count": 1,
                "remote_running_count": 0,
                "queued_count": 0,
                "reconciling_count": 0,
                "terminal_attempt_count": 1,
            }
        )
    adapter = _adapter(
        {
            failed["id"]: {"run": failed, "scheme_results": [], "loo": []},
            recovered["id"]: {
                "run": recovered,
                "scheme_results": [_scheme("equal")],
                "loo": _loo("equal"),
            },
        }
    )

    task = adapter.get_task(task_key_for_run(recovered), scheme="equal")

    assert task["task"]["status"] == "partial_recovered"
    recovered_loop = next(
        loop for loop in task["loops"] if loop["run_id"] == recovered["id"]
    )
    assert recovered_loop["status"] == "completed"
    assert recovered_loop["raw_status"] == "partial_recovered"


def test_failed_runs_with_disjoint_schemes_do_not_break_task_render() -> None:
    # Failed/partial run 已持久化的 scheme 结果也是研究证据，必须保留并可切换。
    run_ok = _run("run_ok", status="succeeded")
    run_fail_a = _run("run_fail_a", status="failed", oos_start="2024-07-02", oos_end="2025-05-31")
    run_fail_b = _run("run_fail_b", status="failed", oos_start="2025-06-01", oos_end="2026-03-10")
    task_key = task_key_for_run(run_ok)
    adapter = _adapter({
        "run_ok": {"run": run_ok, "scheme_results": [_scheme("ic_weighted")], "loo": _loo()},
        "run_fail_a": {"run": run_fail_a, "scheme_results": [_scheme("equal"), _scheme("orthogonality_aware")], "loo": []},
        "run_fail_b": {"run": run_fail_b, "scheme_results": [_scheme("rank_fusion_borda"), _scheme("rank_fusion_rrf")], "loo": []},
    })

    result = adapter.get_trajectory(task_key)

    assert result["available_schemes"] == [
        "ic_weighted",
        "orthogonality_aware",
        "equal",
        "rank_fusion_rrf",
        "rank_fusion_borda",
    ]
    assert result["scheme"] == "ic_weighted"


def test_all_runs_failed_falls_back_to_default_scheme_not_crash() -> None:
    # 全部 run failed 时仍展示真实持久化 scheme，而不是伪造默认 scheme。
    run_fail_a = _run("run_fa", status="failed")
    run_fail_b = _run("run_fb", status="failed")
    task_key = task_key_for_run(run_fail_a)
    adapter = _adapter({
        "run_fa": {"run": run_fail_a, "scheme_results": [_scheme("equal")], "loo": []},
        "run_fb": {"run": run_fail_b, "scheme_results": [_scheme("rank_fusion_rrf")], "loo": []},
    })

    result = adapter.get_trajectory(task_key)

    assert result["available_schemes"] == ["equal", "rank_fusion_rrf"]
    assert result["scheme"] == "equal"


def test_succeeded_runs_with_disjoint_schemes_render_sparse_comparison() -> None:
    run_a = _run("run_sa", status="succeeded", oos_start="2024-07-02", oos_end="2025-05-31")
    run_b = _run("run_sb", status="succeeded", oos_start="2025-06-01", oos_end="2026-03-10")
    task_key = task_key_for_run(run_a)
    adapter = _adapter({
        "run_sa": {"run": run_a, "scheme_results": [_scheme("equal")], "loo": []},
        "run_sb": {"run": run_b, "scheme_results": [_scheme("ic_weighted")], "loo": []},
    })

    result = adapter.get_trajectory(task_key)

    assert result["available_schemes"] == ["ic_weighted", "equal"]
    assert result["scheme"] == "ic_weighted"
    assert result["trajectory"][0]["metrics_json"]["annualized_return"] is None
    assert result["trajectory"][1]["metrics_json"]["annualized_return"] == pytest.approx(1.0)


def test_partial_failed_run_keeps_computable_scheme_progress_and_sota() -> None:
    run_ok = _run("run_ok", status="succeeded", topk=25)
    run_partial = _run(
        "run_partial",
        status="failed",
        topk=50,
        reason={
            "logical_status": "partial_failed",
            "phase": "completed",
            "heartbeat_at": "2026-06-26T03:00:00+00:00",
            "progress": {"completed": 3, "total": 4, "pending": 0},
        },
    )
    task_key = task_key_for_run(run_ok)
    adapter = _adapter({
        "run_ok": {"run": run_ok, "scheme_results": [_scheme("equal", cagr=0.8)], "loo": []},
        "run_partial": {
            "run": run_partial,
            "scheme_results": [
                _scheme("equal", cagr=1.2),
                {"weighting_scheme": "ic_weighted", "skipped": True, "skipped_reason": "no positive weights"},
            ],
            "loo": [],
        },
    })

    result = adapter.get_task(task_key, scheme="equal")
    partial_loop = next(loop for loop in result["loops"] if loop["run_id"] == "run_partial")

    assert result["task"]["status"] == "partial_failed"
    assert result["task"]["partial_failed_count"] == 1
    assert partial_loop["raw_status"] == "partial_failed"
    assert partial_loop["progress"] == {"completed": 3, "total": 4, "pending": 0}
    assert partial_loop["is_sota"] is True
    assert partial_loop["scheme_results"][1]["skipped_reason"] == "no positive weights"
