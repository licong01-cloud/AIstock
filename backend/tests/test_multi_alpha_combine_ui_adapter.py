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


def test_succeeded_run_missing_scheme_results_fails_loud() -> None:
    run = _run("run_missing")
    task_key = task_key_for_run(run)
    adapter = _adapter({"run_missing": {"run": run, "scheme_results": [], "loo": []}})

    with pytest.raises(CombineUIAdapterError) as excinfo:
        adapter.get_trajectory(task_key)

    assert excinfo.value.reason_code == "combine_ui_scheme_results_missing"
    assert excinfo.value.context["run_id"] == "run_missing"


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


def test_failed_runs_with_disjoint_schemes_do_not_break_task_render() -> None:
    # BUG-541: failed run 的 scheme_result 不应参与 common scheme 交集计算。
    # 一个 succeeded run(ic_weighted) + 两个 scheme 互不相交的 failed run。
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

    # 只取 succeeded run 的交集 → ic_weighted;不再抛 combine_ui_no_common_weighting_scheme
    assert result["available_schemes"] == ["ic_weighted"]
    assert result["scheme"] == "ic_weighted"


def test_all_runs_failed_falls_back_to_default_scheme_not_crash() -> None:
    # 全部 run 非 succeeded(无成功 run)→ 降级 default scheme,不崩。
    run_fail_a = _run("run_fa", status="failed")
    run_fail_b = _run("run_fb", status="failed")
    task_key = task_key_for_run(run_fail_a)
    adapter = _adapter({
        "run_fa": {"run": run_fail_a, "scheme_results": [_scheme("equal")], "loo": []},
        "run_fb": {"run": run_fail_b, "scheme_results": [_scheme("rank_fusion_rrf")], "loo": []},
    })

    result = adapter.get_trajectory(task_key)

    assert result["available_schemes"] == ["ic_weighted"]  # DEFAULT_SCHEME


def test_succeeded_runs_with_truly_disjoint_schemes_still_raise() -> None:
    # 真实异常不掩盖:两个 succeeded run scheme 无交集 → 仍显式报错。
    run_a = _run("run_sa", status="succeeded", oos_start="2024-07-02", oos_end="2025-05-31")
    run_b = _run("run_sb", status="succeeded", oos_start="2025-06-01", oos_end="2026-03-10")
    task_key = task_key_for_run(run_a)
    adapter = _adapter({
        "run_sa": {"run": run_a, "scheme_results": [_scheme("equal")], "loo": []},
        "run_sb": {"run": run_b, "scheme_results": [_scheme("ic_weighted")], "loo": []},
    })

    with pytest.raises(CombineUIAdapterError) as excinfo:
        adapter.get_trajectory(task_key)

    assert excinfo.value.reason_code == "combine_ui_no_common_weighting_scheme"
    assert set(excinfo.value.context["run_ids"]) == {"run_sa", "run_sb"}
