from pathlib import Path
import asyncio

from backend.routers import quantevolver_evolution as qe
from backend.services.quantevolver import qe_evolution_service


def test_evolution_router_no_longer_reads_worker_workspace_paths() -> None:
    source = Path(qe.__file__).read_text(encoding="utf-8")

    assert "QE_WORKSPACE_WIN" not in source
    assert "RDAGENT_WORKSPACE_WIN" not in source
    assert "positions_normal_1day.pkl" not in source
    assert "_find_positions_pickle" not in source


def test_summary_read_path_selects_cached_json_for_compact_projection() -> None:
    source = Path(qe_evolution_service.__file__).read_text(encoding="utf-8")
    detail_start = source.index("async def get_task_detail")
    comparison_start = source.index("def get_loop_comparison")
    payload_start = source.index("def get_loop_payload")
    detail_source = source[detail_start:comparison_start]
    comparison_source = source[comparison_start:payload_start]

    assert "config_json, metrics_json" in detail_source
    assert "config_json, metrics_json" in comparison_source
    assert "result[\"loops\"] = [compact_loop_row(loop_data)" in detail_source
    assert "loops = [compact_loop_row(dict(row))" in comparison_source


def test_position_enrichment_missing_metrics_is_read_only() -> None:
    enhanced = {"summary": {"IC": 0.0312}}

    returned, changed = qe._augment_enhanced_metrics_with_positions(
        "qe_read_only",
        "Loop1",
        1,
        enhanced,
    )

    assert returned is enhanced
    assert changed is False
    assert "position_summary" not in returned
    assert "holding_audit" not in returned
    assert "absolute_returns" not in returned


def test_position_enrichment_derives_compact_summary_from_cached_trades() -> None:
    enhanced = {
        "absolute_returns": {
            "final_cash": 100.0,
            "final_stock_value": 900.0,
            "final_total_value": 1000.0,
        },
        "return_curves": {"dates": ["2026-01-01", "2026-01-02"]},
        "stock_trades": {
            "AAA": [{"date": "2026-01-01", "type": "buy"}],
            "BBB": [{"date": "2026-01-02", "type": "buy"}],
        },
    }

    returned, changed = qe._augment_enhanced_metrics_with_positions(
        "qe_read_only",
        "Loop1",
        1,
        enhanced,
    )

    assert returned is not enhanced
    assert changed is True
    assert returned["position_summary"]["position_count_avg"] == 1.5
    assert returned["position_summary"]["position_count_max"] == 2
    assert returned["position_summary"]["final_cash"] == 100.0
    assert returned["position_summary"]["final_stock_value"] == 900.0


def test_loop_id_normalization_accepts_full_db_id_and_numeric_id() -> None:
    assert qe._normalize_rdagent_loop_id("qe_demo", "qe_demo_Loop3") == "Loop3"
    assert qe._normalize_rdagent_loop_id("qe_demo", "3") == "Loop3"
    assert qe._normalize_rdagent_loop_id("qe_demo", "loop03") == "Loop3"


def test_task_detail_does_not_update_db_for_optional_position_enrichment(monkeypatch) -> None:
    async def fake_get_task_detail(task_id: str):
        return {
            "task_id": task_id,
            "status": "completed",
            "current_loop": 1,
            "max_loops": 1,
            "loops": [
                {
                    "loop_id": f"{task_id}_Loop1",
                    "loop_index": 1,
                    "status": "completed",
                    "metrics_json": {"enhanced_metrics": {"summary": {"IC": 0.041}}},
                }
            ],
        }

    def fail_get_conn():
        raise AssertionError("task detail read path must not update DB for optional artifacts")

    def fake_augment(task_id, loop_id, loop_index, enhanced_metrics):
        enriched = dict(enhanced_metrics)
        enriched["artifact_unavailable"] = True
        return enriched, True

    monkeypatch.setattr(qe.scheduler, "get_task_detail", fake_get_task_detail)
    monkeypatch.setattr(qe, "_augment_enhanced_metrics_with_positions", fake_augment)
    monkeypatch.setattr(qe, "get_conn", fail_get_conn)

    result = asyncio.run(qe.get_evolution_task_detail("qe_read_only"))

    assert result["status"] == "success"
    assert result["data"]["task_id"] == "qe_read_only"
    loop = result["data"]["loops"][0]
    assert loop["metrics_json"]["enhanced_metrics"]["summary"]["IC"] == 0.041
    assert loop["metrics_json"]["enhanced_metrics"]["artifact_unavailable"] is True
