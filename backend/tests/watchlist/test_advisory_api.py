from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import advisory as advisory_router
from backend.services.advisory_program import AdvisoryProgramService, InMemoryAdvisoryProgramRepository


def _client() -> tuple[TestClient, AdvisoryProgramService]:
    app = FastAPI()
    app.include_router(advisory_router.router, prefix="/api/v1")
    service = AdvisoryProgramService(repository=InMemoryAdvisoryProgramRepository(), selection_service=None)
    app.dependency_overrides[advisory_router.get_advisory_program_service] = lambda: service
    return TestClient(app), service


def test_advisory_api_program_review_leaderboard_and_replay() -> None:
    client, _service = _client()

    created = client.post(
        "/api/v1/advisory/programs",
        json={
            "program_name": "Fusion advisory",
            "package_mode": "fusion_pool",
            "package_ids": ["pkg_a", "pkg_b"],
            "package_weights": {"pkg_a": 0.6, "pkg_b": 0.4},
            "target_count": 1,
            "status": "ENABLED",
        },
    )
    assert created.status_code == 200
    program = created.json()["program"]
    assert program["fusion_method"] == "weighted_rank_fusion"

    review = client.post(
        f"/api/v1/advisory/programs/{program['program_id']}/reviews/run",
        json={
            "trade_date": "2026-06-01",
            "candidates": [
                {
                    "symbol": "000001.SZ",
                    "rank": 1,
                    "score": 0.9,
                    "next_open_executable": 10,
                    "component_scores": {
                        "fusion_method": "weighted_rank_fusion",
                        "package_ranks": {"pkg_a": 1, "pkg_b": 2},
                        "package_raw_scores": {"pkg_a": 0.8, "pkg_b": 0.7},
                    },
                }
            ],
            "market_by_symbol": {"000001.SZ": {"next_open_executable": 10}},
        },
    )
    assert review.status_code == 200
    payload = review.json()["review"]
    assert payload["review_status"] == "SUCCEEDED"
    assert payload["active_pool"][0]["symbol"] == "000001.SZ"

    board = client.get("/api/v1/advisory/leaderboard")
    assert board.status_code == 200
    row = board.json()["leaderboard"][0]
    assert row["program_id"] == program["program_id"]
    assert "last_review_status" in row
    assert "eligible_episode_count" not in row
    assert "data_excluded_count" not in row

    replay = client.post(
        f"/api/v1/advisory/programs/{program['program_id']}/replay",
        json={
            "start_date": "2026-06-01",
            "end_date": "2026-06-02",
            "candidates_by_date": {
                "2026-06-01": [{"symbol": "000002.SZ", "rank": 1, "next_open_executable": 20}],
                "2026-06-02": [{"symbol": "000002.SZ", "rank": 1, "next_open_executable": 22}],
            },
            "market_by_date": {
                "2026-06-01": {"000002.SZ": {"next_open_executable": 20}},
                "2026-06-02": {"000002.SZ": {"next_open_executable": 22, "mark_price": 22}},
            },
        },
    )
    assert replay.status_code == 200
    assert replay.json()["replay"]["summary"]["win_rate"] == 1.0


def test_advisory_quality_report_endpoint_rejects_future_decision_inputs() -> None:
    client, _service = _client()

    response = client.post(
        "/api/v1/advisory/quality-report",
        json={
            "records": [
                {
                    "code": "000001.SZ",
                    "trade_date": "2026-06-01",
                    "current_price": 10,
                    "entry_band_json": {"max_buy_price": 10.1},
                    "action": "HOLD",
                    "reason_code": "HOLD",
                    "decision_input_json": {"forward_return_bps": 100},
                }
            ],
            "min_bucket_size": 1,
        },
    )

    assert response.status_code == 400
    assert "future outcome fields" in response.json()["detail"]["message"]
