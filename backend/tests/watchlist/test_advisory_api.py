from __future__ import annotations

from datetime import date

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import advisory as advisory_router
from backend.services.advisory_program import AdvisoryProgramService, InMemoryAdvisoryProgramRepository
from backend.services.selection_center.models import SelectionCandidate, SelectionMode, SelectionRun, SelectionRunStatus


class FakeTradingCalendar:
    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        return [day for day in [date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 8)] if start_date <= day <= end_date]

    def next_trading_day(self, anchor_date: date, *, inclusive: bool = False) -> date:
        start = anchor_date if inclusive else anchor_date.fromordinal(anchor_date.toordinal() + 1)
        eligible = self.list_trading_days(start, date(2026, 12, 31))
        return eligible[0] if eligible else start


def _client(selection_service=None) -> tuple[TestClient, AdvisoryProgramService]:
    app = FastAPI()
    app.include_router(advisory_router.router, prefix="/api/v1")
    service = AdvisoryProgramService(
        repository=InMemoryAdvisoryProgramRepository(),
        selection_service=selection_service,
        calendar_provider=FakeTradingCalendar(),
    )
    app.dependency_overrides[advisory_router.get_advisory_program_service] = lambda: service
    return TestClient(app), service


def test_advisory_api_program_review_leaderboard_and_replay() -> None:
    client, _service = _client()

    created = client.post(
        "/api/v1/advisory/programs",
        json={
            "program_name": "Native multi-alpha parent advisory",
            "package_mode": "single_package",
            "package_ids": ["pkg_multi_alpha_parent"],
            "package_weights": {"pkg_multi_alpha_parent": 1.0},
            "target_count": 1,
            "status": "ENABLED",
        },
    )
    assert created.status_code == 200
    program = created.json()["program"]
    assert program["fusion_method"] is None

    bindings = client.get(f"/api/v1/advisory/programs/{program['program_id']}/bindings")
    assert bindings.status_code == 200
    assert len(bindings.json()["bindings"]) == 1
    assert bindings.json()["bindings"][0]["activation_status"] == "ACTIVE"

    review = client.post(
        f"/api/v1/advisory/programs/{program['program_id']}/reviews/run",
        json={
            "trade_date": "2026-06-01",
            "candidates": [
                {
                    "symbol": "000001.SZ",
                    "stock_name": "平安银行",
                    "rank": 1,
                    "score": 0.9,
                    "next_open_executable": 10,
                    "component_scores": {"multi_alpha_artifact_id": "artifact_parent_v1"},
                }
            ],
            "market_by_symbol": {"000001.SZ": {"next_open_executable": 10}},
        },
    )
    assert review.status_code == 200
    payload = review.json()["review"]
    assert payload["review_status"] == "SUCCEEDED"
    assert payload["active_pool"][0]["symbol"] == "000001.SZ"
    assert payload["active_pool"][0]["stock_name"] == "平安银行"
    assert payload["binding_version_id"] == bindings.json()["bindings"][0]["binding_version_id"]
    assert payload["review_run_id"].startswith("advrun_")
    assert payload["list_version_id"].startswith("advlv_")
    assert payload["change_summary"]["entered_count"] == 1
    assert payload["list_items"][0]["action"] == "ENTER"
    assert payload["list_items"][0]["stock_name"] == "平安银行"
    assert payload["list_items"][0]["operation_advice_json"]["advice_type"] == "ENTER"

    second_review = client.post(
        f"/api/v1/advisory/programs/{program['program_id']}/reviews/run",
        json={
            "trade_date": "2026-06-02",
            "candidates": [
                {
                    "symbol": "000001.SZ",
                    "stock_name": "平安银行",
                    "rank": 1,
                    "score": 0.8,
                    "next_open_executable": 11,
                    "component_scores": {"multi_alpha_artifact_id": "artifact_parent_v1"},
                }
            ],
            "market_by_symbol": {"000001.SZ": {"next_open_executable": 11, "mark_price": 11}},
        },
    )
    assert second_review.status_code == 200
    assert second_review.json()["review"]["change_summary"]["held_count"] == 1
    assert second_review.json()["review"]["change_summary"]["previous_list_version_id"] == payload["list_version_id"]

    versions = client.get(f"/api/v1/advisory/programs/{program['program_id']}/list-versions")
    assert versions.status_code == 200
    assert [row["version_status"] for row in versions.json()["list_versions"]] == ["PUBLISHED", "PUBLISHED"]

    detail = client.get(f"/api/v1/advisory/list-versions/{payload['list_version_id']}")
    assert detail.status_code == 200
    assert detail.json()["list_version"]["list_version_id"] == payload["list_version_id"]
    assert detail.json()["items"][0]["symbol"] == "000001.SZ"
    assert detail.json()["items"][0]["stock_name"] == "平安银行"

    review_page = client.get(f"/api/v1/advisory/programs/{program['program_id']}/reviews?limit=1&offset=1")
    assert review_page.status_code == 200
    review_page_payload = review_page.json()
    assert review_page_payload["total_count"] == 2
    assert review_page_payload["limit"] == 1
    assert review_page_payload["offset"] == 1
    assert [row["trade_date"] for row in review_page_payload["reviews"]] == ["2026-06-01"]
    assert review_page_payload["reviews"][0]["stock_name"] == "平安银行"

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


def test_advisory_api_preview_auto_reviews_without_selection_run_id() -> None:
    class FakeSelectionService:
        def __init__(self) -> None:
            self.runtime_config = None

        def run_packages(self, *, package_ids, mode, trade_date, data_source, runtime_config):
            assert mode == SelectionMode.SINGLE_PACKAGE
            self.runtime_config = dict(runtime_config)
            return SelectionRun(
                mode=mode,
                trade_date=trade_date,
                data_source=data_source,
                package_ids=list(package_ids),
                runtime_config=dict(runtime_config),
                status=SelectionRunStatus.SUCCEEDED,
                aggregate_results=[
                    SelectionCandidate(
                        symbol="000001.SZ",
                        rank=1,
                        score=0.9,
                        selection_entry_price=10.0,
                        reference_price=10.0,
                    )
                ],
            )

    fake_selection = FakeSelectionService()
    client, _service = _client(selection_service=fake_selection)
    created = client.post(
        "/api/v1/advisory/programs",
        json={
            "program_name": "Auto review",
            "package_mode": "single_package",
            "package_ids": ["pkg_a"],
            "target_count": 20,
            "status": "ENABLED",
        },
    )
    assert created.status_code == 200
    program_id = created.json()["program"]["program_id"]

    review = client.post(f"/api/v1/advisory/programs/{program_id}/reviews/preview", json={"trade_date": "2026-06-08"})

    assert review.status_code == 200
    assert review.json()["review"]["review_status"] == "SUCCEEDED"
    assert fake_selection.runtime_config["selection_artifact_config"]["auto_generate"] is True
    assert fake_selection.runtime_config["selection_artifact_config"]["pit_mode"] == "PREVIOUS_TRADING_DAY_CLOSE"
    assert fake_selection.runtime_config["runtime_profile"]["selection"]["top_k"] == 40


def test_advisory_api_accepts_target_date_and_selection_cutoff_without_manual_run_id() -> None:
    class FakeSelectionService:
        def __init__(self) -> None:
            self.runtime_config = None
            self.trade_date = None

        def run_packages(self, *, package_ids, mode, trade_date, data_source, runtime_config):
            self.trade_date = trade_date
            self.runtime_config = dict(runtime_config)
            return SelectionRun(
                mode=mode,
                trade_date=trade_date,
                data_source=data_source,
                package_ids=list(package_ids),
                runtime_config={
                    **runtime_config,
                    "point_in_time_context": {
                        "reference_price_trade_date": runtime_config["advisory_date_context"]["selection_as_of_trade_date"],
                    },
                },
                status=SelectionRunStatus.SUCCEEDED,
                aggregate_results=[
                    SelectionCandidate(
                        symbol="000001.SZ",
                        rank=1,
                        score=0.9,
                        selection_entry_price=10.0,
                        reference_price=10.0,
                    )
                ],
            )

    fake_selection = FakeSelectionService()
    client, _service = _client(selection_service=fake_selection)
    created = client.post(
        "/api/v1/advisory/programs",
        json={
            "program_name": "Next day review",
            "package_mode": "single_package",
            "package_ids": ["pkg_a"],
            "target_count": 20,
            "status": "ENABLED",
        },
    )
    assert created.status_code == 200
    program_id = created.json()["program"]["program_id"]

    review = client.post(
        f"/api/v1/advisory/programs/{program_id}/reviews/run",
        json={
            "trade_date": "2026-06-10",
            "target_trade_date": "2026-06-10",
            "selection_as_of_trade_date": "2026-06-09",
        },
    )

    assert review.status_code == 200
    assert fake_selection.trade_date == date(2026, 6, 10)
    assert fake_selection.runtime_config["advisory_date_context"] == {
        "target_trade_date": "2026-06-10",
        "selection_as_of_trade_date": "2026-06-09",
    }
    assert fake_selection.runtime_config["selection_artifact_config"]["cutoff_date"] == "2026-06-09"
    assert fake_selection.runtime_config["runtime_profile"]["tradability"]["exclude_suspended"] is False
    payload = review.json()["review"]
    assert payload["trade_date"] == "2026-06-10"
    assert payload["change_summary"]["advisory_date_context"]["selection_as_of_trade_date"] == "2026-06-09"


def test_advisory_api_rejects_duplicate_published_review_for_trade_date() -> None:
    client, _service = _client()
    created = client.post(
        "/api/v1/advisory/programs",
        json={
            "program_name": "No duplicate review",
            "package_mode": "single_package",
            "package_ids": ["pkg_a"],
            "target_count": 1,
            "status": "ENABLED",
        },
    )
    assert created.status_code == 200
    program_id = created.json()["program"]["program_id"]
    review_payload = {
        "trade_date": "2026-06-01",
        "candidates": [{"symbol": "000001.SZ", "rank": 1, "score": 0.9, "next_open_executable": 10}],
        "market_by_symbol": {"000001.SZ": {"next_open_executable": 10}},
    }

    first = client.post(f"/api/v1/advisory/programs/{program_id}/reviews/run", json=review_payload)
    duplicate = client.post(f"/api/v1/advisory/programs/{program_id}/reviews/run", json=review_payload)

    assert first.status_code == 200
    assert duplicate.status_code == 400
    assert "already published" in duplicate.json()["detail"]["message"]


def test_advisory_review_creates_exit_item_with_operation_advice() -> None:
    client, _service = _client()
    created = client.post(
        "/api/v1/advisory/programs",
        json={
            "program_name": "Exit advice",
            "package_mode": "single_package",
            "package_ids": ["pkg_a"],
            "target_count": 1,
            "status": "ENABLED",
        },
    )
    assert created.status_code == 200
    program_id = created.json()["program"]["program_id"]

    entry = client.post(
        f"/api/v1/advisory/programs/{program_id}/reviews/run",
        json={
            "trade_date": "2026-06-01",
            "candidates": [{"symbol": "000001.SZ", "rank": 1, "score": 0.9, "next_open_executable": 10}],
            "market_by_symbol": {"000001.SZ": {"next_open_executable": 10}},
        },
    )
    assert entry.status_code == 200
    exit_review = client.post(
        f"/api/v1/advisory/programs/{program_id}/reviews/run",
        json={
            "trade_date": "2026-06-02",
            "candidates": [{"symbol": "000001.SZ", "rank": 2, "score": 0.2, "next_open_executable": 8}],
            "market_by_symbol": {"000001.SZ": {"next_open_executable": 8, "mark_price": 8}},
        },
    )

    assert exit_review.status_code == 200
    items = exit_review.json()["review"]["list_items"]
    exit_items = [row for row in items if row["action"] == "EXIT"]
    assert len(exit_items) == 1
    assert exit_items[0]["item_state"] == "EXITED"
    assert exit_items[0]["operation_advice_json"]["advice_type"] == "EXIT"
    assert exit_items[0]["operation_advice_json"]["suggested_price"] == 8.0


def test_advisory_apply_binding_without_replay_gate_retires_previous_and_keeps_active_pool() -> None:
    client, _service = _client()
    created = client.post(
        "/api/v1/advisory/programs",
        json={
            "program_name": "Manual binding apply",
            "package_mode": "single_package",
            "package_ids": ["pkg_a"],
            "target_count": 1,
            "status": "ENABLED",
        },
    )
    assert created.status_code == 200
    program = created.json()["program"]
    program_id = program["program_id"]

    before_binding = client.get(f"/api/v1/advisory/programs/{program_id}/bindings/active").json()["binding"]
    defaults = client.get(f"/api/v1/advisory/programs/{program_id}/bindings/defaults")
    assert defaults.status_code == 200
    assert defaults.json()["expected_program_version"] == program["version"]
    assert defaults.json()["expected_binding_version_id"] == before_binding["binding_version_id"]
    review = client.post(
        f"/api/v1/advisory/programs/{program_id}/reviews/run",
        json={
            "trade_date": "2026-06-01",
            "candidates": [{"symbol": "000001.SZ", "rank": 1, "score": 0.9, "next_open_executable": 10}],
            "market_by_symbol": {"000001.SZ": {"next_open_executable": 10}},
        },
    )
    assert review.status_code == 200
    active_before = client.get(f"/api/v1/advisory/programs/{program_id}/active-pool").json()["active_pool"]

    applied = client.post(
        f"/api/v1/advisory/programs/{program_id}/bindings/apply",
        json={
            "binding": {
                "package_mode": "single_package",
                "package_ids": ["pkg_multi_alpha_parent_b"],
                "package_weights": {"pkg_multi_alpha_parent_b": 1.0},
                "target_count": 1,
            },
            "activation_reason": "manual operator confirmation without replay hard gate",
            "expected_program_version": defaults.json()["expected_program_version"],
            "expected_binding_version_id": defaults.json()["expected_binding_version_id"],
            "created_by": "tester",
        },
    )

    assert applied.status_code == 200
    after_binding = applied.json()["binding"]
    assert after_binding["binding_version_id"] != before_binding["binding_version_id"]
    assert after_binding["activation_status"] == "ACTIVE"
    assert applied.json()["program"]["version"] == program["version"] + 1
    all_bindings = client.get(f"/api/v1/advisory/programs/{program_id}/bindings").json()["bindings"]
    assert sum(1 for row in all_bindings if row["activation_status"] == "ACTIVE") == 1
    assert any(row["binding_version_id"] == before_binding["binding_version_id"] and row["activation_status"] == "RETIRED" for row in all_bindings)
    active_after = client.get(f"/api/v1/advisory/programs/{program_id}/active-pool").json()["active_pool"]
    assert [row["symbol"] for row in active_after] == [row["symbol"] for row in active_before]


def test_advisory_program_update_and_clone_return_active_binding_contract() -> None:
    client, _service = _client()
    created = client.post(
        "/api/v1/advisory/programs",
        json={
            "program_name": "Binding API contract",
            "package_mode": "single_package",
            "package_ids": ["pkg_a"],
            "target_count": 1,
        },
    )
    assert created.status_code == 200
    created_payload = created.json()
    program_id = created_payload["program"]["program_id"]
    assert created_payload["binding"]["program_id"] == program_id
    assert created_payload["binding"]["effective_from_trade_date"] is not None
    assert created_payload["binding"]["binding_interval_semantics"] == "LEFT_CLOSED_RIGHT_OPEN"
    assert len(created_payload["binding"]["binding_payload_hash"]) == 64

    updated = client.patch(f"/api/v1/advisory/programs/{program_id}", json={"program_name": "Renamed contract"})
    assert updated.status_code == 200
    assert updated.json()["binding"]["program_id"] == program_id

    cloned = client.post(
        f"/api/v1/advisory/programs/{program_id}/clone",
        json={"program_name": "Cloned contract"},
    )
    assert cloned.status_code == 200
    assert cloned.json()["binding"]["program_id"] == cloned.json()["program"]["program_id"]
    assert cloned.json()["binding"]["effective_from_trade_date"] is not None


def test_advisory_replay_draft_binding_does_not_mutate_active_binding() -> None:
    client, _service = _client()
    created = client.post(
        "/api/v1/advisory/programs",
        json={
            "program_name": "Draft replay",
            "package_mode": "single_package",
            "package_ids": ["pkg_a"],
            "target_count": 1,
            "status": "ENABLED",
        },
    )
    assert created.status_code == 200
    program_id = created.json()["program"]["program_id"]
    active_before = client.get(f"/api/v1/advisory/programs/{program_id}/bindings/active").json()["binding"]

    replay = client.post(
        f"/api/v1/advisory/programs/{program_id}/replay",
        json={
            "start_date": "2026-06-01",
            "end_date": "2026-06-02",
            "draft_binding": {
                "package_mode": "single_package",
                "package_ids": ["pkg_multi_alpha_parent_b"],
                "package_weights": {"pkg_multi_alpha_parent_b": 1.0},
                "target_count": 1,
            },
            "candidates_by_date": {
                "2026-06-01": [{"symbol": "000002.SZ", "rank": 1, "score": 0.9, "next_open_executable": 20}],
                "2026-06-02": [{"symbol": "000002.SZ", "rank": 1, "score": 0.8, "next_open_executable": 22}],
            },
            "market_by_date": {
                "2026-06-01": {"000002.SZ": {"next_open_executable": 20}},
                "2026-06-02": {"000002.SZ": {"next_open_executable": 22, "mark_price": 22}},
            },
        },
    )

    assert replay.status_code == 200
    payload = replay.json()["replay"]
    assert payload["summary"]["manual_gate"] is False
    assert payload["summary"]["draft_binding"]["package_mode"] == "single_package"
    assert payload["summary"]["draft_binding"]["package_ids"] == ["pkg_multi_alpha_parent_b"]
    assert [row["list_version"]["version_status"] for row in payload["daily_list_versions"]] == ["REPLAY", "REPLAY"]
    active_after = client.get(f"/api/v1/advisory/programs/{program_id}/bindings/active").json()["binding"]
    assert active_after["binding_version_id"] == active_before["binding_version_id"]
    all_bindings = client.get(f"/api/v1/advisory/programs/{program_id}/bindings").json()["bindings"]
    assert any(row["activation_status"] == "DRAFT" for row in all_bindings)


@pytest.mark.parametrize("package_mode", ["fusion_pool", "weighted_rank_fusion", "union", "intersection"])
def test_advisory_api_retires_manual_multi_package_modes(package_mode: str) -> None:
    client, _service = _client()
    created = client.post(
        "/api/v1/advisory/programs",
        json={
            "program_name": f"Mode {package_mode}",
            "package_mode": package_mode,
            "package_ids": ["pkg_a", "pkg_b"],
            "package_weights": {"pkg_a": 0.6, "pkg_b": 0.4},
            "target_count": 1,
            "status": "ENABLED",
        },
    )
    assert created.status_code == 422
    assert created.json()["detail"]["context"]["reason_code"] == "ADVISORY_MANUAL_MULTI_PACKAGE_DEPRECATED"
