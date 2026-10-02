from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.routers import advisory as advisory_router

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def test_economic_formal_GET_serializes_actual_projection_without_recapturing(tmp_path, study):
    from backend.tests.advisory_model_first.test_economic_entry_daily_service import _formal_consumer
    from backend.services.advisory_model_first.entry_price_daily_service import EntryWorkBudget, BoundedEntryReadSession
    service, loaded, role, pointer, source, observed, _ = _formal_consumer(tmp_path, study)
    session = BoundedEntryReadSession(EntryWorkBudget())
    service._capture_formal(role=role, pointer=pointer, loaded=loaded, target=observed["target_date"], budget=session.budget, session=session)
    app.dependency_overrides[advisory_router.get_advisory_economic_entry_service] = lambda: service
    def forbidden_legacy():
        raise AssertionError("economic advice cannot depend on legacy ranking/M4")
    app.dependency_overrides[advisory_router.get_advisory_model_shadow_service] = forbidden_legacy
    try:
        response = TestClient(app).get("/api/v1/advisory/programs/program/entry-value/status",
            params={"target_trade_date": observed["target_date"].isoformat()})
        result = response.json()
        assert response.status_code == 200 and result["status"] == "PUBLISHED" and result["deployable"] is True
        assert source.calls == 1 and result["advice"][0]["risk_budget"]["configuration_sha256"] == loaded.confirmation_request.business_risk.configuration_sha256
        assert result["advice"][0]["role_binding_sha256"] == role.role_sha256
        assert result["advice"][0]["evidence_state"] == "CONFIRMED_ENTRY_VALUE"
    finally:
        session.close()
        app.dependency_overrides.pop(advisory_router.get_advisory_economic_entry_service, None)
        app.dependency_overrides.pop(advisory_router.get_advisory_model_shadow_service, None)


def test_economic_entry_routes_are_read_only_and_do_not_depend_on_legacy_ranking():
    from backend.services.advisory_model_first.economic_entry_daily_service import AdvisoryEconomicEntryDailyServiceV1
    calls = []
    class ReadOnlyResearch(AdvisoryEconomicEntryDailyServiceV1):
        def read_research(self, **kwargs):
            calls.append(kwargs)
            return {"status": "RESEARCH_NAVIGATION", "deployable": False, "advice": []}
        def run_once(self):
            raise AssertionError("GET cannot capture")
    def forbidden_legacy():
        raise AssertionError("independent economic GET must not resolve ranking/M4")
    app.dependency_overrides[advisory_router.get_advisory_economic_entry_service] = lambda: ReadOnlyResearch()
    app.dependency_overrides[advisory_router.get_advisory_model_shadow_service] = forbidden_legacy
    try:
        client = TestClient(app)
        base = "/api/v1/advisory/programs/program/entry-value"
        status = client.get(f"{base}/status", params={"target_trade_date": "2025-01-29"})
        assert status.status_code == 200 and status.json()["status"] == "NOT_CONFIGURED" and calls == []
        research = client.get(f"{base}/research", params={"bundle_id": "adveserve_" + "a" * 24, "target_trade_date": "2025-01-29"})
        assert research.status_code == 200 and research.json()["deployable"] is False and len(calls) == 1
        assert client.get(f"{base}/research", params={"bundle_id": "../../foreign", "target_trade_date": "2025-01-29"}).status_code == 422
        assert len(calls) == 1
    finally:
        app.dependency_overrides.pop(advisory_router.get_advisory_economic_entry_service, None)
        app.dependency_overrides.pop(advisory_router.get_advisory_model_shadow_service, None)


def test_entry_v2_is_opt_in_and_available_even_when_ranking_is_unavailable():
    from backend.tests.advisory_model_first.test_entry_price_service import integrated_service
    core, _, args = integrated_service()
    entry = core.evaluate(**args).as_payload()
    calls = []
    class Legacy:
        def model_shadow(self, **kwargs):
            return {"status": "MODEL_UNAVAILABLE", "program_id": kwargs["program_id"], "price_range": None}
    class Entry:
        def read_price(self, **kwargs):
            calls.append(kwargs)
            return entry
        def status(self, **kwargs):
            return {"configured": True, "program_id": kwargs["program_id"]}
    app.dependency_overrides[advisory_router.get_advisory_model_shadow_service] = lambda: Legacy()
    app.dependency_overrides[advisory_router.get_advisory_entry_price_service] = lambda: Entry()
    try:
        client = TestClient(app)
        path = f"/api/v1/advisory/programs/{args['program_id']}/model-shadow"
        legacy = client.get(path, params={"target_trade_date": args["target_trade_date"].isoformat()})
        assert legacy.status_code == 200 and "entry_price" not in legacy.json() and calls == []
        response = client.get(path, params={"target_trade_date": args["target_trade_date"].isoformat(), "price_contract": "entry-v2"})
        assert response.status_code == 200
        assert response.json()["status"] == "MODEL_UNAVAILABLE"
        assert response.json()["entry_price"] == entry and len(calls) == 1
        assert client.get(f"/api/v1/advisory/programs/{args['program_id']}/entry-price/status").json()["configured"]
        assert client.get(path, params={"target_trade_date": "2026-07-21", "price_contract": "invalid"}).status_code == 422
    finally:
        app.dependency_overrides.pop(advisory_router.get_advisory_model_shadow_service, None)
        app.dependency_overrides.pop(advisory_router.get_advisory_entry_price_service, None)


class _ForwardService:
    def status(self):
        return {"schema_version": "advisory_forward_status_v1", "run_count": 1}

    def run_once(self):
        return {
            "schema_version": "advisory_forward_run_once_v1",
            "publication_due": True,
            "results": [
                {
                    "program_id": "advp_test",
                    "forward_run_id": "advfwd_test",
                    "status": "PUBLISHED",
                    "model_status": "UNAVAILABLE",
                }
            ],
        }

    def list_runs(self, *, program_id: str | None = None, limit: int = 100):
        assert (program_id, limit) == ("advp_test", 20)
        return [{"forward_run_id": "advfwd_test", "publication_status": "PUBLISHED"}]

    def detail(self, forward_run_id: str):
        assert forward_run_id == "advfwd_test"
        return {
            "forward_run": {"forward_run_id": forward_run_id, "publication_status": "PUBLISHED"},
            "model_observation": {"status": "UNAVAILABLE"},
            "model_outcome": None,
        }

    def model_metrics(self, program_id: str):
        assert program_id == "advp_test"
        return {
            "schema_version": "advisory_forward_model_metrics_response_v1",
            "program_id": program_id,
            "status": "EVIDENCE_IMMATURE",
            "observation_count": 1,
            "due_observation_count": 0,
            "evaluation": None,
        }


def test_forward_api_exposes_status_run_once_history_and_detail(monkeypatch) -> None:
    service = _ForwardService()
    app.dependency_overrides[advisory_router.get_advisory_forward_service] = lambda: service
    monkeypatch.setattr(
        advisory_router.advisory_forward_scheduler,
        "status",
        lambda: {"configured_enabled": False, "running": False},
    )
    monkeypatch.setattr(advisory_router.advisory_forward_scheduler, "run_once", service.run_once)
    client = TestClient(app)
    try:
        status = client.get("/api/v1/advisory/forward/status")
        run = client.post("/api/v1/advisory/forward/run-once")
        history = client.get("/api/v1/advisory/programs/advp_test/forward-runs", params={"limit": 20})
        detail = client.get("/api/v1/advisory/forward-runs/advfwd_test")
        metrics = client.get("/api/v1/advisory/programs/advp_test/forward-model-metrics")
    finally:
        app.dependency_overrides.pop(advisory_router.get_advisory_forward_service, None)

    assert status.status_code == 200
    assert status.json()["scheduler"] == {"configured_enabled": False, "running": False}
    assert run.status_code == 200
    assert run.json()["results"][0]["model_status"] == "UNAVAILABLE"
    assert history.status_code == 200
    assert history.json()["forward_runs"][0]["publication_status"] == "PUBLISHED"
    assert detail.status_code == 200
    assert detail.json()["model_observation"]["status"] == "UNAVAILABLE"
    assert metrics.status_code == 200
    assert metrics.json()["status"] == "EVIDENCE_IMMATURE"
