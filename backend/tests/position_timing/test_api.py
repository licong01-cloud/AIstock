from datetime import datetime, timedelta

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers.position_timing import get_position_timing_service, router
from conftest import CHINA_TZ


def test_block_one_get_endpoints_do_not_create_timing_artifacts(service_factory) -> None:
    service = service_factory()
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.dependency_overrides[get_position_timing_service] = lambda: service
    client = TestClient(app)

    assert client.get("/api/v1/position-timing/intents").status_code == 200
    assert client.get("/api/v1/position-timing/cards/current").status_code == 200
    assert client.get("/api/v1/position-timing/evidence").status_code == 200
    assert client.get("/api/v1/position-timing/alerts/poll").status_code == 200
    assert not service.store.root.exists()


def test_block_one_api_surface_and_side_effect_boundaries(service_factory) -> None:
    service = service_factory()
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.dependency_overrides[get_position_timing_service] = lambda: service
    client = TestClient(app)

    intents = client.get("/api/v1/position-timing/intents")
    assert intents.status_code == 200
    assert len(intents.json()["items"]) == 2

    intent_write = client.put(
        "/api/v1/position-timing/intents/600000.SH",
        json={"planned_full_notional_cny": "120000", "desired_target_exposure": "0.5"},
    )
    assert intent_write.status_code == 200
    assert intent_write.json()["effective_card_policy"] == "NEXT_DECISION_TRADE_DATE"
    intent_retry = client.put(
        "/api/v1/position-timing/intents/600000.SH",
        json={"planned_full_notional_cny": "120000", "desired_target_exposure": "0.5"},
    )
    assert intent_retry.status_code == 200
    assert intent_retry.json()["status"] == "UNCHANGED"

    scope_write = client.put(
        "/api/v1/position-timing/analysis-scope/600000.SH",
        json={"analysis_enabled": True},
    )
    assert scope_write.status_code == 200
    assert scope_write.json()["effective_card_policy"] == "NEXT_CARD_SET_ONLY"
    assert scope_write.json()["status"] == "UPDATED"

    materialize = client.post("/api/v1/position-timing/materialize")
    assert materialize.status_code == 200
    assert materialize.json()["outcome_materialization_status"] == "NO_DUE_OUTCOMES"
    assert len(materialize.json()["card_set"]["cards"]) == 2
    assert len(materialize.json()["card_set"]["cards_sha256"]) == 64

    retry = client.post("/api/v1/position-timing/materialize")
    assert retry.status_code == 200
    assert retry.json()["status"] == "ALREADY_MATERIALIZED"
    assert retry.json()["card_set_artifact_sha256"] == materialize.json()["card_set_artifact_sha256"]

    current = client.get("/api/v1/position-timing/cards/current")
    assert current.status_code == 200
    assert current.json()["status"] == "UPCOMING"

    evidence = client.get("/api/v1/position-timing/evidence")
    assert evidence.status_code == 200
    assert evidence.json()["l2_runtime_status"] == "OFFLINE_PIPELINE_AVAILABLE_NO_RUNTIME_MODEL"
    assert evidence.json()["l2_formal_audit"]["effect_evidence"] == "INCONCLUSIVE"
    assert evidence.json()["l2_formal_audit"]["selected_model_id"] is None
    assert evidence.json()["l2_formal_audit"]["runtime_model_written"] is False
    assert len(evidence.json()["l2_formal_audit"]["reference_sha256"]) == 64
    assert len(evidence.json()["l2_research_contract_sha256"]) == 64
    assert evidence.json()["event_counts"] == {"CARD_ISSUED": 2}
    assert evidence.json()["outcome_evidence"]["coverage_counts"]["pending_derived"] == 10

    route_paths = {route.path for route in app.routes}
    assert not any("order" in path for path in route_paths)
    assert "/api/v1/position-timing/alerts/poll" in route_paths
    assert "/api/v1/position-timing/alerts/{trigger_id}/claim" in route_paths


def test_alert_poll_and_claim_api_use_bounded_snapshot(service_factory) -> None:
    clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]

    def quote_loader(symbols):
        return {
            symbol: {
                "quote_price_raw": "9",
                "quote_open_raw": "9",
                "quote_observed_at": clock[0] - timedelta(seconds=2),
                "price_basis": "raw_cny",
            }
            for symbol in symbols
        }

    service = service_factory(now=lambda: clock[0], quote=quote_loader)
    service.materialize()
    clock[0] = datetime(2026, 9, 4, 9, 31, tzinfo=CHINA_TZ)
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.dependency_overrides[get_position_timing_service] = lambda: service
    client = TestClient(app)

    poll = client.get("/api/v1/position-timing/alerts/poll")
    assert poll.status_code == 200
    edge = poll.json()["items"][0]
    request = {
        key: edge[key]
        for key in (
            "card_id",
            "eligibility_identity",
            "quote_price_raw",
            "quote_open_raw",
            "quote_observed_at",
            "alert_evaluated_at",
            "quote_source",
            "position_snapshot_sha256",
            "intent_snapshot_sha256",
        )
    }
    claim = client.post(
        f"/api/v1/position-timing/alerts/{edge['trigger_id']}/claim",
        json=request,
    )
    assert claim.status_code == 200
    assert claim.json()["granted"] is True
    assert client.post(
        f"/api/v1/position-timing/alerts/{edge['trigger_id']}/claim",
        json=request,
    ).json()["granted"] is False
