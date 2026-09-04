from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers.position_timing import get_position_timing_service, router


def test_block_one_get_endpoints_do_not_create_timing_artifacts(service_factory) -> None:
    service = service_factory()
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.dependency_overrides[get_position_timing_service] = lambda: service
    client = TestClient(app)

    assert client.get("/api/v1/position-timing/intents").status_code == 200
    assert client.get("/api/v1/position-timing/cards/current").status_code == 200
    assert client.get("/api/v1/position-timing/evidence").status_code == 200
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

    materialize = client.post("/api/v1/position-timing/materialize")
    assert materialize.status_code == 200
    assert materialize.json()["outcome_materialization_status"] == "DEFERRED_TO_IMPLEMENTATION_BLOCK_TWO"
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
    assert evidence.json()["l2_runtime_status"] == "PIPELINE_DEFERRED_BY_APPROVED_SCOPE"
    assert len(evidence.json()["l2_research_contract_sha256"]) == 64
    assert evidence.json()["event_counts"] == {"CARD_ISSUED": 2}

    route_paths = {route.path for route in app.routes}
    assert not any("alert" in path or "order" in path for path in route_paths)
