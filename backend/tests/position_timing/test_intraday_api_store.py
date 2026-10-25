from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers.position_timing import get_intraday_service, router
from backend.services.position_timing.intraday.contracts import PlanWrite, payload
from backend.services.position_timing.intraday.service import IntradayService
from backend.services.position_timing.intraday.store import IntradayStore
from test_intraday_core import at, case


@pytest.fixture
def minute_api(tmp_path):
    plan, market = case()
    clock = [at()]
    service = IntradayService(store=IntradayStore(tmp_path), source_commit="1" * 40,
                              market_loader=lambda _: market, now_provider=lambda: clock[0])
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.dependency_overrides[get_intraday_service] = lambda: service
    with TestClient(app) as client:
        yield client, service, plan, clock
    service._pool.shutdown(wait=True)


def create(client, plan, revision=0, key="create"):
    fields = plan.model_dump(exclude={"schema_version", "plan_id", "revision", "policy", "policy_sha256"})
    return client.put("/api/v1/position-timing/intraday/plans/" + plan.plan_id,
                      json=payload(PlanWrite(expected_revision=revision, idempotency_key=key, plan=fields)))


def batch(client, plan, key="evaluate"):
    return client.post("/api/v1/position-timing/intraday/evaluations", json={"plans": [{"plan_id": plan.plan_id, "revision": plan.revision}], "idempotency_key": key})


def test_api_read_only_get_cas_idempotency_and_single_claim(minute_api):
    client, service, plan, clock = minute_api
    base = "/api/v1/position-timing/intraday"
    assert client.get(base + f"/plans/{plan.plan_id}/status").status_code == 404
    assert not service.store.root.exists()
    written = create(client, plan)
    assert written.status_code == 200 and written.json()["orders_created"] is False
    assert create(client, plan).json() == written.json()
    assert create(client, plan, key="stale-revision").status_code == 409
    first = batch(client, plan)
    assert first.status_code == 200 and first.json()["items"][0]["status"] == "OPPORTUNITY"
    assert batch(client, plan).json() == first.json()
    decision = first.json()["items"][0]["decision"]
    claim_body = {"plan_id": plan.plan_id, "expected_revision": 1, "decision_id": decision["decision_id"], "idempotency_key": "claim"}
    claim_url = base + f"/alerts/{decision['opportunity_id']}/claim"
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: client.post(claim_url, json=claim_body).json(), range(2)))
    assert sum(item["authorized"] for item in responses) == 1
    assert len(list((service.store.root / "claims").glob("*.json"))) == 1
    before = {str(p): p.stat().st_mtime_ns for p in service.store.root.rglob("*.json")}
    assert client.get(base + f"/alerts?plan_ids={plan.plan_id}").json()["items"][0]["already_alerted"]
    clock[0] = at("09:37:00")
    assert client.get(base + f"/plans/{plan.plan_id}/status").json()["status"] == "UNAVAILABLE"
    assert client.post(claim_url, json=claim_body).status_code == 409
    assert before == {str(p): p.stat().st_mtime_ns for p in service.store.root.rglob("*.json")}


def test_source_failure_invalidates_prior_opportunity_and_cancellation_wins(minute_api):
    client, service, plan, clock = minute_api
    assert create(client, plan).status_code == 200
    assert batch(client, plan).json()["items"][0]["status"] == "OPPORTUNITY"
    with service._cache_lock:
        service._cache.clear()
    def broken(_):
        raise RuntimeError("not returned to client")
    service.market_loader = broken
    failed = batch(client, plan, "source-failed").json()
    assert failed["items"][0]["status"] == "UNAVAILABLE"
    status = client.get(f"/api/v1/position-timing/intraday/plans/{plan.plan_id}/status").json()
    assert status["status"] == "UNAVAILABLE" and status["decision"] is None
    cancelled = plan.model_copy(update={"lifecycle": "CANCELLED"})
    assert create(client, cancelled, revision=1, key="cancel").status_code == 200
    assert client.get(f"/api/v1/position-timing/intraday/plans/{plan.plan_id}/status").json()["status"] == "EXPIRED"
    assert batch(client, plan, "old-revision").json()["items"][0]["reason_codes"] == ["PLAN_REVISION_CONFLICT"]


def test_immutable_transition_survives_lost_head_commit(minute_api, monkeypatch):
    client, service, plan, _ = minute_api
    assert create(client, plan).status_code == 200
    original = service.store.write
    def crash(path, value, **kwargs):
        if kwargs.get("mutable"):
            raise OSError("simulated lost head write")
        return original(path, value, **kwargs)
    monkeypatch.setattr(service.store, "write", crash)
    assert batch(client, plan, "crash").status_code == 500
    monkeypatch.setattr(service.store, "write", original)
    retry = batch(client, plan, "crash")
    assert retry.json()["items"][0]["status"] == "OPPORTUNITY"
    assert service.store.head(plan.plan_id)["decision"] is not None


def test_api_rejects_forged_source_extra_fields_and_key_reuse(minute_api):
    client, _, plan, _ = minute_api
    assert client.get('/api/v1/position-timing/intraday/plans/' + 'x' * 97 + '/status').status_code == 422
    assert create(client, plan.model_copy(update={"source_kind": "ADVISORY"})).status_code == 422
    assert create(client, plan.model_copy(update={"source_kind": "FROZEN_REPLAY"})).status_code == 422
    assert create(client, plan).status_code == 200
    assert batch(client, plan).status_code == 200
    conflict = client.post("/api/v1/position-timing/intraday/evaluations", json={"idempotency_key": "evaluate", "plans": [{"plan_id": "different", "revision": 1}]})
    assert conflict.status_code == 409
    extra = client.post("/api/v1/position-timing/intraday/evaluations", json={"idempotency_key": "extra", "plans": [{"plan_id": plan.plan_id, "revision": 1}], "as_of": at().isoformat()})
    assert extra.status_code == 422


def test_budget_defers_all_items_and_late_input_does_not_commit(minute_api):
    client, service, plan, _ = minute_api
    assert create(client, plan).status_code == 200
    gate = Event()
    _, market = case()
    service.budget = 0.03
    service.market_loader = lambda _: (gate.wait(1), market)[1]
    try:
        response = batch(client, plan, "budget").json()
        assert response["partial"] and response["items"][0]["status"] == "DEFERRED"
        assert service.store.head(plan.plan_id)["decision"] is None
        gate.set()
        service._pool.shutdown(wait=True)
        assert service.store.head(plan.plan_id)["decision"] is None
        assert batch(client, plan, "budget").json() == response
    finally:
        gate.set()


def test_advisory_source_revocation_blocks_claim_without_trusting_browser(minute_api):
    client, service, plan, _ = minute_api
    service.source_resolver = lambda _: True
    plan = plan.model_copy(update={"source_kind": "ADVISORY"})
    assert create(client, plan).status_code == 200
    decision = batch(client, plan).json()["items"][0]["decision"]
    service.source_resolver = lambda _: False
    response = client.post(f"/api/v1/position-timing/intraday/alerts/{decision['opportunity_id']}/claim", json={
        "plan_id": plan.plan_id, "expected_revision": 1, "decision_id": decision["decision_id"], "idempotency_key": "revoked"})
    assert response.status_code == 503 and response.json()["detail"]["error_code"] == "SOURCE_UNVERIFIED"
    assert not (service.store.root / "claims").exists()
