from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from backend.routers.advisory_workbench import get_service, router
from backend.services.advisory_workbench.projection import WorkbenchError


def client(service):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_service] = lambda: service
    return TestClient(app)


@pytest.mark.parametrize("limit", [20, 50, 100])
def test_real_query_string_pagination_is_coerced_and_bounded(limit):
    response = client(SimpleNamespace(daily=lambda *a, **k: k)).get(f"/workbench/programs/p/daily?limit={limit}&offset=20")
    assert response.status_code == 200
    assert response.json()["limit"] == limit and response.json()["offset"] == 20


@pytest.mark.parametrize("query", ["limit=21", "limit=1000", "offset=-1", "target_date=oops", "group=DROP", "snapshot_id=oops"])
def test_invalid_filters_do_not_reach_source(query):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid request reached source")
    assert client(SimpleNamespace(daily=forbidden)).get("/workbench/programs/p/daily?" + query).status_code == 422


def test_snapshot_conflict_and_source_errors_are_not_empty_success():
    def changed(*args, **kwargs):
        raise WorkbenchError("SNAPSHOT_CHANGED", "请刷新", 409)
    response = client(SimpleNamespace(program=changed)).get("/workbench/programs/p")
    assert response.status_code == 409 and response.json()["detail"]["reason_code"] == "SNAPSHOT_CHANGED"
    def failed(*args, **kwargs):
        raise RuntimeError("private-dsn-not-public")
    response = client(SimpleNamespace(program=failed)).get("/workbench/programs/p")
    assert response.status_code == 503 and "private-dsn" not in response.text


def test_operator_routes_never_offer_mutation_or_inference():
    app = client(SimpleNamespace()).app
    routes = [r for r in app.routes if r.path.startswith("/workbench")]
    assert len(routes) == 8 and all(r.methods == {"GET"} for r in routes)
    assert client(SimpleNamespace()).post("/workbench/overview").status_code == 405
