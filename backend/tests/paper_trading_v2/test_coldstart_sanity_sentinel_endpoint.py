from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import paper_trading_v2
from backend.services.paper_trading_v2.coldstart_sentinel import (
    ColdstartSentinelService,
    PaperV2DaemonUnavailableError,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


TEST_PACKAGE_ID = "pkg_coldstart_sentinel_test"


def _manifest_row(*, status: PackageStatus = PackageStatus.PAPER_ENABLED) -> dict[str, Any]:
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_id": TEST_PACKAGE_ID, "package_status": status}))
    return {
        "package_id": manifest.package_id,
        "package_name": manifest.package_name,
        "package_version": manifest.package_version,
        "package_status": manifest.package_status.value,
        "manifest_sha256": manifest.manifest_sha256,
        "manifest_json": manifest.model_dump(mode="json"),
    }


class FakeCursor:
    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn
        self.last_sql = ""

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def execute(self, sql: str, params: object | None = None) -> None:
        self.last_sql = sql
        self.conn.last_params = params
        self.conn.executed.append((sql, params))

    def fetchone(self) -> Any:
        if "FROM strategy_pkg.package" in self.last_sql:
            package_id = None
            if isinstance(self.conn.last_params, tuple) and self.conn.last_params:
                package_id = self.conn.last_params[0]
            if self.conn.package_row and self.conn.package_row["package_id"] == package_id:
                return self.conn.package_row
            return None
        return None

    def fetchall(self) -> list[dict[str, Any]]:
        if "FROM information_schema.columns" in self.last_sql:
            return [{"column_name": column} for column in sorted(self.conn.capture_columns)]
        return []


class FakeConnection:
    def __init__(
        self,
        *,
        package_row: dict[str, Any] | None = None,
        capture_columns: set[str] | None = None,
    ) -> None:
        self.package_row = package_row
        self.capture_columns = capture_columns or {"created_at", "updated_at", "intended_price", "fill_market_context"}
        self.executed: list[tuple[str, object | None]] = []
        self.last_params: object | None = None
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, *args: object, **kwargs: object) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


@contextmanager
def _conn_factory(conn: FakeConnection):
    yield conn


def _client(monkeypatch: pytest.MonkeyPatch, conn: FakeConnection, *, daemon_running: bool = True, now: dt.datetime | None = None) -> TestClient:
    def service_factory() -> ColdstartSentinelService:
        return ColdstartSentinelService(
            conn_factory=lambda: _conn_factory(conn),
            daemon_checker=lambda _name: daemon_running,
            now_factory=lambda: now or dt.datetime(2026, 5, 11, 8, 30, tzinfo=dt.UTC),
        )

    monkeypatch.setattr(paper_trading_v2, "ColdstartSentinelService", service_factory)
    app = FastAPI()
    app.include_router(paper_trading_v2.router, prefix="/api/v1")
    return TestClient(app)


def _payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "run_id": "sanity-test-20260511",
        "package_id": TEST_PACKAGE_ID,
        "symbol": "000001.SZ",
        "side": "BUY",
        "quantity": 100,
        "qty": 100,
        "intended_price": "10.00",
        "source": "paper_v2_coldstart_sanity",
        "broker_backend": "local_sim",
    }
    payload.update(overrides)
    return payload


def test_sentinel_endpoint_records_cleanup_scoped_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConnection(package_row=_manifest_row())
    client = _client(monkeypatch, conn)

    response = client.post("/api/v1/paper-v2/coldstart-sanity/sentinel-order", json=_payload())

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["status"] == "accepted"
    assert body["run_id"] == "sanity-test-20260511"
    assert body["package_id"] == conn.package_row["package_id"]
    assert body["intended_price"] == "10.00"
    assert body["routing_class"] == "telemetry"
    assert body["fill_market_context"]["stock_id"] == "000001.SZ"
    assert body["asset_ref"].startswith("governance/coldstart_sanity/sanity-test-20260511/")
    assert conn.commits == 1
    assert conn.rollbacks == 0

    sql_text = "\n".join(sql for sql, _ in conn.executed)
    assert "INSERT INTO paper_v2.fills" in sql_text
    assert "intended_price, fill_market_context" in sql_text
    assert "INSERT INTO qe_archive.outbox_event" in sql_text
    assert "INSERT INTO strategy_pkg.package_validation_run" in sql_text
    assert "INSERT INTO strategy_pkg.package_asset" in sql_text
    assert "session-scheduler" not in sql_text
    fill_params = next(params for sql, params in conn.executed if "INSERT INTO paper_v2.fills" in sql)
    assert str(fill_params[13]) == "10.00"
    assert fill_params[14].adapted["stock_id"] == "000001.SZ"
    assert fill_params[14].adapted["broker_backend"] == "local_sim"
    portfolio_sql, portfolio_params = next((sql, params) for sql, params in conn.executed if "INSERT INTO paper_v2.portfolio" in sql)
    assert "broker_backend" not in portfolio_sql
    assert portfolio_params[10].adapted["broker_backend"] == "local_sim"
    outbox_params = next(params for sql, params in conn.executed if "INSERT INTO qe_archive.outbox_event" in sql)
    assert outbox_params[5].adapted["routing_class"] == "telemetry"


@pytest.mark.parametrize(
    ("overrides", "needle"),
    [
        ({"run_id": "prod-run"}, "run_id"),
        ({"package_id": "   "}, "package_id"),
        ({"symbol": "000002.SZ"}, "symbol"),
        ({"side": "SELL"}, "side"),
        ({"quantity": 200, "qty": 200}, "quantity"),
        ({"intended_price": "10.01"}, "intended_price"),
        ({"source": "other"}, "source"),
        ({"broker_backend": "miniqmt_sim"}, "broker_backend"),
    ],
)
def test_sentinel_endpoint_rejects_invalid_inputs(monkeypatch: pytest.MonkeyPatch, overrides: dict[str, Any], needle: str) -> None:
    conn = FakeConnection(package_row=_manifest_row())
    client = _client(monkeypatch, conn)

    response = client.post("/api/v1/paper-v2/coldstart-sanity/sentinel-order", json=_payload(**overrides))

    assert response.status_code == 400
    assert needle in str(response.json()["detail"]["context"]["failures"])
    assert conn.commits == 0
    assert conn.executed == []


@pytest.mark.parametrize("payload", [{}, {"package_id": ""}])
def test_sentinel_endpoint_rejects_missing_or_empty_package_id_at_schema(monkeypatch: pytest.MonkeyPatch, payload: dict[str, Any]) -> None:
    conn = FakeConnection(package_row=_manifest_row())
    client = _client(monkeypatch, conn)

    request = _payload()
    request.update(payload)
    if "package_id" not in payload:
        request.pop("package_id")

    response = client.post("/api/v1/paper-v2/coldstart-sanity/sentinel-order", json=request)

    assert response.status_code == 422
    assert conn.commits == 0
    assert conn.executed == []


def test_sentinel_endpoint_rejects_when_governance_gate_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConnection(package_row=None)
    client = _client(monkeypatch, conn)

    response = client.post("/api/v1/paper-v2/coldstart-sanity/sentinel-order", json=_payload())

    assert response.status_code == 409
    assert response.json()["detail"]["error_code"] == "INVALID_STATE_TRANSITION"
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_sentinel_endpoint_rejects_unknown_package_id_before_capture_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConnection(package_row=_manifest_row())
    client = _client(monkeypatch, conn)

    response = client.post("/api/v1/paper-v2/coldstart-sanity/sentinel-order", json=_payload(package_id="pkg_missing"))

    assert response.status_code == 409
    assert response.json()["detail"]["context"]["package_id"] == "pkg_missing"
    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert not any("FROM information_schema.columns" in sql for sql, _ in conn.executed)


def test_sentinel_endpoint_allows_backtest_approved_package_status(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConnection(package_row=_manifest_row(status=PackageStatus.BACKTEST_APPROVED))
    client = _client(monkeypatch, conn)

    response = client.post("/api/v1/paper-v2/coldstart-sanity/sentinel-order", json=_payload(package_id=conn.package_row["package_id"]))

    assert response.status_code == 200
    assert response.json()["package_id"] == conn.package_row["package_id"]
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert any("INSERT INTO paper_v2.fills" in sql for sql, _ in conn.executed)


def test_sentinel_endpoint_rejects_retired_package_status_before_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConnection(package_row=_manifest_row(status=PackageStatus.RETIRED))
    client = _client(monkeypatch, conn)

    response = client.post("/api/v1/paper-v2/coldstart-sanity/sentinel-order", json=_payload(package_id=conn.package_row["package_id"]))

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["context"]["package_status"] == PackageStatus.RETIRED.value
    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert not any("INSERT INTO" in sql for sql, _ in conn.executed)


def test_sentinel_endpoint_rejects_missing_capture_fields_before_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConnection(package_row=_manifest_row(), capture_columns={"created_at", "updated_at"})
    client = _client(monkeypatch, conn)

    response = client.post("/api/v1/paper-v2/coldstart-sanity/sentinel-order", json=_payload(package_id=conn.package_row["package_id"]))

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["context"]["table"] == "paper_v2.fills"
    assert detail["context"]["missing_columns"] == ["fill_market_context", "intended_price"]
    assert detail["context"]["ddl_file"] == "backend/db/add_paper_v2_capture_fields_20260510.sql"
    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert any("FROM information_schema.columns" in sql for sql, _ in conn.executed)
    assert not any("INSERT INTO" in sql for sql, _ in conn.executed)


def test_sentinel_endpoint_rejects_when_daemon_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConnection(package_row=_manifest_row())
    client = _client(monkeypatch, conn, daemon_running=False)

    response = client.post("/api/v1/paper-v2/coldstart-sanity/sentinel-order", json=_payload())

    assert response.status_code == 503
    assert response.json()["detail"]["error_code"] == PaperV2DaemonUnavailableError.error_code
    assert conn.executed == []


def test_sentinel_endpoint_allows_intraday_recovery_sanity(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConnection(package_row=_manifest_row())
    client = _client(monkeypatch, conn, now=dt.datetime(2026, 5, 11, 9, 31, tzinfo=dt.timezone(dt.timedelta(hours=8))))

    response = client.post("/api/v1/paper-v2/coldstart-sanity/sentinel-order", json=_payload())

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert conn.commits == 1


def test_sentinel_endpoint_present_in_openapi(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConnection(package_row=_manifest_row())
    client = _client(monkeypatch, conn)

    openapi = client.get("/openapi.json").json()

    assert "/api/v1/paper-v2/coldstart-sanity/sentinel-order" in openapi["paths"]
