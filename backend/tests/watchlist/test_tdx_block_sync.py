from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import tdx_blocks
from backend.services import tdx_block_service


class _FakeCursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return [
            {"category_id": 7, "display_name": "短线池", "code": "000001.SZ"},
            {"category_id": 7, "display_name": "短线池", "code": "600519.SH"},
            {"category_id": 7, "display_name": "短线池", "code": "invalid"},
        ]


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self.cursor_obj = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def cursor(self, **_kwargs):
        return self.cursor_obj


class _FakeTq:
    def __init__(self) -> None:
        self.created = []
        self.cleared = []
        self.sent = []

    def create_sector(self, *, block_code: str, block_name: str) -> None:
        self.created.append((block_code, block_name))

    def clear_sector(self, *, block_code: str) -> None:
        self.cleared.append(block_code)

    def send_user_block(self, *, block_code: str, stocks: list[str]) -> None:
        self.sent.append((block_code, stocks))


def test_sync_from_category_id_uses_category_id_and_filters_valid_codes(monkeypatch):
    cursor = _FakeCursor()
    tq = _FakeTq()
    monkeypatch.setattr(tdx_block_service, "get_conn", lambda: _FakeConn(cursor))
    monkeypatch.setattr(tdx_block_service, "_ensure_tq", lambda: tq)

    result = tdx_block_service.sync_from_category_id(7)

    assert "WHERE c.id = %s" in cursor.sql
    assert cursor.params == (7,)
    assert tq.created == [("AIstock_7", "短线池")]
    assert tq.cleared == ["AIstock_7"]
    assert tq.sent == [("AIstock_7", ["000001.SZ", "600519.SH"])]
    assert result == {
        "name": "AIstock_7",
        "display_name": "短线池",
        "count": 2,
        "codes": ["000001.SZ", "600519.SH"],
    }


def test_tdx_blocks_router_sync_from_category_id(monkeypatch):
    monkeypatch.setattr(tdx_blocks.tdx_block_service, "is_available", lambda: True)
    monkeypatch.setattr(
        tdx_blocks.tdx_block_service,
        "sync_from_category_id",
        lambda category_id: {
            "name": f"AIstock_{category_id}",
            "display_name": "短线池",
            "count": 1,
            "codes": ["000001.SZ"],
        },
    )
    app = FastAPI()
    app.include_router(tdx_blocks.router, prefix="/api/v1")

    response = TestClient(app).post(
        "/api/v1/tdx-blocks/sync-from-category-id",
        json={"category_id": 7},
    )

    assert response.status_code == 200
    assert response.json()["name"] == "AIstock_7"


def test_tdx_blocks_router_fails_closed_when_tdx_unavailable(monkeypatch):
    monkeypatch.setattr(tdx_blocks.tdx_block_service, "is_available", lambda: False)
    app = FastAPI()
    app.include_router(tdx_blocks.router, prefix="/api/v1")

    response = TestClient(app).post(
        "/api/v1/tdx-blocks/sync-from-category-id",
        json={"category_id": 7},
    )

    assert response.status_code == 503
    assert "TDX_CLIENT_PATH" in response.json()["detail"]
