"""Real file receipts and ASGI reads; no backend startup or database access."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from backend.services.simulation_execution.localsim.dual_mode_projection import DualModeReadService
from backend.services.simulation_execution.localsim.dual_mode_research import run_study
from backend.services.simulation_execution.localsim.timing_store import read_json
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture


def test_readonly_router_complete_accounts_paging_and_corrupt_receipt(tmp_path, monkeypatch):
    request, root = dual_fixture(tmp_path, monkeypatch, two_days=False, matrix="BASIC_2")
    materialized = run_study(request)
    service = DualModeReadService(root=root, name_resolver=lambda *args: "测试模型＋两项因子")
    overview = service.list_studies()
    assert len(overview["items"]) == 1 and overview["items"][0]["complete"]
    key = overview["items"][0]["key"]
    details = service.detail(key)
    assert len(details["accounts"]) == 2
    assert {a["mode"] for a in details["accounts"]} == {"原生执行", "模拟人工"}
    account = details["accounts"][0]["key"]
    first = service.account_view(key, account, view="trades", page_size=1)
    assert first["total_count"] >= 3 and first["next_cursor"]
    following = service.account_view(key, account, view="trades", page_size=1, cursor=first["next_cursor"])
    assert first["snapshot_set"] == following["snapshot_set"]
    assert first["items"] != following["items"]
    for a in details["accounts"]:
        for view in ("positions", "trades", "orders", "actions", "pnl"):
            result = service.account_view(key,a["key"],view=view,page_size=2)
            assert len(result["items"]) <= 2
            if view == "orders":
                assert all(item["time"] for item in result["items"])  # From a hash-checked parent audit, not missing fact timestamps.
    with pytest.raises(RuntimeConfigInvalidError):
        service.account_view(key, details["accounts"][1]["key"], view="trades", cursor=first["next_cursor"])
    with pytest.raises(RuntimeConfigInvalidError):
        service.account_view(key, account, view="pnl", start="not-a-date")
    with pytest.raises(DataUnavailableError):
        service.detail("../request")
    import backend.db.pg_pool as pool
    def forbidden(*args, **kwargs):
        raise AssertionError("Read-only research must not access a database")
    monkeypatch.setattr(pool, "get_conn", forbidden)
    from backend.routers import simulation_runtime as api
    app = FastAPI()
    app.include_router(api.router, prefix="/api/v1")
    app.dependency_overrides[api.get_localsim_dual_mode_service] = lambda: service
    with TestClient(app) as client:
        prefix = "/api/v1/simulation-runtime/localsim/comparisons"
        assert client.get(prefix).status_code == 200
        assert client.get(prefix + "/" + key).json()["complete"]
        result = client.get(prefix + f"/{key}/accounts/{account}", params={"view": "pnl"})
        assert result.status_code == 200 and len(result.json()["items"]) == 1
        assert client.get(prefix + f"/{key}/accounts/{account}", params={"view":"pnl", "start":"bad"}).status_code == 422
        assert client.post(prefix + "/" + key).status_code == 405
    results = read_json(Path(materialized["results"]["path"]))
    day = read_json(Path(results["accounts"][0]["days"][0]["path"]))
    receipt = Path(day["account_receipt"]["path"])
    receipt.write_bytes(receipt.read_bytes() + b"\n")
    with pytest.raises(DataUnavailableError, match="完整性校验失败"):
        service.account_view(key, account, view="positions")


def test_unconfigured_research_does_not_touch_native_accounts(monkeypatch):
    monkeypatch.delenv("LOCALSIM_DUAL_MODE_RESEARCH_ROOT", raising=False)
    service = DualModeReadService()
    assert service.list_studies()["items"] == []
    assert service.list_studies()["configured"] is False
