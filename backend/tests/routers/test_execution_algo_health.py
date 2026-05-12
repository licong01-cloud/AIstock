from __future__ import annotations

from contextlib import contextmanager

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import execution_algo_health


class FakeCursor:
    def __init__(self, rows, *, has_asset_namespace: bool = True):
        self.rows = rows
        self.has_asset_namespace = has_asset_namespace
        self.description = [("exists",)]
        self.executed_sql = None
        self.mode = "exists"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql):
        self.executed_sql = sql
        if "information_schema.columns" in sql:
            self.mode = "exists"
            self.description = [("exists",)]
        else:
            self.mode = "rows"
            self.description = [
                ("algo_code",),
                ("algo_name",),
                ("default_config",),
                ("is_enabled",),
                ("supported_freqs",),
                ("min_bars",),
                ("asset_namespace",),
            ]

    def fetchone(self):
        return (self.has_asset_namespace,)

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows, *, has_asset_namespace: bool = True):
        self.rows = rows
        self.has_asset_namespace = has_asset_namespace

    def cursor(self):
        return FakeCursor(self.rows, has_asset_namespace=self.has_asset_namespace)


@contextmanager
def fake_conn(rows, *, has_asset_namespace: bool = True):
    yield FakeConn(rows, has_asset_namespace=has_asset_namespace)


def _install_fake_db(monkeypatch, rows, *, has_asset_namespace: bool = True):
    monkeypatch.setattr(execution_algo_health, "get_conn", lambda: fake_conn(rows, has_asset_namespace=has_asset_namespace))


def test_execution_algo_health_reports_ok_cached_and_missing_read_only(monkeypatch, tmp_path):
    cache_root = tmp_path / "model_cache"
    monkeypatch.setenv("AISTOCK_MODEL_CACHE_DIR", str(cache_root))
    direct_model = tmp_path / "direct.pt"
    direct_model.write_bytes(b"model")
    cached_model = cache_root / "V25_TWO_STAGE" / "late.pt"
    cached_model.parent.mkdir(parents=True)
    cached_model.write_bytes(b"model")
    alias_cached_model = cache_root / "V25_TWO_STAGE" / "early.pt"
    alias_cached_model.write_bytes(b"model")

    rows = [
        (
            "CLOSE_PRICE",
            "Close price",
            {},
            True,
            ["1d"],
            1,
            None,
        ),
        (
            "V25_TWO_STAGE",
            "V25",
            {
                "early_model_path": str(direct_model),
                "late_model_path": "/remote/models/late.pt",
            },
            True,
            ["1m"],
            240,
            None,
        ),
        (
            "V25_1_SMALL_CAP",
            "V25.1",
            {
                "early_model_path": "/remote/models/early.pt",
                "late_model_path": "/remote/models/missing_late.pt",
            },
            True,
            ["1m"],
            240,
            "V25_TWO_STAGE",
        ),
    ]
    _install_fake_db(monkeypatch, rows)
    monkeypatch.setattr(
        execution_algo_health.ModelAssetResolver,
        "_copy_to_cache",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("health endpoint must be read-only")),
    )

    response = execution_algo_health.get_execution_algo_health()

    assert response["overall_status"] == "missing"
    assert response["status_counts"] == {"ok": 1, "cached": 1, "missing": 1}
    by_code = {algo["algo_code"]: algo for algo in response["algos"]}
    assert by_code["CLOSE_PRICE"]["status"] == "ok"
    assert by_code["CLOSE_PRICE"]["assets"] == []
    assert by_code["V25_TWO_STAGE"]["status"] == "cached"
    assert [asset["status"] for asset in by_code["V25_TWO_STAGE"]["assets"]] == ["ok", "cached"]
    assert by_code["V25_TWO_STAGE"]["assets"][1]["source"] == "legacy_cache"
    assert by_code["V25_1_SMALL_CAP"]["asset_namespace"] == "V25_TWO_STAGE"
    assert by_code["V25_1_SMALL_CAP"]["status"] == "missing"
    assert by_code["V25_1_SMALL_CAP"]["assets"][0]["source"] == "legacy_cache"
    assert by_code["V25_1_SMALL_CAP"]["assets"][0]["status"] == "cached"


def test_execution_algo_health_rejects_invalid_hashed_cache_sidecar(monkeypatch, tmp_path):
    cache_root = tmp_path / "model_cache"
    monkeypatch.setenv("AISTOCK_MODEL_CACHE_DIR", str(cache_root))
    resolver = execution_algo_health.ModelAssetResolver(cache_root=cache_root)
    original_model = "/remote/models/v24.pt"
    hashed_cache = resolver._cache_destination("V24_PLAN", original_model)
    hashed_cache.parent.mkdir(parents=True)
    hashed_cache.write_bytes(b"model-without-sidecar")
    _install_fake_db(
        monkeypatch,
        [
            (
                "V24_PLAN",
                "V24",
                {"model_path": original_model},
                True,
                ["1m"],
                31,
                None,
            )
        ],
    )

    response = execution_algo_health.get_execution_algo_health()

    asset = response["algos"][0]["assets"][0]
    assert response["overall_status"] == "missing"
    assert asset["status"] == "missing"
    assert asset["source"] == "hashed_cache"
    assert "missing sidecar metadata" in asset["reason"]


def test_execution_algo_health_uses_top_level_asset_namespace(monkeypatch, tmp_path):
    cache_root = tmp_path / "model_cache"
    monkeypatch.setenv("AISTOCK_MODEL_CACHE_DIR", str(cache_root))
    cached_model = cache_root / "V25_TWO_STAGE" / "early.pt"
    cached_model.parent.mkdir(parents=True)
    cached_model.write_bytes(b"legacy-source")
    _install_fake_db(
        monkeypatch,
        [
            (
                "V25_1_SMALL_CAP",
                "V25.1",
                {
                    "early_model_path": "/remote/models/early.pt",
                    "late_model_path": "/remote/models/missing_late.pt",
                },
                True,
                ["1m"],
                240,
                "V25_TWO_STAGE",
            )
        ],
    )

    response = execution_algo_health.get_execution_algo_health()

    algo = response["algos"][0]
    assert algo["asset_namespace"] == "V25_TWO_STAGE"
    assert algo["assets"][0]["source"] == "legacy_cache"
    assert algo["assets"][0]["status"] == "cached"


def test_execution_algo_health_reports_invalid_asset_namespace(monkeypatch, tmp_path):
    monkeypatch.setenv("AISTOCK_MODEL_CACHE_DIR", str(tmp_path / "model_cache"))
    _install_fake_db(
        monkeypatch,
        [
            (
                "V25_1_SMALL_CAP",
                "V25.1",
                {
                    "early_model_path": "/remote/models/early.pt",
                    "late_model_path": "/remote/models/late.pt",
                },
                True,
                ["1m"],
                240,
                "C:V25_TWO_STAGE",
            )
        ],
    )

    response = execution_algo_health.get_execution_algo_health()

    assert response["overall_status"] == "missing"
    assert response["algos"][0]["status"] == "missing"
    assert "asset namespace is invalid" in response["algos"][0]["assets"][0]["reason"]


def test_execution_algo_health_route_is_mounted_by_router(monkeypatch, tmp_path):
    monkeypatch.setenv("AISTOCK_MODEL_CACHE_DIR", str(tmp_path / "model_cache"))
    _install_fake_db(
        monkeypatch,
        [
            (
                "TWAP",
                "TWAP",
                {},
                True,
                ["1m"],
                1,
                None,
            )
        ],
    )
    app = FastAPI()
    app.include_router(execution_algo_health.router, prefix="/api/v1")

    response = TestClient(app).get("/api/v1/execution-algos/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["overall_status"] == "ok"
    assert payload["algos"][0]["algo_code"] == "TWAP"
