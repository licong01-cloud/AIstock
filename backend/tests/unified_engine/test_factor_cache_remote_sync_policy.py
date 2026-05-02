import json
from pathlib import Path

import pytest

from backend.services.quantevolver import factor_cache_remote_sync_service as sync_module
from backend.services.quantevolver.factor_cache_remote_sync_service import (
    FactorCacheRemoteSyncService,
    RemoteCacheNode,
)
from backend.services.trading_core.errors import StrategyPackageValidationError


class _RowsConn:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def cursor(self, *args, **kwargs):
        return self

    def execute(self, *args, **kwargs):
        return None

    def fetchall(self):
        return self.rows


class _FakeNodeApi:
    def __init__(self, *, remote_meta=None, exists=True):
        self.remote_meta = remote_meta or {"factors": {}}
        self.exists = exists
        self.uploads = []

    def get_meta(self, *, cache_dir):
        return self.remote_meta

    def factor_exists(self, *, factor_name, cache_dir):
        return self.exists

    def upload_sync_bundle(self, *, cache_dir, factor_files, merged_meta, timeout_s):
        self.uploads.append(
            {
                "cache_dir": cache_dir,
                "factor_names": sorted(factor_files),
                "paths": {name: str(path) for name, path in factor_files.items()},
                "merged_meta": merged_meta,
                "timeout_s": timeout_s,
            }
        )
        return {"ok": True, "uploaded": sorted(factor_files)}


def _make_service(tmp_path, monkeypatch, fake_api):
    root = tmp_path / "rdagent_assets" / "factor_values"
    monkeypatch.setenv("AISTOCK_SAFE_ARTIFACT_ROOTS", str(root))
    service = FactorCacheRemoteSyncService(
        local_cache_root=root,
        node_api_client_factory=lambda node, timeout_s: fake_api,
    )
    node = RemoteCacheNode(
        node_id="node-api",
        display_name="Node API",
        api_base_url="http://127.0.0.1:9000",
        factor_cache_dir="/home/node/aistock_cache/factor_values",
        status="online",
    )
    service.get_node = lambda node_id: node
    return service, root, node


def _write_factor_cache(root: Path, factor_name: str = "AlphaA") -> None:
    (root / "single").mkdir(parents=True)
    (root / "single" / f"{factor_name}.parquet").write_bytes(b"PAR1")
    (root / "_meta.json").write_text(
        json.dumps(
            {
                "as_of_date": "2026-05-02",
                "factors": {
                    factor_name: {
                        "status": "ok",
                        "date_range": "2020-01-01~2026-05-02",
                        "source_hash_raw": "hash-a",
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def test_factor_cache_remote_sync_has_no_direct_worker_directory_commands() -> None:
    source = Path(sync_module.__file__).read_text(encoding="utf-8")

    assert "subprocess" not in source
    assert "['wsl'" not in source
    assert '"wsl"' not in source
    assert "rsync" not in source
    assert "ssh" not in source
    assert "_run_wsl_bash" not in source
    assert "_run_rsync" not in source
    assert "_win_to_wsl" not in source


def test_factor_cache_remote_sync_lists_localhost_api_nodes_without_ssh_user(monkeypatch) -> None:
    rows = [
        {
            "node_id": "wsl2-api",
            "display_name": "WSL API",
            "api_base_url": "http://127.0.0.1:9000",
            "factor_cache_dir": None,
            "status": "online",
        }
    ]
    monkeypatch.setattr(sync_module, "get_conn", lambda: _RowsConn(rows))

    nodes = FactorCacheRemoteSyncService().list_remote_nodes()

    assert [node.node_id for node in nodes] == ["wsl2-api"]
    assert nodes[0].api_base_url == "http://127.0.0.1:9000"
    assert nodes[0].resolved_cache_dir == "node-api-default"


def test_factor_cache_remote_sync_refuses_worker_cache_root(tmp_path, monkeypatch) -> None:
    worker_root = tmp_path / "worker_qe"
    worker_root.mkdir()
    monkeypatch.setenv("QE_WORKSPACE_WIN", str(worker_root))

    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        FactorCacheRemoteSyncService(local_cache_root=worker_root / "factor_values")


def test_factor_cache_remote_sync_uploads_through_node_api(tmp_path, monkeypatch) -> None:
    fake_api = _FakeNodeApi(remote_meta={"factors": {}})
    service, root, _node = _make_service(tmp_path, monkeypatch, fake_api)
    _write_factor_cache(root)

    job = service.sync_to_node("node-api", configure_default_dir=True)

    assert job["status"] == "completed"
    assert job["sync_count"] == 1
    assert len(fake_api.uploads) == 1
    upload = fake_api.uploads[0]
    assert upload["factor_names"] == ["AlphaA"]
    assert upload["cache_dir"] == "/home/node/aistock_cache/factor_values"
    assert upload["paths"]["AlphaA"].endswith("AlphaA.parquet")
    assert upload["merged_meta"]["_last_remote_sync"]["transport"] == "node_api"


def test_factor_cache_remote_sync_skips_synced_factor_via_node_api_status(tmp_path, monkeypatch) -> None:
    fake_api = _FakeNodeApi(
        remote_meta={
            "factors": {
                "AlphaA": {
                    "date_range": "2020-01-01~2026-05-02",
                    "source_hash_raw": "hash-a",
                }
            }
        },
        exists=True,
    )
    service, root, node = _make_service(tmp_path, monkeypatch, fake_api)
    _write_factor_cache(root)

    plan = service.plan_sync(node)

    assert plan["sync_items"] == []
    assert plan["skipped_items"][0]["factor_name"] == "AlphaA"


def test_factor_cache_remote_sync_refuses_factor_name_traversal(tmp_path, monkeypatch) -> None:
    fake_api = _FakeNodeApi(remote_meta={"factors": {}})
    service, root, _node = _make_service(tmp_path, monkeypatch, fake_api)
    (root / "single").mkdir(parents=True)
    (root / "_meta.json").write_text(
        json.dumps({"factors": {"../escape": {"status": "ok", "date_range": "2020~2026"}}}),
        encoding="utf-8",
    )

    with pytest.raises(StrategyPackageValidationError, match="single safe path segment"):
        service.plan_sync(RemoteCacheNode("node-api", None, "http://127.0.0.1:9000", None, "online"))
