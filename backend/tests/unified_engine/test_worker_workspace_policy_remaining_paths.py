import json
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.routers import rdagent as rdagent_router
from backend.routers import rdagent_sync_admin
from backend.services.paper_trading import db_selection_service as db_selection
from backend.services.paper_trading import training_service as paper_training
from backend.services.rdagent_task_sync_service import RDAgentTaskSyncService
from backend.services import rdagent_task_sync_service as task_sync_module
from backend.services import rdagent_catalog_etl_service as catalog_etl
from backend.services.rdagent_asset_service import RDAgentAssetService
from backend.services import rdagent_factor_catalog_sync as factor_sync
from backend.services import rdagent_model_catalog_sync as model_sync
from backend.services.selection_center import hmm_runtime
from backend.services.strategy_package.workspace_policy import (
    ensure_not_forbidden_worker_workspace_path,
    is_forbidden_worker_workspace_path,
)
from backend.services.trading_core.errors import StrategyPackageValidationError


class _OneRowConn:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def cursor(self, *args, **kwargs):
        return self

    def execute(self, *args, **kwargs):
        return None

    def fetchone(self):
        return self.row


def test_paper_db_selection_qe_paths_use_node_api_resolver_not_workspace_path() -> None:
    source = Path(db_selection.__file__).read_text(encoding="utf-8")
    relevant = source[source.index("def check_factor_readiness"): source.index("# ============================================================\n# Main entry point")]

    assert "SELECT workspace_path FROM qe_experiments" not in relevant
    assert "Path(row[0])" not in relevant
    assert "_load_experiment_manifest" not in relevant
    assert "_prepare_qe_runtime_workspace_for_db_selection" in relevant


def test_paper_db_selection_prepare_materializes_qe_assets_with_resolver(tmp_path, monkeypatch) -> None:
    workspace = tmp_path / "runtime_workspace"
    workspace.mkdir()
    manifest_path = workspace / "manifest.json"
    manifest_path.write_text(json.dumps({"primary_assets": {}, "assets": {}}), encoding="utf-8")

    class FakeResolver:
        def __init__(self, cache_root):
            self.cache_root = Path(cache_root)

        def load_source(self, experiment_id):
            return SimpleNamespace(
                qe_task_id="task-a",
                qe_loop_id="Loop1",
                execution_node_id="node-a",
            )

        def prepare_workspace(self, *, package_id, manifest_sha256, source):
            assert package_id.startswith("legacy_db_selection_qe_unit")
            assert manifest_sha256
            return SimpleNamespace(workspace_path=workspace, manifest_path=manifest_path)

    monkeypatch.setattr(db_selection, "QEExperimentRuntimeAssetResolver", FakeResolver)

    task_dir, manifest = db_selection._prepare_qe_runtime_workspace_for_db_selection("qe_unit")

    assert task_dir == workspace
    assert manifest == {"primary_assets": {}, "assets": {}}


def test_paper_db_selection_factor_code_refuses_worker_path(tmp_path, monkeypatch) -> None:
    worker_root = tmp_path / "worker_qe_workspace"
    worker_root.mkdir()
    worker_code = worker_root / "factor.py"
    worker_code.write_text("x = 1", encoding="utf-8")
    monkeypatch.setenv("QE_WORKSPACE_WIN", str(worker_root))
    monkeypatch.setattr(db_selection, "get_conn", lambda: _OneRowConn((str(worker_code), None)))

    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        db_selection._load_factor_code("bad_factor")


def test_paper_training_source_config_refuses_worker_path(tmp_path, monkeypatch) -> None:
    worker_root = tmp_path / "worker_rdagent"
    worker_config = worker_root / "task-a" / "conf.yaml"
    worker_config.parent.mkdir(parents=True)
    worker_config.write_text("task: {}", encoding="utf-8")
    monkeypatch.setenv("RDAGENT_WORKSPACE_WIN", str(worker_root))

    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        paper_training.TrainingService._load_source_config({"source_config_path": str(worker_config)})


def test_paper_training_catalog_workspace_path_is_metadata_only(tmp_path, monkeypatch) -> None:
    worker_root = tmp_path / "worker_rdagent"
    worker_workspace = worker_root / "task-a" / "loop1"
    worker_workspace.mkdir(parents=True)
    monkeypatch.setenv("RDAGENT_WORKSPACE_WIN", str(worker_root))
    monkeypatch.setattr(paper_training, "get_conn", lambda: _OneRowConn((None, None, str(worker_workspace))))

    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        paper_training.TrainingService._load_source_config(
            {"signal_source": "rdagent_task", "signal_source_id": "task-a", "signal_loop_id": 1}
        )


def test_worker_policy_allows_wsl_mounted_aistock_runtime_cache(monkeypatch) -> None:
    safe_root = "/mnt/t/aistock_safe/rdagent_assets/strategy_package_runtime"
    mounted_runtime_workspace = f"{safe_root}/pkg/hash"
    monkeypatch.setenv("AISTOCK_SAFE_ARTIFACT_ROOTS", safe_root)

    ensure_not_forbidden_worker_workspace_path(
        mounted_runtime_workspace,
        purpose="StrategyPackage WSL runtime cache",
    )
    assert is_forbidden_worker_workspace_path(mounted_runtime_workspace) is False


def test_worker_policy_still_refuses_wsl_worker_workspace(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_SAFE_ARTIFACT_ROOTS", "/mnt/t/aistock_safe/rdagent_assets/strategy_package_runtime")

    assert is_forbidden_worker_workspace_path("/mnt/t/remote/rdagent_workspace/task-a") is True


def test_rdagent_local_manifest_read_refuses_worker_path(tmp_path, monkeypatch) -> None:
    worker_root = tmp_path / "worker_rdagent"
    worker_manifest = worker_root / "task-a" / "manifest.json"
    worker_manifest.parent.mkdir(parents=True)
    worker_manifest.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("RDAGENT_WORKSPACE_WIN", str(worker_root))
    monkeypatch.setattr(
        task_sync_module,
        "_get_task_row",
        lambda task_id: {"task_id": task_id, "manifest_path": str(worker_manifest)},
    )
    service = RDAgentTaskSyncService.__new__(RDAgentTaskSyncService)
    service.assets_root = tmp_path / "rdagent_assets" / "rdagent_tasks"

    result = service.get_local_manifest_text(task_id="task-a")

    assert result["ok"] is False
    assert "manifest_path_forbidden" in result["error"]


def test_rdagent_router_asset_status_refuses_worker_manifest_path(tmp_path, monkeypatch) -> None:
    worker_root = tmp_path / "worker_rdagent"
    worker_manifest = worker_root / "task-a" / "manifest.json"
    worker_manifest.parent.mkdir(parents=True)
    worker_manifest.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("RDAGENT_WORKSPACE_WIN", str(worker_root))

    status = rdagent_router._get_local_task_assets_status(
        manifest_path=str(worker_manifest),
        manifest_obj={"primary_assets": {"factor_entry_relpath": "factor.py", "model_weight_relpath": "model.pkl"}},
    )

    assert status["task_dir"] is None
    assert "direct worker workspace" in status["policy_error"]
    assert status["factor_entry"]["exists"] is False
    assert status["model_weight"]["exists"] is False


def test_rdagent_factor_and_model_sync_refuse_worker_task_dir(tmp_path, monkeypatch) -> None:
    worker_root = tmp_path / "worker_rdagent"
    worker_task_dir = worker_root / "task-a"
    worker_task_dir.mkdir(parents=True)
    monkeypatch.setenv("RDAGENT_WORKSPACE_WIN", str(worker_root))

    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        factor_sync._ensure_local_task_dir(
            str(worker_task_dir),
            purpose="unit test factor sync worker refusal",
        )

    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        model_sync._save_model_code_to_file(str(worker_task_dir), 1, "class M: pass")

    assert not (worker_task_dir / "models").exists()


def test_rdagent_asset_service_refuses_worker_bundle_base_dir(tmp_path, monkeypatch) -> None:
    worker_root = tmp_path / "worker_rdagent_assets"
    worker_root.mkdir()
    monkeypatch.setenv("RDAGENT_WORKSPACE_WIN", str(worker_root))

    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        RDAgentAssetService(base_dir=str(worker_root))


def _test_owned_bundle_service(tmp_path, monkeypatch) -> RDAgentAssetService:
    base_dir = tmp_path / "rdagent_assets"
    monkeypatch.setenv("AISTOCK_SAFE_ARTIFACT_ROOTS", str(base_dir / "production_bundles"))
    return RDAgentAssetService(base_dir=str(base_dir))


def test_rdagent_asset_service_refuses_bundle_id_path_traversal(tmp_path, monkeypatch) -> None:
    service = _test_owned_bundle_service(tmp_path, monkeypatch)

    with pytest.raises(StrategyPackageValidationError, match="single safe path segment"):
        service.get_bundle_path("../escape")


def test_rdagent_asset_service_refuses_zip_slip_members(tmp_path, monkeypatch) -> None:
    service = _test_owned_bundle_service(tmp_path, monkeypatch)
    zip_path = tmp_path / "bad_bundle.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("../escape.txt", "should not be written")

    with zipfile.ZipFile(zip_path, "r") as archive:
        with pytest.raises(StrategyPackageValidationError, match="local bundle root"):
            service._safe_extract_zip(archive, service.get_bundle_path("bundle-a"))

    assert not (service.bundles_dir / "escape.txt").exists()
    assert not (tmp_path / "escape.txt").exists()


def test_rdagent_asset_service_refuses_manifest_relpath_traversal(tmp_path, monkeypatch) -> None:
    service = _test_owned_bundle_service(tmp_path, monkeypatch)
    bundle_path = service.get_bundle_path("bundle-a")
    bundle_path.mkdir(parents=True)
    (bundle_path / "weights").mkdir()
    (bundle_path / "weights" / "model.pkl").write_bytes(b"model")
    (bundle_path / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "primary_workspace_id": "ws-1",
                "primary_assets": {
                    "factor_entry_relpath": "../escape.py",
                    "model_weight_relpath": "weights/model.pkl",
                    "config_relpath": None,
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(StrategyPackageValidationError, match="local bundle root"):
        service.get_strategy_files("bundle-a", "ws-1")


def test_rdagent_asset_service_refuses_workspace_id_path_traversal(tmp_path, monkeypatch) -> None:
    service = _test_owned_bundle_service(tmp_path, monkeypatch)
    bundle_path = service.get_bundle_path("bundle-a")
    bundle_path.mkdir(parents=True)

    with pytest.raises(StrategyPackageValidationError, match="local bundle root"):
        service.get_strategy_files("bundle-a", "../escape")


def test_rdagent_workspace_path_normalization_preserves_remote_metadata() -> None:
    remote_path = "/mnt/f/rdagent_workspace/task-a"

    assert task_sync_module._normalize_workspace_path(remote_path) == remote_path


def test_selection_hmm_runtime_refuses_mnt_worker_artifact_path() -> None:
    with pytest.raises(StrategyPackageValidationError, match="direct worker workspace"):
        hmm_runtime._resolve_local_path("/mnt/f/worker_hmm/models.json")


def test_rdagent_catalog_etl_never_reads_workspace_path_model_meta(monkeypatch) -> None:
    def fail_path(*args, **kwargs):
        raise AssertionError("workspace_path must remain remote metadata, not a local Path")

    monkeypatch.setattr(catalog_etl, "Path", fail_path)

    payload = {
        "results": [
            {
                "task_id": "task-a",
                "loop_results": [
                    {
                        "loop_id": 1,
                        "workspace_path": "/mnt/f/rdagent_workspace/task-a/loop1",
                        "metrics": {"IC": 0.12},
                        "factors": [{"factor_name": "factor_a"}],
                        "model_weight": {"model_type": "LGBModel"},
                    }
                ],
            }
        ]
    }

    result = catalog_etl._build_loop_payload_from_scan(payload)

    loop = result["loops"][0]
    assert loop["workspace_path"] == "/mnt/f/rdagent_workspace/task-a/loop1"
    assert loop["raw_payload"]["model_type"] == "LGBModel"
    assert loop["raw_payload"]["model_meta_path"] is None


def test_rdagent_sync_admin_complete_assets_uses_results_api_not_wsl(monkeypatch) -> None:
    source = Path(rdagent_sync_admin.__file__).read_text(encoding="utf-8")
    complete_assets_segment = source[source.index("def get_task_complete_assets"):]

    assert "subprocess" not in complete_assets_segment
    assert "['wsl'" not in complete_assets_segment
    assert "QLIB_WSL_PYTHON" not in complete_assets_segment

    calls = []

    class FakeClient:
        def __init__(self):
            calls.append(("default", None))

        @classmethod
        def for_node(cls, node_id):
            calls.append(("node", node_id))
            return cls()

        def get_task_complete_assets(self, task_id):
            calls.append(("complete_assets", task_id))
            return {"ok": True, "task_id": task_id}

    monkeypatch.setattr(rdagent_sync_admin, "RDAgentResultsApiClient", FakeClient)

    result = rdagent_sync_admin.get_task_complete_assets("task-a")

    assert result == {"ok": True, "task_id": "task-a"}
    assert ("complete_assets", "task-a") in calls
