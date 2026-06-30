from __future__ import annotations

import io
import tarfile
from pathlib import Path
from typing import Any

import pytest

from backend.services.strategy_package.models import FactorAsset, SourceType
from backend.services.strategy_package.package_asset_freeze import (
    QERuntimeAssetLocator,
    StrategyPackageAssetSource,
    _params_from_mlruns_archive,
    _remote_relpath,
)
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    DataUnavailableError,
    RuntimeConfigInvalidError,
    StrategyPackageValidationError,
)
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class MissingModelStore:
    def get_pointer(self, *, experiment_id: str) -> dict[str, Any]:
        raise DataUnavailableError(
            "missing pointer",
            context={"reason_code": "unit_missing_pointer", "experiment_id": experiment_id},
        )

    def pull_params_path(self, *, run_id: str) -> Path:
        raise DataUnavailableError("missing params", context={"reason_code": "unit_missing_params", "run_id": run_id})


class PointerModelStore:
    def __init__(self, uri: str) -> None:
        self.uri = uri

    def get_pointer(self, *, experiment_id: str) -> dict[str, Any]:
        return {"mlflow_artifact_uri": self.uri, "pointer_status": "ok", "experiment_id": experiment_id}

    def pull_params_path(self, *, run_id: str) -> Path:  # pragma: no cover - central experiment hit should return first.
        raise AssertionError("pull_params_path should not be called on experiment pointer hit")


class RunPointerModelStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def get_pointer(self, *, experiment_id: str) -> dict[str, Any]:
        raise DataUnavailableError("missing pointer", context={"experiment_id": experiment_id})

    def pull_params_path(self, *, run_id: str) -> Path:
        return self.path


class ArtifactStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def resolve_artifact_path(self, *_args: Any, **_kwargs: Any) -> Path:
        return self.path


class FakeWorkspaceClient:
    def __init__(
        self,
        *,
        params_payload: bytes | BaseException | None = None,
        files: dict[str, bytes | BaseException] | None = None,
    ) -> None:
        self.params_payload = params_payload
        self.files = files or {}

    async def __aenter__(self) -> "FakeWorkspaceClient":
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def download_mlruns_params(self, task_id: str, loop_id: str) -> bytes | None:
        if isinstance(self.params_payload, BaseException):
            raise self.params_payload
        return self.params_payload

    async def download_workspace_file_bytes(self, task_id: str, loop_id: str, file_path: str) -> bytes:
        value = self.files.get(file_path)
        if isinstance(value, BaseException):
            raise value
        if value is None:
            raise RuntimeError(f"missing file {file_path}")
        return value


class RecordingWorkspaceFactory:
    def __init__(self, client: FakeWorkspaceClient) -> None:
        self.client = client
        self.calls: list[str] = []

    def __call__(self, node_id: str) -> FakeWorkspaceClient:
        self.calls.append(node_id)
        return self.client


class FakeConn:
    def __init__(self, rows_by_kind: dict[str, list[dict[str, Any]]]) -> None:
        self.rows_by_kind = rows_by_kind

    def __enter__(self) -> "FakeConn":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def cursor(self, **_kwargs: Any) -> "FakeCursor":
        return FakeCursor(self.rows_by_kind)


class FakeCursor:
    def __init__(self, rows_by_kind: dict[str, list[dict[str, Any]]]) -> None:
        self.rows_by_kind = rows_by_kind
        self.rows: list[dict[str, Any]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, query: str, _params: tuple[Any, ...] = ()) -> None:
        if "FROM strategy_pkg.candidate_strategy_package" in query:
            self.rows = list(self.rows_by_kind.get("candidate", []))
        elif "FROM qe_experiments e" in query:
            self.rows = list(self.rows_by_kind.get("task_loop", []))
        elif "FROM qe_experiments" in query:
            self.rows = list(self.rows_by_kind.get("experiment", []))
        elif "FROM qe_evolution_loops" in query:
            self.rows = list(self.rows_by_kind.get("loop_node", []))
        elif "FROM infra.compute_nodes" in query:
            self.rows = list(self.rows_by_kind.get("compute_nodes", []))
        elif "FROM aistock_factor_catalog" in query:
            self.rows = list(self.rows_by_kind.get("factor_catalog", []))
        else:  # pragma: no cover - defensive fail-fast for changed SQL.
            raise AssertionError(f"unexpected query: {query}")

    def fetchone(self) -> dict[str, Any] | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows


def _manifest(*, task_id: str = "qe_unit_task", loop_id: str = "Loop2", node_id: str = "node-unit"):
    base = make_manifest()
    return base.model_copy(
        update={
            "source": base.source.model_copy(
                update={
                    "source_type": SourceType.QE_EVOLUTION_LOOP,
                    "source_id": task_id,
                    "loop_id": loop_id,
                    "run_id": f"{task_id}_L2",
                }
            ),
            "source_evidence": {
                "experiment_id": f"{task_id}_L2",
                "qe_task_id": task_id,
                "qe_loop_id": loop_id,
                "execution_node_id": node_id,
            },
            "manifest_sha256": None,
        }
    )


def _conn_factory_raising() -> Any:
    raise RuntimeError("unit DB unavailable")


def _tar_with_params(data: bytes = b"model-params") -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        info = tarfile.TarInfo("mlruns/run_1/artifacts/params.pkl")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    return output.getvalue()


def _tar_with_file(name: str, data: bytes = b"x") -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        info = tarfile.TarInfo(name)
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    return output.getvalue()


def _tar_with_symlink() -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        info = tarfile.TarInfo("mlruns/run_1/artifacts/params.pkl")
        info.type = tarfile.SYMTYPE
        info.linkname = "../unsafe.pkl"
        archive.addfile(info)
    return output.getvalue()


def test_central_model_hit_does_not_call_qe_node(tmp_path: Path) -> None:
    params_path = tmp_path / "params.pkl"
    params_path.write_bytes(b"central-params")
    workspace_factory = RecordingWorkspaceFactory(FakeWorkspaceClient(params_payload=AssertionError("should not call node")))
    source = StrategyPackageAssetSource(
        model_store=PointerModelStore("artifact://central/run"),
        artifact_store=ArtifactStore(params_path),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=workspace_factory,
        local_workspace_roots=[],
    )

    result = source.model_params_bytes(_manifest())

    assert result.data == b"central-params"
    assert result.source_uri == "artifact://central/run/model_params"
    assert workspace_factory.calls == []


def test_central_run_id_pointer_hit_does_not_call_qe_node(tmp_path: Path) -> None:
    params_path = tmp_path / "params.pkl"
    params_path.write_bytes(b"run-pointer-params")
    workspace_factory = RecordingWorkspaceFactory(FakeWorkspaceClient(params_payload=AssertionError("should not call node")))
    source = StrategyPackageAssetSource(
        model_store=RunPointerModelStore(params_path),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=workspace_factory,
        local_workspace_roots=[],
    )

    result = source.model_params_bytes(_manifest())

    assert result.data == b"run-pointer-params"
    assert result.source_uri.startswith("aistock-prediction-store://runs/")
    assert workspace_factory.calls == []


def test_central_miss_qe_node_model_params_archive_hit() -> None:
    workspace_factory = RecordingWorkspaceFactory(FakeWorkspaceClient(params_payload=_tar_with_params(b"node-params")))
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=workspace_factory,
        local_workspace_roots=[],
    )

    result = source.model_params_bytes(_manifest())

    assert result.data == b"node-params"
    assert result.source_uri.startswith("qe-workspace://node/node-unit/tasks/qe_unit_task/loops/Loop2/")
    assert workspace_factory.calls == ["node-unit"]


def test_node_miss_local_workspace_model_params_hit(tmp_path: Path) -> None:
    params_path = tmp_path / "qe_unit_task" / "Loop2" / "mlruns" / "run_1" / "artifacts" / "params.pkl"
    params_path.parent.mkdir(parents=True)
    params_path.write_bytes(b"local-params")
    workspace_factory = RecordingWorkspaceFactory(FakeWorkspaceClient(params_payload=RuntimeError("node missing")))
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=workspace_factory,
        local_workspace_roots=[tmp_path],
    )

    result = source.model_params_bytes(_manifest())

    assert result.data == b"local-params"
    assert result.source_uri.startswith("file://")
    assert "params.pkl" in result.source_uri
    assert workspace_factory.calls == ["node-unit", "wsl2-5080"]


def test_factor_catalog_miss_qe_node_factor_file_hit() -> None:
    workspace_factory = RecordingWorkspaceFactory(
        FakeWorkspaceClient(files={"factors/factor_a.py": b"# recovered factor\nVALUE = 1\n"})
    )
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=workspace_factory,
        local_workspace_roots=[],
    )

    result = source.factor_code_bytes(FactorAsset(factor_id="factor_a", factor_name="factor_a"), _manifest())

    assert result.data == b"# recovered factor\nVALUE = 1\n"
    assert result.source_uri.startswith("qe-workspace://node/node-unit/tasks/qe_unit_task/loops/Loop2/factors/factor_a.py")
    assert workspace_factory.calls == ["node-unit"]


def test_factor_catalog_unique_code_returns_central_factor() -> None:
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=lambda: FakeConn(
            {
                "factor_catalog": [
                    {"id": "factor_a_catalog", "code_text": "VALUE = 10\n", "source": "catalog", "is_available": True}
                ]
            }
        ),
        workspace_client_factory=RecordingWorkspaceFactory(FakeWorkspaceClient()),
        local_workspace_roots=[],
    )

    result = source.factor_code_bytes(FactorAsset(factor_id="factor_a", factor_name="factor_a"), _manifest())

    assert result.data == b"VALUE = 10\n"
    assert result.source_uri == "aistock_factor_catalog:factor_a_catalog:code_text"


def test_empty_factor_name_fails_before_source_lookup() -> None:
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=RecordingWorkspaceFactory(FakeWorkspaceClient()),
        local_workspace_roots=[],
    )

    with pytest.raises(StrategyPackageValidationError) as excinfo:
        source.factor_code_bytes(FactorAsset(factor_id="", factor_name=""), _manifest())

    assert excinfo.value.context["reason_code"] == "strategy_package_factor_code_missing"


def test_factor_catalog_ambiguous_qe_node_factor_file_wins() -> None:
    workspace_factory = RecordingWorkspaceFactory(
        FakeWorkspaceClient(files={"factors/factor_a.py": b"# exact workspace factor\nVALUE = 3\n"})
    )
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=lambda: FakeConn(
            {
                "factor_catalog": [
                    {"id": "factor_a_old", "code_text": "VALUE = 1\n", "source": "old", "is_available": True},
                    {"id": "factor_a_new", "code_text": "VALUE = 2\n", "source": "new", "is_available": True},
                ]
            }
        ),
        workspace_client_factory=workspace_factory,
        local_workspace_roots=[],
    )

    result = source.factor_code_bytes(FactorAsset(factor_id="factor_a", factor_name="factor_a"), _manifest())

    assert result.data == b"# exact workspace factor\nVALUE = 3\n"
    assert result.source_uri.startswith("qe-workspace://node/node-unit/")
    assert workspace_factory.calls == ["node-unit"]


def test_factor_catalog_ambiguous_without_workspace_fallback_fails_with_attempts() -> None:
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=lambda: FakeConn(
            {
                "factor_catalog": [
                    {"id": "factor_a_old", "code_text": "VALUE = 1\n", "source": "old", "is_available": True},
                    {"id": "factor_a_new", "code_text": "VALUE = 2\n", "source": "new", "is_available": True},
                ]
            }
        ),
        workspace_client_factory=RecordingWorkspaceFactory(
            FakeWorkspaceClient(files={"factors/factor_a.py": RuntimeError("node factor missing")})
        ),
        local_workspace_roots=[],
    )

    with pytest.raises(StrategyPackageValidationError) as excinfo:
        source.factor_code_bytes(FactorAsset(factor_id="factor_a", factor_name="factor_a"), _manifest())

    context = excinfo.value.context
    assert context["reason_code"] == "strategy_package_factor_code_ambiguous"
    assert any(attempt["source"] == "central_store" for attempt in context["attempted_sources"])
    assert any(attempt["source"] == "qe_node" for attempt in context["attempted_sources"])


def test_node_miss_local_workspace_factor_code_hit(tmp_path: Path) -> None:
    factor_path = tmp_path / "qe_unit_task" / "Loop2" / "factors" / "factor_a.py"
    factor_path.parent.mkdir(parents=True)
    factor_path.write_bytes(b"# local factor\nVALUE = 2\n")
    workspace_factory = RecordingWorkspaceFactory(FakeWorkspaceClient(files={"factors/factor_a.py": RuntimeError("node missing")}))
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=workspace_factory,
        local_workspace_roots=[tmp_path],
    )

    result = source.factor_code_bytes(FactorAsset(factor_id="factor_a", factor_name="factor_a"), _manifest())

    assert result.data == b"# local factor\nVALUE = 2\n"
    assert result.source_uri.startswith("file://")
    assert "factor_a.py" in result.source_uri
    assert workspace_factory.calls == ["node-unit", "wsl2-5080"]


def test_invalid_factor_file_name_fails_loud_after_catalog_miss() -> None:
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=RecordingWorkspaceFactory(FakeWorkspaceClient()),
        local_workspace_roots=[],
    )

    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        source.factor_code_bytes(FactorAsset(factor_id="bad", factor_name="../bad"), _manifest())

    assert excinfo.value.context["factor_name"] == "../bad"


def test_all_sources_miss_reports_central_node_and_wsl_attempts() -> None:
    workspace_factory = RecordingWorkspaceFactory(FakeWorkspaceClient(params_payload=RuntimeError("node missing")))
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=workspace_factory,
        local_workspace_roots=[],
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        source.model_params_bytes(_manifest(task_id="qe_unit_missing_999999", loop_id="Loop99"))

    context = excinfo.value.context
    assert context["reason_code"] == "strategy_package_model_params_missing"
    attempted_sources = {attempt["source"] for attempt in context["attempted_sources"]}
    assert {"central_store", "qe_node", "wsl_workspace"}.issubset(attempted_sources)
    assert any(attempt.get("error") == "node missing" or "node missing" in attempt.get("error", "") for attempt in context["attempted_sources"])


def test_no_qe_locator_still_reports_node_and_workspace_attempts() -> None:
    workspace_factory = RecordingWorkspaceFactory(FakeWorkspaceClient(params_payload=RuntimeError("should not call")))
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=workspace_factory,
        local_workspace_roots=[],
    )
    manifest = _manifest().model_copy(
        update={
            "source": _manifest().source.model_copy(update={"source_type": SourceType.QE_EXPERIMENT, "source_id": "", "run_id": None, "loop_id": None}),
            "source_evidence": {},
            "manifest_sha256": None,
        }
    )

    with pytest.raises(DataUnavailableError) as excinfo:
        source.model_params_bytes(manifest)

    attempts = excinfo.value.context["attempted_sources"]
    assert any(attempt["source"] == "qe_node" and "no QE node locator" in attempt["error"] for attempt in attempts)
    assert any(attempt["source"] == "wsl_workspace" for attempt in attempts)
    assert workspace_factory.calls == []


def test_unsafe_node_params_tar_fails_loud_without_silent_repair() -> None:
    workspace_factory = RecordingWorkspaceFactory(FakeWorkspaceClient(params_payload=_tar_with_symlink()))
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=_conn_factory_raising,
        workspace_client_factory=workspace_factory,
        local_workspace_roots=[],
    )

    with pytest.raises(ArtifactGenerationFailedError) as excinfo:
        source.model_params_bytes(_manifest())

    assert excinfo.value.context["reason_code"] == "strategy_package_mlruns_archive_unsafe"
    assert excinfo.value.context["locator"]["node_id"] == "node-unit"
    assert any(attempt["source"] == "qe_node" for attempt in excinfo.value.context["attempted_sources"])


def test_mlruns_archive_rejects_invalid_missing_params_and_unsafe_paths() -> None:
    locator = QERuntimeAssetLocator(qe_task_id="qe_task", qe_loop_id="Loop1", node_id="node")
    with pytest.raises(ArtifactGenerationFailedError):
        _params_from_mlruns_archive(b"not-a-tar", locator=locator)
    with pytest.raises(DataUnavailableError):
        _params_from_mlruns_archive(_tar_with_file("mlruns/run/artifacts/not_params.pkl"), locator=locator)
    with pytest.raises(ArtifactGenerationFailedError):
        _params_from_mlruns_archive(_tar_with_file("../artifacts/params.pkl"), locator=locator)
    with pytest.raises(ArtifactGenerationFailedError):
        _params_from_mlruns_archive(_tar_with_file("C:/unsafe/artifacts/params.pkl"), locator=locator)


def test_remote_relpath_rejects_absolute_parent_and_drive_paths() -> None:
    assert _remote_relpath("factors/factor_a.py") == "factors/factor_a.py"
    for value in ("", "/abs/path.py", "../factor.py", "C:/factor.py"):
        with pytest.raises(RuntimeConfigInvalidError):
            _remote_relpath(value)


def test_candidate_source_resolves_to_underlying_qe_coordinates() -> None:
    rows = {
        "candidate": [
            {
                "candidate_id": "csp_unit",
                "source_type": "qe_evolution_loop",
                "source_id": "qe_candidate_task_L3",
                "source_task_id": "qe_candidate_task",
                "source_loop_id": "qe_candidate_task_Loop3",
                "source_experiment_id": "qe_candidate_task_L3",
                "status": "APPROVED",
            }
        ],
        "experiment": [
            {
                "experiment_id": "qe_candidate_task_L3",
                "qe_task_id": "qe_candidate_task",
                "qe_loop_id": "Loop3",
                "loop_index": 3,
                "custom_params": {"execution_node_id": "node-exp"},
                "result_metrics": {},
            }
        ],
        "task_loop": [
            {
                "experiment_id": "qe_candidate_task_L3",
                "qe_task_id": "qe_candidate_task",
                "qe_loop_id": "Loop3",
                "loop_index": 3,
                "custom_params": {},
                "result_metrics": {},
                "node_id": "node-loop",
            }
        ],
    }
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=lambda: FakeConn(rows),
        workspace_client_factory=RecordingWorkspaceFactory(FakeWorkspaceClient(params_payload=RuntimeError("unused"))),
        local_workspace_roots=[],
    )
    manifest = _manifest(task_id="not_used", loop_id="Loop1").model_copy(
        update={
            "source": _manifest().source.model_copy(
                update={
                    "source_type": SourceType.CANDIDATE_STRATEGY_PACKAGE,
                    "source_id": "csp_unit",
                    "loop_id": None,
                    "run_id": None,
                }
            ),
            "source_evidence": {},
            "manifest_sha256": None,
        }
    )

    locators = source._runtime_asset_locators(manifest, attempts=[])  # noqa: SLF001 - candidate resolution contract.

    assert any(locator.qe_task_id == "qe_candidate_task" for locator in locators)
    assert any(locator.qe_loop_id == "Loop3" for locator in locators)
    assert any(locator.experiment_id == "qe_candidate_task_L3" for locator in locators)
    assert any(locator.node_id in {"node-exp", "node-loop"} for locator in locators)


def test_candidate_qe_experiment_source_resolves_experiment_id() -> None:
    rows = {
        "candidate": [
            {
                "candidate_id": "csp_exp",
                "source_type": "qe_experiment",
                "source_id": "qe_exp_only",
                "source_task_id": None,
                "source_loop_id": None,
                "source_experiment_id": None,
                "status": "APPROVED",
            }
        ],
        "experiment": [
            {
                "experiment_id": "qe_exp_only",
                "qe_task_id": "qe_exp_task",
                "qe_loop_id": "Loop4",
                "loop_index": 4,
                "custom_params": {"execution_node_id": "node-exp"},
                "result_metrics": {},
            }
        ],
    }
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=lambda: FakeConn(rows),
        workspace_client_factory=RecordingWorkspaceFactory(FakeWorkspaceClient(params_payload=RuntimeError("unused"))),
        local_workspace_roots=[],
    )
    manifest = _manifest().model_copy(
        update={
            "source": _manifest().source.model_copy(
                update={"source_type": SourceType.CANDIDATE_STRATEGY_PACKAGE, "source_id": "csp_exp", "loop_id": None, "run_id": None}
            ),
            "source_evidence": {},
            "manifest_sha256": None,
        }
    )

    locators = source._runtime_asset_locators(manifest, attempts=[])  # noqa: SLF001 - candidate resolution contract.

    assert any(locator.experiment_id == "qe_exp_only" for locator in locators)
    assert any(locator.qe_task_id == "qe_exp_task" and locator.qe_loop_id == "Loop4" for locator in locators)


def test_compute_node_catalog_and_wsl_root_translation_are_used(tmp_path: Path) -> None:
    rows = {
        "task_loop": [],
        "compute_nodes": [
            {
                "node_id": "node-extra",
                "workspace_base": "/mnt/f/Dev/RD-Agent-main/qe_workspace",
                "qlib_rdagent_root": "/mnt/f/Dev/RD-Agent-main",
            }
        ],
    }
    source = StrategyPackageAssetSource(
        model_store=MissingModelStore(),
        conn_factory=lambda: FakeConn(rows),
        workspace_client_factory=RecordingWorkspaceFactory(FakeWorkspaceClient(params_payload=RuntimeError("unused"))),
        local_workspace_roots=[tmp_path],
    )
    attempts: list[dict[str, Any]] = []

    locators = source._runtime_asset_locators(_manifest(), attempts=attempts)  # noqa: SLF001 - node expansion contract.
    roots = source._resolved_local_workspace_roots(attempts)  # noqa: SLF001 - local-root discovery contract.

    assert any(locator.node_id == "node-extra" for locator in locators)
    assert tmp_path in roots
    assert any(str(root).endswith("RD-Agent-main\\qe_workspace") and str(root).startswith("F:") for root in roots)
