from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from backend.services.multi_alpha.combine_backtest import CombineBacktestRequest
from backend.services.multi_alpha.durable_identity import (
    DurableExecutionIdentityResolver,
    _sha256_tree,
)
from backend.services.multi_alpha.durable_models import (
    DurableRunSpec,
    durable_run_request_payload,
    request_hash_for,
)
from backend.services.multi_alpha.durable_plan import DeterministicChildPlanner
from backend.services.multi_alpha.panels import PanelLegSpec
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceDatasetIdentity,
    QEWorkspaceExecutionEnvironment,
)


REPO_ROOT = Path(__file__).resolve().parents[3]


class _ModelStore:
    def __init__(self, root: Path) -> None:
        self._root = root

    def get_pointer(self, *, run_id: str) -> dict[str, str]:
        return {"mlflow_artifact_uri": f"prediction-store://runs/{run_id}"}

    def prediction_path(self, *, run_id: str) -> Path:
        return self._root / f"{run_id}.pkl"


def _request(tmp_path: Path) -> CombineBacktestRequest:
    runtime_template = tmp_path / "runtime"
    runtime_template.mkdir()
    (runtime_template / "conf.yaml").write_text("qlib: runtime", encoding="utf-8")
    return CombineBacktestRequest(
        roster=(
            PanelLegSpec("leg_a", ("qe_a_L1",)),
            PanelLegSpec("leg_b", ("qe_b_L1",)),
        ),
        oos_start="2024-07-01",
        oos_end="2026-06-29",
        weighting_schemes=("equal",),
        normalize_method="rank",
        walk_forward={"enabled": True, "window": 60, "min_periods": 20},
        backtest_config={
            "node_id": "wsl2-5080",
            "node_parallelism": {"wsl2-5080": 2},
            "runtime_template_dir": str(runtime_template),
            "conda_environment_lock_sha256": "a" * 64,
            "executor_code_commit": "b" * 40,
            "aistock_commit": "c" * 40,
        },
        baseline_leg_id="leg_a",
        topk=20,
        run_async=True,
        scheme_timeout_seconds=120,
        run_timeout_seconds=600,
    )


def _environment() -> QEWorkspaceExecutionEnvironment:
    return QEWorkspaceExecutionEnvironment(
        schema_version="qe_execution_environment_manifest_v1",
        execution_environment_snapshot_id="qeenv_test_snapshot",
        execution_environment_manifest_sha256="d" * 64,
        manifest={
            "executor_file_set_sha256": "e" * 64,
            "declared_runtime_identity": {},
        },
    )


def _dataset(*, complete: bool) -> QEWorkspaceDatasetIdentity:
    if not complete:
        return QEWorkspaceDatasetIdentity(
            schema_version="qe_dataset_identity_evidence_v1",
            complete=False,
            reason_code="qe_dataset_manifest_missing",
            missing=("qe_dataset_manifest.json",),
            acquisition_suggestions=("publish immutable dataset manifest",),
            dataset=None,
        )
    return QEWorkspaceDatasetIdentity(
        schema_version="qe_dataset_identity_v1",
        complete=True,
        reason_code=None,
        missing=(),
        acquisition_suggestions=(),
        dataset={
            "deployment_snapshot_id": "qe_data_20260721",
            "dataset_manifest_sha256": "f" * 64,
            "cutoff_trade_date": "2026-06-30",
            "qlib_calendar_sha256": "1" * 64,
            "qlib_instruments_sha256": "2" * 64,
            "st_pit_snapshot_id": "qe_st_pit_20260630",
            "st_pit_manifest_sha256": "3" * 64,
            "resolved_node_id": "wsl2-5080",
            "resolved_data_root_uri": "/home/lc999/data/factor_data",
        },
    )


def test_execution_identity_is_content_addressed_and_child_plan_carries_it(tmp_path: Path) -> None:
    for run_id in ("qe_a_L1", "qe_b_L1"):
        (tmp_path / f"{run_id}.pkl").write_bytes(f"prediction:{run_id}".encode("utf-8"))
    request = _request(tmp_path)
    resolver = DurableExecutionIdentityResolver(
        model_store=_ModelStore(tmp_path),  # type: ignore[arg-type]
        environment_loader=lambda _node_id: _environment(),
        dataset_loader=lambda _node_id, _root: _dataset(complete=True),
        node_info_resolver=lambda _node_id: SimpleNamespace(qlib_data_path="/home/lc999/data/factor_data"),
        source_root=REPO_ROOT,
    )

    resolution = resolver.resolve(request=request, node_id="wsl2-5080")

    assert resolution.complete is True
    assert resolution.identity is not None
    assert resolution.identity.payload["dataset"]["dataset_manifest_sha256"] == "f" * 64
    assert resolution.identity.payload["runtime"]["execution_environment_manifest_sha256"] == "d" * 64
    assert len(resolution.identity.payload["prediction_sources"]) == 2

    run_kwargs = {
        "run_id": "macb_identity_test",
        "task_id": "mact_identity_test",
        "roster_hash": "roster_identity_test",
        "roster": [{"leg_id": "leg_a", "seed_run_ids": ["qe_a_L1"]}, {"leg_id": "leg_b", "seed_run_ids": ["qe_b_L1"]}],
        "oos_start": request.oos_start,
        "oos_end": request.oos_end,
        "normalize_method": request.normalize_method,
        "walk_forward": request.walk_forward,
        "backtest_config": request.backtest_config,
        "baseline_leg_id": request.baseline_leg_id,
        "node_parallelism": request.backtest_config["node_parallelism"],
        "execution_identity": resolution.identity.payload,
        "execution_identity_hash": resolution.identity.identity_hash,
        "execution_identity_evidence": resolution.evidence,
    }
    run_payload = durable_run_request_payload(
        roster_hash=run_kwargs["roster_hash"],
        roster=run_kwargs["roster"],
        oos_start=run_kwargs["oos_start"],
        oos_end=run_kwargs["oos_end"],
        normalize_method=run_kwargs["normalize_method"],
        walk_forward=run_kwargs["walk_forward"],
        backtest_config=run_kwargs["backtest_config"],
        baseline_leg_id=run_kwargs["baseline_leg_id"],
        node_parallelism=run_kwargs["node_parallelism"],
        execution_identity=run_kwargs["execution_identity"],
        execution_identity_hash=run_kwargs["execution_identity_hash"],
        execution_identity_evidence=run_kwargs["execution_identity_evidence"],
    )
    run = DurableRunSpec(
        request_hash=request_hash_for(run_payload),
        **run_kwargs,
    )
    specs = DeterministicChildPlanner.build_child_specs(run_spec=run, request=request)

    assert specs
    assert specs[0].input_manifest["execution_identity_hash"] == resolution.identity.identity_hash
    assert specs[0].input_manifest["execution_identity_evidence"]["complete"] is True


def test_runtime_identity_does_not_dereference_node_bound_qe_data_links(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for run_id in ("qe_a_L1", "qe_b_L1"):
        (tmp_path / f"{run_id}.pkl").write_bytes(f"prediction:{run_id}".encode("utf-8"))
    request = _request(tmp_path)
    external_data = Path(str(request.backtest_config["runtime_template_dir"])) / "bak_basic.h5"
    external_data.write_bytes(b"stand-in for an unreadable DrvFS data link")
    monkeypatch.setattr(
        "backend.services.multi_alpha.durable_identity.is_runtime_external_data_link",
        lambda path: path == external_data,
    )
    resolver = DurableExecutionIdentityResolver(
        model_store=_ModelStore(tmp_path),  # type: ignore[arg-type]
        environment_loader=lambda _node_id: _environment(),
        dataset_loader=lambda _node_id, _root: _dataset(complete=True),
        node_info_resolver=lambda _node_id: SimpleNamespace(qlib_data_path="/home/lc999/data/factor_data"),
        source_root=REPO_ROOT,
    )

    resolution = resolver.resolve(request=request, node_id="wsl2-5080")

    assert resolution.complete is True
    assert resolution.identity is not None
    assert len(resolution.identity.payload["runtime"]["qlib_runtime_template_sha256"]) == 64


def test_runtime_template_identity_excludes_python_cache_but_tracks_source(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    package = runtime / "aistock_models"
    package.mkdir(parents=True)
    source = package / "model.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    before = _sha256_tree(runtime)

    cache = package / "__pycache__"
    cache.mkdir()
    (cache / "model.cpython-310.pyc").write_bytes(b"environment-specific-cache")
    (package / "legacy.pyo").write_bytes(b"optimized-cache")

    assert _sha256_tree(runtime) == before

    source.write_text("VALUE = 2\n", encoding="utf-8")
    assert _sha256_tree(runtime) != before


def test_missing_dataset_manifest_is_visible_evidence_not_a_research_rejection(tmp_path: Path) -> None:
    for run_id in ("qe_a_L1", "qe_b_L1"):
        (tmp_path / f"{run_id}.pkl").write_bytes(b"prediction")
    request = _request(tmp_path)
    resolver = DurableExecutionIdentityResolver(
        model_store=_ModelStore(tmp_path),  # type: ignore[arg-type]
        environment_loader=lambda _node_id: _environment(),
        dataset_loader=lambda _node_id, _root: _dataset(complete=False),
        node_info_resolver=lambda _node_id: SimpleNamespace(qlib_data_path="/home/lc999/data/factor_data"),
        source_root=REPO_ROOT,
    )

    resolution = resolver.resolve(request=request, node_id="wsl2-5080")

    assert resolution.complete is False
    assert resolution.identity is None
    assert resolution.evidence["reason_code"] == "multi_alpha_execution_identity_incomplete"
    assert "dataset.qe_dataset_manifest.json" in resolution.evidence["missing"]
    assert resolution.evidence["acquisition_suggestions"]
