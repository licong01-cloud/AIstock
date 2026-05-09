"""Tests for paper_v2 model_params_origin provenance tracking.

Covers the silent-fallback fix in
``backend/services/strategy_package/live_inference.py`` and the new
``model_params_origin`` column on ``paper_v2.run`` (C1 audit commit 88bc89c
follow-up; feedback_no_silent_errors).
"""

from __future__ import annotations

import io
import json
import logging
import tarfile
from datetime import date

import pytest

from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.models import PaperRun
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceFileNotFound,
)
from backend.services.strategy_package.live_inference import (
    QEExperimentRuntimeAssetResolver,
)
from backend.services.trading_core.models import RunStatus


def _params_archive() -> bytes:
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        data = b"node-fetched model params"
        info = tarfile.TarInfo("mlruns/1/artifacts/params.pkl")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    return payload.getvalue()


class _FakeNodeClient:
    """Minimal stand-in for QEWorkspaceClient covering the params download path."""

    def __init__(self, *, params_tar: bytes | None, params_exc: Exception | None = None):
        self._params_tar = params_tar
        self._params_exc = params_exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def download_workspace_file_bytes(self, task_id, loop_id, file_path):
        if file_path == "conf.yaml":
            return b"data_handler_config: {}\n"
        if file_path == "factors/factor_a.py":
            return b"def calculate():\n    return None\n"
        if file_path == "model.py":
            raise QEWorkspaceFileNotFound(
                task_id, loop_id, file_path, "http://node/files/model.py"
            )
        raise AssertionError(f"unexpected workspace file request: {file_path}")

    async def download_mlruns_params(self, task_id, loop_id):
        if self._params_exc is not None:
            raise self._params_exc
        return self._params_tar


def test_node_success_records_origin_node(tmp_path, monkeypatch) -> None:
    """Happy path: download_mlruns_params succeeds -> origin == 'node'."""

    client = _FakeNodeClient(params_tar=_params_archive())
    monkeypatch.setattr(
        QEWorkspaceClient, "for_node", staticmethod(lambda _node_id: client)
    )

    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime_cache")
    source_dir, model_params_origin = resolver._materialize_runtime_source_from_node(
        experiment_id="qe_node_success",
        qe_task_id="qe_task_node",
        qe_loop_id="Loop1",
        execution_node_id="node-1",
        factor_names=["factor_a"],
        custom_params={"disable_alpha158": True},
        data_split={},
    )

    assert model_params_origin == "node"
    assert list(source_dir.glob("**/artifacts/params.pkl"))

    # Verify the InMemory repo writes the origin through to the run record.
    repo = InMemoryPaperTradingV2Repository()
    portfolio_id = "pf_test_node"
    repo.portfolios[portfolio_id] = type("P", (), {"portfolio_id": portfolio_id})()
    run = PaperRun(
        portfolio_id=portfolio_id,
        trade_date=date(2026, 5, 10),
        status=RunStatus.RUNNING,
        data_source=MinuteDataSource.DB_HISTORICAL,
        model_params_origin=model_params_origin,
    )
    repo.runs[run.run_id] = run
    fetched = repo.get_run(run.run_id)
    assert fetched.model_params_origin == "node"


def test_node_failure_propagates_by_default(tmp_path, monkeypatch) -> None:
    """download_mlruns_params raises -> exception propagates (no silent fallback)."""

    cache_root = tmp_path / "runtime_cache"
    package_cache = cache_root / "pkg_cached" / "manifest_hash"
    (package_cache / "model").mkdir(parents=True)
    (package_cache / "model" / "params.pkl").write_bytes(b"cached model params")
    (package_cache / "manifest.json").write_text(
        json.dumps({"diagnostics": {"qe_experiment_id": "qe_propagate_err"}}),
        encoding="utf-8",
    )

    fetch_error = RuntimeError("node mlruns params endpoint returned 500")
    client = _FakeNodeClient(params_tar=None, params_exc=fetch_error)
    monkeypatch.setattr(
        QEWorkspaceClient, "for_node", staticmethod(lambda _node_id: client)
    )

    resolver = QEExperimentRuntimeAssetResolver(cache_root=cache_root)

    # Default: allow_cache_fallback=False. The cached params on disk MUST be
    # ignored; the original RuntimeError must surface (wrapped per the
    # outer try/except in _materialize_runtime_source_from_node).
    with pytest.raises(Exception) as exc_info:
        resolver._materialize_runtime_source_from_node(
            experiment_id="qe_propagate_err",
            qe_task_id="qe_task_node",
            qe_loop_id="Loop1",
            execution_node_id="node-1",
            factor_names=["factor_a"],
            custom_params={"disable_alpha158": True},
            data_split={},
        )

    # Either the RuntimeError is re-raised verbatim or wrapped in
    # DataUnavailableError with the original as __cause__.
    chain = []
    cur = exc_info.value
    while cur is not None:
        chain.append(cur)
        cur = cur.__cause__
    assert any(isinstance(item, RuntimeError) for item in chain), (
        f"expected RuntimeError in cause chain, got: {[type(e).__name__ for e in chain]}"
    )


def test_explicit_cache_fallback_records_origin_cache(tmp_path, monkeypatch, caplog) -> None:
    """allow_cache_fallback=True + node fails + cache exists -> origin=='cache' + warning logged."""

    cache_root = tmp_path / "runtime_cache"
    package_cache = cache_root / "pkg_cached" / "manifest_hash"
    (package_cache / "model").mkdir(parents=True)
    (package_cache / "model" / "params.pkl").write_bytes(b"cached model params")
    (package_cache / "manifest.json").write_text(
        json.dumps({"diagnostics": {"qe_experiment_id": "qe_cache_fallback_ok"}}),
        encoding="utf-8",
    )

    fetch_error = RuntimeError("node mlruns params endpoint returned 404")
    client = _FakeNodeClient(params_tar=None, params_exc=fetch_error)
    monkeypatch.setattr(
        QEWorkspaceClient, "for_node", staticmethod(lambda _node_id: client)
    )

    resolver = QEExperimentRuntimeAssetResolver(cache_root=cache_root)

    with caplog.at_level(
        logging.WARNING, logger="backend.services.strategy_package.live_inference"
    ):
        source_dir, model_params_origin = resolver._materialize_runtime_source_from_node(
            experiment_id="qe_cache_fallback_ok",
            qe_task_id="qe_task_node",
            qe_loop_id="Loop1",
            execution_node_id="node-1",
            factor_names=["factor_a"],
            custom_params={"disable_alpha158": True},
            data_split={},
            allow_cache_fallback=True,
        )

    assert model_params_origin == "cache"
    copied = list(source_dir.glob("**/artifacts/params.pkl"))
    assert len(copied) == 1
    assert copied[0].read_bytes() == b"cached model params"

    # A structured warning is emitted on the cache fallback path.
    assert any(
        "mlruns_params_cache_fallback" in record.getMessage()
        for record in caplog.records
    )

    # The origin propagates through the run record via update_run_model_params_origin.
    repo = InMemoryPaperTradingV2Repository()
    portfolio_id = "pf_test_cache"
    repo.portfolios[portfolio_id] = type("P", (), {"portfolio_id": portfolio_id})()
    run = PaperRun(
        portfolio_id=portfolio_id,
        trade_date=date(2026, 5, 10),
        status=RunStatus.RUNNING,
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    repo.runs[run.run_id] = run
    updated = repo.update_run_model_params_origin(run, model_params_origin)
    assert updated.model_params_origin == "cache"
    assert repo.get_run(run.run_id).model_params_origin == "cache"
