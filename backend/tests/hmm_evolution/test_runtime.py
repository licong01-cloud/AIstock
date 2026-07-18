from __future__ import annotations

from backend.services.hmm_evolution.runtime import (
    ManagedQEWorkspaceReadClient,
    build_runtime,
)


def test_build_runtime_wires_shared_qe_read_client_without_constructor_drift(
    monkeypatch,
) -> None:
    monkeypatch.delenv("HMM_EVOLUTION_ARTIFACT_ROOTS_JSON", raising=False)

    runtime = build_runtime()

    assert isinstance(runtime.qe_read_client, ManagedQEWorkspaceReadClient)
    assert runtime.qe_asset_reader._client is runtime.qe_read_client
    assert runtime.artifact_resolver._qe_asset_reader is runtime.qe_asset_reader
