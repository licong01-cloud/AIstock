"""Consumer/address contracts; numerical algorithms stay in native modules."""

from dataclasses import replace
from datetime import date, timedelta
import hashlib
import json
import shlex
import subprocess
from types import SimpleNamespace

import pytest

from backend.infra.wsl_qlib_runner import win_to_wsl_path
from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_RULE_VERSION, CANONICAL_PIT_UNIVERSE_KEY,
    canonical_rule_parameters_digest,
)
from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.simulation_runtime.localsim_selection_inference import (
    LOCALSIM_INFERENCE_SCOPE, LocalSimInferenceScope, LocalSimReferencedAssetResolver,
    LocalSimSharedInferenceProvider,
)
from backend.services.simulation_runtime.localsim_selection_signal import LocalSimReadOnlyHmmRuntime
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import FittedPreprocessorAsset, StrategyPackageCanonicalPitBindingV2
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.trading_core.errors import DataUnavailableError
from backend.tests.strategy_package.test_runtime_package_assets_batch2 import _ForbiddenConn, _frozen_manifest
from scripts.strategy_package_live_inference import _atomic_json
from backend.services.simulation_runtime.localsim_selection_probe import _disk_bytes


DAY = date(2026, 1, 5)


def test_disk_accounting_does_not_count_hardlink_aliases_as_copies(tmp_path):
    import os
    payload = b"one-physical-input"
    source = tmp_path / "original.dat"
    source.write_bytes(payload)
    os.link(source, tmp_path / "alias.dat")
    assert _disk_bytes(tmp_path) == len(payload)


@pytest.mark.parametrize("package_id,sequence_length,factor_count", [("brand-new-lstm", 7, 2), ("another-new-tcn", 31, 1)])
def test_native_metadata_and_frozen_assets_are_consumed_in_place(tmp_path, package_id, sequence_length, factor_count):
    store = LocalPackageAssetStore(tmp_path / "formal-test-store")
    manifest = _frozen_manifest(store, model_code_files={"model.py": f"SEQUENCE_LENGTH = {sequence_length}\n".encode()})
    processor = store.put(b"fitted-training-processor", kind="preprocessor")
    model = manifest.model_asset.model_copy(update={"preprocessor_asset": FittedPreprocessorAsset(
        asset_ref=processor.uri, sha256=processor.sha256, size_bytes=processor.size_bytes)})
    evidence = {**manifest.source_evidence, "custom_params": {"sequence_length": sequence_length}}
    manifest = freeze_manifest(manifest.model_copy(update={"package_id": package_id, "factor_set": manifest.factor_set[:factor_count],
                                                         "model_asset": model, "source_evidence": evidence, "manifest_sha256": None}))
    scope = LocalSimInferenceScope("generic-native-run", tmp_path / "own-run", SelectionSignalDayV1(DAY, DAY, DAY), {})
    resolver = LocalSimReferencedAssetResolver(conn_factory=lambda: _ForbiddenConn(), asset_store=store, cache_root=scope.root / "assets")
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        source = resolver.load_source_for_strategy_package(manifest=manifest, package_id=package_id)
        workspace = resolver.prepare_workspace(package_id=package_id, manifest_sha256=manifest.manifest_sha256,
                                               source=source, path_converter=win_to_wsl_path)
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)
    assert source.custom_params["sequence_length"] == sequence_length
    assert len(workspace.dynamic_factors) == factor_count
    assert (workspace.workspace_path / "model" / "model.py").read_text(encoding="utf-8") == f"SEQUENCE_LENGTH = {sequence_length}\n"
    node_manifest = json.loads(workspace.manifest_path.read_text(encoding="utf-8"))
    assert node_manifest["primary_assets"]["model_weight_relpath"] == win_to_wsl_path(str(workspace.model_params_path))
    assert workspace.model_params_path.is_relative_to(store.root)
    assert workspace.dataset_processor_path.is_relative_to(store.root)
    assert not list(scope.root.rglob("params.pkl")) and not list(scope.root.rglob("dataset"))
    assert "pit_identity" not in node_manifest


def test_day_scopes_materialize_distinct_small_workspaces(tmp_path):
    store = LocalPackageAssetStore(tmp_path / "formal-test-store")
    manifest = _frozen_manifest(store)
    resolver = LocalSimReferencedAssetResolver(asset_store=store, cache_root=tmp_path / "assets")
    roots = []
    for day in (DAY, DAY + timedelta(days=1)):
        scope = LocalSimInferenceScope(day.isoformat(), tmp_path / day.isoformat(), SelectionSignalDayV1(day, day, day), {})
        token = LOCALSIM_INFERENCE_SCOPE.set(scope)
        try:
            source = resolver.load_source_for_strategy_package(manifest=manifest, package_id=manifest.package_id)
            roots.append(source.asset_workspace_path)
        finally:
            LOCALSIM_INFERENCE_SCOPE.reset(token)
    assert roots[0] != roots[1] and roots[0].parent.parent != roots[1].parent.parent


@pytest.mark.parametrize("with_runtime_lease", [False, True])
def test_training_binding_is_preserved_and_only_native_runtime_lease_binds_inference(tmp_path, with_runtime_lease):
    store = LocalPackageAssetStore(tmp_path / "formal-test-store")
    manifest = _frozen_manifest(store)
    binding = StrategyPackageCanonicalPitBindingV2(
        authority_id=CANONICAL_PIT_AUTHORITY_ID, rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(), release_id="new-release-not-an-inference-plan",
        release_cutoff=DAY, frozen_snapshot_digest="a" * 64, release_manifest_digest="b" * 64,
        qualification_method="REVALIDATED", qualification_evidence_digest="c" * 64)
    # Address-only test: upstream package validation owns v2 qualification.
    manifest = manifest.model_copy(update={"canonical_pit_binding": binding, "manifest_version": "alpha_core_v2"})
    lease = {"schema_version": "selection_canonical_pit_runtime_lease_v1", "authority_id": CANONICAL_PIT_AUTHORITY_ID,
             "authority_status": "ACTIVE_CANONICAL", "universe_key": CANONICAL_PIT_UNIVERSE_KEY,
             "rule_version": CANONICAL_PIT_RULE_VERSION, "rule_parameters_digest": canonical_rule_parameters_digest(),
             "activation_generation": 7, "activation_envelope_digest": "d" * 64, "expected_source_commit": "source-release",
             "state_source_digest": "e" * 64, "coverage_start": "2020-01-01", "coverage_end": "2026-10-14"}
    scope = LocalSimInferenceScope("native-pit-run", tmp_path / "own-run", SelectionSignalDayV1(DAY, DAY, DAY), {})
    resolver = LocalSimReferencedAssetResolver(asset_store=store, cache_root=scope.root / "assets")
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        source = resolver.load_source_for_strategy_package(manifest=manifest, package_id=manifest.package_id)
        workspace = resolver.prepare_workspace(package_id=manifest.package_id, manifest_sha256=manifest.manifest_sha256,
                                               source=source, path_converter=win_to_wsl_path,
                                               runtime_config={"canonical_pit_authority": lease} if with_runtime_lease else {})
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)
    actual = json.loads(workspace.manifest_path.read_text(encoding="utf-8"))
    assert actual["native_package_identity"]["canonical_pit_binding"] == binding.model_dump(mode="json")
    if with_runtime_lease:
        assert actual["pit_identity"]["mode"] == "canonical_v2_rolling_runtime"
        assert actual["pit_identity"]["activation_generation"] == 7
        assert actual["pit_identity"]["universe_key"] == CANONICAL_PIT_UNIVERSE_KEY
    else:
        assert "pit_identity" not in actual  # No invented frozen inference population.


def _mock_owned_node(monkeypatch, scope, *, foreign=False, failure=False):
    commands = []

    def run(arguments, **_kwargs):
        if arguments[-2] == "cat":
            return SimpleNamespace(returncode=0, stdout=scope.run_id, stderr="")
        commands.append(arguments[-1])
        root = next((scope.root / "node").iterdir())
        output = root / "batch_dates" / DAY.isoformat() / "scores.json"
        _atomic_json(output, {"ok": True, "scores": [{"symbol": "000001.SZ", "score": 1.0}], "universe_count": 2,
                              "input_context": {"universe_input_hash": "c" * 64}, "source_read_receipts": [],
                              "metadata": {"consumer_scope": "ADVISORY" if foreign else "LOCALSIM", "run_id": scope.run_id,
                                           "clocks": scope.clocks.to_dict(), "binding": "b" * 64,
                                           "universe_members": ["000001.SZ", "600000.SH"]}})
        item = {"output_path": win_to_wsl_path(str(output)), "sha256": hashlib.sha256(output.read_bytes()).hexdigest()}
        _atomic_json(root / "batch.json", {"consumer_scope": "LOCALSIM", "run_id": scope.run_id, "binding": "b" * 64,
                                          "status": "FAILED" if failure else "COMPLETE", "ok": not failure, "results": [item],
                                          "reason_code": "TEST_NODE_FAILURE", "failed_stage": "raw_inference"})
        return SimpleNamespace(returncode=1 if failure else 0, stdout="bounded native output", stderr="")

    monkeypatch.setattr("backend.services.simulation_runtime.localsim_selection_inference.subprocess.run", run)
    return commands


def _provider(tmp_path, monkeypatch):
    scope = LocalSimInferenceScope("own-run", tmp_path / "own-run", SelectionSignalDayV1(DAY, DAY-timedelta(days=1), DAY), {})
    scope.root.mkdir()
    workspace = SimpleNamespace(workspace_path=tmp_path / "workspace", manifest_path=tmp_path / "workspace" / "manifest.json")
    _atomic_json(workspace.manifest_path, {})
    provider = LocalSimSharedInferenceProvider(repo_root=tmp_path)
    monkeypatch.setattr(provider, "_build_env_exports", lambda **_: "")
    return scope, workspace, provider


def test_shared_cli_receives_owned_scope_day_and_readonly_boundary(tmp_path, monkeypatch):
    scope, workspace, provider = _provider(tmp_path, monkeypatch)
    commands = _mock_owned_node(monkeypatch, scope)
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        result = provider.run(workspace=workspace, trade_date=DAY, cutoff_date=scope.clocks.data_cutoff)
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)
    assert result.universe_count == 2 and len(result.scores) == 1
    tokens = shlex.split(commands[0])
    assert tokens[tokens.index("--consumer-scope") + 1] == "LOCALSIM"
    assert tokens[tokens.index("--run-id") + 1] == "own-run"
    assert "--historical-read-only" in commands[0] and "default_transaction_read_only=on" in commands[0]
    assert scope.source_universes[0]["members"] == ["000001.SZ", "600000.SH"]
    assert scope.completed_prefix[0]["stage"] == "shared_raw_inference"


@pytest.mark.parametrize("foreign,failure", [(True, False), (False, True)])
def test_foreign_or_failed_node_is_never_accepted(tmp_path, monkeypatch, foreign, failure):
    scope, workspace, provider = _provider(tmp_path, monkeypatch)
    _mock_owned_node(monkeypatch, scope, foreign=foreign, failure=failure)
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        with pytest.raises((ValueError, DataUnavailableError)) as error:
            provider.run(workspace=workspace, trade_date=DAY, cutoff_date=scope.clocks.data_cutoff)
        if failure:
            assert error.value.context["failed_stage"] == "raw_inference"
            assert len(error.value.context["completed_prefix"]) == 1
        assert scope.source_universes == []
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)


def test_timeout_cannot_expose_credential_bearing_command(tmp_path, monkeypatch):
    scope, workspace, provider = _provider(tmp_path, monkeypatch)

    def timeout(arguments, **_kwargs):
        if arguments[-2] == "cat":
            return SimpleNamespace(returncode=0, stdout=scope.run_id, stderr="")
        raise subprocess.TimeoutExpired("TDX_DB_PASSWORD=secret-test-value", 2)

    monkeypatch.setattr("backend.services.simulation_runtime.localsim_selection_inference.subprocess.run", timeout)
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        with pytest.raises(DataUnavailableError) as error:
            provider.run(workspace=workspace, trade_date=DAY, cutoff_date=scope.clocks.data_cutoff)
        assert error.value.context["reason_code"] == "LOCALSIM_NODE_TIMEOUT"
        assert "secret-test-value" not in str(error.value)
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)


def test_hmm_preserves_native_decision_label_and_cannot_generate(monkeypatch, tmp_path):
    scope = LocalSimInferenceScope("hmm-clock-run", tmp_path, SelectionSignalDayV1(DAY, DAY-timedelta(days=1), DAY), {})
    monkeypatch.setattr(SectorHMMRuntime, "adjust_candidates_with_receipt", lambda _self, **kwargs: kwargs)
    monkeypatch.setattr(SectorHMMRuntime, "_load_coefficients", lambda _self, **kwargs: kwargs)
    runtime = LocalSimReadOnlyHmmRuntime(snapshot_provider=object())
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        assert runtime.adjust_candidates_with_receipt(trade_date=DAY)["trade_date"] == DAY
        assert runtime._load_coefficients(allow_generate=True)["allow_generate"] is False
        with pytest.raises(ValueError, match="signal clocks"):
            runtime.adjust_candidates_with_receipt(trade_date=scope.clocks.data_cutoff)
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)


def test_scope_clock_conflict_fails_before_node_launch(tmp_path, monkeypatch):
    scope, workspace, provider = _provider(tmp_path, monkeypatch)
    scope = replace(scope, clocks_bound=True)
    monkeypatch.setattr("backend.services.simulation_runtime.localsim_selection_inference.subprocess.run",
                        lambda *_args, **_kwargs: pytest.fail("mismatched day launched inference"))
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        with pytest.raises(ValueError, match="different C/D/T"):
            provider.run(workspace=workspace, trade_date=DAY, cutoff_date=DAY)
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)
