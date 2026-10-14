from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
from types import SimpleNamespace
from threading import RLock

import pytest

from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
from backend.services.selection_center.models import SelectionMode
from backend.services.selection_center.runtime_profile import parse_selection_runtime_profile
from backend.services.simulation_runtime.localsim_selection_inference import LOCALSIM_INFERENCE_SCOPE, LocalSimInferenceScope
from backend.services.simulation_runtime.localsim_selection_signal import (
    LocalSimDailyEvidenceWriter, LocalSimKernelComputation, LocalSimRawRepository, LocalSimSelectionSignalService,
    _serialize_projection,
)
from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository
from backend.services.simulation_runtime.scheduler import SimulationLifecycleScheduler
from backend.services.simulation_runtime.models import SimulationBrokerBackend
from backend.services.simulation_runtime.localsim_selection_probe import _comparison
from backend.services.strategy_package.selection_computation import (
    StrategyPackageSelectionComputationRequestV1, selection_runtime_profile_sha256,
)
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.simulation_signal.strategy_package_selection import StrategyPackageSelectionService
from backend.tests.advisory_historical_range.test_r2b_candidate_producer import _fixture, TRADE_DATE
from scripts.strategy_package_live_inference import _atomic_json


def _computed(tmp_path, *, run_id="localsim-independent-run", cutoff=None, unscored=False):
    producer, _, _, program = _fixture(tmp_path / "explicit-reference-fixture")
    prepared = deepcopy(producer._signal_preparation.result)
    artifact = prepared.raw.artifact
    cutoff = cutoff or TRADE_DATE
    artifact.metadata["artifact_input_context"]["effective_trade_date"] = cutoff.isoformat()
    members = [row["symbol"] for row in artifact.scores_json]
    if unscored:
        members += ["600999.SH"]
    artifact = artifact.model_copy(update={"universe_count": len(members)})
    artifact.metadata["universe_members"] = members
    artifact = artifact.model_copy(update={"artifact_payload_sha256": None})
    scope = LocalSimInferenceScope(run_id, tmp_path / run_id, SelectionSignalDayV1(TRADE_DATE, cutoff, TRADE_DATE), {})
    scope.root.mkdir(parents=True)
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        LocalSimRawRepository().save(artifact)
        profile = parse_selection_runtime_profile(program.runtime_config)
        request = StrategyPackageSelectionComputationRequestV1(
            trade_date=TRADE_DATE, data_source="DB_HISTORICAL", selection_mode=SelectionMode.SINGLE_PACKAGE,
            ordered_package_ids=(program.package_id,), package_runtime_profiles={program.package_id: profile},
            package_runtime_profile_hashes={program.package_id: selection_runtime_profile_sha256(profile)},
            package_top_k={program.package_id: 2},
        )
        result = LocalSimKernelComputation().compute(request=request, prepared_signals={program.package_id: prepared.prepared_signal},
                                                   providers=producer._signal_kernel._providers)
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)
    return scope, result, program, producer


def _commit(scope, computation, program, repository):
    service = LocalSimSelectionSignalService.__new__(LocalSimSelectionSignalService)
    service.native_repository = repository
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        evidence = service._build_evidence(package_id=program.package_id, manifest_sha256=program.manifest_sha256,
                                          trade_date=TRADE_DATE, data_source="DB_HISTORICAL", runtime_config=program.runtime_config,
                                          selected=list(computation.package_results[program.package_id]),
                                          excluded=list(computation.excluded_results[program.package_id]),
                                          runtime_release=None, created_by="isolated-contract-test")
        projection = _serialize_projection({"runtime_config": program.runtime_config,
                                            "package_results": computation.package_results, "excluded_results": computation.excluded_results,
                                            "manifest_sha256_by_package": computation.manifest_sha256_by_package,
                                            "stage_trace_by_package": computation.stage_trace_by_package})
        projection.update(aggregate_results=[v.model_dump(mode="json") for v in computation.aggregate_results],
                          valid_no_candidate=computation.valid_no_candidate, no_candidate_reason=computation.no_candidate_reason)
        _atomic_json(scope.root / "projection.json", projection)
        stored = LocalSimDailyEvidenceWriter(repository).save_daily_selection_evidence(evidence)
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)
    identity = scope.root.parent.name
    _atomic_json(scope.root.parent / "attempt.json", {"identity": identity, "consumer_scope": "LOCALSIM", "run_id": scope.run_id,
                                                     "package_ids": [program.package_id]})
    return service, stored, identity


def test_full_raw_unknown_and_native_topk_are_preserved(tmp_path):
    scope, computed, program, _ = _computed(tmp_path, unscored=True)
    assert len(scope.signal_result.raw_scores[program.package_id]) == 3
    assert len(computed.aggregate_results) == 2
    assert scope.signal_result.membership_status[program.package_id] == "COMPLETE"
    assert scope.signal_result.unscored_members[program.package_id] == ({"symbol": "600999.SH", "availability": "UNKNOWN"},)
    assert scope.artifacts[program.package_id].universe_count == 4


def test_same_input_two_localsim_runs_have_independent_envelopes(tmp_path):
    first, a, program, _ = _computed(tmp_path / "a", run_id="own-a")
    other, b, _, _ = _computed(tmp_path / "b", run_id="own-b")
    assert a.package_results == b.package_results
    assert first.signal_result.semantic_identity == other.signal_result.semantic_identity
    assert first.signal_result.storage_identity != other.signal_result.storage_identity
    first.artifacts[program.package_id].metadata["local-mutated"] = True
    assert "local-mutated" not in other.artifacts[program.package_id].metadata


def test_raw_repository_cannot_discover_another_run(tmp_path):
    scope, _, program, _ = _computed(tmp_path)
    artifact = scope.artifacts[program.package_id]
    other = replace(scope, run_id="other-run", artifacts={})
    token = LOCALSIM_INFERENCE_SCOPE.set(other)
    try:
        with pytest.raises(DataUnavailableError, match="has not computed"):
            LocalSimRawRepository().get(package_id=artifact.package_id, manifest_sha256=artifact.manifest_sha256,
                                       trade_date=artifact.trade_date, data_source=artifact.data_source,
                                       runtime_config_hash=artifact.runtime_config_hash)
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)


def test_native_commit_before_checkpoint_recovers_without_recomputing(tmp_path):
    scope, computed, program, _ = _computed(tmp_path, cutoff=TRADE_DATE-timedelta(days=1), unscored=True)
    repository = InMemorySimulationRuntimeRepository()
    service, evidence, identity = _commit(scope, computed, program, repository)
    recovered = service._recover(scope.root.parent, identity)
    assert recovered.aggregate_results == list(computed.aggregate_results)
    assert recovered.stage_trace_by_package == dict(computed.stage_trace_by_package)
    assert recovered.evidence_by_package[program.package_id] == evidence
    assert recovered.score_artifacts_by_package[program.package_id].scores_json == scope.artifacts[program.package_id].scores_json
    assert len(repository.daily_selection_evidences) == 1
    checkpoint = json.loads((scope.root.parent / "checkpoint.json").read_text(encoding="utf-8"))
    assert checkpoint["recovery"] == "NATIVE_EVIDENCE_READBACK"
    again = service._recover(scope.root.parent, identity)
    assert again.evidence_by_package == recovered.evidence_by_package
    assert len(repository.daily_selection_evidences) == 1


def test_pending_file_alone_does_not_claim_native_commit(tmp_path):
    scope, computed, program, _ = _computed(tmp_path)
    repository = InMemorySimulationRuntimeRepository()
    service, _, identity = _commit(scope, computed, program, repository)
    repository.daily_selection_evidences.clear()
    repository.daily_selection_hash_index.clear()
    assert service._recover(scope.root.parent, identity) is None
    assert not (scope.root.parent / "checkpoint.json").exists()


def test_recovery_rejects_changed_projection_even_with_unchanged_native_evidence(tmp_path):
    scope, computed, program, _ = _computed(tmp_path)
    repository = InMemorySimulationRuntimeRepository()
    service, _, identity = _commit(scope, computed, program, repository)
    path = scope.root / "projection.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["aggregate_results"][0]["score"] = -999
    _atomic_json(path, payload)
    with pytest.raises(ValueError, match="projection content differs"):
        service._recover(scope.root.parent, identity)
    assert not (scope.root.parent / "checkpoint.json").exists()


def test_recovery_cannot_bypass_readback_with_empty_package_population(tmp_path):
    scope, computed, program, _ = _computed(tmp_path)
    service, _, identity = _commit(scope, computed, program, InMemorySimulationRuntimeRepository())
    path = scope.root / "projection.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["package_results"] = {}
    _atomic_json(path, payload)
    with pytest.raises(ValueError, match="package population"):
        service._recover(scope.root.parent, identity)


def test_identical_seed_populations_publish_original_unscored_members(tmp_path):
    scope, _, program, _ = _computed(tmp_path, unscored=True)
    artifact = scope.artifacts[program.package_id]
    members = artifact.metadata["universe_members"]
    scope.source_universes = [{"members": members, "count": 4, "input_hash": "same-input"}] * 3
    artifact.metadata.pop("universe_members")
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        actual = LocalSimRawRepository().save(artifact)
        assert actual.metadata["universe_members"] == members
        assert scope.source_universes == []
        scope.source_universes = [{"members": members, "count": 4, "input_hash": "same-input"}] * 3
        scope.source_universes[-1] = {"members": members, "count": 4, "input_hash": "another-input"}
        with pytest.raises(ValueError, match="populations conflict"):
            LocalSimRawRepository().save(artifact)
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)


def test_failed_day_preserves_prefix_and_retry_has_a_new_run(tmp_path, monkeypatch):
    service = LocalSimSelectionSignalService.__new__(LocalSimSelectionSignalService)
    service.temporary_root = tmp_path
    service.native_repository = InMemorySimulationRuntimeRepository()
    service.package_repository = SimpleNamespace(get=lambda _: SimpleNamespace(manifest_sha256="a" * 64))

    def fail(**_kwargs):
        from backend.services.simulation_runtime.localsim_selection_inference import own_scope
        scope = own_scope()
        scope.stage = "shared_raw_inference"
        scope.completed_prefix.append({"stage": "native_asset_addressing"})
        raise DataUnavailableError("injected native failure", context={"reason_code": "TEST_NATIVE_FAILURE", "failed_stage": "raw_inference"})

    monkeypatch.setattr(StrategyPackageSelectionService, "run_selection", lambda _self, **kwargs: fail(**kwargs))
    kwargs = dict(package_ids=["never-whitelisted"], mode=SelectionMode.SINGLE_PACKAGE,
                  trade_date=TRADE_DATE, data_source="DB_HISTORICAL", runtime_config={})
    for _ in range(2):
        with pytest.raises(DataUnavailableError, match="native failure"):
            service.run_selection(**kwargs)
    failures = [json.loads(p.read_text(encoding="utf-8")) for p in tmp_path.rglob("failure.json")]
    assert len(failures) == 2 and len({v["run_id"] for v in failures}) == 2
    assert all(v["reason_code"] == "TEST_NATIVE_FAILURE" and v["failed_stage"] == "raw_inference"
               and v["completed_prefix"] == [{"stage": "native_asset_addressing"}] for v in failures)
    assert not list(tmp_path.rglob("checkpoint.json"))


def test_foreign_native_evidence_and_corrupt_own_artifact_fail_loudly(tmp_path):
    scope, computed, program, _ = _computed(tmp_path)
    repository = InMemorySimulationRuntimeRepository()
    service, evidence, identity = _commit(scope, computed, program, repository)
    repository.daily_selection_evidences[evidence.evidence_id] = evidence.model_copy(update={"artifact_hash": "f" * 64})
    with pytest.raises(ValueError, match="readback"):
        service._recover(scope.root.parent, identity)
    repository.daily_selection_evidences[evidence.evidence_id] = evidence
    path = scope.root / "evidence" / (hashlib.sha256(program.package_id.encode()).hexdigest() + ".json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["evidence_payload_json"]["localsim_signal_run"]["raw_artifact"]["scores_json"][0]["score"] = -999
    _atomic_json(path, payload)
    with pytest.raises(ValueError, match="artifact_hash"):
        service._recover(scope.root.parent, identity)


def test_native_evidence_transaction_failure_retains_intent_not_checkpoint(tmp_path):
    scope, computed, program, _ = _computed(tmp_path)
    class FailedWriter(InMemorySimulationRuntimeRepository):
        def save_daily_selection_evidence(self, _evidence):
            raise RuntimeError("injected transaction failure")
    with pytest.raises(RuntimeError, match="transaction failure"):
        _commit(scope, computed, program, FailedWriter())
    assert (scope.root / "projection.json").is_file()
    assert len(list((scope.root / "evidence").glob("*.json"))) == 1
    assert not (scope.root.parent / "checkpoint.json").exists()


def test_native_raw_runtime_hash_is_not_polluted_by_consumer_run(tmp_path):
    a, _, program, _ = _computed(tmp_path / "a", run_id="one")
    b, _, _, _ = _computed(tmp_path / "b", run_id="two")
    assert a.artifacts[program.package_id].runtime_config_hash == b.artifacts[program.package_id].runtime_config_hash
    assert a.artifacts[program.package_id].artifact_payload_sha256 == b.artifacts[program.package_id].artifact_payload_sha256


@pytest.mark.parametrize("backend", [SimulationBrokerBackend.LOCAL_SIM, SimulationBrokerBackend.MINIQMT_SIM])
def test_scheduler_cache_is_partitioned_from_other_broker_consumers(backend):
    scheduler = SimulationLifecycleScheduler.__new__(SimulationLifecycleScheduler)
    scheduler._selection_service_injected = False
    binding = SimpleNamespace(package_id="unlisted-package", manifest_sha256="a"*64, release_id="release", release_hash="b"*64,
                              broker_backend=backend)
    key = scheduler._selection_cache_key(binding=binding, trade_date=TRADE_DATE, data_source="DB_HISTORICAL")
    assert (key[-1] == "LOCALSIM") == (backend == SimulationBrokerBackend.LOCAL_SIM)


def test_default_localsim_never_queries_global_artifact_hit():
    scheduler = SimulationLifecycleScheduler.__new__(SimulationLifecycleScheduler)
    scheduler._selection_service_injected = False
    scheduler._selection_artifact_repository = lambda: pytest.fail("foreign artifact repository was queried")
    assert scheduler._authoritative_selection_artifact_exists(binding=SimpleNamespace(broker_backend=SimulationBrokerBackend.LOCAL_SIM),
                                                              runtime_release=None, trade_date=TRADE_DATE,
                                                              data_source="DB_HISTORICAL", runtime_config={}) is False


def test_default_localsim_routes_to_own_writer_and_injected_service_stays_explicit(monkeypatch):
    calls = []
    scheduler = SimulationLifecycleScheduler.__new__(SimulationLifecycleScheduler)
    scheduler._selection_service_injected = False
    scheduler._localsim_selection_service = None
    scheduler._selection_inference_lock = RLock()
    scheduler.repository = InMemorySimulationRuntimeRepository()
    scheduler.selection_service = SimpleNamespace(run_selection=lambda **_: "explicit-existing-service")

    def factory(*, repository):
        assert repository is scheduler.repository
        return SimpleNamespace(run_selection=lambda **kwargs: calls.append(kwargs) or "own-independent-service")

    monkeypatch.setattr("backend.services.simulation_runtime.localsim_selection_signal.LocalSimSelectionSignalService", factory)
    binding = SimpleNamespace(broker_backend=SimulationBrokerBackend.LOCAL_SIM, package_id="unlisted-package")
    kwargs = dict(binding=binding, runtime_release=object(), trade_date=TRADE_DATE, data_source="DB_HISTORICAL",
                  runtime_config={"selection_artifact_config": {"auto_generate": True}}, created_by="contract")
    assert scheduler._run_selection_sync(**kwargs) == "own-independent-service"
    assert calls[0]["package_ids"] == ["unlisted-package"]
    scheduler._selection_service_injected = True
    assert scheduler._run_selection_sync(**kwargs) == "explicit-existing-service"


def test_parity_comparison_verifies_lineage_and_does_not_mask_other_stage_values(tmp_path):
    scope, computed, program, _ = _computed(tmp_path, unscored=True)
    _, evidence, _ = _commit(scope, computed, program, InMemorySimulationRuntimeRepository())
    native = scope.artifacts[program.package_id].model_dump(mode="json")
    payload = {"raw_artifact": native, "selected": [v.model_dump(mode="json") for v in computed.aggregate_results],
               "excluded": {p: [v.model_dump(mode="json") for v in rows] for p, rows in computed.excluded_results.items()},
               "evidence": evidence.model_dump(mode="json"), "stage_semantic_hashes": {program.package_id: {"alpha_raw": "original-native-hash"}},
               "status": {"valid_no_candidate": computed.valid_no_candidate, "no_candidate_reason": computed.no_candidate_reason}}
    # The IO fixture's original native alpha receipt may omit lineage fields;
    # declare the exact input relationship for this comparison-only case.
    alpha = payload["evidence"]["evidence_payload_json"]["localsim_signal_run"]["projection"]["stage_trace_by_package"][program.package_id]["alpha_raw"]
    alpha["semantic_payload"]["artifact_id"] = native["artifact_id"]
    reference = deepcopy(payload)
    reference["raw_artifact"]["artifact_id"] = "independent-reference-artifact"
    other_alpha = reference["evidence"]["evidence_payload_json"]["localsim_signal_run"]["projection"]["stage_trace_by_package"][program.package_id]["alpha_raw"]
    other_alpha["semantic_payload"]["artifact_id"] = "independent-reference-artifact"
    result = _comparison(payload, reference)
    assert all(result["checks"].values()) and result["native_alpha_storage_ids_differ"] is True
    other_alpha["status"] = "FAILED"
    assert _comparison(payload, reference)["checks"]["native_stage_values"] is False
    other_alpha["semantic_payload"]["artifact_id"] = "foreign-artifact"
    assert _comparison(payload, reference)["checks"]["native_alpha_lineage_verified"] is False
