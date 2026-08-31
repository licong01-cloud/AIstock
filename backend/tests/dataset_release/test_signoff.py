from __future__ import annotations

from dataclasses import replace

import pytest

from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.contracts import Component, ComponentAction, RunOutcome
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.signoff import (
    ComponentSignoff,
    SafetyCounters,
    SignoffError,
    SignoffRequest,
    ValidationResult,
    ValidationStatus,
    build_signoff,
)


def _digest(char: str) -> str:
    return char * 64


def _components(action: ComponentAction) -> tuple[ComponentSignoff, ...]:
    return tuple(
        ComponentSignoff(
            component,
            action,
            "all",
            ValidationStatus.PASS,
            _digest(str(index + 1)),
            _digest(str(index + 5)),
            source_rows=10,
            artifact_rows=10,
        )
        for index, component in enumerate(Component)
    )


def _request(outcome: RunOutcome, components=None, safety=None) -> SignoffRequest:
    return SignoffRequest(
        outcome=outcome,
        profile="qe_hmm_full_v1",
        scope="full",
        cutoff="2026-07-31",
        resolved_intent_key=_digest("a"),
        source_content_root=_digest("b"),
        source_provenance_root=_digest("c"),
        pit_snapshot_digest=_digest("d"),
        semantic_profile_digest=_digest("e"),
        action_plan_digest=_digest("f"),
        components=components or _components(ComponentAction.NOOP),
        validations=(ValidationResult("all_required", ValidationStatus.PASS),),
        safety=safety or SafetyCounters(),
        candidate_identity=_digest("1"),
        release_digest=_digest("2"),
        attestation_key=_digest("3"),
        source_probe_key=_digest("4"),
    )


def test_noop_signoff_is_typed_zero_write_and_cas_persisted(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    receipt = build_signoff(_request(RunOutcome.NO_OP_VERIFIED))
    reference = receipt.write_to_cas(cas)
    payload = cas.get_json(reference)
    assert payload["outcome"] == "NO_OP_VERIFIED"
    assert payload["activation"] == "not_requested"
    assert payload["hmm"]["consumer"] == "not_activated"
    assert all(value == 0 for value in payload["safety"].values())
    assert payload["signoff_digest"] == receipt.digest


@pytest.mark.parametrize(
    "safety",
    [
        SafetyCounters(database_writes=1),
        SafetyCounters(production_writes=1),
        SafetyCounters(production_deletes=1),
        SafetyCounters(production_pointer_changes=1),
        SafetyCounters(service_process_controls=1),
    ],
)
def test_signoff_rejects_any_db_production_or_process_mutation(safety) -> None:
    with pytest.raises(SignoffError, match="forbidden"):
        build_signoff(_request(RunOutcome.NO_OP_VERIFIED, safety=safety))


def test_reattest_signoff_rejects_candidate_write() -> None:
    with pytest.raises(SignoffError, match="cannot write candidate"):
        build_signoff(
            _request(
                RunOutcome.REATTESTED,
                components=_components(ComponentAction.REATTEST),
                safety=SafetyCounters(candidate_writes=1),
            )
        )


def test_reattest_signoff_requires_fresh_probe_identity() -> None:
    request = replace(
        _request(
            RunOutcome.REATTESTED,
            components=_components(ComponentAction.REATTEST),
        ),
        source_probe_key=None,
    )
    with pytest.raises(SignoffError, match="fresh probe"):
        build_signoff(request)


def test_candidate_validated_accepts_mixed_materialization_actions() -> None:
    components = list(_components(ComponentAction.REUSE))
    components[0] = replace(components[0], action=ComponentAction.INCREMENTAL)
    components[1] = replace(components[1], action=ComponentAction.SELECTIVE_REBUILD)
    receipt = build_signoff(
        _request(
            RunOutcome.CANDIDATE_VALIDATED,
            components=tuple(components),
            safety=SafetyCounters(candidate_writes=42),
        )
    )
    assert receipt.payload["outcome"] == "CANDIDATE_VALIDATED"
    assert receipt.payload["safety"]["candidate_writes"] == 42
    assert receipt.payload["safety"]["production_writes"] == 0


def test_success_cannot_hide_required_validation_failure() -> None:
    request = replace(
        _request(RunOutcome.NO_OP_VERIFIED),
        validations=(ValidationResult("value_parity", ValidationStatus.FAIL),),
    )
    with pytest.raises(SignoffError, match="failed required evidence"):
        build_signoff(request)


def test_blocked_signoff_requires_and_preserves_typed_failure() -> None:
    request = replace(
        _request(RunOutcome.BLOCKED),
        validations=(ValidationResult("source", ValidationStatus.BLOCKED),),
        failure_code="BLOCKED_SOURCE_SNAPSHOT_DRIFT",
    )
    receipt = build_signoff(request)
    assert receipt.payload["outcome"] == "BLOCKED"
    assert receipt.payload["failure_code"] == "BLOCKED_SOURCE_SNAPSHOT_DRIFT"


def test_non_success_requires_typed_failure_code() -> None:
    request = replace(
        _request(RunOutcome.FAILED),
        validations=(ValidationResult("source", ValidationStatus.FAIL),),
        failure_code=None,
    )
    with pytest.raises(SignoffError, match="typed failure code"):
        build_signoff(request)


def test_success_requires_explicit_required_validation() -> None:
    request = replace(
        _request(RunOutcome.NO_OP_VERIFIED),
        validations=(ValidationResult("optional_diagnostic", ValidationStatus.PASS, required=False),),
    )
    with pytest.raises(SignoffError, match="explicit required validation"):
        build_signoff(request)


def test_signoff_rejects_duplicate_component_partition() -> None:
    components = _components(ComponentAction.NOOP)
    request = replace(
        _request(RunOutcome.NO_OP_VERIFIED),
        components=components + (components[0],),
    )
    with pytest.raises(SignoffError, match="duplicate component partitions"):
        build_signoff(request)
