"""Deterministic Phase 0A.1 handoff normalization for Phase 1 consumers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .models import (
    AuditReceipt,
    AuditRequest,
    AuditTarget,
    AvailabilityStatus,
    CandidateAuthorityReport,
    CandidateAuthorityStatus,
    FormalOOSStatus,
    HandoffAdmissionScope,
    HandoffEvidenceScope,
    HandoffReadiness,
    HandoffReadinessReport,
    HandoffTarget,
    OOSInterval,
    Phase0APolicyRegistry,
    Phase1HandoffBundle,
    RuntimeSemanticsEvidence,
    TargetAuditResult,
)
from .policy import canonical_json_sha256, normalized_reason_codes, stable_identifier


AUDIT_RECEIPT_SCHEMA_VERSION = "advisory_phase0a_receipt_v1"
HANDOFF_READINESS_SCHEMA_VERSION = "advisory_phase0a_handoff_readiness_v1"
PHASE1_HANDOFF_BUNDLE_SCHEMA_VERSION = "advisory_phase0a_handoff_bundle_v2"

REASON_AUDIT_ID_MISMATCH = "ADVISORY_PHASE0A_HANDOFF_AUDIT_ID_MISMATCH"
REASON_AUDIT_POLICY_MISMATCH = "ADVISORY_PHASE0A_HANDOFF_AUDIT_POLICY_MISMATCH"
REASON_REQUEST_HASH_MISMATCH = "ADVISORY_PHASE0A_HANDOFF_REQUEST_HASH_MISMATCH"
REASON_RESULT_HASH_MISMATCH = "ADVISORY_PHASE0A_HANDOFF_RESULT_HASH_MISMATCH"
REASON_MANIFEST_HASH_MISMATCH = "ADVISORY_PHASE0A_HANDOFF_MANIFEST_HASH_MISMATCH"
REASON_TARGET_SCOPE_MISSING = "ADVISORY_PHASE0A_HANDOFF_TARGET_SCOPE_MISSING"
REASON_TARGET_SCOPE_MISMATCH = "ADVISORY_PHASE0A_HANDOFF_TARGET_SCOPE_MISMATCH"
REASON_ASSET_LEDGER_INCOMPLETE = "ADVISORY_PHASE0A_HANDOFF_ASSET_LEDGER_INCOMPLETE"
REASON_RUNTIME_SEMANTICS_MISSING = "ADVISORY_PHASE0A_HANDOFF_RUNTIME_SEMANTICS_MISSING"
REASON_RUNTIME_SEMANTICS_CONFLICT = "ADVISORY_PHASE0A_HANDOFF_RUNTIME_SEMANTICS_CONFLICT"
REASON_EFFECTIVE_CONFIG_MISSING = "ADVISORY_PHASE0A_HANDOFF_EFFECTIVE_CONFIG_MISSING"
REASON_EFFECTIVE_CONFIG_CONFLICT = "ADVISORY_PHASE0A_HANDOFF_EFFECTIVE_CONFIG_CONFLICT"
REASON_CALENDAR_HASH_MISSING = "ADVISORY_PHASE0A_HANDOFF_CALENDAR_HASH_MISSING"
REASON_CALENDAR_HASH_MISMATCH = "ADVISORY_PHASE0A_HANDOFF_CALENDAR_HASH_MISMATCH"
REASON_DECISION_CLOCK_MISSING = "ADVISORY_PHASE0A_HANDOFF_DECISION_CLOCK_MISSING"
REASON_SIGNAL_CONTEXT_MISSING = "ADVISORY_PHASE0A_HANDOFF_SIGNAL_CONTEXT_MISSING"
REASON_SIGNAL_EVIDENCE_MISSING = "ADVISORY_PHASE0A_HANDOFF_SIGNAL_EVIDENCE_MISSING"
REASON_SCOPE_EVIDENCE_UNAVAILABLE = "ADVISORY_PHASE0A_HANDOFF_SCOPE_EVIDENCE_UNAVAILABLE"
REASON_NO_ADMISSION_SCOPES = "ADVISORY_PHASE0A_HANDOFF_NO_ADMISSION_SCOPES"


class Phase0AHandoffNormalizer:
    """Pure Phase 0A.1 normalizer; it never mutates audit evidence or accepts overrides."""

    def __init__(self, *, policy: Phase0APolicyRegistry) -> None:
        self._policy = policy

    def normalize(
        self,
        *,
        receipt: AuditReceipt,
        request: AuditRequest,
        created_at: datetime | None = None,
    ) -> tuple[HandoffReadinessReport, Phase1HandoffBundle | None]:
        report = build_handoff_readiness_report(receipt=receipt, request=request, policy=self._policy)
        bundle = build_phase1_handoff_bundle(
            report=report,
            policy=self._policy,
            created_at=created_at,
        )
        return report, bundle


def audit_request_identity_payload(request: AuditRequest) -> dict[str, Any]:
    return {
        "schema_version": "advisory_phase0a_request_identity_v1",
        "audit_id": request.audit_id,
        "audit_policy_version": request.audit_policy_version,
        "targets": [
            target.model_dump(mode="python")
            for target in sorted(request.targets, key=lambda item: item.audit_target_id)
        ],
    }


def target_scope_entry(target: AuditTarget) -> dict[str, Any]:
    """Freeze the complete target contract; no current runtime value is inferred here."""

    return {
        "audit_target_id": target.audit_target_id,
        "program_id": target.program_id,
        "package_id": target.package_id,
        "manifest_sha256": target.manifest_sha256,
        "binding_resolution_mode": target.binding_resolution_mode.value,
        "expected_alpha_mode": target.expected_alpha_mode.value,
        "decision_date_range": target.decision_date_range.model_dump(mode="python"),
        "decision_dates": target.decision_dates,
        "selection_evidence_ids_by_decision_date": target.selection_evidence_ids_by_decision_date,
        "style_family": target.style_family,
        "requested_capabilities": target.requested_capabilities,
        "audit_policy_version": target.audit_policy_version,
    }


def target_scope_registry_payload(*, receipt: AuditReceipt, request: AuditRequest) -> dict[str, Any]:
    return {
        "schema_version": "advisory_phase0a_target_scope_registry_v1",
        "audit_id": receipt.audit_id,
        "request_hash": receipt.request_hash,
        "targets": [
            target_scope_entry(target)
            for target in sorted(request.targets, key=lambda item: item.audit_target_id)
        ],
    }


def phase1_handoff_hashes(*, results: list[TargetAuditResult], policy: Phase0APolicyRegistry) -> dict[str, str]:
    """Freeze every Phase 1 source identity without asserting any human approval."""

    return {
        "source_availability_matrix_hash": canonical_json_sha256(
            [entry.model_dump(mode="python") for result in results for entry in result.source_availability]
        ),
        "universe_survivorship_hash": canonical_json_sha256(
            [entry.model_dump(mode="python") for result in results for entry in result.universe_survivorship]
        ),
        "asset_runtime_hmm_ledger_hash": canonical_json_sha256(
            {
                "assets": [entry.model_dump(mode="python") for result in results for entry in result.asset_ledger],
                "runtime": [entry.model_dump(mode="python") for result in results for entry in result.runtime_semantics],
                "hmm": [entry.model_dump(mode="python") for result in results for entry in result.hmm_vintages],
            }
        ),
        "oos_interval_report_hash": canonical_json_sha256(
            [entry.model_dump(mode="python") for result in results for entry in result.oos_intervals]
        ),
        "candidate_authority_stage_capability_hash": canonical_json_sha256(
            [entry.model_dump(mode="python") for result in results for entry in result.candidate_authority]
        ),
        "metric_label_policy_hash": canonical_json_sha256(
            {
                "benchmark": policy.benchmark_policy,
                "cost": policy.cost_policy,
                "label": policy.label_policy,
                "embargo": {
                    "policy_id": policy.embargo_policy_id,
                    "policy_version": policy.embargo_policy_version,
                    "policy_hash": policy.embargo_policy_hash,
                },
            }
        ),
        "prior_registry_hash": canonical_json_sha256(policy.prior_policy),
        "multiple_testing_registry_hash": canonical_json_sha256(policy.multiple_testing_policy),
        "policy_registry_hash": canonical_json_sha256(policy),
    }


def audit_manifest_base(
    *,
    audit_id: str,
    audit_policy_version: str,
    request_hash: str,
    result_hash: str,
    results: list[TargetAuditResult],
    policy: Phase0APolicyRegistry,
) -> dict[str, Any]:
    return {
        "schema_version": AUDIT_RECEIPT_SCHEMA_VERSION,
        "audit_id": audit_id,
        "request_hash": request_hash,
        "audit_policy_version": audit_policy_version,
        "policy_hash": canonical_json_sha256(policy),
        "serializer_version": policy.serializer_version,
        "result_hash": result_hash,
        "phase1_handoff_hashes": phase1_handoff_hashes(results=results, policy=policy),
        "read_only": True,
        "raw_intermediate_location": f"tmp/advisory_phase0a/{audit_id}",
        "raw_intermediate_cleanup_status": "NOT_CREATED",
    }


def build_handoff_readiness_report(
    *,
    receipt: AuditReceipt,
    request: AuditRequest,
    policy: Phase0APolicyRegistry,
) -> HandoffReadinessReport:
    """Classify every target scope deterministically; no input can be manually overridden."""

    global_reasons = _receipt_integrity_reason_codes(receipt=receipt, request=request, policy=policy)
    report_reasons = list(global_reasons)
    target_by_id = {target.audit_target_id: target for target in request.targets}
    targets: list[HandoffTarget] = []
    for result in sorted(receipt.results, key=lambda item: item.audit_target_id):
        target = target_by_id.get(result.audit_target_id)
        if target is None:
            target_reasons = [*global_reasons, REASON_TARGET_SCOPE_MISSING]
            targets.append(_blocked_target(result=result, reasons=target_reasons))
            report_reasons.extend(target_reasons)
            continue
        targets.append(_target_handoff(result=result, target=target, policy=policy, global_reasons=global_reasons))

    missing_results = sorted(set(target_by_id) - {result.audit_target_id for result in receipt.results})
    for target_id in missing_results:
        target = target_by_id[target_id]
        target_reasons = [*global_reasons, REASON_TARGET_SCOPE_MISSING]
        targets.append(_missing_result_target(target=target, reasons=target_reasons))
        report_reasons.extend(target_reasons)

    targets = sorted(targets, key=lambda item: item.audit_target_id)
    scopes = [scope for target in targets for scope in target.admission_scopes]
    readiness = _global_readiness(scopes)
    if report_reasons and readiness == HandoffReadiness.READY:
        readiness = HandoffReadiness.PARTIAL
    blocking_reasons = normalized_reason_codes(
        [
            *report_reasons,
            *([REASON_NO_ADMISSION_SCOPES] if not scopes else []),
            *[reason for scope in scopes if scope.readiness == HandoffReadiness.BLOCKED for reason in scope.blocking_reason_codes],
        ]
    )
    hashes = {
        "target_scope_registry_hash": canonical_json_sha256(
            target_scope_registry_payload(receipt=receipt, request=request)
        ),
        **phase1_handoff_hashes(results=receipt.results, policy=policy),
    }
    core = {
        "schema_version": HANDOFF_READINESS_SCHEMA_VERSION,
        "audit_id": receipt.audit_id,
        "audit_manifest_hash": receipt.audit_manifest_hash,
        "request_hash": receipt.request_hash,
        "readiness": readiness,
        "sorted_target_handoffs": [target.model_dump(mode="python") for target in targets],
        "global_handoff_hashes": hashes,
        "blocking_reason_codes": blocking_reasons,
    }
    return HandoffReadinessReport(
        **core,
        handoff_readiness_hash=canonical_json_sha256(core),
    )


def build_phase1_handoff_bundle(
    *,
    report: HandoffReadinessReport,
    policy: Phase0APolicyRegistry,
    created_at: datetime | None = None,
) -> Phase1HandoffBundle | None:
    """Emit a consumer bundle only when at least one scope is automatically consumable."""

    if report.readiness == HandoffReadiness.BLOCKED:
        return None
    targets: list[HandoffTarget] = []
    for target in report.sorted_target_handoffs:
        scopes = [scope for scope in target.admission_scopes if scope.readiness != HandoffReadiness.BLOCKED]
        if scopes:
            targets.append(_handoff_target(target.audit_target_id, target.target_scope_hash, scopes))
    if not targets:
        return None
    targets = sorted(targets, key=lambda item: item.audit_target_id)
    admission_scopes = [scope for target in targets for scope in target.admission_scopes]
    admission_scope_set_hash = canonical_json_sha256(
        [scope.model_dump(mode="python") for scope in admission_scopes]
    )
    core = {
        "schema_version": PHASE1_HANDOFF_BUNDLE_SCHEMA_VERSION,
        "audit_id": report.audit_id,
        "audit_manifest_hash": report.audit_manifest_hash,
        "request_hash": report.request_hash,
        "serializer_version": policy.serializer_version,
        "global_handoff_hashes": report.global_handoff_hashes,
        "sorted_target_handoffs": [target.model_dump(mode="python") for target in targets],
        "admission_scope_set_hash": admission_scope_set_hash,
        "handoff_readiness_report_hash": report.handoff_readiness_hash,
    }
    return Phase1HandoffBundle(
        **core,
        phase1_handoff_bundle_hash=canonical_json_sha256(core),
        created_at=created_at or datetime.now(UTC),
    )


def _receipt_integrity_reason_codes(
    *,
    receipt: AuditReceipt,
    request: AuditRequest,
    policy: Phase0APolicyRegistry,
) -> list[str]:
    reasons: list[str] = []
    if receipt.audit_id != request.audit_id:
        reasons.append(REASON_AUDIT_ID_MISMATCH)
    if receipt.audit_policy_version != request.audit_policy_version or policy.policy_version != request.audit_policy_version:
        reasons.append(REASON_AUDIT_POLICY_MISMATCH)
    request_hash = canonical_json_sha256(audit_request_identity_payload(request))
    if receipt.request_hash != request_hash:
        reasons.append(REASON_REQUEST_HASH_MISMATCH)
    result_hash = canonical_json_sha256([result.model_dump(mode="python") for result in receipt.results])
    if receipt.result_hash != result_hash:
        reasons.append(REASON_RESULT_HASH_MISMATCH)
    manifest_hash = canonical_json_sha256(
        audit_manifest_base(
            audit_id=receipt.audit_id,
            audit_policy_version=receipt.audit_policy_version,
            request_hash=receipt.request_hash,
            result_hash=receipt.result_hash,
            results=receipt.results,
            policy=policy,
        )
    )
    if receipt.audit_manifest_hash != manifest_hash:
        reasons.append(REASON_MANIFEST_HASH_MISMATCH)
    return normalized_reason_codes(reasons)


def _target_handoff(
    *,
    result: TargetAuditResult,
    target: AuditTarget,
    policy: Phase0APolicyRegistry,
    global_reasons: list[str],
) -> HandoffTarget:
    target_scope_hash = canonical_json_sha256(target_scope_entry(target))
    target_reasons = list(global_reasons)
    if result.package_id != target.package_id or result.manifest_sha256 != target.manifest_sha256:
        target_reasons.append(REASON_TARGET_SCOPE_MISMATCH)
    scopes = [
        _scope_for_interval(
            result=result,
            target=target,
            target_scope_hash=target_scope_hash,
            interval=interval,
            policy=policy,
            target_reasons=target_reasons,
        )
        for interval in sorted(result.oos_intervals, key=lambda item: (item.interval_id, item.start_date, item.end_date))
    ]
    return _handoff_target(result.audit_target_id, target_scope_hash, scopes)


def _blocked_target(*, result: TargetAuditResult, reasons: list[str]) -> HandoffTarget:
    target_scope_hash = canonical_json_sha256(
        {
            "audit_target_id": result.audit_target_id,
            "program_id": result.program_id,
            "package_id": result.package_id,
            "manifest_sha256": result.manifest_sha256,
        }
    )
    return _handoff_target(result.audit_target_id, target_scope_hash, [])


def _missing_result_target(*, target: AuditTarget, reasons: list[str]) -> HandoffTarget:
    del reasons
    return _handoff_target(target.audit_target_id, canonical_json_sha256(target_scope_entry(target)), [])


def _handoff_target(
    audit_target_id: str,
    target_scope_hash: str,
    scopes: list[HandoffAdmissionScope],
) -> HandoffTarget:
    ordered_scopes = sorted(scopes, key=lambda item: item.admission_scope_id)
    core = {
        "audit_target_id": audit_target_id,
        "target_scope_hash": target_scope_hash,
        "admission_scopes": [scope.model_dump(mode="python") for scope in ordered_scopes],
    }
    return HandoffTarget(
        **core,
        target_handoff_hash=canonical_json_sha256(core),
    )


def _scope_for_interval(
    *,
    result: TargetAuditResult,
    target: AuditTarget,
    target_scope_hash: str,
    interval: OOSInterval,
    policy: Phase0APolicyRegistry,
    target_reasons: list[str],
) -> HandoffAdmissionScope:
    dates = set(_inclusive_dates(interval.start_date, interval.end_date, result))
    authorities = [
        item
        for item in result.candidate_authority
        if item.decision_date in dates and item.signal_context_hash == interval.signal_context_hash
    ]
    runtimes = [item for item in result.runtime_semantics if item.decision_date in dates]
    reasons = [*target_reasons]
    if not interval.signal_context_hash:
        reasons.append(REASON_SIGNAL_CONTEXT_MISSING)
    if not authorities or {item.decision_date for item in authorities} != dates:
        reasons.append(REASON_SIGNAL_EVIDENCE_MISSING)
    if not runtimes or {item.decision_date for item in runtimes} != dates:
        reasons.append(REASON_RUNTIME_SEMANTICS_MISSING)

    reasons.extend(_asset_ledger_reason_codes(result=result, target=target))
    stable_payload, stable_reasons = _stable_semantics_payload(
        target=target,
        runtimes=runtimes,
        authorities=authorities,
        policy=policy,
    )
    reasons.extend(stable_reasons)
    evidence_scope, base_readiness = _scope_evidence(interval)
    if base_readiness == HandoffReadiness.BLOCKED:
        reasons.append(REASON_SCOPE_EVIDENCE_UNAVAILABLE)
    if interval.formal_oos_status == FormalOOSStatus.FORMAL_OOS and _signal_evidence_level(authorities) != CandidateAuthorityStatus.FORMAL:
        reasons.append(REASON_SIGNAL_EVIDENCE_MISSING)

    normalizer_reasons = normalized_reason_codes(reasons)
    blocking_reasons = normalized_reason_codes([*normalizer_reasons, *interval.phase0a_reason_codes])
    readiness = base_readiness if not normalizer_reasons else HandoffReadiness.BLOCKED
    signal_evidence_level = _signal_evidence_level(authorities)
    capability_hash = canonical_json_sha256(
        [
            {
                "decision_date": item.decision_date,
                "stage_capabilities": item.stage_capabilities,
                "status": item.status,
            }
            for item in sorted(authorities, key=lambda item: item.decision_date)
        ]
    )
    clock_hash = canonical_json_sha256(
        [
            item.decision_clock.model_dump(mode="python") if item.decision_clock is not None else None
            for item in sorted(authorities, key=lambda item: item.decision_date)
        ]
    ) if authorities else None
    oos_interval_hash = canonical_json_sha256(interval)
    identity = {
        "audit_target_id": result.audit_target_id,
        "phase0a_signal_context_hash": interval.signal_context_hash,
        "oos_interval_id": interval.interval_id,
        "capability": interval.signal_capability,
        "date_start": interval.start_date,
        "date_end": interval.end_date,
    }
    admission_scope_id = stable_identifier("advscope", identity)
    scope_core = {
        "admission_scope_id": admission_scope_id,
        "audit_target_id": result.audit_target_id,
        "target_scope_hash": target_scope_hash,
        "phase0a_signal_context_hash": interval.signal_context_hash,
        "oos_interval_id": interval.interval_id,
        "oos_interval_hash": oos_interval_hash,
        "capability": interval.signal_capability,
        "capability_hash": capability_hash,
        "date_start": interval.start_date,
        "date_end": interval.end_date,
        "formal_oos_status": interval.formal_oos_status,
        "signal_evidence_level": signal_evidence_level,
        "evidence_scope": evidence_scope,
        "readiness": readiness,
        "stable_signal_semantics_payload_v1": stable_payload,
        "stable_signal_semantics_hash": canonical_json_sha256(stable_payload) if stable_payload is not None else None,
        "decision_clock_hash": clock_hash,
        "blocking_reason_codes": blocking_reasons,
    }
    return HandoffAdmissionScope(
        **scope_core,
        admission_scope_hash=canonical_json_sha256(scope_core),
    )


def _inclusive_dates(start: Any, end: Any, result: TargetAuditResult) -> list[Any]:
    return [
        item.decision_date
        for item in result.oos_classifications
        if start <= item.decision_date <= end
    ]


def _asset_ledger_reason_codes(*, result: TargetAuditResult, target: AuditTarget) -> list[str]:
    entries = result.asset_ledger
    if not entries:
        return [REASON_ASSET_LEDGER_INCOMPLETE]
    if any(entry.package_id != target.package_id for entry in entries):
        return [REASON_ASSET_LEDGER_INCOMPLETE]
    if any(
        not entry.asset_sha256 or entry.admissibility != "FORMAL"
        for entry in entries
    ):
        return [REASON_ASSET_LEDGER_INCOMPLETE]
    return []


def _stable_semantics_payload(
    *,
    target: AuditTarget,
    runtimes: list[RuntimeSemanticsEvidence],
    authorities: list[CandidateAuthorityReport],
    policy: Phase0APolicyRegistry,
) -> tuple[dict[str, Any] | None, list[str]]:
    reasons: list[str] = []
    if not runtimes:
        return None, [REASON_RUNTIME_SEMANTICS_MISSING]
    semantics_ids = {item.selection_runtime_semantics_id for item in runtimes if item.selection_runtime_semantics_id}
    if len(semantics_ids) != 1 or len(semantics_ids) != len({item.selection_runtime_semantics_id for item in runtimes}):
        reasons.append(REASON_RUNTIME_SEMANTICS_CONFLICT)
    if not all(item.effective_config_chain_complete for item in runtimes):
        reasons.append(REASON_EFFECTIVE_CONFIG_MISSING)
    config_maps = [item.effective_config_hashes for item in runtimes]
    if not config_maps or any(not mapping or any(not value for value in mapping.values()) for mapping in config_maps):
        reasons.append(REASON_EFFECTIVE_CONFIG_MISSING)
    elif any(mapping != config_maps[0] for mapping in config_maps[1:]):
        reasons.append(REASON_EFFECTIVE_CONFIG_CONFLICT)
    if not policy.calendar_hash:
        reasons.append(REASON_CALENDAR_HASH_MISSING)
    clocks = [item.decision_clock for item in authorities]
    if not clocks or any(clock is None or not clock.is_formal_canonical_clock for clock in clocks):
        reasons.append(REASON_DECISION_CLOCK_MISSING)
    elif any(clock.calendar_hash != policy.calendar_hash for clock in clocks):
        reasons.append(REASON_CALENDAR_HASH_MISMATCH)
    if reasons:
        return None, normalized_reason_codes(reasons)
    runtime_semantics_id = next(iter(semantics_ids))
    config_hashes = config_maps[0]
    payload = {
        "schema_version": "advisory_stable_signal_semantics_payload_v1",
        "package_id": target.package_id,
        "manifest_sha256": target.manifest_sha256,
        "selection_runtime_semantics_hash": canonical_json_sha256(
            {"selection_runtime_semantics_id": runtime_semantics_id}
        ),
        "package_effective_config_hash": canonical_json_sha256(config_hashes),
        "calendar_hash": policy.calendar_hash,
    }
    return payload, []


def _signal_evidence_level(authorities: list[CandidateAuthorityReport]) -> CandidateAuthorityStatus:
    statuses = {item.status for item in authorities}
    return next(iter(statuses)) if len(statuses) == 1 else CandidateAuthorityStatus.NONE


def _scope_evidence(interval: OOSInterval) -> tuple[HandoffEvidenceScope, HandoffReadiness]:
    if (
        interval.formal_oos_status == FormalOOSStatus.FORMAL_OOS
        and interval.availability_status == AvailabilityStatus.AVAILABLE
    ):
        return HandoffEvidenceScope.FORMAL_OOS, HandoffReadiness.READY
    if (
        interval.formal_oos_status == FormalOOSStatus.RETROSPECTIVE_RESEARCH_ONLY
        and interval.availability_status == AvailabilityStatus.UNAVAILABLE
    ):
        return HandoffEvidenceScope.RETROSPECTIVE_RESEARCH_ONLY, HandoffReadiness.PARTIAL
    if (
        interval.formal_oos_status == FormalOOSStatus.NONE
        and interval.availability_status == AvailabilityStatus.UNAVAILABLE
        and interval.research_replay_eligible
    ):
        return HandoffEvidenceScope.GAP_ONLY, HandoffReadiness.PARTIAL
    return HandoffEvidenceScope.GAP_ONLY, HandoffReadiness.BLOCKED


def _global_readiness(scopes: list[HandoffAdmissionScope]) -> HandoffReadiness:
    if not scopes or all(scope.readiness == HandoffReadiness.BLOCKED for scope in scopes):
        return HandoffReadiness.BLOCKED
    if all(scope.readiness == HandoffReadiness.READY for scope in scopes):
        return HandoffReadiness.READY
    return HandoffReadiness.PARTIAL
