"""Read-only Phase 0A audit orchestration and deterministic receipt writing."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import (
    AuditReceipt,
    AuditRequest,
    EmbargoEvidence,
    HMMVintageEvidence,
    LabelMaturityStatus,
    OOSClassificationInput,
    Phase0AAuditError,
    Phase0APolicyRegistry,
    SourceAvailabilityStatus,
    TargetAuditResult,
)
from .policy import (
    canonical_json_sha256,
    canonical_json_text,
    classify_oos,
    coalesce_oos_intervals,
    embargo_formal_start,
    effective_cutoff,
    missing_embargo_policy_reason_codes,
    missing_policy_reason_codes,
    normalized_reason_codes,
)
from .resolvers import (
    AuditReaders,
    build_asset_ledger,
    resolve_audit_day,
    resolve_candidate_authority,
    resolve_decision_clock,
    resolve_hmm_vintage,
    resolve_risk_policy_evidence,
    resolve_runtime_semantics,
    resolve_source_availability,
    resolve_universe_survivorship,
)


REASON_POLICY_VERSION_MISMATCH = "ADVISORY_PHASE0A_POLICY_VERSION_MISMATCH"
REASON_SURVIVORSHIP_PIT_MISSING = "ADVISORY_PHASE0A_SURVIVORSHIP_PIT_UNIVERSE_MISSING"
REASON_LABEL_SOURCE_UNAVAILABLE = "ADVISORY_PHASE0A_LABEL_SOURCE_UNAVAILABLE"
RECEIPT_SCHEMA_VERSION = "advisory_phase0a_receipt_v1"


class AdvisoryPhase0AAuditService:
    """Build evidence receipts only; this class has no write-capable dependency."""

    def __init__(self, *, readers: AuditReaders, policy: Phase0APolicyRegistry) -> None:
        self._readers = readers
        self._policy = policy

    def audit(self, request: AuditRequest) -> AuditReceipt:
        if request.audit_policy_version != self._policy.policy_version:
            raise Phase0AAuditError(
                f"{REASON_POLICY_VERSION_MISMATCH}: request={request.audit_policy_version} policy={self._policy.policy_version}"
            )
        results = [self._audit_target(target) for target in sorted(request.targets, key=lambda item: item.audit_target_id)]
        request_payload = {
            "schema_version": "advisory_phase0a_request_identity_v1",
            "audit_id": request.audit_id,
            "audit_policy_version": request.audit_policy_version,
            "targets": [target.model_dump(mode="python") for target in sorted(request.targets, key=lambda item: item.audit_target_id)],
        }
        request_hash = canonical_json_sha256(request_payload)
        policy_hash = canonical_json_sha256(self._policy)
        result_payload = [result.model_dump(mode="python") for result in results]
        result_hash = canonical_json_sha256(result_payload)
        handoff_hashes = _phase1_handoff_hashes(results=results, policy=self._policy)
        audit_manifest_base = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "audit_id": request.audit_id,
            "request_hash": request_hash,
            "audit_policy_version": request.audit_policy_version,
            "policy_hash": policy_hash,
            "serializer_version": self._policy.serializer_version,
            "result_hash": result_hash,
            "phase1_handoff_hashes": handoff_hashes,
            "read_only": True,
            "raw_intermediate_location": f"tmp/advisory_phase0a/{request.audit_id}",
            "raw_intermediate_cleanup_status": "NOT_CREATED",
        }
        audit_manifest_hash = canonical_json_sha256(audit_manifest_base)
        phase0a_reasons = normalized_reason_codes(
            [code for result in results for code in result.phase0a_reason_codes]
        )
        upstream_reasons = normalized_reason_codes(
            [code for result in results for code in result.upstream_reason_codes]
        )
        return AuditReceipt(
            audit_id=request.audit_id,
            audit_policy_version=request.audit_policy_version,
            request_hash=request_hash,
            audit_manifest_hash=audit_manifest_hash,
            result_hash=result_hash,
            results=results,
            phase0a_reason_codes=phase0a_reasons,
            upstream_reason_codes=upstream_reasons,
        )

    def _audit_target(self, target: Any) -> TargetAuditResult:
        asset_ledger_by_identity: dict[str, Any] = {}
        runtime_semantics = []
        hmm_vintages: list[HMMVintageEvidence] = []
        source_availability = []
        universe_survivorship = []
        risk_policy_evidence = []
        embargo_evidence = []
        candidate_authority = []
        classifications = []
        binding_version_ids: dict[Any, str | None] = {}
        all_reasons: list[str] = []
        upstream_reasons: list[str] = []
        manifest_sha256: str | None = None

        for decision_date in target.decision_dates:
            resolved = resolve_audit_day(readers=self._readers, target=target, decision_date=decision_date)
            binding_version_ids[decision_date] = resolved.binding.binding_version_id if resolved.binding is not None else None
            if resolved.package is not None:
                manifest_sha256 = manifest_sha256 or resolved.package.manifest_sha256
            asset_ledger = build_asset_ledger(resolved)
            for entry in asset_ledger:
                asset_ledger_by_identity[canonical_json_sha256(entry)] = entry
            runtime = resolve_runtime_semantics(resolved)
            hmm = resolve_hmm_vintage(resolved)
            clock = resolve_decision_clock(resolved)
            universe = resolve_universe_survivorship(resolved)
            risk = resolve_risk_policy_evidence(resolved)
            sources = resolve_source_availability(resolved, source_probe=self._readers.source_probe)
            authority = resolve_candidate_authority(
                readers=self._readers,
                resolved=resolved,
                hmm=hmm,
                clock=clock,
                risk=risk,
                universe=universe,
            )
            label_policy_hash = str(self._policy.label_policy.get("policy_hash") or "").strip() or None
            label_context_hash = (
                canonical_json_sha256(
                    {
                        "canonical_signal_observation_id": authority.canonical_signal_observation_id,
                        "label_policy_hash": label_policy_hash,
                    }
                )
                if authority.canonical_signal_observation_id is not None and label_policy_hash is not None
                else None
            )
            authority = authority.model_copy(update={"label_context_hash": label_context_hash})
            runtime_semantics.append(runtime)
            hmm_vintages.append(hmm)
            source_availability.extend(sources)
            universe_survivorship.append(universe)
            risk_policy_evidence.append(risk)
            candidate_authority.append(authority)

            formal_assets = [entry for entry in asset_ledger if entry.admissibility != "FORBIDDEN"]
            assets_complete = bool(formal_assets) and all(entry.admissibility == "FORMAL" for entry in formal_assets)
            assets_identity_complete = bool(formal_assets) and all(
                entry.asset_sha256
                and "ADVISORY_PHASE0A_ASSET_HASH_MISSING" not in entry.reason_codes
                and "ADVISORY_PHASE0A_ASSET_CLOSURE_MISSING" not in entry.reason_codes
                for entry in formal_assets
            )
            hmm_complete = not hmm.enabled or hmm.status == "FORMAL"
            hmm_identity_complete = not hmm.enabled or bool(
                hmm.model_snapshot_id
                and hmm.signal_preset
                and hmm.model_artifact_sha256
                and hmm.coefficient_sha256
                and hmm.as_of_trade_date == decision_date
                and hmm.effective_trade_date == (resolved.evidence.target_trade_date if resolved.evidence is not None else None)
            )
            point_in_time_available = any(
                row.is_point_in_time and row.status == SourceAvailabilityStatus.FORMAL_READY for row in sources
            ) and all(row.status == SourceAvailabilityStatus.FORMAL_READY for row in sources)
            cutoff_inputs: dict[str, Any] = {
                f"asset_{index}": self._asset_effective_cutoff(entry) for index, entry in enumerate(formal_assets)
            }
            cutoff_inputs["runtime_semantics"] = runtime.historical_available_at.date() if runtime.historical_available_at else None
            if risk.risk_policy_hash or risk.industry_blacklist_hash or risk.tradability_policy_hash:
                cutoff_inputs["risk_policy"] = risk.policy_available_at.date() if risk.policy_available_at else None
            cutoff_inputs.update(
                {
                    f"universe_policy_{index}": layer.policy_available_at.date() if layer.policy_available_at else None
                    for index, layer in enumerate(universe.layers)
                }
            )
            if hmm.enabled:
                cutoff_inputs["hmm"] = self._hmm_effective_cutoff(resolved, hmm)
            resolved_cutoff, cutoff_reasons = effective_cutoff(cutoff_inputs)
            embargo = self._embargo_evidence(
                effective_cutoff=resolved_cutoff,
                decision_dates=target.decision_dates,
            )
            embargo_evidence.append(embargo)
            universe_formal = all(
                layer.status == SourceAvailabilityStatus.FORMAL_READY
                for layer in universe.layers
            )
            universe_identity_complete = bool(universe.layers) and all(
                layer.status not in {SourceAvailabilityStatus.MISSING, SourceAvailabilityStatus.FORBIDDEN}
                for layer in universe.layers
            )
            clock_identity_complete = bool(
                clock.selection_as_of_trade_date == decision_date
                and clock.effective_cutoff_date == decision_date
                and clock.target_trade_date is not None
                and clock.target_trade_date > decision_date
            )
            base_reasons = [
                *resolved.phase0a_reason_codes,
                *[code for entry in asset_ledger for code in entry.reason_codes],
                *runtime.reason_codes,
                *hmm.reason_codes,
                *clock.reason_codes,
                *risk.reason_codes,
                *universe.reason_codes,
                *embargo.reason_codes,
                *[code for row in sources for code in row.reason_codes],
                *authority.phase0a_reason_codes,
                *cutoff_reasons,
                *missing_policy_reason_codes(self._policy),
            ]
            mandatory_closure = bool(
                resolved.binding is not None
                and resolved.package is not None
                and resolved.evidence is not None
                and assets_identity_complete
                and hmm_identity_complete
                and clock_identity_complete
                and universe_identity_complete
            )
            classification = classify_oos(
                OOSClassificationInput(
                    decision_date=decision_date,
                    formal_start_date=embargo.formal_start_date,
                    effective_cutoff=resolved_cutoff,
                    mandatory_closure_complete=mandatory_closure,
                    historical_semantics_available=(
                        runtime.is_historical_binding
                        and hmm_complete
                        and assets_complete
                        and clock.is_formal_canonical_clock
                        and universe_formal
                        and embargo.status == SourceAvailabilityStatus.FORMAL_READY
                    ),
                    point_in_time_source_available=point_in_time_available,
                    candidate_authority_formal=authority.status.value == "FORMAL",
                    research_replay_eligible=bool(
                        resolved.binding is not None
                        and resolved.package is not None
                        and resolved.evidence is not None
                        and authority.status.value != "NONE"
                    ),
                    reason_codes=base_reasons,
                    upstream_reason_codes=authority.upstream_reason_codes,
                )
            )
            classification = classification.model_copy(
                update={
                    "signal_context_hash": authority.signal_context_hash,
                    "signal_capability": "candidate_signal",
                    "label_maturity_status": self._label_maturity_status(decision_date),
                    "phase0a_reason_codes": normalized_reason_codes(
                        [
                            *classification.phase0a_reason_codes,
                            *([] if self._label_maturity_status(decision_date) != LabelMaturityStatus.UNAVAILABLE else [REASON_LABEL_SOURCE_UNAVAILABLE]),
                        ]
                    ),
                }
            )
            classifications.append(classification)
            all_reasons.extend(classification.phase0a_reason_codes)
            upstream_reasons.extend(classification.upstream_reason_codes)

        ordered_assets = sorted(asset_ledger_by_identity.values(), key=lambda entry: (entry.asset_type, entry.asset_ref, entry.asset_sha256 or ""))
        return TargetAuditResult(
            audit_target_id=target.audit_target_id,
            program_id=target.program_id,
            package_id=target.package_id,
            manifest_sha256=manifest_sha256,
            binding_version_ids=binding_version_ids,
            asset_ledger=ordered_assets,
            runtime_semantics=sorted(runtime_semantics, key=lambda item: item.decision_date),
            hmm_vintages=sorted(hmm_vintages, key=lambda item: item.decision_date),
            source_availability=sorted(source_availability, key=lambda item: (item.decision_date, item.source_id, item.capability)),
            universe_survivorship=sorted(universe_survivorship, key=lambda item: item.decision_date),
            risk_policy_evidence=sorted(risk_policy_evidence, key=lambda item: item.decision_date),
            embargo_evidence=sorted(embargo_evidence, key=lambda item: (item.effective_cutoff or item.formal_start_date or target.decision_dates[0])),
            candidate_authority=sorted(candidate_authority, key=lambda item: item.decision_date),
            oos_classifications=sorted(classifications, key=lambda item: item.decision_date),
            oos_intervals=coalesce_oos_intervals(classifications),
            phase0a_reason_codes=normalized_reason_codes(all_reasons),
            upstream_reason_codes=normalized_reason_codes(upstream_reasons),
        )

    @staticmethod
    def _hmm_data_cutoff(resolved: Any) -> Any:
        if resolved.evidence is None:
            return None
        payload = resolved.evidence.evidence_payload_json or {}
        metadata = payload.get("phase0a_hmm_metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        value = metadata.get("data_cutoff")
        if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
            return value.date() if hasattr(value, "date") else value
        if isinstance(value, str):
            try:
                from datetime import date

                return date.fromisoformat(value[:10])
            except ValueError:
                return None
        return None

    @classmethod
    def _hmm_effective_cutoff(cls, resolved: Any, hmm: HMMVintageEvidence) -> Any:
        values = [cls._hmm_data_cutoff(resolved)]
        if hmm.available_at is not None:
            values.append(hmm.available_at.date())
        if hmm.snapshot_trained_at is not None:
            values.append(hmm.snapshot_trained_at.date())
        if hmm.training_information_cutoff is not None:
            values.append(hmm.training_information_cutoff)
        return max((value for value in values if value is not None), default=None)

    @staticmethod
    def _asset_effective_cutoff(entry: Any) -> Any:
        values = [
            entry.data_cutoff,
            entry.information_cutoff_ts.date() if entry.information_cutoff_ts else None,
            entry.training_data_end_ts.date() if entry.training_data_end_ts else None,
            entry.model_selection_decision_ts.date() if entry.model_selection_decision_ts else None,
            entry.research_decision_ts.date() if entry.research_decision_ts else None,
            entry.frozen_at.date() if entry.frozen_at else None,
            entry.promoted_or_activated_at.date() if entry.promoted_or_activated_at else None,
            entry.available_at.date() if entry.available_at else None,
        ]
        return max((value for value in values if value is not None), default=None)

    def _embargo_evidence(self, *, effective_cutoff: Any, decision_dates: list[Any]) -> EmbargoEvidence:
        policy = self._policy
        policy_reasons = missing_embargo_policy_reason_codes(policy)
        trading_days = None
        calendar_reasons: list[str] = []
        if effective_cutoff is not None and self._readers.calendar is not None:
            try:
                trading_days = self._readers.calendar.list_trading_days(
                    start_date=effective_cutoff,
                    end_date=max(decision_dates),
                )
            except Exception:
                trading_days = None
        formal_start, embargo_reasons = embargo_formal_start(
            effective_cutoff=effective_cutoff,
            trading_days=trading_days,
            minimum_trading_day_gap=policy.minimum_trading_day_gap,
        )
        reasons = normalized_reason_codes([*policy_reasons, *calendar_reasons, *embargo_reasons])
        return EmbargoEvidence(
            policy_id=policy.embargo_policy_id,
            policy_version=policy.embargo_policy_version,
            policy_hash=policy.embargo_policy_hash,
            minimum_trading_day_gap=policy.minimum_trading_day_gap,
            cutoff_timestamp_normalization=policy.cutoff_timestamp_normalization,
            training_label_information_end_rule=policy.training_label_information_end_rule,
            calendar_version=policy.calendar_version,
            calendar_hash=policy.calendar_hash,
            effective_cutoff=effective_cutoff,
            formal_start_date=formal_start,
            status=SourceAvailabilityStatus.FORMAL_READY if not reasons and formal_start is not None else SourceAvailabilityStatus.MISSING,
            reason_codes=reasons,
        )

    def _label_maturity_status(self, decision_date: Any) -> LabelMaturityStatus:
        mapping = self._policy.label_policy.get("maturity_status_by_decision_date")
        mapping = mapping if isinstance(mapping, dict) else {}
        value = mapping.get(decision_date.isoformat())
        try:
            return LabelMaturityStatus(str(value)) if value else LabelMaturityStatus.UNAVAILABLE
        except ValueError:
            return LabelMaturityStatus.UNAVAILABLE


def receipt_artifact_payloads(
    *,
    receipt: AuditReceipt,
    request: AuditRequest,
    policy: Phase0APolicyRegistry,
) -> dict[str, Any]:
    """Construct all tracked compact receipts without candidate or market-data detail."""

    results = receipt.results
    policy_payload = policy.model_dump(mode="python")
    target_scope = {
        "schema_version": "advisory_phase0a_target_scope_registry_v1",
        "audit_id": receipt.audit_id,
        "request_hash": receipt.request_hash,
        "targets": [
            {
                "audit_target_id": target.audit_target_id,
                "program_id": target.program_id,
                "package_id": target.package_id,
                "manifest_sha256": target.manifest_sha256,
                "expected_alpha_mode": target.expected_alpha_mode.value,
                "decision_date_range": target.decision_date_range.model_dump(mode="json"),
                "decision_dates": [item.isoformat() for item in target.decision_dates],
                "style_family": target.style_family,
                "requested_capabilities": target.requested_capabilities,
            }
            for target in sorted(request.targets, key=lambda item: item.audit_target_id)
        ],
    }
    asset_ledger = {
        "schema_version": "advisory_phase0a_asset_vintage_ledger_v1",
        "audit_id": receipt.audit_id,
        "entries": [entry.model_dump(mode="python") for result in results for entry in result.asset_ledger],
    }
    runtime = {
        "schema_version": "advisory_phase0a_runtime_semantics_ledger_v1",
        "audit_id": receipt.audit_id,
        "entries": [entry.model_dump(mode="python") for result in results for entry in result.runtime_semantics],
    }
    hmm = {
        "schema_version": "advisory_phase0a_hmm_vintage_ledger_v1",
        "audit_id": receipt.audit_id,
        "entries": [entry.model_dump(mode="python") for result in results for entry in result.hmm_vintages],
    }
    source = {
        "schema_version": "advisory_phase0a_source_availability_matrix_v1",
        "audit_id": receipt.audit_id,
        "entries": [entry.model_dump(mode="python") for result in results for entry in result.source_availability],
    }
    survivorship = {
        "schema_version": "advisory_phase0a_universe_survivorship_report_v1",
        "audit_id": receipt.audit_id,
        "entries": [entry.model_dump(mode="python") for result in results for entry in result.universe_survivorship],
        "fallback_reason_codes": [REASON_SURVIVORSHIP_PIT_MISSING]
        if not any(result.universe_survivorship for result in results)
        else [],
    }
    oos = {
        "schema_version": "advisory_phase0a_oos_interval_report_v1",
        "audit_id": receipt.audit_id,
        "classifications": [entry.model_dump(mode="python") for result in results for entry in result.oos_classifications],
        "intervals": [entry.model_dump(mode="python") for result in results for entry in result.oos_intervals],
        "embargo": [entry.model_dump(mode="python") for result in results for entry in result.embargo_evidence],
    }
    metric_label = {
        "schema_version": "advisory_phase0a_metric_label_policy_v1",
        "audit_id": receipt.audit_id,
        "policy": policy_payload,
        "policy_hash": canonical_json_sha256(policy_payload),
    }
    prior = {
        "schema_version": "advisory_phase0a_prior_registry_v1",
        "audit_id": receipt.audit_id,
        "policy": policy.prior_policy,
        "status": "NOT_CONSUMED_IN_PHASE0A",
    }
    multiple_testing = {
        "schema_version": "advisory_phase0a_multiple_testing_registry_v1",
        "audit_id": receipt.audit_id,
        "policy": policy.multiple_testing_policy,
        "status": "NOT_CONSUMED_IN_PHASE0A",
    }
    authority_entries = [
        (result, entry)
        for result in results
        for entry in result.candidate_authority
    ]
    canonical_groups: dict[str, list[dict[str, Any]]] = {}
    for result, entry in authority_entries:
        if not entry.signal_context_hash:
            continue
        canonical_groups.setdefault(entry.signal_context_hash, []).append(
            {
                "canonical_signal_observation_id": entry.canonical_signal_observation_id,
                "audit_target_id": result.audit_target_id,
                "program_id": result.program_id,
                "package_id": result.package_id,
                "decision_date": entry.decision_date,
                "binding_version_id": result.binding_version_ids.get(entry.decision_date),
            }
        )
    capability = {
        "schema_version": "advisory_phase0a_candidate_authority_stage_capability_v1",
        "audit_id": receipt.audit_id,
        "entries": [entry.model_dump(mode="python") for _result, entry in authority_entries],
        "canonical_observation_groups": [
            {
                "signal_context_hash": signal_hash,
                "lineage": sorted(
                    lineage,
                    key=lambda item: (item["decision_date"], item["program_id"], item["audit_target_id"]),
                ),
            }
            for signal_hash, lineage in sorted(canonical_groups.items())
        ],
    }
    prior_cohort = {
        "schema_version": "advisory_phase0a_prior_cohort_report_v1",
        "audit_id": receipt.audit_id,
        "status": "NOT_REGISTERED",
        "reason_codes": ["ADVISORY_PHASE0A_PRIOR_COHORT_UNAVAILABLE"],
    }
    approval = {
        "schema_version": "advisory_phase0a_approval_receipt_v1",
        "audit_id": receipt.audit_id,
        "approval_status": "NOT_APPROVED",
        "approved_request_reference": request.approved_request_reference,
        "automatic_approval": False,
        "phase1_exit_gate_status": "BLOCKED_PENDING_MANUAL_APPROVAL",
    }
    manifest_base = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "audit_id": receipt.audit_id,
        "request_hash": receipt.request_hash,
        "audit_policy_version": receipt.audit_policy_version,
        "policy_hash": canonical_json_sha256(policy_payload),
        "serializer_version": policy.serializer_version,
        "result_hash": receipt.result_hash,
        "phase1_handoff_hashes": _phase1_handoff_hashes(results=results, policy=policy),
        "read_only": True,
        "raw_intermediate_location": f"tmp/advisory_phase0a/{receipt.audit_id}",
        "raw_intermediate_cleanup_status": "NOT_CREATED",
    }
    manifest = {**manifest_base, "audit_manifest_hash": receipt.audit_manifest_hash}
    summary = _summary_markdown(receipt)
    payloads: dict[str, Any] = {
        "target_scope_registry.json": target_scope,
        "package_asset_vintage_ledger.json": asset_ledger,
        "runtime_semantics_ledger.json": runtime,
        "hmm_vintage_ledger.json": hmm,
        "source_availability_matrix.json": source,
        "universe_survivorship_report.json": survivorship,
        "oos_interval_report.json": oos,
        "metric_label_policy_registry.json": metric_label,
        "prior_registry.json": prior,
        "multiple_testing_registry.json": multiple_testing,
        "audit_manifest.json": manifest,
        "audit_summary.md": summary,
        "candidate_authority_stage_capability_report.json": capability,
        "prior_cohort_report.json": prior_cohort,
        "approval_receipt.json": approval,
    }
    return payloads


def write_receipt_artifacts(
    *,
    receipt: AuditReceipt,
    request: AuditRequest,
    policy: Phase0APolicyRegistry,
    output_root: Path,
) -> Path:
    """Write append-only compact receipts. Existing audit ids are never overwritten."""

    destination = output_root / receipt.audit_id
    if destination.exists():
        raise Phase0AAuditError(f"receipt destination already exists: {destination}")
    output_root.mkdir(parents=True, exist_ok=True)
    staging = output_root / f".{receipt.audit_id}.staging"
    if staging.exists():
        raise Phase0AAuditError(f"receipt staging destination already exists: {staging}")
    staging.mkdir(parents=False)
    for filename, payload in receipt_artifact_payloads(receipt=receipt, request=request, policy=policy).items():
        path = staging / filename
        if filename.endswith(".md"):
            path.write_text(str(payload), encoding="utf-8", newline="\n")
        else:
            path.write_text(canonical_json_text(payload) + "\n", encoding="utf-8", newline="\n")
    staging.replace(destination)
    return destination


def _summary_markdown(receipt: AuditReceipt) -> str:
    lines = [
        "# Advisory Phase 0A Audit",
        "",
        f"- Audit ID: `{receipt.audit_id}`",
        f"- Policy: `{receipt.audit_policy_version}`",
        f"- Request hash: `{receipt.request_hash}`",
        f"- Manifest hash: `{receipt.audit_manifest_hash}`",
        f"- Result hash: `{receipt.result_hash}`",
        "- Approval status: `NOT_APPROVED`",
        "",
        "| Target | Formal OOS | Retrospective | None |",
        "|---|---:|---:|---:|",
    ]
    for result in sorted(receipt.results, key=lambda item: item.audit_target_id):
        counts = {"FORMAL_OOS": 0, "RETROSPECTIVE_RESEARCH_ONLY": 0, "NONE": 0}
        for classification in result.oos_classifications:
            counts[classification.formal_oos_status.value] += 1
        lines.append(
            f"| `{result.audit_target_id}` | {counts['FORMAL_OOS']} | "
            f"{counts['RETROSPECTIVE_RESEARCH_ONLY']} | {counts['NONE']} |"
        )
    return "\n".join(lines) + "\n"


def _phase1_handoff_hashes(*, results: list[TargetAuditResult], policy: Phase0APolicyRegistry) -> dict[str, str]:
    """Freeze Phase 1 input identities without asserting that Phase 1 is approved."""

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
