from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd
from pydantic import ValidationError

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.qe_alpha_generator_contracts import (
    GENERATOR_EXPERIMENT_ID,
    GENERATOR_FAMILY_ID,
    GENERATOR_ALLOWED_FIELDS,
    GENERATOR_PROMPT_SCHEMA_V2,
    SHA256_PATTERN,
    AdvisoryQEAlphaGenerationReceiptV1,
    FrozenAdvisoryQEAlphaGeneratorRequestV1,
    QEAlphaGeneratorModelIdentityV1,
    QEAlphaGeneratorProposalV1,
    build_generation_receipt,
    build_generator_mve_receipt,
    build_generator_proposal,
    build_generator_request,
    generator_allowed_fields_for_source,
)
from backend.services.advisory_model_first.qe_alpha_mve_contracts import (
    MVE_FAMILIES,
    QEAlphaProposalV1,
)
from backend.services.advisory_model_first.qe_alpha_mve_pipeline import (
    CURRENT_PARENT_ARM_ID,
    _deflated_sharpe_diagnostic,
    _local_path,
    _moving_block_interval,
    _peak_rss_bytes,
    _safe_correlation,
    _static_schema_sha256,
    build_source_panel,
    compile_proposal_scores,
)
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import (
    ConsumedWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)
from backend.services.quantevolver.llm_client import get_llm_kwargs
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


CATALOG_SNAPSHOT_SCHEMA = "advisory_qe_alpha_generator_catalog_snapshot_v1"
GENERATION_BUNDLE_SCHEMA = "advisory_qe_alpha_generation_bundle_v1"
MVE_BUNDLE_SCHEMA = "advisory_qe_alpha_generator_mve_bundle_v1"
CATALOG_COLUMNS = (
    "factor_name",
    "source",
    "catalog_version",
    "expression",
    "formula_hint",
    "variables",
    "factor_formulation",
    "factor_type",
    "data_source",
    "dedup_hash",
    "is_available",
    "code_text",
)


def build_catalog_snapshot(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for raw in rows:
        code = raw.get("code_text")
        variables = raw.get("variables")
        if not isinstance(variables, (dict, list, tuple)):
            variables = None
        records.append(
            {
                "factor_name": str(raw.get("factor_name") or ""),
                "source": str(raw.get("source") or ""),
                "catalog_version": str(raw.get("catalog_version") or ""),
                "expression": _optional_text(raw.get("expression")),
                "formula_hint": _optional_text(raw.get("formula_hint")),
                "variables": variables,
                "factor_formulation": _optional_text(raw.get("factor_formulation")),
                "factor_type": _optional_text(raw.get("factor_type")),
                "data_source": _optional_text(raw.get("data_source")),
                "dedup_hash": _optional_text(raw.get("dedup_hash")),
                "is_available": bool(raw.get("is_available", False)),
                "code_text_sha256": (
                    hashlib.sha256(str(code).encode("utf-8")).hexdigest() if code not in (None, "") else None
                ),
            }
        )
    records.sort(key=lambda item: (item["factor_name"], item["source"], item["catalog_version"]))
    payload = {
        "schema_version": CATALOG_SNAPSHOT_SCHEMA,
        "source_table": "public.aistock_factor_catalog",
        "read_only_transaction": True,
        "performance_fields_included": False,
        "secret_fields_included": False,
        "records": records,
        "row_count": len(records),
    }
    payload["snapshot_sha256"] = canonical_json_sha256(payload)
    payload["snapshot_id"] = f"advqegencat_{payload['snapshot_sha256'][:24]}"
    return payload


def snapshot_dev_factor_catalog(output_path: str | Path) -> dict[str, Any]:
    from backend.db.pg_pool import get_conn

    query = """
        SELECT factor_name, source, catalog_version, expression, formula_hint,
               variables, factor_formulation, factor_type, data_source,
               dedup_hash, is_available, code_text
        FROM aistock_factor_catalog
        ORDER BY factor_name, source, catalog_version, id
    """
    with get_conn() as connection:
        connection.set_session(readonly=True, autocommit=False)
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = [dict(zip(CATALOG_COLUMNS, row, strict=True)) for row in cursor.fetchall()]
        finally:
            connection.rollback()
    snapshot = build_catalog_snapshot(rows)
    _write_immutable_json(Path(output_path), snapshot, "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
    return snapshot


def prepare_generator_request(
    *,
    parent_qe_bundle_path: str | Path,
    parent_overlay_bundle_path: str | Path,
    minute_bundle_path: str | Path,
    catalog_snapshot_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> FrozenAdvisoryQEAlphaGeneratorRequestV1:
    parent = _local_path(parent_qe_bundle_path)
    overlay = _local_path(parent_overlay_bundle_path)
    minute = _local_path(minute_bundle_path)
    catalog_path = _local_path(catalog_snapshot_path)
    parent_request = _read_json(parent / "request.json", "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
    parent_manifest = parent / "manifest.json"
    overlay_manifest = overlay / "manifest.json"
    minute_manifest = minute / "manifest.json"
    old_roster_path = parent / "proposal_roster.json"
    for required in (parent_manifest, overlay_manifest, minute_manifest, old_roster_path, catalog_path):
        if not required.is_file():
            _raise(
                "QE alpha generator evidence is missing",
                "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID",
                path=required.as_posix(),
            )
    catalog = _read_catalog_snapshot(catalog_path)
    roster = _read_json(old_roster_path, "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
    old_proposals = tuple(QEAlphaProposalV1.model_validate(item) for item in roster.get("proposals", ()))
    if len(old_proposals) != 24:
        _raise("QE alpha generator old proposal roster is not exact", "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
    old_source_fields = tuple(sorted({field for item in old_proposals for field in item.source_fields}))
    allowed_fields = _allowed_fields_from_parent_source(parent_request, old_source_fields=old_source_fields)
    model_kwargs = get_llm_kwargs("evolution_researcher")
    model_identity = QEAlphaGeneratorModelIdentityV1(model=str(model_kwargs.get("model", "")))
    repo = Path(repository_root).resolve()
    commit = _git_commit(repo)
    n2b = _local_path(parent_request["n2b_bundle_path"])
    n2b_manifest = _read_json(n2b / "manifest.json", "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
    benchmark = _benchmark_from_parent_request(parent_request)
    refs = (
        evidence_reference_for_file(parent_manifest, role="n3_generator_parent_qe_manifest"),
        evidence_reference_for_file(overlay_manifest, role="n3_generator_parent_overlay_manifest"),
        evidence_reference_for_file(minute_manifest, role="n3_generator_minute_manifest"),
        evidence_reference_for_file(catalog_path, role="n3_generator_catalog_snapshot"),
        evidence_reference_for_file(old_roster_path, role="n3_generator_old_proposal_roster"),
    )
    request = build_generator_request(
        parent_qe_bundle_path=parent.as_posix(),
        parent_overlay_bundle_path=overlay.as_posix(),
        minute_bundle_path=minute.as_posix(),
        catalog_snapshot_path=catalog_path.as_posix(),
        evidence_refs=refs,
        factor_root=str(parent_request["factor_root"]),
        qlib_daily_root=str(parent_request["qlib_daily_root"]),
        n2b_bundle_path=n2b.as_posix(),
        outcomes_path=_local_path(parent_request["outcomes_path"]).as_posix(),
        dataset_identity=str(parent_request["dataset_identity"]),
        policy_identity=str(n2b_manifest["policy_identity"]),
        benchmark_instrument=benchmark,
        old_expression_hashes=tuple(item.expression_sha256 for item in old_proposals),
        old_source_fields=old_source_fields,
        allowed_fields=allowed_fields,
        model_identity=model_identity,
        registry_path=str(parent_request["registry_path"]),
        route_path=str(parent_request["route_path"]),
        repository_root=repo.as_posix(),
        repository_commit=commit,
        output_root=Path(output_root).resolve().as_posix(),
    )
    if catalog["row_count"] <= 0:
        _raise("QE alpha generator catalog snapshot is empty", "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
    _write_immutable_json(
        Path(output_path), request.model_dump(mode="json"), "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID"
    )
    return request


def generate_alpha_candidates(
    request_path: str | Path,
    *,
    llm_call: Callable[[str, str, FrozenAdvisoryQEAlphaGeneratorRequestV1], tuple[str, Mapping[str, Any]]]
    | None = None,
) -> dict[str, Any]:
    request = _load_request(request_path)
    existing = _find_bundle(Path(request.output_root) / "qe_alpha_generation_bundles", request.request_sha256)
    recovery_parent: Path | None = None
    if existing is not None:
        prior = _read_generation_bundle(existing)
        if prior["receipt"].status != "INFRASTRUCTURE_FAILURE" or not _has_generation_recovery_capacity(
            prior["attempts"], request
        ):
            inspected = inspect_generation_bundle(existing)
            return {**inspected, "exact_retry": True, "bundle_path": existing.as_posix()}
        recovery_parent = existing
        old_roster = prior["old_roster"]
        catalog = prior["catalog"]
        accepted = list(prior["proposals"])
        attempts = [dict(item) for item in prior["attempts"]]
        rejected = [dict(item) for item in prior["rejections"]]
        transcript = [dict(item) for item in prior["transcript"]]
    else:
        parent = _local_path(request.parent_qe_bundle_path)
        old_roster = _read_json(parent / "proposal_roster.json", "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
        catalog = _read_catalog_snapshot(_local_path(request.catalog_snapshot_path))
        accepted = []
        attempts = []
        rejected = []
        transcript = []
    old_proposals = tuple(QEAlphaProposalV1.model_validate(item) for item in old_roster["proposals"])
    caller = llm_call or _default_llm_call
    old_fingerprints = {item.proposal_id: expression_fingerprint(item.expression) for item in old_proposals}
    for family in MVE_FAMILIES:
        family_attempts = [item for item in attempts if item.get("family") == family]
        if family_attempts and family_attempts[-1].get("status") != "CALL_FAILED":
            continue
        if len(family_attempts) >= 2 or len(attempts) >= request.max_generation_calls:
            continue
        system_prompt, user_prompt = build_family_prompt(request, family, old_proposals, catalog)
        while len(family_attempts) < 2 and len(attempts) < request.max_generation_calls:
            call_index = len(family_attempts) + 1
            retry_prompt = user_prompt
            if family_attempts and family_attempts[-1].get("status") == "SCHEMA_REJECTED":
                retry_prompt = _schema_retry_prompt(
                    user_prompt,
                    family_attempts[-1].get("violations", ()),
                    request=request,
                )
            started = time.monotonic()
            try:
                raw_response, telemetry = caller(system_prompt, retry_prompt, request)
            except AdvisoryModelFirstError:
                raise
            except Exception as exc:
                call_failure = _attempt_row(
                    family,
                    call_index,
                    "CALL_FAILED",
                    system_prompt,
                    retry_prompt,
                    "",
                    {},
                    0,
                    (_safe_error(exc),),
                    time.monotonic() - started,
                )
                attempts.append(call_failure)
                family_attempts.append(call_failure)
                break
            try:
                raw_items = _parse_generation_response(raw_response)
                parsed = _parse_family_proposals(family, raw_items, allowed_fields=request.allowed_fields)
            except Exception as exc:
                violations = (_safe_error(exc),)
                schema_failure = _attempt_row(
                    family,
                    call_index,
                    "SCHEMA_REJECTED",
                    system_prompt,
                    retry_prompt,
                    raw_response,
                    telemetry,
                    4,
                    violations,
                    time.monotonic() - started,
                )
                attempts.append(schema_failure)
                family_attempts.append(schema_failure)
                transcript.append(_transcript_row(family, call_index, system_prompt, retry_prompt, raw_response))
                rejected.extend(
                    {"family": family, "reason_codes": ["SCHEMA_OR_AST_INVALID"], "attempt_index": call_index}
                    for _ in range(4)
                )
                if len(family_attempts) < 2 and len(attempts) < request.max_generation_calls:
                    continue
                break
            parsed_attempt = _attempt_row(
                family,
                call_index,
                "PARSED",
                system_prompt,
                retry_prompt,
                raw_response,
                telemetry,
                4,
                (),
                time.monotonic() - started,
            )
            attempts.append(parsed_attempt)
            family_attempts.append(parsed_attempt)
            transcript.append(_transcript_row(family, call_index, system_prompt, retry_prompt, raw_response))
            family_proposals: list[QEAlphaGeneratorProposalV1] = []
            for proposal in parsed:
                reasons = preliminary_originality_reasons(
                    proposal,
                    old_proposals=old_proposals,
                    accepted=accepted + family_proposals,
                    old_fingerprints=old_fingerprints,
                    catalog=catalog,
                    old_source_fields=set(request.old_source_fields),
                )
                if reasons:
                    rejected.append(
                        {
                            "family": family,
                            "proposal_id": proposal.proposal_id,
                            "expression_sha256": proposal.expression_sha256,
                            "reason_codes": reasons,
                            "attempt_index": call_index,
                        }
                    )
                else:
                    family_proposals.append(proposal)
            accepted.extend(family_proposals)
            break
    support_reasons = generation_support_reasons(accepted, request)
    unresolved_families = _unresolved_generation_families(attempts)
    if unresolved_families:
        support_reasons = ["LLM_PROVIDER_CALL_FAILURE", *support_reasons]
    raw_attempts = sum(int(item.get("raw_generation_attempt_count") or 0) for item in attempts)
    status = (
        "INFRASTRUCTURE_FAILURE"
        if unresolved_families
        else ("COMPLETE" if not support_reasons else "INCOMPLETE_SUPPORT")
    )
    receipt = build_generation_receipt(
        request_sha256=request.request_sha256,
        generation_call_count=len(attempts),
        raw_generation_attempt_count=raw_attempts,
        accepted_expression_count=len(accepted),
        rejected_expression_count=len(rejected),
        proposals_sha256=canonical_json_sha256([item.model_dump(mode="json") for item in accepted]),
        status=status,
        support_reason_codes=tuple(support_reasons),
    )
    bundle = _publish_generation_bundle(
        request=request,
        catalog=catalog,
        old_roster=old_roster,
        attempts=attempts,
        transcript=transcript,
        accepted=accepted,
        rejected=rejected,
        receipt=receipt,
        recovery_parent=recovery_parent,
    )
    return {
        **inspect_generation_bundle(bundle),
        "exact_retry": False,
        "recovery_attempted": recovery_parent is not None,
        "bundle_path": bundle.as_posix(),
    }


def build_family_prompt(
    request: FrozenAdvisoryQEAlphaGeneratorRequestV1,
    family: str,
    old_proposals: Sequence[QEAlphaProposalV1],
    catalog: Mapping[str, Any],
) -> tuple[str, str]:
    unused = sorted(set(request.allowed_fields) - set(request.old_source_fields))
    catalog_rows = _catalog_prompt_rows(
        catalog,
        family,
        allowed_fields=request.allowed_fields,
        limit=96,
    )
    exclusions = [
        {
            "hypothesis": item.economic_hypothesis,
            "fields": list(item.source_fields),
            "expression": item.expression,
        }
        for item in old_proposals
        if item.family == family
    ]
    system = (
        "You are a bounded A-share alpha hypothesis generator. Return JSON only. "
        "You may design declarative expressions but never Python, code, imports, files, URLs, tools, or execution steps. "
        "No target, label, return, IC, selection result, or performance feedback is available."
    )
    user_payload = {
        "schema_version": request.prompt_schema_version,
        "family": family,
        "required_count": 4,
        "allowed_fields": list(request.allowed_fields),
        "prioritize_unused_fields": unused,
        "allowed_operators": list(request.allowed_operators),
        "known_effect_roster": list(request.known_effects),
        "old_family_exclusions": exclusions,
        "catalog_metadata_without_performance": catalog_rows,
        "constraints": {
            "at_least_one_old_roster_unused_field_per_proposal": True,
            "at_least_two_proposals_use_two_unused_fields": True,
            "window_only_or_name_only_variants_forbidden": True,
            "direction_must_be_economic_prior_not_result_selected": True,
            "known_effect_exposures_count": "1..3",
            "max_nodes": 64,
            "max_depth": 8,
            "max_fields": 8,
        },
        "output_shape": {
            "proposals": [
                {
                    "economic_hypothesis": "string",
                    "mechanism": "string",
                    "known_effect_exposures": ["ONE_OR_MORE_FIXED_VALUES"],
                    "expression": {"op": "ALLOWLIST_OPERATOR", "args": []},
                }
            ]
        },
    }
    if request.prompt_schema_version == GENERATOR_PROMPT_SCHEMA_V2:
        user_payload["operator_contract"] = _operator_parameter_contract(request.allowed_operators)
        user_payload["response_contract"] = {
            "transport": "json_object",
            "top_level_keys": ["proposals"],
            "proposals_count": 4,
            "prose_or_markdown_allowed": False,
        }
    return system, json.dumps(user_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def expression_fingerprint(expression: Mapping[str, Any]) -> Counter[str]:
    tokens: Counter[str] = Counter()

    def visit(node: Any, parent: str = "ROOT") -> None:
        if not isinstance(node, Mapping):
            return
        op = str(node.get("op", ""))
        tokens[f"op:{op}"] += 3
        tokens[f"edge:{parent}>{op}"] += 2
        if "field" in node:
            tokens[f"field:{node['field']}"] += 4
        for key in ("window", "periods"):
            if key in node:
                tokens[f"{key}:{node[key]}"] += 1
        for child in node.get("args", ()):
            visit(child, op)

    visit(expression)
    return tokens


def weighted_jaccard(left: Counter[str], right: Counter[str]) -> float:
    keys = set(left) | set(right)
    denominator = sum(max(left[key], right[key]) for key in keys)
    return float(sum(min(left[key], right[key]) for key in keys) / denominator) if denominator else 1.0


def preliminary_originality_reasons(
    proposal: QEAlphaGeneratorProposalV1,
    *,
    old_proposals: Sequence[QEAlphaProposalV1],
    accepted: Sequence[QEAlphaGeneratorProposalV1],
    old_fingerprints: Mapping[str, Counter[str]],
    catalog: Mapping[str, Any],
    old_source_fields: set[str],
) -> list[str]:
    reasons: list[str] = []
    if proposal.expression_sha256 in {item.expression_sha256 for item in old_proposals}:
        reasons.append("OLD_ROSTER_EXACT_DUPLICATE")
    if proposal.expression_sha256 in {item.expression_sha256 for item in accepted}:
        reasons.append("CURRENT_BATCH_EXACT_DUPLICATE")
    fingerprint = expression_fingerprint(proposal.expression)
    if any(weighted_jaccard(fingerprint, item) >= 0.90 for item in old_fingerprints.values()):
        reasons.append("OLD_ROSTER_STRUCTURAL_DUPLICATE")
    if any(weighted_jaccard(fingerprint, expression_fingerprint(item.expression)) >= 0.90 for item in accepted):
        reasons.append("CURRENT_BATCH_STRUCTURAL_DUPLICATE")
    novel_fields = set(proposal.source_fields) - old_source_fields
    if not novel_fields:
        reasons.append("NO_OLD_ROSTER_UNUSED_FIELD")
    text_key = _normalized_text(proposal.economic_hypothesis)
    catalog_keys = _catalog_text_keys(catalog)
    if text_key and text_key in catalog_keys:
        reasons.append("CATALOG_TEXT_EXACT_DUPLICATE")
    if not proposal.known_effect_exposures:
        reasons.append("KNOWN_EFFECT_DECLARATION_MISSING")
    return sorted(set(reasons))


def run_generator_mve(request_path: str | Path, generation_bundle_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    request = _load_request(request_path)
    generation_path = _local_path(generation_bundle_path)
    generation = _read_generation_bundle(generation_path)
    receipt: AdvisoryQEAlphaGenerationReceiptV1 = generation["receipt"]
    if receipt.request_sha256 != request.request_sha256:
        _raise("QE alpha generation/request identity mismatch", "ADVISORY_QE_ALPHA_GENERATOR_SOURCE_IDENTITY_MISMATCH")
    if receipt.status == "INFRASTRUCTURE_FAILURE":
        _raise(
            "QE alpha generation has unresolved provider call failures",
            "ADVISORY_QE_ALPHA_GENERATOR_LLM_CALL_FAILED",
            support_reason_codes=list(receipt.support_reason_codes),
            generation_bundle_id=generation_path.name,
        )
    if receipt.status != "COMPLETE":
        _raise(
            "QE alpha generation support is insufficient for economic evaluation",
            "ADVISORY_QE_ALPHA_GENERATOR_GENERATION_SUPPORT_INSUFFICIENT",
            support_reason_codes=list(receipt.support_reason_codes),
            generation_bundle_id=generation_path.name,
        )
    existing = _find_bundle(Path(request.output_root) / "qe_alpha_generator_mve_bundles", request.request_sha256)
    if existing is not None:
        delivered = _deliver_result_bundle(request, existing)
        return {
            **inspect_generator_mve_bundle(existing),
            **delivered,
            "exact_retry": True,
            "bundle_path": existing.as_posix(),
        }
    proposals = list(generation["proposals"])
    old_roster_payload = generation["old_roster"]
    old_proposals = tuple(QEAlphaProposalV1.model_validate(item) for item in old_roster_payload["proposals"])
    _verify_request_source_fields(
        request, old_source_fields=tuple(sorted({field for item in old_proposals for field in item.source_fields}))
    )
    target_keys = _load_target_free_parent_keys(request)
    adapter = SimpleNamespace(
        signal_start=request.signal_start,
        signal_end=request.signal_end,
        qlib_daily_root=request.qlib_daily_root,
        factor_root=request.factor_root,
        proposals=tuple([*proposals, *old_proposals]),
    )
    source_panel = build_source_panel(request=adapter, outcomes=target_keys, benchmark=request.benchmark_instrument)
    compiled = compile_proposal_scores(panel=source_panel, proposals=tuple([*proposals, *old_proposals]))
    signal_scores = compiled.loc[
        compiled["datetime"].between(pd.Timestamp(request.signal_start), pd.Timestamp(request.signal_end))
        & compiled["pit_eligible"]
    ].copy()
    signal_scores = signal_scores.merge(
        target_keys[["decision_as_of_trade_date", "instrument", "score"]].rename(
            columns={"decision_as_of_trade_date": "datetime", "score": "parent_score"}
        ),
        on=["datetime", "instrument"],
        how="inner",
        validate="one_to_one",
    )
    originality = target_free_score_overlap(signal_scores, proposals, old_proposals, request)
    accepted_ids = {item["proposal_id"] for item in originality if item["accepted"]}
    proposals = [item for item in proposals if item.proposal_id in accepted_ids]
    _validate_generation_support(proposals, request)
    outcomes = _load_economic_outcomes(request)
    score_columns = ["datetime", "instrument", *[item.proposal_id for item in proposals]]
    economic_panel = outcomes.merge(
        signal_scores[score_columns].rename(columns={"datetime": "decision_as_of_trade_date"}),
        on=["decision_as_of_trade_date", "instrument"],
        how="left",
        validate="one_to_one",
    )
    score_panel, daily_metrics, proposal_summary, stability_report, frontier = evaluate_generated_overlays(
        panel=economic_panel,
        proposals=proposals,
        request=request,
        originality=originality,
    )
    elapsed = time.monotonic() - started
    bundle = _publish_result_bundle(
        request=request,
        generation_bundle=generation_path,
        generation_receipt=receipt,
        originality=originality,
        score_panel=score_panel,
        daily_metrics=daily_metrics,
        proposal_summary=proposal_summary,
        stability_report=stability_report,
        frontier=frontier,
        elapsed_seconds=elapsed,
    )
    delivered = _deliver_result_bundle(request, bundle)
    return {**inspect_generator_mve_bundle(bundle), **delivered, "exact_retry": False, "bundle_path": bundle.as_posix()}


def target_free_score_overlap(
    score_panel: pd.DataFrame,
    proposals: Sequence[QEAlphaGeneratorProposalV1],
    old_proposals: Sequence[QEAlphaProposalV1],
    request: FrozenAdvisoryQEAlphaGeneratorRequestV1,
) -> list[dict[str, Any]]:
    columns = [
        *[item.proposal_id for item in proposals],
        *[item.proposal_id for item in old_proposals],
        "parent_score",
    ]
    sums = pd.DataFrame(0.0, index=columns, columns=columns)
    counts = pd.DataFrame(0, index=columns, columns=columns, dtype="int64")
    for _, frame in score_panel.groupby("datetime", sort=True):
        ranked = frame[columns].apply(pd.to_numeric, errors="coerce").rank(method="average", pct=True)
        correlation = ranked.corr(method="pearson", min_periods=3)
        finite = correlation.notna()
        sums = sums.add(correlation.fillna(0.0), fill_value=0.0)
        counts = counts.add(finite.astype("int64"), fill_value=0).astype("int64")
    means = sums.divide(counts.where(counts > 0))
    rows: list[dict[str, Any]] = []
    for proposal in proposals:
        parent_value = means.loc[proposal.proposal_id, "parent_score"]
        parent_mean = float(parent_value) if np.isfinite(parent_value) else None
        old_means = {
            old.proposal_id: (
                float(means.loc[proposal.proposal_id, old.proposal_id])
                if np.isfinite(means.loc[proposal.proposal_id, old.proposal_id])
                else None
            )
            for old in old_proposals
        }
        finite_old = {key: value for key, value in old_means.items() if value is not None}
        max_old_id = max(finite_old, key=lambda key: abs(float(finite_old[key]))) if finite_old else None
        max_old = float(finite_old[max_old_id]) if max_old_id else None
        reasons: list[str] = []
        if parent_mean is None or abs(parent_mean) >= request.maximum_parent_spearman:
            reasons.append("PARENT_SCORE_OVERLAP_REJECTED")
        if max_old is None or abs(max_old) >= request.maximum_old_score_spearman:
            reasons.append("OLD_SCORE_OVERLAP_REJECTED")
        rows.append(
            {
                "proposal_id": proposal.proposal_id,
                "parent_score_spearman_mean": parent_mean,
                "max_abs_old_score_proposal_id": max_old_id,
                "max_abs_old_score_spearman_mean": max_old,
                "accepted": not reasons,
                "reason_codes": reasons,
                "target_or_economic_metric_accessed": False,
            }
        )
    return rows


def evaluate_generated_overlays(
    *,
    panel: pd.DataFrame,
    proposals: Sequence[QEAlphaGeneratorProposalV1],
    request: FrozenAdvisoryQEAlphaGeneratorRequestV1,
    originality: Sequence[Mapping[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    base = panel.copy()
    base["decision_as_of_trade_date"] = pd.to_datetime(base["decision_as_of_trade_date"]).dt.normalize()
    daily_rows: list[dict[str, Any]] = []
    panel_output = base[
        ["decision_as_of_trade_date", "instrument", "score", "economic_net_excess_bps", "outcome_known"]
    ].copy()
    summaries: list[dict[str, Any]] = []
    stability_rows: list[dict[str, Any]] = []
    originality_by_id = {str(item["proposal_id"]): item for item in originality}
    cumulative_count = request.cumulative_prior_trial_count + len(proposals)
    for proposal_index, proposal in enumerate(proposals):
        overlay_column = f"{proposal.proposal_id}__overlay"
        panel_output[proposal.proposal_id] = pd.to_numeric(base[proposal.proposal_id], errors="coerce")
        panel_output[overlay_column] = np.nan
        previous_overlay_top: set[str] | None = None
        proposal_daily: list[dict[str, Any]] = []
        for decision_date, indices in base.groupby("decision_as_of_trade_date", sort=True).groups.items():
            frame = base.loc[
                indices, ["instrument", "score", "economic_net_excess_bps", "outcome_known", proposal.proposal_id]
            ].copy()
            for column in ("score", "economic_net_excess_bps", proposal.proposal_id):
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            finite = np.isfinite(frame["score"]) & np.isfinite(frame[proposal.proposal_id])
            frame["parent_rank"] = frame["score"].where(finite).rank(method="average", pct=True)
            frame["alpha_rank"] = frame[proposal.proposal_id].where(finite).rank(method="average", pct=True)
            frame["overlay_score"] = (1.0 - request.overlay_weight) * frame[
                "parent_rank"
            ] + request.overlay_weight * frame["alpha_rank"]
            panel_output.loc[indices, overlay_column] = frame["overlay_score"].to_numpy()
            known = frame["outcome_known"].fillna(False).astype(bool) & np.isfinite(frame["economic_net_excess_bps"])
            evaluable = frame.loc[known & np.isfinite(frame["overlay_score"])].copy()
            parent_eval = frame.loc[known & np.isfinite(frame["score"])].copy()
            overlay_pool = frame.loc[np.isfinite(frame["overlay_score"])].copy()
            parent_pool = frame.loc[np.isfinite(frame["score"])].copy()
            overlay_top = overlay_pool.nlargest(5, "overlay_score", keep="first")
            parent_top = parent_pool.nlargest(5, "score", keep="first")
            overlay_ids = set(overlay_top["instrument"].astype(str)) if len(overlay_top) == 5 else set()
            parent_ids = set(parent_top["instrument"].astype(str)) if len(parent_top) == 5 else set()
            intervention = len(overlay_ids) == 5 and len(parent_ids) == 5 and overlay_ids != parent_ids
            churn = (
                np.nan
                if previous_overlay_top is None or len(overlay_ids) != 5
                else 1.0 - len(previous_overlay_top & overlay_ids) / 5.0
            )
            if len(overlay_ids) == 5:
                previous_overlay_top = overlay_ids
            overlay_top_known = (
                len(overlay_top) == 5
                and overlay_top["outcome_known"].fillna(False).astype(bool).all()
                and np.isfinite(overlay_top["economic_net_excess_bps"]).all()
            )
            parent_top_known = (
                len(parent_top) == 5
                and parent_top["outcome_known"].fillna(False).astype(bool).all()
                and np.isfinite(parent_top["economic_net_excess_bps"]).all()
            )
            overlay_top5 = float(overlay_top["economic_net_excess_bps"].mean()) if overlay_top_known else np.nan
            parent_top5 = float(parent_top["economic_net_excess_bps"].mean()) if parent_top_known else np.nan
            overlay_ic = _safe_correlation(
                evaluable["overlay_score"], evaluable["economic_net_excess_bps"], method="spearman"
            )
            parent_ic = _safe_correlation(
                parent_eval["score"], parent_eval["economic_net_excess_bps"], method="spearman"
            )
            row = {
                "proposal_id": proposal.proposal_id,
                "decision_as_of_trade_date": pd.Timestamp(decision_date),
                "row_count": len(frame),
                "finite_score_count": int(finite.sum()),
                "finite_fraction": float(finite.mean()) if len(frame) else 0.0,
                "evaluable_count": len(evaluable),
                "parent_rank_ic": parent_ic,
                "overlay_rank_ic": overlay_ic,
                "rank_ic_delta": overlay_ic - parent_ic,
                "parent_top5_net_excess_bps": parent_top5,
                "overlay_top5_net_excess_bps": overlay_top5,
                "top5_lift_bps": overlay_top5 - parent_top5,
                "intervention": intervention,
                "top5_churn": churn,
            }
            proposal_daily.append(row)
            daily_rows.append(row)
        daily = pd.DataFrame(proposal_daily)
        summary, blocks = _summarize_overlay(
            proposal=proposal,
            daily=daily,
            total_rows=len(base),
            finite_rows=int(np.isfinite(pd.to_numeric(base[proposal.proposal_id], errors="coerce")).sum()),
            request=request,
            cumulative_trial_count=cumulative_count,
            seed=request.bootstrap_seed + proposal_index * 101,
            originality=originality_by_id[proposal.proposal_id],
        )
        summaries.append(summary)
        stability_rows.extend(blocks)
    eligible = [item for item in summaries if item["eligible"]]
    eligible.sort(
        key=lambda item: (
            -float(item["cumulative_familywise_top5_lift_lower_bps"]),
            -float(item["cumulative_familywise_rank_ic_delta_lower"]),
            str(item["proposal_id"]),
        )
    )
    selected = eligible[0]["proposal_id"] if eligible else None
    frontier = {
        "schema_version": "advisory_qe_alpha_generator_frontier_v1",
        "request_sha256": request.request_sha256,
        "selection_rule": "CUMULATIVE_FAMILYWISE_TOP5_THEN_RANKIC_THEN_ID_V1",
        "eligible_proposal_ids": [item["proposal_id"] for item in eligible],
        "selected_proposal_id": selected,
        "selected_trial_count": 1 if selected else 0,
        "candidate_reselection_allowed": False,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    proposal_summary = {
        "schema_version": "advisory_qe_alpha_generator_proposal_summary_v1",
        "request_sha256": request.request_sha256,
        "evaluated_trial_count": len(proposals),
        "cumulative_prior_trial_count": request.cumulative_prior_trial_count,
        "cumulative_trial_count": cumulative_count,
        "proposals": summaries,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
    }
    stability_report = {
        "schema_version": "advisory_qe_alpha_generator_stability_report_v1",
        "request_sha256": request.request_sha256,
        "rows": stability_rows,
        "four_block_rule": "AT_LEAST_THREE_BLOCKS_HAVE_POSITIVE_RANKIC_DELTA_AND_TOP5_LIFT",
    }
    return panel_output, pd.DataFrame(daily_rows), proposal_summary, stability_report, frontier


def _summarize_overlay(
    *,
    proposal: QEAlphaGeneratorProposalV1,
    daily: pd.DataFrame,
    total_rows: int,
    finite_rows: int,
    request: FrozenAdvisoryQEAlphaGeneratorRequestV1,
    cumulative_trial_count: int,
    seed: int,
    originality: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rank_delta = _finite_array(daily["rank_ic_delta"])
    lift = _finite_array(daily["top5_lift_bps"])
    current_rank = _moving_block_interval(
        rank_delta,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed,
        alpha=0.05 / len(MVE_FAMILIES) / 4,
    )
    current_lift = _moving_block_interval(
        lift,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed + 1,
        alpha=0.05 / len(MVE_FAMILIES) / 4,
    )
    cumulative_rank = _moving_block_interval(
        rank_delta,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed,
        alpha=0.05 / cumulative_trial_count,
    )
    cumulative_lift = _moving_block_interval(
        lift,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed + 1,
        alpha=0.05 / cumulative_trial_count,
    )
    dates = tuple(sorted(pd.to_datetime(daily["decision_as_of_trade_date"]).unique()))
    blocks: list[dict[str, Any]] = []
    for block_index, block_dates in enumerate(np.array_split(np.asarray(dates), 4), start=1):
        frame = daily.loc[daily["decision_as_of_trade_date"].isin(block_dates)]
        blocks.append(
            {
                "proposal_id": proposal.proposal_id,
                "block_index": block_index,
                "start_date": pd.Timestamp(block_dates[0]).date().isoformat() if len(block_dates) else None,
                "end_date": pd.Timestamp(block_dates[-1]).date().isoformat() if len(block_dates) else None,
                "day_count": len(frame),
                "rank_ic_delta_mean": _finite_mean(frame["rank_ic_delta"]),
                "top5_lift_mean_bps": _finite_mean(frame["top5_lift_bps"]),
            }
        )
    stable_blocks = sum(
        (item["rank_ic_delta_mean"] or 0) > 0 and (item["top5_lift_mean_bps"] or 0) > 0 for item in blocks
    )
    midpoint = len(daily) // 2
    late = daily.iloc[midpoint:]
    late_rank = _finite_mean(late["rank_ic_delta"])
    late_lift = _finite_mean(late["top5_lift_bps"])
    intervention_days = int(daily["intervention"].fillna(False).astype(bool).sum())
    intervention_dates = pd.to_datetime(daily.loc[daily["intervention"].fillna(False), "decision_as_of_trade_date"])
    intervention_quarters = int(intervention_dates.dt.to_period("Q").nunique()) if len(intervention_dates) else 0
    evaluable_days = min(len(rank_delta), len(lift))
    finite_fraction = float(finite_rows / total_rows) if total_rows else 0.0
    reasons: list[str] = []
    checks = (
        (evaluable_days >= request.minimum_evaluable_days, "EVALUABLE_DAYS_BELOW_MINIMUM"),
        (finite_fraction >= request.minimum_finite_fraction, "FINITE_COVERAGE_BELOW_MINIMUM"),
        (intervention_days >= request.minimum_intervention_days, "INTERVENTION_DAYS_BELOW_MINIMUM"),
        (
            intervention_days / max(len(daily), 1) >= request.minimum_intervention_fraction,
            "INTERVENTION_FRACTION_BELOW_MINIMUM",
        ),
        (intervention_quarters >= request.minimum_intervention_quarters, "INTERVENTION_QUARTERS_BELOW_MINIMUM"),
        (cumulative_rank[0] is not None and cumulative_rank[0] > 0, "CUMULATIVE_FAMILYWISE_RANKIC_LOWER_NOT_POSITIVE"),
        (
            cumulative_lift[0] is not None and cumulative_lift[0] > 0,
            "CUMULATIVE_FAMILYWISE_TOP5_LIFT_LOWER_NOT_POSITIVE",
        ),
        (late_rank is not None and late_rank > 0, "LATE_HALF_RANKIC_DELTA_NOT_POSITIVE"),
        (late_lift is not None and late_lift > 0, "LATE_HALF_TOP5_LIFT_NOT_POSITIVE"),
        (stable_blocks >= 3, "FOUR_BLOCK_STABILITY_BELOW_MINIMUM"),
    )
    reasons.extend(reason for passed, reason in checks if not passed)
    dsr = _deflated_sharpe_diagnostic(lift, trial_count=cumulative_trial_count)
    return (
        {
            "proposal_id": proposal.proposal_id,
            "family": proposal.family,
            "economic_hypothesis": proposal.economic_hypothesis,
            "mechanism": proposal.mechanism,
            "known_effect_exposures": list(proposal.known_effect_exposures),
            "expression_sha256": proposal.expression_sha256,
            "source_fields": list(proposal.source_fields),
            "finite_fraction": finite_fraction,
            "evaluable_day_count": evaluable_days,
            "rank_ic_delta_mean": _finite_mean(rank_delta),
            "top5_lift_mean_bps": _finite_mean(lift),
            "current_familywise_rank_ic_delta_lower": current_rank[0],
            "current_familywise_rank_ic_delta_upper": current_rank[1],
            "current_familywise_top5_lift_lower_bps": current_lift[0],
            "current_familywise_top5_lift_upper_bps": current_lift[1],
            "cumulative_familywise_rank_ic_delta_lower": cumulative_rank[0],
            "cumulative_familywise_rank_ic_delta_upper": cumulative_rank[1],
            "cumulative_familywise_top5_lift_lower_bps": cumulative_lift[0],
            "cumulative_familywise_top5_lift_upper_bps": cumulative_lift[1],
            "late_half_rank_ic_delta_mean": late_rank,
            "late_half_top5_lift_mean_bps": late_lift,
            "positive_joint_time_block_count": stable_blocks,
            "intervention_day_count": intervention_days,
            "intervention_day_fraction": intervention_days / max(len(daily), 1),
            "intervention_quarter_count": intervention_quarters,
            "top5_churn_mean": _finite_mean(daily["top5_churn"]),
            "parent_score_spearman_mean": originality["parent_score_spearman_mean"],
            "max_abs_old_score_spearman_mean": originality["max_abs_old_score_spearman_mean"],
            "daily_lift_sharpe": dsr["observed_sharpe"],
            "daily_lift_skew": dsr["skew"],
            "daily_lift_kurtosis": dsr["kurtosis"],
            "deflated_sharpe_probability": dsr["deflated_sharpe_probability"],
            "eligible": not reasons,
            "reason_codes": reasons,
            "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        },
        blocks,
    )


def inspect_generation_bundle(path: str | Path) -> dict[str, Any]:
    bundle = _read_generation_bundle(_local_path(path))
    receipt: AdvisoryQEAlphaGenerationReceiptV1 = bundle["receipt"]
    failed_calls = [item for item in bundle["attempts"] if item.get("status") == "CALL_FAILED"]
    unresolved_families = _unresolved_generation_families(bundle["attempts"])
    return {
        "status": "VALID",
        "bundle_id": bundle["manifest"]["bundle_id"],
        "request_sha256": receipt.request_sha256,
        "generation_call_count": receipt.generation_call_count,
        "raw_generation_attempt_count": receipt.raw_generation_attempt_count,
        "accepted_expression_count": receipt.accepted_expression_count,
        "rejected_expression_count": receipt.rejected_expression_count,
        "generation_status": receipt.status,
        "failed_call_count": len(failed_calls),
        "unresolved_failed_family_count": len(unresolved_families),
        "unresolved_failed_families": unresolved_families,
        "recovery_parent_bundle_id": (
            bundle["recovery_parent"].get("parent_bundle_id") if bundle["recovery_parent"] else None
        ),
        "support_reason_codes": list(receipt.support_reason_codes),
        "target_or_economic_metric_exposed": False,
        "secret_persisted": False,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }


def inspect_generator_mve_bundle(path: str | Path) -> dict[str, Any]:
    bundle = _read_result_bundle(_local_path(path))
    receipt = bundle["receipt"]
    return {
        "status": "VALID",
        "bundle_id": bundle["manifest"]["bundle_id"],
        "receipt_id": receipt.receipt_id,
        "request_sha256": receipt.request_sha256,
        "generated_trial_count": receipt.generated_trial_count,
        "evaluated_trial_count": receipt.evaluated_trial_count,
        "selected_trial_count": receipt.selected_trial_count,
        "selected_proposal_id": receipt.selected_proposal_id,
        "eligible_proposal_ids": list(receipt.eligible_proposal_ids),
        "next_task": receipt.next_task,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "runtime_eligible": False,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }


def _default_llm_call(
    system_prompt: str,
    user_prompt: str,
    request: FrozenAdvisoryQEAlphaGeneratorRequestV1,
) -> tuple[str, Mapping[str, Any]]:
    import litellm

    kwargs = get_llm_kwargs(request.model_identity.agent_locator)
    if str(kwargs.get("model", "")) != request.model_identity.model:
        _raise("QE alpha generator model identity drift", "ADVISORY_QE_ALPHA_GENERATOR_MODEL_IDENTITY_MISMATCH")
    completion_kwargs = dict(kwargs)
    response_format = None
    if request.prompt_schema_version == GENERATOR_PROMPT_SCHEMA_V2:
        response_format = {"type": "json_object"}
        configured_response_format = completion_kwargs.get("response_format")
        if configured_response_format not in (None, response_format):
            _raise(
                "QE alpha generator response format drift",
                "ADVISORY_QE_ALPHA_GENERATOR_MODEL_IDENTITY_MISMATCH",
            )
        completion_kwargs["response_format"] = response_format
    response = litellm.completion(
        messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        temperature=request.model_identity.temperature,
        top_p=request.model_identity.top_p,
        timeout=request.model_identity.timeout_seconds,
        **completion_kwargs,
    )
    content = str(response.choices[0].message.content or "")
    usage = getattr(response, "usage", None)
    telemetry = {
        "model": request.model_identity.model,
        "prompt_tokens": getattr(usage, "prompt_tokens", None),
        "completion_tokens": getattr(usage, "completion_tokens", None),
        "total_tokens": getattr(usage, "total_tokens", None),
        "response_format": "json_object" if response_format else "provider_default",
    }
    return content, telemetry


def _operator_parameter_contract(allowed_operators: Sequence[str]) -> list[dict[str, Any]]:
    contract = [
        {
            "operators": ["FIELD"],
            "required_keys": ["op", "field"],
            "args_count": 0,
            "parameter_rules": {"field": "one exact value from allowed_fields"},
        },
        {
            "operators": ["CONST"],
            "required_keys": ["op", "value"],
            "args_count": 0,
            "parameter_rules": {"value": "finite JSON number"},
        },
        {
            "operators": ["ADD", "SUBTRACT", "MULTIPLY", "SAFE_DIVIDE"],
            "required_keys": ["op", "args"],
            "args_count": 2,
            "parameter_rules": {},
        },
        {
            "operators": ["ABS", "SIGN", "LOG1P_ABS", "SQRT_ABS", "SAME_DATE_RANK", "SAME_DATE_ZSCORE"],
            "required_keys": ["op", "args"],
            "args_count": 1,
            "parameter_rules": {},
        },
        {
            "operators": ["LAG", "DELTA"],
            "required_keys": ["op", "args", "periods"],
            "args_count": 1,
            "parameter_rules": {"periods": "integer 1..252 inclusive"},
        },
        {
            "operators": ["TRAILING_SUM", "TRAILING_MEAN", "TRAILING_STD", "TRAILING_MIN", "TRAILING_MAX"],
            "required_keys": ["op", "args", "window"],
            "args_count": 1,
            "parameter_rules": {"window": "integer 2..252 inclusive"},
        },
        {
            "operators": ["CLIP"],
            "required_keys": ["op", "args", "lower", "upper"],
            "args_count": 1,
            "parameter_rules": {"lower": "finite JSON number", "upper": "finite JSON number greater than lower"},
        },
    ]
    declared = [operator for item in contract for operator in item["operators"]]
    if len(declared) != len(set(declared)) or set(declared) != set(allowed_operators):
        raise ValueError("QE alpha generator prompt operator contract drift")
    return contract


def _schema_retry_prompt(
    user_prompt: str,
    violations: Sequence[str],
    *,
    request: FrozenAdvisoryQEAlphaGeneratorRequestV1,
) -> str:
    if request.prompt_schema_version != GENERATOR_PROMPT_SCHEMA_V2:
        return user_prompt + "\n\nSchema-only retry violations:\n" + "\n".join(f"- {item}" for item in violations)
    payload = json.loads(user_prompt)
    payload["schema_only_retry"] = {
        "violations": list(violations),
        "instruction": (
            "Return one fresh complete JSON object satisfying the unchanged output, operator, field, and budget contracts."
        ),
        "economic_feedback_included": False,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _parse_generation_response(raw: str) -> Sequence[Mapping[str, Any]]:
    start, end = raw.find("{"), raw.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("response contains no JSON object")
    payload = json.loads(raw[start : end + 1])
    if set(payload) != {"proposals"} or not isinstance(payload["proposals"], list) or len(payload["proposals"]) != 4:
        raise ValueError("response must contain exactly four proposals")
    return payload["proposals"]


def _parse_family_proposals(
    family: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    allowed_fields: Sequence[str],
) -> list[QEAlphaGeneratorProposalV1]:
    output: list[QEAlphaGeneratorProposalV1] = []
    frozen_allowed_fields = set(allowed_fields)
    for index, row in enumerate(rows, start=1):
        if set(row) != {"economic_hypothesis", "mechanism", "known_effect_exposures", "expression"}:
            raise ValueError("proposal response fields drift")
        proposal = build_generator_proposal(
            proposal_id=f"N3G_{family}_{index:02d}",
            family=family,
            economic_hypothesis=row["economic_hypothesis"],
            mechanism=row["mechanism"],
            known_effect_exposures=tuple(row["known_effect_exposures"]),
            expression=row["expression"],
        )
        if not set(proposal.source_fields).issubset(frozen_allowed_fields):
            raise ValueError("proposal uses a field outside the frozen request source schema")
        output.append(proposal)
    return output


def generation_support_reasons(
    proposals: Sequence[QEAlphaGeneratorProposalV1], request: FrozenAdvisoryQEAlphaGeneratorRequestV1
) -> list[str]:
    reasons: list[str] = []
    if len(proposals) < request.minimum_accepted_expressions or len(proposals) > request.max_evaluated_expressions:
        reasons.append("TOTAL_ACCEPTED_EXPRESSION_SUPPORT_INSUFFICIENT")
    old = set(request.old_source_fields)
    for family in MVE_FAMILIES:
        family_rows = [item for item in proposals if item.family == family]
        if len(family_rows) < request.minimum_per_family:
            reasons.append(f"{family}_ACCEPTED_SUPPORT_INSUFFICIENT")
        if sum(len(set(item.source_fields) - old) >= 2 for item in family_rows) < 2:
            reasons.append(f"{family}_MULTI_NEW_FIELD_SUPPORT_INSUFFICIENT")
    return sorted(set(reasons))


def _validate_generation_support(
    proposals: Sequence[QEAlphaGeneratorProposalV1], request: FrozenAdvisoryQEAlphaGeneratorRequestV1
) -> None:
    reasons = generation_support_reasons(proposals, request)
    if reasons:
        _raise(
            "QE alpha generator accepted expression support is insufficient",
            "ADVISORY_QE_ALPHA_GENERATOR_GENERATION_SUPPORT_INSUFFICIENT",
            accepted=len(proposals),
            support_reason_codes=reasons,
        )


def _publish_generation_bundle(
    *,
    request: FrozenAdvisoryQEAlphaGeneratorRequestV1,
    catalog: Mapping[str, Any],
    old_roster: Mapping[str, Any],
    attempts: Sequence[Mapping[str, Any]],
    transcript: Sequence[Mapping[str, Any]],
    accepted: Sequence[QEAlphaGeneratorProposalV1],
    rejected: Sequence[Mapping[str, Any]],
    receipt: AdvisoryQEAlphaGenerationReceiptV1,
    recovery_parent: Path | None = None,
) -> Path:
    root = Path(request.output_root) / "qe_alpha_generation_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".qe-alpha-generation-", dir=root))
    try:
        _write_json(temporary / "request.json", request.model_dump(mode="json"))
        _write_json(temporary / "catalog_snapshot.json", catalog)
        _write_json(temporary / "old_proposal_roster.json", old_roster)
        _write_json(temporary / "attempts.json", {"attempts": list(attempts)})
        _write_json(temporary / "transcript.json", {"transcript": list(transcript), "secret_persisted": False})
        _write_json(
            temporary / "proposal_roster.json", {"proposals": [item.model_dump(mode="json") for item in accepted]}
        )
        _write_json(temporary / "rejection_ledger.json", {"rejections": list(rejected)})
        _write_json(temporary / "generation_receipt.json", receipt.model_dump(mode="json"))
        if recovery_parent is not None:
            parent_manifest = recovery_parent / "manifest.json"
            _write_json(
                temporary / "recovery_parent.json",
                {
                    "schema_version": "advisory_qe_alpha_generation_recovery_parent_v1",
                    "parent_bundle_id": recovery_parent.name,
                    "parent_manifest_sha256": hashlib.sha256(parent_manifest.read_bytes()).hexdigest(),
                },
            )
        members = _member_descriptors(temporary)
        bundle_id = canonical_json_sha256(
            {"schema_version": GENERATION_BUNDLE_SCHEMA, "request_sha256": request.request_sha256, "members": members}
        )
        manifest = {
            "schema_version": GENERATION_BUNDLE_SCHEMA,
            "bundle_id": bundle_id,
            "request_sha256": request.request_sha256,
            "members": members,
            "target_or_economic_metric_exposed": False,
            "secret_persisted": False,
            "sealed_holdout_accessed": False,
            "deployable": False,
        }
        _write_json(temporary / "manifest.json", manifest)
        target = root / bundle_id
        if target.exists():
            shutil.rmtree(temporary)
            inspect_generation_bundle(target)
            return target
        os.replace(temporary, target)
        inspect_generation_bundle(target)
        return target
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise


def _publish_result_bundle(
    *,
    request: FrozenAdvisoryQEAlphaGeneratorRequestV1,
    generation_bundle: Path,
    generation_receipt: AdvisoryQEAlphaGenerationReceiptV1,
    originality: Sequence[Mapping[str, Any]],
    score_panel: pd.DataFrame,
    daily_metrics: pd.DataFrame,
    proposal_summary: Mapping[str, Any],
    stability_report: Mapping[str, Any],
    frontier: Mapping[str, Any],
    elapsed_seconds: float,
) -> Path:
    root = Path(request.output_root) / "qe_alpha_generator_mve_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".qe-alpha-generator-mve-", dir=root))
    try:
        _write_json(temporary / "request.json", request.model_dump(mode="json"))
        generation_manifest = generation_bundle / "manifest.json"
        _write_json(
            temporary / "generation_reference.json",
            evidence_reference_for_file(generation_manifest, role="n3_qe_alpha_generation_manifest").model_dump(
                mode="json"
            ),
        )
        _write_json(
            temporary / "target_free_originality.json",
            {"rows": list(originality), "target_or_economic_metric_accessed": False},
        )
        score_panel.to_parquet(temporary / "score_panel.parquet", index=False)
        daily_metrics.to_parquet(temporary / "daily_metrics.parquet", index=False)
        _write_json(temporary / "proposal_summary.json", proposal_summary)
        _write_json(temporary / "stability_report.json", stability_report)
        _write_json(temporary / "frontier_receipt.json", frontier)
        resource = {
            "schema_version": "advisory_qe_alpha_generator_resource_report_v1",
            "elapsed_seconds": elapsed_seconds,
            "peak_rss_bytes": _peak_rss_bytes(),
            "temp_bytes": _directory_size(temporary),
            "max_rss_bytes": request.resource_max_rss_bytes,
            "max_temp_bytes": request.resource_max_temp_bytes,
            "wall_time_gate": None,
        }
        if resource["peak_rss_bytes"] > request.resource_max_rss_bytes:
            _raise(
                "QE alpha generator RSS limit exceeded",
                "ADVISORY_QE_ALPHA_GENERATOR_RESOURCE_LIMIT_EXCEEDED",
                peak_rss_bytes=resource["peak_rss_bytes"],
            )
        _write_json(temporary / "resource_report.json", resource)
        result_names = (
            "generation_reference.json",
            "target_free_originality.json",
            "score_panel.parquet",
            "daily_metrics.parquet",
            "proposal_summary.json",
            "stability_report.json",
            "frontier_receipt.json",
            "resource_report.json",
        )
        result_hash = canonical_json_sha256({name: _file_descriptor(temporary / name) for name in result_names})
        source_identity = canonical_json_sha256(
            {
                "dataset_identity": request.dataset_identity,
                "policy_identity": request.policy_identity,
                "outcomes": _file_descriptor(_local_path(request.outcomes_path)),
                "generation_bundle_id": generation_bundle.name,
            }
        )
        selected = frontier.get("selected_proposal_id")
        eligible = tuple(str(item) for item in frontier.get("eligible_proposal_ids", ()))
        receipt = build_generator_mve_receipt(
            request_sha256=request.request_sha256,
            generation_receipt_sha256=generation_receipt.receipt_sha256,
            generated_trial_count=generation_receipt.raw_generation_attempt_count,
            evaluated_trial_count=int(proposal_summary["evaluated_trial_count"]),
            selected_trial_count=1 if selected else 0,
            selected_proposal_id=selected,
            eligible_proposal_ids=eligible,
            next_task=(
                "N3_QE_ALPHA_GENERATOR_CANDIDATE_CONFIRMATION_DESIGN"
                if selected
                else "N3_UPSTREAM_ALPHA_NEW_DATA_SOURCE_MVE_DESIGN"
            ),
            source_identity_sha256=source_identity,
            result_files_sha256=result_hash,
            resource_report_sha256=_sha256_file(temporary / "resource_report.json"),
        )
        _write_json(temporary / "receipt.json", receipt.model_dump(mode="json"))
        generation_ref = evidence_reference_for_file(generation_manifest, role="n3_qe_alpha_generation_manifest")
        parent_ref = next(item for item in request.evidence_refs if item.role == "n3_generator_parent_qe_manifest")
        record = build_trial_record(
            experiment_id=GENERATOR_EXPERIMENT_ID,
            attempt_id=request.request_id,
            research_stage="N3_QE_ALPHA_GENERATOR_MVE",
            study_type=ResearchStudyType.EXPLORATORY_SCREEN,
            hypothesis_family_id=GENERATOR_FAMILY_ID,
            parent_lineage=("ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1", "ADVISORY-N3-PARENT-INCREMENTAL-OVERLAY-V1"),
            unique_variable="AUTOMATED_TARGET_FREE_LLM_DECLARATIVE_AST_GENERATION",
            objective_contract=ObjectiveContract.ALPHA_RANKING,
            dataset_identity=request.dataset_identity,
            schema_identity=canonical_json_sha256(
                {"fields": list(request.allowed_fields), "operators": list(request.allowed_operators)}
            ),
            policy_identity=request.policy_identity,
            planned_trial_count=request.max_raw_generation_attempts,
            generated_trial_count=receipt.generated_trial_count,
            evaluated_trial_count=receipt.evaluated_trial_count,
            selected_trial_count=receipt.selected_trial_count,
            consumed_windows=(
                ConsumedWindowV1(
                    window_id="P0_C_DEVELOPMENT_CONSUMED",
                    dataset_identity=request.dataset_identity,
                    start_date=request.signal_start,
                    end_date=request.signal_end,
                ),
            ),
            result_class=ResearchResultClass.EXPLORATORY,
            decision_use=DecisionUse.NAVIGATION_ONLY,
            evidence_refs=(generation_ref, parent_ref),
        )
        _write_json(temporary / "registry_record.json", record.model_dump(mode="json"))
        final_temp_bytes = _directory_size(temporary)
        if final_temp_bytes > request.resource_max_temp_bytes:
            _raise(
                "QE alpha generator temp limit exceeded",
                "ADVISORY_QE_ALPHA_GENERATOR_RESOURCE_LIMIT_EXCEEDED",
                temp_bytes=final_temp_bytes,
            )
        members = _member_descriptors(temporary)
        bundle_id = canonical_json_sha256(
            {"schema_version": MVE_BUNDLE_SCHEMA, "request_sha256": request.request_sha256, "members": members}
        )
        manifest = {
            "schema_version": MVE_BUNDLE_SCHEMA,
            "bundle_id": bundle_id,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
            "members": members,
            "factor_catalog_written": False,
            "strategy_package_written": False,
            "runtime_eligible": False,
            "sealed_holdout_accessed": False,
            "deployable": False,
        }
        _write_json(temporary / "manifest.json", manifest)
        target = root / bundle_id
        if target.exists():
            shutil.rmtree(temporary)
            inspect_generator_mve_bundle(target)
            return target
        os.replace(temporary, target)
        inspect_generator_mve_bundle(target)
        return target
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise


def _deliver_result_bundle(request: FrozenAdvisoryQEAlphaGeneratorRequestV1, bundle: Path) -> dict[str, Any]:
    data = _read_result_bundle(bundle)
    registry = AdvisoryResearchTrialRegistryV1(_local_path(request.registry_path))
    registry_result = registry.append_batch((data["registry_record"],))
    receipt = data["receipt"]
    route_payload = {
        "schema_version": "advisory_current_research_route_v1",
        "active_main_line": "N3_QE_ALPHA_GENERATOR_MVE",
        "active_auxiliary_line": "NONE",
        "next_task": receipt.next_task,
        "exploratory_candidate": receipt.selected_proposal_id or "NONE",
        "bundle_id": bundle.name,
        "request_sha256": request.request_sha256,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
    }
    route_payload["route_sha256"] = canonical_json_sha256(route_payload)
    route = _local_path(request.route_path)
    route.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(
        ["# Advisory current research route", "", *[f"- {key}: `{value}`" for key, value in route_payload.items()], ""]
    )
    _write_atomic_text(route, text)
    return {"registry": registry_result, "route": route_payload}


def _read_generation_bundle(path: Path) -> dict[str, Any]:
    manifest = _verify_manifest(path, GENERATION_BUNDLE_SCHEMA)
    request = FrozenAdvisoryQEAlphaGeneratorRequestV1.model_validate(
        _read_json(path / "request.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    )
    receipt = AdvisoryQEAlphaGenerationReceiptV1.model_validate(
        _read_json(path / "generation_receipt.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    )
    roster = _read_json(path / "proposal_roster.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    proposals = tuple(QEAlphaGeneratorProposalV1.model_validate(item) for item in roster.get("proposals", ()))
    attempts_payload = _read_json(path / "attempts.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    transcript_payload = _read_json(path / "transcript.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    rejection_payload = _read_json(path / "rejection_ledger.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    attempts = attempts_payload.get("attempts")
    transcript = transcript_payload.get("transcript")
    rejections = rejection_payload.get("rejections")
    if not isinstance(attempts, list) or not isinstance(transcript, list) or not isinstance(rejections, list):
        _raise("QE alpha generation ledger shape is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    attempt_keys: set[tuple[str, int]] = set()
    transcript_keys: set[tuple[str, int]] = set()
    for item in attempts:
        if not isinstance(item, dict):
            _raise("QE alpha generation attempt row is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
        family = str(item.get("family") or "")
        attempt_index = int(item.get("attempt_index") or 0)
        status = str(item.get("status") or "")
        raw_count = int(item.get("raw_generation_attempt_count") or 0)
        key = (family, attempt_index)
        if (
            family not in MVE_FAMILIES
            or attempt_index not in (1, 2)
            or status not in {"CALL_FAILED", "SCHEMA_REJECTED", "PARSED"}
            or key in attempt_keys
            or raw_count != (0 if status == "CALL_FAILED" else 4)
            or (status == "CALL_FAILED") != (item.get("response_sha256") is None)
        ):
            _raise("QE alpha generation attempt relation is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
        attempt_keys.add(key)
    for item in transcript:
        if not isinstance(item, dict):
            _raise("QE alpha generation transcript row is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
        key = (str(item.get("family") or ""), int(item.get("attempt_index") or 0))
        if key in transcript_keys or key not in attempt_keys or not isinstance(item.get("response_sha256"), str):
            _raise("QE alpha generation transcript relation is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
        matching = next(row for row in attempts if (row.get("family"), row.get("attempt_index")) == key)
        if matching.get("status") == "CALL_FAILED" or matching.get("response_sha256") != item.get("response_sha256"):
            _raise("QE alpha generation transcript identity is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
        transcript_keys.add(key)
    response_attempt_keys = {
        (str(item.get("family")), int(item.get("attempt_index")))
        for item in attempts
        if item.get("status") != "CALL_FAILED"
    }
    unresolved_families = _unresolved_generation_families(attempts)
    recovery_parent = None
    recovery_path = path / "recovery_parent.json"
    if recovery_path.is_file():
        recovery_parent = _read_json(recovery_path, "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
        if (
            set(recovery_parent) != {"schema_version", "parent_bundle_id", "parent_manifest_sha256"}
            or recovery_parent.get("schema_version") != "advisory_qe_alpha_generation_recovery_parent_v1"
            or not re.fullmatch(SHA256_PATTERN, str(recovery_parent.get("parent_bundle_id") or ""))
            or not re.fullmatch(SHA256_PATTERN, str(recovery_parent.get("parent_manifest_sha256") or ""))
        ):
            _raise("QE alpha generation recovery reference is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
        parent_path = path.parent / str(recovery_parent["parent_bundle_id"])
        parent_manifest_path = parent_path / "manifest.json"
        parent_receipt_path = parent_path / "generation_receipt.json"
        if (
            not parent_manifest_path.is_file()
            or hashlib.sha256(parent_manifest_path.read_bytes()).hexdigest()
            != recovery_parent["parent_manifest_sha256"]
        ):
            _raise("QE alpha generation recovery parent is unavailable", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
        parent_manifest = _read_json(parent_manifest_path, "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
        parent_receipt = AdvisoryQEAlphaGenerationReceiptV1.model_validate(
            _read_json(parent_receipt_path, "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
        )
        parent_attempts = _read_json(parent_path / "attempts.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID").get(
            "attempts"
        )
        parent_transcript = _read_json(
            parent_path / "transcript.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID"
        ).get("transcript")
        parent_rejections = _read_json(
            parent_path / "rejection_ledger.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID"
        ).get("rejections")
        parent_roster = _read_json(
            parent_path / "proposal_roster.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID"
        ).get("proposals")
        current_roster = roster.get("proposals")
        if not all(
            isinstance(item, list)
            for item in (parent_attempts, parent_transcript, parent_rejections, parent_roster, current_roster)
        ):
            _raise(
                "QE alpha generation recovery parent ledger is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID"
            )
        new_attempts = attempts[len(parent_attempts) :]
        recoverable_families = set(_unresolved_generation_families(parent_attempts))
        if (
            parent_manifest.get("request_sha256") != request.request_sha256
            or parent_receipt.request_sha256 != request.request_sha256
            or parent_receipt.status != "INFRASTRUCTURE_FAILURE"
            or parent_receipt.generation_call_count >= receipt.generation_call_count
            or attempts[: len(parent_attempts)] != parent_attempts
            or transcript[: len(parent_transcript)] != parent_transcript
            or rejections[: len(parent_rejections)] != parent_rejections
            or current_roster[: len(parent_roster)] != parent_roster
            or any(str(item.get("family") or "") not in recoverable_families for item in new_attempts)
        ):
            _raise("QE alpha generation recovery lineage is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    proposals_sha256 = canonical_json_sha256([item.model_dump(mode="json") for item in proposals])
    if (
        receipt.request_sha256 != request.request_sha256
        or receipt.accepted_expression_count != len(proposals)
        or receipt.proposals_sha256 != proposals_sha256
        or receipt.generation_call_count != len(attempts)
        or receipt.raw_generation_attempt_count
        != sum(int(item.get("raw_generation_attempt_count") or 0) for item in attempts)
        or receipt.rejected_expression_count != len(rejections)
        or response_attempt_keys != transcript_keys
        or any(not set(item.source_fields).issubset(request.allowed_fields) for item in proposals)
        or (receipt.status == "INFRASTRUCTURE_FAILURE") != bool(unresolved_families)
        or manifest.get("target_or_economic_metric_exposed") is not False
        or manifest.get("secret_persisted") is not False
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("deployable") is not False
    ):
        _raise(
            "QE alpha generation bundle relational identity is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID"
        )
    return {
        "manifest": manifest,
        "request": request,
        "receipt": receipt,
        "proposals": proposals,
        "catalog": _read_catalog_snapshot(path / "catalog_snapshot.json"),
        "attempts": tuple(attempts),
        "transcript": tuple(transcript),
        "rejections": tuple(rejections),
        "recovery_parent": recovery_parent,
        "old_roster": _read_json(path / "old_proposal_roster.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID"),
    }


def _read_result_bundle(path: Path) -> dict[str, Any]:
    manifest = _verify_manifest(path, MVE_BUNDLE_SCHEMA)
    request = FrozenAdvisoryQEAlphaGeneratorRequestV1.model_validate(
        _read_json(path / "request.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    )
    receipt_payload = _read_json(path / "receipt.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    from backend.services.advisory_model_first.qe_alpha_generator_contracts import AdvisoryQEAlphaGeneratorMVEReceiptV1

    receipt = AdvisoryQEAlphaGeneratorMVEReceiptV1.model_validate(receipt_payload)
    record_payload = _read_json(path / "registry_record.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    from backend.services.advisory_model_first.research_control_contracts import AdvisoryResearchTrialRecordV1

    record = AdvisoryResearchTrialRecordV1.model_validate(record_payload)
    frontier = _read_json(path / "frontier_receipt.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    if (
        manifest["receipt_sha256"] != receipt.receipt_sha256
        or receipt.request_sha256 != request.request_sha256
        or record.attempt_id != request.request_id
        or record.generated_trial_count != receipt.generated_trial_count
        or record.evaluated_trial_count != receipt.evaluated_trial_count
        or record.selected_trial_count != receipt.selected_trial_count
        or frontier.get("selected_proposal_id") != receipt.selected_proposal_id
        or tuple(frontier.get("eligible_proposal_ids", ())) != receipt.eligible_proposal_ids
        or manifest.get("factor_catalog_written") is not False
        or manifest.get("strategy_package_written") is not False
        or manifest.get("runtime_eligible") is not False
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("deployable") is not False
    ):
        _raise(
            "QE alpha generator MVE bundle relational identity is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID"
        )
    return {"manifest": manifest, "request": request, "receipt": receipt, "registry_record": record}


def _verify_manifest(path: Path, expected_schema: str) -> dict[str, Any]:
    manifest = _read_json(path / "manifest.json", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    if (
        manifest.get("schema_version") != expected_schema
        or manifest.get("bundle_id") != path.name
        or not isinstance(manifest.get("request_sha256"), str)
        or not isinstance(manifest.get("members"), dict)
    ):
        _raise("QE alpha generator bundle identity is invalid", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    expected = manifest.get("members")
    actual = _member_descriptors(path, exclude={"manifest.json"})
    if expected != actual:
        _raise("QE alpha generator bundle member identity drift", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    digest = canonical_json_sha256(
        {"schema_version": expected_schema, "request_sha256": manifest["request_sha256"], "members": actual}
    )
    if digest != path.name:
        _raise("QE alpha generator bundle content address mismatch", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    return manifest


def _find_bundle(root: Path, request_sha256: str) -> Path | None:
    if not root.is_dir():
        return None
    matches: list[Path] = []
    for child in root.iterdir():
        if not child.is_dir() or child.name.startswith(".") or not (child / "manifest.json").is_file():
            continue
        try:
            manifest = json.loads((child / "manifest.json").read_text(encoding="utf-8"))
        except Exception:
            continue
        if manifest.get("request_sha256") == request_sha256:
            matches.append(child)
    if len(matches) > 1 and root.name == "qe_alpha_generation_bundles":
        ranked = [(_read_generation_bundle(item)["receipt"].generation_call_count, item) for item in matches]
        max_call_count = max(item[0] for item in ranked)
        matches = [item[1] for item in ranked if item[0] == max_call_count]
    if len(matches) > 1:
        _raise("QE alpha generator request maps to ambiguous bundles", "ADVISORY_QE_ALPHA_GENERATOR_BUNDLE_INVALID")
    return matches[0] if matches else None


def _load_target_free_parent_keys(request: FrozenAdvisoryQEAlphaGeneratorRequestV1) -> pd.DataFrame:
    columns = ["arm_id", "decision_as_of_trade_date", "instrument", "score"]
    try:
        frame = pd.read_parquet(
            _local_path(request.outcomes_path), columns=columns, filters=[("arm_id", "=", CURRENT_PARENT_ARM_ID)]
        )
    except Exception as exc:
        _raise(
            "QE alpha generator target-free parent keys cannot be projected",
            "ADVISORY_QE_ALPHA_GENERATOR_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    frame = frame.loc[frame["arm_id"].astype(str) == CURRENT_PARENT_ARM_ID].copy()
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str).str.upper()
    frame = frame.loc[
        frame["decision_as_of_trade_date"].between(pd.Timestamp(request.signal_start), pd.Timestamp(request.signal_end))
    ]
    if frame.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise("QE alpha generator target-free keys are duplicated", "ADVISORY_QE_ALPHA_GENERATOR_PIT_LEAKAGE")
    return frame.sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True)


def _load_economic_outcomes(request: FrozenAdvisoryQEAlphaGeneratorRequestV1) -> pd.DataFrame:
    columns = ["arm_id", "decision_as_of_trade_date", "instrument", "score", "economic_net_excess_bps", "outcome_known"]
    try:
        frame = pd.read_parquet(
            _local_path(request.outcomes_path),
            columns=columns,
            filters=[("arm_id", "=", CURRENT_PARENT_ARM_ID)],
        )
    except Exception as exc:
        _raise(
            "QE alpha generator economic outcomes cannot be projected",
            "ADVISORY_QE_ALPHA_GENERATOR_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    frame = frame.loc[frame["arm_id"].astype(str) == CURRENT_PARENT_ARM_ID].copy()
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str).str.upper()
    frame = frame.loc[
        frame["decision_as_of_trade_date"].between(pd.Timestamp(request.signal_start), pd.Timestamp(request.signal_end))
    ]
    if frame.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise("QE alpha generator economic outcome keys are duplicated", "ADVISORY_QE_ALPHA_GENERATOR_PIT_LEAKAGE")
    return frame.sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True)


def _benchmark_from_parent_request(parent_request: Mapping[str, Any]) -> str:
    for ref in parent_request.get("evidence_refs", ()):
        if ref.get("role") == "n3_n1_oracle_receipt":
            n1 = _read_json(
                _local_path(ref["artifact_uri"]).parent / "request.json", "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID"
            )
            benchmark = str(n1.get("cost_policy", {}).get("benchmark_instrument", ""))
            if benchmark:
                return benchmark
    _raise("QE alpha generator benchmark identity is missing", "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
    raise AssertionError


def _catalog_prompt_rows(
    catalog: Mapping[str, Any],
    family: str,
    *,
    allowed_fields: Sequence[str],
    limit: int,
) -> list[dict[str, Any]]:
    prefixes = {
        "PRICE_VOLUME_BEHAVIOR": ("db_", "liquidity", "PriceStrength"),
        "MONEYFLOW_BEHAVIOR": ("mf_",),
        "FUNDAMENTAL_CHANGE": ("bb_", "value_"),
        "SECTOR_RELATIVE": ("sw2_",),
        "CROWDING_DISPERSION": ("db_", "cp_", "size_", "liquidity"),
        "REGIME_CONDITIONED": ("sw2_", "db_", "mf_", "value_"),
    }[family]
    forbidden_fields = set(GENERATOR_ALLOWED_FIELDS) - set(allowed_fields)
    selected: list[dict[str, Any]] = []
    for record in catalog.get("records", ()):
        blob = json.dumps(
            {"variables": record.get("variables"), "data_source": record.get("data_source")}, ensure_ascii=False
        )
        if not any(prefix in blob for prefix in prefixes):
            continue
        metadata_blob = json.dumps(
            {key: record.get(key) for key in ("factor_name", "variables", "formula_hint", "data_source")},
            ensure_ascii=False,
        )
        if any(
            re.search(rf"(?<![A-Za-z0-9_]){re.escape(field)}(?![A-Za-z0-9_])", metadata_blob)
            for field in forbidden_fields
        ):
            continue
        selected.append(
            {
                key: record.get(key)
                for key in ("factor_name", "source", "variables", "formula_hint", "factor_type", "data_source")
            }
        )
        if len(selected) >= limit:
            break
    return selected


def _catalog_text_keys(catalog: Mapping[str, Any]) -> set[str]:
    keys: set[str] = set()
    for record in catalog.get("records", ()):
        for name in ("factor_name", "expression", "formula_hint", "factor_formulation"):
            value = _normalized_text(record.get(name))
            if value:
                keys.add(value)
    return keys


def _transcript_row(
    family: str,
    attempt_index: int,
    system_prompt: str,
    user_prompt: str,
    raw_response: str,
) -> dict[str, Any]:
    return {
        "family": family,
        "attempt_index": attempt_index,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "raw_response": raw_response,
        "response_sha256": hashlib.sha256(raw_response.encode("utf-8")).hexdigest(),
    }


def _unresolved_generation_families(attempts: Sequence[Mapping[str, Any]]) -> list[str]:
    latest: dict[str, Mapping[str, Any]] = {}
    for item in attempts:
        family = str(item.get("family") or "")
        if family in MVE_FAMILIES:
            latest[family] = item
    return [family for family in MVE_FAMILIES if latest.get(family, {}).get("status") == "CALL_FAILED"]


def _has_generation_recovery_capacity(
    attempts: Sequence[Mapping[str, Any]], request: FrozenAdvisoryQEAlphaGeneratorRequestV1
) -> bool:
    if len(attempts) >= request.max_generation_calls:
        return False
    for family in _unresolved_generation_families(attempts):
        if sum(1 for item in attempts if item.get("family") == family) < 2:
            return True
    return False


def _attempt_row(
    family: str,
    attempt_index: int,
    status: str,
    system_prompt: str,
    user_prompt: str,
    response: str,
    telemetry: Mapping[str, Any],
    raw_attempt_count: int,
    violations: Sequence[str],
    elapsed: float,
) -> dict[str, Any]:
    return {
        "family": family,
        "attempt_index": attempt_index,
        "status": status,
        "system_prompt_sha256": hashlib.sha256(system_prompt.encode("utf-8")).hexdigest(),
        "user_prompt_sha256": hashlib.sha256(user_prompt.encode("utf-8")).hexdigest(),
        "response_sha256": hashlib.sha256(response.encode("utf-8")).hexdigest() if response else None,
        "telemetry": dict(telemetry),
        "raw_generation_attempt_count": raw_attempt_count,
        "violations": list(violations),
        "elapsed_seconds": elapsed,
        "target_or_economic_metric_exposed": False,
    }


def _read_catalog_snapshot(path: Path) -> dict[str, Any]:
    payload = _read_json(path, "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
    supplied = payload.pop("snapshot_sha256", None)
    snapshot_id = payload.pop("snapshot_id", None)
    digest = canonical_json_sha256(payload)
    payload["snapshot_sha256"] = supplied
    payload["snapshot_id"] = snapshot_id
    if (
        payload.get("schema_version") != CATALOG_SNAPSHOT_SCHEMA
        or supplied != digest
        or snapshot_id != f"advqegencat_{digest[:24]}"
    ):
        _raise("QE alpha generator catalog snapshot identity is invalid", "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
    if payload.get("performance_fields_included") or payload.get("secret_fields_included"):
        _raise(
            "QE alpha generator catalog snapshot includes forbidden fields",
            "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID",
        )
    if payload.get("row_count") != len(payload.get("records", ())):
        _raise("QE alpha generator catalog snapshot row count drift", "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID")
    return payload


def _allowed_fields_from_parent_source(
    parent_request: Mapping[str, Any],
    *,
    old_source_fields: tuple[str, ...],
) -> tuple[str, ...]:
    try:
        static_ref = EvidenceReferenceV1.model_validate(parent_request["static_factor_ref"])
        expected_schema_sha256 = str(parent_request["static_schema_sha256"])
    except (KeyError, ValidationError, ValueError) as exc:
        _raise(
            "QE alpha generator parent static source contract is invalid",
            "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    if static_ref.role != "n3_static_factors_parquet":
        _raise(
            "QE alpha generator parent static source role is invalid",
            "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID",
            role=static_ref.role,
        )
    static_path = _local_path(static_ref.artifact_uri)
    try:
        actual_ref = evidence_reference_for_file(static_path, role=static_ref.role)
        import pyarrow.parquet as pq

        schema = pq.ParquetFile(static_path).schema_arrow
        actual_schema_sha256 = _static_schema_sha256(static_path)
    except Exception as exc:
        _raise(
            "QE alpha generator concrete static source cannot be read",
            "ADVISORY_QE_ALPHA_GENERATOR_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
            path=static_path.as_posix(),
        )
    if (actual_ref.sha256, actual_ref.size_bytes) != (
        static_ref.sha256,
        static_ref.size_bytes,
    ) or actual_schema_sha256 != expected_schema_sha256:
        _raise(
            "QE alpha generator concrete static source identity drift",
            "ADVISORY_QE_ALPHA_GENERATOR_SOURCE_IDENTITY_MISMATCH",
            path=static_path.as_posix(),
        )
    try:
        return generator_allowed_fields_for_source(
            available_static_fields=frozenset(schema.names),
            old_source_fields=old_source_fields,
        )
    except ValueError as exc:
        _raise(
            "QE alpha generator concrete static source omits required fields",
            "ADVISORY_QE_ALPHA_GENERATOR_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    raise AssertionError


def _verify_request_source_fields(
    request: FrozenAdvisoryQEAlphaGeneratorRequestV1,
    *,
    old_source_fields: tuple[str, ...],
) -> None:
    parent_request = _read_json(
        _local_path(request.parent_qe_bundle_path) / "request.json",
        "ADVISORY_QE_ALPHA_GENERATOR_SOURCE_IDENTITY_MISMATCH",
    )
    actual_allowed_fields = _allowed_fields_from_parent_source(
        parent_request,
        old_source_fields=old_source_fields,
    )
    if request.allowed_fields != actual_allowed_fields:
        _raise(
            "QE alpha generator frozen allowed fields do not match the concrete source schema",
            "ADVISORY_QE_ALPHA_GENERATOR_SOURCE_IDENTITY_MISMATCH",
            missing_fields=sorted(set(request.allowed_fields) - set(actual_allowed_fields)),
            unexpected_fields=sorted(set(actual_allowed_fields) - set(request.allowed_fields)),
        )


def _load_request(path: str | Path) -> FrozenAdvisoryQEAlphaGeneratorRequestV1:
    try:
        return FrozenAdvisoryQEAlphaGeneratorRequestV1.model_validate_json(
            _local_path(path).read_text(encoding="utf-8")
        )
    except (OSError, ValidationError, ValueError) as exc:
        _raise(
            "QE alpha generator request is invalid",
            "ADVISORY_QE_ALPHA_GENERATOR_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    raise AssertionError


def _git_commit(root: Path) -> str:
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=True)
    commit = result.stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("repository commit is invalid")
    status = subprocess.run(["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True, check=True)
    if status.stdout.strip():
        raise ValueError("repository must be clean before freezing generator request")
    return commit


def _member_descriptors(root: Path, *, exclude: set[str] | None = None) -> dict[str, dict[str, Any]]:
    excluded = exclude or set()
    return {
        path.name: _file_descriptor(path)
        for path in sorted(root.iterdir(), key=lambda item: item.name)
        if path.is_file() and path.name not in excluded
    }


def _file_descriptor(path: Path) -> dict[str, Any]:
    return {"sha256": _sha256_file(path), "size_bytes": path.stat().st_size}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False),
        encoding="utf-8",
    )


def _write_immutable_json(path: Path, payload: Any, reason_code: str) -> None:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            _raise("immutable QE alpha generator file conflicts", reason_code, path=path.as_posix())
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(encoded, encoding="utf-8")
    os.replace(temporary, path)


def _write_atomic_text(path: Path, value: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _read_json(path: Path, reason_code: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        _raise(
            "QE alpha generator JSON cannot be read", reason_code, path=path.as_posix(), error_type=type(exc).__name__
        )
    if not isinstance(payload, dict):
        _raise("QE alpha generator JSON root is not an object", reason_code, path=path.as_posix())
    return payload


def _directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _finite_array(values: Sequence[Any] | pd.Series | np.ndarray) -> np.ndarray:
    array = np.asarray(pd.to_numeric(pd.Series(values), errors="coerce"), dtype=float)
    return array[np.isfinite(array)]


def _finite_mean(values: Sequence[Any] | pd.Series | np.ndarray) -> float | None:
    array = _finite_array(values)
    return float(array.mean()) if len(array) else None


def _optional_text(value: Any) -> str | None:
    return None if value in (None, "") else str(value)


def _normalized_text(value: Any) -> str:
    return re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", "", str(value or "").lower())


def _safe_error(exc: Exception) -> str:
    text = re.sub(
        r"(?i)(api[_-]?key|authorization|password|token|secret)\s*[:=]\s*\S+",
        r"\1=<redacted>",
        str(exc),
    )
    text = re.sub(r"(?i)\bbearer\s+\S+", "Bearer <redacted>", text)
    text = re.sub(r"\b(?:sk|ds)-[A-Za-z0-9_-]{12,}\b", "<redacted-token>", text)
    return f"{type(exc).__name__}:{text[:500]}"


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message=message, reason_code=reason_code, context=context)


__all__ = [
    "build_catalog_snapshot",
    "build_family_prompt",
    "evaluate_generated_overlays",
    "expression_fingerprint",
    "generate_alpha_candidates",
    "generation_support_reasons",
    "inspect_generation_bundle",
    "inspect_generator_mve_bundle",
    "preliminary_originality_reasons",
    "prepare_generator_request",
    "run_generator_mve",
    "snapshot_dev_factor_catalog",
    "target_free_score_overlap",
    "weighted_jaccard",
]
