from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.qe_alpha_generator_contracts import (
    GENERATOR_ALLOWED_FIELDS,
    GENERATOR_PROMPT_SCHEMA_V1,
    GENERATOR_PROMPT_SCHEMA_V2,
    QEAlphaGeneratorModelIdentityV1,
    build_generation_receipt,
    build_generator_mve_receipt,
    build_generator_proposal,
    build_generator_request,
    generator_allowed_fields_for_source,
)
from backend.services.advisory_model_first.qe_alpha_generator_pipeline import build_catalog_snapshot
from backend.services.advisory_model_first.qe_alpha_mve_contracts import (
    build_default_proposals,
    validate_expression,
)
from backend.services.advisory_model_first.research_control import evidence_reference_for_file


def make_generator_request(tmp_path, *, allowed_fields=None, prompt_schema_version=GENERATOR_PROMPT_SCHEMA_V2):
    parent = tmp_path / "parent"
    overlay = tmp_path / "overlay"
    minute = tmp_path / "minute"
    for directory in (parent, overlay, minute):
        directory.mkdir()
        (directory / "manifest.json").write_text("{}", encoding="utf-8")
    old = build_default_proposals()
    (parent / "proposal_roster.json").write_text(
        json.dumps({"proposals": [item.model_dump(mode="json") for item in old]}), encoding="utf-8"
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(build_catalog_snapshot([{"factor_name": "prior_factor", "source": "qe"}])),
        encoding="utf-8",
    )
    refs = (
        evidence_reference_for_file(parent / "manifest.json", role="n3_generator_parent_qe_manifest"),
        evidence_reference_for_file(overlay / "manifest.json", role="n3_generator_parent_overlay_manifest"),
        evidence_reference_for_file(minute / "manifest.json", role="n3_generator_minute_manifest"),
        evidence_reference_for_file(catalog_path, role="n3_generator_catalog_snapshot"),
        evidence_reference_for_file(parent / "proposal_roster.json", role="n3_generator_old_proposal_roster"),
    )
    return build_generator_request(
        parent_qe_bundle_path=parent.as_posix(),
        parent_overlay_bundle_path=overlay.as_posix(),
        minute_bundle_path=minute.as_posix(),
        catalog_snapshot_path=catalog_path.as_posix(),
        evidence_refs=refs,
        factor_root=(tmp_path / "factors").as_posix(),
        qlib_daily_root=(tmp_path / "qlib").as_posix(),
        n2b_bundle_path=(tmp_path / "n2b").as_posix(),
        outcomes_path=(tmp_path / "outcomes.parquet").as_posix(),
        dataset_identity="a" * 64,
        policy_identity="b" * 64,
        benchmark_instrument="000300.SH",
        old_expression_hashes=tuple(item.expression_sha256 for item in old),
        old_source_fields=tuple(sorted({field for item in old for field in item.source_fields})),
        allowed_fields=(tuple(sorted(GENERATOR_ALLOWED_FIELDS)) if allowed_fields is None else tuple(allowed_fields)),
        model_identity=QEAlphaGeneratorModelIdentityV1(),
        prompt_schema_version=prompt_schema_version,
        registry_path=(tmp_path / "registry.jsonl").as_posix(),
        route_path=(tmp_path / "route.md").as_posix(),
        repository_root=tmp_path.as_posix(),
        repository_commit="c" * 40,
        output_root=(tmp_path / "output").as_posix(),
    )


def test_generator_request_defaults_to_prompt_v2_and_keeps_v1_readable(tmp_path) -> None:
    current_root = tmp_path / "current"
    current_root.mkdir()
    current = make_generator_request(current_root)
    assert current.prompt_schema_version == GENERATOR_PROMPT_SCHEMA_V2

    legacy_root = tmp_path / "legacy"
    legacy_root.mkdir()
    legacy = make_generator_request(legacy_root, prompt_schema_version=GENERATOR_PROMPT_SCHEMA_V1)
    parsed = type(legacy).model_validate_json(legacy.model_dump_json())
    assert parsed.prompt_schema_version == GENERATOR_PROMPT_SCHEMA_V1
    assert parsed.request_sha256 == legacy.request_sha256


def test_generator_request_freezes_only_concrete_source_fields(tmp_path) -> None:
    base_root = tmp_path / "base"
    base_root.mkdir()
    base = make_generator_request(base_root)
    available_static = set(GENERATOR_ALLOWED_FIELDS) - {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "market_regime",
    }
    available_static -= {"md_rqmcl", "md_rzye"}
    allowed = generator_allowed_fields_for_source(
        available_static_fields=available_static,
        old_source_fields=base.old_source_fields,
    )

    request_root = tmp_path / "filtered"
    request_root.mkdir()
    request = make_generator_request(request_root, allowed_fields=allowed)
    assert "md_rqmcl" not in request.allowed_fields
    assert "md_rzye" not in request.allowed_fields
    assert "db_turnover_rate" in request.allowed_fields

    with pytest.raises(ValueError, match="omits old proposal fields"):
        generator_allowed_fields_for_source(
            available_static_fields=available_static - {"db_turnover_rate"},
            old_source_fields=base.old_source_fields,
        )


def test_generator_expression_extends_schema_without_widening_old_contract() -> None:
    expression = {
        "op": "ADD",
        "args": [
            {"op": "FIELD", "field": "db_pe_ttm"},
            {"op": "FIELD", "field": "mf_lg_buy_amt"},
        ],
    }
    with pytest.raises(ValueError, match="not allowed"):
        validate_expression(expression)

    proposal = build_generator_proposal(
        proposal_id="N3G_FUNDAMENTAL_CHANGE_01",
        family="FUNDAMENTAL_CHANGE",
        economic_hypothesis="盈利估值与大单需求共同反映重估空间",
        mechanism="将时点可见估值与大单需求组合，避免只复刻单一价值暴露",
        known_effect_exposures=("VALUE", "LIQUIDITY"),
        expression=expression,
    )
    assert proposal.source_fields == ("db_pe_ttm", "mf_lg_buy_amt")
    assert set(proposal.source_fields).issubset(GENERATOR_ALLOWED_FIELDS)


def test_generator_contract_rejects_code_unknown_effect_and_identity_drift(tmp_path) -> None:
    with pytest.raises(ValueError, match="not implemented"):
        build_generator_proposal(
            proposal_id="N3G_PRICE_VOLUME_BEHAVIOR_01",
            family="PRICE_VOLUME_BEHAVIOR",
            economic_hypothesis="arbitrary code is forbidden",
            mechanism="this must be rejected before any execution can occur",
            known_effect_exposures=("MOMENTUM",),
            expression={"op": "PYTHON", "code": "import os"},
        )
    with pytest.raises(ValidationError, match="known-effect"):
        build_generator_proposal(
            proposal_id="N3G_PRICE_VOLUME_BEHAVIOR_01",
            family="PRICE_VOLUME_BEHAVIOR",
            economic_hypothesis="unknown claimed effect is forbidden",
            mechanism="the declaration must bind to the frozen known-effect roster",
            known_effect_exposures=("MAGIC",),
            expression={"op": "FIELD", "field": "db_pe_ttm"},
        )

    request = make_generator_request(tmp_path)
    payload = request.model_dump(mode="json")
    payload["max_raw_generation_attempts"] = 52
    with pytest.raises(ValidationError):
        type(request).model_validate(payload)


def test_generation_and_mve_receipts_bind_counts_and_next_task() -> None:
    generation = build_generation_receipt(
        request_sha256="a" * 64,
        generation_call_count=6,
        raw_generation_attempt_count=24,
        accepted_expression_count=20,
        rejected_expression_count=4,
        proposals_sha256="b" * 64,
    )
    assert generation.secret_persisted is False

    result = build_generator_mve_receipt(
        request_sha256="a" * 64,
        generation_receipt_sha256=generation.receipt_sha256,
        generated_trial_count=24,
        evaluated_trial_count=20,
        selected_trial_count=0,
        selected_proposal_id=None,
        eligible_proposal_ids=(),
        next_task="N3_UPSTREAM_ALPHA_NEW_DATA_SOURCE_MVE_DESIGN",
        source_identity_sha256="c" * 64,
        result_files_sha256="d" * 64,
        resource_report_sha256="e" * 64,
    )
    payload = result.model_dump(mode="json")
    payload["next_task"] = "N3_QE_ALPHA_GENERATOR_CANDIDATE_CONFIRMATION_DESIGN"
    with pytest.raises(ValidationError, match="selection/next-task"):
        type(result).model_validate(payload)


def test_generation_receipt_separates_provider_failure_from_expression_attempts() -> None:
    failure = build_generation_receipt(
        request_sha256="a" * 64,
        generation_call_count=6,
        raw_generation_attempt_count=0,
        accepted_expression_count=0,
        rejected_expression_count=0,
        proposals_sha256="b" * 64,
        status="INFRASTRUCTURE_FAILURE",
        support_reason_codes=("LLM_PROVIDER_CALL_FAILURE", "TOTAL_ACCEPTED_EXPRESSION_SUPPORT_INSUFFICIENT"),
    )
    assert failure.status == "INFRASTRUCTURE_FAILURE"
    assert failure.raw_generation_attempt_count == 0

    payload = failure.model_dump(mode="json")
    payload["status"] = "INCOMPLETE_SUPPORT"
    with pytest.raises(ValidationError, match="status/support"):
        type(failure).model_validate(payload)
