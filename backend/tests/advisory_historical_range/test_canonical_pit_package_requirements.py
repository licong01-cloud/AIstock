from __future__ import annotations

from backend.services.advisory_historical_range.models import (
    HistoricalRangeAdmittedPackageProjectionV1,
    HistoricalRangeFrozenProgramV1,
)
from backend.services.advisory_historical_range.requirement_planner import (
    HistoricalRangeSourceRequirementPlanner,
)
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    canonical_rule_parameters_digest,
)
from backend.services.strategy_package.advisory_input_projection import (
    CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH,
)
from backend.services.strategy_package.models import StrategyPackageCanonicalPitBindingV2
from backend.tests.advisory_historical_range.conftest import digest, resolved_request


def _canonical_program(program: HistoricalRangeFrozenProgramV1) -> HistoricalRangeFrozenProgramV1:
    binding = StrategyPackageCanonicalPitBindingV2(
        authority_id=CANONICAL_PIT_AUTHORITY_ID,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(),
        release_id="qe_hmm_full_v2_20260731",
        release_cutoff="2026-07-31",
        frozen_snapshot_digest="a" * 64,
        release_manifest_digest="b" * 64,
        qualification_method="REVALIDATED",
        qualification_evidence_digest="c" * 64,
    )
    projection_payload = program.admitted_package_projection.model_dump(mode="json")
    projection_payload["canonical_pit_binding"] = binding.model_dump(mode="json")
    projection = HistoricalRangeAdmittedPackageProjectionV1.model_validate(projection_payload)
    payload = program.model_dump(mode="json")
    payload.update(
        {
            "admitted_package_projection": projection.model_dump(mode="json"),
            "admitted_package_projection_hash": digest(projection.model_dump(mode="json")),
            "frozen_program_hash": None,
        }
    )
    return HistoricalRangeFrozenProgramV1.model_validate(payload)


def test_v2_package_requirements_use_frozen_manifest_universe_without_legacy_fallback() -> None:
    resolved = resolved_request()
    programs = tuple(_canonical_program(program) for program in resolved.frozen_programs)

    plan = HistoricalRangeSourceRequirementPlanner().build(
        request=resolved.request,
        date_plan=resolved.date_plan,
        frozen_programs=programs,
        calendar_identity_hash=digest("calendar"),
        code_release_hash=programs[0].code_release_hash,
    )

    expected_key = "aistock_equity_pit_snapshot_qe_hmm_full_v2_20260731"
    pit_bound = [
        item
        for item in plan.requirements
        if item.query_template_id
        in {
            "historical_pit_universe_existing_readonly",
            "historical_market_history_window",
            "historical_fundamental_moneyflow_window",
            "historical_decision_mark_market_state",
        }
    ]
    assert pit_bound
    assert all(item.parameter_template["universe_key"] == expected_key for item in pit_bound)
    assert plan.query_contract_hash == CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH
    assert all(
        item.package_id == programs[0].package_id
        for item in pit_bound
        if item.source_role == "decision_mark_market_state"
    )
