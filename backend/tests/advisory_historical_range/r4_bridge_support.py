"""Shared R4 bridge builders retained after duplicate maturity scenarios were removed."""

from __future__ import annotations

from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeLineageIdentity,
    HistoricalRangeOutcomePolicyBundleV1,
    HistoricalRangePolicyComponentV1,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge import _ref


_COMPONENT_HASHES = {
    role: character * 64
    for role, character in zip(
        ("BARRIER", "BENCHMARK", "CALENDAR", "CASH_RETURN", "CORPORATE_ACTION", "COST", "EXECUTION", "MARKET_DATA", "TERMINAL"),
        "abcdefabc",
        strict=True,
    )
}


def _policy_with_projections(projections: tuple[str, ...]) -> HistoricalRangeOutcomePolicyBundleV1:
    return HistoricalRangeOutcomePolicyBundleV1(
        package_id="pkg-1",
        manifest_sha256="1" * 64,
        alpha_mode="single_alpha",
        style_family="TREND",
        style_resolution_reason="FROZEN_TEST_POLICY",
        calendar_version="calendar-v1",
        calendar_hash=_COMPONENT_HASHES["CALENDAR"],
        components=tuple(
            HistoricalRangePolicyComponentV1(
                component_role=role,
                component_ref=f"components/{role.lower()}-v1",
                component_hash=_COMPONENT_HASHES[role],
            )
            for role in sorted(_COMPONENT_HASHES)
        ),
        horizons=(1,),
        projections_by_horizon={1: projections},
        candidate_reference_notional="100000",
        benchmark_portfolio_notional="100000",
    )


def _lineage_for(candidate_ref: HistoricalRangeArtifactRefV1, *, day_run_id: str) -> HistoricalRangeLineageIdentity:
    return HistoricalRangeLineageIdentity(
        historical_range_request_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "a"),
        historical_range_frozen_program_ref=_ref(HistoricalRangeArtifactKind.FROZEN_PROGRAM, "f"),
        range_run_id="run-1",
        range_day_run_id=day_run_id,
        candidate_artifact_ref=candidate_ref,
        package_id="pkg-1",
        manifest_sha256="1" * 64,
        code_release_hash="2" * 64,
        signal_source_revision_set_hash="3" * 64,
        oos_interval_hash="4" * 64,
    )
