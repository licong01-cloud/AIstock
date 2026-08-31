"""Versioned source-role ownership for Phase 1R R3 consumers.

Candidate inference and decision marks consume different source members from
the same sealed catalog.  Keeping this selector here prevents broad
package/component matching from accidentally giving a candidate artifact a
mark lineage, or vice versa.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from backend.services.advisory_historical_range.models import (
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeSourceRevisionMemberV1,
)


CANDIDATE_SOURCE_ROLES_V2 = frozenset(
    {
        "code_release",
        "package_runtime_assets",
        "pit_universe",
        "trading_calendar",
        "st_risk",
        "suspend",
        "industry",
        "hmm_frozen_evidence",
        "market_history",
        "fundamental_moneyflow",
    }
)
DECISION_MARK_SOURCE_ROLES_V1 = frozenset(
    {"decision_mark_daily_market", "decision_mark_market_state"}
)
REQUEST_SEAL_SOURCE_ROLES = frozenset({"code_release", "package_runtime_assets"})
_KNOWN_SOURCE_ROLES = CANDIDATE_SOURCE_ROLES_V2 | DECISION_MARK_SOURCE_ROLES_V1


@dataclass(frozen=True)
class HistoricalRangeDaySourceRoleSelectionV1:
    """Exact selected members for one Program/date evidence consumer."""

    candidate_members: tuple[HistoricalRangeSourceRevisionMemberV1, ...]
    decision_mark_members: tuple[HistoricalRangeSourceRevisionMemberV1, ...]


def select_day_source_roles(
    *,
    catalog: HistoricalRangeSourceRevisionCatalogV1,
    research_program_id: str,
    package_id: str,
    component_ids: Iterable[str],
    decision_trade_date: date,
) -> HistoricalRangeDaySourceRoleSelectionV1:
    """Select the two disjoint R3 source sets and prove catalog exhaustiveness.

    The selection remains an evidence ownership contract.  It never decides
    whether a package is admitted or whether a candidate depth is sufficient.
    """

    normalized_program = str(research_program_id or "").strip()
    normalized_package = str(package_id or "").strip()
    normalized_components = frozenset(str(item or "").strip() for item in component_ids)
    if not normalized_program or not normalized_package or not normalized_components:
        raise ValueError("research_program_id, package_id, and component_ids are required")

    scoped = tuple(
        member
        for member in catalog.members
        if _matches_program_day(
            member,
            research_program_id=normalized_program,
            package_id=normalized_package,
            component_ids=normalized_components,
            decision_trade_date=decision_trade_date,
        )
    )
    unknown = sorted({member.source_role for member in scoped} - _KNOWN_SOURCE_ROLES)
    if unknown:
        raise ValueError(f"sealed catalog contains unknown R3 source roles: {unknown}")

    candidate = tuple(member for member in scoped if member.source_role in CANDIDATE_SOURCE_ROLES_V2)
    decision_mark = tuple(member for member in scoped if member.source_role in DECISION_MARK_SOURCE_ROLES_V1)
    candidate_ids = {member.requirement_id for member in candidate}
    mark_ids = {member.requirement_id for member in decision_mark}
    if candidate_ids & mark_ids:
        raise ValueError("candidate and decision-mark source selections must be disjoint")

    # Catalog members do not duplicate the requirement purpose.  The two
    # REQUEST_SEAL roles are the only non-day roles in the R3 role vocabulary;
    # every other selected member must be owned by exactly one day consumer.
    day_execution = tuple(member for member in scoped if member.source_role not in REQUEST_SEAL_SOURCE_ROLES)
    covered = candidate_ids | mark_ids
    uncovered = [
        member.requirement_id
        for member in day_execution
        if member.requirement_id not in covered
    ]
    if uncovered:
        raise ValueError(f"R3 source-role selection leaves DAY_EXECUTION members uncovered: {sorted(uncovered)}")
    if not decision_mark:
        raise ValueError("R3 decision-mark source selection is empty")
    return HistoricalRangeDaySourceRoleSelectionV1(
        candidate_members=tuple(sorted(candidate, key=lambda item: item.requirement_id)),
        decision_mark_members=tuple(sorted(decision_mark, key=lambda item: item.requirement_id)),
    )


def _matches_program_day(
    member: HistoricalRangeSourceRevisionMemberV1,
    *,
    research_program_id: str,
    package_id: str,
    component_ids: frozenset[str],
    decision_trade_date: date,
) -> bool:
    if member.decision_trade_date not in {None, decision_trade_date}:
        return False
    if member.package_id not in {None, package_id}:
        return False
    if member.component_id not in {None, *component_ids}:
        return False
    if member.source_role == "hmm_frozen_evidence":
        selector = (member.bound_parameters or {}).get("selector")
        return isinstance(selector, dict) and str(selector.get("research_program_id") or "") == research_program_id
    return True
