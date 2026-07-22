"""Canonical Phase 1R R3 list semantics.

The hash is derived from this versioned payload.  Callers cannot provide an
opaque list semantics hash, because that would make replay identity
unverifiable.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

from backend.services.advisory_historical_range.canonical import canonical_json_sha256


LIST_SEMANTICS_VERSION_V2 = "advisory_historical_range_list_semantics_v2"


class HistoricalRangeListSemanticsV2(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[LIST_SEMANTICS_VERSION_V2] = LIST_SEMANTICS_VERSION_V2
    action_priority: tuple[str, ...] = ("STOP_LOSS", "TIME_STOP", "RANK_DROP", "TAKE_PROFIT", "HOLD")
    rank_observation_policy: str = "historical_evidence_closed_observation_v2"
    valid_empty_policy: str = "retain_active_and_evaluate_marks_v1"
    mark_policy: str = "pit_decision_then_mature_mark_v1"
    adjustment_policy: str = "corporate_action_normalized_from_raw_v1"
    replacement_slot_policy: str = "rank_budget_slots_v1"
    guidance_policy: str = "range_end_unresolved_next_session_v1"
    deterministic_identity_schema: str = "historical_range_list_identity_v2"

    @field_validator(
        "rank_observation_policy",
        "valid_empty_policy",
        "mark_policy",
        "adjustment_policy",
        "replacement_slot_policy",
        "guidance_policy",
        "deterministic_identity_schema",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("list semantics fields must be non-empty")
        return normalized

    @property
    def semantics_hash(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))


def canonical_list_semantics_v2() -> HistoricalRangeListSemanticsV2:
    """Return the sole R3 baseline list semantics payload."""

    return HistoricalRangeListSemanticsV2()
