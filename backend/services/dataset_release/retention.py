"""Fail-closed retention policy for immutable dataset releases.

The policy classifies releases only.  It never deletes files; cleanup remains
an exact, separately authorized maintenance action.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RetentionClass(StrEnum):
    FULL_IMMUTABLE = "FULL_IMMUTABLE"
    METADATA_ONLY_CLEANUP_CANDIDATE = "METADATA_ONLY_CLEANUP_CANDIDATE"


@dataclass(frozen=True, slots=True)
class DatasetReferenceState:
    experiment_referenced: bool = False
    training_referenced: bool = False
    production_activated: bool = False
    audit_hold: bool = False
    published: bool = False
    terminal_failure: bool = False
    reference_absence_proven: bool = False


@dataclass(frozen=True, slots=True)
class RetentionDecision:
    retention_class: RetentionClass
    retain_complete_dataset: bool
    retain_all_txt: bool
    retain_pit_snapshot: bool
    retain_manifests_and_receipts: bool
    automatic_deletion_allowed: bool
    reason_codes: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "retention_class": self.retention_class.value,
            "retain_complete_dataset": self.retain_complete_dataset,
            "retain_all_txt": self.retain_all_txt,
            "retain_pit_snapshot": self.retain_pit_snapshot,
            "retain_manifests_and_receipts": self.retain_manifests_and_receipts,
            "automatic_deletion_allowed": self.automatic_deletion_allowed,
            "reason_codes": list(self.reason_codes),
        }


def classify_dataset_retention(state: DatasetReferenceState) -> RetentionDecision:
    reasons = tuple(
        name
        for name, enabled in (
            ("experiment_referenced", state.experiment_referenced),
            ("training_referenced", state.training_referenced),
            ("production_activated", state.production_activated),
            ("audit_hold", state.audit_hold),
            ("published", state.published),
        )
        if enabled
    )
    if reasons:
        return RetentionDecision(
            retention_class=RetentionClass.FULL_IMMUTABLE,
            retain_complete_dataset=True,
            retain_all_txt=True,
            retain_pit_snapshot=True,
            retain_manifests_and_receipts=True,
            automatic_deletion_allowed=False,
            reason_codes=reasons,
        )
    if state.terminal_failure and state.reference_absence_proven:
        return RetentionDecision(
            retention_class=RetentionClass.METADATA_ONLY_CLEANUP_CANDIDATE,
            retain_complete_dataset=False,
            retain_all_txt=True,
            retain_pit_snapshot=True,
            retain_manifests_and_receipts=True,
            automatic_deletion_allowed=False,
            reason_codes=("unreferenced_terminal_failure", "exact_cleanup_authorization_required"),
        )
    # Unknown/unsettled state retains everything.  A terminal failure alone
    # is not proof that no experiment, audit, or operator hold references it.
    return RetentionDecision(
        retention_class=RetentionClass.FULL_IMMUTABLE,
        retain_complete_dataset=True,
        retain_all_txt=True,
        retain_pit_snapshot=True,
        retain_manifests_and_receipts=True,
        automatic_deletion_allowed=False,
        reason_codes=("reference_state_unsettled",),
    )


__all__ = [
    "DatasetReferenceState",
    "RetentionClass",
    "RetentionDecision",
    "classify_dataset_retention",
]
