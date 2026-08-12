from backend.services.dataset_release.retention import (
    DatasetReferenceState,
    RetentionClass,
    classify_dataset_retention,
)


def test_experiment_referenced_release_retains_complete_dataset_not_only_all_txt() -> None:
    decision = classify_dataset_retention(DatasetReferenceState(experiment_referenced=True, published=True))
    assert decision.retention_class is RetentionClass.FULL_IMMUTABLE
    assert decision.retain_complete_dataset is True
    assert decision.retain_all_txt is True
    assert decision.retain_pit_snapshot is True
    assert decision.retain_manifests_and_receipts is True
    assert decision.automatic_deletion_allowed is False
    assert decision.as_dict()["retain_complete_dataset"] is True


def test_unreferenced_failed_candidate_is_only_a_cleanup_candidate() -> None:
    decision = classify_dataset_retention(
        DatasetReferenceState(terminal_failure=True, reference_absence_proven=True)
    )
    assert decision.retention_class is RetentionClass.METADATA_ONLY_CLEANUP_CANDIDATE
    assert decision.retain_complete_dataset is False
    assert decision.retain_manifests_and_receipts is True
    assert decision.automatic_deletion_allowed is False


def test_unknown_reference_state_fails_closed_to_full_retention() -> None:
    decision = classify_dataset_retention(DatasetReferenceState(terminal_failure=True))
    assert decision.retention_class is RetentionClass.FULL_IMMUTABLE
    assert decision.retain_complete_dataset is True
    assert decision.reason_codes == ("reference_state_unsettled",)
