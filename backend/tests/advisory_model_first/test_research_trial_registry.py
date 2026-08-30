from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    resolve_evidence_reference,
)
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)


HASH_A = "a" * 64


def _record(index: int = 0, **overrides):
    values = {
        "experiment_id": f"EXPERIMENT-{index}",
        "attempt_id": "formal-v1",
        "research_stage": "STAGE_A",
        "study_type": ResearchStudyType.CANDIDATE_MODEL,
        "hypothesis_family_id": "family-v1",
        "parent_lineage": ("parent",),
        "unique_variable": f"variable-{index}",
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "dataset_identity": "dataset-v1",
        "schema_identity": "schema-v1",
        "policy_identity": HASH_A,
        "planned_trial_count": 1,
        "generated_trial_count": 1,
        "evaluated_trial_count": 1,
        "selected_trial_count": 0,
        "consumed_windows": (),
        "result_class": ResearchResultClass.NEGATIVE,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "evidence_refs": (
            EvidenceReferenceV1(
                role="manifest",
                artifact_uri=f"F:/fixture/{index}.json",
                sha256=HASH_A,
                size_bytes=10,
            ),
        ),
    }
    values.update(overrides)
    return build_trial_record(**values)


def test_registry_append_is_idempotent_and_strictly_append_only(tmp_path):
    path = tmp_path / "registry.jsonl"
    registry = AdvisoryResearchTrialRegistryV1(path)
    record = _record()

    first = registry.append_batch((record,))
    original = path.read_bytes()
    second = registry.append_batch((record,))

    assert first["appended_count"] == 1
    assert second["appended_count"] == 0
    assert second["duplicate_noop_count"] == 1
    assert path.read_bytes() == original
    assert registry.read() == (record,)


def test_registry_rejects_experiment_identity_drift_without_partial_append(tmp_path):
    path = tmp_path / "registry.jsonl"
    registry = AdvisoryResearchTrialRegistryV1(path)
    first = _record(1)
    drifted = _record(1, attempt_id="retry-v2", dataset_identity="other-dataset")

    with pytest.raises(AdvisoryModelFirstError) as captured:
        registry.append_batch((first, drifted))

    assert captured.value.reason_code == "ADVISORY_RESEARCH_REGISTRY_CONFLICT"
    assert not path.exists()


def test_registry_rejects_post_hoc_unique_variable_or_lineage_drift(tmp_path):
    path = tmp_path / "registry.jsonl"
    registry = AdvisoryResearchTrialRegistryV1(path)
    registry.append_batch((_record(1),))

    drifted = _record(
        1,
        attempt_id="retry-v2",
        unique_variable="post-hoc-loss-change",
        parent_lineage=("different-parent",),
    )
    with pytest.raises(AdvisoryModelFirstError) as captured:
        registry.append_batch((drifted,))

    assert captured.value.reason_code == "ADVISORY_RESEARCH_REGISTRY_CONFLICT"
    assert len(registry.read()) == 1


def test_registry_rejects_multiple_results_for_one_attempt_stage(tmp_path):
    path = tmp_path / "registry.jsonl"
    registry = AdvisoryResearchTrialRegistryV1(path)
    registry.append_batch((_record(2),))
    changed_result = _record(2, result_class=ResearchResultClass.REJECTED)

    with pytest.raises(AdvisoryModelFirstError) as captured:
        registry.append_batch((changed_result,))

    assert captured.value.reason_code == "ADVISORY_RESEARCH_REGISTRY_CONFLICT"
    assert len(registry.read()) == 1


def test_navigation_only_artifact_cannot_be_reintroduced_as_activation_evidence(tmp_path):
    path = tmp_path / "registry.jsonl"
    registry = AdvisoryResearchTrialRegistryV1(path)
    registry.append_batch((_record(7),))
    activation = _record(
        7,
        experiment_id="ACTIVATION-7",
        study_type=ResearchStudyType.ACTIVATION,
        decision_use=DecisionUse.ACTIVATION_EVIDENCE,
        result_class=ResearchResultClass.ACTIVATED,
    )

    with pytest.raises(AdvisoryModelFirstError) as captured:
        registry.append_batch((activation,))

    assert captured.value.reason_code == "ADVISORY_RESEARCH_REGISTRY_CONFLICT"
    assert len(registry.read()) == 1


def test_evidence_uri_content_identity_cannot_drift_between_records(tmp_path):
    path = tmp_path / "registry.jsonl"
    registry = AdvisoryResearchTrialRegistryV1(path)
    registry.append_batch((_record(8),))
    drifted_ref = EvidenceReferenceV1(
        role="manifest",
        artifact_uri="F:/fixture/8.json",
        sha256="b" * 64,
        size_bytes=11,
    )

    with pytest.raises(AdvisoryModelFirstError) as captured:
        registry.append_batch((_record(9, evidence_refs=(drifted_ref,)),))

    assert captured.value.reason_code == "ADVISORY_RESEARCH_REGISTRY_CONFLICT"


@pytest.mark.parametrize(
    "payload",
    (
        b'{"truncated":true}',
        b"not-json\n",
        b"\n",
    ),
)
def test_registry_rejects_truncated_corrupt_or_blank_jsonl(tmp_path, payload):
    path = tmp_path / "registry.jsonl"
    path.write_bytes(payload)

    with pytest.raises(AdvisoryModelFirstError) as captured:
        AdvisoryResearchTrialRegistryV1(path).read()

    assert captured.value.reason_code == "ADVISORY_RESEARCH_REGISTRY_INVALID"


def test_registry_concurrent_append_never_interleaves_json_lines(tmp_path):
    path = tmp_path / "registry.jsonl"

    def append(index: int) -> None:
        AdvisoryResearchTrialRegistryV1(path).append_batch((_record(index),))

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(append, range(24)))

    records = AdvisoryResearchTrialRegistryV1(path).read()
    raw_lines = path.read_text(encoding="utf-8").splitlines()
    assert len(records) == len(raw_lines) == 24
    assert {json.loads(line)["experiment_id"] for line in raw_lines} == {
        f"EXPERIMENT-{index}" for index in range(24)
    }


def test_evidence_resolution_hashes_file_and_rejects_root_escape(tmp_path):
    root = tmp_path / "artifacts"
    root.mkdir()
    evidence = root / "manifest.json"
    evidence.write_text('{"status":"negative"}\n', encoding="utf-8")

    resolved = resolve_evidence_reference(
        artifact_root=root, relative_path="manifest.json", role="manifest"
    )
    assert resolved.size_bytes == evidence.stat().st_size
    assert resolved.sha256 != HASH_A

    with pytest.raises(AdvisoryModelFirstError) as captured:
        resolve_evidence_reference(
            artifact_root=root, relative_path="../outside.json", role="escape"
        )
    assert captured.value.reason_code == "ADVISORY_RESEARCH_EVIDENCE_MISSING"
