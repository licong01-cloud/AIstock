from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.dataset_release.contracts import ValidationCompatibility
from backend.services.dataset_release.errors import IdentityConflictError
from backend.services.dataset_release.fingerprints import (
    ComponentFingerprints,
    FingerprintChange,
    classify_fingerprint_change,
    fingerprint_dependency_files,
    fingerprint_payload,
    producer_fingerprint,
)


def _digest(label: str) -> str:
    return fingerprint_payload("test", {"label": label})


def _fingerprints(**changes: str) -> ComponentFingerprints:
    values = {
        "semantic_fingerprint": _digest("semantic"),
        "source_input_digest": _digest("source"),
        "producer_fingerprint": _digest("producer"),
        "artifact_fingerprint": _digest("artifact"),
        "validation_fingerprint": _digest("validation"),
        "resource_policy_digest": _digest("resource"),
    }
    values.update(changes)
    return ComponentFingerprints(**values)


def test_producer_fingerprint_hashes_only_declared_dependencies(tmp_path: Path) -> None:
    (tmp_path / "producer.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / "unrelated.md").write_text("notes\n", encoding="utf-8")
    dependencies = fingerprint_dependency_files(
        tmp_path,
        ["producer.py"],
        dirty_paths=["unrelated.md"],
        symbols={"producer.py": ["VALUE"]},
    )
    assert dependencies[0].path == "producer.py"
    assert len(producer_fingerprint("daily_bin", dependencies)) == 64

    with pytest.raises(IdentityConflictError, match="dirty path intersects"):
        fingerprint_dependency_files(
            tmp_path,
            ["producer.py"],
            dirty_paths=["producer.py"],
        )


def test_resource_policy_change_does_not_invalidate_data_identity() -> None:
    previous = _fingerprints()
    current = _fingerprints(resource_policy_digest=_digest("smaller-batch"))
    assert previous.data_identity_digest == current.data_identity_digest
    assert classify_fingerprint_change(previous, current) is FingerprintChange.RESOURCE_POLICY_ONLY


def test_validation_change_requires_explicit_compatibility() -> None:
    previous = _fingerprints()
    current = _fingerprints(validation_fingerprint=_digest("stronger-validator"))
    with pytest.raises(IdentityConflictError, match="explicit compatibility"):
        classify_fingerprint_change(previous, current)
    assert (
        classify_fingerprint_change(
            previous,
            current,
            validation_compatibility=(ValidationCompatibility.VALIDATOR_STRENGTHENING_COMPATIBLE),
        )
        is FingerprintChange.VALIDATOR_STRENGTHENING_COMPATIBLE
    )
    tuned = _fingerprints(
        validation_fingerprint=_digest("stronger-validator"),
        resource_policy_digest=_digest("smaller-batch"),
    )
    assert (
        classify_fingerprint_change(
            previous,
            tuned,
            validation_compatibility=(ValidationCompatibility.VALIDATOR_STRENGTHENING_COMPATIBLE),
        )
        is FingerprintChange.VALIDATOR_STRENGTHENING_COMPATIBLE
    )


def test_semantic_change_dominates_resource_change() -> None:
    previous = _fingerprints()
    current = _fingerprints(
        semantic_fingerprint=_digest("new-units"),
        resource_policy_digest=_digest("smaller-batch"),
    )
    assert classify_fingerprint_change(previous, current) is FingerprintChange.SEMANTIC_CONTRACT_CHANGED
