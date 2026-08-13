from __future__ import annotations

import hashlib

import pytest

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SNAPSHOT_PREFIX,
    CANONICAL_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
    canonical_rule_parameters_digest,
)
from backend.services.canonical_pit_dataset_consumer import (
    CanonicalPitDatasetConsumerError,
    FormalDatasetUsage,
    require_formal_dataset_pit_identity,
)
from backend.services.dataset_release.cas_store import canonical_json_bytes
from backend.services.dataset_release.pit import (
    DATASET_CANDIDATE_MANIFEST_SCHEMA,
    DATASET_PIT_BINDING_SCHEMA,
)


SHA_A = "a" * 64


def _manifest(*, scope: str = "full") -> dict:
    release_id = "qe-hmm-v2-20260731"
    cutoff = "2026-07-31"
    return {
        "schema_version": DATASET_CANDIDATE_MANIFEST_SCHEMA,
        "release_id": release_id,
        "cutoff": cutoff,
        "scope": scope,
        "artifact_root": "b" * 64,
        "pit_binding": {
            "schema_version": DATASET_PIT_BINDING_SCHEMA,
            "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "authority_status": PitAuthorityStatus.ACTIVE_CANONICAL.value,
            "scope": scope,
            "rolling_universe_key": CANONICAL_PIT_UNIVERSE_KEY,
            "frozen_universe_key": f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{release_id}",
            "rule_version": CANONICAL_PIT_RULE_VERSION,
            "rule_parameters_digest": canonical_rule_parameters_digest(),
            "cutoff": cutoff,
            "rolling_cutoff_spans_sha256": SHA_A,
            "frozen_snapshot_digest": SHA_A,
            "release_id": release_id,
        },
    }


def _digest(value: dict) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


@pytest.mark.parametrize("usage_mode", list(FormalDatasetUsage))
def test_full_v2_manifest_returns_one_formal_identity_without_database_access(monkeypatch, usage_mode) -> None:
    import backend.services.canonical_equity_pit as canonical_pit

    monkeypatch.setattr(
        canonical_pit,
        "get_conn",
        lambda *args, **kwargs: pytest.fail("formal frozen consumer must not query the online PIT database"),
    )
    manifest = _manifest()

    identity = require_formal_dataset_pit_identity(
        manifest,
        usage_mode=usage_mode,
        expected_manifest_digest=_digest(manifest),
    )

    assert identity.as_dict() == {
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
        "rule_version": CANONICAL_PIT_RULE_VERSION,
        "rule_parameters_digest": canonical_rule_parameters_digest(),
        "release_id": "qe-hmm-v2-20260731",
        "cutoff": "2026-07-31",
        "frozen_snapshot_digest": SHA_A,
        "manifest_digest": _digest(manifest),
    }


def test_sample_manifest_cannot_drive_formal_consumers() -> None:
    manifest = _manifest(scope="sample")

    with pytest.raises(CanonicalPitDatasetConsumerError, match="sample PIT binding cannot drive"):
        require_formal_dataset_pit_identity(
            manifest,
            usage_mode=FormalDatasetUsage.TRAINING,
            expected_manifest_digest=_digest(manifest),
        )


@pytest.mark.parametrize(
    "legacy_manifest",
    [
        {"schema_version": "dataset_release_candidate_manifest_v0"},
        {"schema_version": DATASET_PIT_BINDING_SCHEMA},
        {"universe_key": "shsz_st_pit_active_v1", "reproduction_mode": True},
    ],
)
def test_legacy_v1_and_bare_bindings_are_rejected(legacy_manifest) -> None:
    with pytest.raises(CanonicalPitDatasetConsumerError, match="legacy/v1 manifests are forbidden"):
        require_formal_dataset_pit_identity(
            legacy_manifest,
            usage_mode=FormalDatasetUsage.PREDICTION,
            expected_manifest_digest=_digest(legacy_manifest),
        )


def test_manifest_digest_drift_is_rejected_before_binding_use() -> None:
    manifest = _manifest()
    expected_digest = _digest(manifest)
    manifest["artifact_root"] = "c" * 64

    with pytest.raises(CanonicalPitDatasetConsumerError, match="digest differs from immutable reference"):
        require_formal_dataset_pit_identity(
            manifest,
            usage_mode=FormalDatasetUsage.TRAINING,
            expected_manifest_digest=expected_digest,
        )


def test_canonical_pit_binding_tamper_is_rejected() -> None:
    manifest = _manifest()
    manifest["pit_binding"]["frozen_snapshot_digest"] = "d" * 64

    with pytest.raises(CanonicalPitDatasetConsumerError, match="rolling/frozen PIT digests differ"):
        require_formal_dataset_pit_identity(
            manifest,
            usage_mode=FormalDatasetUsage.PREDICTION,
            expected_manifest_digest=_digest(manifest),
        )


def test_unknown_usage_and_invalid_expected_digest_are_typed_failures() -> None:
    manifest = _manifest()
    with pytest.raises(CanonicalPitDatasetConsumerError, match="unsupported formal dataset usage_mode"):
        require_formal_dataset_pit_identity(
            manifest,
            usage_mode="reproduction",
            expected_manifest_digest=_digest(manifest),
        )
    with pytest.raises(CanonicalPitDatasetConsumerError, match="expected_manifest_digest"):
        require_formal_dataset_pit_identity(
            manifest,
            usage_mode=FormalDatasetUsage.TRAINING,
            expected_manifest_digest="not-a-digest",
        )


def test_non_mapping_manifest_is_a_typed_failure() -> None:
    with pytest.raises(CanonicalPitDatasetConsumerError, match="release_manifest must be a mapping"):
        require_formal_dataset_pit_identity(  # type: ignore[arg-type]
            [],
            usage_mode=FormalDatasetUsage.TRAINING,
            expected_manifest_digest=SHA_A,
        )
